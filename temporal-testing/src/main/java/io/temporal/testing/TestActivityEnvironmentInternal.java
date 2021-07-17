/*
 *  Copyright (C) 2020 Temporal Technologies, Inc. All Rights Reserved.
 *
 *  Copyright 2012-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *  Modifications copyright (C) 2017 Uber Technologies, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"). You may not
 *  use this file except in compliance with the License. A copy of the License is
 *  located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 *  or in the "license" file accompanying this file. This file is distributed on
 *  an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *  express or implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 */

package io.temporal.testing;

import com.google.common.base.Defaults;
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import io.temporal.activity.Activity;
import io.temporal.activity.ActivityOptions;
import io.temporal.activity.LocalActivityOptions;
import io.temporal.api.common.v1.ActivityType;
import io.temporal.api.common.v1.Payloads;
import io.temporal.api.common.v1.WorkflowExecution;
import io.temporal.api.enums.v1.RetryState;
import io.temporal.api.workflowservice.v1.PollActivityTaskQueueResponse;
import io.temporal.api.workflowservice.v1.RecordActivityTaskHeartbeatRequest;
import io.temporal.api.workflowservice.v1.RecordActivityTaskHeartbeatResponse;
import io.temporal.api.workflowservice.v1.RespondActivityTaskCanceledRequest;
import io.temporal.api.workflowservice.v1.RespondActivityTaskCompletedRequest;
import io.temporal.api.workflowservice.v1.RespondActivityTaskFailedRequest;
import io.temporal.api.workflowservice.v1.WorkflowServiceGrpc;
import io.temporal.common.converter.EncodedValues;
import io.temporal.common.interceptors.WorkflowOutboundCallsInterceptor;
import io.temporal.failure.ActivityFailure;
import io.temporal.failure.CanceledFailure;
import io.temporal.failure.FailureConverter;
import io.temporal.internal.common.ProtobufTimeUtils;
import io.temporal.internal.sync.*;
import io.temporal.internal.worker.ActivityTask;
import io.temporal.internal.worker.ActivityTaskHandler;
import io.temporal.internal.worker.ActivityTaskHandler.Result;
import io.temporal.serviceclient.WorkflowServiceStubs;
import io.temporal.serviceclient.WorkflowServiceStubsOptions;
import io.temporal.workflow.Functions;
import io.temporal.workflow.Functions.Func;
import io.temporal.workflow.Promise;
import io.temporal.workflow.Workflow;
import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiPredicate;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class TestActivityEnvironmentInternal implements TestActivityEnvironment {
  private static final Logger log = LoggerFactory.getLogger(TestActivityEnvironmentInternal.class);

  private final POJOActivityTaskHandler activityTaskHandler;
  private final TestEnvironmentOptions testEnvironmentOptions;
  private final AtomicInteger idSequencer = new AtomicInteger();
  private ClassConsumerPair<Object> activityHeartbetListener;
  private static final ScheduledExecutorService heartbeatExecutor =
      Executors.newScheduledThreadPool(20);
  private static final ExecutorService activityWorkerExecutor =
      Executors.newSingleThreadExecutor(r -> new Thread(r, "test-service-activity-worker"));
  private final WorkflowServiceStubs workflowServiceStubs;
  private final Server mockServer;
  private final AtomicBoolean cancellationRequested = new AtomicBoolean();
  private final ManagedChannel channel;

  public TestActivityEnvironmentInternal(TestEnvironmentOptions options) {
    this.testEnvironmentOptions =
        TestEnvironmentOptions.newBuilder(options).validateAndBuildWithDefaults();

    // Initialize an in-memory mock service.
    String serverName = InProcessServerBuilder.generateName();
    try {
      mockServer =
          InProcessServerBuilder.forName(serverName)
              .directExecutor()
              .addService(new HeartbeatInterceptingService())
              .build()
              .start();
    } catch (IOException e) {
      // This should not happen with in-memory services, but rethrow just in case.
      throw new RuntimeException(e);
    }
    channel = InProcessChannelBuilder.forName(serverName).directExecutor().build();
    workflowServiceStubs =
        WorkflowServiceStubs.newInstance(
            WorkflowServiceStubsOptions.newBuilder()
                .setChannel(channel)
                .setMetricsScope(options.getMetricsScope())
                .setQueryRpcTimeout(Duration.ofSeconds(60))
                .setDisableHealthCheck(true)
                .build());
    activityTaskHandler =
        new POJOActivityTaskHandler(
            workflowServiceStubs,
            testEnvironmentOptions.getWorkflowClientOptions().getNamespace(),
            testEnvironmentOptions.getWorkflowClientOptions().getDataConverter(),
            heartbeatExecutor,
            testEnvironmentOptions.getWorkerFactoryOptions().getWorkerInterceptors());
  }

  private class HeartbeatInterceptingService extends WorkflowServiceGrpc.WorkflowServiceImplBase {
    @Override
    public void recordActivityTaskHeartbeat(
        RecordActivityTaskHeartbeatRequest request,
        StreamObserver<RecordActivityTaskHeartbeatResponse> responseObserver) {
      try {
        if (activityHeartbetListener != null) {
          Optional<Payloads> requestDetails =
              request.hasDetails() ? Optional.of(request.getDetails()) : Optional.empty();

          Object details =
              testEnvironmentOptions
                  .getWorkflowClientOptions()
                  .getDataConverter()
                  .fromPayloads(
                      0,
                      requestDetails,
                      activityHeartbetListener.valueClass,
                      activityHeartbetListener.valueType);
          activityHeartbetListener.consumer.apply(details);
        }
        responseObserver.onNext(
            RecordActivityTaskHeartbeatResponse.newBuilder()
                .setCancelRequested(cancellationRequested.get())
                .build());
        responseObserver.onCompleted();
      } catch (StatusRuntimeException e) {
        responseObserver.onError(e);
      }
    }
  }

  /**
   * Register activity implementation objects with a worker. Overwrites previously registered
   * objects. As activities are reentrant and stateless only one instance per activity type is
   * registered.
   *
   * <p>Implementations that share a worker must implement different interfaces as an activity type
   * is identified by the activity interface, not by the implementation.
   *
   * <p>
   */
  @Override
  public void registerActivitiesImplementations(Object... activityImplementations) {
    activityTaskHandler.registerActivityImplementations(activityImplementations);
  }

  /**
   * Creates client stub to activities that implement given interface.
   *
   * @param activityInterface interface type implemented by activities
   */
  @Override
  public <T> T newActivityStub(Class<T> activityInterface) {
    ActivityOptions options =
        ActivityOptions.newBuilder()
            .setScheduleToCloseTimeout(Duration.ofDays(1))
            .setHeartbeatTimeout(Duration.ofSeconds(1))
            .build();
    InvocationHandler invocationHandler =
        ActivityInvocationHandler.newInstance(
            activityInterface, options, null, new TestActivityExecutor());
    invocationHandler = new DeterministicRunnerWrapper(invocationHandler);
    return ActivityInvocationHandlerBase.newProxy(activityInterface, invocationHandler);
  }

  /**
   * Creates client stub to activities that implement given interface.
   *
   * @param activityInterface interface type implemented by activities
   * @param options options that specify the activity invocation parameters
   */
  @Override
  public <T> T newActivityStub(Class<T> activityInterface, ActivityOptions options) {
    InvocationHandler invocationHandler =
        ActivityInvocationHandler.newInstance(
            activityInterface, options, null, new TestActivityExecutor());
    invocationHandler = new DeterministicRunnerWrapper(invocationHandler);
    return ActivityInvocationHandlerBase.newProxy(activityInterface, invocationHandler);
  }

  /**
   * Creates client stub to activities that implement given interface.
   *
   * @param activityInterface interface type implemented by activities
   * @param options options that specify the activity invocation parameters
   * @param activityMethodOptions activity method-specific invocation parameters
   */
  @Override
  public <T> T newLocalActivityStub(
      Class<T> activityInterface,
      LocalActivityOptions options,
      Map<String, LocalActivityOptions> activityMethodOptions) {
    InvocationHandler invocationHandler =
        LocalActivityInvocationHandler.newInstance(
            activityInterface, options, activityMethodOptions, new TestActivityExecutor());
    invocationHandler = new DeterministicRunnerWrapper(invocationHandler);
    return ActivityInvocationHandlerBase.newProxy(activityInterface, invocationHandler);
  }

  @Override
  public void requestCancelActivity() {
    cancellationRequested.set(true);
  }

  @Override
  public <T> void setActivityHeartbeatListener(Class<T> detailsClass, Functions.Proc1<T> listener) {
    setActivityHeartbeatListener(detailsClass, detailsClass, listener);
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> void setActivityHeartbeatListener(
      Class<T> detailsClass, Type detailsType, Functions.Proc1<T> listener) {
    activityHeartbetListener = new ClassConsumerPair(detailsClass, detailsType, listener);
  }

  @Override
  public void close() {
    heartbeatExecutor.shutdownNow();
    activityWorkerExecutor.shutdownNow();
    channel.shutdownNow();
    try {
      channel.awaitTermination(100, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    mockServer.shutdown();
    try {
      mockServer.awaitTermination();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw Activity.wrap(e);
    }
  }

  private class TestActivityExecutor implements WorkflowOutboundCallsInterceptor {

    @Override
    public <T> ActivityOutput<T> executeActivity(ActivityInput<T> i) {
      Optional<Payloads> payloads =
          testEnvironmentOptions
              .getWorkflowClientOptions()
              .getDataConverter()
              .toPayloads(i.getArgs());
      ActivityOptions options = i.getOptions();
      PollActivityTaskQueueResponse.Builder taskBuilder =
          PollActivityTaskQueueResponse.newBuilder()
              .setScheduleToCloseTimeout(
                  ProtobufTimeUtils.toProtoDuration(options.getScheduleToCloseTimeout()))
              .setHeartbeatTimeout(ProtobufTimeUtils.toProtoDuration(options.getHeartbeatTimeout()))
              .setStartToCloseTimeout(
                  ProtobufTimeUtils.toProtoDuration(options.getStartToCloseTimeout()))
              .setScheduledTime(ProtobufTimeUtils.getCurrentProtoTime())
              .setStartedTime(ProtobufTimeUtils.getCurrentProtoTime())
              .setTaskToken(ByteString.copyFrom("test-task-token".getBytes(StandardCharsets.UTF_8)))
              .setActivityId(String.valueOf(idSequencer.incrementAndGet()))
              .setWorkflowExecution(
                  WorkflowExecution.newBuilder()
                      .setWorkflowId("test-workflow-id")
                      .setRunId(UUID.randomUUID().toString())
                      .build())
              .setActivityType(ActivityType.newBuilder().setName(i.getActivityName()).build());
      if (payloads.isPresent()) {
        taskBuilder.setInput(payloads.get());
      }
      PollActivityTaskQueueResponse task = taskBuilder.build();
      return new ActivityOutput<>(
          Workflow.newPromise(
              getReply(task, executeActivity(task, false), i.getResultClass(), i.getResultType())));
    }

    @Override
    public <R> LocalActivityOutput<R> executeLocalActivity(LocalActivityInput<R> i) {
      Optional<Payloads> payloads =
          testEnvironmentOptions
              .getWorkflowClientOptions()
              .getDataConverter()
              .toPayloads(i.getArgs());
      LocalActivityOptions options = i.getOptions();
      PollActivityTaskQueueResponse.Builder taskBuilder =
          PollActivityTaskQueueResponse.newBuilder()
              .setScheduleToCloseTimeout(
                  ProtobufTimeUtils.toProtoDuration(options.getScheduleToCloseTimeout()))
              .setStartToCloseTimeout(
                  ProtobufTimeUtils.toProtoDuration(options.getStartToCloseTimeout()))
              .setScheduledTime(ProtobufTimeUtils.getCurrentProtoTime())
              .setStartedTime(ProtobufTimeUtils.getCurrentProtoTime())
              .setTaskToken(ByteString.copyFrom("test-task-token".getBytes(StandardCharsets.UTF_8)))
              .setActivityId(String.valueOf(idSequencer.incrementAndGet()))
              .setWorkflowExecution(
                  WorkflowExecution.newBuilder()
                      .setWorkflowId("test-workflow-id")
                      .setRunId(UUID.randomUUID().toString())
                      .build())
              .setActivityType(ActivityType.newBuilder().setName(i.getActivityName()).build());
      if (payloads.isPresent()) {
        taskBuilder.setInput(payloads.get());
      }
      PollActivityTaskQueueResponse task = taskBuilder.build();
      return new LocalActivityOutput<>(
          Workflow.newPromise(
              getReply(task, executeActivity(task, true), i.getResultClass(), i.getResultType())));
    }

    /**
     * We execute the activity task on a separate activity worker to emulate what actually happens
     * in our production setup
     *
     * @param activityTask activity task to execute
     * @param localActivity true if it's a local activity
     * @return result of activity execution
     */
    private Result executeActivity(
        PollActivityTaskQueueResponse activityTask, boolean localActivity) {
      Future<Result> activityFuture =
          activityWorkerExecutor.submit(
              () ->
                  activityTaskHandler.handle(
                      new ActivityTask(activityTask, () -> {}),
                      testEnvironmentOptions.getMetricsScope(),
                      localActivity));

      try {
        // 10 seconds is just a "reasonable" wait to not make an infinite waiting
        return activityFuture.get(10, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      } catch (ExecutionException e) {
        log.error("Exception during processing of activity task");
        throw new RuntimeException(e);
      } catch (TimeoutException e) {
        log.error("Timeout trying execute activity task {}", activityTask);
        throw new RuntimeException(e);
      }
    }

    @Override
    public <R> ChildWorkflowOutput<R> executeChildWorkflow(ChildWorkflowInput<R> input) {
      throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public Random newRandom() {
      throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public SignalExternalOutput signalExternalWorkflow(SignalExternalInput input) {
      throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public CancelWorkflowOutput cancelWorkflow(CancelWorkflowInput input) {
      throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public void sleep(Duration duration) {
      throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public boolean await(Duration timeout, String reason, Supplier<Boolean> unblockCondition) {
      throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public void await(String reason, Supplier<Boolean> unblockCondition) {
      throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public Promise<Void> newTimer(Duration duration) {
      throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public <R> R sideEffect(Class<R> resultClass, Type resultType, Func<R> func) {
      throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public <R> R mutableSideEffect(
        String id, Class<R> resultClass, Type resultType, BiPredicate<R, R> updated, Func<R> func) {
      throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public int getVersion(String changeId, int minSupported, int maxSupported) {
      throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public void continueAsNew(ContinueAsNewInput input) {
      throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public void registerQuery(RegisterQueryInput input) {
      throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public void registerSignalHandlers(RegisterSignalHandlersInput input) {
      throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public void registerDynamicSignalHandler(RegisterDynamicSignalHandlerInput input) {
      throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public void registerDynamicQueryHandler(RegisterDynamicQueryHandlerInput input) {
      throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public UUID randomUUID() {
      throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public void upsertSearchAttributes(Map<String, Object> searchAttributes) {
      throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public Object newChildThread(Runnable runnable, boolean detached, String name) {
      throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public long currentTimeMillis() {
      throw new UnsupportedOperationException("not implemented");
    }

    private <T> T getReply(
        PollActivityTaskQueueResponse task,
        ActivityTaskHandler.Result response,
        Class<T> resultClass,
        Type resultType) {
      RespondActivityTaskCompletedRequest taskCompleted = response.getTaskCompleted();
      if (taskCompleted != null) {
        Optional<Payloads> result =
            taskCompleted.hasResult() ? Optional.of(taskCompleted.getResult()) : Optional.empty();
        return testEnvironmentOptions
            .getWorkflowClientOptions()
            .getDataConverter()
            .fromPayloads(0, result, resultClass, resultType);
      } else {
        RespondActivityTaskFailedRequest taskFailed =
            response.getTaskFailed().getTaskFailedRequest();
        if (taskFailed != null) {
          Exception cause =
              FailureConverter.failureToException(
                  taskFailed.getFailure(),
                  testEnvironmentOptions.getWorkflowClientOptions().getDataConverter());
          throw new ActivityFailure(
              0,
              0,
              task.getActivityType().getName(),
              task.getActivityId(),
              RetryState.RETRY_STATE_NON_RETRYABLE_FAILURE,
              "TestActivityEnvironment",
              cause);
        } else {
          RespondActivityTaskCanceledRequest taskCanceled = response.getTaskCanceled();
          if (taskCanceled != null) {
            throw new CanceledFailure(
                "canceled",
                new EncodedValues(
                    taskCanceled.hasDetails()
                        ? Optional.of(taskCanceled.getDetails())
                        : Optional.empty(),
                    testEnvironmentOptions.getWorkflowClientOptions().getDataConverter()),
                null);
          }
        }
      }
      return Defaults.defaultValue(resultClass);
    }
  }

  private static class ClassConsumerPair<T> {

    final Functions.Proc1<T> consumer;
    final Class<T> valueClass;
    final Type valueType;

    ClassConsumerPair(Class<T> valueClass, Type valueType, Functions.Proc1<T> consumer) {
      this.valueClass = Objects.requireNonNull(valueClass);
      this.valueType = Objects.requireNonNull(valueType);
      this.consumer = Objects.requireNonNull(consumer);
    }
  }
}
