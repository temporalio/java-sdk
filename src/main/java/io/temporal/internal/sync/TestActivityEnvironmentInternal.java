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

package io.temporal.internal.sync;

import static io.temporal.internal.common.OptionsUtils.roundUpToSeconds;

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
import io.temporal.common.converter.EncodedValue;
import io.temporal.common.interceptors.WorkflowCallsInterceptor;
import io.temporal.failure.ActivityFailure;
import io.temporal.failure.CanceledFailure;
import io.temporal.failure.FailureConverter;
import io.temporal.internal.metrics.NoopScope;
import io.temporal.internal.worker.ActivityTaskHandler;
import io.temporal.internal.worker.ActivityTaskHandler.Result;
import io.temporal.proto.common.ActivityType;
import io.temporal.proto.common.Payloads;
import io.temporal.proto.common.RetryStatus;
import io.temporal.proto.common.WorkflowExecution;
import io.temporal.proto.workflowservice.PollForActivityTaskResponse;
import io.temporal.proto.workflowservice.RecordActivityTaskHeartbeatRequest;
import io.temporal.proto.workflowservice.RecordActivityTaskHeartbeatResponse;
import io.temporal.proto.workflowservice.RespondActivityTaskCanceledRequest;
import io.temporal.proto.workflowservice.RespondActivityTaskCompletedRequest;
import io.temporal.proto.workflowservice.RespondActivityTaskFailedRequest;
import io.temporal.proto.workflowservice.WorkflowServiceGrpc;
import io.temporal.serviceclient.WorkflowServiceStubs;
import io.temporal.serviceclient.WorkflowServiceStubsOptions;
import io.temporal.testing.TestActivityEnvironment;
import io.temporal.testing.TestEnvironmentOptions;
import io.temporal.workflow.ChildWorkflowOptions;
import io.temporal.workflow.ContinueAsNewOptions;
import io.temporal.workflow.Functions;
import io.temporal.workflow.Functions.Func;
import io.temporal.workflow.Functions.Func1;
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
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import java.util.function.Supplier;

public final class TestActivityEnvironmentInternal implements TestActivityEnvironment {

  private final POJOActivityTaskHandler activityTaskHandler;
  private final TestEnvironmentOptions testEnvironmentOptions;
  private final AtomicInteger idSequencer = new AtomicInteger();
  private ClassConsumerPair<Object> activityHeartbetListener;
  private static final ScheduledExecutorService heartbeatExecutor =
      Executors.newScheduledThreadPool(20);
  private WorkflowServiceStubs workflowServiceStubs;
  private Server mockServer;
  private AtomicBoolean cancellationRequested = new AtomicBoolean();
  private ManagedChannel channel;

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
                .build());
    activityTaskHandler =
        new POJOActivityTaskHandler(
            workflowServiceStubs,
            testEnvironmentOptions.getWorkflowClientOptions().getNamespace(),
            testEnvironmentOptions.getWorkflowClientOptions().getDataConverter(),
            heartbeatExecutor);
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
                      requestDetails,
                      activityHeartbetListener.valueClass,
                      activityHeartbetListener.valueType);
          activityHeartbetListener.consumer.accept(details);
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
    activityTaskHandler.setActivitiesImplementation(activityImplementations);
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
            activityInterface, options, new TestActivityExecutor());
    invocationHandler = new DeterministicRunnerWrapper(invocationHandler);
    return ActivityInvocationHandlerBase.newProxy(activityInterface, invocationHandler);
  }

  @Override
  public void requestCancelActivity() {
    cancellationRequested.set(true);
  }

  @Override
  public <T> void setActivityHeartbeatListener(Class<T> detailsClass, Consumer<T> listener) {
    setActivityHeartbeatListener(detailsClass, detailsClass, listener);
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> void setActivityHeartbeatListener(
      Class<T> detailsClass, Type detailsType, Consumer<T> listener) {
    activityHeartbetListener = new ClassConsumerPair(detailsClass, detailsType, listener);
  }

  @Override
  public void close() {
    channel.shutdownNow();
    try {
      channel.awaitTermination(100, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
    }
    mockServer.shutdown();
    try {
      mockServer.awaitTermination();
    } catch (InterruptedException e) {
      throw Activity.wrap(e);
    }
  }

  private class TestActivityExecutor implements WorkflowCallsInterceptor {

    @Override
    public <T> Promise<T> executeActivity(
        String activityType,
        Class<T> resultClass,
        Type resultType,
        Object[] args,
        ActivityOptions options) {
      Optional<Payloads> input =
          testEnvironmentOptions.getWorkflowClientOptions().getDataConverter().toPayloads(args);
      PollForActivityTaskResponse.Builder taskBuilder =
          PollForActivityTaskResponse.newBuilder()
              .setScheduleToCloseTimeoutSeconds(
                  roundUpToSeconds(options.getScheduleToCloseTimeout()))
              .setHeartbeatTimeoutSeconds(roundUpToSeconds(options.getHeartbeatTimeout()))
              .setStartToCloseTimeoutSeconds(roundUpToSeconds(options.getStartToCloseTimeout()))
              .setScheduledTimestamp(Duration.ofMillis(System.currentTimeMillis()).toNanos())
              .setStartedTimestamp(Duration.ofMillis(System.currentTimeMillis()).toNanos())
              .setTaskToken(ByteString.copyFrom("test-task-token".getBytes(StandardCharsets.UTF_8)))
              .setActivityId(String.valueOf(idSequencer.incrementAndGet()))
              .setWorkflowExecution(
                  WorkflowExecution.newBuilder()
                      .setWorkflowId("test-workflow-id")
                      .setRunId(UUID.randomUUID().toString())
                      .build())
              .setActivityType(ActivityType.newBuilder().setName(activityType).build());
      if (input.isPresent()) {
        taskBuilder.setInput(input.get());
      }
      PollForActivityTaskResponse task = taskBuilder.build();
      Result taskResult = activityTaskHandler.handle(task, NoopScope.getInstance(), false);
      return Workflow.newPromise(getReply(task, taskResult, resultClass, resultType));
    }

    @Override
    public <R> Promise<R> executeLocalActivity(
        String activityName,
        Class<R> resultClass,
        Type resultType,
        Object[] args,
        LocalActivityOptions options) {
      throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public <R> WorkflowResult<R> executeChildWorkflow(
        String workflowType,
        Class<R> resultClass,
        Type resultType,
        Object[] args,
        ChildWorkflowOptions options) {
      throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public Random newRandom() {
      throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public Promise<Void> signalExternalWorkflow(
        WorkflowExecution execution, String signalName, Object[] args) {
      throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public Promise<Void> cancelWorkflow(WorkflowExecution execution) {
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
    public void continueAsNew(
        Optional<String> workflowType, Optional<ContinueAsNewOptions> options, Object[] args) {
      throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public void registerQuery(
        String queryType,
        Class<?>[] argTypes,
        Type[] genericArgTypes,
        Func1<Object[], Object> callback) {
      throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public void registerSignal(
        String signalType,
        Class<?>[] argTypes,
        Type[] genericArgTypes,
        Functions.Proc1<Object[]> callback) {
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

    private <T> T getReply(
        PollForActivityTaskResponse task,
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
            .fromPayloads(result, resultClass, resultType);
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
              RetryStatus.NonRetryableFailure,
              "TestActivityEnvironment",
              cause);
        } else {
          RespondActivityTaskCanceledRequest taskCancelled = response.getTaskCancelled();
          if (taskCancelled != null) {
            throw new CanceledFailure(
                "canceled",
                new EncodedValue(
                    taskCancelled.hasDetails()
                        ? Optional.of(taskCancelled.getDetails())
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

    final Consumer<T> consumer;
    final Class<T> valueClass;
    final Type valueType;

    ClassConsumerPair(Class<T> valueClass, Type valueType, Consumer<T> consumer) {
      this.valueClass = Objects.requireNonNull(valueClass);
      this.valueType = Objects.requireNonNull(valueType);
      this.consumer = Objects.requireNonNull(consumer);
    }
  }
}
