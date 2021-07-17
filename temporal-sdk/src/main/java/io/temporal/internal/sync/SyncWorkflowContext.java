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

import static io.temporal.internal.common.HeaderUtils.intoPayloadMapWithDefaultConverter;
import static io.temporal.internal.common.HeaderUtils.toHeaderGrpc;
import static io.temporal.internal.common.SerializerUtils.toRetryPolicy;

import com.uber.m3.tally.Scope;
import io.temporal.activity.ActivityOptions;
import io.temporal.activity.LocalActivityOptions;
import io.temporal.api.command.v1.ContinueAsNewWorkflowExecutionCommandAttributes;
import io.temporal.api.command.v1.ScheduleActivityTaskCommandAttributes;
import io.temporal.api.command.v1.SignalExternalWorkflowExecutionCommandAttributes;
import io.temporal.api.command.v1.StartChildWorkflowExecutionCommandAttributes;
import io.temporal.api.common.v1.ActivityType;
import io.temporal.api.common.v1.Memo;
import io.temporal.api.common.v1.Payload;
import io.temporal.api.common.v1.Payloads;
import io.temporal.api.common.v1.SearchAttributes;
import io.temporal.api.common.v1.WorkflowExecution;
import io.temporal.api.common.v1.WorkflowType;
import io.temporal.api.enums.v1.ParentClosePolicy;
import io.temporal.api.failure.v1.Failure;
import io.temporal.api.taskqueue.v1.TaskQueue;
import io.temporal.api.workflowservice.v1.PollActivityTaskQueueResponse;
import io.temporal.client.WorkflowException;
import io.temporal.common.RetryOptions;
import io.temporal.common.context.ContextPropagator;
import io.temporal.common.converter.DataConverter;
import io.temporal.common.interceptors.Header;
import io.temporal.common.interceptors.WorkflowInboundCallsInterceptor;
import io.temporal.common.interceptors.WorkflowOutboundCallsInterceptor;
import io.temporal.failure.CanceledFailure;
import io.temporal.failure.ChildWorkflowFailure;
import io.temporal.failure.FailureConverter;
import io.temporal.failure.TemporalFailure;
import io.temporal.internal.common.InternalUtils;
import io.temporal.internal.common.OptionsUtils;
import io.temporal.internal.common.ProtobufTimeUtils;
import io.temporal.internal.replay.ChildWorkflowTaskFailedException;
import io.temporal.internal.replay.ExecuteActivityParameters;
import io.temporal.internal.replay.ExecuteLocalActivityParameters;
import io.temporal.internal.replay.ReplayWorkflowContext;
import io.temporal.internal.replay.StartChildWorkflowExecutionParameters;
import io.temporal.worker.WorkflowImplementationOptions;
import io.temporal.workflow.CancellationScope;
import io.temporal.workflow.ChildWorkflowOptions;
import io.temporal.workflow.CompletablePromise;
import io.temporal.workflow.ContinueAsNewOptions;
import io.temporal.workflow.Functions;
import io.temporal.workflow.Functions.Func;
import io.temporal.workflow.Promise;
import io.temporal.workflow.Workflow;
import java.lang.reflect.Type;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiPredicate;
import java.util.function.Supplier;
import javax.annotation.Nullable;

final class SyncWorkflowContext implements WorkflowOutboundCallsInterceptor {

  private final ReplayWorkflowContext context;
  private final DataConverter converter;
  private final List<ContextPropagator> contextPropagators;
  private final SignalDispatcher signalDispatcher;
  private final QueryDispatcher queryDispatcher;
  private final Optional<Payloads> lastCompletionResult;
  private final Optional<Failure> lastFailure;
  private WorkflowInboundCallsInterceptor headInboundInterceptor;
  private WorkflowOutboundCallsInterceptor headOutboundInterceptor;
  private DeterministicRunner runner;

  private ActivityOptions defaultActivityOptions = null;
  private Map<String, ActivityOptions> activityOptionsMap = new HashMap<>();

  public SyncWorkflowContext(
      ReplayWorkflowContext context,
      DataConverter converter,
      List<ContextPropagator> contextPropagators,
      Optional<Payloads> lastCompletionResult,
      Optional<Failure> lastFailure) {
    this(context, null, converter, contextPropagators, lastCompletionResult, lastFailure);
  }

  public SyncWorkflowContext(
      ReplayWorkflowContext context,
      WorkflowImplementationOptions workflowImplementationOptions,
      DataConverter converter,
      List<ContextPropagator> contextPropagators,
      Optional<Payloads> lastCompletionResult,
      Optional<Failure> lastFailure) {
    this.context = context;
    this.converter = converter;
    this.contextPropagators = contextPropagators;
    this.lastCompletionResult = lastCompletionResult;
    this.lastFailure = lastFailure;
    this.signalDispatcher = new SignalDispatcher(converter);
    this.queryDispatcher = new QueryDispatcher(converter);
    if (workflowImplementationOptions != null) {
      this.defaultActivityOptions = workflowImplementationOptions.getDefaultActivityOptions();
      this.activityOptionsMap = workflowImplementationOptions.getActivityOptions();
    }
  }

  /**
   * Using setter, as runner is initialized with this context, so it is not ready during
   * construction of this.
   */
  public void setRunner(DeterministicRunner runner) {
    this.runner = runner;
  }

  public DeterministicRunner getRunner() {
    return runner;
  }

  public WorkflowOutboundCallsInterceptor getWorkflowOutboundInterceptor() {
    // This is needed for unit tests that create DeterministicRunner directly.
    return headOutboundInterceptor == null ? this : headOutboundInterceptor;
  }

  public WorkflowInboundCallsInterceptor getWorkflowInboundInterceptor() {
    // This is needed for unit tests that create DeterministicRunner directly.
    return headInboundInterceptor == null
        ? new BaseRootWorkflowInboundCallsInterceptor(this)
        : headInboundInterceptor;
  }

  public void initHeadOutboundCallsInterceptor(WorkflowOutboundCallsInterceptor head) {
    if (headOutboundInterceptor == null) {
      headOutboundInterceptor = head;
    } else {
      throw new IllegalStateException("headOutboundInterceptor can be initialized only once");
    }
  }

  public void initHeadInboundCallsInterceptor(WorkflowInboundCallsInterceptor head) {
    if (headInboundInterceptor == null) {
      headInboundInterceptor = head;
      signalDispatcher.setInboundCallsInterceptor(head);
      queryDispatcher.setInboundCallsInterceptor(head);
    } else {
      throw new IllegalStateException("headInboundInterceptor can be initialized only once");
    }
  }

  public ActivityOptions getDefaultActivityOptions() {
    return defaultActivityOptions;
  }

  public Map<String, ActivityOptions> getActivityOptions() {
    return activityOptionsMap;
  }

  public void setDefaultActivityOptions(ActivityOptions defaultActivityOptions) {
    this.defaultActivityOptions =
        (this.defaultActivityOptions == null)
            ? defaultActivityOptions
            : this.defaultActivityOptions
                .toBuilder()
                .mergeActivityOptions(defaultActivityOptions)
                .build();
  }

  public void setActivityOptions(Map<String, ActivityOptions> activityMethodOptions) {
    Objects.requireNonNull(activityMethodOptions);
    if (this.activityOptionsMap == null) {
      this.activityOptionsMap = new HashMap<>(activityMethodOptions);
      return;
    }
    activityMethodOptions.forEach(
        (key, value) ->
            this.activityOptionsMap.merge(
                key, value, (o1, o2) -> o1.toBuilder().mergeActivityOptions(o2).build()));
  }

  @Override
  public <T> ActivityOutput<T> executeActivity(ActivityInput<T> input) {
    Optional<Payloads> args = converter.toPayloads(input.getArgs());
    Promise<Optional<Payloads>> binaryResult =
        executeActivityOnce(input.getActivityName(), input.getOptions(), input.getHeader(), args);
    if (input.getResultType() == Void.TYPE) {
      return new ActivityOutput<>(binaryResult.thenApply((r) -> null));
    }
    return new ActivityOutput<>(
        binaryResult.thenApply(
            (r) -> converter.fromPayloads(0, r, input.getResultClass(), input.getResultType())));
  }

  private Promise<Optional<Payloads>> executeActivityOnce(
      String name, ActivityOptions options, Header header, Optional<Payloads> input) {
    ActivityCallback callback = new ActivityCallback();
    ExecuteActivityParameters params =
        constructExecuteActivityParameters(name, options, header, input);
    Functions.Proc1<Exception> cancellationCallback =
        context.scheduleActivityTask(params, callback::invoke);
    CancellationScope.current()
        .getCancellationRequest()
        .thenApply(
            (reason) -> {
              cancellationCallback.apply(new CanceledFailure(reason));
              return null;
            });
    return callback.result;
  }

  public void handleInterceptedSignal(WorkflowInboundCallsInterceptor.SignalInput input) {
    signalDispatcher.handleInterceptedSignal(input);
  }

  public void handleSignal(String signalName, Optional<Payloads> input, long eventId) {
    signalDispatcher.handleSignal(signalName, input, eventId);
  }

  public WorkflowInboundCallsInterceptor.QueryOutput handleInterceptedQuery(
      WorkflowInboundCallsInterceptor.QueryInput input) {
    return queryDispatcher.handleInterceptedQuery(input);
  }

  public Optional<Payloads> handleQuery(String queryName, Optional<Payloads> input) {
    return queryDispatcher.handleQuery(queryName, input);
  }

  private class ActivityCallback {
    private final CompletablePromise<Optional<Payloads>> result = Workflow.newPromise();

    public void invoke(Optional<Payloads> output, Failure failure) {
      if (failure != null) {
        runner.executeInWorkflowThread(
            "activity failure callback",
            () ->
                result.completeExceptionally(
                    (RuntimeException)
                        FailureConverter.failureToException(failure, getDataConverter())));
      } else {
        runner.executeInWorkflowThread(
            "activity completion callback", () -> result.complete(output));
      }
    }
  }

  @Override
  public <R> LocalActivityOutput<R> executeLocalActivity(LocalActivityInput<R> input) {
    long startTime = WorkflowInternal.currentTimeMillis();
    return new LocalActivityOutput<>(
        WorkflowRetryerInternal.retryAsync(
            (attempt, currentStart) ->
                executeLocalActivityOnce(input, currentStart - startTime, attempt),
            1,
            startTime));
  }

  private <T> Promise<T> executeLocalActivityOnce(
      LocalActivityInput<T> input, long elapsed, int attempt) {
    Optional<Payloads> payloads = converter.toPayloads(input.getArgs());
    Promise<Optional<Payloads>> binaryResult =
        executeLocalActivityOnce(
            input.getActivityName(), input.getOptions(), input.getHeader(), payloads, attempt);
    if (input.getResultClass() == Void.TYPE) {
      return binaryResult.thenApply((r) -> null);
    }
    return binaryResult.thenApply(
        (r) -> converter.fromPayloads(0, r, input.getResultClass(), input.getResultType()));
  }

  private Promise<Optional<Payloads>> executeLocalActivityOnce(
      String name,
      LocalActivityOptions options,
      Header header,
      Optional<Payloads> input,
      int attempt) {
    ActivityCallback callback = new ActivityCallback();
    ExecuteLocalActivityParameters params =
        constructExecuteLocalActivityParameters(name, options, header, input, attempt);
    Functions.Proc cancellationCallback =
        context.scheduleLocalActivityTask(params, callback::invoke);
    CancellationScope.current()
        .getCancellationRequest()
        .thenApply(
            (reason) -> {
              cancellationCallback.apply();
              return null;
            });
    return callback.result;
  }

  private ExecuteActivityParameters constructExecuteActivityParameters(
      String name, ActivityOptions options, Header header, Optional<Payloads> input) {
    String taskQueue = options.getTaskQueue();
    if (taskQueue == null) {
      taskQueue = context.getTaskQueue();
    }
    ScheduleActivityTaskCommandAttributes.Builder attributes =
        ScheduleActivityTaskCommandAttributes.newBuilder()
            .setActivityType(ActivityType.newBuilder().setName(name))
            .setTaskQueue(TaskQueue.newBuilder().setName(taskQueue))
            .setScheduleToStartTimeout(
                ProtobufTimeUtils.toProtoDuration(options.getScheduleToStartTimeout()))
            .setStartToCloseTimeout(
                ProtobufTimeUtils.toProtoDuration(options.getStartToCloseTimeout()))
            .setScheduleToCloseTimeout(
                ProtobufTimeUtils.toProtoDuration(options.getScheduleToCloseTimeout()))
            .setHeartbeatTimeout(ProtobufTimeUtils.toProtoDuration(options.getHeartbeatTimeout()));

    if (input.isPresent()) {
      attributes.setInput(input.get());
    }
    RetryOptions retryOptions = options.getRetryOptions();
    if (retryOptions != null) {
      attributes.setRetryPolicy(toRetryPolicy(retryOptions));
    }

    // Set the context value.  Use the context propagators from the ActivityOptions
    // if present, otherwise use the ones configured on the WorkflowContext
    List<ContextPropagator> propagators = options.getContextPropagators();
    if (propagators == null) {
      propagators = this.contextPropagators;
    }
    io.temporal.api.common.v1.Header grpcHeader =
        toHeaderGrpc(header, extractContextsAndConvertToBytes(propagators));
    if (grpcHeader != null) {
      attributes.setHeader(grpcHeader);
    }
    return new ExecuteActivityParameters(attributes, options.getCancellationType());
  }

  private ExecuteLocalActivityParameters constructExecuteLocalActivityParameters(
      String name,
      LocalActivityOptions options,
      Header header,
      Optional<Payloads> input,
      int attempt) {
    options = LocalActivityOptions.newBuilder(options).validateAndBuildWithDefaults();

    PollActivityTaskQueueResponse.Builder activityTask =
        PollActivityTaskQueueResponse.newBuilder()
            .setActivityId(this.context.randomUUID().toString())
            .setWorkflowNamespace(this.context.getNamespace())
            .setWorkflowType(this.context.getWorkflowType())
            .setWorkflowExecution(this.context.getWorkflowExecution())
            .setScheduledTime(ProtobufTimeUtils.getCurrentProtoTime())
            .setStartToCloseTimeout(
                ProtobufTimeUtils.toProtoDuration(options.getStartToCloseTimeout()))
            .setScheduleToCloseTimeout(
                ProtobufTimeUtils.toProtoDuration(options.getScheduleToCloseTimeout()))
            .setStartedTime(ProtobufTimeUtils.getCurrentProtoTime())
            .setActivityType(ActivityType.newBuilder().setName(name))
            .setAttempt(attempt);
    io.temporal.api.common.v1.Header grpcHeader =
        toHeaderGrpc(header, extractContextsAndConvertToBytes(contextPropagators));
    if (grpcHeader != null) {
      activityTask.setHeader(grpcHeader);
    }
    if (input.isPresent()) {
      activityTask.setInput(input.get());
    }
    RetryOptions retryOptions = options.getRetryOptions();
    activityTask.setRetryPolicy(
        toRetryPolicy(RetryOptions.newBuilder(retryOptions).validateBuildWithDefaults()));
    Duration localRetryThreshold = options.getLocalRetryThreshold();
    if (localRetryThreshold == null) {
      localRetryThreshold = context.getWorkflowTaskTimeout().multipliedBy(6);
    }
    return new ExecuteLocalActivityParameters(
        activityTask, localRetryThreshold, options.isDoNotIncludeArgumentsIntoMarker());
  }

  @Override
  public <R> ChildWorkflowOutput<R> executeChildWorkflow(ChildWorkflowInput<R> input) {
    Optional<Payloads> payloads = converter.toPayloads(input.getArgs());
    CompletablePromise<WorkflowExecution> execution = Workflow.newPromise();
    Promise<Optional<Payloads>> output =
        executeChildWorkflow(
            input.getWorkflowId(),
            input.getWorkflowType(),
            input.getOptions(),
            input.getHeader(),
            payloads,
            execution);
    Promise<R> result =
        output.thenApply(
            (b) -> converter.fromPayloads(0, b, input.getResultClass(), input.getResultType()));
    return new ChildWorkflowOutput<>(result, execution);
  }

  private Promise<Optional<Payloads>> executeChildWorkflow(
      String workflowId,
      String name,
      ChildWorkflowOptions options,
      Header header,
      Optional<Payloads> input,
      CompletablePromise<WorkflowExecution> executionResult) {
    CompletablePromise<Optional<Payloads>> result = Workflow.newPromise();
    if (CancellationScope.current().isCancelRequested()) {
      CanceledFailure CanceledFailure = new CanceledFailure("execute called from a canceled scope");
      executionResult.completeExceptionally(CanceledFailure);
      result.completeExceptionally(CanceledFailure);
      return result;
    }
    List<ContextPropagator> propagators = options.getContextPropagators();
    if (propagators == null) {
      propagators = this.contextPropagators;
    }

    final StartChildWorkflowExecutionCommandAttributes.Builder attributes =
        StartChildWorkflowExecutionCommandAttributes.newBuilder()
            .setWorkflowType(WorkflowType.newBuilder().setName(name).build());
    attributes.setWorkflowId(workflowId);
    attributes.setNamespace(OptionsUtils.safeGet(options.getNamespace()));
    if (input.isPresent()) {
      attributes.setInput(input.get());
    }
    attributes.setWorkflowRunTimeout(
        ProtobufTimeUtils.toProtoDuration(options.getWorkflowRunTimeout()));
    attributes.setWorkflowExecutionTimeout(
        ProtobufTimeUtils.toProtoDuration(options.getWorkflowExecutionTimeout()));
    attributes.setWorkflowTaskTimeout(
        ProtobufTimeUtils.toProtoDuration(options.getWorkflowTaskTimeout()));
    String taskQueue = options.getTaskQueue();
    TaskQueue.Builder tl = TaskQueue.newBuilder();
    if (taskQueue != null) {
      attributes.setTaskQueue(TaskQueue.newBuilder().setName(taskQueue));
    }
    if (options.getWorkflowIdReusePolicy() != null) {
      attributes.setWorkflowIdReusePolicy(options.getWorkflowIdReusePolicy());
    }
    RetryOptions retryOptions = options.getRetryOptions();
    if (retryOptions != null) {
      attributes.setRetryPolicy(toRetryPolicy(retryOptions));
    }
    attributes.setCronSchedule(OptionsUtils.safeGet(options.getCronSchedule()));
    Map<String, Object> searchAttributes = options.getSearchAttributes();
    if (searchAttributes != null) {
      attributes.setSearchAttributes(InternalUtils.convertMapToSearchAttributes(searchAttributes));
    }
    Map<String, Object> memo = options.getMemo();
    if (memo != null) {
      attributes.setMemo(Memo.newBuilder().putAllFields(intoPayloadMapWithDefaultConverter(memo)));
    }
    io.temporal.api.common.v1.Header grpcHeader =
        toHeaderGrpc(header, extractContextsAndConvertToBytes(propagators));
    attributes.setHeader(grpcHeader);
    ParentClosePolicy parentClosePolicy = options.getParentClosePolicy();
    if (parentClosePolicy != null) {
      attributes.setParentClosePolicy(parentClosePolicy);
    }
    StartChildWorkflowExecutionParameters parameters =
        new StartChildWorkflowExecutionParameters(attributes, options.getCancellationType());

    Functions.Proc1<Exception> cancellationCallback =
        context.startChildWorkflow(
            parameters,
            (we) ->
                runner.executeInWorkflowThread(
                    "child workflow started callback", () -> executionResult.complete(we)),
            (output, failure) -> {
              if (failure != null) {
                runner.executeInWorkflowThread(
                    "child workflow failure callback",
                    () -> result.completeExceptionally(mapChildWorkflowException(failure)));
              } else {
                runner.executeInWorkflowThread(
                    "child workflow completion callback",
                    () -> {
                      result.complete(output);
                    });
              }
            });
    AtomicBoolean callbackCalled = new AtomicBoolean();
    CancellationScope.current()
        .getCancellationRequest()
        .thenApply(
            (reason) -> {
              if (!callbackCalled.getAndSet(true)) {
                cancellationCallback.apply(new CanceledFailure(reason));
              }
              return null;
            });
    return result;
  }

  private Header extractContextsAndConvertToBytes(List<ContextPropagator> contextPropagators) {
    if (contextPropagators == null) {
      return null;
    }
    Map<String, Payload> result = new HashMap<>();
    for (ContextPropagator propagator : contextPropagators) {
      result.putAll(propagator.serializeContext(propagator.getCurrentContext()));
    }
    return new Header(result);
  }

  private RuntimeException mapChildWorkflowException(Exception failure) {
    if (failure == null) {
      return null;
    }
    if (failure instanceof TemporalFailure) {
      ((TemporalFailure) failure).setDataConverter(getDataConverter());
    }
    if (failure instanceof CanceledFailure) {
      return (CanceledFailure) failure;
    }
    if (failure instanceof WorkflowException) {
      return (RuntimeException) failure;
    }
    if (failure instanceof ChildWorkflowFailure) {
      return (ChildWorkflowFailure) failure;
    }
    if (!(failure instanceof ChildWorkflowTaskFailedException)) {
      return new IllegalArgumentException("Unexpected exception type: ", failure);
    }
    ChildWorkflowTaskFailedException taskFailed = (ChildWorkflowTaskFailedException) failure;
    Throwable cause =
        FailureConverter.failureToException(taskFailed.getFailure(), getDataConverter());
    // To support WorkflowExecutionAlreadyStarted set at handleStartChildWorkflowExecutionFailed
    if (cause == null) {
      cause = failure.getCause();
    }
    return new ChildWorkflowFailure(
        0,
        0,
        taskFailed.getWorkflowType().getName(),
        taskFailed.getWorkflowExecution(),
        null,
        taskFailed.getRetryState(),
        cause);
  }

  @Override
  public Promise<Void> newTimer(Duration delay) {
    CompletablePromise<Void> p = Workflow.newPromise();
    Functions.Proc1<RuntimeException> cancellationHandler =
        context.newTimer(
            delay,
            (e) -> {
              runner.executeInWorkflowThread(
                  "timer-callback",
                  () -> {
                    if (e == null) {
                      p.complete(null);
                    } else {
                      p.completeExceptionally(e);
                    }
                  });
            });
    CancellationScope.current()
        .getCancellationRequest()
        .thenApply(
            (r) -> {
              cancellationHandler.apply(new CanceledFailure(r));
              return r;
            });
    return p;
  }

  @Override
  public <R> R sideEffect(Class<R> resultClass, Type resultType, Func<R> func) {
    try {
      DataConverter dataConverter = getDataConverter();
      CompletablePromise<Optional<Payloads>> result = Workflow.newPromise();
      context.sideEffect(
          () -> {
            R r = func.apply();
            return dataConverter.toPayloads(r);
          },
          (p) ->
              runner.executeInWorkflowThread(
                  "side-effect-callback", () -> result.complete(Objects.requireNonNull(p))));
      return dataConverter.fromPayloads(0, result.get(), resultClass, resultType);
    } catch (Error e) {
      throw e;
    } catch (Exception e) {
      // SideEffect cannot throw normal exception as it can lead to non deterministic behavior
      // So fail the workflow task by throwing an Error.
      throw new Error(e);
    }
  }

  @Override
  public <R> R mutableSideEffect(
      String id, Class<R> resultClass, Type resultType, BiPredicate<R, R> updated, Func<R> func) {
    try {
      return mutableSideEffectImpl(id, resultClass, resultType, updated, func);
    } catch (Error e) {
      throw e;
    } catch (Exception e) {
      // MutableSideEffect cannot throw normal exception as it can lead to non deterministic
      // behavior
      // So fail the workflow task by throwing an Error.
      throw new Error(e);
    }
  }

  private <R> R mutableSideEffectImpl(
      String id, Class<R> resultClass, Type resultType, BiPredicate<R, R> updated, Func<R> func) {
    CompletablePromise<Optional<Payloads>> result = Workflow.newPromise();
    AtomicReference<R> unserializedResult = new AtomicReference<>();
    context.mutableSideEffect(
        id,
        (storedBinary) -> {
          Optional<R> stored =
              storedBinary.map(
                  (b) -> converter.fromPayloads(0, Optional.of(b), resultClass, resultType));
          R funcResult =
              Objects.requireNonNull(func.apply(), "mutableSideEffect function " + "returned null");
          if (!stored.isPresent() || updated.test(stored.get(), funcResult)) {
            unserializedResult.set(funcResult);
            return converter.toPayloads(funcResult);
          }
          return Optional.empty(); // returned only when value doesn't need to be updated
        },
        (p) ->
            runner.executeInWorkflowThread(
                "mutable-side-effect-callback", () -> result.complete(Objects.requireNonNull(p))));

    if (!result.get().isPresent()) {
      throw new IllegalArgumentException("No value found for mutableSideEffectId=" + id);
    }
    // An optimization that avoids unnecessary deserialization of the result.
    R unserialized = unserializedResult.get();
    if (unserialized != null) {
      return unserialized;
    }
    return converter.fromPayloads(0, result.get(), resultClass, resultType);
  }

  @Override
  public int getVersion(String changeId, int minSupported, int maxSupported) {
    CompletablePromise<Integer> result = Workflow.newPromise();
    context.getVersion(
        changeId,
        minSupported,
        maxSupported,
        (v) -> runner.executeInWorkflowThread("version-callback", () -> result.complete(v)));
    int r = result.get();
    return r;
  }

  @Override
  public void registerQuery(RegisterQueryInput request) {
    queryDispatcher.registerQueryHandlers(request);
  }

  @Override
  public void registerSignalHandlers(RegisterSignalHandlersInput input) {
    signalDispatcher.registerSignalHandlers(input);
  }

  @Override
  public void registerDynamicSignalHandler(RegisterDynamicSignalHandlerInput input) {
    signalDispatcher.registerDynamicSignalHandler(input);
  }

  @Override
  public void registerDynamicQueryHandler(RegisterDynamicQueryHandlerInput input) {
    queryDispatcher.registerDynamicQueryHandler(input);
  }

  @Override
  public UUID randomUUID() {
    return context.randomUUID();
  }

  @Override
  public Random newRandom() {
    return context.newRandom();
  }

  public DataConverter getDataConverter() {
    return converter;
  }

  boolean isReplaying() {
    return context.isReplaying();
  }

  public ReplayWorkflowContext getContext() {
    return context;
  }

  @Override
  public SignalExternalOutput signalExternalWorkflow(SignalExternalInput input) {
    SignalExternalWorkflowExecutionCommandAttributes.Builder attributes =
        SignalExternalWorkflowExecutionCommandAttributes.newBuilder();
    attributes.setSignalName(input.getSignalName());
    attributes.setExecution(input.getExecution());
    Optional<Payloads> payloads = getDataConverter().toPayloads(input.getArgs());
    if (payloads.isPresent()) {
      attributes.setInput(payloads.get());
    }
    CompletablePromise<Void> result = Workflow.newPromise();
    Functions.Proc1<Exception> cancellationCallback =
        context.signalExternalWorkflowExecution(
            attributes,
            (output, failure) -> {
              if (failure != null) {
                runner.executeInWorkflowThread(
                    "child workflow failure callback",
                    () ->
                        result.completeExceptionally(
                            FailureConverter.failureToException(failure, getDataConverter())));
              } else {
                runner.executeInWorkflowThread(
                    "child workflow completion callback", () -> result.complete(output));
              }
            });
    CancellationScope.current()
        .getCancellationRequest()
        .thenApply(
            (reason) -> {
              cancellationCallback.apply(new CanceledFailure(reason));
              return null;
            });
    return new SignalExternalOutput(result);
  }

  @Override
  public void sleep(Duration duration) {
    newTimer(duration).get();
  }

  @Override
  public boolean await(Duration timeout, String reason, Supplier<Boolean> unblockCondition) {
    Promise<Void> timer = newTimer(timeout);
    WorkflowThread.await(reason, () -> (timer.isCompleted() || unblockCondition.get()));
    return !timer.isCompleted();
  }

  @Override
  public void await(String reason, Supplier<Boolean> unblockCondition) {
    WorkflowThread.await(reason, unblockCondition);
  }

  @Override
  public void continueAsNew(ContinueAsNewInput input) {
    ContinueAsNewWorkflowExecutionCommandAttributes.Builder attributes =
        ContinueAsNewWorkflowExecutionCommandAttributes.newBuilder();
    Optional<String> workflowType = input.getWorkflowType();
    if (workflowType.isPresent()) {
      attributes.setWorkflowType(WorkflowType.newBuilder().setName(workflowType.get()));
    }
    Optional<ContinueAsNewOptions> options = input.getOptions();
    if (options.isPresent()) {
      ContinueAsNewOptions ops = options.get();
      attributes.setWorkflowRunTimeout(
          ProtobufTimeUtils.toProtoDuration(ops.getWorkflowRunTimeout()));
      attributes.setWorkflowTaskTimeout(
          ProtobufTimeUtils.toProtoDuration(ops.getWorkflowTaskTimeout()));
      if (!ops.getTaskQueue().isEmpty()) {
        attributes.setTaskQueue(TaskQueue.newBuilder().setName(ops.getTaskQueue()));
      }
      Map<String, Object> memo = ops.getMemo();
      if (memo != null) {
        attributes.setMemo(
            Memo.newBuilder().putAllFields(intoPayloadMapWithDefaultConverter(memo)));
      }
      Map<String, Object> searchAttributes = ops.getSearchAttributes();
      if (searchAttributes != null) {
        attributes.setSearchAttributes(
            SearchAttributes.newBuilder()
                .putAllIndexedFields(intoPayloadMapWithDefaultConverter(searchAttributes)));
      }
    }
    Optional<Payloads> payloads = getDataConverter().toPayloads(input.getArgs());
    if (payloads.isPresent()) {
      attributes.setInput(payloads.get());
    }
    // TODO(maxim): Find out what to do about header
    context.continueAsNewOnCompletion(attributes.build());
    WorkflowThread.exit(null);
  }

  @Override
  public CancelWorkflowOutput cancelWorkflow(CancelWorkflowInput input) {
    CompletablePromise<Void> result = Workflow.newPromise();
    context.requestCancelExternalWorkflowExecution(
        input.getExecution(),
        (r, exception) -> {
          if (exception == null) {
            result.complete(null);
          } else {
            result.completeExceptionally(exception);
          }
        });
    return new CancelWorkflowOutput(result);
  }

  public Scope getMetricsScope() {
    return context.getMetricsScope();
  }

  public boolean isLoggingEnabledInReplay() {
    return context.getEnableLoggingInReplay();
  }

  public <R> R getLastCompletionResult(Class<R> resultClass, Type resultType) {
    DataConverter dataConverter = getDataConverter();
    return dataConverter.fromPayloads(0, lastCompletionResult, resultClass, resultType);
  }

  public Optional<Failure> getPreviousRunFailure() {
    return lastFailure;
  }

  @Override
  public void upsertSearchAttributes(Map<String, Object> searchAttributes) {
    if (searchAttributes.isEmpty()) {
      throw new IllegalArgumentException("Empty search attributes");
    }

    SearchAttributes attr = InternalUtils.convertMapToSearchAttributes(searchAttributes);
    context.upsertSearchAttributes(attr);
  }

  public Object newWorkflowMethodThreadIntercepted(Runnable runnable, @Nullable String name) {
    return runner.newWorkflowThread(runnable, false, name);
  }

  public Object newWorkflowCallbackThreadIntercepted(Runnable runnable, @Nullable String name) {
    return runner.newCallbackThread(runnable, name);
  }

  @Override
  public Object newChildThread(Runnable runnable, boolean detached, String name) {
    return runner.newWorkflowThread(runnable, detached, name);
  }

  @Override
  public long currentTimeMillis() {
    return context.currentTimeMillis();
  }
}
