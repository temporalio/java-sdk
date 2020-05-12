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

import com.google.protobuf.InvalidProtocolBufferException;
import com.uber.m3.tally.Scope;
import io.temporal.activity.ActivityOptions;
import io.temporal.activity.LocalActivityOptions;
import io.temporal.common.RetryOptions;
import io.temporal.common.context.ContextPropagator;
import io.temporal.common.converter.DataConverter;
import io.temporal.common.converter.DataConverterException;
import io.temporal.common.interceptors.WorkflowCallsInterceptor;
import io.temporal.internal.common.InternalUtils;
import io.temporal.internal.common.RetryParameters;
import io.temporal.internal.metrics.MetricsType;
import io.temporal.internal.replay.ActivityTaskFailedException;
import io.temporal.internal.replay.ActivityTaskTimeoutException;
import io.temporal.internal.replay.ChildWorkflowTaskFailedException;
import io.temporal.internal.replay.ContinueAsNewWorkflowExecutionParameters;
import io.temporal.internal.replay.DecisionContext;
import io.temporal.internal.replay.ExecuteActivityParameters;
import io.temporal.internal.replay.ExecuteLocalActivityParameters;
import io.temporal.internal.replay.SignalExternalWorkflowParameters;
import io.temporal.internal.replay.StartChildWorkflowExecutionParameters;
import io.temporal.proto.common.ActivityType;
import io.temporal.proto.common.Payload;
import io.temporal.proto.common.Payloads;
import io.temporal.proto.common.SearchAttributes;
import io.temporal.proto.common.WorkflowType;
import io.temporal.proto.execution.WorkflowExecution;
import io.temporal.workflow.ActivityException;
import io.temporal.workflow.ActivityFailureException;
import io.temporal.workflow.ActivityTimeoutException;
import io.temporal.workflow.CancellationScope;
import io.temporal.workflow.ChildWorkflowException;
import io.temporal.workflow.ChildWorkflowFailureException;
import io.temporal.workflow.ChildWorkflowOptions;
import io.temporal.workflow.ChildWorkflowTimedOutException;
import io.temporal.workflow.CompletablePromise;
import io.temporal.workflow.ContinueAsNewOptions;
import io.temporal.workflow.Functions;
import io.temporal.workflow.Functions.Func;
import io.temporal.workflow.Promise;
import io.temporal.workflow.SignalExternalWorkflowException;
import io.temporal.workflow.Workflow;
import java.lang.reflect.Type;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class SyncDecisionContext implements WorkflowCallsInterceptor {

  private static class SignalData {
    private final Optional<Payloads> payload;
    private final long eventId;

    private SignalData(Optional<Payloads> payload, long eventId) {
      this.payload = payload;
      this.eventId = eventId;
    }

    public Optional<Payloads> getPayload() {
      return payload;
    }

    public long getEventId() {
      return eventId;
    }
  }

  private static final Logger log = LoggerFactory.getLogger(SyncDecisionContext.class);

  private final DecisionContext context;
  private DeterministicRunner runner;
  private final DataConverter converter;
  private final List<ContextPropagator> contextPropagators;
  private WorkflowCallsInterceptor headInterceptor;
  private final WorkflowTimers timers = new WorkflowTimers();
  private final Map<String, Functions.Func1<Optional<Payloads>, Optional<Payloads>>>
      queryCallbacks = new HashMap<>();
  private final Map<String, Functions.Proc2<Optional<Payloads>, Long>> signalCallbacks =
      new HashMap<>();
  /**
   * Buffers signals which don't have registered listener. Key is signal type. Value is signal data.
   */
  private final Map<String, List<SignalData>> signalBuffers = new HashMap<>();

  private final Optional<Payloads> lastCompletionResult;

  public SyncDecisionContext(
      DecisionContext context,
      DataConverter converter,
      List<ContextPropagator> contextPropagators,
      Optional<Payloads> lastCompletionResult) {
    this.context = context;
    this.converter = converter;
    this.contextPropagators = contextPropagators;
    this.lastCompletionResult = lastCompletionResult;
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

  public WorkflowCallsInterceptor getWorkflowInterceptor() {
    // This is needed for unit tests that create DeterministicRunner directly.
    return headInterceptor == null ? this : headInterceptor;
  }

  public void setHeadInterceptor(WorkflowCallsInterceptor head) {
    if (headInterceptor == null) {
      this.headInterceptor = head;
    }
  }

  @Override
  public <T> Promise<T> executeActivity(
      String activityName,
      Class<T> returnClass,
      Type resultType,
      Object[] args,
      ActivityOptions options) {
    Optional<Payloads> input = converter.toData(args);
    Promise<Optional<Payloads>> binaryResult = executeActivityOnce(activityName, options, input);
    if (returnClass == Void.TYPE) {
      return binaryResult.thenApply((r) -> null);
    }
    return binaryResult.thenApply((r) -> converter.fromData(r, returnClass, resultType));
  }

  private Promise<Optional<Payloads>> executeActivityOnce(
      String name, ActivityOptions options, Optional<Payloads> input) {
    ActivityCallback callback = new ActivityCallback();
    ExecuteActivityParameters params = constructExecuteActivityParameters(name, options, input);
    Consumer<Exception> cancellationCallback =
        context.scheduleActivityTask(params, callback::invoke);
    CancellationScope.current()
        .getCancellationRequest()
        .thenApply(
            (reason) -> {
              cancellationCallback.accept(new CancellationException(reason));
              return null;
            });
    return callback.result;
  }

  private class ActivityCallback {
    private CompletablePromise<Optional<Payloads>> result = Workflow.newPromise();

    public void invoke(Optional<Payloads> output, Exception failure) {
      if (failure != null) {
        runner.executeInWorkflowThread(
            "activity failure callback",
            () -> result.completeExceptionally(mapActivityException(failure)));
      } else {
        runner.executeInWorkflowThread(
            "activity completion callback", () -> result.complete(output));
      }
    }
  }

  private RuntimeException mapActivityException(Exception failure) {
    if (failure == null) {
      return null;
    }
    if (failure instanceof CancellationException) {
      return (CancellationException) failure;
    }
    if (failure instanceof ActivityTaskFailedException) {
      ActivityTaskFailedException taskFailed = (ActivityTaskFailedException) failure;
      String causeClassName = taskFailed.getReason();
      Class<? extends Exception> causeClass;
      Exception cause;
      try {
        @SuppressWarnings("unchecked") // cc is just to have a place to put this annotation
        Class<? extends Exception> cc = (Class<? extends Exception>) Class.forName(causeClassName);
        causeClass = cc;
        cause = getDataConverter().fromData(taskFailed.getDetails(), causeClass, causeClass);
      } catch (Exception e) {
        cause = e;
      }
      if (cause instanceof SimulatedTimeoutExceptionInternal) {
        // This exception is thrown only in unit tests to mock the activity timeouts
        SimulatedTimeoutExceptionInternal testTimeout = (SimulatedTimeoutExceptionInternal) cause;
        Optional<Payloads> details;
        if (testTimeout.getDetails().length == 0) {
          details = Optional.empty();
        } else {
          try {
            details = Optional.of(Payloads.parseFrom(testTimeout.getDetails()));
          } catch (InvalidProtocolBufferException e) {
            throw new DataConverterException(e);
          }
        }
        return new ActivityTimeoutException(
            taskFailed.getEventId(),
            taskFailed.getActivityType(),
            taskFailed.getActivityId(),
            testTimeout.getTimeoutType(),
            details,
            getDataConverter());
      }
      return new ActivityFailureException(
          taskFailed.getEventId(), taskFailed.getActivityType(), taskFailed.getActivityId(), cause);
    }
    if (failure instanceof ActivityTaskTimeoutException) {
      ActivityTaskTimeoutException timedOut = (ActivityTaskTimeoutException) failure;
      return new ActivityTimeoutException(
          timedOut.getEventId(),
          timedOut.getActivityType(),
          timedOut.getActivityId(),
          timedOut.getTimeoutType(),
          timedOut.getDetails(),
          getDataConverter());
    }
    if (failure instanceof ActivityException) {
      return (ActivityException) failure;
    }
    throw new IllegalArgumentException(
        "Unexpected exception type: " + failure.getClass().getName(), failure);
  }

  @Override
  public <R> Promise<R> executeLocalActivity(
      String activityName,
      Class<R> resultClass,
      Type resultType,
      Object[] args,
      LocalActivityOptions options) {
    long startTime = WorkflowInternal.currentTimeMillis();
    return WorkflowRetryerInternal.retryAsync(
        (attempt, currentStart) ->
            executeLocalActivityOnce(
                activityName,
                options,
                args,
                resultClass,
                resultType,
                currentStart - startTime,
                attempt),
        1,
        startTime);
  }

  private <T> Promise<T> executeLocalActivityOnce(
      String name,
      LocalActivityOptions options,
      Object[] args,
      Class<T> returnClass,
      Type returnType,
      long elapsed,
      int attempt) {
    Optional<Payloads> input = converter.toData(args);
    Promise<Optional<Payloads>> binaryResult =
        executeLocalActivityOnce(name, options, input, elapsed, attempt);
    if (returnClass == Void.TYPE) {
      return binaryResult.thenApply((r) -> null);
    }
    return binaryResult.thenApply((r) -> converter.fromData(r, returnClass, returnType));
  }

  private Promise<Optional<Payloads>> executeLocalActivityOnce(
      String name,
      LocalActivityOptions options,
      Optional<Payloads> input,
      long elapsed,
      int attempt) {
    ActivityCallback callback = new ActivityCallback();
    ExecuteLocalActivityParameters params =
        constructExecuteLocalActivityParameters(name, options, input, elapsed, attempt);
    Consumer<Exception> cancellationCallback =
        context.scheduleLocalActivityTask(params, callback::invoke);
    CancellationScope.current()
        .getCancellationRequest()
        .thenApply(
            (reason) -> {
              cancellationCallback.accept(new CancellationException(reason));
              return null;
            });
    return callback.result;
  }

  private ExecuteActivityParameters constructExecuteActivityParameters(
      String name, ActivityOptions options, Optional<Payloads> input) {
    ExecuteActivityParameters parameters = new ExecuteActivityParameters();
    // TODO: Real task list
    String taskList = options.getTaskList();
    if (taskList == null) {
      taskList = context.getTaskList();
    }
    parameters
        .withActivityType(ActivityType.newBuilder().setName(name).build())
        .withInput(input.orElse(null))
        .withTaskList(taskList)
        .withScheduleToStartTimeoutSeconds(roundUpToSeconds(options.getScheduleToStartTimeout()))
        .withStartToCloseTimeoutSeconds(roundUpToSeconds(options.getStartToCloseTimeout()))
        .withScheduleToCloseTimeoutSeconds(roundUpToSeconds(options.getScheduleToCloseTimeout()))
        .withHeartbeatTimeoutSeconds(roundUpToSeconds(options.getHeartbeatTimeout()))
        .withCancellationType(options.getCancellationType());
    RetryOptions retryOptions = options.getRetryOptions();
    if (retryOptions != null) {
      parameters.setRetryParameters(new RetryParameters(retryOptions));
    }

    // Set the context value.  Use the context propagators from the ActivityOptions
    // if present, otherwise use the ones configured on the DecisionContext
    List<ContextPropagator> propagators = options.getContextPropagators();
    if (propagators == null) {
      propagators = this.contextPropagators;
    }
    parameters.setContext(extractContextsAndConvertToBytes(propagators));

    return parameters;
  }

  private ExecuteLocalActivityParameters constructExecuteLocalActivityParameters(
      String name,
      LocalActivityOptions options,
      Optional<Payloads> input,
      long elapsed,
      int attempt) {
    options = LocalActivityOptions.newBuilder(options).validateAndBuildWithDefaults();
    ExecuteLocalActivityParameters parameters = new ExecuteLocalActivityParameters();
    parameters
        .withActivityType(ActivityType.newBuilder().setName(name).build())
        .withInput(input.orElse(null))
        .withStartToCloseTimeout(options.getStartToCloseTimeout())
        .withScheduleToCloseTimeout(options.getScheduleToCloseTimeout());
    RetryOptions retryOptions = options.getRetryOptions();
    if (retryOptions != null) {
      parameters.setRetryOptions(retryOptions);
    }
    parameters.setAttempt(attempt);
    parameters.setElapsedTime(elapsed);
    parameters.setWorkflowNamespace(this.context.getNamespace());
    parameters.setWorkflowExecution(this.context.getWorkflowExecution());
    return parameters;
  }

  @Override
  public <R> WorkflowResult<R> executeChildWorkflow(
      String workflowType,
      Class<R> returnClass,
      Type returnType,
      Object[] args,
      ChildWorkflowOptions options) {
    Optional<Payloads> input = converter.toData(args);
    CompletablePromise<WorkflowExecution> execution = Workflow.newPromise();
    Promise<Optional<Payloads>> output =
        executeChildWorkflow(workflowType, options, input, execution);
    Promise<R> result = output.thenApply((b) -> converter.fromData(b, returnClass, returnType));
    return new WorkflowResult<>(result, execution);
  }

  private Promise<Optional<Payloads>> executeChildWorkflow(
      String name,
      ChildWorkflowOptions options,
      Optional<Payloads> input,
      CompletablePromise<WorkflowExecution> executionResult) {
    CompletablePromise<Optional<Payloads>> result = Workflow.newPromise();
    if (CancellationScope.current().isCancelRequested()) {
      CancellationException cancellationException =
          new CancellationException("execute called from a cancelled scope");
      executionResult.completeExceptionally(cancellationException);
      result.completeExceptionally(cancellationException);
      return result;
    }
    RetryParameters retryParameters = null;
    RetryOptions retryOptions = options.getRetryOptions();
    if (retryOptions != null) {
      retryParameters = new RetryParameters(retryOptions);
    }
    List<ContextPropagator> propagators = options.getContextPropagators();
    if (propagators == null) {
      propagators = this.contextPropagators;
    }

    StartChildWorkflowExecutionParameters parameters =
        new StartChildWorkflowExecutionParameters.Builder()
            .setWorkflowType(WorkflowType.newBuilder().setName(name).build())
            .setWorkflowId(options.getWorkflowId())
            .setInput(input.orElse(null))
            .setWorkflowRunTimeoutSeconds(roundUpToSeconds(options.getWorkflowRunTimeout()))
            .setWorkflowExecutionTimeoutSeconds(
                roundUpToSeconds(options.getWorkflowExecutionTimeout()))
            .setNamespace(options.getNamespace())
            .setTaskList(options.getTaskList())
            .setWorkflowTaskTimeoutSeconds(roundUpToSeconds(options.getWorkflowTaskTimeout()))
            .setWorkflowIdReusePolicy(options.getWorkflowIdReusePolicy())
            .setRetryParameters(retryParameters)
            .setCronSchedule(options.getCronSchedule())
            .setContext(extractContextsAndConvertToBytes(propagators))
            .setParentClosePolicy(options.getParentClosePolicy())
            .setCancellationType(options.getCancellationType())
            .build();
    Consumer<Exception> cancellationCallback =
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
                    "child workflow completion callback", () -> result.complete(output));
              }
            });
    CancellationScope.current()
        .getCancellationRequest()
        .thenApply(
            (reason) -> {
              cancellationCallback.accept(new CancellationException(reason));
              return null;
            });
    return result;
  }

  private Map<String, Payload> extractContextsAndConvertToBytes(
      List<ContextPropagator> contextPropagators) {
    if (contextPropagators == null) {
      return null;
    }
    Map<String, Payload> result = new HashMap<>();
    for (ContextPropagator propagator : contextPropagators) {
      result.putAll(propagator.serializeContext(propagator.getCurrentContext()));
    }
    return result;
  }

  private RuntimeException mapChildWorkflowException(Exception failure) {
    if (failure == null) {
      return null;
    }
    if (failure instanceof CancellationException) {
      return (CancellationException) failure;
    }
    if (failure instanceof ChildWorkflowException) {
      return (ChildWorkflowException) failure;
    }
    if (!(failure instanceof ChildWorkflowTaskFailedException)) {
      return new IllegalArgumentException("Unexpected exception type: ", failure);
    }
    ChildWorkflowTaskFailedException taskFailed = (ChildWorkflowTaskFailedException) failure;
    String causeClassName = taskFailed.getReason();
    Exception cause;
    try {
      @SuppressWarnings("unchecked")
      Class<? extends Exception> causeClass =
          (Class<? extends Exception>) Class.forName(causeClassName);
      cause = getDataConverter().fromData(taskFailed.getDetails(), causeClass, causeClass);
    } catch (Exception e) {
      cause = e;
    }
    if (cause instanceof SimulatedTimeoutExceptionInternal) {
      // This exception is thrown only in unit tests to mock the child workflow timeouts
      return new ChildWorkflowTimedOutException(
          taskFailed.getEventId(), taskFailed.getWorkflowExecution(), taskFailed.getWorkflowType());
    }
    return new ChildWorkflowFailureException(
        taskFailed.getEventId(),
        taskFailed.getWorkflowExecution(),
        taskFailed.getWorkflowType(),
        cause);
  }

  @Override
  public Promise<Void> newTimer(Duration delay) {
    Objects.requireNonNull(delay);
    long delaySeconds = roundUpToSeconds(delay);
    if (delaySeconds < 0) {
      throw new IllegalArgumentException("negative delay");
    }
    if (delaySeconds == 0) {
      return Workflow.newPromise(null);
    }
    CompletablePromise<Void> timer = Workflow.newPromise();
    long fireTime = context.currentTimeMillis() + TimeUnit.SECONDS.toMillis(delaySeconds);
    timers.addTimer(fireTime, timer);
    CancellationScope.current()
        .getCancellationRequest()
        .thenApply(
            (reason) -> {
              timers.removeTimer(fireTime, timer);
              timer.completeExceptionally(new CancellationException(reason));
              return null;
            });
    return timer;
  }

  @Override
  public <R> R sideEffect(Class<R> resultClass, Type resultType, Func<R> func) {
    DataConverter dataConverter = getDataConverter();
    Optional<Payloads> result =
        context.sideEffect(
            () -> {
              R r = func.apply();
              return dataConverter.toData(r);
            });
    return dataConverter.fromData(result, resultClass, resultType);
  }

  @Override
  public <R> R mutableSideEffect(
      String id, Class<R> resultClass, Type resultType, BiPredicate<R, R> updated, Func<R> func) {
    AtomicReference<R> unserializedResult = new AtomicReference<>();
    Optional<Payloads> payloads =
        context.mutableSideEffect(
            id,
            converter,
            (storedBinary) -> {
              Optional<R> stored =
                  storedBinary.map(
                      (b) -> converter.fromData(Optional.of(b), resultClass, resultType));
              R funcResult =
                  Objects.requireNonNull(
                      func.apply(), "mutableSideEffect function " + "returned null");
              if (!stored.isPresent() || updated.test(stored.get(), funcResult)) {
                unserializedResult.set(funcResult);
                return converter.toData(funcResult);
              }
              return Optional.empty(); // returned only when value doesn't need to be updated
            });
    if (!payloads.isPresent()) {
      throw new IllegalArgumentException(
          "No value found for mutableSideEffectId="
              + id
              + ", during replay it usually indicates a different workflow runId than the original one");
    }
    // An optimization that avoids unnecessary deserialization of the result.
    R unserialized = unserializedResult.get();
    if (unserialized != null) {
      return unserialized;
    }
    return converter.fromData(payloads, resultClass, resultType);
  }

  @Override
  public int getVersion(String changeId, int minSupported, int maxSupported) {
    return context.getVersion(changeId, converter, minSupported, maxSupported);
  }

  void fireTimers() {
    timers.fireTimers(context.currentTimeMillis());
  }

  boolean hasTimersToFire() {
    return timers.hasTimersToFire(context.currentTimeMillis());
  }

  long getNextFireTime() {
    return timers.getNextFireTime();
  }

  public Optional<Payloads> query(String type, Optional<Payloads> args) {
    Functions.Func1<Optional<Payloads>, Optional<Payloads>> callback = queryCallbacks.get(type);
    if (callback == null) {
      throw new IllegalArgumentException(
          "Unknown query type: " + type + ", knownTypes=" + queryCallbacks.keySet());
    }
    return callback.apply(args);
  }

  public void signal(String signalName, Optional<Payloads> args, long eventId) {
    Functions.Proc2<Optional<Payloads>, Long> callback = signalCallbacks.get(signalName);
    if (callback == null) {
      List<SignalData> buffer = signalBuffers.get(signalName);
      if (buffer == null) {
        buffer = new ArrayList<>();
        signalBuffers.put(signalName, buffer);
      }
      buffer.add(new SignalData(args, eventId));
    } else {
      callback.apply(args, eventId);
    }
  }

  @Override
  public void registerQuery(
      String queryType,
      Class<?>[] argTypes,
      Type[] genericArgTypes,
      Functions.Func1<Object[], Object> callback) {
    if (queryCallbacks.containsKey(queryType)) {
      throw new IllegalStateException("Query \"" + queryType + "\" is already registered");
    }
    queryCallbacks.put(
        queryType,
        (input) -> {
          Object[] args = converter.fromDataArray(input, argTypes, genericArgTypes);
          Object result = callback.apply(args);
          return converter.toData(result);
        });
  }

  @Override
  public void registerSignal(
      String signalType,
      Class<?>[] argTypes,
      Type[] genericArgTypes,
      Functions.Proc1<Object[]> callback) {
    if (signalCallbacks.containsKey(signalType)) {
      throw new IllegalStateException("Signal \"" + signalType + "\" is already registered");
    }
    Functions.Proc2<Optional<Payloads>, Long> signalCallback =
        (input, eventId) -> {
          try {
            Object[] args = converter.fromDataArray(input, argTypes, genericArgTypes);
            callback.apply(args);
          } catch (DataConverterException e) {
            logSerializationException(signalType, eventId, e);
          }
        };
    List<SignalData> buffer = signalBuffers.remove(signalType);
    if (buffer != null) {
      for (SignalData signalData : buffer) {
        signalCallback.apply(signalData.getPayload(), signalData.getEventId());
      }
    }
    signalCallbacks.put(signalType, signalCallback);
  }

  void logSerializationException(
      String signalName, Long eventId, DataConverterException exception) {
    log.error(
        "Failure deserializing signal input for \""
            + signalName
            + "\" at eventId "
            + eventId
            + ". Dropping it.",
        exception);
    Workflow.getMetricsScope().counter(MetricsType.CORRUPTED_SIGNALS_COUNTER).inc(1);
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

  public DecisionContext getContext() {
    return context;
  }

  @Override
  public Promise<Void> signalExternalWorkflow(
      WorkflowExecution execution, String signalName, Object[] args) {
    SignalExternalWorkflowParameters parameters = new SignalExternalWorkflowParameters();
    parameters.setSignalName(signalName);
    parameters.setWorkflowId(execution.getWorkflowId());
    parameters.setRunId(execution.getRunId());
    Optional<Payloads> input = getDataConverter().toData(args);
    parameters.setInput(input);
    CompletablePromise<Void> result = Workflow.newPromise();

    Consumer<Exception> cancellationCallback =
        context.signalWorkflowExecution(
            parameters,
            (output, failure) -> {
              if (failure != null) {
                runner.executeInWorkflowThread(
                    "child workflow failure callback",
                    () -> result.completeExceptionally(mapSignalWorkflowException(failure)));
              } else {
                runner.executeInWorkflowThread(
                    "child workflow completion callback", () -> result.complete(output));
              }
            });
    CancellationScope.current()
        .getCancellationRequest()
        .thenApply(
            (reason) -> {
              cancellationCallback.accept(new CancellationException(reason));
              return null;
            });
    return result;
  }

  @Override
  public void sleep(Duration duration) {
    WorkflowThread.await(
        duration.toMillis(),
        "sleep",
        () -> {
          CancellationScope.throwCancelled();
          return false;
        });
  }

  @Override
  public boolean await(Duration timeout, String reason, Supplier<Boolean> unblockCondition) {
    return WorkflowThread.await(timeout.toMillis(), reason, unblockCondition);
  }

  @Override
  public void await(String reason, Supplier<Boolean> unblockCondition) {
    WorkflowThread.await(reason, unblockCondition);
  }

  @Override
  public void continueAsNew(
      Optional<String> workflowType, Optional<ContinueAsNewOptions> options, Object[] args) {
    ContinueAsNewWorkflowExecutionParameters parameters =
        new ContinueAsNewWorkflowExecutionParameters();
    if (workflowType.isPresent()) {
      parameters.setWorkflowType(workflowType.get());
    }
    if (options.isPresent()) {
      ContinueAsNewOptions ops = options.get();
      parameters.setWorkflowRunTimeoutSeconds(roundUpToSeconds(ops.getWorkflowRunTimeout()));
      parameters.setWorkflowTaskTimeoutSeconds(roundUpToSeconds(ops.getWorkflowTaskTimeout()));
      parameters.setTaskList(ops.getTaskList());
    }
    parameters.setInput(getDataConverter().toData(args).orElse(null));
    context.continueAsNewOnCompletion(parameters);
    WorkflowThread.exit(null);
  }

  @Override
  public Promise<Void> cancelWorkflow(WorkflowExecution execution) {
    return context.requestCancelWorkflowExecution(execution);
  }

  private RuntimeException mapSignalWorkflowException(Exception failure) {
    if (failure == null) {
      return null;
    }
    if (failure instanceof CancellationException) {
      return (CancellationException) failure;
    }

    if (!(failure instanceof SignalExternalWorkflowException)) {
      return new IllegalArgumentException("Unexpected exception type: ", failure);
    }
    return (SignalExternalWorkflowException) failure;
  }

  public Scope getMetricsScope() {
    return context.getMetricsScope();
  }

  public boolean isLoggingEnabledInReplay() {
    return context.getEnableLoggingInReplay();
  }

  public <R> R getLastCompletionResult(Class<R> resultClass, Type resultType) {
    DataConverter dataConverter = getDataConverter();
    return dataConverter.fromData(lastCompletionResult, resultClass, resultType);
  }

  @Override
  public void upsertSearchAttributes(Map<String, Object> searchAttributes) {
    if (searchAttributes.isEmpty()) {
      throw new IllegalArgumentException("Empty search attributes");
    }

    SearchAttributes attr =
        InternalUtils.convertMapToSearchAttributes(
            searchAttributes, getDataConverter().getPayloadConverter());
    context.upsertSearchAttributes(attr);
  }
}
