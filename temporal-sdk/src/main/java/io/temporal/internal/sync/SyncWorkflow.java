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

import io.temporal.api.common.v1.Payloads;
import io.temporal.api.common.v1.WorkflowType;
import io.temporal.api.enums.v1.EventType;
import io.temporal.api.failure.v1.Failure;
import io.temporal.api.history.v1.HistoryEvent;
import io.temporal.api.history.v1.WorkflowExecutionStartedEventAttributes;
import io.temporal.api.query.v1.WorkflowQuery;
import io.temporal.client.WorkflowClient;
import io.temporal.common.context.ContextPropagator;
import io.temporal.common.converter.DataConverter;
import io.temporal.internal.replay.ReplayWorkflow;
import io.temporal.internal.replay.ReplayWorkflowContext;
import io.temporal.internal.replay.WorkflowExecutorCache;
import io.temporal.internal.worker.WorkflowExecutionException;
import io.temporal.worker.WorkflowImplementationOptions;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SyncWorkflow supports workflows that use synchronous blocking code. An instance is created per
 * cached workflow run.
 */
class SyncWorkflow implements ReplayWorkflow {

  private static final Logger log = LoggerFactory.getLogger(SyncWorkflow.class);

  private final DataConverter dataConverter;
  private final List<ContextPropagator> contextPropagators;
  private final ExecutorService threadPool;
  private final SyncWorkflowDefinition workflow;
  WorkflowImplementationOptions workflowImplementationOptions;
  private final WorkflowExecutorCache cache;
  private final long defaultDeadlockDetectionTimeout;
  private WorkflowExecuteRunnable workflowProc;
  private DeterministicRunner runner;

  public SyncWorkflow(
      SyncWorkflowDefinition workflow,
      WorkflowImplementationOptions workflowImplementationOptions,
      DataConverter dataConverter,
      ExecutorService threadPool,
      WorkflowExecutorCache cache,
      List<ContextPropagator> contextPropagators,
      long defaultDeadlockDetectionTimeout) {
    this.workflow = Objects.requireNonNull(workflow);
    this.workflowImplementationOptions =
        workflowImplementationOptions == null
            ? WorkflowImplementationOptions.newBuilder().build()
            : workflowImplementationOptions;
    this.dataConverter = Objects.requireNonNull(dataConverter);
    this.threadPool = Objects.requireNonNull(threadPool);
    this.cache = cache;
    this.contextPropagators = contextPropagators;
    this.defaultDeadlockDetectionTimeout = defaultDeadlockDetectionTimeout;
  }

  @Override
  public WorkflowImplementationOptions getWorkflowImplementationOptions() {
    return workflowImplementationOptions;
  }

  @Override
  public void start(HistoryEvent event, ReplayWorkflowContext context) {
    if (event.getEventType() != EventType.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED
        || !event.hasWorkflowExecutionStartedEventAttributes()) {
      throw new IllegalArgumentException(
          "first event is not WorkflowExecutionStarted, but " + event.getEventType());
    }

    WorkflowExecutionStartedEventAttributes startEvent =
        event.getWorkflowExecutionStartedEventAttributes();
    WorkflowType workflowType = startEvent.getWorkflowType();
    if (workflow == null) {
      throw new IllegalArgumentException("Unknown workflow type: " + workflowType);
    }

    Optional<Payloads> result =
        startEvent.hasLastCompletionResult()
            ? Optional.of(startEvent.getLastCompletionResult())
            : Optional.empty();
    Optional<Failure> lastFailure =
        startEvent.hasContinuedFailure()
            ? Optional.of(startEvent.getContinuedFailure())
            : Optional.empty();
    SyncWorkflowContext syncContext =
        new SyncWorkflowContext(
            context,
            workflowImplementationOptions,
            dataConverter,
            contextPropagators,
            result,
            lastFailure);

    workflowProc = new WorkflowExecuteRunnable(syncContext, workflow, startEvent);
    // The following order is ensured by this code and DeterministicRunner implementation:
    // 1. workflow.initialize
    // 2. signal handler (if signalWithStart was called)
    // 3. main workflow method
    runner =
        DeterministicRunner.newRunner(
            threadPool,
            syncContext,
            () -> {
              workflow.initialize();
              WorkflowInternal.newWorkflowMethodThread(
                      () -> workflowProc.run(), DeterministicRunnerImpl.WORKFLOW_MAIN_THREAD_NAME)
                  .start();
            },
            cache);
  }

  @Override
  public void handleSignal(String signalName, Optional<Payloads> input, long eventId) {
    runner.executeInWorkflowThread(
        "signal " + signalName, () -> workflowProc.handleSignal(signalName, input, eventId));
  }

  @Override
  public boolean eventLoop() {
    if (runner == null) {
      return false;
    }
    runner.runUntilAllBlocked(defaultDeadlockDetectionTimeout);
    return runner.isDone() || workflowProc.isDone(); // Do not wait for all other threads.
  }

  @Override
  public Optional<Payloads> getOutput() {
    return workflowProc.getOutput();
  }

  @Override
  public void cancel(String reason) {
    runner.cancel(reason);
  }

  @Override
  public void close() {
    if (runner != null) {
      runner.close();
    }
  }

  @Override
  public Optional<Payloads> query(WorkflowQuery query) {
    if (WorkflowClient.QUERY_TYPE_REPLAY_ONLY.equals(query.getQueryType())) {
      return Optional.empty();
    }
    if (WorkflowClient.QUERY_TYPE_STACK_TRACE.equals(query.getQueryType())) {
      return dataConverter.toPayloads(runner.stackTrace());
    }
    Optional<Payloads> args =
        query.hasQueryArgs() ? Optional.of(query.getQueryArgs()) : Optional.empty();
    return workflowProc.handleQuery(query.getQueryType(), args);
  }

  @Override
  public WorkflowExecutionException mapUnexpectedException(Throwable failure) {
    return POJOWorkflowImplementationFactory.mapToWorkflowExecutionException(
        failure, dataConverter);
  }
}
