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

package io.temporal.internal.worker;

import static io.temporal.internal.common.GrpcRetryer.DEFAULT_SERVICE_OPERATION_RETRY_OPTIONS;

import com.google.common.base.Strings;
import com.google.protobuf.ByteString;
import com.uber.m3.tally.Scope;
import com.uber.m3.tally.Stopwatch;
import com.uber.m3.util.ImmutableMap;
import io.temporal.internal.common.GrpcRetryer;
import io.temporal.internal.common.RpcRetryOptions;
import io.temporal.internal.common.WorkflowExecutionHistory;
import io.temporal.internal.common.WorkflowExecutionUtils;
import io.temporal.internal.logging.LoggerTag;
import io.temporal.internal.metrics.MetricsTag;
import io.temporal.internal.metrics.MetricsType;
import io.temporal.common.v1.Payloads;
import io.temporal.common.v1.WorkflowExecution;
import io.temporal.common.v1.WorkflowType;
import io.temporal.history.v1.History;
import io.temporal.history.v1.HistoryEvent;
import io.temporal.history.v1.WorkflowExecutionStartedEventAttributes;
import io.temporal.query.v1.WorkflowQuery;
import io.temporal.workflowservice.v1.GetWorkflowExecutionHistoryResponse;
import io.temporal.workflowservice.v1.PollForDecisionTaskResponse;
import io.temporal.workflowservice.v1.RespondDecisionTaskCompletedRequest;
import io.temporal.workflowservice.v1.RespondDecisionTaskFailedRequest;
import io.temporal.workflowservice.v1.RespondQueryTaskCompletedRequest;
import io.temporal.serviceclient.WorkflowServiceStubs;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.function.Consumer;
import org.slf4j.MDC;

public final class WorkflowWorker
    implements SuspendableWorker, Consumer<PollForDecisionTaskResponse> {

  private static final String POLL_THREAD_NAME_PREFIX = "Workflow Poller taskList=";

  private SuspendableWorker poller = new NoopSuspendableWorker();
  private PollTaskExecutor<PollForDecisionTaskResponse> pollTaskExecutor;
  private final DecisionTaskHandler handler;
  private final WorkflowServiceStubs service;
  private final String namespace;
  private final String taskList;
  private final SingleWorkerOptions options;
  private final String stickyTaskListName;
  private final WorkflowRunLockManager runLocks = new WorkflowRunLockManager();

  public WorkflowWorker(
      WorkflowServiceStubs service,
      String namespace,
      String taskList,
      SingleWorkerOptions options,
      DecisionTaskHandler handler,
      String stickyTaskListName) {
    this.service = Objects.requireNonNull(service);
    this.namespace = Objects.requireNonNull(namespace);
    this.taskList = Objects.requireNonNull(taskList);
    this.handler = handler;
    this.stickyTaskListName = stickyTaskListName;

    PollerOptions pollerOptions = options.getPollerOptions();
    if (pollerOptions.getPollThreadNamePrefix() == null) {
      pollerOptions =
          PollerOptions.newBuilder(pollerOptions)
              .setPollThreadNamePrefix(
                  POLL_THREAD_NAME_PREFIX + "\"" + taskList + "\", namespace=\"" + namespace + "\"")
              .build();
    }
    this.options = SingleWorkerOptions.newBuilder(options).setPollerOptions(pollerOptions).build();
  }

  @Override
  public void start() {
    if (handler.isAnyTypeSupported()) {
      pollTaskExecutor =
          new PollTaskExecutor<>(namespace, taskList, options, new TaskHandlerImpl(handler));
      poller =
          new Poller<>(
              options.getIdentity(),
              new WorkflowPollTask(
                  service, namespace, taskList, options.getMetricsScope(), options.getIdentity()),
              pollTaskExecutor,
              options.getPollerOptions(),
              options.getMetricsScope());
      poller.start();
      options.getMetricsScope().counter(MetricsType.WORKER_START_COUNTER).inc(1);
    }
  }

  @Override
  public boolean isStarted() {
    if (poller == null) {
      return false;
    }
    return poller.isStarted();
  }

  @Override
  public boolean isShutdown() {
    if (poller == null) {
      return true;
    }
    return poller.isShutdown();
  }

  @Override
  public boolean isTerminated() {
    if (poller == null) {
      return true;
    }
    return poller.isTerminated();
  }

  public Optional<Payloads> queryWorkflowExecution(
      WorkflowExecution exec, String queryType, Optional<Payloads> args) throws Exception {
    GetWorkflowExecutionHistoryResponse historyResponse =
        WorkflowExecutionUtils.getHistoryPage(service, namespace, exec, ByteString.EMPTY);
    History history = historyResponse.getHistory();
    WorkflowExecutionHistory workflowExecutionHistory = new WorkflowExecutionHistory(history);
    return queryWorkflowExecution(
        queryType, args, workflowExecutionHistory, historyResponse.getNextPageToken());
  }

  public Optional<Payloads> queryWorkflowExecution(
      String jsonSerializedHistory, String queryType, Optional<Payloads> args) throws Exception {
    WorkflowExecutionHistory history = WorkflowExecutionHistory.fromJson(jsonSerializedHistory);
    return queryWorkflowExecution(queryType, args, history, ByteString.EMPTY);
  }

  public Optional<Payloads> queryWorkflowExecution(
      WorkflowExecutionHistory history, String queryType, Optional<Payloads> args)
      throws Exception {
    return queryWorkflowExecution(queryType, args, history, ByteString.EMPTY);
  }

  private Optional<Payloads> queryWorkflowExecution(
      String queryType,
      Optional<Payloads> args,
      WorkflowExecutionHistory history,
      ByteString nextPageToken)
      throws Exception {
    WorkflowQuery.Builder query = WorkflowQuery.newBuilder().setQueryType(queryType);
    if (args.isPresent()) {
      query.setQueryArgs(args.get());
    }
    PollForDecisionTaskResponse.Builder task =
        PollForDecisionTaskResponse.newBuilder()
            .setWorkflowExecution(history.getWorkflowExecution())
            .setStartedEventId(Long.MAX_VALUE)
            .setPreviousStartedEventId(Long.MAX_VALUE)
            .setNextPageToken(nextPageToken)
            .setQuery(query);
    List<HistoryEvent> events = history.getEvents();
    HistoryEvent startedEvent = events.get(0);
    WorkflowExecutionStartedEventAttributes started =
        startedEvent.getWorkflowExecutionStartedEventAttributes();
    if (started == null) {
      throw new IllegalStateException(
          "First event of the history is not WorkflowExecutionStarted: " + startedEvent);
    }
    WorkflowType workflowType = started.getWorkflowType();
    task.setWorkflowType(workflowType);
    task.setHistory(History.newBuilder().addAllEvents(events));
    DecisionTaskHandler.Result result = handler.handleDecisionTask(task.build());
    if (result.getQueryCompleted() != null) {
      RespondQueryTaskCompletedRequest r = result.getQueryCompleted();
      if (!r.getErrorMessage().isEmpty()) {
        throw new RuntimeException(
            "query failure for "
                + history.getWorkflowExecution()
                + ", queryType="
                + queryType
                + ", args="
                + args
                + ", error="
                + r.getErrorMessage());
      }
      if (r.hasQueryResult()) {
        return Optional.of(r.getQueryResult());
      } else {
        return Optional.empty();
      }
    }
    throw new RuntimeException("Query returned wrong response: " + result);
  }

  @Override
  public void shutdown() {
    if (poller == null) {
      return;
    }
    poller.shutdown();
  }

  @Override
  public void shutdownNow() {
    if (poller == null) {
      return;
    }
    poller.shutdownNow();
  }

  @Override
  public void awaitTermination(long timeout, TimeUnit unit) {
    if (poller == null || !poller.isStarted()) {
      return;
    }

    poller.awaitTermination(timeout, unit);
  }

  @Override
  public void suspendPolling() {
    if (poller == null) {
      return;
    }
    poller.suspendPolling();
  }

  @Override
  public void resumePolling() {
    if (poller == null) {
      return;
    }
    poller.resumePolling();
  }

  @Override
  public boolean isSuspended() {
    if (poller == null) {
      return false;
    }
    return poller.isSuspended();
  }

  @Override
  public void accept(PollForDecisionTaskResponse pollForDecisionTaskResponse) {
    pollTaskExecutor.process(pollForDecisionTaskResponse);
  }

  private class TaskHandlerImpl
      implements PollTaskExecutor.TaskHandler<PollForDecisionTaskResponse> {

    final DecisionTaskHandler handler;

    private TaskHandlerImpl(DecisionTaskHandler handler) {
      this.handler = handler;
    }

    @Override
    public void handle(PollForDecisionTaskResponse task) throws Exception {
      Scope metricsScope =
          options
              .getMetricsScope()
              .tagged(ImmutableMap.of(MetricsTag.WORKFLOW_TYPE, task.getWorkflowType().getName()));

      MDC.put(LoggerTag.WORKFLOW_ID, task.getWorkflowExecution().getWorkflowId());
      MDC.put(LoggerTag.WORKFLOW_TYPE, task.getWorkflowType().getName());
      MDC.put(LoggerTag.RUN_ID, task.getWorkflowExecution().getRunId());

      Lock runLock = null;
      if (!Strings.isNullOrEmpty(stickyTaskListName)) {
        runLock = runLocks.getLockForLocking(task.getWorkflowExecution().getRunId());
        runLock.lock();
      }

      try {
        Stopwatch sw = metricsScope.timer(MetricsType.DECISION_EXECUTION_LATENCY).start();
        DecisionTaskHandler.Result response = handler.handleDecisionTask(task);
        sw.stop();

        sw = metricsScope.timer(MetricsType.DECISION_RESPONSE_LATENCY).start();
        sendReply(service, task.getTaskToken(), response);
        sw.stop();

        metricsScope.counter(MetricsType.DECISION_TASK_COMPLETED_COUNTER).inc(1);
      } finally {
        MDC.remove(LoggerTag.WORKFLOW_ID);
        MDC.remove(LoggerTag.WORKFLOW_TYPE);
        MDC.remove(LoggerTag.RUN_ID);

        if (runLock != null) {
          runLocks.unlock(task.getWorkflowExecution().getRunId());
        }
      }
    }

    @Override
    public Throwable wrapFailure(PollForDecisionTaskResponse task, Throwable failure) {
      WorkflowExecution execution = task.getWorkflowExecution();
      return new RuntimeException(
          "Failure processing decision task. WorkflowId="
              + execution.getWorkflowId()
              + ", RunId="
              + execution.getRunId(),
          failure);
    }

    private void sendReply(
        WorkflowServiceStubs service, ByteString taskToken, DecisionTaskHandler.Result response) {
      RpcRetryOptions ro = response.getRequestRetryOptions();
      RespondDecisionTaskCompletedRequest taskCompleted = response.getTaskCompleted();
      if (taskCompleted != null) {
        ro = RpcRetryOptions.newBuilder().setRetryOptions(ro).validateBuildWithDefaults();

        RespondDecisionTaskCompletedRequest request =
            taskCompleted
                .toBuilder()
                .setIdentity(options.getIdentity())
                .setTaskToken(taskToken)
                .build();
        GrpcRetryer.retry(ro, () -> service.blockingStub().respondDecisionTaskCompleted(request));
      } else {
        RespondDecisionTaskFailedRequest taskFailed = response.getTaskFailed();
        if (taskFailed != null) {
          ro =
              RpcRetryOptions.newBuilder(DEFAULT_SERVICE_OPERATION_RETRY_OPTIONS)
                  .setRetryOptions(ro)
                  .validateBuildWithDefaults();

          RespondDecisionTaskFailedRequest request =
              taskFailed
                  .toBuilder()
                  .setIdentity(options.getIdentity())
                  .setTaskToken(taskToken)
                  .build();
          GrpcRetryer.retry(ro, () -> service.blockingStub().respondDecisionTaskFailed(request));
        } else {
          RespondQueryTaskCompletedRequest queryCompleted = response.getQueryCompleted();
          if (queryCompleted != null) {
            queryCompleted = queryCompleted.toBuilder().setTaskToken(taskToken).build();
            // Do not retry query response.
            service.blockingStub().respondQueryTaskCompleted(queryCompleted);
          }
        }
      }
    }
  }
}
