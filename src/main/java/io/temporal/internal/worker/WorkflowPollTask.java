/*
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

import com.uber.m3.tally.Scope;
import com.uber.m3.tally.Stopwatch;
import com.uber.m3.util.Duration;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.temporal.PollForDecisionTaskRequest;
import io.temporal.PollForDecisionTaskResponse;
import io.temporal.TaskList;
import io.temporal.internal.metrics.MetricsType;
import io.temporal.serviceclient.GrpcWorkflowServiceFactory;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class WorkflowPollTask implements Poller.PollTask<PollForDecisionTaskResponse> {

  private final Scope metricScope;
  private final GrpcWorkflowServiceFactory service;
  private final String domain;
  private final String taskList;
  private final String identity;
  private static final Logger log = LoggerFactory.getLogger(WorkflowWorker.class);

  WorkflowPollTask(
      GrpcWorkflowServiceFactory service,
      String domain,
      String taskList,
      Scope metricScope,
      String identity) {
    this.identity = Objects.requireNonNull(identity);
    this.service = Objects.requireNonNull(service);
    this.domain = Objects.requireNonNull(domain);
    this.taskList = Objects.requireNonNull(taskList);
    this.metricScope = Objects.requireNonNull(metricScope);
  }

  @Override
  public PollForDecisionTaskResponse poll() {
    metricScope.counter(MetricsType.DECISION_POLL_COUNTER).inc(1);
    Stopwatch sw = metricScope.timer(MetricsType.DECISION_POLL_LATENCY).start();

    PollForDecisionTaskRequest pollRequest =
        PollForDecisionTaskRequest.newBuilder()
            .setDomain(domain)
            .setIdentity(identity)
            .setTaskList(TaskList.newBuilder().setName(taskList).build())
            .build();

    if (log.isDebugEnabled()) {
      log.debug("poll request begin: " + pollRequest);
    }
    PollForDecisionTaskResponse result;
    try {
      result = service.blockingStub().pollForDecisionTask(pollRequest);
    } catch (StatusRuntimeException e) {
      Status.Code code = e.getStatus().getCode();
      if (code == Status.Code.INTERNAL || code == Status.Code.RESOURCE_EXHAUSTED) {
        metricScope.counter(MetricsType.DECISION_POLL_TRANSIENT_FAILED_COUNTER).inc(1);
        throw e;
      } else if (code == Status.Code.UNAVAILABLE) {
        // Happens during shutdown. No need to log error.
        return null;
      } else {
        metricScope.counter(MetricsType.DECISION_POLL_FAILED_COUNTER).inc(1);
        throw e;
      }
    }

    if (log.isDebugEnabled()) {
      log.debug(
          "poll request returned decision task: workflowType="
              + result.getWorkflowType()
              + ", workflowExecution="
              + result.getWorkflowExecution()
              + ", startedEventId="
              + result.getStartedEventId()
              + ", previousStartedEventId="
              + result.getPreviousStartedEventId()
              + (result.getQuery() != null
                  ? ", queryType=" + result.getQuery().getQueryType()
                  : ""));
    }

    if (result == null || result.getTaskToken() == null) {
      metricScope.counter(MetricsType.DECISION_POLL_NO_TASK_COUNTER).inc(1);
      return null;
    }

    metricScope.counter(MetricsType.DECISION_POLL_SUCCEED_COUNTER).inc(1);
    metricScope
        .timer(MetricsType.DECISION_SCHEDULED_TO_START_LATENCY)
        .record(Duration.ofNanos(result.getStartedTimestamp() - result.getScheduledTimestamp()));
    sw.stop();
    return result;
  }
}
