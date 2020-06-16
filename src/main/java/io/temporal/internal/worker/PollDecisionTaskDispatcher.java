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

import io.temporal.failure.FailureConverter;
import io.temporal.enums.v1.DecisionTaskFailed;
import io.temporal.workflowservice.v1.PollForDecisionTaskResponse;
import io.temporal.workflowservice.v1.RespondDecisionTaskFailedRequest;
import io.temporal.serviceclient.WorkflowServiceStubs;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class PollDecisionTaskDispatcher
    implements ShutdownableTaskExecutor<PollForDecisionTaskResponse> {

  private static final Logger log = LoggerFactory.getLogger(PollDecisionTaskDispatcher.class);
  private final Map<String, Consumer<PollForDecisionTaskResponse>> subscribers =
      new ConcurrentHashMap<>();
  private WorkflowServiceStubs service;
  private Thread.UncaughtExceptionHandler uncaughtExceptionHandler =
      (t, e) -> log.error("uncaught exception", e);
  private AtomicBoolean shutdown = new AtomicBoolean();

  public PollDecisionTaskDispatcher(WorkflowServiceStubs service) {
    this.service = Objects.requireNonNull(service);
  }

  public PollDecisionTaskDispatcher(
      WorkflowServiceStubs service, Thread.UncaughtExceptionHandler exceptionHandler) {
    this.service = Objects.requireNonNull(service);
    if (exceptionHandler != null) {
      this.uncaughtExceptionHandler = exceptionHandler;
    }
  }

  @Override
  public void process(PollForDecisionTaskResponse t) {
    if (isShutdown()) {
      throw new RejectedExecutionException("shutdown");
    }
    String taskListName = t.getWorkflowExecutionTaskList().getName();
    if (subscribers.containsKey(taskListName)) {
      subscribers.get(taskListName).accept(t);
    } else {
      Exception exception =
          new Exception(
              String.format(
                  "No handler is subscribed for the PollForDecisionTaskResponse.WorkflowExecutionTaskList %s",
                  taskListName));
      RespondDecisionTaskFailedRequest request =
          RespondDecisionTaskFailedRequest.newBuilder()
              .setTaskToken(t.getTaskToken())
              .setCause(DecisionTaskFailedCause.ResetStickyTasklist)
              .setFailure(FailureConverter.exceptionToFailure(exception))
              .build();
      log.warn("unexpected", exception);

      try {
        service.blockingStub().respondDecisionTaskFailed(request);
      } catch (Exception e) {
        uncaughtExceptionHandler.uncaughtException(Thread.currentThread(), e);
      }
    }
  }

  public void subscribe(String taskList, Consumer<PollForDecisionTaskResponse> consumer) {
    subscribers.put(taskList, consumer);
  }

  @Override
  public boolean isShutdown() {
    return shutdown.get();
  }

  @Override
  public boolean isTerminated() {
    return shutdown.get();
  }

  @Override
  public void shutdown() {
    shutdown.set(true);
  }

  @Override
  public void shutdownNow() {
    shutdown.set(true);
  }

  @Override
  public void awaitTermination(long timeout, TimeUnit unit) {}
}
