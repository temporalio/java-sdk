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

import static io.temporal.internal.sync.DeterministicRunnerImpl.currentThreadInternal;

import io.temporal.failure.CanceledFailure;
import io.temporal.workflow.CancellationScope;
import java.util.Optional;
import java.util.concurrent.Future;
import java.util.function.Supplier;

/** Thread that is scheduled deterministically by {@link DeterministicRunner}. */
public interface WorkflowThread extends CancellationScope {

  /**
   * Block current thread until unblockCondition is evaluated to true. This method is intended for
   * framework level libraries, never use directly in a workflow implementation.
   *
   * @param reason reason for blocking
   * @param unblockCondition condition that should return true to indicate that thread should
   *     unblock.
   * @throws CanceledFailure if thread (or current cancellation scope was canceled).
   * @throws DestroyWorkflowThreadError if thread was asked to be destroyed.
   */
  static void await(String reason, Supplier<Boolean> unblockCondition)
      throws DestroyWorkflowThreadError {
    currentThreadInternal().yield(reason, unblockCondition);
  }

  /**
   * Creates a new thread instance.
   *
   * @param runnable thread function to run
   * @param detached If this thread is detached from the parent {@link CancellationScope}
   * @return
   */
  static WorkflowThread newThread(Runnable runnable, boolean detached) {
    return newThread(runnable, detached, null);
  }

  static WorkflowThread newThread(Runnable runnable, boolean detached, String name) {
    return (WorkflowThread)
        currentThreadInternal()
            .getWorkflowContext()
            .getWorkflowOutboundInterceptor()
            .newChildThread(runnable, detached, name);
  }

  void start();

  boolean isStarted();

  void setName(String name);

  String getName();

  long getId();

  int getPriority();

  String getStackTrace();

  DeterministicRunnerImpl getRunner();

  SyncWorkflowContext getWorkflowContext();

  boolean runUntilBlocked(long deadlockDetectionTimeout);

  Throwable getUnhandledException();

  boolean isDone();

  Future<?> stopNow();

  void addStackTrace(StringBuilder result);

  void yield(String reason, Supplier<Boolean> unblockCondition) throws DestroyWorkflowThreadError;

  /**
   * Stop executing all workflow threads and puts {@link DeterministicRunner} into closed state. To
   * be called only from a workflow thread.
   *
   * @param value accessible through {@link DeterministicRunner#getExitValue()}.
   */
  static <R> void exit(R value) {
    currentThreadInternal().exitThread(value); // needed to close WorkflowThreadContext
  }

  <R> void exitThread(R value);

  <T> void setThreadLocal(WorkflowThreadLocalInternal<T> key, T value);

  <T> Optional<T> getThreadLocal(WorkflowThreadLocalInternal<T> key);

  WorkflowThreadContext getWorkflowThreadContext();
}
