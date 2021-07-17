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

import io.temporal.internal.replay.WorkflowExecutorCache;
import io.temporal.workflow.CancellationScope;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;
import javax.annotation.Nullable;

/**
 * Executes code passed to {@link #newRunner(Runnable)} as well as threads created from it using
 * {@link WorkflowThread#newThread(Runnable, boolean)} deterministically. Requires use of provided
 * wrappers for synchronization and notification instead of native ones.
 */
interface DeterministicRunner {

  boolean debugMode = System.getenv("TEMPORAL_DEBUG") != null;

  long DEFAULT_DEADLOCK_DETECTION_TIMEOUT = 1000;

  static long getDeadlockDetectionTimeout() {
    return debugMode ? Long.MAX_VALUE : DEFAULT_DEADLOCK_DETECTION_TIMEOUT;
  }

  static DeterministicRunner newRunner(Runnable root) {
    return new DeterministicRunnerImpl(root);
  }

  static DeterministicRunner newRunner(Supplier<Long> clock, Runnable root) {
    return new DeterministicRunnerImpl(clock, root);
  }

  /**
   * Create new instance of DeterministicRunner
   *
   * @param workflowContext workflow context to use
   * @param root function that root thread of the runner executes.
   * @param cache WorkflowExecutorCache used cache inflight workflows. New workflow threads will
   *     evict this cache when the thread pool runs out
   * @return instance of the DeterministicRunner.
   */
  static DeterministicRunner newRunner(
      ExecutorService threadPool,
      SyncWorkflowContext workflowContext,
      Runnable root,
      WorkflowExecutorCache cache) {
    return new DeterministicRunnerImpl(threadPool, workflowContext, root, cache);
  }

  /**
   * Create new instance of DeterministicRunner
   *
   * @param workflowContext workflow context to use
   * @param root function that root thread of the runner executes.
   * @return instance of the DeterministicRunner.
   */
  static DeterministicRunner newRunner(
      ExecutorService threadPool, SyncWorkflowContext workflowContext, Runnable root) {
    return new DeterministicRunnerImpl(threadPool, workflowContext, root, null);
  }

  /**
   * ExecuteUntilAllBlocked executes threads one by one in deterministic order until all of them are
   * completed or blocked.
   *
   * @throws Throwable if one of the threads didn't handle an exception.
   * @param deadlockDetectionTimeout the maximum time in milliseconds a thread can run without
   *     calling yield.
   */
  void runUntilAllBlocked(long deadlockDetectionTimeout);

  /** IsDone returns true when all of threads are completed */
  boolean isDone();

  /** @return exit value passed to {@link WorkflowThread#exit(Object)} */
  Object getExitValue();

  /**
   * Request cancellation of the computation. Calls {@link CancellationScope#cancel(String)} on the
   * root scope that wraps the root Runnable.
   */
  void cancel(String reason);

  /**
   * Destroys all threads by throwing {@link DestroyWorkflowThreadError} without waiting for their
   * completion
   */
  void close();

  /** Stack trace of all threads owned by the DeterministicRunner instance */
  String stackTrace();

  /**
   * Executes a runnable in a specially created workflow thread. This newly created thread is given
   * chance to run before any other existing threads. This is used to ensure that some operations
   * (like signal callbacks) are executed before all other threads which is important to guarantee
   * their processing even if they were received after workflow code decided to complete. To be
   * called before runUntilAllBlocked.
   */
  void executeInWorkflowThread(String name, Runnable r);

  /**
   * Creates a new instance of a workflow child thread. To be called only from another workflow
   * thread.
   */
  WorkflowThread newWorkflowThread(Runnable runnable, boolean detached, @Nullable String name);

  /** Creates a new instance of a workflow callback thread. */
  WorkflowThread newCallbackThread(Runnable runnable, @Nullable String name);
}
