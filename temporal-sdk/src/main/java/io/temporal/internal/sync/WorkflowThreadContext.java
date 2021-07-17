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

import static io.temporal.internal.sync.DeterministicRunner.getDeadlockDetectionTimeout;

import com.google.common.base.Throwables;
import io.temporal.workflow.Functions;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.function.Supplier;

public class WorkflowThreadContext {

  // Shared runner lock
  private final Lock lock;
  // Used to block await call
  private final Condition yieldCondition;
  // Used to block runUntilBlocked call
  private final Condition runCondition;
  // Used to block evaluateInCoroutineContext
  private final Condition evaluationCondition;

  private Status status = Status.CREATED;
  private Functions.Proc1<String> evaluationFunction;
  private Throwable unhandledException;
  private boolean inRunUntilBlocked;
  private boolean remainedBlocked;
  private String yieldReason;
  private boolean destroyRequested;
  private Thread currentThread;

  WorkflowThreadContext(Lock lock) {
    this.lock = lock;
    this.yieldCondition = lock.newCondition();
    this.runCondition = lock.newCondition();
    this.evaluationCondition = lock.newCondition();
  }

  public void initialYield() {
    Status status = getStatus();
    if (status == Status.DONE) {
      throw new DestroyWorkflowThreadError("done in initialYield");
    }
    if (status != Status.RUNNING) {
      throw new IllegalStateException("not in RUNNING but in " + status + " state");
    }
    this.yield("created", () -> true);
  }

  public void yield(String reason, Supplier<Boolean> unblockFunction) {
    if (unblockFunction == null) {
      throw new IllegalArgumentException("null unblockFunction");
    }
    // Evaluates unblockFunction out of the lock to avoid deadlocks.
    lock.lock();
    try {
      // TODO: Verify that calling unblockFunction under the lock is a sane thing to do.
      while (!inRunUntilBlocked || !unblockFunction.get()) {
        if (destroyRequested) {
          throw new DestroyWorkflowThreadError();
        }
        status = Status.YIELDED;
        runCondition.signal();
        yieldCondition.await();
        mayBeEvaluate(reason);
        yieldReason = reason;
      }
    } catch (InterruptedException e) {
      // Throwing Error in workflow code aborts workflow task without failing workflow.
      throw new Error("Unexpected interrupt", e);
    } finally {
      setStatus(Status.RUNNING);
      remainedBlocked = false;
      yieldReason = null;
      lock.unlock();
    }
  }

  /**
   * Execute evaluation function by the thread that owns this context if {@link
   * #evaluateInCoroutineContext(Functions.Proc1)} was called.
   *
   * @param reason human readable reason for current thread blockage passed to await call.
   */
  private void mayBeEvaluate(String reason) {
    if (status == Status.EVALUATING) {
      try {
        evaluationFunction.apply(reason);
      } catch (Exception e) {
        evaluationFunction.apply(Throwables.getStackTraceAsString(e));
      } finally {
        status = Status.YIELDED;
        evaluationCondition.signal();
      }
    }
  }

  /**
   * Call function by the thread that owns this context and is currently blocked in a await. Used to
   * get information about current state of the thread like current stack trace.
   *
   * @param function to evaluate. Consumes reason for yielding parameter.
   */
  public void evaluateInCoroutineContext(Functions.Proc1<String> function) {
    lock.lock();
    try {
      if (function == null) {
        throw new IllegalArgumentException("null function");
      }
      if (status != Status.YIELDED && status != Status.RUNNING) {
        throw new IllegalStateException("Not in yielded status: " + status);
      }
      if (evaluationFunction != null) {
        throw new IllegalStateException("Already evaluating");
      }
      if (inRunUntilBlocked) {
        throw new IllegalStateException("Running runUntilBlocked");
      }
      evaluationFunction = function;
      status = Status.EVALUATING;
      yieldCondition.signal();
      while (status == Status.EVALUATING) {
        evaluationCondition.await();
      }
    } catch (InterruptedException e) {
      throw new Error("Unexpected interrupt", e);
    } finally {
      evaluationFunction = null;
      lock.unlock();
    }
  }

  public Status getStatus() {
    lock.lock();
    try {
      return status;
    } finally {
      lock.unlock();
    }
  }

  public void setStatus(Status status) {
    // Unblock runUntilBlocked if thread exited instead of yielding.
    lock.lock();
    try {
      this.status = status;
      if (isDone()) {
        runCondition.signal();
      }
    } finally {
      lock.unlock();
    }
  }

  public boolean isDone() {
    lock.lock();
    try {
      return status == Status.DONE;
    } finally {
      lock.unlock();
    }
  }

  public Throwable getUnhandledException() {
    lock.lock();
    try {
      return unhandledException;
    } finally {
      lock.unlock();
    }
  }

  public void setUnhandledException(Throwable unhandledException) {
    lock.lock();
    try {
      this.unhandledException = unhandledException;
    } finally {
      lock.unlock();
    }
  }

  public void setCurrentThread(Thread currentThread) {
    this.currentThread = currentThread;
  }

  public String getYieldReason() {
    return yieldReason;
  }

  /**
   * @return true if thread made some progress. Which is await was unblocked and some code after it
   *     was executed.
   * @param deadlockDetectionTimeout
   */
  public boolean runUntilBlocked(long deadlockDetectionTimeout) {
    lock.lock();
    try {
      if (status == Status.DONE) {
        return false;
      }
      if (evaluationFunction != null) {
        throw new IllegalStateException("Cannot runUntilBlocked while evaluating");
      }
      inRunUntilBlocked = true;
      if (status != Status.CREATED) {
        status = Status.RUNNING;
      }
      remainedBlocked = true;
      yieldCondition.signal();
      while (status == Status.RUNNING || status == Status.CREATED) {
        if (!runCondition.await(deadlockDetectionTimeout, TimeUnit.MILLISECONDS)) {
          throw new PotentialDeadlockException(
              currentThread.getName(), currentThread.getStackTrace(), this);
        }
        if (evaluationFunction != null) {
          throw new IllegalStateException("Cannot runUntilBlocked while evaluating");
        }
      }
      return !remainedBlocked;
    } catch (InterruptedException e) {
      if (!isDestroyRequested()) {
        throw new Error("Unexpected interrupt", e);
      }
      return true;
    } finally {
      inRunUntilBlocked = false;
      lock.unlock();
    }
  }

  public boolean isDestroyRequested() {
    lock.lock();
    try {
      return destroyRequested;
    } finally {
      lock.unlock();
    }
  }

  public void destroy() {
    lock.lock();
    try {
      destroyRequested = true;
      if (status == Status.CREATED || status == Status.RUNNING || status == Status.DONE) {
        status = Status.DONE;
        return;
      }
    } finally {
      lock.unlock();
    }
    evaluateInCoroutineContext(
        (r) -> {
          throw new DestroyWorkflowThreadError();
        });
    runUntilBlocked(getDeadlockDetectionTimeout());
  }

  /** To be called only from a workflow thread. */
  public void exit() {
    lock.lock();
    try {
      destroyRequested = true;
    } finally {
      lock.unlock();
    }
    throw new DestroyWorkflowThreadError();
  }
}
