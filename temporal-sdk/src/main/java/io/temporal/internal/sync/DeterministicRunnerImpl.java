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

import com.google.common.primitives.Ints;
import com.uber.m3.tally.NoopScope;
import com.uber.m3.tally.Scope;
import io.temporal.api.command.v1.ContinueAsNewWorkflowExecutionCommandAttributes;
import io.temporal.api.command.v1.SignalExternalWorkflowExecutionCommandAttributes;
import io.temporal.api.common.v1.Payloads;
import io.temporal.api.common.v1.SearchAttributes;
import io.temporal.api.common.v1.WorkflowExecution;
import io.temporal.api.common.v1.WorkflowType;
import io.temporal.api.failure.v1.Failure;
import io.temporal.common.context.ContextPropagator;
import io.temporal.common.converter.DataConverter;
import io.temporal.failure.CanceledFailure;
import io.temporal.internal.WorkflowThreadMarker;
import io.temporal.internal.context.ContextThreadLocal;
import io.temporal.internal.replay.ExecuteActivityParameters;
import io.temporal.internal.replay.ExecuteLocalActivityParameters;
import io.temporal.internal.replay.ReplayWorkflowContext;
import io.temporal.internal.replay.StartChildWorkflowExecutionParameters;
import io.temporal.internal.replay.WorkflowExecutorCache;
import io.temporal.serviceclient.CheckedExceptionWrapper;
import io.temporal.workflow.Functions;
import io.temporal.workflow.Functions.Func;
import io.temporal.workflow.Functions.Func1;
import io.temporal.workflow.Promise;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Throws Error in case of any unexpected condition. It is to fail a workflow task, not a workflow.
 */
class DeterministicRunnerImpl implements DeterministicRunner {

  private static final int ROOT_THREAD_PRIORITY = 0;
  private static final int CALLBACK_THREAD_PRIORITY = 10;
  private static final int WORKFLOW_THREAD_PRIORITY = 20000000;

  static final String WORKFLOW_ROOT_THREAD_NAME = "workflow-root";
  static final String WORKFLOW_MAIN_THREAD_NAME = "workflow-method";

  private static final Logger log = LoggerFactory.getLogger(DeterministicRunnerImpl.class);
  private static final ThreadLocal<WorkflowThread> currentThreadThreadLocal = new ThreadLocal<>();
  // Note that threads field is a set. So we need to make sure that getPriority never returns the
  // same value for different threads. We use addedThreads variable for this. Protected by lock
  private final Set<WorkflowThread> threads =
      new TreeSet<>((t1, t2) -> Ints.compare(t1.getPriority(), t2.getPriority()));
  // Values from RunnerLocalInternal
  private final Map<RunnerLocalInternal<?>, Object> runnerLocalMap = new HashMap<>();
  private final List<WorkflowThread> workflowThreadsToAdd =
      Collections.synchronizedList(new ArrayList<>());
  private final List<WorkflowThread> callbackThreadsToAdd =
      Collections.synchronizedList(new ArrayList<>());
  private final List<NamedRunnable> toExecuteInWorkflowThread = new ArrayList<>();
  private final Lock lock = new ReentrantLock();
  private final Runnable rootRunnable;
  private final ExecutorService threadPool;
  private final SyncWorkflowContext workflowContext;
  private final WorkflowExecutorCache cache;
  private boolean inRunUntilAllBlocked;
  private boolean closeRequested;
  private boolean closed;
  private int addedThreads;

  private static class NamedRunnable {
    private final String name;
    private final Runnable runnable;

    private NamedRunnable(String name, Runnable runnable) {
      this.name = name;
      this.runnable = runnable;
    }
  }

  static WorkflowThread currentThreadInternal() {
    WorkflowThread result = currentThreadThreadLocal.get();
    if (result == null) {
      throw new Error("Called from non workflow or workflow callback thread");
    }
    return result;
  }

  static Optional<WorkflowThread> currentThreadInternalIfPresent() {
    WorkflowThread result = currentThreadThreadLocal.get();
    if (result == null) {
      return Optional.empty();
    }
    return Optional.of(result);
  }

  static void setCurrentThreadInternal(WorkflowThread coroutine) {
    if (coroutine != null) {
      currentThreadThreadLocal.set(coroutine);
      WorkflowThreadMarkerAccessor.markAsWorkflowThread();
    } else {
      currentThreadThreadLocal.set(null);
      WorkflowThreadMarkerAccessor.markAsNonWorkflowThread();
    }
  }

  /**
   * Used to check for failedPromises that contain an error, but never where accessed. It is to
   * avoid failure swallowing by failedPromises which is very hard to troubleshoot.
   */
  private final Set<Promise> failedPromises = new HashSet<>();

  private boolean exitRequested;
  private Object exitValue;
  private WorkflowThread rootWorkflowThread;
  private final CancellationScopeImpl runnerCancellationScope;

  DeterministicRunnerImpl(Runnable root) {
    this(System::currentTimeMillis, root);
  }

  DeterministicRunnerImpl(Supplier<Long> clock, Runnable root) {
    this(getDefaultThreadPool(), newDummySyncWorkflowContext(), root, null);
  }

  DeterministicRunnerImpl(
      ExecutorService threadPool, SyncWorkflowContext workflowContext, Runnable root) {
    this(threadPool, workflowContext, root, null);
  }

  DeterministicRunnerImpl(
      ExecutorService threadPool,
      SyncWorkflowContext workflowContext,
      Runnable root,
      WorkflowExecutorCache cache) {
    this.threadPool = threadPool;
    this.workflowContext =
        workflowContext != null ? workflowContext : newDummySyncWorkflowContext();
    this.workflowContext.setRunner(this);
    this.cache = cache;
    runnerCancellationScope = new CancellationScopeImpl(true, null, null);
    this.rootRunnable = root;
  }

  private static SyncWorkflowContext newDummySyncWorkflowContext() {
    SyncWorkflowContext context =
        new SyncWorkflowContext(
            new DummyReplayWorkflowContext(), DataConverter.getDefaultInstance(), null, null, null);
    context.initHeadOutboundCallsInterceptor(context);
    context.initHeadInboundCallsInterceptor(new BaseRootWorkflowInboundCallsInterceptor(context));
    return context;
  }

  SyncWorkflowContext getWorkflowContext() {
    return workflowContext;
  }

  @Override
  public void runUntilAllBlocked(long deadlockDetectionTimeout) {
    if (rootWorkflowThread == null) {
      rootWorkflowThread = newRootThread(rootRunnable);
      threads.add(rootWorkflowThread);
      rootWorkflowThread.start();
    }
    if (!rootWorkflowThread.isStarted()) {
      throw new IllegalStateException("start not called");
    }
    lock.lock();
    try {
      checkClosed();

      inRunUntilAllBlocked = true;
      Throwable unhandledException = null;
      // Keep repeating until at least one of the threads makes progress.
      boolean progress;
      outerLoop:
      do {
        if (!toExecuteInWorkflowThread.isEmpty()) {
          for (NamedRunnable nr : toExecuteInWorkflowThread) {
            workflowContext.getWorkflowInboundInterceptor().newCallbackThread(nr.runnable, nr.name);
          }

          // It is important to prepend threads as there are callbacks
          // like signals that have to run before any other threads.
          // Otherwise signal might be never processed if it was received
          // after workflow decided to close.
          // Adding the callbacks in the same order as they appear in history.
          for (int i = callbackThreadsToAdd.size() - 1; i >= 0; i--) {
            threads.add(callbackThreadsToAdd.get(i));
          }
          callbackThreadsToAdd.clear();
        }
        toExecuteInWorkflowThread.clear();
        progress = false;
        Iterator<WorkflowThread> ci = threads.iterator();
        while (ci.hasNext()) {
          WorkflowThread c = ci.next();
          progress = c.runUntilBlocked(deadlockDetectionTimeout) || progress;
          if (exitRequested) {
            close();
            break outerLoop;
          }
          if (c.isDone()) {
            ci.remove();
            if (c.getUnhandledException() != null) {
              unhandledException = c.getUnhandledException();
              break;
            }
          }
        }
        if (unhandledException != null) {
          close();
          throw WorkflowInternal.wrap(unhandledException);
        }
        threads.addAll(workflowThreadsToAdd);
        workflowThreadsToAdd.clear();
      } while (progress && !threads.isEmpty());
    } catch (PotentialDeadlockException e) {
      StringBuilder dump = new StringBuilder();
      for (WorkflowThread t : threads) {
        if (t.getWorkflowThreadContext() != e.getWorkflowThreadContext()) {
          if (dump.length() > 0) {
            dump.append("\n");
          }
          dump.append(t.getStackTrace());
        }
      }
      e.setStackDump(dump.toString());
      throw e;
    } finally {
      inRunUntilAllBlocked = false;
      lock.unlock();
      // Close was requested while running
      if (closeRequested) {
        close();
      }
    }
  }

  @Override
  public boolean isDone() {
    lock.lock();
    try {
      return closed || threads.isEmpty();
    } finally {
      lock.unlock();
    }
  }

  @Override
  public Object getExitValue() {
    lock.lock();
    try {
      if (!closed) {
        throw new Error("not done");
      }
    } finally {
      lock.unlock();
    }
    return exitValue;
  }

  @Override
  public void cancel(String reason) {
    executeInWorkflowThread("cancel workflow callback", () -> rootWorkflowThread.cancel(reason));
  }

  @Override
  public void close() {
    List<Future<?>> threadFutures = new ArrayList<>();
    lock.lock();
    if (closed) {
      lock.unlock();
      return;
    }
    // Do not close while runUntilAllBlocked executes.
    // closeRequested tells it to call close() at the end.
    closeRequested = true;
    if (inRunUntilAllBlocked) {
      lock.unlock();
      return;
    }
    try {
      for (WorkflowThread c : workflowThreadsToAdd) {
        threads.add(c);
      }
      workflowThreadsToAdd.clear();

      for (WorkflowThread c : threads) {
        threadFutures.add(c.stopNow());
      }
      threads.clear();

      // We cannot use an iterator to unregister failed Promises since f.get()
      // will remove the promise directly from failedPromises. This causes an
      // ConcurrentModificationException
      // For this reason we will loop over a copy of failedPromises.
      Set<Promise> failedPromisesLoop = new HashSet<>(failedPromises);
      for (Promise f : failedPromisesLoop) {
        if (!f.isCompleted()) {
          throw new Error("expected failed");
        }
        try {
          f.get();
          throw new Error("unreachable");
        } catch (RuntimeException e) {
          log.warn(
              "Promise completed with exception and was never accessed. The ignored exception:",
              CheckedExceptionWrapper.unwrap(e));
        }
      }
    } finally {
      closed = true;
      lock.unlock();
    }

    // Context is destroyed in c.StopNow(). Wait on all tasks outside the lock since
    // these tasks use the same lock to execute.
    for (Future<?> future : threadFutures) {
      try {
        future.get();
      } catch (InterruptedException e) {
        throw new Error("Unexpected interrupt", e);
      } catch (ExecutionException e) {
        throw new Error("Unexpected failure stopping coroutine", e);
      }
    }
  }

  @Override
  public String stackTrace() {
    StringBuilder result = new StringBuilder();
    lock.lock();
    try {
      if (closed) {
        return "Workflow is closed.";
      }
      for (WorkflowThread coroutine : threads) {
        if (result.length() > 0) {
          result.append("\n");
        }
        coroutine.addStackTrace(result);
      }
    } finally {
      lock.unlock();
    }
    return result.toString();
  }

  private void checkClosed() {
    if (closed) {
      throw new Error("closed");
    }
  }

  /** Creates a new instance of a root workflow thread. */
  private WorkflowThread newRootThread(Runnable runnable) {
    String name = WORKFLOW_ROOT_THREAD_NAME;
    // TODO: workflow instance specific thread name
    // String name = "workflow[" + workflowContext.getContext().getWorkflowId() + "]-root";
    if (rootWorkflowThread != null) {
      throw new IllegalStateException(
          "newRootThread can be called only if there is no existing root workflow thread");
    }
    rootWorkflowThread =
        new WorkflowThreadImpl(
            threadPool,
            this,
            name,
            ROOT_THREAD_PRIORITY,
            false,
            runnerCancellationScope,
            runnable,
            cache,
            getContextPropagators(),
            getPropagatedContexts());
    return rootWorkflowThread;
  }

  @Override
  public WorkflowThread newWorkflowThread(
      Runnable runnable, boolean detached, @Nullable String name) {
    if (name == null) {
      name = "workflow[" + workflowContext.getContext().getWorkflowId() + "]-" + addedThreads;
    }
    if (rootWorkflowThread == null) {
      throw new IllegalStateException(
          "newChildThread can be called only with existing root workflow thread");
    }
    checkWorkflowThreadOnly();
    checkClosed();
    WorkflowThread result =
        new WorkflowThreadImpl(
            threadPool,
            this,
            name,
            WORKFLOW_THREAD_PRIORITY + (addedThreads++),
            detached,
            CancellationScopeImpl.current(),
            runnable,
            cache,
            getContextPropagators(),
            getPropagatedContexts());
    workflowThreadsToAdd.add(result); // This is synchronized collection.
    return result;
  }

  @Override
  public WorkflowThread newCallbackThread(Runnable runnable, @Nullable String name) {
    if (name == null) {
      name = "workflow[" + workflowContext.getContext().getWorkflowId() + "]-" + addedThreads;
    }
    WorkflowThread result =
        new WorkflowThreadImpl(
            threadPool,
            this,
            name,
            CALLBACK_THREAD_PRIORITY
                + (addedThreads++), // maintain the order in toExecuteInWorkflowThread
            false,
            runnerCancellationScope,
            runnable,
            cache,
            getContextPropagators(),
            getPropagatedContexts());
    callbackThreadsToAdd.add(result);
    return result;
  }

  /**
   * Executes before any other threads next time runUntilBlockedCalled. Must never be called from
   * any workflow threads.
   */
  @Override
  public void executeInWorkflowThread(String name, Runnable runnable) {
    lock.lock();
    try {
      checkClosed();
      toExecuteInWorkflowThread.add(new NamedRunnable(name, runnable));
    } finally {
      lock.unlock();
    }
  }

  Lock getLock() {
    return lock;
  }

  /** Register a promise that had failed but wasn't accessed yet. */
  void registerFailedPromise(Promise promise) {
    failedPromises.add(promise);
  }

  /** Forget a failed promise as it was accessed. */
  void forgetFailedPromise(Promise promise) {
    failedPromises.remove(promise);
  }

  <R> void exit(R value) {
    checkClosed();
    checkWorkflowThreadOnly();
    this.exitValue = value;
    this.exitRequested = true;
  }

  private void checkWorkflowThreadOnly() {
    if (!inRunUntilAllBlocked) {
      throw new Error("called from non workflow thread");
    }
  }

  @SuppressWarnings("unchecked")
  <T> Optional<T> getRunnerLocal(RunnerLocalInternal<T> key) {
    if (!runnerLocalMap.containsKey(key)) {
      return Optional.empty();
    }
    return Optional.of((T) runnerLocalMap.get(key));
  }

  <T> void setRunnerLocal(RunnerLocalInternal<T> key, T value) {
    runnerLocalMap.put(key, value);
  }

  /**
   * If we're executing as part of a workflow, get the current thread's context. Otherwise get the
   * context info from the workflow context.
   */
  private Map<String, Object> getPropagatedContexts() {
    if (currentThreadThreadLocal.get() != null) {
      return ContextThreadLocal.getCurrentContextForPropagation();
    } else {
      return workflowContext.getContext().getPropagatedContexts();
    }
  }

  private List<ContextPropagator> getContextPropagators() {
    if (currentThreadThreadLocal.get() != null) {
      return ContextThreadLocal.getContextPropagators();
    } else {
      return workflowContext.getContext().getContextPropagators();
    }
  }

  private static ThreadPoolExecutor getDefaultThreadPool() {
    ThreadPoolExecutor result =
        new ThreadPoolExecutor(0, 1000, 1, TimeUnit.SECONDS, new SynchronousQueue<>());
    result.setThreadFactory(r -> new Thread(r, "deterministic runner thread"));
    return result;
  }

  private static final class DummyReplayWorkflowContext implements ReplayWorkflowContext {

    private final Timer timer = new Timer();

    @Override
    public WorkflowExecution getWorkflowExecution() {
      throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public WorkflowExecution getParentWorkflowExecution() {
      throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public WorkflowType getWorkflowType() {
      return WorkflowType.newBuilder().setName("dummy-workflow").build();
    }

    @Override
    public boolean isCancelRequested() {
      throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public ContinueAsNewWorkflowExecutionCommandAttributes getContinueAsNewOnCompletion() {
      throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public void setContinueAsNewOnCompletion(
        ContinueAsNewWorkflowExecutionCommandAttributes attributes) {
      throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public Optional<String> getContinuedExecutionRunId() {
      throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public String getTaskQueue() {
      return "dummy-task-queue";
    }

    @Override
    public String getNamespace() {
      return "dummy-namespace";
    }

    @Override
    public String getWorkflowId() {
      return "dummy-workflow-id";
    }

    @Override
    public String getRunId() {
      return "dummy-run-id";
    }

    @Override
    public Duration getWorkflowRunTimeout() {
      throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public Duration getWorkflowExecutionTimeout() {
      return Duration.ZERO;
    }

    @Override
    public long getRunStartedTimestampMillis() {
      return 0;
    }

    @Override
    public long getWorkflowExecutionExpirationTimestampMillis() {
      return 0;
    }

    @Override
    public Duration getWorkflowTaskTimeout() {
      throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public SearchAttributes getSearchAttributes() {
      throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public Map<String, Object> getPropagatedContexts() {
      return null;
    }

    @Override
    public List<ContextPropagator> getContextPropagators() {
      return null;
    }

    @Override
    public Functions.Proc1<Exception> scheduleActivityTask(
        ExecuteActivityParameters parameters,
        Functions.Proc2<Optional<Payloads>, Failure> callback) {
      throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public Functions.Proc scheduleLocalActivityTask(
        ExecuteLocalActivityParameters parameters,
        Functions.Proc2<Optional<Payloads>, Failure> callback) {
      throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public Functions.Proc1<Exception> startChildWorkflow(
        StartChildWorkflowExecutionParameters parameters,
        Functions.Proc1<WorkflowExecution> executionCallback,
        Functions.Proc2<Optional<Payloads>, Exception> callback) {
      throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public Functions.Proc1<Exception> signalExternalWorkflowExecution(
        SignalExternalWorkflowExecutionCommandAttributes.Builder attributes,
        Functions.Proc2<Void, Failure> callback) {
      throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public void requestCancelExternalWorkflowExecution(
        WorkflowExecution execution, Functions.Proc2<Void, RuntimeException> callback) {
      throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public void continueAsNewOnCompletion(
        ContinueAsNewWorkflowExecutionCommandAttributes attributes) {
      throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public long currentTimeMillis() {
      return System.currentTimeMillis();
    }

    @Override
    public Functions.Proc1<RuntimeException> newTimer(
        Duration delay, Functions.Proc1<RuntimeException> callback) {
      timer.schedule(
          new TimerTask() {
            @Override
            public void run() {
              callback.apply(null);
            }
          },
          delay.toMillis());
      return (e) -> {
        callback.apply(new CanceledFailure(null));
      };
    }

    @Override
    public void sideEffect(
        Func<Optional<Payloads>> func, Functions.Proc1<Optional<Payloads>> callback) {
      callback.apply(func.apply());
    }

    @Override
    public void mutableSideEffect(
        String id,
        Func1<Optional<Payloads>, Optional<Payloads>> func,
        Functions.Proc1<Optional<Payloads>> callback) {
      callback.apply(func.apply(Optional.empty()));
    }

    @Override
    public boolean isReplaying() {
      return false;
    }

    @Override
    public void getVersion(
        String changeId, int minSupported, int maxSupported, Functions.Proc1<Integer> callback) {
      throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public Random newRandom() {
      throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public Scope getMetricsScope() {
      return new NoopScope();
    }

    @Override
    public boolean getEnableLoggingInReplay() {
      return false;
    }

    @Override
    public UUID randomUUID() {
      return UUID.randomUUID();
    }

    @Override
    public void upsertSearchAttributes(SearchAttributes searchAttributes) {
      throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public int getAttempt() {
      return 1;
    }
  }

  private static class WorkflowThreadMarkerAccessor extends WorkflowThreadMarker {
    public static void markAsWorkflowThread() {
      isWorkflowThreadThreadLocal.set(true);
    }

    public static void markAsNonWorkflowThread() {
      isWorkflowThreadThreadLocal.set(false);
    }
  }
}
