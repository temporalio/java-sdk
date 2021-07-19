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

package io.temporal.internal.testservice;

import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import io.grpc.Deadline;
import io.grpc.Status;
import io.temporal.api.common.v1.WorkflowExecution;
import io.temporal.api.enums.v1.EventType;
import io.temporal.api.enums.v1.HistoryEventFilterType;
import io.temporal.api.history.v1.History;
import io.temporal.api.history.v1.HistoryEvent;
import io.temporal.api.taskqueue.v1.StickyExecutionAttributes;
import io.temporal.api.workflow.v1.WorkflowExecutionInfo;
import io.temporal.api.workflowservice.v1.GetWorkflowExecutionHistoryRequest;
import io.temporal.api.workflowservice.v1.GetWorkflowExecutionHistoryResponse;
import io.temporal.api.workflowservice.v1.PollActivityTaskQueueRequest;
import io.temporal.api.workflowservice.v1.PollActivityTaskQueueResponse;
import io.temporal.api.workflowservice.v1.PollWorkflowTaskQueueRequest;
import io.temporal.api.workflowservice.v1.PollWorkflowTaskQueueResponse;
import io.temporal.failure.ApplicationFailure;
import io.temporal.internal.common.WorkflowExecutionUtils;
import io.temporal.internal.testservice.RequestContext.Timer;
import io.temporal.workflow.Functions;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class TestWorkflowStoreImpl implements TestWorkflowStore {

  private static class HistoryStore {

    private final Lock lock;
    private final Condition newEventsCondition;
    private final ExecutionId id;
    private final List<HistoryEvent> history = new ArrayList<>();
    private boolean completed;

    private HistoryStore(ExecutionId id, Lock lock) {
      this.id = id;
      this.lock = lock;
      this.newEventsCondition = lock.newCondition();
    }

    public boolean isCompleted() {
      return completed;
    }

    public List<HistoryEvent> getHistory() {
      return history;
    }

    private void checkNextEventId(long nextEventId) {
      if (nextEventId != history.size() + 1L && (nextEventId != 0 && history.size() != 0)) {
        throw new IllegalStateException(
            "NextEventId=" + nextEventId + ", historySize=" + history.size() + " for " + id);
      }
    }

    void addAllLocked(List<HistoryEvent> events, Timestamp eventTime) {
      for (HistoryEvent event : events) {
        HistoryEvent.Builder eBuilder = event.toBuilder();
        if (completed) {
          throw ApplicationFailure.newNonRetryableFailure("Workflow execution completed.", "test");
        }
        eBuilder.setEventId(history.size() + 1L);
        // It can be set in StateMachines.startActivityTask
        if (Timestamps.toMillis(eBuilder.getEventTime()) == 0) {
          eBuilder.setEventTime(eventTime);
        }
        history.add(eBuilder.build());
        completed = completed || WorkflowExecutionUtils.isWorkflowExecutionCompletedEvent(eBuilder);
      }
      newEventsCondition.signal();
    }

    long getNextEventIdLocked() {
      return history.size() + 1L;
    }

    List<HistoryEvent> getEventsLocked() {
      return history;
    }

    List<HistoryEvent> waitForNewEvents(
        long expectedNextEventId, HistoryEventFilterType filterType, Deadline deadline) {
      long start = System.currentTimeMillis();
      lock.lock();
      try {
        while (true) {
          if (completed || getNextEventIdLocked() > expectedNextEventId) {
            if (filterType == HistoryEventFilterType.HISTORY_EVENT_FILTER_TYPE_CLOSE_EVENT) {
              if (completed) {
                List<HistoryEvent> result = new ArrayList<>(1);
                result.add(history.get(history.size() - 1));
                return result;
              }
              expectedNextEventId = getNextEventIdLocked();
              continue;
            }
            List<HistoryEvent> result =
                new ArrayList<>(((int) (getNextEventIdLocked() - expectedNextEventId)));
            for (int i = (int) expectedNextEventId; i < getNextEventIdLocked(); i++) {
              result.add(history.get(i));
            }
            return result;
          }
          try {
            long toWait;
            if (deadline != null) {
              toWait =
                  deadline.timeRemaining(TimeUnit.MILLISECONDS)
                      - System.currentTimeMillis()
                      + start;
              if (toWait <= 0) {
                return null;
              }
              newEventsCondition.await(toWait, TimeUnit.MILLISECONDS);
            } else {
              newEventsCondition.await();
            }
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
          }
        }
      } finally {
        lock.unlock();
      }
    }
  }

  private static final Logger log = LoggerFactory.getLogger(TestWorkflowStoreImpl.class);

  private final Lock lock = new ReentrantLock();

  private final Map<ExecutionId, HistoryStore> histories = new HashMap<>();

  private final Map<TaskQueueId, BlockingQueue<PollActivityTaskQueueResponse.Builder>>
      activityTaskQueues = new HashMap<>();

  private final Map<TaskQueueId, BlockingQueue<PollWorkflowTaskQueueResponse.Builder>>
      workflowTaskQueues = new HashMap<>();

  private final SelfAdvancingTimer timerService;
  private LockHandle emptyHistoryLockHandle;

  public TestWorkflowStoreImpl(long initialTimeMillis) {
    timerService = new SelfAdvancingTimerImpl(initialTimeMillis);
    // locked until the first save
    emptyHistoryLockHandle = timerService.lockTimeSkipping("TestWorkflowStoreImpl constructor");
  }

  @Override
  public SelfAdvancingTimer getTimer() {
    return timerService;
  }

  @Override
  public Timestamp currentTime() {
    return Timestamps.fromMillis(timerService.getClock().getAsLong());
  }

  @Override
  public long save(RequestContext ctx) {
    long result;
    lock.lock();
    try {
      ExecutionId executionId = ctx.getExecutionId();
      HistoryStore history = histories.get(executionId);
      List<HistoryEvent> events = ctx.getEvents();
      if (history == null) {
        if (events.isEmpty()
            || events.get(0).getEventType() != EventType.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED) {
          throw new IllegalStateException("No history found for " + executionId);
        }
        history = new HistoryStore(executionId, lock);
        histories.put(executionId, history);
      }
      history.checkNextEventId(ctx.getInitialEventId());
      history.addAllLocked(events, ctx.currentTime());
      result = history.getNextEventIdLocked();
      timerService.updateLocks(ctx.getTimerLocks());
      ctx.fireCallbacks(history.getEventsLocked().size());
    } finally {
      if (emptyHistoryLockHandle != null && !histories.isEmpty()) {
        // Initially locked in the constructor
        emptyHistoryLockHandle.unlock("TestWorkflowStoreImpl first save");
        emptyHistoryLockHandle = null;
      }
      lock.unlock();
    }
    // Push tasks to the queues out of locks
    WorkflowTask workflowTask = ctx.getWorkflowTask();

    if (workflowTask != null) {
      StickyExecutionAttributes attributes =
          ctx.getWorkflowMutableState().getStickyExecutionAttributes();
      TaskQueueId id =
          new TaskQueueId(
              workflowTask.getTaskQueueId().getNamespace(),
              attributes == null
                  ? workflowTask.getTaskQueueId().getTaskQueueName()
                  : attributes.getWorkerTaskQueue().getName());
      if (id.getTaskQueueName().isEmpty() || id.getNamespace().isEmpty()) {
        throw Status.INTERNAL.withDescription("Invalid TaskQueueId: " + id).asRuntimeException();
      }
      BlockingQueue<PollWorkflowTaskQueueResponse.Builder> workflowTaskQueue =
          getWorkflowTaskQueueQueue(id);
      workflowTaskQueue.add(workflowTask.getTask());
    }

    List<ActivityTask> activityTasks = ctx.getActivityTasks();
    if (activityTasks != null) {
      for (ActivityTask activityTask : activityTasks) {
        BlockingQueue<PollActivityTaskQueueResponse.Builder> activityTaskQueue =
            getActivityTaskQueueQueue(activityTask.getTaskQueueId());
        activityTaskQueue.add(activityTask.getTask());
      }
    }

    List<Timer> timers = ctx.getTimers();
    if (timers != null) {
      for (Timer t : timers) {
        log.trace(
            "scheduling timer with " + t.getDelay() + "delay. Current time=" + this.currentTime());
        Functions.Proc cancellationHandle =
            timerService.schedule(t.getDelay(), t.getCallback(), t.getTaskInfo());
        t.setCancellationHandle(cancellationHandle);
      }
    }
    return result;
  }

  @Override
  public void applyTimersAndLocks(RequestContext ctx) {
    lock.lock();
    try {
      timerService.updateLocks(ctx.getTimerLocks());
    } finally {
      lock.unlock();
    }

    List<Timer> timers = ctx.getTimers();
    if (timers != null) {
      for (Timer t : timers) {
        Functions.Proc cancellationHandle =
            timerService.schedule(t.getDelay(), t.getCallback(), t.getTaskInfo());
        t.setCancellationHandle(cancellationHandle);
      }
    }

    ctx.clearTimersAndLocks();
  }

  @Override
  public void registerDelayedCallback(Duration delay, Runnable r) {
    timerService.schedule(delay, r, "registerDelayedCallback");
  }

  private BlockingQueue<PollActivityTaskQueueResponse.Builder> getActivityTaskQueueQueue(
      TaskQueueId taskQueueId) {
    lock.lock();
    try {
      {
        BlockingQueue<PollActivityTaskQueueResponse.Builder> activityTaskQueue =
            activityTaskQueues.get(taskQueueId);
        if (activityTaskQueue == null) {
          activityTaskQueue = new LinkedBlockingQueue<>();
          activityTaskQueues.put(taskQueueId, activityTaskQueue);
        }
        return activityTaskQueue;
      }
    } finally {
      lock.unlock();
    }
  }

  private BlockingQueue<PollWorkflowTaskQueueResponse.Builder> getWorkflowTaskQueueQueue(
      TaskQueueId taskQueueId) {
    lock.lock();
    try {
      BlockingQueue<PollWorkflowTaskQueueResponse.Builder> workflowTaskQueue =
          workflowTaskQueues.get(taskQueueId);
      if (workflowTaskQueue == null) {
        workflowTaskQueue = new LinkedBlockingQueue<>();
        workflowTaskQueues.put(taskQueueId, workflowTaskQueue);
      }
      return workflowTaskQueue;
    } finally {
      lock.unlock();
    }
  }

  @Override
  public Optional<PollWorkflowTaskQueueResponse.Builder> pollWorkflowTaskQueue(
      PollWorkflowTaskQueueRequest pollRequest, Deadline deadline) {
    TaskQueueId taskQueueId =
        new TaskQueueId(pollRequest.getNamespace(), pollRequest.getTaskQueue().getName());
    BlockingQueue<PollWorkflowTaskQueueResponse.Builder> workflowTaskQueue =
        getWorkflowTaskQueueQueue(taskQueueId);
    if (log.isTraceEnabled()) {
      log.trace(
          "Poll request on workflow task queue about to block waiting for a task on "
              + taskQueueId);
    }
    PollWorkflowTaskQueueResponse.Builder result = null;
    try {
      if (deadline == null) {
        result = workflowTaskQueue.take();
      } else {
        result =
            workflowTaskQueue.poll(
                deadline.timeRemaining(TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    return Optional.ofNullable(result);
  }

  @Override
  public Optional<PollActivityTaskQueueResponse.Builder> pollActivityTaskQueue(
      PollActivityTaskQueueRequest pollRequest, Deadline deadline) {
    TaskQueueId taskQueueId =
        new TaskQueueId(pollRequest.getNamespace(), pollRequest.getTaskQueue().getName());
    BlockingQueue<PollActivityTaskQueueResponse.Builder> activityTaskQueue =
        getActivityTaskQueueQueue(taskQueueId);
    PollActivityTaskQueueResponse.Builder result = null;
    try {
      if (deadline == null) {
        result = activityTaskQueue.take();
      } else {
        result =
            activityTaskQueue.poll(
                deadline.timeRemaining(TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    return Optional.ofNullable(result);
  }

  @Override
  public void sendQueryTask(
      ExecutionId executionId, TaskQueueId taskQueue, PollWorkflowTaskQueueResponse.Builder task) {
    lock.lock();
    try {
      HistoryStore historyStore = getHistoryStore(executionId);
      List<HistoryEvent> events = new ArrayList<>(historyStore.getEventsLocked());
      History.Builder history = History.newBuilder();
      PeekingIterator<HistoryEvent> iterator = Iterators.peekingIterator(events.iterator());
      long startedEventId = 0;
      long previousStaredEventId = 0;
      while (iterator.hasNext()) {
        HistoryEvent event = iterator.next();
        if (event.getEventType() == EventType.EVENT_TYPE_WORKFLOW_TASK_STARTED) {
          if (!iterator.hasNext()
              || iterator.peek().getEventType() == EventType.EVENT_TYPE_WORKFLOW_TASK_COMPLETED) {
            previousStaredEventId = startedEventId;
            startedEventId = event.getEventId();
          }
        } else if (WorkflowExecutionUtils.isWorkflowExecutionCompletedEvent(event)) {
          previousStaredEventId = startedEventId;
          startedEventId = 0;
          if (iterator.hasNext()) {
            throw Status.INTERNAL
                .withDescription("Unexpected event after the completion event: " + iterator.peek())
                .asRuntimeException();
          }
        }
      }
      task.setPreviousStartedEventId(previousStaredEventId);
      task.setStartedEventId(startedEventId);
      if (taskQueue.getTaskQueueName().equals(task.getWorkflowExecutionTaskQueue().getName())) {
        history.addAllEvents(events);
      } else {
        history.addAllEvents(new ArrayList<>());
      }
      task.setHistory(history);
    } finally {
      lock.unlock();
    }
    BlockingQueue<PollWorkflowTaskQueueResponse.Builder> workflowTaskQueue =
        getWorkflowTaskQueueQueue(taskQueue);
    workflowTaskQueue.add(task);
  }

  @Override
  public GetWorkflowExecutionHistoryResponse getWorkflowExecutionHistory(
      ExecutionId executionId, GetWorkflowExecutionHistoryRequest getRequest, Deadline deadline) {
    HistoryStore history;
    // Used to eliminate the race condition on waitForNewEvents
    long expectedNextEventId;
    lock.lock();
    try {
      history = getHistoryStore(executionId);
      if (!getRequest.getWaitNewEvent()
          && getRequest.getHistoryEventFilterType()
              != HistoryEventFilterType.HISTORY_EVENT_FILTER_TYPE_CLOSE_EVENT) {
        List<HistoryEvent> events = history.getEventsLocked();
        // Copy the list as it is mutable. Individual events assumed immutable.
        ArrayList<HistoryEvent> eventsCopy = new ArrayList<>(events);
        return GetWorkflowExecutionHistoryResponse.newBuilder()
            .setHistory(History.newBuilder().addAllEvents(eventsCopy))
            .build();
      }
      expectedNextEventId = history.getNextEventIdLocked();
    } finally {
      lock.unlock();
    }
    List<HistoryEvent> events =
        history.waitForNewEvents(
            expectedNextEventId, getRequest.getHistoryEventFilterType(), deadline);
    GetWorkflowExecutionHistoryResponse.Builder result =
        GetWorkflowExecutionHistoryResponse.newBuilder();
    if (events != null) {
      result.setHistory(History.newBuilder().addAllEvents(events));
    }
    return result.build();
  }

  private HistoryStore getHistoryStore(ExecutionId executionId) {
    HistoryStore result = histories.get(executionId);
    if (result == null) {
      WorkflowExecution execution = executionId.getExecution();
      throw Status.NOT_FOUND
          .withDescription(
              String.format(
                  "Workflow execution result not found.  " + "WorkflowId: %s, RunId: %s",
                  execution.getWorkflowId(), execution.getRunId()))
          .asRuntimeException();
    }
    return result;
  }

  @Override
  public void getDiagnostics(StringBuilder result) {
    result.append("Stored Workflows:\n");
    lock.lock();
    try {
      {
        for (Entry<ExecutionId, HistoryStore> entry : this.histories.entrySet()) {
          result.append(entry.getKey());
          result.append("\n");
          result.append(
              WorkflowExecutionUtils.prettyPrintHistory(
                  entry.getValue().getEventsLocked().iterator(), true));
          result.append("\n");
        }
      }
    } finally {
      lock.unlock();
    }
    // Uncomment to troubleshoot time skipping issues.
    //    timerService.getDiagnostics(result);
  }

  @Override
  public List<WorkflowExecutionInfo> listWorkflows(
      WorkflowState state, Optional<String> filterWorkflowId) {
    List<WorkflowExecutionInfo> result = new ArrayList<>();
    for (Entry<ExecutionId, HistoryStore> entry : this.histories.entrySet()) {
      if (state == WorkflowState.OPEN) {
        if (entry.getValue().isCompleted()) {
          continue;
        }
        ExecutionId executionId = entry.getKey();
        String workflowId = executionId.getWorkflowId().getWorkflowId();
        if (filterWorkflowId.isPresent() && !workflowId.equals(filterWorkflowId.get())) {
          continue;
        }
        List<HistoryEvent> history = entry.getValue().getHistory();
        WorkflowExecutionInfo info =
            WorkflowExecutionInfo.newBuilder()
                .setExecution(executionId.getExecution())
                .setHistoryLength(history.size())
                .setStartTime(history.get(0).getEventTime())
                .setType(
                    history.get(0).getWorkflowExecutionStartedEventAttributes().getWorkflowType())
                .build();
        result.add(info);
      } else {
        if (!entry.getValue().isCompleted()) {
          continue;
        }
        ExecutionId executionId = entry.getKey();
        String workflowId = executionId.getWorkflowId().getWorkflowId();
        if (filterWorkflowId.isPresent() && !workflowId.equals(filterWorkflowId.get())) {
          continue;
        }
        List<HistoryEvent> history = entry.getValue().getHistory();
        WorkflowExecutionInfo info =
            WorkflowExecutionInfo.newBuilder()
                .setExecution(executionId.getExecution())
                .setHistoryLength(history.size())
                .setStartTime(history.get(0).getEventTime())
                .setType(
                    history.get(0).getWorkflowExecutionStartedEventAttributes().getWorkflowType())
                .setStatus(WorkflowExecutionUtils.getCloseStatus(history.get(history.size() - 1)))
                .build();
        result.add(info);
      }
    }
    return result;
  }

  @Override
  public void close() {
    timerService.shutdown();
  }
}
