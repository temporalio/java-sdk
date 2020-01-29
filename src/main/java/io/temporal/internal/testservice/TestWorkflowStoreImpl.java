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

package io.temporal.internal.testservice;

import io.temporal.BadRequestError;
import io.temporal.EntityNotExistsError;
import io.temporal.EventType;
import io.temporal.GetWorkflowExecutionHistoryRequest;
import io.temporal.GetWorkflowExecutionHistoryResponse;
import io.temporal.History;
import io.temporal.HistoryEvent;
import io.temporal.HistoryEventFilterType;
import io.temporal.InternalServiceError;
import io.temporal.PollForActivityTaskRequest;
import io.temporal.PollForActivityTaskResponse;
import io.temporal.PollForDecisionTaskRequest;
import io.temporal.PollForDecisionTaskResponse;
import io.temporal.StickyExecutionAttributes;
import io.temporal.WorkflowExecution;
import io.temporal.WorkflowExecutionInfo;
import io.temporal.internal.common.WorkflowExecutionUtils;
import io.temporal.internal.testservice.RequestContext.Timer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

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

    void addAllLocked(List<HistoryEvent> events, long timeInNanos) throws EntityNotExistsError {
      for (HistoryEvent event : events) {
        if (completed) {
          throw new EntityNotExistsError(
              "Attempt to add an event after a completion event: "
                  + WorkflowExecutionUtils.prettyPrintHistoryEvent(event));
        }
        event.setEventId(history.size() + 1L);
        // It can be set in StateMachines.startActivityTask
        if (!event.isSetTimestamp()) {
          event.setTimestamp(timeInNanos);
        }
        history.add(event);
        completed = completed || WorkflowExecutionUtils.isWorkflowExecutionCompletedEvent(event);
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
        long expectedNextEventId, HistoryEventFilterType filterType) {
      lock.lock();
      try {
        while (true) {
          if (completed || getNextEventIdLocked() > expectedNextEventId) {
            if (filterType == HistoryEventFilterType.CLOSE_EVENT) {
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
            newEventsCondition.await();
          } catch (InterruptedException e) {
            return null;
          }
        }
      } finally {
        lock.unlock();
      }
    }
  }

  private final Lock lock = new ReentrantLock();

  private final Map<ExecutionId, HistoryStore> histories = new HashMap<>();

  private final Map<TaskListId, BlockingQueue<PollForActivityTaskResponse>> activityTaskLists =
      new HashMap<>();

  private final Map<TaskListId, BlockingQueue<PollForDecisionTaskResponse>> decisionTaskLists =
      new HashMap<>();

  private final SelfAdvancingTimer timerService =
      new SelfAdvancingTimerImpl(System.currentTimeMillis());

  public TestWorkflowStoreImpl() {
    // locked until the first save
    timerService.lockTimeSkipping("TestWorkflowStoreImpl constructor");
  }

  @Override
  public SelfAdvancingTimer getTimer() {
    return timerService;
  }

  @Override
  public long currentTimeMillis() {
    return timerService.getClock().getAsLong();
  }

  @Override
  public long save(RequestContext ctx)
      throws InternalServiceError, EntityNotExistsError, BadRequestError {
    long result;
    lock.lock();
    boolean historiesEmpty = histories.isEmpty();
    try {
      ExecutionId executionId = ctx.getExecutionId();
      HistoryStore history = histories.get(executionId);
      List<HistoryEvent> events = ctx.getEvents();
      if (history == null) {
        if (events.isEmpty()
            || events.get(0).getEventType() != EventType.WorkflowExecutionStarted) {
          throw new IllegalStateException("No history found for " + executionId);
        }
        history = new HistoryStore(executionId, lock);
        histories.put(executionId, history);
      }
      history.checkNextEventId(ctx.getInitialEventId());
      history.addAllLocked(events, ctx.currentTimeInNanoseconds());
      result = history.getNextEventIdLocked();
      timerService.updateLocks(ctx.getTimerLocks(), "TestWorkflowStoreImpl save");
      ctx.fireCallbacks(history.getEventsLocked().size());
    } finally {
      if (historiesEmpty && !histories.isEmpty()) {
        timerService.unlockTimeSkipping(
            "TestWorkflowStoreImpl save"); // Initially locked in the constructor
      }
      lock.unlock();
    }
    // Push tasks to the queues out of locks
    DecisionTask decisionTask = ctx.getDecisionTask();

    if (decisionTask != null) {
      StickyExecutionAttributes attributes =
          ctx.getWorkflowMutableState().getStickyExecutionAttributes();
      TaskListId id =
          new TaskListId(
              decisionTask.getTaskListId().getDomain(),
              attributes == null
                  ? decisionTask.getTaskListId().getTaskListName()
                  : attributes.getWorkerTaskList().getName());

      BlockingQueue<PollForDecisionTaskResponse> decisionsQueue = getDecisionTaskListQueue(id);
      decisionsQueue.add(decisionTask.getTask());
    }

    List<ActivityTask> activityTasks = ctx.getActivityTasks();
    if (activityTasks != null) {
      for (ActivityTask activityTask : activityTasks) {
        BlockingQueue<PollForActivityTaskResponse> activitiesQueue =
            getActivityTaskListQueue(activityTask.getTaskListId());
        activitiesQueue.add(activityTask.getTask());
      }
    }

    List<Timer> timers = ctx.getTimers();
    if (timers != null) {
      for (Timer t : timers) {
        timerService.schedule(
            Duration.ofSeconds(t.getDelaySeconds()), t.getCallback(), t.getTaskInfo());
      }
    }
    return result;
  }

  @Override
  public void applyTimersAndLocks(RequestContext ctx) {
    lock.lock();
    try {
      timerService.updateLocks(ctx.getTimerLocks(), "TestWorkflowStoreImpl applyTimersAndLocks");
    } finally {
      lock.unlock();
    }

    List<Timer> timers = ctx.getTimers();
    if (timers != null) {
      for (Timer t : timers) {
        timerService.schedule(
            Duration.ofSeconds(t.getDelaySeconds()), t.getCallback(), t.getTaskInfo());
      }
    }

    ctx.clearTimersAndLocks();
  }

  @Override
  public void registerDelayedCallback(Duration delay, Runnable r) {
    timerService.schedule(delay, r, "registerDelayedCallback");
  }

  private BlockingQueue<PollForActivityTaskResponse> getActivityTaskListQueue(
      TaskListId taskListId) {
    lock.lock();
    try {
      {
        BlockingQueue<PollForActivityTaskResponse> activitiesQueue =
            activityTaskLists.get(taskListId);
        if (activitiesQueue == null) {
          activitiesQueue = new LinkedBlockingQueue<>();
          activityTaskLists.put(taskListId, activitiesQueue);
        }
        return activitiesQueue;
      }
    } finally {
      lock.unlock();
    }
  }

  private BlockingQueue<PollForDecisionTaskResponse> getDecisionTaskListQueue(
      TaskListId taskListId) {
    lock.lock();
    try {
      BlockingQueue<PollForDecisionTaskResponse> decisionsQueue = decisionTaskLists.get(taskListId);
      if (decisionsQueue == null) {
        decisionsQueue = new LinkedBlockingQueue<>();
        decisionTaskLists.put(taskListId, decisionsQueue);
      }
      return decisionsQueue;
    } finally {
      lock.unlock();
    }
  }

  @Override
  public PollForDecisionTaskResponse pollForDecisionTask(PollForDecisionTaskRequest pollRequest)
      throws InterruptedException {
    TaskListId taskListId =
        new TaskListId(pollRequest.getDomain(), pollRequest.getTaskList().getName());
    BlockingQueue<PollForDecisionTaskResponse> decisionsQueue =
        getDecisionTaskListQueue(taskListId);
    return decisionsQueue.take();
  }

  @Override
  public PollForActivityTaskResponse pollForActivityTask(PollForActivityTaskRequest pollRequest)
      throws InterruptedException {
    TaskListId taskListId =
        new TaskListId(pollRequest.getDomain(), pollRequest.getTaskList().getName());
    BlockingQueue<PollForActivityTaskResponse> activityTaskQueue =
        getActivityTaskListQueue(taskListId);
    return activityTaskQueue.take();
  }

  @Override
  public void sendQueryTask(
      ExecutionId executionId, TaskListId taskList, PollForDecisionTaskResponse task)
      throws EntityNotExistsError {
    lock.lock();
    try {
      HistoryStore historyStore = getHistoryStore(executionId);
      List<HistoryEvent> events = new ArrayList<>(historyStore.getEventsLocked());
      History history = new History();
      if (taskList.getTaskListName().equals(task.getWorkflowExecutionTaskList().getName())) {
        history.setEvents(events);
      } else {
        history.setEvents(new ArrayList<>());
      }
      task.setHistory(history);
    } finally {
      lock.unlock();
    }
    BlockingQueue<PollForDecisionTaskResponse> decisionsQueue = getDecisionTaskListQueue(taskList);
    decisionsQueue.add(task);
  }

  @Override
  public GetWorkflowExecutionHistoryResponse getWorkflowExecutionHistory(
      ExecutionId executionId, GetWorkflowExecutionHistoryRequest getRequest)
      throws EntityNotExistsError {
    HistoryStore history;
    // Used to eliminate the race condition on waitForNewEvents
    long expectedNextEventId;
    lock.lock();
    try {
      history = getHistoryStore(executionId);
      if (!getRequest.isWaitForNewEvent()
          && getRequest.getHistoryEventFilterType() != HistoryEventFilterType.CLOSE_EVENT) {
        List<HistoryEvent> events = history.getEventsLocked();
        // Copy the list as it is mutable. Individual events assumed immutable.
        ArrayList<HistoryEvent> eventsCopy = new ArrayList<>(events);
        return new GetWorkflowExecutionHistoryResponse()
            .setHistory(new History().setEvents(eventsCopy));
      }
      expectedNextEventId = history.getNextEventIdLocked();
    } finally {
      lock.unlock();
    }
    List<HistoryEvent> events =
        history.waitForNewEvents(expectedNextEventId, getRequest.getHistoryEventFilterType());
    GetWorkflowExecutionHistoryResponse result = new GetWorkflowExecutionHistoryResponse();
    if (events != null) {
      result.setHistory(new History().setEvents(events));
    }
    return result;
  }

  private HistoryStore getHistoryStore(ExecutionId executionId) throws EntityNotExistsError {
    HistoryStore result = histories.get(executionId);
    if (result == null) {
      WorkflowExecution execution = executionId.getExecution();
      throw new EntityNotExistsError(
          String.format(
              "Workflow execution result not found.  " + "WorkflowId: %s, RunId: %s",
              execution.getWorkflowId(), execution.getRunId()));
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
    timerService.getDiagnostics(result);
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
            new WorkflowExecutionInfo()
                .setExecution(executionId.getExecution())
                .setHistoryLength(history.size())
                .setStartTime(history.get(0).getTimestamp())
                .setType(
                    history.get(0).getWorkflowExecutionStartedEventAttributes().getWorkflowType());
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
            new WorkflowExecutionInfo()
                .setExecution(executionId.getExecution())
                .setHistoryLength(history.size())
                .setStartTime(history.get(0).getTimestamp())
                .setType(
                    history.get(0).getWorkflowExecutionStartedEventAttributes().getWorkflowType())
                .setCloseStatus(
                    WorkflowExecutionUtils.getCloseStatus(history.get(history.size() - 1)));
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
