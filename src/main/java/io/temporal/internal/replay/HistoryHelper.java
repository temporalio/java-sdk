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

package io.temporal.internal.replay;

import com.google.common.collect.PeekingIterator;
import io.temporal.api.enums.v1.EventType;
import io.temporal.api.history.v1.HistoryEvent;
import io.temporal.api.workflowservice.v1.PollWorkflowTaskQueueResponseOrBuilder;
import io.temporal.internal.common.WorkflowExecutionUtils;
import io.temporal.internal.worker.WorkflowTaskWithHistoryIterator;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

class HistoryHelper {

  /**
   * Events of a single decision. It includes all new events in the history since the last decision
   * as events. It doesn't include events that are decision events of the previous decision. The
   * decision events are events that this decision produced when executed for the first time.
   */
  static final class DecisionEvents {

    private final List<HistoryEvent> events;
    private final List<HistoryEvent> decisionEvents;
    private final List<HistoryEvent> markers = new ArrayList<>();
    private final boolean replay;
    private final long replayCurrentTimeMilliseconds;
    private final long nextDecisionEventId;

    DecisionEvents(
        List<HistoryEvent> events,
        List<HistoryEvent> decisionEvents,
        boolean replay,
        long replayCurrentTimeMilliseconds,
        long nextDecisionEventId) {
      if (nextDecisionEventId <= 0) {
        throw new Error("nextDecisionEventId is not set");
      }
      this.events = events;
      this.decisionEvents = decisionEvents;
      for (HistoryEvent event : decisionEvents) {
        if (event.getEventType() == EventType.EVENT_TYPE_MARKER_RECORDED) {
          markers.add(event);
        }
      }
      this.replay = replay;
      this.replayCurrentTimeMilliseconds = replayCurrentTimeMilliseconds;
      this.nextDecisionEventId = nextDecisionEventId;
    }

    public List<HistoryEvent> getEvents() {
      return events;
    }

    List<HistoryEvent> getDecisionEvents() {
      return decisionEvents;
    }

    Optional<HistoryEvent> getOptionalDecisionEvent(long eventId) {
      int index = (int) (eventId - nextDecisionEventId);
      if (index < 0 || index >= decisionEvents.size()) {
        return Optional.empty();
      }
      return Optional.of(decisionEvents.get(index));
    }

    public List<HistoryEvent> getMarkers() {
      return markers;
    }

    public boolean isReplay() {
      return replay;
    }

    public long getReplayCurrentTimeMilliseconds() {
      return replayCurrentTimeMilliseconds;
    }

    public long getNextDecisionEventId() {
      return nextDecisionEventId;
    }

    @Override
    public String toString() {
      return "DecisionEvents{"
          + "events="
          + WorkflowExecutionUtils.prettyPrintHistory(events.iterator(), true)
          + ", decisionEvents="
          + WorkflowExecutionUtils.prettyPrintHistory(decisionEvents.iterator(), true)
          + ", replay="
          + replay
          + ", replayCurrentTimeMilliseconds="
          + replayCurrentTimeMilliseconds
          + ", nextDecisionEventId="
          + nextDecisionEventId
          + '}';
    }
  }

  /** Allows peeking for the next event. */
  private static final class EventsIterator implements PeekingIterator<HistoryEvent> {

    private Iterator<HistoryEvent> events;
    private HistoryEvent next;

    EventsIterator(Iterator<HistoryEvent> events) {
      this.events = events;
      if (events.hasNext()) {
        next = events.next();
      }
    }

    @Override
    public HistoryEvent peek() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      return next;
    }

    @Override
    public boolean hasNext() {
      return next != null;
    }

    @Override
    public HistoryEvent next() {
      HistoryEvent result = next;
      if (events.hasNext()) {
        next = events.next();
      } else {
        next = null;
      }
      return result;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException("not implemented");
    }
  }

  /**
   * Iterates through decisions and returns one DecisionEvents instance per WorkflowTaskStarted
   * event.
   */
  private static class DecisionEventsIterator implements Iterator<DecisionEvents> {

    private EventsIterator events;
    private long replayCurrentTimeMilliseconds;

    DecisionEventsIterator(
        WorkflowTaskWithHistoryIterator workflowTaskWithHistoryIterator,
        long replayCurrentTimeMilliseconds) {
      this.events = new EventsIterator(workflowTaskWithHistoryIterator.getHistory());
      this.replayCurrentTimeMilliseconds = replayCurrentTimeMilliseconds;
    }

    @Override
    public boolean hasNext() {
      return events.hasNext();
    }

    @Override
    public DecisionEvents next() {
      List<HistoryEvent> decisionEvents = new ArrayList<>();
      List<HistoryEvent> newEvents = new ArrayList<>();
      boolean replay = true;
      long nextDecisionEventId = -1;
      while (events.hasNext()) {
        HistoryEvent event = events.next();
        EventType eventType = event.getEventType();

        // Sticky workers receive an event history that starts with WorkflowTaskCompleted
        if (eventType == EventType.EVENT_TYPE_WORKFLOW_TASK_COMPLETED
            && nextDecisionEventId == -1) {
          nextDecisionEventId = event.getEventId() + 1;
          break;
        }

        if (eventType == EventType.EVENT_TYPE_WORKFLOW_TASK_STARTED || !events.hasNext()) {
          replayCurrentTimeMilliseconds = TimeUnit.NANOSECONDS.toMillis(event.getTimestamp());
          if (!events.hasNext()) {
            replay = false;
            nextDecisionEventId = event.getEventId() + 2; // +1 for next, +1 for DecisionCompleted
            break;
          }
          HistoryEvent peeked = events.peek();
          EventType peekedType = peeked.getEventType();
          if (peekedType == EventType.EVENT_TYPE_WORKFLOW_TASK_TIMED_OUT
              || peekedType == EventType.EVENT_TYPE_WORKFLOW_TASK_FAILED) {
            continue;
          } else if (peekedType == EventType.EVENT_TYPE_WORKFLOW_TASK_COMPLETED) {
            events.next(); // consume WorkflowTaskCompleted
            nextDecisionEventId = peeked.getEventId() + 1; // +1 for next and skip over completed
            break;
          } else {
            throw new Error(
                "Unexpected event after WorkflowTaskStarted: "
                    + peeked
                    + " WorkflowTaskStarted Event: "
                    + event);
          }
        }
        newEvents.add(event);
      }
      while (events.hasNext()) {
        if (!WorkflowExecutionUtils.isDecisionEvent(events.peek())) {
          break;
        }
        decisionEvents.add(events.next());
      }
      DecisionEvents result =
          new DecisionEvents(
              newEvents,
              decisionEvents,
              replay,
              replayCurrentTimeMilliseconds,
              nextDecisionEventId);
      return result;
    }
  }

  private final WorkflowTaskWithHistoryIterator workflowTaskWithHistoryIterator;
  private final Iterator<DecisionEvents> iterator;

  HistoryHelper(WorkflowTaskWithHistoryIterator workflowTasks, long replayCurrentTimeMilliseconds) {
    this.workflowTaskWithHistoryIterator = workflowTasks;
    this.iterator = new DecisionEventsIterator(workflowTasks, replayCurrentTimeMilliseconds);
  }

  public Iterator<DecisionEvents> getIterator() {
    return iterator;
  }

  public PollWorkflowTaskQueueResponseOrBuilder getWorkflowTask() {
    return workflowTaskWithHistoryIterator.getWorkflowTask();
  }

  @Override
  public String toString() {
    return WorkflowExecutionUtils.prettyPrintHistory(
        workflowTaskWithHistoryIterator.getWorkflowTask().getHistory().getEventsList().iterator(),
        true);
  }

  long getPreviousStartedEventId() {
    return getWorkflowTask().getPreviousStartedEventId();
  }
}
