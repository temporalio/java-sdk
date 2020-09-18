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

package io.temporal.internal.statemachines;

import static io.temporal.internal.common.CheckedExceptionWrapper.unwrap;
import static io.temporal.internal.common.WorkflowExecutionUtils.getEventTypeForCommand;
import static io.temporal.internal.common.WorkflowExecutionUtils.isCommandEvent;
import static io.temporal.internal.statemachines.LocalActivityStateMachine.LOCAL_ACTIVITY_MARKER_NAME;
import static io.temporal.internal.statemachines.LocalActivityStateMachine.MARKER_ACTIVITY_ID_KEY;
import static io.temporal.internal.statemachines.VersionStateMachine.MARKER_CHANGE_ID_KEY;
import static io.temporal.internal.statemachines.VersionStateMachine.VERSION_MARKER_NAME;

import com.cronutils.utils.VisibleForTesting;
import com.google.common.base.Strings;
import io.temporal.api.command.v1.CancelWorkflowExecutionCommandAttributes;
import io.temporal.api.command.v1.Command;
import io.temporal.api.command.v1.ContinueAsNewWorkflowExecutionCommandAttributes;
import io.temporal.api.command.v1.RequestCancelExternalWorkflowExecutionCommandAttributes;
import io.temporal.api.command.v1.ScheduleActivityTaskCommandAttributes;
import io.temporal.api.command.v1.SignalExternalWorkflowExecutionCommandAttributes;
import io.temporal.api.command.v1.StartChildWorkflowExecutionCommandAttributes;
import io.temporal.api.command.v1.StartTimerCommandAttributes;
import io.temporal.api.common.v1.Payloads;
import io.temporal.api.common.v1.SearchAttributes;
import io.temporal.api.common.v1.WorkflowExecution;
import io.temporal.api.enums.v1.EventType;
import io.temporal.api.failure.v1.Failure;
import io.temporal.api.history.v1.ActivityTaskScheduledEventAttributes;
import io.temporal.api.history.v1.HistoryEvent;
import io.temporal.api.history.v1.MarkerRecordedEventAttributes;
import io.temporal.api.history.v1.StartChildWorkflowExecutionInitiatedEventAttributes;
import io.temporal.common.converter.DataConverter;
import io.temporal.common.converter.EncodedValues;
import io.temporal.failure.CanceledFailure;
import io.temporal.internal.replay.ExecuteActivityParameters;
import io.temporal.internal.replay.ExecuteLocalActivityParameters;
import io.temporal.internal.replay.InternalWorkflowTaskException;
import io.temporal.internal.replay.StartChildWorkflowExecutionParameters;
import io.temporal.internal.worker.ActivityTaskHandler;
import io.temporal.workflow.ChildWorkflowCancellationType;
import io.temporal.workflow.Functions;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.Random;
import java.util.UUID;

public final class WorkflowStateMachines {

  enum HandleEventStatus {
    OK,
    NON_MATCHING_EVENT,
  }

  private final DataConverter dataConverter = DataConverter.getDefaultInstance();

  /**
   * The eventId of the last event in the history which is expected to be startedEventId unless it
   * is replay from a JSON file.
   */
  private long workflowTaskStartedEventId;

  /** The eventId of the started event of the last successfully executed workflow task. */
  private long previousStartedEventId;

  private final EntityManagerListener callbacks;

  /** Callback to send new commands to. */
  private final Functions.Proc1<CancellableCommand> commandSink;

  /**
   * currentRunId is used as seed by Workflow.newRandom and randomUUID. It allows to generate them
   * deterministically.
   */
  private String currentRunId;

  /** Used Workflow.newRandom and randomUUID together with currentRunId. */
  private long idCounter;

  /** Current workflow time. */
  private long currentTimeMillis = -1;

  private long replayTimeUpdatedAtMillis;

  private final Map<Long, EntityStateMachine> stateMachines = new HashMap<>();

  private final Queue<CancellableCommand> commands = new ArrayDeque<>();

  /**
   * Commands generated by the currently processed workflow task. It is a queue as commands can be
   * added (due to marker based commands) while iterating over already added commands.
   */
  private final Queue<CancellableCommand> cancellableCommands = new ArrayDeque<>();

  /** EventId of the last handled WorkflowTaskStartedEvent. */
  private long currentStartedEventId;

  /** Is workflow executing new code or replaying from the history. */
  private boolean replaying;

  /** Used to ensure that event loop is not executed recursively. */
  private boolean eventLoopExecuting;

  /**
   * Used to avoid recursive calls to {@link #prepareCommands()}.
   *
   * <p>Such calls happen when sideEffects and localActivity markers are processed.
   */
  private boolean preparing;

  /** Key is mutable side effect id */
  private final Map<String, MutableSideEffectStateMachine> mutableSideEffects = new HashMap<>();

  /** Key is changeId */
  private final Map<String, VersionStateMachine> vesions = new HashMap<>();

  /** Map of local activities by their id. */
  private final Map<String, LocalActivityStateMachine> localActivityMap = new HashMap<>();

  private List<ExecuteLocalActivityParameters> localActivityRequests = new ArrayList<>();

  private final Functions.Proc1<ExecuteLocalActivityParameters> localActivityRequestSink;
  private final Functions.Proc1<StateMachine> stateMachineSink;

  public WorkflowStateMachines(EntityManagerListener callbacks) {
    this.callbacks = Objects.requireNonNull(callbacks);
    commandSink = (command) -> cancellableCommands.add(command);
    stateMachineSink = (stateMachine) -> {};
    localActivityRequestSink = (request) -> localActivityRequests.add(request);
  }

  @VisibleForTesting
  public WorkflowStateMachines(
      EntityManagerListener callbacks, Functions.Proc1<StateMachine> stateMachineSink) {
    this.callbacks = Objects.requireNonNull(callbacks);
    commandSink = (command) -> cancellableCommands.add(command);
    this.stateMachineSink = stateMachineSink;
    localActivityRequestSink = (request) -> localActivityRequests.add(request);
  }

  public void setStartedIds(long previousStartedEventId, long workflowTaskStartedEventId) {
    this.previousStartedEventId = previousStartedEventId;
    this.workflowTaskStartedEventId = workflowTaskStartedEventId;
    replaying = previousStartedEventId > 0;
  }

  /**
   * Handle a single event from the workflow history.
   *
   * @param event event from the history.
   * @param hasNextEvent false if this is the last event in the history.
   */
  public final void handleEvent(HistoryEvent event, boolean hasNextEvent) {
    try {
      handleEventImpl(event, hasNextEvent);
    } catch (RuntimeException e) {
      throw new InternalWorkflowTaskException(
          "Failure handling event "
              + event.getEventId()
              + " of '"
              + event.getEventType()
              + "' type. IsReplaying="
              + this.isReplaying()
              + ", PreviousStartedEventId="
              + this.getLastStartedEventId()
              + ", workflowTaskStartedEventId="
              + this.workflowTaskStartedEventId
              + ", Currently Processing StartedEventId="
              + this.currentStartedEventId,
          unwrap(e));
    }
  }

  private void handleEventImpl(HistoryEvent event, boolean hasNextEvent) {
    if (isCommandEvent(event)) {
      handleCommandEvent(event);
      return;
    }
    if (replaying
        && currentStartedEventId >= previousStartedEventId
        && event.getEventType() != EventType.EVENT_TYPE_WORKFLOW_TASK_COMPLETED) {
      replaying = false;
    }
    Long initialCommandEventId = getInitialCommandEventId(event);
    EntityStateMachine c = stateMachines.get(initialCommandEventId);
    if (c != null) {
      c.handleEvent(event, hasNextEvent);
      if (c.isFinalState()) {
        stateMachines.remove(initialCommandEventId);
      }
    } else {
      handleNonStatefulEvent(event, hasNextEvent);
    }
  }

  /**
   * Handles command event. Command event is an event which is generated from a command emitted by a
   * past decision. Each command has a correspondent event. For example ScheduleActivityTaskCommand
   * is recorded to the history as ActivityTaskScheduledEvent.
   *
   * <p>Command events always follow WorkflowTaskCompletedEvent.
   *
   * <p>The handling consists from verifying that the next command in the commands queue matches the
   * event, command state machine is notified about the event and the command is removed from the
   * commands queue.
   */
  private void handleCommandEvent(HistoryEvent event) {
    if (handleLocalActivityMarker(event)) {
      return;
    }
    // Match event to the next command in the stateMachine queue.
    // After matching the command is notified about the event and is removed from the
    // queue.
    CancellableCommand command;
    while (true) {
      // handleVersionMarker can skip a marker event if the getVersion call was removed.
      // In this case we don't want to consume a command.
      // That's why peek is used instead of poll.
      command = commands.peek();
      if (command == null) {
        throw new IllegalStateException("No command scheduled that corresponds to " + event);
      }
      // Note that handleEvent can cause a command cancellation in case
      // of MutableSideEffect
      HandleEventStatus status = command.handleEvent(event, true);
      if (status == HandleEventStatus.NON_MATCHING_EVENT) {
        if (handleVersionMarker(event)) {
          // return without consuming the command as this event is a version marker for removed
          // getVersion call.
          return;
        }
        if (!command.isCanceled()) {
          throw new IllegalStateException(
              "Event "
                  + event.getEventId()
                  + " of "
                  + event.getEventType()
                  + " does not"
                  + " match command "
                  + command.getCommandType());
        }
      }
      // Consume the command
      commands.poll();
      if (!command.isCanceled()) {
        break;
      }
    }
    validateCommand(command.getCommand(), event);

    EntityStateMachine stateMachine = command.getStateMachine();
    if (!stateMachine.isFinalState()) {
      stateMachines.put(event.getEventId(), stateMachine);
    }
    // Marker is the only command processing of which can cause workflow code execution
    // and generation of new state machines.
    if (event.getEventType() == EventType.EVENT_TYPE_MARKER_RECORDED) {
      prepareCommands();
    }
  }

  private boolean handleVersionMarker(HistoryEvent event) {
    if (event.getEventType() != EventType.EVENT_TYPE_MARKER_RECORDED) {
      return false;
    }
    MarkerRecordedEventAttributes attributes = event.getMarkerRecordedEventAttributes();
    if (!attributes.getMarkerName().equals(VERSION_MARKER_NAME)) {
      return false;
    }
    Map<String, Payloads> detailsMap = attributes.getDetailsMap();
    Optional<Payloads> oid = Optional.ofNullable(detailsMap.get(MARKER_CHANGE_ID_KEY));
    String changeId = dataConverter.fromPayloads(0, oid, String.class, String.class);
    VersionStateMachine versionStateMachine =
        vesions.computeIfAbsent(
            changeId,
            (idKey) ->
                VersionStateMachine.newInstance(
                    changeId, this::isReplaying, commandSink, stateMachineSink));
    versionStateMachine.handleNonMatchingEvent(event);
    return true;
  }

  public List<Command> takeCommands() {
    List<Command> result = new ArrayList<>(commands.size());
    for (CancellableCommand command : commands) {
      if (!command.isCanceled()) {
        result.add(command.getCommand());
      }
    }
    return result;
  }

  private void prepareCommands() {
    if (preparing) {
      return;
    }
    preparing = true;
    try {
      prepareImpl();
    } finally {
      preparing = false;
    }
  }

  private void prepareImpl() {
    // handleCommand can lead to code execution because of SideEffect, MutableSideEffect or local
    // activity completion. And code execution can lead to creation of new commands and
    // cancellation of existing commands. That is the reason for using Queue as a data structure for
    // commands.
    while (true) {
      CancellableCommand command = cancellableCommands.poll();
      if (command == null) {
        break;
      }
      // handleCommand should be called even on canceled ones to support mutableSideEffect
      command.handleCommand(command.getCommandType());
      commands.add(command);
    }
  }

  /**
   * Local activity is different from all other entities. It don't schedule a marker command when
   * the {@link #scheduleLocalActivityTask(ExecuteLocalActivityParameters, Functions.Proc2)} is
   * called. The marker is scheduled only when activity completes through ({@link
   * #handleLocalActivityCompletion(ActivityTaskHandler.Result)}).
   *
   * <p>That's why the normal logic of {@link #handleCommandEvent(HistoryEvent)}, which assumes that
   * each event has a correspondent command during replay, doesn't work. Instead local activities
   * are matched by their id using localActivityMap.
   *
   * @return true if matched and false if normal event handling should continue.
   */
  private boolean handleLocalActivityMarker(HistoryEvent event) {
    if (event.getEventType() != EventType.EVENT_TYPE_MARKER_RECORDED) {
      return false;
    }
    MarkerRecordedEventAttributes attr = event.getMarkerRecordedEventAttributes();
    if (!attr.getMarkerName().equals(LOCAL_ACTIVITY_MARKER_NAME)) {
      return false;
    }
    Map<String, Payloads> detailsMap = attr.getDetailsMap();
    Optional<Payloads> idPayloads = Optional.ofNullable(detailsMap.get(MARKER_ACTIVITY_ID_KEY));
    String id = dataConverter.fromPayloads(0, idPayloads, String.class, String.class);
    LocalActivityStateMachine stateMachine = localActivityMap.remove(id);
    if (stateMachine == null) {
      throw new IllegalStateException("Unexpected local activity id: " + id);
    }
    // RESULT_NOTIFIED state means that there is outstanding command that has to be matched
    // using standard logic. So return false to let the handleCommand method to run its standard
    // logic.
    if (stateMachine.getState() == LocalActivityStateMachine.State.RESULT_NOTIFIED) {
      return false;
    }
    stateMachine.handleEvent(event, true);
    eventLoop();
    return true;
  }

  private void eventLoop() {
    if (eventLoopExecuting) {
      return;
    }
    eventLoopExecuting = true;
    try {
      callbacks.eventLoop();
    } finally {
      eventLoopExecuting = false;
    }
    prepareCommands();
  }

  private void handleNonStatefulEvent(HistoryEvent event, boolean hasNextEvent) {
    switch (event.getEventType()) {
      case EVENT_TYPE_WORKFLOW_EXECUTION_STARTED:
        this.currentRunId =
            event.getWorkflowExecutionStartedEventAttributes().getOriginalExecutionRunId();
        callbacks.start(event);
        break;
      case EVENT_TYPE_WORKFLOW_TASK_SCHEDULED:
        WorkflowTaskStateMachine c =
            WorkflowTaskStateMachine.newInstance(
                workflowTaskStartedEventId, new WorkflowTaskCommandsListener());
        c.handleEvent(event, hasNextEvent);
        stateMachines.put(event.getEventId(), c);
        break;
      case EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED:
        callbacks.signal(event);
        break;
      case EVENT_TYPE_WORKFLOW_EXECUTION_CANCEL_REQUESTED:
        callbacks.cancel(event);
        break;
      case UNRECOGNIZED:
        break;
      default:
        throw new IllegalArgumentException("Unexpected event:" + event);
        // TODO(maxim)
    }
  }

  private long setCurrentTimeMillis(long currentTimeMillis) {
    if (this.currentTimeMillis < currentTimeMillis) {
      this.currentTimeMillis = currentTimeMillis;
      this.replayTimeUpdatedAtMillis = System.currentTimeMillis();
    }
    return this.currentTimeMillis;
  }

  public long getLastStartedEventId() {
    return currentStartedEventId;
  }

  /**
   * @param attributes attributes used to schedule an activity
   * @param callback completion callback
   * @return an instance of ActivityCommands
   */
  public Functions.Proc scheduleActivityTask(
      ExecuteActivityParameters attributes, Functions.Proc2<Optional<Payloads>, Failure> callback) {
    checkEventLoopExecuting();
    ActivityStateMachine activityStateMachine =
        ActivityStateMachine.newInstance(
            attributes,
            (p, f) -> {
              callback.apply(p, f);
              if (f != null && f.getCause() != null && f.getCause().hasCanceledFailureInfo()) {
                eventLoop();
              }
            },
            commandSink,
            stateMachineSink);
    return () -> activityStateMachine.cancel();
  }

  /**
   * Creates a new timer state machine
   *
   * @param attributes timer command attributes
   * @param completionCallback invoked when timer fires or reports cancellation. One of
   *     TimerFiredEvent, TimerCanceledEvent.
   * @return cancellation callback that should be invoked to initiate timer cancellation
   */
  public Functions.Proc newTimer(
      StartTimerCommandAttributes attributes, Functions.Proc1<HistoryEvent> completionCallback) {
    checkEventLoopExecuting();
    TimerStateMachine timer =
        TimerStateMachine.newInstance(
            attributes,
            (event) -> {
              completionCallback.apply(event);
              // Needed due to immediate cancellation
              if (event.getEventType() == EventType.EVENT_TYPE_TIMER_CANCELED) {
                eventLoop();
              }
            },
            commandSink,
            stateMachineSink);
    return () -> timer.cancel();
  }

  /**
   * Creates a new child state machine
   *
   * @param parameters child workflow start command parameters.
   * @param startedCallback callback that is notified about child start
   * @param completionCallback invoked when child reports completion or failure.
   * @return cancellation callback that should be invoked to cancel the child
   */
  public Functions.Proc startChildWorkflow(
      StartChildWorkflowExecutionParameters parameters,
      Functions.Proc1<WorkflowExecution> startedCallback,
      Functions.Proc2<Optional<Payloads>, Exception> completionCallback) {
    checkEventLoopExecuting();
    StartChildWorkflowExecutionCommandAttributes attributes = parameters.getRequest().build();
    ChildWorkflowCancellationType cancellationType = parameters.getCancellationType();
    ChildWorkflowStateMachine child =
        ChildWorkflowStateMachine.newInstance(
            attributes, startedCallback, completionCallback, commandSink, stateMachineSink);
    return () -> {
      if (cancellationType == ChildWorkflowCancellationType.ABANDON) {
        notifyChildCanceled(attributes, completionCallback);
        return;
      }
      // The only time child can be canceled directly is before its start command
      // was sent out to the service. After that RequestCancelExternal should be used.
      if (child.isCancellable()) {
        child.cancel();
        return;
      }
      if (!child.isFinalState()) {
        requestCancelExternalWorkflowExecution(
            RequestCancelExternalWorkflowExecutionCommandAttributes.newBuilder()
                .setWorkflowId(attributes.getWorkflowId())
                .setNamespace(attributes.getNamespace())
                .build(),
            (r, e) -> { // TODO(maxim): Decide what to do if an error is passed to the callback.
              if (cancellationType == ChildWorkflowCancellationType.WAIT_CANCELLATION_REQUESTED) {
                notifyChildCanceled(attributes, completionCallback);
              }
            });
        if (cancellationType == ChildWorkflowCancellationType.TRY_CANCEL) {
          notifyChildCanceled(attributes, completionCallback);
        }
      }
    };
  }

  private void notifyChildCanceled(
      StartChildWorkflowExecutionCommandAttributes attributes,
      Functions.Proc2<Optional<Payloads>, Exception> completionCallback) {
    CanceledFailure failure =
        new CanceledFailure("Child canceled", new EncodedValues(Optional.empty()), null);
    completionCallback.apply(Optional.empty(), failure);
    eventLoop();
  }

  /**
   * @param attributes
   * @param completionCallback invoked when signal delivery completes of fails. The following types
   */
  public Functions.Proc signalExternalWorkflowExecution(
      SignalExternalWorkflowExecutionCommandAttributes attributes,
      Functions.Proc2<Void, Failure> completionCallback) {
    checkEventLoopExecuting();
    return SignalExternalStateMachine.newInstance(
        attributes, completionCallback, commandSink, stateMachineSink);
  }

  /**
   * @param attributes attributes to use to cancel external worklfow
   * @param completionCallback one of ExternalWorkflowExecutionCancelRequestedEvent,
   */
  public void requestCancelExternalWorkflowExecution(
      RequestCancelExternalWorkflowExecutionCommandAttributes attributes,
      Functions.Proc2<Void, RuntimeException> completionCallback) {
    checkEventLoopExecuting();
    CancelExternalStateMachine.newInstance(
        attributes, completionCallback, commandSink, stateMachineSink);
  }

  public void upsertSearchAttributes(SearchAttributes attributes) {
    checkEventLoopExecuting();
    UpsertSearchAttributesStateMachine.newInstance(attributes, commandSink, stateMachineSink);
  }

  public void completeWorkflow(Optional<Payloads> workflowOutput) {
    checkEventLoopExecuting();
    CompleteWorkflowStateMachine.newInstance(workflowOutput, commandSink, stateMachineSink);
  }

  public void failWorkflow(Failure failure) {
    checkEventLoopExecuting();
    FailWorkflowStateMachine.newInstance(failure, commandSink, stateMachineSink);
  }

  public void cancelWorkflow() {
    checkEventLoopExecuting();
    CancelWorkflowStateMachine.newInstance(
        CancelWorkflowExecutionCommandAttributes.getDefaultInstance(),
        commandSink,
        stateMachineSink);
  }

  public void continueAsNewWorkflow(ContinueAsNewWorkflowExecutionCommandAttributes attributes) {
    checkEventLoopExecuting();
    ContinueAsNewWorkflowStateMachine.newInstance(attributes, commandSink, stateMachineSink);
  }

  public boolean isReplaying() {
    return replaying;
  }

  public long currentTimeMillis() {
    return currentTimeMillis;
  }

  public UUID randomUUID() {
    checkEventLoopExecuting();
    String runId = currentRunId;
    if (runId == null) {
      throw new Error("null currentRunId");
    }
    String id = runId + ":" + idCounter++;
    byte[] bytes = id.getBytes(StandardCharsets.UTF_8);
    return UUID.nameUUIDFromBytes(bytes);
  }

  public Random newRandom() {
    checkEventLoopExecuting();
    return new Random(randomUUID().getLeastSignificantBits());
  }

  public void sideEffect(
      Functions.Func<Optional<Payloads>> func, Functions.Proc1<Optional<Payloads>> callback) {
    checkEventLoopExecuting();
    SideEffectStateMachine.newInstance(
        this::isReplaying,
        func,
        (payloads) -> {
          callback.apply(payloads);
          // callback unblocked sideEffect call. Give workflow code chance to make progress.
          eventLoop();
        },
        commandSink,
        stateMachineSink);
  }

  /**
   * @param id mutable side effect id
   * @param func given the value from the last marker returns value to store. If result is empty
   *     nothing is recorded into the history.
   * @param callback used to report result or failure
   */
  public void mutableSideEffect(
      String id,
      Functions.Func1<Optional<Payloads>, Optional<Payloads>> func,
      Functions.Proc1<Optional<Payloads>> callback) {
    checkEventLoopExecuting();
    MutableSideEffectStateMachine stateMachine =
        mutableSideEffects.computeIfAbsent(
            id,
            (idKey) ->
                MutableSideEffectStateMachine.newInstance(
                    idKey, this::isReplaying, commandSink, stateMachineSink));
    stateMachine.mutableSideEffect(
        func,
        (r) -> {
          callback.apply(r);
          // callback unblocked mutableSideEffect call. Give workflow code chance to make progress.
          eventLoop();
        },
        stateMachineSink);
  }

  public void getVersion(
      String changeId, int minSupported, int maxSupported, Functions.Proc1<Integer> callback) {
    VersionStateMachine stateMachine =
        vesions.computeIfAbsent(
            changeId,
            (idKey) ->
                VersionStateMachine.newInstance(
                    changeId, this::isReplaying, commandSink, stateMachineSink));
    stateMachine.getVersion(
        minSupported,
        maxSupported,
        (v) -> {
          callback.apply(v);
          eventLoop();
        });
  }

  public List<ExecuteLocalActivityParameters> takeLocalActivityRequests() {
    List<ExecuteLocalActivityParameters> result = localActivityRequests;
    localActivityRequests = new ArrayList<>();
    for (ExecuteLocalActivityParameters parameters : result) {
      LocalActivityStateMachine stateMachine =
          localActivityMap.get(parameters.getActivityTask().getActivityId());
      stateMachine.markAsSent();
    }
    return result;
  }

  public void handleLocalActivityCompletion(ActivityTaskHandler.Result laCompletion) {
    LocalActivityStateMachine commands = localActivityMap.get(laCompletion.getActivityId());
    if (commands == null) {
      throw new IllegalStateException("Unknown local activity: " + laCompletion.getActivityId());
    }
    commands.handleCompletion(laCompletion);
    prepareCommands();
  }

  public Functions.Proc scheduleLocalActivityTask(
      ExecuteLocalActivityParameters parameters,
      Functions.Proc2<Optional<Payloads>, Failure> callback) {
    checkEventLoopExecuting();
    String activityId = parameters.getActivityTask().getActivityId();
    if (Strings.isNullOrEmpty(activityId)) {
      throw new IllegalArgumentException("Missing activityId: " + activityId);
    }
    if (localActivityMap.containsKey(activityId)) {
      throw new IllegalArgumentException("Duplicated local activity id: " + activityId);
    }
    LocalActivityStateMachine commands =
        LocalActivityStateMachine.newInstance(
            this::isReplaying,
            this::setCurrentTimeMillis,
            parameters,
            (r, e) -> {
              callback.apply(r, e);
              // callback unblocked local activity call. Give workflow code chance to make progress.
              eventLoop();
            },
            localActivityRequestSink,
            commandSink,
            stateMachineSink);
    localActivityMap.put(activityId, commands);
    return () -> commands.cancel();
  }

  /** Validates that command matches the event during replay. */
  private void validateCommand(Command command, HistoryEvent event) {
    // TODO(maxim): Add more thorough validation logic. For example check if activity IDs are
    // matching.
    assertMatch(
        command, event, getEventTypeForCommand(command.getCommandType()), event.getEventType());
    switch (command.getCommandType()) {
      case COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK:
        {
          ScheduleActivityTaskCommandAttributes commandAttributes =
              command.getScheduleActivityTaskCommandAttributes();
          ActivityTaskScheduledEventAttributes eventAttributes =
              event.getActivityTaskScheduledEventAttributes();
          assertMatch(
              command, event, commandAttributes.getActivityId(), eventAttributes.getActivityId());
          assertMatch(
              command,
              event,
              commandAttributes.getActivityType(),
              eventAttributes.getActivityType());
        }
        break;
      case COMMAND_TYPE_START_CHILD_WORKFLOW_EXECUTION:
        {
          StartChildWorkflowExecutionCommandAttributes commandAttributes =
              command.getStartChildWorkflowExecutionCommandAttributes();
          StartChildWorkflowExecutionInitiatedEventAttributes eventAttributes =
              event.getStartChildWorkflowExecutionInitiatedEventAttributes();
          assertMatch(
              command, event, commandAttributes.getWorkflowId(), eventAttributes.getWorkflowId());
          assertMatch(
              command,
              event,
              commandAttributes.getWorkflowType(),
              eventAttributes.getWorkflowType());
        }
        break;
      case COMMAND_TYPE_REQUEST_CANCEL_ACTIVITY_TASK:
      case COMMAND_TYPE_START_TIMER:
      case COMMAND_TYPE_CANCEL_TIMER:
      case COMMAND_TYPE_CANCEL_WORKFLOW_EXECUTION:
      case COMMAND_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION:
      case COMMAND_TYPE_RECORD_MARKER:
      case COMMAND_TYPE_CONTINUE_AS_NEW_WORKFLOW_EXECUTION:
      case COMMAND_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION:
      case COMMAND_TYPE_UPSERT_WORKFLOW_SEARCH_ATTRIBUTES:
      case COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION:
      case COMMAND_TYPE_FAIL_WORKFLOW_EXECUTION:
        break;
      case UNRECOGNIZED:
      case COMMAND_TYPE_UNSPECIFIED:
        throw new IllegalArgumentException("Unexpected command type: " + command.getCommandType());
    }
  }

  private void assertMatch(Command command, HistoryEvent event, Object expected, Object actual) {
    if (!expected.equals(actual)) {
      throw new IllegalStateException(
          command.getCommandType()
              + " doesn't match "
              + event.getEventType()
              + " with EventId="
              + event.getEventId());
    }
  }

  private class WorkflowTaskCommandsListener implements WorkflowTaskStateMachine.Listener {
    @Override
    public void workflowTaskStarted(
        long startedEventId, long currentTimeMillis, boolean nonProcessedWorkflowTask) {
      // If some new commands are pending and there are no more command events.
      for (CancellableCommand cancellableCommand : commands) {
        if (cancellableCommand == null) {
          break;
        }
        cancellableCommand.handleWorkflowTaskStarted();
      }
      // Give local activities a chance to recreate their requests if they were lost due
      // to the last workflow task failure. The loss could happen only the last workflow task
      // was forcibly created by setting forceCreate on RespondWorkflowTaskCompletedRequest.
      if (nonProcessedWorkflowTask) {
        for (LocalActivityStateMachine value : localActivityMap.values()) {
          value.nonReplayWorkflowTaskStarted();
        }
      }
      WorkflowStateMachines.this.currentStartedEventId = startedEventId;
      setCurrentTimeMillis(currentTimeMillis);
      eventLoop();
    }

    @Override
    public void updateRunId(String currentRunId) {
      WorkflowStateMachines.this.currentRunId = currentRunId;
    }
  }

  private long getInitialCommandEventId(HistoryEvent event) {
    switch (event.getEventType()) {
      case EVENT_TYPE_ACTIVITY_TASK_STARTED:
        return event.getActivityTaskStartedEventAttributes().getScheduledEventId();
      case EVENT_TYPE_ACTIVITY_TASK_COMPLETED:
        return event.getActivityTaskCompletedEventAttributes().getScheduledEventId();
      case EVENT_TYPE_ACTIVITY_TASK_FAILED:
        return event.getActivityTaskFailedEventAttributes().getScheduledEventId();
      case EVENT_TYPE_ACTIVITY_TASK_TIMED_OUT:
        return event.getActivityTaskTimedOutEventAttributes().getScheduledEventId();
      case EVENT_TYPE_ACTIVITY_TASK_CANCEL_REQUESTED:
        return event.getActivityTaskCancelRequestedEventAttributes().getScheduledEventId();
      case EVENT_TYPE_ACTIVITY_TASK_CANCELED:
        return event.getActivityTaskCanceledEventAttributes().getScheduledEventId();
      case EVENT_TYPE_TIMER_FIRED:
        return event.getTimerFiredEventAttributes().getStartedEventId();
      case EVENT_TYPE_TIMER_CANCELED:
        return event.getTimerCanceledEventAttributes().getStartedEventId();
      case EVENT_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION_FAILED:
        return event
            .getRequestCancelExternalWorkflowExecutionFailedEventAttributes()
            .getInitiatedEventId();
      case EVENT_TYPE_EXTERNAL_WORKFLOW_EXECUTION_CANCEL_REQUESTED:
        return event
            .getExternalWorkflowExecutionCancelRequestedEventAttributes()
            .getInitiatedEventId();
      case EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_FAILED:
        return event.getStartChildWorkflowExecutionFailedEventAttributes().getInitiatedEventId();
      case EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_STARTED:
        return event.getChildWorkflowExecutionStartedEventAttributes().getInitiatedEventId();
      case EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_COMPLETED:
        return event.getChildWorkflowExecutionCompletedEventAttributes().getInitiatedEventId();
      case EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_FAILED:
        return event.getChildWorkflowExecutionFailedEventAttributes().getInitiatedEventId();
      case EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_CANCELED:
        return event.getChildWorkflowExecutionCanceledEventAttributes().getInitiatedEventId();
      case EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_TIMED_OUT:
        return event.getChildWorkflowExecutionTimedOutEventAttributes().getInitiatedEventId();
      case EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_TERMINATED:
        return event.getChildWorkflowExecutionTerminatedEventAttributes().getInitiatedEventId();
      case EVENT_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_FAILED:
        return event
            .getSignalExternalWorkflowExecutionFailedEventAttributes()
            .getInitiatedEventId();
      case EVENT_TYPE_EXTERNAL_WORKFLOW_EXECUTION_SIGNALED:
        return event.getExternalWorkflowExecutionSignaledEventAttributes().getInitiatedEventId();
      case EVENT_TYPE_WORKFLOW_TASK_STARTED:
        return event.getWorkflowTaskStartedEventAttributes().getScheduledEventId();
      case EVENT_TYPE_WORKFLOW_TASK_COMPLETED:
        return event.getWorkflowTaskCompletedEventAttributes().getScheduledEventId();
      case EVENT_TYPE_WORKFLOW_TASK_TIMED_OUT:
        return event.getWorkflowTaskTimedOutEventAttributes().getScheduledEventId();
      case EVENT_TYPE_WORKFLOW_TASK_FAILED:
        return event.getWorkflowTaskFailedEventAttributes().getScheduledEventId();

      case EVENT_TYPE_ACTIVITY_TASK_SCHEDULED:
      case EVENT_TYPE_TIMER_STARTED:
      case EVENT_TYPE_MARKER_RECORDED:
      case EVENT_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_INITIATED:
      case EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_INITIATED:
      case EVENT_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION_INITIATED:
      case EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW:
      case EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED:
      case EVENT_TYPE_UPSERT_WORKFLOW_SEARCH_ATTRIBUTES:
      case EVENT_TYPE_WORKFLOW_TASK_SCHEDULED:
      case EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED:
      case EVENT_TYPE_WORKFLOW_EXECUTION_STARTED:
      case EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED:
      case EVENT_TYPE_WORKFLOW_EXECUTION_FAILED:
      case EVENT_TYPE_WORKFLOW_EXECUTION_TIMED_OUT:
      case EVENT_TYPE_WORKFLOW_EXECUTION_CANCEL_REQUESTED:
      case EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED:
        return event.getEventId();
      case UNRECOGNIZED:
      case EVENT_TYPE_UNSPECIFIED:
        throw new IllegalArgumentException("Unexpected event type: " + event.getEventType());
    }
    throw new IllegalStateException("unreachable");
  }

  /**
   * Workflow code executes only while event loop is running. So operations that can be invoked from
   * the workflow have to satisfy this condition.
   */
  private void checkEventLoopExecuting() {
    if (!eventLoopExecuting) {
      throw new IllegalStateException("Operation allowed only while eventLoop is running");
    }
  }
}
