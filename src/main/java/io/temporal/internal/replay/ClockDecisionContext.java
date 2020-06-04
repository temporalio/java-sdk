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

import com.google.common.base.Strings;
import io.temporal.common.converter.DataConverter;
import io.temporal.failure.FailureConverter;
import io.temporal.internal.common.LocalActivityMarkerData;
import io.temporal.internal.sync.WorkflowInternal;
import io.temporal.internal.worker.LocalActivityWorker;
import io.temporal.proto.common.ActivityType;
import io.temporal.proto.common.Payloads;
import io.temporal.proto.common.SearchAttributes;
import io.temporal.proto.decision.StartTimerDecisionAttributes;
import io.temporal.proto.event.HistoryEvent;
import io.temporal.proto.event.MarkerRecordedEventAttributes;
import io.temporal.proto.event.TimerCanceledEventAttributes;
import io.temporal.proto.event.TimerFiredEventAttributes;
import io.temporal.workflow.ActivityFailureException;
import io.temporal.workflow.Functions.Func;
import io.temporal.workflow.Functions.Func1;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Clock that must be used inside workflow definition code to ensure replay determinism. */
public final class ClockDecisionContext {

  private static final String SIDE_EFFECT_MARKER_NAME = "SideEffect";
  private static final String MUTABLE_SIDE_EFFECT_MARKER_NAME = "MutableSideEffect";
  public static final String VERSION_MARKER_NAME = "Version";
  public static final String LOCAL_ACTIVITY_MARKER_NAME = "LocalActivity";

  private static final Logger log = LoggerFactory.getLogger(ClockDecisionContext.class);

  private final class TimerCancellationHandler implements Consumer<Exception> {

    private final long startEventId;

    TimerCancellationHandler(long timerId) {
      this.startEventId = timerId;
    }

    @Override
    public void accept(Exception reason) {
      decisions.cancelTimer(startEventId, () -> timerCancelled(startEventId, reason));
    }
  }

  private final DecisionsHelper decisions;
  // key is startedEventId
  private final Map<Long, OpenRequestInfo<?, Long>> scheduledTimers = new HashMap<>();
  private long replayCurrentTimeMilliseconds = -1;
  // Local time when replayCurrentTimeMilliseconds was updated.
  private long replayTimeUpdatedAtMillis = -1;
  private boolean replaying = true;
  // Key is side effect marker eventId
  private final Map<Long, Optional<Payloads>> sideEffectResults = new HashMap<>();
  private final MarkerHandler mutableSideEffectHandler;
  private final MarkerHandler versionHandler;
  private final BiFunction<LocalActivityWorker.Task, Duration, Boolean> laTaskPoller;
  private final Map<String, OpenRequestInfo<Optional<Payloads>, ActivityType>> pendingLaTasks =
      new HashMap<>();
  private final Map<String, ExecuteLocalActivityParameters> unstartedLaTasks = new HashMap<>();
  private final ReplayDecider replayDecider;
  private final DataConverter dataConverter;
  private final Condition taskCondition;
  private boolean taskCompleted = false;

  ClockDecisionContext(
      DecisionsHelper decisions,
      BiFunction<LocalActivityWorker.Task, Duration, Boolean> laTaskPoller,
      ReplayDecider replayDecider,
      DataConverter dataConverter) {
    this.decisions = decisions;
    this.taskCondition = replayDecider.getLock().newCondition();
    mutableSideEffectHandler =
        new MarkerHandler(decisions, MUTABLE_SIDE_EFFECT_MARKER_NAME, () -> replaying);
    versionHandler = new MarkerHandler(decisions, VERSION_MARKER_NAME, () -> replaying);
    this.laTaskPoller = laTaskPoller;
    this.replayDecider = replayDecider;
    this.dataConverter = dataConverter;
  }

  public long currentTimeMillis() {
    return replayCurrentTimeMilliseconds;
  }

  private long replayTimeUpdatedAtMillis() {
    return replayTimeUpdatedAtMillis;
  }

  void setReplayCurrentTimeMilliseconds(long replayCurrentTimeMilliseconds) {
    if (this.replayCurrentTimeMilliseconds < replayCurrentTimeMilliseconds) {
      this.replayCurrentTimeMilliseconds = replayCurrentTimeMilliseconds;
      this.replayTimeUpdatedAtMillis = System.currentTimeMillis();
    }
  }

  boolean isReplaying() {
    return replaying;
  }

  Consumer<Exception> createTimer(long delaySeconds, Consumer<Exception> callback) {
    if (delaySeconds < 0) {
      throw new IllegalArgumentException("Negative delaySeconds: " + delaySeconds);
    }
    if (delaySeconds == 0) {
      callback.accept(null);
      return null;
    }
    long firingTime = currentTimeMillis() + TimeUnit.SECONDS.toMillis(delaySeconds);
    final OpenRequestInfo<?, Long> context = new OpenRequestInfo<>(firingTime);
    final StartTimerDecisionAttributes timer =
        StartTimerDecisionAttributes.newBuilder()
            .setStartToFireTimeoutSeconds(delaySeconds)
            .setTimerId(String.valueOf(decisions.getAndIncrementNextId()))
            .build();
    long startEventId = decisions.startTimer(timer);
    context.setCompletionHandle((ctx, e) -> callback.accept(e));
    scheduledTimers.put(startEventId, context);
    return new TimerCancellationHandler(startEventId);
  }

  void setReplaying(boolean replaying) {
    this.replaying = replaying;
  }

  void handleTimerFired(TimerFiredEventAttributes attributes) {
    long startedEventId = attributes.getStartedEventId();
    if (decisions.handleTimerClosed(attributes)) {
      OpenRequestInfo<?, Long> scheduled = scheduledTimers.remove(startedEventId);
      if (scheduled != null) {
        // Server doesn't guarantee that the timer fire timestamp is larger or equal of the
        // expected fire time. So fix the time or timer firing will be ignored.
        long firingTime = scheduled.getUserContext();
        if (replayCurrentTimeMilliseconds < firingTime) {
          setReplayCurrentTimeMilliseconds(firingTime);
        }
        BiConsumer<?, Exception> completionCallback = scheduled.getCompletionCallback();
        completionCallback.accept(null, null);
      }
    }
  }

  void handleTimerCanceled(HistoryEvent event) {
    TimerCanceledEventAttributes attributes = event.getTimerCanceledEventAttributes();
    long startedEventId = attributes.getStartedEventId();
    if (decisions.handleTimerCanceled(event)) {
      timerCancelled(startedEventId, null);
    }
  }

  private void timerCancelled(long startEventId, Exception reason) {
    OpenRequestInfo<?, ?> scheduled = scheduledTimers.remove(startEventId);
    if (scheduled == null) {
      return;
    }
    BiConsumer<?, Exception> context = scheduled.getCompletionCallback();
    CancellationException exception = new CancellationException("Cancelled by request");
    exception.initCause(reason);
    context.accept(null, exception);
  }

  Optional<Payloads> sideEffect(Func<Optional<Payloads>> func) {
    decisions.addAllMissingVersionMarker();
    long sideEffectEventId = decisions.getNextDecisionEventId();
    Optional<Payloads> result;
    if (replaying) {
      result = sideEffectResults.get(sideEffectEventId);
      if (result == null) {
        throw new Error("No cached result found for SideEffect EventId=" + sideEffectEventId);
      }
    } else {
      try {
        result = func.apply();
      } catch (Error e) {
        throw e;
      } catch (Exception e) {
        throw new Error("sideEffect function failed", e);
      }
    }
    decisions.recordMarker(SIDE_EFFECT_MARKER_NAME, null, result);
    return result;
  }

  /**
   * @param id mutable side effect id
   * @param func given the value from the last marker returns value to store. If result is empty
   *     nothing is recorded into the history.
   * @return the latest value returned by func
   */
  Optional<Payloads> mutableSideEffect(
      String id, DataConverter converter, Func1<Optional<Payloads>, Optional<Payloads>> func) {
    decisions.addAllMissingVersionMarker();
    return mutableSideEffectHandler.handle(id, converter, func);
  }

  void upsertSearchAttributes(SearchAttributes searchAttributes) {
    decisions.upsertSearchAttributes(searchAttributes);
  }

  void handleMarkerRecorded(HistoryEvent event) {
    MarkerRecordedEventAttributes attributes = event.getMarkerRecordedEventAttributes();
    String name = attributes.getMarkerName();
    if (SIDE_EFFECT_MARKER_NAME.equals(name)) {
      Optional<Payloads> details =
          attributes.hasDetails() ? Optional.of(attributes.getDetails()) : Optional.empty();
      sideEffectResults.put(event.getEventId(), details);
    } else if (LOCAL_ACTIVITY_MARKER_NAME.equals(name)) {
      handleLocalActivityMarker(attributes);
    } else if (!MUTABLE_SIDE_EFFECT_MARKER_NAME.equals(name) && !VERSION_MARKER_NAME.equals(name)) {
      if (log.isWarnEnabled()) {
        log.warn("Unexpected marker: " + event);
      }
    }
  }

  private void handleLocalActivityMarker(MarkerRecordedEventAttributes attributes) {
    LocalActivityMarkerData marker =
        LocalActivityMarkerData.fromEventAttributes(
            attributes, dataConverter.getPayloadConverter());
    if (pendingLaTasks.containsKey(marker.getActivityId())) {
      if (log.isDebugEnabled()) {
        log.debug("Handle LocalActivityMarker for activity " + marker.getActivityId());
      }

      Optional<Payloads> details =
          attributes.hasDetails() ? Optional.of(attributes.getDetails()) : Optional.empty();
      decisions.recordMarker(
          LOCAL_ACTIVITY_MARKER_NAME,
          marker.getHeader(dataConverter.getPayloadConverter()),
          details);

      OpenRequestInfo<Optional<Payloads>, ActivityType> scheduled =
          pendingLaTasks.remove(marker.getActivityId());
      unstartedLaTasks.remove(marker.getActivityId());

      Exception failure = null;
      if (marker.getFailure().isPresent()) {
        Throwable cause =
            FailureConverter.failureToException(marker.getFailure().get(), dataConverter);
        ActivityType activityType =
            ActivityType.newBuilder().setName(marker.getActivityType()).build();
        failure =
            new ActivityFailureException(
                attributes.getDecisionTaskCompletedEventId(),
                activityType,
                marker.getActivityId(),
                cause,
                marker.getAttempt(),
                marker.getBackoff());
      }

      BiConsumer<Optional<Payloads>, Exception> completionHandle =
          scheduled.getCompletionCallback();
      completionHandle.accept(marker.getResult(), failure);
      setReplayCurrentTimeMilliseconds(marker.getReplayTimeMillis());

      taskCompleted = true;
      // This method is already called under the lock.
      taskCondition.signal();
    }
  }

  /**
   * During replay getVersion should account for the following situations at the current eventId.
   *
   * <ul>
   *   <li>There is correspondent Marker with the same changeId: return version from the marker.
   *   <li>There is no Marker with the same changeId: return DEFAULT_VERSION,
   *   <li>There is marker with a different changeId (possibly more than one) and the marker with
   *       matching changeId follows them: add fake decisions for all the version markers that
   *       precede the matching one as the correspondent getVersion calls were removed
   *   <li>There is marker with a different changeId (possibly more than one) and no marker with
   *       matching changeId follows them: return DEFAULT_VERSION as it looks like the getVersion
   *       was added after that part of code has executed
   *   <li>Another case is when there is no call to getVersion and there is a version marker: insert
   *       fake decisions for all version markers up to the event that caused the lookup.
   * </ul>
   */
  int getVersion(String changeId, DataConverter converter, int minSupported, int maxSupported) {
    decisions.addAllMissingVersionMarker(Optional.of(changeId), Optional.of(converter));

    Optional<Payloads> result =
        versionHandler.handle(
            changeId,
            converter,
            (stored) -> {
              if (stored.isPresent()) {
                return Optional.empty();
              }
              return converter.toData(maxSupported);
            });

    if (!result.isPresent()) {
      return WorkflowInternal.DEFAULT_VERSION;
    }
    int version = converter.fromData(result, Integer.class, Integer.class);
    validateVersion(changeId, version, minSupported, maxSupported);
    return version;
  }

  private void validateVersion(String changeId, int version, int minSupported, int maxSupported) {
    if ((version < minSupported || version > maxSupported)
        && version != WorkflowInternal.DEFAULT_VERSION) {
      throw new Error(
          String.format(
              "Version %d of changeId %s is not supported. Supported version is between %d and %d.",
              version, changeId, minSupported, maxSupported));
    }
  }

  Consumer<Exception> scheduleLocalActivityTask(
      ExecuteLocalActivityParameters params, BiConsumer<Optional<Payloads>, Exception> callback) {
    final OpenRequestInfo<Optional<Payloads>, ActivityType> context =
        new OpenRequestInfo<>(params.getActivityType());
    context.setCompletionHandle(callback);
    if (Strings.isNullOrEmpty(params.getActivityId())) {
      params.setActivityId(decisions.getAndIncrementNextId());
    }
    pendingLaTasks.put(params.getActivityId(), context);
    unstartedLaTasks.put(params.getActivityId(), params);
    return null;
  }

  boolean startUnstartedLaTasks(Duration maxWaitAllowed) {
    long startTime = System.currentTimeMillis();
    for (ExecuteLocalActivityParameters params : unstartedLaTasks.values()) {
      long currTime = System.currentTimeMillis();
      maxWaitAllowed = maxWaitAllowed.minus(Duration.ofMillis(currTime - startTime));
      boolean applied =
          laTaskPoller.apply(
              new LocalActivityWorker.Task(
                  params,
                  replayDecider.getLocalActivityCompletionSink(),
                  replayDecider.getWorkflowTaskTimeoutSeconds(),
                  this::currentTimeMillis,
                  this::replayTimeUpdatedAtMillis),
              maxWaitAllowed);
      if (!applied) {
        return false;
      }
    }
    unstartedLaTasks.clear();
    return true;
  }

  int numPendingLaTasks() {
    return pendingLaTasks.size();
  }

  void awaitTaskCompletion(Duration duration) throws InterruptedException {
    while (!taskCompleted) {
      // This call is called from already locked object
      taskCondition.awaitNanos(duration.toNanos());
    }
    taskCompleted = false;
  }
}
