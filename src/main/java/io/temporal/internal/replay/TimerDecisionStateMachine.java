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

package io.temporal.internal.replay;

import io.temporal.CancelTimerDecisionAttributes;
import io.temporal.Decision;
import io.temporal.DecisionType;
import io.temporal.HistoryEvent;
import io.temporal.StartTimerDecisionAttributes;

/**
 * Timer doesn't have separate initiation decision as it is started immediately. But from the state
 * machine point of view it is modeled the same as activity with no TimerStarted event used as
 * initiation event.
 *
 * @author fateev
 */
class TimerDecisionStateMachine extends DecisionStateMachineBase {

  private StartTimerDecisionAttributes attributes;

  private boolean canceled;

  public TimerDecisionStateMachine(DecisionId id, StartTimerDecisionAttributes attributes) {
    super(id);
    this.attributes = attributes;
  }

  /** Used for unit testing */
  TimerDecisionStateMachine(
      DecisionId id, StartTimerDecisionAttributes attributes, DecisionState state) {
    super(id, state);
    this.attributes = attributes;
  }

  @Override
  public Decision getDecision() {
    switch (state) {
      case CREATED:
        return createStartTimerDecision();
      case CANCELED_AFTER_INITIATED:
        return createCancelTimerDecision();
      default:
        return null;
    }
  }

  @Override
  public void handleDecisionTaskStartedEvent() {
    switch (state) {
      case CANCELED_AFTER_INITIATED:
        stateHistory.add("handleDecisionTaskStartedEvent");
        state = DecisionState.CANCELLATION_DECISION_SENT;
        stateHistory.add(state.toString());
        break;
      default:
        super.handleDecisionTaskStartedEvent();
    }
  }

  @Override
  public void handleCancellationFailureEvent(HistoryEvent event) {
    switch (state) {
      case CANCELLATION_DECISION_SENT:
        stateHistory.add("handleCancellationFailureEvent");
        state = DecisionState.INITIATED;
        stateHistory.add(state.toString());
        break;
      default:
        super.handleCancellationFailureEvent(event);
    }
  }

  @Override
  public boolean cancel(Runnable immediateCancellationCallback) {
    canceled = true;
    immediateCancellationCallback.run();
    return super.cancel(null);
  }

  /**
   * As timer is canceled immediately there is no need for waiting after cancellation decision was
   * sent.
   */
  @Override
  public boolean isDone() {
    return state == DecisionState.COMPLETED || canceled;
  }

  private Decision createCancelTimerDecision() {
    CancelTimerDecisionAttributes tryCancel = new CancelTimerDecisionAttributes();
    tryCancel.setTimerId(attributes.getTimerId());
    Decision decision = new Decision();
    decision.setCancelTimerDecisionAttributes(tryCancel);
    decision.setDecisionType(DecisionType.CancelTimer);
    return decision;
  }

  private Decision createStartTimerDecision() {
    Decision decision = new Decision();
    decision.setStartTimerDecisionAttributes(attributes);
    decision.setDecisionType(DecisionType.StartTimer);
    return decision;
  }
}
