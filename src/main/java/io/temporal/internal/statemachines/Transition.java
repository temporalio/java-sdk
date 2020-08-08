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

import java.util.Objects;

class Transition<State, ExplicitEvent> {

  final State from;
  final ExplicitEvent explicitEvent;

  public Transition(State from, ExplicitEvent explicitEvent) {
    this.from = Objects.requireNonNull(from);
    this.explicitEvent = Objects.requireNonNull(explicitEvent);
  }

  public State getFrom() {
    return from;
  }

  public ExplicitEvent getExplicitEvent() {
    return explicitEvent;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Transition<?, ?> that = (Transition<?, ?>) o;
    return com.google.common.base.Objects.equal(from, that.from)
        && com.google.common.base.Objects.equal(explicitEvent, that.explicitEvent);
  }

  @Override
  public int hashCode() {
    return com.google.common.base.Objects.hashCode(from, explicitEvent);
  }

  @Override
  public String toString() {
    return "Transition{" + "from='" + from + '\'' + ", explicitEvent=" + explicitEvent + '}';
  }
}
