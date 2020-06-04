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

package io.temporal.failure;

import io.temporal.proto.failure.ActivityTaskFailureInfo;
import io.temporal.proto.failure.Failure;

public final class ActivityException extends RemoteException {

  private final long scheduledEventId;
  private final long startedEventId;
  private final String identity;

  public ActivityException(Failure failure, Exception cause) {
    super(failure, cause);
    if (!failure.hasActivityTaskFailureInfo()) {
      throw new IllegalArgumentException(
          "Activity failure expected: " + failure.getFailureInfoCase());
    }
    ActivityTaskFailureInfo info = failure.getActivityTaskFailureInfo();
    this.scheduledEventId = info.getScheduledEventId();
    this.startedEventId = info.getStartedEventId();
    this.identity = info.getIdentity();
  }

  public long getScheduledEventId() {
    return scheduledEventId;
  }

  public long getStartedEventId() {
    return startedEventId;
  }

  public String getIdentity() {
    return identity;
  }
}