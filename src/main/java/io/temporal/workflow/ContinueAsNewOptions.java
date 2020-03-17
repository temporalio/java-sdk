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

package io.temporal.workflow;

import io.temporal.internal.common.OptionsUtils;
import java.time.Duration;

public final class ContinueAsNewOptions {

  public static Builder newBuilder() {
    return new Builder();
  }

  public static Builder newBuilder(ContinueAsNewOptions options) {
    return new Builder(options);
  }

  public static ContinueAsNewOptions getDefaultInstance() {
    return DEFAULT_INSTANCE;
  }

  private static final ContinueAsNewOptions DEFAULT_INSTANCE;

  static {
    DEFAULT_INSTANCE = ContinueAsNewOptions.newBuilder().build();
  }

  public static final class Builder {

    private Duration executionStartToCloseTimeout;
    private String taskList;
    private Duration taskStartToCloseTimeout;

    private Builder() {}

    private Builder(ContinueAsNewOptions options) {
      if (options == null) {
        return;
      }
      this.executionStartToCloseTimeout = options.executionStartToCloseTimeout;
      this.taskList = options.taskList;
      this.taskStartToCloseTimeout = options.taskStartToCloseTimeout;
    }

    public Builder setExecutionStartToCloseTimeout(Duration executionStartToCloseTimeout) {
      this.executionStartToCloseTimeout = executionStartToCloseTimeout;
      return this;
    }

    public Builder setTaskList(String taskList) {
      this.taskList = taskList;
      return this;
    }

    public Builder setTaskStartToCloseTimeout(Duration taskStartToCloseTimeout) {
      this.taskStartToCloseTimeout = taskStartToCloseTimeout;
      return this;
    }

    public ContinueAsNewOptions build() {
      return new ContinueAsNewOptions(
          OptionsUtils.roundUpToSeconds(executionStartToCloseTimeout),
          taskList,
          OptionsUtils.roundUpToSeconds(taskStartToCloseTimeout));
    }
  }

  private final Duration executionStartToCloseTimeout;
  private final String taskList;
  private final Duration taskStartToCloseTimeout;

  public ContinueAsNewOptions(
      Duration executionStartToCloseTimeout, String taskList, Duration taskStartToCloseTimeout) {
    this.executionStartToCloseTimeout = executionStartToCloseTimeout;
    this.taskList = taskList;
    this.taskStartToCloseTimeout = taskStartToCloseTimeout;
  }

  public Duration getExecutionStartToCloseTimeout() {
    return executionStartToCloseTimeout;
  }

  public String getTaskList() {
    return taskList;
  }

  public Duration getTaskStartToCloseTimeout() {
    return taskStartToCloseTimeout;
  }
}
