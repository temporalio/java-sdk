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

package io.temporal.internal.sync;

import io.temporal.internal.replay.DecisionContext;
import io.temporal.proto.common.SearchAttributes;
import io.temporal.workflow.WorkflowInfo;
import java.time.Duration;

final class WorkflowInfoImpl implements WorkflowInfo {

  private final DecisionContext context;

  WorkflowInfoImpl(DecisionContext context) {
    this.context = context;
  }

  @Override
  public String getDomain() {
    return context.getDomain();
  }

  @Override
  public String getWorkflowId() {
    return context.getWorkflowId();
  }

  @Override
  public String getRunId() {
    return context.getRunId();
  }

  @Override
  public String getWorkflowType() {
    return context.getWorkflowType().getName();
  }

  @Override
  public String getTaskList() {
    return context.getTaskList();
  }

  @Override
  public Duration getExecutionStartToCloseTimeout() {
    return context.getExecutionStartToCloseTimeout();
  }

  @Override
  public SearchAttributes getSearchAttributes() {
    return context.getSearchAttributes();
  }
}
