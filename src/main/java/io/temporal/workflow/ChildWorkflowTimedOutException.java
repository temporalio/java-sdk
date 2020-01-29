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

import io.temporal.WorkflowExecution;
import io.temporal.WorkflowType;

/**
 * Indicates that a child workflow exceeded its execution timeout and was forcefully terminated by
 * the Cadence service.
 */
@SuppressWarnings("serial")
public final class ChildWorkflowTimedOutException extends ChildWorkflowException {

  public ChildWorkflowTimedOutException(
      long eventId, WorkflowExecution workflowExecution, WorkflowType workflowType) {
    super("Time Out", eventId, workflowExecution, workflowType);
  }
}
