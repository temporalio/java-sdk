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

package io.temporal.workflow;

import io.temporal.proto.event.WorkflowExecutionFailedCause;
import io.temporal.proto.execution.WorkflowExecution;

/**
 * Exception used to communicate failure of a request to signal an external workflow. TODO: Hook it
 * up with RequestCancelExternalWorkflowExecutionFailed and WorkflowExecutionCancelRequested
 */
@SuppressWarnings("serial")
public final class CancelExternalWorkflowException extends WorkflowOperationException {

  private WorkflowExecutionFailedCause failureCause;

  private WorkflowExecution signaledExecution;

  public CancelExternalWorkflowException(
      long eventId, WorkflowExecution signaledExecution, WorkflowExecutionFailedCause cause) {
    super(cause + " for signaledExecution=\"" + signaledExecution, eventId);
    this.signaledExecution = signaledExecution;
    this.failureCause = cause;
  }

  public WorkflowExecutionFailedCause getFailureCause() {
    return failureCause;
  }

  public WorkflowExecution getSignaledExecution() {
    return signaledExecution;
  }
}
