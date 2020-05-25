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

package io.temporal.internal.worker;

import io.temporal.internal.common.RpcRetryOptions;
import io.temporal.proto.workflowservice.PollForDecisionTaskResponse;
import io.temporal.proto.workflowservice.RespondDecisionTaskCompletedRequest;
import io.temporal.proto.workflowservice.RespondDecisionTaskFailedRequest;
import io.temporal.proto.workflowservice.RespondQueryTaskCompletedRequest;

/**
 * Interface of workflow task handlers.
 *
 * @author fateev, suskin
 */
public interface DecisionTaskHandler {

  final class Result {
    private final RespondDecisionTaskCompletedRequest taskCompleted;
    private final RespondDecisionTaskFailedRequest taskFailed;
    private final RespondQueryTaskCompletedRequest queryCompleted;
    private final RpcRetryOptions requestRetryOptions;
    private final boolean finalDecision;

    public Result(
        RespondDecisionTaskCompletedRequest taskCompleted,
        RespondDecisionTaskFailedRequest taskFailed,
        RespondQueryTaskCompletedRequest queryCompleted,
        RpcRetryOptions requestRetryOptions,
        boolean finalDecision) {
      this.taskCompleted = taskCompleted;
      this.taskFailed = taskFailed;
      this.queryCompleted = queryCompleted;
      this.requestRetryOptions = requestRetryOptions;
      this.finalDecision = finalDecision;
    }

    public RespondDecisionTaskCompletedRequest getTaskCompleted() {
      return taskCompleted;
    }

    public RespondDecisionTaskFailedRequest getTaskFailed() {
      return taskFailed;
    }

    public RespondQueryTaskCompletedRequest getQueryCompleted() {
      return queryCompleted;
    }

    public RpcRetryOptions getRequestRetryOptions() {
      return requestRetryOptions;
    }

    public boolean isFinalDecision() {
      return finalDecision;
    }

    @Override
    public String toString() {
      return "Result{"
          + "taskCompleted="
          + taskCompleted
          + ", taskFailed="
          + taskFailed
          + ", queryCompleted="
          + queryCompleted
          + ", requestRetryOptions="
          + requestRetryOptions
          + '}';
    }
  }

  /**
   * Handles a single workflow task. Shouldn't throw any exceptions. A compliant implementation
   * should return any unexpected errors as RespondDecisionTaskFailedRequest.
   *
   * @param decisionTask The decision task to handle.
   * @return One of the possible decision task replies: RespondDecisionTaskCompletedRequest,
   *     RespondQueryTaskCompletedRequest, RespondDecisionTaskFailedRequest
   */
  Result handleDecisionTask(PollForDecisionTaskResponse decisionTask) throws Exception;

  /** True if this handler handles at least one workflow type. */
  boolean isAnyTypeSupported();
}
