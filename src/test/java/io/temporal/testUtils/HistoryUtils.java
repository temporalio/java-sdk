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

package io.temporal.testUtils;

import static io.temporal.internal.common.InternalUtils.createNormalTaskList;
import static io.temporal.internal.common.InternalUtils.createStickyTaskList;
import static io.temporal.testUtils.TestServiceUtils.*;

import io.temporal.internal.testservice.TestWorkflowService;
import io.temporal.proto.workflowservice.PollForDecisionTaskResponse;
import io.temporal.serviceclient.GrpcWorkflowServiceFactory;

public class HistoryUtils {
  private HistoryUtils() {}

  private static final String DOMAIN = "domain";
  private static final String TASK_LIST = "taskList";
  private static final String HOST_TASK_LIST = "stickyTaskList";
  private static final String WORKFLOW_TYPE = "workflowType";

  public static PollForDecisionTaskResponse generateDecisionTaskWithInitialHistory()
      throws Exception {
    try (TestWorkflowService testService = new TestWorkflowService(true);
        GrpcWorkflowServiceFactory service = testService.newClientStub(); ) {
      return generateDecisionTaskWithInitialHistory(DOMAIN, TASK_LIST, WORKFLOW_TYPE, service);
    }
  }

  public static PollForDecisionTaskResponse generateDecisionTaskWithInitialHistory(
      String domain, String tasklistName, String workflowType, GrpcWorkflowServiceFactory service)
      throws Exception {
    startWorkflowExecution(domain, tasklistName, workflowType, service);
    return pollForDecisionTask(domain, createNormalTaskList(tasklistName), service);
  }

  public static PollForDecisionTaskResponse generateDecisionTaskWithPartialHistory()
      throws Exception {
    return generateDecisionTaskWithPartialHistory(DOMAIN, TASK_LIST, WORKFLOW_TYPE);
  }

  public static PollForDecisionTaskResponse generateDecisionTaskWithPartialHistory(
      String domain, String tasklistName, String workflowType) throws Exception {
    try (TestWorkflowService testService = new TestWorkflowService(true);
        GrpcWorkflowServiceFactory service = testService.newClientStub(); ) {
      PollForDecisionTaskResponse response =
          generateDecisionTaskWithInitialHistory(domain, tasklistName, workflowType, service);
      return generateDecisionTaskWithPartialHistoryFromExistingTask(
          response, domain, HOST_TASK_LIST, service);
    }
  }

  public static PollForDecisionTaskResponse generateDecisionTaskWithPartialHistoryFromExistingTask(
      PollForDecisionTaskResponse response,
      String domain,
      String stickyTaskListName,
      GrpcWorkflowServiceFactory service)
      throws Exception {
    signalWorkflow(response.getWorkflowExecution(), domain, service);
    respondDecisionTaskCompletedWithSticky(response.getTaskToken(), stickyTaskListName, service);
    return pollForDecisionTask(domain, createStickyTaskList(stickyTaskListName), service);
  }
}
