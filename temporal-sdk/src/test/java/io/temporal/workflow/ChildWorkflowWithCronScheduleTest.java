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

import static io.temporal.workflow.WorkflowTest.lastCompletionResult;
import static io.temporal.workflow.shared.TestOptions.newWorkflowOptionsWithTimeouts;
import static org.junit.Assert.*;
import static org.junit.Assert.assertTrue;

import io.temporal.client.WorkflowFailedException;
import io.temporal.client.WorkflowStub;
import io.temporal.failure.CanceledFailure;
import io.temporal.workflow.shared.SDKTestWorkflowRule;
import io.temporal.workflow.shared.TestActivities;
import io.temporal.workflow.shared.TestWorkflows;
import java.time.Duration;
import org.junit.Assume;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

public class ChildWorkflowWithCronScheduleTest {

  private final TestActivities.TestActivitiesImpl activitiesImpl =
      new TestActivities.TestActivitiesImpl(null);

  @Rule public TestName testName = new TestName();

  @Rule
  public SDKTestWorkflowRule testWorkflowRule =
      SDKTestWorkflowRule.newBuilder()
          .setWorkflowTypes(
              TestCronParentWorkflow.class, WorkflowTest.TestWorkflowWithCronScheduleImpl.class)
          .setActivityImplementations(activitiesImpl)
          .build();

  @Test
  public void testChildWorkflowWithCronSchedule() {
    // Min interval in cron is 1min. So we will not test it against real service in Jenkins.
    // Feel free to uncomment the line below and test in local.
    Assume.assumeFalse("skipping as test will timeout", SDKTestWorkflowRule.useExternalService);

    WorkflowStub client =
        testWorkflowRule
            .getWorkflowClient()
            .newUntypedWorkflowStub(
                "TestWorkflow1",
                newWorkflowOptionsWithTimeouts(testWorkflowRule.getTaskQueue())
                    .toBuilder()
                    .setWorkflowRunTimeout(Duration.ofHours(10))
                    .build());
    client.start(testName.getMethodName());
    testWorkflowRule.getTestEnvironment().sleep(Duration.ofHours(3));
    client.cancel();

    try {
      client.getResult(String.class);
      fail("unreachable");
    } catch (WorkflowFailedException e) {
      assertTrue(e.getCause() instanceof CanceledFailure);
    }

    // Run 3 failed. So on run 4 we get the last completion result from run 2.
    assertEquals("run 2", lastCompletionResult);
  }

  public static class TestCronParentWorkflow implements TestWorkflows.TestWorkflow1 {
    private final WorkflowTest.TestWorkflowWithCronSchedule cronChild =
        Workflow.newChildWorkflowStub(WorkflowTest.TestWorkflowWithCronSchedule.class);

    @Override
    public String execute(String taskQueue) {
      return cronChild.execute(taskQueue);
    }
  }
}
