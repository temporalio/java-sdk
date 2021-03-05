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

import io.temporal.activity.ActivityOptions;
import io.temporal.client.WorkflowException;
import io.temporal.failure.ActivityFailure;
import io.temporal.failure.ApplicationFailure;
import io.temporal.workflow.shared.SDKTestWorkflowRule;
import io.temporal.workflow.shared.TestActivities;
import io.temporal.workflow.shared.TestWorkflows;
import java.time.Duration;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

public class ActivityApplicationNoSpecifiedRetryTest {

  private final TestActivities.TestActivitiesImpl activitiesImpl =
      new TestActivities.TestActivitiesImpl(null);

  @Rule
  public SDKTestWorkflowRule testWorkflowRule =
      SDKTestWorkflowRule.newBuilder()
          .setWorkflowTypes(TestActivityApplicationNoSpecifiedRetry.class)
          .setActivityImplementations(activitiesImpl)
          .build();

  @Test
  public void testActivityApplicationNoSpecifiedRetry() {
    TestWorkflows.TestWorkflow1 workflowStub =
        testWorkflowRule.newWorkflowStubTimeoutOptions(TestWorkflows.TestWorkflow1.class);
    try {
      workflowStub.execute(testWorkflowRule.getTaskQueue());
      Assert.fail("unreachable");
    } catch (WorkflowException e) {
      Assert.assertTrue(e.getCause() instanceof ActivityFailure);
      Assert.assertTrue(e.getCause().getCause() instanceof ApplicationFailure);
      Assert.assertEquals(
          "simulatedType", ((ApplicationFailure) e.getCause().getCause()).getType());
    }

    // Since no retry policy is passed by the client, we fall back to the default retry policy of
    // the mock server, which mimics the default on a default Temporal deployment.
    Assert.assertEquals(3, activitiesImpl.applicationFailureCounter.get());
  }

  public static class TestActivityApplicationNoSpecifiedRetry
      implements TestWorkflows.TestWorkflow1 {

    private TestActivities activities;

    @Override
    public String execute(String taskQueue) {
      ActivityOptions options =
          ActivityOptions.newBuilder()
              .setTaskQueue(taskQueue)
              .setScheduleToCloseTimeout(Duration.ofSeconds(200))
              .setStartToCloseTimeout(Duration.ofSeconds(1))
              .build();
      activities = Workflow.newActivityStub(TestActivities.class, options);
      activities.throwApplicationFailureThreeTimes();
      return "ignored";
    }
  }
}
