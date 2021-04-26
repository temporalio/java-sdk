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

package io.temporal.activity;

import io.temporal.worker.WorkflowImplementationOptions;
import io.temporal.workflow.Workflow;
import io.temporal.workflow.shared.SDKTestWorkflowRule;
import io.temporal.workflow.shared.TestActivities.TestActivity;
import io.temporal.workflow.shared.TestActivities.TestActivityImpl;
import io.temporal.workflow.shared.TestOptions;
import io.temporal.workflow.shared.TestWorkflows.TestWorkflow3;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

public class DefaultActivityOptionsSetOnWorkflowTest {

  public static final ActivityOptions defaultOps = TestOptions.newActivityOptions1();
  private static final ActivityOptions activityOps2 =
      TestOptions.newActivityOptions20sScheduleToClose();

  Map<String, ActivityOptions> activity2MethodOptions =
      new HashMap<String, ActivityOptions>() {
        {
          put("Activity2", activityOps2);
        }
      };

  @Rule
  public SDKTestWorkflowRule testWorkflowRule =
      SDKTestWorkflowRule.newBuilder()
          .setWorkflowTypes(
              WorkflowImplementationOptions.newBuilder()
                  .setDefaultActivityOptions(defaultOps)
                  .setActivityOptions(activity2MethodOptions)
                  .build(),
              TestSetDefaultActivityOptionsWorkflowImpl.class)
          .setActivityImplementations(new TestActivityImpl())
          .build();

  @Test
  public void testSetWorkflowImplementationOptions() {
    TestWorkflow3 workflowStub =
        testWorkflowRule.newWorkflowStubTimeoutOptions(TestWorkflow3.class);
    Map<String, Map<String, Duration>> result = workflowStub.execute();

    // Check that activity1 has default options.
    Map<String, Duration> activity1Values = result.get("Activity1");
    Assert.assertEquals(defaultOps.getHeartbeatTimeout(), activity1Values.get("HeartbeatTimeout"));
    Assert.assertEquals(
        defaultOps.getScheduleToCloseTimeout(), activity1Values.get("ScheduleToCloseTimeout"));
    Assert.assertEquals(
        defaultOps.getStartToCloseTimeout(), activity1Values.get("StartToCloseTimeout"));

    // Check that default options for activity2 were overwritten.
    // Note that if scheduleToStartTimeout or startToCloseTimeout are null, they are set to the
    // scheduleToCloseTimeout value.
    Map<String, Duration> activity2Values = result.get("Activity2");
    Assert.assertEquals(defaultOps.getHeartbeatTimeout(), activity2Values.get("HeartbeatTimeout"));
    Assert.assertEquals(
        activityOps2.getScheduleToCloseTimeout(), activity2Values.get("ScheduleToCloseTimeout"));
    Assert.assertEquals(
        activityOps2.getStartToCloseTimeout(), activity2Values.get("StartToCloseTimeout"));
  }

  public static class TestSetDefaultActivityOptionsWorkflowImpl implements TestWorkflow3 {
    @Override
    public Map<String, Map<String, Duration>> execute() {
      Map<String, Map<String, Duration>> result = new HashMap<>();
      TestActivity activities = Workflow.newActivityStub(TestActivity.class);
      result.put("Activity1", activities.activity1());
      result.put("Activity2", activities.activity2());
      return result;
    }
  }
}
