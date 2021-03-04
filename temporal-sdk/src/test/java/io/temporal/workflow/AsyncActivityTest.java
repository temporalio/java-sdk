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

import static org.junit.Assert.assertEquals;

import io.temporal.testing.TestOptions;
import io.temporal.testing.TestWorkflowRule;
import io.temporal.workflow.shared.TestActivities;
import io.temporal.workflow.shared.TestWorkflows;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

public class AsyncActivityTest {
  private final TestActivities.TestActivitiesImpl activitiesImpl =
      new TestActivities.TestActivitiesImpl(null);

  @Rule
  public TestWorkflowRule testWorkflowRule =
      TestWorkflowRule.newBuilder()
          .setWorkflowTypes(TestAsyncActivityWorkflowImpl.class)
          .setActivityImplementations(activitiesImpl)
          .build();

  @Test
  public void testAsyncActivity() {
    // TODO: (vkoby) See if this activityImpl could be constructed from within the rule with the
    // right completion client.
    activitiesImpl.completionClient =
        testWorkflowRule.getWorkflowClient().newActivityCompletionClient();
    TestWorkflows.TestWorkflow1 client =
        testWorkflowRule.newWorkflowStubTimeoutOptions(TestWorkflows.TestWorkflow1.class);
    String result = client.execute(testWorkflowRule.getTaskQueue());
    Assert.assertEquals("workflow", result);
    Assert.assertEquals("proc", activitiesImpl.procResult.get(0));
    Assert.assertEquals("1", activitiesImpl.procResult.get(1));
    Assert.assertEquals("12", activitiesImpl.procResult.get(2));
    Assert.assertEquals("123", activitiesImpl.procResult.get(3));
    Assert.assertEquals("1234", activitiesImpl.procResult.get(4));
    Assert.assertEquals("12345", activitiesImpl.procResult.get(5));
    Assert.assertEquals("123456", activitiesImpl.procResult.get(6));
  }

  public static class TestAsyncActivityWorkflowImpl implements TestWorkflows.TestWorkflow1 {

    @Override
    public String execute(String taskQueue) {
      TestActivities testActivities =
          Workflow.newActivityStub(
              TestActivities.class, TestOptions.newActivityOptions20sScheduleToClose());
      Promise<String> a = Async.function(testActivities::activity);
      Promise<Integer> a1 = Async.function(testActivities::activity1, 1);
      Promise<String> a2 = Async.function(testActivities::activity2, "1", 2);
      Promise<String> a3 = Async.function(testActivities::activity3, "1", 2, 3);
      Promise<String> a4 = Async.function(testActivities::activity4, "1", 2, 3, 4);
      Promise<String> a5 = Async.function(testActivities::activity5, "1", 2, 3, 4, 5);
      Promise<String> a6 = Async.function(testActivities::activity6, "1", 2, 3, 4, 5, 6);
      assertEquals("activity", a.get());
      assertEquals(1, (int) a1.get());
      assertEquals("12", a2.get());
      assertEquals("123", a3.get());
      assertEquals("1234", a4.get());
      assertEquals("12345", a5.get());
      assertEquals("123456", a6.get());

      Async.procedure(testActivities::proc).get();
      Async.procedure(testActivities::proc1, "1").get();
      Async.procedure(testActivities::proc2, "1", 2).get();
      Async.procedure(testActivities::proc3, "1", 2, 3).get();
      Async.procedure(testActivities::proc4, "1", 2, 3, 4).get();
      Async.procedure(testActivities::proc5, "1", 2, 3, 4, 5).get();
      Async.procedure(testActivities::proc6, "1", 2, 3, 4, 5, 6).get();

      // Test serialization of generic data structure
      List<UUID> uuids = new ArrayList<>();
      uuids.add(Workflow.randomUUID());
      uuids.add(Workflow.randomUUID());
      List<UUID> uuidsResult = Async.function(testActivities::activityUUIDList, uuids).get();
      assertEquals(uuids, uuidsResult);
      return "workflow";
    }
  }
}
