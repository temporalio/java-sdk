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

import io.temporal.client.WorkflowOptions;
import io.temporal.workflow.shared.SDKTestWorkflowRule;
import io.temporal.workflow.shared.TestActivities;
import io.temporal.workflow.shared.TestMultiargdsWorkflowFunctions;
import io.temporal.workflow.shared.TestWorkflows;
import java.time.Duration;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

public class UntypedChildStubWorkflowAsyncInvokeTest {

  private final TestActivities.TestActivitiesImpl activitiesImpl =
      new TestActivities.TestActivitiesImpl(null);

  @Rule
  public SDKTestWorkflowRule testWorkflowRule =
      SDKTestWorkflowRule.newBuilder()
          .setWorkflowTypes(
              TestUntypedChildStubWorkflowAsyncInvoke.class,
              TestMultiargdsWorkflowFunctions.TestMultiargsWorkflowsImpl.class)
          .setActivityImplementations(activitiesImpl)
          .build();

  @Test
  public void testUntypedChildStubWorkflowAsyncInvoke() {
    WorkflowOptions options =
        WorkflowOptions.newBuilder()
            .setWorkflowRunTimeout(Duration.ofSeconds(200))
            .setWorkflowTaskTimeout(Duration.ofSeconds(60))
            .setTaskQueue(testWorkflowRule.getTaskQueue())
            .build();
    TestWorkflows.TestWorkflow1 client =
        testWorkflowRule
            .getWorkflowClient()
            .newWorkflowStub(TestWorkflows.TestWorkflow1.class, options);
    Assert.assertEquals(null, client.execute(testWorkflowRule.getTaskQueue()));
  }

  public static class TestUntypedChildStubWorkflowAsyncInvoke
      implements TestWorkflows.TestWorkflow1 {

    @Override
    public String execute(String taskQueue) {
      ChildWorkflowOptions workflowOptions =
          ChildWorkflowOptions.newBuilder().setTaskQueue(taskQueue).build();
      ChildWorkflowStub stubF =
          Workflow.newUntypedChildWorkflowStub("TestMultiargsWorkflowsFunc", workflowOptions);
      Assert.assertEquals("func", Async.function(stubF::<String>execute, String.class).get());
      // Workflow type overridden through the @WorkflowMethod.name
      ChildWorkflowStub stubF1 = Workflow.newUntypedChildWorkflowStub("func1", workflowOptions);
      Assert.assertEquals("1", Async.function(stubF1::<String>execute, String.class, "1").get());
      ChildWorkflowStub stubF2 =
          Workflow.newUntypedChildWorkflowStub("TestMultiargsWorkflowsFunc2", workflowOptions);
      Assert.assertEquals(
          "12", Async.function(stubF2::<String>execute, String.class, "1", 2).get());
      ChildWorkflowStub stubF3 =
          Workflow.newUntypedChildWorkflowStub("TestMultiargsWorkflowsFunc3", workflowOptions);
      Assert.assertEquals(
          "123", Async.function(stubF3::<String>execute, String.class, "1", 2, 3).get());
      ChildWorkflowStub stubF4 =
          Workflow.newUntypedChildWorkflowStub("TestMultiargsWorkflowsFunc4", workflowOptions);
      Assert.assertEquals(
          "1234", Async.function(stubF4::<String>execute, String.class, "1", 2, 3, 4).get());
      ChildWorkflowStub stubF5 =
          Workflow.newUntypedChildWorkflowStub("TestMultiargsWorkflowsFunc5", workflowOptions);
      Assert.assertEquals(
          "12345", Async.function(stubF5::<String>execute, String.class, "1", 2, 3, 4, 5).get());

      ChildWorkflowStub stubP =
          Workflow.newUntypedChildWorkflowStub("TestMultiargsWorkflowsProc", workflowOptions);
      Async.procedure(stubP::<Void>execute, Void.class).get();
      ChildWorkflowStub stubP1 =
          Workflow.newUntypedChildWorkflowStub("TestMultiargsWorkflowsProc1", workflowOptions);
      Async.procedure(stubP1::<Void>execute, Void.class, "1").get();
      ChildWorkflowStub stubP2 =
          Workflow.newUntypedChildWorkflowStub("TestMultiargsWorkflowsProc2", workflowOptions);
      Async.procedure(stubP2::<Void>execute, Void.class, "1", 2).get();
      ChildWorkflowStub stubP3 =
          Workflow.newUntypedChildWorkflowStub("TestMultiargsWorkflowsProc3", workflowOptions);
      Async.procedure(stubP3::<Void>execute, Void.class, "1", 2, 3).get();
      ChildWorkflowStub stubP4 =
          Workflow.newUntypedChildWorkflowStub("TestMultiargsWorkflowsProc4", workflowOptions);
      Async.procedure(stubP4::<Void>execute, Void.class, "1", 2, 3, 4).get();
      ChildWorkflowStub stubP5 =
          Workflow.newUntypedChildWorkflowStub("TestMultiargsWorkflowsProc5", workflowOptions);
      Async.procedure(stubP5::<Void>execute, Void.class, "1", 2, 3, 4, 5).get();
      return null;
    }
  }
}
