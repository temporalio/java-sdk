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

import static org.junit.Assert.assertTrue;

import com.google.common.base.Throwables;
import io.temporal.client.WorkflowOptions;
import io.temporal.testing.TestWorkflowRule;
import io.temporal.testing.TracingWorkerInterceptor;
import io.temporal.workflow.shared.TestActivities;
import io.temporal.workflow.shared.TestWorkflows;
import java.time.Duration;
import org.junit.Rule;
import org.junit.Test;

public class ChildWorkflowTimeoutTest {

  private final TestActivities.TestActivitiesImpl activitiesImpl =
      new TestActivities.TestActivitiesImpl(null);

  @Rule
  public TestWorkflowRule testWorkflowRule =
      TestWorkflowRule.newBuilder()
          .setWorkflowTypes(TestParentWorkflowWithChildTimeout.class, WorkflowTest.TestChild.class)
          .setActivityImplementations(activitiesImpl)
          .setWorkerInterceptors(
              new TracingWorkerInterceptor(new TracingWorkerInterceptor.FilteredTrace()))
          .build();

  @Test
  public void testChildWorkflowTimeout() {
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
    String result = client.execute(testWorkflowRule.getTaskQueue());
    assertTrue(result, result.contains("ChildWorkflowFailure"));
    assertTrue(result, result.contains("TimeoutFailure"));
  }

  public static class TestParentWorkflowWithChildTimeout implements TestWorkflows.TestWorkflow1 {

    private final WorkflowTest.ITestChild child;

    public TestParentWorkflowWithChildTimeout() {
      ChildWorkflowOptions options =
          ChildWorkflowOptions.newBuilder().setWorkflowRunTimeout(Duration.ofSeconds(1)).build();
      child = Workflow.newChildWorkflowStub(WorkflowTest.ITestChild.class, options);
    }

    @Override
    public String execute(String taskQueue) {
      try {
        child.execute("Hello ", (int) Duration.ofDays(1).toMillis());
      } catch (Exception e) {
        return Throwables.getStackTraceAsString(e);
      }
      throw new RuntimeException("not reachable");
    }
  }
}
