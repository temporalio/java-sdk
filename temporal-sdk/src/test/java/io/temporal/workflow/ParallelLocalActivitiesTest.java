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

import io.temporal.client.*;
import io.temporal.testing.TracingWorkerInterceptor;
import io.temporal.workflow.shared.SDKTestWorkflowRule;
import io.temporal.workflow.shared.TestActivities;
import io.temporal.workflow.shared.TestOptions;
import io.temporal.workflow.shared.TestWorkflows;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

public class ParallelLocalActivitiesTest {

  private final TestActivities.TestActivitiesImpl activitiesImpl =
      new TestActivities.TestActivitiesImpl(null);

  @Rule
  public SDKTestWorkflowRule testWorkflowRule =
          SDKTestWorkflowRule.newBuilder()
              .setWorkflowTypes(TestParallelLocalActivitiesWorkflowImpl.class)
              .setActivityImplementations(activitiesImpl)
              .setWorkerInterceptors(
                  new TracingWorkerInterceptor(new TracingWorkerInterceptor.FilteredTrace()))
              .build();

  @Test
  public void testParallelLocalActivities() {
    WorkflowOptions options =
        WorkflowOptions.newBuilder()
            .setWorkflowRunTimeout(Duration.ofMinutes(5))
            .setWorkflowTaskTimeout(Duration.ofSeconds(3))
            .setTaskQueue(testWorkflowRule.getTaskQueue())
            .build();

    TestWorkflows.TestWorkflow1 workflowStub =
        testWorkflowRule
            .getWorkflowClient()
            .newWorkflowStub(TestWorkflows.TestWorkflow1.class, options);
    String result = workflowStub.execute(testWorkflowRule.getTaskQueue());
    Assert.assertEquals("done", result);
    Assert.assertEquals(activitiesImpl.toString(), 100, activitiesImpl.invocations.size());
    List<String> expected = new ArrayList<String>();
    expected.add("interceptExecuteWorkflow " + SDKTestWorkflowRule.UUID_REGEXP);
    expected.add("newThread workflow-method");
    for (int i = 0; i < WorkflowTest.TestParallelLocalActivitiesWorkflowImpl.COUNT; i++) {
      expected.add("executeLocalActivity SleepActivity");
      expected.add("currentTimeMillis");
    }
    for (int i = 0; i < WorkflowTest.TestParallelLocalActivitiesWorkflowImpl.COUNT; i++) {
      expected.add("local activity SleepActivity");
    }
    testWorkflowRule
        .getInterceptor(TracingWorkerInterceptor.class)
        .setExpected(expected.toArray(new String[0]));
  }

  public static class TestParallelLocalActivitiesWorkflowImpl
      implements TestWorkflows.TestWorkflow1 {
    static final int COUNT = 100;

    @Override
    public String execute(String taskQueue) {
      TestActivities localActivities =
          Workflow.newLocalActivityStub(
              TestActivities.class, TestOptions.newLocalActivityOptions());
      List<Promise<String>> laResults = new ArrayList<>();
      Random r = Workflow.newRandom();
      for (int i = 0; i < COUNT; i++) {
        laResults.add(Async.function(localActivities::sleepActivity, (long) r.nextInt(3000), i));
      }
      Promise.allOf(laResults).get();
      return "done";
    }
  }
}
