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

import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowOptions;
import io.temporal.client.WorkflowStub;
import io.temporal.testing.TestWorkflowRule;
import io.temporal.workflow.shared.TestActivities;
import io.temporal.workflow.shared.TestOptions;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

public class LocalActivityAndQueryTest {

  private final TestActivities.TestActivitiesImpl activitiesImpl =
      new TestActivities.TestActivitiesImpl(null);

  @Rule
  public TestWorkflowRule testWorkflowRule =
      TestWorkflowRule.newBuilder()
          .setWorkflowTypes(TestLocalActivityAndQueryWorkflow.class)
          .setActivityImplementations(activitiesImpl)
          .build();

  @Test
  public void testLocalActivityAndQuery() throws ExecutionException, InterruptedException {
    WorkflowOptions options =
        WorkflowOptions.newBuilder()
            .setWorkflowRunTimeout(Duration.ofMinutes(30))
            // Large workflow task timeout to avoid workflow task heartbeating
            .setWorkflowTaskTimeout(Duration.ofSeconds(30))
            .setTaskQueue(testWorkflowRule.getTaskQueue())
            .build();
    TestWorkflowQuery workflowStub =
        testWorkflowRule.getWorkflowClient().newWorkflowStub(TestWorkflowQuery.class, options);
    WorkflowClient.start(workflowStub::execute, testWorkflowRule.getTaskQueue());

    // Ensure that query doesn't see intermediate results of the local activities execution
    // as all these activities are executed in a single workflow task.
    while (true) {
      String queryResult = workflowStub.query();
      Assert.assertTrue(queryResult, queryResult.equals("run4"));
      List<ForkJoinTask<String>> tasks = new ArrayList<ForkJoinTask<String>>();
      int threads = 30;
      if (queryResult.equals("run4")) {
        for (int i = 0; i < threads; i++) {
          ForkJoinTask<String> task = ForkJoinPool.commonPool().submit(() -> workflowStub.query());
          tasks.add(task);
        }
        for (int i = 0; i < threads; i++) {
          Assert.assertEquals("run4", tasks.get(i).get());
        }
        break;
      }
    }
    String result = WorkflowStub.fromTyped(workflowStub).getResult(String.class);
    Assert.assertEquals("done", result);
    Assert.assertEquals("run4", workflowStub.query());
    activitiesImpl.assertInvocations(
        "sleepActivity", "sleepActivity", "sleepActivity", "sleepActivity", "sleepActivity");
  }

  @WorkflowInterface
  public interface TestWorkflowQuery {
    @WorkflowMethod()
    String execute(String taskQueue);

    @QueryMethod()
    String query();
  }

  public static final class TestLocalActivityAndQueryWorkflow implements TestWorkflowQuery {

    String message = "initial value";

    @Override
    public String execute(String taskQueue) {
      TestActivities localActivities =
          Workflow.newLocalActivityStub(
              TestActivities.class, TestOptions.newLocalActivityOptions());
      for (int i = 0; i < 5; i++) {
        localActivities.sleepActivity(1000, i);
        message = "run" + i;
      }
      return "done";
    }

    @Override
    public String query() {
      return message;
    }
  }
}
