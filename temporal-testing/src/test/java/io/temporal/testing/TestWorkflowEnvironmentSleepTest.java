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

package io.temporal.testing;

import static org.junit.Assert.*;

import io.temporal.api.enums.v1.TimeoutType;
import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowFailedException;
import io.temporal.client.WorkflowOptions;
import io.temporal.client.WorkflowStub;
import io.temporal.failure.TimeoutFailure;
import io.temporal.worker.Worker;
import io.temporal.workflow.SignalMethod;
import io.temporal.workflow.Workflow;
import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;
import java.time.Duration;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

public class TestWorkflowEnvironmentSleepTest {

  @Rule
  public TestWatcher watchman =
      new TestWatcher() {
        @Override
        protected void failed(Throwable e, Description description) {
          if (testEnv != null) {
            System.err.println(testEnv.getDiagnostics());
            testEnv.close();
          }
        }
      };

  @WorkflowInterface
  public interface ExampleWorkflow {
    @WorkflowMethod
    void execute();

    @SignalMethod
    void signal();
  }

  public static class HingingWorkflowWithSignalImpl implements ExampleWorkflow {
    @Override
    public void execute() {
      Workflow.sleep(Duration.ofMinutes(20));
    }

    @Override
    public void signal() {}
  }

  private TestWorkflowEnvironment testEnv;
  private Worker worker;
  private WorkflowClient client;
  private static final String WORKFLOW_TASK_QUEUE = "EXAMPLE";

  @Before
  public void setUp() {
    testEnv = TestWorkflowEnvironment.newInstance();
    worker = testEnv.newWorker(WORKFLOW_TASK_QUEUE);
    client = testEnv.getWorkflowClient();
    worker.registerWorkflowImplementationTypes(HingingWorkflowWithSignalImpl.class);
    testEnv.start();
  }

  @After
  public void tearDown() {
    testEnv.close();
  }

  @Test(timeout = 2000)
  public void testSignalAfterStartThenSleep() {
    ExampleWorkflow workflow =
        client.newWorkflowStub(
            ExampleWorkflow.class,
            WorkflowOptions.newBuilder().setTaskQueue(WORKFLOW_TASK_QUEUE).build());
    WorkflowClient.start(workflow::execute);
    workflow.signal();
    testEnv.sleep(Duration.ofMinutes(50L));
  }

  @Test
  public void testWorkflowTimeoutDuringSleep() {
    ExampleWorkflow workflow =
        client.newWorkflowStub(
            ExampleWorkflow.class,
            WorkflowOptions.newBuilder()
                .setWorkflowExecutionTimeout(Duration.ofMinutes(3))
                .setTaskQueue(WORKFLOW_TASK_QUEUE)
                .build());

    WorkflowClient.start(workflow::execute);

    testEnv.sleep(Duration.ofMinutes(11L));

    WorkflowStub workflowStub = WorkflowStub.fromTyped(workflow);
    try {
      workflowStub.getResult(Void.class);
      fail("Workflow should fail with timeout exception");
    } catch (WorkflowFailedException e) {
      Throwable cause = e.getCause();
      assertTrue(cause instanceof TimeoutFailure);
      assertEquals(
          TimeoutType.TIMEOUT_TYPE_START_TO_CLOSE, ((TimeoutFailure) cause).getTimeoutType());
    }
  }
}
