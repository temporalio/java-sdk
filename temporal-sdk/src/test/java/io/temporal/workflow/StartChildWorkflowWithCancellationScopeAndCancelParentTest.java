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
import static org.junit.Assert.fail;

import io.temporal.client.WorkflowFailedException;
import io.temporal.client.WorkflowStub;
import io.temporal.failure.CanceledFailure;
import io.temporal.testing.TracingWorkerInterceptor;
import io.temporal.workflow.shared.SDKTestWorkflowRule;
import io.temporal.workflow.shared.TestActivities;
import java.util.ArrayList;
import java.util.List;
import org.junit.Rule;
import org.junit.Test;

public class StartChildWorkflowWithCancellationScopeAndCancelParentTest {

  private final TestActivities.TestActivitiesImpl activitiesImpl =
      new TestActivities.TestActivitiesImpl(null);

  @Rule
  public SDKTestWorkflowRule testWorkflowRule =
      SDKTestWorkflowRule.newBuilder()
          .setWorkflowTypes(ParentThatStartsChildInCancellationScope.class, SleepyChild.class)
          .setActivityImplementations(activitiesImpl)
          .setWorkerInterceptors(
              new TracingWorkerInterceptor(new TracingWorkerInterceptor.FilteredTrace()))
          .build();

  @Test
  public void testStartChildWorkflowWithCancellationScopeAndCancelParent() {
    WorkflowStub workflow = testWorkflowRule.newUntypedWorkflowStubTimeoutOptions("TestWorkflow");
    workflow.start(ChildWorkflowCancellationType.WAIT_CANCELLATION_COMPLETED);
    workflow.cancel();
    try {
      workflow.getResult(Void.class);
      fail("unreachable");
    } catch (WorkflowFailedException e) {
      assertTrue(e.getCause() instanceof CanceledFailure);
    }
  }

  public static class ParentThatStartsChildInCancellationScope
      implements WorkflowTest.TestWorkflow {
    @Override
    public void execute(ChildWorkflowCancellationType cancellationType) {
      WorkflowTest.TestChildWorkflow child =
          Workflow.newChildWorkflowStub(
              WorkflowTest.TestChildWorkflow.class,
              ChildWorkflowOptions.newBuilder().setCancellationType(cancellationType).build());
      List<Promise<Void>> children = new ArrayList<>();
      // This is a non blocking call that returns immediately.
      // Use child.composeGreeting("Hello", name) to call synchronously.
      CancellationScope scope =
          Workflow.newCancellationScope(
              () -> {
                Promise<Void> promise = Async.procedure(child::execute);
                children.add(promise);
              });
      scope.run();
      Promise.allOf(children).get();
    }
  }

  public static class SleepyChild implements WorkflowTest.TestChildWorkflow {
    @Override
    public void execute() {
      Workflow.await(() -> false);
    }
  }
}
