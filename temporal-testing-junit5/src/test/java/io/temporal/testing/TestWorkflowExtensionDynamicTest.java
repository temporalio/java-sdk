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

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.temporal.activity.Activity;
import io.temporal.activity.ActivityInfo;
import io.temporal.activity.ActivityOptions;
import io.temporal.activity.DynamicActivity;
import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowOptions;
import io.temporal.client.WorkflowStub;
import io.temporal.common.converter.EncodedValues;
import io.temporal.workflow.ActivityStub;
import io.temporal.workflow.DynamicWorkflow;
import io.temporal.workflow.Workflow;
import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;

@Timeout(value = 30, unit = TimeUnit.SECONDS)
public class TestWorkflowExtensionDynamicTest {

  @RegisterExtension
  public static final TestWorkflowExtension testWorkflow =
      TestWorkflowExtension.newBuilder()
          .setWorkflowTypes(HelloDynamicWorkflowImpl.class)
          .setActivityImplementations(new HelloDynamicActivityImpl())
          .build();

  public static class HelloDynamicActivityImpl implements DynamicActivity {

    @Override
    public Object execute(EncodedValues args) {
      String name = args.get(0, String.class);
      ActivityInfo activityInfo = Activity.getExecutionContext().getInfo();
      return String.format(
          "Hello %s from activity %s and workflow %s",
          name, activityInfo.getActivityType(), activityInfo.getWorkflowType());
    }
  }

  @WorkflowInterface
  public interface HelloWorkflow {

    @WorkflowMethod
    String sayHello(String name);
  }

  public static class HelloDynamicWorkflowImpl implements DynamicWorkflow {

    private static final Logger logger = Workflow.getLogger(HelloDynamicWorkflowImpl.class);

    private final ActivityStub activity =
        Workflow.newUntypedActivityStub(
            ActivityOptions.newBuilder().setStartToCloseTimeout(Duration.ofMinutes(1)).build());

    @Override
    public Object execute(EncodedValues args) {
      String name = args.get(0, String.class);
      logger.info("Hello, {}", name);
      Workflow.sleep(Duration.ofHours(1));
      return activity.execute("BuildGreeting", String.class, name);
    }
  }

  @Test
  public void extensionShouldResolveDynamicWorkflowParameters(HelloWorkflow workflow) {
    assertEquals(
        "Hello World from activity BuildGreeting and workflow HelloWorkflow",
        workflow.sayHello("World"));
  }

  @Test
  public void extensionShouldSupportLaunchingViaUntypedWorkflowStubs(
      WorkflowClient workflowClient, WorkflowOptions workflowOptions) {
    WorkflowStub workflow =
        workflowClient.newUntypedWorkflowStub("AnotherHelloWorkflow", workflowOptions);
    workflow.start("World");
    assertEquals(
        "Hello World from activity BuildGreeting and workflow AnotherHelloWorkflow",
        workflow.getResult(String.class));
  }
}
