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

package io.temporal.workflow.activityTests;

import io.temporal.client.WorkflowException;
import io.temporal.common.RetryOptions;
import io.temporal.failure.ApplicationFailure;
import io.temporal.internal.sync.DeterministicRunnerTest;
import io.temporal.workflow.Async;
import io.temporal.workflow.Workflow;
import io.temporal.workflow.shared.SDKTestWorkflowRule;
import io.temporal.workflow.shared.TestWorkflows;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

public class AsyncRetryTest {

  @Rule
  public SDKTestWorkflowRule testWorkflowRule =
      SDKTestWorkflowRule.newBuilder().setWorkflowTypes(TestAsyncRetryWorkflowImpl.class).build();

  /** @see DeterministicRunnerTest#testRetry() */
  @Test
  public void testAsyncRetry() {
    TestWorkflows.TestWorkflow2 client =
        testWorkflowRule.newWorkflowStubTimeoutOptions(TestWorkflows.TestWorkflow2.class);
    String result = null;
    try {
      result = client.execute(SDKTestWorkflowRule.useExternalService);
      Assert.fail("unreachable");
    } catch (WorkflowException e) {
      Assert.assertTrue(e.getCause() instanceof ApplicationFailure);
      Assert.assertEquals("test", ((ApplicationFailure) e.getCause()).getType());
      Assert.assertEquals(
          "message='simulated', type='test', nonRetryable=false", e.getCause().getMessage());
    }
    Assert.assertNull(result);
    List<String> trace = client.getTrace();
    Assert.assertEquals(trace.toString(), 3, trace.size());
    Assert.assertEquals("started", trace.get(0));
    Assert.assertTrue(trace.get(1).startsWith("retry at "));
    Assert.assertTrue(trace.get(2).startsWith("retry at "));
  }

  public static class TestAsyncRetryWorkflowImpl implements TestWorkflows.TestWorkflow2 {

    private static final RetryOptions retryOptions =
        RetryOptions.newBuilder()
            .setInitialInterval(Duration.ofSeconds(1))
            .setMaximumInterval(Duration.ofSeconds(1))
            .setBackoffCoefficient(1)
            .build();

    private final List<String> trace = new ArrayList<>();

    @Override
    public String execute(boolean useExternalService) {
      trace.clear(); // clear because of replay
      trace.add("started");
      Async.retry(
              retryOptions,
              Optional.of(Duration.ofSeconds(2)),
              () -> {
                trace.add("retry at " + Workflow.currentTimeMillis());
                return Workflow.newFailedPromise(
                    ApplicationFailure.newFailure("simulated", "test"));
              })
          .get();
      trace.add("beforeSleep");
      Workflow.sleep(60000);
      trace.add("done");
      return "";
    }

    @Override
    public List<String> getTrace() {
      return trace;
    }
  }
}
