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

package io.temporal.internal.sync;

import io.temporal.api.common.v1.Payloads;
import io.temporal.common.converter.DataConverter;
import io.temporal.common.converter.EncodedValues;
import io.temporal.common.converter.Values;
import io.temporal.common.interceptors.Header;
import io.temporal.common.interceptors.WorkerInterceptor;
import io.temporal.common.interceptors.WorkflowInboundCallsInterceptor;
import io.temporal.common.interceptors.WorkflowOutboundCallsInterceptor;
import io.temporal.workflow.DynamicWorkflow;
import io.temporal.workflow.Functions;
import java.util.Optional;

final class DynamicSyncWorkflowDefinition implements SyncWorkflowDefinition {

  private final Functions.Func<? extends DynamicWorkflow> factory;
  private final WorkerInterceptor[] workerInterceptors;
  private final DataConverter dataConverter;
  private WorkflowInboundCallsInterceptor workflowInvoker;

  public DynamicSyncWorkflowDefinition(
      Functions.Func<? extends DynamicWorkflow> factory,
      WorkerInterceptor[] workerInterceptors,
      DataConverter dataConverter) {
    this.factory = factory;
    this.workerInterceptors = workerInterceptors;
    this.dataConverter = dataConverter;
  }

  @Override
  public void initialize() {
    SyncWorkflowContext workflowContext = WorkflowInternal.getRootWorkflowContext();
    workflowInvoker = new RootWorkflowInboundCallsInterceptor(workflowContext);
    for (WorkerInterceptor workerInterceptor : workerInterceptors) {
      workflowInvoker = workerInterceptor.interceptWorkflow(workflowInvoker);
    }
    workflowContext.initHeadInboundCallsInterceptor(workflowInvoker);
    workflowInvoker.init(workflowContext);
  }

  @Override
  public Optional<Payloads> execute(Header header, Optional<Payloads> input) {
    Values args = new EncodedValues(input, dataConverter);
    WorkflowInboundCallsInterceptor.WorkflowOutput result =
        workflowInvoker.execute(
            new WorkflowInboundCallsInterceptor.WorkflowInput(header, new Object[] {args}));
    return dataConverter.toPayloads(result.getResult());
  }

  private class RootWorkflowInboundCallsInterceptor
      extends BaseRootWorkflowInboundCallsInterceptor {
    private DynamicWorkflow workflow;

    public RootWorkflowInboundCallsInterceptor(SyncWorkflowContext workflowContext) {
      super(workflowContext);
    }

    @Override
    public void init(WorkflowOutboundCallsInterceptor outboundCalls) {
      super.init(outboundCalls);
      newInstance();
      WorkflowInternal.registerListener(workflow);
    }

    @Override
    public WorkflowOutput execute(WorkflowInput input) {
      Object result = workflow.execute((EncodedValues) input.getArguments()[0]);
      return new WorkflowOutput(result);
    }

    private void newInstance() {
      if (workflow != null) {
        throw new IllegalStateException("Already called");
      }
      workflow = factory.apply();
    }
  }
}
