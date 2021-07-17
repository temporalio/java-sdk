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

import io.temporal.common.interceptors.WorkflowInboundCallsInterceptor;
import io.temporal.common.interceptors.WorkflowOutboundCallsInterceptor;

public class BaseRootWorkflowInboundCallsInterceptor implements WorkflowInboundCallsInterceptor {
  protected final SyncWorkflowContext workflowContext;

  public BaseRootWorkflowInboundCallsInterceptor(SyncWorkflowContext workflowContext) {
    this.workflowContext = workflowContext;
  }

  @Override
  public void init(WorkflowOutboundCallsInterceptor outboundCalls) {
    workflowContext.initHeadOutboundCallsInterceptor(outboundCalls);
  }

  @Override
  public WorkflowOutput execute(WorkflowInput input) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void handleSignal(SignalInput input) {
    workflowContext.handleInterceptedSignal(input);
  }

  @Override
  public QueryOutput handleQuery(QueryInput input) {
    return workflowContext.handleInterceptedQuery(input);
  }

  @Override
  public Object newWorkflowMethodThread(Runnable runnable, String name) {
    return workflowContext.newWorkflowMethodThreadIntercepted(runnable, name);
  }

  @Override
  public Object newCallbackThread(Runnable runnable, String name) {
    return workflowContext.newWorkflowCallbackThreadIntercepted(runnable, name);
  }
}
