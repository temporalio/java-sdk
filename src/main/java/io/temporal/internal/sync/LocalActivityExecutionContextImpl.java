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

import io.temporal.activity.ActivityTask;
import io.temporal.client.ActivityCompletionException;
import io.temporal.proto.common.WorkflowExecution;
import io.temporal.serviceclient.WorkflowServiceStubs;
import java.lang.reflect.Type;
import java.util.Optional;

class LocalActivityExecutionContextImpl implements ActivityExecutionContext {
  private final WorkflowServiceStubs service;
  private final String domain;
  private final ActivityTask task;

  LocalActivityExecutionContextImpl(
      WorkflowServiceStubs service, String domain, ActivityTask task) {
    this.domain = domain;
    this.service = service;
    this.task = task;
  }

  @Override
  public byte[] getTaskToken() {
    throw new UnsupportedOperationException("getTaskToken is not supported for local activities");
  }

  @Override
  public WorkflowExecution getWorkflowExecution() {
    return task.getWorkflowExecution();
  }

  @Override
  public ActivityTask getTask() {
    return task;
  }

  @Override
  public <V> void recordActivityHeartbeat(V details) throws ActivityCompletionException {
    throw new UnsupportedOperationException(
        "recordActivityHeartbeat is not supported for local activities");
  }

  @Override
  public <V> Optional<V> getHeartbeatDetails(Class<V> detailsClass, Type detailsType) {
    throw new UnsupportedOperationException(
        "getHeartbeatDetails is not supported for local activities");
  }

  @Override
  public void doNotCompleteOnReturn() {
    throw new UnsupportedOperationException(
        "doNotCompleteOnReturn is not supported for local activities");
  }

  @Override
  public boolean isDoNotCompleteOnReturn() {
    throw new UnsupportedOperationException(
        "isDoNotCompleteOnReturn is not supported for local activities");
  }

  @Override
  public WorkflowServiceStubs getService() {
    return service;
  }

  @Override
  public String getDomain() {
    return domain;
  }
}
