/*
 *  Copyright 2012-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *  Modifications copyright (C) 2017 Uber Technologies, Inc.
 *
 *  Modifications copyright (C) 2020 Temporal Technologies, Inc.
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

package io.temporal.serviceclient;

import io.temporal.proto.workflowservice.WorkflowServiceGrpc;
import java.util.concurrent.TimeUnit;

public interface WorkflowServiceStubs {

  /**
   * Create gRPC connection stubs using default options. The options default to the connection to
   * the locally running temporal service.
   */
  static WorkflowServiceStubs newInstance() {
    return new WorkflowServiceStubsImpl(null, WorkflowServiceStubsOptions.getDefaultInstance());
  }

  /** Create gRPC connection stubs using provided options. */
  static WorkflowServiceStubs newInstance(WorkflowServiceStubsOptions options) {
    return new WorkflowServiceStubsImpl(null, options);
  }

  /**
   * Create gRPC connection stubs that connect to the provided service implementation using an
   * in-memory channel. Useful for testing, usually with mock and spy services.
   */
  static WorkflowServiceStubs newInstance(
      WorkflowServiceGrpc.WorkflowServiceImplBase service, WorkflowServiceStubsOptions options) {
    return new WorkflowServiceStubsImpl(service, options);
  }

  /** @return Blocking (synchronous) stub that allows direct calls to service. */
  WorkflowServiceGrpc.WorkflowServiceBlockingStub blockingStub();

  /** @return Future (asynchronous) stub that allows direct calls to service. */
  WorkflowServiceGrpc.WorkflowServiceFutureStub futureStub();

  void shutdown();

  void shutdownNow();

  boolean isShutdown();

  boolean isTerminated();

  boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException;
}
