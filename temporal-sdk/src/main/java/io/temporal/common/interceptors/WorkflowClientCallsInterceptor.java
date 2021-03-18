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

package io.temporal.common.interceptors;

import io.temporal.api.common.v1.WorkflowExecution;
import io.temporal.api.enums.v1.WorkflowExecutionStatus;
import io.temporal.client.WorkflowOptions;
import io.temporal.common.Experimental;
import java.lang.reflect.Type;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Experimental
public interface WorkflowClientCallsInterceptor {

  WorkflowStartOutput start(WorkflowStartInput input);

  // TODO Spikhalskiy return SignalOutput?
  void signal(WorkflowSignalInput input);

  WorkflowStartOutput signalWithStart(WorkflowStartWithSignalInput input);

  <R> GetResultOutput<R> getResult(GetResultInput<R> input) throws TimeoutException;

  <R> GetResultAsyncOutput<R> getResultAsync(GetResultInput<R> input);

  <R> QueryOutput<R> query(QueryInput<R> input);

  final class WorkflowStartInput {
    private final String workflowId;
    private final String workflowType;
    private final Header header;
    private final Object[] arguments;
    private final WorkflowOptions options;

    public WorkflowStartInput(
        String workflowId,
        String workflowType,
        Header header,
        Object[] arguments,
        WorkflowOptions options) {
      if (workflowId == null) {
        throw new IllegalArgumentException("workflowId should be specified for start call");
      }
      this.workflowId = workflowId;
      if (workflowType == null) {
        throw new IllegalArgumentException("workflowType should be specified for start call");
      }
      this.workflowType = workflowType;
      this.header = header;
      this.arguments = arguments;
      if (options == null) {
        throw new IllegalArgumentException(
            "options should be specified and not be null for start call");
      }
      this.options = options;
    }

    public String getWorkflowId() {
      return workflowId;
    }

    public String getWorkflowType() {
      return workflowType;
    }

    public Header getHeader() {
      return header;
    }

    public Object[] getArguments() {
      return arguments;
    }

    public WorkflowOptions getOptions() {
      return options;
    }
  }

  final class WorkflowSignalInput {
    private final String workflowId;
    private final String signalName;
    private final Object[] arguments;

    public WorkflowSignalInput(String workflowId, String signalName, Object[] signalArguments) {
      if (workflowId == null) {
        throw new IllegalArgumentException("workflowId should be specified for signal call");
      }
      this.workflowId = workflowId;
      if (signalName == null) {
        throw new IllegalArgumentException("signalName should be specified for signal call");
      }
      this.signalName = signalName;
      this.arguments = signalArguments;
    }

    public String getWorkflowId() {
      return workflowId;
    }

    public String getSignalName() {
      return signalName;
    }

    public Object[] getArguments() {
      return arguments;
    }
  }

  final class WorkflowStartWithSignalInput {
    private final WorkflowStartInput workflowStartInput;
    // TODO Spikhalskiy I'm not sure about this structure.
    // SignalWithStartWorkflowExecutionParameters is
    // StartWorkflowExecutionRequest + signalName + signalInput,
    // not StartWorkflowExecutionRequest + SignalWorkflowExecutionRequest
    private final WorkflowSignalInput workflowSignalInput;

    public WorkflowStartWithSignalInput(
        WorkflowStartInput workflowStartInput, WorkflowSignalInput workflowSignalInput) {
      this.workflowStartInput = workflowStartInput;
      this.workflowSignalInput = workflowSignalInput;
    }

    public WorkflowStartInput getWorkflowStartInput() {
      return workflowStartInput;
    }

    public WorkflowSignalInput getWorkflowSignalInput() {
      return workflowSignalInput;
    }
  }

  final class WorkflowStartOutput {
    private final WorkflowExecution workflowExecution;

    public WorkflowStartOutput(WorkflowExecution workflowExecution) {
      this.workflowExecution = workflowExecution;
    }

    public WorkflowExecution getWorkflowExecution() {
      return workflowExecution;
    }
  }

  final class GetResultInput<R> {
    private final WorkflowExecution workflowExecution;
    private final Optional<String> workflowType;
    private final long timeout;
    private final TimeUnit timeoutUnit;
    private final Class<R> resultClass;
    private final Type resultType;

    public GetResultInput(
        WorkflowExecution workflowExecution,
        Optional<String> workflowType,
        long timeout,
        TimeUnit timeoutUnit,
        Class<R> resultClass,
        Type resultType) {
      this.workflowExecution = workflowExecution;
      this.workflowType = workflowType;
      this.timeout = timeout;
      this.timeoutUnit = timeoutUnit;
      this.resultClass = resultClass;
      this.resultType = resultType;
    }

    public WorkflowExecution getWorkflowExecution() {
      return workflowExecution;
    }

    public Optional<String> getWorkflowType() {
      return workflowType;
    }

    public long getTimeout() {
      return timeout;
    }

    public TimeUnit getTimeoutUnit() {
      return timeoutUnit;
    }

    public Class<R> getResultClass() {
      return resultClass;
    }

    public Type getResultType() {
      return resultType;
    }
  }

  final class GetResultOutput<R> {
    private final R result;

    public GetResultOutput(R result) {
      this.result = result;
    }

    public R getResult() {
      return result;
    }
  }

  final class GetResultAsyncOutput<R> {
    private final CompletableFuture<R> result;

    public GetResultAsyncOutput(CompletableFuture<R> result) {
      this.result = result;
    }

    public CompletableFuture<R> getResult() {
      return result;
    }
  }

  final class QueryInput<R> {
    private final WorkflowExecution workflowExecution;
    private final String queryType;
    private final Object[] arguments;
    private final Class<R> resultClass;
    private final Type resultType;

    public QueryInput(
        WorkflowExecution workflowExecution,
        String queryType,
        Object[] arguments,
        Class<R> resultClass,
        Type resultType) {
      this.workflowExecution = workflowExecution;
      this.queryType = queryType;
      this.arguments = arguments;
      this.resultClass = resultClass;
      this.resultType = resultType;
    }

    public WorkflowExecution getWorkflowExecution() {
      return workflowExecution;
    }

    public String getQueryType() {
      return queryType;
    }

    public Object[] getArguments() {
      return arguments;
    }

    public Class<R> getResultClass() {
      return resultClass;
    }

    public Type getResultType() {
      return resultType;
    }
  }

  final class QueryOutput<R> {
    private final WorkflowExecutionStatus queryRejectedStatus;
    private final R result;

    /**
     * @param queryRejectedStatus should be null if query is not rejected
     * @param result converted result value
     */
    public QueryOutput(WorkflowExecutionStatus queryRejectedStatus, R result) {
      this.queryRejectedStatus = queryRejectedStatus;
      this.result = result;
    }

    public boolean isQueryRejected() {
      return queryRejectedStatus != null;
    }

    public WorkflowExecutionStatus getQueryRejectedStatus() {
      return queryRejectedStatus;
    }

    public R getResult() {
      return result;
    }
  }
}