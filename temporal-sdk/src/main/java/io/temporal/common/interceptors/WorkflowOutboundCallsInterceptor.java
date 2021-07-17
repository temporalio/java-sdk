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

import io.temporal.activity.ActivityOptions;
import io.temporal.activity.LocalActivityOptions;
import io.temporal.api.common.v1.WorkflowExecution;
import io.temporal.workflow.*;
import io.temporal.workflow.Functions.Func;
import java.lang.reflect.Type;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;
import java.util.function.BiPredicate;
import java.util.function.Supplier;

/**
 * Can be used to intercept workflow code calls to the Temporal APIs. An instance should be created
 * through {@link WorkerInterceptor#interceptWorkflow(WorkflowInboundCallsInterceptor)}. An
 * interceptor instance must forward all the calls to the next interceptor passed to the
 * interceptExecuteWorkflow call.
 *
 * <p>The calls to the interceptor are executed in the context of a workflow and must follow the
 * same rules all the other workflow code follows.
 */
public interface WorkflowOutboundCallsInterceptor {

  final class ActivityInput<R> {
    private final String activityName;
    private final Class<R> resultClass;
    private final Type resultType;
    private final Object[] args;
    private final ActivityOptions options;
    private final Header header;

    public ActivityInput(
        String activityName,
        Class<R> resultClass,
        Type resultType,
        Object[] args,
        ActivityOptions options,
        Header header) {
      this.activityName = activityName;
      this.resultClass = resultClass;
      this.resultType = resultType;
      this.args = args;
      this.options = options;
      this.header = header;
    }

    public String getActivityName() {
      return activityName;
    }

    public Class<R> getResultClass() {
      return resultClass;
    }

    public Type getResultType() {
      return resultType;
    }

    public Object[] getArgs() {
      return args;
    }

    public ActivityOptions getOptions() {
      return options;
    }

    public Header getHeader() {
      return header;
    }
  }

  final class ActivityOutput<R> {
    private final Promise<R> result;

    public ActivityOutput(Promise<R> result) {
      this.result = result;
    }

    public Promise<R> getResult() {
      return result;
    }
  }

  final class LocalActivityInput<R> {
    private final String activityName;
    private final Class<R> resultClass;
    private final Type resultType;
    private final Object[] args;
    private final LocalActivityOptions options;
    private final Header header;

    public LocalActivityInput(
        String activityName,
        Class<R> resultClass,
        Type resultType,
        Object[] args,
        LocalActivityOptions options,
        Header header) {
      this.activityName = activityName;
      this.resultClass = resultClass;
      this.resultType = resultType;
      this.args = args;
      this.options = options;
      this.header = header;
    }

    public String getActivityName() {
      return activityName;
    }

    public Class<R> getResultClass() {
      return resultClass;
    }

    public Type getResultType() {
      return resultType;
    }

    public Object[] getArgs() {
      return args;
    }

    public LocalActivityOptions getOptions() {
      return options;
    }

    public Header getHeader() {
      return header;
    }
  }

  final class LocalActivityOutput<R> {
    private final Promise<R> result;

    public LocalActivityOutput(Promise<R> result) {
      this.result = result;
    }

    public Promise<R> getResult() {
      return result;
    }
  }

  final class ChildWorkflowInput<R> {
    private final String workflowId;
    private final String workflowType;
    private final Class<R> resultClass;
    private final Type resultType;
    private final Object[] args;
    private final ChildWorkflowOptions options;
    private final Header header;

    public ChildWorkflowInput(
        String workflowId,
        String workflowType,
        Class<R> resultClass,
        Type resultType,
        Object[] args,
        ChildWorkflowOptions options,
        Header header) {
      this.workflowId = workflowId;
      this.workflowType = workflowType;
      this.resultClass = resultClass;
      this.resultType = resultType;
      this.args = args;
      this.options = options;
      this.header = header;
    }

    public String getWorkflowId() {
      return workflowId;
    }

    public String getWorkflowType() {
      return workflowType;
    }

    public Class<R> getResultClass() {
      return resultClass;
    }

    public Type getResultType() {
      return resultType;
    }

    public Object[] getArgs() {
      return args;
    }

    public ChildWorkflowOptions getOptions() {
      return options;
    }

    public Header getHeader() {
      return header;
    }
  }

  final class ChildWorkflowOutput<R> {

    private final Promise<R> result;
    private final Promise<WorkflowExecution> workflowExecution;

    public ChildWorkflowOutput(Promise<R> result, Promise<WorkflowExecution> workflowExecution) {
      this.result = result;
      this.workflowExecution = workflowExecution;
    }

    public Promise<R> getResult() {
      return result;
    }

    public Promise<WorkflowExecution> getWorkflowExecution() {
      return workflowExecution;
    }
  }

  final class SignalExternalInput {
    private final WorkflowExecution execution;
    private final String signalName;
    private final Object[] args;

    public SignalExternalInput(WorkflowExecution execution, String signalName, Object[] args) {
      this.execution = execution;
      this.signalName = signalName;
      this.args = args;
    }

    public WorkflowExecution getExecution() {
      return execution;
    }

    public String getSignalName() {
      return signalName;
    }

    public Object[] getArgs() {
      return args;
    }
  }

  final class SignalExternalOutput {
    private final Promise<Void> result;

    public SignalExternalOutput(Promise<Void> result) {
      this.result = result;
    }

    public Promise<Void> getResult() {
      return result;
    }
  }

  final class CancelWorkflowInput {
    private final WorkflowExecution execution;

    public CancelWorkflowInput(WorkflowExecution execution) {
      this.execution = execution;
    }

    public WorkflowExecution getExecution() {
      return execution;
    }
  }

  final class CancelWorkflowOutput {
    private final Promise<Void> result;

    public CancelWorkflowOutput(Promise<Void> result) {
      this.result = result;
    }

    public Promise<Void> getResult() {
      return result;
    }
  }

  final class ContinueAsNewInput {
    private final Optional<String> workflowType;
    private final Optional<ContinueAsNewOptions> options;
    private final Object[] args;
    private final Header header;

    public ContinueAsNewInput(
        Optional<String> workflowType,
        Optional<ContinueAsNewOptions> options,
        Object[] args,
        Header header) {
      this.workflowType = workflowType;
      this.options = options;
      this.args = args;
      this.header = header;
    }

    public Optional<String> getWorkflowType() {
      return workflowType;
    }

    public Optional<ContinueAsNewOptions> getOptions() {
      return options;
    }

    public Object[] getArgs() {
      return args;
    }

    public Header getHeader() {
      return header;
    }
  }

  final class SignalRegistrationRequest {
    private final String signalType;
    private final Class<?>[] argTypes;
    private final Type[] genericArgTypes;
    private final Functions.Proc1<Object[]> callback;

    public SignalRegistrationRequest(
        String signalType,
        Class<?>[] argTypes,
        Type[] genericArgTypes,
        Functions.Proc1<Object[]> callback) {
      this.signalType = signalType;
      this.argTypes = argTypes;
      this.genericArgTypes = genericArgTypes;
      this.callback = callback;
    }

    public String getSignalType() {
      return signalType;
    }

    public Class<?>[] getArgTypes() {
      return argTypes;
    }

    public Type[] getGenericArgTypes() {
      return genericArgTypes;
    }

    public Functions.Proc1<Object[]> getCallback() {
      return callback;
    }
  }

  final class RegisterSignalHandlersInput {
    private final List<SignalRegistrationRequest> requests;

    public RegisterSignalHandlersInput(List<SignalRegistrationRequest> requests) {
      this.requests = requests;
    }

    public List<SignalRegistrationRequest> getRequests() {
      return requests;
    }
  }

  final class RegisterQueryInput {
    private final String queryType;
    private final Class<?>[] argTypes;
    private final Type[] genericArgTypes;
    private final Functions.Func1<Object[], Object> callback;

    public RegisterQueryInput(
        String queryType,
        Class<?>[] argTypes,
        Type[] genericArgTypes,
        Functions.Func1<Object[], Object> callback) {
      this.queryType = queryType;
      this.argTypes = argTypes;
      this.genericArgTypes = genericArgTypes;
      this.callback = callback;
    }

    public String getQueryType() {
      return queryType;
    }

    public Class<?>[] getArgTypes() {
      return argTypes;
    }

    public Type[] getGenericArgTypes() {
      return genericArgTypes;
    }

    public Functions.Func1<Object[], Object> getCallback() {
      return callback;
    }
  }

  final class RegisterDynamicQueryHandlerInput {
    private final DynamicQueryHandler handler;

    public RegisterDynamicQueryHandlerInput(DynamicQueryHandler handler) {
      this.handler = handler;
    }

    public DynamicQueryHandler getHandler() {
      return handler;
    }
  }

  final class RegisterDynamicSignalHandlerInput {
    private final DynamicSignalHandler handler;

    public RegisterDynamicSignalHandlerInput(DynamicSignalHandler handler) {
      this.handler = handler;
    }

    public DynamicSignalHandler getHandler() {
      return handler;
    }
  }

  <R> ActivityOutput<R> executeActivity(ActivityInput<R> input);

  <R> LocalActivityOutput<R> executeLocalActivity(LocalActivityInput<R> input);

  <R> ChildWorkflowOutput<R> executeChildWorkflow(ChildWorkflowInput<R> input);

  Random newRandom();

  SignalExternalOutput signalExternalWorkflow(SignalExternalInput input);

  CancelWorkflowOutput cancelWorkflow(CancelWorkflowInput input);

  void sleep(Duration duration);

  boolean await(Duration timeout, String reason, Supplier<Boolean> unblockCondition);

  void await(String reason, Supplier<Boolean> unblockCondition);

  Promise<Void> newTimer(Duration duration);

  <R> R sideEffect(Class<R> resultClass, Type resultType, Func<R> func);

  <R> R mutableSideEffect(
      String id, Class<R> resultClass, Type resultType, BiPredicate<R, R> updated, Func<R> func);

  int getVersion(String changeId, int minSupported, int maxSupported);

  void continueAsNew(ContinueAsNewInput input);

  void registerQuery(RegisterQueryInput input);

  void registerSignalHandlers(RegisterSignalHandlersInput input);

  void registerDynamicSignalHandler(RegisterDynamicSignalHandlerInput handler);

  void registerDynamicQueryHandler(RegisterDynamicQueryHandlerInput input);

  UUID randomUUID();

  void upsertSearchAttributes(Map<String, Object> searchAttributes);

  /**
   * Intercepts creation of the workflow child thread.
   *
   * <p>Please note, that "workflow child thread" and "child workflow" are different and independent
   * concepts.
   *
   * @param runnable thread function to run
   * @param detached if this thread is detached from the parent {@link CancellationScope}
   * @param name name of the thread
   * @return created WorkflowThread
   */
  Object newChildThread(Runnable runnable, boolean detached, String name);

  long currentTimeMillis();
}
