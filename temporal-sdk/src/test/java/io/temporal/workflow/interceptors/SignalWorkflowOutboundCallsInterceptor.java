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

package io.temporal.workflow.interceptors;

import io.temporal.api.common.v1.WorkflowExecution;
import io.temporal.common.interceptors.WorkflowOutboundCallsInterceptor;
import io.temporal.workflow.ChildWorkflowOptions;
import io.temporal.workflow.ContinueAsNewOptions;
import io.temporal.workflow.DynamicQueryHandler;
import io.temporal.workflow.DynamicSignalHandler;
import io.temporal.workflow.Functions;
import io.temporal.workflow.Promise;
import java.lang.reflect.Type;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.Supplier;

public class SignalWorkflowOutboundCallsInterceptor implements WorkflowOutboundCallsInterceptor {

  private final Function<Object[], Object[]> overrideArgs;
  private final Function<String, String> overrideSignalName;
  private final WorkflowOutboundCallsInterceptor next;

  public SignalWorkflowOutboundCallsInterceptor(
      Function<Object[], Object[]> overrideArgs,
      Function<String, String> overrideSignalName,
      WorkflowOutboundCallsInterceptor next) {
    this.overrideArgs = overrideArgs;
    this.overrideSignalName = overrideSignalName;
    this.next = Objects.requireNonNull(next);
  }

  @Override
  public <T> ActivityOutput<T> executeActivity(ActivityInput<T> input) {
    return next.executeActivity(input);
  }

  @Override
  public <R> LocalActivityOutput<R> executeLocalActivity(LocalActivityInput<R> input) {
    return next.executeLocalActivity(input);
  }

  @Override
  public <R> WorkflowResult<R> executeChildWorkflow(
      String workflowType,
      Class<R> resultClass,
      Type resultType,
      Object[] args,
      ChildWorkflowOptions options) {
    return next.executeChildWorkflow(workflowType, resultClass, resultType, args, options);
  }

  @Override
  public Random newRandom() {
    return next.newRandom();
  }

  @Override
  public Promise<Void> signalExternalWorkflow(
      WorkflowExecution execution, String signalName, Object[] args) {
    if (args != null && args.length > 0) {
      args = new Object[] {"corrupted signal"};
    }
    return next.signalExternalWorkflow(
        execution, overrideSignalName.apply(signalName), overrideArgs.apply(args));
  }

  @Override
  public Promise<Void> cancelWorkflow(WorkflowExecution execution) {
    return next.cancelWorkflow(execution);
  }

  @Override
  public void sleep(Duration duration) {
    next.sleep(duration);
  }

  @Override
  public boolean await(Duration timeout, String reason, Supplier<Boolean> unblockCondition) {
    return next.await(timeout, reason, unblockCondition);
  }

  @Override
  public void await(String reason, Supplier<Boolean> unblockCondition) {
    next.await(reason, unblockCondition);
  }

  @Override
  public Promise<Void> newTimer(Duration duration) {
    return next.newTimer(duration);
  }

  @Override
  public <R> R sideEffect(Class<R> resultClass, Type resultType, Functions.Func<R> func) {
    return next.sideEffect(resultClass, resultType, func);
  }

  @Override
  public <R> R mutableSideEffect(
      String id,
      Class<R> resultClass,
      Type resultType,
      BiPredicate<R, R> updated,
      Functions.Func<R> func) {
    return null;
  }

  @Override
  public int getVersion(String changeId, int minSupported, int maxSupported) {
    return next.getVersion(changeId, minSupported, maxSupported);
  }

  @Override
  public void continueAsNew(
      Optional<String> workflowType, Optional<ContinueAsNewOptions> options, Object[] args) {
    next.continueAsNew(workflowType, options, args);
  }

  @Override
  public void registerQuery(
      String queryType,
      Class<?>[] argTypes,
      Type[] genericArgTypes,
      Functions.Func1<Object[], Object> callback) {
    next.registerQuery(queryType, argTypes, genericArgTypes, callback);
  }

  @Override
  public void registerSignalHandlers(List<SignalRegistrationRequest> requests) {
    next.registerSignalHandlers(requests);
  }

  @Override
  public void registerDynamicSignalHandler(DynamicSignalHandler handler) {
    next.registerDynamicSignalHandler(handler);
  }

  @Override
  public void registerDynamicQueryHandler(DynamicQueryHandler handler) {
    next.registerDynamicQueryHandler(handler);
  }

  @Override
  public UUID randomUUID() {
    return next.randomUUID();
  }

  @Override
  public void upsertSearchAttributes(Map<String, Object> searchAttributes) {
    next.upsertSearchAttributes(searchAttributes);
  }

  @Override
  public Object newThread(Runnable runnable, boolean detached, String name) {
    return next.newThread(runnable, detached, name);
  }

  @Override
  public long currentTimeMillis() {
    return next.currentTimeMillis();
  }
}
