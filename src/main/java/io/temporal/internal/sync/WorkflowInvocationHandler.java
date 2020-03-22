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

import static io.temporal.internal.common.InternalUtils.getWorkflowMethod;
import static io.temporal.internal.common.InternalUtils.getWorkflowType;

import com.google.common.base.Defaults;
import io.temporal.client.DuplicateWorkflowException;
import io.temporal.client.WorkflowClientInterceptor;
import io.temporal.client.WorkflowOptions;
import io.temporal.client.WorkflowStub;
import io.temporal.common.CronSchedule;
import io.temporal.common.MethodRetry;
import io.temporal.common.converter.DataConverter;
import io.temporal.internal.common.InternalUtils;
import io.temporal.internal.external.GenericWorkflowClientExternal;
import io.temporal.proto.common.WorkflowExecution;
import io.temporal.proto.enums.WorkflowIdReusePolicy;
import io.temporal.workflow.QueryMethod;
import io.temporal.workflow.SignalMethod;
import io.temporal.workflow.WorkflowMethod;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.Optional;

/**
 * Dynamic implementation of a strongly typed workflow interface that can be used to start, signal
 * and query workflows from external processes.
 */
class WorkflowInvocationHandler implements InvocationHandler {

  public enum InvocationType {
    SYNC,
    START,
    EXECUTE,
    SIGNAL_WITH_START,
  }

  interface SpecificInvocationHandler {
    InvocationType getInvocationType();

    void invoke(WorkflowStub untyped, Method method, Object[] args) throws Throwable;

    <R> R getResult(Class<R> resultClass);
  }

  private static final ThreadLocal<SpecificInvocationHandler> invocationContext =
      new ThreadLocal<>();

  /** Must call {@link #closeAsyncInvocation()} if this one was called. */
  static void initAsyncInvocation(InvocationType type) {
    initAsyncInvocation(type, null);
  }

  /** Must call {@link #closeAsyncInvocation()} if this one was called. */
  static <T> void initAsyncInvocation(InvocationType type, T value) {
    if (invocationContext.get() != null) {
      throw new IllegalStateException("already in start invocation");
    }
    if (type == InvocationType.START) {
      invocationContext.set(new StartWorkflowInvocationHandler());
    } else if (type == InvocationType.EXECUTE) {
      invocationContext.set(new ExecuteWorkflowInvocationHandler());
    } else if (type == InvocationType.SIGNAL_WITH_START) {
      @SuppressWarnings("unchecked")
      SignalWithStartBatchRequest batch = (SignalWithStartBatchRequest) value;
      invocationContext.set(new SignalWithStartWorkflowInvocationHandler(batch));
    } else {
      throw new IllegalArgumentException("Unexpected InvocationType: " + type);
    }
  }

  @SuppressWarnings("unchecked")
  static <R> R getAsyncInvocationResult(Class<R> resultClass) {
    SpecificInvocationHandler invocation = invocationContext.get();
    if (invocation == null) {
      throw new IllegalStateException("initAsyncInvocation wasn't called");
    }
    return invocation.getResult(resultClass);
  }

  /** Closes async invocation created through {@link #initAsyncInvocation(InvocationType)} */
  static void closeAsyncInvocation() {
    invocationContext.remove();
  }

  private final WorkflowStub untyped;

  WorkflowInvocationHandler(
      Class<?> workflowInterface,
      GenericWorkflowClientExternal genericClient,
      WorkflowExecution execution,
      DataConverter dataConverter,
      WorkflowClientInterceptor[] interceptors) {
    Method workflowMethod = getWorkflowMethod(workflowInterface);
    WorkflowMethod annotation = workflowMethod.getAnnotation(WorkflowMethod.class);
    String workflowType = getWorkflowType(workflowMethod, annotation);

    WorkflowStub stub =
        new WorkflowStubImpl(genericClient, dataConverter, Optional.of(workflowType), execution);
    for (WorkflowClientInterceptor i : interceptors) {
      stub = i.newUntypedWorkflowStub(execution, Optional.of(workflowType), stub);
    }
    this.untyped = stub;
  }

  WorkflowInvocationHandler(
      Class<?> workflowInterface,
      GenericWorkflowClientExternal genericClient,
      WorkflowOptions options,
      DataConverter dataConverter,
      WorkflowClientInterceptor[] interceptors) {
    Method workflowMethod = getWorkflowMethod(workflowInterface);
    MethodRetry methodRetry = workflowMethod.getAnnotation(MethodRetry.class);
    CronSchedule cronSchedule = workflowMethod.getAnnotation(CronSchedule.class);
    WorkflowMethod annotation = workflowMethod.getAnnotation(WorkflowMethod.class);
    String workflowType = getWorkflowType(workflowMethod, annotation);
    WorkflowOptions mergedOptions =
        WorkflowOptions.merge(annotation, methodRetry, cronSchedule, options);
    WorkflowStub stub =
        new WorkflowStubImpl(genericClient, dataConverter, workflowType, mergedOptions);
    for (WorkflowClientInterceptor i : interceptors) {
      stub = i.newUntypedWorkflowStub(workflowType, mergedOptions, stub);
    }
    this.untyped = stub;
  }

  @Override
  public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
    try {
      if (method.equals(Object.class.getMethod("toString"))) {
        // TODO: workflow info
        return "WorkflowInvocationHandler";
      }
    } catch (NoSuchMethodException e) {
      throw new Error("unexpected", e);
    }
    // Implement StubMarker
    if (method.getName().equals(StubMarker.GET_UNTYPED_STUB_METHOD)) {
      return untyped;
    }
    if (!method.getDeclaringClass().isInterface()) {
      throw new IllegalArgumentException(
          "Interface type is expected: " + method.getDeclaringClass());
    }
    SpecificInvocationHandler handler = invocationContext.get();
    if (handler == null) {
      handler = new SyncWorkflowInvocationHandler();
    }
    handler.invoke(untyped, method, args);
    if (handler.getInvocationType() == InvocationType.SYNC) {
      return handler.getResult(method.getReturnType());
    }
    return Defaults.defaultValue(method.getReturnType());
  }

  private static void startWorkflow(WorkflowStub untyped, Object[] args) {
    Optional<WorkflowOptions> options = untyped.getOptions();
    if (untyped.getExecution() == null
        || (options.isPresent()
            && options.get().getWorkflowIdReusePolicy()
                == WorkflowIdReusePolicy.WorkflowIdReusePolicyAllowDuplicate)) {
      try {
        untyped.start(args);
      } catch (DuplicateWorkflowException e) {
        // We do allow duplicated calls if policy is not AllowDuplicate. Semantic is to wait for
        // result.
        if (options.isPresent()
            && options.get().getWorkflowIdReusePolicy()
                == WorkflowIdReusePolicy.WorkflowIdReusePolicyAllowDuplicate) {
          throw e;
        }
      }
    }
  }

  static void checkAnnotations(
      Method method,
      WorkflowMethod workflowMethod,
      QueryMethod queryMethod,
      SignalMethod signalMethod) {
    int count =
        (workflowMethod == null ? 0 : 1)
            + (queryMethod == null ? 0 : 1)
            + (signalMethod == null ? 0 : 1);
    if (count > 1) {
      throw new IllegalArgumentException(
          method
              + " must contain at most one annotation "
              + "from @WorkflowMethod, @QueryMethod or @SignalMethod");
    }
  }

  private static class StartWorkflowInvocationHandler implements SpecificInvocationHandler {

    private Object result;

    @Override
    public InvocationType getInvocationType() {
      return InvocationType.START;
    }

    @Override
    public void invoke(WorkflowStub untyped, Method method, Object[] args) {
      WorkflowMethod workflowMethod = method.getAnnotation(WorkflowMethod.class);
      if (workflowMethod == null) {
        throw new IllegalArgumentException(
            "WorkflowClient.start can be called only on a method annotated with @WorkflowMethod");
      }
      result = untyped.start(args);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <R> R getResult(Class<R> resultClass) {
      return (R) result;
    }
  }

  private static class SyncWorkflowInvocationHandler implements SpecificInvocationHandler {

    private Object result;

    @Override
    public InvocationType getInvocationType() {
      return InvocationType.SYNC;
    }

    @Override
    public void invoke(WorkflowStub untyped, Method method, Object[] args) {
      WorkflowMethod workflowMethod = method.getAnnotation(WorkflowMethod.class);
      QueryMethod queryMethod = method.getAnnotation(QueryMethod.class);
      SignalMethod signalMethod = method.getAnnotation(SignalMethod.class);
      checkAnnotations(method, workflowMethod, queryMethod, signalMethod);
      if (workflowMethod != null) {
        result = startWorkflow(untyped, method, args);
      } else if (queryMethod != null) {
        result = queryWorkflow(untyped, method, queryMethod, args);
      } else if (signalMethod != null) {
        signalWorkflow(untyped, method, signalMethod, args);
        result = null;
      } else {
        throw new IllegalArgumentException(
            method + " is not annotated with @WorkflowMethod or @QueryMethod");
      }
    }

    @Override
    @SuppressWarnings("unchecked")
    public <R> R getResult(Class<R> resultClass) {
      return (R) result;
    }

    private void signalWorkflow(
        WorkflowStub untyped, Method method, SignalMethod signalMethod, Object[] args) {
      if (method.getReturnType() != Void.TYPE) {
        throw new IllegalArgumentException("Signal method must have void return type: " + method);
      }

      String signalName = nameFromMethodAndAnnotation(method, signalMethod.name());
      untyped.signal(signalName, args);
    }

    private Object queryWorkflow(
        WorkflowStub untyped, Method method, QueryMethod queryMethod, Object[] args) {
      if (method.getReturnType() == Void.TYPE) {
        throw new IllegalArgumentException("Query method cannot have void return type: " + method);
      }
      String queryType = nameFromMethodAndAnnotation(method, queryMethod.name());

      return untyped.query(queryType, method.getReturnType(), method.getGenericReturnType(), args);
    }

    @SuppressWarnings("FutureReturnValueIgnored")
    private Object startWorkflow(WorkflowStub untyped, Method method, Object[] args) {
      WorkflowInvocationHandler.startWorkflow(untyped, args);
      return untyped.getResult(method.getReturnType(), method.getGenericReturnType());
    }
  }

  private static String nameFromMethodAndAnnotation(Method method, String name) {
    String signalName = name;
    if (signalName.isEmpty()) {
      signalName = InternalUtils.getSimpleName(method);
    }
    return signalName;
  }

  private static class ExecuteWorkflowInvocationHandler implements SpecificInvocationHandler {

    private Object result;

    @Override
    public InvocationType getInvocationType() {
      return InvocationType.EXECUTE;
    }

    @Override
    public void invoke(WorkflowStub untyped, Method method, Object[] args) {
      WorkflowMethod workflowMethod = method.getAnnotation(WorkflowMethod.class);
      if (workflowMethod == null) {
        throw new IllegalArgumentException(
            "WorkflowClient.execute can be called only on a method annotated with @WorkflowMethod");
      }
      WorkflowInvocationHandler.startWorkflow(untyped, args);
      result = untyped.getResultAsync(method.getReturnType(), method.getGenericReturnType());
    }

    @Override
    @SuppressWarnings("unchecked")
    public <R> R getResult(Class<R> resultClass) {
      return (R) result;
    }
  }

  private static class SignalWithStartWorkflowInvocationHandler
      implements SpecificInvocationHandler {

    private final SignalWithStartBatchRequest batch;

    public SignalWithStartWorkflowInvocationHandler(SignalWithStartBatchRequest batch) {
      this.batch = batch;
    }

    @Override
    public InvocationType getInvocationType() {
      return InvocationType.SIGNAL_WITH_START;
    }

    @Override
    public void invoke(WorkflowStub untyped, Method method, Object[] args) throws Throwable {
      QueryMethod queryMethod = method.getAnnotation(QueryMethod.class);
      SignalMethod signalMethod = method.getAnnotation(SignalMethod.class);
      WorkflowMethod workflowMethod = method.getAnnotation(WorkflowMethod.class);
      checkAnnotations(method, workflowMethod, queryMethod, signalMethod);
      if (queryMethod != null) {
        throw new IllegalArgumentException(
            "SignalWithStart batch doesn't accept methods annotated with @QueryMethod");
      }
      if (workflowMethod != null) {
        batch.start(untyped, args);
      } else if (signalMethod != null) {
        String signalName = nameFromMethodAndAnnotation(method, signalMethod.name());
        batch.signal(untyped, signalName, args);
      } else {
        throw new IllegalArgumentException(
            method + " is not annotated with @WorkflowMethod or @SignalMethod");
      }
    }

    @Override
    public <R> R getResult(Class<R> resultClass) {
      throw new IllegalStateException("No result is expected");
    }
  }
}
