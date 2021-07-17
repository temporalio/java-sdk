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

import static io.temporal.internal.sync.WorkflowInternal.unwrap;
import static io.temporal.serviceclient.CheckedExceptionWrapper.wrap;

import com.google.common.base.Preconditions;
import io.temporal.api.common.v1.Payloads;
import io.temporal.api.common.v1.WorkflowType;
import io.temporal.api.failure.v1.Failure;
import io.temporal.common.context.ContextPropagator;
import io.temporal.common.converter.DataConverter;
import io.temporal.common.interceptors.Header;
import io.temporal.common.interceptors.WorkerInterceptor;
import io.temporal.common.interceptors.WorkflowInboundCallsInterceptor;
import io.temporal.common.interceptors.WorkflowOutboundCallsInterceptor;
import io.temporal.common.metadata.POJOWorkflowImplMetadata;
import io.temporal.common.metadata.POJOWorkflowInterfaceMetadata;
import io.temporal.common.metadata.POJOWorkflowMethodMetadata;
import io.temporal.failure.CanceledFailure;
import io.temporal.failure.FailureConverter;
import io.temporal.failure.TemporalFailure;
import io.temporal.internal.replay.ReplayWorkflow;
import io.temporal.internal.replay.ReplayWorkflowFactory;
import io.temporal.internal.replay.WorkflowExecutorCache;
import io.temporal.internal.worker.SingleWorkerOptions;
import io.temporal.internal.worker.WorkflowExecutionException;
import io.temporal.worker.WorkflowImplementationOptions;
import io.temporal.workflow.DynamicWorkflow;
import io.temporal.workflow.Functions;
import io.temporal.workflow.Functions.Func;
import io.temporal.workflow.Workflow;
import io.temporal.workflow.WorkflowInfo;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class POJOWorkflowImplementationFactory implements ReplayWorkflowFactory {

  private static final Logger log =
      LoggerFactory.getLogger(POJOWorkflowImplementationFactory.class);
  private final WorkerInterceptor[] workerInterceptors;

  private DataConverter dataConverter;
  private final List<ContextPropagator> contextPropagators;
  private final long defaultDeadlockDetectionTimeout;

  /** Key: workflow type name, Value: function that creates SyncWorkflowDefinition instance. */
  private final Map<String, Functions.Func<SyncWorkflowDefinition>> workflowDefinitions =
      Collections.synchronizedMap(new HashMap<>());

  private final Map<String, WorkflowImplementationOptions> implementationOptions =
      Collections.synchronizedMap(new HashMap<>());

  private final Map<Class<?>, Functions.Func<?>> workflowImplementationFactories =
      Collections.synchronizedMap(new HashMap<>());

  /** If present then it is called for any unknown workflow type. */
  private Functions.Func<? extends DynamicWorkflow> dynamicWorkflowImplementationFactory;

  private final ExecutorService threadPool;
  private final WorkflowExecutorCache cache;

  POJOWorkflowImplementationFactory(
      SingleWorkerOptions singleWorkerOptions,
      ExecutorService threadPool,
      WorkerInterceptor[] workerInterceptors,
      WorkflowExecutorCache cache) {
    Objects.requireNonNull(singleWorkerOptions);
    this.dataConverter = singleWorkerOptions.getDataConverter();
    this.threadPool = Objects.requireNonNull(threadPool);
    this.workerInterceptors = Objects.requireNonNull(workerInterceptors);
    this.cache = cache;
    this.contextPropagators = singleWorkerOptions.getContextPropagators();
    this.defaultDeadlockDetectionTimeout = singleWorkerOptions.getDefaultDeadlockDetectionTimeout();
  }

  void registerWorkflowImplementationTypes(
      WorkflowImplementationOptions options, Class<?>[] workflowImplementationTypes) {
    for (Class<?> type : workflowImplementationTypes) {
      registerWorkflowImplementationType(options, type);
    }
  }

  <R> void addWorkflowImplementationFactory(Class<R> clazz, Functions.Func<R> factory) {
    @SuppressWarnings("unchecked")
    WorkflowImplementationOptions unitTestingOptions =
        WorkflowImplementationOptions.newBuilder()
            .setFailWorkflowExceptionTypes(Throwable.class)
            .build();
    addWorkflowImplementationFactory(unitTestingOptions, clazz, factory);
  }

  @SuppressWarnings("unchecked")
  <R> void addWorkflowImplementationFactory(
      WorkflowImplementationOptions options, Class<R> clazz, Functions.Func<R> factory) {
    if (DynamicWorkflow.class.isAssignableFrom(clazz)) {
      if (dynamicWorkflowImplementationFactory != null) {
        throw new IllegalStateException(
            "An implementation of DynamicWorkflow or its factory is already registered with the worker");
      }
      dynamicWorkflowImplementationFactory = (Func<? extends DynamicWorkflow>) factory;
      return;
    }
    workflowImplementationFactories.put(clazz, factory);
    POJOWorkflowInterfaceMetadata workflowMetadata =
        POJOWorkflowInterfaceMetadata.newInstance(clazz);
    if (!workflowMetadata.getWorkflowMethod().isPresent()) {
      throw new IllegalArgumentException(
          "Workflow interface doesn't contain a method annotated with @WorkflowMethod: " + clazz);
    }
    List<POJOWorkflowMethodMetadata> methodsMetadata = workflowMetadata.getMethodsMetadata();
    for (POJOWorkflowMethodMetadata methodMetadata : methodsMetadata) {
      switch (methodMetadata.getType()) {
        case WORKFLOW:
          String workflowName = methodMetadata.getName();
          if (workflowDefinitions.containsKey(workflowName)) {
            throw new IllegalStateException(
                workflowName + " workflow type is already registered with the worker");
          }
          workflowDefinitions.put(
              workflowName,
              () ->
                  new POJOWorkflowImplementation(
                      clazz, methodMetadata.getName(), methodMetadata.getWorkflowMethod()));
          implementationOptions.put(workflowName, options);
          break;
        case SIGNAL:
          // Signals are registered through Workflow.registerListener
          break;
      }
    }
  }

  private <T> void registerWorkflowImplementationType(
      WorkflowImplementationOptions options, Class<T> workflowImplementationClass) {
    if (DynamicWorkflow.class.isAssignableFrom(workflowImplementationClass)) {
      addWorkflowImplementationFactory(
          options,
          workflowImplementationClass,
          () -> {
            try {
              return workflowImplementationClass.getDeclaredConstructor().newInstance();
            } catch (NoSuchMethodException
                | InstantiationException
                | IllegalAccessException
                | InvocationTargetException e) {
              // Error to fail workflow task as this can be fixed by a new deployment.
              throw new Error(
                  "Failure instantiating workflow implementation class "
                      + workflowImplementationClass.getName(),
                  e);
            }
          });
      return;
    }
    boolean hasWorkflowMethod = false;
    POJOWorkflowImplMetadata workflowMetadata =
        POJOWorkflowImplMetadata.newInstance(workflowImplementationClass);
    for (POJOWorkflowInterfaceMetadata workflowInterface :
        workflowMetadata.getWorkflowInterfaces()) {
      Optional<POJOWorkflowMethodMetadata> workflowMethod = workflowInterface.getWorkflowMethod();
      if (!workflowMethod.isPresent()) {
        continue;
      }
      POJOWorkflowMethodMetadata methodMetadata = workflowMethod.get();
      String workflowName = methodMetadata.getName();
      Method method = methodMetadata.getWorkflowMethod();
      Functions.Func<SyncWorkflowDefinition> factory =
          () -> new POJOWorkflowImplementation(workflowImplementationClass, workflowName, method);

      if (workflowDefinitions.containsKey(workflowName)) {
        throw new IllegalStateException(
            workflowName + " workflow type is already registered with the worker");
      }
      workflowDefinitions.put(workflowName, factory);
      implementationOptions.put(workflowName, options);
      hasWorkflowMethod = true;
    }
    if (!hasWorkflowMethod) {
      throw new IllegalArgumentException(
          "Workflow implementation doesn't implement any interface "
              + "with a workflow method annotated with @WorkflowMethod: "
              + workflowImplementationClass);
    }
  }

  private SyncWorkflowDefinition getWorkflowDefinition(WorkflowType workflowType) {
    Functions.Func<SyncWorkflowDefinition> factory =
        workflowDefinitions.get(workflowType.getName());
    if (factory == null) {
      if (dynamicWorkflowImplementationFactory != null) {
        return new DynamicSyncWorkflowDefinition(
            dynamicWorkflowImplementationFactory, workerInterceptors, dataConverter);
      }
      // throw Error to abort the workflow task task, not fail the workflow
      throw new Error(
          "Unknown workflow type \""
              + workflowType.getName()
              + "\". Known types are "
              + workflowDefinitions.keySet());
    }
    try {
      return factory.apply();
    } catch (Exception e) {
      throw new Error(e);
    }
  }

  public void setDataConverter(DataConverter dataConverter) {
    this.dataConverter = dataConverter;
  }

  @Override
  public ReplayWorkflow getWorkflow(WorkflowType workflowType) {
    SyncWorkflowDefinition workflow = getWorkflowDefinition(workflowType);
    WorkflowImplementationOptions options = implementationOptions.get(workflowType.getName());
    return new SyncWorkflow(
        workflow,
        options,
        dataConverter,
        threadPool,
        cache,
        contextPropagators,
        defaultDeadlockDetectionTimeout);
  }

  @Override
  public boolean isAnyTypeSupported() {
    return !workflowDefinitions.isEmpty() || dynamicWorkflowImplementationFactory != null;
  }

  private class POJOWorkflowImplementation implements SyncWorkflowDefinition {

    private final String workflowName;
    private final Method workflowMethod;
    private final Class<?> workflowImplementationClass;
    private WorkflowInboundCallsInterceptor workflowInvoker;

    public POJOWorkflowImplementation(
        Class<?> workflowImplementationClass, String workflowName, Method workflowMethod) {
      this.workflowName = workflowName;
      this.workflowMethod = workflowMethod;
      this.workflowImplementationClass = workflowImplementationClass;
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
    public Optional<Payloads> execute(Header header, Optional<Payloads> input)
        throws CanceledFailure, WorkflowExecutionException {
      Object[] args =
          DataConverter.arrayFromPayloads(
              dataConverter,
              input,
              workflowMethod.getParameterTypes(),
              workflowMethod.getGenericParameterTypes());
      Preconditions.checkNotNull(workflowInvoker, "initialize not called");
      WorkflowInboundCallsInterceptor.WorkflowOutput result =
          workflowInvoker.execute(new WorkflowInboundCallsInterceptor.WorkflowInput(header, args));
      if (workflowMethod.getReturnType() == Void.TYPE) {
        return Optional.empty();
      }
      return dataConverter.toPayloads(result.getResult());
    }

    private class RootWorkflowInboundCallsInterceptor
        extends BaseRootWorkflowInboundCallsInterceptor {
      private Object workflow;

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
        WorkflowInfo info = Workflow.getInfo();
        try {
          Object result = workflowMethod.invoke(workflow, input.getArguments());
          return new WorkflowOutput(result);
        } catch (IllegalAccessException e) {
          throw new Error(mapToWorkflowExecutionException(e, dataConverter));
        } catch (InvocationTargetException e) {
          Throwable target = e.getTargetException();
          if (target instanceof DestroyWorkflowThreadError) {
            throw (DestroyWorkflowThreadError) target;
          }
          Throwable exception = unwrap(target);

          WorkflowImplementationOptions options = implementationOptions.get(info.getWorkflowType());
          Class<? extends Throwable>[] failTypes = options.getFailWorkflowExceptionTypes();
          if (exception instanceof TemporalFailure) {
            logWorkflowExecutionException(info, exception);
            throw mapToWorkflowExecutionException(exception, dataConverter);
          }
          for (Class<? extends Throwable> failType : failTypes) {
            if (failType.isAssignableFrom(exception.getClass())) {
              // fail workflow
              if (log.isErrorEnabled()) {
                boolean cancelRequested =
                    WorkflowInternal.getRootWorkflowContext().getContext().isCancelRequested();
                if (!cancelRequested || !FailureConverter.isCanceledCause(exception)) {
                  logWorkflowExecutionException(info, exception);
                }
              }
              throw mapToWorkflowExecutionException(exception, dataConverter);
            }
          }
          throw wrap(exception);
        }
      }

      private void logWorkflowExecutionException(WorkflowInfo info, Throwable exception) {
        log.error(
            "Workflow execution failure "
                + "WorkflowId="
                + info.getWorkflowId()
                + ", RunId="
                + info.getRunId()
                + ", WorkflowType="
                + info.getWorkflowType(),
            exception);
      }

      protected void newInstance() {
        Func<?> factory = workflowImplementationFactories.get(workflowImplementationClass);
        if (factory != null) {
          workflow = factory.apply();
        } else {
          try {
            workflow = workflowImplementationClass.getDeclaredConstructor().newInstance();
          } catch (NoSuchMethodException
              | InstantiationException
              | IllegalAccessException
              | InvocationTargetException e) {
            // Error to fail workflow task as this can be fixed by a new deployment.
            throw new Error(
                "Failure instantiating workflow implementation class "
                    + workflowImplementationClass.getName(),
                e);
          }
        }
      }
    }
  }

  static WorkflowExecutionException mapToWorkflowExecutionException(
      Throwable exception, DataConverter dataConverter) {
    Throwable e = exception;
    while (e != null) {
      if (e instanceof TemporalFailure) {
        ((TemporalFailure) e).setDataConverter(dataConverter);
      }
      e = e.getCause();
    }
    Failure failure = FailureConverter.exceptionToFailure(exception);
    return new WorkflowExecutionException(failure);
  }

  @Override
  public String toString() {
    return "POJOWorkflowImplementationFactory{"
        + "registeredWorkflowTypes="
        + workflowDefinitions.keySet()
        + '}';
  }
}
