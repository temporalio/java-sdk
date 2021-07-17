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

import static io.temporal.internal.common.LambdaUtils.getTarget;

import io.temporal.common.RetryOptions;
import io.temporal.internal.common.LambdaUtils;
import io.temporal.workflow.ActivityStub;
import io.temporal.workflow.ChildWorkflowStub;
import io.temporal.workflow.CompletablePromise;
import io.temporal.workflow.ExternalWorkflowStub;
import io.temporal.workflow.Functions;
import io.temporal.workflow.Promise;
import io.temporal.workflow.Workflow;
import java.lang.invoke.MethodHandleInfo;
import java.lang.invoke.SerializedLambda;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Contains support for asynchronous invocations. The basic idea is that any code is invoked in a
 * separate WorkflowThread. Internally it maps to a task executed by a thread pool, so there is not
 * much overhead doing it if operation doesn't block for a long time tying a physical thread. Async
 * allows to have asynchronous implementation of synchronous interfaces. If synchronous interface is
 * invoked using {@link #execute(boolean, Functions.Func)} then {@link #isAsync()} going to return
 * true and implementation can take non blocking path and return {@link Promise} as a result using
 * {@link #setAsyncResult(Promise)}. Then it can return any value from the sync method as it is
 * going to be ignored.
 */
public final class AsyncInternal {

  public interface AsyncMarker {}

  private static final ThreadLocal<AtomicReference<Promise<?>>> asyncResult = new ThreadLocal<>();

  /**
   * Invokes zero argument function asynchronously.
   *
   * @param function Function to execute asynchronously
   * @return promise that contains function result or failure
   */
  public static <R> Promise<R> function(Functions.Func<R> function) {
    return execute(isAsync(function), () -> function.apply());
  }

  /**
   * Invokes one argument function asynchronously.
   *
   * @param function Function to execute asynchronously
   * @param arg1 first function argument
   * @return promise that contains function result or failure
   */
  public static <A1, R> Promise<R> function(Functions.Func1<A1, R> function, A1 arg1) {
    return execute(isAsync(function), () -> function.apply(arg1));
  }

  /**
   * Invokes two argument function asynchronously.
   *
   * @param function Function to execute asynchronously
   * @param arg1 first function argument
   * @param arg2 second function argument
   * @return promise that contains function result or failure
   */
  public static <A1, A2, R> Promise<R> function(
      Functions.Func2<A1, A2, R> function, A1 arg1, A2 arg2) {
    return execute(isAsync(function), () -> function.apply(arg1, arg2));
  }

  /**
   * Invokes three argument function asynchronously.
   *
   * @param function Function to execute asynchronously
   * @param arg1 first function argument
   * @param arg2 second function argument
   * @param arg3 third function argument
   * @return promise that contains function result or failure
   */
  public static <A1, A2, A3, R> Promise<R> function(
      Functions.Func3<A1, A2, A3, R> function, A1 arg1, A2 arg2, A3 arg3) {
    return execute(isAsync(function), () -> function.apply(arg1, arg2, arg3));
  }

  /**
   * Invokes four argument function asynchronously.
   *
   * @param function Function to execute asynchronously
   * @param arg1 first function argument
   * @param arg2 second function argument
   * @param arg3 third function argument
   * @param arg4 forth function argument
   * @return promise that contains function result or failure
   */
  public static <A1, A2, A3, A4, R> Promise<R> function(
      Functions.Func4<A1, A2, A3, A4, R> function, A1 arg1, A2 arg2, A3 arg3, A4 arg4) {
    return execute(isAsync(function), () -> function.apply(arg1, arg2, arg3, arg4));
  }

  /**
   * Invokes five argument function asynchronously.
   *
   * @param function Function to execute asynchronously
   * @param arg1 first function argument
   * @param arg2 second function argument
   * @param arg3 third function argument
   * @param arg4 forth function argument
   * @param arg5 fifth function argument
   * @return promise that contains function result or failure
   */
  public static <A1, A2, A3, A4, A5, R> Promise<R> function(
      Functions.Func5<A1, A2, A3, A4, A5, R> function,
      A1 arg1,
      A2 arg2,
      A3 arg3,
      A4 arg4,
      A5 arg5) {
    return execute(isAsync(function), () -> function.apply(arg1, arg2, arg3, arg4, arg5));
  }

  /**
   * Invokes six argument function asynchronously.
   *
   * @param function Function to execute asynchronously
   * @param arg1 first function argument
   * @param arg2 second function argument
   * @param arg3 third function argument
   * @param arg4 forth function argument
   * @param arg5 fifth function argument
   * @param arg6 sixth function argument
   * @return promise that contains function result or failure
   */
  public static <A1, A2, A3, A4, A5, A6, R> Promise<R> function(
      Functions.Func6<A1, A2, A3, A4, A5, A6, R> function,
      A1 arg1,
      A2 arg2,
      A3 arg3,
      A4 arg4,
      A5 arg5,
      A6 arg6) {
    return execute(isAsync(function), () -> function.apply(arg1, arg2, arg3, arg4, arg5, arg6));
  }

  /**
   * Invokes zero argument procedure asynchronously.
   *
   * @param procedure Procedure to execute asynchronously
   * @return promise that contains procedure result or failure
   */
  public static Promise<Void> procedure(Functions.Proc procedure) {
    return execute(
        isAsync(procedure),
        () -> {
          procedure.apply();
          return null;
        });
  }

  private static Promise<Void> procedure(boolean async, Functions.Proc procedure) {
    return execute(
        async,
        () -> {
          procedure.apply();
          return null;
        });
  }

  /**
   * Invokes one argument procedure asynchronously.
   *
   * @param procedure Procedure to execute asynchronously
   * @param arg1 first procedure argument
   * @return promise that contains procedure result or failure
   */
  public static <A1> Promise<Void> procedure(Functions.Proc1<A1> procedure, A1 arg1) {
    return procedure(isAsync(procedure), () -> procedure.apply(arg1));
  }

  /**
   * Invokes two argument procedure asynchronously.
   *
   * @param procedure Procedure to execute asynchronously
   * @param arg1 first procedure argument
   * @param arg2 second procedure argument
   * @return promise that contains procedure result or failure
   */
  public static <A1, A2> Promise<Void> procedure(
      Functions.Proc2<A1, A2> procedure, A1 arg1, A2 arg2) {
    return procedure(isAsync(procedure), () -> procedure.apply(arg1, arg2));
  }

  /**
   * Invokes three argument procedure asynchronously.
   *
   * @param procedure Procedure to execute asynchronously
   * @param arg1 first procedure argument
   * @param arg2 second procedure argument
   * @param arg3 third procedure argument
   * @return promise that contains procedure result or failure
   */
  public static <A1, A2, A3> Promise<Void> procedure(
      Functions.Proc3<A1, A2, A3> procedure, A1 arg1, A2 arg2, A3 arg3) {
    return procedure(isAsync(procedure), () -> procedure.apply(arg1, arg2, arg3));
  }

  /**
   * Invokes four argument procedure asynchronously.
   *
   * @param procedure Procedure to execute asynchronously
   * @param arg1 first procedure argument
   * @param arg2 second procedure argument
   * @param arg3 third procedure argument
   * @param arg4 forth procedure argument
   * @return promise that contains procedure result or failure
   */
  public static <A1, A2, A3, A4> Promise<Void> procedure(
      Functions.Proc4<A1, A2, A3, A4> procedure, A1 arg1, A2 arg2, A3 arg3, A4 arg4) {
    return procedure(isAsync(procedure), () -> procedure.apply(arg1, arg2, arg3, arg4));
  }

  /**
   * Invokes five argument procedure asynchronously.
   *
   * @param procedure Procedure to execute asynchronously
   * @param arg1 first procedure argument
   * @param arg2 second procedure argument
   * @param arg3 third procedure argument
   * @param arg4 forth procedure argument
   * @param arg5 fifth procedure argument
   * @return promise that contains procedure result or failure
   */
  public static <A1, A2, A3, A4, A5> Promise<Void> procedure(
      Functions.Proc5<A1, A2, A3, A4, A5> procedure, A1 arg1, A2 arg2, A3 arg3, A4 arg4, A5 arg5) {
    return procedure(isAsync(procedure), () -> procedure.apply(arg1, arg2, arg3, arg4, arg5));
  }

  /**
   * Invokes six argument procedure asynchronously.
   *
   * @param procedure Procedure to execute asynchronously
   * @param arg1 first procedure argument
   * @param arg2 second procedure argument
   * @param arg3 third procedure argument
   * @param arg4 forth procedure argument
   * @param arg5 fifth procedure argument
   * @param arg6 sixth procedure argument
   * @return promise that contains procedure result or failure
   */
  public static <A1, A2, A3, A4, A5, A6> Promise<Void> procedure(
      Functions.Proc6<A1, A2, A3, A4, A5, A6> procedure,
      A1 arg1,
      A2 arg2,
      A3 arg3,
      A4 arg4,
      A5 arg5,
      A6 arg6) {
    return procedure(isAsync(procedure), () -> procedure.apply(arg1, arg2, arg3, arg4, arg5, arg6));
  }

  public static <R> Promise<R> retry(
      RetryOptions options, Optional<Duration> expiration, Functions.Func<Promise<R>> fn) {
    return WorkflowRetryerInternal.retryAsync(options, expiration, fn);
  }

  private static <R> Promise<R> execute(boolean async, Functions.Func<R> func) {
    if (async) {
      initAsyncInvocation();
      try {
        func.apply();
        return getAsyncInvocationResult();
      } catch (Exception e) {
        return Workflow.newFailedPromise(Workflow.wrap(e));
      } finally {
        closeAsyncInvocation();
      }
    } else {
      CompletablePromise<R> result = Workflow.newPromise();
      WorkflowThread.newThread(
              () -> {
                try {
                  result.complete(func.apply());
                } catch (Exception e) {
                  result.completeExceptionally(Workflow.wrap(e));
                }
              },
              false)
          .start();
      return result;
    }
  }

  public static boolean isAsync(Object func) {
    SerializedLambda lambda = LambdaUtils.toSerializedLambda(func);
    Object target = getTarget(lambda);
    return target instanceof ActivityStub
        || target instanceof ChildWorkflowStub
        || target instanceof ExternalWorkflowStub
        || (target instanceof AsyncMarker
            && lambda.getImplMethodKind() == MethodHandleInfo.REF_invokeInterface);
  }

  public static boolean isAsync() {
    return asyncResult.get() != null;
  }

  public static <R> void setAsyncResult(Promise<R> result) {
    AtomicReference<Promise<?>> placeholder = asyncResult.get();
    if (placeholder == null) {
      throw new IllegalStateException("not in invoke invocation");
    }
    placeholder.set(result);
  }

  /**
   * Indicate to the dynamic interface implementation that call was done through
   *
   * @link Async#invoke}. Must be closed through {@link #closeAsyncInvocation()}
   */
  private static void initAsyncInvocation() {
    if (asyncResult.get() != null) {
      throw new IllegalStateException("already in start invocation");
    }
    asyncResult.set(new AtomicReference<>());
  }

  /** @return asynchronous result of an invocation. */
  private static <R> Promise<R> getAsyncInvocationResult() {
    AtomicReference<Promise<?>> reference = asyncResult.get();
    if (reference == null) {
      throw new IllegalStateException("initAsyncInvocation wasn't called");
    }
    @SuppressWarnings("unchecked")
    Promise<R> result = (Promise<R>) reference.get();
    if (result == null) {
      throw new IllegalStateException("start result wasn't set");
    }
    return result;
  }

  /** Closes async invocation created through {@link #initAsyncInvocation()} */
  public static void closeAsyncInvocation() {
    asyncResult.remove();
  }

  /** Prohibit instantiation */
  private AsyncInternal() {}
}
