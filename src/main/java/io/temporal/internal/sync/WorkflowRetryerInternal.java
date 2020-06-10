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

import io.temporal.common.RetryOptions;
import io.temporal.workflow.CompletablePromise;
import io.temporal.workflow.Functions;
import io.temporal.workflow.Promise;
import io.temporal.workflow.Workflow;
import java.time.Duration;
import java.util.Optional;

/**
 * Implements operation retry logic for both synchronous and asynchronous operations. Internal
 * class. Do not reference this class directly. Use {@link Workflow#retry(RetryOptions, Optional,
 * Functions.Func)} or Async{@link #retry(RetryOptions, Optional, Functions.Proc)}.
 */
final class WorkflowRetryerInternal {

  /**
   * Retry procedure synchronously.
   *
   * @param options retry options.
   * @param proc procedure to retry.
   */
  public static void retry(
      RetryOptions options, Optional<Duration> expiration, Functions.Proc proc) {
    retry(
        options,
        expiration,
        () -> {
          proc.apply();
          return null;
        });
  }

  public static <R> R validateOptionsAndRetry(
      RetryOptions options, Optional<Duration> expiration, Functions.Func<R> func) {
    return retry(RetryOptions.merge(null, options), expiration, func);
  }

  /**
   * Retry function synchronously.
   *
   * @param options retry options.
   * @param func procedure to retry.
   * @return result of func if ever completed successfully.
   */
  public static <R> R retry(
      RetryOptions options, Optional<Duration> expiration, Functions.Func<R> func) {
    int attempt = 1;
    long startTime = WorkflowInternal.currentTimeMillis();
    // Records retry options in the history allowing changing them without breaking determinism.
    String retryId = WorkflowInternal.randomUUID().toString();
    RetryOptions retryOptions =
        WorkflowInternal.mutableSideEffect(
            retryId,
            RetryOptions.class,
            RetryOptions.class,
            Object::equals,
            () -> RetryOptions.newBuilder(options).validateBuildWithDefaults());
    while (true) {
      long nextSleepTime = retryOptions.calculateSleepTime(attempt);
      try {
        return func.apply();
      } catch (Exception e) {
        long elapsed = WorkflowInternal.currentTimeMillis() - startTime;
        if (retryOptions.shouldRethrow(e, expiration, attempt, elapsed, nextSleepTime)) {
          throw WorkflowInternal.wrap(e);
        }
      }
      attempt++;
      WorkflowInternal.sleep(Duration.ofMillis(nextSleepTime));
    }
  }

  /**
   * Retry function asynchronously.
   *
   * @param options retry options.
   * @param expiration if present limits duration of retries
   * @param func procedure to retry.
   * @return result promise to the result or failure if retries stopped according to options.
   */
  public static <R> Promise<R> retryAsync(
      RetryOptions options, Optional<Duration> expiration, Functions.Func<Promise<R>> func) {
    String retryId = WorkflowInternal.randomUUID().toString();
    long startTime = WorkflowInternal.currentTimeMillis();
    return retryAsync(retryId, options, expiration, func, startTime, 1);
  }

  private static <R> Promise<R> retryAsync(
      String retryId,
      RetryOptions options,
      Optional<Duration> expiration,
      Functions.Func<Promise<R>> func,
      long startTime,
      long attempt) {
    RetryOptions retryOptions =
        WorkflowInternal.mutableSideEffect(
            retryId,
            RetryOptions.class,
            RetryOptions.class,
            Object::equals,
            () -> RetryOptions.newBuilder(options).validateBuildWithDefaults());

    CompletablePromise<R> funcResult = WorkflowInternal.newCompletablePromise();
    try {
      funcResult.completeFrom(func.apply());
    } catch (RuntimeException e) {
      funcResult.completeExceptionally(e);
    }
    return funcResult
        .handle(
            (r, e) -> {
              if (e == null) {
                return WorkflowInternal.newPromise(r);
              }
              long elapsed = WorkflowInternal.currentTimeMillis() - startTime;
              long sleepTime = retryOptions.calculateSleepTime(attempt);
              if (retryOptions.shouldRethrow(e, expiration, attempt, elapsed, sleepTime)) {
                throw e;
              }
              // newTimer runs in a separate thread, so it performs trampolining eliminating tail
              // recursion.
              return WorkflowInternal.newTimer(Duration.ofMillis(sleepTime))
                  .thenCompose(
                      (nil) ->
                          retryAsync(
                              retryId, retryOptions, expiration, func, startTime, attempt + 1));
            })
        .thenCompose((r) -> r);
  }

  static <R> Promise<R> retryAsync(
      Functions.Func2<Integer, Long, Promise<R>> func, int attempt, long startTime) {

    CompletablePromise<R> funcResult = WorkflowInternal.newCompletablePromise();
    try {
      funcResult.completeFrom(func.apply(attempt, startTime));
    } catch (RuntimeException e) {
      funcResult.completeExceptionally(e);
    }

    return funcResult
        .handle(
            (r, e) -> {
              if (e == null) {
                return WorkflowInternal.newPromise(r);
              }
              throw e;
            })
        .thenCompose((r) -> r);
  }

  private WorkflowRetryerInternal() {}
}
