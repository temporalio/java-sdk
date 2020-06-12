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

package io.temporal.workflow;

import java.util.concurrent.TimeUnit;

public interface QueueConsumer<E> {

  /**
   * Retrieves and removes the head of this queue, waiting if necessary until an element becomes
   * available. It is not unblocked in case of the enclosing * CancellationScope cancellation. Use
   * {@link #cancellableTake()} instead.
   *
   * @return the head of this queue
   */
  E take();

  /**
   * Retrieves and removes the head of this queue, waiting if necessary until an element becomes
   * available.
   *
   * @return the head of this queue
   * @throws java.util.concurrent.CanceledException if surrounding @{@link CancellationScope} is
   *     cancelled while waiting
   */
  E cancellableTake();

  /**
   * Retrieves and removes the head of this queue if it is not empty without blocking.
   *
   * @return the head of this queue, or {@code null} if the specified waiting time elapses before an
   *     element is available
   */
  E poll();

  /**
   * Retrieves the head of this queue keeping it in the queue if it is not empty without blocking.
   *
   * @return the head of this queue, or {@code null} if the specified waiting time elapses before an
   *     element is available
   */
  E peek();

  /**
   * Retrieves and removes the head of this queue, waiting up to the specified wait time if
   * necessary for an element to become available. It is not unblocked in case of the enclosing
   * CancellationScope cancellation. Use {@link #cancellablePoll(long, TimeUnit)} instead.
   *
   * @param timeout how long to wait before giving up, in units of {@code unit}
   * @param unit a {@code TimeUnit} determining how to interpret the {@code timeout} parameter
   * @return the head of this queue, or {@code null} if the specified waiting time elapses before an
   *     element is available
   */
  E poll(long timeout, TimeUnit unit);

  /**
   * Retrieves and removes the head of this queue, waiting up to the specified wait time if
   * necessary for an element to become available.
   *
   * @param timeout how long to wait before giving up, in units of {@code unit}
   * @param unit a {@code TimeUnit} determining how to interpret the {@code timeout} parameter
   * @return the head of this queue, or {@code null} if the specified waiting time elapses before an
   *     element is available
   * @throws java.util.concurrent.CanceledException if surrounding @{@link CancellationScope} is
   *     cancelled while waiting
   */
  E cancellablePoll(long timeout, TimeUnit unit);

  /**
   * Returns a queue consisting of the results of applying the given function to the elements of
   * this queue.
   *
   * @param mapper a non-interfering, stateless function to apply to each element
   * @return the new queue backed by this one.
   */
  <R> QueueConsumer<R> map(Functions.Func1<? super E, ? extends R> mapper);
}
