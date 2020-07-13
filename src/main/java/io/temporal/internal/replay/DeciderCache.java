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

package io.temporal.internal.replay;

import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.uber.m3.tally.Scope;
import io.temporal.api.workflowservice.v1.PollWorkflowTaskQueueResponseOrBuilder;
import io.temporal.internal.metrics.MetricsType;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public final class DeciderCache {
  private final Scope metricsScope;
  private LoadingCache<String, Decider> cache;
  private Lock cacheLock = new ReentrantLock();
  private Set<String> inProcessing = new HashSet<>();

  public DeciderCache(int workflowCacheSize, Scope scope) {
    Preconditions.checkArgument(workflowCacheSize > 0, "Max cache size must be greater than 0");
    this.metricsScope = Objects.requireNonNull(scope);
    this.cache =
        CacheBuilder.newBuilder()
            .maximumSize(workflowCacheSize)
            .removalListener(
                e -> {
                  Decider entry = (Decider) e.getValue();
                  if (entry != null) {
                    entry.close();
                  }
                })
            .build(
                new CacheLoader<String, Decider>() {
                  @Override
                  public Decider load(String key) {
                    return null;
                  }
                });
  }

  public Decider getOrCreate(
      PollWorkflowTaskQueueResponseOrBuilder workflowTask,
      Scope metricsScope,
      Callable<Decider> deciderFunc)
      throws Exception {
    String runId = workflowTask.getWorkflowExecution().getRunId();
    if (isFullHistory(workflowTask)) {
      invalidate(runId, metricsScope);
      return deciderFunc.call();
    }

    Decider decider = getForProcessing(runId, metricsScope);
    if (decider != null) {
      return decider;
    }
    return deciderFunc.call();
  }

  private Decider getForProcessing(String runId, Scope metricsScope) throws Exception {
    cacheLock.lock();
    try {
      Decider decider = cache.get(runId);
      inProcessing.add(runId);
      metricsScope.counter(MetricsType.STICKY_CACHE_HIT).inc(1);
      return decider;
    } catch (CacheLoader.InvalidCacheLoadException e) {
      // We don't have a default loader and don't want to have one. So it's ok to get null value.
      metricsScope.counter(MetricsType.STICKY_CACHE_MISS).inc(1);
      return null;
    } finally {
      cacheLock.unlock();
    }
  }

  void markProcessingDone(PollWorkflowTaskQueueResponseOrBuilder workflowTask) {
    String runId = workflowTask.getWorkflowExecution().getRunId();

    cacheLock.lock();
    try {
      inProcessing.remove(runId);
    } finally {
      cacheLock.unlock();
    }
  }

  public void addToCache(PollWorkflowTaskQueueResponseOrBuilder workflowTask, Decider decider) {
    String runId = workflowTask.getWorkflowExecution().getRunId();
    cache.put(runId, decider);
  }

  public boolean evictAnyNotInProcessing(String runId, Scope metricsScope) {
    cacheLock.lock();
    try {
      this.metricsScope.gauge(MetricsType.STICKY_CACHE_SIZE).update(size());
      for (String key : cache.asMap().keySet()) {
        if (!key.equals(runId) && !inProcessing.contains(key)) {
          cache.invalidate(key);
          this.metricsScope.gauge(MetricsType.STICKY_CACHE_SIZE).update(size());
          metricsScope.counter(MetricsType.STICKY_CACHE_THREAD_FORCED_EVICTION).inc(1);
          return true;
        }
      }

      return false;
    } finally {
      cacheLock.unlock();
    }
  }

  void invalidate(String runId, Scope metricsScope) {
    cacheLock.lock();
    try {
      cache.invalidate(runId);
      inProcessing.remove(runId);
      metricsScope.counter(MetricsType.STICKY_CACHE_TOTAL_FORCED_EVICTION).inc(1);
    } finally {
      cacheLock.unlock();
    }
  }

  public long size() {
    return cache.size();
  }

  private boolean isFullHistory(PollWorkflowTaskQueueResponseOrBuilder workflowTask) {
    return workflowTask.getHistory() != null
        && workflowTask.getHistory().getEventsCount() > 0
        && workflowTask.getHistory().getEvents(0).getEventId() == 1;
  }

  public void invalidateAll() {
    cache.invalidateAll();
  }
}
