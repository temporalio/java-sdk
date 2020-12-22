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

package io.temporal.testing;

import com.google.common.annotations.VisibleForTesting;
import com.uber.m3.tally.NoopScope;
import com.uber.m3.tally.Scope;
import io.temporal.client.WorkflowClientOptions;
import io.temporal.worker.WorkerFactoryOptions;

@VisibleForTesting
public final class TestEnvironmentOptions {

  public static Builder newBuilder() {
    return new Builder();
  }

  public static Builder newBuilder(TestEnvironmentOptions options) {
    return new Builder(options);
  }

  public static TestEnvironmentOptions getDefaultInstance() {
    return DEFAULT_INSTANCE;
  }

  private static final TestEnvironmentOptions DEFAULT_INSTANCE;

  static {
    DEFAULT_INSTANCE = TestEnvironmentOptions.newBuilder().build();
  }

  public static final class Builder {

    private WorkerFactoryOptions workerFactoryOptions;

    private WorkflowClientOptions workflowClientOptions;

    private Scope metricsScope;

    private boolean useExternalService;

    private String serviceAddress;

    private Builder() {}

    private Builder(TestEnvironmentOptions o) {
      workerFactoryOptions = o.workerFactoryOptions;
      workflowClientOptions = o.workflowClientOptions;
      useExternalService = o.useExternalService;
      serviceAddress = o.serviceAddress;
    }

    public Builder setWorkflowClientOptions(WorkflowClientOptions workflowClientOptions) {
      this.workflowClientOptions = workflowClientOptions;
      return this;
    }

    /** Set factoryOptions for worker factory used to create workers. */
    public Builder setWorkerFactoryOptions(WorkerFactoryOptions options) {
      this.workerFactoryOptions = options;
      return this;
    }

    public Builder setMetricsScope(Scope metricsScope) {
      this.metricsScope = metricsScope;
      return this;
    }

    public Builder setUseExternalService(boolean useExternalService) {
      this.useExternalService = useExternalService;
      return this;
    }

    public Builder setServiceAddress(String serviceAddress) {
      this.serviceAddress = serviceAddress;
      return this;
    }

    public TestEnvironmentOptions build() {
      return new TestEnvironmentOptions(
          workflowClientOptions,
          workerFactoryOptions,
          useExternalService,
          serviceAddress,
          metricsScope);
    }

    public TestEnvironmentOptions validateAndBuildWithDefaults() {
      return new TestEnvironmentOptions(
          WorkflowClientOptions.newBuilder(workflowClientOptions).validateAndBuildWithDefaults(),
          WorkerFactoryOptions.newBuilder(workerFactoryOptions).validateAndBuildWithDefaults(),
          useExternalService,
          serviceAddress,
          metricsScope == null ? new NoopScope() : metricsScope);
    }
  }

  private final WorkerFactoryOptions workerFactoryOptions;
  private final WorkflowClientOptions workflowClientOptions;
  private final Scope metricsScope;
  private final boolean useExternalService;
  private final String serviceAddress;

  private TestEnvironmentOptions(
      WorkflowClientOptions workflowClientOptions,
      WorkerFactoryOptions workerFactoryOptions,
      boolean useExternalService,
      String serviceAddress,
      Scope metricsScope) {
    this.workflowClientOptions = workflowClientOptions;
    this.workerFactoryOptions = workerFactoryOptions;
    this.metricsScope = metricsScope;
    this.useExternalService = useExternalService;
    this.serviceAddress = serviceAddress;
  }

  public WorkerFactoryOptions getWorkerFactoryOptions() {
    return workerFactoryOptions;
  }

  public WorkflowClientOptions getWorkflowClientOptions() {
    return workflowClientOptions;
  }

  public Scope getMetricsScope() {
    return metricsScope;
  }

  /**
   * Returns true if the test environment is using external temporal service or false for in-memory
   * test implementation.
   */
  public boolean isUseExternalService() {
    return useExternalService;
  }

  public String getServiceAddress() {
    return serviceAddress;
  }

  @Override
  public String toString() {
    return "TestEnvironmentOptions{"
        + "workerFactoryOptions="
        + workerFactoryOptions
        + ", workflowClientOptions="
        + workflowClientOptions
        + '}';
  }
}
