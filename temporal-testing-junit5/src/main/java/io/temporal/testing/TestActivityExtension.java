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

import io.temporal.activity.DynamicActivity;
import io.temporal.common.metadata.POJOActivityImplMetadata;
import io.temporal.common.metadata.POJOActivityInterfaceMetadata;
import java.lang.reflect.Constructor;
import java.util.HashSet;
import java.util.Set;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ExtensionContext.Namespace;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;

/**
 * JUnit Jupiter extension that simplifies testing of Temporal activities and supports calls to
 * {@link io.temporal.activity.Activity} methods from the tested activities.
 *
 * <p>This extension can inject activity stubs as well as instance of {@link
 * TestActivityEnvironment}, into test methods.
 *
 * <p>Usage example:
 *
 * <pre><code>
 * public class MyTest {
 *
 *  {@literal @}RegisterExtension
 *   public static final TestActivityExtension activityExtension =
 *       TestActivityExtension.newBuilder()
 *           .setActivityImplementations(new MyActivitiesImpl())
 *           .build();
 *
 *  {@literal @}Test
 *   public void testMyActivities(MyActivities activities) {
 *     // Test code that calls MyActivities methods
 *   }
 * }
 * </code></pre>
 */
public class TestActivityExtension
    implements ParameterResolver, BeforeEachCallback, AfterEachCallback {

  private static final String TEST_ENVIRONMENT_KEY = "testEnvironment";

  private final TestEnvironmentOptions testEnvironmentOptions;
  private final Object[] activityImplementations;

  private final Set<Class<?>> supportedParameterTypes = new HashSet<>();

  private boolean includesDynamicActivity;

  private TestActivityExtension(Builder builder) {
    testEnvironmentOptions = builder.testEnvironmentOptions;
    activityImplementations = builder.activityImplementations;

    supportedParameterTypes.add(TestActivityEnvironment.class);

    for (Object activity : activityImplementations) {
      if (DynamicActivity.class.isAssignableFrom(activity.getClass())) {
        includesDynamicActivity = true;
        continue;
      }
      POJOActivityImplMetadata metadata = POJOActivityImplMetadata.newInstance(activity.getClass());
      for (POJOActivityInterfaceMetadata activityInterface : metadata.getActivityInterfaces()) {
        supportedParameterTypes.add(activityInterface.getInterfaceClass());
      }
    }
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  @Override
  public boolean supportsParameter(
      ParameterContext parameterContext, ExtensionContext extensionContext)
      throws ParameterResolutionException {

    if (parameterContext.getParameter().getDeclaringExecutable() instanceof Constructor) {
      // Constructor injection is not supported
      return false;
    }

    if (includesDynamicActivity) {
      return true;
    }

    Class<?> parameterType = parameterContext.getParameter().getType();
    return supportedParameterTypes.contains(parameterType);
  }

  @Override
  public Object resolveParameter(
      ParameterContext parameterContext, ExtensionContext extensionContext)
      throws ParameterResolutionException {

    TestActivityEnvironment testEnvironment = getTestEnvironment(extensionContext);

    Class<?> parameterType = parameterContext.getParameter().getType();
    if (parameterType == TestActivityEnvironment.class) {
      return testEnvironment;
    } else {
      // Activity stub
      return testEnvironment.newActivityStub(parameterType);
    }
  }

  @Override
  public void beforeEach(ExtensionContext context) throws Exception {
    TestActivityEnvironment testEnvironment =
        TestActivityEnvironment.newInstance(testEnvironmentOptions);
    testEnvironment.registerActivitiesImplementations(activityImplementations);

    setTestEnvironment(context, testEnvironment);
  }

  @Override
  public void afterEach(ExtensionContext context) throws Exception {
    TestActivityEnvironment testEnvironment = getTestEnvironment(context);
    testEnvironment.close();
  }

  private TestActivityEnvironment getTestEnvironment(ExtensionContext context) {
    return getStore(context).get(TEST_ENVIRONMENT_KEY, TestActivityEnvironment.class);
  }

  private void setTestEnvironment(
      ExtensionContext context, TestActivityEnvironment testEnvironment) {
    getStore(context).put(TEST_ENVIRONMENT_KEY, testEnvironment);
  }

  private ExtensionContext.Store getStore(ExtensionContext context) {
    Namespace namespace =
        Namespace.create(TestActivityExtension.class, context.getRequiredTestMethod());
    return context.getStore(namespace);
  }

  public static class Builder {

    private static final Object[] NO_ACTIVITIES = new Object[0];

    private TestEnvironmentOptions testEnvironmentOptions =
        TestEnvironmentOptions.getDefaultInstance();
    private Object[] activityImplementations = NO_ACTIVITIES;

    private Builder() {}

    /** @see TestActivityEnvironment#newInstance(TestEnvironmentOptions) */
    public Builder setTestEnvironmentOptions(TestEnvironmentOptions testEnvironmentOptions) {
      this.testEnvironmentOptions = testEnvironmentOptions;
      return this;
    }

    /**
     * Specify activity implementations to register with the Temporal worker
     *
     * @see TestActivityEnvironment#registerActivitiesImplementations(Object...)
     */
    public Builder setActivityImplementations(Object... activityImplementations) {
      this.activityImplementations = activityImplementations;
      return this;
    }

    public TestActivityExtension build() {
      return new TestActivityExtension(this);
    }
  }
}
