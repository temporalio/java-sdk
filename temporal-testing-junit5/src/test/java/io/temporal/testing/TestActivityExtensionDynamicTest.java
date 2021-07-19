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

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.temporal.activity.Activity;
import io.temporal.activity.ActivityInterface;
import io.temporal.activity.ActivityMethod;
import io.temporal.activity.DynamicActivity;
import io.temporal.common.converter.EncodedValues;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

public class TestActivityExtensionDynamicTest {

  @RegisterExtension
  public static final TestActivityExtension activityExtension =
      TestActivityExtension.newBuilder()
          .setActivityImplementations(new MyDynamicActivityImpl())
          .build();

  @ActivityInterface
  public interface MyActivity {

    @ActivityMethod(name = "OverriddenActivityMethod")
    String activity1(String input);
  }

  private static class MyDynamicActivityImpl implements DynamicActivity {

    @Override
    public Object execute(EncodedValues args) {
      return Activity.getExecutionContext().getInfo().getActivityType()
          + "-"
          + args.get(0, String.class);
    }
  }

  @Test
  public void extensionShouldResolveDynamicActivitiesParameters(MyActivity activity) {
    assertEquals("OverriddenActivityMethod-input1", activity.activity1("input1"));
  }
}
