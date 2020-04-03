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

import io.temporal.client.WorkflowClient;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * WorkflowInterface annotation indicates that an interface is a Workflow interface. Only interfaces
 * annotated with this annotation can be used as parameters to {@link
 * WorkflowClient#newWorkflowStub(Class)} and {@link Workflow#newChildWorkflowStub(Class)} methods.
 *
 * <p>All methods of an interface annotated with WorkflowInterface must have one of the following
 * annotations: {@literal @}WorkflowMethod, {@literal @}SignalMethod or {@literal @}QueryMethod
 *
 * <p>An interface annotated with WorkflowInterface can extend other interfaces annotated with
 * WorkflowInterface having that it can have at most one method annotated with
 * {@literal @}WorkflowMethod including all inherited methods.
 *
 * <p>The prefix of workflow, signal and query type names is the name of the declaring interface
 * annotated with WorkflowInterface. If a method is declared in non annotated interface the prefix
 * comes from the first sub-interface that has the WorkflowInterface annotation.
 *
 * <p>A workflow implementation object must have exactly one method annotated with
 * {@literal @}WorkflowMethod inherited from all the interfaces it implements.
 *
 * <p>Example:
 *
 * <pre><code>
 *
 *  public interface A {
 *     {@literal @}SignalMethod
 *      a();
 *      aa();
 *  }
 *
 * {@literal @}WorkflowInterface
 *  public interface B extends A {
 *     {@literal @}SignalMethod
 *      b();
 *
 *     {@literal @}SignalMethod // must to define the type of the inherited method
 *      aa();
 *  }
 *
 * {@literal @}WorkflowInterface
 *  public interface C extends B {
 *    {@literal @}WorkflowMethod
 *     c();
 *  }
 *
 * {@literal @}WorkflowInterface
 *  public interface D extends C {
 *    {@literal @}QueryMethod
 *     String d();
 *  }
 *
 *  public class CImpl implements C {
 *      public void a() {}
 *      public void aa() {}
 *      public void b() {}
 *      public void c() {}
 *      public String d() { return "foo"; }
 *  }
 * </code></pre>
 *
 * When <code>CImpl</code> instance is registered with the {@link io.temporal.worker.Worker} the
 * following is registered:
 *
 * <p>
 *
 * <ul>
 *   <li>B_a signal handler
 *   <li>B_b signal handler
 *   <li>B_aa signal handler
 *   <li>C_c workflow main method
 *   <li>D_d query method
 * </ul>
 *
 * Note that methods <code>a()</code> and <code>aa()</code> are registered with "B_" prefix because
 * interface <code>A</code> lacks the WorkflowInterface annotation. The client code can call signals
 * through stubs to <code>B</code>, <code>C</code> and <code>D</code> interfaces. A call to crate a
 * stub to <code>A</code> interface will fail as <code>A</code> is not annotated with the
 * WorkflowInterface.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface WorkflowInterface {}
