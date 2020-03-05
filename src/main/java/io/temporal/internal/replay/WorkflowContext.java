/*
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

<<<<<<< HEAD:src/main/java/io/temporal/internal/replay/WorkflowContext.java
import io.temporal.*;
import java.nio.ByteBuffer;
import java.util.HashMap;
=======
import com.uber.cadence.*;
import com.uber.cadence.context.ContextPropagator;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
>>>>>>> cadence/master:src/main/java/com/uber/cadence/internal/replay/WorkflowContext.java
import java.util.Map;

final class WorkflowContext {

  private final PollForDecisionTaskResponse decisionTask;
  private boolean cancelRequested;
  private ContinueAsNewWorkflowExecutionParameters continueAsNewOnCompletion;
  private WorkflowExecutionStartedEventAttributes startedAttributes;
  private final String domain;
  // RunId can change when reset happens. This remembers the actual runId that is used
  // as in this particular part of the history.
  private String currentRunId;
  private SearchAttributes searchAttributes;
<<<<<<< HEAD:src/main/java/io/temporal/internal/replay/WorkflowContext.java
=======
  private List<ContextPropagator> contextPropagators;
>>>>>>> cadence/master:src/main/java/com/uber/cadence/internal/replay/WorkflowContext.java

  WorkflowContext(
      String domain,
      PollForDecisionTaskResponse decisionTask,
      WorkflowExecutionStartedEventAttributes startedAttributes,
      List<ContextPropagator> contextPropagators) {
    this.domain = domain;
    this.decisionTask = decisionTask;
    this.startedAttributes = startedAttributes;
    this.currentRunId = startedAttributes.getOriginalExecutionRunId();
    this.searchAttributes = startedAttributes.getSearchAttributes();
<<<<<<< HEAD:src/main/java/io/temporal/internal/replay/WorkflowContext.java
=======
    this.contextPropagators = contextPropagators;
>>>>>>> cadence/master:src/main/java/com/uber/cadence/internal/replay/WorkflowContext.java
  }

  WorkflowExecution getWorkflowExecution() {
    return decisionTask.getWorkflowExecution();
  }

  WorkflowType getWorkflowType() {
    return decisionTask.getWorkflowType();
  }

  boolean isCancelRequested() {
    return cancelRequested;
  }

  void setCancelRequested(boolean flag) {
    cancelRequested = flag;
  }

  ContinueAsNewWorkflowExecutionParameters getContinueAsNewOnCompletion() {
    return continueAsNewOnCompletion;
  }

  void setContinueAsNewOnCompletion(ContinueAsNewWorkflowExecutionParameters continueParameters) {
    if (continueParameters == null) {
      continueParameters = new ContinueAsNewWorkflowExecutionParameters();
    }
    //            continueParameters.setChildPolicy(startedAttributes);
    if (continueParameters.getExecutionStartToCloseTimeoutSeconds() == 0) {
      continueParameters.setExecutionStartToCloseTimeoutSeconds(
          startedAttributes.getExecutionStartToCloseTimeoutSeconds());
    }
    if (continueParameters.getTaskList() == null) {
      continueParameters.setTaskList(startedAttributes.getTaskList().getName());
    }
    if (continueParameters.getTaskStartToCloseTimeoutSeconds() == 0) {
      continueParameters.setTaskStartToCloseTimeoutSeconds(
          startedAttributes.getTaskStartToCloseTimeoutSeconds());
    }
    this.continueAsNewOnCompletion = continueParameters;
  }

  // TODO: Implement as soon as WorkflowExecutionStartedEventAttributes have these fields added.
  ////    WorkflowExecution getParentWorkflowExecution() {
  //        WorkflowExecutionStartedEventAttributes attributes =
  // getWorkflowStartedEventAttributes();
  //        return attributes.getParentWorkflowExecution();
  //    }

  ////    io.temporal.ChildPolicy getChildPolicy() {
  //        WorkflowExecutionStartedEventAttributes attributes =
  // getWorkflowStartedEventAttributes();
  //        return ChildPolicy.fromValue(attributes.getChildPolicy());
  //    }

  ////    String getContinuedExecutionRunId() {
  //        WorkflowExecutionStartedEventAttributes attributes =
  // getWorkflowStartedEventAttributes();
  //        return attributes.getContinuedExecutionRunId();
  //    }

  int getExecutionStartToCloseTimeoutSeconds() {
    WorkflowExecutionStartedEventAttributes attributes = getWorkflowStartedEventAttributes();
    return attributes.getExecutionStartToCloseTimeoutSeconds();
  }

  int getDecisionTaskTimeoutSeconds() {
    return startedAttributes.getTaskStartToCloseTimeoutSeconds();
  }

  String getTaskList() {
    WorkflowExecutionStartedEventAttributes attributes = getWorkflowStartedEventAttributes();
    return attributes.getTaskList().getName();
  }

  String getDomain() {
    return domain;
  }

  private WorkflowExecutionStartedEventAttributes getWorkflowStartedEventAttributes() {
    return startedAttributes;
  }

  void setCurrentRunId(String currentRunId) {
    this.currentRunId = currentRunId;
  }

  String getCurrentRunId() {
    return currentRunId;
  }

  SearchAttributes getSearchAttributes() {
    return searchAttributes;
  }

<<<<<<< HEAD:src/main/java/io/temporal/internal/replay/WorkflowContext.java
=======
  public List<ContextPropagator> getContextPropagators() {
    return contextPropagators;
  }

  /** Returns a map of propagated context objects, keyed by propagator name */
  Map<String, Object> getPropagatedContexts() {
    if (contextPropagators == null || contextPropagators.isEmpty()) {
      return new HashMap<>();
    }

    Header headers = startedAttributes.getHeader();
    if (headers == null) {
      return new HashMap<>();
    }

    Map<String, byte[]> headerData = new HashMap<>();
    headers
        .getFields()
        .forEach(
            (k, v) -> {
              headerData.put(k, org.apache.thrift.TBaseHelper.byteBufferToByteArray(v));
            });

    Map<String, Object> contextData = new HashMap<>();
    for (ContextPropagator propagator : contextPropagators) {
      contextData.put(propagator.getName(), propagator.deserializeContext(headerData));
    }

    return contextData;
  }

>>>>>>> cadence/master:src/main/java/com/uber/cadence/internal/replay/WorkflowContext.java
  void mergeSearchAttributes(SearchAttributes searchAttributes) {
    if (searchAttributes == null) {
      return;
    }
    if (this.searchAttributes == null) {
      this.searchAttributes = newSearchAttributes();
    }
    Map<String, ByteBuffer> current = this.searchAttributes.getIndexedFields();
    searchAttributes
        .getIndexedFields()
        .forEach(
            (k, v) -> {
              current.put(k, v);
            });
  }

  private SearchAttributes newSearchAttributes() {
    SearchAttributes result = new SearchAttributes();
    result.setIndexedFields(new HashMap<String, ByteBuffer>());
    return result;
  }
}
