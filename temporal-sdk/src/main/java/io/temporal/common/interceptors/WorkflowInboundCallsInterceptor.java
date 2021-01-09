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

package io.temporal.common.interceptors;

import io.temporal.api.common.v1.Payload;
import java.util.Map;

/**
 * Intercepts calls to the workflow execution. Executes under workflow context. So all the
 * restrictions on the workflow code should be obeyed.
 */
public interface WorkflowInboundCallsInterceptor {
  final class WorkflowInput {
    private final Map<String, Payload> header;
    private final Object[] arguments;

    public WorkflowInput(Map<String, Payload> header, Object[] arguments) {
      this.header = header;
      this.arguments = arguments;
    }

    public Map<String, Payload> getHeader() {
      return header;
    }

    public Object[] getArguments() {
      return arguments;
    }
  }

  final class WorkflowOutput {
    private final Object result;

    public WorkflowOutput(Object result) {
      this.result = result;
    }

    public Object getResult() {
      return result;
    }
  }

  final class SignalInput {
    private final String signalName;
    private final Object[] arguments;
    private final long EventId;

    public SignalInput(String signalName, Object[] arguments, long eventId) {
      this.signalName = signalName;
      this.arguments = arguments;
      EventId = eventId;
    }

    public String getSignalName() {
      return signalName;
    }

    public Object[] getArguments() {
      return arguments;
    }

    public long getEventId() {
      return EventId;
    }
  }

  /**
   * Called when workflow class is instantiated.
   *
   * @param outboundCalls interceptor for calls that workflow makes to the SDK
   */
  void init(WorkflowOutboundCallsInterceptor outboundCalls);

  /**
   * Called when workflow main method is called.
   *
   * @return result of the workflow execution.
   */
  WorkflowOutput execute(WorkflowInput input);

  /** Called when signal is delivered to the workflow instance. */
  void processSignal(SignalInput input);
}
