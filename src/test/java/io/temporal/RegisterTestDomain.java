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

package io.temporal;

import static io.temporal.workflow.WorkflowTest.DOMAIN;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.temporal.proto.workflowservice.RegisterDomainRequest;
import io.temporal.serviceclient.WorkflowServiceStubs;

/** Waits for local service to become available and registers UnitTest domain. */
public class RegisterTestDomain {
  private static final boolean useDockerService =
      Boolean.parseBoolean(System.getenv("USE_DOCKER_SERVICE"));

  public static void main(String[] args) throws InterruptedException {
    if (!useDockerService) {
      return;
    }

    WorkflowServiceStubs service =
        WorkflowServiceStubs.newInstance(WorkflowServiceStubs.LOCAL_DOCKER_TARGET);
    RegisterDomainRequest request =
        RegisterDomainRequest.newBuilder()
            .setName(DOMAIN)
            .setWorkflowExecutionRetentionPeriodInDays(1)
            .build();
    while (true) {
      try {
        service.blockingStub().registerDomain(request);
        break;
      } catch (StatusRuntimeException e) {
        if (e.getStatus().getCode() == Status.Code.ALREADY_EXISTS) {
          break;
        }
        if (e.getStatus().getCode() == Status.Code.DEADLINE_EXCEEDED
            || e.getStatus().getCode() == Status.Code.UNAVAILABLE) {
          e.printStackTrace();
          Thread.sleep(500);
        }
        continue;
      } catch (Throwable e) {
        e.printStackTrace();
        System.exit(1);
      }
    }
    System.exit(0);
  }
}
