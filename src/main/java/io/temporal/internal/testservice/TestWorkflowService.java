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

package io.temporal.internal.testservice;

import com.google.common.base.Throwables;
import io.grpc.Context;
import io.grpc.Deadline;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import io.temporal.common.v1.Payloads;
import io.temporal.common.v1.RetryPolicy;
import io.temporal.common.v1.WorkflowExecution;
import io.temporal.decision.v1.SignalExternalWorkflowExecutionDecisionAttributes;
import io.temporal.enums.v1.SignalExternalWorkflowExecutionFailedCause;
import io.temporal.enums.v1.WorkflowExecutionStatus;
import io.temporal.enums.v1.WorkflowIdReusePolicy;
import io.temporal.errordetails.v1.WorkflowExecutionAlreadyStartedFailure;
import io.temporal.history.v1.WorkflowExecutionContinuedAsNewEventAttributes;
import io.temporal.internal.common.StatusUtils;
import io.temporal.internal.testservice.TestWorkflowStore.WorkflowState;
import io.temporal.serviceclient.WorkflowServiceStubs;
import io.temporal.serviceclient.WorkflowServiceStubsOptions;
import io.temporal.workflow.v1.WorkflowExecutionInfo;
import io.temporal.workflowservice.v1.GetWorkflowExecutionHistoryRequest;
import io.temporal.workflowservice.v1.GetWorkflowExecutionHistoryResponse;
import io.temporal.workflowservice.v1.ListClosedWorkflowExecutionsRequest;
import io.temporal.workflowservice.v1.ListClosedWorkflowExecutionsResponse;
import io.temporal.workflowservice.v1.ListOpenWorkflowExecutionsRequest;
import io.temporal.workflowservice.v1.ListOpenWorkflowExecutionsResponse;
import io.temporal.workflowservice.v1.PollForActivityTaskRequest;
import io.temporal.workflowservice.v1.PollForActivityTaskResponse;
import io.temporal.workflowservice.v1.PollForDecisionTaskRequest;
import io.temporal.workflowservice.v1.PollForDecisionTaskResponse;
import io.temporal.workflowservice.v1.QueryWorkflowRequest;
import io.temporal.workflowservice.v1.QueryWorkflowResponse;
import io.temporal.workflowservice.v1.RecordActivityTaskHeartbeatByIdRequest;
import io.temporal.workflowservice.v1.RecordActivityTaskHeartbeatByIdResponse;
import io.temporal.workflowservice.v1.RecordActivityTaskHeartbeatRequest;
import io.temporal.workflowservice.v1.RecordActivityTaskHeartbeatResponse;
import io.temporal.workflowservice.v1.RequestCancelWorkflowExecutionRequest;
import io.temporal.workflowservice.v1.RequestCancelWorkflowExecutionResponse;
import io.temporal.workflowservice.v1.RespondActivityTaskCanceledByIdRequest;
import io.temporal.workflowservice.v1.RespondActivityTaskCanceledByIdResponse;
import io.temporal.workflowservice.v1.RespondActivityTaskCanceledRequest;
import io.temporal.workflowservice.v1.RespondActivityTaskCanceledResponse;
import io.temporal.workflowservice.v1.RespondActivityTaskCompletedByIdRequest;
import io.temporal.workflowservice.v1.RespondActivityTaskCompletedByIdResponse;
import io.temporal.workflowservice.v1.RespondActivityTaskCompletedRequest;
import io.temporal.workflowservice.v1.RespondActivityTaskCompletedResponse;
import io.temporal.workflowservice.v1.RespondActivityTaskFailedByIdRequest;
import io.temporal.workflowservice.v1.RespondActivityTaskFailedByIdResponse;
import io.temporal.workflowservice.v1.RespondActivityTaskFailedRequest;
import io.temporal.workflowservice.v1.RespondActivityTaskFailedResponse;
import io.temporal.workflowservice.v1.RespondDecisionTaskCompletedRequest;
import io.temporal.workflowservice.v1.RespondDecisionTaskCompletedResponse;
import io.temporal.workflowservice.v1.RespondDecisionTaskFailedRequest;
import io.temporal.workflowservice.v1.RespondDecisionTaskFailedResponse;
import io.temporal.workflowservice.v1.RespondQueryTaskCompletedRequest;
import io.temporal.workflowservice.v1.RespondQueryTaskCompletedResponse;
import io.temporal.workflowservice.v1.SignalWithStartWorkflowExecutionRequest;
import io.temporal.workflowservice.v1.SignalWithStartWorkflowExecutionResponse;
import io.temporal.workflowservice.v1.SignalWorkflowExecutionRequest;
import io.temporal.workflowservice.v1.SignalWorkflowExecutionResponse;
import io.temporal.workflowservice.v1.StartWorkflowExecutionRequest;
import io.temporal.workflowservice.v1.StartWorkflowExecutionResponse;
import io.temporal.workflowservice.v1.WorkflowServiceGrpc;
import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * In memory implementation of the Temporal service. To be used for testing purposes only. Do not
 * use directly. Instead use {@link io.temporal.testing.TestWorkflowEnvironment}.
 */
public final class TestWorkflowService extends WorkflowServiceGrpc.WorkflowServiceImplBase
    implements AutoCloseable {

  private static final Logger log = LoggerFactory.getLogger(TestWorkflowService.class);

  private final Lock lock = new ReentrantLock();

  private final TestWorkflowStore store = new TestWorkflowStoreImpl();

  private final Map<ExecutionId, TestWorkflowMutableState> executions = new HashMap<>();

  // key->WorkflowId
  private final Map<WorkflowId, TestWorkflowMutableState> executionsByWorkflowId = new HashMap<>();

  private final ForkJoinPool forkJoinPool = new ForkJoinPool(4);

  private final String serverName;
  private ManagedChannel channel;
  private WorkflowServiceStubs stubs;

  public WorkflowServiceStubs newClientStub() {
    return stubs;
  }

  public TestWorkflowService(boolean lockTimeSkipping) {
    this();
    if (lockTimeSkipping) {
      this.lockTimeSkipping("constructor");
    }
  }

  // TODO: Shutdown.
  public TestWorkflowService() {
    serverName = InProcessServerBuilder.generateName();
    try {
      Server server =
          InProcessServerBuilder.forName(serverName)
              .directExecutor()
              .addService(this)
              .build()
              .start();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    channel = InProcessChannelBuilder.forName(serverName).directExecutor().build();
    stubs =
        WorkflowServiceStubs.newInstance(
            WorkflowServiceStubsOptions.newBuilder().setChannel(channel).build());
  }

  @Override
  public void close() {
    channel.shutdown();
    try {
      channel.awaitTermination(1, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      log.debug("interrupted", e);
    }
    store.close();
  }

  private TestWorkflowMutableState getMutableState(ExecutionId executionId) {
    return getMutableState(executionId, true);
  }

  private TestWorkflowMutableState getMutableState(ExecutionId executionId, boolean failNotExists) {
    lock.lock();
    try {
      if (executionId.getExecution().getRunId().isEmpty()) {
        return getMutableState(executionId.getWorkflowId(), failNotExists);
      }
      TestWorkflowMutableState mutableState = executions.get(executionId);
      if (mutableState == null && failNotExists) {
        throw Status.NOT_FOUND
            .withDescription(
                "Execution \""
                    + executionId
                    + "\" not found in mutable state. Known executions: "
                    + executions.values()
                    + ", service="
                    + this)
            .asRuntimeException();
      }
      return mutableState;
    } finally {
      lock.unlock();
    }
  }

  private TestWorkflowMutableState getMutableState(WorkflowId workflowId) {
    return getMutableState(workflowId, true);
  }

  private TestWorkflowMutableState getMutableState(WorkflowId workflowId, boolean failNotExists) {
    lock.lock();
    try {
      TestWorkflowMutableState mutableState = executionsByWorkflowId.get(workflowId);
      if (mutableState == null && failNotExists) {
        throw Status.NOT_FOUND
            .withDescription("Execution not found in mutable state: " + workflowId)
            .asRuntimeException();
      }
      return mutableState;
    } finally {
      lock.unlock();
    }
  }

  @Override
  public void startWorkflowExecution(
      StartWorkflowExecutionRequest request,
      StreamObserver<StartWorkflowExecutionResponse> responseObserver) {
    try {
      StartWorkflowExecutionResponse response =
          startWorkflowExecutionImpl(
              request, 0, Optional.empty(), OptionalLong.empty(), Optional.empty());
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (StatusRuntimeException e) {
      if (e.getStatus().getCode() == Status.Code.INTERNAL) {
        log.error("unexpected", e);
      }
      responseObserver.onError(e);
    }
  }

  StartWorkflowExecutionResponse startWorkflowExecutionImpl(
      StartWorkflowExecutionRequest startRequest,
      int backoffStartIntervalInSeconds,
      Optional<TestWorkflowMutableState> parent,
      OptionalLong parentChildInitiatedEventId,
      Optional<SignalWorkflowExecutionRequest> signalWithStartSignal) {
    String requestWorkflowId = requireNotNull("WorkflowId", startRequest.getWorkflowId());
    String namespace = requireNotNull("Namespace", startRequest.getNamespace());
    WorkflowId workflowId = new WorkflowId(namespace, requestWorkflowId);
    TestWorkflowMutableState existing;
    lock.lock();
    try {
      existing = executionsByWorkflowId.get(workflowId);
      if (existing != null) {
        WorkflowExecutionStatus status = existing.getWorkflowExecutionStatus();
        WorkflowIdReusePolicy policy = startRequest.getWorkflowIdReusePolicy();
        if (status == WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_RUNNING
            || policy == WorkflowIdReusePolicy.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE) {
          return throwDuplicatedWorkflow(startRequest, existing);
        }
        if (policy == WorkflowIdReusePolicy.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE_FAILED_ONLY
            && (status == WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_COMPLETED
                || status == WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW)) {
          return throwDuplicatedWorkflow(startRequest, existing);
        }
      }
      Optional<RetryState> retryState;
      if (startRequest.hasRetryPolicy()) {
        long expirationInterval = startRequest.getWorkflowExecutionTimeoutSeconds();
        retryState = newRetryStateLocked(startRequest.getRetryPolicy(), expirationInterval);
      } else {
        retryState = Optional.empty();
      }
      return startWorkflowExecutionNoRunningCheckLocked(
          startRequest,
          UUID.randomUUID().toString(),
          Optional.empty(),
          retryState,
          backoffStartIntervalInSeconds,
          null,
          parent,
          parentChildInitiatedEventId,
          signalWithStartSignal,
          workflowId);
    } finally {
      lock.unlock();
    }
  }

  private Optional<RetryState> newRetryStateLocked(
      RetryPolicy retryPolicy, long expirationInterval) {
    long expirationTime =
        expirationInterval == 0 ? 0 : store.currentTimeMillis() + expirationInterval;
    return Optional.of(new RetryState(retryPolicy, expirationTime));
  }

  private StartWorkflowExecutionResponse throwDuplicatedWorkflow(
      StartWorkflowExecutionRequest startRequest, TestWorkflowMutableState existing) {
    WorkflowExecution execution = existing.getExecutionId().getExecution();
    WorkflowExecutionAlreadyStartedFailure error =
        WorkflowExecutionAlreadyStartedFailure.newBuilder()
            .setRunId(execution.getRunId())
            .setStartRequestId(startRequest.getRequestId())
            .build();
    throw StatusUtils.newException(
        Status.ALREADY_EXISTS.withDescription(
            String.format(
                "WorkflowId: %s, " + "RunId: %s", execution.getWorkflowId(), execution.getRunId())),
        error);
  }

  private StartWorkflowExecutionResponse startWorkflowExecutionNoRunningCheckLocked(
      StartWorkflowExecutionRequest startRequest,
      String runId,
      Optional<String> continuedExecutionRunId,
      Optional<RetryState> retryState,
      int backoffStartIntervalInSeconds,
      Payloads lastCompletionResult,
      Optional<TestWorkflowMutableState> parent,
      OptionalLong parentChildInitiatedEventId,
      Optional<SignalWorkflowExecutionRequest> signalWithStartSignal,
      WorkflowId workflowId) {
    String namespace = startRequest.getNamespace();
    TestWorkflowMutableState mutableState =
        new TestWorkflowMutableStateImpl(
            startRequest,
            runId,
            retryState,
            backoffStartIntervalInSeconds,
            lastCompletionResult,
            parent,
            parentChildInitiatedEventId,
            continuedExecutionRunId,
            this,
            store);
    WorkflowExecution execution = mutableState.getExecutionId().getExecution();
    ExecutionId executionId = new ExecutionId(namespace, execution);
    executionsByWorkflowId.put(workflowId, mutableState);
    executions.put(executionId, mutableState);
    mutableState.startWorkflow(continuedExecutionRunId.isPresent(), signalWithStartSignal);
    return StartWorkflowExecutionResponse.newBuilder().setRunId(execution.getRunId()).build();
  }

  @Override
  public void getWorkflowExecutionHistory(
      GetWorkflowExecutionHistoryRequest getRequest,
      StreamObserver<GetWorkflowExecutionHistoryResponse> responseObserver) {
    ExecutionId executionId = new ExecutionId(getRequest.getNamespace(), getRequest.getExecution());
    TestWorkflowMutableState mutableState = getMutableState(executionId);
    forkJoinPool.execute(
        () -> {
          try {
            Deadline deadline = Context.current().getDeadline();
            responseObserver.onNext(
                store.getWorkflowExecutionHistory(
                    mutableState.getExecutionId(), getRequest, deadline));
            responseObserver.onCompleted();
          } catch (StatusRuntimeException e) {
            if (e.getStatus().getCode() == Status.Code.INTERNAL) {
              log.error("unexpected", e);
            }
            responseObserver.onError(e);
          } catch (Exception e) {
            log.error("unexpected", e);
            responseObserver.onError(e);
          }
        });
  }

  @Override
  public void pollForDecisionTask(
      PollForDecisionTaskRequest pollRequest,
      StreamObserver<PollForDecisionTaskResponse> responseObserver) {
    Deadline deadline = Context.current().getDeadline();
    Optional<PollForDecisionTaskResponse.Builder> optionalTask =
        store.pollForDecisionTask(pollRequest, deadline);
    if (!optionalTask.isPresent()) {
      responseObserver.onNext(PollForDecisionTaskResponse.getDefaultInstance());
      responseObserver.onCompleted();
      return;
    }
    PollForDecisionTaskResponse.Builder task = optionalTask.get();

    ExecutionId executionId =
        new ExecutionId(pollRequest.getNamespace(), task.getWorkflowExecution());
    TestWorkflowMutableState mutableState = getMutableState(executionId);
    try {
      mutableState.startDecisionTask(task, pollRequest);
      // The task always has the original tasklist is was created on as part of the response. This
      // may different
      // then the task list it was scheduled on as in the case of sticky execution.
      task.setWorkflowExecutionTaskList(mutableState.getStartRequest().getTaskList());
      PollForDecisionTaskResponse response = task.build();
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (StatusRuntimeException e) {
      if (e.getStatus().getCode() == Status.Code.NOT_FOUND) {
        if (log.isDebugEnabled()) {
          log.debug("Skipping outdated decision task for " + executionId, e);
        }
        // The real service doesn't return this call on outdated task.
        // For simplicity we return empty result here.
        responseObserver.onNext(PollForDecisionTaskResponse.getDefaultInstance());
        responseObserver.onCompleted();
      } else {
        if (e.getStatus().getCode() == Status.Code.INTERNAL) {
          log.error("unexpected", e);
        }
        responseObserver.onError(e);
      }
    }
  }

  @Override
  public void respondDecisionTaskCompleted(
      RespondDecisionTaskCompletedRequest request,
      StreamObserver<RespondDecisionTaskCompletedResponse> responseObserver) {
    try {
      DecisionTaskToken taskToken = DecisionTaskToken.fromBytes(request.getTaskToken());
      TestWorkflowMutableState mutableState = getMutableState(taskToken.getExecutionId());
      mutableState.completeDecisionTask(taskToken.getHistorySize(), request);
      responseObserver.onNext(RespondDecisionTaskCompletedResponse.getDefaultInstance());
      responseObserver.onCompleted();
    } catch (StatusRuntimeException e) {
      if (e.getStatus().getCode() == Status.Code.INTERNAL) {
        log.error("unexpected", e);
      }
      responseObserver.onError(e);
    } catch (Throwable e) {
      responseObserver.onError(
          Status.INTERNAL
              .withDescription(Throwables.getStackTraceAsString(e))
              .withCause(e)
              .asRuntimeException());
    }
  }

  @Override
  public void respondDecisionTaskFailed(
      RespondDecisionTaskFailedRequest failedRequest,
      StreamObserver<RespondDecisionTaskFailedResponse> responseObserver) {
    try {
      DecisionTaskToken taskToken = DecisionTaskToken.fromBytes(failedRequest.getTaskToken());
      TestWorkflowMutableState mutableState = getMutableState(taskToken.getExecutionId());
      mutableState.failDecisionTask(failedRequest);
      responseObserver.onNext(RespondDecisionTaskFailedResponse.getDefaultInstance());
      responseObserver.onCompleted();
    } catch (StatusRuntimeException e) {
      if (e.getStatus().getCode() == Status.Code.INTERNAL) {
        log.error("unexpected", e);
      }
      responseObserver.onError(e);
    }
  }

  @Override
  public void pollForActivityTask(
      PollForActivityTaskRequest pollRequest,
      StreamObserver<PollForActivityTaskResponse> responseObserver) {
    while (true) {
      Deadline deadline = Context.current().getDeadline();
      Optional<PollForActivityTaskResponse.Builder> optionalTask =
          store.pollForActivityTask(pollRequest, deadline);
      if (!optionalTask.isPresent()) {
        responseObserver.onNext(PollForActivityTaskResponse.getDefaultInstance());
        responseObserver.onCompleted();
        return;
      }
      PollForActivityTaskResponse.Builder task = optionalTask.get();
      ExecutionId executionId =
          new ExecutionId(pollRequest.getNamespace(), task.getWorkflowExecution());
      TestWorkflowMutableState mutableState = getMutableState(executionId);
      try {
        mutableState.startActivityTask(task, pollRequest);
        responseObserver.onNext(task.build());
        responseObserver.onCompleted();
        return;
      } catch (StatusRuntimeException e) {
        if (e.getStatus().getCode() == Status.Code.NOT_FOUND) {
          if (log.isDebugEnabled()) {
            log.debug("Skipping outdated activity task for " + executionId, e);
          }
          responseObserver.onNext(PollForActivityTaskResponse.getDefaultInstance());
          responseObserver.onCompleted();
        } else {
          if (e.getStatus().getCode() == Status.Code.INTERNAL) {
            log.error("unexpected", e);
          }
          responseObserver.onError(e);
          return;
        }
      }
    }
  }

  @Override
  public void recordActivityTaskHeartbeat(
      RecordActivityTaskHeartbeatRequest heartbeatRequest,
      StreamObserver<RecordActivityTaskHeartbeatResponse> responseObserver) {
    try {
      ActivityId activityId = ActivityId.fromBytes(heartbeatRequest.getTaskToken());
      TestWorkflowMutableState mutableState = getMutableState(activityId.getExecutionId());
      boolean cancelRequested =
          mutableState.heartbeatActivityTask(
              activityId.getScheduledEventId(), heartbeatRequest.getDetails());
      responseObserver.onNext(
          RecordActivityTaskHeartbeatResponse.newBuilder()
              .setCancelRequested(cancelRequested)
              .build());
      responseObserver.onCompleted();
    } catch (StatusRuntimeException e) {
      if (e.getStatus().getCode() == Status.Code.INTERNAL) {
        log.error("unexpected", e);
      }
      responseObserver.onError(e);
    }
  }

  @Override
  public void recordActivityTaskHeartbeatById(
      RecordActivityTaskHeartbeatByIdRequest heartbeatRequest,
      StreamObserver<RecordActivityTaskHeartbeatByIdResponse> responseObserver) {
    try {
      ExecutionId execution =
          new ExecutionId(
              heartbeatRequest.getNamespace(),
              heartbeatRequest.getWorkflowId(),
              heartbeatRequest.getRunId());
      TestWorkflowMutableState mutableState = getMutableState(execution);
      boolean cancelRequested =
          mutableState.heartbeatActivityTaskById(
              heartbeatRequest.getActivityId(), heartbeatRequest.getDetails());
      responseObserver.onNext(
          RecordActivityTaskHeartbeatByIdResponse.newBuilder()
              .setCancelRequested(cancelRequested)
              .build());
      responseObserver.onCompleted();
    } catch (StatusRuntimeException e) {
      if (e.getStatus().getCode() == Status.Code.INTERNAL) {
        log.error("unexpected", e);
      }
      responseObserver.onError(e);
    }
  }

  @Override
  public void respondActivityTaskCompleted(
      RespondActivityTaskCompletedRequest completeRequest,
      StreamObserver<RespondActivityTaskCompletedResponse> responseObserver) {
    try {
      ActivityId activityId = ActivityId.fromBytes(completeRequest.getTaskToken());
      TestWorkflowMutableState mutableState = getMutableState(activityId.getExecutionId());
      mutableState.completeActivityTask(activityId.getScheduledEventId(), completeRequest);
      responseObserver.onNext(RespondActivityTaskCompletedResponse.getDefaultInstance());
      responseObserver.onCompleted();
    } catch (StatusRuntimeException e) {
      if (e.getStatus().getCode() == Status.Code.INTERNAL) {
        log.error("unexpected", e);
      }
      responseObserver.onError(e);
    }
  }

  @Override
  public void respondActivityTaskCompletedById(
      RespondActivityTaskCompletedByIdRequest completeRequest,
      StreamObserver<RespondActivityTaskCompletedByIdResponse> responseObserver) {
    try {
      ExecutionId executionId =
          new ExecutionId(
              completeRequest.getNamespace(),
              completeRequest.getWorkflowId(),
              completeRequest.getRunId());
      TestWorkflowMutableState mutableState = getMutableState(executionId);
      mutableState.completeActivityTaskById(completeRequest.getActivityId(), completeRequest);
      responseObserver.onNext(RespondActivityTaskCompletedByIdResponse.getDefaultInstance());
      responseObserver.onCompleted();
    } catch (StatusRuntimeException e) {
      if (e.getStatus().getCode() == Status.Code.INTERNAL) {
        log.error("unexpected", e);
      }
      responseObserver.onError(e);
    }
  }

  @Override
  public void respondActivityTaskFailed(
      RespondActivityTaskFailedRequest failRequest,
      StreamObserver<RespondActivityTaskFailedResponse> responseObserver) {
    try {
      ActivityId activityId = ActivityId.fromBytes(failRequest.getTaskToken());
      TestWorkflowMutableState mutableState = getMutableState(activityId.getExecutionId());
      mutableState.failActivityTask(activityId.getScheduledEventId(), failRequest);
      responseObserver.onNext(RespondActivityTaskFailedResponse.getDefaultInstance());
      responseObserver.onCompleted();
    } catch (StatusRuntimeException e) {
      if (e.getStatus().getCode() == Status.Code.INTERNAL) {
        log.error("unexpected", e);
      }
      responseObserver.onError(e);
    }
  }

  @Override
  public void respondActivityTaskFailedById(
      RespondActivityTaskFailedByIdRequest failRequest,
      StreamObserver<RespondActivityTaskFailedByIdResponse> responseObserver) {
    try {
      ExecutionId executionId =
          new ExecutionId(
              failRequest.getNamespace(), failRequest.getWorkflowId(), failRequest.getRunId());
      TestWorkflowMutableState mutableState = getMutableState(executionId);
      mutableState.failActivityTaskById(failRequest.getActivityId(), failRequest);
      responseObserver.onNext(RespondActivityTaskFailedByIdResponse.getDefaultInstance());
      responseObserver.onCompleted();
    } catch (StatusRuntimeException e) {
      if (e.getStatus().getCode() == Status.Code.INTERNAL) {
        log.error("unexpected", e);
      }
      responseObserver.onError(e);
    }
  }

  @Override
  public void respondActivityTaskCanceled(
      RespondActivityTaskCanceledRequest canceledRequest,
      StreamObserver<RespondActivityTaskCanceledResponse> responseObserver) {
    try {
      ActivityId activityId = ActivityId.fromBytes(canceledRequest.getTaskToken());
      TestWorkflowMutableState mutableState = getMutableState(activityId.getExecutionId());
      mutableState.cancelActivityTask(activityId.getScheduledEventId(), canceledRequest);
      responseObserver.onNext(RespondActivityTaskCanceledResponse.getDefaultInstance());
      responseObserver.onCompleted();
    } catch (StatusRuntimeException e) {
      if (e.getStatus().getCode() == Status.Code.INTERNAL) {
        log.error("unexpected", e);
      }
      responseObserver.onError(e);
    }
  }

  @Override
  public void respondActivityTaskCanceledById(
      RespondActivityTaskCanceledByIdRequest canceledRequest,
      StreamObserver<RespondActivityTaskCanceledByIdResponse> responseObserver) {
    try {
      ExecutionId executionId =
          new ExecutionId(
              canceledRequest.getNamespace(),
              canceledRequest.getWorkflowId(),
              canceledRequest.getRunId());
      TestWorkflowMutableState mutableState = getMutableState(executionId);
      mutableState.cancelActivityTaskById(canceledRequest.getActivityId(), canceledRequest);
      responseObserver.onNext(RespondActivityTaskCanceledByIdResponse.getDefaultInstance());
      responseObserver.onCompleted();
    } catch (StatusRuntimeException e) {
      if (e.getStatus().getCode() == Status.Code.INTERNAL) {
        log.error("unexpected", e);
      }
      responseObserver.onError(e);
    }
  }

  @Override
  public void requestCancelWorkflowExecution(
      RequestCancelWorkflowExecutionRequest cancelRequest,
      StreamObserver<RequestCancelWorkflowExecutionResponse> responseObserver) {
    try {
      requestCancelWorkflowExecution(cancelRequest, Optional.empty());
      responseObserver.onNext(RequestCancelWorkflowExecutionResponse.getDefaultInstance());
      responseObserver.onCompleted();
    } catch (StatusRuntimeException e) {
      if (e.getStatus().getCode() == Status.Code.INTERNAL) {
        log.error("unexpected", e);
      }
      responseObserver.onError(e);
    }
  }

  void requestCancelWorkflowExecution(
      RequestCancelWorkflowExecutionRequest cancelRequest,
      Optional<TestWorkflowMutableStateImpl.CancelExternalWorkflowExecutionCallerInfo> callerInfo) {
    ExecutionId executionId =
        new ExecutionId(cancelRequest.getNamespace(), cancelRequest.getWorkflowExecution());
    TestWorkflowMutableState mutableState = getMutableState(executionId);
    mutableState.requestCancelWorkflowExecution(cancelRequest, callerInfo);
  }

  @Override
  public void signalWorkflowExecution(
      SignalWorkflowExecutionRequest signalRequest,
      StreamObserver<SignalWorkflowExecutionResponse> responseObserver) {
    try {
      ExecutionId executionId =
          new ExecutionId(signalRequest.getNamespace(), signalRequest.getWorkflowExecution());
      TestWorkflowMutableState mutableState = getMutableState(executionId);
      mutableState.signal(signalRequest);
      responseObserver.onNext(SignalWorkflowExecutionResponse.getDefaultInstance());
      responseObserver.onCompleted();
    } catch (StatusRuntimeException e) {
      if (e.getStatus().getCode() == Status.Code.INTERNAL) {
        log.error("unexpected", e);
      }
      responseObserver.onError(e);
    }
  }

  @Override
  public void signalWithStartWorkflowExecution(
      SignalWithStartWorkflowExecutionRequest r,
      StreamObserver<SignalWithStartWorkflowExecutionResponse> responseObserver) {
    try {
      if (!r.hasTaskList()) {
        throw Status.INVALID_ARGUMENT
            .withDescription("request missing required taskList field")
            .asRuntimeException();
      }
      if (!r.hasWorkflowType()) {
        throw Status.INVALID_ARGUMENT
            .withDescription("request missing required workflowType field")
            .asRuntimeException();
      }
      ExecutionId executionId = new ExecutionId(r.getNamespace(), r.getWorkflowId(), null);
      TestWorkflowMutableState mutableState = getMutableState(executionId, false);
      SignalWorkflowExecutionRequest signalRequest =
          SignalWorkflowExecutionRequest.newBuilder()
              .setInput(r.getSignalInput())
              .setSignalName(r.getSignalName())
              .setWorkflowExecution(executionId.getExecution())
              .setRequestId(r.getRequestId())
              .setControl(r.getControl())
              .setNamespace(r.getNamespace())
              .setIdentity(r.getIdentity())
              .build();
      if (mutableState != null && !mutableState.isTerminalState()) {
        mutableState.signal(signalRequest);
        responseObserver.onNext(
            SignalWithStartWorkflowExecutionResponse.newBuilder()
                .setRunId(mutableState.getExecutionId().getExecution().getRunId())
                .build());
        responseObserver.onCompleted();
        return;
      }
      StartWorkflowExecutionRequest.Builder startRequest =
          StartWorkflowExecutionRequest.newBuilder()
              .setRequestId(r.getRequestId())
              .setInput(r.getInput())
              .setWorkflowExecutionTimeoutSeconds(r.getWorkflowExecutionTimeoutSeconds())
              .setWorkflowRunTimeoutSeconds(r.getWorkflowRunTimeoutSeconds())
              .setWorkflowTaskTimeoutSeconds(r.getWorkflowTaskTimeoutSeconds())
              .setNamespace(r.getNamespace())
              .setTaskList(r.getTaskList())
              .setWorkflowId(r.getWorkflowId())
              .setWorkflowIdReusePolicy(r.getWorkflowIdReusePolicy())
              .setIdentity(r.getIdentity())
              .setWorkflowType(r.getWorkflowType())
              .setCronSchedule(r.getCronSchedule())
              .setRequestId(r.getRequestId());
      if (r.hasRetryPolicy()) {
        startRequest.setRetryPolicy(r.getRetryPolicy());
      }
      if (r.hasHeader()) {
        startRequest.setHeader(r.getHeader());
      }
      if (r.hasMemo()) {
        startRequest.setMemo(r.getMemo());
      }
      if (r.hasSearchAttributes()) {
        startRequest.setSearchAttributes(r.getSearchAttributes());
      }
      StartWorkflowExecutionResponse startResult =
          startWorkflowExecutionImpl(
              startRequest.build(),
              0,
              Optional.empty(),
              OptionalLong.empty(),
              Optional.of(signalRequest));
      responseObserver.onNext(
          SignalWithStartWorkflowExecutionResponse.newBuilder()
              .setRunId(startResult.getRunId())
              .build());
      responseObserver.onCompleted();
    } catch (StatusRuntimeException e) {
      if (e.getStatus().getCode() == Status.Code.INTERNAL) {
        log.error("unexpected", e);
      }
      responseObserver.onError(e);
    }
  }

  public void signalExternalWorkflowExecution(
      String signalId,
      SignalExternalWorkflowExecutionDecisionAttributes a,
      TestWorkflowMutableState source) {
    String namespace;
    if (a.getNamespace().isEmpty()) {
      namespace = source.getExecutionId().getNamespace();
    } else {
      namespace = a.getNamespace();
    }
    ExecutionId executionId = new ExecutionId(namespace, a.getExecution());
    TestWorkflowMutableState mutableState = null;
    try {
      mutableState = getMutableState(executionId);
      mutableState.signalFromWorkflow(a);
      source.completeSignalExternalWorkflowExecution(
          signalId, mutableState.getExecutionId().getExecution().getRunId());
    } catch (StatusRuntimeException e) {
      if (e.getStatus().getCode() == Status.Code.NOT_FOUND) {
        source.failSignalExternalWorkflowExecution(
            signalId,
            SignalExternalWorkflowExecutionFailedCause
                .SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_FAILED_CAUSE_EXTERNAL_WORKFLOW_EXECUTION_NOT_FOUND);
      } else {
        throw e;
      }
    }
  }

  /**
   * Creates next run of a workflow execution
   *
   * @return RunId
   */
  public String continueAsNew(
      StartWorkflowExecutionRequest previousRunStartRequest,
      WorkflowExecutionContinuedAsNewEventAttributes a,
      Optional<RetryState> retryState,
      String identity,
      ExecutionId executionId,
      Optional<TestWorkflowMutableState> parent,
      OptionalLong parentChildInitiatedEventId) {
    StartWorkflowExecutionRequest.Builder startRequestBuilder =
        StartWorkflowExecutionRequest.newBuilder()
            .setRequestId(UUID.randomUUID().toString())
            .setWorkflowType(a.getWorkflowType())
            .setWorkflowRunTimeoutSeconds(a.getWorkflowRunTimeoutSeconds())
            .setWorkflowTaskTimeoutSeconds(a.getWorkflowTaskTimeoutSeconds())
            .setNamespace(executionId.getNamespace())
            .setTaskList(a.getTaskList())
            .setWorkflowId(executionId.getWorkflowId().getWorkflowId())
            .setWorkflowIdReusePolicy(previousRunStartRequest.getWorkflowIdReusePolicy())
            .setIdentity(identity)
            .setCronSchedule(previousRunStartRequest.getCronSchedule());
    if (previousRunStartRequest.hasRetryPolicy()) {
      startRequestBuilder.setRetryPolicy(previousRunStartRequest.getRetryPolicy());
    }
    if (a.hasInput()) {
      startRequestBuilder.setInput(a.getInput());
    }
    StartWorkflowExecutionRequest startRequest = startRequestBuilder.build();
    lock.lock();
    try {
      StartWorkflowExecutionResponse response =
          startWorkflowExecutionNoRunningCheckLocked(
              startRequest,
              a.getNewExecutionRunId(),
              Optional.of(executionId.getExecution().getRunId()),
              retryState,
              a.getBackoffStartIntervalInSeconds(),
              a.getLastCompletionResult(),
              parent,
              parentChildInitiatedEventId,
              Optional.empty(),
              executionId.getWorkflowId());
      return response.getRunId();
    } finally {
      lock.unlock();
    }
  }

  @Override
  public void listOpenWorkflowExecutions(
      ListOpenWorkflowExecutionsRequest listRequest,
      StreamObserver<ListOpenWorkflowExecutionsResponse> responseObserver) {
    try {
      Optional<String> workflowIdFilter;
      if (listRequest.hasExecutionFilter()
          && !listRequest.getExecutionFilter().getWorkflowId().isEmpty()) {
        workflowIdFilter = Optional.of(listRequest.getExecutionFilter().getWorkflowId());
      } else {
        workflowIdFilter = Optional.empty();
      }
      List<WorkflowExecutionInfo> result =
          store.listWorkflows(WorkflowState.OPEN, workflowIdFilter);
      responseObserver.onNext(
          ListOpenWorkflowExecutionsResponse.newBuilder().addAllExecutions(result).build());
      responseObserver.onCompleted();
    } catch (StatusRuntimeException e) {
      if (e.getStatus().getCode() == Status.Code.INTERNAL) {
        log.error("unexpected", e);
      }
      responseObserver.onError(e);
    }
  }

  @Override
  public void listClosedWorkflowExecutions(
      ListClosedWorkflowExecutionsRequest listRequest,
      StreamObserver<ListClosedWorkflowExecutionsResponse> responseObserver) {
    try {
      Optional<String> workflowIdFilter;
      if (listRequest.hasExecutionFilter()
          && !listRequest.getExecutionFilter().getWorkflowId().isEmpty()) {
        workflowIdFilter = Optional.of(listRequest.getExecutionFilter().getWorkflowId());
      } else {
        workflowIdFilter = Optional.empty();
      }
      List<WorkflowExecutionInfo> result =
          store.listWorkflows(WorkflowState.CLOSED, workflowIdFilter);
      responseObserver.onNext(
          ListClosedWorkflowExecutionsResponse.newBuilder().addAllExecutions(result).build());
      responseObserver.onCompleted();
    } catch (StatusRuntimeException e) {
      if (e.getStatus().getCode() == Status.Code.INTERNAL) {
        log.error("unexpected", e);
      }
      responseObserver.onError(e);
    }
  }

  @Override
  public void respondQueryTaskCompleted(
      RespondQueryTaskCompletedRequest completeRequest,
      StreamObserver<RespondQueryTaskCompletedResponse> responseObserver) {
    try {
      QueryId queryId = QueryId.fromBytes(completeRequest.getTaskToken());
      TestWorkflowMutableState mutableState = getMutableState(queryId.getExecutionId());
      mutableState.completeQuery(queryId, completeRequest);
      responseObserver.onNext(RespondQueryTaskCompletedResponse.getDefaultInstance());
      responseObserver.onCompleted();
    } catch (StatusRuntimeException e) {
      if (e.getStatus().getCode() == Status.Code.INTERNAL) {
        log.error("unexpected", e);
      }
      responseObserver.onError(e);
    }
  }

  @Override
  public void queryWorkflow(
      QueryWorkflowRequest queryRequest, StreamObserver<QueryWorkflowResponse> responseObserver) {
    try {
      ExecutionId executionId =
          new ExecutionId(queryRequest.getNamespace(), queryRequest.getExecution());
      TestWorkflowMutableState mutableState = getMutableState(executionId);
      Deadline deadline = Context.current().getDeadline();
      QueryWorkflowResponse result =
          mutableState.query(queryRequest, deadline.timeRemaining(TimeUnit.MILLISECONDS));
      responseObserver.onNext(result);
      responseObserver.onCompleted();
    } catch (StatusRuntimeException e) {
      if (e.getStatus().getCode() == Status.Code.INTERNAL) {
        log.error("unexpected", e);
      }
      responseObserver.onError(e);
    }
  }

  private <R> R requireNotNull(String fieldName, R value) {
    if (value == null) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Missing requried field \"" + fieldName + "\".")
          .asRuntimeException();
    }
    return value;
  }

  /**
   * Adds diagnostic data about internal service state to the provided {@link StringBuilder}.
   * Currently includes histories of all workflow instances stored in the service.
   */
  public void getDiagnostics(StringBuilder result) {
    store.getDiagnostics(result);
  }

  public long currentTimeMillis() {
    return store.getTimer().getClock().getAsLong();
  }

  /** Invokes callback after the specified delay according to internal service clock. */
  public void registerDelayedCallback(Duration delay, Runnable r) {
    store.registerDelayedCallback(delay, r);
  }

  /**
   * Disables time skipping. To enable back call {@link #unlockTimeSkipping(String)}. These calls
   * are counted, so calling unlock does not guarantee that time is going to be skipped immediately
   * as another lock can be holding it.
   */
  public void lockTimeSkipping(String caller) {
    store.getTimer().lockTimeSkipping(caller);
  }

  public void unlockTimeSkipping(String caller) {
    store.getTimer().unlockTimeSkipping(caller);
  }

  /**
   * Blocks calling thread until internal clock doesn't pass the current + duration time. Might not
   * block at all due to time skipping.
   */
  public void sleep(Duration duration) {
    CompletableFuture<Void> result = new CompletableFuture<>();
    store
        .getTimer()
        .schedule(
            duration,
            () -> {
              store.getTimer().lockTimeSkipping("TestWorkflowService sleep");
              result.complete(null);
            },
            "workflow sleep");
    store.getTimer().unlockTimeSkipping("TestWorkflowService sleep");
    try {
      result.get();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } catch (ExecutionException e) {
      throw new RuntimeException(e);
    }
  }
}
