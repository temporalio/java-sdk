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

package io.temporal.workflow.versionTests;

import static io.temporal.api.enums.v1.EventType.EVENT_TYPE_WORKFLOW_TASK_FAILED;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowStub;
import io.temporal.worker.WorkerOptions;
import io.temporal.workflow.CancellationScope;
import io.temporal.workflow.Promise;
import io.temporal.workflow.SignalMethod;
import io.temporal.workflow.Workflow;
import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;
import io.temporal.workflow.WorkflowQueue;
import io.temporal.workflow.shared.SDKTestWorkflowRule;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.StringJoiner;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;

public class GetVersionAndCancelTimerTest {

  @Rule
  public SDKTestWorkflowRule testWorkflowRule =
      SDKTestWorkflowRule.newBuilder()
          .setWorkflowTypes(ReminderWorkflowImpl.class)
          .setWorkerOptions(WorkerOptions.newBuilder().build())
          .setInitialTime(Instant.parse("2020-01-01T00:00:00Z"))
          .build();

  @Test
  public void testGetVersionAndCancelTimer() {
    ReminderWorkflow workflowStub =
        testWorkflowRule.newWorkflowStubTimeoutOptions(ReminderWorkflow.class);

    WorkflowClient.start(workflowStub::start);

    Instant now = now();

    workflowStub.scheduleReminder(
        new ScheduleReminderSignal(now.plusSeconds(2), "Reminder 1 in 2s"));
    workflowStub.scheduleReminder(
        new ScheduleReminderSignal(now.plusSeconds(4), "Reminder 2 in 4s"));

    WorkflowStub untypedWorkflowStub = WorkflowStub.fromTyped(workflowStub);

    untypedWorkflowStub.getResult(Void.TYPE);

    testWorkflowRule.assertNoHistoryEvent(
        untypedWorkflowStub.getExecution(), EVENT_TYPE_WORKFLOW_TASK_FAILED);
  }

  private Instant now() {
    return Instant.ofEpochMilli(testWorkflowRule.getTestEnvironment().currentTimeMillis());
  }

  @WorkflowInterface
  public interface ReminderWorkflow {

    @WorkflowMethod
    void start();

    @SignalMethod
    void scheduleReminder(ScheduleReminderSignal signal);
  }

  public static final class ScheduleReminderSignal {

    private final Instant reminderTime;
    private final String reminderText;

    @JsonCreator
    public ScheduleReminderSignal(
        @JsonProperty("reminderTime") Instant reminderTime,
        @JsonProperty("reminderText") String reminderText) {
      this.reminderTime = reminderTime;
      this.reminderText = reminderText;
    }

    @JsonProperty("reminderTime")
    public Instant getReminderTime() {
      return reminderTime;
    }

    @JsonProperty("reminderText")
    public String getReminderText() {
      return reminderText;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      ScheduleReminderSignal that = (ScheduleReminderSignal) o;
      return Objects.equals(reminderTime, that.reminderTime)
          && Objects.equals(reminderText, that.reminderText);
    }

    @Override
    public int hashCode() {
      return Objects.hash(reminderTime, reminderText);
    }

    @Override
    public String toString() {
      return new StringJoiner(", ", ScheduleReminderSignal.class.getSimpleName() + "[", "]")
          .add("reminderTime=" + reminderTime)
          .add("reminderText='" + reminderText + "'")
          .toString();
    }
  }

  public static final class ReminderWorkflowImpl implements ReminderWorkflow {

    private static final Logger logger = Workflow.getLogger(ReminderWorkflowImpl.class);

    private final WorkflowQueue<ScheduleReminderSignal> signalQueue = Workflow.newWorkflowQueue(10);

    private Promise<Void> activeReminder;
    private CancellationScope activeScope;

    @Override
    public void start() {
      logger.info("<{}> Workflow started", now());
      ScheduleReminderSignal signalToProcess = signalQueue.take();
      while (signalToProcess != null) {
        logger.info("<{}> Will process signal {}", now(), signalToProcess);
        processScheduleReminder(signalToProcess);

        Workflow.await(
            () ->
                signalQueue.peek() != null
                    || activeReminder == null
                    || activeReminder.isCompleted());

        signalToProcess = signalQueue.poll();
      }
      logger.info("<{}> Workflow completed", now());
    }

    @Override
    public void scheduleReminder(ScheduleReminderSignal signal) {
      logger.info("<{}> Got signal {}", now(), signal);
      signalQueue.put(signal);
      logger.info("<{}> Enqueued signal {}", now(), signal);
    }

    private void processScheduleReminder(ScheduleReminderSignal signal) {
      if (activeScope != null) {
        logger.info("<{}> Cancelling previous reminder", now());
        activeScope.cancel("New reminder");
        if (activeReminder != null) {
          try {
            // Consume the cancelled promise to avoid noisy warnings
            activeReminder.get();
          } catch (Exception ignored) {
          }
        }
      }

      activeScope =
          Workflow.newCancellationScope(
              () -> {
                Duration reminderSleepDuration = Duration.between(now(), signal.getReminderTime());
                if (reminderSleepDuration.isNegative()) {
                  logger.info(
                      "<{}> Got a request {} for an outdated reminder, ignoring it", now(), signal);
                  activeReminder = null;
                  return;
                }
                logger.info(
                    "<{}> Scheduled a reminder for time {}", now(), signal.getReminderTime());
                activeReminder =
                    Workflow.newTimer(reminderSleepDuration)
                        .thenApply(
                            (t) -> {
                              logger.info("<{}> Reminder: {}", now(), signal.getReminderText());
                              return null;
                            });
              });
      activeScope.run();

      Workflow.getVersion("some-change", Workflow.DEFAULT_VERSION, 1);
    }

    private Instant now() {
      return Instant.ofEpochMilli(Workflow.currentTimeMillis());
    }
  }
}
