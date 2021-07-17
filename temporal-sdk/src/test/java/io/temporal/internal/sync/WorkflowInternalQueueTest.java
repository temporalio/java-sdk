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

package io.temporal.internal.sync;

import static io.temporal.internal.sync.DeterministicRunner.getDeadlockDetectionTimeout;
import static org.junit.Assert.*;

import io.temporal.client.WorkflowOptions;
import io.temporal.failure.CanceledFailure;
import io.temporal.testing.TestWorkflowEnvironment;
import io.temporal.worker.Worker;
import io.temporal.workflow.QueueConsumer;
import io.temporal.workflow.Workflow;
import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;
import io.temporal.workflow.WorkflowQueue;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.Rule;
import org.junit.Test;

public class WorkflowInternalQueueTest {

  @Rule public final Tracer trace = new Tracer();

  @Test
  public void testTakeBlocking() {
    DeterministicRunner r =
        DeterministicRunner.newRunner(
            () -> {
              WorkflowQueue<Boolean> f = WorkflowInternal.newWorkflowQueue(1);
              trace.add("root begin");
              WorkflowThread.newThread(
                      () -> {
                        trace.add("thread1 begin");
                        assertTrue(f.take());
                        trace.add("thread1 take success");
                      },
                      false)
                  .start();
              WorkflowThread.newThread(
                      () -> {
                        trace.add("thread2 begin");
                        f.put(true);
                        trace.add("thread2 put success");
                      },
                      false)
                  .start();
              trace.add("root done");
            });
    r.runUntilAllBlocked(getDeadlockDetectionTimeout());
    String[] expected =
        new String[] {
          "root begin",
          "root done",
          "thread1 begin",
          "thread2 begin",
          "thread2 put success",
          "thread1 take success",
        };
    trace.setExpected(expected);
    r.close();
  }

  @Test
  public void testTakeCanceled() {
    DeterministicRunner r =
        DeterministicRunner.newRunner(
            () -> {
              WorkflowQueue<Boolean> f = WorkflowInternal.newWorkflowQueue(1);
              trace.add("root begin");
              WorkflowThread.newThread(
                      () -> {
                        trace.add("thread1 begin");
                        try {
                          assertTrue(f.take());
                        } catch (CanceledFailure e) {
                          trace.add("thread1 CanceledException");
                        }
                        trace.add("thread1 done");
                      },
                      false)
                  .start();
              trace.add("root done");
            });
    r.runUntilAllBlocked(getDeadlockDetectionTimeout());
    r.cancel("test");
    r.runUntilAllBlocked(getDeadlockDetectionTimeout());

    String[] expected =
        new String[] {
          "root begin", "root done", "thread1 begin",
        };
    trace.setExpected(expected);
    r.close();
  }

  @Test
  public void testCancellableTakeCanceled() {
    DeterministicRunner r =
        DeterministicRunner.newRunner(
            () -> {
              WorkflowQueue<Boolean> f = WorkflowInternal.newWorkflowQueue(1);
              trace.add("root begin");
              WorkflowThread.newThread(
                      () -> {
                        trace.add("thread1 begin");
                        try {
                          assertTrue(f.cancellableTake());
                        } catch (CanceledFailure e) {
                          trace.add("thread1 CanceledFailure");
                        }
                        trace.add("thread1 done");
                      },
                      false)
                  .start();
              trace.add("root done");
            });
    r.runUntilAllBlocked(getDeadlockDetectionTimeout());
    r.cancel("test");
    r.runUntilAllBlocked(getDeadlockDetectionTimeout());

    String[] expected =
        new String[] {
          "root begin", "root done", "thread1 begin", "thread1 CanceledFailure", "thread1 done",
        };
    trace.setExpected(expected);
  }

  @WorkflowInterface
  public interface WorkflowQueueTestWorkflow {
    @WorkflowMethod
    List<String> test();
  }

  public static class TestPutBlocking implements WorkflowQueueTestWorkflow {

    @Override
    public List<String> test() {
      List<String> trace = new ArrayList<>();

      WorkflowQueue<Boolean> f = WorkflowInternal.newWorkflowQueue(1);
      trace.add("root begin");
      WorkflowThread thread1 =
          WorkflowThread.newThread(
              () -> {
                trace.add("thread1 begin");
                Workflow.sleep(2000);
                assertTrue(f.take());
                trace.add("thread1 take1 success");
                assertFalse(f.take());
                trace.add("thread1 take2 success");
              },
              false);

      thread1.start();
      WorkflowThread thread2 =
          WorkflowThread.newThread(
              () -> {
                trace.add("thread2 begin");
                f.put(true);
                trace.add("thread2 put1 success");
                f.put(false);
                trace.add("thread2 put2 success");
              },
              false);
      thread2.start();
      trace.add("root done");
      Workflow.await(() -> thread1.isDone() && thread2.isDone());
      return trace;
    }
  }

  @Test
  public void testPutBlocking() {
    TestWorkflowEnvironment testEnv = TestWorkflowEnvironment.newInstance();
    String testTaskQueue = "testTaskQueue";
    Worker worker = testEnv.newWorker(testTaskQueue);
    worker.registerWorkflowImplementationTypes(TestPutBlocking.class);
    testEnv.start();
    WorkflowQueueTestWorkflow workflow =
        testEnv
            .getWorkflowClient()
            .newWorkflowStub(
                WorkflowQueueTestWorkflow.class,
                WorkflowOptions.newBuilder().setTaskQueue(testTaskQueue).build());
    List<String> trace = workflow.test();
    List<String> expected =
        Arrays.asList(
            "root begin",
            "root done",
            "thread1 begin",
            "thread2 begin",
            "thread2 put1 success",
            "thread1 take1 success",
            "thread2 put2 success",
            "thread1 take2 success");
    assertEquals(expected, trace);
    testEnv.close();
  }

  public static class TestOfferPollPeek implements WorkflowQueueTestWorkflow {

    @Override
    public List<String> test() {
      List<String> trace = new ArrayList<>();
      WorkflowQueue<Integer> f = WorkflowInternal.newWorkflowQueue(1);
      trace.add("root begin");
      trace.add("peek " + f.peek());
      trace.add("offer " + f.offer(12));
      trace.add("offer " + f.offer(21));
      trace.add("peek " + f.peek());
      trace.add("poll " + f.poll());
      trace.add("offer " + f.offer(23));
      trace.add("offer " + f.offer(34, Duration.ofSeconds(100)));
      trace.add("take " + f.take());
      trace.add("root done");

      return trace;
    }
  }

  @Test
  public void testOfferPollPeek() {
    TestWorkflowEnvironment testEnv = TestWorkflowEnvironment.newInstance();
    String testTaskQueue = "testTaskQueue";
    Worker worker = testEnv.newWorker(testTaskQueue);
    worker.registerWorkflowImplementationTypes(TestOfferPollPeek.class);
    testEnv.start();
    WorkflowQueueTestWorkflow workflow =
        testEnv
            .getWorkflowClient()
            .newWorkflowStub(
                WorkflowQueueTestWorkflow.class,
                WorkflowOptions.newBuilder().setTaskQueue(testTaskQueue).build());
    List<String> trace = workflow.test();
    List<String> expected =
        Arrays.asList(
            "root begin",
            "peek null",
            "offer true",
            "offer false",
            "peek 12",
            "poll 12",
            "offer true",
            "offer false",
            "take 23",
            "root done");
    assertEquals(expected, trace);
    testEnv.close();
  }

  @Test
  public void testPutCanceled() {
    DeterministicRunner r =
        DeterministicRunner.newRunner(
            () -> {
              WorkflowQueue<Boolean> f = WorkflowInternal.newWorkflowQueue(1);
              trace.add("root begin");
              WorkflowThread.newThread(
                      () -> {
                        trace.add("thread1 begin");
                        try {
                          f.put(true);
                          f.put(true);
                        } catch (CanceledFailure e) {
                          trace.add("thread1 CanceledFailure");
                        }
                        trace.add("thread1 done");
                      },
                      false)
                  .start();
              trace.add("root done");
            });
    r.runUntilAllBlocked(getDeadlockDetectionTimeout());
    r.cancel("test");
    r.runUntilAllBlocked(getDeadlockDetectionTimeout());

    String[] expected =
        new String[] {
          "root begin", "root done", "thread1 begin",
        };
    trace.setExpected(expected);
    r.close();
  }

  @Test
  public void testCancellablePutCanceled() {
    DeterministicRunner r =
        DeterministicRunner.newRunner(
            () -> {
              WorkflowQueue<Boolean> f = WorkflowInternal.newWorkflowQueue(1);
              trace.add("root begin");
              WorkflowThread.newThread(
                      () -> {
                        trace.add("thread1 begin");
                        try {
                          f.put(true);
                          f.cancellablePut(true);
                        } catch (CanceledFailure e) {
                          trace.add("thread1 CanceledFailure");
                        }
                        trace.add("thread1 done");
                      },
                      false)
                  .start();
              trace.add("root done");
            });
    r.runUntilAllBlocked(getDeadlockDetectionTimeout());
    r.cancel("test");
    r.runUntilAllBlocked(getDeadlockDetectionTimeout());

    String[] expected =
        new String[] {
          "root begin", "root done", "thread1 begin", "thread1 CanceledFailure", "thread1 done",
        };
    trace.setExpected(expected);
    r.close();
  }

  @Test
  public void testMap() {
    DeterministicRunner r =
        DeterministicRunner.newRunner(
            () -> {
              WorkflowQueue<Integer> queue = WorkflowInternal.newWorkflowQueue(1);
              trace.add("root begin");
              WorkflowThread.newThread(
                      () -> {
                        QueueConsumer<String> mapped = queue.map((s) -> s + "-mapped");
                        trace.add("thread1 begin");
                        for (int i = 0; i < 10; i++) {
                          trace.add("thread1 " + mapped.take());
                        }
                        trace.add("thread1 done");
                      },
                      false)
                  .start();
              trace.add("root thread1 started");
              for (int i = 0; i < 10; i++) {
                queue.put(i);
              }
              trace.add("root done");
            });
    r.runUntilAllBlocked(getDeadlockDetectionTimeout());
    r.cancel("test");
    r.runUntilAllBlocked(getDeadlockDetectionTimeout());

    String[] expected =
        new String[] {
          "root begin",
          "root thread1 started",
          "thread1 begin",
          "thread1 0-mapped",
          "thread1 1-mapped",
          "thread1 2-mapped",
          "thread1 3-mapped",
          "thread1 4-mapped",
          "thread1 5-mapped",
          "thread1 6-mapped",
          "thread1 7-mapped",
          "thread1 8-mapped",
          "root done",
          "thread1 9-mapped",
          "thread1 done",
        };
    trace.setExpected(expected);
    r.close();
  }

  @Test
  public void testQueueOrder() {
    WorkflowQueue<Integer> queue = WorkflowInternal.newWorkflowQueue(3);
    int[] result = new int[3];
    DeterministicRunner r =
        DeterministicRunner.newRunner(
            () -> {
              queue.put(1);
              queue.put(2);
              queue.put(3);
              result[0] = queue.take();
              result[1] = queue.poll();
              result[2] = queue.poll();
            });
    r.runUntilAllBlocked(getDeadlockDetectionTimeout());
    r.cancel("test");
    r.runUntilAllBlocked(getDeadlockDetectionTimeout());

    int[] expected = new int[] {1, 2, 3};
    assertArrayEquals(expected, result);

    r.close();
  }
}
