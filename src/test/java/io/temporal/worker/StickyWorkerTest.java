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

package io.temporal.worker;

import static io.temporal.workflow.WorkflowTest.DOMAIN;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import com.uber.m3.tally.RootScopeBuilder;
import com.uber.m3.tally.Scope;
import com.uber.m3.tally.StatsReporter;
import com.uber.m3.util.ImmutableMap;
import io.temporal.activity.ActivityMethod;
import io.temporal.activity.ActivityOptions;
import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowOptions;
import io.temporal.internal.metrics.MetricsTag;
import io.temporal.internal.metrics.MetricsType;
import io.temporal.internal.replay.DeciderCache;
import io.temporal.testing.TestEnvironmentOptions;
import io.temporal.testing.TestWorkflowEnvironment;
import io.temporal.workflow.Async;
import io.temporal.workflow.CompletablePromise;
import io.temporal.workflow.Promise;
import io.temporal.workflow.QueryMethod;
import io.temporal.workflow.SignalMethod;
import io.temporal.workflow.Workflow;
import io.temporal.workflow.WorkflowMethod;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.junit.*;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(Parameterized.class)
public class StickyWorkerTest {

  private static final boolean useDockerService =
      Boolean.parseBoolean(System.getenv("USE_DOCKER_SERVICE"));

  @Parameterized.Parameter public boolean useExternalService;

  @Parameterized.Parameters(name = "{1}")
  public static Object[] data() {
    if (!useDockerService) {
      return new Object[][] {{false, "TestService"}};
    } else {
      return new Object[][] {{true, "Docker"}};
    }
  }

  @Parameterized.Parameter(1)
  public String testType;

  @Rule public TestName testName = new TestName();

  private static WorkflowServiceTChannel service;

  @BeforeClass
  public static void setUp() {
    if (useDockerService) {
      service = new WorkflowServiceTChannel();
    }
  }

  @AfterClass
  public static void tearDown() {
    if (service != null) {
      service.close();
    }
  }

  @Test
  public void whenStickyIsEnabledThenTheWorkflowIsCachedSignals() throws Exception {
    // Arrange
    String taskListName = "cachedStickyTest_Signal";

    StatsReporter reporter = mock(StatsReporter.class);
    Scope scope =
        new RootScopeBuilder()
            .reporter(reporter)
            .reportEvery(com.uber.m3.util.Duration.ofMillis(300));

    TestEnvironmentWrapper wrapper =
        new TestEnvironmentWrapper(
            new Worker.FactoryOptions.Builder()
                .setDisableStickyExecution(false)
                .setMetricScope(scope)
                .build());
    Worker.Factory factory = wrapper.getWorkerFactory();
    Worker worker = factory.newWorker(taskListName, new WorkerOptions.Builder().build());
    worker.registerWorkflowImplementationTypes(GreetingSignalWorkflowImpl.class);
    factory.start();

    WorkflowOptions workflowOptions =
        new WorkflowOptions.Builder()
            .setTaskList(taskListName)
            .setExecutionStartToCloseTimeout(Duration.ofDays(30))
            .setTaskStartToCloseTimeout(Duration.ofSeconds(30))
            .build();
    GreetingSignalWorkflow workflow =
        wrapper.getWorkflowClient().newWorkflowStub(GreetingSignalWorkflow.class, workflowOptions);

    // Act
    WorkflowClient.start(workflow::getGreeting);
    Thread.sleep(300);
    workflow.waitForName("World");
    String greeting = workflow.getGreeting();
    assertEquals("Hello World!", greeting);

    // Assert
    DeciderCache cache = factory.getCache();
    assertNotNull(cache);
    assertEquals(1, cache.size());

    // Verify the workflow succeeded without having to recover from a failure
    Map<String, String> tags =
        new ImmutableMap.Builder<String, String>(2)
            .put(MetricsTag.DOMAIN, DOMAIN)
            .put(MetricsTag.TASK_LIST, factory.getHostName())
            .build();
    Thread.sleep(600);
    verify(reporter, atLeastOnce())
        .reportCounter(eq(MetricsType.STICKY_CACHE_HIT), eq(tags), anyInt());
    verify(reporter, never()).reportCounter(eq(MetricsType.STICKY_CACHE_MISS), eq(tags), anyInt());

    // Finish Workflow
    wrapper.close();
  }

  @Test
  public void workflowCacheEvictionDueToThreads() {
    // Arrange
    String taskListName = "workflowCacheEvictionDueToThreads";

    StatsReporter reporter = mock(StatsReporter.class);
    Scope scope =
        new RootScopeBuilder()
            .reporter(reporter)
            .reportEvery(com.uber.m3.util.Duration.ofMillis(300));

    TestEnvironmentWrapper wrapper =
        new TestEnvironmentWrapper(
            new Worker.FactoryOptions.Builder()
                .setDisableStickyExecution(false)
                .setMetricScope(scope)
                .setMaxWorkflowThreadCount(10)
                .setCacheMaximumSize(100)
                .build());
    Worker.Factory factory = wrapper.getWorkerFactory();
    Worker worker =
        factory.newWorker(
            taskListName,
            new WorkerOptions.Builder().setMaxConcurrentWorkflowExecutionSize(5).build());
    worker.registerWorkflowImplementationTypes(ActivitiesWorkflowImpl.class);
    worker.registerActivitiesImplementations(new ActivitiesImpl());
    factory.start();

    WorkflowOptions workflowOptions =
        new WorkflowOptions.Builder()
            .setTaskList(taskListName)
            .setExecutionStartToCloseTimeout(Duration.ofDays(30))
            .setTaskStartToCloseTimeout(Duration.ofSeconds(1))
            .build();

    int count = 100;
    ActivitiesWorkflow[] workflows = new ActivitiesWorkflow[count];
    WorkflowParams w = new WorkflowParams();
    w.TemporalSleep = Duration.ofSeconds(1);
    w.ChainSequence = 2;
    w.ConcurrentCount = 1;
    w.PayloadSizeBytes = 10;
    w.TaskListName = taskListName;
    for (int i = 0; i < count; i++) {
      ActivitiesWorkflow workflow =
          wrapper.getWorkflowClient().newWorkflowStub(ActivitiesWorkflow.class, workflowOptions);
      workflows[i] = workflow;
      WorkflowClient.start(workflow::execute, w);
    }

    for (int i = 0; i < count; i++) {
      workflows[i].execute(w);
    }

    // Finish Workflow
    wrapper.close();
  }

  @Test
  public void whenStickyIsEnabledThenTheWorkflowIsCachedActivities() throws Exception {
    // Arrange
    String taskListName = "cachedStickyTest_Activities";

    StatsReporter reporter = mock(StatsReporter.class);
    Scope scope =
        new RootScopeBuilder()
            .reporter(reporter)
            .reportEvery(com.uber.m3.util.Duration.ofMillis(300));

    TestEnvironmentWrapper wrapper =
        new TestEnvironmentWrapper(
            new Worker.FactoryOptions.Builder()
                .setDisableStickyExecution(false)
                .setMetricScope(scope)
                .build());
    Worker.Factory factory = wrapper.getWorkerFactory();
    Worker worker = factory.newWorker(taskListName, new WorkerOptions.Builder().build());
    worker.registerWorkflowImplementationTypes(ActivitiesWorkflowImpl.class);
    worker.registerActivitiesImplementations(new ActivitiesImpl());
    factory.start();

    WorkflowOptions workflowOptions =
        new WorkflowOptions.Builder()
            .setTaskList(taskListName)
            .setExecutionStartToCloseTimeout(Duration.ofDays(30))
            .setTaskStartToCloseTimeout(Duration.ofSeconds(30))
            .build();
    ActivitiesWorkflow workflow =
        wrapper.getWorkflowClient().newWorkflowStub(ActivitiesWorkflow.class, workflowOptions);

    // Act
    WorkflowParams w = new WorkflowParams();
    w.TemporalSleep = Duration.ofSeconds(1);
    w.ChainSequence = 2;
    w.ConcurrentCount = 1;
    w.PayloadSizeBytes = 10;
    w.TaskListName = taskListName;
    workflow.execute(w);

    // Wait for reporter
    Thread.sleep(600);

    // Verify the workflow succeeded without having to recover from a failure
    Map<String, String> tags =
        new ImmutableMap.Builder<String, String>(2)
            .put(MetricsTag.DOMAIN, DOMAIN)
            .put(MetricsTag.TASK_LIST, factory.getHostName())
            .build();
    verify(reporter, atLeastOnce())
        .reportCounter(eq(MetricsType.STICKY_CACHE_HIT), eq(tags), anyInt());
    verify(reporter, never()).reportCounter(eq(MetricsType.STICKY_CACHE_MISS), eq(tags), anyInt());

    // Finish Workflow
    wrapper.close();
  }

  @Test
  public void whenStickyIsEnabledThenTheWorkflowIsCachedChildWorkflows() throws Exception {
    // Arrange
    String taskListName = "cachedStickyTest_ChildWorkflows";

    StatsReporter reporter = mock(StatsReporter.class);
    Scope scope =
        new RootScopeBuilder()
            .reporter(reporter)
            .reportEvery(com.uber.m3.util.Duration.ofMillis(300));

    TestEnvironmentWrapper wrapper =
        new TestEnvironmentWrapper(
            new Worker.FactoryOptions.Builder()
                .setDisableStickyExecution(false)
                .setMetricScope(scope)
                .build());
    Worker.Factory factory = wrapper.getWorkerFactory();
    Worker worker = factory.newWorker(taskListName, new WorkerOptions.Builder().build());
    worker.registerWorkflowImplementationTypes(
        GreetingParentWorkflowImpl.class, GreetingChildImpl.class);
    factory.start();

    WorkflowOptions workflowOptions =
        new WorkflowOptions.Builder()
            .setTaskList(taskListName)
            .setExecutionStartToCloseTimeout(Duration.ofDays(30))
            .setTaskStartToCloseTimeout(Duration.ofSeconds(30))
            .build();
    GreetingParentWorkflow workflow =
        wrapper.getWorkflowClient().newWorkflowStub(GreetingParentWorkflow.class, workflowOptions);

    // Act
    Assert.assertEquals("Hello World!", workflow.getGreeting("World"));

    // Wait for reporter
    Thread.sleep(600);

    // Verify the workflow succeeded without having to recover from a failure
    Map<String, String> tags =
        new ImmutableMap.Builder<String, String>(2)
            .put(MetricsTag.DOMAIN, DOMAIN)
            .put(MetricsTag.TASK_LIST, factory.getHostName())
            .build();
    verify(reporter, atLeastOnce())
        .reportCounter(eq(MetricsType.STICKY_CACHE_HIT), eq(tags), anyInt());
    verify(reporter, never()).reportCounter(eq(MetricsType.STICKY_CACHE_MISS), eq(tags), anyInt());
    // Finish Workflow
    wrapper.close();
  }

  @Test
  public void whenStickyIsEnabledThenTheWorkflowIsCachedMutableSideEffect() throws Exception {
    // Arrange
    String taskListName = "cachedStickyTest_MutableSideEffect";

    StatsReporter reporter = mock(StatsReporter.class);
    Scope scope =
        new RootScopeBuilder()
            .reporter(reporter)
            .reportEvery(com.uber.m3.util.Duration.ofMillis(300));

    TestEnvironmentWrapper wrapper =
        new TestEnvironmentWrapper(
            new Worker.FactoryOptions.Builder()
                .setDisableStickyExecution(false)
                .setMetricScope(scope)
                .build());
    Worker.Factory factory = wrapper.getWorkerFactory();
    Worker worker = factory.newWorker(taskListName, new WorkerOptions.Builder().build());
    worker.registerWorkflowImplementationTypes(TestMutableSideEffectWorkflowImpl.class);
    factory.start();

    WorkflowOptions workflowOptions =
        new WorkflowOptions.Builder()
            .setTaskList(taskListName)
            .setExecutionStartToCloseTimeout(Duration.ofDays(30))
            .setTaskStartToCloseTimeout(Duration.ofSeconds(30))
            .build();
    TestMutableSideEffectWorkflow workflow =
        wrapper
            .getWorkflowClient()
            .newWorkflowStub(TestMutableSideEffectWorkflow.class, workflowOptions);

    // Act
    ArrayDeque<Long> values = new ArrayDeque<>();
    values.add(1234L);
    values.add(1234L);
    values.add(123L); // expected to be ignored as it is smaller than 1234.
    values.add(3456L);
    mutableSideEffectValue.put(taskListName, values);
    String result = workflow.execute(taskListName);
    assertEquals("1234, 1234, 1234, 3456", result);

    // Wait for reporter
    Thread.sleep(600);

    // Verify the workflow succeeded without having to recover from a failure
    Map<String, String> tags =
        new ImmutableMap.Builder<String, String>(2)
            .put(MetricsTag.DOMAIN, DOMAIN)
            .put(MetricsTag.TASK_LIST, factory.getHostName())
            .build();
    verify(reporter, atLeastOnce())
        .reportCounter(eq(MetricsType.STICKY_CACHE_HIT), eq(tags), anyInt());
    verify(reporter, never()).reportCounter(eq(MetricsType.STICKY_CACHE_MISS), eq(tags), anyInt());
    // Finish Workflow
    wrapper.close();
  }

  @Test
  public void whenStickyIsNotEnabledThenTheWorkflowIsNotCached() {
    // Arrange
    String taskListName = "notCachedStickyTest";
    TestEnvironmentWrapper wrapper =
        new TestEnvironmentWrapper(
            new Worker.FactoryOptions.Builder().setDisableStickyExecution(true).build());
    Worker.Factory factory = wrapper.getWorkerFactory();
    Worker worker = factory.newWorker(taskListName);
    worker.registerWorkflowImplementationTypes(GreetingSignalWorkflowImpl.class);
    factory.start();

    WorkflowOptions workflowOptions =
        new WorkflowOptions.Builder()
            .setTaskList(taskListName)
            .setExecutionStartToCloseTimeout(Duration.ofDays(30))
            .setTaskStartToCloseTimeout(Duration.ofSeconds(30))
            .build();
    GreetingSignalWorkflow workflow =
        wrapper.getWorkflowClient().newWorkflowStub(GreetingSignalWorkflow.class, workflowOptions);

    // Act
    WorkflowClient.start(workflow::getGreeting);
    workflow.waitForName("World");
    String greeting = workflow.getGreeting();
    assertEquals("Hello World!", greeting);

    // Assert
    DeciderCache cache = factory.getCache();
    assertNull(cache);
    wrapper.close();
  }

  @Test
  public void whenCacheIsEvictedTheWorkerCanRecover() throws Exception {
    // Arrange
    String taskListName = "evictedStickyTest";
    TestEnvironmentWrapper wrapper =
        new TestEnvironmentWrapper(
            new Worker.FactoryOptions.Builder().setDisableStickyExecution(false).build());
    Worker.Factory factory = wrapper.getWorkerFactory();
    Worker worker = factory.newWorker(taskListName);
    worker.registerWorkflowImplementationTypes(GreetingSignalWorkflowImpl.class);
    factory.start();

    WorkflowOptions workflowOptions =
        new WorkflowOptions.Builder()
            .setTaskList(taskListName)
            .setExecutionStartToCloseTimeout(Duration.ofDays(30))
            .setTaskStartToCloseTimeout(Duration.ofSeconds(30))
            .build();
    GreetingSignalWorkflow workflow =
        wrapper.getWorkflowClient().newWorkflowStub(GreetingSignalWorkflow.class, workflowOptions);

    // Act
    WorkflowClient.start(workflow::getGreeting);

    Thread.sleep(200); // Wait for workflow to start

    DeciderCache cache = factory.getCache();
    assertNotNull(cache);
    assertEquals(1, cache.size());
    cache.invalidateAll();
    assertEquals(0, cache.size());

    workflow.waitForName("World");
    String greeting = workflow.getGreeting();

    // Assert
    assertEquals("Hello World!", greeting);
    wrapper.close();
  }

  @Test
  public void workflowsCanBeQueried() throws Exception {
    // Arrange
    String taskListName = "queryStickyTest";
    TestEnvironmentWrapper wrapper =
        new TestEnvironmentWrapper(
            new Worker.FactoryOptions.Builder().setDisableStickyExecution(false).build());
    Worker.Factory factory = wrapper.getWorkerFactory();
    Worker worker = factory.newWorker(taskListName);
    worker.registerWorkflowImplementationTypes(GreetingSignalWorkflowImpl.class);
    factory.start();

    WorkflowOptions workflowOptions =
        new WorkflowOptions.Builder()
            .setTaskList(taskListName)
            .setExecutionStartToCloseTimeout(Duration.ofDays(30))
            .setTaskStartToCloseTimeout(Duration.ofSeconds(30))
            .build();
    GreetingSignalWorkflow workflow =
        wrapper.getWorkflowClient().newWorkflowStub(GreetingSignalWorkflow.class, workflowOptions);

    // Act
    WorkflowClient.start(workflow::getGreeting);

    Thread.sleep(200); // Wait for workflow to start

    DeciderCache cache = factory.getCache();
    assertNotNull(cache);
    assertEquals(1, cache.size());

    // Assert
    assertEquals(workflow.getProgress(), GreetingSignalWorkflow.Status.WAITING_FOR_NAME);

    workflow.waitForName("World");
    String greeting = workflow.getGreeting();

    assertEquals("Hello World!", greeting);
    assertEquals(workflow.getProgress(), GreetingSignalWorkflow.Status.GREETING_GENERATED);
    wrapper.close();
  }

  @Test
  public void workflowsCanBeQueriedAfterEviction() throws Exception {
    // Arrange
    String taskListName = "queryEvictionStickyTest";
    TestEnvironmentWrapper wrapper =
        new TestEnvironmentWrapper(
            new Worker.FactoryOptions.Builder().setDisableStickyExecution(false).build());
    Worker.Factory factory = wrapper.getWorkerFactory();
    Worker worker = factory.newWorker(taskListName);
    worker.registerWorkflowImplementationTypes(GreetingSignalWorkflowImpl.class);
    factory.start();

    WorkflowOptions workflowOptions =
        new WorkflowOptions.Builder()
            .setTaskList(taskListName)
            .setExecutionStartToCloseTimeout(Duration.ofDays(30))
            .setTaskStartToCloseTimeout(Duration.ofSeconds(30))
            .build();
    GreetingSignalWorkflow workflow =
        wrapper.getWorkflowClient().newWorkflowStub(GreetingSignalWorkflow.class, workflowOptions);

    // Act
    WorkflowClient.start(workflow::getGreeting);

    Thread.sleep(200); // Wait for workflow to start

    DeciderCache cache = factory.getCache();
    assertNotNull(cache);
    assertEquals(1, cache.size());
    cache.invalidateAll();
    assertEquals(0, cache.size());

    // Assert
    assertEquals(workflow.getProgress(), GreetingSignalWorkflow.Status.WAITING_FOR_NAME);

    workflow.waitForName("World");
    String greeting = workflow.getGreeting();

    assertEquals("Hello World!", greeting);
    assertEquals(workflow.getProgress(), GreetingSignalWorkflow.Status.GREETING_GENERATED);
    wrapper.close();
  }

  // Todo: refactor TestEnvironment to toggle between real and test service.
  private class TestEnvironmentWrapper {

    private TestWorkflowEnvironment testEnv;
    private Worker.Factory factory;

    public TestEnvironmentWrapper(Worker.FactoryOptions options) {
      if (options == null) {
        options = new Worker.FactoryOptions.Builder().setDisableStickyExecution(false).build();
      }
      if (useExternalService) {
        factory = new Worker.Factory(service, DOMAIN, options);
      } else {
        TestEnvironmentOptions testOptions =
            new TestEnvironmentOptions.Builder()
                .setDomain(DOMAIN)
                .setFactoryOptions(options)
                .build();
        testEnv = TestWorkflowEnvironment.newInstance(testOptions);
      }
    }

    private Worker.Factory getWorkerFactory() {
      return useExternalService ? factory : testEnv.getWorkerFactory();
    }

    private WorkflowClient getWorkflowClient() {
      return useExternalService
          ? WorkflowClient.newInstance(service, DOMAIN)
          : testEnv.newWorkflowClient();
    }

    private void close() {
      if (useExternalService) {
        factory.shutdown();
        factory.awaitTermination(1, TimeUnit.SECONDS);
      } else {
        testEnv.close();
      }
    }
  }

  public static class WorkflowParams {

    public int ChainSequence;
    public int ConcurrentCount;
    public String TaskListName;
    public int PayloadSizeBytes;
    public Duration TemporalSleep; // nano
  }

  public interface GreetingSignalWorkflow {
    /** @return greeting string */
    @QueryMethod
    Status getProgress();

    /** @return greeting string */
    @WorkflowMethod
    String getGreeting();

    /** Receives name through an external signal. */
    @SignalMethod
    void waitForName(String name);

    enum Status {
      WAITING_FOR_NAME,
      GREETING_GENERATED
    }
  }

  /** GreetingSignalWorkflow implementation that returns a greeting. */
  public static class GreetingSignalWorkflowImpl implements GreetingSignalWorkflow {

    private final CompletablePromise<String> name = Workflow.newPromise();
    private Status status = Status.WAITING_FOR_NAME;

    @Override
    public Status getProgress() {
      return status;
    }

    @Override
    public String getGreeting() {
      String greeting = "Hello " + name.get() + "!";
      status = Status.GREETING_GENERATED;
      return greeting;
    }

    @Override
    public void waitForName(String name) {
      this.name.complete(name);
    }
  }

  public interface GreetingParentWorkflow {
    @WorkflowMethod(executionStartToCloseTimeoutSeconds = 10)
    String getGreeting(String name);
  }

  public interface GreetingChild {
    @WorkflowMethod
    String composeGreeting(String greeting, String name);
  }

  public static class GreetingParentWorkflowImpl implements GreetingParentWorkflow {

    @Override
    public String getGreeting(String name) {
      // Workflows are stateful. So a new stub must be created for each new child.
      GreetingChild child = Workflow.newChildWorkflowStub(GreetingChild.class);

      // This is a blocking call that returns only after the child has completed.
      Promise<String> greeting = Async.function(child::composeGreeting, "Hello", name);
      // Do something else here.
      return greeting.get(); // blocks waiting for the child to complete.
    }
  }

  public static class GreetingChildImpl implements GreetingChild {
    @Override
    public String composeGreeting(String greeting, String name) {
      return greeting + " " + name + "!";
    }
  }

  public interface ActivitiesWorkflow {

    @WorkflowMethod()
    void execute(WorkflowParams params);
  }

  public static class ActivitiesWorkflowImpl implements ActivitiesWorkflow {

    @Override
    public void execute(WorkflowParams params) {
      SleepActivity activity =
          Workflow.newActivityStub(
              SleepActivity.class,
              new ActivityOptions.Builder()
                  .setTaskList(params.TaskListName)
                  .setScheduleToStartTimeout(Duration.ofMinutes(1))
                  .setStartToCloseTimeout(Duration.ofMinutes(1))
                  .setHeartbeatTimeout(Duration.ofSeconds(20))
                  .build());

      for (int i = 0; i < params.ChainSequence; i++) {
        List<Promise<Void>> promises = new ArrayList<>();
        for (int j = 0; j < params.ConcurrentCount; j++) {
          byte[] bytes = new byte[params.PayloadSizeBytes];
          new Random().nextBytes(bytes);
          Promise<Void> promise = Async.procedure(activity::sleep, i, j, bytes);
          promises.add(promise);
        }

        for (Promise<Void> promise : promises) {
          promise.get();
        }

        Workflow.sleep(params.TemporalSleep);
      }
    }
  }

  public interface SleepActivity {

    @ActivityMethod()
    void sleep(int chain, int concurrency, byte[] bytes);
  }

  public static class ActivitiesImpl implements SleepActivity {
    private static final Logger log = LoggerFactory.getLogger("sleep-activity");

    @Override
    public void sleep(int chain, int concurrency, byte[] bytes) {
      log.info("sleep called");
    }
  }

  public interface TestMutableSideEffectWorkflow {

    @WorkflowMethod
    String execute(String taskList);
  }

  private static final Map<String, Queue<Long>> mutableSideEffectValue =
      Collections.synchronizedMap(new HashMap<>());

  public static class TestMutableSideEffectWorkflowImpl implements TestMutableSideEffectWorkflow {

    @Override
    public String execute(String taskList) {
      StringBuilder result = new StringBuilder();
      for (int i = 0; i < 4; i++) {
        long value =
            Workflow.mutableSideEffect(
                "id1",
                Long.class,
                (o, n) -> n > o,
                () -> mutableSideEffectValue.get(taskList).poll());
        if (result.length() > 0) {
          result.append(", ");
        }
        result.append(value);
        // Sleep is here to ensure that mutableSideEffect works when replaying a history.
        if (i >= 3) {
          Workflow.sleep(Duration.ofSeconds(1));
        }
      }
      return result.toString();
    }
  }
}
