/*-
 * -\-\-
 * Spotify Styx Scheduler Service
 * --
 * Copyright (C) 2016 Spotify AB
 * --
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * -/-/-
 */

package com.spotify.styx.monitoring;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.spotify.metrics.core.MetricId;
import com.spotify.metrics.core.SemanticMetricRegistry;
import com.spotify.styx.model.WorkflowId;
import com.spotify.styx.state.RunState;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import javaslang.Tuple;
import javaslang.Tuple2;

public final class MetricsStats implements Stats {

  private static final String UNIT_SECOND = "s";
  private static final String UNIT_MILLISECOND = "ms";
  private static final MetricId BASE = MetricId.build("styx");

  private static final MetricId QUEUED_EVENTS = BASE
      .tagged("what", "queued-events-count")
      .tagged("unit", "events");

  private static final MetricId ACTIVE_STATES_PER_RUNSTATE_PER_TRIGGER = BASE
      .tagged("what", "active-states-per-runstate-per-trigger-count")
      .tagged("unit", "state");

  private static final MetricId ACTIVE_STATES_PER_WORKFLOW = BASE
      .tagged("what", "active-states-per-workflow-count")
      .tagged("unit", "state");

  private static final MetricId WORKFLOW_COUNT = BASE
      .tagged("what", "workflow-count")
      .tagged("unit", "workflow");

  private static final MetricId RESOURCE_USED = BASE
      .tagged("what", "resource-used");

  private static final MetricId RESOURCE_CONFIGURED = BASE
      .tagged("what", "resource-configured");

  private static final MetricId EXIT_CODE_RATE = BASE
      .tagged("what", "exit-code-rate");

  private static final MetricId STORAGE_DURATION = BASE
      .tagged("what", "storage-operation-duration")
      .tagged("unit", UNIT_MILLISECOND);

  private static final MetricId STORAGE_RATE = BASE
      .tagged("what", "storage-operation-rate")
      .tagged("unit", "operation");

  private static final MetricId DOCKER_DURATION = BASE
      .tagged("what", "docker-operation-duration")
      .tagged("unit", UNIT_MILLISECOND);

  private static final MetricId DOCKER_RATE = BASE
      .tagged("what", "docker-operation-rate")
      .tagged("unit", "operation");

  private static final MetricId TRANSITIONING_DURATION = BASE
      .tagged("what", "time-transitioning-between-submitted-running")
      .tagged("unit", UNIT_SECOND);

  private static final MetricId PULL_IMAGE_ERROR_RATE = BASE
      .tagged("what", "pull-image-error-rate")
      .tagged("unit", "error");

  private static final MetricId NATURAL_TRIGGER_RATE = BASE
      .tagged("what", "natural-trigger-rate")
      .tagged("unit", "trigger");

  private static final MetricId TERMINATION_LOG_MISSING = BASE
      .tagged("what", "termination-log-missing");

  private static final MetricId TERMINATION_LOG_INVALID = BASE
      .tagged("what", "termination-log-invalid");

  private static final MetricId EXIT_CODE_MISMATCH = BASE
      .tagged("what", "exit-code-mismatch");

  private static final MetricId SUBMISSION_RATE_LIMIT = BASE
      .tagged("what", "submission-rate-limit")
      .tagged("unit", "submission/s");

  private final SemanticMetricRegistry registry;

  private final Histogram submitToRunning;
  private final Meter pullImageErrorMeter;
  private final Meter naturalTrigger;
  private final Meter terminationLogMissing;
  private final Meter terminationLogInvalid;
  private final Meter exitCodeMismatch;
  private final ConcurrentMap<String, Histogram> storageOperationHistograms;
  private final ConcurrentMap<String, Meter> storageOperationMeters;
  private final ConcurrentMap<String, Histogram> dockerOperationHistograms;
  private final ConcurrentMap<String, Meter> dockerOperationMeters;
  private final ConcurrentMap<WorkflowId, Gauge> activeStatesPerWorkflowGauges;
  private final ConcurrentMap<Tuple2<WorkflowId, Integer>, Meter> exitCodePerWorkflowMeters;
  private final ConcurrentMap<String, Histogram> resourceUsedHistograms;

  public MetricsStats(SemanticMetricRegistry registry) {
    this.registry = Objects.requireNonNull(registry);

    this.submitToRunning = registry.histogram(TRANSITIONING_DURATION);
    this.pullImageErrorMeter = registry.meter(PULL_IMAGE_ERROR_RATE);
    this.naturalTrigger = registry.meter(NATURAL_TRIGGER_RATE);
    this.terminationLogMissing = registry.meter(TERMINATION_LOG_MISSING);
    this.terminationLogInvalid = registry.meter(TERMINATION_LOG_INVALID);
    this.exitCodeMismatch = registry.meter(EXIT_CODE_MISMATCH);
    this.storageOperationHistograms = new ConcurrentHashMap<>();
    this.storageOperationMeters = new ConcurrentHashMap<>();
    this.dockerOperationHistograms = new ConcurrentHashMap<>();
    this.dockerOperationMeters = new ConcurrentHashMap<>();
    this.activeStatesPerWorkflowGauges = new ConcurrentHashMap<>();
    this.exitCodePerWorkflowMeters = new ConcurrentHashMap<>();
    this.resourceUsedHistograms = new ConcurrentHashMap<>();
  }

  @Override
  public void storageOperation(String operation, long durationMillis) {
    storageOpHistogram(operation).update(durationMillis);
    storageOpMeter(operation).mark();
  }

  @Override
  public void dockerOperation(String operation, long durationMillis) {
    dockerOpHistogram(operation).update(durationMillis);
    dockerOpMeter(operation).mark();
  }

  @Override
  public void submitToRunningTime(long durationSeconds) {
    submitToRunning.update(durationSeconds);
  }

  @Override
  public void registerQueuedEvents(Gauge<Long> queuedEventsCount) {
    registry.register(QUEUED_EVENTS, queuedEventsCount);
  }

  @Override
  public void registerActiveStates(RunState.State state, String triggerName,
                                   Gauge<Long> activeStatesCount) {
    registry.register(ACTIVE_STATES_PER_RUNSTATE_PER_TRIGGER.tagged(
        "state", state.name(), "trigger", triggerName), activeStatesCount);
  }

  @Override
  public void registerActiveStates(WorkflowId workflowId, Gauge<Long> activeStatesCount) {
    activeStatesPerWorkflowGauges.computeIfAbsent(
        workflowId, (ignoreKey) -> registry.register(
            ACTIVE_STATES_PER_WORKFLOW.tagged(
                "component-id", workflowId.componentId(), "workflow-id", workflowId.id()),
            activeStatesCount));
  }

  @Override
  public void registerWorkflowCount(String status, Gauge<Long> workflowCount) {
    registry.register(WORKFLOW_COUNT.tagged("status", status), workflowCount);
  }

  @Override
  public void exitCode(WorkflowId workflowId, int exitCode) {
    exitCodeMeter(workflowId, exitCode).mark();
  }

  @Override
  public void registerSubmissionRateLimit(Gauge<Double> submissionRateLimit) {
    registry.register(SUBMISSION_RATE_LIMIT, submissionRateLimit);
  }

  @Override
  public void terminationLogMissing() {
    terminationLogMissing.mark();
  }

  @Override
  public void terminationLogInvalid() {
    terminationLogInvalid.mark();
  }

  @Override
  public void exitCodeMismatch() {
    exitCodeMismatch.mark();
  }

  @Override
  public void naturalTrigger() {
    naturalTrigger.mark();
  }

  @Override
  public void pullImageError() {
    pullImageErrorMeter.mark();
  }

  @Override
  public void registerResourceConfigured(String resource, Gauge<Long> resourceConfigured) {
    registry.register(RESOURCE_CONFIGURED.tagged("resource", resource), resourceConfigured);
  }

  @Override
  public void unregisterResourceConfigured(String resource) {
    registry.remove(RESOURCE_CONFIGURED.tagged("resource", resource));
  }

  @Override
  public void resourceUsed(String resource, long used) {
    resourceUsedHistogram(resource).update(used);
  }

  private Meter exitCodeMeter(WorkflowId workflowId, int exitCode) {
    return exitCodePerWorkflowMeters
        .computeIfAbsent(Tuple.of(workflowId, exitCode), (tuple) ->
            registry.meter(EXIT_CODE_RATE.tagged(
                "component-id", tuple._1.componentId(),
                "workflow-id", tuple._1.id(),
                "exit-code", String.valueOf(tuple._2))));
  }

  private Histogram storageOpHistogram(String operation) {
    return storageOperationHistograms.computeIfAbsent(
        operation, (op) -> registry.histogram(STORAGE_DURATION.tagged("operation", op)));
  }

  private Meter storageOpMeter(String operation) {
    return storageOperationMeters.computeIfAbsent(
        operation, (op) -> registry.meter(STORAGE_RATE.tagged("operation", op)));
  }

  private Histogram dockerOpHistogram(String operation) {
    return dockerOperationHistograms.computeIfAbsent(
        operation, (op) -> registry.histogram(DOCKER_DURATION.tagged("operation", op)));
  }

  private Meter dockerOpMeter(String operation) {
    return dockerOperationMeters.computeIfAbsent(
        operation, (op) -> registry.meter(DOCKER_RATE.tagged("operation", op)));
  }

  private Histogram resourceUsedHistogram(String resource) {
    return resourceUsedHistograms.computeIfAbsent(
        resource, (op) -> registry.histogram(RESOURCE_USED.tagged("resource", resource)));
  }
}
