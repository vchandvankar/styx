/*
 * -\-\-
 * Spotify Styx Service Common
 * --
 * Copyright (C) 2019 Spotify AB
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

package com.spotify.styx.util;

import static java.lang.String.format;

import com.spotify.styx.model.Workflow;
import java.time.ZonedDateTime;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class BasicWorkflowValidator implements WorkflowValidator {

  private static final int MAX_ID_LENGTH = 256;
  private static final int MAX_DOCKER_ARGS_TOTAL = 1000000;
  private static final int MAX_RESOURCES = 5;
  private static final int MAX_RESOURCE_LENGTH = 256;
  private static final int MAX_COMMIT_SHA_LENGTH = 256;
  private static final int MAX_SECRET_NAME_LENGTH = 253;
  private static final int MAX_SECRET_MOUNT_PATH_LENGTH = 1024;
  private static final int MAX_SERVICE_ACCOUNT_LENGTH = 256;
  private static final int MAX_ENV_VARS = 128;
  private static final int MAX_ENV_SIZE = 16 * 1024;

  private final DockerImageValidator dockerImageValidator;

  public BasicWorkflowValidator(DockerImageValidator dockerImageValidator) {
    this.dockerImageValidator = Objects.requireNonNull(dockerImageValidator);
  }

  @Override
  public List<String> validateWorkflow(Workflow workflow) {
    var workflowId = workflow.id();
    var cfg = workflow.configuration();

    final List<String> e = new ArrayList<>();

    var componentId = workflowId.componentId();
    if (componentId.isEmpty()) {
      e.add("component id cannot be empty");
    } else if (componentId.contains("#")) {
      e.add("component id cannot contain #");
    }

    if (workflowId.id().isEmpty()) {
      e.add("workflow id cannot be empty");
    }

    if (!workflowId.id().equals(cfg.id())) {
      e.add("workflow id mismatch");
    }

    // TODO: validate more of the contents

    upperLimit(e, cfg.id().length(),
        MAX_ID_LENGTH, "id too long");
    upperLimit(e, cfg.commitSha().map(String::length).orElse(0),
        MAX_COMMIT_SHA_LENGTH, "commitSha too long");
    upperLimit(e, cfg.secret().map(s -> s.name().length()).orElse(0),
        MAX_SECRET_NAME_LENGTH, "secret name too long");
    upperLimit(e, cfg.secret().map(s -> s.mountPath().length()).orElse(0),
        MAX_SECRET_MOUNT_PATH_LENGTH, "secret mount path too long");
    upperLimit(e, cfg.serviceAccount().map(String::length).orElse(0),
        MAX_SERVICE_ACCOUNT_LENGTH, "service account too long");
    upperLimit(e, cfg.resources().size(),
        MAX_RESOURCES, "too many resources");
    upperLimit(e, cfg.env().size(),
        MAX_ENV_VARS, "too many env vars");
    upperLimit(e, cfg.env().entrySet().stream()
            .mapToInt(entry -> entry.getKey().length() + entry.getValue().length()).sum(),
        MAX_ENV_SIZE, "env too big");

    cfg.dockerImage().ifPresent(image ->
        dockerImageValidator.validateImageReference(image).stream()
            .map(s -> "invalid image: " + s)
            .forEach(e::add));

    cfg.resources().stream().map(String::length).forEach(v ->
        upperLimit(e, v, MAX_RESOURCE_LENGTH, "resource name too long"));

    cfg.dockerArgs().ifPresent(args -> {
      final int dockerArgs = args.size() + args.stream().mapToInt(String::length).sum();
      upperLimit(e, dockerArgs, MAX_DOCKER_ARGS_TOTAL, "docker args is too large");
    });

    cfg.offset().ifPresent(offset -> {
      try {
        TimeUtil.addOffset(ZonedDateTime.now(), offset);
      } catch (DateTimeParseException ex) {
        e.add(format("invalid offset: %s", ex.getMessage()));
      }
    });

    try {
      TimeUtil.cron(cfg.schedule());
    } catch (IllegalArgumentException ex) {
      e.add("invalid schedule");
    }

    return e;
  }
}
