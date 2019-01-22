/*-
 * -\-\-
 * styx-client
 * --
 * Copyright (C) 2016 - 2017 Spotify AB
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

package com.spotify.styx.client;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.spotify.apollo.Status;
import com.spotify.apollo.StatusType;
import com.spotify.styx.api.Api;
import com.spotify.styx.api.BackfillPayload;
import com.spotify.styx.api.BackfillsPayload;
import com.spotify.styx.api.ResourcesPayload;
import com.spotify.styx.api.RunStateDataPayload;
import com.spotify.styx.client.auth.GoogleIdTokenAuth;
import com.spotify.styx.model.Backfill;
import com.spotify.styx.model.BackfillInput;
import com.spotify.styx.model.EditableBackfillInput;
import com.spotify.styx.model.Event;
import com.spotify.styx.model.Resource;
import com.spotify.styx.model.TriggerParameters;
import com.spotify.styx.model.TriggerRequest;
import com.spotify.styx.model.Workflow;
import com.spotify.styx.model.WorkflowConfiguration;
import com.spotify.styx.model.WorkflowId;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.model.WorkflowState;
import com.spotify.styx.model.data.EventInfo;
import com.spotify.styx.model.data.WorkflowInstanceExecutionData;
import com.spotify.styx.serialization.Json;
import com.spotify.styx.util.EventUtil;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.security.GeneralSecurityException;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import okhttp3.HttpUrl;
import okio.ByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default Styx Client Implementation. In case of API errors, the {@link Throwable} in the returned
 * {@link CompletionStage} will be of kind {@link ApiErrorException}. Other errors will be treated
 * as {@link RuntimeException} instead.
 */
class DefaultStyxClient implements StyxClient {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultStyxClient.class);

  static final String STYX_API_VERSION = Api.Version.V3.name().toLowerCase();

  private static final String STYX_CLIENT_VERSION =
      "Styx Client " + DefaultStyxClient.class.getPackage().getImplementationVersion();

  private final URI apiHost;
  private final GoogleIdTokenAuth auth;
  private final HttpClient client;

  private DefaultStyxClient(String apiHost, HttpClient client, GoogleIdTokenAuth auth) {
    if (apiHost.contains("://")) {
      this.apiHost = URI.create(apiHost);
    } else {
      this.apiHost = URI.create("https://" + apiHost);
    }
    this.client = Objects.requireNonNull(client, "client");
    this.auth = Objects.requireNonNull(auth, "auth");
  }

  public static StyxClient create(String apiHost) {
    return create(apiHost, HttpClient.newHttpClient(), GoogleIdTokenAuth.ofDefaultCredential());
  }

  public static StyxClient create(String apiHost, HttpClient client) {
    return create(apiHost, client, GoogleIdTokenAuth.ofDefaultCredential());
  }

  static StyxClient create(String apiHost, HttpClient client, GoogleIdTokenAuth auth) {
    return new DefaultStyxClient(apiHost, client, auth);
  }

  @Override
  public CompletionStage<RunStateDataPayload> activeStates(Optional<String> componentId) {
    final HttpUrl.Builder url = urlBuilder("status", "activeStates");
    componentId.ifPresent(id -> url.addQueryParameter("component", id));
    return execute(forUri(url), RunStateDataPayload.class);
  }

  @Override
  public CompletionStage<List<EventInfo>> eventsForWorkflowInstance(String componentId,
                                                                    String workflowId,
                                                                    String parameter) {
    return execute(forUri(urlBuilder("status", "events", componentId, workflowId, parameter)))
        .thenApply(response -> {
          final JsonNode jsonNode;
          try {
            jsonNode = Json.OBJECT_MAPPER.readTree(response.body());
          } catch (IOException e) {
            throw new RuntimeException("Invalid json returned from API", e);
          }

          if (!jsonNode.isObject()) {
            throw new RuntimeException("Unexpected json returned from API");
          }

          final ArrayNode events = ((ObjectNode) jsonNode).withArray("events");

          return StreamSupport.stream(events.spliterator(), false)
              .map(eventWithTimestamp -> {
                final long ts = eventWithTimestamp.get("timestamp").asLong();
                final JsonNode event = eventWithTimestamp.get("event");

                try {
                  final Event typedEvent = Json.OBJECT_MAPPER.convertValue(event, Event.class);
                  return EventInfo.create(ts, EventUtil.name(typedEvent), EventUtil.info(typedEvent));
                } catch (IllegalArgumentException e) {
                  // fall back to just inspecting the json
                  return EventInfo.create(ts, event.get("@type").asText(), "");
                }
              })
              .collect(Collectors.toList());
        });
  }

  @Override
  public CompletionStage<Workflow> workflow(String componentId, String workflowId) {
    return execute(forUri(urlBuilder("workflows", componentId, workflowId)), Workflow.class);
  }

  @Override
  public CompletionStage<List<Workflow>> workflows() {
    return execute(forUri(urlBuilder("workflows")), Workflow[].class)
        .thenApply(Arrays::asList);
  }

  @Override
  public CompletionStage<Workflow> createOrUpdateWorkflow(String componentId, WorkflowConfiguration workflowConfig) {
    return execute(forUri(urlBuilder("workflows", componentId), "POST", workflowConfig),
                   Workflow.class);
  }

  @Override
  public CompletionStage<Void> deleteWorkflow(String componentId, String workflowId) {
    return execute(forUri(urlBuilder("workflows", componentId, workflowId), "DELETE"))
        .thenApply(response -> null);
  }

  @Override
  public CompletionStage<WorkflowState> workflowState(String componentId, String workflowId) {
    return execute(forUri(urlBuilder("workflows", componentId, workflowId, "state")),
                   WorkflowState.class);
  }

  @Override
  public CompletionStage<WorkflowInstanceExecutionData> workflowInstanceExecutions(String componentId,
                                                                                   String workflowId,
                                                                                   String parameter) {
    return execute(forUri(urlBuilder("workflows", componentId, workflowId, "instances", parameter)),
                   WorkflowInstanceExecutionData.class);
  }

  @Override
  public CompletionStage<WorkflowState> updateWorkflowState(String componentId, String workflowId,
                                                            WorkflowState workflowState) {
    return execute(forUri(urlBuilder("workflows", componentId, workflowId, "state"), "PATCH", workflowState),
                   WorkflowState.class);
  }

  @Override
  public CompletionStage<Void> triggerWorkflowInstance(String componentId, String workflowId,
      String parameter) {
    return triggerWorkflowInstance(componentId, workflowId, parameter, TriggerParameters.zero());
  }

  @Override
  public CompletionStage<Void> triggerWorkflowInstance(String componentId,
                                                       String workflowId,
                                                       String parameter,
                                                       TriggerParameters triggerParameters) {
    return triggerWorkflowInstance(componentId, workflowId, parameter, triggerParameters, false);
  }

  @Override
  public CompletionStage<Void> triggerWorkflowInstance(String componentId,
                                                       String workflowId,
                                                       String parameter,
                                                       TriggerParameters triggerParameters,
                                                       boolean allowFuture) {
    final TriggerRequest triggerRequest =
        TriggerRequest.of(WorkflowId.create(componentId, workflowId), parameter, triggerParameters);
    return execute(
        forUri(urlBuilder("scheduler", "trigger")
            .addQueryParameter("allowFuture", String.valueOf(allowFuture)), "POST", triggerRequest))
        .thenApply(response -> null);
  }

  @Override
  public CompletionStage<Void> haltWorkflowInstance(String componentId,
                                                    String workflowId,
                                                    String parameter) {
    final HttpUrl.Builder url = urlBuilder("scheduler", "halt");
    final WorkflowInstance workflowInstance = WorkflowInstance.create(
        WorkflowId.create(componentId, workflowId),
        parameter);
    return execute(forUri(url, "POST", workflowInstance))
        .thenApply(response -> null);
  }

  @Override
  public CompletionStage<Void> retryWorkflowInstance(String componentId,
                                                     String workflowId,
                                                     String parameter) {
    final HttpUrl.Builder url = urlBuilder("scheduler", "retry");
    final WorkflowInstance workflowInstance = WorkflowInstance.create(
        WorkflowId.create(componentId, workflowId),
        parameter);
    return execute(forUri(url, "POST", workflowInstance))
        .thenApply(response -> null);
  }

  @Override
  public CompletionStage<Resource> resourceCreate(String resourceId, int concurrency) {
    final Resource resource = Resource.create(resourceId, concurrency);
    return execute(forUri(urlBuilder("resources"), "POST", resource),
                   Resource.class);
  }

  @Override
  public CompletionStage<Resource> resourceEdit(String resourceId, int concurrency) {
    final Resource resource = Resource.create(resourceId, concurrency);
    return execute(forUri(urlBuilder("resources", resourceId), "PUT", resource),
                   Resource.class);
  }

  @Override
  public CompletionStage<Resource> resource(String resourceId) {
    final HttpUrl.Builder url = urlBuilder("resources", resourceId);
    return execute(forUri(url), Resource.class);
  }

  @Override
  public CompletionStage<ResourcesPayload> resourceList() {
    final HttpUrl.Builder url = urlBuilder("resources");
    return execute(forUri(url), ResourcesPayload.class);
  }

  @Override
  public CompletionStage<Backfill> backfillCreate(String componentId, String workflowId,
                                                  String start, String end,
                                                  int concurrency) {
    return backfillCreate(componentId, workflowId, start, end, concurrency, null);
  }

  @Override
  public CompletionStage<Backfill> backfillCreate(String componentId, String workflowId,
                                                  String start, String end,
                                                  int concurrency,
                                                  String description) {
    final BackfillInput backfill = BackfillInput.newBuilder()
        .start(Instant.parse(start))
        .end(Instant.parse(end))
        .component(componentId)
        .workflow(workflowId)
        .concurrency(concurrency)
        .description(Optional.ofNullable(description))
        .build();
    return backfillCreate(backfill);
  }

  @Override
  public CompletionStage<Backfill> backfillCreate(BackfillInput backfill) {
    return backfillCreate(backfill, false);
  }

  @Override
  public CompletionStage<Backfill> backfillCreate(BackfillInput backfill, boolean allowFuture) {
    return execute(forUri(
        urlBuilder("backfills")
            .addQueryParameter("allowFuture", String.valueOf(allowFuture)),
        "POST", backfill), Backfill.class);
  }

  @Override
  public CompletionStage<Backfill> backfillEditConcurrency(String backfillId, int concurrency) {
    final EditableBackfillInput editableBackfillInput = EditableBackfillInput.newBuilder()
        .id(backfillId)
        .concurrency(concurrency)
        .build();
    final HttpUrl.Builder url = urlBuilder("backfills", backfillId);
    return execute(forUri(url, "PUT", editableBackfillInput), Backfill.class);
  }

  @Override
  public CompletionStage<Void> backfillHalt(String backfillId) {
    return execute(forUri(urlBuilder("backfills", backfillId), "DELETE"))
        .thenApply(response -> null);
  }

  @Override
  public CompletionStage<BackfillPayload> backfill(String backfillId, boolean includeStatus) {
    final HttpUrl.Builder url = urlBuilder("backfills", backfillId);
    url.addQueryParameter("status", Boolean.toString(includeStatus));
    return execute(forUri(url), BackfillPayload.class);
  }

  @Override
  public CompletionStage<BackfillsPayload> backfillList(Optional<String> componentId,
                                                        Optional<String> workflowId,
                                                        boolean showAll,
                                                        boolean includeStatus) {
    final HttpUrl.Builder url = urlBuilder("backfills");
    componentId.ifPresent(c -> url.addQueryParameter("component", c));
    workflowId.ifPresent(w -> url.addQueryParameter("workflow", w));
    url.addQueryParameter("showAll", Boolean.toString(showAll));
    url.addQueryParameter("status", Boolean.toString(includeStatus));
    return execute(forUri(url), BackfillsPayload.class);
  }

  private <T> CompletionStage<T> execute(HttpRequest.Builder request, Class<T> tClass) {
    return execute(request).thenApply(response -> {
      try {
        return Json.OBJECT_MAPPER.readValue(response.body(), tClass);
      } catch (IOException e) {
        throw new RuntimeException("Error while reading the received payload: " + e.getMessage(), e);
      }
    });
  }

  private HttpRequest.Builder decorateRequest(HttpRequest.Builder request, String requestId,
                                             Optional<String> authToken) {
    authToken.ifPresent(t -> request.header("Authorization", "Bearer " + t));
    return request
        .header("User-Agent", STYX_CLIENT_VERSION)
        .header("X-Request-Id", requestId);
  }

  private CompletionStage<HttpResponse<byte[]>> execute(HttpRequest.Builder requestBuilder) {
    final Optional<String> authToken;
    try {
      authToken = auth.getToken(apiHost.toString());
    } catch (IOException | GeneralSecurityException e) {
      // Credential probably invalid, configured wrongly or the token request failed.
      throw new ClientErrorException("Authentication failure: " + e.getMessage(), e);
    }
    final String requestId = UUID.randomUUID().toString().replace("-", "");  // UUID with no dashes, easier to deal with
    final HttpRequest request = decorateRequest(requestBuilder, requestId, authToken).build();
    LOG.debug("{} {}", request.method(), request.uri());
    final long start = System.nanoTime();
    return client.sendAsync(request, BodyHandlers.ofByteArray()).handle((response, e) -> {
      if (e != null) {
        LOG.debug("{} {}: failed (latency: {}s)", request.method(), request.uri(), latency(start), e);
        throw new ClientErrorException("Request failed: " + request.method() + " " + request.uri(), e);
      } else {
        LOG.debug("{} {}: {} (latency: {}s)", request.method(), request.uri(), response.statusCode(), latency(start));
        final String effectiveRequestId;
        final Optional<String> responseRequestId = response.headers().firstValue("X-Request-Id");
        if (responseRequestId.isPresent() && !responseRequestId.get().equals(requestId)) {
          // If some proxy etc dropped our request ID header, we might get another one back.
          effectiveRequestId = responseRequestId.get();
          LOG.warn("Request ID mismatch: '{}' != '{}'", requestId, responseRequestId);
        } else {
          effectiveRequestId = requestId;
        }
        final StatusType status = Status.createForCode(response.statusCode());
        if (status.family() != StatusType.Family.SUCCESSFUL) {
          throw new ApiErrorException(status.code() + " " + status.reasonPhrase(), response.statusCode(),
              authToken.isPresent(), effectiveRequestId);
        }
        return response;
      }
    });
  }

  private static String latency(long startNanos) {
    final long end = System.nanoTime();
    return Long.toString(TimeUnit.NANOSECONDS.toSeconds(end - startNanos));
  }

  private HttpUrl.Builder urlBuilder(String... pathSegments) {
    final HttpUrl.Builder builder = new HttpUrl.Builder()
        .scheme(apiHost.getScheme())
        .host(apiHost.getHost())
        .addPathSegment("api")
        .addPathSegment(STYX_API_VERSION);
    Arrays.stream(pathSegments).forEach(builder::addPathSegment);
    if (apiHost.getPort() != -1) {
      builder.port(apiHost.getPort());
    }
    return builder;
  }

  @Override
  public void close() {
    // nop
  }

  private static HttpRequest.Builder internalForUri(HttpUrl uri, String method, ByteString payload) {
    return HttpRequest.newBuilder().uri(uri.uri())
        .header("Content-Type", "application/json")
        .method(method, HttpRequest.BodyPublishers.ofByteArray(payload.toByteArray()));
  }

  static HttpRequest.Builder forUri(HttpUrl uri, String method, Object payload) {
    try {
      return internalForUri(uri, method, Json.serialize(payload));
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  static HttpRequest.Builder forUri(HttpUrl.Builder uriBuilder, String method, Object payload) {
    return forUri(uriBuilder.build(), method, payload);
  }

  static HttpRequest.Builder forUri(HttpUrl uri, String method) {
    return HttpRequest.newBuilder().uri(uri.uri()).method(method, HttpRequest.BodyPublishers.noBody());
  }

  static HttpRequest.Builder forUri(HttpUrl.Builder uriBuilder, String method) {
    return forUri(uriBuilder.build(), method);
  }

  static HttpRequest.Builder forUri(HttpUrl.Builder uriBuilder) {
    return forUri(uriBuilder.build());
  }

  static HttpRequest.Builder forUri(HttpUrl uri) {
    return HttpRequest.newBuilder().uri(uri.uri());
  }
}
