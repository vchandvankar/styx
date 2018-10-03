/*-
 * -\-\-
 * Spotify Styx API Client
 * --
 * Copyright (C) 2016 - 2018 Spotify AB
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

import static com.spotify.styx.client.Json.GSON;

import com.google.api.client.http.ByteArrayContent;
import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpContent;
import com.google.api.client.http.HttpHeaders;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import okhttp3.HttpUrl;
import okhttp3.MediaType;
import okhttp3.Protocol;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.Response.Builder;
import okhttp3.ResponseBody;
import okio.Buffer;
import okio.ByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class FutureOkHttpClient implements AutoCloseable {

  private static final Logger log = LoggerFactory.getLogger(FutureOkHttpClient.class);

  private static final Duration DEFAULT_CONNECT_TIMEOUT = Duration.ofSeconds(10);
  private static final Duration DEFAULT_READ_TIMEOUT = Duration.ofSeconds(90);
  // TODO: no way to set write timeout, enforce a full request timeout instead?
  private static final Duration DEFAULT_WRITE_TIMEOUT = Duration.ofSeconds(90);
  private static final MediaType APPLICATION_JSON =
      Objects.requireNonNull(MediaType.parse("application/json"));

  private final HttpTransport transport;

  private final ForkJoinPool forkJoinPool = new ForkJoinPool();

  static FutureOkHttpClient create(HttpTransport transport) {
    return new FutureOkHttpClient(transport);
  }

  static FutureOkHttpClient createDefault() {
    return FutureOkHttpClient.create(new NetHttpTransport());
  }

  private FutureOkHttpClient(HttpTransport transport) {
    this.transport = Objects.requireNonNull(transport);
  }

  CompletionStage<Response> send(Request request) {

    // mx native-image --enable-https --enable-all-security-services -H:+JNI -H:IncludeResourceBundles=net.sourceforge.argparse4j.internal.ArgumentParserImpl -H:ReflectionConfigurationFiles=reflectionconfig.json -jar /Users/dano/projects/styx/styx-cli/target/styx-cli.jar

    return CompletableFuture.completedFuture(request).thenApplyAsync(r -> {
      log.debug("{} {}", request.method(), request.url());
      final long start = System.nanoTime();
      try {
        HttpContent content = null;
        if (request.body() != null) {
          final Buffer buffer = new Buffer();
          request.body().writeTo(buffer);
          content = new ByteArrayContent(request.body().contentType().toString(), buffer.readByteArray());
        }
        HttpHeaders headers = new HttpHeaders();
        request.headers().toMultimap().forEach((k, vs) -> {
          List<String> values = (List<String>) headers.get(k);
          if (values == null) {
            values = new ArrayList<>();
            headers.set(k, values);
          }
          values.addAll(vs);
        });
        final HttpResponse response = transport.createRequestFactory()
            .buildRequest(request.method(), new GenericUrl(request.url().toString()), content)
            .setConnectTimeout((int) DEFAULT_CONNECT_TIMEOUT.toMillis())
            .setReadTimeout((int) DEFAULT_READ_TIMEOUT.toMillis())
            .setHeaders(headers)
            .execute();
        final byte[] payload;
        try (InputStream is = response.getContent()) {
          payload = toByteArray(is);
        }
        log.debug("{} {}: {} {} (latency: {}s)", request.method(), request.url(), response.getStatusCode(),
            response.getStatusMessage(), latency(start));
        final Builder builder = new Builder();
        response.getHeaders().forEach((k, v) -> {
          builder.addHeader(k, v.toString());
        });
        final MediaType mediaType;
        if (response.getMediaType() != null) {
          mediaType = MediaType.parse(response.getMediaType().toString());
        } else {
          mediaType = null;
        }
        return builder
            .request(request)
            .protocol(Protocol.HTTP_1_1)
            .code(response.getStatusCode())
            .body(ResponseBody.create(mediaType, payload))
            .message(response.getStatusMessage())
            .build();
      } catch (IOException e) {
        log.debug("{} {}: failed (latency: {}s)", request.method(), request.url(), latency(start), e);
        throw new CompletionException(e);
      }
    }, forkJoinPool);
  }

  private static byte[] toByteArray(InputStream is) throws IOException {
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    final byte[] buf = new byte[1024];
    int len;
    while ((len = is.read(buf)) != -1) {
      baos.write(buf, 0, len);
    }
    return baos.toByteArray();
  }

  private static String latency(long startNanos) {
    final long end = System.nanoTime();
    return Long.toString(TimeUnit.NANOSECONDS.toSeconds(end - startNanos));
  }

  private static Request internalForUri(HttpUrl uri, String method, ByteString payload) {
    return new Request.Builder().url(uri.uri().toString())
        .method(method, RequestBody.create(APPLICATION_JSON, payload))
        .build();
  }

  static Request forUri(HttpUrl uri, String method, Object payload) {
    final String json = GSON.toJson(payload);
    return internalForUri(uri, method, ByteString.encodeUtf8(json));
  }

  static Request forUri(HttpUrl.Builder uriBuilder, String method, Object payload) {
    return forUri(uriBuilder.build(), method, payload);
  }

  static Request forUri(HttpUrl uri, String method) {
    return new Request.Builder().url(uri.uri().toString()).method(method, null).build();
  }

  static Request forUri(HttpUrl.Builder uriBuilder, String method) {
    return forUri(uriBuilder.build(), method);
  }

  static Request forUri(HttpUrl.Builder uriBuilder) {
    return forUri(uriBuilder.build());
  }

  static Request forUri(HttpUrl uri) {
    return new Request.Builder().url(uri.uri().toString()).build();
  }

  @Override
  public void close() {
  }
}
