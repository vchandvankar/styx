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

import static com.spotify.styx.client.OkHttpTestUtil.bytesOfRequestBody;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.core.IsInstanceOf.instanceOf;

import com.google.api.client.http.HttpResponseException;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import java.net.InetAddress;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import okhttp3.HttpUrl;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.tls.HandshakeCertificates;
import okhttp3.tls.HeldCertificate;
import okio.ByteString;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class FutureOkHttpClientTest {

  @Rule public ExpectedException thrown = ExpectedException.none();

  private final MockWebServer server = new MockWebServer();

  private HttpUrl uri;
  private Request request;
  private HttpTransport transport;
  private FutureOkHttpClient client;

  @Before
  public void setUp() throws Exception {
    final String localhost = InetAddress.getByName("localhost").getCanonicalHostName();
    final HeldCertificate localhostCertificate = new HeldCertificate.Builder()
        .addSubjectAlternativeName(localhost)
        .build();
    final HandshakeCertificates serverCertificates = new HandshakeCertificates.Builder()
        .heldCertificate(localhostCertificate)
        .build();
    final HandshakeCertificates clientCertificates = new HandshakeCertificates.Builder()
        .addTrustedCertificate(localhostCertificate.certificate())
        .build();
    server.useHttps(serverCertificates.sslSocketFactory(), false);
    server.start();
    transport = new NetHttpTransport.Builder()
        .setSslSocketFactory(clientCertificates.sslSocketFactory())
        .build();
    client = FutureOkHttpClient.create(transport);

    uri = new HttpUrl.Builder()
        .scheme("https")
        .host(server.getHostName())
        .port(server.getPort())
        .build();
    request = new Request.Builder()
        .url(uri.uri().toString())
        .header("foo", "bar")
        .build();
  }

  @Test
  public void testMethod() {
    final Request request = FutureOkHttpClient.forUri(uri, "DELETE");
    assertThat(request.url(), is(uri));
    assertThat(request.method(), is("DELETE"));
  }

  @Test
  public void testSimpleGet() throws Exception {
    server.enqueue(new MockResponse()
        .setResponseCode(200)
        .setBody("hello")
        .setHeader("foo", "bar"));

    final Response response = client.send(request).toCompletableFuture().get(30, SECONDS);

    assertThat(response.code(), is(200));
    assertThat(new String(response.body().bytes(), UTF_8), is("hello"));
  }

  @Test
  public void testSomethingGoesWrong() throws Exception {
    final FutureOkHttpClient client = FutureOkHttpClient.create(transport);
    server.enqueue(new MockResponse().setResponseCode(500));
    final CompletableFuture<Response> response = client.send(request).toCompletableFuture();
    thrown.expectCause(instanceOf(HttpResponseException.class));
    response.get(30, SECONDS);
  }

  @Test
  public void testRequestWithBody() {
    final Request request = FutureOkHttpClient.forUri(uri, "POST", Arrays.asList(1, 2, 3));

    assertThat(request.url(), is(uri));
    assertThat(request.method(), is("POST"));
    assertThat(bytesOfRequestBody(request), is(ByteString.encodeUtf8("[1,2,3]")));
  }

}
