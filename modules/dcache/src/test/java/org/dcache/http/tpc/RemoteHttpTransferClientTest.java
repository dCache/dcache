/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2025 Deutsches Elektronen-Synchrotron
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.dcache.http.tpc;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import com.google.common.collect.ImmutableMap;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import diskCacheV111.util.ThirdPartyTransferFailedCacheException;
import diskCacheV111.vehicles.RemoteHttpDataTransferProtocolInfo;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HexFormat;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.http.HttpEntity;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.dcache.util.Checksum;
import org.dcache.util.ChecksumType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class RemoteHttpTransferClientTest {

    private static final byte[] DATA = "some file content".getBytes(StandardCharsets.UTF_8);

    private static final RemoteHttpTransferClient.Progress NO_PROGRESS =
          new RemoteHttpTransferClient.Progress() {
              @Override
              public long getBytesTransferred() {
                  return 0;
              }

              @Override
              public long getSize() {
                  return DATA.length;
              }
          };

    private HttpServer server;
    private CloseableHttpClient client;
    private final List<String> requests = Collections.synchronizedList(new ArrayList<>());
    private final AtomicInteger connected = new AtomicInteger();
    private ByteArrayOutputStream uploaded;

    @Before
    public void setUp() throws IOException {
        server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        server.start();
        client = HttpClients.createDefault();
        uploaded = new ByteArrayOutputStream();
    }

    @After
    public void tearDown() throws IOException {
        server.stop(0);
        client.close();
    }

    private static String md5(byte[] data) throws Exception {
        return HexFormat.of().formatHex(MessageDigest.getInstance("MD5").digest(data));
    }

    private static Checksum md5Checksum(byte[] data) throws Exception {
        return new Checksum(ChecksumType.MD5_TYPE, md5(data));
    }

    @FunctionalInterface
    private interface Handler {

        void handle(HttpExchange exchange) throws Exception;
    }

    private void handle(String path, Handler handler) {
        server.createContext(path, exchange -> {
            requests.add(exchange.getRequestMethod() + " " + exchange.getRequestURI().getPath());
            try {
                handler.handle(exchange);
            } catch (IOException | RuntimeException e) {
                throw e;
            } catch (Exception e) {
                throw new RuntimeException(e);
            } finally {
                exchange.close();
            }
        });
    }

    private static void respond(HttpExchange exchange, int status, byte[] body,
          String... headers) throws IOException {
        for (int i = 0; i < headers.length; i += 2) {
            exchange.getResponseHeaders().add(headers[i], headers[i + 1]);
        }
        if (body == null) {
            exchange.sendResponseHeaders(status, -1);
        } else {
            exchange.sendResponseHeaders(status, body.length);
            exchange.getResponseBody().write(body);
        }
    }

    private RemoteHttpDataTransferProtocolInfo info(String path, boolean verificationRequired) {
        return new RemoteHttpDataTransferProtocolInfo("RemoteHttpDataTransfer", 1, 1,
              new InetSocketAddress(InetAddress.getLoopbackAddress(),
                    server.getAddress().getPort()),
              "http://127.0.0.1:" + server.getAddress().getPort() + path,
              verificationRequired, ImmutableMap.of(), List.of());
    }

    private RemoteHttpTransferClient clientFor(RemoteHttpDataTransferProtocolInfo info) {
        return new RemoteHttpTransferClient(client, info, connected::incrementAndGet);
    }

    private static HttpEntity entity() {
        return new ByteArrayEntity(DATA);
    }

    @Test
    public void shouldDownloadFileAndReportChecksum() throws Exception {
        handle("/file", e -> respond(e, 200, DATA, "Content-MD5", md5(DATA)));
        List<Checksum> received = new ArrayList<>();
        ByteArrayOutputStream body = new ByteArrayOutputStream();
        List<Long> lengths = new ArrayList<>();
        RemoteHttpTransferClient transfer = clientFor(info("/file", true));

        transfer.get(received::add, entity -> entity.writeTo(body), NO_PROGRESS, lengths::add);

        assertThat(body.toByteArray(), equalTo(DATA));
        assertThat(received, contains(md5Checksum(DATA)));
        assertThat(lengths, contains((long) DATA.length));
        assertEquals(Long.valueOf(DATA.length), transfer.getBytesExpected());
        assertEquals(1, connected.get());
    }

    @Test
    public void shouldFailDownloadWhenRemoteRejectsRequest() throws Exception {
        handle("/missing", e -> respond(e, 404, null));
        RemoteHttpTransferClient transfer = clientFor(info("/missing", false));

        try {
            transfer.get(c -> {
            }, entity -> {
            }, NO_PROGRESS, l -> {
            });
            fail("expected failure");
        } catch (ThirdPartyTransferFailedCacheException e) {
            assertThat(e.getMessage(), containsString("rejected GET: 404"));
        }
    }

    @Test
    public void shouldFailDownloadWhenVerificationRequiredButNoChecksumOffered() throws Exception {
        handle("/file", e -> respond(e, 200, DATA));
        RemoteHttpTransferClient transfer = clientFor(info("/file", true));

        try {
            transfer.get(c -> {
            }, entity -> entity.writeTo(new ByteArrayOutputStream()), NO_PROGRESS, l -> {
            });
            fail("expected failure");
        } catch (ThirdPartyTransferFailedCacheException e) {
            assertThat(e.getMessage(), containsString("no checksum in HEAD response"));
        }
    }

    @Test
    public void shouldUseChecksumFromHeadWhenGetOffersNone() throws Exception {
        handle("/file", e -> {
            if (e.getRequestMethod().equals("HEAD")) {
                respond(e, 200, null, "Content-MD5", md5(DATA));
            } else {
                respond(e, 200, DATA);
            }
        });
        List<Checksum> received = new ArrayList<>();
        RemoteHttpTransferClient transfer = clientFor(info("/file", true));

        transfer.get(received::add, entity -> entity.writeTo(new ByteArrayOutputStream()),
              NO_PROGRESS, l -> {
              });

        assertThat(received, contains(md5Checksum(DATA)));
        assertThat(requests, contains("GET /file", "HEAD /file"));
    }

    @Test
    public void shouldUploadAndVerifyFile() throws Exception {
        handle("/file", e -> {
            switch (e.getRequestMethod()) {
                case "PUT":
                    e.getRequestBody().transferTo(uploaded);
                    respond(e, 201, null);
                    break;
                case "HEAD":
                    respond(e, 200, null, "Content-Length", String.valueOf(DATA.length),
                          "Content-MD5", md5(DATA));
                    break;
                default:
                    respond(e, 405, null);
            }
        });
        RemoteHttpTransferClient transfer = clientFor(info("/file", true));

        transfer.putAndVerify(RemoteHttpTransferClientTest::entity, NO_PROGRESS, DATA.length,
              Optional.of(Set.of(md5Checksum(DATA))));

        assertThat(uploaded.toByteArray(), equalTo(DATA));
        assertThat(requests, contains("PUT /file", "HEAD /file"));
    }

    @Test
    public void shouldFollowRedirectOnUpload() throws Exception {
        handle("/file", e -> respond(e, 307, null, "Location",
              "http://127.0.0.1:" + server.getAddress().getPort() + "/target"));
        handle("/target", e -> {
            switch (e.getRequestMethod()) {
                case "PUT":
                    e.getRequestBody().transferTo(uploaded);
                    respond(e, 201, null);
                    break;
                default:
                    respond(e, 200, null, "Content-Length", String.valueOf(DATA.length));
            }
        });
        RemoteHttpTransferClient transfer = clientFor(info("/file", false));

        transfer.putAndVerify(RemoteHttpTransferClientTest::entity, NO_PROGRESS, DATA.length,
              Optional.empty());

        assertThat(uploaded.toByteArray(), equalTo(DATA));
        assertThat(requests.get(0), equalTo("PUT /file"));
        assertThat(requests.get(1), equalTo("PUT /target"));
    }

    @Test
    public void shouldDeleteRemoteFileWhenChecksumMismatches() throws Exception {
        handle("/file", e -> {
            switch (e.getRequestMethod()) {
                case "PUT":
                    e.getRequestBody().transferTo(uploaded);
                    respond(e, 201, null);
                    break;
                case "HEAD":
                    respond(e, 200, null, "Content-Length", String.valueOf(DATA.length),
                          "Content-MD5", md5("other".getBytes(StandardCharsets.UTF_8)));
                    break;
                case "DELETE":
                    respond(e, 204, null);
                    break;
                default:
                    respond(e, 405, null);
            }
        });
        RemoteHttpTransferClient transfer = clientFor(info("/file", true));

        try {
            transfer.putAndVerify(RemoteHttpTransferClientTest::entity, NO_PROGRESS,
                  DATA.length, Optional.of(Set.of(md5Checksum(DATA))));
            fail("expected failure");
        } catch (ThirdPartyTransferFailedCacheException e) {
            assertThat(e.getMessage(), containsString("verification failed"));
        }

        assertThat(requests, contains("PUT /file", "HEAD /file", "DELETE /file"));
    }

    @Test
    public void shouldFailUploadWhenRemoteRejectsPut() throws Exception {
        handle("/file", e -> {
            e.getRequestBody().transferTo(uploaded);
            respond(e, 403, null);
        });
        RemoteHttpTransferClient transfer = clientFor(info("/file", false));

        try {
            transfer.putAndVerify(RemoteHttpTransferClientTest::entity, NO_PROGRESS,
                  DATA.length, Optional.empty());
            fail("expected failure");
        } catch (ThirdPartyTransferFailedCacheException e) {
            assertThat(e.getMessage(), containsString("rejected PUT: 403"));
        }

        assertThat(requests, contains("PUT /file"));
    }

    @Test
    public void shouldNotReportEndpointsBeforeAnyRequest() {
        RemoteHttpTransferClient transfer = clientFor(info("/file", false));

        assertThat(transfer.localAddress().isPresent(), equalTo(false));
        assertThat(transfer.remoteAddress().isPresent(), equalTo(false));
        assertThat(requests, empty());
    }
}
