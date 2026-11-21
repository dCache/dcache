/*
 * dCache - http://www.dcache.org/
 *
 * Copyright (C) 2026 Deutsches Elektronen-Synchrotron
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
package org.dcache.restful.util;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Map;
import org.eclipse.jetty.ee11.servlet.ServletContextHandler;
import org.eclipse.jetty.ee11.servlet.ServletHolder;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Serves {@link StaticDataServlet} from an embedded Jetty and checks that the Content-Length
 * counts bytes, not characters.
 */
public class StaticDataServletTest {

    private static Server server;
    private static HttpClient client;
    private static URI endpoint;

    @BeforeClass
    public static void setup() throws Exception {
        StaticDataServlet servlet = new StaticDataServlet();
        servlet.setData(Map.of("organisation", "DESY – Zeuthen"));

        ServletContextHandler context = new ServletContextHandler();
        context.setContextPath("/");
        context.addServlet(new ServletHolder(servlet), "/config.js");
        context.addServlet(new ServletHolder(servlet), "/config.json");

        server = new Server();
        ServerConnector connector = new ServerConnector(server);
        connector.setHost("127.0.0.1");
        connector.setPort(0);
        server.addConnector(connector);
        server.setHandler(context);
        server.start();

        endpoint = URI.create("http://127.0.0.1:" + connector.getLocalPort());
        client = HttpClient.newHttpClient();
    }

    @AfterClass
    public static void tearDown() throws Exception {
        if (server != null) {
            server.stop();
        }
    }

    @Test
    public void shouldSetContentLengthInBytes() throws Exception {
        HttpResponse<byte[]> response = get("/config.js");

        byte[] body = response.body();
        String text = new String(body, UTF_8);
        assertThat(response.statusCode(), is(200));
        assertThat(text.length() < body.length, is(true));
        assertThat(response.headers().firstValueAsLong("Content-Length").orElse(-1),
              is((long) body.length));
        assertThat(text, startsWith("var CONFIG = "));
        assertThat(text, containsString("DESY – Zeuthen"));
    }

    @Test
    public void shouldServeJson() throws Exception {
        HttpResponse<byte[]> response = get("/config.json");

        byte[] body = response.body();
        assertThat(response.headers().firstValueAsLong("Content-Length").orElse(-1),
              is((long) body.length));
        assertThat(new String(body, UTF_8), containsString("\"organisation\": \"DESY – Zeuthen\""));
    }

    private HttpResponse<byte[]> get(String path) throws Exception {
        return client.send(HttpRequest.newBuilder(endpoint.resolve(path)).GET().build(),
              HttpResponse.BodyHandlers.ofByteArray());
    }
}
