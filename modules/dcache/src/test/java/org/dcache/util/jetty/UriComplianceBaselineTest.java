/* dCache - http://www.dcache.org/
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
package org.dcache.util.jetty;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.dcache.util.jetty.ConnectorFactoryBean.Protocol;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.eclipse.jetty.server.handler.HandlerList;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Records how request paths reach dCache code, so that a change of the
 * embedded Jetty does not silently change which file names clients can use.
 * <p>
 * Two fixtures mirror the production layouts: a handler directly below the
 * server (as {@code MiltonHandler} in the WebDAV door, which uses the raw
 * request URL and the decoded target) and a {@code /api} servlet context with
 * a {@code /v1/*} servlet (as the frontend REST API).
 * <p>
 * The expected values are the behaviour of Jetty 9.4.
 */
public class UriComplianceBaselineTest {

    /**
     * Request path suffix and the expected result for the WebDAV fixture
     * ({@code /dav} + suffix) and the frontend fixture ({@code /api/v1} +
     * suffix). A result is the status code, followed by the values seen by
     * the handler if it was reached.
     */
    private static final String[][] CASES = {
          {"/dir/file",
                "200 url=http://localhost/dav/dir/file target=/dav/dir/file pathInfo=/dav/dir/file",
                "200 uri=/api/v1/dir/file servletPath=/v1 pathInfo=/dir/file"},
          {"/dir/",
                "200 url=http://localhost/dav/dir/ target=/dav/dir/ pathInfo=/dav/dir/",
                "200 uri=/api/v1/dir/ servletPath=/v1 pathInfo=/dir/"},
          {"/dir/a%20b",
                "200 url=http://localhost/dav/dir/a%20b target=/dav/dir/a b pathInfo=/dav/dir/a b",
                "200 uri=/api/v1/dir/a%20b servletPath=/v1 pathInfo=/dir/a b"},
          {"/dir/a+b",
                "200 url=http://localhost/dav/dir/a+b target=/dav/dir/a+b pathInfo=/dav/dir/a+b",
                "200 uri=/api/v1/dir/a+b servletPath=/v1 pathInfo=/dir/a+b"},
          {"/dir/%E2%82%AC",
                "200 url=http://localhost/dav/dir/%E2%82%AC target=/dav/dir/\u20ac"
                + " pathInfo=/dav/dir/\u20ac",
                "200 uri=/api/v1/dir/%E2%82%AC servletPath=/v1 pathInfo=/dir/\u20ac"},
          {"/dir/%C3%A9",
                "200 url=http://localhost/dav/dir/%C3%A9 target=/dav/dir/\u00e9"
                + " pathInfo=/dav/dir/\u00e9",
                "200 uri=/api/v1/dir/%C3%A9 servletPath=/v1 pathInfo=/dir/\u00e9"},
          {"/dir/%C3%28",
                "200 url=http://localhost/dav/dir/%C3%28 target=/dav/dir/\u00c3("
                + " pathInfo=/dav/dir/\u00c3(",
                "200 uri=/api/v1/dir/%C3%28 servletPath=/v1 pathInfo=/dir/\u00c3("},
          {"/dir/a%25b",
                "200 url=http://localhost/dav/dir/a%25b target=/dav/dir/a%b pathInfo=/dav/dir/a%b",
                "200 uri=/api/v1/dir/a%25b servletPath=/v1 pathInfo=/dir/a%b"},
          {"/dir/a%25",
                "200 url=http://localhost/dav/dir/a%25 target=/dav/dir/a% pathInfo=/dav/dir/a%",
                "200 uri=/api/v1/dir/a%25 servletPath=/v1 pathInfo=/dir/a%"},
          {"/dir/a%2Fb",
                "200 url=http://localhost/dav/dir/a%2Fb target=/dav/dir/a/b pathInfo=/dav/dir/a/b",
                "200 uri=/api/v1/dir/a%2Fb servletPath=/v1 pathInfo=/dir/a/b"},
          {"/dir/a%2fb",
                "200 url=http://localhost/dav/dir/a%2fb target=/dav/dir/a/b pathInfo=/dav/dir/a/b",
                "200 uri=/api/v1/dir/a%2fb servletPath=/v1 pathInfo=/dir/a/b"},
          {"/dir/a%5Cb",
                "200 url=http://localhost/dav/dir/a%5Cb target=/dav/dir/a\\b"
                + " pathInfo=/dav/dir/a\\b",
                "200 uri=/api/v1/dir/a%5Cb servletPath=/v1 pathInfo=/dir/a\\b"},
          {"/dir/a%3Bb",
                "200 url=http://localhost/dav/dir/a%3Bb target=/dav/dir/a;b pathInfo=/dav/dir/a;b",
                "200 uri=/api/v1/dir/a%3Bb servletPath=/v1 pathInfo=/dir/a;b"},
          {"/dir/a%3Fb",
                "200 url=http://localhost/dav/dir/a%3Fb target=/dav/dir/a?b pathInfo=/dav/dir/a?b",
                "200 uri=/api/v1/dir/a%3Fb servletPath=/v1 pathInfo=/dir/a?b"},
          {"/dir/a%23b",
                "200 url=http://localhost/dav/dir/a%23b target=/dav/dir/a#b pathInfo=/dav/dir/a#b",
                "200 uri=/api/v1/dir/a%23b servletPath=/v1 pathInfo=/dir/a#b"},
          {"/dir/a%7Eb",
                "200 url=http://localhost/dav/dir/a%7Eb target=/dav/dir/a~b pathInfo=/dav/dir/a~b",
                "200 uri=/api/v1/dir/a%7Eb servletPath=/v1 pathInfo=/dir/a~b"},
          {"/dir/a%00b",
                "400",
                "400"},
          {"/dir//double",
                "200 url=http://localhost/dav/dir//double target=/dav/dir//double"
                + " pathInfo=/dav/dir//double",
                "200 uri=/api/v1/dir//double servletPath=/v1 pathInfo=/dir//double"},
          {"/dir/./x",
                "200 url=http://localhost/dav/dir/./x target=/dav/dir/x pathInfo=/dav/dir/x",
                "200 uri=/api/v1/dir/./x servletPath=/v1 pathInfo=/dir/x"},
          {"/dir/../x",
                "200 url=http://localhost/dav/dir/../x target=/dav/x pathInfo=/dav/x",
                "200 uri=/api/v1/dir/../x servletPath=/v1 pathInfo=/x"},
          {"/dir/%2e/x",
                "200 url=http://localhost/dav/dir/%2e/x target=/dav/dir/x pathInfo=/dav/dir/x",
                "200 uri=/api/v1/dir/%2e/x servletPath=/v1 pathInfo=/dir/x"},
          {"/dir/%2e%2e/x",
                "200 url=http://localhost/dav/dir/%2e%2e/x target=/dav/x pathInfo=/dav/x",
                "200 uri=/api/v1/dir/%2e%2e/x servletPath=/v1 pathInfo=/x"},
          {"/../../x",
                "400",
                "200 url=http://localhost/api/v1/../../x target=/x pathInfo=/x"},
          {"/dir/.hidden",
                "200 url=http://localhost/dav/dir/.hidden target=/dav/dir/.hidden"
                + " pathInfo=/dav/dir/.hidden",
                "200 uri=/api/v1/dir/.hidden servletPath=/v1 pathInfo=/dir/.hidden"},
          {"/dir/...",
                "200 url=http://localhost/dav/dir/... target=/dav/dir/... pathInfo=/dav/dir/...",
                "200 uri=/api/v1/dir/... servletPath=/v1 pathInfo=/dir/..."},
          {"/dir/a;p=1/b",
                "200 url=http://localhost/dav/dir/a;p=1/b target=/dav/dir/a/b"
                + " pathInfo=/dav/dir/a/b",
                "200 uri=/api/v1/dir/a;p=1/b servletPath=/v1 pathInfo=/dir/a/b"},
          {"/dir/a;b",
                "200 url=http://localhost/dav/dir/a;b target=/dav/dir/a pathInfo=/dav/dir/a",
                "200 uri=/api/v1/dir/a;b servletPath=/v1 pathInfo=/dir/a"},
          {"/dir/a:b",
                "200 url=http://localhost/dav/dir/a:b target=/dav/dir/a:b pathInfo=/dav/dir/a:b",
                "200 uri=/api/v1/dir/a:b servletPath=/v1 pathInfo=/dir/a:b"},
          {"/dir/a@b",
                "200 url=http://localhost/dav/dir/a@b target=/dav/dir/a@b pathInfo=/dav/dir/a@b",
                "200 uri=/api/v1/dir/a@b servletPath=/v1 pathInfo=/dir/a@b"},
          {"/dir/a[1]",
                "200 url=http://localhost/dav/dir/a[1] target=/dav/dir/a[1] pathInfo=/dav/dir/a[1]",
                "200 uri=/api/v1/dir/a[1] servletPath=/v1 pathInfo=/dir/a[1]"},
          {"/dir/a%5B1%5D",
                "200 url=http://localhost/dav/dir/a%5B1%5D target=/dav/dir/a[1]"
                + " pathInfo=/dav/dir/a[1]",
                "200 uri=/api/v1/dir/a%5B1%5D servletPath=/v1 pathInfo=/dir/a[1]"},
          {"/dir/a'b",
                "200 url=http://localhost/dav/dir/a'b target=/dav/dir/a'b pathInfo=/dav/dir/a'b",
                "200 uri=/api/v1/dir/a'b servletPath=/v1 pathInfo=/dir/a'b"},
          {"/dir/a&b=c",
                "200 url=http://localhost/dav/dir/a&b=c target=/dav/dir/a&b=c"
                + " pathInfo=/dav/dir/a&b=c",
                "200 uri=/api/v1/dir/a&b=c servletPath=/v1 pathInfo=/dir/a&b=c"},
    };

    private Server server;
    private int port;

    @Before
    public void setUp() throws Exception {
        server = new Server();

        ConnectorFactoryBean factory = new ConnectorFactoryBean();
        factory.setServer(server);
        factory.setProtocol(Protocol.PLAIN);
        factory.setHost("127.0.0.1");
        factory.setPort(0);
        factory.setAcceptors(1);
        factory.setBacklog(16);
        factory.setIdleTimeout(10);
        factory.setIdleTimeoutUnit(TimeUnit.SECONDS);
        ServerConnector connector = factory.getObject();
        server.addConnector(connector);

        ServletContextHandler api = new ServletContextHandler();
        api.setContextPath("/api");
        api.addServlet(new ServletHolder(new EchoServlet()), "/v1/*");

        HandlerList handlers = new HandlerList();
        handlers.addHandler(api);
        handlers.addHandler(new EchoHandler());
        server.setHandler(handlers);
        server.start();
        port = connector.getLocalPort();
    }

    @After
    public void tearDown() throws Exception {
        server.stop();
    }

    @Test
    public void shouldKeepJetty94PathHandling() throws Exception {
        List<String> mismatches = new ArrayList<>();
        StringBuilder actualTable = new StringBuilder();
        for (String[] c : CASES) {
            String dav = get("/dav" + c[0]);
            String api = get("/api/v1" + c[0]);
            actualTable.append(String.format("          {%s, %s, %s},%n",
                  quote(c[0]), quote(dav), quote(api)));
            if (!dav.equals(c[1])) {
                mismatches.add("/dav" + c[0] + ": expected <" + c[1] + "> got <" + dav + ">");
            }
            if (!api.equals(c[2])) {
                mismatches.add("/api/v1" + c[0] + ": expected <" + c[2] + "> got <" + api + ">");
            }
        }
        assertTrue(String.join("\n", mismatches) + "\nActual table:\n" + actualTable,
              mismatches.isEmpty());
    }

    private static String quote(String s) {
        return '"' + s.replace("\\", "\\\\").replace("\"", "\\\"") + '"';
    }

    /**
     * Sends a GET with the path exactly as given and returns the status code
     * followed by the echo line, if any.
     */
    private String get(String path) throws IOException {
        try (Socket socket = new Socket("127.0.0.1", port)) {
            socket.setSoTimeout(10_000);
            OutputStream out = socket.getOutputStream();
            out.write(("GET " + path + " HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
                  .getBytes(UTF_8));
            out.flush();
            InputStream in = socket.getInputStream();
            ByteArrayOutputStream buffer = new ByteArrayOutputStream();
            in.transferTo(buffer);
            String response = buffer.toString(UTF_8);
            String status = response.substring(9, 12);
            int start = response.indexOf("\r\n\r\n") + 4;
            int echo = response.indexOf("echo ", start);
            if (echo < 0) {
                return status;
            }
            int end = response.indexOf('\n', echo);
            return status + " " + response.substring(echo + 5, end < 0 ? response.length() : end);
        }
    }

    /** Stands in for MiltonHandler. */
    private static class EchoHandler extends AbstractHandler {
        @Override
        public void handle(String target, Request baseRequest, HttpServletRequest request,
              HttpServletResponse response) throws IOException {
            baseRequest.setHandled(true);
            response.setContentType("text/plain; charset=UTF-8");
            response.getWriter().printf("echo url=%s target=%s pathInfo=%s%n",
                  request.getRequestURL(), target, request.getPathInfo());
        }
    }

    /** Stands in for the Jersey servlet. */
    private static class EchoServlet extends HttpServlet {
        @Override
        protected void doGet(HttpServletRequest request, HttpServletResponse response)
              throws IOException {
            response.setContentType("text/plain; charset=UTF-8");
            response.getWriter().printf("echo uri=%s servletPath=%s pathInfo=%s%n",
                  request.getRequestURI(), request.getServletPath(), request.getPathInfo());
        }
    }
}
