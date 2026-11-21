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

import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.dcache.util.jetty.ConnectorFactoryBean.Protocol;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Records how request paths reach dCache code, so that a change of the
 * embedded Jetty does not silently change which file names clients can use.
 * <p>
 * Two servlets in a door's servlet context, as {@link ServletContextFactoryBean}
 * creates it, mirror the production layouts: one
 * mapped to {@code /} (as {@code MiltonServlet} in the WebDAV door, which uses
 * the raw request URL and the decoded servlet path) and one mapped to
 * {@code /api/v1/*} (as the frontend REST API).
 * <p>
 * The expected values are the behaviour of Jetty 9.4, except for invalid
 * UTF-8, which is rejected now.
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
                "200 url=http://localhost/dav/dir/file servletPath=/dav/dir/file pathInfo=null",
                "200 url=http://localhost/api/v1/dir/file servletPath=/api/v1 pathInfo=/dir/file"},
          {"/dir/",
                "200 url=http://localhost/dav/dir/ servletPath=/dav/dir/ pathInfo=null",
                "200 url=http://localhost/api/v1/dir/ servletPath=/api/v1 pathInfo=/dir/"},
          {"/dir/a%20b",
                "200 url=http://localhost/dav/dir/a%20b servletPath=/dav/dir/a b pathInfo=null",
                "200 url=http://localhost/api/v1/dir/a%20b servletPath=/api/v1 pathInfo=/dir/a b"},
          {"/dir/a+b",
                "200 url=http://localhost/dav/dir/a+b servletPath=/dav/dir/a+b pathInfo=null",
                "200 url=http://localhost/api/v1/dir/a+b servletPath=/api/v1 pathInfo=/dir/a+b"},
          {"/dir/%E2%82%AC",
                "200 url=http://localhost/dav/dir/%E2%82%AC servletPath=/dav/dir/\u20ac"
                + " pathInfo=null",
                "200 url=http://localhost/api/v1/dir/%E2%82%AC servletPath=/api/v1 pathInfo=/dir/\u20ac"},
          {"/dir/%C3%A9",
                "200 url=http://localhost/dav/dir/%C3%A9 servletPath=/dav/dir/\u00e9"
                + " pathInfo=null",
                "200 url=http://localhost/api/v1/dir/%C3%A9 servletPath=/api/v1 pathInfo=/dir/\u00e9"},
          // invalid UTF-8: Jetty 9.4 decoded it as ISO-8859-1 (200, "\u00c3("); Jetty 12 can
          // only reject it or replace it with U+FFFD, we reject it.
          {"/dir/%C3%28",
                "400",
                "400"},
          {"/dir/a%25b",
                "200 url=http://localhost/dav/dir/a%25b servletPath=/dav/dir/a%b pathInfo=null",
                "200 url=http://localhost/api/v1/dir/a%25b servletPath=/api/v1 pathInfo=/dir/a%b"},
          {"/dir/a%25",
                "200 url=http://localhost/dav/dir/a%25 servletPath=/dav/dir/a% pathInfo=null",
                "200 url=http://localhost/api/v1/dir/a%25 servletPath=/api/v1 pathInfo=/dir/a%"},
          {"/dir/a%2Fb",
                "200 url=http://localhost/dav/dir/a%2Fb servletPath=/dav/dir/a/b pathInfo=null",
                "200 url=http://localhost/api/v1/dir/a%2Fb servletPath=/api/v1 pathInfo=/dir/a/b"},
          {"/dir/a%2fb",
                "200 url=http://localhost/dav/dir/a%2fb servletPath=/dav/dir/a/b pathInfo=null",
                "200 url=http://localhost/api/v1/dir/a%2fb servletPath=/api/v1 pathInfo=/dir/a/b"},
          {"/dir/a%5Cb",
                "200 url=http://localhost/dav/dir/a%5Cb servletPath=/dav/dir/a\\b"
                + " pathInfo=null",
                "200 url=http://localhost/api/v1/dir/a%5Cb servletPath=/api/v1 pathInfo=/dir/a\\b"},
          {"/dir/a%3Bb",
                "200 url=http://localhost/dav/dir/a%3Bb servletPath=/dav/dir/a;b pathInfo=null",
                "200 url=http://localhost/api/v1/dir/a%3Bb servletPath=/api/v1 pathInfo=/dir/a;b"},
          {"/dir/a%3Fb",
                "200 url=http://localhost/dav/dir/a%3Fb servletPath=/dav/dir/a?b pathInfo=null",
                "200 url=http://localhost/api/v1/dir/a%3Fb servletPath=/api/v1 pathInfo=/dir/a?b"},
          {"/dir/a%23b",
                "200 url=http://localhost/dav/dir/a%23b servletPath=/dav/dir/a#b pathInfo=null",
                "200 url=http://localhost/api/v1/dir/a%23b servletPath=/api/v1 pathInfo=/dir/a#b"},
          {"/dir/a%7Eb",
                "200 url=http://localhost/dav/dir/a%7Eb servletPath=/dav/dir/a~b pathInfo=null",
                "200 url=http://localhost/api/v1/dir/a%7Eb servletPath=/api/v1 pathInfo=/dir/a~b"},
          {"/dir/a%00b",
                "400",
                "400"},
          {"/dir//double",
                "200 url=http://localhost/dav/dir//double servletPath=/dav/dir//double"
                + " pathInfo=null",
                "200 url=http://localhost/api/v1/dir//double servletPath=/api/v1 pathInfo=/dir//double"},
          {"/dir/./x",
                "200 url=http://localhost/dav/dir/./x servletPath=/dav/dir/x pathInfo=null",
                "200 url=http://localhost/api/v1/dir/./x servletPath=/api/v1 pathInfo=/dir/x"},
          {"/dir/../x",
                "200 url=http://localhost/dav/dir/../x servletPath=/dav/x pathInfo=null",
                "200 url=http://localhost/api/v1/dir/../x servletPath=/api/v1 pathInfo=/x"},
          {"/dir/%2e/x",
                "200 url=http://localhost/dav/dir/%2e/x servletPath=/dav/dir/x pathInfo=null",
                "200 url=http://localhost/api/v1/dir/%2e/x servletPath=/api/v1 pathInfo=/dir/x"},
          {"/dir/%2e%2e/x",
                "200 url=http://localhost/dav/dir/%2e%2e/x servletPath=/dav/x pathInfo=null",
                "200 url=http://localhost/api/v1/dir/%2e%2e/x servletPath=/api/v1 pathInfo=/x"},
          {"/../../x",
                "400",
                "200 url=http://localhost/api/v1/../../x servletPath=/x pathInfo=null"},
          {"/dir/.hidden",
                "200 url=http://localhost/dav/dir/.hidden servletPath=/dav/dir/.hidden"
                + " pathInfo=null",
                "200 url=http://localhost/api/v1/dir/.hidden servletPath=/api/v1 pathInfo=/dir/.hidden"},
          {"/dir/...",
                "200 url=http://localhost/dav/dir/... servletPath=/dav/dir/... pathInfo=null",
                "200 url=http://localhost/api/v1/dir/... servletPath=/api/v1 pathInfo=/dir/..."},
          {"/dir/a;p=1/b",
                "200 url=http://localhost/dav/dir/a;p=1/b servletPath=/dav/dir/a/b"
                + " pathInfo=null",
                "200 url=http://localhost/api/v1/dir/a;p=1/b servletPath=/api/v1 pathInfo=/dir/a/b"},
          {"/dir/a;b",
                "200 url=http://localhost/dav/dir/a;b servletPath=/dav/dir/a pathInfo=null",
                "200 url=http://localhost/api/v1/dir/a;b servletPath=/api/v1 pathInfo=/dir/a"},
          {"/dir/a:b",
                "200 url=http://localhost/dav/dir/a:b servletPath=/dav/dir/a:b pathInfo=null",
                "200 url=http://localhost/api/v1/dir/a:b servletPath=/api/v1 pathInfo=/dir/a:b"},
          {"/dir/a@b",
                "200 url=http://localhost/dav/dir/a@b servletPath=/dav/dir/a@b pathInfo=null",
                "200 url=http://localhost/api/v1/dir/a@b servletPath=/api/v1 pathInfo=/dir/a@b"},
          {"/dir/a[1]",
                "200 url=http://localhost/dav/dir/a[1] servletPath=/dav/dir/a[1] pathInfo=null",
                "200 url=http://localhost/api/v1/dir/a[1] servletPath=/api/v1 pathInfo=/dir/a[1]"},
          {"/dir/a%5B1%5D",
                "200 url=http://localhost/dav/dir/a%5B1%5D servletPath=/dav/dir/a[1]"
                + " pathInfo=null",
                "200 url=http://localhost/api/v1/dir/a%5B1%5D servletPath=/api/v1 pathInfo=/dir/a[1]"},
          {"/dir/a'b",
                "200 url=http://localhost/dav/dir/a'b servletPath=/dav/dir/a'b pathInfo=null",
                "200 url=http://localhost/api/v1/dir/a'b servletPath=/api/v1 pathInfo=/dir/a'b"},
          {"/dir/a&b=c",
                "200 url=http://localhost/dav/dir/a&b=c servletPath=/dav/dir/a&b=c"
                + " pathInfo=null",
                "200 url=http://localhost/api/v1/dir/a&b=c servletPath=/api/v1 pathInfo=/dir/a&b=c"},
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

        ServletContextFactoryBean context = new ServletContextFactoryBean();
        context.setServlets(List.of(servlet("/"), servlet("/api/v1/*")));
        context.afterPropertiesSet();

        server.setHandler(context.getObject());
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

    private static ServletContextFactoryBean.ServletEntry servlet(String pathSpec) {
        ServletContextFactoryBean.ServletEntry entry = new ServletContextFactoryBean.ServletEntry();
        entry.setServlet(new EchoServlet());
        entry.setPathSpecs(List.of(pathSpec));
        return entry;
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

    /** Stands in for MiltonServlet and the Jersey servlet. */
    private static class EchoServlet extends HttpServlet {
        @Override
        protected void doGet(HttpServletRequest request, HttpServletResponse response)
              throws IOException {
            response.setContentType("text/plain; charset=UTF-8");
            response.getWriter().printf("echo url=%s servletPath=%s pathInfo=%s%n",
                  request.getRequestURL(), request.getServletPath(), request.getPathInfo());
        }
    }
}
