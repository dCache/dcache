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
package org.dcache.webdav.transfer;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.startsWith;

import java.io.IOException;
import java.util.Map;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.eclipse.jetty.ee8.servlet.ServletContextHandler;
import org.eclipse.jetty.ee8.servlet.ServletHolder;
import org.eclipse.jetty.server.LocalConnector;
import org.eclipse.jetty.server.Server;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * The third-party copy response must carry the Digest trailer: Jetty only sends trailers that
 * are set before the response is committed.
 */
public class CommitResponseTest {

    private static final String DIGEST = "adler32=1a299e89";

    private Server server;
    private LocalConnector connector;

    @Before
    public void setUp() throws Exception {
        ServletContextHandler root = new ServletContextHandler();
        root.setContextPath("/");
        root.addServlet(new ServletHolder(new HttpServlet() {
            @Override
            protected void service(HttpServletRequest request, HttpServletResponse response)
                  throws IOException {
                RemoteTransferHandler.commitResponse(request, response,
                      () -> Map.of("Digest", DIGEST));
                response.getWriter().println("success: Created");
            }
        }), "/");

        server = new Server();
        connector = new LocalConnector(server);
        server.addConnector(connector);
        server.setHandler(root.get());
        server.start();
    }

    @After
    public void tearDown() throws Exception {
        server.stop();
    }

    @Test
    public void shouldSendTrailerIfAccepted() throws Exception {
        String response = copy("HTTP/1.1", "TE: trailers\r\n");

        assertThat(response, startsWith("HTTP/1.1 202 "));
        assertThat(response, containsString("success: Created"));
        assertThat(response, containsString("\r\n0\r\nDigest: " + DIGEST + "\r\n"));
    }

    @Test
    public void shouldNotSendTrailerIfNotAccepted() throws Exception {
        String response = copy("HTTP/1.1", "");

        assertThat(response, startsWith("HTTP/1.1 202 "));
        assertThat(response, containsString("success: Created"));
        assertThat(response, not(containsString("Digest: ")));
    }

    @Test
    public void shouldNotFailForHttp10() throws Exception {
        String response = copy("HTTP/1.0", "TE: trailers\r\n");

        assertThat(response, containsString(" 202 Accepted"));
        assertThat(response, containsString("success: Created"));
        assertThat(response, not(containsString("Digest: ")));
    }

    private String copy(String version, String headers) throws Exception {
        return connector.getResponse("COPY /file " + version + "\r\n"
              + "Host: localhost\r\n"
              + headers
              + "Connection: close\r\n"
              + "\r\n");
    }
}
