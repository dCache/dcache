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
package org.dcache.webdav;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import org.dcache.tests.cells.SpringServiceContext;
import org.eclipse.jetty.server.Server;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Starts the WebDAV door's Spring context, outside a cell, with the shipped defaults.
 */
public class WebdavDoorContextTest {

    private static SpringServiceContext context;
    private static HttpClient client;
    private static URI endpoint;

    @BeforeClass
    public static void setup() throws Exception {
        Path localStatic = Files.createTempDirectory("webdav-static");
        localStatic.toFile().deleteOnExit();

        context = SpringServiceContext.start("webdav", "classpath:org/dcache/webdav/webdav.xml",
              Map.of("webdav.net.port", "0",
                    "webdav.net.listen", "127.0.0.1",
                    "webdav.authn.protocol", "http",
                    "webdav.static-content.dir.local", localStatic.toString()),
              "missing-files-false", "owncloud-false", "macaroons-true");
        endpoint = URI.create("http://127.0.0.1:" + context.jettyPort());
        client = HttpClient.newHttpClient();
    }

    @AfterClass
    public static void tearDown() throws Exception {
        if (context != null) {
            context.close();
        }
    }

    @Test
    public void shouldStartJetty() {
        assertThat(context.getBean("jetty", Server.class).isStarted(), is(true));
    }

    @Test
    public void shouldServeDefaultStaticContent() throws Exception {
        HttpResponse<String> response = get("/.webdav/css/custom.css");

        assertThat(response.statusCode(), is(200));
        assertThat(response.headers().firstValue("Content-Type").orElse(""),
              containsString("text/css"));
    }

    @Test
    public void shouldServeRobotsTxtFromDomainContext() throws Exception {
        context.domainContext().put("robots.txt", "User-agent: *\nDisallow: /");

        HttpResponse<String> response = get("/robots.txt");

        assertThat(response.statusCode(), is(200));
        assertThat(response.body(), containsString("Disallow: /"));
    }

    private static HttpResponse<String> get(String path) throws Exception {
        return client.send(HttpRequest.newBuilder(endpoint.resolve(path)).GET().build(),
              HttpResponse.BodyHandlers.ofString());
    }
}
