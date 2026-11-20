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
package org.dcache.services.httpd;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Map;
import org.dcache.tests.cells.SpringServiceContext;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Starts the httpd service's Spring context, outside a cell, with the shipped defaults, and
 * creates aliases with the commands of httpd.batch.
 */
public class HttpdContextTest {

    private static SpringServiceContext context;
    private static HttpClient client;
    private static URI endpoint;

    @BeforeClass
    public static void setup() throws Exception {
        context = SpringServiceContext.start("httpd",
              "classpath:org/dcache/services/httpd/httpd.xml",
              Map.of("httpd.net.port", "0",
                    "httpd.net.listen", "127.0.0.1"));
        context.domainContext().put("missing.html", "<html>missing</html>");
        context.domainContext().put("robots.txt", "User-agent: *");
        context.command("set alias <default> context missing.html -status=404");
        context.command("set alias robots.txt context robots.txt");
        context.command("set alias docs directory " + context.property("httpd.static-content.docs"));
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
    public void shouldServeDirectoryAlias() throws Exception {
        HttpResponse<String> response = get("/docs/statisticsHelp.html");

        assertThat(response.statusCode(), is(200));
        assertThat(response.headers().firstValue("Content-Type").orElse(""),
              containsString("text/html"));
    }

    @Test
    public void shouldServeContextAlias() throws Exception {
        HttpResponse<String> response = get("/robots.txt");

        assertThat(response.statusCode(), is(200));
        assertThat(response.body(), containsString("User-agent: *"));
    }

    @Test
    public void shouldUseDefaultAliasForUnknownAlias() throws Exception {
        HttpResponse<String> response = get("/no-such-alias");

        assertThat(response.statusCode(), is(404));
        assertThat(response.body(), containsString("missing"));
    }

    private static HttpResponse<String> get(String path) throws Exception {
        return client.send(HttpRequest.newBuilder(endpoint.resolve(path)).GET().build(),
              HttpResponse.BodyHandlers.ofString());
    }
}
