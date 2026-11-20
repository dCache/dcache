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
package org.dcache.restful;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Map;
import org.dcache.tests.cells.SpringServiceContext;
import org.eclipse.jetty.server.Server;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Starts the frontend door's Spring context, outside a cell, with the shipped defaults.
 */
public class FrontendDoorContextTest {

    private static SpringServiceContext context;
    private static HttpClient client;
    private static URI endpoint;

    @BeforeClass
    public static void setup() throws Exception {
        context = SpringServiceContext.start("frontend",
              "classpath:org/dcache/frontend/frontend.xml",
              Map.of("frontend.net.port", "0",
                    "frontend.net.listen", "127.0.0.1",
                    "frontend.authn.protocol", "http"),
              "macaroons-true");
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
    public void shouldServeDcacheView() throws Exception {
        assertThat(get("/events.html").statusCode(), is(200));
    }

    @Test
    public void shouldServeSwaggerUi() throws Exception {
        HttpResponse<String> response = get("/api/v1/index.html");

        assertThat(response.statusCode(), is(200));
        assertThat(response.body(), containsString("swagger"));
    }

    @Test
    public void shouldServeSwaggerUiAsWelcomeFile() throws Exception {
        HttpResponse<String> response = get("/api/v1/");

        assertThat(response.statusCode(), is(200));
        assertThat(response.body(), containsString("swagger"));
    }

    private static HttpResponse<String> get(String path) throws Exception {
        return client.send(HttpRequest.newBuilder(endpoint.resolve(path)).GET().build(),
              HttpResponse.BodyHandlers.ofString());
    }
}
