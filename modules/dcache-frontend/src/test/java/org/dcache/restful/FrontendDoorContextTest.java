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
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;

import java.io.IOException;
import java.net.JarURLConnection;
import java.net.URI;
import java.net.URL;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Map;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
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
        String[] swaggerUi = swaggerUiJar();
        context = SpringServiceContext.start("frontend",
              "classpath:org/dcache/frontend/frontend.xml",
              Map.of("frontend.net.port", "0",
                    "frontend.net.listen", "127.0.0.1",
                    "frontend.authn.protocol", "http",
                    "dcache.paths.swagger-ui.jar", swaggerUi[0],
                    "dcache.paths.swagger-ui.jar.internal", swaggerUi[1]),
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

    @Test
    public void shouldServeSwaggerUiWithoutTrailingSlash() throws Exception {
        HttpResponse<String> response = get("/api/v1");

        assertThat(response.statusCode(), is(200));
        assertThat(response.body(), containsString("swagger"));
    }

    @Test
    public void shouldServeSwaggerUiLibraryFromJar() throws Exception {
        HttpResponse<String> response = get("/api/libraries/swagger-ui/swagger-ui.css");

        assertThat(response.statusCode(), is(200));
        assertThat(response.headers().firstValue("Content-Type").orElse(""),
              startsWith("text/css"));
    }

    @Test
    public void shouldServeStaticDataAsJavaScript() throws Exception {
        HttpResponse<String> response = get("/scripts/config.js");

        assertThat(response.statusCode(), is(200));
        assertThat(response.body(), startsWith("var CONFIG = {"));
    }

    @Test
    public void shouldServeStaticDataAsJson() throws Exception {
        HttpResponse<String> response = client.send(
              HttpRequest.newBuilder(endpoint.resolve("/scripts/config.js"))
                    .header("Accept", "text/html, application/json;q=0.9").GET().build(),
              HttpResponse.BodyHandlers.ofString());

        assertThat(response.statusCode(), is(200));
        assertThat(response.headers().firstValue("Content-Type").orElse(""),
              startsWith("application/json"));
        assertThat(response.body(), startsWith("{"));
    }

    @Test
    public void shouldRedirectAdminToDirectory() throws Exception {
        HttpResponse<String> response = get("/admin");

        assertThat(response.statusCode(), is(302));
        assertThat(response.headers().firstValue("Location").orElse(""), endsWith("/admin/"));
    }

    @Test
    public void shouldServeDcacheViewAdmin() throws Exception {
        assertThat(get("/admin/").statusCode(), is(200));
    }

    @Test
    public void shouldAnswerUnknownPath() throws Exception {
        assertThat(get("/no-such-file").statusCode(), is(404));
    }

    @Test
    public void shouldAnswerUnknownWellKnownEndpoint() throws Exception {
        assertThat(get("/.well-known/no-such-endpoint").statusCode(), is(404));
    }

    @Test
    public void shouldAuthenticateApiRequests() throws Exception {
        HttpResponse<String> response = client.send(
              HttpRequest.newBuilder(endpoint.resolve("/api/v1/user"))
                    .header("Authorization", "Bearer token").GET().build(),
              HttpResponse.BodyHandlers.ofString());

        // The authentication filter refuses bearer tokens sent unencrypted.
        assertThat(response.statusCode(), is(401));
        assertThat(response.headers().firstValue("WWW-Authenticate").orElse(""),
              containsString("Basic realm="));
    }

    /**
     * Returns the swagger-ui webjar on the class path and the directory of swagger-ui in it.
     */
    private static String[] swaggerUiJar() throws IOException {
        URL url = FrontendDoorContextTest.class.getClassLoader()
              .getResource("META-INF/resources/webjars/swagger-ui/");
        JarURLConnection connection = (JarURLConnection) url.openConnection();
        connection.setUseCaches(false);
        try (JarFile jar = connection.getJarFile()) {
            String css = jar.stream()
                  .map(JarEntry::getName)
                  .filter(n -> n.endsWith("/swagger-ui.css"))
                  .findFirst().orElseThrow();
            return new String[]{jar.getName(),
                  "/" + css.substring(0, css.length() - "/swagger-ui.css".length())};
        }
    }

    private static HttpResponse<String> get(String path) throws Exception {
        return client.send(HttpRequest.newBuilder(endpoint.resolve(path)).GET().build(),
              HttpResponse.BodyHandlers.ofString());
    }
}
