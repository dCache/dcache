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
package org.dcache.util.jetty;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;
import javax.servlet.http.HttpServletResponse;
import org.eclipse.jetty.ee8.nested.AbstractHandler;
import org.eclipse.jetty.ee8.nested.ContextHandler;
import org.eclipse.jetty.ee8.nested.HandlerList;
import org.eclipse.jetty.ee8.nested.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class FilterHandlerTest {

    private Server server;
    private URI endpoint;
    private final HttpClient client = HttpClient.newHttpClient();

    @Before
    public void setUp() throws Exception {
        FilterHandler filterHandler = new FilterHandler();
        filterHandler.setFilter(new BlockingFilter());
        filterHandler.setHandler(new InnerHandler());

        HandlerList handlers = new HandlerList();
        handlers.addHandler(filterHandler);
        handlers.addHandler(new AnsweringHandler("fallback"));

        ContextHandler context = new ContextHandler("/");
        context.setHandler(handlers);

        server = new Server();
        ServerConnector connector = new ServerConnector(server);
        connector.setHost("127.0.0.1");
        connector.setPort(0);
        server.addConnector(connector);
        server.setHandler(context.get());
        server.start();
        endpoint = URI.create("http://127.0.0.1:" + connector.getLocalPort());
    }

    @After
    public void tearDown() throws Exception {
        server.stop();
    }

    @Test
    public void shouldPassRequestToWrappedHandler() throws Exception {
        HttpResponse<String> response = get("/inner");

        assertThat(response.statusCode(), is(200));
        assertThat(response.body(), is("inner wrapped"));
    }

    @Test
    public void shouldHandleRequestTheFilterAnswers() throws Exception {
        assertThat(get("/blocked").statusCode(), is(403));
    }

    @Test
    public void shouldFallThroughWhenWrappedHandlerDoesNotHandle() throws Exception {
        HttpResponse<String> response = get("/other");

        assertThat(response.statusCode(), is(200));
        assertThat(response.body(), is("fallback"));
    }

    private HttpResponse<String> get(String path) throws Exception {
        return client.send(HttpRequest.newBuilder(endpoint.resolve(path)).GET().build(),
              HttpResponse.BodyHandlers.ofString());
    }

    /**
     * Answers 403 on /blocked; passes other requests on, wrapped.
     */
    public static class BlockingFilter implements Filter {

        @Override
        public void doFilter(ServletRequest request, ServletResponse response,
              FilterChain chain) throws IOException, ServletException {
            HttpServletRequest httpRequest = (HttpServletRequest) request;
            if (httpRequest.getRequestURI().equals("/blocked")) {
                ((HttpServletResponse) response).sendError(HttpServletResponse.SC_FORBIDDEN);
            } else {
                chain.doFilter(new HttpServletRequestWrapper(httpRequest) {
                    @Override
                    public Object getAttribute(String name) {
                        return name.equals("wrapped") ? "wrapped" : super.getAttribute(name);
                    }
                }, response);
            }
        }
    }

    /**
     * Handles /inner only, showing that it sees the request of the filter.
     */
    private static class InnerHandler extends AbstractHandler {

        @Override
        public void handle(String target, Request baseRequest, HttpServletRequest request,
              HttpServletResponse response) throws IOException {
            if (target.equals("/inner")) {
                response.getWriter().print("inner " + request.getAttribute("wrapped"));
                baseRequest.setHandled(true);
            }
        }
    }

    private static class AnsweringHandler extends AbstractHandler {

        private final String body;

        AnsweringHandler(String body) {
            this.body = body;
        }

        @Override
        public void handle(String target, Request baseRequest, HttpServletRequest request,
              HttpServletResponse response) throws IOException {
            response.getWriter().print(body);
            baseRequest.setHandled(true);
        }
    }
}
