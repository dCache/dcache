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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.dcache.util.spring.RequiredPropertyChecker;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.junit.After;
import org.junit.Test;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.context.support.GenericXmlApplicationContext;
import org.springframework.core.io.ByteArrayResource;

public class ServletContextFactoryBeanTest {

    private static final String FACTORY = ServletContextFactoryBean.class.getName();
    private static final String FILTER = FACTORY + ".FilterEntry";
    private static final String SERVLET = FACTORY + ".ServletEntry";

    /**
     * Filter A on all paths, filter B on /b/*, a servlet on /b/* and a default servlet.
     */
    private static final String BEANS =
          "<bean id='context' class='" + FACTORY + "'>"
                + "  <property name='sessionHandler'>"
                + "    <bean class='org.eclipse.jetty.ee8.nested.SessionHandler'/>"
                + "  </property>"
                + "  <property name='filters'><list>"
                + "    <bean class='" + FILTER + "'>"
                + "      <property name='filter'><bean class='" + MarkingFilter.class.getName() + "'/></property>"
                + "      <property name='initParameters'><map><entry key='mark' value='A'/></map></property>"
                + "    </bean>"
                + "    <bean class='" + FILTER + "'>"
                + "      <property name='filter'><bean class='" + MarkingFilter.class.getName() + "'/></property>"
                + "      <property name='pathSpecs'><list><value>/b/*</value></list></property>"
                + "      <property name='initParameters'><map><entry key='mark' value='B'/></map></property>"
                + "    </bean>"
                + "  </list></property>"
                + "  <property name='servlets'><list>"
                + "    <bean class='" + SERVLET + "'>"
                + "      <property name='servlet'><bean class='" + PathServlet.class.getName() + "'/></property>"
                + "      <property name='pathSpecs'><list><value>/b/*</value></list></property>"
                + "      <property name='initParameters'><map><entry key='name' value='b'/></map></property>"
                + "    </bean>"
                + "    <bean class='" + SERVLET + "'>"
                + "      <property name='servlet'><bean class='" + PathServlet.class.getName() + "'/></property>"
                + "      <property name='pathSpecs'><list><value>/</value></list></property>"
                + "      <property name='initParameters'><map><entry key='name' value='default'/></map></property>"
                + "    </bean>"
                + "  </list></property>"
                + "</bean>";

    private GenericXmlApplicationContext context;
    private Server server;
    private final HttpClient client = HttpClient.newHttpClient();

    @After
    public void tearDown() throws Exception {
        if (server != null) {
            server.stop();
        }
        if (context != null) {
            context.close();
        }
    }

    @Test
    public void shouldRunFiltersInOrderBeforeServlet() throws Exception {
        start(BEANS);

        HttpResponse<String> response = get("/b/file");

        assertThat(response.statusCode(), is(200));
        assertThat(response.headers().allValues("X-Mark"), contains("A", "B"));
        assertThat(response.body(), is("b /b /file"));
    }

    @Test
    public void shouldApplyFilterOnlyToItsPathSpecs() throws Exception {
        start(BEANS);

        HttpResponse<String> response = get("/other");

        assertThat(response.headers().allValues("X-Mark"), contains("A"));
        assertThat(response.body(), is("default /other null"));
    }

    @Test
    public void shouldStopChainWhenFilterHandlesRequest() throws Exception {
        start(BEANS);

        HttpResponse<String> response = get("/b/stop-at-B");

        assertThat(response.statusCode(), is(403));
        assertThat(response.headers().allValues("X-Mark"), contains("A", "B"));
        assertThat(response.body(), is("stopped by B"));
    }

    @Test
    public void shouldNotRunFiltersOnForward() throws Exception {
        start(BEANS);

        HttpResponse<String> response = get("/forward-to/b/file");

        assertThat(response.headers().allValues("X-Mark"), contains("A"));
        assertThat(response.body(), is("b /b /file"));
    }

    @Test
    public void shouldProvideSessions() throws Exception {
        start(BEANS);

        HttpResponse<String> response = get("/b/session");

        assertThat(response.headers().firstValue("Set-Cookie").orElse(""),
              containsString("JSESSIONID"));
    }

    @Test
    public void shouldAnswerNotFoundWithoutMatchingServlet() throws Exception {
        start(BEANS.replace("<value>/</value>", "<value>/c/*</value>"));

        HttpResponse<String> response = get("/other");

        assertThat(response.statusCode(), is(404));
        assertThat(response.headers().allValues("X-Mark"), contains("A"));
    }

    @Test
    public void shouldWorkWithoutFiltersAndSessions() throws Exception {
        start("<bean id='context' class='" + FACTORY + "'>"
              + "  <property name='servlets'><list>"
              + "    <bean class='" + SERVLET + "'>"
              + "      <property name='servlet'><bean class='" + PathServlet.class.getName() + "'/></property>"
              + "      <property name='pathSpecs'><list><value>/</value></list></property>"
              + "    </bean>"
              + "  </list></property>"
              + "</bean>");

        HttpResponse<String> response = get("/b/session");

        assertThat(response.statusCode(), is(500));
        assertThat(response.headers().allValues("X-Mark"), is(empty()));
    }

    @Test
    public void shouldRequireServletOfEntry() {
        assertThrows(BeanCreationException.class,
              () -> start("<bean id='context' class='" + FACTORY + "'>"
                    + "  <property name='servlets'><list>"
                    + "    <bean class='" + SERVLET + "'>"
                    + "      <property name='pathSpecs'><list><value>/</value></list></property>"
                    + "    </bean>"
                    + "  </list></property>"
                    + "</bean>"));
    }

    private void start(String beans) throws Exception {
        String xml = "<?xml version='1.0' encoding='UTF-8'?>"
              + "<beans xmlns='http://www.springframework.org/schema/beans'"
              + "       xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance'"
              + "       xsi:schemaLocation='http://www.springframework.org/schema/beans"
              + "           http://www.springframework.org/schema/beans/spring-beans.xsd'>"
              + beans
              + "</beans>";
        context = new GenericXmlApplicationContext();
        RequiredPropertyChecker.register(context);
        context.load(new ByteArrayResource(xml.getBytes(UTF_8)));
        context.refresh();

        server = new Server();
        ServerConnector connector = new ServerConnector(server);
        connector.setHost("127.0.0.1");
        server.addConnector(connector);
        server.setHandler(context.getBean("context", Handler.class));
        server.start();
    }

    private HttpResponse<String> get(String path) throws Exception {
        int port = ((ServerConnector) server.getConnectors()[0]).getLocalPort();
        return client.send(HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + path))
              .GET().build(), HttpResponse.BodyHandlers.ofString());
    }

    /**
     * Adds its {@code mark} init parameter as X-Mark header; handles paths ending in
     * {@code stop-at-<mark>} itself.
     */
    public static class MarkingFilter implements Filter {

        private String mark;

        @Override
        public void init(FilterConfig config) {
            mark = config.getInitParameter("mark");
        }

        @Override
        public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
              throws IOException, ServletException {
            HttpServletResponse httpResponse = (HttpServletResponse) response;
            httpResponse.addHeader("X-Mark", mark);
            if (((HttpServletRequest) request).getRequestURI().endsWith("stop-at-" + mark)) {
                httpResponse.setStatus(403);
                httpResponse.getWriter().print("stopped by " + mark);
                return;
            }
            chain.doFilter(request, response);
        }
    }

    /**
     * Writes its {@code name} init parameter, servlet path and path info. Under
     * {@code /forward-to} it forwards the rest of the path; {@code /b/session} creates a session,
     * which fails without a session handler.
     */
    public static class PathServlet extends HttpServlet {

        @Override
        protected void doGet(HttpServletRequest request, HttpServletResponse response)
              throws ServletException, IOException {
            String path = request.getRequestURI();
            if (path.startsWith("/forward-to/")) {
                request.getRequestDispatcher(path.substring("/forward-to".length()))
                      .forward(request, response);
                return;
            }
            if (path.equals("/b/session")) {
                request.getSession(true);
            }
            response.getWriter().print(getInitParameter("name") + " "
                  + request.getServletPath() + " " + request.getPathInfo());
        }
    }
}
