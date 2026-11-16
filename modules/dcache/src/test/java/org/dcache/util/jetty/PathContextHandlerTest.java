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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.eclipse.jetty.ee8.nested.AbstractHandler;
import org.eclipse.jetty.ee8.nested.ContextHandler;
import org.eclipse.jetty.ee8.nested.HandlerList;
import org.eclipse.jetty.ee8.nested.Request;
import org.eclipse.jetty.ee8.nested.ResourceHandler;
import org.eclipse.jetty.http.HttpTester;
import org.eclipse.jetty.server.LocalConnector;
import org.eclipse.jetty.server.Server;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class PathContextHandlerTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private Server server;
    private LocalConnector connector;

    @Before
    public void setUp() throws Exception {
        Path docs = folder.newFolder("docs").toPath();
        Files.writeString(docs.resolve("index.html"), "welcome");

        ResourceHandler resources = new ResourceHandler();
        resources.setBaseResource(docs);
        PathContextHandler doc = new PathContextHandler();
        doc.setContextPath("/doc");
        doc.setHandler(resources);

        PathContextHandler stat = new PathContextHandler();
        stat.setContextPath("/static/");
        stat.setHandler(new EchoHandler("static"));

        PathContextHandler robots = new PathContextHandler();
        robots.setContextPath("/robots.txt");
        robots.setAllowNullPathInfo(true);
        robots.setHandler(new EchoHandler("robots"));

        PathContextHandler declining = new PathContextHandler();
        declining.setContextPath("/declined");
        declining.setHandler(new AbstractHandler() {
            @Override
            public void handle(String target, Request baseRequest, HttpServletRequest request,
                  HttpServletResponse response) {
                // not handled: next handler in the list gets the request
            }
        });

        FallThroughHandler fallThrough = new FallThroughHandler();
        fallThrough.setFirst(List.of(stat, robots, declining, doc));
        fallThrough.setHandler(new EchoHandler("other"));

        Path rootDocs = folder.newFolder("root").toPath();
        Files.writeString(rootDocs.resolve("index.html"), "root welcome");
        ResourceHandler rootResources = new ResourceHandler();
        rootResources.setBaseResource(rootDocs);

        ContextHandler root = new ContextHandler("/");
        HandlerList handlers = new HandlerList();
        handlers.addHandler(rootResources);
        handlers.addHandler(fallThrough);
        root.setHandler(handlers);

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
    public void shouldPassPrefixAsContextPathAndRestAsPathInfo() throws Exception {
        assertEquals("static target=/a/b.txt context=/static servlet= path=/a/b.txt"
              + " uri=/static/a/b.txt", get("/static/a/b.txt").getContent());
    }

    @Test
    public void shouldRedirectPrefixWithoutSlash() throws Exception {
        HttpTester.Response response = get("/static?x=1");
        assertEquals(302, response.getStatus());
        assertTrue(response.get("Location"), response.get("Location").endsWith("/static/?x=1"));
    }

    @Test
    public void shouldAcceptNullPathInfoIfAllowed() throws Exception {
        assertEquals("robots target=/ context=/robots.txt servlet= path=null uri=/robots.txt",
              get("/robots.txt").getContent());
    }

    @Test
    public void shouldNotMatchOtherPrefixes() throws Exception {
        assertEquals("other target=/staticfile context= servlet= path=/staticfile"
              + " uri=/staticfile", get("/staticfile").getContent());
    }

    @Test
    public void shouldFallThroughIfNotHandled() throws Exception {
        assertEquals("other target=/declined/x context= servlet= path=/declined/x"
              + " uri=/declined/x", get("/declined/x").getContent());
    }

    @Test
    public void shouldServeWelcomeFileBelowPrefix() throws Exception {
        HttpTester.Response response = get("/doc/");
        assertEquals(200, response.getStatus());
        assertEquals("welcome", response.getContent());
    }

    @Test
    public void shouldServeRootWelcomeFile() throws Exception {
        assertEquals("root welcome", get("/").getContent());
    }

    @Test
    public void shouldRejectRootAsPrefix() {
        assertThrows(IllegalArgumentException.class,
              () -> new PathContextHandler().setContextPath("/"));
    }

    private HttpTester.Response get(String path) throws Exception {
        String request = "GET " + path + " HTTP/1.1\r\n"
              + "Host: localhost\r\n"
              + "Connection: close\r\n"
              + "\r\n";
        return HttpTester.parseResponse(connector.getResponse(request));
    }

    private static class EchoHandler extends AbstractHandler {

        private final String name;

        EchoHandler(String name) {
            this.name = name;
        }

        @Override
        public void handle(String target, Request baseRequest, HttpServletRequest request,
              HttpServletResponse response) throws IOException {
            baseRequest.setHandled(true);
            response.setContentType("text/plain");
            response.getWriter().printf("%s target=%s context=%s servlet=%s path=%s uri=%s", name,
                  target, request.getContextPath(), request.getServletPath(),
                  request.getPathInfo(), request.getRequestURI());
        }
    }
}
