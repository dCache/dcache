package org.dcache.util.jetty;

import static org.junit.Assert.assertEquals;

import java.time.temporal.ChronoUnit;
import org.eclipse.jetty.http.HttpStatus;
import org.eclipse.jetty.http.HttpTester;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.LocalConnector;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Response;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.util.Callback;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class RateLimitedHandlerTest {

    private Server server;
    private LocalConnector connector;

    @Before
    public void setUp() throws Exception {

        var config = new RateLimitedHandler.Configuration();
        config.setMaxClientsToTrack(100);
        config.setGlobalRequestsPerSecond(100);
        config.setNumErrorsBeforeBlocking(3);
        config.setLimitPercentagePerClient(25);
        config.setClientIdleTime(10);
        config.setClientIdleTimeUnit(ChronoUnit.MINUTES);
        config.setClientBlockingTime(10);
        config.setClientBlockingTimeUnit(ChronoUnit.SECONDS);
        config.setErrorCountingWindow(20);
        config.setErrorCountingWindowUnit(ChronoUnit.SECONDS);

        RateLimitedHandler rateLimiter = new RateLimitedHandler(config);
        rateLimiter.setHandler(new Handler.Abstract() {
            @Override
            public boolean handle(Request request, Response response, Callback callback) {
                if (Request.getPathInContext(request).startsWith("/bad-auth")) {
                    response.setStatus(HttpStatus.UNAUTHORIZED_401);
                }
                callback.succeeded();
                return true;
            }
        });

        server = new Server();
        connector = new LocalConnector(server);
        server.addConnector(connector);
        server.setHandler(rateLimiter);
        server.start();
    }

    @After
    public void tearDown() throws Exception {
        server.stop();
    }

    @Test
    public void testGlobalRateOk() throws Exception {
        for (int i = 0; i < 20; i++) {
            assertEquals(HttpStatus.OK_200, get("/foo", "127.1.1." + i).getStatus());
        }
    }

    @Test
    public void testBlockBadAuth() throws Exception {
        for (int i = 0; i < 3; i++) {
            assertEquals(HttpStatus.UNAUTHORIZED_401, get("/bad-auth", "127.1.1.1").getStatus());
        }

        HttpTester.Response blocked = get("/foo", "127.1.1.1");
        assertEquals(HttpStatus.TOO_MANY_REQUESTS_429, blocked.getStatus());
        assertEquals("Server is busy. Please try again later.", blocked.getContent());

        assertEquals(HttpStatus.UNAUTHORIZED_401, get("/bad-auth", "127.1.1.2").getStatus());
        assertEquals(HttpStatus.OK_200, get("/foo", "127.1.1.2").getStatus());
    }

    private HttpTester.Response get(String path, String client) throws Exception {
        String request = "GET " + path + " HTTP/1.1\r\n"
              + "Host: localhost\r\n"
              + "X-Forwarded-For: " + client + "\r\n"
              + "Connection: close\r\n"
              + "\r\n";
        return HttpTester.parseResponse(connector.getResponse(request));
    }
}
