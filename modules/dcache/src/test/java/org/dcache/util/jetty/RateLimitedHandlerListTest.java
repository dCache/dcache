package org.dcache.util.jetty;

import org.eclipse.jetty.http.HttpStatus;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import javax.servlet.ServletException;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;

import static org.mockito.BDDMockito.given;

public class RateLimitedHandlerListTest {

    private RateLimitedHandlerList handlerList;

    @Before
    public void setUp() throws Exception {

        var config = new RateLimitedHandlerList.Configuration();
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

        handlerList = new RateLimitedHandlerList(config);

        handlerList.addHandler(new AbstractHandler() {
            @Override
            public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) {
                try {
                    // fake some work otherwise the rate limiter will block right away
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        });
        handlerList.start();
        //handlerList.setMaxGlobalRequestsPerSecond(10);
    }

    @Test
    public void testGlobalRateOk() throws ServletException, IOException, InterruptedException {

        Request baseRequest = new Request(null, null);
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);

        String[] ips = IntStream.range(2,21).mapToObj(i -> "127.1.1." + i).toArray(String[]::new);
        given(request.getRemoteAddr()).willReturn("127.1.1.1", ips);

        HttpServletResponse response = new SimpleResponse();

        for (int i = 0; i < 20; i++) {
            response.setStatus(0);
            handlerList.handle("/foo", baseRequest, request, response);
        }

        assertEquals(0, response.getStatus());
    }


    @Test
    public void testGlobalRateExceeded() throws ServletException, IOException, InterruptedException {

        handlerList.setMaxGlobalRequestsPerSecond(10);

        Request baseRequest = new Request(null, null);
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);


        String[] ips = IntStream.range(2,21).mapToObj(i -> "127.1.1." + i).toArray(String[]::new);
        given(request.getRemoteAddr()).willReturn("127.1.1.1", ips);
        HttpServletResponse response = new SimpleResponse();

        for (int i = 0; i < 20; i++) {
            handlerList.handle("/foo", baseRequest, request, response);
        }

        assertEquals(HttpStatus.TOO_MANY_REQUESTS_429, response.getStatus());
    }


    @Test
    public void testBlockBadAuth() throws ServletException, IOException, InterruptedException {

        Handler badAuthHandler = new AbstractHandler() {
            @Override
            public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) {
                response.setStatus(HttpStatus.UNAUTHORIZED_401);
                baseRequest.setHandled(true);
            }
        };
        handlerList.addHandler(badAuthHandler);


        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);

        given(request.getRemoteAddr()).willReturn("127.1.1.1");
        HttpServletResponse response = new SimpleResponse();

        for (int i = 0; i < 11; i++) {
            Request baseRequest = new Request(null, null);
            response.setStatus(0);
            handlerList.handle("/foo", baseRequest, request, response);
        }

        assertEquals(HttpStatus.TOO_MANY_REQUESTS_429, response.getStatus());

        given(request.getRemoteAddr()).willReturn("127.1.1.2");
        response.setStatus(0);
        Request baseRequest = new Request(null, null);
        handlerList.handle("/foo", baseRequest, request, response);

        assertEquals(HttpStatus.UNAUTHORIZED_401, response.getStatus());
    }



    private static class SimpleResponse implements HttpServletResponse {

        int status;

        @Override
        public void addCookie(Cookie cookie) {

        }

        @Override
        public boolean containsHeader(String s) {
            return false;
        }

        @Override
        public String encodeURL(String s) {
            return "";
        }

        @Override
        public String encodeRedirectURL(String s) {
            return "";
        }

        @Override
        public String encodeUrl(String s) {
            return "";
        }

        @Override
        public String encodeRedirectUrl(String s) {
            return "";
        }

        @Override
        public void sendError(int i, String s) throws IOException {

        }

        @Override
        public void sendError(int i) throws IOException {

        }

        @Override
        public void sendRedirect(String s) throws IOException {

        }

        @Override
        public void setDateHeader(String s, long l) {

        }

        @Override
        public void addDateHeader(String s, long l) {

        }

        @Override
        public void setHeader(String s, String s1) {

        }

        @Override
        public void addHeader(String s, String s1) {

        }

        @Override
        public void setIntHeader(String s, int i) {

        }

        @Override
        public void addIntHeader(String s, int i) {

        }

        @Override
        public void setStatus(int i) {
            status = i;
        }

        @Override
        public void setStatus(int i, String s) {
            status = i;
        }

        @Override
        public int getStatus() {
            return status;
        }

        @Override
        public String getHeader(String s) {
            return "";
        }

        @Override
        public Collection<String> getHeaders(String s) {
            return List.of();
        }

        @Override
        public Collection<String> getHeaderNames() {
            return List.of();
        }

        @Override
        public String getCharacterEncoding() {
            return "";
        }

        @Override
        public String getContentType() {
            return "";
        }

        @Override
        public ServletOutputStream getOutputStream() throws IOException {
            return null;
        }

        @Override
        public PrintWriter getWriter() throws IOException {
            return new PrintWriter(PrintWriter.nullWriter());
        }

        @Override
        public void setCharacterEncoding(String s) {

        }

        @Override
        public void setContentLength(int i) {

        }

        @Override
        public void setContentLengthLong(long l) {

        }

        @Override
        public void setContentType(String s) {

        }

        @Override
        public void setBufferSize(int i) {

        }

        @Override
        public int getBufferSize() {
            return 0;
        }

        @Override
        public void flushBuffer() throws IOException {

        }

        @Override
        public void resetBuffer() {

        }

        @Override
        public boolean isCommitted() {
            return false;
        }

        @Override
        public void reset() {

        }

        @Override
        public void setLocale(Locale locale) {

        }

        @Override
        public Locale getLocale() {
            return null;
        }
    }
}