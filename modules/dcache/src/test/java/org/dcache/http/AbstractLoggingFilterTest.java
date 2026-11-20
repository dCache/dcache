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
package org.dcache.http;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import javax.servlet.AsyncContext;
import javax.servlet.AsyncListener;
import javax.servlet.FilterChain;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.slf4j.Logger;

public class AbstractLoggingFilterTest {

    private final Map<String, Object> attributes = new HashMap<>();
    private final Logger accessLog = mock(Logger.class);

    private AbstractLoggingFilter filter;
    private HttpServletRequest request;
    private HttpServletResponse response;
    private FilterChain chain;

    @Before
    public void setUp() {
        filter = new AbstractLoggingFilter() {
            @Override
            protected Logger accessLogger() {
                return accessLog;
            }

            @Override
            protected String requestEventName() {
                return "org.dcache.test.request";
            }
        };

        request = mock(HttpServletRequest.class);
        when(request.getRemoteAddr()).thenReturn("192.0.2.1");
        when(request.getRemotePort()).thenReturn(40000);
        when(request.getMethod()).thenReturn("GET");
        when(request.getRequestURL()).thenReturn(new StringBuffer("http://localhost/file"));
        doAnswer(i -> attributes.put(i.getArgument(0), i.getArgument(1)))
              .when(request).setAttribute(anyString(), any());
        when(request.getAttribute(anyString())).thenAnswer(i -> attributes.get(i.getArgument(0)));

        response = mock(HttpServletResponse.class);
        chain = mock(FilterChain.class);
    }

    @Test
    public void shouldLogCompletedRequest() throws Exception {
        when(response.getStatus()).thenReturn(200);

        filter.doFilter(request, response, chain);

        verify(chain).doFilter(request, response);
        String line = loggedAtInfo();
        assertThat(line, containsString("org.dcache.test.request"));
        assertThat(line, containsString("request.method=GET"));
        assertThat(line, containsString("response.code=200"));
        assertThat(line, containsString("socket.remote=192.0.2.1:40000"));
    }

    @Test
    public void shouldLogClientErrorAsWarning() throws Exception {
        when(response.getStatus()).thenReturn(404);

        filter.doFilter(request, response, chain);

        verify(accessLog).warn(contains("response.code=404"));
    }

    @Test
    public void shouldLogAndRethrowFailure() throws Exception {
        doThrow(new IOException("connection reset")).when(chain).doFilter(any(), any());

        assertThrows(IOException.class, () -> filter.doFilter(request, response, chain));

        assertThat(loggedAtInfo(), containsString("connection reset"));
    }

    @Test
    public void shouldLogAsyncRequestOnCompletion() throws Exception {
        AsyncContext async = mock(AsyncContext.class);
        when(request.isAsyncStarted()).thenReturn(true);
        when(request.getAsyncContext()).thenReturn(async);

        filter.doFilter(request, response, chain);

        verify(accessLog, never()).info(anyString());
        verify(async).addListener(any(AsyncListener.class));
    }

    private String loggedAtInfo() {
        ArgumentCaptor<String> line = ArgumentCaptor.forClass(String.class);
        verify(accessLog).info(line.capture());
        return line.getValue();
    }
}
