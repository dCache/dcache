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
package org.dcache.webdav.macaroons;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import dmg.cells.nucleus.CellAddressCore;
import javax.servlet.FilterChain;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.junit.Before;
import org.junit.Test;

public class MacaroonRequestFilterTest {

    private MacaroonRequestFilter filter;
    private HttpServletRequest request;
    private HttpServletResponse response;
    private FilterChain chain;

    @Before
    public void setUp() {
        filter = new MacaroonRequestFilter();
        filter.setCellAddress(new CellAddressCore("webdav", "test"));
        request = mock(HttpServletRequest.class);
        when(request.getRemoteAddr()).thenReturn("192.0.2.1");
        when(request.getServletPath()).thenReturn("/data/file");
        response = mock(HttpServletResponse.class);
        chain = mock(FilterChain.class);
    }

    @Test
    public void shouldPassOnOtherRequests() throws Exception {
        when(request.getMethod()).thenReturn("POST");
        when(request.getContentType()).thenReturn("application/octet-stream");

        filter.doFilter(request, response, chain);

        verify(chain).doFilter(request, response);
    }

    @Test
    public void shouldRefuseMacaroonRequestOverPlainHttp() throws Exception {
        when(request.getMethod()).thenReturn("POST");
        when(request.getContentType()).thenReturn("application/macaroon-request");
        when(request.isSecure()).thenReturn(false);

        filter.doFilter(request, response, chain);

        verify(chain, never()).doFilter(any(), any());
        verify(response).setStatus(HttpServletResponse.SC_BAD_REQUEST, "Not secure transport");
    }
}
