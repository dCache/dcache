package org.dcache.webdav;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

import java.net.UnknownHostException;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.Optional;
import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;

import org.dcache.util.HttpExtHeader;
import org.junit.Test;

public class DcacheResourceFactoryTest {

    @Test
    public void shouldFindSciTagHeaderIgnoringCase() {
        HttpServletRequest request = mock(HttpServletRequest.class);
        given(request.getHeaderNames()).willReturn(Collections.enumeration(List.of("scitag")));
        given(request.getHeader("scitag")).willReturn("313");

        Optional<String> header =
              DcacheResourceFactory.findHeaderIgnoreCase(request, "SciTag");

        assertThat(header.isPresent(), is(true));
        assertThat(header.get(), is("313"));
    }

    @Test
    public void shouldFindTransferHeaderSciTagIgnoringCase() {
        HttpServletRequest request = mock(HttpServletRequest.class);
        given(request.getHeaderNames()).willReturn(
              Collections.enumeration(List.of("transferheaderscitag")));
        given(request.getHeader("transferheaderscitag")).willReturn("777");

        Optional<String> header =
              DcacheResourceFactory.findHeaderIgnoreCase(request, "TransferHeaderSciTag");

        assertThat(header.isPresent(), is(true));
        assertThat(header.get(), is("777"));
    }

    @Test
    public void shouldFallbackToServletHeaderLookup() {
        HttpServletRequest request = mock(HttpServletRequest.class);
        given(request.getHeaderNames()).willReturn(null);
        given(request.getHeader("SciTag")).willReturn("313");

        Optional<String> header =
              DcacheResourceFactory.findHeaderIgnoreCase(request, "SciTag");

        assertThat(header.isPresent(), is(true));
        assertThat(header.get(), is("313"));
    }

    @Test
    public void testWantDigestShouldPreferRfc9530Header() {
        Enumeration<String> e1 = Collections.enumeration(List.of("sha-256", "adler32"));
        Enumeration<String> e2 = Collections.enumeration(List.of("sha-256"));
        HttpServletRequest request = mock(HttpServletRequest.class);
        ServletContext servletContext = mock(ServletContext.class);
        given(request.getMethod()).willReturn("PUT");
        given(request.getRequestURL()).willReturn(new StringBuffer("http://localhost/test"));
        given(request.getHeaders(HttpExtHeader.WANT_DIGEST)).willReturn(e1);
        given(request.getHeaders(HttpExtHeader.WANT_REPR_DIGEST)).willReturn(e2);

        new io.milton.servlet.ServletRequest(request, servletContext);

        assertEquals("sha-256", DcacheResourceFactory.wantDigest(HttpExtHeader.WANT_REPR_DIGEST).get());
    }
}
