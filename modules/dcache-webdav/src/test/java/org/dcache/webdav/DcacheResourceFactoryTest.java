package org.dcache.webdav;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.servlet.http.HttpServletRequest;
import org.junit.Test;

public class DcacheResourceFactoryTest {

    @Test
    public void shouldFindSciTagHeaderIgnoringCase() {
        HttpServletRequest request = mock(HttpServletRequest.class);
        given(request.getHeaderNames()).willReturn(Collections.enumeration(List.of("scitag")));
        given(request.getHeader("scitag")).willReturn("313");

        Optional<Map.Entry<String, String>> header =
              DcacheResourceFactory.findHeaderIgnoreCase(request, "SciTag");

        assertThat(header.isPresent(), is(true));
        assertThat(header.get().getKey(), is("scitag"));
        assertThat(header.get().getValue(), is("313"));
    }

    @Test
    public void shouldFindTransferHeaderSciTagIgnoringCase() {
        HttpServletRequest request = mock(HttpServletRequest.class);
        given(request.getHeaderNames()).willReturn(
              Collections.enumeration(List.of("transferheaderscitag")));
        given(request.getHeader("transferheaderscitag")).willReturn("777");

        Optional<Map.Entry<String, String>> header =
              DcacheResourceFactory.findHeaderIgnoreCase(request, "TransferHeaderSciTag");

        assertThat(header.isPresent(), is(true));
        assertThat(header.get().getKey(), is("transferheaderscitag"));
        assertThat(header.get().getValue(), is("777"));
    }

    @Test
    public void shouldFallbackToServletHeaderLookup() {
        HttpServletRequest request = mock(HttpServletRequest.class);
        given(request.getHeaderNames()).willReturn(null);
        given(request.getHeader("SciTag")).willReturn("313");

        Optional<Map.Entry<String, String>> header =
              DcacheResourceFactory.findHeaderIgnoreCase(request, "SciTag");

        assertThat(header.isPresent(), is(true));
        assertThat(header.get().getKey(), is("SciTag"));
        assertThat(header.get().getValue(), is("313"));
    }
}