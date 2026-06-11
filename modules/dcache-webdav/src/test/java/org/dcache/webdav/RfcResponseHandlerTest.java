package org.dcache.webdav;

import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;

import io.milton.http.Request;
import io.milton.http.Response;
import io.milton.http.webdav.WebDavResponseHandler;
import io.milton.servlet.ServletRequest;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import org.junit.Test;
import org.mockito.ArgumentMatchers;

public class RfcResponseHandlerTest {

    private static final String SHA256_BASE64 = "47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFU=";
    private static final String DIGEST_VALUE = "sha-256=" + SHA256_BASE64 ;
    private static final String REPR_DIGEST_VALUE = "sha-256=:" + SHA256_BASE64 + ":";

    private static HttpServletRequest requestWith(String wantDigest, String wantReprDigest) {
        HttpServletRequest req = mock(HttpServletRequest.class);
        given(req.getMethod()).willReturn("GET");
        given(req.getRequestURL()).willReturn(new StringBuffer("http://localhost/test"));
        given(req.getHeaders("Want-Digest")).willReturn(wantDigest == null
              ? Collections.emptyEnumeration()
              : Collections.enumeration(List.of(wantDigest)));
        given(req.getHeaders("Want-Repr-Digest")).willReturn(wantReprDigest == null
              ? Collections.emptyEnumeration()
              : Collections.enumeration(List.of(wantReprDigest)));
        return req;
    }

    @Test
    public void shouldSetDigestHeaderForRfc3230Request() {
        HttpServletRequest req = requestWith("sha-256", null);
        new ServletRequest(req, mock(ServletContext.class));

        DcacheFileResource resource = mock(DcacheFileResource.class);
        given(resource.getRfcDigest("Want-Digest")).willReturn(Optional.of(DIGEST_VALUE));
        Response response = mock(Response.class);

        RfcResponseHandler.wrap(mock(WebDavResponseHandler.class))
              .respondHead(resource, response, mock(Request.class));

        then(response).should().setNonStandardHeader("Digest", DIGEST_VALUE);
        then(response).should(never())
              .setNonStandardHeader(ArgumentMatchers.eq("Repr-Digest"), ArgumentMatchers.anyString());
    }

    @Test
    public void shouldSetReprDigestHeaderForRfc9530Request() {
        HttpServletRequest req = requestWith(null, "sha-256");
        new ServletRequest(req, mock(ServletContext.class));

        DcacheFileResource resource = mock(DcacheFileResource.class);
        given(resource.getRfcDigest("Want-Repr-Digest")).willReturn(Optional.of(REPR_DIGEST_VALUE));
        Response response = mock(Response.class);

        RfcResponseHandler.wrap(mock(WebDavResponseHandler.class))
              .respondHead(resource, response, mock(Request.class));

        then(response).should().setNonStandardHeader("Repr-Digest", REPR_DIGEST_VALUE);
        then(response).should(never())
              .setNonStandardHeader(ArgumentMatchers.eq("Digest"), ArgumentMatchers.anyString());
    }

    @Test
    public void shouldPreferReprDigestWhenBothHeadersPresent() {
        HttpServletRequest req = requestWith("adler32", "sha-256");
        new ServletRequest(req, mock(ServletContext.class));

        DcacheFileResource resource = mock(DcacheFileResource.class);
        given(resource.getRfcDigest("Want-Repr-Digest")).willReturn(Optional.of(REPR_DIGEST_VALUE));
        Response response = mock(Response.class);

        RfcResponseHandler.wrap(mock(WebDavResponseHandler.class))
              .respondHead(resource, response, mock(Request.class));

        then(response).should().setNonStandardHeader("Repr-Digest", REPR_DIGEST_VALUE);
        then(response).should(never())
              .setNonStandardHeader(ArgumentMatchers.eq("Digest"), ArgumentMatchers.anyString());
    }
}
