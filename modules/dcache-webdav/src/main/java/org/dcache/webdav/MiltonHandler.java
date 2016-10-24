package org.dcache.webdav;

import com.google.common.collect.ImmutableList;
import io.milton.http.Auth;
import io.milton.http.HttpManager;
import io.milton.servlet.ServletRequest;
import io.milton.servlet.ServletResponse;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.eclipse.jetty.server.handler.ContextHandler;

import javax.security.auth.Subject;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.AccessController;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import dmg.cells.nucleus.CDC;
import dmg.cells.nucleus.CellAddressCore;
import dmg.cells.nucleus.CellIdentityAware;

import org.dcache.auth.Subjects;
import org.dcache.util.Transfer;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * A Jetty handler that wraps a Milton HttpManager. Makes it possible
 * to embed Milton in Jetty without using the Milton servlet.
 */
public class MiltonHandler
    extends AbstractHandler
    implements CellIdentityAware
{
    private static final ImmutableList<String> ALLOWED_ORIGIN_PROTOCOL = ImmutableList.of("http", "https");

    private HttpManager _httpManager;
    private CellAddressCore _myAddress;
    private List<String> _allowedClientOrigins;

    public void setHttpManager(HttpManager httpManager)
    {
        _httpManager = httpManager;
    }

    public void setAllowedClientOrigins(String origins)
    {
        if (origins.isEmpty()) {
            _allowedClientOrigins = Collections.emptyList();
        } else {
            List<String> originList = Arrays.stream(origins.split(","))
                    .map(String::trim)
                    .collect(Collectors.toList());
            originList.forEach(MiltonHandler::checkOrigin);
            _allowedClientOrigins = originList;
        }
    }

    private static void checkOrigin(String s)
    {
        try {
            URI uri = new URI(s);

            checkArgument(ALLOWED_ORIGIN_PROTOCOL.contains(uri.toURL().getProtocol()), "Invalid URL: The URL: %s " +
                    "contain unsupported protocol. Use either http or https.", s);
            checkArgument(!uri.getHost().isEmpty(), "Invalid URL: the host name is not provided in %s:", s);
            checkArgument(uri.getUserInfo() == null, "The URL: %s is invalid. User information is not allowed " +
                    "to be part of the URL.", s);
            checkArgument(uri.toURL().getPath().isEmpty(), "The URL: %s is invalid. Remove the \"path\" part of the " +
                    "URL.", s);
            checkArgument(uri.toURL().getQuery() == null, "The URL: %s is invalid. Remove the query-path part of " +
                    "the URL.", s);
            checkArgument(uri.toURL().getRef() == null, "URL: %s is invalid. Reason: no reference or fragment " +
                    "allowed in the URL.", s);
            checkArgument(!uri.isOpaque(), "URL: %s is invalid. Check the scheme part of the URL", s);
        } catch (MalformedURLException | URISyntaxException e) {
            throw new IllegalArgumentException(e);
        }
    }

    private void setCORSHeaders (HttpServletRequest request, HttpServletResponse response)
    {
        String clientOrigin = request.getHeader("origin");
        if (Objects.equals(request.getMethod(), "OPTIONS")) {
            response.setHeader("Access-Control-Allow-Methods", "PUT");
            response.setHeader("Access-Control-Allow-Headers", "Authorization, Content-Type");
            response.setHeader("Access-Control-Allow-Credentials", "true");
            response.setHeader("Access-Control-Allow-Origin", clientOrigin);
            if (_allowedClientOrigins.size() > 1) {
                response.setHeader("Vary", "Origin");
            }
        } else {
            response.setHeader("Access-Control-Allow-Origin", clientOrigin);
            if (_allowedClientOrigins.size() > 1) {
                response.setHeader("Vary", "Origin");
            }
        }
    }

    @Override
    public void setCellAddress(CellAddressCore address)
    {
        _myAddress = address;
    }

    @Override
    public void handle(String target, Request baseRequest,
                       HttpServletRequest request,HttpServletResponse response)
        throws IOException, ServletException
    {
        try (CDC ignored = CDC.reset(_myAddress)) {
            Transfer.initSession(false, false);
            ServletContext context = ContextHandler.getCurrentContext();
            String clientOrigin = request.getHeader("origin");

            boolean isOriginAllow = _allowedClientOrigins.contains(clientOrigin);
            if (isOriginAllow) {
                setCORSHeaders(request, response);
            }

            switch (request.getMethod()) {
            case "USERINFO":
                response.sendError(501, "Not implemented");
                break;
            case "OPTIONS":
                if (isOriginAllow) {
                    setCORSHeaders(request, response);
                }
                break;
            default:
                Subject subject = Subject.getSubject(AccessController.getContext());
                ServletRequest req = new DcacheServletRequest(request, context);
                ServletResponse resp = new DcacheServletResponse(response);

                /* Although we don't rely on the authorization tag
                 * ourselves, Milton uses it to detect that the request
                 * was preauthenticated.
                 */
                req.setAuthorization(new Auth(Subjects.getUserName(subject), subject));

                baseRequest.setHandled(true);
                _httpManager.process(req, resp);
            }
            response.getOutputStream().flush();
            response.flushBuffer();
        }
    }


    /**
     * Dcache specific subclass to workaround various Jetty/Milton problems.
     */
    private class DcacheServletRequest extends ServletRequest {
        public DcacheServletRequest(HttpServletRequest request,
                                    ServletContext context) {
            super(request, context);
        }

        @Override
        public InputStream getInputStream() {
            /* Jetty tells the client to continue uploading data as
             * soon as the input stream is retrieved by the servlet.
             * We want to redirect the request before that happens,
             * hence we query the input stream lazily.
             */
            return new InputStream() {
                private InputStream inner;

                private InputStream getRealInputStream() throws IOException
                {
                    if (inner == null) {
                        inner = DcacheServletRequest.super.getInputStream();
                    }
                    return inner;
                }

                @Override
                public int read() throws IOException
                {
                    return getRealInputStream().read();
                }

                @Override
                public int read(byte[] b) throws IOException
                {
                    return getRealInputStream().read(b);
                }

                @Override
                public int read(byte[] b, int off, int len)
                        throws IOException
                {
                    return getRealInputStream().read(b, off, len);
                }

                @Override
                public long skip(long n) throws IOException
                {
                    return getRealInputStream().skip(n);
                }

                @Override
                public int available() throws IOException
                {
                    return getRealInputStream().available();
                }

                @Override
                public void close() throws IOException
                {
                    getRealInputStream().close();
                }

                @Override
                public synchronized void mark(int readlimit)
                {
                    throw new UnsupportedOperationException("Mark is unsupported");
                }

                @Override
                public synchronized void reset() throws IOException
                {
                    getRealInputStream().reset();
                }

                @Override
                public boolean markSupported()
                {
                    return false;
                }
            };
        }
    }

    /**
     *  dCache specific subclass to workaround various Jetty/Milton problems.
     */
    private class DcacheServletResponse extends ServletResponse
    {
        public DcacheServletResponse(HttpServletResponse r)
        {
            super(r);
        }

        @Override
        public void setContentLengthHeader(Long length) {
            /* If the length is unknown, Milton insists on
            * setting an empty string value for the
            * Content-Length header.
            *
            * Instead we want the Content-Length header
            * to be skipped and rely on Jetty using
            * chunked encoding.
            */
            if (length != null) {
                super.setContentLengthHeader(length);
            }
        }
    }
}
