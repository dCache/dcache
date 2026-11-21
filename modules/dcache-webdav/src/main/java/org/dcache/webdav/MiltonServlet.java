package org.dcache.webdav;

import com.google.common.collect.ImmutableList;
import dmg.cells.nucleus.CDC;
import dmg.cells.nucleus.CellAddressCore;
import dmg.cells.nucleus.CellIdentityAware;
import io.milton.http.Auth;
import io.milton.http.HttpManager;
import io.milton.servlet.ServletRequest;
import io.milton.servlet.ServletResponse;
import java.io.IOException;
import java.io.InputStream;
import javax.security.auth.Subject;
import javax.servlet.ServletContext;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.dcache.auth.Subjects;
import org.dcache.util.Transfer;
import org.eclipse.jetty.ee8.nested.Request;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A servlet that wraps a Milton HttpManager. Makes it possible to embed Milton in Jetty
 * without using the Milton servlet.
 */
public class MiltonServlet
      extends HttpServlet
      implements CellIdentityAware {

    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = LoggerFactory.getLogger(MiltonServlet.class);
    private static final ImmutableList<String> ALLOWED_ORIGIN_PROTOCOL = ImmutableList.of("http",
          "https");

    private transient HttpManager _httpManager;
    private transient CellAddressCore _myAddress;

    public void setHttpManager(HttpManager httpManager) {
        _httpManager = httpManager;
    }

    @Override
    public void setCellAddress(CellAddressCore address) {
        _myAddress = address;
    }

    @Override
    protected void service(HttpServletRequest request, HttpServletResponse response)
          throws IOException {
        try (CDC ignored = CDC.reset(_myAddress)) {
            Transfer.initSession(false, false);
            ServletContext context = getServletContext();

            if ("USERINFO".equals(request.getMethod())) {
                response.sendError(501);
            } else {
                Subject subject = Subject.current();
                ServletRequest req = new DcacheServletRequest(request, context);
                ServletResponse resp = new DcacheServletResponse(response);

                /* Although we don't rely on the authorization tag
                 * ourselves, Milton uses it to detect that the request
                 * was preauthenticated.
                 */
                req.setAuthorization(new Auth(Subjects.getUserName(subject), subject));

                _httpManager.process(req, resp);
            }
            if (!request.isAsyncStarted()) {
                response.getOutputStream().flush();
                response.flushBuffer();
            }
        }
    }


    /**
     * Dcache specific subclass to workaround various Jetty/Milton problems.
     */
    class DcacheServletRequest extends ServletRequest {

        private final HttpServletRequest request;

        public DcacheServletRequest(HttpServletRequest request,
              ServletContext context) {
            super(request, context);
            this.request = request;
        }

        /**
         * Is there content from the client that is unparsed by Jetty. This is equivalent to {@code
         * getInputStream().available() > 0} with the distinction that it does not result in the
         * "100 Continue" response that {@code getInputStream()} would normally by triggered.
         */
        public boolean isClientSendingEntity() {
            try {
                /* This is a work-around for Jetty where calling getInputStream
                 * results in Jetty immediately returning "100 Continue" to
                 * the client.  The Jetty-specific getHttpInput method
                 * provides a "back door" that returns the InputStream without
                 * triggering this behaviour.
                 */
                InputStream in = request instanceof Request
                      ? ((Request) request).getHttpInput()
                      : getInputStream();

                return in.available() > 0;
            } catch (IOException e) {
                LOGGER.warn("Got exception in hasContent: {}", e.toString());
                return false;
            }
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

                private InputStream getRealInputStream() throws IOException {
                    if (inner == null) {
                        inner = DcacheServletRequest.super.getInputStream();
                    }
                    return inner;
                }

                @Override
                public int read() throws IOException {
                    return getRealInputStream().read();
                }

                @Override
                public int read(byte[] b) throws IOException {
                    return getRealInputStream().read(b);
                }

                @Override
                public int read(byte[] b, int off, int len)
                      throws IOException {
                    return getRealInputStream().read(b, off, len);
                }

                @Override
                public long skip(long n) throws IOException {
                    return getRealInputStream().skip(n);
                }

                @Override
                public int available() throws IOException {
                    return getRealInputStream().available();
                }

                @Override
                public void close() throws IOException {
                    getRealInputStream().close();
                }

                @Override
                public synchronized void mark(int readlimit) {
                    throw new UnsupportedOperationException("Mark is unsupported");
                }

                @Override
                public synchronized void reset() throws IOException {
                    getRealInputStream().reset();
                }

                @Override
                public boolean markSupported() {
                    return false;
                }
            };
        }
    }

    /**
     * dCache specific subclass to workaround various Jetty/Milton problems.
     */
    private class DcacheServletResponse extends ServletResponse {

        public DcacheServletResponse(HttpServletResponse r) {
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
