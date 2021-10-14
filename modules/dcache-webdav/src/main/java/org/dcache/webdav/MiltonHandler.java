package org.dcache.webdav;

import com.google.common.collect.ImmutableList;
import dmg.cells.nucleus.CDC;
import dmg.cells.nucleus.CellAddressCore;
import dmg.cells.nucleus.CellIdentityAware;
import io.milton.http.Auth;
import io.milton.http.FileItem;
import io.milton.http.HttpManager;
import io.milton.http.RequestParseException;
import io.milton.servlet.ServletRequest;
import io.milton.servlet.ServletResponse;
import java.io.IOException;
import java.io.InputStream;
import java.security.AccessController;
import java.util.Map;
import javax.security.auth.Subject;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.commons.fileupload.FileUploadException;
import org.dcache.auth.Subjects;
import org.dcache.util.Transfer;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.eclipse.jetty.server.handler.ContextHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Jetty handler that wraps a Milton HttpManager. Makes it possible to embed Milton in Jetty
 * without using the Milton servlet.
 */
public class MiltonHandler
      extends AbstractHandler
      implements CellIdentityAware {

    private static final Logger LOGGER = LoggerFactory.getLogger(MiltonHandler.class);
    private static final ImmutableList<String> ALLOWED_ORIGIN_PROTOCOL = ImmutableList.of("http",
          "https");

    private HttpManager _httpManager;
    private CellAddressCore _myAddress;

    public void setHttpManager(HttpManager httpManager) {
        _httpManager = httpManager;
    }

    @Override
    public void setCellAddress(CellAddressCore address) {
        _myAddress = address;
    }

    @Override
    public void handle(String target, Request baseRequest,
          HttpServletRequest request, HttpServletResponse response)
          throws IOException, ServletException {
        try (CDC ignored = CDC.reset(_myAddress)) {
            Transfer.initSession(false, false);
            ServletContext context = ContextHandler.getCurrentContext();

            if ("USERINFO".equals(request.getMethod())) {
                response.sendError(501);
            } else {
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
    class DcacheServletRequest extends ServletRequest {

        private final HttpServletRequest request;

        public DcacheServletRequest(HttpServletRequest request,
              ServletContext context) {
            super(request, context);
            this.request = request;
        }

        @Override
        public void parseRequestParameters(Map<String, String> params, Map<String, FileItem> files)
              throws RequestParseException {
            /*
             * io.milton.http.ResourceHandlerHelper#process calls
             * Request#parseRequestParameters and catches any
             * RequestParseException thrown.  Unfortunately, it logs this
             * with a stack-trace, but otherwise ignores such failures.
             *
             * See  https://github.com/miltonio/milton2/issues/93 for details.
             *
             * As a work-around, such exceptions are caught here and
             * converted into an unchecked exception that results in
             * the server responding with a 400 Bad Request.
             */
            try {
                super.parseRequestParameters(params, files);
            } catch (RequestParseException e) {
                // Inexplicably, Milton wraps any FileUploadException with a
                // RequestParseException containing a meaningless message.
                String message = e.getCause() instanceof FileUploadException
                      ? e.getCause().getMessage()
                      : e.getMessage();
                throw new UncheckedBadRequestException(message, e, null);
            }
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
