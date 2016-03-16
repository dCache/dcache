package org.dcache.webdav;

import io.milton.http.Auth;
import io.milton.http.HttpManager;
import io.milton.servlet.ServletRequest;
import io.milton.servlet.ServletResponse;

import org.dcache.auth.Subjects;

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
import java.security.AccessController;
import java.util.function.Supplier;

import dmg.cells.nucleus.CDC;
import dmg.cells.nucleus.CellEndpoint;
import dmg.cells.nucleus.CellInfo;
import dmg.cells.nucleus.CellInfoAware;

import dmg.cells.nucleus.CellMessageSender;

import org.dcache.util.Transfer;

/**
 * A Jetty handler that wraps a Milton HttpManager. Makes it possible
 * to embed Milton in Jetty without using the Milton servlet.
 */
public class MiltonHandler
    extends AbstractHandler
    implements CellMessageSender, CellInfoAware
{
    private HttpManager _httpManager;
    private String _cellName;
    private String _domainName;
    private boolean _isNameSiteUnique;

    public void setHttpManager(HttpManager httpManager)
    {
        _httpManager = httpManager;
    }

    @Override
    public void setCellEndpoint(CellEndpoint endpoint)
    {
        _isNameSiteUnique = endpoint.getArgs().getBooleanOption("export");
    }

    @Override
    public void setCellInfoSupplier(Supplier<CellInfo> supplier)
    {
        CellInfo info = supplier.get();
        _cellName = info.getCellName();
        _domainName = info.getDomainName();
    }

    @Override
    public void handle(String target, Request baseRequest,
                       HttpServletRequest request,HttpServletResponse response)
        throws IOException, ServletException
    {
        try (CDC ignored = CDC.reset(_cellName, _domainName)) {
            Transfer.initSession(_isNameSiteUnique, false);
            ServletContext context = ContextHandler.getCurrentContext();
            switch (request.getMethod()) {
            case "USERINFO":
                response.sendError(501, "Not implemented");
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
