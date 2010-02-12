package org.dcache.webdav;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bradmcevoy.http.Filter;
import com.bradmcevoy.http.FilterChain;
import com.bradmcevoy.http.Request;
import com.bradmcevoy.http.Response;
import com.bradmcevoy.http.Handler;
import com.bradmcevoy.http.HttpManager;
import com.bradmcevoy.http.exceptions.ConflictException;
import com.bradmcevoy.http.exceptions.NotAuthorizedException;

/**
 * Custom StandardFilter for Milton.
 *
 * As we do some things differently in dCache than expected by the
 * Milton WebDAV framework, we cannot always perform error handling
 * the Milton way. We therefore have a hierarchy of WebDAV
 * RuntimeExceptions which are caught by DcacheStandardFilter and
 * translated to HTTP response codes.
 *
 * Essentially the same as com.bradmcevoy.http.StandardFilter.
 */
public class DcacheStandardFilter implements Filter
{
    private final static Logger log =
        LoggerFactory.getLogger(DcacheStandardFilter.class);

    public static final String METHOD_NOT_IMPLEMENTED_HTML =
        "<html><body><h1>Method Not Implemented</h1></body></html>";
    public final static String INTERNAL_SERVER_ERROR_HTML =
        "<html><body><h1>Internal Server Error (500)</h1></body></html>";
    public final static String FORBIDDEN_HTML =
        "<html><body><h1>Permission denied (403)</h1></body></html>";

    private final Map<Request.Method, Handler> _handlers =
        new ConcurrentHashMap<Request.Method, Handler>();

    private HttpManager _manager;

    public void setHttpManager(HttpManager manager)
    {
        _manager = manager;
        _handlers.put(Request.Method.HEAD, manager.getHeadHandler());
        _handlers.put(Request.Method.PROPFIND, manager.getPropFindHandler());
        _handlers.put(Request.Method.PROPPATCH, manager.getPropPatchHandler());
        _handlers.put(Request.Method.MKCOL, manager.getMkColHandler());
        _handlers.put(Request.Method.COPY, manager.getCopyHandler());
        _handlers.put(Request.Method.MOVE, manager.getMoveHandler());
        _handlers.put(Request.Method.LOCK, manager.getLockHandler());
        _handlers.put(Request.Method.UNLOCK, manager.getUnlockHandler());
        _handlers.put(Request.Method.DELETE, manager.getDeleteHandler());
        _handlers.put(Request.Method.GET, manager.getGetHandler());
        _handlers.put(Request.Method.OPTIONS, manager.getOptionsHandler());
        _handlers.put(Request.Method.POST, manager.getPostHandler());
        _handlers.put(Request.Method.PUT, manager.getPutHandler());
    }

    private void errorResponse(Response response,
                               Response.Status status, String error)
    {
        try {
            response.setStatus(status);
            response.getOutputStream().write(error.getBytes());
        } catch (IOException ex) {
            log.warn("exception writing content");
        }
    }

    public void process(FilterChain chain, Request request, Response response)
    {
        if (_manager != chain.getHttpManager()) {
            throw new IllegalArgumentException("FilterChain belongs to wrong HttpManager");
        }

        try {
            Request.Method method = request.getMethod();
            Handler handler = _handlers.get(method);
            if (handler == null) {
                errorResponse(response, Response.Status.SC_NOT_IMPLEMENTED,
                              METHOD_NOT_IMPLEMENTED_HTML);
                return;
            }

            handler.process(_manager, request, response);
        } catch (ConflictException ex) {
            _manager.getResponseHandler().respondConflict(ex.getResource(),
                                                          response, request,
                                                          INTERNAL_SERVER_ERROR_HTML);
        } catch (NotAuthorizedException ex) {
            _manager.getResponseHandler().respondUnauthorised(ex.getResource(),
                                                              response, request);
        } catch (ForbiddenException e) {
            errorResponse(response,
                          Response.Status.SC_FORBIDDEN, FORBIDDEN_HTML);
        } catch (WebDavException e) {
            log.warn("Internal server error: " + e);
            errorResponse(response,
                          Response.Status.SC_INTERNAL_SERVER_ERROR,
                          INTERNAL_SERVER_ERROR_HTML);
        } catch (RuntimeException e) {
            log.error("Internal server error", e);
            errorResponse(response,
                          Response.Status.SC_INTERNAL_SERVER_ERROR,
                          INTERNAL_SERVER_ERROR_HTML);
        } finally {
            response.close();
        }
    }
}
