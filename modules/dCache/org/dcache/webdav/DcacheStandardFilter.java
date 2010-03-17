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
import com.bradmcevoy.http.exceptions.BadRequestException;

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

    public final static String FORBIDDEN_HTML =
        "<html><body><h1>Permission denied (403)</h1></body></html>";

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
        HttpManager manager = chain.getHttpManager();

        try {
            Request.Method method = request.getMethod();
            Handler handler = manager.getMethodHandler(method);
            if (handler == null) {
                manager.getResponseHandler().respondMethodNotImplemented(new EmptyResource(request), response, request);
                return;
            }

            handler.process(manager,request,response);
        } catch (BadRequestException e) {
            manager.getResponseHandler().respondBadRequest(e.getResource(),
                                                           response, request);
        } catch (ConflictException e) {
            manager.getResponseHandler().respondConflict(e.getResource(),
                                                         response, request,
                                                         e.getMessage());
        } catch (NotAuthorizedException e) {
            manager.getResponseHandler().respondUnauthorised(e.getResource(),
                                                             response, request);
        } catch (ForbiddenException e) {
            errorResponse(response,
                          Response.Status.SC_FORBIDDEN, FORBIDDEN_HTML);
        } catch (WebDavException e) {
            log.warn("Internal server error: " + e);
            manager.getResponseHandler().respondServerError(request, response,
                                                            e.getMessage());
        } catch (RuntimeException e) {
            log.error("Internal server error", e);
            manager.getResponseHandler().respondServerError(request, response,
                                                            e.getMessage());
        } finally {
            response.close();
        }
    }
}
