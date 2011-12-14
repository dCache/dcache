package org.dcache.webdav;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bradmcevoy.http.Filter;
import com.bradmcevoy.http.FilterChain;
import com.bradmcevoy.http.Request;
import com.bradmcevoy.http.Response;
import com.bradmcevoy.http.Handler;
import com.bradmcevoy.http.HttpManager;
import com.bradmcevoy.http.http11.Http11ResponseHandler;
import com.bradmcevoy.http.exceptions.ConflictException;
import com.bradmcevoy.http.exceptions.NotAuthorizedException;
import com.bradmcevoy.http.exceptions.BadRequestException;
import com.bradmcevoy.http.exceptions.NotFoundException;

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

    public void process(FilterChain chain, Request request, Response response)
    {
        HttpManager manager = chain.getHttpManager();
        Http11ResponseHandler responseHandler = manager.getResponseHandler();

        try {
            Request.Method method = request.getMethod();
            Handler handler = manager.getMethodHandler(method);
            if (handler == null) {
                responseHandler.respondMethodNotImplemented(new EmptyResource(request), response, request);
                return;
            }

            try {
                handler.process(manager,request,response);
            } catch (RuntimeException e) {
                /* Milton wraps our WebDavException in a
                 * RuntimeException.
                 */
                if (e.getCause() instanceof WebDavException) {
                    throw (WebDavException) e.getCause();
                }
                /* Milton also wraps critical errors that should not
                 * be caught.
                 */
                if (e.getCause() instanceof Error) {
                    throw (Error) e.getCause();
                }
                throw e;
            }
        } catch (BadRequestException e) {
            responseHandler.respondBadRequest(e.getResource(), response, request);
        } catch (ConflictException e) {
            responseHandler.respondConflict(e.getResource(), response, request, e.getMessage());
        } catch (NotAuthorizedException e) {
            responseHandler.respondUnauthorised(e.getResource(), response, request);
        } catch (UnauthorizedException e) {
            responseHandler.respondUnauthorised(e.getResource(), response, request);
        } catch (ForbiddenException e) {
            responseHandler.respondForbidden(e.getResource(), response, request);
        } catch (NotFoundException e) {
            responseHandler.respondNotFound(response, request);
        } catch (WebDavException e) {
            log.warn("Internal server error: " + e);
            responseHandler.respondServerError(request, response, e.getMessage());
        } catch (RuntimeException e) {
            log.error("Internal server error", e);
            responseHandler.respondServerError(request, response, e.getMessage());
        } finally {
            response.close();
        }
    }
}
