package org.dcache.webdav;

import io.milton.http.Filter;
import io.milton.http.FilterChain;
import io.milton.http.Handler;
import io.milton.http.HttpManager;
import io.milton.http.Request;
import io.milton.http.Response;
import io.milton.http.exceptions.BadRequestException;
import io.milton.http.exceptions.ConflictException;
import io.milton.http.exceptions.NotAuthorizedException;
import io.milton.http.exceptions.NotFoundException;
import io.milton.http.http11.Http11ResponseHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private static final Logger log =
        LoggerFactory.getLogger(DcacheStandardFilter.class);

    @Override
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
                if (response.getEntity() != null) {
                    manager.sendResponseEntity(response);
                }
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
        } catch (RedirectException e) {
            /* Milton's response handler does not support selecting the type of
             * redirect to use, so we need this workaround to issue a 307 reply.
             *
             * See http://jira.ettrema.com:8080/browse/MIL-120
             */
            response.setStatus(Response.Status.SC_TEMPORARY_REDIRECT);
            response.setLocationHeader(e.getUrl());
        } catch (WebDavException e) {
            log.warn("Internal server error: {}", e.toString());
            responseHandler.respondServerError(request, response, e.getMessage());
        } catch (RuntimeException e) {
            log.error("Internal server error", e);
            responseHandler.respondServerError(request, response, e.getMessage());
        } catch (Exception e) {
            log.error("Internal server error: {}", e.toString());
            responseHandler.respondServerError(request, response, e.getMessage());
        } finally {
            manager.closeResponse(response);
        }
    }
}
