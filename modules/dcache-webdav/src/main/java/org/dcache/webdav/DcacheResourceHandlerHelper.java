package org.dcache.webdav;

import com.google.common.collect.ImmutableList;
import io.milton.http.AuthenticationService;
import io.milton.http.HandlerHelper;
import io.milton.http.HttpManager;
import io.milton.http.Request;
import io.milton.http.ResourceHandler;
import io.milton.http.ResourceHandlerHelper;
import io.milton.http.Response;
import io.milton.http.UrlAdapter;
import io.milton.http.exceptions.BadRequestException;
import io.milton.http.exceptions.ConflictException;
import io.milton.http.exceptions.NotAuthorizedException;
import io.milton.http.http11.Http11ResponseHandler;
import io.milton.resource.Resource;

import java.util.List;

import org.dcache.webdav.transfer.CopyFilter;

/**
 * This class provides extended behaviour for Milton so it can support
 * some experimental/new protocol extensions, like 3rd-party transfers.
 */
public class DcacheResourceHandlerHelper extends ResourceHandlerHelper
{
    // REVISIT can we generate a dummy DcacheFileResource and use that instead?
    private static final List<String> OPTIONS_ALLOWED_METHODS_FOR_MISSING_ENTITY
            = ImmutableList.of("GET", "HEAD", "PROPFIND", "OPTIONS", "DELETE",
                    "MOVE", "GET", "HEAD", "PROPPATCH");

    public DcacheResourceHandlerHelper(HandlerHelper handlerHelper,
            UrlAdapter urlAdapter, Http11ResponseHandler responseHandler,
            AuthenticationService authenticationService)
    {
        super(handlerHelper, urlAdapter, responseHandler, authenticationService);
    }

    @Override
    public void process(HttpManager manager, Request request, Response response,
                        ResourceHandler handler) throws NotAuthorizedException,
                                                        ConflictException,
                                                        BadRequestException
    {
        String url = getUrlAdapter().getUrl(request);
        String host = request.getHostHeader();
        if (CopyFilter.isRequestThirdPartyCopy(request)) {
            Resource resource = manager.getResourceFactory().getResource(host, url);
            /* Bypass check to see if file exists: our CopyFilter will handle the request */
            handler.processResource(manager, request, response, resource);
        } else if (request.getMethod() == Request.Method.OPTIONS) {
            Resource resource;
            try {
                resource = manager.getResourceFactory().getResource(host, url);
            } catch (WebDavException e) {
                // Treat all errors as if there was no such file.  We should
                // always provide a successful response to a CORS request.
                resource = null;
            }
            if (resource == null) {
                // Milton ResourceHandlerHelper returns 404 for OPTIONS request
                // targeting non-existing entity.  This breaks CORS uploads.
                getResponseHandler().respondWithOptions(resource, response,
                        request, OPTIONS_ALLOWED_METHODS_FOR_MISSING_ENTITY);
            } else {
                super.process(manager, request, response, handler);
            }
        } else {
            super.process(manager, request, response, handler);
        }
    }
}
