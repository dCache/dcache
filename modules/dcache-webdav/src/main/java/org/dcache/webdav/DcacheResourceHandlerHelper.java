package org.dcache.webdav;

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
import org.dcache.webdav.transfer.CopyFilter;

/**
 * This class provides extended behaviour for Milton so it can support some experimental/new
 * protocol extensions, like 3rd-party transfers.
 */
public class DcacheResourceHandlerHelper extends ResourceHandlerHelper {

    public DcacheResourceHandlerHelper(HandlerHelper handlerHelper,
          UrlAdapter urlAdapter, Http11ResponseHandler responseHandler,
          AuthenticationService authenticationService) {
        super(handlerHelper, urlAdapter, responseHandler, authenticationService);
    }

    @Override
    public void process(HttpManager manager, Request request, Response response,
          ResourceHandler handler) throws NotAuthorizedException,
          ConflictException,
          BadRequestException {
        String url = getUrlAdapter().getUrl(request);
        String host = request.getHostHeader();
        if (CopyFilter.isRequestThirdPartyCopy(request)) {
            Resource resource = manager.getResourceFactory().getResource(host, url);
            /* Bypass check to see if file exists: our CopyFilter will handle the request */
            handler.processResource(manager, request, response, resource);
        } else {
            super.process(manager, request, response, handler);
        }
    }
}
