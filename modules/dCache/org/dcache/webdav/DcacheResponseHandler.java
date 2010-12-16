package org.dcache.webdav;

import com.bradmcevoy.http.Resource;
import com.bradmcevoy.http.GetableResource;
import com.bradmcevoy.http.Response;
import com.bradmcevoy.http.Request;
import com.bradmcevoy.http.AuthenticationService;
import com.bradmcevoy.http.http11.DefaultHttp11ResponseHandler;
import com.bradmcevoy.http.webdav.DefaultWebDavResponseHandler;
import com.bradmcevoy.http.webdav.WebDavResourceTypeHelper;
import com.bradmcevoy.http.webdav.PropFindXmlGenerator;
import com.bradmcevoy.http.values.ValueWriters;

/**
 * Response handler that contains workarounds for bugs in Milton.
 */
public class DcacheResponseHandler extends DefaultWebDavResponseHandler
{
    public DcacheResponseHandler(AuthenticationService authenticationService)
    {
        super(new Http11ResponseHandler(authenticationService),
              new WebDavResourceTypeHelper(),
              new PropFindXmlGenerator(new ValueWriters()));
    }

    protected static class Http11ResponseHandler extends DefaultHttp11ResponseHandler
    {
        public Http11ResponseHandler(AuthenticationService authenticationService)
        {
            super(authenticationService);
        }

        @Override
        public void respondHead(Resource resource, Response response, Request request ) {
            setRespondContentCommonHeaders(response, resource, request.getAuthorization());

            if (resource instanceof GetableResource) {
                GetableResource gr = (GetableResource) resource;
                Long contentLength = gr.getContentLength();
                if (contentLength != null) {
                    response.setContentLengthHeader(contentLength);
                }
                String acc = request.getAcceptHeader();
                String ct = gr.getContentType(acc);
                if (ct != null) {
                    response.setContentTypeHeader(ct);
                }
            }
        }
    }
}