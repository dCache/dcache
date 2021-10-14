package org.dcache.webdav.federation;

import io.milton.http.AbstractWrappingResponseHandler;
import io.milton.http.Request;
import io.milton.http.Response;
import io.milton.http.webdav.WebDavResponseHandler;
import io.milton.resource.Resource;

/**
 * This class implements support for the "Global Access Service" feature that allows seamless access
 * to a global federation of storage.  This is achieved by the storage system responding to certain
 * local failures by redirecting the client to the next replica from a supplied stack of replicas.
 * This process is described here:
 * <p>
 * https://svnweb.cern.ch/trac/lcgdm/wiki/Dpm/WebDAV/Extensions
 * <p>
 * The client supplies a carefully crafted URL with additional metadata encoded as a query string.
 * The format for this query string is described in the ReplicaInfo class.  If dCache would return a
 * NOT_FOUND or FORBIDDEN response, this is replaced by a MOVED_TEMPORARILY response with the next
 * replica in the stack as the location.  A list of the failures that the client has already
 * experienced are propagated as part of the next request.
 * <p>
 * Normally, the final replica in the stack of supplied replicas is the catalogue itself.  This is
 * to allow the catalogue to update its internal cache with the failed responses and suggest
 * additional replicas to the client, if any.
 * <p>
 * Because of this, we do not anticipate receiving a federated request that fails with no next
 * replica to redirect; this scenario is not described in the above wiki page.  Under these
 * circumstances, dCache will allow the FORBIDDEN or NOT_FOUND error response to propagate back to
 * the client.
 * <p>
 * If the client supplies no query string, or the query string is malformed then dCache will treat
 * the request as a normal request and all errors will be propagated back to the client.
 */
public class FederationResponseHandler extends AbstractWrappingResponseHandler {

    public FederationResponseHandler(WebDavResponseHandler wrapped) {
        super(wrapped);
    }

    @Override
    public void respondNotFound(Response response, Request request) {
        ReplicaInfo info = ReplicaInfo.forRequest(request);

        if (info.hasNext()) {
            super.respondRedirect(response, request, info.buildLocationWhenNotFound());
        } else {
            super.respondNotFound(response, request);
        }
    }

    @Override
    public void respondForbidden(Resource resource, Response response, Request request) {
        ReplicaInfo info = ReplicaInfo.forRequest(request);

        if (info.hasNext()) {
            super.respondRedirect(response, request, info.buildLocationWhenForbidden());
        } else {
            super.respondForbidden(resource, response, request);
        }
    }
}
