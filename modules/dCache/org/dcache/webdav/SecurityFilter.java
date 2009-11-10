package org.dcache.webdav;

import com.bradmcevoy.http.Filter;
import com.bradmcevoy.http.FilterChain;
import com.bradmcevoy.http.Request;
import com.bradmcevoy.http.Response;
import com.bradmcevoy.http.ServletRequest;

import java.net.UnknownHostException;
import java.net.InetAddress;

import javax.security.auth.Subject;
import java.security.PrivilegedAction;

import org.dcache.acl.Origin;
import org.dcache.acl.enums.AuthType;

import org.apache.log4j.Logger;

/**
 * SecurityFilter for WebDAV door.
 *
 * This filter must be injected into the Milton HttpManager. Milton
 * provides its own SecurityManager infrastructure, however it has
 * several shortcomings:
 *
 * - It relies on access to a Resource object, meaning authentication
 *   is performed after we resolve a path to a DcacheResource. Thus
 *   lookup permissions cannot be handled.
 *
 * - Milton performs authentication after it checks whether a request
 *   should be redirected (I assume the idea is that the target of
 *   the redirect performs authorisation). For dCache we want to
 *   authorise the request before redirecting to a pool.
 *
 * For these reasons we have our own SecurityFilter for performing
 * authentication. The rest of the request processing is performed
 * with the priviledges of the authenticated user using JAAS.
 */
public class SecurityFilter implements Filter
{
    private final Logger _log = Logger.getLogger(SecurityFilter.class);

    private String _realm;
    private boolean _isReadOnly;
    private boolean _allowAnonymous;

    @Override
    public void process(final FilterChain chain,
                        final Request request,
                        final Response response)
    {
        /* We don't support authentication right now, so anonymous
         * access is the only thing we can provide.
         */
        if (!_allowAnonymous) {
            response.setStatus(Response.Status.SC_UNAUTHORIZED);
            response.setAuthenticateHeader(getRealm());
            return;
        }

        /* Only a subset of the HTTP methods are allowed in read-only
         * mode.
         */
        if (_isReadOnly) {
            switch (request.getMethod()) {
            case GET:
            case HEAD:
            case OPTIONS:
            case PROPFIND:
                break;
            default:
                response.setStatus(Response.Status.SC_METHOD_NOT_ALLOWED);
                return;
            }
        }

        /* Perform authentication. Currently only the Origin is used.
         */
        Subject subject = new Subject();
        String address = ServletRequest.getRequest().getRemoteAddr();
        try {
            Origin origin =
                new Origin(AuthType.ORIGIN_AUTHTYPE_STRONG,
                           InetAddress.getByName(address));
            subject.getPrincipals().add(origin);
        } catch (UnknownHostException e) {
            _log.warn("Failed to resolve " + address + ": " + e.getMessage());
        }

        /* Process the request as the authenticated user.
         */
        Subject.doAs(subject, new PrivilegedAction<Void>() {
                @Override
                public Void run()
                {
                    chain.process(request, response);
                    return null;
                }
            });
    }

    public String getRealm()
    {
        return _realm;
    }

    /**
     * Sets the HTTP realm used for basic authentication.
     */
    public void setRealm(String realm)
    {
        _realm = realm;
    }

    public boolean isReadOnly()
    {
        return _isReadOnly;
    }

    /**
     * Specifies whether the door is read only.
     */
    public void setReadOnly(boolean isReadOnly)
    {
        _isReadOnly = isReadOnly;
    }

    public boolean getAllowAnonymous()
    {
        return _allowAnonymous;
    }

    /**
     * Specifies whether anonymous requests are allowed. If set to
     * true, an anonymous user can access to world accessible files
     * and directories.
     */
    public void setAllowAnonymous(boolean anonymous)
    {
        _allowAnonymous = anonymous;
    }
}