package org.dcache.webdav;

import com.bradmcevoy.http.Filter;
import com.bradmcevoy.http.FilterChain;
import com.bradmcevoy.http.Request;
import com.bradmcevoy.http.Response;
import com.bradmcevoy.http.ServletRequest;
import com.bradmcevoy.http.HttpManager;

import javax.servlet.http.HttpServletRequest;
import javax.security.auth.Subject;
import java.security.cert.X509Certificate;
import java.security.PrivilegedAction;
import java.net.UnknownHostException;
import java.net.InetAddress;
import java.util.Arrays;

import org.dcache.cells.CellStub;
import org.dcache.auth.AuthzQueryHelper;
import org.dcache.auth.Subjects;
import org.dcache.auth.RecordConvert;

import gplazma.authz.AuthorizationController;
import gplazma.authz.AuthorizationException;
import org.dcache.auth.Origin;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private final Logger _log = LoggerFactory.getLogger(SecurityFilter.class);

    private static final String X509_CERTIFICATE_ATTRIBUTE =
        "javax.servlet.request.X509Certificate";

    /**
     * Defines which operations are allowed for anonymous access.
     */
    public enum AnonymousAccess
    {
        NONE, READONLY, FULL
    }

    private String _realm;
    private boolean _isReadOnly;
    private AnonymousAccess _anonymousAccess;
    private CellStub _gPlazmaStub;
    private String _gPlazmaPolicyFilePath;
    private boolean _useGplazmaCell;
    private boolean _useGplazmaModule;

    @Override
    public void process(final FilterChain filterChain,
                        final Request request,
                        final Response response)
    {
        HttpManager manager = filterChain.getHttpManager();

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
                manager.getResponseHandler().respondMethodNotAllowed(new EmptyResource(request), response, request);
                return;
            }
        }

        /* Perform authentication.
         */
        Subject subject = null;

        HttpServletRequest servletRequest = ServletRequest.getRequest();
        Object object =
            servletRequest.getAttribute(X509_CERTIFICATE_ATTRIBUTE);
        if (object instanceof X509Certificate[]) {
            X509Certificate[] chain = (X509Certificate[]) object;

            if (_useGplazmaCell) {
                try {
                    AuthzQueryHelper helper =
                        new AuthzQueryHelper(_gPlazmaStub);
                    subject =
                        Subjects.getSubject(RecordConvert.gPlazmaToAuthorizationRecord(helper.authorize(chain, null).getgPlazmaAuthzMap()));
                } catch (AuthorizationException e) {
                    _log.info("Failed to authorize through gPlazma cell: "
                              + e.getMessage());
                }
            }

            if (subject == null && _useGplazmaModule) {
                try {
                    AuthorizationController authCtrl
                        = new AuthorizationController(_gPlazmaPolicyFilePath);
                    subject =
                        Subjects.getSubject(RecordConvert.gPlazmaToAuthorizationRecord(authCtrl.authorize(chain, null, null, null)));
                } catch (AuthorizationException e) {
                    _log.info("Failed to authorize through gPlazma module: "
                              + e.getMessage());
                }
            }
        }

        /* If we don't have a subject by now, then we will try
         * anonymous access.
         */
        if (subject == null) {
            switch (request.getMethod()) {
            case GET:
            case HEAD:
            case OPTIONS:
            case PROPFIND:
                if (_anonymousAccess == AnonymousAccess.NONE) {
                    manager.getResponseHandler().respondUnauthorised(new EmptyResource(request), response, request);
                    return;
                }
                break;
            default:
                if (_anonymousAccess != AnonymousAccess.FULL) {
                    manager.getResponseHandler().respondUnauthorised(new EmptyResource(request), response, request);
                    return;
                }
                break;
            }

            subject = new Subject();
        }

        /* Add the origin of the request to the subject.
         */
        String address = servletRequest.getRemoteAddr();
        try {
            Origin origin =
                new Origin(Origin.AuthType.ORIGIN_AUTHTYPE_STRONG,
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
                    filterChain.process(request, response);
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

    public AnonymousAccess getAnonymousAccess()
    {
        return _anonymousAccess;
    }

    /**
     * Specifies which anonymous requests are allowed.
     */
    public void setAnonymousAccess(AnonymousAccess anonymousAccess)
    {
        _anonymousAccess = anonymousAccess;
    }

    public void setGplazmaStub(CellStub stub)
    {
        _gPlazmaStub = stub;
    }

    public void setGplazmaPolicyFilePath(String path)
    {
        _gPlazmaPolicyFilePath = path;
    }

    public String getGplazmaPolicyFilePath()
    {
        return _gPlazmaPolicyFilePath;
    }

    public void setUseGplazmaCell(boolean useCell)
    {
        _useGplazmaCell = useCell;
    }

    public boolean getUseGplazmaCell()
    {
        return _useGplazmaCell;
    }

    public void setUseGplazmaModule(boolean useModule)
    {
        _useGplazmaModule = useModule;
    }

    public boolean getUseGplazmaModule()
    {
        return _useGplazmaModule;
    }
}