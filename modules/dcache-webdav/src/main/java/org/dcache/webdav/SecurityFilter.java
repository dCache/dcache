package org.dcache.webdav;

import io.milton.http.Filter;
import io.milton.http.FilterChain;
import io.milton.http.Request;
import io.milton.http.Response;
import io.milton.http.HttpManager;
import io.milton.http.Auth;
import io.milton.servlet.ServletRequest;

import javax.servlet.http.HttpServletRequest;
import javax.security.auth.Subject;
import java.security.cert.X509Certificate;
import java.security.PrivilegedAction;
import java.net.UnknownHostException;
import java.net.InetAddress;

import org.dcache.auth.LoginStrategy;
import org.dcache.auth.LoginReply;
import org.dcache.auth.PasswordCredential;
import org.dcache.auth.attributes.LoginAttribute;
import org.dcache.auth.attributes.ReadOnly;
import org.dcache.auth.attributes.HomeDirectory;
import org.dcache.auth.attributes.RootDirectory;
import org.dcache.auth.Origin;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.FsPath;
import diskCacheV111.util.PermissionDeniedCacheException;

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
    public static final String USER_ROOT_DIRECTORY="org.dcache.user.root";
    public static final String USER_HOME_DIRECTORY="org.dcache.user.home";

    private String _realm;
    private boolean _isReadOnly;
    private boolean _isBasicAuthenticationEnabled;
    private LoginStrategy _loginStrategy;

    @Override
    public void process(final FilterChain filterChain,
                        final Request request,
                        final Response response)
    {
        HttpManager manager = filterChain.getHttpManager();
        Subject subject = new Subject();

        if (!isAllowedMethod(request.getMethod())) {
            manager.getResponseHandler().respondMethodNotAllowed(new EmptyResource(request), response, request);
            return;
        }

        try {
            HttpServletRequest servletRequest = ServletRequest.getRequest();

            addX509ChainToSubject(servletRequest, subject);
            addOriginToSubject(servletRequest, subject);
            addPasswordCredentialToSubject(request, subject);

            LoginReply login = _loginStrategy.login(subject);
            subject = login.getSubject();

            if (!isAuthorizedMethod(request.getMethod(), login)) {
                throw new PermissionDeniedCacheException("Permission denied");
            }
            /*
             * Add user root and home directory to request
             */
            for (LoginAttribute attribute: login.getLoginAttributes()) {
                if (attribute instanceof RootDirectory) {
                    request.getAttributes().put(USER_ROOT_DIRECTORY,
                                                new FsPath(((RootDirectory) attribute).getRoot()));
                } else if (attribute instanceof HomeDirectory) {
                    request.getAttributes().put(USER_HOME_DIRECTORY,
                                                new FsPath(((HomeDirectory) attribute).getHome()));
                }
            }
            /* Add the origin of the request to the subject. This
             * ought to be processed in the LoginStrategy, but our
             * LoginStrategies currently do not process the Origin.
             */
            addOriginToSubject(servletRequest, subject);

            /* Although we don't rely on the authorization tag
             * ourselves, Milton uses it to detect that the request
             * was preauthenticated.
             */
            Auth auth = request.getAuthorization();
            if (auth != null) {
                auth.setTag(subject);
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
        } catch (PermissionDeniedCacheException e) {
            _log.warn("Login failed for " + subject);
            manager.getResponseHandler().respondUnauthorised(new EmptyResource(request), response, request);
        } catch (CacheException e) {
            _log.error("Internal server error: " + e);
            manager.getResponseHandler().respondServerError(request, response, e.getMessage());
        }
    }

    private void addX509ChainToSubject(HttpServletRequest request, Subject subject)
    {
        Object object = request.getAttribute(X509_CERTIFICATE_ATTRIBUTE);
        if (object instanceof X509Certificate[]) {
            X509Certificate[] chain = (X509Certificate[]) object;
            subject.getPublicCredentials().add(chain);
        }
    }

    private void addOriginToSubject(HttpServletRequest request, Subject subject)
    {
        String address = request.getRemoteAddr();
        try {
            Origin origin =
                new Origin(Origin.AuthType.ORIGIN_AUTHTYPE_STRONG,
                           InetAddress.getByName(address));
            subject.getPrincipals().add(origin);
        } catch (UnknownHostException e) {
            _log.warn("Failed to resolve " + address + ": " + e.getMessage());
        }
    }

    private void addPasswordCredentialToSubject(Request request, Subject subject)
    {
        Auth auth = request.getAuthorization();
        if (auth != null && auth.getScheme().equals(Auth.Scheme.BASIC) &&
            _isBasicAuthenticationEnabled) {
            PasswordCredential credential =
                new PasswordCredential(auth.getUser(), auth.getPassword());
            subject.getPrivateCredentials().add(credential);
        }
    }

    private boolean isAllowedMethod(Request.Method method)
    {
        return !_isReadOnly || isReadMethod(method);
    }

    private boolean isAuthorizedMethod(Request.Method method, LoginReply login)
    {
        return !isUserReadOnly(login) || isReadMethod(method);
    }

    private boolean isUserReadOnly(LoginReply login)
    {
        for (LoginAttribute attribute: login.getLoginAttributes()) {
            if (attribute instanceof ReadOnly) {
                return ((ReadOnly) attribute).isReadOnly();
            }
        }
        return false;
    }

    private boolean isReadMethod(Request.Method method)
    {
        switch (method) {
        case GET:
        case HEAD:
        case OPTIONS:
        case PROPFIND:
            return true;
        default:
            return false;
        }
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

    public void setEnableBasicAuthentication(boolean isEnabled)
    {
        _isBasicAuthenticationEnabled = isEnabled;
    }

    public void setLoginStrategy(LoginStrategy loginStrategy)
    {
        _loginStrategy = loginStrategy;
    }
}
