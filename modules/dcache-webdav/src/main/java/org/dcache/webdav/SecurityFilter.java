package org.dcache.webdav;

import io.milton.http.Auth;
import io.milton.http.Filter;
import io.milton.http.FilterChain;
import io.milton.http.HttpManager;
import io.milton.http.Request;
import io.milton.http.Response;
import io.milton.servlet.ServletRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;
import javax.servlet.http.HttpServletRequest;

import java.io.File;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.security.PrivilegedAction;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.FsPath;
import diskCacheV111.util.PermissionDeniedCacheException;

import org.dcache.auth.LoginReply;
import org.dcache.auth.LoginStrategy;
import org.dcache.auth.Origin;
import org.dcache.auth.PasswordCredential;
import org.dcache.auth.Subjects;
import org.dcache.auth.attributes.HomeDirectory;
import org.dcache.auth.attributes.LoginAttribute;
import org.dcache.auth.attributes.ReadOnly;
import org.dcache.auth.attributes.RootDirectory;
import org.dcache.util.CertificateFactories;
import org.dcache.util.NetLoggerBuilder;

import static java.util.Arrays.asList;

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
    public static final String X509_CERTIFICATE_ATTRIBUTE =
        "javax.servlet.request.X509Certificate";
    public static final String DCACHE_SUBJECT_ATTRIBUTE =
            "org.dcache.subject";

    private String _realm;
    private boolean _isReadOnly;
    private boolean _isBasicAuthenticationEnabled;
    private LoginStrategy _loginStrategy;
    private FsPath _rootPath = new FsPath();
    private CertificateFactory _cf;
    private FsPath _uploadPath;

    public SecurityFilter()
    {
        _cf = CertificateFactories.newX509CertificateFactory();
    }

    @Override
    public void process(final FilterChain filterChain,
                        final Request request,
                        final Response response)
    {
        HttpManager manager = filterChain.getHttpManager();
        Subject subject = new Subject();

        if (!isAllowedMethod(request.getMethod())) {
            _log.debug("Failing {} from {} as door is read-only",
                    request.getMethod(), request.getRemoteAddr());
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

            servletRequest.setAttribute(DCACHE_SUBJECT_ATTRIBUTE, subject);

            if (!isAuthorizedMethod(request.getMethod(), login)) {
                throw new PermissionDeniedCacheException("Permission denied: " +
                        "read-only user");
            }

            checkRootPath(request, login);

            /* Add the origin of the request to the subject. This
             * ought to be processed in the LoginStrategy, but our
             * LoginStrategies currently do not process the Origin.
             */
            addOriginToSubject(servletRequest, subject);

            /* Although we don't rely on the authorization tag
             * ourselves, Milton uses it to detect that the request
             * was preauthenticated.
             */
            request.setAuthorization(new Auth(Subjects.getUserName(subject), subject));

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
        } catch (RedirectException e) {
            manager.getResponseHandler().respondRedirect(response, request, e.getUrl());
        } catch (PermissionDeniedCacheException e) {
            _log.warn("{} for path {} and user {}", e.getMessage(), request.getAbsolutePath(),
                    NetLoggerBuilder.describeSubject(subject));
            manager.getResponseHandler().respondUnauthorised(new EmptyResource(request), response, request);
        } catch (CacheException e) {
            _log.error("Internal server error: " + e);
            manager.getResponseHandler().respondServerError(request, response, e.getMessage());
        }
    }

    /**
     * Verifies that the request is within the root directory of the given user.
     *
     * @throws RedirectException If the request should be redirected
     * @throws PermissionDeniedCacheException If the request is denied
     * @throws CacheException If the request fails
     */
    private void checkRootPath(Request request, LoginReply login) throws CacheException
    {
        FsPath userRoot = new FsPath();
        FsPath userHome = new FsPath();
        for (LoginAttribute attribute: login.getLoginAttributes()) {
            if (attribute instanceof RootDirectory) {
                userRoot = new FsPath(((RootDirectory) attribute).getRoot());
            } else if (attribute instanceof HomeDirectory) {
                userHome = new FsPath(((HomeDirectory) attribute).getHome());
            }
        }

        String path = request.getAbsolutePath();
        FsPath fullPath = new FsPath(_rootPath, new FsPath(path));
        if (!fullPath.startsWith(userRoot) &&
                (_uploadPath == null || !fullPath.startsWith(_uploadPath))) {
            if (!path.equals("/")) {
                throw new PermissionDeniedCacheException("Permission denied: " +
                        "path outside user's root");
            }

            try {
                FsPath redirectFullPath = new FsPath(userRoot, userHome);
                String redirectPath = _rootPath.relativize(redirectFullPath).toString();
                URI uri = new URI(request.getAbsoluteUrl());
                URI redirect = new URI(uri.getScheme(), uri.getAuthority(), redirectPath, null, null);
                throw new RedirectException(null, redirect.toString());
            } catch (URISyntaxException e) {
                throw new CacheException(e.getMessage(), e);
            }
        }
    }

    private void addX509ChainToSubject(HttpServletRequest request, Subject subject)
            throws CacheException
    {
        Object object = request.getAttribute(X509_CERTIFICATE_ATTRIBUTE);
        if (object instanceof X509Certificate[]) {
            try {
                subject.getPublicCredentials().add(_cf.generateCertPath(asList((X509Certificate[]) object)));
            } catch (CertificateException e) {
                throw new CacheException("Failed to generate X.509 certificate path: " + e.getMessage(), e);
            }
        }
    }

    private void addOriginToSubject(HttpServletRequest request, Subject subject)
    {
        String address = request.getRemoteAddr();
        try {
            subject.getPrincipals().add(new Origin(address));
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

    public void setRootPath(String path)
    {
        _rootPath = new FsPath(path);
    }

    public String getRootPath()
    {
        return _rootPath.toString();
    }

    public void setUploadPath(File uploadPath)
    {
        this._uploadPath = uploadPath.isAbsolute() ? new FsPath(uploadPath.getPath()) : null;
    }

    public File getUploadPath()
    {
        return (_uploadPath == null) ? null : new File(_uploadPath.toString());
    }
}
