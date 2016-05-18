package org.dcache.webdav;

import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.net.InetAddresses;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.FsPath;
import diskCacheV111.util.PermissionDeniedCacheException;

import org.dcache.auth.LoginReply;
import org.dcache.auth.LoginStrategy;
import org.dcache.auth.Origin;
import org.dcache.auth.Subjects;
import org.dcache.auth.LoginNamePrincipal;
import org.dcache.auth.PasswordCredential;
import org.dcache.auth.attributes.HomeDirectory;
import org.dcache.auth.attributes.LoginAttribute;
import org.dcache.auth.attributes.RootDirectory;
import org.dcache.util.CertificateFactories;
import org.dcache.util.NetLoggerBuilder;

import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.HandlerWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;

import javax.security.auth.Subject;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.security.PrivilegedAction;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.Base64;
import java.util.List;

import org.dcache.auth.attributes.Restriction;
import org.dcache.auth.attributes.Restrictions;

import static com.google.common.base.Strings.nullToEmpty;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Lists.reverse;
import static com.google.common.base.Preconditions.checkNotNull;

import static java.util.Arrays.asList;

public class AuthenticationHandler extends HandlerWrapper {

    private final static Logger LOG = LoggerFactory.getLogger(AuthenticationHandler.class);
    public static final String X509_CERTIFICATE_ATTRIBUTE =
            "javax.servlet.request.X509Certificate";
    public static final String DCACHE_SUBJECT_ATTRIBUTE =
            "org.dcache.subject";
    public static final String DCACHE_RESTRICTION_ATTRIBUTE =
            "org.dcache.restriction";

    private static final InetAddress UNKNOWN_ADDRESS = InetAddresses.forString("0.0.0.0");

    private String _realm;
    private Restriction _doorRestriction;
    private boolean _isBasicAuthenticationEnabled;
    private LoginStrategy _loginStrategy;
    private PathMapper _pathMapper;

    private CertificateFactory _cf = CertificateFactories.newX509CertificateFactory();
    private FsPath _uploadPath;


    @Override
    public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response)
            throws IOException, ServletException {
        if (isStarted() && !baseRequest.isHandled()) {
            Subject subject = new Subject();

            try {
                addX509ChainToSubject(request, subject);
                addOriginToSubject(request, subject);
                addPasswordCredentialToSubject(request, subject);

                LoginReply login = _loginStrategy.login(subject);
                subject = login.getSubject();
                Restriction restriction = Restrictions.concat(_doorRestriction, login.getRestriction());

                request.setAttribute(DCACHE_SUBJECT_ATTRIBUTE, subject);
                request.setAttribute(DCACHE_RESTRICTION_ATTRIBUTE, restriction);

                checkRootPath(request, login);
                /* Process the request as the authenticated user.*/
                Exception problem = Subject.doAs(subject, (PrivilegedAction<Exception>) () -> {
                    try {
                        AuthenticationHandler.super.handle(target, baseRequest, request, response);
                    } catch (IOException | ServletException e) {
                        return e;
                    }
                    return null;
                });
                if (problem != null) {
                    Throwables.propagateIfInstanceOf(problem, IOException.class);
                    Throwables.propagateIfInstanceOf(problem, ServletException.class);
                    throw Throwables.propagate(problem);
                }
            } catch (RedirectException e) {
                LOG.warn("{} for path {} to url:{}", e.getMessage(), request.getPathInfo(), e.getUrl());
                response.sendRedirect(e.getUrl());
            } catch (PermissionDeniedCacheException e) {
                LOG.warn("{} for path {} and user {}", e.getMessage(), request.getPathInfo(),
                        NetLoggerBuilder.describeSubject(subject));
                if (Subjects.isNobody(subject)) {
                    response.setHeader("WWW-Authenticate", "Basic realm=\"" + getRealm() + "\"");
                    response.sendError(HttpServletResponse.SC_UNAUTHORIZED);
                } else {
                    response.sendError(HttpServletResponse.SC_FORBIDDEN);
                }
            } catch (CacheException e) {
                LOG.error("Internal server error: " + e);
                response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            }
        }
    }

    /**
     * Verifies that the request is within the root directory of the given user.
     *
     * @throws RedirectException If the request should be redirected
     * @throws PermissionDeniedCacheException If the request is denied
     * @throws CacheException If the request fails
     */
    private void checkRootPath(HttpServletRequest request, LoginReply login)
            throws CacheException, RedirectException {
        FsPath userRoot = new FsPath();
        FsPath userHome = new FsPath();
        for (LoginAttribute attribute: login.getLoginAttributes()) {
            if (attribute instanceof RootDirectory) {
                userRoot = new FsPath(((RootDirectory) attribute).getRoot());
            } else if (attribute instanceof HomeDirectory) {
                userHome = new FsPath(((HomeDirectory) attribute).getHome());
            }
        }

        String path = request.getPathInfo();
        FsPath fullPath = _pathMapper.asDcachePath(request, path);
        if (!fullPath.startsWith(userRoot) &&
                (_uploadPath == null || !fullPath.startsWith(_uploadPath))) {
            if (!path.equals("/")) {
                throw new PermissionDeniedCacheException("Permission denied: " +
                        "path outside user's root");
            }

            try {
                FsPath redirectFullPath = new FsPath(userRoot, userHome);
                String redirectPath = _pathMapper.asRequestPath(request, redirectFullPath);
                URI uri = new URI(request.getRequestURL().toString());
                URI redirect = new URI(uri.getScheme(), uri.getAuthority(), redirectPath, null, null);
                throw new RedirectException(null, redirect.toString());
            } catch (URISyntaxException e) {
                throw new CacheException(e.getMessage(), e);
            }
        }
    }

    private void addX509ChainToSubject(HttpServletRequest request, Subject subject)
            throws CacheException {
        Object object = request.getAttribute(X509_CERTIFICATE_ATTRIBUTE);
        if (object instanceof X509Certificate[]) {
            try {
                subject.getPublicCredentials().add(_cf.generateCertPath(asList((X509Certificate[]) object)));
            } catch (CertificateException e) {
                throw new CacheException("Failed to generate X.509 certificate path: " + e.getMessage(), e);
            }
        }
    }

    private void addXForwardForAddresses(ImmutableList.Builder<InetAddress> addresses,
            HttpServletRequest request)
    {
        String xff = nullToEmpty(request.getHeader("X-Forwarded-For"));
        List<String> ids = newArrayList(Splitter.on(',').trimResults().omitEmptyStrings().split(xff));
        reverse(ids).stream().
                map(id -> {
                        try {
                            return InetAddresses.forString(id);
                        } catch (IllegalArgumentException e) {
                            LOG.warn("Fail to parse \"{}\" in X-Forwarded-For " +
                                    "header \"{}\": {}", id, xff, e.getMessage());
                            return UNKNOWN_ADDRESS;
                        }
                    }).
                forEach(addresses::add);
    }

    private void addOriginToSubject(HttpServletRequest request, Subject subject)
    {
        ImmutableList.Builder<InetAddress> addresses = ImmutableList.builder();

        String address = request.getRemoteAddr();
        try {
            addresses.add(InetAddress.getByName(address));
        } catch (UnknownHostException e) {
            LOG.warn("Failed to resolve " + address + ": " + e.getMessage());
            return;
        }

        // REVISIT: although RFC 7239 specifies a more powerful format, it
        // is currently not widely used; whereas X-Forward-For header, while not
        // standardised is the de facto standard and widely supported.
        addXForwardForAddresses(addresses, request);

        subject.getPrincipals().add(new Origin(addresses.build()));
    }


    private void addPasswordCredentialToSubject(HttpServletRequest request, Subject subject) {
        if (!_isBasicAuthenticationEnabled) {
            return;
        }

        String header = request.getHeader("Authorization");
        if (header == null) {
            LOG.trace("No credentials found in Authorization header");
            return;
        }

        if (header.length() == 0) {
            LOG.trace("Credentials in Authorization header are not-null, but are empty");
            return;
        }

        int space = header.indexOf(" ");
        String authScheme = space >= 0 ? header.substring(0, space).toUpperCase() : HttpServletRequest.BASIC_AUTH;
        String authData = space >= 0 ? header.substring(space + 1) : header;

        switch (authScheme) {
        case HttpServletRequest.BASIC_AUTH:
            try {
                byte[] bytes = Base64.getDecoder().decode(authData.getBytes(StandardCharsets.US_ASCII));
                String credential = new String(bytes, StandardCharsets.UTF_8);
                int colon = credential.indexOf(":");
                if (colon >= 0) {
                    String user = credential.substring(0, colon);
                    String password = credential.substring(colon + 1);
                    subject.getPrivateCredentials().add(new PasswordCredential(user, password));
                } else {
                    subject.getPrincipals().add(new LoginNamePrincipal(credential));
                }
            } catch (IllegalArgumentException e) {
                LOG.warn("Authentication Data in the header received is not Base64 encoded {}", header);
            }
            break;
        default:
            LOG.trace("Unknown authentication scheme {}", authScheme);
        }
    }

    public String getRealm() {
        return _realm;
    }

    /**
     * Sets the HTTP realm used for basic authentication.
     */
    public void setRealm(String realm) {
        _realm = realm;
    }

    /**
     * Specifies whether the door is read only.
     */
    public void setReadOnly(boolean isReadOnly) {
        _doorRestriction = isReadOnly ? Restrictions.readOnly() : Restrictions.none();
    }

    public void setEnableBasicAuthentication(boolean isEnabled) {
        _isBasicAuthenticationEnabled = isEnabled;
    }

    public void setLoginStrategy(LoginStrategy loginStrategy) {
        _loginStrategy = loginStrategy;
    }

    @Required
    public void setPathMapper(PathMapper mapper) {
        _pathMapper = checkNotNull(mapper);
    }

    public void setUploadPath(File uploadPath) {
        this._uploadPath = uploadPath.isAbsolute() ? new FsPath(uploadPath.getPath()) : null;
    }

    public File getUploadPath() {
        return (_uploadPath == null) ? null : new File(_uploadPath.toString());
    }
}
