package org.dcache.webdav;

import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.net.InetAddresses;
import org.eclipse.jetty.http.HttpHeader;
import org.eclipse.jetty.security.UserAuthentication;
import org.eclipse.jetty.server.Authentication;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.UserIdentity;
import org.eclipse.jetty.server.handler.HandlerWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpServletResponseWrapper;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.security.PrivilegedAction;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.Base64;
import java.util.List;
import java.util.Optional;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.PermissionDeniedCacheException;

import org.dcache.auth.BearerTokenCredential;
import org.dcache.auth.LoginNamePrincipal;
import org.dcache.auth.LoginReply;
import org.dcache.auth.LoginStrategy;
import org.dcache.auth.Origin;
import org.dcache.auth.PasswordCredential;
import org.dcache.auth.Subjects;
import org.dcache.auth.attributes.Restriction;
import org.dcache.auth.attributes.Restrictions;
import org.dcache.util.CertificateFactories;
import org.dcache.util.NetLoggerBuilder;

import static com.google.common.base.Strings.nullToEmpty;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Lists.reverse;
import static java.util.Arrays.asList;

public class AuthenticationHandler extends HandlerWrapper {

    private final static Logger LOG = LoggerFactory.getLogger(AuthenticationHandler.class);
    public static final String X509_CERTIFICATE_ATTRIBUTE =
            "javax.servlet.request.X509Certificate";
    public static final String DCACHE_SUBJECT_ATTRIBUTE =
            "org.dcache.subject";
    public static final String DCACHE_RESTRICTION_ATTRIBUTE =
            "org.dcache.restriction";
    public static final String DCACHE_LOGIN_ATTRIBUTES =
            "org.dcache.login";

    private static final InetAddress UNKNOWN_ADDRESS = InetAddresses.forString("0.0.0.0");

    private String _realm;
    private Restriction _doorRestriction;
    private boolean _isBasicAuthenticationEnabled;
    private boolean _isSpnegoAuthenticationEnabled;
    private LoginStrategy _loginStrategy;

    private CertificateFactory _cf = CertificateFactories.newX509CertificateFactory();

    @Override
    public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse servletResponse)
            throws IOException, ServletException {
        if (isStarted() && !baseRequest.isHandled()) {
            Subject subject = new Subject();
            AuthHandlerResponse response = new AuthHandlerResponse(servletResponse);
            try {
                addX509ChainToSubject(request, subject);
                addOriginToSubject(request, subject);
                addAuthCredentialsToSubject(request, subject);
                addSpnegoCredentialsToSubject(baseRequest, request, subject);

                LoginReply login = _loginStrategy.login(subject);
                subject = login.getSubject();
                Restriction restriction = Restrictions.concat(_doorRestriction, login.getRestriction());

                request.setAttribute(DCACHE_SUBJECT_ATTRIBUTE, subject);
                request.setAttribute(DCACHE_RESTRICTION_ATTRIBUTE, restriction);
                request.setAttribute(DCACHE_LOGIN_ATTRIBUTES, login.getLoginAttributes());

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
            } catch (PermissionDeniedCacheException e) {
                LOG.warn("{} for path {} and user {}", e.getMessage(), request.getPathInfo(),
                        NetLoggerBuilder.describeSubject(subject));
                response.sendError((Subjects.isNobody(subject)) ? HttpServletResponse.SC_UNAUTHORIZED :
                        HttpServletResponse.SC_FORBIDDEN);
                baseRequest.setHandled(true);
            } catch (CacheException e) {
                LOG.error("Internal server error: {}", e);
                response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
                baseRequest.setHandled(true);
            }
        }
    }

    private void addSpnegoCredentialsToSubject(Request baseRequest,
                                               HttpServletRequest request,
                                               Subject subject)
    {
        if (_isSpnegoAuthenticationEnabled) {
            Authentication spnegoAuth = baseRequest.getAuthentication();
            if (spnegoAuth instanceof Authentication.Deferred) {
                Authentication spnegoUser = ((Authentication.Deferred) spnegoAuth).authenticate(request);
                if (spnegoUser instanceof UserAuthentication) {
                    UserIdentity identity = ((UserAuthentication) spnegoUser).getUserIdentity();
                    subject.getPrincipals().add(new KerberosPrincipal(identity.getUserPrincipal().getName()));
                }
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



    private void addAuthCredentialsToSubject(HttpServletRequest request, Subject subject) {
        if (!_isBasicAuthenticationEnabled) {
            return;
        }

        Optional<AuthInfo> optional = parseAuthenticationHeader(request);
        if (optional.isPresent()) {
            AuthInfo info = optional.get();
            switch (info.getScheme()) {
                case HttpServletRequest.BASIC_AUTH:
                    try {
                        byte[] bytes = Base64.getDecoder().decode(info.getData().getBytes(StandardCharsets.US_ASCII));
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
                        LOG.warn("Authentication Data in the header received is not Base64 encoded {}",
                                request.getHeader("Authorization"));
                    }
                    break;
                case "BEARER":
                    try {
                        subject.getPrivateCredentials().add(new BearerTokenCredential(info.getData()));
                    } catch (IllegalArgumentException e) {
                        LOG.info("Bearer Token in invalid {}",
                                request.getHeader("Authorization"));
                    }
                    break;
                default:
                    LOG.debug("Unknown authentication scheme {}", info.getScheme());
            }
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

    public void setEnableSpnegoAuthentication(boolean isEnabled) {
        _isSpnegoAuthenticationEnabled = isEnabled;
    }

    public void setLoginStrategy(LoginStrategy loginStrategy) {
        _loginStrategy = loginStrategy;
    }

    private class AuthHandlerResponse extends HttpServletResponseWrapper {

        public AuthHandlerResponse(HttpServletResponse response) {
            super(response);
        }

        @Override
        public void setStatus(int code) {
            addAuthenticationChallenges(code);
            super.setStatus(code);
        }

        @Override
        public void setStatus(int code, String message) {
            addAuthenticationChallenges(code);
            super.setStatus(code, message);
        }

        @Override
        public void sendError(int code) throws IOException {
            addAuthenticationChallenges(code);
            super.sendError(code);
        }

        @Override
        public void sendError(int code, String message) throws IOException {
            addAuthenticationChallenges(code);
            super.sendError(code, message);
        }

        private void addAuthenticationChallenges(int code) {
            if (code == HttpServletResponse.SC_UNAUTHORIZED) {
                if (_isSpnegoAuthenticationEnabled) {
                    // Firefox always defaults to the first available authentication mechanism
                    // Conversely, Chrome and Safari choose the strongest mechanism
                    setHeader(HttpHeader.WWW_AUTHENTICATE.asString(), HttpHeader.NEGOTIATE.asString());
                    addHeader(HttpHeader.WWW_AUTHENTICATE.asString(), "Basic realm=\"" + getRealm() + "\"");
                } else {
                    setHeader(HttpHeader.WWW_AUTHENTICATE.asString(), "Basic realm=\"" + getRealm() + "\"");
                }
            }
        }
    }


    private class AuthInfo {
        private final String _scheme;
        private final String _data;

        AuthInfo(String scheme, String data)
        {
            _scheme = scheme;
            _data = data;
        }

        public String getScheme()
        {
            return _scheme;
        }

        public String getData()
        {
            return _data;
        }
    }

    private Optional<AuthInfo> parseAuthenticationHeader(HttpServletRequest request) {
        String header = request.getHeader("Authorization");
        if (header == null) {
            LOG.debug("No credentials found in Authorization header");
            return Optional.empty();
        }

        if (header.length() == 0) {
            LOG.debug("Credentials in Authorization header are not-null, but are empty");
            return Optional.empty();
        }

        int space = header.indexOf(" ");
        String authScheme = space >= 0 ? header.substring(0, space).toUpperCase() : HttpServletRequest.BASIC_AUTH;
        String authData = space >= 0 ? header.substring(space + 1) : header;
        return Optional.of(new AuthInfo(authScheme, authData));
    }
}
