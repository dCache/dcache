/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2014 - 2018 Deutsches Elektronen-Synchrotron
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.dcache.webdav.transfer;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.concurrent.TimeUnit.MINUTES;

import com.google.common.base.CharMatcher;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.net.InternetDomainName;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.FsPath;
import diskCacheV111.util.PermissionDeniedCacheException;
import io.milton.http.Filter;
import io.milton.http.FilterChain;
import io.milton.http.Request;
import io.milton.http.Response;
import io.milton.http.Response.Status;
import io.milton.http.exceptions.BadRequestException;
import io.milton.servlet.ServletRequest;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.AccessController;
import java.security.cert.X509Certificate;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import javax.annotation.PostConstruct;
import javax.security.auth.Subject;
import javax.servlet.http.HttpServletRequest;
import org.dcache.auth.BearerTokenCredential;
import org.dcache.auth.OidcSubjectPrincipal;
import org.dcache.auth.OpenIdClientSecret;
import org.dcache.auth.Subjects;
import org.dcache.auth.attributes.Restriction;
import org.dcache.http.AuthenticationHandler;
import org.dcache.http.PathMapper;
import org.dcache.webdav.transfer.RemoteTransferHandler.Direction;
import org.dcache.webdav.transfer.RemoteTransferHandler.TransferType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;

/**
 * The CopyFilter adds support for initiating third-party copies via WebDAV.  This makes use of a
 * protocol extension described here:
 * <p>
 * https://svnweb.cern.ch/trac/lcgdm/wiki/Dpm/WebDAV/Extensions#ThirdPartyCopies
 * <p>
 * In essence, the client makes a normal WebDAV COPY request, but uses a non-local destination URI
 * as the Destination request header.  The client receives periodic progress reports, similar to
 * those provided on an FTP control channel, followed by a final report of either success or
 * failure.
 * <p>
 * Authorisation is assumed to be via X.509 client certificates.  To achieve authorisation on the
 * remote server, dCache needs to have a valid credential with which to authenticate with the remote
 * server.  A client-supplied credential is needed, and fetched from an srm service.
 * <p>
 * If the srm credential stores holds no valid credential then the client is requested to delegate a
 * credential.  This is achieved by redirecting the client back to the door with a custom header
 * pointing to the delegation endpoint.  In the redirection, the request URI is modified by adding a
 * query part.  This is used to detect request loops when identity delegation is required but the
 * client fails to delegate.
 * <p>
 * Orchestrating the transfers is handled by an instance of the RemoteTransferHandler class.  After
 * a transfer has been accepted, the server responds with periodic progress markers.  This is
 * handled by the RemoteTransferHandler class.
 */
public class CopyFilter implements Filter {

    private static final Logger LOGGER =
          LoggerFactory.getLogger(CopyFilter.class);

    private static final String QUERY_KEY_ASKED_TO_DELEGATE = "asked-to-delegate";
    private static final String REQUEST_HEADER_CREDENTIAL = "Credential";
    private static final String REQUEST_HEADER_VERIFICATION = "RequireChecksumVerification";
    private static final String TPC_ERROR_ATTRIBUTE = "org.dcache.tpc-error";
    private static final String TPC_CREDENTIAL_ATTRIBUTE = "org.dcache.tpc-credential";
    private static final String TPC_REQUIRE_CHECKSUM_VERIFICATION_ATTRIBUTE = "org.dcache.tpc-require-checksum-verify";
    private static final String TPC_SOURCE_ATTRIBUTE = "org.dcache.tpc-source";
    private static final String TPC_DESTINATION_ATTRIBUTE = "org.dcache.tpc-destination";

    private ImmutableMap<String, String> _clientIds;
    private ImmutableMap<String, String> _clientSecrets;
    private ImmutableMap<String, OpenIdClientSecret> _oidcClientCredentials = ImmutableMap.of();
    private boolean _defaultVerification;

    /**
     * Describes where to fetch the delegated credential, if at all.
     */
    public enum CredentialSource {
        /* Get it from an SRM's GridSite credential store */
        GRIDSITE("gridsite"),

        /* OpenID Connect Bearer Token */
        OIDC("oidc"),

        /* Don't get a credential */
        NONE("none");

        private static final Map<String, CredentialSource> SOURCE_FOR_HEADER =
              new HashMap<>();

        private final String _headerValue;

        static {
            for (CredentialSource source : CredentialSource.values()) {
                SOURCE_FOR_HEADER.put(source.getHeaderValue(), source);
            }
        }

        public static CredentialSource forHeaderValue(String value) {
            LOGGER.debug("Source for Header {}", SOURCE_FOR_HEADER.get(value));
            return SOURCE_FOR_HEADER.get(value);
        }

        public static Iterable<String> headerValues() {
            return SOURCE_FOR_HEADER.keySet();
        }

        CredentialSource(String value) {
            _headerValue = value;
        }

        public String getHeaderValue() {
            return _headerValue;
        }
    }

    private CredentialServiceClient _credentialService;
    private PathMapper _pathMapper;
    private RemoteTransferHandler _remoteTransfers;

    /**
     * Provide a description of why the TPC failed, or null if the transfer has not (yet) failed.
     */
    public static String getTpcError(HttpServletRequest request) {
        return (String) request.getAttribute(TPC_ERROR_ATTRIBUTE);
    }

    /**
     * Provide the credential type used for the TPC.
     */
    public static String getTpcCredential(HttpServletRequest request) {
        return (String) request.getAttribute(TPC_CREDENTIAL_ATTRIBUTE);
    }

    /**
     * Whether checksum verification is required.
     */
    public static String getTpcRequireChecksumVerification(HttpServletRequest request) {
        return (String) request.getAttribute(TPC_REQUIRE_CHECKSUM_VERIFICATION_ATTRIBUTE);
    }

    public static URI getTpcSource(HttpServletRequest request) {
        return (URI) request.getAttribute(TPC_SOURCE_ATTRIBUTE);
    }

    public static URI getTpcDestination(HttpServletRequest request) {
        return (URI) request.getAttribute(TPC_DESTINATION_ATTRIBUTE);
    }

    @Required
    public void setPathMapper(PathMapper mapper) {
        _pathMapper = mapper;
    }

    @Required
    public void setCredentialServiceClient(CredentialServiceClient client) {
        _credentialService = client;
    }

    @Required
    public void setRemoteTransferHandler(RemoteTransferHandler handler) {
        _remoteTransfers = handler;
    }

    @Required
    public void setDefaultVerification(boolean verify) {
        _defaultVerification = verify;
    }

    public boolean isDefaultVerification() {
        return _defaultVerification;
    }

    public void setOidClientIds(ImmutableMap<String, String> clientIds) {
        checkArgument(clientIds.entrySet()
                    .stream()
                    .allMatch(e -> CharMatcher.ASCII.matchesAllOf(e.getValue())),
              "Client Ids must be ASCII Characters only");
        _clientIds = clientIds;
    }

    public void setOidClientSecrets(ImmutableMap<String, String> clientSecrets) {
        checkArgument(clientSecrets.entrySet()
                    .stream()
                    .allMatch(e -> CharMatcher.ASCII.matchesAllOf(e.getValue())),
              "Client Secrets must be ASCII Characters only");
        _clientSecrets = clientSecrets;
    }

    @PostConstruct
    public void validateOidcClientParameters() {
        if (_clientSecrets.isEmpty() && _clientIds.isEmpty()) {
            LOGGER.debug("Client Credentials not configured for OpenId Connect Token Exchange");
            return;
        }

        checkArgument(!_clientSecrets.isEmpty(),
              "Client Secret not configured for OpenId Token Exchange");

        checkArgument(!_clientIds.isEmpty(), "Client Id not configured for OpenId Token Exchange");

        checkArgument(_clientIds.keySet().equals(_clientSecrets.keySet()),
              "Client Ids and Client Secrets must have the same set of hosts");

        Map<Boolean, Set<String>> validatedProviders =
              _clientIds.keySet()
                    .stream()
                    .collect(Collectors.groupingBy(InternetDomainName::isValid,
                          Collectors.toSet()));

        checkArgument(!validatedProviders.containsKey(Boolean.FALSE),
              String.format("Client credentials for invalid hostnames provided: %s",
                    Joiner.on(", ")
                          .join(validatedProviders.getOrDefault(Boolean.FALSE, new HashSet<>()))));

        Set<String> hostnames = validatedProviders.get(Boolean.TRUE);

        ImmutableMap.Builder<String, OpenIdClientSecret> builder = ImmutableMap.builder();
        hostnames.stream()
              .forEach((host) -> builder.put(host,
                    new OpenIdClientSecret(_clientIds.get(host),
                          _clientSecrets.get(host)))
              );
        _oidcClientCredentials = builder.build();
    }

    @Override
    public void process(FilterChain chain, Request request, Response response) {
        try {
            if (isRequestThirdPartyCopy(request)) {
                processThirdPartyCopy(request, response);
            } else {
                chain.process(request, response);
            }
        } catch (ErrorResponseException e) {
            response.sendError(e.getStatus(), e.getMessage());
        } catch (BadRequestException e) {
            response.sendError(Status.SC_BAD_REQUEST, e.getMessage());
        } catch (InterruptedException ignored) {
            response.sendError(Response.Status.SC_SERVICE_UNAVAILABLE,
                  "dCache is shutting down");
        }
    }

    public static boolean isRequestThirdPartyCopy(Request request)
          throws BadRequestException {
        if (request.getMethod() != Request.Method.COPY) {
            return false;
        }

        URI uri = getRemoteLocation();

        // We treat any URI that has scheme and host parts as a
        // third-party transfer request.  This isn't guaranteed but
        // probably good enough for now.
        return uri.getScheme() != null && uri.getHost() != null;
    }

    private static Direction getDirection() throws BadRequestException {
        // Note that each invocation of Request#getHeaders creates a HashMap
        // and populates it with all headers.  Therefore, we use ServletRequest
        // which Milton only makes available via a ThreadLocal.

        // Note that HttpSerlvetRequest#getHeader is case insensitive, as
        // required by:
        //
        //     https://tools.ietf.org/html/rfc7230#section-3.2
        //
        HttpServletRequest servletRequest = ServletRequest.getRequest();

        String pullUrl = servletRequest.getHeader(Direction.PULL.getHeaderName());
        String pushUrl = servletRequest.getHeader(Direction.PUSH.getHeaderName());

        if (pullUrl == null && pushUrl == null) {
            throw new BadRequestException("COPY request is missing both " +
                  Direction.PUSH.getHeaderName() + " and " +
                  Direction.PULL.getHeaderName() + " request headers");
        }

        if (pullUrl != null && pushUrl != null) {
            throw new BadRequestException("COPY request contains both " +
                  Direction.PUSH.getHeaderName() + " and " +
                  Direction.PULL.getHeaderName() + " request headers");
        }

        return pushUrl != null ? Direction.PUSH : Direction.PULL;
    }

    private static URI getRemoteLocation() throws BadRequestException {
        Direction direction = getDirection();
        String remote = ServletRequest.getRequest().getHeader(direction.getHeaderName());

        try {
            return new URI(remote);
        } catch (URISyntaxException e) {
            throw new BadRequestException(direction.getHeaderName() +
                  " request header contains an invalid URI: " + e.getMessage());
        }
    }

    private static Optional<String> getWantDigest(HttpServletRequest request) {
        List<String> wantDigests = Collections.list(request.getHeaders("Want-Digest"));
        return wantDigests.isEmpty()
              ? Optional.empty()
              : Optional.of(wantDigests.stream()
                    .filter(s -> !s.isEmpty())
                    .collect(Collectors.joining(",")));

    }

    private void processThirdPartyCopy(Request request, Response response)
          throws BadRequestException, InterruptedException, ErrorResponseException {
        Direction direction = getDirection();
        URI remote = getRemoteLocation();

        setRemoteUrlAttribute(direction, remote);

        TransferType type = TransferType.fromScheme(remote.getScheme());
        if (type == null) {
            throw new ErrorResponseException(Status.SC_BAD_REQUEST,
                  "The " + direction.getHeaderName() + " request header URI " +
                        "contains unsupported scheme; supported schemes are " +
                        Joiner.on(", ").join(TransferType.validSchemes()));
        }

        if (remote.getPath() == null) {
            throw new ErrorResponseException(Status.SC_BAD_REQUEST,
                  direction.getHeaderName() + " header is missing a path");
        }

        FsPath path = _pathMapper.asDcachePath(ServletRequest.getRequest(),
              request.getAbsolutePath(), m -> new ErrorResponseException(Status.SC_FORBIDDEN, m));

        // Always check any client-supplied Overwrite header, to throw an error if the value is malformed.
        boolean overwriteAllowed = clientAllowsOverwrite();

        Optional<String> wantDigest = getWantDigest(ServletRequest.getRequest());

        CredentialSource source = getCredentialSource(request, type);
        Object credential = fetchCredential(source);
        if (source != CredentialSource.NONE && credential == null) {
            if (source == CredentialSource.GRIDSITE) {
                redirectWithDelegation(response);
            } else {
                LOGGER.error("Error performing OpenId Connect Token Exchange");
                response.sendError(Status.SC_INTERNAL_SERVER_ERROR,
                      "Error performing OpenId Connect Token Exchange");
            }
        } else {
            Optional<String> error = _remoteTransfers.acceptRequest(response,
                  request.getHeaders(), getSubject(), getRestriction(), path,
                  remote, credential, direction, isVerificationRequired(),
                  overwriteAllowed, wantDigest);
            error.ifPresent(e -> ServletRequest.getRequest().setAttribute(TPC_ERROR_ATTRIBUTE, e));
        }
    }

    private void setRemoteUrlAttribute(Direction direction, URI remote) {
        HttpServletRequest request = ServletRequest.getRequest();

        switch (direction) {
            case PULL:
                request.setAttribute(TPC_SOURCE_ATTRIBUTE, remote);
                break;
            case PUSH:
                request.setAttribute(TPC_DESTINATION_ATTRIBUTE, remote);
                break;
        }
    }


    private CredentialSource getCredentialSource(Request request, TransferType type)
          throws ErrorResponseException {
        // Note that HttpSerlvetRequest#getHeader is case insensitive, as
        // required by:
        //
        //     https://tools.ietf.org/html/rfc7230#section-3.2
        //
        String headerValue = ServletRequest.getRequest().getHeader(REQUEST_HEADER_CREDENTIAL);
        CredentialSource source;

        if (headerValue != null) {
            source = CredentialSource.forHeaderValue(headerValue);

            if (source == null) {
                throw new ErrorResponseException(Status.SC_BAD_REQUEST,
                      "HTTP header 'Credential' has unknown value \"" +
                            headerValue + "\".  Valid values are: " +
                            Joiner.on(',').join(CredentialSource.headerValues()));
            }
            setCredentialAttribute(headerValue);
        } else if (clientAuthnUsingOidc()) {
            source = CredentialSource.OIDC;
            setCredentialAttribute("(oidc)");
        } else if (clientSuppliedX509Certificate()) {
            source = CredentialSource.GRIDSITE;
            setCredentialAttribute("(gridsite)");
        } else {
            source = CredentialSource.NONE;
            setCredentialAttribute("(none)");
        }

        if (!type.isSupported(source)) {
            throw new ErrorResponseException(Status.SC_BAD_REQUEST,
                  "Delegation " + source + " is not supported for " + type.getScheme());
        }

        return source;
    }

    private Object fetchCredential(CredentialSource source)
          throws InterruptedException, ErrorResponseException {
        Subject subject = Subject.getSubject(AccessController.getContext());
        switch (source) {
            case GRIDSITE:
                try {
                    HttpServletRequest request = ServletRequest.getRequest();
                    // Use the X.509 identity from TLS, even if that wasn't used to
                    // establish the user's identity.  This allows the local activity of
                    // the COPY (i.e., the ability to read a file, or create a new file)
                    // to be authorized based on some non-X.509 identity, while using a
                    // delegated X.509 credential when authenticating for the
                    // third-party copy, based on the client credential presented when
                    // establishing the TLS connection.
                    Subject x509Subject = AuthenticationHandler.getX509Identity(request);
                    String dn = x509Subject == null ? null : Subjects.getDn(x509Subject);
                    if (dn == null) {
                        throw new ErrorResponseException(Response.Status.SC_UNAUTHORIZED,
                              "user must present valid X.509 certificate");
                    }
                    String fqan = Objects.toString(Subjects.getPrimaryFqan(x509Subject), null);

                    /* If delegation has been requested and declined then
                     * potentially use the existing delegated credential.  We don't
                     * want to artifically fail requests that might otherwise
                     * succeed.
                     */
                    int minLifetimeInMinutes = hasClientAlreadyBeenRedirected(request)
                          ? 2 : 20;
                    return _credentialService.getDelegatedCredential(dn, fqan,
                          minLifetimeInMinutes, MINUTES);
                } catch (PermissionDeniedCacheException e) {
                    throw new ErrorResponseException(Status.SC_UNAUTHORIZED,
                          "Presented X.509 certificate not valid");
                } catch (CacheException e) {
                    throw new ErrorResponseException(Status.SC_INTERNAL_SERVER_ERROR,
                          "Internal problem: " + e.getMessage());
                }

            case OIDC:
                BearerTokenCredential bearer = subject.getPrivateCredentials()
                      .stream()
                      .filter(BearerTokenCredential.class::isInstance)
                      .map(BearerTokenCredential.class::cast)
                      .findFirst()
                      .orElseThrow(() ->
                            new ErrorResponseException(Status.SC_UNAUTHORIZED,
                                  "User must authenticate with OpenID for " +
                                        "OpenID delegation"));

                return _credentialService.getDelegatedCredential(bearer.getToken(),
                      _oidcClientCredentials);
            case NONE:
                return null;

            default:
                throw new RuntimeException("Unsupported source " + source);
        }
    }


    private void setCredentialAttribute(String value) {
        ServletRequest.getRequest().setAttribute(TPC_CREDENTIAL_ATTRIBUTE, value);
    }


    private void redirectWithDelegation(Response response) {
        /* The Request#getParams method looks promising, but does not seem to
         * provide the parameters of the request URI, despite what the JavaDoc
         * says.  Instead, the HttpServletRequest object is used to
         * discover any query values.
         */
        HttpServletRequest request = ServletRequest.getRequest();

        if (hasClientAlreadyBeenRedirected(request)) {
            LOGGER.debug("client failed to delegate a credential before re-requesting the COPY");
            response.sendError(Response.Status.SC_UNAUTHORIZED,
                  "client failed to delegate a credential");
            return;
        }

        try {
            response.setNonStandardHeader("X-Delegate-To",
                  _credentialService.getDelegationEndpoints().stream()
                        .map(URI::toASCIIString).collect(Collectors.joining(" ")));
            response.sendRedirect(buildRedirectUrl(request));
        } catch (IllegalArgumentException e) {
            LOGGER.debug(e.getMessage());
            response.sendError(Response.Status.SC_INTERNAL_SERVER_ERROR,
                  e.getMessage());
        }
    }


    private String buildRedirectUrl(HttpServletRequest servletRequest) {
        Map<String, String[]> requestParameters = servletRequest.getParameterMap();

        URI request;
        try {
            request = new URI(servletRequest.getRequestURL().toString());
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("cannot parse request URI: " +
                  e.getReason(), e);
        }

        Map<String, String[]> parameters = new HashMap<>(requestParameters);
        parameters.put(QUERY_KEY_ASKED_TO_DELEGATE, new String[]{"true"});

        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, String[]> entry : parameters.entrySet()) {
            String key = entry.getKey();
            String[] values = entry.getValue();
            if (sb.length() > 0 && values.length > 0) {
                sb.append('&');
            }
            for (String value : values) {
                sb.append(key).append("=").append(value);
            }
        }

        try {
            return new URI(request.getScheme(), request.getAuthority(),
                  request.getPath(), sb.toString(), null).toASCIIString();
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("cannot create redirection URI: " +
                  e.getReason(), e);
        }
    }


    private boolean hasClientAlreadyBeenRedirected(HttpServletRequest request) {
        return request.getParameter(QUERY_KEY_ASKED_TO_DELEGATE) != null;
    }

    private boolean clientAllowsOverwrite() throws ErrorResponseException {
        String overwrite = ServletRequest.getRequest().getHeader("Overwrite");
        if (overwrite != null) {
            switch (overwrite) {
                case "T":
                    return true;
                case "F":
                    return false;
                default:
                    throw new ErrorResponseException(Status.SC_BAD_REQUEST,
                          "Invalid Overwrite request header value: must be either 'T' or 'F'");
            }
        }

        return true;
    }

    private Subject getSubject() {
        return Subject.getSubject(AccessController.getContext());
    }

    private Restriction getRestriction() {
        HttpServletRequest servletRequest = ServletRequest.getRequest();
        return (Restriction) servletRequest.getAttribute(
              AuthenticationHandler.DCACHE_RESTRICTION_ATTRIBUTE);
    }

    private boolean clientAuthnUsingOidc() {
        return Subject.getSubject(AccessController.getContext()).getPrincipals().stream()
              .anyMatch(OidcSubjectPrincipal.class::isInstance);
    }

    private boolean clientSuppliedX509Certificate() {
        HttpServletRequest request = ServletRequest.getRequest();
        return Optional.ofNullable(request.getAttribute("javax.servlet.request.X509Certificate"))
              .filter(X509Certificate[].class::isInstance)
              .isPresent();
    }

    private static <T> Predicate<T> not(Predicate<T> t) {
        return t.negate();
    }

    private boolean isVerificationRequired() throws ErrorResponseException {
        // Note that HttpSerlvetRequest#getHeader is case insensitive, as
        // required by:
        //
        //     https://tools.ietf.org/html/rfc7230#section-3.2
        //
        String header = ServletRequest.getRequest().getHeader(REQUEST_HEADER_VERIFICATION);

        if (header == null) {
            setRequireChecksumVerificationAttribute(_defaultVerification ? "(true)" : "(false)");
            return _defaultVerification;
        }

        switch (header) {
            case "true":
                setRequireChecksumVerificationAttribute("true");
                return true;
            case "false":
                setRequireChecksumVerificationAttribute("false");
                return false;
            default:
                throw new ErrorResponseException(Status.SC_BAD_REQUEST,
                      "HTTP request header '" + REQUEST_HEADER_VERIFICATION + "' " +
                            "has unknown value \"" + header + "\": " +
                            "valid values are true or false");
        }
    }

    private void setRequireChecksumVerificationAttribute(String value) {
        ServletRequest.getRequest()
              .setAttribute(TPC_REQUIRE_CHECKSUM_VERIFICATION_ATTRIBUTE, value);
    }
}
