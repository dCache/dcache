/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2014 Deutsches Elektronen-Synchrotron
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

import com.google.common.base.Joiner;
import eu.emi.security.authn.x509.X509Credential;
import io.milton.http.Filter;
import io.milton.http.FilterChain;
import io.milton.http.Request;
import io.milton.http.Response;
import io.milton.http.Response.Status;
import io.milton.http.exceptions.BadRequestException;
import io.milton.servlet.ServletRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;

import javax.security.auth.Subject;
import javax.servlet.http.HttpServletRequest;

import java.net.URI;
import java.net.URISyntaxException;
import java.security.AccessController;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.FileNotFoundCacheException;
import diskCacheV111.util.FsPath;
import diskCacheV111.util.PermissionDeniedCacheException;
import diskCacheV111.util.PnfsHandler;

import org.dcache.acl.enums.AccessMask;
import org.dcache.auth.Subjects;
import org.dcache.auth.attributes.Restriction;
import org.dcache.cells.CellStub;
import org.dcache.namespace.FileType;
import org.dcache.vehicles.FileAttributes;
import org.dcache.webdav.PathMapper;
import org.dcache.webdav.transfer.RemoteTransferHandler.Direction;
import org.dcache.webdav.AuthenticationHandler;
import org.dcache.webdav.transfer.RemoteTransferHandler.TransferType;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.dcache.namespace.FileAttribute.*;

/**
 * The CopyFilter adds support for initiating third-party copies via
 * WebDAV.  This makes use of a protocol extension described here:
 *
 * https://svnweb.cern.ch/trac/lcgdm/wiki/Dpm/WebDAV/Extensions#ThirdPartyCopies
 *
 * In essence, the client makes a normal WebDAV COPY request, but uses a
 * non-local destination URI as the Destination request header.  The client
 * receives periodic progress reports, similar to those provided on an FTP
 * control channel, followed by a final report of either success or failure.
 *
 * Authorisation is assumed to be via X.509 client certificates.  To achieve
 * authorisation on the remote server, dCache needs to have a valid credential
 * with which to authenticate with the remote server.  A client-supplied
 * credential is needed, and fetched from an srm service.
 *
 * If the srm credential stores holds no valid credential then the client is
 * requested to delegate a credential.  This is achieved by redirecting the
 * client back to the door with a custom header pointing to the delegation
 * endpoint.  In the redirection, the request URI is modified by adding a
 * query part.  This is used to detect request loops when identity delegation
 * is required but the client fails to delegate.
 *
 * Orchestrating the transfers is handled by an instance of the
 * RemoteTransferHandler class.  After a transfer has been accepted, the server
 * responds with periodic progress markers.  This is handled by the
 * RemoteTransferHandler class.
 */
public class CopyFilter implements Filter
{
    private static final Logger _log =
            LoggerFactory.getLogger(CopyFilter.class);

    private static final String QUERY_KEY_ASKED_TO_DELEGATE = "asked-to-delegate";
    private static final String REQUEST_HEADER_CREDENTIAL = "Credential";
    private static final Set<AccessMask> READ_ACCESS_MASK =
            EnumSet.of(AccessMask.READ_DATA);
    private static final Set<AccessMask> WRITE_ACCESS_MASK =
            EnumSet.of(AccessMask.WRITE_DATA);
    private static final Set<AccessMask> CREATE_ACCESS_MASK =
            EnumSet.of(AccessMask.ADD_FILE);

    /**
     * Describes where to fetch the delegated credential, if at all.
     */
    public enum CredentialSource {
        /* Get it from an SRM's GridSite credential store */
        GRIDSITE("gridsite"),

        /* Don't get a credential */
        NONE("none");

        private static final Map<String,CredentialSource> SOURCE_FOR_HEADER =
                new HashMap<>();

        private final String _headerValue;

        static {
            for (CredentialSource source : CredentialSource.values()) {
                SOURCE_FOR_HEADER.put(source.getHeaderValue(), source);
            }
        }

        public static CredentialSource forHeaderValue(String value)
        {
            return SOURCE_FOR_HEADER.get(value);
        }

        public static Iterable<String> headerValues()
        {
            return SOURCE_FOR_HEADER.keySet();
        }

        CredentialSource(String value)
        {
            _headerValue = value;
        }

        public String getHeaderValue()
        {
            return _headerValue;
        }
    }

    private PnfsHandler _pnfs;
    private CredentialServiceClient _credentialService;
    private PathMapper _pathMapper;
    private RemoteTransferHandler _remoteTransfers;

    @Required
    public void setPathMapper(PathMapper mapper)
    {
        _pathMapper = mapper;
    }

    @Required
    public void setPnfsStub(CellStub stub)
    {
        _pnfs = new PnfsHandler(stub);
    }

    @Required
    public void setCredentialServiceClient(CredentialServiceClient client)
    {
        _credentialService = client;
    }

    @Required
    public void setRemoteTransferHandler(RemoteTransferHandler handler)
    {
        _remoteTransfers = handler;
    }

    @Override
    public void process(FilterChain chain, Request request, Response response)
    {
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
            throws BadRequestException
    {
        if (request.getMethod() != Request.Method.COPY) {
            return false;
        }

        URI uri = getRemoteLocation();

        // We treat any URI that has scheme and host parts as a
        // third-party transfer request.  This isn't guaranteed but
        // probably good enough for now.
        return uri.getScheme() != null && uri.getHost() != null;
    }

    private static Direction getDirection() throws BadRequestException
    {
        // Note that each invocation of Request#getHeaders creates a HashMap
        // and populates it with all headers.  Therefore, we use ServletRequest
        // which Milton only makes available via a ThreadLocal.
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

    private static URI getRemoteLocation() throws BadRequestException
    {
        Direction direction = getDirection();
        String remote = ServletRequest.getRequest().getHeader(direction.getHeaderName());

        try {
            return new URI(remote);
        } catch (URISyntaxException e) {
            throw new BadRequestException(direction.getHeaderName() +
                    " request header contains an invalid URI: " + e.getMessage());
        }
    }

    private void processThirdPartyCopy(Request request, Response response)
            throws BadRequestException, InterruptedException, ErrorResponseException
    {
        Direction direction = getDirection();
        URI remote = getRemoteLocation();

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
                request.getAbsolutePath());
        checkPath(path, direction);

        CredentialSource source = getCredentialSource(request, type);
        X509Credential credential = fetchCredential(source);
        if (credential == null && source != CredentialSource.NONE) {
            redirectWithDelegation(response);
        } else {
            _remoteTransfers.acceptRequest(response.getOutputStream(),
                    request.getHeaders(), getSubject(), getRestriction(), path,
                    remote, credential, direction);
        }
    }

    private CredentialSource getCredentialSource(Request request, TransferType type)
            throws ErrorResponseException
    {
        String headerValue = request.getHeaders().get(REQUEST_HEADER_CREDENTIAL);

        CredentialSource source = headerValue != null ?
                CredentialSource.forHeaderValue(headerValue) :
                type.getDefaultCredentialSource();

        if (source == null) {
            throw new ErrorResponseException(Status.SC_BAD_REQUEST,
                    "HTTP header 'Credential' has unknown value \"" +
                    headerValue + "\".  Valid values are: " +
                    Joiner.on(',').join(CredentialSource.headerValues()));
        }

        if (!type.isSupported(source)) {
            throw new ErrorResponseException(Status.SC_BAD_REQUEST,
                    "HTTP header 'Credential' value \"" + headerValue + "\" is not " +
                    "supported for transport " + type.getScheme());
        }

        return source;
    }

    private X509Credential fetchCredential(CredentialSource source)
            throws InterruptedException, ErrorResponseException
    {
        switch (source) {
        case GRIDSITE:
            Subject subject = Subject.getSubject(AccessController.getContext());
            String dn = Subjects.getDn(subject);
            if (dn == null) {
                throw new ErrorResponseException(Response.Status.SC_UNAUTHORIZED,
                                                 "user must present valid X.509 certificate");
            }

            return _credentialService.getDelegatedCredential(
                    dn, Objects.toString(Subjects.getPrimaryFqan(subject), null),
                    20, MINUTES);

        case NONE:
            return null;

        default:
            throw new RuntimeException("Unsupported source " + source);
        }
    }


    private void redirectWithDelegation(Response response)
    {
        /* The Request#getParams method looks promising, but does not seem to
         * provide the parameters of the request URI, despite what the JavaDoc
         * says.  Instead, the HttpServletRequest object is used to
         * discover any query values.
         */
        HttpServletRequest request = ServletRequest.getRequest();

        if (hasClientAlreadyBeenRedirected(request)) {
            _log.debug("client failed to delegate a credential before re-requesting the COPY");
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
            _log.debug(e.getMessage());
            response.sendError(Response.Status.SC_INTERNAL_SERVER_ERROR,
                    e.getMessage());
        }
    }


    private String buildRedirectUrl(HttpServletRequest servletRequest)
    {
        Map<String,String[]> requestParameters = servletRequest.getParameterMap();

        URI request;
        try {
            request = new URI(servletRequest.getRequestURL().toString());
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("cannot parse request URI: " +
                    e.getReason(), e);
        }

        Map<String,String[]> parameters = new HashMap<>(requestParameters);
        parameters.put(QUERY_KEY_ASKED_TO_DELEGATE, new String[]{"true"});

        StringBuilder sb = new StringBuilder();
        for(Map.Entry<String,String[]> entry : parameters.entrySet()) {
            String key = entry.getKey();
            String[] values = entry.getValue();
            if(sb.length() > 0 && values.length > 0) {
                sb.append('&');
            }
            for(String value : values) {
                sb.append(key).append("=").append(value);
            }
        }

        try {
            return new URI(request.getScheme(), request.getAuthority(),
                    request.getPath(), sb.toString(),null).toASCIIString();
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("cannot create redirection URI: " +
                    e.getReason(), e);
        }
    }


    private boolean hasClientAlreadyBeenRedirected(HttpServletRequest request)
    {
        return request.getParameter(QUERY_KEY_ASKED_TO_DELEGATE) != null;
    }

    private boolean clientAllowsOverwrite() throws ErrorResponseException
    {
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

    /**
     * Check whether the path is allowed for this transfer.
     */
    private void checkPath(FsPath path, Direction direction) throws ErrorResponseException
    {
        PnfsHandler pnfs = new PnfsHandler(_pnfs, getSubject(), getRestriction());

        // Always check any client-supplied Overwrite header.
        boolean overwriteAllowed = clientAllowsOverwrite();

        Set<AccessMask> mask = direction == Direction.PUSH ? READ_ACCESS_MASK : WRITE_ACCESS_MASK;
        FileAttributes attributes;
        try {
            try {
                attributes = pnfs.getFileAttributes(path.toString(),
                        EnumSet.of(PNFSID, TYPE), mask, false);

                if (attributes.getFileType() != FileType.REGULAR) {
                    throw new ErrorResponseException(Status.SC_BAD_REQUEST, "Not a file");
                }

                if (direction == Direction.PULL) {
                    if (!overwriteAllowed) {
                        throw new ErrorResponseException(Status.SC_PRECONDITION_FAILED,
                                "File already exists");
                    }

                    // REVISIT: ideally, the transfermanager would handle
                    // deleting existing entry.
                    try {
                        pnfs.deletePnfsEntry(attributes.getPnfsId(), path.toString(),
                                EnumSet.of(FileType.REGULAR));
                    } catch (FileNotFoundCacheException ignored) {
                        // Ignore this: someone else deleted the file, which
                        // suggests we might be unlucky pulling the data.
                    }
                }
            } catch (FileNotFoundCacheException e) {
                if (direction == Direction.PUSH) {
                    throw new ErrorResponseException(Status.SC_NOT_FOUND, "no such file");
                } else {
                    pnfs.getFileAttributes(path.getParent().toString(), Collections.emptySet(),
                                           CREATE_ACCESS_MASK, false);
                }
            }
        } catch (PermissionDeniedCacheException e) {
            _log.debug("Permission denied: {}", e.getMessage());
            throw new ErrorResponseException(Status.SC_UNAUTHORIZED,
                    "Permission denied");
        } catch (CacheException e) {
            _log.error("failed query file {} for copy request: {}", path,
                    e.getMessage());
            throw new ErrorResponseException(Status.SC_INTERNAL_SERVER_ERROR,
                    "Internal problem with server");
        }
    }

    private Subject getSubject()
    {
        return Subject.getSubject(AccessController.getContext());
    }

    private Restriction getRestriction()
    {
        HttpServletRequest servletRequest = ServletRequest.getRequest();
        return (Restriction) servletRequest.getAttribute(AuthenticationHandler.DCACHE_RESTRICTION_ATTRIBUTE);
    }
}
