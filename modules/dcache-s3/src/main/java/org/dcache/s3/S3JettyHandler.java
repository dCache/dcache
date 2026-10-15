/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2026 Deutsches Elektronen-Synchrotron
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
package org.dcache.s3;

import static java.util.Objects.requireNonNull;

import com.google.common.base.Throwables;
import com.google.common.net.HttpHeaders;
import com.google.common.net.InetAddresses;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeoutException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.gaul.s3proxy.AwsHttpHeaders;
import org.gaul.s3proxy.S3ErrorCode;
import org.gaul.s3proxy.S3ProxyHandler;
import org.gaul.s3proxy.auth.S3AuthorizationHeader;
import org.gaul.s3proxy.blobstore.S3Exceptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.awscore.exception.AwsServiceException;

/**
 * Jetty handler of the S3 door: binds the client address to the request thread, lets s3proxy
 * handle the request and renders failures as S3 error documents.  Modelled on s3proxy's
 * {@code S3ProxyHandlerJetty}, which needs Jetty 12.
 */
public class S3JettyHandler extends AbstractHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(S3JettyHandler.class);

    /**
     * Request attribute holding the access key a request claims, for the access log.
     */
    static final String ACCESS_KEY_ATTRIBUTE = "org.dcache.s3.access-key";

    /**
     * Request attribute holding the S3 operation of a request, for the access log.
     */
    static final String OPERATION_ATTRIBUTE = "org.dcache.s3.operation";

    private final DcacheS3ProxyHandler handler;

    public S3JettyHandler(DcacheS3ProxyHandler handler) {
        this.handler = requireNonNull(handler);
    }

    @Override
    public void handle(String target, Request baseRequest, HttpServletRequest request,
          HttpServletResponse response) throws IOException {
        baseRequest.setHandled(true);
        accessKey(request).ifPresent(key -> request.setAttribute(ACCESS_KEY_ATTRIBUTE, key));
        var ctx = new S3ProxyHandler.RequestContext();
        InetAddress client = InetAddresses.forString(request.getRemoteAddr());
        try (var origin = RequestOrigin.bind(client);
              InputStream is = request.getInputStream()) {
            handler.doHandle(request, request, response, is, ctx);
        } catch (IllegalArgumentException e) {
            LOGGER.debug("Bad request: {}", e.toString());
            response.sendError(HttpServletResponse.SC_BAD_REQUEST, e.getMessage());
        } catch (AwsServiceException e) {
            sendError(request, response, e);
        } catch (UnsupportedOperationException e) {
            LOGGER.debug("Not implemented: {}", e.toString());
            handler.sendError(request, response, S3ErrorCode.NOT_IMPLEMENTED);
        } catch (IOException | RuntimeException e) {
            List<Throwable> causes = Throwables.getCausalChain(e);
            Optional<AwsServiceException> s3Error = causes.stream()
                  .filter(AwsServiceException.class::isInstance)
                  .map(AwsServiceException.class::cast)
                  .findFirst();
            if (s3Error.isPresent()) {
                // e.g. BadDigest found while reading the body
                sendError(request, response, s3Error.get());
            } else if (causes.stream().anyMatch(TimeoutException.class::isInstance)) {
                handler.sendError(request, response, S3ErrorCode.REQUEST_TIMEOUT);
            } else if (e instanceof IOException) {
                // Typically the client went away; nothing left to tell it.
                LOGGER.debug("S3 request failed: {}", e.toString());
                throw (IOException) e;
            } else {
                LOGGER.error("Bug while handling S3 request {} {}", request.getMethod(),
                      request.getRequestURI(), e);
                handler.sendError(request, response, S3ErrorCode.INTERNAL_ERROR);
            }
        } finally {
            if (ctx.getOperation() != null) {
                request.setAttribute(OPERATION_ATTRIBUTE, ctx.getOperation().getValue());
            }
        }
    }

    /**
     * Renders an error a blob store or s3proxy raised.  An error with an S3 code renders as it
     * is; one with only a status is mapped to an error document by status, as s3proxy does.
     */
    private void sendError(HttpServletRequest request, HttpServletResponse response,
          AwsServiceException e) throws IOException {
        var errorResponse = S3Exceptions.httpResponse(e);
        if (errorResponse != null) {
            for (String header : List.of(HttpHeaders.ETAG, AwsHttpHeaders.DELETE_MARKER,
                  AwsHttpHeaders.VERSION_ID, HttpHeaders.ALLOW)) {
                errorResponse.firstMatchingHeader(header)
                      .ifPresent(value -> response.setHeader(header, value));
            }
        }

        String code = S3Exceptions.errorCode(e);
        if (code != null) {
            String message = e.awsErrorDetails().errorMessage();
            handler.sendError(request, response, code, e.statusCode(),
                  message == null ? "" : message);
            return;
        }

        switch (e.statusCode()) {
            case HttpServletResponse.SC_NOT_MODIFIED ->
                  response.setStatus(HttpServletResponse.SC_NOT_MODIFIED);
            case HttpServletResponse.SC_PRECONDITION_FAILED ->
                  handler.sendError(request, response, S3ErrorCode.PRECONDITION_FAILED);
            case HttpServletResponse.SC_REQUESTED_RANGE_NOT_SATISFIABLE ->
                  handler.sendError(request, response, S3ErrorCode.INVALID_RANGE);
            case HttpServletResponse.SC_METHOD_NOT_ALLOWED ->
                  handler.sendError(request, response, S3ErrorCode.METHOD_NOT_ALLOWED);
            case HttpServletResponse.SC_FORBIDDEN ->
                  handler.sendError(request, response, S3ErrorCode.ACCESS_DENIED);
            default -> {
                LOGGER.debug("S3 error without code: {}", e.toString());
                response.setStatus(e.statusCode());
            }
        }
    }

    /**
     * Returns the access key a request claims: from the Authorization header, or from the query
     * of a presigned URL.  The claim is not verified; it is only logged.
     */
    private static Optional<String> accessKey(HttpServletRequest request) {
        String authorization = request.getHeader(HttpHeaders.AUTHORIZATION);
        if (authorization != null) {
            try {
                return Optional.ofNullable(new S3AuthorizationHeader(authorization).getIdentity());
            } catch (IllegalArgumentException e) {
                return Optional.empty();
            }
        }
        String v2 = request.getParameter("AWSAccessKeyId");
        if (v2 != null) {
            return Optional.of(v2);
        }
        String credential = request.getParameter("X-Amz-Credential");
        if (credential != null) {
            int slash = credential.indexOf('/');
            return Optional.of(slash == -1 ? credential : credential.substring(0, slash));
        }
        return Optional.empty();
    }
}
