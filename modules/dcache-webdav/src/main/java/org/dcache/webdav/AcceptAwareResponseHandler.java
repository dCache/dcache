/*
 * dCache - http://www.dcache.org/
 *
 * Copyright (C) 2021 Deutsches Elektronen-Synchrotron
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
package org.dcache.webdav;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Comparator.comparingDouble;
import static java.util.Objects.requireNonNull;

import com.google.common.base.Splitter;
import com.google.common.collect.Multimaps;
import com.google.common.net.MediaType;
import io.milton.http.HrefStatus;
import io.milton.http.Range;
import io.milton.http.Request;
import io.milton.http.Response;
import io.milton.http.exceptions.BadRequestException;
import io.milton.http.exceptions.NotAuthorizedException;
import io.milton.http.exceptions.NotFoundException;
import io.milton.http.http11.Bufferable;
import io.milton.http.http11.DefaultHttp11ResponseHandler.BUFFERING;
import io.milton.http.quota.StorageChecker;
import io.milton.http.webdav.PropFindResponse;
import io.milton.http.webdav.WebDavResponseHandler;
import io.milton.resource.GetableResource;
import io.milton.resource.Resource;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is an implementation of WebDavResponseHandler that selects the way in which dCache responds
 * to a request based on the 'Accept' HTTP request header.  This is the standard mechanism for a
 * client to indicate what kind of response it is expecting.
 */
public class AcceptAwareResponseHandler implements WebDavResponseHandler, Bufferable {

    private static final Logger LOGGER = LoggerFactory.getLogger(AcceptAwareResponseHandler.class);

    /**
     * Describes which handler to prefer if the client's Accept request header highest q-value
     * supported selects multiple handlers, of which none are the default handler.  This exists
     * mostly to provide consistent behaviour: the exact choice (probably) doesn't matter too much.
     */
    private static final Comparator<MediaType> PREFERRING_SHORTER_NAMES =
          Comparator.<MediaType>comparingInt(m -> m.toString().length())
                .thenComparing(Object::toString);

    private final Map<MediaType, WebDavResponseHandler> handlers = new HashMap<>();
    private MediaType defaultType;
    private WebDavResponseHandler defaultHandler;
    private BUFFERING buffering;

    public void addResponse(MediaType media, WebDavResponseHandler handler) {
        handlers.put(requireNonNull(media), requireNonNull(handler));

        if (buffering != null && handler instanceof Bufferable) {
            ((Bufferable) handler).setBuffering(buffering);
        }
    }

    public void setDefaultResponse(MediaType media) {
        checkState(defaultType == null, "Default response already set");

        WebDavResponseHandler handler = handlers.get(media);
        checkArgument(handler != null,
              "No registered handler for media %s", media);

        defaultType = media;
        defaultHandler = handler;
    }

    @Override
    public void setBuffering(BUFFERING buffering) {
        this.buffering = buffering;

        handlers.values().stream()
              .filter(Bufferable.class::isInstance)
              .map(Bufferable.class::cast)
              .forEach(b -> b.setBuffering(buffering));
    }

    @Override
    public BUFFERING getBuffering() {
        return buffering;
    }

    @Override
    public void responseMultiStatus(Resource resource, Response response,
          Request request, List<HrefStatus> statii) {
        selectHandler(request).responseMultiStatus(resource, response, request, statii);
    }

    @Override
    public void respondPropFind(List<PropFindResponse> propFindResponses,
          Response response, Request request, Resource r) {
        selectHandler(request).respondPropFind(propFindResponses, response, request, r);
    }

    @Override
    public void respondInsufficientStorage(Request request, Response response,
          StorageChecker.StorageErrorReason storageErrorReason) {
        selectHandler(request).respondInsufficientStorage(request, response, storageErrorReason);
    }

    @Override
    public void respondLocked(Request request, Response response,
          Resource existingResource) {
        selectHandler(request).respondLocked(request, response, existingResource);
    }

    @Override
    public void respondPreconditionFailed(Request request, Response response,
          Resource resource) {
        selectHandler(request).respondPreconditionFailed(request, response, resource);
    }

    @Override
    public void respondNoContent(Resource resource, Response response,
          Request request) {
        selectHandler(request).respondNoContent(resource, response, request);
    }

    @Override
    public void respondContent(Resource resource, Response response,
          Request request, Map<String, String> params)
          throws NotAuthorizedException, BadRequestException, NotFoundException {
        selectHandler(request).respondContent(resource, response, request, params);
    }

    @Override
    public void respondPartialContent(GetableResource resource, Response response,
          Request request, Map<String, String> params, Range range)
          throws NotAuthorizedException, BadRequestException, NotFoundException {
        selectHandler(request).respondPartialContent(resource, response, request,
              params, range);
    }

    @Override
    public void respondPartialContent(GetableResource resource, Response response,
          Request request, Map<String, String> params, List<Range> ranges)
          throws NotAuthorizedException, BadRequestException, NotFoundException {
        selectHandler(request).respondPartialContent(resource, response, request,
              params, ranges);
    }

    @Override
    public void respondCreated(Resource resource, Response response,
          Request request) {
        selectHandler(request).respondCreated(resource, response, request);
    }

    @Override
    public void respondUnauthorised(Resource resource, Response response,
          Request request) {
        selectHandler(request).respondUnauthorised(resource, response, request);
    }

    @Override
    public void respondMethodNotImplemented(Resource resource, Response response,
          Request request) {
        selectHandler(request).respondMethodNotImplemented(resource, response, request);
    }

    @Override
    public void respondMethodNotAllowed(Resource res, Response response,
          Request request) {
        selectHandler(request).respondMethodNotAllowed(res, response, request);
    }

    @Override
    public void respondConflict(Resource resource, Response response,
          Request request, String message) {
        selectHandler(request).respondConflict(resource, response, request, message);
    }

    @Override
    public void respondRedirect(Response response, Request request,
          String redirectUrl) {
        selectHandler(request).respondRedirect(response, request, redirectUrl);
    }

    @Override
    public void respondNotModified(GetableResource resource, Response response,
          Request request) {
        selectHandler(request).respondNotModified(resource, response, request);
    }

    @Override
    public void respondNotFound(Response response, Request request) {
        selectHandler(request).respondNotFound(response, request);
    }

    @Override
    public void respondWithOptions(Resource resource, Response response,
          Request request, List<String> methodsAllowed) {
        selectHandler(request).respondWithOptions(resource, response, request,
              methodsAllowed);
    }

    @Override
    public void respondHead(Resource resource, Response response, Request request) {
        selectHandler(request).respondHead(resource, response, request);
    }

    @Override
    public void respondExpectationFailed(Response response, Request request) {
        selectHandler(request).respondExpectationFailed(response, request);
    }

    @Override
    public void respondBadRequest(Resource resource, Response response,
          Request request) {
        selectHandler(request).respondBadRequest(resource, response, request);
    }

    @Override
    public void respondForbidden(Resource resource, Response response,
          Request request) {
        selectHandler(request).respondForbidden(resource, response, request);
    }

    @Override
    public void respondDeleteFailed(Request request, Response response,
          Resource resource, Response.Status status) {
        selectHandler(request).respondDeleteFailed(request, response, resource,
              status);
    }

    @Override
    public void respondServerError(Request request, Response response,
          String reason) {
        selectHandler(request).respondServerError(request, response, reason);
    }

    @Override
    public String generateEtag(Resource r) {
        // REVISIT: does this need to be Accept-specific?
        return defaultHandler.generateEtag(r);
    }

    private WebDavResponseHandler selectHandler(Request request) {
        String accept = request.getRequestHeader(Request.Header.ACCEPT);

        if (accept == null) {
            LOGGER.debug("Client did not specify Accept header,"
                  + " responding with default MIME-Type \"{}\"", defaultType);
            return defaultHandler;
        }

        LOGGER.debug("Client indicated response preference as \"Accept: {}\"", accept);
        var acceptMimeTypes = Splitter.on(',').omitEmptyStrings().trimResults().splitToList(accept);
        Comparator<MediaType> preferDefaultType = (MediaType m1, MediaType m2)
              -> m1.equals(defaultType) ? -1 : m2.equals(defaultType) ? 1 : 0;

        try {
            var responseType = acceptMimeTypes.stream()
                  .map(MediaType::parse)
                  .sorted(comparingDouble(AcceptAwareResponseHandler::qValueOf).reversed())
                  .map(AcceptAwareResponseHandler::dropQParameter)
                  .flatMap(acceptType -> handlers.keySet().stream()
                        .filter(m -> m.is(acceptType))
                        .sorted(preferDefaultType.thenComparing(PREFERRING_SHORTER_NAMES)))
                  .findFirst();

            responseType.ifPresent(m -> LOGGER.debug("Responding with MIME-Type \"{}\"", m));

            return responseType.map(handlers::get).orElseGet(() -> {
                LOGGER.debug("Responding with default MIME-Type \"{}\"", defaultType);
                return defaultHandler;
            });
        } catch (IllegalArgumentException e) {
            // Client supplied an invalid media type.  Oh well, let's use a default.
            LOGGER.debug("Client supplied invalid Accept header \"{}\": {}",
                  accept, e.getMessage());
            return defaultHandler;
        }
    }

    /**
     * Filter out the 'q' value from the MIME-Type, if one is present.  This is needed because the
     * MIME-Type matching requires the server supports all parameters the client supplied, which
     * includes the 'q' value. As examples: {@literal "Accept: text/plain"               matches
     *    "text/plain;charset=UTF_8" "Accept: text/plain;charset=UTF_8" matches
     * "text/plain;charset=UTF_8" "Accept: text/plain;q=0.5"         does NOT match
     * "text/plain;charset=UTF_8" } as there is no {@literal q} parameter in the right-hand-side.
     * <p>
     * Stripping off the q value allows {@literal Accept: text/plain;q=0.5} (matched as {@literal
     * text/plain}) to match {@literal text/plain;charset=UTF_8}.
     */
    private static MediaType dropQParameter(MediaType acceptType) {
        var params = acceptType.parameters();

        MediaType typeWithoutQ;
        if (params.get("q").isEmpty()) {
            LOGGER.debug("MIME-Type \"{}\" has no q-value", acceptType);
            typeWithoutQ = acceptType;
        } else {
            var paramsWithoutQ = Multimaps.filterKeys(params, k -> !k.equals("q"));
            typeWithoutQ = acceptType.withParameters(paramsWithoutQ);
            LOGGER.debug("Stripping q-value from MIME-Type \"{}\" --> \"{}\"",
                  acceptType, typeWithoutQ);
        }

        return typeWithoutQ;
    }

    private static float qValueOf(MediaType m) {
        List<String> qValues = m.parameters().get("q");

        if (qValues.isEmpty()) {
            return 1.0f;
        }

        String lastQValue = qValues.get(qValues.size() - 1);
        try {
            return Float.parseFloat(lastQValue);
        } catch (NumberFormatException e) {
            LOGGER.debug("MIME-Type \"{}\" has invalid q value: {}", m,
                  lastQValue);
            return 1.0f;
        }
    }
}
