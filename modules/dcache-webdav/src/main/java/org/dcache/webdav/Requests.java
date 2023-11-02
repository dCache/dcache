/*
 * dCache - http://www.dcache.org/
 *
 * Copyright (C) 2023 Deutsches Elektronen-Synchrotron
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

import static java.util.Comparator.comparingDouble;

import com.google.common.base.Splitter;
import com.google.common.collect.Multimaps;
import com.google.common.net.MediaType;
import java.util.Comparator;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import javax.annotation.Nullable;
import java.net.URI;
import java.net.URL;
import java.nio.file.Path;
import java.util.Collection;

/**
 * Utility class for handling common aspects of an HTTP request.
 */
public class Requests {

    private static final Logger LOGGER = LoggerFactory.getLogger(Requests.class);

    /**
     * Describes which handler to prefer if the client's Accept request header highest q-value
     * supported selects multiple handlers, of which none are the default handler.  This exists
     * mostly to provide consistent behaviour: the exact choice (probably) doesn't matter too much.
     */
    private static final Comparator<MediaType> PREFERRING_SHORTER_NAMES =
          Comparator.<MediaType>comparingInt(m -> m.toString().length())
                .thenComparing(Object::toString);
    private static final Comparator<MediaType> PREFERRING_NON_WILDCARD_TYPES = (MediaType m1, MediaType m2)
              -> m1.hasWildcard() == m2.hasWildcard() ? 0 : m1.hasWildcard() ? 1 : -1;

    private Requests() { /* Prevent initialisation */ }

    /**
     * Choose a MediaType based on the client's Accept request header value.
     * <p>
     * The supportedTypes collection is used to filter the accept request
     * header terms.  The q-values are honoured, if specified.  For different
     * values with the same q-value, the selection favours non-wildcard over
     * wildcard type.  If a wildcard matches then the default is preferred.  If
     * the default type is not selected then selection favours shorter named
     * types.
     * <p>
     * The default media type is used if the client doesn't provide any
     * indication of which media type is desired or the client does not
     * request any of the supported types, or if the matching term is a
     * wildcard.
     * <p>
     * It is not required that the default type is part of the
     * collection of supported types; however, if the default type is missing
     * from the supportedTypes then the resulting behaviour may be confusing.
     * @param accept The Accept request header value.
     * @param supportedTypes A collection of media types that are supported.
     * @param defaultType The value to use if the client isn't selective.
     * @return The desired media type for this request.
     */
    public static MediaType selectResponseType(@Nullable String accept,
            Collection<MediaType> supportedTypes, MediaType defaultType) {
        if (accept == null) {
            LOGGER.debug("Client did not specify Accept header,"
                  + " responding with default MIME-Type \"{}\"", defaultType);
            return defaultType;
        }

        LOGGER.debug("Client indicated response preference: {}", accept);
        var acceptMimeTypes = Splitter.on(',').omitEmptyStrings().trimResults()
                .splitToList(accept);

        Comparator<MediaType> preferDefaultType = (MediaType m1, MediaType m2)
              -> m1.equals(defaultType) ? -1 : m2.equals(defaultType) ? 1 : 0;

        try {
            var responseType = acceptMimeTypes.stream()
                  .map(MediaType::parse)
                  .sorted(preferDefaultType)
                  .sorted(PREFERRING_NON_WILDCARD_TYPES)
                  .sorted(comparingDouble(Requests::qValueOf).reversed())
                  .map(Requests::dropQParameter)
                  .flatMap(acceptType -> supportedTypes.stream()
                        .filter(m -> m.is(acceptType))
                        .sorted(preferDefaultType.thenComparing(PREFERRING_SHORTER_NAMES)))
                  .findFirst();

            responseType.ifPresent(m -> LOGGER.debug("Responding with MIME-Type \"{}\"", m));

            return responseType.orElseGet(() -> {
                LOGGER.debug("Responding with default MIME-Type \"{}\"", defaultType);
                return defaultType;
            });
        } catch (IllegalArgumentException e) {
            // Client supplied an invalid media type.  Oh well, let's use a default.
            LOGGER.debug("Client supplied invalid Accept header \"{}\": {}",
                  accept, e.getMessage());
            return defaultType;
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

    /**
     * Extract the normalized path element of the given URL String excluding query information.
     *
     * @param url The string representation of the URL.
     * @return The path component of the URL.
     */
    public static String stripToPath(String uri) {
        return stripToPath(URI.create(uri).getPath());
    }

    /**
     * Extract the normalized path element of the given URL excluding query information.
     *
     * @param url The URL to extract path from.
     * @return The path component of the URL.
     */
    public static String stripToPath(URL url) {
        return Path.of(url.getPath()).normalize().toString();
    }
}
