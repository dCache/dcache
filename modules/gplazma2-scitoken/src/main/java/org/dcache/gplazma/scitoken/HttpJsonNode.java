/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2019 Deutsches Elektronen-Synchrotron
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
package org.dcache.gplazma.scitoken;

import com.google.common.net.MediaType;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.MissingNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;
import static org.dcache.util.Exceptions.messageOrClassName;

/**
 * A JsonNode that represents a JSON document at some remote location.  If there
 * is a network problem, or the document cannot be parsed as a JSON document
 * then this node behaves as if it is a MissingNode.  Otherwise it behaves as
 * the root node of the parsed document; e.g., an ObjectNode if the JSON document
 * is a JSON Object.
 * <p>
 * The node is cached for a configurable period, fetching fresh results once
 * that period has elapsed.  The caching period for successful and unsuccessful
 * reads may be different, allowing a more aggressive querying if the document
 * is missing.
 * <p>
 * Any values (e.g., JsonNode) returned by this class represent a concrete
 * JSON document.  Therefore, no network activity is triggered while navigating
 * the cached document.
 */
public class HttpJsonNode extends PreparationJsonNode
{
    private static final Logger LOGGER = LoggerFactory.getLogger(HttpJsonNode.class);

    private final HttpClient client;
    private final Supplier<Optional<String>> urlSupplier;
    private final Duration cacheHit;
    private final Duration cacheMiss;
    private final ObjectMapper mapper = new ObjectMapper();

    private volatile JsonNode cached = MissingNode.getInstance();
    private Instant nextCheck;

    public HttpJsonNode(HttpClient client, String url, Duration cacheHit, Duration cacheMiss)
    {
        this(client, () -> Optional.of(url), cacheHit, cacheMiss);
    }

    /**
     * An HttpJsonNode that never caches the result.  Each request to this node
     * will trigger an HTTP request to fetch up-to-date information.
     */
    public HttpJsonNode(HttpClient client, Supplier<Optional<String>> url)
    {
        this(client, url, Duration.ZERO, Duration.ZERO);
    }

    public HttpJsonNode(HttpClient client, Supplier<Optional<String>> url, Duration cacheHit, Duration cacheMiss)
    {
        this.client = requireNonNull(client);
        this.urlSupplier = url;
        this.cacheHit = cacheHit;
        this.cacheMiss = cacheMiss;
    }

    @Override
    protected synchronized void prepare()
    {
        Instant now = Instant.now();
        if (nextCheck == null || now.isAfter(nextCheck)) {
            cached = fetch();
            nextCheck = now.plus(cached.isMissingNode() ? cacheMiss : cacheHit);
        }
    }

    private JsonNode fetch()
    {
        return urlSupplier.get()
                .flatMap(this::fetch)
                .orElse(MissingNode.getInstance());
    }

    private Optional<JsonNode> fetch(String url)
    {
        HttpGet request = new HttpGet(url);
        request.addHeader("Accept", "application/json");
        try {
            HttpResponse response = client.execute(request);
            String entity = readEntity(response);
            return Optional.of(mapper.readValue(entity, JsonNode.class));
        } catch (IOException e) {
            LOGGER.error("Failed to fetch {}: {}", url, messageOrClassName(e));
            return Optional.empty();
        }
    }

    private static String readEntity(HttpResponse response) throws IOException
    {
        Charset charset = readCharset(response);

        HttpEntity entity = response.getEntity();

        if (entity == null) {
            throw new IOException("Missing entity in HTTP response");
        }

        long length = entity.getContentLength();
        if (length > Integer.MAX_VALUE) {
            throw new IOException("Document too big to be parsed");
        }

        ByteArrayOutputStream os = (length > 0) ? new ByteArrayOutputStream((int)length) : new ByteArrayOutputStream();
        entity.writeTo(os);
        return os.toString(charset.name());
    }


    private static Charset readCharset(HttpResponse response) throws IOException
    {

        Header contentType = response.getLastHeader("Content-Type");
        if (contentType == null || contentType.getValue() == null) {
            return StandardCharsets.US_ASCII;
        }

        MediaType type = MediaType.parse(contentType.getValue());
        String baseType = type.withoutParameters().toString().toLowerCase();
        switch (baseType) {
            case "application/json":
            case "application/octet-stream": // basically "unknown"
                break;
            default:
                throw new IOException("Unexpected Content-Type: " + baseType);
        }
        return type.charset().or(StandardCharsets.US_ASCII);
    }

    @Override
    protected JsonNode delegate()
    {
        return cached;
    }
}
