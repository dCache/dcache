/*
 * dCache - http://www.dcache.org/
 *
 * Copyright (C) 2024 Deutsches Elektronen-Synchrotron
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
package org.dcache.gplazma.alise;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
import com.google.common.hash.Hashing;
import com.google.common.net.PercentEscaper;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.nio.charset.StandardCharsets;
import java.security.Principal;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import org.dcache.auth.FullNamePrincipal;
import org.dcache.auth.UserNamePrincipal;
import org.dcache.util.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Objects.requireNonNull;
import static org.dcache.util.TimeUtils.TimeUnitFormat.SHORT;
import static org.dcache.util.TimeUtils.appendDuration;

/**
 * Make an HTTP request to ALISE to discover the local identity of a user.
 */
public class AliseLookupAgent implements LookupAgent {

    private static final Logger LOGGER = LoggerFactory.getLogger(AliseLookupAgent.class);

    private final String apikey;
    private final HttpClient client;
    private final URI endpoint;
    private final String target;
    private final Duration timeout;

    public AliseLookupAgent(URI endpoint, String target, String apikey,
            String timeout) {
        this(HttpClient.newHttpClient(), endpoint, target, apikey, timeout);
    }

    @VisibleForTesting
    AliseLookupAgent(HttpClient client, URI endpoint, String target,
            String apikey, String timeout) {
        this.client = requireNonNull(client);
        this.apikey = requireNonNull(apikey);
        this.endpoint = requireNonNull(endpoint);
        this.target = requireNonNull(target);
        this.timeout = Duration.parse(timeout);
    }

    private URI buildQueryUrl(Identity identity) {
        URI issuer = identity.issuer();
        var issuerHash = Hashing.sha1().hashString(issuer.toASCIIString(), StandardCharsets.UTF_8).toString();

        String subject = identity.sub();
        var encodedSub = new PercentEscaper(".", false).escape(subject);

        String relPath = "api/v1/target/" + target + "/mapping/issuer/" + issuerHash + "/user/" + encodedSub + "?apikey=" + apikey;
        return endpoint.resolve(relPath);
    }

    @Override
    public Result<Collection<Principal>,String> lookup(Identity identity) {
        LOGGER.debug("Querying for identity {}", identity);
        URI queryUrl = buildQueryUrl(identity);
        var request = HttpRequest.newBuilder(queryUrl).timeout(timeout).build();

        try {
            LOGGER.debug("Making ALISE request {}", queryUrl);
            Stopwatch waitingForResponse = Stopwatch.createStarted();
            var response = client.send(request, BodyHandlers.ofString());

            if (LOGGER.isDebugEnabled()) {
                Duration delay = waitingForResponse.elapsed();
                var sb = new StringBuilder("ALISE response took ");
                appendDuration(sb, delay, SHORT)
                        .append(": ")
                        .append(response.statusCode())
                        .append(' ')
                        .append(response.body());
                LOGGER.debug(sb.toString());
            }

            return resultFromResponse(response);
        } catch (InterruptedException | IOException e) {
            LOGGER.debug("Problem contacting ALISE server: {}", e.toString());
            return Result.failure("problem communicating with ALISE server: "
                    + e.toString());
        }
    }

    private Result<Collection<Principal>, String> resultFromResponse(HttpResponse<String> response) {
        Optional<String> contentType = response.headers().firstValue("Content-Type");
        if (contentType.isPresent()) {
            String mediaType = contentType.get();
            if (!mediaType.equals("application/json")) {
                return Result.failure("Response not JSON (" + mediaType + ")");
            }
        }

        JsonNode json;
        try {
            ObjectMapper mapper = new ObjectMapper();
            json = mapper.readValue(response.body(), JsonNode.class);
        } catch (JsonProcessingException e) {
            return Result.failure("Bad JSON in response: " + e.getMessage());
        }

        if (response.statusCode() != 200) {
            String message = buildErrorMessage(json);
            return Result.failure("ALISE reported a problem: " + message);
        }

        return resultFromSuccessfulHttpRequest(json);
    }

    private String buildErrorMessage(JsonNode body) {
        if (body.has("message")) {
            JsonNode messageNode = body.get("message");
            if (messageNode.isTextual()) {
                return messageNode.asText();
            } else {
                return "unknown (\"message\" field is not textual)";
            }
        }

        if (body.has("detail")) {
            JsonNode detailArrayNode = body.get("detail");
            if (!detailArrayNode.isArray()) {
                return "unknown (\"detail\" not array)";
            }

            StringBuilder sb = new StringBuilder();
            for (JsonNode detail : detailArrayNode) {
                if (sb.length() != 0) {
                    sb.append(", ");
                }

                if (!detail.isObject()) {
                    sb.append("unknown (\"detail\" item not object)");
                    continue;
                }

                if (detail.has("msg")) {
                    JsonNode msgNode = detail.get("msg");
                    if (msgNode.isTextual()) {
                        sb.append(msgNode.asText());
                    } else {
                        sb.append("unknown (").append(msgNode).append(')');
                    }
                } else {
                    sb.append("unknown (no \"msg\" field)");
                }

                if (detail.has("loc")) {
                    JsonNode locArrayNode = detail.get("loc");
                    if (locArrayNode.isArray()) {
                        sb.append('[');
                        boolean haveFirst = false;
                        for (JsonNode locNode : locArrayNode) {
                            if (haveFirst) {
                                sb.append(", ");
                            }
                            if (locNode.isTextual()) {
                                sb.append(locNode.asText());
                            } else {
                                sb.append("unknown (").append(locNode).append(')');
                            }
                            haveFirst = true;
                        }
                        sb.append(']');
                    }
                }
            }
            return sb.toString();
        } else {
            return "Unknown problem";
        }
    }

    private Result<Collection<Principal>, String> resultFromSuccessfulHttpRequest(JsonNode json) {
        if (!json.isObject()) {
            return Result.failure("lookup not JSON object");
        }

        if (!json.has("internal")) {
            return Result.failure("lookup missing \"internal\" field");
        }

        JsonNode internalNode = json.get("internal");
        if (!internalNode.isObject()) {
            return Result.failure("\"internal\" field is not object");
        }

        if (!internalNode.has("username")) {
            return Result.failure("\"internal\" field missing \"username\" field");
        }

        JsonNode usernameNode = internalNode.get("username");
        if (!usernameNode.isTextual()) {
            return Result.failure("Non-textual \"username\" field");
        }

        List<Principal> principals = new ArrayList<>(2);
        principals.add(new UserNamePrincipal(usernameNode.asText()));

        if (internalNode.has("display_name")) {
            JsonNode displayNameNode = internalNode.get("display_name");
            if (displayNameNode.isTextual()) {
                principals.add(new FullNamePrincipal(displayNameNode.asText()));
            }
        }

        return Result.success(principals);
    }
}
