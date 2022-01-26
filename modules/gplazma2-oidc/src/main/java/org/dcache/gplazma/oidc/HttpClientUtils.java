/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2022 Deutsches Elektronen-Synchrotron
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
package org.dcache.gplazma.oidc;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.MissingNode;
import com.google.common.base.Stopwatch;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import javax.annotation.Nullable;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.dcache.util.Strings;
import org.dcache.util.TimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class for HttpClient instances.
 */
public class HttpClientUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(HttpClientUtils.class);

    private HttpClientUtils() {} // Prevent instantiation: utility class

    /**
     * Issue an HTTP GET request to read a JSON document.
     * @param client The HttpClient to use
     * @param url The location of the JSON document
     * @return The parsed JSON document
     * @throws IOException if there's any problem.
     */
    public static JsonNode readJson(HttpClient client, URI url) throws IOException {
        HttpGet request = new HttpGet(url);

        Stopwatch httpTiming = Stopwatch.createStarted();
        HttpEntity entity = null;
        HttpResponse response = null;
        try {
            response = client.execute(request);
            entity = response.getEntity();
            return asJson(entity);
        } finally {
            if (LOGGER.isDebugEnabled()) {
                httpTiming.stop();

                String entityDescription = entity == null ? "a missing"
                        : entity.getContentLength() < 0 ? "an unknown sized"
                                : entity.getContentLength() == 0 ? "an empty"
                                        : Strings.describeSize(entity.getContentLength());

                LOGGER.debug("GET {} took {} returning {} entity: {}",
                        url,
                        TimeUtils.describe(httpTiming.elapsed()).orElse("(no time)"),
                        entityDescription,
                        response.getStatusLine());
            }
        }
    }

    private static JsonNode asJson(@Nullable HttpEntity response) throws IOException {
        if (response == null) {
            return MissingNode.getInstance();
        }

        ByteArrayOutputStream os = new ByteArrayOutputStream();
        response.writeTo(os);
        String responseAsJson = os.toString(UTF_8);
        var mapper = new ObjectMapper();
        return mapper.readValue(responseAsJson, JsonNode.class);
    }
}
