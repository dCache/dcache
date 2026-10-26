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
package org.dcache.gplazma.scitoken;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.time.Duration;
import java.util.Optional;
import org.apache.http.HttpResponse;
import org.apache.http.HttpVersion;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.entity.StringEntity;
import org.apache.http.message.BasicHttpResponse;
import org.junit.Before;
import org.junit.Test;
import tools.jackson.databind.JsonNode;

public class HttpJsonDocumentTest {

    private static final String URL = "https://example.org/.well-known/openid-configuration";

    private HttpClient client;

    @Before
    public void setup() {
        client = mock(HttpClient.class);
    }

    @Test
    public void shouldSupplyParsedDocument() throws Exception {
        givenResponse("{\"jwks_uri\": \"https://example.org/jwks\"}");
        var document = new HttpJsonDocument(client, URL, Duration.ofHours(1), Duration.ZERO);

        JsonNode node = document.get();

        assertThat(node.isObject(), is(true));
        assertThat(node.get("jwks_uri").stringValue(), is(equalTo("https://example.org/jwks")));
    }

    @Test
    public void shouldSupplyMissingNodeOnMalformedDocument() throws Exception {
        givenResponse("{\"jwks_uri\": ");
        var document = new HttpJsonDocument(client, URL, Duration.ofHours(1), Duration.ZERO);

        assertThat(document.get().isMissingNode(), is(true));
    }

    @Test
    public void shouldSupplyMissingNodeOnNetworkError() throws Exception {
        when(client.execute(any(HttpUriRequest.class))).thenThrow(new IOException("down"));
        var document = new HttpJsonDocument(client, URL, Duration.ofHours(1), Duration.ZERO);

        assertThat(document.get().isMissingNode(), is(true));
    }

    @Test
    public void shouldSupplyMissingNodeWithoutRequestIfNoUrl() throws Exception {
        var document = new HttpJsonDocument(client, Optional::empty);

        assertThat(document.get().isMissingNode(), is(true));
        verify(client, never()).execute(any(HttpUriRequest.class));
    }

    @Test
    public void shouldNotRefetchCachedDocument() throws Exception {
        givenResponse("{}");
        var document = new HttpJsonDocument(client, URL, Duration.ofHours(1), Duration.ZERO);

        document.get();
        document.get();

        verify(client, times(1)).execute(any(HttpUriRequest.class));
    }

    @Test
    public void shouldNotRefetchMissingDocumentWithinCacheMissPeriod() throws Exception {
        givenResponse("not json");
        var document = new HttpJsonDocument(client, URL, Duration.ZERO, Duration.ofHours(1));

        document.get();
        document.get();

        verify(client, times(1)).execute(any(HttpUriRequest.class));
    }

    @Test
    public void shouldRefetchMissingDocumentAfterCacheMissPeriod() throws Exception {
        givenResponse("not json");
        var document = new HttpJsonDocument(client, URL, Duration.ofHours(1), Duration.ZERO);

        document.get();
        Thread.sleep(5);
        document.get();

        verify(client, times(2)).execute(any(HttpUriRequest.class));
    }

    private void givenResponse(String body) throws IOException {
        // a fresh response for each request, as the entity is consumed when read.
        when(client.execute(any(HttpUriRequest.class))).thenAnswer(i -> {
            HttpResponse response = new BasicHttpResponse(HttpVersion.HTTP_1_1, 200, "OK");
            response.setHeader("Content-Type", "application/json");
            response.setEntity(new StringEntity(body));
            return response;
        });
    }
}
