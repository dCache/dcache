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
package org.dcache.gplazma.oidc.jwt;

import com.fasterxml.jackson.core.JsonParser.NumberType;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import java.time.Duration;
import java.util.Optional;
import java.util.function.Supplier;
import org.apache.http.client.HttpClient;
import org.dcache.gplazma.oidc.MockHttpClientBuilder;
import org.junit.Before;
import org.junit.Test;

import static java.time.temporal.ChronoUnit.MINUTES;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.dcache.gplazma.oidc.MockHttpClientBuilder.aClient;
import static org.hamcrest.Matchers.is;

public class HttpJsonNodeTest {
    private HttpClient client;
    private HttpJsonNode jsonNode;

    @Before
    public void setup() {
        client = null;
        jsonNode = null;
    }

    @Test
    public void shouldAcceptNullResponse() throws Exception {
        given(aClient().onGet("https://example.org/").responds().withEntity("null"));
        given(aNode().targeting("https://example.org/").cachingFor(Duration.of(1, MINUTES)));

        jsonNode.toString();

        assertThat(jsonNode.getNodeType(), equalTo(JsonNodeType.NULL));
        verify(client).execute(any());
    }

    @Test
    public void shouldAcceptStringResponse() throws Exception {
        given(aClient().onGet("https://example.org/").responds().withEntity("\"Hello, world\""));
        given(aNode().targeting("https://example.org/").cachingFor(Duration.of(1, MINUTES)));

        jsonNode.toString();

        verify(client).execute(any());
        assertThat(jsonNode.getNodeType(), equalTo(JsonNodeType.STRING));
        assertThat(jsonNode.asText(), is(equalTo("Hello, world")));
    }

    @Test
    public void shouldAcceptNumberResponse() throws Exception {
        given(aClient().onGet("https://example.org/").responds().withEntity("42"));
        given(aNode().targeting("https://example.org/").cachingFor(Duration.of(1, MINUTES)));

        jsonNode.toString();

        verify(client).execute(any());
        assertThat(jsonNode.getNodeType(), equalTo(JsonNodeType.NUMBER));
        assertThat(jsonNode.numberType(), is(equalTo(NumberType.INT)));
        assertThat(jsonNode.asInt(), is(equalTo(42)));
    }

    @Test
    public void shouldAcceptArrayOfStringResponse() throws Exception {
        given(aClient().onGet("https://example.org/").responds().withEntity("[\"foo\", \"bar\", \"baz\"]"));
        given(aNode().targeting("https://example.org/").cachingFor(Duration.of(1, MINUTES)));

        jsonNode.toString();

        verify(client).execute(any());
        assertThat(jsonNode.getNodeType(), equalTo(JsonNodeType.ARRAY));
        assertThat(jsonNode.size(), equalTo(3));
        assertThat(jsonNode.get(0).isTextual(), equalTo(true));
        assertThat(jsonNode.get(0).textValue(), equalTo("foo"));
        assertThat(jsonNode.get(1).isTextual(), equalTo(true));
        assertThat(jsonNode.get(1).textValue(), equalTo("bar"));
        assertThat(jsonNode.get(2).isTextual(), equalTo(true));
        assertThat(jsonNode.get(2).textValue(), equalTo("baz"));
    }

    @Test
    public void shouldAcceptJsonObjectResponse() throws Exception {
        given(aClient().onGet("https://example.org/").responds().withEntity("{\"string-item\": \"foo\", "
                + "\"boolean-item\": true, "
                + "\"number-item\": 42, "
                + "\"null-item\": null, "
                + "\"array-item\": [\"first\", \"second\"]}"));
        given(aNode().targeting("https://example.org/").cachingFor(Duration.of(1, MINUTES)));

        jsonNode.toString();

        verify(client).execute(any());
        assertThat(jsonNode.getNodeType(), equalTo(JsonNodeType.OBJECT));
        assertThat(jsonNode.size(), equalTo(5));
        assertThat(jsonNode.get("string-item").isTextual(), equalTo(true));
        assertThat(jsonNode.get("string-item").textValue(), equalTo("foo"));
        assertThat(jsonNode.get("boolean-item").isBoolean(), equalTo(true));
        assertThat(jsonNode.get("boolean-item").asBoolean(), equalTo(true));
        assertThat(jsonNode.get("number-item").isNumber(), equalTo(true));
        assertThat(jsonNode.get("number-item").asInt(), equalTo(42));
        assertThat(jsonNode.get("null-item").isNull(), equalTo(true));
        assertThat(jsonNode.get("array-item").isArray(), equalTo(true));
        assertThat(jsonNode.get("array-item").size(), equalTo(2));
        assertThat(jsonNode.get("array-item").get(0).isTextual(), equalTo(true));
        assertThat(jsonNode.get("array-item").get(0).textValue(), equalTo("first"));
        assertThat(jsonNode.get("array-item").get(1).isTextual(), equalTo(true));
        assertThat(jsonNode.get("array-item").get(1).textValue(), equalTo("second"));
    }

    @Test
    public void shouldRejectBadJson() throws Exception {
        given(aClient().onGet("https://example.org/").responds().withEntity("{bad json}"));
        given(aNode().targeting("https://example.org/").cachingFor(Duration.of(1, MINUTES)));

        jsonNode.toString();

        verify(client).execute(any());
        assertThat(jsonNode.getNodeType(), equalTo(JsonNodeType.MISSING));
    }

    private void given(HttpJsonNodeBuilder builder) {
        jsonNode = builder.build();
    }

    private void given(MockHttpClientBuilder builder) {
        client = builder.build();
    }

    private HttpJsonNodeBuilder aNode() {
        return new HttpJsonNodeBuilder();
    }


    /**
     * Class to provide an HttpJsonNode using a fluent interface.
     */
    private class HttpJsonNodeBuilder {
        private Supplier<Optional<String>> urlSupplier;
        private Duration cacheHit;
        private Duration cacheMiss;

        public HttpJsonNodeBuilder targeting(String url) {
            urlSupplier = () -> Optional.of(url);
            return this;
        }

        public HttpJsonNodeBuilder cachingFor(Duration duration) {
            cacheHit = duration;
            cacheMiss = duration;
            return this;
        }

        public HttpJsonNode build() {
            return new HttpJsonNode(client, urlSupplier, cacheHit, cacheMiss);
        }
    }
}