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

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpHeaders;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.security.Principal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.dcache.auth.FullNamePrincipal;
import org.dcache.auth.UserNamePrincipal;
import org.dcache.util.Result;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import static com.github.npathai.hamcrestopt.OptionalMatchers.*;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class AliseLookupAgentTest {

    private HttpClient client;
    private AliseLookupAgent agent;
    private Identity identity;
    private HttpRequest request;
    private Result<Collection<Principal>, String> result;

    @Before
    public void setup() {
        client = null;
        agent = null;
        identity = null;
        request = null;
        result = null;
    }

    @Test
    public void testGoodResponseNoDisplayName() throws Exception {
        given(anHttpClient().thatRespondsWith(aResponse()
                .withHeaders(someHeaders().withHeader("Content-Type", "application/json"))
                .withBody("{\"internal\": {\"username\": \"pmillar\"}}")
                .withStatusCode(200)));
        given(anAliseLookupAgent()
                .withApiKey("APIKEY")
                .withEndpoint("https://alise.example.org/")
                .withTarget("vega-kc"));
        given(anIdentity().withSub("paul").withIssuer("https://issuer.example.org/"));

        whenAgentCalledWithIdentity();

        var expected = URI.create("https://alise.example.org/api/v1"
                + "/target/vega-kc"
                + "/mapping/issuer/95d86dadf13ccfbd25f13d5d15edb31b4b30c971"
                + "/user/paul?apikey=APIKEY");
        assertThat(request.uri(), is(equalTo(expected)));
        assertThat(request.method(), is(equalTo("GET")));

        assertThat(result.getSuccess(), isPresentAnd(contains(
                new UserNamePrincipal("pmillar"))));
    }

    @Test
    public void testGoodResponseWithDisplayName() throws Exception {
        given(anHttpClient().thatRespondsWith(aResponse()
                .withHeaders(someHeaders().withHeader("Content-Type", "application/json"))
                .withBody("{\"internal\": {\"username\": \"pmillar\", \"display_name\": \"Paul Millar\"}}")
                .withStatusCode(200)));
        given(anAliseLookupAgent()
                .withApiKey("APIKEY")
                .withEndpoint("https://alise.example.org/")
                .withTarget("vega-kc"));
        given(anIdentity().withSub("paul").withIssuer("https://issuer.example.org/"));

        whenAgentCalledWithIdentity();

        var expected = URI.create("https://alise.example.org/api/v1"
                + "/target/vega-kc"
                + "/mapping/issuer/95d86dadf13ccfbd25f13d5d15edb31b4b30c971"
                + "/user/paul?apikey=APIKEY");
        assertThat(request.uri(), is(equalTo(expected)));
        assertThat(request.method(), is(equalTo("GET")));

        assertThat(result.getSuccess(), isPresentAnd(containsInAnyOrder(
                new UserNamePrincipal("pmillar"),
                new FullNamePrincipal("Paul Millar"))));
    }

    @Test
    public void testMalformedResponse() throws Exception {
        given(anHttpClient().thatRespondsWith(aResponse()
                .withHeaders(someHeaders().withHeader("Content-Type", "application/json"))
                .withBody("{}")
                .withStatusCode(200)));
        given(anAliseLookupAgent()
                .withApiKey("APIKEY")
                .withEndpoint("https://alise.example.org/")
                .withTarget("vega-kc"));
        given(anIdentity().withSub("paul").withIssuer("https://issuer.example.org/"));

        whenAgentCalledWithIdentity();

        var expected = URI.create("https://alise.example.org/api/v1"
               + "/target/vega-kc"
               + "/mapping/issuer/95d86dadf13ccfbd25f13d5d15edb31b4b30c971"
               + "/user/paul?apikey=APIKEY");
        assertThat(request.uri(), is(equalTo(expected)));
        assertThat(request.method(), is(equalTo("GET")));

        assertThat(result.getFailure(), isPresentAnd(containsString("internal")));
    }

    @Test
    public void testMessageErrorResponse() throws Exception {
        given(anHttpClient().thatRespondsWith(aResponse()
                .withHeaders(someHeaders().withHeader("Content-Type", "application/json"))
                .withBody("{\"message\": \"FNORD\"}")
                .withStatusCode(400)));
        given(anAliseLookupAgent()
                .withApiKey("APIKEY")
                .withEndpoint("https://alise.example.org/")
                .withTarget("vega-kc"));
        given(anIdentity().withSub("paul").withIssuer("https://issuer.example.org/"));

        whenAgentCalledWithIdentity();

        var expected = URI.create("https://alise.example.org/api/v1"
               + "/target/vega-kc"
               + "/mapping/issuer/95d86dadf13ccfbd25f13d5d15edb31b4b30c971"
               + "/user/paul?apikey=APIKEY");
        assertThat(request.uri(), is(equalTo(expected)));
        assertThat(request.method(), is(equalTo("GET")));

        assertThat(result.getFailure(), isPresentAnd(containsString("FNORD")));
    }

    @Test
    public void testDetailErrorResponse() throws Exception {
        given(anHttpClient().thatRespondsWith(aResponse()
                .withHeaders(someHeaders().withHeader("Content-Type", "application/json"))
                .withBody("{\"detail\": [{\"msg\": \"FOO\", \"loc\": [\"item-1\", \"item-2\"]}]}")
                .withStatusCode(400)));
        given(anAliseLookupAgent()
                .withApiKey("APIKEY")
                .withEndpoint("https://alise.example.org/")
                .withTarget("vega-kc"));
        given(anIdentity().withSub("paul").withIssuer("https://issuer.example.org/"));

        whenAgentCalledWithIdentity();

        var expected = URI.create("https://alise.example.org/api/v1"
               + "/target/vega-kc"
               + "/mapping/issuer/95d86dadf13ccfbd25f13d5d15edb31b4b30c971"
               + "/user/paul?apikey=APIKEY");
        assertThat(request.uri(), is(equalTo(expected)));
        assertThat(request.method(), is(equalTo("GET")));

        assertTrue(result.isFailure());
        String failureMessage = result.getFailure().get();
        assertThat(failureMessage, containsString("FOO"));
        assertThat(failureMessage, containsString("item-1"));
        assertThat(failureMessage, containsString("item-2"));
    }

    private void whenAgentCalledWithIdentity() {
        checkState(identity != null);
        result = agent.lookup(identity);

        ArgumentCaptor<HttpRequest> requestCaptor = ArgumentCaptor.forClass(HttpRequest.class);
        try {
            verify(client).send(requestCaptor.capture(), any());
        } catch (InterruptedException | IOException e) {
            throw new RuntimeException("verify unexpectedly throw an exception", e);
        }

        request = requestCaptor.getValue();
    }

    public AliseLookupAgentBuilder anAliseLookupAgent() {
        return new AliseLookupAgentBuilder();
    }

    public HttpClientBuilder anHttpClient() {
        return new HttpClientBuilder();
    }

    public HttpResponseBuilder aResponse() {
        return new HttpResponseBuilder();
    }

    public HttpHeadersBuilder someHeaders() {
        return new HttpHeadersBuilder();
    }

    public IdentityBuilder anIdentity() {
        return new IdentityBuilder();
    }

    public void given(HttpClientBuilder builder) {
        client = builder.build();
    }

    public void given(AliseLookupAgentBuilder builder) {
        agent = builder.build();
    }

    public void given(IdentityBuilder builder) {
        identity = builder.build();
    }

    public static class HttpClientBuilder {
        private final HttpClient client = mock(HttpClient.class);
        private HttpResponse response;

        public HttpClientBuilder thatRespondsWith(HttpResponseBuilder builder) {
            checkState(response==null);
            response = builder.build();
            return this;
        }

        public HttpClient build() {
            try {
                when(client.send(any(), any())).thenReturn(response);
            } catch (InterruptedException | IOException e) {
                throw new RuntimeException("Mocking generated exception", e);
            }
            return client;
        }
    }

    public static class HttpResponseBuilder {
        private final HttpResponse<String> response = mock(HttpResponse.class);
        private int statusCode;
        private HttpHeaders headers;
        private String body;

        public HttpResponseBuilder withHeaders(HttpHeadersBuilder builder) {
            checkState(headers==null);
            headers = builder.build();
            return this;
        }

        public HttpResponseBuilder withStatusCode(int code) {
            checkArgument(code > 0);
            checkState(statusCode == 0);
            statusCode = code;
            return this;
        }

        public HttpResponseBuilder withBody(String value) {
            checkState(body == null);
            body = requireNonNull(value);
            return this;
        }

        public HttpResponse<String> build() {
            if (headers != null) {
                when(response.headers()).thenReturn(headers);
            }
            if (statusCode != 0) {
                when(response.statusCode()).thenReturn(statusCode);
            }
            if (body != null) {
                when(response.body()).thenReturn(body);
            }
            return response;
        }
    }

    public static class HttpHeadersBuilder {
        private final Map<String,List<String>> headers = new HashMap<>();

        public HttpHeadersBuilder withHeader(String key, String value) {
            headers.computeIfAbsent(key, k -> new ArrayList<String>())
                    .add(value);
            return this;
        }

        public HttpHeaders build() {
            return HttpHeaders.of(headers, (k,v) -> true);
        }
    }

    public class AliseLookupAgentBuilder {
        private String apikey;
        private String timeout = "PT1M";
        private URI endpoint;
        private String target;

        public AliseLookupAgentBuilder withApiKey(String key) {
            apikey = requireNonNull(key);
            return this;
        }

        public AliseLookupAgentBuilder withTimeout(String timeout) {
            this.timeout = requireNonNull(timeout);
            return this;
        }

        public AliseLookupAgentBuilder withEndpoint(String endpoint) {
            this.endpoint = URI.create(requireNonNull(endpoint));
            return this;
        }

        public AliseLookupAgentBuilder withTarget(String target) {
            this.target = requireNonNull(target);
            return this;
        }

        public AliseLookupAgent build() {
            return new AliseLookupAgent(requireNonNull(client), endpoint,
                    target, requireNonNull(apikey), timeout);
        }
    }

    public static class IdentityBuilder {
        private String subject;
        private URI issuer;

        public IdentityBuilder withIssuer(String issuer) {
            this.issuer = URI.create(requireNonNull(issuer));
            return this;
        }

        public IdentityBuilder withSub(String sub) {
            subject = requireNonNull(sub);
            return this;
        }

        public Identity build() {
            return new Identity(requireNonNull(issuer), requireNonNull(subject));
        }
    }
}
