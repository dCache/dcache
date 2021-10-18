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

import com.google.common.base.Splitter;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.ProtocolVersion;
import org.apache.http.StatusLine;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.impl.EnglishReasonPhraseCatalog;
import org.mockito.ArgumentMatcher;
import org.mockito.BDDMockito;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;

/**
 * A fluent class for building a mock HttpClient.
 */
public class MockHttpClientBuilder {
    private final HttpClient client = mock(HttpClient.class);
    private final List<Interaction> interactions = new ArrayList<>();

    public static MockHttpClientBuilder aClient() {
        return new MockHttpClientBuilder();
    }

    public MockHttpClientBuilder() {
        try {
            BDDMockito.given(client.execute(any(HttpUriRequest.class)))
                    .willAnswer(i -> {
                        HttpUriRequest request = i.getArgument(0, HttpUriRequest.class);
                        Iterator<Interaction> itr = interactions.iterator();
                        while (itr.hasNext()) {
                            Interaction interation = itr.next();
                            if (interation.matches(request)) {
                                itr.remove();
                                return interation.getResponse();
                            }
                        }
                        throw new RuntimeException("Failed to match request " + request);
                    });
        } catch (IOException e) {
            throw new RuntimeException("Impossible exception caught.", e);
        }
    }

    public RequestMatcherBuilder onGet(String url) {
        return new RequestMatcherBuilder("GET", url);
    }

    public HttpClient build() {
        return client;
    }

    /**
     * Fluent class to build the details of which request is being described.
     */
    public class RequestMatcherBuilder implements ArgumentMatcher<HttpUriRequest> {
        private final String target;
        private final String method;

        public RequestMatcherBuilder(String method, String url) {
            this.method = requireNonNull(method);
            target = requireNonNull(url);
        }

        public ResponseBuilder responds() {
            return new ResponseBuilder(this);
        }

        @Override
        public boolean matches(HttpUriRequest argument) {
            return argument.getMethod().equals(method) && argument.getURI().toASCIIString().equals(target);
        }
    }

    /**
     * Fluent class to describe the simulated response from the endpoint.
     */
    public class ResponseBuilder {
        private final ArgumentMatcher<HttpUriRequest> matchingRequest;
        private final HttpResponse response = mock(HttpResponse.class);
        private boolean hasStatusLine;

        public ResponseBuilder(ArgumentMatcher<HttpUriRequest> matchingRequest) {
            this.matchingRequest = requireNonNull(matchingRequest);
        }

        private void mockEntity(String payload) {
            HttpEntity entity = mock(HttpEntity.class);

            BDDMockito.given(entity.getContentLength()).willReturn((long)payload.length());
            try {
                BDDMockito.willAnswer(i -> {
                              OutputStream out = i.getArgument(0, OutputStream.class);
                              out.write(payload.getBytes(UTF_8));
                              out.flush();
                              return null;
                        }).given(entity).writeTo(any(OutputStream.class));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            BDDMockito.given(response.getEntity()).willReturn(entity);
        }

        private void mockStatusLine(String protocol, int code, String reason) {
            StatusLine status = mock(StatusLine.class);

            var protocolDetails = Splitter.on('/').limit(2).splitToList(protocol);
            String protocolName = protocolDetails.get(0);
            var versionDetails = Splitter.on('.').limit(2).splitToList(protocolDetails.get(1));
            int major = Integer.parseInt(versionDetails.get(0));
            int minor = Integer.parseInt(versionDetails.get(1));

            ProtocolVersion version = new ProtocolVersion(protocolName, major, minor);
            BDDMockito.given(status.getProtocolVersion()).willReturn(version);
            BDDMockito.given(status.getReasonPhrase()).willReturn(reason);
            BDDMockito.given(status.getStatusCode()).willReturn(code);

            BDDMockito.given(response.getStatusLine()).willReturn(status);
        }

        public ResponseBuilder withStatusLine(int code, String reason) {
            mockStatusLine("HTTP/1.1", code, reason);
            hasStatusLine = true;
            return this;
        }

        public ResponseBuilder withStatusCode(int code) {
            String reason = EnglishReasonPhraseCatalog.INSTANCE.getReason(code, Locale.ENGLISH);
            mockStatusLine("HTTP/1.1", code, reason);
            hasStatusLine = true;
            return this;
        }

        private void finish() {
            if (!hasStatusLine) {
                withStatusCode(HttpStatus.SC_OK);
            }
            interactions.add(new Interaction(matchingRequest, response));
        }

        public MockHttpClientBuilder withEntity(String payload) {
            mockEntity(payload);
            finish();
            return MockHttpClientBuilder.this;
        }

        public MockHttpClientBuilder withoutEntity() {
            finish();
            return MockHttpClientBuilder.this;
        }
    }

    /**
     * A combination of a request matcher and its corresponding response.
     */
    private static class Interaction implements ArgumentMatcher<HttpUriRequest> {
        private final ArgumentMatcher<HttpUriRequest> requestMatcher;
        private final HttpResponse response;

        private Interaction(ArgumentMatcher<HttpUriRequest> requestMatcher, HttpResponse response) {
            this.requestMatcher = requireNonNull(requestMatcher);
            this.response = requireNonNull(response);
        }

        private HttpResponse getResponse() {
            return response;
        }

        @Override
        public boolean matches(HttpUriRequest argument) {
            return requestMatcher.matches(argument);
        }
    }
}
