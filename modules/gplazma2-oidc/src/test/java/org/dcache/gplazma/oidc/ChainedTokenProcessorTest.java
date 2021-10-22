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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.HashMap;
import java.util.Map;
import org.dcache.gplazma.AuthenticationException;
import org.junit.Test;
import org.mockito.Mockito;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.mockito.Mockito.verify;

public class ChainedTokenProcessorTest {

    private final ObjectMapper mapper = new ObjectMapper();

    private class ResultBuilder {
        private final Map<String,JsonNode> claims = new HashMap<>();
        private IdentityProvider provider;

        public ResultBuilder withIP(String name) {
            provider = new IdentityProvider(name, "https://" + name + ".example.org/");
            return this;
        }

        public ResultBuilder withClaim(String key, String value) throws JsonProcessingException {
            JsonNode json = mapper.readTree(value);
            claims.put(key, json);
            return this;
        }

        ExtractResult build() {
            return new ExtractResult(provider, claims);
        }
    }

    @Test
    public void shouldCallShutdownOnAllProcessors() throws Exception {
        var processor1 = Mockito.mock(TokenProcessor.class);
        var processor2 = Mockito.mock(TokenProcessor.class);
        var combined = ChainedTokenProcessor.tryWith(processor1).andThenTryWith(processor2);

        combined.shutdown();

        verify(processor1).shutdown();
        verify(processor2).shutdown();
    }

    @Test
    public void shouldReturnResultFromSingleSuccessfulTokenProcessor() throws Exception {
        var expectedResult = given(aResult().withIP("foo").withClaim("sub", "\"0123456789\""));

        var actualResult = ChainedTokenProcessor.tryWith(t -> expectedResult).extract("a-token");

        assertThat(actualResult, equalTo(expectedResult));
    }

    @Test(expected=UnableToProcess.class)
    public void shouldReturnUnprocessedFromUnprocessedResult() throws Exception {
        ChainedTokenProcessor
                .tryWith(t -> {throw new UnableToProcess("Token in wrong format");})
                .extract("a-token");
    }

    @Test(expected=AuthenticationException.class)
    public void shouldThrowAuthenticationExceptionIfFirstThrowsException() throws Exception {
        ChainedTokenProcessor
                .tryWith(t -> {throw new AuthenticationException("Token has expired");})
                .extract("a-token");
    }

    @Test
    public void shouldReturnResultFromFirstTokenProcessor() throws Exception {
        var expectedResult = given(aResult().withIP("foo").withClaim("sub", "\"0123456789\""));

        var actualResult = ChainedTokenProcessor
                .tryWith(t -> expectedResult)
                .andThenTryWith(t -> {throw new AssertionError("Should not get here");})
                .extract("a-token");

        assertThat(actualResult, equalTo(expectedResult));
    }

    @Test
    public void shouldReturnResultFromSecondTokenProcessor() throws Exception {
        var expectedResult = given(aResult().withIP("foo").withClaim("sub", "\"0123456789\""));

        var actualResult = ChainedTokenProcessor
                .tryWith(t -> {throw new UnableToProcess("Token in wrong format");})
                .andThenTryWith(t -> expectedResult)
                .extract("a-token");

        assertThat(actualResult, equalTo(expectedResult));
    }

    @Test(expected=AuthenticationException.class)
    public void shouldThrowExceptionFromSecondTokenProcessor() throws Exception {
        ChainedTokenProcessor
                .tryWith(t -> {throw new UnableToProcess("Token in wrong format");})
                .andThenTryWith(t -> {throw new AuthenticationException();})
                .extract("a-token");
    }

    @Test(expected=UnableToProcess.class)
    public void shouldReturnUnprocessedWhenAllUnprocessed() throws Exception {
        ChainedTokenProcessor
                .tryWith(t -> {throw new UnableToProcess("Token in wrong format");})
                .andThenTryWith(t -> {throw new UnableToProcess("Unable to access user-info endpoint");})
                .extract("a-token");
    }

    private ExtractResult given(ResultBuilder builder) {
        return builder.build();
    }

    private ResultBuilder aResult() {
        return new ResultBuilder();
    }
}
