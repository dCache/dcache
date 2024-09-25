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

import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.junit.Test;

import static org.dcache.gplazma.oidc.MockHttpClientBuilder.aClient;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ExtractResultTest {
    private static final Profile IGNORE_ALL = (i,c) -> new ProfileResult(Collections.emptySet());
    private static final List<String> NO_SUPPRESSION = List.of();
    private final ObjectMapper mapper = new ObjectMapper();


    @Test(expected=NullPointerException.class)
    public void shouldThrowNpeOnNullIp() {
        new ExtractResult(null, Collections.emptyMap());
    }

    @Test(expected=NullPointerException.class)
    public void shouldThrowNpeOnNullMap() {
        new ExtractResult(new IdentityProvider("test", URI.create("https://example.org/"), IGNORE_ALL,
                aClient().build(), Duration.ofSeconds(2), NO_SUPPRESSION), null);
    }

    @Test
    public void shouldMatchIp() {
        var idp = new IdentityProvider("test", URI.create("https://example.org"), IGNORE_ALL,
                aClient().build(), Duration.ofSeconds(2), NO_SUPPRESSION);

        var result = new ExtractResult(idp, Collections.emptyMap());

        assertThat(result.idp(), equalTo(idp));
    }

    @Test
    public void shouldMatchClaims() throws Exception {
        var result = new ExtractResult(new IdentityProvider("test", URI.create("https://example.org"),
                IGNORE_ALL, aClient().build(), Duration.ofSeconds(2), NO_SUPPRESSION),
                Map.of("sub", mapper.readTree("\"abcdefg012345\"")));

        assertThat(result.claims(), aMapWithSize(1));
        assertThat(result.claims(), hasEntry("sub", mapper.readTree("\"abcdefg012345\"")));
    }

    @Test
    public void twoSameResultsShouldBeEqual() throws Exception {
        var result1 = new ExtractResult(new IdentityProvider("test", URI.create("https://example.org"),
                IGNORE_ALL, aClient().build(), Duration.ofSeconds(2), NO_SUPPRESSION),
                Map.of("sub", mapper.readTree("\"abcdefg012345\"")));

        var result2 = new ExtractResult(new IdentityProvider("test", URI.create("https://example.org"),
                IGNORE_ALL, aClient().build(), Duration.ofSeconds(2), NO_SUPPRESSION),
                Map.of("sub", mapper.readTree("\"abcdefg012345\"")));

        assertTrue(result1.equals(result2));
    }

    @Test
    public void twoResultsWithDifferentIpShouldNotBeEqual() throws Exception {
        var result1 = new ExtractResult(new IdentityProvider("test", URI.create("https://example.org"),
                IGNORE_ALL, aClient().build(), Duration.ofSeconds(2), NO_SUPPRESSION),
                Map.of("sub", mapper.readTree("\"abcdefg012345\"")));

        var result2 = new ExtractResult(new IdentityProvider("test2", URI.create("https://example.org"),
                IGNORE_ALL, aClient().build(), Duration.ofSeconds(2), NO_SUPPRESSION),
                Map.of("sub", mapper.readTree("\"abcdefg012345\"")));

        assertFalse(result1.equals(result2));
    }

    @Test
    public void twoResultsWithDifferentClaimsShouldNotBeEqual() throws Exception {
        var result1 = new ExtractResult(new IdentityProvider("test", URI.create("https://example.org"),
                IGNORE_ALL, aClient().build(), Duration.ofSeconds(2), NO_SUPPRESSION),
                Map.of("sub", mapper.readTree("\"abcdefg012345\"")));

        var result2 = new ExtractResult(new IdentityProvider("test2", URI.create("https://example.org"),
                IGNORE_ALL, aClient().build(), Duration.ofSeconds(2), NO_SUPPRESSION),
                Map.of("sub", mapper.readTree("\"some-other-value\"")));

        assertFalse(result1.equals(result2));
    }

    @Test
    public void twoSameResultsShouldHaveSameHash() throws Exception {
        var result1 = new ExtractResult(new IdentityProvider("test", URI.create("https://example.org"),
                IGNORE_ALL, aClient().build(), Duration.ofSeconds(2), NO_SUPPRESSION),
                Map.of("sub", mapper.readTree("\"abcdefg012345\"")));

        var result2 = new ExtractResult(new IdentityProvider("test", URI.create("https://example.org"),
                IGNORE_ALL, aClient().build(), Duration.ofSeconds(2), NO_SUPPRESSION),
                Map.of("sub", mapper.readTree("\"abcdefg012345\"")));

        assertThat(result2.hashCode(), equalTo(result1.hashCode()));
    }

    @Test
    public void shouldHaveElementsInToString() throws Exception {
        var description = new ExtractResult(new IdentityProvider("test", URI.create("https://example.org"),
                IGNORE_ALL, aClient().build(), Duration.ofSeconds(2), NO_SUPPRESSION),
                Map.of("sub", mapper.readTree("\"abcdefg012345\"")))
                .toString();

        assertThat(description, containsString("test"));
        assertThat(description, containsString("https://example.org"));
        assertThat(description, containsString("sub"));
        assertThat(description, containsString("abcdefg012345"));
    }
}