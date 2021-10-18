/*
 * dCache - http://www.dcache.org/
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
package org.dcache.gplazma.oidc.userinfo;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Collections;
import java.util.Map;
import org.dcache.gplazma.oidc.IdentityProvider;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

public class LookupResultTest {

    private static final IdentityProvider EXAMPLE_IP = new IdentityProvider("example-op", "https://example.org/");

    @Test(expected=NullPointerException.class)
    public void shouldThrowNpeOnErrorWithNullIp() {
        LookupResult.error(null, "a valid error message");
    }

    @Test(expected=NullPointerException.class)
    public void shouldThrowNpeOnErrorWithNullMessage() {
        LookupResult.error(EXAMPLE_IP, null);
    }

    @Test
    public void shouldReturnIpOnError() {
        var result = LookupResult.error(EXAMPLE_IP, "a valid error message");

        assertThat(result.getIdentityProvider(), is(equalTo(EXAMPLE_IP)));
    }

    @Test
    public void shouldReturnMessageOnError() {
        var result = LookupResult.error(EXAMPLE_IP, "a valid error message");

        assertThat(result.getError(), is(equalTo("a valid error message")));
    }

    @Test
    public void shouldReturnFalseOnIsSuccessForError() {
        var result = LookupResult.error(EXAMPLE_IP, "a valid error message");

        assertThat(result.isSuccess(), is(equalTo(false)));
    }

    @Test
    public void shouldReturnEmptyClaimsForError() {
        var result = LookupResult.error(EXAMPLE_IP, "a valid error message");

        assertThat(result.getClaims(), is(aMapWithSize(0)));
    }

    @Test(expected=NullPointerException.class)
    public void shouldThrowNpeOnSuccessWithNullIp() {
        LookupResult.success(null, Collections.emptyMap());
    }

    @Test(expected=NullPointerException.class)
    public void shouldThrowNpeOnSuccessWithNullClaims() {
        LookupResult.success(EXAMPLE_IP, null);
    }

    @Test
    public void shouldReturnTrueOnIsSuccessForSuccess() {
        var result = LookupResult.success(EXAMPLE_IP, Collections.emptyMap());

        assertThat(result.isSuccess(), is(equalTo(true)));
    }

    @Test
    public void shouldReturnNullAsErrorForSuccess() {
        var result = LookupResult.success(EXAMPLE_IP, Collections.emptyMap());

        assertThat(result.getError(), is(nullValue()));
    }

    @Test
    public void shouldReturnIpOnSuccess() {
        var result = LookupResult.success(EXAMPLE_IP, Collections.emptyMap());

        assertThat(result.getIdentityProvider(), is(equalTo(EXAMPLE_IP)));
    }

    @Test
    public void shouldReturnEmptyMapOnSuccessWithEmptyMap() {
        var result = LookupResult.success(EXAMPLE_IP, Collections.emptyMap());

        assertThat(result.getClaims(), is(aMapWithSize(0)));
    }

    @Test
    public void shouldReturnMapOnSuccessWithNonemptyMap() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        Map<String,JsonNode> claims = Map.of("sub", mapper.readTree("\"abcdefg012345\""));

        var result = LookupResult.success(EXAMPLE_IP, claims);

        assertThat(result.getClaims(), is(aMapWithSize(1)));
        assertThat(result.getClaims(), hasEntry("sub", mapper.readTree("\"abcdefg012345\"")));
    }
}