/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2021 Deutsches Elektronen-Synchrotron
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
package org.dcache.auth;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

import org.junit.Test;

public class OidcSubjectPrincipalTest {

    @Test(expected = NullPointerException.class)
    public void shouldRejectNullSubClaim() {
        new OidcSubjectPrincipal(null, "OP");
    }

    @Test(expected = NullPointerException.class)
    public void shouldRejectNullOP() {
        new OidcSubjectPrincipal("sub-claim", null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldRejectNonASCIISubClaim() {
        new OidcSubjectPrincipal("\uD80C\uDC80", "OP");
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldRejectTooLongSubClaim() {
        new OidcSubjectPrincipal(
              "01234567890123456789012345678901234567890123456789"
                    + "01234567890123456789012345678901234567890123456789"
                    + "01234567890123456789012345678901234567890123456789"
                    + "01234567890123456789012345678901234567890123456789"
                    + "01234567890123456789012345678901234567890123456789"
                    + "123456", "OP");
    }

    @Test
    public void shouldReturnSubClaim() {
        OidcSubjectPrincipal p = new OidcSubjectPrincipal("sub-claim", "OP");

        assertThat(p.getSubClaim(), is(equalTo("sub-claim")));
    }

    @Test
    public void shouldReturnOP() {
        OidcSubjectPrincipal p = new OidcSubjectPrincipal("sub-claim", "OP");

        assertThat(p.getOP(), is(equalTo("OP")));
    }

    @Test
    public void shouldReturnReasonableName() {
        OidcSubjectPrincipal p = new OidcSubjectPrincipal("sub-claim", "OP");

        assertThat(p.getName(), is(equalTo("sub-claim@OP")));
    }

    @Test
    public void shouldBeEqualWithSameObject() {
        OidcSubjectPrincipal p = new OidcSubjectPrincipal("sub-claim", "OP");

        assertThat(p, is(equalTo(p)));
    }

    @Test
    public void shouldBeEqualWithOidcPrincipalWithSameStrings() {
        OidcSubjectPrincipal p = new OidcSubjectPrincipal("sub-claim", "OP");

        assertThat(p, is(equalTo(new OidcSubjectPrincipal("sub-claim", "OP"))));
        assertThat(p.hashCode(),
              is(equalTo(new OidcSubjectPrincipal("sub-claim", "OP").hashCode())));
    }

    @Test
    public void shouldNotBeEqualWithOidcPrincipalWithDifferentSubClaim() {
        OidcSubjectPrincipal p = new OidcSubjectPrincipal("sub-claim", "OP");

        assertThat(p, is(not(equalTo(new OidcSubjectPrincipal("different-sub-claim", "OP")))));
    }

    @Test
    public void shouldNotBeEqualWithOidcPrincipalWithDifferentOP() {
        OidcSubjectPrincipal p = new OidcSubjectPrincipal("sub-claim", "OP");

        assertThat(p, is(not(equalTo(new OidcSubjectPrincipal("sub-claim", "other-OP")))));
    }

    @Test
    public void shouldHaveExpectedToString() {
        OidcSubjectPrincipal p = new OidcSubjectPrincipal("sub-claim", "OP");

        assertThat(p.toString(), is(equalTo("OidcSubjectPrincipal[sub-claim@OP]")));
    }
}
