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
package org.dcache.gplazma.omnisession;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.emptyString;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.security.Principal;
import java.util.function.Predicate;
import javax.security.auth.kerberos.KerberosPrincipal;
import org.dcache.auth.EmailAddressPrincipal;
import org.dcache.auth.EntitlementPrincipal;
import org.dcache.auth.FQANPrincipal;
import org.dcache.auth.GidPrincipal;
import org.dcache.auth.GroupNamePrincipal;
import org.dcache.auth.LoginNamePrincipal;
import org.dcache.auth.OidcSubjectPrincipal;
import org.dcache.auth.OpenIdGroupPrincipal;
import org.dcache.auth.UidPrincipal;
import org.dcache.auth.UserNamePrincipal;
import org.dcache.gplazma.omnisession.PrincipalPredicates.PredicateParserException;
import org.globus.gsi.gssapi.jaas.GlobusPrincipal;
import org.junit.Test;

public class PrincipalPredicatesTest {

    Predicate<Principal> predicate;
    String remaining;

    @Test(expected = PredicateParserException.class)
    public void shouldFailEmptyString() throws Exception {
        PrincipalPredicates.parseFirstPredicate("");
    }

    @Test(expected = PredicateParserException.class)
    public void shouldFailSpaceString() throws Exception {
        PrincipalPredicates.parseFirstPredicate(" ");
    }

    @Test(expected = PredicateParserException.class)
    public void shouldFailTwoSpacesString() throws Exception {
        PrincipalPredicates.parseFirstPredicate("  ");
    }

    @Test(expected = PredicateParserException.class)
    public void shouldFailMissingColon() throws Exception {
        PrincipalPredicates.parseFirstPredicate("dn");
    }

    @Test(expected = PredicateParserException.class)
    public void shouldFailMissingType() throws Exception {
        PrincipalPredicates.parseFirstPredicate(":foo");
    }

    @Test(expected = PredicateParserException.class)
    public void shouldFailMissingValue() throws Exception {
        PrincipalPredicates.parseFirstPredicate("dn:");
    }

    @Test
    public void shouldParseValidUsername() throws Exception {
        givenPredicate("username:paul");

        assertThat(remaining, emptyString());

        assertTrue(predicate.test(new UserNamePrincipal("paul")));
        assertFalse(predicate.test(new LoginNamePrincipal("paul")));
        assertFalse(predicate.test(new UserNamePrincipal("paul.millar")));
        assertFalse(predicate.test(new UserNamePrincipal("tigran")));
    }

    @Test(expected = PredicateParserException.class)
    public void shouldRejectInvalidDn() throws Exception {
        givenPredicate("dn:INVALID");
    }

    @Test
    public void shouldParseValidDn() throws Exception {
        givenPredicate("dn:\"/C=DE/O=GermanGrid/OU=DESY/CN=Alexander Paul Millar\"");

        assertThat(remaining, emptyString());

        assertTrue(predicate.test(
              new GlobusPrincipal("/C=DE/O=GermanGrid/OU=DESY/CN=Alexander Paul Millar")));
        assertFalse(predicate.test(new UserNamePrincipal("Alexander Paul Millar")));
        assertFalse(predicate.test(new GlobusPrincipal(
              "/C=DE/O=GermanGrid/OU=DESY/CN=Robot - grid client - Paul Millar (client software tester)")));
    }

    @Test(expected = PredicateParserException.class)
    public void shouldRejectInvalidEmail() throws Exception {
        givenPredicate("email:INVALID");
    }

    @Test
    public void shouldParseValidEmail() throws Exception {
        givenPredicate("email:paul@example.org");

        assertThat(remaining, emptyString());

        assertTrue(predicate.test(new EmailAddressPrincipal("paul@example.org")));
        assertFalse(predicate.test(new UserNamePrincipal("paul@example.org")));
        assertFalse(predicate.test(new EmailAddressPrincipal("tigran@example.org")));
    }

    @Test(expected = PredicateParserException.class)
    public void shouldRejectInvalidGid() throws Exception {
        givenPredicate("gid:INVALID");
    }

    @Test(expected = PredicateParserException.class)
    public void shouldRejectInvalidQualifiedGid() throws Exception {
        givenPredicate("gid:1000,FOO");
    }

    @Test
    public void shouldParseValidGid() throws Exception {
        givenPredicate("gid:1000");

        assertThat(remaining, emptyString());

        assertTrue(predicate.test(new GidPrincipal(1000, false)));
        assertTrue(predicate.test(new GidPrincipal(1000, true)));
        assertFalse(predicate.test(new UidPrincipal(1000)));
        assertFalse(predicate.test(new GidPrincipal(2000, false)));
        assertFalse(predicate.test(new GidPrincipal(2000, true)));
    }

    @Test
    public void shouldParseValidQualifiedPrimaryGid() throws Exception {
        givenPredicate("gid:1000,primary");

        assertThat(remaining, emptyString());

        assertTrue(predicate.test(new GidPrincipal(1000, true)));
        assertFalse(predicate.test(new GidPrincipal(1000, false)));
        assertFalse(predicate.test(new UidPrincipal(1000)));
        assertFalse(predicate.test(new GidPrincipal(2000, false)));
        assertFalse(predicate.test(new GidPrincipal(2000, true)));
    }

    @Test
    public void shouldParseValidQualifiedNonprimaryGid() throws Exception {
        givenPredicate("gid:1000,nonprimary");

        assertThat(remaining, emptyString());

        assertTrue(predicate.test(new GidPrincipal(1000, false)));
        assertFalse(predicate.test(new GidPrincipal(1000, true)));
        assertFalse(predicate.test(new UidPrincipal(1000)));
        assertFalse(predicate.test(new GidPrincipal(2000, false)));
        assertFalse(predicate.test(new GidPrincipal(2000, true)));
    }

    @Test
    public void shouldParseGroupName() throws Exception {
        givenPredicate("group:hackers");

        assertThat(remaining, emptyString());

        assertTrue(predicate.test(new GroupNamePrincipal("hackers", false)));
        assertTrue(predicate.test(new GroupNamePrincipal("hackers", true)));
        assertFalse(predicate.test(new UserNamePrincipal("hackers")));
        assertFalse(predicate.test(new GroupNamePrincipal("normal", false)));
        assertFalse(predicate.test(new GroupNamePrincipal("normal", true)));
    }

    @Test(expected = PredicateParserException.class)
    public void shouldRejectInvalidQualifiedGroupName() throws Exception {
        givenPredicate("group:hackers,FOO");
    }

    @Test
    public void shouldParseQualifiedPrimaryGroupName() throws Exception {
        givenPredicate("group:hackers,primary");

        assertThat(remaining, emptyString());

        assertTrue(predicate.test(new GroupNamePrincipal("hackers", true)));
        assertFalse(predicate.test(new GroupNamePrincipal("hackers", false)));
        assertFalse(predicate.test(new UserNamePrincipal("hackers")));
        assertFalse(predicate.test(new GroupNamePrincipal("normal", false)));
        assertFalse(predicate.test(new GroupNamePrincipal("normal", true)));
    }

    @Test
    public void shouldParseQualifiedNonprimaryGroupName() throws Exception {
        givenPredicate("group:hackers,nonprimary");

        assertThat(remaining, emptyString());

        assertTrue(predicate.test(new GroupNamePrincipal("hackers", false)));
        assertFalse(predicate.test(new GroupNamePrincipal("hackers", true)));
        assertFalse(predicate.test(new UserNamePrincipal("hackers")));
        assertFalse(predicate.test(new GroupNamePrincipal("normal", false)));
        assertFalse(predicate.test(new GroupNamePrincipal("normal", true)));
    }

    @Test(expected = PredicateParserException.class)
    public void shouldRejectInvalidFqan() throws Exception {
        givenPredicate("fqan:INVALID");
    }

    @Test
    public void shouldParseFqan() throws Exception {
        givenPredicate("fqan:/dteam");

        assertThat(remaining, emptyString());

        assertTrue(predicate.test(new FQANPrincipal("/dteam", false)));
        assertTrue(predicate.test(new FQANPrincipal("/dteam", true)));
        assertFalse(predicate.test(new GroupNamePrincipal("/dteam")));
        assertFalse(predicate.test(new FQANPrincipal("/dteam/dcache", false)));
        assertFalse(predicate.test(new FQANPrincipal("/dteam/dcache", true)));
        assertFalse(predicate.test(new FQANPrincipal("/wlcg", false)));
        assertFalse(predicate.test(new FQANPrincipal("/wlcg", true)));
    }

    @Test(expected = PredicateParserException.class)
    public void shouldRejectInvalidQualifiedFqan() throws Exception {
        givenPredicate("fqan:/dteam,FOO");
    }

    @Test
    public void shouldParseQualifiedPrimaryFqan() throws Exception {
        givenPredicate("fqan:/dteam,primary");

        assertThat(remaining, emptyString());

        assertTrue(predicate.test(new FQANPrincipal("/dteam", true)));
        assertFalse(predicate.test(new FQANPrincipal("/dteam", false)));
        assertFalse(predicate.test(new GroupNamePrincipal("/dteam")));
        assertFalse(predicate.test(new FQANPrincipal("/dteam/dcache", false)));
        assertFalse(predicate.test(new FQANPrincipal("/dteam/dcache", true)));
        assertFalse(predicate.test(new FQANPrincipal("/wlcg", false)));
        assertFalse(predicate.test(new FQANPrincipal("/wlcg", true)));
    }

    @Test
    public void shouldParseQualifiedNonprimaryFqan() throws Exception {
        givenPredicate("fqan:/dteam,nonprimary");

        assertThat(remaining, emptyString());

        assertTrue(predicate.test(new FQANPrincipal("/dteam", false)));
        assertFalse(predicate.test(new FQANPrincipal("/dteam", true)));
        assertFalse(predicate.test(new GroupNamePrincipal("/dteam")));
        assertFalse(predicate.test(new FQANPrincipal("/dteam/dcache", false)));
        assertFalse(predicate.test(new FQANPrincipal("/dteam/dcache", true)));
        assertFalse(predicate.test(new FQANPrincipal("/wlcg", false)));
        assertFalse(predicate.test(new FQANPrincipal("/wlcg", true)));
    }

    @Test(expected = PredicateParserException.class)
    public void shouldRejectInvalidKerberos() throws Exception {
        givenPredicate("kerberos:INVALID");
    }

    @Test
    public void shouldParseKerberos() throws Exception {
        givenPredicate("kerberos:paul@DESY.DE");

        assertThat(remaining, emptyString());

        assertTrue(predicate.test(new KerberosPrincipal("paul@DESY.DE")));
        assertFalse(predicate.test(new UserNamePrincipal("paul@DESY.DE")));
        assertFalse(predicate.test(new KerberosPrincipal("tigran@DESY.DE")));
    }

    @Test(expected = PredicateParserException.class)
    public void shouldRejectOidcWithNoOP() throws Exception {
        givenPredicate("oidc:123456789");
    }

    @Test(expected = PredicateParserException.class)
    public void shouldRejectOidcWithInitialAt() throws Exception {
        givenPredicate("oidc:@123456789");
    }

    @Test(expected = PredicateParserException.class)
    public void shouldRejectOidcWithFinalAt() throws Exception {
        givenPredicate("oidc:123456789@");
    }

    @Test
    public void shouldParseOidcSub() throws Exception {
        givenPredicate("oidc:123456789@OP");

        assertThat(remaining, emptyString());

        assertTrue(predicate.test(new OidcSubjectPrincipal("123456789", "OP")));
        assertFalse(predicate.test(new UserNamePrincipal("123456789")));
        assertFalse(predicate.test(new OidcSubjectPrincipal("123456789", "OTHER-OP")));
        assertFalse(predicate.test(new OidcSubjectPrincipal("12345678", "OP")));
    }

    @Test
    public void shouldParseOidcSubWithAtSubClaim() throws Exception {
        givenPredicate("oidc:paul@example.org@OP");

        assertThat(remaining, emptyString());

        assertTrue(predicate.test(new OidcSubjectPrincipal("paul@example.org", "OP")));
        assertFalse(predicate.test(new UserNamePrincipal("paul@example.org")));
        assertFalse(predicate.test(new OidcSubjectPrincipal("paul@example.org", "OTHER-OP")));
        assertFalse(predicate.test(new OidcSubjectPrincipal("paul", "OP")));
    }

    @Test
    public void shouldParseOidcGroup() throws Exception {
        givenPredicate("oidcgrp:/group");

        assertThat(remaining, emptyString());

        assertTrue(predicate.test(new OpenIdGroupPrincipal("/group")));
        assertFalse(predicate.test(new GroupNamePrincipal("/group")));
        assertFalse(predicate.test(new OpenIdGroupPrincipal("/group/other")));
        assertFalse(predicate.test(new OpenIdGroupPrincipal("/other")));
    }

    @Test(expected = PredicateParserException.class)
    public void shouldRejectInvalidUid() throws Exception {
        givenPredicate("uid:INVALID");
    }

    @Test
    public void shouldParseUid() throws Exception {
        givenPredicate("uid:1000");

        assertThat(remaining, emptyString());

        assertTrue(predicate.test(new UidPrincipal(1000)));
        assertFalse(predicate.test(new GidPrincipal(1000, true)));
        assertFalse(predicate.test(new GidPrincipal(1000, false)));
        assertFalse(predicate.test(new UidPrincipal(2000)));
    }

    @Test
    public void shouldParseEntitlement() throws Exception {
        givenPredicate("entitlement:foo");

        assertThat(remaining, emptyString());

        assertTrue(predicate.test(new EntitlementPrincipal("foo")));
        assertFalse(predicate.test(new GroupNamePrincipal("foo", true)));
        assertFalse(predicate.test(new GroupNamePrincipal("foo", false)));
        assertFalse(predicate.test(new EntitlementPrincipal("bar")));
    }

    @Test
    public void shouldParseSimpleValueWithTrailingSpace() throws Exception {
        givenPredicate("username:paul ");

        assertThat(remaining, emptyString());

        assertTrue(predicate.test(new UserNamePrincipal("paul")));
    }

    @Test
    public void shouldParseSimpleValueWithTwoTrailingSpaces() throws Exception {
        givenPredicate("username:paul  ");

        assertThat(remaining, emptyString());

        assertTrue(predicate.test(new UserNamePrincipal("paul")));
    }

    @Test
    public void shouldParseSimpleValueWithTrailingSpaceAndSingleChar() throws Exception {
        givenPredicate("username:paul f");

        assertThat(remaining, equalTo("f"));

        assertTrue(predicate.test(new UserNamePrincipal("paul")));
    }

    @Test
    public void shouldParseSimpleValueWithTwoTrailingSpacesAndSingleChar() throws Exception {
        givenPredicate("username:paul  f");

        assertThat(remaining, equalTo("f"));

        assertTrue(predicate.test(new UserNamePrincipal("paul")));
    }

    @Test
    public void shouldParseSimpleValueWithTrailingSpaceAndValue() throws Exception {
        givenPredicate("username:paul foo");

        assertThat(remaining, equalTo("foo"));

        assertTrue(predicate.test(new UserNamePrincipal("paul")));
    }

    @Test
    public void shouldParseSimpleValueWithTwoTrailingSpacesAndValue() throws Exception {
        givenPredicate("username:paul  foo");

        assertThat(remaining, equalTo("foo"));

        assertTrue(predicate.test(new UserNamePrincipal("paul")));
    }

    @Test
    public void shouldParseQuotedValue() throws Exception {
        givenPredicate("username:\"paul\"");

        assertThat(remaining, emptyString());

        assertTrue(predicate.test(new UserNamePrincipal("paul")));
    }

    @Test
    public void shouldParseQuotedValueWithTrailingSpace() throws Exception {
        givenPredicate("username:\"paul\" ");

        assertThat(remaining, emptyString());

        assertTrue(predicate.test(new UserNamePrincipal("paul")));
    }

    @Test
    public void shouldParseQuotedValueWithTwoTrailingSpaces() throws Exception {
        givenPredicate("username:\"paul\"  ");

        assertThat(remaining, emptyString());

        assertTrue(predicate.test(new UserNamePrincipal("paul")));
    }

    @Test
    public void shouldParseQuotedValueWithTrailingSpaceAndSingleChar() throws Exception {
        givenPredicate("username:\"paul\" f");

        assertThat(remaining, equalTo("f"));

        assertTrue(predicate.test(new UserNamePrincipal("paul")));
    }

    @Test
    public void shouldParseQuotedValueWithTwoTrailingSpacesAndSingleChar() throws Exception {
        givenPredicate("username:\"paul\"  f");

        assertThat(remaining, equalTo("f"));

        assertTrue(predicate.test(new UserNamePrincipal("paul")));
    }

    @Test
    public void shouldParseQuotedValueWithTrailingSpaceAndValue() throws Exception {
        givenPredicate("username:\"paul\" foo");

        assertThat(remaining, equalTo("foo"));

        assertTrue(predicate.test(new UserNamePrincipal("paul")));
    }

    @Test
    public void shouldParseQuotedValueWithTwoTrailingSpacesAndValue() throws Exception {
        givenPredicate("username:\"paul\"  foo");

        assertThat(remaining, equalTo("foo"));

        assertTrue(predicate.test(new UserNamePrincipal("paul")));
    }

    @Test(expected = PredicateParserException.class)
    public void shouldRejectQuotedValueWithMissingCloseQuote() throws Exception {
        PrincipalPredicates.parseFirstPredicate("username:\"paul");
    }

    void givenPredicate(String arg) throws PredicateParserException {
        var result = PrincipalPredicates.parseFirstPredicate(arg);
        predicate = result.predicate();
        remaining = result.remaining();
    }
}
