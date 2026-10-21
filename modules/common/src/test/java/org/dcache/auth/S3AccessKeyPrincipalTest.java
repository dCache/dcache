package org.dcache.auth;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

import org.junit.Test;

public class S3AccessKeyPrincipalTest {

    @Test(expected = IllegalArgumentException.class)
    public void shouldRejectEmptyAccessKey() {
        new S3AccessKeyPrincipal("");
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldRejectAccessKeyWithWhitespace() {
        new S3AccessKeyPrincipal("AKIA EXAMPLE");
    }

    @Test
    public void shouldReturnAccessKeyAsName() {
        S3AccessKeyPrincipal principal = new S3AccessKeyPrincipal("AKIAEXAMPLE");

        assertThat(principal.getName(), is(equalTo("AKIAEXAMPLE")));
    }

    @Test
    public void shouldBeEqualForSameAccessKey() {
        S3AccessKeyPrincipal p1 = new S3AccessKeyPrincipal("AKIAEXAMPLE");
        S3AccessKeyPrincipal p2 = new S3AccessKeyPrincipal("AKIAEXAMPLE");

        assertThat(p1, is(equalTo(p2)));
        assertThat(p1.hashCode(), is(equalTo(p2.hashCode())));
    }

    @Test
    public void shouldNotBeEqualForDifferentAccessKey() {
        S3AccessKeyPrincipal p1 = new S3AccessKeyPrincipal("AKIAEXAMPLE");
        S3AccessKeyPrincipal p2 = new S3AccessKeyPrincipal("AKIAOTHER");

        assertThat(p1, is(not(equalTo(p2))));
    }

    @Test
    public void shouldNotBeEqualToUserNameWithSameValue() {
        S3AccessKeyPrincipal p1 = new S3AccessKeyPrincipal("kermit");
        UserNamePrincipal p2 = new UserNamePrincipal("kermit");

        assertThat(p1, is(not(equalTo(p2))));
    }
}
