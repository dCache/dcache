package org.dcache.gplazma.plugins;

import org.dcache.auth.EmailAddressPrincipal;
import org.dcache.auth.GidPrincipal;
import org.dcache.auth.OidcSubjectPrincipal;
import org.dcache.auth.UidPrincipal;
import org.dcache.auth.UserNamePrincipal;
import org.dcache.gplazma.AuthenticationException;

import org.globus.gsi.gssapi.jaas.GlobusPrincipal;
import org.hamcrest.Matcher;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.security.auth.kerberos.KerberosPrincipal;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.security.Principal;
import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;


public class GplazmaMultiMapFileTest {

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    private GplazmaMultiMapFile mapFile;
    private Set<Principal> principals;
    private File config;

    @After
    public void tearDown() throws Exception
    {
        config.delete();
        config = null;
    }

    @Test
    public void shouldFailWhenWrongMapFormatDN() throws Exception {
        givenConfig("dn:kermit@dcache.org    username:kermit");

        whenMapUsername(withDN("\"dn:/C=DE/S=Hamburg/OU=desy.de/CN=Kermit The Frog\""));

        assertThat(principals, is(empty()));
    }

    @Test
    public void shouldFailWhenWrongMapFormatDN1() throws Exception {
        givenConfig("dn:/C=DE/S=Hamburg/OU=desy.de/CN=Kermit The Frog    username:kermit");

        whenMapUsername(withDN("dn:\"/C=DE/S=Hamburg/OU=desy.de/CN=Kermit The Frog\""));

        assertThat(principals, is(empty()));
    }

    @Test
    public void shouldFailWhenWrongMapFormatKerberos() throws Exception {
        givenConfig("krb:kermit@DESY.DE    username:kermit");

        whenMapUsername(withKerberos("kermit@DESY.DE"));

        assertThat(principals, is(empty()));
    }

    @Test
    public void shouldFailWhenWrongMapFormatOidc() throws Exception {
        givenConfig("oid:googleopenidsubject    username:kermit");

        whenMapUsername(withOidcSubject("googleopenidsubject"));

        assertThat(principals, is(empty()));
    }

    @Test
    public void shouldFailWhenWrongMapFormatEmail() throws Exception {
        givenConfig("mail:kermit@dcache.org    username:kermit");

        whenMapUsername(withEmail("kermit@dcache.org"));

        assertThat(principals, is(empty()));
    }

    @Test
    public void shouldFailWhenWrongMapFormatEmail2() throws Exception {
        givenConfig("email:kermit.dcache.org    username:kermit");

        whenMapUsername(withEmail("kermit@dcache.org"));

        assertThat(principals, is(empty()));
    }

    @Test
    public void shouldPassWhenEmailMapped() throws Exception {
        givenConfig("email:kermit@dcache.org    username:kermit");

        whenMapUsername(withEmail("kermit@dcache.org"));

        assertThat(principals, is(not(empty())));
        assertThat(principals, hasUserNamePrincipal("kermit"));
    }

    @Test
    public void shouldPassWhenEmailMapped1() throws Exception {
        givenConfig("\"email:kermit@dcache.org\"    username:kermit");

        whenMapUsername(withEmail("kermit@dcache.org"));

        assertThat(principals, is(not(empty())));
        assertThat(principals, hasUserNamePrincipal("kermit"));
    }

    @Test
    public void shouldPassWhenEmailMapped2() throws Exception {
        givenConfig("\"email:kermit@dcache.org\"    \"username:kermit\"");

        whenMapUsername(withEmail("kermit@dcache.org"));

        assertThat(principals, is(not(empty())));
        assertThat(principals, hasUserNamePrincipal("kermit"));
    }

    @Test
    public void shouldPassWhenDNMapped() throws Exception {
        givenConfig("\"dn:/C=DE/O=Hamburg/OU=desy.de/CN=Kermit The Frog\"    username:kermit");

        whenMapUsername(withDN("/C=DE/O=Hamburg/OU=desy.de/CN=Kermit The Frog"));

        assertThat(principals, is(not(empty())));
        assertThat(principals, hasUserNamePrincipal("kermit"));
    }

    @Test
    public void shouldPassWhenOidcMapped() throws Exception {
        givenConfig("oidc:googleoidcsubject    username:kermit");

        whenMapUsername(withOidcSubject("googleoidcsubject"));

        assertThat(principals, is(not(empty())));
        assertThat(principals, hasUserNamePrincipal("kermit"));
    }

    @Test
    public void shouldPassWhenUidMapped() throws Exception {
        givenConfig("oidc:googleoidcsubject    uid:1000");

        whenMapUsername(withOidcSubject("googleoidcsubject"));

        assertThat(principals, is(not(empty())));
        assertThat(principals, hasUidPrincipal("1000"));
    }

    @Test
    public void shouldPassWhenUidPrimaryGidTrueMapped() throws Exception {
        givenConfig("oidc:googleoidcsubject   gid:1000,true  uid:1000  ");

        whenMapUsername(withOidcSubject("googleoidcsubject"));

        assertThat(principals, is(not(empty())));
        assertThat(principals, hasUidPrincipal("1000"));
        assertThat(principals, hasGidPrincipal("1000", true));
    }

    @Test
    public void shouldPassWhenUidPrimaryGidFalseMapped() throws Exception {
        givenConfig("oidc:googleoidcsubject   gid:1000,false  uid:1000  ");

        whenMapUsername(withOidcSubject("googleoidcsubject"));

        assertThat(principals, is(not(empty())));
        assertThat(principals, hasUidPrincipal("1000"));
        assertThat(principals, hasGidPrincipal("1000", false));
    }

    @Test
    public void shouldPassWhenUidGidMapped() throws Exception {
        givenConfig("oidc:googleoidcsubject   gid:1000  uid:1000  ");

        whenMapUsername(withOidcSubject("googleoidcsubject"));

        assertThat(principals, is(not(empty())));
        assertThat(principals, hasUidPrincipal("1000"));
        assertThat(principals, hasGidPrincipal("1000", false));
    }

    @Test
    public void shouldFailWhenGidFormatWrong() throws Exception {
        givenConfig("oidc:googleoidcsubject   gid:1000,,true  uid:1000  ");

        whenMapUsername(withOidcSubject("googleoidcsubject"));

        assertThat(principals, is(empty()));
    }

    @Test
    public void shouldFailWhenGidFormatWrong2() throws Exception {
        givenConfig("oidc:googleoidcsubject   gid:1000,true,  uid:1000  ");

        whenMapUsername(withOidcSubject("googleoidcsubject"));

        assertThat(principals, is(empty()));
    }

    @Test
    public void shouldPassWhenUidGidMapped2() throws Exception {
        givenConfig("oidc:googleoidcsubject   gid:1000,true  gid:2000 uid:1000  uid:2000");

        whenMapUsername(withOidcSubject("googleoidcsubject"));

        assertThat(principals, is(not(empty())));
        assertThat(principals, hasUidPrincipal("2000"));
        assertThat(principals, hasUidPrincipal("1000"));
        assertThat(principals, hasGidPrincipal("2000", false));
        assertThat(principals, hasGidPrincipal("1000", true));
    }

    @Test
    public void testRefresh() throws Exception {
        givenConfig("  \n");

        whenMapUsername(withEmail("kermit@dcache.org"));
        assertThat(principals, is(empty()));

        appendConfig("email:kermit@dcache.org    username:kermit\n");

        whenMapUsername(withEmail("kermit@dcache.org"));
        assertThat(principals, is(empty()));

        mapFile.ensureUpToDate();

        whenMapUsername(withEmail("kermit@dcache.org"));
        assertThat(principals, is(not(empty())));
        assertThat(principals, hasUserNamePrincipal("kermit"));
    }

    /*----------------------- Helpers -----------------------------*/

    private void givenConfig(String mapping) throws IOException, AuthenticationException {
        config = tempFolder.newFile("multi-mapfile");
        Files.write(config.toPath(), mapping.getBytes(), StandardOpenOption.APPEND);
        mapFile = new GplazmaMultiMapFile(config);
        mapFile.ensureUpToDate();
    }

    private void appendConfig(String mapping) throws InterruptedException, IOException {
        Files.write(config.toPath(), mapping.getBytes(), StandardOpenOption.APPEND);
        // Add 1 sec to modified time because not all platforms
        // support file-modification times to the milli-second
        config.setLastModified(System.currentTimeMillis()+1000);
    }

    private void whenMapUsername(Principal principal) {
        principals = mapFile.getMappedPrincipals(principal);
    }

    private Principal withDN(String s) {
        return new GlobusPrincipal(s);
    }

    private Principal withKerberos(String s) {
        return new KerberosPrincipal(s);
    }

    private Principal withEmail(String s) {
        return new EmailAddressPrincipal(s);
    }

    private Principal withOidcSubject(String s) {
        return new OidcSubjectPrincipal(s);
    }

    private Matcher<Iterable<? super UserNamePrincipal>> hasUserNamePrincipal(String username) {
        return hasItem(new UserNamePrincipal(username));
    }

    private Matcher<Iterable<? super UidPrincipal>> hasUidPrincipal(String uid) {
        return hasItem(new UidPrincipal(uid));
    }

    private Matcher<Iterable<? super GidPrincipal>> hasGidPrincipal(String gid, boolean isPrimary) {
        return hasItem(new GidPrincipal(gid, isPrimary));
    }

}