package org.dcache.gplazma.plugins;

import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import org.globus.gsi.gssapi.jaas.GlobusPrincipal;
import org.hamcrest.Matcher;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.security.auth.kerberos.KerberosPrincipal;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.Principal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.dcache.auth.EmailAddressPrincipal;
import org.dcache.auth.FQANPrincipal;
import org.dcache.auth.GidPrincipal;
import org.dcache.auth.GroupNamePrincipal;
import org.dcache.auth.GroupPrincipal;
import org.dcache.auth.OidcSubjectPrincipal;
import org.dcache.auth.OpenIdGroupPrincipal;
import org.dcache.auth.UidPrincipal;
import org.dcache.auth.UserNamePrincipal;
import org.dcache.gplazma.AuthenticationException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;


public class GplazmaMultiMapFileTest {

    private GplazmaMultiMapFile mapFile;
    private Set<Principal> mappedPrincipals;
    private Path config;
    private List<String> warnings;

    @Before
    public void setup() throws Exception
    {
        FileSystem fileSystem = Jimfs.newFileSystem(Configuration.unix());
        config = fileSystem.getPath("/etc/dcache/multimap.conf");
        Files.createDirectories(config.getParent());
        mapFile = new GplazmaMultiMapFile(config);
        warnings = new ArrayList<>();
        mapFile.setWarningConsumer(warnings::add);
    }

    @Test
    public void shouldFailWhenWrongMapFormatDN() throws Exception {
        givenConfig("dn:kermit@dcache.org    username:kermit");

        whenMapping(new GlobusPrincipal("\"dn:/C=DE/S=Hamburg/OU=desy.de/CN=Kermit The Frog\""));

        // REVISIT should warn of invalid DN syntax
        assertThat(mappedPrincipals, is(empty()));
    }

    @Test
    public void shouldFailWhenWrongMapFormatDN1() throws Exception {
        givenConfig("dn:/C=DE/S=Hamburg/OU=desy.de/CN=Kermit The Frog    username:kermit");

        whenMapping(new GlobusPrincipal("dn:/C=DE/S=Hamburg/OU=desy.de/CN=Kermit The Frog"));

        assertThat(warnings, is(not(empty())));
        assertThat(mappedPrincipals, is(empty()));
    }

    public void shouldFailWhenWrongMapFormatKerberos() throws Exception {
        givenConfig("krb:kermit@DESY.DE    username:kermit");

        whenMapping(new KerberosPrincipal("kermit@DESY.DE"));

        assertThat(warnings, is(not(empty())));
        assertThat(mappedPrincipals, is(empty()));
    }

    @Test
    public void shouldFailWhenWrongMapFormatOidc() throws Exception {
        givenConfig("oid:googleopenidsubject    username:kermit");

        whenMapping(new OidcSubjectPrincipal("googleopenidsubject"));

        assertThat(warnings, is(not(empty())));
        assertThat(mappedPrincipals, is(empty()));
    }

    @Test
    public void shouldFailWhenWrongMapFormatEmail() throws Exception {
        givenConfig("mail:kermit@dcache.org    username:kermit");

        whenMapping(new EmailAddressPrincipal("kermit@dcache.org"));

        assertThat(warnings, is(not(empty())));
        assertThat(mappedPrincipals, is(empty()));
    }

    @Test
    public void shouldFailWhenWrongMapFormatEmail2() throws Exception {
        givenConfig("email:kermit.dcache.org    username:kermit");

        whenMapping(new EmailAddressPrincipal("kermit@dcache.org"));

        assertThat(warnings, is(not(empty())));
        assertThat(mappedPrincipals, is(empty()));
    }

    @Test
    public void shouldPassWhenEmailMapped() throws Exception {
        givenConfig("email:kermit@dcache.org    username:kermit");

        whenMapping(new EmailAddressPrincipal("kermit@dcache.org"));

        assertThat(warnings, is(empty()));
        assertThat(mappedPrincipals, hasItem(new UserNamePrincipal("kermit")));
    }

    @Test
    public void shouldPassWhenEmailMapped1() throws Exception {
        givenConfig("\"email:kermit@dcache.org\"    username:kermit");

        whenMapping(new EmailAddressPrincipal("kermit@dcache.org"));

        assertThat(warnings, is(empty()));
        assertThat(mappedPrincipals, hasItem(new UserNamePrincipal("kermit")));
    }

    @Test
    public void shouldPassWhenEmailMapped2() throws Exception {
        givenConfig("\"email:kermit@dcache.org\"    \"username:kermit\"");

        whenMapping(new EmailAddressPrincipal("kermit@dcache.org"));

        assertThat(warnings, is(empty()));
        assertThat(mappedPrincipals, hasItem(new UserNamePrincipal("kermit")));
    }

    @Test
    public void shouldPassWhenDNMapped() throws Exception {
        givenConfig("\"dn:/C=DE/O=Hamburg/OU=desy.de/CN=Kermit The Frog\"    username:kermit");

        whenMapping(new GlobusPrincipal("/C=DE/O=Hamburg/OU=desy.de/CN=Kermit The Frog"));

        assertThat(warnings, is(empty()));
        assertThat(mappedPrincipals, hasItem(new UserNamePrincipal("kermit")));
    }

    @Test
    public void shouldPassWhenOidcMapped() throws Exception {
        givenConfig("oidc:googleoidcsubject    username:kermit");

        whenMapping(new OidcSubjectPrincipal("googleoidcsubject"));

        assertThat(warnings, is(empty()));
        assertThat(mappedPrincipals, hasItem(new UserNamePrincipal("kermit")));
    }

    @Test
    public void shouldPassWhenOpenIdGroupMapped() throws Exception {
        givenConfig("oidcgrp:Users    group:desy");

        whenMapping(new OpenIdGroupPrincipal("Users"));

        assertThat(warnings, is(empty()));
        assertThat(mappedPrincipals, hasItem(new GroupNamePrincipal("desy")));
    }

    @Test
    public void shouldPassWhenUidMapped() throws Exception {
        givenConfig("oidc:googleoidcsubject    uid:1000");

        whenMapping(new OidcSubjectPrincipal("googleoidcsubject"));

        assertThat(warnings, is(empty()));
        assertThat(mappedPrincipals, hasItem(new UidPrincipal("1000")));
    }

    @Test
    public void shouldPassWhenUidPrimaryGidTrueMapped() throws Exception {
        givenConfig("oidc:googleoidcsubject   gid:1000,true  uid:1000  ");

        whenMapping(new OidcSubjectPrincipal("googleoidcsubject"));

        assertThat(warnings, is(empty()));
        assertThat(mappedPrincipals, hasItem(new UidPrincipal("1000")));
        assertThat(mappedPrincipals, hasItem(new GidPrincipal("1000", true)));
    }

    @Test
    public void shouldPassWhenUidPrimaryGidFalseMapped() throws Exception {
        givenConfig("oidc:googleoidcsubject   gid:1000,false  uid:1000  ");

        whenMapping(new OidcSubjectPrincipal("googleoidcsubject"));

        assertThat(warnings, is(empty()));
        assertThat(mappedPrincipals, hasItem(new UidPrincipal("1000")));
        assertThat(mappedPrincipals, hasItem(new GidPrincipal("1000", false)));
    }

    @Test
    public void shouldPassWhenUidGidMapped() throws Exception {
        givenConfig("oidc:googleoidcsubject   gid:1000  uid:1000  ");

        whenMapping(new OidcSubjectPrincipal("googleoidcsubject"));

        assertThat(warnings, is(empty()));
        assertThat(mappedPrincipals, hasItem(new UidPrincipal("1000")));
        assertThat(mappedPrincipals, hasItem(new GidPrincipal("1000", false)));
    }

    @Test
    public void shouldFailWhenGidFormatWrong() throws Exception {
        givenConfig("oidc:googleoidcsubject   gid:1000,,true  uid:1000  ");

        whenMapping(new OidcSubjectPrincipal("googleoidcsubject"));

        assertThat(warnings, is(not(empty())));
        assertThat(mappedPrincipals, is(empty()));
    }

    @Test
    public void shouldFailWhenGidFormatWrong2() throws Exception {
        givenConfig("oidc:googleoidcsubject   gid:1000,true,  uid:1000  ");

        whenMapping(new OidcSubjectPrincipal("googleoidcsubject"));

        assertThat(warnings, is(not(empty())));
        assertThat(mappedPrincipals, is(empty()));
    }

    @Test
    public void shouldPassWhenUidGidMapped2() throws Exception {
        givenConfig("oidc:googleoidcsubject   gid:1000,true  gid:2000 uid:1000  uid:2000");

        whenMapping(new OidcSubjectPrincipal("googleoidcsubject"));

        assertThat(warnings, is(empty()));
        assertThat(mappedPrincipals, hasItem(new UidPrincipal("2000")));
        assertThat(mappedPrincipals, hasItem(new UidPrincipal("1000")));
        assertThat(mappedPrincipals, hasItem(new GidPrincipal("2000", false)));
        assertThat(mappedPrincipals, hasItem(new GidPrincipal("1000", true)));
    }

    @Test
    public void testRefresh() throws Exception {
        givenConfig("  \n");
        givenConfigHasBeenRead();
        givenConfig("email:kermit@dcache.org    username:kermit\n");

        whenMapping(new EmailAddressPrincipal("kermit@dcache.org"));

        assertThat(warnings, is(empty()));
        assertThat(mappedPrincipals, hasItem(new UserNamePrincipal("kermit")));
    }

    @Test
    public void shouldMatchNonPrimarySpecificGroupWithPrimaryGroup() throws Exception {
        givenConfig("group:test gid:1000,false");

        whenMapping(new GroupNamePrincipal("test", true));

        assertThat(warnings, is(empty()));
        assertThat(mappedPrincipals, hasItem(new GidPrincipal(1000, false)));
    }

    @Test
    public void shouldMatchNonPrimarySpecificGroupWithNonPrimaryGroup() throws Exception {
        givenConfig("group:test gid:1000,false");

        whenMapping(new GroupNamePrincipal("test", false));

        assertThat(warnings, is(empty()));
        assertThat(mappedPrincipals, hasItem(new GidPrincipal(1000, false)));
    }

    @Test
    public void shouldMatchNonPrimarySpecificFqanWithPrimaryFqan() throws Exception {
        givenConfig("fqan:/dcache gid:1000,false");

        whenMapping(new FQANPrincipal("/dcache", true));

        assertThat(warnings, is(empty()));
        assertThat(mappedPrincipals, hasItem(new GidPrincipal(1000, false)));
    }

    @Test
    public void shouldMatchNonPrimarySpecificFqanWithNonPrimaryFqan() throws Exception {
        givenConfig("fqan:/dcache gid:1000,false");

        whenMapping(new FQANPrincipal("/dcache", false));

        assertThat(warnings, is(empty()));
        assertThat(mappedPrincipals, hasItem(new GidPrincipal(1000, false)));
    }

    @Test
    public void shouldMatchNonPrimarySpecificGidWithPrimaryGid() throws Exception {
        givenConfig("gid:1000 uid:2000");

        whenMapping(new GidPrincipal(1000, true));

        assertThat(warnings, is(empty()));
        assertThat(mappedPrincipals, hasItem(new UidPrincipal(2000)));
    }

    @Test
    public void shouldMatchNonPrimarySpecificGidWithNonPrimaryGid() throws Exception {
        givenConfig("gid:1000 uid:2000");

        whenMapping(new GidPrincipal(1000, false));

        assertThat(warnings, is(empty()));
        assertThat(mappedPrincipals, hasItem(new UidPrincipal(2000)));
    }

    /*----------------------- Helpers -----------------------------*/

    private void givenConfig(String mapping) throws Exception
    {
        if (Files.exists(config)) {
            Thread.sleep(1); // Ensure file's mtime value is different.
            Files.write(config, mapping.getBytes(), StandardOpenOption.TRUNCATE_EXISTING);
            Thread.sleep(100); // sleep is necessary because mtime values are cached.
        } else {
            Files.write(config, mapping.getBytes(), StandardOpenOption.CREATE_NEW);
        }
    }

    private void givenConfigHasBeenRead() throws Exception
    {
        mapFile.mapping();
    }

    private void whenMapping(Principal principal) throws Exception
    {
        mappedPrincipals = mapFile.mapping().entrySet().stream()
                .filter(e -> e.getKey().matches(principal))
                .map(e -> e.getValue())
                .findFirst()
                .orElse(Collections.emptySet());
    }
}