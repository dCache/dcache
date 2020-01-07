package org.dcache.gplazma.plugins;

import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;

import org.dcache.auth.UserNamePrincipal;
import org.dcache.gplazma.AuthenticationException;

import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.Principal;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import org.dcache.auth.GidPrincipal;
import org.dcache.util.PrincipalSetMaker;

import static org.dcache.util.PrincipalSetMaker.aSetOfPrincipals;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;


public class GplazmaMultiMapPluginTest
{
    private Path config;
    private GplazmaMultiMapPlugin plugin;
    private Set<Principal> results;

    @Before
    public void setup() throws Exception
    {
        FileSystem filesystem = Jimfs.newFileSystem(Configuration.unix());
        config = filesystem.getPath("/etc/dcache/multimap.conf");
        Files.createDirectories(config.getParent());

        Properties configuration = new Properties();
        configuration.put("gplazma.multimap.file", config.toString());

        plugin = new GplazmaMultiMapPlugin(filesystem, configuration);
    }

    @Test(expected = AuthenticationException.class)
    public void shouldFailWhenFileDoesNotExist() throws Exception
    {
        whenMapCalledWith(aSetOfPrincipals().withOidc("googleoidcsub"));
    }

    @Test(expected = AuthenticationException.class)
    public void shouldFailWhenNoMapping() throws Exception
    {
        givenConfig("   ");

        whenMapCalledWith(aSetOfPrincipals().withOidc("googleoidcsub"));
    }

    @Test
    public void shouldMapOidcToUsername() throws Exception
    {
        givenConfig("oidc:googleoidcsub  username:kermit");

        whenMapCalledWith(aSetOfPrincipals().withOidc("googleoidcsub"));

        assertThat(results, hasItem(new UserNamePrincipal("kermit")));
    }

    @Test
    public void shouldMapGlobusToUsername() throws Exception
    {
        givenConfig("\"dn:/O=DE/O=Hamburg/OU=desy.de/CN=Kermit The Frog\"    username:kermit");

        whenMapCalledWith(aSetOfPrincipals().withDn("/O=DE/O=Hamburg/OU=desy.de/CN=Kermit The Frog"));

        assertThat(results, hasItem(new UserNamePrincipal("kermit")));
    }

    @Test(expected = AuthenticationException.class)
    public void shouldFailGlobusToUsernameNotMapping() throws Exception
    {
        givenConfig("\"dn:/O=ES/O=Madrid/OU=upm.es/CN=Kermit The Frog\"    username:kermit");

        whenMapCalledWith(aSetOfPrincipals().withDn("/O=DE/O=Hamburg/OU=desy.de/CN=Kermit The Frog"));
    }


    @Test
    public void shouldMapEmailToUsername() throws Exception
    {
        givenConfig("email:kermit.the.frog@email.com    username:kermit");

        whenMapCalledWith(aSetOfPrincipals().withEmail("kermit.the.frog@email.com"));

        assertThat(results, hasItem(new UserNamePrincipal("kermit")));
    }

    @Test(expected = AuthenticationException.class)
    public void shouldFailEmailToUsernameWhenSuppliedKerberos() throws Exception
    {
        givenConfig("kerberos:kermit@DESY.DE    username:kermit");

        whenMapCalledWith(aSetOfPrincipals().withEmail("kermit.the.frog@email.com"));
    }

    @Test
    public void shouldMapKerberosToUsername() throws Exception
    {
        givenConfig("kerberos:kermit@DESY.DE    username:kermit");

        whenMapCalledWith(aSetOfPrincipals().withKerberos("kermit@DESY.DE"));

        assertThat(results, hasItem(new UserNamePrincipal("kermit")));
    }


    @Test(expected = AuthenticationException.class)
    public void shouldFailKerberosToUsernameWhenSuppliedOidc() throws Exception
    {
        givenConfig("oidc:googleoidcsub    username:kermit");

        whenMapCalledWith(aSetOfPrincipals().withKerberos("kermit@DESY.DE"));
    }

    @Test
    public void shouldReturnPrimaryGid() throws Exception
    {
        givenConfig("username:paul  gid:1000,true");

        whenMapCalledWith(aSetOfPrincipals().withUsername("paul"));

        assertThat(results, hasItem(new GidPrincipal(1000, true)));
    }

    @Test
    public void shouldReturnExplicitNonprimaryGid() throws Exception
    {
        givenConfig("username:paul  gid:1000,false");

        whenMapCalledWith(aSetOfPrincipals().withUsername("paul"));

        assertThat(results, hasItem(new GidPrincipal(1000, false)));
    }

    @Test
    public void shouldReturnImplicitNonprimaryGid() throws Exception
    {
        givenConfig("username:paul  gid:1000");

        whenMapCalledWith(aSetOfPrincipals().withUsername("paul"));

        assertThat(results, hasItem(new GidPrincipal(1000, false)));
    }

    @Test
    public void shouldReturnNonprimaryGidWhenPrimaryGidAlreadyPresent() throws Exception
    {
        givenConfig("username:paul  gid:1000,true");

        whenMapCalledWith(aSetOfPrincipals()
                .withUsername("paul")
                .withPrimaryGid(2000));

        assertThat(results, hasItem(new GidPrincipal(1000, false)));
        assertThat(results, not(hasItem(new GidPrincipal(1000, true))));
    }

    @Test
    public void shouldReturnSinglePrimaryGidWithMultipleMappedPrimaryGids() throws Exception
    {
        givenConfig("username:paul  gid:1000,true\n"
                  + "group:foo  gid:2000,true");

        whenMapCalledWith(aSetOfPrincipals()
                .withUsername("paul")
                .withGroupname("foo"));

        assertThat(results, hasItem(new GidPrincipal(1000, true)));
        assertThat(results, hasItem(new GidPrincipal(2000, false)));
        assertThat(results, not(hasItem(new GidPrincipal(1000, false))));
        assertThat(results, not(hasItem(new GidPrincipal(2000, true))));
    }

    @Test
    public void shouldReturnSinglePrimaryGidWithMultipleMappedPrimaryGidsReverseOrder() throws Exception
    {
        givenConfig("group:foo  gid:2000,true\n"
                  + "username:paul  gid:1000,true");

        whenMapCalledWith(aSetOfPrincipals()
                .withUsername("paul")
                .withGroupname("foo"));

        assertThat(results, hasItem(new GidPrincipal(2000, true)));
        assertThat(results, hasItem(new GidPrincipal(1000, false)));
        assertThat(results, not(hasItem(new GidPrincipal(2000, false))));
        assertThat(results, not(hasItem(new GidPrincipal(1000, true))));
    }

    @Test
    public void shouldReturnOnlyNonPrimaryGidsWhenPrimaryGidAlreadyPresent() throws Exception
    {
        givenConfig("username:paul  gid:1000,true\n"
                  + "group:foo  gid:2000,true");

        whenMapCalledWith(aSetOfPrincipals()
                .withUsername("paul")
                .withGroupname("foo")
                .withPrimaryGid(20));

        assertThat(results, hasItem(new GidPrincipal(1000, false)));
        assertThat(results, hasItem(new GidPrincipal(2000, false)));
        assertThat(results, not(hasItem(new GidPrincipal(1000, true))));
        assertThat(results, not(hasItem(new GidPrincipal(2000, true))));
    }

    /*------------------------- Helpers ---------------------------*/

    private void givenConfig(String mapping) throws IOException, AuthenticationException
    {
        Files.write(config, mapping.getBytes(), StandardOpenOption.CREATE_NEW);
    }

    private void whenMapCalledWith(PrincipalSetMaker principals)
            throws AuthenticationException
    {
        results = new HashSet<>();
        results.addAll(principals.build());
        plugin.map(results);
    }
}