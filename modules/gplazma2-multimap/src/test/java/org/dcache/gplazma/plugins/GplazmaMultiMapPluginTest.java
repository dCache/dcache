package org.dcache.gplazma.plugins;

import org.apache.commons.io.FileUtils;
import org.dcache.auth.EmailAddressPrincipal;
import org.dcache.auth.OidcSubjectPrincipal;
import org.dcache.auth.UserNamePrincipal;
import org.dcache.gplazma.AuthenticationException;
import org.globus.gsi.gssapi.jaas.GlobusPrincipal;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

import javax.security.auth.kerberos.KerberosPrincipal;
import java.io.File;
import java.io.IOException;
import java.security.Principal;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import static org.hamcrest.Matchers.*;
import static org.hamcrest.collection.IsEmptyCollection.empty;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;


public class GplazmaMultiMapPluginTest {

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    private Set<Principal> principals;
    private Set<Principal> result;
    private Properties givenConfiguration = new Properties();
    private final UserNamePrincipal testUser = new UserNamePrincipal("kermit");

    @BeforeClass
    public static void init() throws Exception
    {
    }

    @Before
    public void setUp() throws Exception
    {
        result = new HashSet<>();
        result.add(testUser);
        principals = new HashSet<>();
    }

    @After
    public void tearDown() throws Exception
    {
    }

    @Test(expected = AuthenticationException.class)
    public void shouldFailWhenNoMappingUsingMockedMap() throws Exception
    {
        whenMapPluginCalledWith(mockedMapFileWithNoMapping(),
                withOidcPrincipal("googleopenidcsubject"));
    }

    @Test(expected = AuthenticationException.class)
    public void shouldFailWhenNoMapping() throws Exception
    {
        givenConfig("   ");

        whenMapPluginCalledWith(
                withConfig(),
                withOidcPrincipal("googleopenidcsubject"));
    }

    @Test
    public void shouldMapOidcToUsernameUsingMockedMap() throws Exception
    {
        whenMapPluginCalledWith(
                mockedMapFile(),
                withOidcPrincipal("googleopenidcsubject"));

        assertThat(principals, is(not(empty())));
        assertThat(principals, hasItem(testUser));
    }

    @Test
    public void shouldMapOidcToUsername() throws Exception
    {
        givenConfig("oidc:googleopenidcsubject    username:kermit");

        whenMapPluginCalledWith(
                withConfig(),
                withOidcPrincipal("googleopenidcsubject"));

        assertThat(principals, is(not(empty())));
        assertThat(principals, hasItem(testUser));
    }

    @Test
    public void shouldMapGlobusToUsernameUsingMockedMap() throws Exception
    {
        whenMapPluginCalledWith(
                mockedMapFile(),
                withGlobusPrincipal("/O=DE/O=Hamburg/OU=desy.de/CN=Kermit The Frog")
        );

        assertThat(principals, is(not(empty())));
        assertThat(principals, hasItem(testUser));
    }

    @Test
    public void shouldMapGlobusToUsername() throws Exception
    {
        givenConfig("dn:\"/O=DE/O=Hamburg/OU=desy.de/CN=Kermit The Frog\"    username:kermit");

        whenMapPluginCalledWith(
                withConfig(),
                withGlobusPrincipal("/O=DE/O=Hamburg/OU=desy.de/CN=Kermit The Frog")
        );

        assertThat(principals, is(not(empty())));
        assertThat(principals, hasItem(testUser));
    }

    @Test(expected = AuthenticationException.class)
    public void shouldFailGlobusToUsernameNotMapping() throws Exception
    {
        givenConfig("dn:\"/O=ES/O=Madrid/OU=upm.es/CN=Kermit The Frog\"    username:kermit");

        whenMapPluginCalledWith(
                withConfig(),
                withGlobusPrincipal("/O=DE/O=Hamburg/OU=desy.de/CN=Kermit The Frog")
        );
    }


    @Test
    public void shouldMapEmailToUsernameUsingMockedMap() throws Exception
    {
        whenMapPluginCalledWith(
                mockedMapFile(),
                withEmailPrincipal("kermit.the.frog@email.com")
        );

        assertThat(principals, is(not(empty())));
        assertThat(principals, hasItem(testUser));
    }

    @Test
    public void shouldMapEmailToUsername() throws Exception
    {
        givenConfig("email:kermit.the.frog@email.com    username:kermit");

        whenMapPluginCalledWith(
                withConfig(),
                withEmailPrincipal("kermit.the.frog@email.com")
        );

        assertThat(principals, is(not(empty())));
        assertThat(principals, hasItem(testUser));
    }

    @Test(expected = AuthenticationException.class)
    public void shouldFailEmailToUsernameWhenSuppliedKerberos() throws Exception
    {
        givenConfig("kerberos:kermit@DESY.DE    username:kermit");

        whenMapPluginCalledWith(
                withConfig(),
                withEmailPrincipal("kermit.the.frog@email.com")
        );
    }

    @Test
    public void shouldMapKerberosToUsernameUsingMockedMap() throws Exception
    {
        whenMapPluginCalledWith(
                mockedMapFile(),
                withKerberosPrincipal("kermit@DESY.DE")
        );

        assertThat(principals, is(not(empty())));
        assertThat(principals, hasItem(testUser));
    }

    @Test
    public void shouldMapKerberosToUsername() throws Exception
    {
        givenConfig("kerberos:kermit@DESY.DE    username:kermit");

        whenMapPluginCalledWith(
                withConfig(),
                withKerberosPrincipal("kermit@DESY.DE")
        );

        assertThat(principals, is(not(empty())));
        assertThat(principals, hasItem(testUser));
    }


    @Test(expected = AuthenticationException.class)
    public void shouldFailKerberosToUsernameWhenSuppliedOidc() throws Exception
    {
        givenConfig("oidc:googlesubject    username:kermit");

        whenMapPluginCalledWith(
                withConfig(),
                withKerberosPrincipal("kermit@DESY.DE")
        );
    }

    /*------------------------- Helpers ---------------------------*/

    private void givenConfig(String map) throws IOException {
        final File multimapper = tempFolder.newFile("multimapper.conf");
        givenConfiguration.put("gplazma.multimapper.file", multimapper.getPath());

        FileUtils.writeStringToFile(multimapper, map);
    }


    private void whenMapPluginCalledWith(GplazmaMultiMapFile map, Set<Principal> principals)
            throws AuthenticationException
    {
        GplazmaMultiMapPlugin plugin =  new GplazmaMultiMapPlugin(map);
        plugin.map(principals);
    }

    private void whenMapPluginCalledWith(Properties properties, Set<Principal> principals)
            throws AuthenticationException
    {
        GplazmaMultiMapPlugin plugin =  new GplazmaMultiMapPlugin(properties);
        plugin.map(principals);
    }

    private GplazmaMultiMapFile mockedMapFileWithNoMapping() throws AuthenticationException {
        GplazmaMultiMapFile map =  Mockito.mock(GplazmaMultiMapFile.class);
        doReturn(new HashSet<>()).when(map).getMappedPrincipals(Mockito.any());
        doNothing().when(map).ensureUpToDate();
        return map;
    }


    private GplazmaMultiMapFile mockedMapFile() throws AuthenticationException {
        GplazmaMultiMapFile map =  Mockito.mock(GplazmaMultiMapFile.class);
        doReturn(result).when(map).getMappedPrincipals(Mockito.any());
        doNothing().when(map).ensureUpToDate();
        return map;
    }

    private Set<Principal> withOidcPrincipal(String oidc) {
        principals.add(new OidcSubjectPrincipal(oidc));
        return principals;
    }

    private Set<Principal> withGlobusPrincipal(String dn) {
        principals.add(new GlobusPrincipal(dn));
        return principals;
    }

    private Set<Principal> withEmailPrincipal(String email) {
        principals.add(new EmailAddressPrincipal(email));
        return principals;
    }

    private Set<Principal> withKerberosPrincipal(String kerberos) {
        principals.add(new KerberosPrincipal(kerberos));
        return principals;
    }

    private Properties withConfig() {
        return givenConfiguration;
    }
}