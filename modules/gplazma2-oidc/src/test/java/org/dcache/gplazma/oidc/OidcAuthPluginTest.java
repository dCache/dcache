package org.dcache.gplazma.oidc;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.hamcrest.Matcher;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.net.URI;
import java.security.Principal;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.dcache.auth.BearerTokenCredential;
import org.dcache.auth.EmailAddressPrincipal;
import org.dcache.auth.FullNamePrincipal;
import org.dcache.auth.LoA;
import org.dcache.auth.LoAPrincipal;
import org.dcache.auth.OidcSubjectPrincipal;
import org.dcache.auth.OpenIdGroupPrincipal;
import org.dcache.gplazma.AuthenticationException;
import org.dcache.gplazma.oidc.exceptions.OidcException;
import org.dcache.gplazma.oidc.helpers.JsonHttpClient;

import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;

public class OidcAuthPluginTest {
    private static final String OIDC_PROPERTY_NAME = "gplazma.oidc.hostnames";
    private static final String OIDC_PROPERTY_OP_PREFIX = "gplazma.oidc.provider!";
    private Properties givenConfiguration;
    private JsonHttpClient httpClient;

    @Before
    public void setUp() throws Exception
    {
        httpClient = Mockito.mock(JsonHttpClient.class);

        givenConfiguration = new Properties();
        givenConfiguration.put("gplazma.oidc.concurrent-requests", "20");
        givenConfiguration.put("gplazma.oidc.http.total-concurrent-requests", "20");
        givenConfiguration.put("gplazma.oidc.http.per-route-concurrent-requests", "10");
        givenConfiguration.put("gplazma.oidc.http.slow-threshold", "2");
        givenConfiguration.put("gplazma.oidc.http.slow-threshold.unit", "SECONDS");
        givenConfiguration.put("gplazma.oidc.http.timeout", "30");
        givenConfiguration.put("gplazma.oidc.http.timeout.unit", "SECONDS");
        givenConfiguration.put("gplazma.oidc.discovery-cache", "1");
        givenConfiguration.put("gplazma.oidc.discovery-cache.unit", "HOURS");
        givenConfiguration.put("gplazma.oidc.access-token-cache.size", "1000");
        givenConfiguration.put("gplazma.oidc.access-token-cache.refresh", "100");
        givenConfiguration.put("gplazma.oidc.access-token-cache.refresh.unit", "SECONDS");
        givenConfiguration.put("gplazma.oidc.access-token-cache.expire", "120");
        givenConfiguration.put("gplazma.oidc.access-token-cache.expire.unit", "SECONDS");

        givenConfiguration.setProperty(OIDC_PROPERTY_NAME, "");
    }

    @After
    public void tearDown() throws Exception
    {

    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldFailWithEmptyOidcHostList() throws Exception
    {
        givenHostnameConfig("   ");
        whenOidcPluginCreated();
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldFailWithInvalidHostnames() throws Exception
    {
        givenHostnameConfig(" \" ");
        whenOidcPluginCreated();
    }

    @Test(expected = AuthenticationException.class)
    public void shouldFailWithNoCredential() throws Exception
    {
        givenHostnameConfig("accounts.google.com  idc-iam.example.org");
        whenOidcPluginCalledWithNoCredentials();
    }

    @Test(expected = AuthenticationException.class)
    public void shouldFailWhenNoDiscoveryDoc() throws Exception
    {
        givenHostnameConfig("accounts.google.com  idc-iam.example.org");

        whenOidcPluginCalledWith(
                withIOException(),
                withUserInfo("{}"),
                withBearerToken(null));
    }

    @Test(expected = AuthenticationException.class)
    public void shouldFailWhenHttpGetUserInfoFails() throws Exception
    {
        givenHostnameConfig("accounts.google.com  idc-iam.example.org");

        whenOidcPluginCalledWith(
                withDiscoveryDoc("{ \"userinfo_endpoint\":\"https://www.googleapis.com/oauth2/v3/userinfo\" }"),
                withIOException(),
                withBearerToken(null));
    }

    @Test(expected = AuthenticationException.class)
    public void shouldFailWhenInvalidTokenOneProvider() throws Exception
    {
        givenHostnameConfig("accounts.google.com");

        whenOidcPluginCalledWith(
                withDiscoveryDoc("{ \"userinfo_endpoint\":\"https://www.googleapis.com/oauth2/v3/userinfo\" }"),
                withUserInfo("{\"error\":\"invalid_token\",\"error_description\":\"Invalid Credentials\"}"),
                withBearerToken("thisisnotatoken"));
    }

    @Test(expected = AuthenticationException.class)
    public void shouldFailWhenInvalidToken() throws Exception
    {
        givenHostnameConfig("accounts.google.com  idc-iam.example.org");

        whenOidcPluginCalledWith(
                withDiscoveryDoc("{ \"userinfo_endpoint\":\"https://www.googleapis.com/oauth2/v3/userinfo\" }"),
                withUserInfo("{\"error\":\"invalid_token\",\"error_description\":\"Invalid Credentials\"}"),
                withBearerToken("thisisnotatoken"));
    }

    @Test(expected = AuthenticationException.class)
    public void shouldFailWhenNoOidcSubject() throws Exception
    {
        givenHostnameConfig("accounts.google.com  idc-iam.example.org");

        whenOidcPluginCalledWith(
                withDiscoveryDoc("{ \"userinfo_endpoint\":\"https://www.googleapis.com/oauth2/v3/userinfo\" }"),
                withUserInfo("{\"nosub\":\"\"}"),
                withBearerToken("validtoken"));
    }

    @Test
    public void successWhenValidToken() throws Exception
    {
        givenHostnameConfig("accounts.google.com  idc-iam.example.org");

        Set<Principal> principals =
                whenOidcPluginCalledWith(
                        withDiscoveryDoc("{ \"userinfo_endpoint\":\"https://www.googleapis.com/oauth2/v3/userinfo \"}"),
                        withUserInfo(new StringBuilder()
                                .append("{\"sub\":\"214234823942934792371\",")
                                .append("\"name\":\"Kermit The Frog\",  ")
                                .append("\"given_name\": \"Kermit The\",  ")
                                .append("\"family_name\": \"Frog\",  ")
                                .append("\"groups\": [ \"Users\", \"Developers\" ], ")
                                .append("\"eduperson_assurance\": [ \"https://refeds.org/assurance/IAP/medium\", \"https://refeds.org/assurance/ID/unique\", \"https://refeds.org/assurance/IAP/local-enterprise\" ], ")
                                .append("\"picture\": \"https://lh3.googleusercontent.com/gjworasdfjasgjdlsjvlsjlv/photo.jpg\",  ")
                                .append("\"email\": \"kermit.the.frog@email.com\",  ")
                                .append("\"email_verified\": true } ")
                                .toString()),
                        withBearerToken("validtoken"));

        assertThat(principals, hasSubject("214234823942934792371"));
        assertThat(principals, hasFullName("Kermit The", "Frog", "Kermit The Frog"));
        assertThat(principals, hasEmail("kermit.the.frog@email.com"));
        assertThat(principals, hasGroup("Users"));
        assertThat(principals, hasGroup("Developers"));
        assertThat(principals, hasLoA(LoA.REFEDS_IAP_MEDIUM));
        assertThat(principals, hasLoA(LoA.REFEDS_ID_UNIQUE));

        // No '-local' so, no local-enterprise LoA
        assertThat(principals, not(hasLoA(LoA.REFEDS_IAP_LOCAL_ENTERPRISE)));
    }

    @Test
    public void successWhenValidTokenNonLocal() throws Exception
    {
        givenPrefixConfig("IAM", "idc-iam.example.org");

        Set<Principal> principals =
                whenOidcPluginCalledWith(
                        withDiscoveryDoc("{ \"userinfo_endpoint\":\"https://www.googleapis.com/oauth2/v3/userinfo \"}"),
                        withUserInfo(new StringBuilder()
                                .append("{\"sub\":\"214234823942934792371\",")
                                .append("\"name\":\"Kermit The Frog\",  ")
                                .append("\"given_name\": \"Kermit The\",  ")
                                .append("\"family_name\": \"Frog\",  ")
                                .append("\"groups\": [ \"Users\", \"Developers\" ], ")
                                .append("\"eduperson_assurance\": [ \"https://refeds.org/assurance/IAP/medium\", \"https://refeds.org/assurance/ID/unique\", \"https://refeds.org/assurance/IAP/local-enterprise\" ], ")
                                .append("\"picture\": \"https://lh3.googleusercontent.com/gjworasdfjasgjdlsjvlsjlv/photo.jpg\",  ")
                                .append("\"email\": \"kermit.the.frog@email.com\",  ")
                                .append("\"email_verified\": true } ")
                                .toString()),
                        withBearerToken("validtoken"));

        assertThat(principals, hasSubject("214234823942934792371"));
        assertThat(principals, hasFullName("Kermit The", "Frog", "Kermit The Frog"));
        assertThat(principals, hasEmail("kermit.the.frog@email.com"));
        assertThat(principals, hasGroup("Users"));
        assertThat(principals, hasGroup("Developers"));
        assertThat(principals, hasLoA(LoA.REFEDS_IAP_MEDIUM));
        assertThat(principals, hasLoA(LoA.REFEDS_ID_UNIQUE));

        // We currently don't trust OPs to assert REFEDS_IAP_LOCAL_ENTERPRISE
        assertThat(principals, not(hasLoA(LoA.REFEDS_IAP_LOCAL_ENTERPRISE)));
    }

    /*-------------------------------- Helpers --------------------------------------*/

    private void givenHostnameConfig(String config)
    {
        givenConfiguration.put(OIDC_PROPERTY_NAME, config);
    }

    private void givenPrefixConfig(String name, String config)
    {
        givenConfiguration.put(OIDC_PROPERTY_OP_PREFIX+name, config);
    }

    private Set<Principal> whenOidcPluginCalledWith(JsonNode discoveryDoc,
                                                    JsonNode userInfo,
                                                    BearerTokenCredential token)
            throws ExecutionException, IOException, AuthenticationException
    {
        OidcAuthPlugin plugin = new OidcAuthPlugin(givenConfiguration, httpClient);

        Mockito.doReturn(discoveryDoc).when(httpClient).doGet(Mockito.any(URI.class));
        Mockito.doReturn(userInfo).when(httpClient).doGetWithToken(Mockito.anyString(), Mockito.anyString());

        Set<Object> priv = new HashSet<>();
        Set<Principal> principals = new HashSet<>();
        if (token != null) {
            priv.add(token);
        }

        plugin.authenticate(new HashSet<>(), priv, principals);

        plugin.stop();
        return principals;
    }

    private void whenOidcPluginCreated()
    {
        OidcAuthPlugin plugin = new OidcAuthPlugin(givenConfiguration, httpClient);
        plugin.stop();
    }

    private void whenOidcPluginCalledWithNoCredentials() throws AuthenticationException
    {
        whenPluginCreated();
    }

    private Set<Principal> whenOidcPluginCalledWith(JsonNode discoveryDoc,
                                                    IOException e,
                                                    BearerTokenCredential token)
            throws ExecutionException, IOException, AuthenticationException
    {

        Mockito.doReturn(discoveryDoc).when(httpClient).doGet(Mockito.any(URI.class));
        Mockito.doThrow(e).when(httpClient).doGetWithToken(Mockito.anyString(), Mockito.anyString());

        return whenPluginCreated(token);
    }

    private Set<Principal> whenOidcPluginCalledWith(IOException e,
                                                    JsonNode userInfo,
                                                    BearerTokenCredential token)
            throws ExecutionException, IOException, AuthenticationException, OidcException
    {
        Mockito.doThrow(e).when(httpClient).doGet(Mockito.any(URI.class));
        Mockito.doReturn(userInfo).when(httpClient).doGetWithToken(Mockito.anyString(), Mockito.anyString());

        return whenPluginCreated(token);
    }

    private Set<Principal> whenPluginCreated() throws AuthenticationException
    {
        return this.whenPluginCreated(null);
    }

    private Set<Principal> whenPluginCreated(BearerTokenCredential token) throws AuthenticationException
    {
        OidcAuthPlugin plugin = new OidcAuthPlugin(givenConfiguration, httpClient);
        Set<Object> priv = new HashSet<>();
        Set<Principal> principals = new HashSet<>();
        if (token != null) {
            priv.add(token);
        }

        plugin.authenticate(new HashSet<>(), priv, principals);

        plugin.stop();
        return principals;
    }

    private IOException withIOException()
    {
        return Mockito.mock(IOException.class);
    }

    private ExecutionException withExecutionException()
    {
        return Mockito.mock(ExecutionException.class);
    }

    private JsonNode withDiscoveryDoc(String json) throws IOException
    {
        return new ObjectMapper().readTree(json);
    }

    private JsonNode withUserInfo(String json) throws IOException
    {
        return new ObjectMapper().readTree(json);
    }

    private BearerTokenCredential withBearerToken(String token) throws IOException
    {
        return (token == null) ? null : new BearerTokenCredential(token);
    }

    public static Matcher<Iterable<? super OidcSubjectPrincipal>> hasSubject(String dn)
    {
        return hasItem(new OidcSubjectPrincipal(dn));
    }

    public static Matcher<Iterable<? super EmailAddressPrincipal>> hasEmail(String email)
    {
        return hasItem(new EmailAddressPrincipal(email));
    }

    public static Matcher<Iterable<? super OpenIdGroupPrincipal>> hasGroup(String group)
    {
        return hasItem(new OpenIdGroupPrincipal(group));
    }

    public static Matcher<Iterable<? super LoAPrincipal>> hasLoA(LoA loa)
    {
        return hasItem(new LoAPrincipal(loa));
    }

    public static Matcher<Iterable<? super FullNamePrincipal>> hasFullName(String givenName,
                                                                           String familyName, String fullName)
    {
        if (fullName != null && !fullName.isEmpty()) {
            return hasItem(new FullNamePrincipal(fullName));
        } else {
            return hasItem(new FullNamePrincipal(givenName, familyName));
        }
    }
}