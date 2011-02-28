package org.dcache.gplazma.plugins;

import java.io.IOException;
import java.security.Principal;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;

import junit.framework.AssertionFailedError;

import org.dcache.auth.FQANPrincipal;
import org.dcache.auth.GidPrincipal;
import org.dcache.auth.UidPrincipal;
import org.dcache.auth.UserNamePrincipal;
import org.dcache.gplazma.AuthenticationException;
import org.globus.gsi.jaas.GlobusPrincipal;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;

public class GPlazmaVORolePluginTest {

    private static final String USERNAME_KLAUS = "klaus";
    private static final String USERNAME_HORST = CachedMapsProvider.VALID_WC_USERNAME_RESPONSE;
    private static final String USERNAME_DTEAMUSER = CachedMapsProvider.VALID_ROLE_WC_USERNAME_RESPONSE;
    private static final String USERNAME_TIGRAN = CachedMapsProvider.VALID_USERNAME_RESPONSE;

    private static final Principal VALID_DN_PRINCIPAL = new GlobusPrincipal(CachedMapsProvider.VALID_DN);
    private static final Principal VALID_DN_PRINCIPAL_2 = new GlobusPrincipal("/O=GermanGrid/OU=DESY/CN=Klaus Maus");
    private static final Principal INVALID_DN_PRINCIPAL = new GlobusPrincipal("/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=flavia/CN=388195/CN=Flavia Donno");
    private static final Principal VALID_ROLE_PRINCIPAL = new FQANPrincipal(CachedMapsProvider.VALID_FQAN_LONG_ROLE, true);
    private static final Principal VALID_ROLE_PRINCIPAL_2 = new FQANPrincipal(CachedMapsProvider.VALID_FQAN_SHORT_ROLE, true);
    private static final Principal VALID_ROLE_PRINCIPAL_3 = new FQANPrincipal("/cms/Role=NULL/Capability=NULL", true);
    private static final Principal INVALID_ROLE_PRINCIPAL = new FQANPrincipal("/invalid/ROLE=NULL", true);
    private static final Principal TOTAL_NONSENSE_PRINCIPAL  = new FQANPrincipal("TotalNonsense!", true);

    private static final Principal DUMMY_PRINCIPAL = new UidPrincipal(CachedMapsProvider.INVALID_UID);

    private static Set<Principal> ValidAuthorisationPrincipals;
    private static Set<Principal> ValidAuthorisationSinglePrincipal;
    private static Set<Principal> InvalidRolePrincipals;
    private static Set<Principal> NonsensePrincipals;
    private static Set<Principal> ValidMappingPrincipals;

    @SuppressWarnings("unchecked")
    @Before
    public void setUp() {
        ValidAuthorisationPrincipals = new HashSet<Principal>(Arrays.asList(
                DUMMY_PRINCIPAL,
                VALID_DN_PRINCIPAL,
                VALID_DN_PRINCIPAL_2,
                VALID_ROLE_PRINCIPAL,
                DUMMY_PRINCIPAL));

        ValidAuthorisationSinglePrincipal = new HashSet<Principal>(Arrays.asList(
                VALID_ROLE_PRINCIPAL_2, VALID_DN_PRINCIPAL));

        InvalidRolePrincipals = new HashSet<Principal>(Arrays.asList(DUMMY_PRINCIPAL ,VALID_DN_PRINCIPAL, INVALID_ROLE_PRINCIPAL));
        NonsensePrincipals = new HashSet<Principal>(Arrays.asList(DUMMY_PRINCIPAL, TOTAL_NONSENSE_PRINCIPAL));
        ValidMappingPrincipals = new HashSet<Principal>(Arrays.asList(
                new FQANPrincipal("Some FQAN Principal", false),
                new UserNamePrincipal(USERNAME_TIGRAN),
                new UserNamePrincipal(USERNAME_DTEAMUSER),
                new UserNamePrincipal(USERNAME_HORST),
                new UserNamePrincipal(USERNAME_KLAUS)
                ));
    }

    @After
    public void tearDown() {
        ValidAuthorisationPrincipals = null;
        ValidAuthorisationSinglePrincipal = null;
        InvalidRolePrincipals = null;
        NonsensePrincipals = null;
        ValidMappingPrincipals = null;
    }

    @Test(expected=AuthenticationException.class)
    public void testAuthenticateValidArgsWithAllNullParams() throws AuthenticationException, IOException {

        GPlazmaVORolePlugin plugin = new GPlazmaVORolePlugin(CachedMapsProvider.createCachedVOMapWithWildcards(), CachedMapsProvider.createCachedAuthzMap());

        plugin.authenticate(null, null, null, null);
    }

    @Test
    public void testAuthenticateSinglePrincipalWithValidDNValidRole() throws AuthenticationException, IOException {

        GPlazmaVORolePlugin plugin = new GPlazmaVORolePlugin(CachedMapsProvider.createCachedVOMapWithWildcards(), CachedMapsProvider.createCachedAuthzMap());

        plugin.authenticate(null, new HashSet<Object>(), new HashSet<Object>(), ValidAuthorisationSinglePrincipal );

        AssertContainsPrincipal(ValidAuthorisationSinglePrincipal, new UserNamePrincipal(USERNAME_TIGRAN));
    }

    @Test
    public void testAuthenticateMultiplePrincipalsWithValidDNValidRole() throws AuthenticationException, IOException {

        GPlazmaVORolePlugin plugin = new GPlazmaVORolePlugin(CachedMapsProvider.createCachedVOMapWithWildcards(), CachedMapsProvider.createCachedAuthzMap());

        plugin.authenticate(null, new HashSet<Object>(), new HashSet<Object>(), ValidAuthorisationPrincipals );

        AssertContainsPrincipal(ValidAuthorisationPrincipals, new UserNamePrincipal(USERNAME_TIGRAN));
    }

    /**
     * Tests throwing of AuthenticationException if no matching combination in vorolemap exists. Uses vorolemap file without wildcards
     * @throws AuthenticationException
     * @throws IOException
     */
    @Test
    public void testAuthenticateWithInvalidDNValidRole() throws AuthenticationException, IOException {

        GPlazmaVORolePlugin plugin = new GPlazmaVORolePlugin(CachedMapsProvider.createCachedVOMap(), CachedMapsProvider.createCachedAuthzMap());
        LinkedHashSet<Principal> identifiedPrincipals = new LinkedHashSet<Principal>(Arrays.asList(VALID_ROLE_PRINCIPAL, VALID_ROLE_PRINCIPAL_3));

        try {
            plugin.authenticate(null, new HashSet<Object>(), new HashSet<Object>(), identifiedPrincipals );
            Assert.fail("FAIL: Expected AuthenticationException not raised for invalid DN.");
        } catch (AuthenticationException e){
            Assert.assertEquals(2, identifiedPrincipals.size());
        }
    }

    /**
     * similar to the test above, but with valid dn and invalid role
     * @throws AuthenticationException
     * @throws IOException
     */
    @Test
    public void testAuthenticateWithValidDNInvalidRole() throws AuthenticationException, IOException {

        GPlazmaVORolePlugin plugin = new GPlazmaVORolePlugin(CachedMapsProvider.createCachedVOMap(), CachedMapsProvider.createCachedAuthzMap());

        try {
            plugin.authenticate(null, new HashSet<Object>(), new HashSet<Object>(), InvalidRolePrincipals );
            Assert.fail("FAIL: Expected AuthenticationException not raised for invalid role.");
        } catch (AuthenticationException e){
            Assert.assertEquals(3, InvalidRolePrincipals.size());
        }
    }

    /**
     * The "invalid" DN/Role is only matched against the "* horst" entry
     * @throws AuthenticationException
     * @throws IOException
     */
    @Test
    public void testAuthenticateWithInvalidDNInvalidRole() throws AuthenticationException, IOException {

        GPlazmaVORolePlugin plugin = new GPlazmaVORolePlugin(CachedMapsProvider.createCachedVOMapWithWildcards(), CachedMapsProvider.createCachedAuthzMap());
        LinkedHashSet<Principal> identifiedPrincipals = new LinkedHashSet<Principal>(Arrays.asList(INVALID_DN_PRINCIPAL, INVALID_ROLE_PRINCIPAL));

        plugin.authenticate(null, new HashSet<Object>(), new HashSet<Object>(), identifiedPrincipals );

        Assert.assertEquals(3, identifiedPrincipals.size());
        AssertContainsPrincipal(identifiedPrincipals, new UserNamePrincipal(USERNAME_HORST));
    }

    /**
     * This principal is not even matched by the wildcard entries because of the '!'.
     * @throws AuthenticationException
     * @throws IOException
     */
    @Test
    public void testAuthenticateWithNonsensePrincipalWithWC() throws AuthenticationException, IOException {

        GPlazmaVORolePlugin plugin = new GPlazmaVORolePlugin(CachedMapsProvider.createCachedVOMapWithWildcards(), CachedMapsProvider.createCachedAuthzMap());

        try {
            plugin.authenticate(null, new HashSet<Object>(), new HashSet<Object>(), NonsensePrincipals);
            Assert.fail("FAIL: Expected AuthenticationException not raised for invalid principals.");
        } catch (AuthenticationException e) {
            Assert.assertEquals(2, NonsensePrincipals.size());
        }
    }

    @Test
    public void testMapWithAllEmptyParamsWithValidPluginArgs() throws AuthenticationException, IOException {

        GPlazmaVORolePlugin plugin = new GPlazmaVORolePlugin(CachedMapsProvider.createCachedVOMap(), CachedMapsProvider.createCachedAuthzMap());
        HashSet<Principal> authorizedPrincipals = new HashSet<Principal>();
        HashSet<Principal> principals = new HashSet<Principal>();

        plugin.map(null, principals, authorizedPrincipals);

        Assert.assertTrue(principals.isEmpty());
    }

    @Test
    public void testMapWithValidParams() throws AuthenticationException, IOException {

        GPlazmaVORolePlugin plugin = new GPlazmaVORolePlugin(CachedMapsProvider.createCachedVOMap(), CachedMapsProvider.createCachedAuthzMap());

        Set<Principal> authorizedPrincipals = new HashSet<Principal>();

        plugin.map(null, ValidMappingPrincipals, authorizedPrincipals);

        @SuppressWarnings("unchecked")
        Set<Principal> expectedPrincipalsSet = new HashSet<Principal>(Arrays.asList(
                new UidPrincipal(CachedMapsProvider.VALID_USERNAME_UID),
                new GidPrincipal(CachedMapsProvider.VALID_USERNAME_GID, true),
                new UidPrincipal(1001),
                new GidPrincipal(101, true)));

        Assert.assertEquals(expectedPrincipalsSet, authorizedPrincipals);
    }


    private void AssertContainsPrincipal(Collection<Principal> principals, final Principal expected) {

        Collection<Principal> result = Collections2.filter(principals, new Predicate<Principal>() {

            @Override
            public boolean apply(Principal arg0) {
                return expected.equals(arg0);
            }
        });

        if (result == null || result.isEmpty())
            throw new AssertionFailedError(String.format("Principal %s does not exist in collection %s", expected, principals));
    }

}
