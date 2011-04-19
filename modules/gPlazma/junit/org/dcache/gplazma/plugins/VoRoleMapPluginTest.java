package org.dcache.gplazma.plugins;

import java.io.IOException;
import java.security.Principal;
import java.util.Arrays;
import java.util.Collection;
import java.util.Set;

import junit.framework.AssertionFailedError;

import org.dcache.auth.FQANPrincipal;
import org.dcache.auth.GroupNamePrincipal;
import org.dcache.auth.UidPrincipal;
import org.dcache.gplazma.AuthenticationException;
import org.globus.gsi.jaas.GlobusPrincipal;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

import com.google.common.collect.Sets;

import static org.dcache.gplazma.plugins.CachedMapsProvider.*;

public class VoRoleMapPluginTest
{
    private static final String USERNAME_KLAUS = "klaus";
    private static final String USERNAME_HORST = VALID_WC_USERNAME_RESPONSE;
    private static final String USERNAME_DTEAMUSER = VALID_ROLE_WC_USERNAME_RESPONSE;
    private static final String USERNAME_TIGRAN = VALID_USERNAME_RESPONSE;

    private static final Principal VALID_DN_PRINCIPAL =
        new GlobusPrincipal(VALID_DN);
    private static final Principal VALID_DN_PRINCIPAL_2 =
        new GlobusPrincipal("/O=GermanGrid/OU=DESY/CN=Klaus Maus");
    private static final Principal INVALID_DN_PRINCIPAL =
        new GlobusPrincipal("/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=flavia/CN=388195/CN=Flavia Donno");
    private static final Principal VALID_ROLE_PRINCIPAL =
        new FQANPrincipal(VALID_FQAN_LONG_ROLE, true);
    private static final Principal VALID_ROLE_PRINCIPAL_2 =
        new FQANPrincipal(VALID_FQAN_SHORT_ROLE, true);
    private static final Principal VALID_ROLE_PRINCIPAL_3 =
        new FQANPrincipal("/cms/Role=NULL/Capability=NULL", true);
    private static final Principal INVALID_ROLE_PRINCIPAL =
        new FQANPrincipal("/invalid/ROLE=NULL", true);
    private static final Principal TOTAL_NONSENSE_PRINCIPAL =
        new FQANPrincipal("TotalNonsense!", true);
    private static final Principal DUMMY_PRINCIPAL =
        new UidPrincipal(INVALID_UID);

    private Set<Principal> validAuthorisationPrincipals;
    private Set<Principal> validAuthorisationSinglePrincipal;
    private Set<Principal> invalidRolePrincipals;
    private Set<Principal> nonsensePrincipals;
    private Set<Principal> validMappingPrincipals;
    private Set<Principal> authorizedPrincipals;

    @SuppressWarnings("unchecked")
    @Before
    public void setUp()
    {
        validAuthorisationPrincipals =
            Sets.newHashSet(DUMMY_PRINCIPAL,
                            VALID_DN_PRINCIPAL,
                            VALID_DN_PRINCIPAL_2,
                            VALID_ROLE_PRINCIPAL,
                            DUMMY_PRINCIPAL);

        validAuthorisationSinglePrincipal =
            Sets.newHashSet(VALID_ROLE_PRINCIPAL_2, VALID_DN_PRINCIPAL);

        invalidRolePrincipals =
            Sets.newHashSet(DUMMY_PRINCIPAL ,VALID_DN_PRINCIPAL, INVALID_ROLE_PRINCIPAL);
        nonsensePrincipals =
            Sets.newHashSet(DUMMY_PRINCIPAL, TOTAL_NONSENSE_PRINCIPAL);
        validMappingPrincipals =
            Sets.newHashSet((Principal) new FQANPrincipal("Some FQAN Principal", false),
                            (Principal) new GroupNamePrincipal(USERNAME_TIGRAN),
                            (Principal) new GroupNamePrincipal(USERNAME_DTEAMUSER),
                            (Principal) new GroupNamePrincipal(USERNAME_HORST),
                            (Principal) new GroupNamePrincipal(USERNAME_KLAUS));
        authorizedPrincipals = Sets.newHashSet();
    }

    @After
    public void tearDown()
    {
        validAuthorisationPrincipals = null;
        validAuthorisationSinglePrincipal = null;
        invalidRolePrincipals = null;
        nonsensePrincipals = null;
        validMappingPrincipals = null;
        authorizedPrincipals = null;
    }

    @Test(expected=NullPointerException.class)
    public void testValidArgsWithAllNullParams()
        throws AuthenticationException, IOException
    {
        VoRoleMapPlugin plugin =
            new VoRoleMapPlugin(createCachedVOMapWithWildcards());
        plugin.map(null, null, null);
    }

    @Test
    public void testSinglePrincipalWithValidDNValidRole()
        throws AuthenticationException, IOException
    {
        VoRoleMapPlugin plugin =
            new VoRoleMapPlugin(createCachedVOMapWithWildcards());

        plugin.map(null,
                   validAuthorisationSinglePrincipal,
                   authorizedPrincipals);

        assertContainsPrincipal(validAuthorisationSinglePrincipal,
                                new GroupNamePrincipal(USERNAME_TIGRAN, true));
        assertContainsPrincipal(authorizedPrincipals,
                                new GroupNamePrincipal(USERNAME_TIGRAN, true));
    }

    @Test
    public void testMultiplePrincipalsWithValidDNValidRole()
        throws AuthenticationException, IOException
    {
        VoRoleMapPlugin plugin =
            new VoRoleMapPlugin(createCachedVOMapWithWildcards());

        plugin.map(null, validAuthorisationPrincipals, authorizedPrincipals);

        assertContainsPrincipal(validAuthorisationPrincipals,
                                new GroupNamePrincipal(USERNAME_TIGRAN, true));
        assertContainsPrincipal(authorizedPrincipals,
                                new GroupNamePrincipal(USERNAME_TIGRAN, true));
    }

    /**
     * Tests throwing of AuthenticationException if no matching
     * combination in vorolemap exists. Uses vorolemap file without
     * wildcards.
     *
     * @throws AuthenticationException
     * @throws IOException
     */
    @Test
    public void testWithInvalidDNValidRole()
        throws AuthenticationException, IOException
    {
        VoRoleMapPlugin plugin =
            new VoRoleMapPlugin(createCachedVOMap());
        Set<Principal> identifiedPrincipals =
            Sets.newHashSet(VALID_ROLE_PRINCIPAL, VALID_ROLE_PRINCIPAL_3);

        try {
            plugin.map(null, identifiedPrincipals, authorizedPrincipals);
            fail("FAIL: Expected AuthenticationException not raised for invalid DN.");
        } catch (AuthenticationException e){
            assertEquals(2, identifiedPrincipals.size());
        }
    }

    /**
     * Similar to testAuthenticationWithInvalidDNValidRole, but with
     * valid dn and invalid role.
     *
     * @throws AuthenticationException
     * @throws IOException
     */
    @Test
    public void testWithValidDNInvalidRole()
        throws AuthenticationException, IOException
    {
        VoRoleMapPlugin plugin =
            new VoRoleMapPlugin(createCachedVOMap());

        try {
            plugin.map(null, invalidRolePrincipals, authorizedPrincipals);
            fail("FAIL: Expected AuthenticationException not raised for invalid role.");
        } catch (AuthenticationException e){
            assertEquals(3, invalidRolePrincipals.size());
            assertEquals(0, authorizedPrincipals.size());
        }
    }

    /**
     * The "invalid" DN/Role is only matched against the "* horst" entry
     * @throws AuthenticationException
     * @throws IOException
     */
    @Test
    public void testWithInvalidDNInvalidRole()
        throws AuthenticationException, IOException
    {
        VoRoleMapPlugin plugin =
            new VoRoleMapPlugin(createCachedVOMapWithWildcards());
        Set<Principal> identifiedPrincipals =
            Sets.newHashSet(INVALID_DN_PRINCIPAL, INVALID_ROLE_PRINCIPAL);

        plugin.map(null, identifiedPrincipals, authorizedPrincipals);

        assertEquals(3, identifiedPrincipals.size());
        assertContainsPrincipal(identifiedPrincipals,
                                new GroupNamePrincipal(USERNAME_HORST, true));
        assertContainsPrincipal(authorizedPrincipals,
                                new GroupNamePrincipal(USERNAME_HORST, true));
    }

    /**
     * This principal is not even matched by the wildcard entries
     * because of the '!'.
     *
     * @throws AuthenticationException
     * @throws IOException
     */
    @Test
    public void testWithNonsensePrincipalWithWC()
        throws AuthenticationException, IOException
    {
        VoRoleMapPlugin plugin =
            new VoRoleMapPlugin(createCachedVOMapWithWildcards());

        try {
            plugin.map(null, nonsensePrincipals, authorizedPrincipals);
            fail("FAIL: Expected AuthenticationException not raised for invalid principals.");
        } catch (AuthenticationException e) {
            assertEquals(2, nonsensePrincipals.size());
        }
    }

    private void assertContainsPrincipal(Collection<Principal> principals,
                                         Principal expected)
    {
        if (!principals.contains(expected)) {
            throw new AssertionFailedError(String.format("Principal %s does not exist in collection %s", expected, principals));
        }
    }
}
