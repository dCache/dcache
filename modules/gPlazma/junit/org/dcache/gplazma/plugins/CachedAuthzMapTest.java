package org.dcache.gplazma.plugins;

import java.util.Collection;

import junit.framework.AssertionFailedError;

import org.junit.Assert;
import org.junit.Test;

import org.dcache.gplazma.plugins.AuthzMapLineParser.UserAuthzInformation;
import static org.dcache.gplazma.plugins.CachedMapsProvider.*;

public class CachedAuthzMapTest
{
    @Test
    public void testValidUsername()
    {
        Collection<UserAuthzInformation> results =
            createCachedAuthzMap().getValuesForPredicatesMatching(VALID_USERNAME_RESPONSE);

        assertCollectionContains(results, new UserAuthzInformation(
                VALID_USERNAME_RESPONSE,
                "read-write",
                VALID_USERNAME_UID,
                new long[] { VALID_USERNAME_GID },
                "/ fff/fff/!@# $% /",
                "/",
                null));

        assertCollectionContainsNot(results, new UserAuthzInformation(
                VALID_USERNAME_RESPONSE,
                null,
                INVALID_UID,
                new long[] { -1 },
                null,
                null,
                null));
    }

    @Test
    public void testInvalidUsername()
    {
        Collection<UserAuthzInformation> results =
            createCachedAuthzMap().getValuesForPredicatesMatching(INVALID_USERNAME);

        Assert.assertTrue(results.isEmpty());
    }

    private void assertCollectionContains(Collection<UserAuthzInformation> collection, UserAuthzInformation userInfo)
    {
        if (!collection.contains(userInfo)) {
            throw new AssertionFailedError("Collection did not contain AuthzUserInfo: " + userInfo);
        }
    }

    private void assertCollectionContainsNot(Collection<UserAuthzInformation> collection, UserAuthzInformation userInfo)
    {
        if (collection.contains(userInfo)) {
            throw new AssertionFailedError("Collection contains AuthzUserInfo: " + userInfo);
        }
    }
}
