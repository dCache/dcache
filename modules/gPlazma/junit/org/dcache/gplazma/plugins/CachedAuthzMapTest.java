package org.dcache.gplazma.plugins;

import java.util.Collection;

import junit.framework.AssertionFailedError;

import org.junit.Assert;
import org.junit.Test;


public class CachedAuthzMapTest {

    @Test
    public void testValidUsername() {

        Collection<AuthzMapLineParser.UserAuthzInformation> results = CachedMapsProvider.createCachedAuthzMap().getValuesForPredicatesMatching(CachedMapsProvider.VALID_USERNAME_RESPONSE);

        assertCollectionContains(results, (new AuthzMapLineParser()).new UserAuthzInformation(
                null,
                null,
                CachedMapsProvider.VALID_USERNAME_UID,
                CachedMapsProvider.VALID_USERNAME_GID,
                null,
                null,
                null));

        assertCollectionContainsNot(results, (new AuthzMapLineParser()).new UserAuthzInformation(
                CachedMapsProvider.VALID_USERNAME_RESPONSE,
                null,
                CachedMapsProvider.INVALID_UID,
                -1,
                null,
                null,
                null));
    }

    @Test
    public void testInvalidUsername() {

        Collection<AuthzMapLineParser.UserAuthzInformation> results = CachedMapsProvider.createCachedAuthzMap().getValuesForPredicatesMatching(CachedMapsProvider.INVALID_USERNAME);

        Assert.assertTrue(results.isEmpty());
    }

    private void assertCollectionContains(Collection<AuthzMapLineParser.UserAuthzInformation> collection, AuthzMapLineParser.UserAuthzInformation userInfo) {
        for (AuthzMapLineParser.UserAuthzInformation userAuthzInformation : collection) {
            if (userInfo.equals(userAuthzInformation)) return;
        }
        throw new AssertionFailedError("Collection did not contain AuthzUserInfo: " + userInfo );
    }

    private void assertCollectionContainsNot(Collection<AuthzMapLineParser.UserAuthzInformation> collection, AuthzMapLineParser.UserAuthzInformation userInfo) {
        for (AuthzMapLineParser.UserAuthzInformation userAuthzInformation : collection) {
            if (userInfo.equals(userAuthzInformation)) throw new AssertionFailedError("Collection contains AuthzUserInfo: " + userInfo );
        }
    }

}
