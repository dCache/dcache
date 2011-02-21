package org.dcache.gplazma.plugins;

import gplazma.authz.util.NameRolePair;

import java.util.Collection;

import junit.framework.Assert;

import org.junit.Test;

public class CachedVOMapTest {

    @Test
    public void testContainsFullDNFQAN() {
        Collection<String> mappedNames = CachedMapsProvider.createCachedVOMap().getValuesForPredicatesMatching(new NameRolePair(CachedMapsProvider.VALID_DN, CachedMapsProvider.VALID_FQAN_LONG_ROLE ));
        Assert.assertTrue(mappedNames.contains(CachedMapsProvider.VALID_USERNAME_RESPONSE));
    }

    @Test
    public void testContainsFullDNFQANWithWC() {
        Collection<String> mappedNames = CachedMapsProvider.createCachedVOMapWithWildcards().getValuesForPredicatesMatching(new NameRolePair(CachedMapsProvider.VALID_DN, CachedMapsProvider.VALID_FQAN_LONG_ROLE));
        Assert.assertTrue(mappedNames.contains(CachedMapsProvider.VALID_USERNAME_RESPONSE));
        Assert.assertTrue(mappedNames.contains(CachedMapsProvider.VALID_ROLE_WC_USERNAME_RESPONSE));
        Assert.assertTrue(mappedNames.contains(CachedMapsProvider.VALID_WC_USERNAME_RESPONSE));
    }

    @Test
    public void testContainsFullDNShortFQAN() {
        Collection<String> mappedNames = CachedMapsProvider.createCachedVOMap().getValuesForPredicatesMatching(new NameRolePair(CachedMapsProvider.VALID_DN, CachedMapsProvider.VALID_FQAN_SHORT_ROLE));
        Assert.assertTrue(mappedNames.contains(CachedMapsProvider.VALID_USERNAME_RESPONSE));
    }

    @Test
    public void testContainsFullDNShortFQANWithWC() {
        Collection<String> mappedNames = CachedMapsProvider.createCachedVOMapWithWildcards().getValuesForPredicatesMatching(new NameRolePair(CachedMapsProvider.VALID_DN, CachedMapsProvider.VALID_FQAN_SHORT_ROLE));
        Assert.assertTrue(mappedNames.contains(CachedMapsProvider.VALID_USERNAME_RESPONSE));
        Assert.assertTrue(mappedNames.contains(CachedMapsProvider.VALID_ROLE_WC_USERNAME_RESPONSE));
        Assert.assertTrue(mappedNames.contains(CachedMapsProvider.VALID_WC_USERNAME_RESPONSE));
    }

    @Test
    public void testDoesContainsWildcardResponseForInvalidUsernameWithWildcards() {
        Collection<String> mappedNames = CachedMapsProvider.createCachedVOMapWithWildcards().getValuesForPredicatesMatching(new NameRolePair( CachedMapsProvider.INVALID_USERNAME, ""));
        Assert.assertTrue(mappedNames.contains(CachedMapsProvider.VALID_WC_USERNAME_RESPONSE));
    }

    @Test
    public void testIsEmptyForInvalidUsernameWithoutWildcards() {
        Collection<String> mappedNames = CachedMapsProvider.createCachedVOMap().getValuesForPredicatesMatching(new NameRolePair(CachedMapsProvider.INVALID_USERNAME, ""));
        Assert.assertTrue(mappedNames.isEmpty());
    }

}
