package org.dcache.gplazma.plugins;

import gplazma.authz.util.NameRolePair;

import com.google.common.io.Resources;

import java.net.URL;
import java.nio.charset.Charset;
import java.util.Collection;
import java.io.IOException;

import junit.framework.Assert;

import org.junit.Test;

public class CachedVOMapTest
{
    public static final String VALID_DN = "/O=GermanGrid/OU=DESY/CN=Tigran Mkrtchyan";
    public static final String VALID_FQAN_LONG_ROLE = "/dteam/Role=NULL/Capability=NULL";
    public static final String VALID_FQAN_SHORT_ROLE = "/dteam";
    public static final String VALID_USERNAME_RESPONSE = "tigran";
    public static final String VALID_ROLE_WC_USERNAME_RESPONSE = "dteamuser";
    public static final String VALID_WC_USERNAME_RESPONSE = "horst";
    public static final String INVALID_USERNAME = "SomeInvalidUser";

    private final static URL TEST_FIXTURE_WITH_WILDCARDS =
        Resources.getResource("org/dcache/gplazma/plugins/vorolemap-wildcard.fixture");
    private final static URL TEST_FIXTURE_WITHOUT_WILDCARDS =
        Resources.getResource("org/dcache/gplazma/plugins/vorolemap-no-wildcard.fixture");

    private SourceBackedPredicateMap<NameRolePair, String>
        loadFixture(URL fixture)
        throws IOException
    {
        return new SourceBackedPredicateMap(new MemoryLineSource(Resources.readLines(fixture, Charset.defaultCharset())), new VOMapLineParser());
    }

    @Test
    public void testContainsFullDNFQAN()
        throws IOException
    {
        Collection<String> mappedNames = loadFixture(TEST_FIXTURE_WITHOUT_WILDCARDS).getValuesForPredicatesMatching(new NameRolePair(VALID_DN, VALID_FQAN_LONG_ROLE ));
        Assert.assertTrue(mappedNames.contains(VALID_USERNAME_RESPONSE));
    }

    @Test
    public void testContainsFullDNFQANWithWC()
        throws IOException
    {
        Collection<String> mappedNames = loadFixture(TEST_FIXTURE_WITH_WILDCARDS).getValuesForPredicatesMatching(new NameRolePair(VALID_DN, VALID_FQAN_LONG_ROLE));
        Assert.assertTrue(mappedNames.contains(VALID_USERNAME_RESPONSE));
        Assert.assertTrue(mappedNames.contains(VALID_ROLE_WC_USERNAME_RESPONSE));
        Assert.assertTrue(mappedNames.contains(VALID_WC_USERNAME_RESPONSE));
    }

    @Test
    public void testContainsFullDNShortFQAN()
        throws IOException
    {
        Collection<String> mappedNames = loadFixture(TEST_FIXTURE_WITHOUT_WILDCARDS).getValuesForPredicatesMatching(new NameRolePair(VALID_DN, VALID_FQAN_SHORT_ROLE));
        Assert.assertTrue(mappedNames.contains(VALID_USERNAME_RESPONSE));
    }

    @Test
    public void testContainsFullDNShortFQANWithWC()
        throws IOException
    {
        Collection<String> mappedNames = loadFixture(TEST_FIXTURE_WITH_WILDCARDS).getValuesForPredicatesMatching(new NameRolePair(VALID_DN, VALID_FQAN_SHORT_ROLE));
        Assert.assertTrue(mappedNames.contains(VALID_USERNAME_RESPONSE));
        Assert.assertTrue(mappedNames.contains(VALID_ROLE_WC_USERNAME_RESPONSE));
        Assert.assertTrue(mappedNames.contains(VALID_WC_USERNAME_RESPONSE));
    }

    @Test
    public void testDoesContainsWildcardResponseForInvalidUsernameWithWildcards()
        throws IOException
    {
        Collection<String> mappedNames = loadFixture(TEST_FIXTURE_WITH_WILDCARDS).getValuesForPredicatesMatching(new NameRolePair( INVALID_USERNAME, ""));
        Assert.assertTrue(mappedNames.contains(VALID_WC_USERNAME_RESPONSE));
    }

    @Test
    public void testIsEmptyForInvalidUsernameWithoutWildcards()
        throws IOException
    {
        Collection<String> mappedNames = loadFixture(TEST_FIXTURE_WITHOUT_WILDCARDS).getValuesForPredicatesMatching(new NameRolePair(INVALID_USERNAME, ""));
        Assert.assertTrue(mappedNames.isEmpty());
    }
}
