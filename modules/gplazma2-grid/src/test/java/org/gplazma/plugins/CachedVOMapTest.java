package org.dcache.gplazma.plugins;

import gplazma.authz.util.NameRolePair;

import com.google.common.io.Resources;
import com.google.common.collect.Lists;

import java.net.URL;
import java.nio.charset.Charset;
import java.util.Collection;
import java.io.IOException;

import static junit.framework.Assert.*;

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
    public static final String DN_ANDREJ = "/C=SI/O=SiGNET/O=IJS/OU=F9/CN=Andrej Filipcic";
    public static final String FQAN_ATLAS_PROD = "/atlas/Role=production";

    private final static URL TEST_FIXTURE_WITH_WILDCARDS =
        Resources.getResource("org/dcache/gplazma/plugins/vorolemap-wildcard.fixture");
    private final static URL TEST_FIXTURE_WITHOUT_WILDCARDS =
        Resources.getResource("org/dcache/gplazma/plugins/vorolemap-no-wildcard.fixture");
    private final static URL TEST_FIXTURE_NDGF =
        Resources.getResource("org/dcache/gplazma/plugins/vorolemap-ndgf.fixture");

    private SourceBackedPredicateMap<NameRolePair, String>
        loadFixture(URL fixture)
        throws IOException
    {
        return new SourceBackedPredicateMap<NameRolePair,String>(new MemoryLineSource(Resources.readLines(fixture, Charset.defaultCharset())), new VOMapLineParser());
    }

    @Test
    public void testContainsFullDNFQAN()
        throws IOException
    {
        Collection<String> mappedNames = loadFixture(TEST_FIXTURE_WITHOUT_WILDCARDS).getValuesForPredicatesMatching(new NameRolePair(VALID_DN, VALID_FQAN_LONG_ROLE ));
        assertTrue(mappedNames.contains(VALID_USERNAME_RESPONSE));
    }

    @Test
    public void testContainsFullDNFQANWithWC()
        throws IOException
    {
        Collection<String> mappedNames = loadFixture(TEST_FIXTURE_WITH_WILDCARDS).getValuesForPredicatesMatching(new NameRolePair(VALID_DN, VALID_FQAN_LONG_ROLE));
        assertTrue(mappedNames.contains(VALID_USERNAME_RESPONSE));
        assertTrue(mappedNames.contains(VALID_ROLE_WC_USERNAME_RESPONSE));
        assertTrue(mappedNames.contains(VALID_WC_USERNAME_RESPONSE));
    }

    @Test
    public void testContainsFullDNShortFQAN()
        throws IOException
    {
        Collection<String> mappedNames = loadFixture(TEST_FIXTURE_WITHOUT_WILDCARDS).getValuesForPredicatesMatching(new NameRolePair(VALID_DN, VALID_FQAN_SHORT_ROLE));
        assertTrue(mappedNames.contains(VALID_USERNAME_RESPONSE));
    }

    @Test
    public void testContainsFullDNShortFQANWithWC()
        throws IOException
    {
        Collection<String> mappedNames = loadFixture(TEST_FIXTURE_WITH_WILDCARDS).getValuesForPredicatesMatching(new NameRolePair(VALID_DN, VALID_FQAN_SHORT_ROLE));
        assertTrue(mappedNames.contains(VALID_USERNAME_RESPONSE));
        assertTrue(mappedNames.contains(VALID_ROLE_WC_USERNAME_RESPONSE));
        assertTrue(mappedNames.contains(VALID_WC_USERNAME_RESPONSE));
    }

    @Test
    public void testDoesContainsWildcardResponseForInvalidUsernameWithWildcards()
        throws IOException
    {
        Collection<String> mappedNames = loadFixture(TEST_FIXTURE_WITH_WILDCARDS).getValuesForPredicatesMatching(new NameRolePair( INVALID_USERNAME, ""));
        assertTrue(mappedNames.contains(VALID_WC_USERNAME_RESPONSE));
    }

    @Test
    public void testIsEmptyForInvalidUsernameWithoutWildcards()
        throws IOException
    {
        Collection<String> mappedNames = loadFixture(TEST_FIXTURE_WITHOUT_WILDCARDS).getValuesForPredicatesMatching(new NameRolePair(INVALID_USERNAME, ""));
        assertTrue(mappedNames.isEmpty());
    }

    @Test
    public void testUsernameWithDash()
        throws IOException
    {
        Collection<String> mappedNames =
            loadFixture(TEST_FIXTURE_NDGF).getValuesForPredicatesMatching(new NameRolePair(DN_ANDREJ, FQAN_ATLAS_PROD));
        assertEquals(Lists.newArrayList("atlas-prod"), mappedNames);
    }
}
