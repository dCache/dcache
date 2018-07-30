package org.dcache.gplazma.plugins;

import com.google.common.io.Resources;
import junit.framework.AssertionFailedError;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.Collection;
import java.util.OptionalLong;

import org.dcache.gplazma.plugins.AuthzMapLineParser.UserAuthzInformation;

public class CachedAuthzMapTest
{
    public static final String VALID_USERNAME_RESPONSE = "tigran";
    public static final String INVALID_USERNAME = "SomeInvalidUser";

    public static final int VALID_USERNAME_UID = 3750;
    public static final int VALID_USERNAME_GID = 500;
    public static final int INVALID_GID = 666;
    public static final int INVALID_UID = 666;

    private final static URL TEST_FIXTURE =
        Resources.getResource("org/dcache/gplazma/plugins/authzdb-parser.fixture");

    private SourceBackedPredicateMap<String,UserAuthzInformation>
        loadFixture(URL fixture)
        throws IOException
    {
        return new SourceBackedPredicateMap<>(new MemoryLineSource(Resources.readLines(fixture, Charset.defaultCharset())), new AuthzMapLineParser());
    }

    @Test
    public void testValidUsername()
        throws IOException
    {
        Collection<UserAuthzInformation> results =
            loadFixture(TEST_FIXTURE).getValuesForPredicatesMatching(VALID_USERNAME_RESPONSE);

        assertCollectionContains(results, new UserAuthzInformation(
                VALID_USERNAME_RESPONSE,
                "read-write",
                VALID_USERNAME_UID,
                new long[] { VALID_USERNAME_GID },
                "/ fff/fff/!@# $% /",
                "/",
                null,
                OptionalLong.empty()));

        assertCollectionContainsNot(results, new UserAuthzInformation(
                VALID_USERNAME_RESPONSE,
                null,
                INVALID_UID,
                new long[] { -1 },
                null,
                null,
                null,
                OptionalLong.empty()));
    }

    @Test
    public void testInvalidUsername()
        throws IOException
    {
        Collection<UserAuthzInformation> results =
            loadFixture(TEST_FIXTURE).getValuesForPredicatesMatching(INVALID_USERNAME);

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
