package org.dcache.util;

import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;

public class CryptoTest
{
    @Test
    public void testGetBannedCipherSuitesFromConfigurationValueWithEmptyString()
    {
        assertThat(Crypto.getBannedCipherSuitesFromConfigurationValue(""), is(emptyArray()));
    }

    @Test
    public void testGetBannedCipherSuitesFromConfigurationValueWithSingleValue()
    {
        assertThat(Crypto.getBannedCipherSuitesFromConfigurationValue("DISABLE_EC"),
                is(arrayContainingInAnyOrder(Crypto.EC_CIPHERS)));
    }

    @Test
    public void testGetBannedCipherSuitesFromConfigurationValueWithWhiteSpace()
    {
        assertThat(Crypto.getBannedCipherSuitesFromConfigurationValue("   DISABLE_EC   "),
                is(arrayContainingInAnyOrder(Crypto.EC_CIPHERS)));
    }
}
