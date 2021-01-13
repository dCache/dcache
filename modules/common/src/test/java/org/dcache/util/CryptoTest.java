package org.dcache.util;

import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

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
                is(arrayContainingInAnyOrder(Crypto.EC_CIPHERS.toArray())));
    }

    @Test
    public void testGetBannedCipherSuitesFromConfigurationValueWithWhiteSpace()
    {
        assertThat(Crypto.getBannedCipherSuitesFromConfigurationValue("   DISABLE_EC   "),
                is(arrayContainingInAnyOrder(Crypto.EC_CIPHERS.toArray())));
    }
}
