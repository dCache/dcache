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

    @Test
    public void testGetBannedCipherSuitesFromConfigurationValueWithMultipleValues() throws Exception
    {
        Crypto.setJavaVersion("1.7.0_20");
        assertThat(Crypto.getBannedCipherSuitesFromConfigurationValue("   DISABLE_EC , DISABLE_BROKEN_DH  "),
                is(arrayContainingInAnyOrder(concat(Crypto.EC_CIPHERS, Crypto.DH_CIPHERS))));
    }

    @Test
    public void testGetBannedCipherSuitesFromConfigurationValueWithEmptyValues() throws Exception
    {
        Crypto.setJavaVersion("1.7.0_20");
        assertThat(Crypto.getBannedCipherSuitesFromConfigurationValue("   DISABLE_EC , , DISABLE_BROKEN_DH  "),
                is(arrayContainingInAnyOrder(concat(Crypto.EC_CIPHERS, Crypto.DH_CIPHERS))));
    }

    @Test
    public void testGetBannedCipherSuitesFromConfigurationValueWithNonBrokenDH() throws Exception
    {
        Crypto.setJavaVersion("1.7.0_1");
        assertThat(Crypto.getBannedCipherSuitesFromConfigurationValue("   DISABLE_BROKEN_DH  "),
                is(emptyArray()));
    }

    private static String[] concat(String[]... arrays)
    {
        Set<String> result = new HashSet<>();
        for (String[] array: arrays) {
            result.addAll(asList(array));
        }
        return result.toArray(new String[result.size()]);
    }
}
