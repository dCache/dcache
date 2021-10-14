package org.dcache.util;

import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.hamcrest.Matchers.emptyArray;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import org.junit.Test;

public class CryptoTest {

    @Test
    public void testGetBannedCipherSuitesFromConfigurationValueWithEmptyString() {
        assertThat(Crypto.getBannedCipherSuitesFromConfigurationValue(""), is(emptyArray()));
    }

    @Test
    public void testGetBannedCipherSuitesFromConfigurationValueWithSingleValue() {
        assertThat(Crypto.getBannedCipherSuitesFromConfigurationValue("DISABLE_EC"),
              is(arrayContainingInAnyOrder(Crypto.EC_CIPHERS.toArray())));
    }

    @Test
    public void testGetBannedCipherSuitesFromConfigurationValueWithWhiteSpace() {
        assertThat(Crypto.getBannedCipherSuitesFromConfigurationValue("   DISABLE_EC   "),
              is(arrayContainingInAnyOrder(Crypto.EC_CIPHERS.toArray())));
    }
}
