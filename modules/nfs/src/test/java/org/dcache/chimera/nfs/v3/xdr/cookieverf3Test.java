package org.dcache.chimera.nfs.v3.xdr;

import org.junit.Test;
import static org.junit.Assert.*;

public class cookieverf3Test {

    @Test
    public void testEquals() {
        byte[] bytes = "verifier".getBytes();
        cookieverf3 verf1 = new cookieverf3(bytes);
        cookieverf3 verf2 = new cookieverf3(bytes);

        assertTrue(verf1.equals(verf2));
        assertTrue(verf1.hashCode() == verf2.hashCode());
    }

    @Test
    public void testNotEquals() {
        byte[] bytes1 = "verifier1".getBytes();
        byte[] bytes2 = "verifier2".getBytes();

        cookieverf3 verf1 = new cookieverf3(bytes1);
        cookieverf3 verf2 = new cookieverf3(bytes2);

        assertFalse(verf1.equals(verf2));
        // we dont chech for hashCode equivalence as it's not enforced by JAVA
    }
}
