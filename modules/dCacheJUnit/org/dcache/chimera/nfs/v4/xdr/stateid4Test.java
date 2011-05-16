package org.dcache.chimera.nfs.v4.xdr;

import org.junit.Test;
import static org.junit.Assert.*;


public class stateid4Test {

    @Test
    public void testEqualsTrue() {

        stateid4 stateidA = new stateid4();
        stateidA.seqid = new uint32_t(1);
        stateidA.other = "state".getBytes();

        stateid4 stateidB = new stateid4();
        stateidB.seqid = new uint32_t(1);
        stateidB.other = "state".getBytes();

        assertTrue("equal keys not equal", stateidA.equals(stateidB));
        assertTrue("equal, but different hashCode", stateidA.hashCode() == stateidB.hashCode() );
    }

    @Test
    public void testEqualsSame() {

        stateid4 stateidA = new stateid4();
        stateidA.seqid = new uint32_t(1);
        stateidA.other = "state".getBytes();

        assertTrue("equal keys not equal", stateidA.equals(stateidA));
    }

    @Test
    public void testDifferSequence() {

        stateid4 stateidA = new stateid4();
        stateidA.seqid = new uint32_t(1);
        stateidA.other = "state".getBytes();

        stateid4 stateidB = new stateid4();
        stateidB.seqid = new uint32_t(2);
        stateidB.other = "state".getBytes();

        assertTrue("differ by sequence should still be equal", stateidA.equals(stateidB));
    }

    @Test
    public void testDifferOther() {

        stateid4 stateidA = new stateid4();
        stateidA.seqid = new uint32_t(1);
        stateidA.other = "stateA".getBytes();

        stateid4 stateidB = new stateid4();
        stateidB.seqid = new uint32_t(1);
        stateidB.other = "stateB".getBytes();

        assertFalse("differ by other not detected", stateidA.equals(stateidB));
    }
}
