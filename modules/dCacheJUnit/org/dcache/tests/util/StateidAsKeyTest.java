package org.dcache.tests.util;

import org.dcache.chimera.nfs.v4.stateid4;
import org.dcache.chimera.nfs.v4.uint32_t;
import org.dcache.chimera.nfsv41.door.NFSv41Door.StateidAsKey;
import org.junit.Test;
import static org.junit.Assert.*;


public class StateidAsKeyTest {

    @Test
    public void testEqualsTrue() {

        stateid4 stateidA = new stateid4();
        stateidA.seqid = new uint32_t(1);
        stateidA.other = "state".getBytes();

        stateid4 stateidB = new stateid4();
        stateidB.seqid = new uint32_t(1);
        stateidB.other = "state".getBytes();

        StateidAsKey keyA = new StateidAsKey(stateidA);
        StateidAsKey keyB = new StateidAsKey(stateidB);

        assertTrue("equal keys not equal", keyA.equals(keyB));
        assertTrue("equal, but different hashCode", keyA.hashCode() == keyB.hashCode() );
    }

    @Test
    public void testEqualsSame() {

        stateid4 stateidA = new stateid4();
        stateidA.seqid = new uint32_t(1);
        stateidA.other = "state".getBytes();

        StateidAsKey keyA = new StateidAsKey(stateidA);

        assertTrue("equal keys not equal", keyA.equals(keyA));
    }

    @Test
    public void testDifferSequence() {

        stateid4 stateidA = new stateid4();
        stateidA.seqid = new uint32_t(1);
        stateidA.other = "state".getBytes();

        stateid4 stateidB = new stateid4();
        stateidB.seqid = new uint32_t(2);
        stateidB.other = "state".getBytes();

        StateidAsKey keyA = new StateidAsKey(stateidA);
        StateidAsKey keyB = new StateidAsKey(stateidB);

        assertFalse("differ by sequence not detected", keyA.equals(keyB));
    }

    @Test
    public void testDifferOther() {

        stateid4 stateidA = new stateid4();
        stateidA.seqid = new uint32_t(1);
        stateidA.other = "stateA".getBytes();

        stateid4 stateidB = new stateid4();
        stateidB.seqid = new uint32_t(1);
        stateidB.other = "stateB".getBytes();

        StateidAsKey keyA = new StateidAsKey(stateidA);
        StateidAsKey keyB = new StateidAsKey(stateidB);

        assertFalse("differ by other not detected", keyA.equals(keyB));
    }
}
