package org.dcache.tests.util;

import org.junit.Test;

import diskCacheV111.util.DCapProrocolChallenge;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class DcapProtocolChallengeTest {


    @Test
    public void testEqualsOk() {

        int id = 17;
        byte[] challenge = "DcapProtocolChallengeTest".getBytes();


        DCapProrocolChallenge challenge1 = new DCapProrocolChallenge(id, challenge);
        DCapProrocolChallenge challenge2 = new DCapProrocolChallenge(id, challenge);

        assertTrue("Challenges not equal", challenge1.equals(challenge2));

    }


    @Test
    public void testEqualsDifferID() {

        int id1 = 17;
        int id2 = 19;
        byte[] challenge = "DcapProtocolChallengeTest".getBytes();


        DCapProrocolChallenge challenge1 = new DCapProrocolChallenge(id1, challenge);
        DCapProrocolChallenge challenge2 = new DCapProrocolChallenge(id2, challenge);

        assertFalse("Challenges with differ id can't be equal", challenge1.equals(challenge2));

    }


    @Test
    public void testEqualsDifferChallenge() {

        int id = 17;
        byte[] challenge1 = "DcapProtocolChallengeTest".getBytes();
        byte[] challenge2 = "differ".getBytes();

        DCapProrocolChallenge pChallenge1 = new DCapProrocolChallenge(id, challenge1);
        DCapProrocolChallenge pChallenge2 = new DCapProrocolChallenge(id, challenge2);

        assertFalse("Challenges with differ challenge can't be equal", pChallenge1.equals(pChallenge2));

    }

}
