package org.dcache.tests.poolmanager;


import org.junit.Test;

import diskCacheV111.pools.PoolV2Mode;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class PoolModeTest {



    @Test
    public void testEnabledDisable() {

        PoolV2Mode poolMode = new PoolV2Mode(PoolV2Mode.DISABLED);

        assertTrue("Disabled pool should be return true on isDisabled", poolMode.isDisabled());
        assertFalse("Disabled pool should be return false on isEnabled", poolMode.isEnabled());

        poolMode.setMode(PoolV2Mode.ENABLED);

        assertTrue("Enabled pool should be return true on isEnabled", poolMode.isEnabled());
        assertFalse("Enabled pool should be return false on isDisabled", poolMode.isDisabled());

    }


    @Test
    public void testEqualsDisable() {

        PoolV2Mode poolMode1 = new PoolV2Mode(PoolV2Mode.DISABLED);
        PoolV2Mode poolMode2 = new PoolV2Mode(PoolV2Mode.DISABLED);

        assertTrue("Two DISABLED modes have to be equal", poolMode1.equals(poolMode2));

    }

    @Test
    public void testEqualsEnable() {

        PoolV2Mode poolMode1 = new PoolV2Mode(PoolV2Mode.ENABLED);
        PoolV2Mode poolMode2 = new PoolV2Mode(PoolV2Mode.ENABLED);

        assertTrue("Two ENABLED modes have to be equal", poolMode1.equals(poolMode2));

    }

    @Test
    public void testEqualsMix() {

        PoolV2Mode poolMode1 = new PoolV2Mode(PoolV2Mode.DISABLED);
        PoolV2Mode poolMode2 = new PoolV2Mode(PoolV2Mode.ENABLED);

        assertFalse("DISABLED and ENABLED modes can't be equal", poolMode1.equals(poolMode2));
    }


    @Test
    public void testPartialDisable() {

        PoolV2Mode poolMode = new PoolV2Mode(PoolV2Mode.DISABLED_RDONLY);

        assertFalse("READ-ONLY disabled have return false on isDisabled(FETCH)", poolMode.isDisabled(PoolV2Mode.DISABLED_FETCH));
        assertTrue("READ-ONLY disabled have return true on isDisabled(STAGE)", poolMode.isDisabled(PoolV2Mode.DISABLED_STAGE));
        assertTrue("READ-ONLY disabled have return true on isDisabled(STORE)", poolMode.isDisabled(PoolV2Mode.DISABLED_STORE));
        assertTrue("READ-ONLY disabled have return true on isDisabled(P2P_CLIENT)", poolMode.isDisabled(PoolV2Mode.DISABLED_P2P_CLIENT));
        assertFalse("READ-ONLY disabled have return false on isDisabled(P2P_SERVER)", poolMode.isDisabled(PoolV2Mode.DISABLED_P2P_SERVER) );

        assertTrue("READ-ONLY disabled have return true on isDisabled()", poolMode.isDisabled() );
        assertFalse("READ-ONLY disabled have return false on isEnabled()", poolMode.isEnabled() );
    }

    @Test
    public void testStrictDisable() {

        PoolV2Mode poolMode = new PoolV2Mode(PoolV2Mode.DISABLED_STRICT);

        assertTrue("STRICT disabled have return true on isDisabled(FETCH)", poolMode.isDisabled(PoolV2Mode.DISABLED_FETCH));
        assertTrue("STRICT disabled have return true on isDisabled(STAGE)", poolMode.isDisabled(PoolV2Mode.DISABLED_STAGE));
        assertTrue("STRICT disabled have return true on isDisabled(STORE)", poolMode.isDisabled(PoolV2Mode.DISABLED_STORE));
        assertTrue("STRICT disabled have return true on isDisabled(P2P_CLIENT)", poolMode.isDisabled(PoolV2Mode.DISABLED_P2P_CLIENT));
        assertTrue("STRICT disabled have return true on isDisabled(P2P_SERVER)", poolMode.isDisabled(PoolV2Mode.DISABLED_P2P_SERVER) );

        assertTrue("STRICT disabled have return true on isDisabled()", poolMode.isDisabled() );
        assertFalse("STRICT disabled have return false on isEnabled()", poolMode.isEnabled() );
    }


}
