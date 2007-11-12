package org.dcache.tests.poolmanager;


import static org.junit.Assert.*;

import org.junit.Test;

import diskCacheV111.pools.PoolV2Mode;

public class PoolModeTest {



    @Test
    public void testEnabledDisable() {

        PoolV2Mode poolMode = new PoolV2Mode(PoolV2Mode.DISABLED);

        assertTrue("Disabled pool should be return true on isDisabled", poolMode.isDisabled());
        assertFalse("Disabled pool should be return false on isEnabled", poolMode.isEnabled());

        poolMode.setMode(PoolV2Mode.ENABLED);

        assertTrue("Enabled pool should be return true on isEnabled", poolMode.isDisabled());
        assertFalse("Enabled pool should be return false on isDisabled", poolMode.isEnabled());

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

}
