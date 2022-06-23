package diskCacheV111.poolManager;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;

public class PoolSelectionUnitV2Test {

    private PoolSelectionUnitV2 psu;

    @Before
    public void setUp() {
        psu = new PoolSelectionUnitV2();
    }

    @Test
    public void testAddGroup() {
        psu.createPoolGroup("group", false);
        psu.getPoolGroups().containsKey("group");
    }

    @Test
    public void testAddPoolToGroup() {
        psu.createPoolGroup("group", false);
        psu.createPool("poolA", true, false, false);
        psu.addToPoolGroup("group", "poolA");

        assertTrue(psu.getPoolGroups().get("group").getPools().contains(psu.getPool("poolA")));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNestedCyclicGroup() {
        psu.createPoolGroup("group", false);
        psu.addToPoolGroup("group", "@group");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNestedCyclicGroupChain() {
        psu.createPoolGroup("groupA", false);
        psu.createPoolGroup("groupB", false);
        psu.createPoolGroup("groupC", false);

        psu.addToPoolGroup("groupA", "@groupB");
        psu.addToPoolGroup("groupB", "@groupC");
        psu.addToPoolGroup("groupC", "@groupA");
    }

    @Test
    public void testNestedGroup() {
        psu.createPoolGroup("group", false);

        psu.createPoolGroup("foo", false);
        psu.createPool("pool-foo", true, false, false);
        psu.addToPoolGroup("foo", "pool-foo");

        psu.createPoolGroup("bar", false);
        psu.createPool("pool-bar", true, false, false);
        psu.addToPoolGroup("bar", "pool-bar");

        psu.addToPoolGroup("group", "@foo");
        psu.addToPoolGroup("group", "@bar");

        assertTrue(psu.getPoolGroups().get("group").getPools().contains(psu.getPool("pool-foo")));
        assertTrue(psu.getPoolGroups().get("group").getPools().contains(psu.getPool("pool-bar")));
    }

    @Test
    public void testRemoveNestedGroup() {
        psu.createPoolGroup("group", false);
        psu.createPoolGroup("foo", false);
        psu.createPoolGroup("bar", false);


        psu.createPool("pool-foo", true, false, false);
        psu.addToPoolGroup("foo", "pool-foo");

        psu.createPool("pool-bar", true, false, false);
        psu.addToPoolGroup("bar", "pool-bar");

        psu.addToPoolGroup("group", "@foo");
        psu.addToPoolGroup("group", "@bar");

        psu.removeFromPoolGroup("group", "@foo");

        assertFalse(psu.getPoolGroups().get("group").getPools().contains(psu.getPool("pool-foo")));
        assertTrue(psu.getPoolGroups().get("group").getPools().contains(psu.getPool("pool-bar")));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testRemoveMissingNestedGroup() {
        psu.createPoolGroup("group", false);
        psu.createPoolGroup("foo", false);
        psu.createPoolGroup("bar", false);

        psu.addToPoolGroup("group", "@foo");

        psu.removeFromPoolGroup("group", "@bar");
    }
}