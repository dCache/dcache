package org.dcache.services.info.stateInfo;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Set;

import org.dcache.services.info.stateInfo.LinkInfo.OPERATION;
import org.dcache.services.info.stateInfo.LinkInfo.UNIT_TYPE;
import org.junit.Before;
import org.junit.Test;

/**
 * A set of tests to check LinkInfo object
 */
public class LinkInfoTests {

    static final String LINK_NAME = "test link";
    LinkInfo _info;

    @Before
    public void setUp() throws Exception {
        _info = new LinkInfo( LINK_NAME);
    }

    @Test
    public void testGetId() {
        assertEquals( "Cannot get correct ID", LINK_NAME, _info.getId());
    }

    @Test
    public void testInitiallyEmptyPools() {
        Set<String> pools = _info.getPools();

        assertEquals( "Unexpected number of pools from empty LinkInfo", 0,
                      pools.size());
    }

    @Test
    public void testInitiallyEmptyPoolgroups() {
        Set<String> poolgroups = _info.getPoolgroups();

        assertEquals( "Unexpected number of poolgroups from empty LinkInfo", 0,
                      poolgroups.size());
    }

    @Test
    public void testInitiallyEmptyUnitGroups() {
        Set<String> unitGroups = _info.getUnitgroups();

        assertEquals( "Unexpected number of unitgroups from empty LinkInfo", 0,
                      unitGroups.size());
    }

    @Test
    public void testInitiallyEmptyUnits() {
        for( LinkInfo.UNIT_TYPE type : LinkInfo.UNIT_TYPE.values()) {
            Set<String> units = _info.getUnits( type);
            assertEquals( "Unexpected number of units of type " + type, 0,
                          units.size());
        }
    }

    @Test
    public void testAddPool() {
        String poolName = "TEST POOL";
        _info.addPool( poolName);

        Set<String> pools = _info.getPools();

        assertEquals( "Unexpected number of pools", 1, pools.size());

        assertTrue( "Unexpected pool set membership", pools.contains( poolName));
    }

    @Test
    public void testAddPoolTwice() {
        String poolName = "TEST POOL";
        _info.addPool( poolName);
        _info.addPool( poolName);

        Set<String> pools = _info.getPools();

        assertEquals( "Unexpected number of pools", 1, pools.size());

        assertTrue( "Unexpected pool set membership", pools.contains( poolName));
    }

    @Test
    public void testAddTwoPools() {
        String poolName1 = "TEST POOL 1";
        String poolName2 = "TEST POOL 2";
        _info.addPool( poolName1);
        _info.addPool( poolName2);

        Set<String> pools = _info.getPools();

        assertEquals( "Unexpected number of pools", 2, pools.size());

        assertTrue( "Unexpected pool set membership",
                    pools.contains( poolName1));
        assertTrue( "Unexpected pool set membership",
                    pools.contains( poolName2));
    }

    @Test
    public void testAddPoolgroup() {
        String poolgroupName = "poolgroup";

        _info.addPoolgroup( poolgroupName);

        Set<String> poolgroups = _info.getPoolgroups();

        assertEquals( "Unexpected number of poolgroups", 1, poolgroups.size());

        assertTrue( "Unexpected poolgroup membership",
                    poolgroups.contains( poolgroupName));
    }

    @Test
    public void testAddPoolgroupTwice() {
        String poolgroupName = "poolgroup";

        _info.addPoolgroup( poolgroupName);
        _info.addPoolgroup( poolgroupName);

        Set<String> poolgroups = _info.getPoolgroups();

        assertEquals( "Unexpected number of poolgroups", 1, poolgroups.size());

        assertTrue( "Unexpected poolgroup membership",
                    poolgroups.contains( poolgroupName));
    }

    @Test
    public void testAddTwoPoolgroups() {
        String poolgroupName1 = "poolgroup 1";
        String poolgroupName2 = "poolgroup 2";

        _info.addPoolgroup( poolgroupName1);
        _info.addPoolgroup( poolgroupName2);

        Set<String> poolgroups = _info.getPoolgroups();

        assertEquals( "Unexpected number of poolgroups", 2, poolgroups.size());

        assertTrue( "Unexpected poolgroup membership",
                    poolgroups.contains( poolgroupName1));
        assertTrue( "Unexpected poolgroup membership",
                    poolgroups.contains( poolgroupName2));
    }

    @Test
    public void testAddUnitStoreUnit() {
        assertUnitType( LinkInfo.UNIT_TYPE.STORE);
    }

    @Test
    public void testAddUnitDcacheUnit() {
        assertUnitType( LinkInfo.UNIT_TYPE.DCACHE);
    }

    @Test
    public void testAddUnitNetworkUnit() {
        assertUnitType( LinkInfo.UNIT_TYPE.NETWORK);
    }

    @Test
    public void testAddUnitProtocolUnit() {
        assertUnitType( LinkInfo.UNIT_TYPE.PROTOCOL);
    }

    @Test
    public void testWritePrefZero() {
        assertSetPref( OPERATION.WRITE, 0);
    }

    @Test
    public void testReadPrefZero() {
        assertSetPref( OPERATION.READ, 0);
    }

    @Test
    public void testP2PPrefZero() {
        assertSetPref( OPERATION.P2P, 0);
    }

    @Test
    public void testCachePrefZero() {
        assertSetPref( OPERATION.CACHE, 0);
    }

    @Test
    public void testP2PPrefMinusOne() {
        assertSetPref( OPERATION.P2P, -1);
    }

    @Test
    public void testWritePrefPositive() {
        assertSetPref( OPERATION.WRITE, 5);
    }

    @Test
    public void testReadPrefPositive() {
        assertSetPref( OPERATION.READ, 5);
    }

    @Test
    public void testP2PPrefPositive() {
        assertSetPref( OPERATION.P2P, 5);
    }

    @Test
    public void testCachePrefPositive() {
        assertSetPref( OPERATION.CACHE, 5);
    }

    @Test
    public void testInitialReadAccessable() {
        assertFalse(
                     "Checking initial state for accessability of READ operation",
                     _info.isAccessableFor( OPERATION.READ));
    }

    @Test
    public void testInitialWriteAccessable() {
        assertFalse(
                     "Checking initial state for accessability of WRITE operation",
                     _info.isAccessableFor( OPERATION.WRITE));
    }

    @Test
    public void testInitialP2pAccessable() {
        assertFalse(
                     "Checking initial state for accessability of P2P operation",
                     _info.isAccessableFor( OPERATION.P2P));
    }

    @Test
    public void testInitialCacheAccessable() {
        assertFalse(
                     "Checking initial state for accessability of CACHE operation",
                     _info.isAccessableFor( OPERATION.CACHE));
    }

    public void testWriteZeroIsAccessableFor() {
        assertSetPref( OPERATION.WRITE, 0);
        assertFalse( "Unexpected result of inaccessable link",
                     _info.isAccessableFor( OPERATION.WRITE));
    }

    public void testReadZeroIsAccessableFor() {
        assertSetPref( OPERATION.READ, 0);
        assertFalse( "Unexpected result of inaccessable link",
                     _info.isAccessableFor( OPERATION.READ));
    }

    public void testP2PZeroIsAccessableFor() {
        assertSetPref( OPERATION.P2P, 0);
        assertFalse( "Unexpected result of inaccessable link",
                     _info.isAccessableFor( OPERATION.P2P));
    }

    public void testCacheZeroIsAccessableFor() {
        assertSetPref( OPERATION.CACHE, 0);
        assertFalse( "Unexpected result of inaccessable link",
                     _info.isAccessableFor( OPERATION.CACHE));
    }

    public void testWritePositiveIsAccessableFor() {
        assertSetPref( OPERATION.WRITE, 5);
        assertTrue( "Unexpected result of accessable link",
                    _info.isAccessableFor( OPERATION.WRITE));
    }

    public void testReadPositiveIsAccessableFor() {
        assertSetPref( OPERATION.READ, 5);
        assertTrue( "Unexpected result of accessable link",
                    _info.isAccessableFor( OPERATION.READ));
    }

    public void testP2PPositiveIsAccessableFor() {
        assertSetPref( OPERATION.P2P, 5);
        assertTrue( "Unexpected result of accessable link",
                    _info.isAccessableFor( OPERATION.P2P));
    }

    public void testCachePositiveIsAccessableFor() {
        assertSetPref( OPERATION.CACHE, 5);
        assertTrue( "Unexpected result of accessable link",
                    _info.isAccessableFor( OPERATION.CACHE));
    }

    public void testP2PMinusOneWriteZeroIsAccessableFor() {
        assertSetPref( OPERATION.P2P, -1);
        assertSetPref( OPERATION.WRITE, 0);
        assertFalse( "Unexpected result of inaccessable link",
                     _info.isAccessableFor( OPERATION.P2P));
    }

    public void testP2PMinusOneWritePositiveIsAccessableFor() {
        assertSetPref( OPERATION.P2P, -1);
        assertSetPref( OPERATION.WRITE, 5);
        assertTrue( "Unexpected result of accessable link",
                    _info.isAccessableFor( OPERATION.P2P));
    }

    @Test
    public void testEmptyEquals() {
        LinkInfo otherInfo = new LinkInfo( "another link");

        assertEquals( "Test empty links are equal", otherInfo, _info);
    }

    @Test
    public void testSetPrefsEquals() {
        LinkInfo otherInfo = new LinkInfo( "another link");

        _info.setOperationPref( OPERATION.READ, 5);
        otherInfo.setOperationPref( OPERATION.READ, 5);

        assertEquals( "Test empty links with set preferences are equal",
                      otherInfo, _info);
    }

    @Test
    public void testDiffPrefsNotEqual() {
        LinkInfo otherInfo = new LinkInfo( "another link");

        _info.setOperationPref( OPERATION.READ, 5);
        otherInfo.setOperationPref( OPERATION.READ, 10);

        assertFalse( "LinkInfo with different READ prefs",
                     _info.equals( otherInfo));
    }

    @Test
    public void testInfoWithPoolEquals() {
        String poolName = "a pool";

        LinkInfo otherInfo = new LinkInfo( "another link");

        otherInfo.addPool( poolName);
        _info.addPool( poolName);

        assertEquals( "Test links with same pool are equal", otherInfo, _info);
    }

    @Test
    public void testInfoWithDifferentPoolEquals() {
        String poolName1 = "pool 1";
        String poolName2 = "pool 2";

        LinkInfo otherInfo = new LinkInfo( "another link");

        otherInfo.addPool( poolName1);
        _info.addPool( poolName2);

        assertFalse( "Test links with same pool are not equal",
                     _info.equals( otherInfo));
    }

    @Test
    public void testInfoWithSamePoolgroupEquals() {
        String poolgroupName = "poolgroup";

        LinkInfo otherInfo = new LinkInfo( "another link");

        otherInfo.addPoolgroup( poolgroupName);
        _info.addPoolgroup( poolgroupName);

        assertEquals( "Test links with same poolgroup are equal", otherInfo,
                      _info);
    }

    @Test
    public void testInfoWithDifferentPoolgroupEquals() {
        String poolgroupName1 = "poolgroup 1";
        String poolgroupName2 = "poolgroup 2";

        LinkInfo otherInfo = new LinkInfo( "another link");

        otherInfo.addPoolgroup( poolgroupName1);
        _info.addPoolgroup( poolgroupName2);

        assertFalse( "Test links with same poolgroup are equal",
                     otherInfo.equals( _info));
    }

    @Test
    public void testInfoWithSameUnitEquals() {
        String unitName = "unit";
        UNIT_TYPE unitType = UNIT_TYPE.DCACHE;

        LinkInfo otherInfo = new LinkInfo( "another link");

        otherInfo.addUnit( unitType, unitName);
        _info.addUnit( unitType, unitName);

        assertEquals( "Test links with same " + unitType + " unit are equal",
                      otherInfo, _info);
    }

    @Test
    public void testInfoWithSameDcacheUnitEquals() {
        assertUnitTypeEquality( UNIT_TYPE.DCACHE);
    }

    @Test
    public void testInfoWithDifferentDcacheUnitEquals() {
        assertUnitTypeInequality( UNIT_TYPE.DCACHE);
    }

    @Test
    public void testInfoWithSameStoreUnitEquals() {
        assertUnitTypeEquality( UNIT_TYPE.STORE);
    }

    @Test
    public void testInfoWithDifferentStoreUnitEquals() {
        assertUnitTypeInequality( UNIT_TYPE.STORE);
    }

    @Test
    public void testInfoWithSameNetworkUnitEquals() {
        assertUnitTypeEquality( UNIT_TYPE.NETWORK);
    }

    @Test
    public void testInfoWithDifferentNetworkUnitEquals() {
        assertUnitTypeInequality( UNIT_TYPE.NETWORK);
    }

    @Test
    public void testInfoWithSameProtocolUnitEquals() {
        assertUnitTypeEquality( UNIT_TYPE.PROTOCOL);
    }

    @Test
    public void testInfoWithDifferentProtocolUnitEquals() {
        assertUnitTypeInequality( UNIT_TYPE.PROTOCOL);
    }

    private void assertUnitTypeEquality( UNIT_TYPE unitType) {
        String unitName = "unit";

        LinkInfo otherInfo = new LinkInfo( "another link");

        otherInfo.addUnit( unitType, unitName);
        _info.addUnit( unitType, unitName);

        assertEquals( "Test links with same " + unitType + " unit are equal",
                      otherInfo, _info);
    }

    private void assertUnitTypeInequality( UNIT_TYPE unitType) {
        String unitName1 = "unit 1";
        String unitName2 = "unit 2";

        LinkInfo otherInfo = new LinkInfo( "another link");

        otherInfo.addUnit( unitType, unitName1);
        _info.addUnit( unitType, unitName2);

        assertFalse( "Test links with different " + unitType +
                     " unit are not equal", otherInfo.equals( _info));
    }

    /**
     * Check whether adding a single unit of the specified type works as
     * expected.
     * 
     * @param type
     *            the LinkInfo.UNIT_TYPE we wish to test.
     */
    private void assertUnitType( LinkInfo.UNIT_TYPE type) {
        String unitName = "unit";

        _info.addUnit( type, unitName);

        for( LinkInfo.UNIT_TYPE testType : LinkInfo.UNIT_TYPE.values()) {
            int expectedEntries = (testType == type) ? 1 : 0;

            Set<String> units = _info.getUnits( testType);
            assertEquals( "Unexpected number of entried for type " + type,
                          expectedEntries, units.size());

            if( testType == type)
                assertTrue( "Unexpected set membership",
                            units.contains( unitName));
        }
    }

    private void assertSetPref( OPERATION operation, int preference) {
        _info.setOperationPref( operation, preference);
        assertEquals( "Unable to set " + operation + " preference of " +
                      Integer.toString( preference), preference,
                      _info.getOperationPref( operation));
    }

}
