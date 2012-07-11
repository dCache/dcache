package org.dcache.services.info.secondaryInfoProviders;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.HashSet;
import java.util.Set;

import org.dcache.services.info.base.MalleableStateTransition;
import org.dcache.services.info.base.PostTransitionStateExhibitor;
import org.dcache.services.info.base.QueuingStateUpdateManager;
import org.dcache.services.info.base.StateExhibitor;
import org.dcache.services.info.base.StatePath;
import org.dcache.services.info.base.StateUpdate;
import org.dcache.services.info.base.StateWatcher;
import org.dcache.services.info.base.TestStateExhibitor;
import org.dcache.services.info.stateInfo.LinkInfo;
import org.dcache.services.info.stateInfo.SpaceInfo;
import org.junit.Before;
import org.junit.Test;

// FIXME the tests do not check that pools are partitioned correctly.
public class NormalisedAccessSpaceMaintainerTests {

    public static final StatePath PATH_NAS = new StatePath("nas");
    public static final StatePath PATH_NAS_INACCESSIBLE = PATH_NAS.newChild( NormalisedAccessSpaceMaintainer.PaintInfo.NAS_NAME_INACCESSIBLE);

    TestStateExhibitor _exhibitor;
    StateWatcher _watcher;
    QueuingStateUpdateManager _sum;
    StateUpdate _update;
    MalleableStateTransition _transition;


    @Before
    public void setUp() throws Exception {
        _exhibitor = new TestStateExhibitor();
        _watcher = new NormalisedAccessSpaceMaintainer();
        _sum = new QueuingStateUpdateManager();
        _update = new StateUpdate();
        _transition = new MalleableStateTransition();
    }

    /**
     * State is empty.  We add a poolgroup.
     */
    @Test
    public void testEmptyNewPoolgroup() {
        String poolgroupName = "new-poolgroup";

        StateLocation.transitionAddsPoolgroup( _transition, poolgroupName, 0);

        triggerWatcher();

        // we expect NASM to establish a NAS.
        assertEquals( "checking number of purges", 0, _update.countPurges());
        assertEquals( "checking number of metrics", 0, _update.count());
    }


    /**
     * State is empty.  We add a pool, but no space metrics yet
     */
    @Test
    public void testEmptyNewPool() {
        String poolName = "new-poolgroup";

        StateLocation.transitionAddsPool( _transition, poolName, 0);

        triggerWatcher();

        // we expect NASM to establish a NAS.
        assertEquals( "checking number of purges", 0, _update.countPurges());
        assertTrue( "checking number of metrics", _update.count() != 0);
    }

    /**
     * State is empty.  We add a pool with space metrics.
     */
    @Test
    public void testEmptyNewPoolWithSpaceMetrics() {
        String poolName = "pool-0";

        StateLocation.transitionAddsPoolMetrics( _transition, poolName, 0, new SpaceInfo( 10, 8, 1, 1));

        triggerWatcher();

        // Structure hasn't changed, we don't purge.
        assertEquals( "checking number of purges", 0, _update.countPurges());

        // Assert that there is a NAS
        Set<String> expectedPools = new HashSet<String>();
        expectedPools.add( poolName);
        assertNas( _update, PATH_NAS_INACCESSIBLE, expectedPools, new SpaceInfo( 10, 8, 1, 1));
    }

    /**
     * State is empty.  We add two pools with space metrics.
     */
    @Test
    public void testEmptyTwoNewPoolsWithSpaceMetrics() {
        String pool1Name = "pool-1";
        String pool2Name = "pool-2";

        StateLocation.transitionAddsPoolMetrics( _transition, pool1Name, 0, new SpaceInfo( 10, 8, 1, 1));
        StateLocation.transitionAddsPoolMetrics( _transition, pool2Name, 1, new SpaceInfo( 20, 16, 2, 2));

        triggerWatcher();

        // Structure hasn't changed, we don't purge.
        assertEquals( "checking number of purges", 0, _update.countPurges());

        // Assert that there is a NAS
        Set<String> expectedPools = new HashSet<String>();
        expectedPools.add( pool1Name);
        expectedPools.add( pool2Name);
        assertNas( _update, PATH_NAS_INACCESSIBLE, expectedPools, new SpaceInfo( 30, 24, 3, 3));
    }

    /**
     * State has a pool with space metrics.  We update these
     * metrics.
     */
    @Test
    public void testPoolWithMetricsUpdatePoolMetrics() {
        String poolName = "pool-0";

        StateLocation.putPoolSpaceMetrics( _exhibitor, poolName, new SpaceInfo( 10, 8, 1, 1));
        StateLocation.transitionAddsPoolMetrics( _transition, poolName, 4, new SpaceInfo( 10, 6, 2, 2));

        triggerWatcher();

        Set<String> expectedPools = new HashSet<String>();
        expectedPools.add( poolName);
        assertNas( _update, PATH_NAS_INACCESSIBLE, expectedPools, new SpaceInfo( 10, 6, 2, 2));
    }

    /**
     * State has a link with no access prefs.  We add a pool to this link that has no
     * space metrics.
     */
    @Test
    public void testLinkTransitionAddsPoolInLink() {
        String linkName = "link-1";
        String poolName = "pool-1";

        StateLocation.putLink( _exhibitor, linkName);
        StateLocation.transitionAddsPoolInLink( _transition, linkName, poolName, 2);

        triggerWatcher();

        assertEquals( "checking number of purges", 1, _update.countPurges());

        Set<String> expectedPools = new HashSet<String>();
        expectedPools.add( poolName);
        assertNas( _update, PATH_NAS_INACCESSIBLE, expectedPools, new SpaceInfo( 0, 0, 0, 0));
    }

    /**
     * State has a pool with space metrics and link.  Transition adds that pool to the link
     */
    @Test
    public void testLinkAndPoolWithSpaceMetricsTransitionAddsPoolInLink() {
        String linkName = "link-1";
        String poolName = "pool-1";

        SpaceInfo poolInfo = new SpaceInfo( 10, 8, 1, 1);

        StateLocation.putPoolSpaceMetrics( _exhibitor, poolName, poolInfo);
        StateLocation.putLink( _exhibitor, linkName);

        StateLocation.transitionAddsPoolInLink( _transition, linkName, poolName, 2);

        triggerWatcher();

        assertEquals( "checking number of purges", 1, _update.countPurges());

        Set<String> expectedPools = new HashSet<String>();
        expectedPools.add( poolName);
        assertNas( _update, PATH_NAS_INACCESSIBLE, expectedPools, poolInfo);
    }


    /**
     * State has a pool with space metrics within a link.  Transition adds a new pool
     * with space metrics.
     */
    @Test
    public void testLinkAndPoolWithSpaceMetricsTransitionAddsPoolWithMetrics() {
        String linkName = "link-1";
        String pool1Name = "pool-1";
        String pool2Name = "pool-2";

        SpaceInfo pool1Info = new SpaceInfo( 10, 8, 1, 1);
        SpaceInfo pool2Info = new SpaceInfo( 20, 16, 2, 2);

        SpaceInfo combinedInfo = new SpaceInfo( pool1Info);
        combinedInfo.add( pool2Info);

        StateLocation.putPoolSpaceMetrics( _exhibitor, pool1Name, pool1Info);
        StateLocation.putPoolInLink( _exhibitor, linkName, pool1Name);

        StateLocation.transitionAddsPoolMetrics( _transition, pool2Name, 1, pool2Info);

        triggerWatcher();

        assertEquals( "checking number of purges", 0, _update.countPurges());

        Set<String> expectedPools = new HashSet<String>();
        expectedPools.add( pool1Name);
        expectedPools.add( pool2Name);
        assertNas( _update, PATH_NAS_INACCESSIBLE, expectedPools, combinedInfo);
    }

    @Test
    public void testNoUnitsAddLinkZeroPrefs() {
        String linkName = "link-1";
        String poolName = "pool-1";

        SpaceInfo poolInfo = new SpaceInfo( 10, 8, 1, 1);

        StateLocation.putPoolSpaceMetrics( _exhibitor, poolName, poolInfo);
        StateLocation.putPoolInLink( _exhibitor, linkName, poolName);

        StateLocation.transitionAddsLinkPrefs( _transition, linkName, 2, 0, 0, 0, 0);

        triggerWatcher();

        assertEquals( "checking number of purges", 1, _update.countPurges());

        Set<String> expectedPools = new HashSet<String>();
        expectedPools.add( poolName);
        assertNas( _update, PATH_NAS_INACCESSIBLE, expectedPools, poolInfo);
    }

    @Test
    public void testNoUnitsAddLinkCanReadPrefs() {
        String linkName = "link-1";
        String poolName = "pool-1";

        SpaceInfo poolInfo = new SpaceInfo( 10, 8, 1, 1);

        StateLocation.putPoolSpaceMetrics( _exhibitor, poolName, poolInfo);
        StateLocation.putPoolInLink( _exhibitor, linkName, poolName);

        StateLocation.transitionAddsLinkPrefs( _transition, linkName, 2, 5, 0, 0, 0);

        triggerWatcher();

        assertEquals( "checking number of purges", 1, _update.countPurges());

        Set<String> expectedPools = new HashSet<String>();
        expectedPools.add( poolName);
        assertNas( _update, PATH_NAS_INACCESSIBLE, expectedPools, poolInfo);
    }


    @Test
    public void testStoreUnitsAddLinkZeroPrefs() {
        String linkName = "link-1";
        String poolName = "pool-1";

        SpaceInfo poolInfo = new SpaceInfo( 10, 8, 1, 1);

        StateLocation.putPoolSpaceMetrics( _exhibitor, poolName, poolInfo);
        StateLocation.putPoolInLink( _exhibitor, linkName, poolName);
        StateLocation.putUnitInLink( _exhibitor, linkName, LinkInfo.UNIT_TYPE.STORE, "dcache@osm");

        StateLocation.transitionAddsLinkPrefs( _transition, linkName, 2, 0, 0, 0, 0);

        triggerWatcher();

        assertEquals( "checking number of purges", 1, _update.countPurges());

        Set<String> expectedPools = new HashSet<String>();
        expectedPools.add( poolName);
        assertNas( _update, PATH_NAS_INACCESSIBLE, expectedPools, poolInfo);
    }

    @Test
    public void testStoreUnitsAddLinkCanReadPrefs() {
        String linkName = "link-1";
        String poolName = "pool-1";

        SpaceInfo poolInfo = new SpaceInfo( 10, 8, 1, 1);

        StateLocation.putPoolSpaceMetrics( _exhibitor, poolName, poolInfo);
        StateLocation.putPoolInLink( _exhibitor, linkName, poolName);
        StateLocation.putUnitInLink( _exhibitor, linkName, LinkInfo.UNIT_TYPE.STORE, "dcache@osm");

        StateLocation.transitionAddsLinkPrefs( _transition, linkName, 2, 5, 0, 0, 0);

        triggerWatcher();

        assertEquals( "checking number of purges", 1, _update.countPurges());

        Set<String> expectedPools = new HashSet<String>();
        expectedPools.add( poolName);

        assertNas( _update, PATH_NAS.newChild("S{R:dcache@osm}"), expectedPools, poolInfo);
    }


    @Test
    public void testStoreUnitsAddReadWriteLink() {
        String linkName = "link-1";
        String poolName = "pool-1";

        SpaceInfo poolInfo = new SpaceInfo( 10, 8, 1, 1);

        StateLocation.putPoolSpaceMetrics( _exhibitor, poolName, poolInfo);
        StateLocation.putPoolInLink( _exhibitor, linkName, poolName);
        StateLocation.putUnitInLink( _exhibitor, linkName, LinkInfo.UNIT_TYPE.STORE, "dcache@osm");

        StateLocation.transitionAddsLinkPrefs( _transition, linkName, 2, 5, 5, 0, 0);

        triggerWatcher();

        assertEquals( "checking number of purges", 1, _update.countPurges());

        Set<String> expectedPools = new HashSet<String>();
        expectedPools.add( poolName);

        assertNas( _update, PATH_NAS.newChild("S{RW:dcache@osm}"), expectedPools, poolInfo);
    }


    @Test
    public void testStoreUnitsAddReadWriteLinkAddReadLink() {
        String link1Name = "link-1";
        String link2Name = "link-2";
        String poolName = "pool-1";

        SpaceInfo poolInfo = new SpaceInfo( 10, 8, 1, 1);

        StateLocation.putPoolSpaceMetrics( _exhibitor, poolName, poolInfo);
        StateLocation.putPoolInLink( _exhibitor, link1Name, poolName);
        StateLocation.putUnitInLink( _exhibitor, link1Name, LinkInfo.UNIT_TYPE.STORE, "dcache@osm");

        StateLocation.putPoolInLink( _exhibitor, link2Name, poolName);
        StateLocation.putUnitInLink( _exhibitor, link2Name, LinkInfo.UNIT_TYPE.STORE, "atlas@osm");

        StateLocation.transitionAddsLinkPrefs( _transition, link1Name, 2, 5, 5, 0, 0);
        StateLocation.transitionAddsLinkPrefs( _transition, link2Name, 2, 5, 0, 0, 0);

        triggerWatcher();

        assertEquals( "checking number of purges", 1, _update.countPurges());

        Set<String> expectedPools = new HashSet<String>();
        expectedPools.add( poolName);
        assertNas( _update, PATH_NAS.newChild("S{R:atlas@osm,dcache@osm;W:dcache@osm}"), expectedPools, poolInfo);
    }

    /*
     *  ADD TWO POOLS THAT END UP IN DIFFERENT NAS
     */

    @Test
    public void testTwoPoolsDifferingInLinks() {
        String pool1Name = "pool-1";
        String pool2Name = "pool-2";

        String link1Name = "link-1";
        String link2Name = "link-2";

        SpaceInfo pool1Info = new SpaceInfo( 10, 8, 1, 1);
        SpaceInfo pool2Info = new SpaceInfo( 20, 16, 2, 2);

        StateLocation.putPoolSpaceMetrics( _exhibitor, pool1Name, pool1Info);
        StateLocation.putPoolInLink( _exhibitor, link1Name, pool1Name);
        StateLocation.putPoolSpaceMetrics( _exhibitor, pool2Name, pool2Info);
        StateLocation.putPoolInLink( _exhibitor, link2Name, pool2Name);

        // Add the same unit selecting the different links.
        StateLocation.putUnitInLink( _exhibitor, link1Name, LinkInfo.UNIT_TYPE.STORE, "dcache@osm");
        StateLocation.putUnitInLink( _exhibitor, link2Name, LinkInfo.UNIT_TYPE.STORE, "dcache@osm");

        StateLocation.transitionAddsLinkPrefs( _transition, link1Name, 2, 5, 0, 0, 0);
        StateLocation.transitionAddsLinkPrefs( _transition, link2Name, 2, 5, 0, 0, 0);

        triggerWatcher();

        Set<String> expectedPools = new HashSet<String>();
        expectedPools.add( pool1Name);
        expectedPools.add( pool2Name);

        SpaceInfo combinedSize = new SpaceInfo( pool1Info);
        combinedSize.add( pool2Info);
        assertNas( _update, PATH_NAS.newChild("S{R:dcache@osm}"), expectedPools, combinedSize);
    }


    /**
     * Check that a NAS metrics are being updated.
     */
    private void assertNas( StateUpdate update, StatePath nasPath, Set<String> pools, SpaceInfo info) {
        StatePath poolsPath = nasPath.newChild( "pools");

        for( String pool : pools) {
            StateLocation
                    .assertUpdateHasBranch("checking for pool " + pool, update, poolsPath
                            .newChild(pool));
        }

        StateLocation.assertSpaceMetrics( update, nasPath.newChild( "space"), info);
    }

    private void triggerWatcher()
    {
        StateExhibitor futureState = new PostTransitionStateExhibitor( _exhibitor, _transition);
        _watcher.trigger( _update, _exhibitor, futureState);
    }
}
