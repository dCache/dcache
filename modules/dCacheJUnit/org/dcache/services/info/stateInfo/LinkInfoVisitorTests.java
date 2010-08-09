package org.dcache.services.info.stateInfo;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.slf4j.LoggerFactory;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.encoder.PatternLayoutEncoder;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.ConsoleAppender;

import org.dcache.services.info.base.IntegerStateValue;
import org.dcache.services.info.base.StatePath;
import org.dcache.services.info.base.TestStateExhibitor;
import org.dcache.services.info.secondaryInfoProviders.StateLocation;
import org.junit.Before;
import org.junit.Test;

/**
 * Test the LinkInfoVisitor class.
 */
public class LinkInfoVisitorTests {

    static final StatePath LINKS_PATH = new StatePath( "links");
    static final private String CONSOLE_APPENDER_NAME = "console";
    static final private String CONSOLE_APPENDER_PATTERN = "%-5level - %msg%n";

    /**
     * Direct all warning or more severe messages from ListInfoVisitor
     * log4j target to the console.
     *
     * FIXME: This should be done by a generic configuration file
     * specified as a system property in ant.
     */
    static {
        LoggerContext loggerContext =
            (LoggerContext) LoggerFactory.getILoggerFactory();
        loggerContext.reset();

        ConsoleAppender<ILoggingEvent> ca =
            new ConsoleAppender<ILoggingEvent>();
        ca.setContext(loggerContext);
        ca.setName(CONSOLE_APPENDER_NAME);
        PatternLayoutEncoder pl = new PatternLayoutEncoder();
        pl.setContext(loggerContext);
        pl.setPattern(CONSOLE_APPENDER_PATTERN);
        pl.start();

        ca.setEncoder(pl);
        ca.start();

        Logger logger = loggerContext.getLogger(Logger.ROOT_LOGGER_NAME);
        logger.addAppender(ca);
        logger.setLevel(Level.WARN);
    }

    /** A mapping between operation preference to the name of the corresponding metric */
    static final Map<LinkInfo.OPERATION,String> OPERATION_METRIC_NAME = Collections.unmodifiableMap( new HashMap<LinkInfo.OPERATION,String>() {
        private static final long serialVersionUID = 2219445130285814842L;
        {
            put( LinkInfo.OPERATION.CACHE, "cache");
            put( LinkInfo.OPERATION.P2P,   "p2p");
            put( LinkInfo.OPERATION.READ,  "read");
            put( LinkInfo.OPERATION.WRITE, "write");
        }});

    /** A mapping between unit type to the name of the corresponding branch */
    static final Map<LinkInfo.UNIT_TYPE,String> UNIT_TYPE_METRIC_NAME = Collections.unmodifiableMap( new HashMap<LinkInfo.UNIT_TYPE,String>() {
        private static final long serialVersionUID = -4333350816329550714L;
        {
            put( LinkInfo.UNIT_TYPE.DCACHE,   "dcache");
            put( LinkInfo.UNIT_TYPE.NETWORK,  "net");
            put( LinkInfo.UNIT_TYPE.PROTOCOL, "protocol");
            put( LinkInfo.UNIT_TYPE.STORE,    "store");
        }});

    TestStateExhibitor _exhibitor;
    LinkInfoVisitor _visitor;

    @Before
    public void setUp() throws Exception {
        _exhibitor = new TestStateExhibitor();
        _visitor = new LinkInfoVisitor();
    }

    @Test
    public void testEmptyState() {
        _exhibitor.visitState( _visitor);

        Map<String,LinkInfo> acquiredInfo = _visitor.getInfo();

        assertEquals( "", 0, acquiredInfo.size());
    }

    @Test
    public void testSingleEmptyLink() {
        Set<String> linkNames = new HashSet<String>();

        linkNames.add( "link-1");

        assertEmptyLinksOk( linkNames);
    }

    @Test
    public void testTwoEmptyLink() {
        Set<String> linkNames = new HashSet<String>();

        linkNames.add( "link-1");
        linkNames.add( "link-2");

        assertEmptyLinksOk( linkNames);
    }

    @Test
    public void testLinkWithSinglePool() {
        Set<String> pools = new HashSet<String>();

        pools.add(  "a pool");

        assertLinkWithPoolsOk( pools);
    }

    @Test
    public void testLinkWithSpaceMetrics() {
        String linkName = "link-1";
        long totalSpace = 10;
        long freeSpace = 8;
        long preciousSpace = 1;
        long removableSpace = 1;

        StateLocation.putLink( _exhibitor, linkName);

        SpaceInfo linkSpace = new SpaceInfo( totalSpace, freeSpace, preciousSpace, removableSpace);

        StateLocation.putLinkSpaceMetrics( _exhibitor, linkName, linkSpace);

        LinkInfo expectedLinkInfo = new LinkInfo(linkName);

        assertSingleLinkInfo( linkName, expectedLinkInfo);
    }

    @Test
    public void testLinkWithTwoPools() {
        Set<String> pools = new HashSet<String>();

        pools.add(  "a pool");
        pools.add(  "another pool");

        assertLinkWithPoolsOk( pools);
    }

    @Test
    public void testLinkWithThreePools() {
        Set<String> pools = new HashSet<String>();

        pools.add(  "a pool");
        pools.add(  "another pool");
        pools.add(  "a third pool");

        assertLinkWithPoolsOk( pools);
    }

    @Test
    public void testLinkWithSinglePoolgroup() {
        Set<String> poolgroups = new HashSet<String>();

        poolgroups.add(  "a poolgroup");

        assertLinkWithPoolgroupsOk( poolgroups);
    }

    @Test
    public void testLinkWithTwoPoolgroups() {
        Set<String> poolgroups = new HashSet<String>();

        poolgroups.add(  "a poolgroup");
        poolgroups.add(  "another poolgroup");

        assertLinkWithPoolgroupsOk( poolgroups);
    }

    @Test
    public void testLinkWithThreePoolgroup() {
        Set<String> poolgroups = new HashSet<String>();

        poolgroups.add(  "a poolgroup");
        poolgroups.add(  "another poolgroup");
        poolgroups.add(  "a third poolgroup");

        assertLinkWithPoolgroupsOk( poolgroups);
    }

    @Test
    public void testLinkWithUnitgroup() {
        Set<String> unitgroups = new HashSet<String>();

        unitgroups.add( "unitgroup");

        assertLinkWithUnitgroupsOk( unitgroups);
    }


    @Test
    public void testLinkWithTwoUnitgroups() {
        Set<String> unitgroups = new HashSet<String>();

        unitgroups.add( "unitgroup-1");
        unitgroups.add( "unitgroup-2");

        assertLinkWithUnitgroupsOk( unitgroups);
    }

    @Test
    public void testLinkWithThreeUnitgroups() {
        Set<String> unitgroups = new HashSet<String>();

        unitgroups.add( "unitgroup-1");
        unitgroups.add( "unitgroup-2");
        unitgroups.add( "unitgroup-3");

        assertLinkWithUnitgroupsOk( unitgroups);
    }


    @Test
    public void testLinkWithReadPref() {
        String linkName = "my link";

        LinkInfo expectedLinkInfo = new LinkInfo("expected link");
        addOperationPrefInLink( linkName, LinkInfo.OPERATION.READ, 5);
        expectedLinkInfo.setOperationPref( LinkInfo.OPERATION.READ, 5);

        assertSingleLinkInfo( linkName, expectedLinkInfo);
    }

    @Test
    public void testLinkWithWritePref() {
        String linkName = "my link";

        LinkInfo expectedLinkInfo = new LinkInfo("expected link");
        addOperationPrefInLink( linkName, LinkInfo.OPERATION.WRITE, 5);
        expectedLinkInfo.setOperationPref( LinkInfo.OPERATION.WRITE, 5);

        assertSingleLinkInfo( linkName, expectedLinkInfo);
    }

    @Test
    public void testLinkWithP2pPref() {
        String linkName = "my link";

        LinkInfo expectedLinkInfo = new LinkInfo("expected link");
        addOperationPrefInLink( linkName, LinkInfo.OPERATION.P2P, 5);
        expectedLinkInfo.setOperationPref( LinkInfo.OPERATION.P2P, 5);

        assertSingleLinkInfo( linkName, expectedLinkInfo);
    }


    @Test
    public void testLinkWithCachePref() {
        String linkName = "my link";

        LinkInfo expectedLinkInfo = new LinkInfo("expected link");
        addOperationPrefInLink( linkName, LinkInfo.OPERATION.CACHE, 5);
        expectedLinkInfo.setOperationPref( LinkInfo.OPERATION.CACHE, 5);

        assertSingleLinkInfo( linkName, expectedLinkInfo);
    }

    @Test
    public void testLinkWithAllPrefs() {
        String linkName = "my link";

        LinkInfo expectedLinkInfo = new LinkInfo("expected link");
        addOperationPrefInLink( linkName, LinkInfo.OPERATION.READ, 5);
        addOperationPrefInLink( linkName, LinkInfo.OPERATION.WRITE, 7);
        addOperationPrefInLink( linkName, LinkInfo.OPERATION.CACHE, 13);
        addOperationPrefInLink( linkName, LinkInfo.OPERATION.P2P, 17);

        expectedLinkInfo.setOperationPref( LinkInfo.OPERATION.READ, 5);
        expectedLinkInfo.setOperationPref( LinkInfo.OPERATION.WRITE, 7);
        expectedLinkInfo.setOperationPref( LinkInfo.OPERATION.CACHE, 13);
        expectedLinkInfo.setOperationPref( LinkInfo.OPERATION.P2P, 17);

        assertSingleLinkInfo( linkName, expectedLinkInfo);
    }

    @Test
    public void testLinkWithStoreUnit() {
        Set<String> units = new HashSet<String>();

        units.add("unit1");

        assertLinkWithSameUnitsOk( LinkInfo.UNIT_TYPE.STORE, units);
    }

    @Test
    public void testLinkWithTwoStoreUnits() {
        Set<String> units = new HashSet<String>();

        units.add("unit1");
        units.add("unit2");

        assertLinkWithSameUnitsOk( LinkInfo.UNIT_TYPE.STORE, units);
    }

    @Test
    public void testLinkWithDcacheUnit() {
        Set<String> units = new HashSet<String>();

        units.add("unit1");

        assertLinkWithSameUnitsOk( LinkInfo.UNIT_TYPE.DCACHE, units);
    }

    @Test
    public void testLinkWithTwoDcacheUnits() {
        Set<String> units = new HashSet<String>();

        units.add("unit1");
        units.add("unit2");

        assertLinkWithSameUnitsOk( LinkInfo.UNIT_TYPE.DCACHE, units);
    }

    @Test
    public void testLinkWithNetUnit() {
        Set<String> units = new HashSet<String>();

        units.add("unit1");

        assertLinkWithSameUnitsOk( LinkInfo.UNIT_TYPE.NETWORK, units);
    }

    @Test
    public void testLinkWithTwoNetUnits() {
        Set<String> units = new HashSet<String>();

        units.add("unit1");
        units.add("unit2");

        assertLinkWithSameUnitsOk( LinkInfo.UNIT_TYPE.NETWORK, units);
    }

    @Test
    public void testLinkWithProtoUnit() {
        Set<String> units = new HashSet<String>();

        units.add("unit1");

        assertLinkWithSameUnitsOk( LinkInfo.UNIT_TYPE.PROTOCOL, units);
    }

    @Test
    public void testLinkWithTwoProtoUnits() {
        Set<String> units = new HashSet<String>();

        units.add("unit1");
        units.add("unit2");

        assertLinkWithSameUnitsOk( LinkInfo.UNIT_TYPE.PROTOCOL, units);
    }

    /**
     *   PRIVATE SUPPORT METHODS
     */

    /**
     * Update the simulated dCache state
     */
    private void addPoolInLink( String linkName, String poolName) {
        _exhibitor.addBranch( LINKS_PATH.newChild( linkName).newChild( "pools").newChild( poolName));
    }

    private void addPoolgroupInLink( String linkName, String poolgroupName) {
        _exhibitor.addBranch( LINKS_PATH.newChild( linkName).newChild( "poolgroups").newChild( poolgroupName));
    }

    private void addOperationPrefInLink( String linkName, LinkInfo.OPERATION operation, long value) {
        _exhibitor.addMetric( LINKS_PATH.newChild( linkName).newChild( "prefs").newChild( OPERATION_METRIC_NAME.get( operation)),
                              new IntegerStateValue( value));
    }

    private void addUnitInLink( String linkName, LinkInfo.UNIT_TYPE unitType, String unit) {
        _exhibitor.addBranch( LINKS_PATH.newChild( linkName).newChild( "units").newChild(UNIT_TYPE_METRIC_NAME.get(  unitType)).newChild( unit));
    }

    private void addUnitgroupInLink( String linkName, String unitgroupName) {
        _exhibitor.addBranch( LINKS_PATH.newChild( linkName).newChild( "unitgroups").newChild( unitgroupName));
    }

    /**
     * Verify that a link with only a specified Set of pools is read correctly.
     */
    private void assertLinkWithPoolsOk( Set<String> pools) {
        LinkInfo expectedLinkInfo = new LinkInfo("expected link");
        String linkName = "a link";

        for( String poolName : pools) {
            addPoolInLink( linkName, poolName);
            expectedLinkInfo.addPool( poolName);
        }

        assertSingleLinkInfo( linkName, expectedLinkInfo);
    }

    private void assertLinkWithUnitgroupsOk( Set<String> unitgroups) {
        LinkInfo expectedLinkInfo = new LinkInfo("expected link");
        String linkName = "a link";

        for( String unitgroupName : unitgroups) {
            addUnitgroupInLink( linkName, unitgroupName);
            expectedLinkInfo.addUnitgroup( unitgroupName);
        }

        assertSingleLinkInfo( linkName, expectedLinkInfo);
    }


    private void assertLinkWithSameUnitsOk( LinkInfo.UNIT_TYPE unitType, Set<String> units) {
        String linkName = "my link";

        LinkInfo expectedLinkInfo = new LinkInfo( "expected link");

        for( String unitName : units) {
            addUnitInLink( linkName, unitType, unitName);
            expectedLinkInfo.addUnit( unitType, unitName);
        }

        assertSingleLinkInfo( linkName, expectedLinkInfo);
    }

    /**
     * Verify that a link with only a specified Set of poolgroups is read correctly.
     */
    private void assertLinkWithPoolgroupsOk( Set<String> poolgroups) {

        LinkInfo expectedLinkInfo = new LinkInfo("expected link");
        String linkName = "a link";

        for( String poolgroupName : poolgroups) {
            addPoolgroupInLink( linkName, poolgroupName);
            expectedLinkInfo.addPoolgroup( poolgroupName);
        }

        assertSingleLinkInfo( linkName, expectedLinkInfo);
    }


    private void assertEmptyLinksOk( Set<String> linkNames) {

        for( String linkName : linkNames)
            _exhibitor.addBranch( LINKS_PATH.newChild( linkName));

        _exhibitor.visitState( _visitor);

        Map<String,LinkInfo> acquiredInfo = _visitor.getInfo();

        assertEquals( "number of found links", linkNames.size(), acquiredInfo.size());

        LinkInfo defaultLinkInfo = new LinkInfo("a representative link");

        for( String linkName : linkNames) {
            LinkInfo linkInfo = acquiredInfo.get( linkName);
            assertEquals( "LinkInfo for link " + linkName, defaultLinkInfo, linkInfo);
        }
    }

    /**
     * Check that when we visit the state we get a single LinkInfo that matches the expected
     * properties.
     * @param linkName
     * @param expectedLinkInfo
     */
    private void assertSingleLinkInfo( String linkName, LinkInfo expectedLinkInfo) {
        _exhibitor.visitState( _visitor);

        Map<String,LinkInfo> acquiredInfo = _visitor.getInfo();
        assertEquals( "wrong number of LinkInfo returned", 1, acquiredInfo.size());

        LinkInfo foundLinkInfo = acquiredInfo.get( linkName);
        assertNotNull( "couldn't find LinkInfo for link " + linkName, foundLinkInfo);

        assertEquals( "wrong information in LinkInfo for link " + linkName, expectedLinkInfo, foundLinkInfo);
    }
}
