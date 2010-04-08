package org.dcache.services.info.base;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

/**
 * This class provides a set of tests to check the behaviour of
 * {@link TestStateExhibitor}. Some of the tests involve the
 * {@link MalleableStateTransition} which, in turn, is tested using a
 * TestStateExhibitor, so not all tests are completely independent.
 */
public class TestStateExhibitorTests {

    TestStateExhibitor _exhibitor;
    static final StatePath METRIC_PATH =
            StatePath.parsePath( "aa.bb.metric value");

    @Before
    public void setUp() throws Exception {
        _exhibitor = new TestStateExhibitor();
    }

    @Test
    public void testEmptyExhibitor() {
        VerifyingVisitor visitor = new VerifyingVisitor();

        _exhibitor.visitState( visitor);

        assertTrue( "VerifyingVisitor is satisfied", visitor.satisfied());
    }

    @Test
    public void testSingleStringMetricExhibitor() {
        assertSingleMetricOk( new StringStateValue( "a typical string value"));
    }

    @Test
    public void testSingleIntegerMetricExhibitor() {
        assertSingleMetricOk( new IntegerStateValue( 42));
    }

    @Test
    public void testSingleFloatingPointMetricExhibitor() {
        assertSingleMetricOk( new FloatingPointStateValue( 42.3));
    }

    @Test
    public void testSingleBooleanMetricExhibitor() {
        assertSingleMetricOk( new BooleanStateValue( true));
    }

    @Test
    public void testMultipleSiblingMetricExhibitor() {
        VerifyingVisitor visitor = new VerifyingVisitor();

        StatePath metricBranch = StatePath.parsePath( "aa.bb");

        StatePath metricPath1 = metricBranch.newChild( "metric 1");
        StatePath metricPath2 = metricBranch.newChild( "metric 2");

        StateValue metricValue1 =
                new StringStateValue( "a typical string value");
        StateValue metricValue2 = new IntegerStateValue( 42);

        _exhibitor.addMetric( metricPath1, metricValue1);
        _exhibitor.addMetric( metricPath2, metricValue2);
        visitor.addExpectedMetric( metricPath1, metricValue1);
        visitor.addExpectedMetric( metricPath2, metricValue2);

        _exhibitor.visitState( visitor);

        assertTrue( "VerifyingVisitor is satisfied", visitor.satisfied());
    }

    @Test
    public void testSingleBranch() {
        StatePath branchPath = StatePath.parsePath( "aa.bb.cc");

        _exhibitor.addBranch( branchPath);

        VerifyingVisitor visitor = new VerifyingVisitor();

        visitor.addExpectedBranch( branchPath);

        _exhibitor.visitState( visitor);

        assertTrue( "VerifyingVisitor is satisfied", visitor.satisfied());
    }

    @Test
    public void testTwoSiblingBranches() {
        StatePath branchPath1 = StatePath.parsePath( "aa.bb.cc.branch1");
        StatePath branchPath2 = StatePath.parsePath( "aa.bb.cc.branch2");

        _exhibitor.addBranch( branchPath1);
        _exhibitor.addBranch( branchPath2);

        VerifyingVisitor visitor = new VerifyingVisitor();

        visitor.addExpectedBranch( branchPath1);
        visitor.addExpectedBranch( branchPath2);

        _exhibitor.visitState( visitor);

        assertTrue( "VerifyingVisitor is satisfied", visitor.satisfied());
    }

    @Test
    public void testThreeSiblingBranches() {
        StatePath branchPath1 = StatePath.parsePath( "aa.bb.cc.branch1");
        StatePath branchPath2 = StatePath.parsePath( "aa.bb.cc.branch2");
        StatePath branchPath3 = StatePath.parsePath( "aa.bb.cc.branch3");

        _exhibitor.addBranch( branchPath1);
        _exhibitor.addBranch( branchPath2);
        _exhibitor.addBranch( branchPath3);

        VerifyingVisitor visitor = new VerifyingVisitor();

        visitor.addExpectedBranch( branchPath1);
        visitor.addExpectedBranch( branchPath2);
        visitor.addExpectedBranch( branchPath3);

        _exhibitor.visitState( visitor);

        assertTrue( "VerifyingVisitor is satisfied", visitor.satisfied());
    }

    @Test
    public void testFourSiblingBranches() {
        StatePath branchPath1 = StatePath.parsePath( "aa.bb.cc.branch1");
        StatePath branchPath2 = StatePath.parsePath( "aa.bb.cc.branch2");
        StatePath branchPath3 = StatePath.parsePath( "aa.bb.cc.branch3");
        StatePath branchPath4 = StatePath.parsePath( "aa.bb.cc.branch4");

        _exhibitor.addBranch( branchPath1);
        _exhibitor.addBranch( branchPath2);
        _exhibitor.addBranch( branchPath3);
        _exhibitor.addBranch( branchPath4);

        VerifyingVisitor visitor = new VerifyingVisitor();

        visitor.addExpectedBranch( branchPath1);
        visitor.addExpectedBranch( branchPath2);
        visitor.addExpectedBranch( branchPath3);
        visitor.addExpectedBranch( branchPath4);

        _exhibitor.visitState( visitor);

        assertTrue( "VerifyingVisitor is satisfied", visitor.satisfied());
    }

    @Test
    public void testFourSiblingBranchesWithSubbranches() {
        StatePath branchPath1 = StatePath.parsePath( "aa.bb.cc.branch1");
        StatePath branchPath2a = StatePath.parsePath( "aa.bb.cc.branch2.a");
        StatePath branchPath2b = StatePath.parsePath( "aa.bb.cc.branch2.b");
        StatePath branchPath3 = StatePath.parsePath( "aa.bb.cc.branch3");
        StatePath branchPath3a = StatePath.parsePath( "aa.bb.cc.branch3.a");
        StatePath branchPath3b = StatePath.parsePath( "aa.bb.cc.branch3.b");
        StatePath branchPath3c = StatePath.parsePath( "aa.bb.cc.branch3.c");
        StatePath branchPath4 = StatePath.parsePath( "aa.bb.cc.branch4");

        _exhibitor.addBranch( branchPath1);
        _exhibitor.addBranch( branchPath2a);
        _exhibitor.addBranch( branchPath2b);
        _exhibitor.addBranch( branchPath3);
        _exhibitor.addBranch( branchPath3a);
        _exhibitor.addBranch( branchPath3b);
        _exhibitor.addBranch( branchPath3c);
        _exhibitor.addBranch( branchPath4);

        VerifyingVisitor visitor = new VerifyingVisitor();

        visitor.addExpectedBranch( branchPath1);
        visitor.addExpectedBranch( branchPath2a);
        visitor.addExpectedBranch( branchPath2b);
        visitor.addExpectedBranch( branchPath3);
        visitor.addExpectedBranch( branchPath3a);
        visitor.addExpectedBranch( branchPath3b);
        visitor.addExpectedBranch( branchPath3c);
        visitor.addExpectedBranch( branchPath4);

        _exhibitor.visitState( visitor);

        assertTrue( "VerifyingVisitor is satisfied", visitor.satisfied());
    }

    @Test
    public void testRealisticExample() {
        StatePath link1Pool1 = StatePath.parsePath( "links.link-1.pools.pool1");
        StatePath link1Pool2 = StatePath.parsePath( "links.link-1.pools.pool2");
        StatePath link1ReadPrefPath =
                StatePath.parsePath( "links.link-1.prefs.read");
        StateValue link1ReadPrefMetric = new IntegerStateValue( 5);
        StatePath link1WritePrefPath =
                StatePath.parsePath( "links.link-1.prefs.write");
        StateValue link1WritePrefMetric = new IntegerStateValue( 7);
        StatePath link1P2pPrefPath =
                StatePath.parsePath( "links.link-1.prefs.p2p");
        StateValue link1P2pPrefMetric = new IntegerStateValue( 11);
        StatePath link1CachePrefPath =
                StatePath.parsePath( "links.link-1.prefs.cache");
        StateValue link1CachePrefMetric = new IntegerStateValue( 13);

        _exhibitor.addBranch( link1Pool1);
        _exhibitor.addBranch( link1Pool2);
        _exhibitor.addMetric( link1ReadPrefPath, link1ReadPrefMetric);
        _exhibitor.addMetric( link1WritePrefPath, link1WritePrefMetric);
        _exhibitor.addMetric( link1P2pPrefPath, link1P2pPrefMetric);
        _exhibitor.addMetric( link1CachePrefPath, link1CachePrefMetric);

        VerifyingVisitor visitor = new VerifyingVisitor();

        visitor.addExpectedBranch( link1Pool1);
        visitor.addExpectedBranch( link1Pool2);
        visitor.addExpectedMetric( link1ReadPrefPath, link1ReadPrefMetric);
        visitor.addExpectedMetric( link1WritePrefPath, link1WritePrefMetric);
        visitor.addExpectedMetric( link1P2pPrefPath, link1P2pPrefMetric);
        visitor.addExpectedMetric( link1CachePrefPath, link1CachePrefMetric);

        _exhibitor.visitState( visitor);

        assertTrue( "VerifyingVisitor is satisfied", visitor.satisfied());
    }

    @Test
    public void testListItemExhibitor() {
        StatePath path = StatePath.parsePath( "SimpleWidget");
        String type = "widget";
        String idName = "id";

        _exhibitor.addListItem( path, type, idName);

        AssertableMetadataCapturingVisitor visitor =
                new AssertableMetadataCapturingVisitor();
        _exhibitor.visitState( visitor);
        visitor.assertListMetadata( path, type, idName);
    }

    @Test
    public void testTwoListItemExhibitor() {
        StatePath path1 = StatePath.parsePath( "SimpleWidget");
        String type1 = "widget";
        String idName1 = "id";

        StatePath path2 = StatePath.parsePath( "bobble");
        String type2 = "bibble";
        String idName2 = "name";

        _exhibitor.addListItem( path1, type1, idName1);
        _exhibitor.addListItem( path2, type2, idName2);

        AssertableMetadataCapturingVisitor visitor =
                new AssertableMetadataCapturingVisitor();
        _exhibitor.visitState( visitor);
        visitor.assertListMetadata( path1, type1, idName1);
        visitor.assertListMetadata( path2, type2, idName2);
    }

    @Test
    public void testSingleDepthSingleListItemExhibitor() {
        StatePath path = StatePath.parsePath( "widgets.SimpleWidget");
        String type = "widget";
        String idName = "id";

        _exhibitor.addListItem( path, type, idName);

        AssertableMetadataCapturingVisitor visitor =
                new AssertableMetadataCapturingVisitor();
        _exhibitor.visitState( visitor);
        visitor.assertListMetadata( path, type, idName);
    }

    @Test
    public void testSingleDepthTwoListItemsExhibitor() {
        StatePath path1 = StatePath.parsePath( "widgets.SimpleWidget");
        StatePath path2 = StatePath.parsePath( "widgets.ComplexWidget");
        String type = "widget";
        String idName = "id";

        _exhibitor.addListItem( path1, type, idName);
        _exhibitor.addListItem( path2, type, idName);

        AssertableMetadataCapturingVisitor visitor =
                new AssertableMetadataCapturingVisitor();
        _exhibitor.visitState( visitor);
        visitor.assertListMetadata( path1, type, idName);
        visitor.assertListMetadata( path2, type, idName);
    }

    @Test
    public void testTwoDeepSingleListItemExhibitor() {
        StatePath path =
                StatePath.parsePath( "widgets.boringWidgets.SimpleWidget");
        String type = "widget";
        String idName = "id";

        _exhibitor.addListItem( path, type, idName);

        AssertableMetadataCapturingVisitor visitor =
                new AssertableMetadataCapturingVisitor();
        _exhibitor.visitState( visitor);
        visitor.assertListMetadata( path, type, idName);
    }

    /**
     * TESTS INVOLVING CLONED Node STRUCTURE
     */

    @Test
    public void testSingleStringMetricClonedExhibitor() {
        assertSingleMetricClonedOk( new StringStateValue(
                                                          "a typical string value"));
    }

    @Test
    public void testSingleIntegerMetricClonedExhibitor() {
        assertSingleMetricClonedOk( new IntegerStateValue( 42));
    }

    @Test
    public void testSingleFloatingPointMetricClonedExhibitor() {
        assertSingleMetricClonedOk( new FloatingPointStateValue( 42.3));
    }

    @Test
    public void testSingleBooleanMetricClonedExhibitor() {
        assertSingleMetricClonedOk( new BooleanStateValue( true));
    }

    @Test
    public void testClonedRealisticExample() {
        StatePath link1Pool1 = StatePath.parsePath( "links.link-1.pools.pool1");
        StatePath link1Pool2 = StatePath.parsePath( "links.link-1.pools.pool2");
        StatePath link1ReadPrefPath =
                StatePath.parsePath( "links.link-1.prefs.read");
        StateValue link1ReadPrefMetric = new IntegerStateValue( 5);
        StatePath link1WritePrefPath =
                StatePath.parsePath( "links.link-1.prefs.write");
        StateValue link1WritePrefMetric = new IntegerStateValue( 7);
        StatePath link1P2pPrefPath =
                StatePath.parsePath( "links.link-1.prefs.p2p");
        StateValue link1P2pPrefMetric = new IntegerStateValue( 11);
        StatePath link1CachePrefPath =
                StatePath.parsePath( "links.link-1.prefs.cache");
        StateValue link1CachePrefMetric = new IntegerStateValue( 13);

        _exhibitor.addBranch( link1Pool1);
        _exhibitor.addBranch( link1Pool2);
        _exhibitor.addMetric( link1ReadPrefPath, link1ReadPrefMetric);
        _exhibitor.addMetric( link1WritePrefPath, link1WritePrefMetric);
        _exhibitor.addMetric( link1P2pPrefPath, link1P2pPrefMetric);
        _exhibitor.addMetric( link1CachePrefPath, link1CachePrefMetric);

        VerifyingVisitor visitor = new VerifyingVisitor();

        visitor.addExpectedBranch( link1Pool1);
        visitor.addExpectedBranch( link1Pool2);
        visitor.addExpectedMetric( link1ReadPrefPath, link1ReadPrefMetric);
        visitor.addExpectedMetric( link1WritePrefPath, link1WritePrefMetric);
        visitor.addExpectedMetric( link1P2pPrefPath, link1P2pPrefMetric);
        visitor.addExpectedMetric( link1CachePrefPath, link1CachePrefMetric);

        _exhibitor.visitClonedState( visitor);

        assertTrue( "VerifyingVisitor is satisfied", visitor.satisfied());
    }

    /**
     * TESTS INVOLVING SKIPPING
     */

    @Test
    public void testSingleMetricSkipNull() {
        StatePath metricPath = StatePath.parsePath( "aa.bb.metric");
        StateValue metricValue = new StringStateValue( "some string");

        _exhibitor.addMetric( metricPath, metricValue);

        VerifyingVisitor visitor = new VerifyingVisitor();
        visitor.addExpectedMetric( metricPath, metricValue);

        _exhibitor.visitState( visitor);

        assertTrue( "visitor is satisfied", visitor.satisfied());
    }

    /*
     * PRIVATE SUPPORT METHODS.
     */

    /**
     * Add a single metric to the {@link TestStateExhibitor} and configure
     * the {@link VerifyingVisitor} so it expects this metric at the same
     * location.
     */
    private void addSingleMetricAndUpdateVisitor( VerifyingVisitor visitor,
                                                  StateValue metricValue) {
        _exhibitor.addMetric( METRIC_PATH, metricValue);
        visitor.addExpectedMetric( METRIC_PATH, metricValue);
    }

    /**
     * Add a single metric to an empty {@link TestStateExhibitor}. An empty
     * {@link VerifyingVisitor} is created and told to expect a metric at the
     * same location. The state is visited using
     * {@link TestStateExhibitor#visitState(StateVisitor)} and the
     * VerifyingVisitor is checked to see whether it is satisfied with the
     * result.
     *
     * @param metricValue the metric value to check.
     * @param useClone if true then visitClonedState() is used instead of
     *            visitState()
     */
    private void assertSingleMetricOk( StateValue metricValue) {
        VerifyingVisitor visitor = new VerifyingVisitor();

        addSingleMetricAndUpdateVisitor( visitor, metricValue);

        _exhibitor.visitState( visitor);

        assertTrue( "VerifyingVisitor is satisfied", visitor.satisfied());
    }

    /**
     * Similar to {@link #assertSingleMetricOk} but visit the state using
     * {@link TestStateExhibitor#visitClonedState(StateVisitor)}
     *
     * @param metricValue
     */
    private void assertSingleMetricClonedOk( StateValue metricValue) {
        VerifyingVisitor visitor = new VerifyingVisitor();

        addSingleMetricAndUpdateVisitor( visitor, metricValue);

        _exhibitor.visitClonedState( visitor);

        assertTrue( "VerifyingVisitor is satisfied", visitor.satisfied());
    }

    /**
     * Visitor that visits all the state, collects metadata and allows
     * list-assertions to be made about collected data.
     */
    private static class AssertableMetadataCapturingVisitor implements
            StateVisitor {

        private static final StatePath ROOT_PATH = null;

        Map<StatePath, Map<String, String>> _collectedMetadata =
                new HashMap<StatePath, Map<String, String>>();

        private Map<String, String> getMetadataForPath( StatePath path) {
            return _collectedMetadata.get( path);
        }

        private void assertListItemMetadata( StatePath path,
                                             String expectedType,
                                             String expectedIdName) {
            Map<String, String> pathMetadata = getMetadataForPath( path);

            String discoveredType =
                    pathMetadata.get( State.METADATA_BRANCH_CLASS_KEY);
            String discoveredIdName =
                    pathMetadata.get( State.METADATA_BRANCH_IDNAME_KEY);

            assertEquals( "class mismatch: " + path, expectedType,
                    discoveredType);
            assertEquals( "idName mismatch: " + path, expectedIdName,
                    discoveredIdName);
        }

        private void assertBranchHasNoMetadata( StatePath path) {
            Map<String, String> pathMetadata = getMetadataForPath( path);
            String pathLabel = path != ROOT_PATH ? path.toString() : "(root)";
            assertNull( "checking branch has no metadata: " + pathLabel,
                    pathMetadata);
        }

        private void assertBranchAndParentsHaveNoMetadata( final StatePath path) {
            StatePath currentPath = path;

            while (!currentPath.isSimplePath()) {
                assertBranchHasNoMetadata( currentPath);

                currentPath = currentPath.parentPath();
            }

            assertBranchHasNoMetadata( currentPath);
        }

        private void assertParentBranchesHaveNoMetadata( StatePath listItemPath) {
            if( !listItemPath.isSimplePath()) {
                StatePath parentPath = listItemPath.parentPath();
                assertBranchAndParentsHaveNoMetadata( parentPath);
            }

            assertBranchHasNoMetadata( ROOT_PATH);
        }

        public void assertListMetadata( StatePath path, String type,
                                        String idName) {
            assertListItemMetadata( path, type, idName);
            assertParentBranchesHaveNoMetadata( path);
        }

        @Override
        public void visitBoolean( StatePath path, BooleanStateValue value) {
        }

        @Override
        public void visitCompositePostDescend( StatePath path,
                                               Map<String, String> metadata) {
        }

        @Override
        public void visitCompositePreDescend( StatePath path,
                                              Map<String, String> metadata) {
            if( metadata != null)
                _collectedMetadata.put( path, metadata);
        }

        @Override
        public void visitFloatingPoint( StatePath path,
                                        FloatingPointStateValue value) {
        }

        @Override
        public void visitInteger( StatePath path, IntegerStateValue value) {
        }

        @Override
        public void visitString( StatePath path, StringStateValue value) {
        }

        @Override
        public boolean isVisitable( StatePath path) {
            return true;
        }
    }
}
