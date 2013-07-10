package org.dcache.services.info.serialisation;

import org.junit.Before;
import org.junit.Test;

import java.io.PrintWriter;
import java.io.StringWriter;

import org.dcache.services.info.base.BooleanStateValue;
import org.dcache.services.info.base.FloatingPointStateValue;
import org.dcache.services.info.base.IntegerStateValue;
import org.dcache.services.info.base.StatePath;
import org.dcache.services.info.base.StringStateValue;
import org.dcache.services.info.base.TestStateExhibitor;

import static org.junit.Assert.assertEquals;

/**
 * This class provides a set of unit-tests for the pretty-print serialiser.  It uses
 * the TestStateExhibitor to provide the StateExhibitor back-end storage.
 * <p>
 * Please note that the tests work by comparing actual output with some "hard-coded"
 * expected output:  should a branch has multiple child elements then the hard-coded
 * expected output imposes a preferred ordering on these elements.
 * <p>
 * The StateExhibitor interface makes no guarantee that sibling StateComponents will be
 * visited in any particular order, or even that the same order will be used between
 * successive visits.
 * <p>
 * It is not intended that the PrettyPrintTextSerialiser presents output with a particular
 * ordering: it may serialise the output in whatever order it visits the dCache state.
 * Therefore, the tests are fragile: the ordering may change and, should this happen, the
 * tests will return incorrectly fail.
 * <p>
 * In practice, both the "real" infrastructure and the test infrastructure
 * (TestStateExhibitor) use a HashMap to store information.  The ordering a StateVistor
 * object will encounter is determined by how the HashMap iterator behaves. This, too, makes
 * no guarantees, but in practice, the small set of distinct strings that are used have
 * (deterministically) the stated ordering.
 */
public class PrettyPrintTextSerialiserTests {

    private static final String ELEMENT_PATH1_NAME = "element-1";
    private static final String ELEMENT_PATH2_NAME = "element-2";
    private static final String ELEMENT_PATH1_1_NAME = "element-1-1";
    private static final String ELEMENT_PATH1_2_NAME = "element-1-2";

    private static final String PRETTY_PRINT_SERIALISER_NAME = "pretty-print";

    private static final StatePath SINGLE_ELEMENT_PATH1 = StatePath.parsePath( ELEMENT_PATH1_NAME);
    private static final StatePath SINGLE_ELEMENT_PATH2 = StatePath.parsePath( ELEMENT_PATH2_NAME);
    private static final StatePath MULTIPLE_ELEMENT_PATH_1_1 = SINGLE_ELEMENT_PATH1.newChild( ELEMENT_PATH1_1_NAME);
    private static final StatePath MULTIPLE_ELEMENT_PATH_1_2 = SINGLE_ELEMENT_PATH1.newChild( ELEMENT_PATH1_2_NAME);

    private PrettyPrintTextSerialiser _serialiser;
    private TestStateExhibitor _exhibitor;

    private StringWriter _expectedResultStringWriter;
    private PrintWriter _expectedResult;

    @Before
    public void setUp() {
        _exhibitor = new TestStateExhibitor();
        PrettyPrintTextSerialiser serialiser = new PrettyPrintTextSerialiser();
        serialiser.setStateExhibitor(_exhibitor);
        _serialiser = serialiser;
        _expectedResultStringWriter = new StringWriter();
        _expectedResult = new PrintWriter( _expectedResultStringWriter);
    }

    @Test
    public void testGetName() {
        String name = _serialiser.getName();
        assertEquals( "checking serialiser's name", PRETTY_PRINT_SERIALISER_NAME, name);
    }

    @Test
    public void testEmptySerialisationNoSkip() {
        _expectedResult.println( "[dCache]");
        assertResult( "checking empty state");
    }

    @Test
    public void testEmptySerialisationWithSingleElementSkip() {
        assertResult("checking empty state with single element skip", SINGLE_ELEMENT_PATH1);
    }

    @Test
    public void testEmptySerialisationWithMultipleElementSkip() {
        assertResult("checking empty state with multiple element skip", MULTIPLE_ELEMENT_PATH_1_1);
    }

    @Test
    public void testSingleRootBranchNoSkip() {
        _expectedResult.println( "[dCache]");
        _expectedResult.println( " | ");
        _expectedResult.println( " +-[" + ELEMENT_PATH1_NAME + "]");

        _exhibitor.addBranch( SINGLE_ELEMENT_PATH1);

        assertResult( "checking single-branch with no skip");
    }

    @Test
    public void testSingleRootBranchSingleElementSkip() {
        _expectedResult.println( "[dCache." + ELEMENT_PATH1_NAME + "]");

        _exhibitor.addBranch( SINGLE_ELEMENT_PATH1);

        assertResult( "checking single-branch with single element skip", SINGLE_ELEMENT_PATH1);
    }

    @Test
    public void testSingleRootBranchMultipleElementSkip() {
        _exhibitor.addBranch( SINGLE_ELEMENT_PATH1);

        assertResult( "checking single-branch with multiple element skip", MULTIPLE_ELEMENT_PATH_1_1);
    }

    @Test
    public void testMultipleRootBranchesNoSkip() {
        _expectedResult.println( "[dCache]");
        _expectedResult.println( " | ");
        _expectedResult.println( " +-[" + ELEMENT_PATH1_NAME + "]");
        _expectedResult.println( " | ");
        _expectedResult.println( " +-[" + ELEMENT_PATH2_NAME + "]");

        _exhibitor.addBranch( SINGLE_ELEMENT_PATH1);
        _exhibitor.addBranch( SINGLE_ELEMENT_PATH2);

        assertResult( "checking multiple root branches with no skip");
    }

    @Test
    public void testMultipleRootBranchesSingleElementSkip() {
        _expectedResult.println( "[dCache."+ ELEMENT_PATH1_NAME +"]");

        _exhibitor.addBranch( SINGLE_ELEMENT_PATH1);
        _exhibitor.addBranch( SINGLE_ELEMENT_PATH2);

        assertResult( "checking multiple root branches with single element skip", SINGLE_ELEMENT_PATH1);
    }


    @Test
    public void testMultipleRootBranchesMultipleElementSkip() {
        _exhibitor.addBranch( SINGLE_ELEMENT_PATH1);
        _exhibitor.addBranch( SINGLE_ELEMENT_PATH2);

        assertResult( "checking multiple root branches with multiple element skip", MULTIPLE_ELEMENT_PATH_1_1);
    }

    @Test
    public void testRootStringMetricNoSkip() {
        String value = "foo";

        _expectedResult.println( "[dCache]");
        _expectedResult.println( " | ");
        _expectedResult.println( " +--" + ELEMENT_PATH1_NAME + ": \"" + value + "\"  [string]");

        _exhibitor.addMetric( SINGLE_ELEMENT_PATH1, new StringStateValue(value));

        assertResult( "checking root string metric with no skip");
    }

    @Test
    public void testRootTwoStringMetricsNoSkip() {
        String value1 = "foo";
        String value2 = "bar";

        _expectedResult.println( "[dCache]");
        _expectedResult.println( " | ");
        _expectedResult.println( " +--" + ELEMENT_PATH1_NAME + ": \"" + value1 + "\"  [string]");
        _expectedResult.println( " +--" + ELEMENT_PATH2_NAME + ": \"" + value2 + "\"  [string]");

        _exhibitor.addMetric( SINGLE_ELEMENT_PATH1, new StringStateValue(value1));
        _exhibitor.addMetric( SINGLE_ELEMENT_PATH2, new StringStateValue(value2));

        assertResult( "checking two root string metrics with no skip");
    }


    @Test
    public void testRootBooleanMetricsNoSkip() {
        final boolean value = false;

        _expectedResult.println( "[dCache]");
        _expectedResult.println( " | ");
        _expectedResult.println( " +--" + ELEMENT_PATH1_NAME + ": false  [boolean]");

        _exhibitor.addMetric( SINGLE_ELEMENT_PATH1, new BooleanStateValue(value));

        assertResult( "checking root boolean metric with no skip");
    }

    @Test
    public void testRootIntegerMetricsNoSkip() {
        final long value = 12345;

        _expectedResult.println( "[dCache]");
        _expectedResult.println( " | ");
        _expectedResult.println( " +--" + ELEMENT_PATH1_NAME + ": " + value + "  [integer]");

        _exhibitor.addMetric( SINGLE_ELEMENT_PATH1, new IntegerStateValue(value));

        assertResult( "checking root integer metric with no skip");
    }


    @Test
    public void testRootFloatMetricsNoSkip() {
        final double value = 3.2F;

        _expectedResult.println( "[dCache]");
        _expectedResult.println( " | ");
        _expectedResult.println( " +--" + ELEMENT_PATH1_NAME + ": " + value + "  [float]");

        _exhibitor.addMetric( SINGLE_ELEMENT_PATH1, new FloatingPointStateValue(value));

        assertResult( "checking root floating-point metric with no skip");
    }

    @Test
    public void testBranchStringMetricNoSkip() {
        final String value="something";

        _expectedResult.println( "[dCache]");
        _expectedResult.println( " | ");
        _expectedResult.println( " +-[" + ELEMENT_PATH1_NAME + "]");
        _expectedResult.println( "    | ");
        _expectedResult.println( "    +--" + ELEMENT_PATH1_1_NAME + ": \"" + value + "\"  [string]");

        _exhibitor.addMetric( MULTIPLE_ELEMENT_PATH_1_1, new StringStateValue(value));

        assertResult( "checking branch string metric with no skip");
    }

    @Test
    public void testBranchStringMetricWithRootBranchNoSkip() {
        final String value="something";

        _expectedResult.println( "[dCache]");
        _expectedResult.println( " | ");
        _expectedResult.println( " +-[" + ELEMENT_PATH1_NAME + "]");
        _expectedResult.println( " |  | ");
        _expectedResult.println( " |  +--" + ELEMENT_PATH1_1_NAME + ": \"" + value + "\"  [string]");
        _expectedResult.println( " | ");
        _expectedResult.println( " +-[" + ELEMENT_PATH2_NAME + "]");

        _exhibitor.addMetric( MULTIPLE_ELEMENT_PATH_1_1, new StringStateValue(value));
        _exhibitor.addBranch( SINGLE_ELEMENT_PATH2);

        assertResult( "checking branch string metric with no skip");
    }

    @Test
    public void testBranchStringMetricWithRootStringMetricNoSkip() {
        final String branchMetricValue="something";
        final String rootMetricValue="else";

        _expectedResult.println( "[dCache]");
        _expectedResult.println( " | ");
        _expectedResult.println( " +-[" + ELEMENT_PATH1_NAME + "]");
        _expectedResult.println( " |  | ");
        _expectedResult.println( " |  +--" + ELEMENT_PATH1_1_NAME + ": \"" + branchMetricValue + "\"  [string]");
        _expectedResult.println( " | ");
        _expectedResult.println( " +--" + ELEMENT_PATH2_NAME + ": \"" + rootMetricValue + "\"  [string]");

        _exhibitor.addMetric( MULTIPLE_ELEMENT_PATH_1_1, new StringStateValue(branchMetricValue));
        _exhibitor.addMetric( SINGLE_ELEMENT_PATH2, new StringStateValue(rootMetricValue));

        assertResult( "checking branch string metric with no skip");
    }


    @Test
    public void testTwoBranchStringMetricsNoSkip() {
        final String branchMetric1Value="something";
        final String branchMetric2Value="else";

        _expectedResult.println( "[dCache]");
        _expectedResult.println( " | ");
        _expectedResult.println( " +-[" + ELEMENT_PATH1_NAME + "]");
        _expectedResult.println( "    | ");
        _expectedResult.println( "    +--" + ELEMENT_PATH1_1_NAME + ": \"" + branchMetric1Value + "\"  [string]");
        _expectedResult.println( "    +--" + ELEMENT_PATH1_2_NAME + ": \"" + branchMetric2Value + "\"  [string]");

        _exhibitor.addMetric( MULTIPLE_ELEMENT_PATH_1_1, new StringStateValue(branchMetric1Value));
        _exhibitor.addMetric( MULTIPLE_ELEMENT_PATH_1_2, new StringStateValue(branchMetric2Value));

        assertResult( "checking branch string metric with no skip");
    }

    @Test
    public void testTwoBranchStringMetricsWithRootMetricNoSkip() {
        final String branchMetric1Value="something";
        final String branchMetric2Value="else";
        final String rootMetricValue="entirely";

        _expectedResult.println( "[dCache]");
        _expectedResult.println( " | ");
        _expectedResult.println( " +-[" + ELEMENT_PATH1_NAME + "]");
        _expectedResult.println( " |  | ");
        _expectedResult.println( " |  +--" + ELEMENT_PATH1_1_NAME + ": \"" + branchMetric1Value + "\"  [string]");
        _expectedResult.println( " |  +--" + ELEMENT_PATH1_2_NAME + ": \"" + branchMetric2Value + "\"  [string]");
        _expectedResult.println( " | ");
        _expectedResult.println( " +--" + ELEMENT_PATH2_NAME + ": \"" + rootMetricValue + "\"  [string]");

        _exhibitor.addMetric( MULTIPLE_ELEMENT_PATH_1_1, new StringStateValue(branchMetric1Value));
        _exhibitor.addMetric( MULTIPLE_ELEMENT_PATH_1_2, new StringStateValue(branchMetric2Value));
        _exhibitor.addMetric( SINGLE_ELEMENT_PATH2, new StringStateValue(rootMetricValue));

        assertResult( "checking branch string metric with no skip");
    }

    @Test
    public void testRootListItem() {
        final String className = "widget";
        final String idName = "id";

        _expectedResult.println( "[dCache]");
        _expectedResult.println( " | ");
        _expectedResult.println( " +-[" + className + ", " + idName + "=\"" + ELEMENT_PATH1_NAME + "\"]");

        _exhibitor.addListItem( SINGLE_ELEMENT_PATH1, className, idName);

        assertResult( "checking single list item from root");
    }

    @Test
    public void testRootTwoListItems() {
        final String className = "widget";
        final String idName = "id";

        _expectedResult.println( "[dCache]");
        _expectedResult.println( " | ");
        _expectedResult.println( " +-[" + className + ", " + idName + "=\"" + ELEMENT_PATH1_NAME + "\"]");
        _expectedResult.println( " | ");
        _expectedResult.println( " +-[" + className + ", " + idName + "=\"" + ELEMENT_PATH2_NAME + "\"]");

        _exhibitor.addListItem( SINGLE_ELEMENT_PATH1, className, idName);
        _exhibitor.addListItem( SINGLE_ELEMENT_PATH2, className, idName);

        assertResult( "checking single list item from root");
    }

    private void assertResult( String message) {
        String actualResult = _serialiser.serialise();
        assertActualResultIsExpected( message, actualResult);
    }

    private void assertResult( String message, StatePath initialSkip) {
        String actualResult = _serialiser.serialise( initialSkip);
        assertActualResultIsExpected( message, actualResult);
    }

    private void assertActualResultIsExpected( String message, String actualResult) {
        String expectedResult = _expectedResultStringWriter.toString();
        assertEquals( message, expectedResult, actualResult);
    }
}
