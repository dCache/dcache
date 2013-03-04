package org.dcache.services.info.base;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

/**
 * A set of tests to verify that MalleableStateTransition behaves as
 * expected.
 * <p>
 * Please note: these tests depend heavily on {@link TestStateExhibitor}.
 */
public class MalleableStateTransitionTests {

    private static final StatePath METRIC_PARENT_PATH = StatePath.parsePath( "aa.bb");
    private static final StatePath METRIC_PATH = METRIC_PARENT_PATH.newChild( "metric");
    private static final StateValue METRIC_VALUE = new StringStateValue(
                                                                         "a string value");

    MalleableStateTransition _transition;
    TestStateExhibitor _exhibitor;
    VerifyingVisitor _visitor;

    @Before
    public void setUp()
    {
        _transition = new MalleableStateTransition();
        _exhibitor = new TestStateExhibitor();
        _visitor = new VerifyingVisitor();
    }

    @Test
    public void testAddSingleMetric() {
        _transition.updateTransitionForNewMetric( METRIC_PATH, METRIC_VALUE, 0);
        _visitor.addExpectedMetric( METRIC_PATH, METRIC_VALUE);

        assertVisitorSatisfied();
    }

    @Test
    public void testAddMetricToRoot() {
        StatePath path = new StatePath("new-metric");
        _transition.updateTransitionForNewMetric( path, METRIC_VALUE, 0);
        _visitor.addExpectedMetric( path, METRIC_VALUE);

        assertVisitorSatisfied();
    }

    @Test
    public void testAddTwoMetricsSameBranch() {
        StateValue metricValue1 = new StringStateValue( "some string value");

        StatePath metricPath2 = METRIC_PARENT_PATH.newChild( "metric2");
        StateValue metricValue2 = new StringStateValue(
                                                        "some other string value");

        _transition.updateTransitionForNewMetric( METRIC_PATH, metricValue1, 0);
        _transition.updateTransitionForNewMetric(
                                                  metricPath2,
                                                  metricValue2,
                                                  METRIC_PARENT_PATH._elements.size());

        _visitor.addExpectedMetric( METRIC_PATH, metricValue1);
        _visitor.addExpectedMetric( metricPath2, metricValue2);

        assertVisitorSatisfied();
    }

    @Test
    public void testAddTwoMetricsDifferentBranches() {
        StatePath commonPath = StatePath.parsePath( "aa");

        StatePath metricPath1 = commonPath.newChild( StatePath.parsePath( "branch1.metric"));
        StateValue metricValue1 = new StringStateValue( "some string value");

        StatePath metricPath2 = commonPath.newChild( StatePath.parsePath( "branch2.metric"));
        StateValue metricValue2 = new StringStateValue(
                                                        "some other string value");

        _transition.updateTransitionForNewMetric( metricPath1, metricValue1, 0);
        _transition.updateTransitionForNewMetric( metricPath2, metricValue2,
                                                  commonPath._elements.size());

        _visitor.addExpectedMetric( metricPath1, metricValue1);
        _visitor.addExpectedMetric( metricPath2, metricValue2);

        assertVisitorSatisfied();
    }

    @Test
    public void testAddSingleBranch() {
        _transition.updateTransitionForNewBranch( METRIC_PARENT_PATH, 0);
        _visitor.addExpectedBranch( METRIC_PARENT_PATH);

        assertVisitorSatisfied();
    }

    @Test
    public void testAddTwoBranches() {
        StatePath branchPath1 = METRIC_PARENT_PATH.newChild( "branch1");
        StatePath branchPath2 = METRIC_PARENT_PATH.newChild( "branch2");

        _transition.updateTransitionForNewBranch( branchPath1, 0);
        _transition.updateTransitionForNewBranch(
                                                  branchPath2,
                                                  METRIC_PARENT_PATH._elements.size());
        _visitor.addExpectedBranch( branchPath1);
        _visitor.addExpectedBranch( branchPath2);

        assertVisitorSatisfied();
    }

    @Test
    public void testUpdateMetric() {
        _exhibitor.addMetric( METRIC_PATH,
                              new StringStateValue( "the old string value"));

        StateValue metricValue = new StringStateValue( "a new string value");

        _transition.updateTransitionForNewMetric( METRIC_PATH, metricValue,
                                                  METRIC_PATH._elements.size());

        _visitor.addExpectedMetric( METRIC_PATH, metricValue);

        assertVisitorSatisfied();
    }

    @Test
    public void testRemoveMetric() {
        _exhibitor.addMetric( METRIC_PATH, METRIC_VALUE);

        _transition.updateTransitionForRemovingElement( METRIC_PATH);

        // the branches leading up to the metric will remain
        _visitor.addExpectedBranch( METRIC_PARENT_PATH);

        assertVisitorSatisfied();
    }

    @Test
    public void testRemoveBranchAndMetric() {
        _exhibitor.addMetric( METRIC_PATH, METRIC_VALUE);

        _transition.updateTransitionForRemovingElement( METRIC_PARENT_PATH);

        // the branches leading up to the metric-branch will remain
        _visitor.addExpectedBranch( METRIC_PARENT_PATH.parentPath());

        assertVisitorSatisfied();
    }

    @Test
    public void testChangeBranchToMetric() {
        _exhibitor.addBranch( METRIC_PATH);

        StateValue metricValue = new StringStateValue("some value");
        _transition.updateTransitionForNewMetric( METRIC_PATH, metricValue, 3);

        _visitor.addExpectedMetric( METRIC_PATH, metricValue);
        assertVisitorSatisfied();
    }

    @Test
    public void testChangeMetricToBranch() {
        _exhibitor.addMetric( METRIC_PATH, METRIC_VALUE);

        _transition.updateTransitionChangingMetricToBranch( METRIC_PATH, 3);

        _visitor.addExpectedBranch( METRIC_PATH);
        assertVisitorSatisfied();
    }

    @Test
    public void testChangeMetricToBranchWithSubbranch() {
        _exhibitor.addMetric( METRIC_PATH, METRIC_VALUE);

        _transition.updateTransitionChangingMetricToBranch( METRIC_PATH, 3);

        StatePath branchPath = METRIC_PATH.newChild("sub-branch");
        _transition.updateTransitionForNewBranch( branchPath, 3);

        _visitor.addExpectedBranch( branchPath);
        assertVisitorSatisfied();
    }

    /**
     * A handy wrapper to check that the {@link VerifyingVisitor} is
     * satisfied with the {@link TestStateExhibitor} when visiting with the
     * current MalleableStateTransition.
     */
    private void assertVisitorSatisfied() {
        StateExhibitor futureState = new PostTransitionStateExhibitor( _exhibitor, _transition);
        futureState.visitState( _visitor);
        assertTrue( "visitor satisfied", _visitor.satisfied());
    }

}
