package org.dcache.services.info.base;

import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;

public class PostTransitionStateExhibitorTests {

    private static final StatePath PATH_ITEMS = new StatePath( "items");
    private static final StatePath PATH_ITEM1 = PATH_ITEMS.newChild( "item-1");
    private static final StatePath PATH_ITEM2 = PATH_ITEMS.newChild( "item-2");
    private static final StatePath PATH_ITEM3 = PATH_ITEMS.newChild( "item-3");

    private static final StatePath PATH_METRIC_BRANCH =
        StatePath.parsePath( "metrics");
    private static final StatePath PATH_STRING_METRIC =
        PATH_METRIC_BRANCH.newChild( "string-metric");
    private static final StatePath PATH_INTEGER_METRIC =
        PATH_METRIC_BRANCH.newChild( "integer-metric");
    private static final StatePath PATH_BOOLEAN_METRIC =
        PATH_METRIC_BRANCH.newChild( "boolean-metric");
    private static final StatePath PATH_FLOAT_METRIC =
        PATH_METRIC_BRANCH.newChild( "float-metric");

    private static final StateValue METRIC_STRING =
        new StringStateValue( "foo");
    private static final StateValue METRIC_INTEGER = new IntegerStateValue( 42);
    private static final StateValue METRIC_BOOLEAN =
        new BooleanStateValue( false);
    private static final StateValue METRIC_FLOAT =
        new FloatingPointStateValue( Math.PI);

    TestStateExhibitor _exhibitor;
    VerifyingVisitor _visitor;
    MalleableStateTransition _transition;
    StateExhibitor _post;

    @Before
    public void setUp() {
        _exhibitor = new TestStateExhibitor();
        _visitor = new VerifyingVisitor();
        _transition = new MalleableStateTransition();
        _post = new PostTransitionStateExhibitor( _exhibitor, _transition);

        addBranchToExhibitor( PATH_ITEM1);
        addBranchToExhibitor( PATH_ITEM2);
        addBranchToExhibitor( PATH_ITEM3);
        addMetricToExhibitor( PATH_STRING_METRIC, METRIC_STRING);
        addMetricToExhibitor( PATH_INTEGER_METRIC, METRIC_INTEGER);
        addMetricToExhibitor( PATH_BOOLEAN_METRIC, METRIC_BOOLEAN);
        addMetricToExhibitor( PATH_FLOAT_METRIC, METRIC_FLOAT);
    }

    @Test
    public void testVisitNoChange() {
        assertVisitorSatisfied();
    }

    @Test
    public void testAddMetricToRoot() {
        addMetricToTransition( new StatePath( "new-metric"),
                new IntegerStateValue( 42), 0);

        assertVisitorSatisfied();
    }

    @Test
    public void testAddBranchToRoot() {
        addBranchToTransition( new StatePath( "new-metric"), 0);

        assertVisitorSatisfied();
    }

    @Test
    public void testVisitAddStringMetric() {
        addMetricToTransition( PATH_METRIC_BRANCH.newChild( "new-metric"),
                new StringStateValue( "bar"), 1);

        assertVisitorSatisfied();
    }

    @Test
    public void testVisitAddIntegerMetric() {
        addMetricToTransition( PATH_METRIC_BRANCH.newChild( "new-metric"),
                new IntegerStateValue( 42), 1);

        assertVisitorSatisfied();
    }

    @Test
    public void testVisitAddBooleanMetric() {
        addMetricToTransition( PATH_METRIC_BRANCH.newChild( "new-metric"),
                new BooleanStateValue( false), 1);

        assertVisitorSatisfied();
    }

    @Test
    public void testVisitAddFloatingPointMetric() {
        addMetricToTransition( PATH_METRIC_BRANCH.newChild( "new-metric"),
                new FloatingPointStateValue( Math.E), 1);

        assertVisitorSatisfied();
    }

    @Test
    public void testVisitAddTwoMetrics() {
        addMetricToTransition( PATH_METRIC_BRANCH.newChild( "new-metric-1"),
                new StringStateValue( "bar"), 1);
        addMetricToTransition( PATH_METRIC_BRANCH.newChild( "new-metric-2"),
                new StringStateValue( "baz"), 1);

        assertVisitorSatisfied();
    }

    @Test
    public void testStringMetricChangesValue() {
        addMetricToTransition( PATH_STRING_METRIC,
                new StringStateValue( "baz"), 2);

        assertVisitorSatisfied();
    }

    @Test
    public void testStringMetricChangesToIntegerValue() {
        addMetricToTransition( PATH_STRING_METRIC, new IntegerStateValue( 99),
                2);

        assertVisitorSatisfied();
    }

    @Test
    public void testIntegerMetricChangesToStringValue() {
        addMetricToTransition( PATH_INTEGER_METRIC,
                new StringStateValue( "yet another value"), 2);

        assertVisitorSatisfied();
    }

    @Test
    public void testFloatingPointMetricChangesToStringValue() {
        addMetricToTransition( PATH_FLOAT_METRIC,
                new StringStateValue( "yet another value"), 2);

        assertVisitorSatisfied();
    }

    @Test
    public void testBooleanMetricChangesToStringValue() {
        addMetricToTransition( PATH_BOOLEAN_METRIC,
                new StringStateValue( "yet another value"), 2);

        assertVisitorSatisfied();
    }

    @Test
    public void testAddBranch() {
        addBranchToTransition( PATH_ITEMS.newChild( "new-item"), 1);

        assertVisitorSatisfied();
    }

    @Test
    public void testAddBranchWithStringMetric() {
        StatePath newMetricBranch = PATH_METRIC_BRANCH.newChild( "new-branch");
        addMetricToTransition( newMetricBranch.newChild( "string-metric"),
                new StringStateValue( "some value"), 1);

        assertVisitorSatisfied();
    }

    @Test
    public void testAddBranchWithIntegerMetric() {
        StatePath newMetricBranch = PATH_METRIC_BRANCH.newChild( "new-branch");
        addMetricToTransition( newMetricBranch.newChild( "int-metric"),
                new IntegerStateValue( 42), 1);

        assertVisitorSatisfied();
    }

    @Test
    public void testAddBranchWithBooleanMetric() {
        StatePath newMetricBranch = PATH_METRIC_BRANCH.newChild( "new-branch");
        addMetricToTransition( newMetricBranch.newChild( "boolean-metric"),
                new BooleanStateValue( true), 1);

        assertVisitorSatisfied();
    }

    @Test
    public void testAddBranchWithFloatMetric() {
        StatePath newMetricBranch = PATH_METRIC_BRANCH.newChild( "new-branch");
        addMetricToTransition( newMetricBranch.newChild( "float-metric"),
                new FloatingPointStateValue( Math.PI), 1);

        assertVisitorSatisfied();
    }

    @Test
    public void testAddBranchWithMultipleMetrics() {
        StatePath newMetricBranch = PATH_METRIC_BRANCH.newChild( "new-branch");
        addMetricToTransition( newMetricBranch.newChild( "string-metric"),
                new StringStateValue( "some value"), 1);
        addMetricToTransition( newMetricBranch.newChild( "int-metric"),
                new IntegerStateValue( 42), 1);

        assertVisitorSatisfied();
    }

    @Test
    public void testAddDeepBranchWithMetric() {
        StatePath newMetricBranch = PATH_METRIC_BRANCH.newChild( "new-branch");
        StatePath newDeeperMetricBranch =
            newMetricBranch.newChild( "new-branch");
        addMetricToTransition(
                newDeeperMetricBranch.newChild( "string-metric"),
                new StringStateValue( "some value"), 1);

        assertVisitorSatisfied();
    }

    @Test
    public void testAddDeepBranchWithMetricsAtDifferentLevels() {
        StatePath newMetricBranch = PATH_METRIC_BRANCH.newChild( "new-branch");

        addMetricToTransition( newMetricBranch.newChild( "string-metric"),
                new StringStateValue( "new value 1"), 1);

        StatePath newDeeperMetricBranch =
            newMetricBranch.newChild( "new-branch");
        addMetricToTransition(
                newDeeperMetricBranch.newChild( "string-metric"),
                new StringStateValue( "new value 2"), 2);

        assertVisitorSatisfied();
    }

    @Test
    public void testBranchBecomesMetric() {
        StatePath path = PATH_METRIC_BRANCH.newChild( "will-be-metric");
        _exhibitor.addBranch( path);

        addMetricToTransition( path, new StringStateValue( "some value"), 2);

        assertVisitorSatisfied();
    }

    @Test
    public void testBranchWithMetricsBecomesMetric() {
        StatePath branchWithMetricsPath =
            PATH_METRIC_BRANCH.newChild( "will-be-metric");
        _exhibitor.addMetric( branchWithMetricsPath.newChild( "string-metric"),
                new StringStateValue( "disappearing"));
        _exhibitor.addMetric( branchWithMetricsPath.newChild( "int-metric"),
                new IntegerStateValue( 31));
        _exhibitor.addMetric( branchWithMetricsPath.newChild( "float-metric"),
                new FloatingPointStateValue( Math.PI));
        _exhibitor.addMetric(
                branchWithMetricsPath.newChild( "boolean-metric"),
                new BooleanStateValue( true));

        addMetricToTransition( branchWithMetricsPath,
                new StringStateValue( "some value"), 2);

        assertVisitorSatisfied();
    }

    @Test
    public void testMetricBecomesPath() {
        StatePath path = PATH_METRIC_BRANCH.newChild( "will-be-branch");
        _exhibitor.addMetric( path,
                new StringStateValue( "old string metric value"));

        _transition.updateTransitionChangingMetricToBranch( path, 2);
        _visitor.addExpectedBranch( path);

        assertVisitorSatisfied();
    }

    @Test
    public void testPurgeBranchWithMetrics() {
        StatePath toBeRemovedBranch =
            PATH_METRIC_BRANCH.newChild( "remove-branch");
        _exhibitor.addMetric( toBeRemovedBranch.newChild( "int-metric"),
                new IntegerStateValue( 42));
        _exhibitor.addMetric( toBeRemovedBranch.newChild( "string-metric"),
                new StringStateValue( "some value"));

        _transition.updateTransitionForRemovingElement( toBeRemovedBranch);

        assertVisitorSatisfied();
    }

    @Test
    public void testPurgeMetrics() {
        StatePath toBeRemovedMetric1 =
            PATH_METRIC_BRANCH.newChild( "remove-metric-1");
        StatePath toBeRemovedMetric2 =
            PATH_METRIC_BRANCH.newChild( "remove-metric-2");
        _exhibitor.addMetric( toBeRemovedMetric1, new IntegerStateValue( 42));
        _exhibitor.addMetric( toBeRemovedMetric2, new IntegerStateValue( 42));

        _transition.updateTransitionForRemovingElement( toBeRemovedMetric1);
        _transition.updateTransitionForRemovingElement( toBeRemovedMetric2);

        assertVisitorSatisfied();
    }

    @Test
    public void testPurgeBranchWithBranches() {
        StatePath toBeRemovedBranch =
            PATH_METRIC_BRANCH.newChild( "remove-branch");
        StatePath doomedBranch1 =
            toBeRemovedBranch.newChild( "doomed-branch-1");
        StatePath doomedBranch2 =
            toBeRemovedBranch.newChild( "doomed-branch-2");
        _exhibitor.addBranch( doomedBranch1);
        _exhibitor.addBranch( doomedBranch2);

        _transition.updateTransitionForRemovingElement( toBeRemovedBranch);

        assertVisitorSatisfied();
    }

    @Test(expected = RuntimeException.class)
    public void testVisitWithTransitionNotAllowed() {
        StateTransition transition = new StateTransition();
        _post.visitState( _visitor, transition);
    }

    /*
     * SUPPORT METHODS ...
     */

    private void assertVisitorSatisfied() {
        _post.visitState( _visitor);
        assertTrue( _visitor.satisfied());
    }

    private void addMetricToTransition( StatePath path, StateValue metric,
                                        int existingPathElements) {
        _transition.updateTransitionForNewMetric( path, metric,
                existingPathElements);
        _visitor.addExpectedMetric( path, metric);
    }

    private void addBranchToTransition( StatePath path, int existingPathElements) {
        _transition.updateTransitionForNewBranch( path, existingPathElements);
        _visitor.addExpectedBranch( path);
    }

    private void addMetricToExhibitor( StatePath path, StateValue metric) {
        _exhibitor.addMetric( path, metric);
        _visitor.addExpectedMetric( path, metric);
    }

    private void addBranchToExhibitor( StatePath path) {
        _exhibitor.addBranch( path);
        _visitor.addExpectedBranch( path);
    }
}
