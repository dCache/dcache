/**
 *
 */
package org.dcache.services.info.base;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Date;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

/**
 * Test the StateComposite class
 *
 * @author Paul Millar <paul.millar@desy.de>
 */
public class StateCompositeTest extends InfoBaseTestHelper {

    static final String BRANCH_EPHEMERAL_NAME = "ephemeral-branch";
    static final String BRANCH_MORTAL_NAME = "mortal-branch";
    static final String BRANCH_IMMORTAL_NAME = "immortal-branch";

    static final StatePath BRANCH_EPHEMERAL_PATH = StatePath.parsePath( BRANCH_EPHEMERAL_NAME);
    static final StatePath BRANCH_MORTAL_PATH = StatePath.parsePath( BRANCH_MORTAL_NAME);
    static final StatePath BRANCH_IMMORTAL_PATH = StatePath.parsePath( BRANCH_IMMORTAL_NAME);

    /** The lifetime of all the mortal composites */
    static final long LIFETIME_MORTAL_COMPOSITE = 10;

    StateComposite _rootComposite, _ephemeralComposite, _mortalComposite, _immortalComposite;


    /**
     */
    @Before
    public void setUp() {

        // Create the immortal branches
        _rootComposite = new StateComposite( null);
        _immortalComposite = new StateComposite( null);

        // Create the ephemeral branches
        _ephemeralComposite = new StateComposite();

        // Create the mortal branches
        _mortalComposite = new StateComposite( LIFETIME_MORTAL_COMPOSITE);

        // Add branches to root branch.
        StateTransition transition = new StateTransition();
        transition.getOrCreateChangeSet(null).recordNewChild( BRANCH_EPHEMERAL_NAME, _ephemeralComposite);
        transition.getOrCreateChangeSet(null).recordNewChild( BRANCH_MORTAL_NAME, _mortalComposite);
        transition.getOrCreateChangeSet(null).recordNewChild( BRANCH_IMMORTAL_NAME, _immortalComposite);
        _rootComposite.applyTransition( null, transition);
    }

    /**
     * Test method for {@link org.dcache.services.info.base.StateComposite#StateComposite(long)}.
     */
    @Test
    public void testStateCompositeLong() {
        assertNotNull( "Cannot create mortal StateComposite with +ve life-time", new StateComposite( 60));
        assertNotNull( "Cannot create mortal StateComposite with 0s life-time.", new StateComposite( 0));
        assertNotNull( "Cannot create mortal StateComposite with -ve life-time.", new StateComposite( -5));
    }

    /**
     * Test method for {@link org.dcache.services.info.base.StateComposite#StateComposite()}.
     */
    @Test
    public void testStateComposite() {
        assertNotNull( "Cannot create ephemeral StateComposite", new StateComposite());
    }

    @Test
    public void testStateCompositeBooleanFalse() {
        assertIsEphemeral( "StateComposite(false)", new StateComposite(false));
    }

    @Test
    public void testStateCompositeBooleanTrue() {
        assertIsImmortal( "StateComposite(true)", new StateComposite(true));
    }

    /**
     * Test method for {@link org.dcache.services.info.base.StateComposite#StateComposite(org.dcache.services.info.base.StatePersistentMetadata)}.
     */
    @Test
    public void testStateCompositeStatePersistentMetadata() {
        assertNotNull( "Cannot create immortal StateComposite", new StateComposite( null));
        assertNotNull( "Cannot create immortal StateComposite", new StateComposite( new StatePersistentMetadata()));
    }

    /**
     * Test method for {@link org.dcache.services.info.base.StateComposite#getEarliestChildExpiryDate()}.
     * @throws MetricStatePathException
     */
    @Test
    public void testGetEarliestChildExpiryDate() throws MetricStatePathException {
        assertNotNull( "immortal StateComposite with mortal children returned null for getEarliestChildExpiryDate()", _rootComposite.getEarliestChildExpiryDate());
        assertNull( "immortal StateComposite with no children returned non-null for getEarliestChildExpiryDate()", _immortalComposite.getEarliestChildExpiryDate());
        assertNull( "ephemeral StateComposite with no children returned non-null for getEarliestChildExpiryDate()", _ephemeralComposite.getEarliestChildExpiryDate());
        assertNull( "mortal StateComposite returned null for getEarliestChildExpiryDate()", _mortalComposite.getEarliestChildExpiryDate());

        // Various durations, must be in descending order.
        long longMetricDuration = 120;
        long mediumMetricDuration = 60;
        long shortishMetricDuration = 20;
        long shortMetricDuration = 10;

        // How accurate Date objects needs to be.
        long tolerance = 1;

        addMetric( BRANCH_IMMORTAL_PATH.newChild( "ephemeralString"), new StringStateValue("ephemeralStringValue"));
        addMetric( BRANCH_MORTAL_PATH.newChild( "ephemeralString"), new StringStateValue("ephemeralStringValue"));
        addMetric( BRANCH_EPHEMERAL_PATH.newChild( "ephemeralString"), new StringStateValue("ephemeralStringValue"));
        assertNull( "immortal StateComposite returned non-NULL for getEarliestChildExpiryDate()", _immortalComposite.getEarliestChildExpiryDate());
        assertNull( "mortal StateComposite returned non-NULL for getEarliestChildExpiryDate()", _mortalComposite.getEarliestChildExpiryDate());
        assertNull( "immortal StateComposite returned non-NULL for getEarliestChildExpiryDate()", _ephemeralComposite.getEarliestChildExpiryDate());

        Date now = new Date();

        // Add a mortal child that will expire in the medium-term
        addMetric( BRANCH_IMMORTAL_PATH.newChild( "mediumDurationMortalString"), new StringStateValue("dummy value", mediumMetricDuration));
        addMetric( BRANCH_MORTAL_PATH.newChild( "mediumDurationMortalString"), new StringStateValue("dummy value", mediumMetricDuration));
        addMetric( BRANCH_EPHEMERAL_PATH.newChild( "mediumDurationMortalString"), new StringStateValue("dummy value", mediumMetricDuration));
        assertDatedDuration("getEarliestChildExp() returned wrong Date", now, _mortalComposite.getEarliestChildExpiryDate(), mediumMetricDuration, tolerance);
        assertDatedDuration("getEarliestChildExp() returned wrong Date", now, _immortalComposite.getEarliestChildExpiryDate(), mediumMetricDuration, tolerance);
        assertDatedDuration("getEarliestChildExp() returned wrong Date", now, _ephemeralComposite.getEarliestChildExpiryDate(), mediumMetricDuration, tolerance);

        // Add a longer-lived mortal child
        addMetric( BRANCH_IMMORTAL_PATH.newChild( "longDurationMortalString"), new StringStateValue("dummy value", longMetricDuration));
        addMetric( BRANCH_MORTAL_PATH.newChild( "longDurationMortalString"), new StringStateValue("dummy value", longMetricDuration));
        addMetric( BRANCH_EPHEMERAL_PATH.newChild( "longDurationMortalString"), new StringStateValue("dummy value", longMetricDuration));
        assertDatedDuration("getEarliestChildExp() returned wrong Date", now, _immortalComposite.getEarliestChildExpiryDate(), mediumMetricDuration, tolerance);
        assertDatedDuration("getEarliestChildExp() returned wrong Date", now, _mortalComposite.getEarliestChildExpiryDate(), mediumMetricDuration, tolerance);
        assertDatedDuration("getEarliestChildExp() returned wrong Date", now, _ephemeralComposite.getEarliestChildExpiryDate(), mediumMetricDuration, tolerance);

        // Add a short-lived mortal child.
        addMetric( BRANCH_IMMORTAL_PATH.newChild( "shortDurationMortalString"), new StringStateValue("dummy value", shortMetricDuration));
        addMetric( BRANCH_MORTAL_PATH.newChild( "shortDurationMortalString"), new StringStateValue("dummy value", shortMetricDuration));
        addMetric( BRANCH_EPHEMERAL_PATH.newChild( "shortDurationMortalString"), new StringStateValue("dummy value", shortMetricDuration));
        assertDatedDuration("getEarliestChildExp() returned wrong Date", now, _immortalComposite.getEarliestChildExpiryDate(), shortMetricDuration, tolerance);
        assertDatedDuration("getEarliestChildExp() returned wrong Date", now, _mortalComposite.getEarliestChildExpiryDate(), shortMetricDuration, tolerance);
        assertDatedDuration("getEarliestChildExp() returned wrong Date", now, _ephemeralComposite.getEarliestChildExpiryDate(), shortMetricDuration, tolerance);

        // Override the short-lived mortal child with a new one that will last a bit longer.
        addMetric( BRANCH_IMMORTAL_PATH.newChild( "shortDurationMortalString"), new StringStateValue("dummy value", shortishMetricDuration));
        addMetric( BRANCH_MORTAL_PATH.newChild( "shortDurationMortalString"), new StringStateValue("dummy value", shortishMetricDuration));
        addMetric( BRANCH_EPHEMERAL_PATH.newChild( "shortDurationMortalString"), new StringStateValue("dummy value", shortishMetricDuration));
        assertDatedDuration("getEarliestChildExp() returned wrong Date", now, _immortalComposite.getEarliestChildExpiryDate(), shortishMetricDuration, tolerance);
        assertDatedDuration("getEarliestChildExp() returned wrong Date", now, _mortalComposite.getEarliestChildExpiryDate(), shortishMetricDuration, tolerance);
        assertDatedDuration("getEarliestChildExp() returned wrong Date", now, _ephemeralComposite.getEarliestChildExpiryDate(), shortishMetricDuration, tolerance);
    }

    /**
     * Test method for {@link org.dcache.services.info.base.StateComposite#toString()}.
     * @throws MetricStatePathException
     */
    @Test
    public void testToString() throws MetricStatePathException {
        assertEquals( "Immortal with three children.", "StateComposite <#> {3}", _rootComposite.toString());
        assertEquals( "Immortal with no children.", "StateComposite <#> {0}", _immortalComposite.toString());
        assertEquals( "Mortal with no children.", "StateComposite <+> {0}", _mortalComposite.toString());
        assertEquals( "Ephemeral with no children.", "StateComposite <*> {0}", _ephemeralComposite.toString());

        // Lets add some child metrics
        addMetric( BRANCH_IMMORTAL_PATH.newChild( "test-child"), new StringStateValue("foo", 10));
        addMetric( BRANCH_MORTAL_PATH.newChild( "test-child"), new StringStateValue("foo", 10));
        addMetric( BRANCH_EPHEMERAL_PATH.newChild( "test-child"), new StringStateValue("foo", 10));

        assertEquals( "Immortal with one child.", "StateComposite <#> {1}", _immortalComposite.toString());
        assertEquals( "Mortal with one child.", "StateComposite <+> {1}", _mortalComposite.toString());
        assertEquals( "Ephemeral with one child.", "StateComposite <*> {1}", _ephemeralComposite.toString());
    }

    /**
     * Test method for {@link org.dcache.services.info.base.StateComposite#getExpiryDate()}.
     */
    @Test
    public void testGetExpiryDate() {
        long expiryTime = 60;

        StateComposite sc = new StateComposite( expiryTime);
        assertDatedDuration("mortal StateComposite has wrong expiry date.", new Date(), sc.getExpiryDate(), expiryTime, 1);

        sc = new StateComposite( 0);
        assertDatedDuration("mortal StateComposite has wrong expiry date.", new Date(), sc.getExpiryDate(), 0, 1);

        sc = new StateComposite( -1);
        assertDatedDuration("mortal StateComposite has wrong expiry date.", new Date(), sc.getExpiryDate(), 0, 1);

        sc = new StateComposite();
        assertNull( "emphemeral StateComposite returned non-null expiryDate", sc.getExpiryDate());

        sc = new StateComposite( new StatePersistentMetadata());
        assertNull( "immortal StateComposite returned non-null expiryDate", sc.getExpiryDate());
    }

    /**
     * Test method for {@link org.dcache.services.info.base.StateComposite#hasExpired()}.
     */
    @Test
    public void testHasExpired() {
        StateComposite sc = new StateComposite( 60);
        assertFalse("mortal StateComposite with +ve lifetime.", sc.hasExpired());

        sc = new StateComposite( 1);
        assertFalse("mortal StateComposite with 1s lifetime.", sc.hasExpired());
        try {
            Thread.sleep( 1000);
        } catch (InterruptedException e) {
            fail("Interrupted");
        }
        assertTrue("mortal StateComposite with 1s lifetime.", sc.hasExpired());

        sc = new StateComposite( 0);
        assertTrue("mortal StateComposite with zero lifetime.", sc.hasExpired());

        sc = new StateComposite( -1);
        assertTrue("mortal StateComposite with -ve lifetime.", sc.hasExpired());

        sc = new StateComposite();
        assertFalse( "emphemeral StateComposite", sc.hasExpired());

        sc = new StateComposite( new StatePersistentMetadata());
        assertFalse( "immortal StateComposite", sc.hasExpired());
    }

    /**
     * Test method for {@link org.dcache.services.info.base.StateComposite#acceptVisitor(org.dcache.services.info.base.StatePath, org.dcache.services.info.base.StatePath, org.dcache.services.info.base.StateVisitor)}.
     */
    @Test
    public void testAcceptVisitorStatePathStatePathStateVisitor() {
        VerifyingVisitor visitor = newDefaultVisitor();

        _rootComposite.acceptVisitor(null, visitor);

        assertTrue( "Doesn't match expected visitor-pattern", visitor.satisfied());
    }

    /**
     *  Test for AcceptVisitor: case 1
     */
    @Test
    public void testAcceptVisitorVarious() {
        VerifyingVisitor visitor = newDefaultVisitor();

        /* Add one of each metric type to the ephemeral branch */
        assertVisitorAddMetric( visitor,
                BRANCH_EPHEMERAL_PATH.newChild("string-metric"), new StringStateValue( "boo"));
        assertVisitorAddMetric( visitor,
                BRANCH_EPHEMERAL_PATH.newChild("int-metric"), new IntegerStateValue( 42));
        assertVisitorAddMetric( visitor,
                BRANCH_EPHEMERAL_PATH.newChild("float-metric"), new FloatingPointStateValue( 3.14159265));
        assertVisitorAddMetric( visitor,
                BRANCH_EPHEMERAL_PATH.newChild("bool-metric"), new BooleanStateValue( true));

        /* Add new metrics to a different branch, the mortal one. */

        assertVisitorAddMetric( visitor,
                BRANCH_MORTAL_PATH.newChild("string-metric"), new StringStateValue( "boo"));
        assertVisitorAddMetric( visitor,
                BRANCH_MORTAL_PATH.newChild("int-metric"), new IntegerStateValue( 42));
        assertVisitorAddMetric( visitor,
                BRANCH_MORTAL_PATH.newChild("float-metric"), new FloatingPointStateValue( 3.14159));
        assertVisitorAddMetric( visitor,
                BRANCH_MORTAL_PATH.newChild("bool-metric"), new BooleanStateValue( true));

        /* Update metrics on ephemeral branch values: same name with different values */

        assertVisitorAddMetric( visitor,
                BRANCH_EPHEMERAL_PATH.newChild("string-metric"), new StringStateValue( "fizz"));
        assertVisitorAddMetric( visitor,
                BRANCH_EPHEMERAL_PATH.newChild("int-metric"), new IntegerStateValue( 23));
        assertVisitorAddMetric( visitor,
                BRANCH_EPHEMERAL_PATH.newChild("float-metric"), new FloatingPointStateValue( 1.41421356));
        assertVisitorAddMetric( visitor,
                BRANCH_EPHEMERAL_PATH.newChild("bool-metric"), new BooleanStateValue( false));

        /* Create metrics on fresh branches */
        assertVisitorAddMetric( visitor,
                BRANCH_EPHEMERAL_PATH.newChild("string-metric-branch").newChild("string-metric"), new StringStateValue( "baz"));
        assertVisitorAddMetric( visitor,
                BRANCH_MORTAL_PATH.newChild("int-metric-branch").newChild("int-metric"), new IntegerStateValue( 42));
        assertVisitorAddMetric( visitor,
                BRANCH_MORTAL_PATH.newChild("float-metric-branch").newChild("float-metric"), new FloatingPointStateValue( 3.14159));
        assertVisitorAddMetric( visitor,
                BRANCH_MORTAL_PATH.newChild("bool-metric-branch").newChild("bool-metric"), new BooleanStateValue( true));
    }

    /**
     * Test method for {@link org.dcache.services.info.base.StateComposite#acceptVisitor(org.dcache.services.info.base.StateTransition, org.dcache.services.info.base.StatePath, org.dcache.services.info.base.StatePath, org.dcache.services.info.base.StateVisitor)}.
     */
    @Test
    public void testAcceptVisitorStateTransitionStatePathStatePathStateVisitor() {
        StateValue metric = new StringStateValue( "Some metric dummy data here");

        assertVisitTransition( StatePath.parsePath( "MyMetric"), metric);

        assertVisitTransition( BRANCH_EPHEMERAL_PATH.newChild("MyMetric"), metric);
        assertVisitTransition( BRANCH_MORTAL_PATH.newChild("MyMetric"), metric);
        assertVisitTransition( BRANCH_IMMORTAL_PATH.newChild("MyMetric"), metric);

        assertVisitTransition( BRANCH_EPHEMERAL_PATH.newChild("new-branch").newChild("MyMetric"), metric);
        assertVisitTransition( BRANCH_MORTAL_PATH.newChild("new-branch").newChild("MyMetric"), metric);
        assertVisitTransition( BRANCH_IMMORTAL_PATH.newChild("new-branch").newChild("MyMetric"), metric);

        assertVisitTransition( StatePath.parsePath( "aaa.bbb.ccc.MyMetric"), metric);
    }

    @Test
    public void testUpdateCompositeWithChildren() throws MetricStatePathException {
        VerifyingVisitor visitor = newDefaultVisitor();

        String metricName = "a metric";
        StateValue metricValue = new StringStateValue( "test string", 20);

        visitor.addExpectedMetric( BRANCH_MORTAL_PATH.newChild( metricName), metricValue);
        addMetric( BRANCH_MORTAL_PATH.newChild( metricName), metricValue);

        visitor.assertSatisfied( "Not satisfied before update", _rootComposite);

        /**
         * Try to update the mortal composite, extending its lifetime.
         */

        StateTransition transition = new StateTransition();
        _rootComposite.buildTransition( null, BRANCH_MORTAL_PATH, new StateComposite( LIFETIME_MORTAL_COMPOSITE +10), transition);

        visitor.assertSatisfiedTransition( "Not satisfied whilst updating", _rootComposite, transition);

        _rootComposite.applyTransition(null, transition);

        visitor.assertSatisfied( "Not satisfied after updating", _rootComposite);
    }

    /**
     * Test method for {@link org.dcache.services.info.base.StateComposite#applyTransition(org.dcache.services.info.base.StatePath, org.dcache.services.info.base.StateTransition)}.
     */
    @Test
    public void testApplyTransition() {
        StatePath metricPath = StatePath.parsePath( "foo.bar.baz");
        StateValue metricValue = new StringStateValue( "some dummy data");

        // Build the visitor we expect to pass after a metric has been added
        VerifyingVisitor visitor = newDefaultVisitor();
        visitor.addExpectedMetric( metricPath, metricValue);

        // Build transition for adding a metric
        StateTransition transition = new StateTransition();
        try {
            _rootComposite.buildTransition( null, metricPath, metricValue, transition);
        } catch (MetricStatePathException e) {
            fail( "Failed to add metric at path " + metricPath);
        }
        _rootComposite.applyTransition(null, transition);

        // Check that, after applying the transition, it works as expected.
        visitor.reset();
        _rootComposite.acceptVisitor( null, visitor);

        assertTrue( "VerifyingVisitor not satisfied after applying transition", visitor.satisfied());
    }


    @Test
    public void testApplyTransitionRemovingMetrics() throws MetricStatePathException {

        // Build a verifying visitor for the initial state: check it passes.
        VerifyingVisitor visitorNoMetric = newDefaultVisitor();
        visitorNoMetric.assertSatisfied("checking no-metric before adding metric", _rootComposite);

        // Add a metric with zero lifetime; so should it will be removed later
        String metricName = "myMetric";
        StatePath metricPath = BRANCH_MORTAL_PATH.newChild( metricName);
        StateValue metricValue = new StringStateValue( "Test string", 0);

        addMetric( metricPath, metricValue);

        // Paranoid: check this actually worked OK.
        VerifyingVisitor visitorWithMetric = newDefaultVisitor();
        visitorWithMetric.addExpectedMetric( metricPath, metricValue);
        visitorWithMetric.assertSatisfied( "problem establishing expiring metric", _rootComposite);

        // Build the removal transition, we should remove the metric we just added
        StateTransition transition = new StateTransition();
        _rootComposite.buildRemovalTransition( null, transition, false);

        StateChangeSet mortalBranchScs = transition.getStateChangeSet( BRANCH_MORTAL_PATH);

        assertScsCount( "root branch change-set", transition.getStateChangeSet( null), 0, 1, 0, 0);

        assertNotNull( "mortal branch change-set is null", mortalBranchScs);
        assertTrue( "mortal branch change-set doesn't contain expected metric", mortalBranchScs._removedChildren.contains( metricName));
        assertScsCount( "mortal branch change-set", mortalBranchScs, 0, 1, 1, 0);

        // Apply the transition
        _rootComposite.applyTransition( null, transition);

        // Check we're back were we started
        visitorNoMetric.assertSatisfied("checking no-metric after removing expired metric", _rootComposite);
    }

    @Test
    public void testApplyTransitionUpdatingStringMetrics() throws MetricStatePathException {
        // Build a verifying visitor for the initial state: check it passes.
        VerifyingVisitor visitorNoMetric = newDefaultVisitor();
        visitorNoMetric.assertSatisfied( "checking no-metric before adding metric", _rootComposite);

        // Add a string metric
        String metricName = "myMetric";
        StatePath metricPath = BRANCH_MORTAL_PATH.newChild( metricName);
        StateValue metricValue = new StringStateValue( "Test string 1", 60);

        addMetric( metricPath, metricValue);

        // Paranoid: check this actually worked OK.
        VerifyingVisitor visitorWithMetric = newDefaultVisitor();
        visitorWithMetric.addExpectedMetric( metricPath, metricValue);
        visitorWithMetric.assertSatisfied( "problem establishing initial metric", _rootComposite);

        // Add the new metric value
        StateValue newMetricValue = new StringStateValue( "Test string 2", 60);

        // Build the removal transition, we want to update the StringStateValue
        StateTransition transition = new StateTransition();
        _rootComposite.buildTransition( null, metricPath, newMetricValue, transition);

        StateChangeSet mortalBranchScs = transition.getStateChangeSet( BRANCH_MORTAL_PATH);

        assertScsCount( "root branch change-set", transition.getStateChangeSet( null), 0, 1, 0, 0);

        assertNotNull( "mortal branch change-set is null", mortalBranchScs);
        assertTrue( "mortal branch change-set doesn't contain expected metric", mortalBranchScs._updatedChildren.containsKey( metricName));
        assertScsCount( "mortal branch change-set", mortalBranchScs, 0, 0, 0, 1);

        // Apply the transition
        _rootComposite.applyTransition( null, transition);

        // Check we're back were we started
        VerifyingVisitor visitorWithNewMetric = newDefaultVisitor();
        visitorWithNewMetric.addExpectedMetric( metricPath, newMetricValue);
        visitorWithNewMetric.assertSatisfied( "problem establishing initial metric", _rootComposite);
    }

    @Test
    public void testApplyTransitionUpdatingIntegerMetrics() throws MetricStatePathException {
        // Build a verifying visitor for the initial state: check it passes.
        VerifyingVisitor visitorNoMetric = newDefaultVisitor();
        visitorNoMetric.assertSatisfied( "checking no-metric before adding metric", _rootComposite);

        // Add a string metric
        String metricName = "myMetric";
        StatePath metricPath = BRANCH_MORTAL_PATH.newChild( metricName);
        StateValue metricValue = new IntegerStateValue( 100, 60);

        addMetric( metricPath, metricValue);

        // Paranoid: check this actually worked OK.
        VerifyingVisitor visitorWithMetric = newDefaultVisitor();
        visitorWithMetric.addExpectedMetric( metricPath, metricValue);
        visitorWithMetric.assertSatisfied( "problem establishing initial metric", _rootComposite);

        // Add the new metric value
        StateValue newMetricValue = new IntegerStateValue( 200, 60);

        // Build the removal transition, we want to update the StringStateValue
        StateTransition transition = new StateTransition();
        _rootComposite.buildTransition( null, metricPath, newMetricValue, transition);

        StateChangeSet mortalBranchScs = transition.getStateChangeSet( BRANCH_MORTAL_PATH);

        assertScsCount( "root branch change-set", transition.getStateChangeSet( null), 0, 1, 0, 0);

        assertNotNull( "mortal branch change-set is null", mortalBranchScs);
        assertTrue( "mortal branch change-set doesn't contain expected metric", mortalBranchScs._updatedChildren.containsKey( metricName));
        assertScsCount( "mortal branch change-set", mortalBranchScs, 0, 0, 0, 1);

        // Apply the transition
        _rootComposite.applyTransition( null, transition);

        // Check we're back were we started
        VerifyingVisitor visitorWithNewMetric = newDefaultVisitor();
        visitorWithNewMetric.addExpectedMetric( metricPath, newMetricValue);
        visitorWithNewMetric.assertSatisfied( "problem establishing initial metric", _rootComposite);
    }



    /**
     * Test method for {@link org.dcache.services.info.base.StateComposite#buildTransition(org.dcache.services.info.base.StatePath, org.dcache.services.info.base.StatePath, org.dcache.services.info.base.StateComponent, org.dcache.services.info.base.StateTransition)}.
     *
     * Try building a transition for a metric added to the root.
     */
    @Test
    public void testBuildTransitionNewMetricForRoot() throws MetricStatePathException {
        StateTransition transition = new StateTransition();
        StateValue newMetricValue = new StringStateValue( "dummy string value", 10);
        String newMetricName = "newStringMetric";

        _rootComposite.buildTransition( null, new StatePath( newMetricName), newMetricValue, transition);

        assertNotNull( transition.getStateChangeSet(null));
        assertNull( transition.getStateChangeSet( BRANCH_IMMORTAL_PATH));
        assertNull( transition.getStateChangeSet( BRANCH_MORTAL_PATH));
        assertNull( transition.getStateChangeSet( BRANCH_EPHEMERAL_PATH));

        StateChangeSet scs = transition.getStateChangeSet(null);

        assertEquals( "new child", 1, scs.getNewChildren().size());
        assertEquals( "itr children", 0, scs.getItrChildren().size());
        assertEquals( "remove children", 0, scs.getRemovedChildren().size());
        assertEquals( "updated children", 0, scs.getUpdatedChildren().size());

        assertTrue( "new metric not found", scs.getNewChildren().contains( newMetricName));
        assertSame( "new metric is not as expected", scs.getNewChildValue( newMetricName), newMetricValue);
    }

    /**
     * Test method for {@link org.dcache.services.info.base.StateComposite#buildTransition(org.dcache.services.info.base.StatePath, org.dcache.services.info.base.StatePath, org.dcache.services.info.base.StateComponent, org.dcache.services.info.base.StateTransition)}.
     *
     * Try building a transition for a metric added to the root.
     */
    @Test
    public void testBuildTransitionUpdatedMetricForRoot() throws MetricStatePathException {
        StateTransition transition = new StateTransition();
        StateValue oldMetricValue = new StringStateValue( "dummy string value (old)", 10);
        StateValue newMetricValue = new StringStateValue( "dummy string value (new)", 10);
        String metricName = "aStringMetric";

        addMetric( new StatePath(metricName), oldMetricValue);

        _rootComposite.buildTransition( null, new StatePath( metricName), newMetricValue, transition);

        assertNotNull( transition.getStateChangeSet(null));
        assertNull( transition.getStateChangeSet( BRANCH_IMMORTAL_PATH));
        assertNull( transition.getStateChangeSet( BRANCH_MORTAL_PATH));
        assertNull( transition.getStateChangeSet( BRANCH_EPHEMERAL_PATH));

        StateChangeSet scs = transition.getStateChangeSet(null);
        assertScsCount( "scs", scs, 0, 0, 0, 1);

        assertTrue( "updated metric not found", scs.getUpdatedChildren().contains( metricName));
        assertSame( "new metric is not as expected", scs.getUpdatedChildValue( metricName), newMetricValue);
    }


    /**
     * Test method for {@link org.dcache.services.info.base.StateComposite#buildTransition(org.dcache.services.info.base.StatePath, org.dcache.services.info.base.StatePath, org.dcache.services.info.base.StateComponent, org.dcache.services.info.base.StateTransition)}.
     *
     * Try building a transition for a metric added an existing branch.
     */
    @Test
    public void testBuildTransitionNewMetricForExistingBranch() throws MetricStatePathException {
        StateTransition transition = new StateTransition();
        StateValue newMetricValue = new StringStateValue( "dummy string value", 10);
        String newMetricName = "newStringMetric";

        _rootComposite.buildTransition( null, BRANCH_EPHEMERAL_PATH.newChild( newMetricName), newMetricValue, transition);

        assertNotNull( transition.getStateChangeSet(null));
        assertNotNull( transition.getStateChangeSet( BRANCH_EPHEMERAL_PATH));
        assertNull( transition.getStateChangeSet( BRANCH_IMMORTAL_PATH));
        assertNull( transition.getStateChangeSet( BRANCH_MORTAL_PATH));

        StateChangeSet rootScs = transition.getStateChangeSet(null);
        assertScsCount( "rootScs", rootScs, 0, 1, 0, 0);

        assertTrue( "itr into ephemeral branch not found", rootScs.getItrChildren().contains( BRANCH_EPHEMERAL_NAME));

        StateChangeSet ephemeralScs = transition.getStateChangeSet( BRANCH_EPHEMERAL_PATH);
        assertScsCount( "ephemeralScs", ephemeralScs, 1, 0, 0, 0);

        assertTrue( "new metric not found", ephemeralScs.getNewChildren().contains( newMetricName));
        assertSame( "new metric is not as expected", ephemeralScs.getNewChildValue( newMetricName), newMetricValue);
    }


    /**
     * Test method for {@link org.dcache.services.info.base.StateComposite#buildTransition(org.dcache.services.info.base.StatePath, org.dcache.services.info.base.StatePath, org.dcache.services.info.base.StateComponent, org.dcache.services.info.base.StateTransition)}.
     *
     * Try building a transition for a metric that creates an on-the-fly Composite.
     */
    @Test
    public void testBuildTransitionNewMetricForNewBranch() throws MetricStatePathException {
        StateTransition transition = new StateTransition();
        StateValue newMetricValue = new StringStateValue( "dummy string value", 10);
        String newMetricName = "newStringMetric";
        String newBranchName = "newBranch";

        StatePath newBranchPath = BRANCH_EPHEMERAL_PATH.newChild( newBranchName);

        _rootComposite.buildTransition( null, newBranchPath.newChild( newMetricName), newMetricValue, transition);

        assertNotNull( transition.getStateChangeSet(null));
        assertNotNull( transition.getStateChangeSet( BRANCH_EPHEMERAL_PATH));
        assertNotNull( transition.getStateChangeSet( newBranchPath));
        assertNull( transition.getStateChangeSet( BRANCH_IMMORTAL_PATH));
        assertNull( transition.getStateChangeSet( BRANCH_MORTAL_PATH));

        StateChangeSet rootScs = transition.getStateChangeSet(null);
        assertScsCount( "root", rootScs, 0, 1, 0, 0);

        assertTrue( "itr into ephemeral branch not found", rootScs.getItrChildren().contains( BRANCH_EPHEMERAL_NAME));

        StateChangeSet ephemeralScs = transition.getStateChangeSet( BRANCH_EPHEMERAL_PATH);
        assertScsCount( "ephemeralBranch", ephemeralScs, 1, 1, 0, 0);

        assertTrue( "new branch not found", ephemeralScs.getNewChildren().contains( newBranchName));
        StateComponent newBranch = ephemeralScs.getNewChildValue( newBranchName);
        assertNotNull( "new branch is null", newBranch);
        assertIsMortal("new branch not mortal", newBranch, StateComposite.DEFAULT_LIFETIME);

        assertTrue( "itr into newBranch branch not found", ephemeralScs.getItrChildren().contains( newBranchName));

        StateChangeSet newBranchScs = transition.getStateChangeSet( newBranchPath);
        assertScsCount( "newBranch", newBranchScs, 1, 0, 0, 0);

        assertTrue( "new metric not found", newBranchScs.getNewChildren().contains( newMetricName));
        assertSame( "new metric is not as expected", newBranchScs.getNewChildValue( newMetricName), newMetricValue);
    }


    /**
     * Test method for {@link org.dcache.services.info.base.StateComposite#isEphemeral()}.
     */
    @Test
    public void testIsEphemeral() {
        StateComposite mortalStateComposite = new StateComposite( 1000);
        assertFalse("Mortal StateComposite returns wrong isEphemeral", mortalStateComposite.isEphemeral());

        StateComposite immortalStateComposite = new StateComposite( null);

        assertFalse("Immortal StateComposite returns wrong isEphemeral", immortalStateComposite.isEphemeral());

        StateComposite ephemeralStateComposite = new StateComposite();

        assertTrue("Ephemeral StateComposite returns wrong isEphemeral", ephemeralStateComposite.isEphemeral());
    }

    /**
     * Test method for {@link org.dcache.services.info.base.StateComposite#isImmortal()}.
     */
    @Test
    public void testIsImmortal() {
        StateComposite mortalStateComposite = new StateComposite( 1000);
        assertFalse("Mortal StateComposite returns wrong isImmortal", mortalStateComposite.isImmortal());

        StateComposite immortalStateComposite = new StateComposite( null);

        assertTrue("Immortal StateComposite returns wrong isImmortal", immortalStateComposite.isImmortal());

        StateComposite ephemeralStateComposite = new StateComposite();

        assertFalse("Ephemeral StateComposite returns wrong isImmortal", ephemeralStateComposite.isImmortal());
    }

    /**
     * Test method for {@link org.dcache.services.info.base.StateComposite#isMortal()}.
     */
    @Test
    public void testIsMortal() {
        StateComposite mortalStateComposite = new StateComposite( 1000);
        assertTrue("Mortal StateComposite returns wrong isMortal", mortalStateComposite.isMortal());

        StateComposite immortalStateComposite = new StateComposite( null);

        assertFalse("Immortal StateComposite returns wrong isMoral", immortalStateComposite.isMortal());

        StateComposite ephemeralStateComposite = new StateComposite();

        assertFalse("Ephemeral StateComposite returns wrong isMortal", ephemeralStateComposite.isMortal());
    }


    /**
     * Test method for {@link org.dcache.services.info.base.StateComposite#buildRemovalTransition(org.dcache.services.info.base.StatePath, org.dcache.services.info.base.StateTransition, boolean)}.
     */
    @Test
    public void testBuildRemovalTransitionNotForcedNoExpiry() {

        StateTransition transition = new StateTransition();

        _rootComposite.buildRemovalTransition(null, transition, false);
        assertNull( transition.getStateChangeSet(null));
        assertNull( transition.getStateChangeSet( BRANCH_EPHEMERAL_PATH));
        assertNull( transition.getStateChangeSet( BRANCH_IMMORTAL_PATH));
        assertNull( transition.getStateChangeSet( BRANCH_MORTAL_PATH));
    }

    /**
     * Test method for {@link org.dcache.services.info.base.StateComposite#buildRemovalTransition(org.dcache.services.info.base.StatePath, org.dcache.services.info.base.StateTransition, boolean)}.
     * @throws MetricStatePathException
     */
    @Test
    public void testBuildRemovalTransitionNotForcedWithMetricInRoot() throws MetricStatePathException {

        String expMetricName = "expiredMetric";

        // Add an already-expired metric
        addMetric( new StatePath( expMetricName), new StringStateValue( "dummy metric value", 0));

        StateTransition transition = new StateTransition();

        _rootComposite.buildRemovalTransition(null, transition, false);
        assertNotNull( transition.getStateChangeSet(null));
        assertNull( transition.getStateChangeSet( BRANCH_EPHEMERAL_PATH));
        assertNull( transition.getStateChangeSet( BRANCH_IMMORTAL_PATH));
        assertNull( transition.getStateChangeSet( BRANCH_MORTAL_PATH));

        StateChangeSet scs = transition.getStateChangeSet(null);

        assertNotNull( "root StateChangeSet is null", scs);
        assertScsCount( "root", scs, 0, 1, 1, 0);

        Set<String> removedChildren = scs.getRemovedChildren();

        assertTrue( "missing expired metric", removedChildren.contains( expMetricName));
        assertFalse( "ephemeral branch present", removedChildren.contains( BRANCH_EPHEMERAL_NAME));
        assertFalse( "immortal branch present", removedChildren.contains( BRANCH_IMMORTAL_NAME));
        assertFalse( "mortal branch present", removedChildren.contains( BRANCH_MORTAL_NAME));
    }


    /**
     * Test method for {@link org.dcache.services.info.base.StateComposite#buildRemovalTransition(org.dcache.services.info.base.StatePath, org.dcache.services.info.base.StateTransition, boolean)}.
     * @throws MetricStatePathException
     */
    @Test
    public void testBuildRemovalTransitionNotForcedWithMetricInBranch() throws MetricStatePathException {

        String expMetricName = "expiredMetric";

        // Add an already-expired metric
        StateTransition t = new StateTransition();
        _rootComposite.buildTransition( null, BRANCH_MORTAL_PATH.newChild(expMetricName), new StringStateValue( "dummy metric value", 0), t);
        _rootComposite.applyTransition(null, t);

        StateTransition transition = new StateTransition();

        _rootComposite.buildRemovalTransition(null, transition, false);
        assertNotNull( transition.getStateChangeSet(null));
        assertNotNull( transition.getStateChangeSet( BRANCH_MORTAL_PATH));
        assertNull( transition.getStateChangeSet( BRANCH_EPHEMERAL_PATH));
        assertNull( transition.getStateChangeSet( BRANCH_IMMORTAL_PATH));

        StateChangeSet rootScs = transition.getStateChangeSet(null);
        assertNotNull( "root branch StateChangeSet is null", rootScs);
        assertScsCount( "root branch", rootScs, 0, 1, 0, 0);

        StateChangeSet mortalScs = transition.getStateChangeSet( BRANCH_MORTAL_PATH);
        assertNotNull( "mortal branch StateChangeSet is null", mortalScs);
        assertScsCount( "mortal branch", mortalScs, 0, 1, 1, 0);

        Set<String> removedChildren = mortalScs.getRemovedChildren();

        assertTrue( "missing expired metric", removedChildren.contains( expMetricName));
    }


    /**
     * Test method for {@link org.dcache.services.info.base.StateComposite#buildRemovalTransition(org.dcache.services.info.base.StatePath, org.dcache.services.info.base.StateTransition, boolean)}.
     * @throws MetricStatePathException
     */
    @Test
    public void testBuildRemovalTransitionNoExpMortalsIsForced() throws MetricStatePathException {

        String metricName = "aStringMetric";

        // Add a yet-to-expire mortal metric to root composite.
        addMetric( new StatePath( metricName), new StringStateValue( "dummy metric value", 10));

        StateTransition transition = new StateTransition();

        // Build a forced removal change-set
        _rootComposite.buildRemovalTransition( null, transition, true);

        assertNull( transition.getStateChangeSet( BRANCH_EPHEMERAL_PATH));
        assertNull( transition.getStateChangeSet( BRANCH_IMMORTAL_PATH));
        assertNull( transition.getStateChangeSet( BRANCH_MORTAL_PATH));

        StateChangeSet scs = transition.getStateChangeSet(null);
        assertNotNull( scs);

        assertScsCount( "root", scs, 0, 4, 4, 0);

        assertTrue( "missing ephemeral branch from itr", scs.getItrChildren().contains( BRANCH_EPHEMERAL_NAME));
        assertTrue( "missing immortal branch from itr", scs.getItrChildren().contains( BRANCH_IMMORTAL_NAME));
        assertTrue( "missing mortal branch from itr", scs.getItrChildren().contains( BRANCH_MORTAL_NAME));
        assertTrue( "missing metric from itr", scs.getItrChildren().contains( metricName));

        assertTrue( "missing ephemeral branch from remove", scs.getRemovedChildren().contains( BRANCH_EPHEMERAL_NAME));
        assertTrue( "missing immortal branch from remove", scs.getRemovedChildren().contains( BRANCH_IMMORTAL_NAME));
        assertTrue( "missing mortal branch from remove", scs.getRemovedChildren().contains( BRANCH_MORTAL_NAME));
        assertTrue( "missing metric from remove", scs.getRemovedChildren().contains( metricName));
    }

    /**
     * Test method for {@link org.dcache.services.info.base.StateComposite#equals(java.lang.Object)}.
     */
    @Test
    public void testEqualsObject() {
        assertTrue( "_rootComposite not equal to itself", _rootComposite.equals( _rootComposite));
        assertTrue( "_mortalComposite not equal to itself", _mortalComposite.equals( _mortalComposite));
        assertTrue( "_immortalComposite not equal to itself", _immortalComposite.equals( _immortalComposite));
        assertTrue( "_ephemeralComposite not equal to itself", _ephemeralComposite.equals( _ephemeralComposite));
    }

    @Test
    public void testRootNullNotEqual() {
        assertFalse( "_rootComposite equal to null", _rootComposite.equals( null));
    }

    @Test
    public void testRootEphemeralNotEqual() {
        assertFalse( "_rootComposite equal to _ephemeralComposite", _rootComposite.equals( _ephemeralComposite));
    }

    @Test
    public void testRootMortalNotEqual() {
        assertFalse( "_rootComposite equal to _mortalComposite", _rootComposite.equals( _mortalComposite));
    }

    @Test
    public void testRootImmortalNotEqual() {
        // Different since _rootComposite has children whilst _immortalComposite doesn't.
        assertFalse( "_rootComposite equal to _immortalComposite", _rootComposite.equals( _immortalComposite));
    }

    @Test
    public void testImmortalRootNotEqual() {
        assertFalse( "_immortalComposite equal to _rootComposite", _immortalComposite.equals( _rootComposite));
    }

    @Test
    public void testEphemeralMortalEqual() {
        assertEquals( "_ephemeralComposite equal to _mortalComposite", _ephemeralComposite, _mortalComposite);
    }

    @Test
    public void testEphemeralImmortalEqual() {
        assertEquals( "_ephemeralComposite equal to _immortalComposite", _ephemeralComposite, _immortalComposite);
    }

    @Test
    public void testEphemeralRootNotEqual() {
        assertFalse( "_ephemeralComposite equal to _rootComposite", _ephemeralComposite.equals( _rootComposite));
    }

    @Test
    public void testMortalRootNotEqual() {
        assertFalse( "_mortalComposite equal to _rootComposite", _mortalComposite.equals( _rootComposite));
    }

    @Test
    public void testMortalImmortalEqual() {
        assertEquals( "_mortalComposite equal to _immortalComposite", _mortalComposite, _immortalComposite);
    }

    @Test
    public void testMortalEphemeralEqual() {
        assertEquals( "_mortalComposite equal to _ephemeralComposite", _mortalComposite, _ephemeralComposite);
    }

    @Test
    public void testImmortalMortalEqual() {
        assertEquals( "_immortalComposite equal to _mortalComposite", _immortalComposite, _mortalComposite);
    }

    @Test
    public void testImmortalEphemeralEqual() {
        assertEquals( "_immortalComposite equal to _ephemeralComposite", _immortalComposite, _ephemeralComposite);
    }

    @Test
    public void testNewMortalComposite() throws InterruptedException, MetricStatePathException {
        /**
         * We sleep to ensure that when we create myMortalComposite it will have
         * a different lifetime end.
         */
        Thread.sleep(1);

        StateComposite myMortalComposite = new StateComposite( LIFETIME_MORTAL_COMPOSITE);

        assertEquals( "Mortal equal to _immortalComposite", myMortalComposite, _immortalComposite);
        assertEquals( "_immortalComposite equal to Mortal", _immortalComposite, myMortalComposite);

        assertEquals( "Mortal equal to _ephemeralComposite", myMortalComposite, _ephemeralComposite);
        assertEquals( "_ephemeralComposite equal to Mortal", _ephemeralComposite, myMortalComposite);

        assertEquals( "Mortal Composite equal to Mortal with different expiry date", _mortalComposite, myMortalComposite);
        assertEquals( "Mortal Composite equal to Mortal with different expiry date", myMortalComposite, _mortalComposite);

        // Quickly add a child metric to myMortalComposite.
        StateTransition t = new StateTransition();
        myMortalComposite.buildTransition( null, new StatePath( "test-child"), new StringStateValue("foo", 10), t);
        myMortalComposite.applyTransition( null, t);

        assertFalse( "Empty mortal equal to one with a child", myMortalComposite.equals( _mortalComposite));
    }

    @Test
    public void testNewImmortalComposite() throws MetricStatePathException {

        StateComposite myImmortalComposite = new StateComposite( null);

        assertEquals( "Immortal Composite not equal to Immortal", _immortalComposite, myImmortalComposite);
        assertEquals( "Immortal Composite not equal to Immortal", myImmortalComposite, _immortalComposite);

        assertEquals( "_ephemeralComposite not equal to Immortal", _ephemeralComposite, myImmortalComposite);
        assertEquals( "Immortal not equal to _ephemeralComposite", myImmortalComposite, _ephemeralComposite);

        assertEquals( "_mortalComposite not equal to Immortal", _mortalComposite,  myImmortalComposite);
        assertEquals( "Immortal not equal to _mortalComposite", myImmortalComposite, _mortalComposite);

        addMetricToComposite( myImmortalComposite, "test-child", new StringStateValue("foo", 10));

        assertFalse( "Empty immortal equal to one with a child", myImmortalComposite.equals( _immortalComposite));
    }

    @Test
    public void testNewEphemeralComposite() throws MetricStatePathException {

        /**
         * Create new object, check the equals() still works as expected.
         */
        StateComposite myEphemeralComposite = new StateComposite();

        assertEquals( "Ephemeral Composite not equal to Ephemeral", _ephemeralComposite, myEphemeralComposite);
        assertEquals( "Ephemeral Composite not equal to Ephemeral", myEphemeralComposite, _ephemeralComposite);

        assertEquals( "_immortalComposite equal to Ephemeral", _immortalComposite, myEphemeralComposite);
        assertEquals( "Ephemeral equal to _rootComposite", myEphemeralComposite, _immortalComposite);

        assertEquals( "_mortalComposite equal to Ephemeral", _mortalComposite, myEphemeralComposite);
        assertEquals( "Ephemeral equal to _mortalComposite", myEphemeralComposite, _mortalComposite);

        // Add a child and check that they are no longer equal.
        addMetricToComposite( myEphemeralComposite, "test-child", new StringStateValue("foo", 10));

        assertFalse( "Empty ephemeral equal to one with a child", myEphemeralComposite.equals( _ephemeralComposite));
    }

    @Test
    public void testHashCodeSame() {
        assertEquals( "_rootComposite hashCode different", _rootComposite.hashCode(), _rootComposite.hashCode());
        assertEquals( "_mortalComposite hashCode different", _mortalComposite.hashCode(), _mortalComposite.hashCode());
        assertEquals( "_immortalComposite hashCode different", _immortalComposite.hashCode(), _immortalComposite.hashCode());
        assertEquals( "_ephemeralComposite hashCode different", _ephemeralComposite.hashCode(), _ephemeralComposite.hashCode());
    }

    @Test
    public void testHashCodeNotSameRootMortal() {
        assertNotSame( "_rootComposite hashCode same as _mortal hashCode", _rootComposite.hashCode(), _mortalComposite.hashCode());
    }

    @Test
    public void testHashCodeNotSameRootEphemeral() {
        assertNotSame( "_rootComposite hashCode same as _mortal hashCode", _rootComposite.hashCode(), _ephemeralComposite.hashCode());
    }

    @Test
    public void testHashCodeNotSameRootImmortal() {
        assertNotSame( "_rootComposite hashCode same as _mortal hashCode", _rootComposite.hashCode(), _immortalComposite.hashCode());
    }

    @Test
    public void testImmortalChildMakingMortalCompositeImmortal() throws MetricStatePathException {
        // Add a yet-to-expire metric to root composite.
        addMetric( BRANCH_MORTAL_PATH.newChild( "immortal metric"), new StringStateValue( "dummy metric value", true));
        assertIsImmortal( "mortal composite not promoted to immortality", _mortalComposite);
    }

    @Test
    public void testImmortalChildMakingEphemeralCompositeImmortal() throws MetricStatePathException {
        addMetric( BRANCH_EPHEMERAL_PATH.newChild( "immortal metric"), new StringStateValue( "dummy metric value", true));
        assertIsImmortal( "ephemeral composite not promoted to immortality", _ephemeralComposite);
    }

    /**
     * Check Purge
     */


    @Test
    public void testPurgeEverythingScs() {
        StateTransition transition = new StateTransition();

        // Remove everything.
        _rootComposite.buildPurgeTransition( transition, null, null);

        StateChangeSet rootScs = transition.getStateChangeSet( null);
        assertNotNull( "Check that root Scs exists", rootScs);

        assertScsCount( "root Scs", rootScs, 0, 3, 3, 0);

        assertTrue( "Ephemeral branch is to be removed", rootScs.childIsRemoved( BRANCH_EPHEMERAL_NAME));
        assertTrue( "Mortal branch is to be removed", rootScs.childIsRemoved( BRANCH_MORTAL_NAME));
        assertTrue( "Immortal branch is to be removed", rootScs.childIsRemoved( BRANCH_IMMORTAL_NAME));

        StateChangeSet mortalBranchScs = transition.getStateChangeSet( BRANCH_MORTAL_PATH);
        assertNull( "Check that mortal Scs exists", mortalBranchScs);

        StateChangeSet immortalBranchScs = transition.getStateChangeSet( BRANCH_IMMORTAL_PATH);
        assertNull( "Check that immortal Scs exists", immortalBranchScs);

        StateChangeSet ephemeralBranchScs = transition.getStateChangeSet( BRANCH_EPHEMERAL_PATH);
        assertNull( "Check that ephemeral Scs exists", ephemeralBranchScs);
    }

    @Test
    public void testPurgeEverythingThenApply() {
        StateTransition transition = new StateTransition();
        _rootComposite.buildPurgeTransition( transition, null, null);

        _rootComposite.applyTransition( null, transition);

        VerifyingVisitor emptyVisitor = new VerifyingVisitor();
        emptyVisitor.assertSatisfied( "checking state is complete empty", _rootComposite);
    }

    @Test
    public void testPurgeEverythingVisit() {
        StateTransition transition = new StateTransition();
        _rootComposite.buildPurgeTransition( transition, null, null);

        VerifyingVisitor emptyVisitor = new VerifyingVisitor();
        emptyVisitor.assertSatisfiedTransition( "checking state is complete empty", _rootComposite, transition);
    }


    @Test
    public void testPurgeMortalScs() {
        StateTransition transition = new StateTransition();

        // Remove everything under the mortal branch.
        _rootComposite.buildPurgeTransition( transition, null, BRANCH_MORTAL_PATH);

        StateChangeSet rootScs = transition.getStateChangeSet( null);
        assertNotNull( "Check that root Scs exists", rootScs);

        assertScsCount( "root Scs", rootScs, 0, 1, 1, 0);

        assertFalse( "Ephemeral branch is to be removed", rootScs.childIsRemoved( BRANCH_EPHEMERAL_NAME));
        assertTrue( "Mortal branch is to be removed", rootScs.childIsRemoved( BRANCH_MORTAL_NAME));
        assertFalse( "Immortal branch is to be removed", rootScs.childIsRemoved( BRANCH_IMMORTAL_NAME));

        StateChangeSet mortalBranchScs = transition.getStateChangeSet( BRANCH_MORTAL_PATH);
        assertNull( "Check that mortal Scs exists", mortalBranchScs);

        StateChangeSet immortalBranchScs = transition.getStateChangeSet( BRANCH_IMMORTAL_PATH);
        assertNull( "Check that immortal Scs exists", immortalBranchScs);

        StateChangeSet ephemeralBranchScs = transition.getStateChangeSet( BRANCH_EPHEMERAL_PATH);
        assertNull( "Check that ephemeral Scs exists", ephemeralBranchScs);
    }

    @Test
    public void testPurgeMortalVisit() {
        // Remove everything under the mortal branch
        StateTransition transition = new StateTransition();
        _rootComposite.buildPurgeTransition( transition, null, BRANCH_MORTAL_PATH);

        VerifyingVisitor visitor = new VerifyingVisitor();
        visitor.addExpectedBranch( BRANCH_EPHEMERAL_PATH);
        visitor.addExpectedBranch( BRANCH_IMMORTAL_PATH);
        visitor.assertSatisfiedTransition( "checking state contains two branches", _rootComposite, transition);
    }

    @Test
    public void testPurgeMortalThenApply() {
        // Remove everything under the mortal branch
        StateTransition transition = new StateTransition();
        _rootComposite.buildPurgeTransition( transition, null, BRANCH_MORTAL_PATH);
        _rootComposite.applyTransition( null, transition);

        VerifyingVisitor visitor = new VerifyingVisitor();
        visitor.addExpectedBranch( BRANCH_EPHEMERAL_PATH);
        visitor.addExpectedBranch( BRANCH_IMMORTAL_PATH);
        visitor.assertSatisfied( "checking state contains two branches", _rootComposite);
    }


    @Test
    public void testPurgeMortalBranchWithMetricScs() throws MetricStatePathException {

        String metricName = "a metric";

        // Add our metric to the mortal branch
        addMetric( BRANCH_MORTAL_PATH.newChild( metricName), new StringStateValue( "String value", 100));

        // Try to remove the mortal branch and everything underneath it.
        StateTransition transition = new StateTransition();
        _rootComposite.buildPurgeTransition( transition, null, BRANCH_MORTAL_PATH);

        StateChangeSet rootScs = transition.getStateChangeSet( null);
        assertNotNull( "Check that root Scs exists", rootScs);

        assertScsCount( "root Scs", rootScs, 0, 1, 1, 0);

        assertFalse( "Ephemeral branch is to be removed", rootScs.childIsRemoved( BRANCH_EPHEMERAL_NAME));
        assertTrue( "Mortal branch is to be removed", rootScs.childIsRemoved( BRANCH_MORTAL_NAME));
        assertFalse( "Immortal branch is to be removed", rootScs.childIsRemoved( BRANCH_IMMORTAL_NAME));

        StateChangeSet immortalBranchScs = transition.getStateChangeSet( BRANCH_IMMORTAL_PATH);
        assertNull( "Check that immortal Scs exists", immortalBranchScs);

        StateChangeSet ephemeralBranchScs = transition.getStateChangeSet( BRANCH_EPHEMERAL_PATH);
        assertNull( "Check that ephemeral Scs exists", ephemeralBranchScs);

        StateChangeSet mortalBranchScs = transition.getStateChangeSet( BRANCH_MORTAL_PATH);
        assertNotNull( "Check that mortal Scs exists", mortalBranchScs);

        assertScsCount( "mortal branch Scs", mortalBranchScs, 0, 1, 1, 0);

        assertTrue( "check metric is to be removed", mortalBranchScs.childIsRemoved( metricName));
    }

    @Test
    public void testPurgeMortalBranchWithMetricThenApply() throws MetricStatePathException {

        String metricName = "a metric";

        // Add our metric to the mortal branch
        addMetric( BRANCH_MORTAL_PATH.newChild( metricName), new StringStateValue( "String value", 100));

        // Try to remove the mortal branch and everything underneath it.
        StateTransition transition = new StateTransition();
        _rootComposite.buildPurgeTransition( transition, null, BRANCH_MORTAL_PATH);

        _rootComposite.applyTransition( null, transition);

        VerifyingVisitor visitor = new VerifyingVisitor();
        visitor.addExpectedBranch( BRANCH_EPHEMERAL_PATH);
        visitor.addExpectedBranch( BRANCH_IMMORTAL_PATH);
        visitor.assertSatisfied( "checking state contains two branches", _rootComposite);
    }

    @Test
    public void testPurgeMortalBranchWithMetricVisit() throws MetricStatePathException {

        String metricName = "a metric";

        // Add our metric to the mortal branch
        addMetric( BRANCH_MORTAL_PATH.newChild( metricName), new StringStateValue( "String value", 100));

        // Try to remove the mortal branch and everything underneath it.
        StateTransition transition = new StateTransition();
        _rootComposite.buildPurgeTransition( transition, null, BRANCH_MORTAL_PATH);

        VerifyingVisitor visitor = new VerifyingVisitor();
        visitor.addExpectedBranch( BRANCH_EPHEMERAL_PATH);
        visitor.addExpectedBranch( BRANCH_IMMORTAL_PATH);
        visitor.assertSatisfiedTransition( "checking state contains two branches", _rootComposite, transition);
    }


    @Test
    public void testPurgeMortalMetricScs() throws MetricStatePathException {

        String metricName = "a metric";
        StatePath metricPath = BRANCH_MORTAL_PATH.newChild( metricName);

        // Add our metric to the mortal branch
        addMetric( metricPath, new StringStateValue( "String value", 100));

        // Build a transition to remove just the metric
        StateTransition transition = new StateTransition();
        _rootComposite.buildPurgeTransition( transition, null, metricPath);

        // Verify that the transition is as expected
        StateChangeSet rootScs = transition.getStateChangeSet( null);
        assertNotNull( "Check that root Scs exists", rootScs);

        assertScsCount( "root Scs", rootScs, 0, 1, 0, 0);

        assertFalse( "Ephemeral branch is to be removed", rootScs.childIsRemoved( BRANCH_EPHEMERAL_NAME));
        assertFalse( "Mortal branch is to be removed", rootScs.childIsRemoved( BRANCH_MORTAL_NAME));
        assertFalse( "Immortal branch is to be removed", rootScs.childIsRemoved( BRANCH_IMMORTAL_NAME));

        StateChangeSet immortalBranchScs = transition.getStateChangeSet( BRANCH_IMMORTAL_PATH);
        assertNull( "Check that immortal Scs exists", immortalBranchScs);

        StateChangeSet ephemeralBranchScs = transition.getStateChangeSet( BRANCH_EPHEMERAL_PATH);
        assertNull( "Check that ephemeral Scs exists", ephemeralBranchScs);

        StateChangeSet mortalBranchScs = transition.getStateChangeSet( BRANCH_MORTAL_PATH);
        assertNotNull( "Check that mortal Scs exists", mortalBranchScs);

        assertScsCount( "mortal branch Scs", mortalBranchScs, 0, 0, 1, 0);

        assertTrue( "check metric is to be removed", mortalBranchScs.childIsRemoved( metricName));
    }

    @Test
    public void testPurgeMortalMetricThenApply() throws MetricStatePathException {

        String metricName = "a metric";
        StatePath metricPath = BRANCH_MORTAL_PATH.newChild( metricName);

        // Add our metric to the mortal branch
        addMetric( metricPath, new StringStateValue( "String value", 100));

        // Build a transition to remove just the metric
        StateTransition transition = new StateTransition();
        _rootComposite.buildPurgeTransition( transition, null, metricPath);

        _rootComposite.applyTransition( null, transition);

        VerifyingVisitor visitor = newDefaultVisitor();
        visitor.assertSatisfied( "checking state contains two branches", _rootComposite);
    }

    @Test
    public void testPurgeMortalMetricVisit() throws MetricStatePathException {

        String metricName = "a metric";
        StatePath metricPath = BRANCH_MORTAL_PATH.newChild( metricName);

        // Add our metric to the mortal branch
        addMetric( metricPath, new StringStateValue( "String value", 100));

        // Build a transition to remove just the metric
        StateTransition transition = new StateTransition();
        _rootComposite.buildPurgeTransition( transition, null, metricPath);

        VerifyingVisitor visitor = newDefaultVisitor();
        visitor.assertSatisfiedTransition( "checking state contains two branches", _rootComposite, transition);
    }

    @Test
    public void testPurgeMortalMetricThenAddMetricScs() throws MetricStatePathException {

        String metricName = "myStringMetric";
        StatePath metricPath = BRANCH_MORTAL_PATH.newChild( metricName);

        // Add our metric to the mortal branch
        addMetric( metricPath, new StringStateValue( "The old value", 100));

        // Build transition to remove just the metric
        StateTransition transition = new StateTransition();
        _rootComposite.buildPurgeTransition( transition, null, metricPath);

        // Now we add a new metric with the same path
        StateValue newMetric = new StringStateValue("The new value", 100);
        _rootComposite.buildTransition( null, metricPath, newMetric, transition);

        // And verify that the transition is as expected.
        StateChangeSet rootScs = transition.getStateChangeSet( null);
        assertScsCount( "root Scs", rootScs, 0, 1, 0, 0);

        assertFalse( "Ephemeral branch is to be removed", rootScs.childIsRemoved( BRANCH_EPHEMERAL_NAME));
        assertFalse( "Mortal branch is to be removed", rootScs.childIsRemoved( BRANCH_MORTAL_NAME));
        assertFalse( "Immortal branch is to be removed", rootScs.childIsRemoved( BRANCH_IMMORTAL_NAME));

        assertNull( "Check that immortal Scs doesn't exists", transition.getStateChangeSet( BRANCH_IMMORTAL_PATH));

        assertNull( "Check that ephemeral Scs doesn't exists", transition.getStateChangeSet( BRANCH_EPHEMERAL_PATH));

        StateChangeSet mortalBranchScs = transition.getStateChangeSet( BRANCH_MORTAL_PATH);
        mortalBranchScs = transition.getStateChangeSet( BRANCH_MORTAL_PATH);
        assertNotNull( "Check that mortal Scs exists", mortalBranchScs);

        assertScsCount( "mortal branch Scs", mortalBranchScs, 0, 0, 0, 1);

        assertTrue( "check metric is to be updated", mortalBranchScs.childIsUpdated( metricName));
        assertFalse( "check metric is not to be removed", mortalBranchScs.childIsRemoved( metricName));
    }

    @Test
    public void testPurgeMortalMetricThenAddMetricThenApply() throws MetricStatePathException {

        String metricName = "myStringMetric";
        StatePath metricPath = BRANCH_MORTAL_PATH.newChild( metricName);

        // Add our metric to the mortal branch
        addMetric( metricPath, new StringStateValue( "The old value", 100));

        // Build transition to remove just the metric
        StateTransition transition = new StateTransition();
        _rootComposite.buildPurgeTransition( transition, null, metricPath);

        // Now we add a new metric with the same path
        StateValue newMetric = new StringStateValue("The new value", 100);
        _rootComposite.buildTransition( null, metricPath, newMetric, transition);

        _rootComposite.applyTransition( null, transition);

        VerifyingVisitor visitor = newDefaultVisitor();
        visitor.addExpectedMetric( metricPath, newMetric);
        visitor.assertSatisfied( "checking state contains two branches", _rootComposite);
    }

    @Test
    public void testPurgeMortalMetricThenAddMetricVisit() throws MetricStatePathException {

        String metricName = "myStringMetric";
        StatePath metricPath = BRANCH_MORTAL_PATH.newChild( metricName);

        // Add our metric to the mortal branch
        addMetric( metricPath, new StringStateValue( "The old value", 100));

        // Build transition to remove just the metric
        StateTransition transition = new StateTransition();
        _rootComposite.buildPurgeTransition( transition, null, metricPath);

        // Now we add a new metric with the same path
        StateValue newMetric = new StringStateValue("The new value", 100);
        _rootComposite.buildTransition( null, metricPath, newMetric, transition);

        VerifyingVisitor visitor = newDefaultVisitor();
        visitor.addExpectedMetric( metricPath, newMetric);
        visitor.assertSatisfiedTransition( "checking state contains two branches", _rootComposite, transition);
    }

    @Test
    public void testPurgeMortalBranchThenAddMetricScs() throws MetricStatePathException {

        String metricName = "a metric";
        StatePath metricPath = BRANCH_MORTAL_PATH.newChild( metricName);

        // Add our metric to the mortal branch
        addMetric( metricPath, new StringStateValue( "String value", 100));

        // Build transition to purge branch (so also removing the metric)
        StateTransition transition = new StateTransition();
        _rootComposite.buildPurgeTransition( transition, null, BRANCH_MORTAL_PATH);

        // Now we add a new metric with the same path
        StateValue newMetric = new StringStateValue( "The new value", 100);
        _rootComposite.buildTransition( null, metricPath, newMetric, transition);

        // And verify that the transition is as expected.
        StateChangeSet rootScs = transition.getStateChangeSet( null);
        assertScsCount( "root Scs", rootScs, 0, 1, 0, 0);

        assertFalse( "Ephemeral branch is to be removed", rootScs.childIsRemoved( BRANCH_EPHEMERAL_NAME));
        assertFalse( "Mortal branch is to be removed", rootScs.childIsRemoved( BRANCH_MORTAL_NAME));
        assertFalse( "Immortal branch is to be removed", rootScs.childIsRemoved( BRANCH_IMMORTAL_NAME));

        assertNull( "Check that immortal Scs doesn't exists", transition.getStateChangeSet( BRANCH_IMMORTAL_PATH));

        assertNull( "Check that ephemeral Scs doesn't exists", transition.getStateChangeSet( BRANCH_EPHEMERAL_PATH));

        StateChangeSet mortalBranchScs = transition.getStateChangeSet( BRANCH_MORTAL_PATH);
        mortalBranchScs = transition.getStateChangeSet( BRANCH_MORTAL_PATH);
        assertNotNull( "Check that mortal Scs exists", mortalBranchScs);

        assertScsCount( "mortal branch Scs", mortalBranchScs, 0, 1, 0, 1);

        assertTrue( "check metric is to be updated", mortalBranchScs.childIsUpdated( metricName));
        assertFalse( "check metric is not to be removed", mortalBranchScs.childIsRemoved( metricName));
    }

    @Test
    public void testPurgeMortalBranchThenAddMetricThenApply() throws MetricStatePathException {

        String metricName = "a metric";
        StatePath metricPath = BRANCH_MORTAL_PATH.newChild( metricName);

        // Add our metric to the mortal branch
        addMetric( metricPath, new StringStateValue( "String value", 100));

        // Build transition to purge branch (so also removing the metric)
        StateTransition transition = new StateTransition();
        _rootComposite.buildPurgeTransition( transition, null, BRANCH_MORTAL_PATH);

        // Now we add a new metric with the same path
        StateValue newMetric = new StringStateValue( "The new value", 100);
        _rootComposite.buildTransition( null, metricPath, newMetric, transition);

        _rootComposite.applyTransition( null, transition);

        VerifyingVisitor visitor = newDefaultVisitor();
        visitor.addExpectedMetric( metricPath, newMetric);
        visitor.assertSatisfied( "checking state contains two branches", _rootComposite);
    }

    @Test
    public void testPurgeMortalBranchThenAddMetricVisit() throws MetricStatePathException {

        String metricName = "a metric";
        StatePath metricPath = BRANCH_MORTAL_PATH.newChild( metricName);

        // Add our metric to the mortal branch
        addMetric( metricPath, new StringStateValue( "String value", 100));

        // Build transition to purge branch (so also removing the metric)
        StateTransition transition = new StateTransition();
        _rootComposite.buildPurgeTransition( transition, null, BRANCH_MORTAL_PATH);

        // Now we add a new metric with the same path
        StateValue newMetric = new StringStateValue( "The new value", 100);
        _rootComposite.buildTransition( null, metricPath, newMetric, transition);

        VerifyingVisitor visitor = newDefaultVisitor();
        visitor.addExpectedMetric( metricPath, newMetric);
        visitor.assertSatisfiedTransition( "checking state contains two branches", _rootComposite, transition);
    }

    @Test
    public void testPurgeMortalBranchWithTwoMetricsThenAddMetricScs() throws MetricStatePathException {

        String metricName = "metric-we-recreate";
        StatePath metricPath = BRANCH_MORTAL_PATH.newChild( metricName);
        String doomedMetricName = "metric-we-allow-to-be-purged";
        StatePath doomedMetricPath = BRANCH_MORTAL_PATH.newChild( doomedMetricName);

        // Add our metric to the mortal branch
        addMetric( metricPath, new StringStateValue( "String value", 100));

        // Add a doomed metric to the mortal branch
        addMetric( doomedMetricPath, new StringStateValue( "String value", 100));


        // Build transition to purge branch (so removing the metric)
        StateTransition transition = new StateTransition();
        _rootComposite.buildPurgeTransition( transition, null, BRANCH_MORTAL_PATH);

        // Now we add a new metric with the same path
        StateValue newMetric = new StringStateValue( "The new value");
        _rootComposite.buildTransition( null, metricPath, newMetric, transition);

        // And verify that the transition is as expected.
        StateChangeSet rootScs = transition.getStateChangeSet( null);
        assertScsCount( "root Scs", rootScs, 0, 1, 0, 0);

        assertFalse( "Ephemeral branch is to be removed", rootScs.childIsRemoved( BRANCH_EPHEMERAL_NAME));
        assertFalse( "Mortal branch is to be removed", rootScs.childIsRemoved( BRANCH_MORTAL_NAME));
        assertFalse( "Immortal branch is to be removed", rootScs.childIsRemoved( BRANCH_IMMORTAL_NAME));

        assertNull( "Check that immortal Scs doesn't exists", transition.getStateChangeSet( BRANCH_IMMORTAL_PATH));

        assertNull( "Check that ephemeral Scs doesn't exists", transition.getStateChangeSet( BRANCH_EPHEMERAL_PATH));

        StateChangeSet mortalBranchScs = transition.getStateChangeSet( BRANCH_MORTAL_PATH);
        mortalBranchScs = transition.getStateChangeSet( BRANCH_MORTAL_PATH);
        assertNotNull( "Check that mortal Scs exists", mortalBranchScs);

        assertScsCount( "mortal branch Scs", mortalBranchScs, 0, 2, 1, 1);

        assertTrue( "check metric is to be updated", mortalBranchScs.childIsUpdated( metricName));
        assertFalse( "check metric is not to be removed", mortalBranchScs.childIsRemoved( metricName));
        assertTrue( "check doomedMetric is still to be removed", mortalBranchScs.childIsRemoved( doomedMetricName));
    }


    @Test
    public void testPurgeMortalBranchWithTwoMetricsThenAddMetricThenApply() throws MetricStatePathException {

        String metricName = "metric-we-recreate";
        StatePath metricPath = BRANCH_MORTAL_PATH.newChild( metricName);
        String doomedMetricName = "metric-we-allow-to-be-purged";
        StatePath doomedMetricPath = BRANCH_MORTAL_PATH.newChild( doomedMetricName);

        // Add our metric to the mortal branch
        addMetric( metricPath, new StringStateValue( "String value", 100));

        // Add a doomed metric to the mortal branch
        addMetric( doomedMetricPath, new StringStateValue( "String value", 100));

        // Build transition to purge branch (so removing the metric)
        StateTransition transition = new StateTransition();
        _rootComposite.buildPurgeTransition( transition, null, BRANCH_MORTAL_PATH);

        // Now we add a new metric with the same path
        StateValue newMetric = new StringStateValue( "The new value");
        _rootComposite.buildTransition( null, metricPath, newMetric, transition);

        _rootComposite.applyTransition( null, transition);

        VerifyingVisitor visitor = newDefaultVisitor();
        visitor.addExpectedMetric( metricPath, newMetric);
        visitor.assertSatisfied( "checking state contains two branches", _rootComposite);
    }


    @Test
    public void testPurgeMortalBranchWithTwoMetricsThenAddMetricVisit() throws MetricStatePathException {

        String metricName = "metric-we-recreate";
        StatePath metricPath = BRANCH_MORTAL_PATH.newChild( metricName);
        String doomedMetricName = "metric-we-allow-to-be-purged";
        StatePath doomedMetricPath = BRANCH_MORTAL_PATH.newChild( doomedMetricName);

        // Add our metric to the mortal branch
        addMetric( metricPath, new StringStateValue( "String value", 100));

        // Add a doomed metric to the mortal branch
        addMetric( doomedMetricPath, new StringStateValue( "String value", 100));

        // Build transition to purge branch (so removing the metric)
        StateTransition transition = new StateTransition();
        _rootComposite.buildPurgeTransition( transition, null, BRANCH_MORTAL_PATH);

        // Now we add a new metric with the same path
        StateValue newMetric = new StringStateValue( "The new value");
        _rootComposite.buildTransition( null, metricPath, newMetric, transition);

        VerifyingVisitor visitor = newDefaultVisitor();
        visitor.addExpectedMetric( metricPath, newMetric);
        visitor.assertSatisfiedTransition( "checking state contains two branches", _rootComposite, transition);
    }


    /**
     * Quickly add a metric to a StateComposite by creating a StateTransition and applying it.
     * @param sc the StateComposite to add the metric
     * @param name the name of the metric
     * @param metric the StateValue
     * @throws MetricStatePathException if unable to add the metric at given location
     */
    private void addMetric( StatePath metricPath, StateValue metricValue) throws MetricStatePathException {
        StateTransition transition = new StateTransition();
        _rootComposite.buildTransition( null, metricPath, metricValue, transition);
        _rootComposite.applyTransition( null, transition);
    }


    /**
     * Quickly add a StateValue directly to a StateComposite.  NB this by-passes some of the infrastructure by
     * treating the StateComposite as the root element, if this StateComposite is part of a hierarchy, then
     * this will break some activity.
     * @param c the StateComposite the child is to be added to.
     * @param metricName the name of the metric
     * @param metricValue the value of the metric
     * @throws MetricStatePathException if metric cannot be added at that location.
     */
    private void addMetricToComposite( StateComposite c, String metricName, StateValue metricValue) throws MetricStatePathException {

        StateTransition t = new StateTransition();
        c.buildTransition( null, new StatePath( metricName), metricValue, t);
        c.applyTransition( null, t);
    }


    /**
     * Assert the number of children that are new, itr, removed, updated in
     * the given StateChangeSet.
     * @param msg the message to display if there's a problem.
     * @param scs the StateChangeSet under consideration
     * @param newChildrenCount the expected number of new children
     * @param itrChildrenCount the expected number of children to iterate down into.
     * @param removeChildrenCount the expected number of children to remove
     * @param updateChildrenCount the expected number of children to update
     */
    private void assertScsCount( String msg, StateChangeSet scs,
                                 int newChildrenCount, int itrChildrenCount,
                                 int removeChildrenCount, int updateChildrenCount) {
        int scsNewChildren = 0, scsItrChildren = 0, scsRemovedChildren = 0, scsUpdatedChildren = 0;

        if( scs != null) {
            if( scs.getNewChildren() != null)
                scsNewChildren = scs.getNewChildren().size();

            if( scs.getItrChildren() != null)
                scsItrChildren = scs.getItrChildren().size();

            if( scs.getRemovedChildren() != null)
                scsRemovedChildren = scs.getRemovedChildren().size();

            if( scs.getUpdatedChildren() != null)
                scsUpdatedChildren = scs.getUpdatedChildren().size();
        }

        assertEquals( msg + " new children", newChildrenCount, scsNewChildren);
        assertEquals( msg + " itr children", itrChildrenCount, scsItrChildren);
        assertEquals( msg + " remove children", removeChildrenCount, scsRemovedChildren);
        assertEquals( msg + " updated children", updateChildrenCount, scsUpdatedChildren);
    }

    /**
     * Check that acceptVisitor with a transition will work. The transition
     * is taken from the supplied parameters. The transition isn't applied,
     * so this method may be called multiple times.
     *
     * @param metricPath
     * @param metricValue
     */
    private void assertVisitTransition( StatePath metricPath,
                                        StateValue metricValue) {

        // Build the visitor we expect to pass after a metric has been added
        VerifyingVisitor visitor = newDefaultVisitor();
        visitor.addExpectedMetric( metricPath, metricValue);

        // Build transition for adding a metric
        StateTransition transition = new StateTransition();
        try {
            _rootComposite.buildTransition( null, metricPath, metricValue,
                    transition);
        } catch (MetricStatePathException e) {
            fail( "Failed to add metric at path " + metricPath);
        }

        // Check that visiting with transition works as expected
        visitor.assertSatisfiedTransition(
                "VerifyingVisitor not satisfied after applying transition",
                _rootComposite, transition);
    }


    /**
     * Assert that we can add the given metric to the tree and visiting gives
     * the correct results.
     *
     * @param visitor
     * @param path
     * @param metric
     */
    private void assertVisitorAddMetric( VerifyingVisitor visitor,
                                         StatePath path, StateValue metric) {
        /* Just to double-check: do we get the right result to begin with? */
        visitor.assertSatisfied(
                "VerifyingVisitor not satisfied before changing anything",
                _rootComposite);

        /* Update the visitor so we check for the new metric */
        visitor.addExpectedMetric( path, metric);

        /* Build a transition for adding this metric */
        StateTransition transition = new StateTransition();
        try {
            _rootComposite.buildTransition( null, path, metric, transition);
        } catch (MetricStatePathException e) {
            fail( "Failed to add metric at path " + path);
        }

        /*
         * Verify that the simulated effect of the transition works as
         * expected.
         */
        visitor.assertSatisfiedTransition(
                "VerifyingVisitor not satisfied with simulating transition",
                _rootComposite, transition);

        /* Apply the transition, so updating the tree */
        _rootComposite.applyTransition( null, transition);

        /* Verify the transition now works */
        visitor.assertSatisfied(
                "VerifyingVisitor not satisfied after applying transition",
                _rootComposite);
    }


    /**
     * Return a new VerifyingVisitor that will pass the default set of
     * branches:
     * <ul>
     * <li>BRANCH_EPHEMERAL_PATH
     * <li>BRANCH_MORTAL_PATH
     * <li>BRANCH_IMMORTAL_PATH
     * </ul>
     *
     * @return
     */
    private VerifyingVisitor newDefaultVisitor() {
        VerifyingVisitor visitor = new VerifyingVisitor();

        visitor.addExpectedBranch( BRANCH_EPHEMERAL_PATH);
        visitor.addExpectedBranch( BRANCH_MORTAL_PATH);
        visitor.addExpectedBranch( BRANCH_IMMORTAL_PATH);

        return visitor;
    }
}
