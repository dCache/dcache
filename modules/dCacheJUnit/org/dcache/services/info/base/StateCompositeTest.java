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

import org.junit.Before;
import org.junit.Test;

/**
 * Test the StateComposite class
 *
 * @author Paul Millar <paul.millar@desy.de>
 */
public class StateCompositeTest extends InfoBaseTest {

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
}
