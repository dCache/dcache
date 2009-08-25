package org.dcache.services.info.base;


import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;

public class StateUpdateTests {

	private static final String NEW_STRING_VALUE = "new string";

	private static final StatePath NEW_STRING_PATH = new StatePath( "aStringMetric");
	private static final StatePath NEW_INTEGER_PATH = StatePath.parsePath( "aBranch.anInteger");
	private static final long MORTAL_METRIC_DURATION = 60;

	StateUpdate _update = new StateUpdate();

	@Before
	public void setUp() throws Exception {
		_update.appendUpdate( NEW_STRING_PATH, new StringStateValue( NEW_STRING_VALUE, MORTAL_METRIC_DURATION));
		_update.appendUpdate( NEW_INTEGER_PATH, new IntegerStateValue( 42, MORTAL_METRIC_DURATION));
	}

	@Test
	public void testCount() {
		assertSame( "Wrong number of entries", 2, _update.count());
	}

	@Test
	public void testEmptyCount() {
		assertSame( "Wrong number of entries", 0, new StateUpdate().count());
	}

	@Test
	public void testAppendUpdate() {
		_update.appendUpdate( NEW_INTEGER_PATH.parentPath().newChild("anotherInteger"), new IntegerStateValue( 39, MORTAL_METRIC_DURATION));
		assertSame( "count not updated", 3, _update.count());
	}

	@Test
	public void testUpdateTransition() throws BadStatePathException {
		StateTransition transition = new StateTransition();

		StateComposite _root = new StateComposite();

		_update.updateTransition( _root, transition);

		StateChangeSet rootScs = transition.getStateChangeSet( null);

		assertTrue( "missing root string", rootScs.childIsNew( NEW_STRING_PATH.getLastElement()));

		StateChangeSet newBranchScs = transition.getStateChangeSet( NEW_INTEGER_PATH.parentPath());

		assertTrue( "missing branch metric", newBranchScs.childIsNew( NEW_INTEGER_PATH.getLastElement()));
	}

	@Test(expected=BadStatePathException.class)
	public void testAddBadPath() throws BadStatePathException {
		StateTransition transition = new StateTransition();
		StateComposite _root = new StateComposite();

		// We will attempt to add aStringMetric.anotherMetric; we anticipate this failing
		_update.appendUpdate( NEW_STRING_PATH.newChild("anotherMetric") , new StringStateValue( "foo", MORTAL_METRIC_DURATION));

		_update.updateTransition( _root, transition);
	}
}
