package org.dcache.services.info.base;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

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
	public void testAppendUpdateCollectionImmortal() {
	    StatePath basePath = StatePath.parsePath( "aa.bb");
	    Set<String> items = new HashSet<String>();
	    items.add( "item1");
        items.add( "item2");
        items.add( "item3");
	    _update.appendUpdateCollection( basePath, items, true);

	    for( String item : items)
	        assertTrue( _update.hasUpdate( basePath.newChild( item), new StateComposite()));
	}

    @Test
    public void testAppendUpdateCollectionEphemeral() {
        StatePath basePath = StatePath.parsePath( "aa.bb");
        Set<String> items = new HashSet<String>();
        items.add( "item1");
        items.add( "item2");
        items.add( "item3");
        _update.appendUpdateCollection( basePath, items, true);

        for( String item : items)
            assertTrue( _update.hasUpdate( basePath.newChild( item), new StateComposite()));
    }

    @Test
    public void testAppendUpdateCollectionMortal() {
        StatePath basePath = StatePath.parsePath( "aa.bb");
        List<String> items = new LinkedList<String>();
        items.add( "item1");
        items.add( "item2");
        items.add( "item3");
        _update.appendUpdateCollection( basePath, items, 10);

        for( String item : items)
            assertTrue( _update.hasUpdate( basePath.newChild( item), new StateComposite()));
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

	@Test
	public void testHasUpdateNull() {
	    StateComponent newComponent = new StringStateValue( "a string value");

	    assertFalse( "check hasUpdate with null StatePath", _update.hasUpdate( null, newComponent));
	    assertFalse( "check hasUpdate with null StateValue", _update.hasUpdate( NEW_STRING_PATH, null));
	}

	@Test
	public void testHasUpdateSameItem() {
	    StateComponent newComponent = new StringStateValue( "a string value");

	    _update.appendUpdate( NEW_STRING_PATH, newComponent);

	    assertTrue( "check same component", _update.hasUpdate( NEW_STRING_PATH, newComponent));
	}

	@Test
	public void testHasUpdateEqualItem() {
	    String metricValue = "a string value";
	    StateComponent newComponent1 = new StringStateValue( metricValue);
        StateComponent newComponent2 = new StringStateValue( metricValue);

	    _update.appendUpdate( NEW_STRING_PATH, newComponent1);

	    assertTrue( "check same component", _update.hasUpdate( NEW_STRING_PATH, newComponent2));
	}

	@Test
	public void testHasUpdateNotEqualItem() {
	    StateComponent newComponent1 = new StringStateValue( "a string value");
	    StateComponent newComponent2 = new StringStateValue( "a different string value");

	    _update.appendUpdate( NEW_STRING_PATH, newComponent1);

	    assertFalse( "check different value", _update.hasUpdate( NEW_STRING_PATH, newComponent2));
	}

	@Test
	public void testHasUpdateSubPath() {
        StateComponent newComponent = new StringStateValue( "a string value");

        _update.appendUpdate( NEW_STRING_PATH, newComponent);

        assertFalse( "check component with sub path", _update.hasUpdate( NEW_STRING_PATH.newChild( "sub-element"), newComponent));
	}

    @Test
    public void testHasUpdateSuperPath() {
        StateComponent newComponent = new StringStateValue( "a string value");

        _update.appendUpdate( NEW_STRING_PATH.newChild( "sub-element"), newComponent);

        assertFalse( "check component with super path", _update.hasUpdate( NEW_STRING_PATH, newComponent));
    }

    @Test
    public void testHasUpdateDifferentPath() {
        StateComponent newComponent = new StringStateValue( "a string value");

        _update.appendUpdate( NEW_STRING_PATH, newComponent);

        assertFalse( "check component with different path", _update.hasUpdate( StatePath.parsePath( "element1.element2"), newComponent));
    }

    @Test
    public void testHasUpdateComposite() {
        StateComponent newComponent = new StateComposite();
        StatePath newComponentPath = StatePath.parsePath( "aa.bb");

        _update.appendUpdate( newComponentPath, newComponent);

        assertTrue( "check composite with same path", _update.hasUpdate( newComponentPath, newComponent));
    }

    @Test
    public void testDebugInfo() {
        String info = _update.debugInfo();
        assertFalse( "length zero", info.length() == 0);
    }
}
