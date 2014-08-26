package org.dcache.services.info.base;

import org.junit.Before;
import org.junit.Test;

import java.util.Collection;
import java.util.Date;

import static org.junit.Assert.*;

public class StateChangeSetTests {

    public final static String METRIC_NAME = "child-metric";

    StateChangeSet _scs;

    @Before
    public void setUp()
    {
        _scs = new StateChangeSet();
    }

    @Test
    public void testRecordNewChild() {
        StateValue value = new TestStateValue( false);
        _scs.recordNewChild( METRIC_NAME, value);
        assertSame( "checking we get metric back", value,
                    _scs.getNewChildValue( METRIC_NAME));

        assertTrue( "checking childIsNew ", _scs.childIsNew( METRIC_NAME));

        Collection<String> newChildren = _scs.getNewChildren();
        assertEquals( "checking new-children Set size ", 1, newChildren.size());
        assertTrue( "checking new-children Set contents ",
                    newChildren.contains( METRIC_NAME));
    }

    @Test
    public void testRecordUpdatedChild() {
        StateValue value = new TestStateValue( false);
        _scs.recordUpdatedChild( METRIC_NAME, value);
        assertSame( "checking we get metric back", value,
                    _scs.getUpdatedChildValue( METRIC_NAME));

        assertTrue( "checking childIsUpdated ",
                    _scs.childIsUpdated( METRIC_NAME));

        Collection<String> updatedChildren = _scs.getUpdatedChildren();
        assertEquals( "checking new-children Set size ", 1,
                      updatedChildren.size());
        assertTrue( "checking new-children Set contents ",
                    updatedChildren.contains( METRIC_NAME));
    }

    @Test
    public void testRecordRemovedChild() {

        _scs.recordRemovedChild( METRIC_NAME);

        assertTrue( "checking childIsRemoved ",
                    _scs.childIsRemoved( METRIC_NAME));

        Collection<String> removedChildren = _scs.getRemovedChildren();
        assertEquals( "checking new-children Set size ", 1,
                      removedChildren.size());
        assertTrue( "checking new-children Set contents ",
                    removedChildren.contains( METRIC_NAME));
    }

    @Test
    public void testRecordChildIsImmortalWith() {
        assertFalse( "checking haveImmortalChild before setting",
                     _scs.haveImmortalChild());
        _scs.recordChildIsImmortal();
        assertTrue( "checking haveImmortalChild after setting",
                    _scs.haveImmortalChild());
    }

    @Test
    public void testRecordNewWhenIShouldExpireDate() {
        assertNull( "Test initial expire date", _scs.getWhenIShouldExpireDate());

        Date someTimeInTheFuture = new Date(
                                             System.currentTimeMillis() + 10 * 1000);

        _scs.recordNewWhenIShouldExpireDate( someTimeInTheFuture);

        Date expireDate = _scs.getWhenIShouldExpireDate();

        assertEquals( "Check new value is recorded", someTimeInTheFuture,
                      expireDate);
    }

    @Test
    public void testInitialExpireDate() {
        assertNull( "Test initial expire date", _scs.getWhenIShouldExpireDate());
    }

    @Test
    public void testRecordChildItr() {
        _scs.recordChildItr( METRIC_NAME);
        assertTrue( "named child not found when added",
                    _scs.getItrChildren().contains( METRIC_NAME));
    }

    @Test
    public void testGetItrChildrenInitial() {
        assertTrue( "checking getItrChildren() initial empty",
                    _scs.getItrChildren().isEmpty());
    }

    @Test
    public void testGetItrChildrenAfterChange() {
        _scs.recordChildItr( METRIC_NAME);
        assertFalse( "checking getItrChildren() not empty",
                     _scs.getItrChildren().isEmpty());
    }

    @Test
    public void testHasChildChangedInitial() {
        assertFalse( "Checking hasChildChanged initially false",
                     _scs.hasChildChanged( METRIC_NAME));
    }

    @Test
    public void testHasChildChangedAfterChange() {
        _scs.recordChildItr( METRIC_NAME);
        assertTrue( "Checking hasChildChanged updated correctly",
                    _scs.hasChildChanged( METRIC_NAME));
    }

    @Test
    public void testHasChildChangedAfterNewChild() {
        StateValue value = new TestStateValue( false);

        _scs.recordNewChild( METRIC_NAME, value);
        assertTrue( "Checking hasChildChanged returns true for new child",
                    _scs.hasChildChanged( METRIC_NAME));
    }

    @Test
    public void testHasChildChangedAfterUpdatedChild() {
        StateValue value = new TestStateValue( false);

        _scs.recordUpdatedChild( METRIC_NAME, value);
        assertTrue( "Checking hasChildChanged returns true for updated child",
                    _scs.hasChildChanged( METRIC_NAME));
    }

    @Test
    public void testHasChildChangedAfterRemovedChild() {
        _scs.recordRemovedChild( METRIC_NAME);
        assertTrue( "Checking hasChildChanged returns true for updated child",
                    _scs.hasChildChanged( METRIC_NAME));
    }

    @Test
    public void testHaveImmortalChildInitial() {
        assertFalse( "checking initially haveImmortalChild() false",
                     _scs.haveImmortalChild());
    }

    @Test
    public void testHaveImmortalChildAfterRegistered() {
        _scs.recordChildIsImmortal();
        assertTrue( "checking initially haveImmortalChild() true",
                    _scs.haveImmortalChild());
    }

    @Test
    public void testChildIsRemovedInitial() {
        assertFalse( "Checking initial childIsRemoved",
                     _scs.childIsRemoved( METRIC_NAME));
    }

    @Test
    public void testChildIsRemovedAfterUpdate() {
        _scs.recordRemovedChild( METRIC_NAME);
        assertTrue( "Checking childIsRemoved after recording removed child",
                    _scs.childIsRemoved( METRIC_NAME));
    }

    @Test
    public void testChildIsUpdatedInitial() {
        assertFalse( "Checking initial childIsUpdated",
                     _scs.childIsUpdated( METRIC_NAME));
    }

    @Test
    public void testChildIsUpdatedAfterUpdate() {
        StateValue metricValue = new TestStateValue( true);
        _scs.recordUpdatedChild( METRIC_NAME, metricValue);
        assertTrue( "Checking childIsUpdated after recording updated metric",
                    _scs.childIsUpdated( METRIC_NAME));
    }

    @Test
    public void testChildIsNewInitial() {
        assertFalse( "Checking initial childIsNew",
                     _scs.childIsNew( METRIC_NAME));
    }

    @Test
    public void testChildIsNewWithMetric() {
        StateValue metricValue = new TestStateValue( true);
        _scs.recordNewChild( METRIC_NAME, metricValue);
        assertTrue( "Checking childIsNew after recording new metric",
                    _scs.childIsNew( METRIC_NAME));
    }

    @Test
    public void testGetFreshChildValueInitial() {
        assertNull( "Checking initial getFreshChildValue",
                    _scs.getFreshChildValue( METRIC_NAME));
    }

    @Test
    public void testGetFreshChildValueAfterUpdate() {
        StateValue metricValue = new TestStateValue( true);
        _scs.recordUpdatedChild( METRIC_NAME, metricValue);

        StateComponent freshComponent = _scs.getFreshChildValue( METRIC_NAME);

        assertSame( "checking recovered StateComponent is what was recorded",
                    metricValue, freshComponent);
    }

    @Test
    public void testGetFreshChildValueAfterNew() {
        StateValue metricValue = new TestStateValue( true);
        _scs.recordNewChild( METRIC_NAME, metricValue);

        StateComponent freshComponent = _scs.getFreshChildValue( METRIC_NAME);

        assertSame( "checking recovered StateComponent is what was recorded",
                    metricValue, freshComponent);
    }

    @Test
    public void testGetFreshChildValueAfterNewAndUpdated() {
        StateValue newMetricValue = new TestStateValue( true);
        _scs.recordNewChild( METRIC_NAME, newMetricValue);

        StateValue updatedMetricValue = new TestStateValue( true);
        _scs.recordUpdatedChild( METRIC_NAME, updatedMetricValue);

        StateComponent freshComponent = _scs.getFreshChildValue( METRIC_NAME);

        assertSame( "checking recovered StateComponent is what was recorded",
                    updatedMetricValue, freshComponent);
    }

    @Test
    public void testGetFreshChildValueAfterUpdatedAndNew() {
        StateValue updatedMetricValue = new TestStateValue( true);
        _scs.recordUpdatedChild( METRIC_NAME, updatedMetricValue);

        StateValue newMetricValue = new TestStateValue( true);
        _scs.recordNewChild( METRIC_NAME, newMetricValue);

        StateComponent freshComponent = _scs.getFreshChildValue( METRIC_NAME);

        assertSame( "checking recovered StateComponent is what was recorded",
                    newMetricValue, freshComponent);
    }

    @Test
    public void testNewThenUpdate() {
        StateValue updatedMetricValue = new TestStateValue( true);
        StateValue newMetricValue = new TestStateValue( true);

        _scs.recordUpdatedChild( METRIC_NAME, updatedMetricValue);
        _scs.recordNewChild( METRIC_NAME, newMetricValue);

        assertFalse( "childIsUpdate", _scs.childIsUpdated( METRIC_NAME));
        assertTrue( "childIsNew", _scs.childIsNew( METRIC_NAME));

        assertNull( "updatedChildValue",
                    _scs.getUpdatedChildValue( METRIC_NAME));
        assertSame( "newChildValue", newMetricValue,
                    _scs.getNewChildValue( METRIC_NAME));

        Collection<String> updatedChildren = _scs.getUpdatedChildren();
        Collection<String> newChildren = _scs.getNewChildren();

        assertFalse( "updatedChildren not null", updatedChildren == null);
        assertFalse( "newChildren not null", newChildren == null);

        assertTrue( "updatedChildren", updatedChildren.isEmpty());
        assertTrue( "newChildren", newChildren.contains( METRIC_NAME));
    }

    @Test
    public void testUpdateThenNew() {
        StateValue updatedMetricValue = new TestStateValue( true);
        StateValue newMetricValue = new TestStateValue( true);

        _scs.recordNewChild( METRIC_NAME, newMetricValue);
        _scs.recordUpdatedChild( METRIC_NAME, updatedMetricValue);

        assertTrue( "childIsUpdate", _scs.childIsUpdated( METRIC_NAME));
        assertFalse( "childIsNew", _scs.childIsNew( METRIC_NAME));

        assertNull( "getNewChildValue", _scs.getNewChildValue( METRIC_NAME));
        assertSame( "getUpdatedChildValue", updatedMetricValue,
                    _scs.getUpdatedChildValue( METRIC_NAME));

        Collection<String> updatedChildren = _scs.getUpdatedChildren();
        Collection<String> newChildren = _scs.getNewChildren();

        assertFalse( "updatedChildren not null", updatedChildren == null);
        assertFalse( "newChildren not null", newChildren == null);

        assertTrue( "newChildren", newChildren.isEmpty());
        assertTrue( "updatedChildren", updatedChildren.contains( METRIC_NAME));
    }

    @Test
    public void testRemovedThenNew() {
        StateValue newMetricValue = new TestStateValue( true);

        _scs.recordRemovedChild( METRIC_NAME);
        _scs.recordNewChild( METRIC_NAME, newMetricValue);

        assertTrue( "childIsNew", _scs.childIsNew( METRIC_NAME));
        assertFalse( "childIsRemoved", _scs.childIsRemoved( METRIC_NAME));

        assertSame( "newChildValue", newMetricValue,
                    _scs.getNewChildValue( METRIC_NAME));

        Collection<String> removedChildren = _scs.getRemovedChildren();
        Collection<String> newChildren = _scs.getNewChildren();

        assertFalse( "removedChildren not null", removedChildren == null);
        assertFalse( "newChildren not null", newChildren == null);

        assertTrue( "removedChildren", removedChildren.isEmpty());
        assertTrue( "newChildren", newChildren.contains( METRIC_NAME));
    }

    @Test
    public void testNewThenRemoved() {
        StateValue newMetricValue = new TestStateValue( true);

        _scs.recordNewChild( METRIC_NAME, newMetricValue);
        _scs.recordRemovedChild( METRIC_NAME);

        assertFalse( "childIsNew()", _scs.childIsNew( METRIC_NAME));
        assertTrue( "childIsRemoved()", _scs.childIsRemoved( METRIC_NAME));

        assertNull( "newChildValue is null",
                    _scs.getNewChildValue( METRIC_NAME));

        Collection<String> removedChildren = _scs.getRemovedChildren();
        Collection<String> newChildren = _scs.getNewChildren();

        assertFalse( "removedChildren not null", removedChildren == null);
        assertFalse( "newChildren not null", newChildren == null);

        assertTrue( "getNewChildren() is empty", newChildren.isEmpty());
        assertTrue( "getRemovedChildren() has METRIC_NAME",
                    removedChildren.contains( METRIC_NAME));
    }

    @Test
    public void testRemoveThenEnsureNotRemoved() {
        _scs.recordRemovedChild( METRIC_NAME);
        _scs.ensureChildNotRemoved( METRIC_NAME);
        assertFalse( "childIsRemoved", _scs.childIsRemoved( METRIC_NAME));

        Collection<String> removedChildren = _scs.getRemovedChildren();

        assertFalse( "removedChildren not null", removedChildren == null);
        assertFalse( "getRemovedChildren() does not have METRIC_NAME",
                     removedChildren.contains( METRIC_NAME));
    }

    @Test
    public void testRemoveTwoChildrenThenEnsureNotRemovedOne() {
        String metricName2 = "otherMetric";

        _scs.recordRemovedChild( METRIC_NAME);
        _scs.recordRemovedChild( metricName2);
        _scs.ensureChildNotRemoved( METRIC_NAME);
        assertFalse( "childIsRemoved", _scs.childIsRemoved( METRIC_NAME));
        assertTrue( "childIsRemoved", _scs.childIsRemoved( metricName2));

        Collection<String> removedChildren = _scs.getRemovedChildren();

        assertFalse( "removedChildren not null", removedChildren == null);
        assertFalse( "getRemovedChildren() does not have METRIC_NAME",
                     removedChildren.contains( METRIC_NAME));
        assertTrue( "getRemovedChildren() does have metricName2",
                    removedChildren.contains( metricName2));
    }

    @Test
    public void testGetNewChildrenInitial() {
        Collection<String> names = _scs.getNewChildren();
        assertNotNull( "Check what we get back isn't null", names);
        assertTrue( "Check that initial state is empty", names.isEmpty());
    }

    @Test
    public void testGetNewChildrenAfterRegistering() {
        StateComponent metricValue = new TestStateValue( true);
        _scs.recordNewChild( METRIC_NAME, metricValue);

        Collection<String> names = _scs.getNewChildren();

        assertNotNull( "Check what we get back isn't null", names);
        assertTrue( "Check that Collection has our metric",
                    names.contains( METRIC_NAME));
    }

    @Test
    public void testGetRemovedChildrenInitial() {
        Collection<String> names = _scs.getRemovedChildren();
        assertNotNull( "Check what we get back isn't null", names);
        assertTrue( "Check that initial state is empty", names.isEmpty());
    }

    @Test
    public void testGetRemovedChildrenAfterRegistering() {
        _scs.recordRemovedChild( METRIC_NAME);

        Collection<String> names = _scs.getRemovedChildren();

        assertNotNull( "Check what we get back isn't null", names);
        assertTrue( "Check that Collection has our metric",
                    names.contains( METRIC_NAME));
    }

    @Test
    public void testGetUpdatedChildrenInitial() {
        Collection<String> names = _scs.getUpdatedChildren();
        assertNotNull( "Check what we get back isn't null", names);
        assertTrue( "Check that initial state is empty", names.isEmpty());
    }

    @Test
    public void testGetUpdatedChildrenAfterRegistering() {
        StateComponent metricValue = new TestStateValue( true);
        _scs.recordUpdatedChild( METRIC_NAME, metricValue);

        Collection<String> names = _scs.getUpdatedChildren();

        assertNotNull( "Check what we get back isn't null", names);
        assertTrue( "Check that Collection has our metric",
                    names.contains( METRIC_NAME));
    }

    @Test
    public void testGetUpdatedChildValueInitial() {
        assertNull( "expect null reply initially",
                    _scs.getUpdatedChildValue( METRIC_NAME));
    }

    @Test
    public void testGetUpdatedChildValueAfterRecording() {
        StateComponent metricValue = new TestStateValue( true);
        _scs.recordUpdatedChild( METRIC_NAME, metricValue);

        StateComponent component = _scs.getUpdatedChildValue( METRIC_NAME);
        assertSame( "checking we get back same value as registered",
                    metricValue, component);
    }

    @Test
    public void testGetNewChildValue() {
        StateComponent metricValue = new TestStateValue( true);
        _scs.recordNewChild( METRIC_NAME, metricValue);

        StateComponent component = _scs.getNewChildValue( METRIC_NAME);

        assertSame( "Checking we get back same value as registered",
                    metricValue, component);
    }

    @Test
    public void testDumpContentsInitial() {
        String dump = _scs.dumpContents();

        assertNotNull( "checking initial dump not null", dump);
        assertTrue( "checking initial dump not empty", dump.length() > 0);
    }

    @Test
    public void testDumpContentsAfterUpdate() {
        StateComponent metricValue = new StringStateValue( "String-value", true);

        String initialDump = _scs.dumpContents();

        _scs.recordUpdatedChild( METRIC_NAME, metricValue);

        String afterUpdateDump = _scs.dumpContents();

        assertNotNull( "checking dump after update not null", afterUpdateDump);
        assertTrue( "checking dump after update not empty",
                    afterUpdateDump.length() > 0);
        assertFalse( "checking dump before and after add are not equal",
                     initialDump.equals( afterUpdateDump));
    }

}
