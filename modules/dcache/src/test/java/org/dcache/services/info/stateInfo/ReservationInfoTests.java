package org.dcache.services.info.stateInfo;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;

/**
 * A set of tests for checking ReservationInfo behaves as expected.
 */
public class ReservationInfoTests {

    int nextReservationId = 1;

    String _reservationId;
    ReservationInfo _info;

    @Before
    public void setUp()
    {
        _reservationId = Integer.toString( nextReservationId);
        _info = new ReservationInfo( _reservationId);
    }

    @Test
    public void testGetId() {
        assertEquals( "checking reservation ID is as set.", _reservationId,
                      _info.getId());
    }

    @Test
    public void testGetAccessLatencyEmpty() {
        assertNull( "check access latency initiall null",
                    _info.getAccessLatency());
    }

    @Test
    public void testInitialHasAccessLatencyValue() {
        assertFalse( "check initial hasAccessLatency value",
                     _info.hasAccessLatency());
    }

    @Test
    public void testSetAccessLatencyThenGet() {
        ReservationInfo.AccessLatency al = ReservationInfo.AccessLatency.NEARLINE;

        _info.setAccessLatency( al);

        assertEquals( "check access latency initiall null", al,
                      _info.getAccessLatency());
    }

    @Test
    public void testSetAccessLatencyThenHasAccessLatency() {
        _info.setAccessLatency( ReservationInfo.AccessLatency.ONLINE);
        assertTrue( "check hasAccessLatency", _info.hasAccessLatency());
    }

    @Test(expected = RuntimeException.class)
    public void testSetAlTwice() {
        ReservationInfo.AccessLatency al = ReservationInfo.AccessLatency.NEARLINE;

        _info.setAccessLatency( al);
        _info.setAccessLatency( al);
    }

    @Test
    public void testDescriptionEmpty() {
        assertNull( "empty description", _info.getDescription());
    }

    @Test
    public void testSetDescriptionThenGet() {
        String description = "a description";

        _info.setDescription( description);

        assertEquals( "check description after setting it.", description,
                      _info.getDescription());
    }

    @Test(expected = RuntimeException.class)
    public void testSetDescriptionTwice() {
        _info.setDescription( "a string");
        _info.setDescription( "another string");
    }

    @Test
    public void testInitialHaveLifetime() {
        assertFalse( "haveLifetime initially", _info.hasLifetime());
    }

    @Test
    public void testSetLifetimeThenGet() {
        int lifetime = 10;

        _info.setLifetime( lifetime);

        assertEquals( "check lifetime after setting it", lifetime,
                      _info.getLifetime());
    }

    @Test(expected = RuntimeException.class)
    public void testSetLifetimeTwice() {
        _info.setLifetime( 10);
        _info.setLifetime( 10);
    }

    @Test
    public void testHaveLifetimeAfterSet() {
        _info.setLifetime( 10);
        assertTrue( "haveLifetime after setting a value", _info.hasLifetime());
    }

    @Test
    public void testInitialRetentionPolicy() {
        assertNull( "check that retention policy inial value",
                    _info.getRetentionPolicy());
    }

    @Test
    public void testInitialHasRetentionPolicyValue() {
        assertFalse( "initial hasRetentionPolicy() value",
                     _info.hasRetentionPolicy());
    }

    @Test
    public void testSetRetentionPolicyThenGet() {
        ReservationInfo.RetentionPolicy rp = ReservationInfo.RetentionPolicy.CUSTODIAL;

        _info.setRetentionPolicy( rp);

        assertEquals( "check that retention policy follows value", rp,
                      _info.getRetentionPolicy());
    }

    @Test
    public void testSetRetentionPolicyThenHasRetentionPolicy() {
        _info.setRetentionPolicy( ReservationInfo.RetentionPolicy.OUTPUT);

        assertTrue( "hasRetentionPolicy()", _info.hasRetentionPolicy());
    }

    @Test(expected = RuntimeException.class)
    public void testSetRetentionPolicyTwice() {
        ReservationInfo.RetentionPolicy rp = ReservationInfo.RetentionPolicy.CUSTODIAL;

        _info.setRetentionPolicy( rp);
        _info.setRetentionPolicy( rp);
    }

    @Test
    public void testGetStateInitialValue() {
        assertNull( "initial state of reservation", _info.getState());
    }

    @Test
    public void testHaveStateInitialValue() {
        assertFalse( "initial state of haveState", _info.hasState());
    }

    @Test
    public void testSetStateThenHaveState() {
        _info.setState( ReservationInfo.State.RESERVED);
        assertTrue( "state of haveState after setting state", _info.hasState());
    }

    @Test
    public void testSetStateInitialValueThenGet() {
        ReservationInfo.State state = ReservationInfo.State.RESERVED;

        _info.setState( state);
        assertEquals( "state of reservation after setting", state,
                      _info.getState());
    }

    @Test(expected = RuntimeException.class)
    public void testSetStateTwice() {
        ReservationInfo.State state = ReservationInfo.State.RESERVED;

        _info.setState( state);
        _info.setState( state);
    }

    @Test
    public void testHaveTotalInitialValue() {
        assertFalse( "initial value of haveTotal", _info.hasTotal());
    }

    @Test
    public void testSetTotalThenGet() {
        int value = 100;
        _info.setTotal( value);
        assertEquals( "value of total after setting it", value,
                      _info.getTotal());
    }

    @Test
    public void testHaveTotalAfterSet() {
        _info.setTotal( 100);
        assertTrue( "haveTotal after setTotal", _info.hasTotal());
    }

    @Test(expected = RuntimeException.class)
    public void testSetTotalTwice() {
        _info.setTotal( 100);
        _info.setTotal( 100);
    }

    @Test
    public void testHaveFreeInitialValue() {
        assertFalse( "initial value of haveFree", _info.hasFree());
    }

    @Test
    public void testSetFreeThenGet() {
        int value = 100;
        _info.setFree( value);
        assertEquals( "value of free after setting it", value, _info.getFree());
    }

    @Test
    public void testHaveFreeAfterSet() {
        _info.setFree( 100);
        assertTrue( "haveFree after setFree", _info.hasFree());
    }

    @Test(expected = RuntimeException.class)
    public void testSetFreeTwice() {
        _info.setFree( 100);
        _info.setFree( 100);
    }

    @Test
    public void testHaveAllocatedInitialValue() {
        assertFalse( "initial value of haveAllocated", _info.hasAllocated());
    }

    @Test
    public void testSetAllocatedThenGet() {
        int value = 100;
        _info.setAllocated( value);
        assertEquals( "value of allocated after setting it", value,
                      _info.getAllocated());
    }

    @Test
    public void testHaveAllocatedAfterSet() {
        _info.setAllocated( 100);
        assertTrue( "haveAllocated after setAllocated", _info.hasAllocated());
    }

    @Test(expected = RuntimeException.class)
    public void testSetAllocatedTwice() {
        _info.setFree( 100);
        _info.setFree( 100);
    }

    @Test
    public void testHaveUsedInitialValue() {
        assertFalse( "initial value of haveTotal", _info.hasUsed());
    }

    @Test
    public void testSetUsedThenGet() {
        int value = 100;
        _info.setUsed( value);
        assertEquals( "value of total after setting it", value, _info.getUsed());
    }

    @Test
    public void testHaveUsedAfterSet() {
        _info.setUsed( 100);
        assertTrue( "haveTotal after setTotal", _info.hasUsed());
    }

    @Test(expected = RuntimeException.class)
    public void testSetUsedTwice() {
        _info.setTotal( 100);
        _info.setTotal( 100);
    }

    @Test
    public void testHaveVoInitialValue() {
        assertFalse( "initial value of haveVo", _info.hasVo());
    }

    @Test
    public void testSetVoThenGet() {
        String voName = "the vo";

        _info.setVo( voName);
        assertEquals( "checking setVo() followed by getVo()", voName,
                      _info.getVo());
    }

    @Test
    public void testSetVoThenHaveVo() {
        _info.setVo( "the vo");
        assertTrue( "haveVo() after setVo()", _info.hasVo());
    }

    @Test(expected = RuntimeException.class)
    public void testSetVoTwice() {
        _info.setVo( "this vo");
        _info.setVo( "that vo");
    }
}
