package org.dcache.services.info.stateInfo;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Map;

import org.dcache.services.info.base.IntegerStateValue;
import org.dcache.services.info.base.StatePath;
import org.dcache.services.info.base.StringStateValue;
import org.dcache.services.info.base.TestStateExhibitor;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests for the ReservationInfoVisitor
 */
public class ReservationInfoVisitorTests {

    static final StatePath PATH_RESERVATIONS = new StatePath( "reservations");

    TestStateExhibitor _exhibitor;
    ReservationInfoVisitor _visitor;

    @Before
    public void setUp() throws Exception {
        _exhibitor = new TestStateExhibitor();
        _visitor = new ReservationInfoVisitor();
    }

    @Test
    public void testEmptyState() {
        _exhibitor.visitState( _visitor);

        Map<String, ReservationInfo> info = _visitor.getReservations();

        assertTrue( "check empty visit is empty", info.isEmpty());
    }

    @Test
    public void testReservationWithoutData() {
        String resvId = "aabbcc";

        _exhibitor.addBranch( reservationPath( resvId));

        _exhibitor.visitState( _visitor);

        Map<String, ReservationInfo> info = _visitor.getReservations();

        assertEquals( "check visit contains one entry", 1, info.size());

        assertTrue( "check visit result has reservation",
                    info.containsKey( resvId));

        ReservationInfo resv = info.get( resvId);

        assertFalse( "checking hasTotal", resv.hasTotal());
        assertFalse( "checking hasAllocated", resv.hasAllocated());
        assertFalse( "checking hasFree", resv.hasFree());
        assertFalse( "checking hasUsed", resv.hasUsed());

        assertFalse( "checking hasLifetime", resv.hasLifetime());

        assertFalse( "checking description", resv.hasDescription());
        assertFalse( "checking access-latency", resv.hasAccessLatency());
        assertFalse( "checking retention-policy", resv.hasRetentionPolicy());
        assertFalse( "has state", resv.hasState());
    }

    @Test
    public void testReservationWithGroupSlashVO() {
        String resvId = "aabbcc";
        String voName = "ATLAS";

        _exhibitor.addMetric(
                              reservationPath( resvId).newChild(
                                                                 ReservationInfoVisitor.PATH_ELEMENT_AUTHORISATION).newChild(
                                                                                                                              ReservationInfoVisitor.PATH_ELEMENT_GROUP),
                              new StringStateValue( "/" + voName));

        _exhibitor.visitState( _visitor);

        Map<String, ReservationInfo> info = _visitor.getReservations();

        assertEquals( "check visit contains one entry", 1, info.size());

        assertTrue( "check visit result has reservation",
                    info.containsKey( resvId));

        ReservationInfo resv = info.get( resvId);

        assertFalse( "checking hasTotal", resv.hasTotal());
        assertFalse( "checking hasAllocated", resv.hasAllocated());
        assertFalse( "checking hasFree", resv.hasFree());
        assertFalse( "checking hasUsed", resv.hasUsed());

        assertFalse( "checking hasLifetime", resv.hasLifetime());

        assertTrue( "checking hasVo", resv.hasVo());

        assertEquals( "checking vo", voName, resv.getVo());

        assertFalse( "checking description", resv.hasDescription());
        assertFalse( "checking access-latency", resv.hasAccessLatency());
        assertFalse( "checking retention-policy", resv.hasRetentionPolicy());
        assertFalse( "checking state", resv.hasState());
    }

    @Test
    public void testReservationWithGroupWithoutSlashVO() {
        String resvId = "aabbcc";
        String voName = "ATLAS";

        _exhibitor.addMetric(
                              reservationPath( resvId).newChild(
                                                                 ReservationInfoVisitor.PATH_ELEMENT_AUTHORISATION).newChild(
                                                                                                                              ReservationInfoVisitor.PATH_ELEMENT_GROUP),
                              new StringStateValue( voName));

        _exhibitor.visitState( _visitor);

        Map<String, ReservationInfo> info = _visitor.getReservations();

        assertEquals( "check visit contains one entry", 1, info.size());

        assertTrue( "check visit result has reservation",
                    info.containsKey( resvId));

        ReservationInfo resv = info.get( resvId);

        assertFalse( "checking hasTotal", resv.hasTotal());
        assertFalse( "checking hasAllocated", resv.hasAllocated());
        assertFalse( "checking hasFree", resv.hasFree());
        assertFalse( "checking hasUsed", resv.hasUsed());

        assertFalse( "checking hasLifetime", resv.hasLifetime());

        assertTrue( "checking hasVo", resv.hasVo());

        assertEquals( "checking vo", voName, resv.getVo());

        assertFalse( "checking description", resv.hasDescription());
        assertFalse( "checking access-latency", resv.hasAccessLatency());
        assertFalse( "checking retention-policy", resv.hasRetentionPolicy());
        assertFalse( "checking state", resv.hasState());
    }

    @Test
    public void testReservationWithGroupVOSlashSomething() {
        String resvId = "aabbcc";
        String voName = "ATLAS";
        String group = "/" + voName + "/someGroup";

        _exhibitor.addMetric(
                              reservationPath( resvId).newChild(
                                                                 ReservationInfoVisitor.PATH_ELEMENT_AUTHORISATION).newChild(
                                                                                                                              ReservationInfoVisitor.PATH_ELEMENT_GROUP),
                              new StringStateValue( group));

        _exhibitor.visitState( _visitor);

        Map<String, ReservationInfo> info = _visitor.getReservations();

        assertEquals( "check visit contains one entry", 1, info.size());

        assertTrue( "check visit result has reservation",
                    info.containsKey( resvId));

        ReservationInfo resv = info.get( resvId);

        assertFalse( "checking hasTotal", resv.hasTotal());
        assertFalse( "checking hasAllocated", resv.hasAllocated());
        assertFalse( "checking hasFree", resv.hasFree());
        assertFalse( "checking hasUsed", resv.hasUsed());

        assertFalse( "checking hasLifetime", resv.hasLifetime());

        assertTrue( "checking hasVo", resv.hasVo());

        assertEquals( "checking vo", voName, resv.getVo());

        assertFalse( "checking description", resv.hasDescription());
        assertFalse( "checking access-latency", resv.hasAccessLatency());
        assertFalse( "checking retention-policy", resv.hasRetentionPolicy());
        assertFalse( "checking have state", resv.hasState());
    }

    @Test
    public void testReservationWithTotal() {
        String resvId = "aabbcc";

        long value = 100;

        _exhibitor.addMetric(
                              reservationPath( resvId).newChild(
                                                                 ReservationInfoVisitor.PATH_ELEMENT_SPACE).newChild(
                                                                                                                      ReservationInfoVisitor.PATH_ELEMENT_TOTAL),
                              new IntegerStateValue( value));

        _exhibitor.visitState( _visitor);

        Map<String, ReservationInfo> info = _visitor.getReservations();

        assertEquals( "check visit contains one entry", 1, info.size());

        assertTrue( "check visit result has reservation",
                    info.containsKey( resvId));

        ReservationInfo resv = info.get( resvId);

        assertTrue( "checking hasTotal", resv.hasTotal());
        assertEquals( "checking total value", value, resv.getTotal());
        assertFalse( "checking hasAllocated", resv.hasAllocated());
        assertFalse( "checking hasFree", resv.hasFree());
        assertFalse( "checking hasUsed", resv.hasUsed());

        assertFalse( "checking hasLifetime", resv.hasLifetime());

        assertFalse( "checking hasVo", resv.hasVo());

        assertFalse( "checking description", resv.hasDescription());
        assertFalse( "checking access-latency", resv.hasAccessLatency());
        assertFalse( "checking retention-policy", resv.hasRetentionPolicy());
        assertFalse( "checking state", resv.hasState());
    }

    @Test
    public void testReservationWithUsed() {
        String resvId = "aabbcc";

        long value = 100;

        _exhibitor.addMetric(
                              reservationPath( resvId).newChild(
                                                                 ReservationInfoVisitor.PATH_ELEMENT_SPACE).newChild(
                                                                                                                      ReservationInfoVisitor.PATH_ELEMENT_USED),
                              new IntegerStateValue( value));

        _exhibitor.visitState( _visitor);

        Map<String, ReservationInfo> info = _visitor.getReservations();

        assertEquals( "check visit contains one entry", 1, info.size());

        assertTrue( "check visit result has reservation",
                    info.containsKey( resvId));

        ReservationInfo resv = info.get( resvId);

        assertTrue( "checking hasUsed", resv.hasUsed());
        assertEquals( "checking used value", value, resv.getUsed());
        assertFalse( "checking hasAllocated", resv.hasAllocated());
        assertFalse( "checking hasFree", resv.hasFree());
        assertFalse( "checking hasTotal", resv.hasTotal());

        assertFalse( "checking hasLifetime", resv.hasLifetime());

        assertFalse( "checking hasVo", resv.hasVo());

        assertFalse( "checking description", resv.hasDescription());
        assertFalse( "checking access-latency", resv.hasAccessLatency());
        assertFalse( "checking retention-policy", resv.hasRetentionPolicy());
        assertFalse( "checking state", resv.hasState());
    }

    @Test
    public void testReservationWithAllocated() {
        String resvId = "aabbcc";

        long value = 100;

        _exhibitor.addMetric(
                              reservationPath( resvId).newChild(
                                                                 ReservationInfoVisitor.PATH_ELEMENT_SPACE).newChild(
                                                                                                                      ReservationInfoVisitor.PATH_ELEMENT_ALLOCATED),
                              new IntegerStateValue( value));

        _exhibitor.visitState( _visitor);

        Map<String, ReservationInfo> info = _visitor.getReservations();

        assertEquals( "check visit contains one entry", 1, info.size());

        assertTrue( "check visit result has reservation",
                    info.containsKey( resvId));

        ReservationInfo resv = info.get( resvId);

        assertTrue( "checking hasAllocated", resv.hasAllocated());
        assertEquals( "checking allocated value", value, resv.getAllocated());
        assertFalse( "checking hasUsed", resv.hasUsed());
        assertFalse( "checking hasFree", resv.hasFree());
        assertFalse( "checking hasTotal", resv.hasTotal());

        assertFalse( "checking hasLifetime", resv.hasLifetime());

        assertFalse( "checking hasVo", resv.hasVo());

        assertFalse( "checking description", resv.hasDescription());
        assertFalse( "checking access-latency", resv.hasAccessLatency());
        assertFalse( "checking retention-policy", resv.hasRetentionPolicy());
        assertFalse( "checking state", resv.hasState());
    }

    @Test
    public void testReservationWithFree() {
        String resvId = "aabbcc";

        long value = 100;

        _exhibitor.addMetric(
                              reservationPath( resvId).newChild(
                                                                 ReservationInfoVisitor.PATH_ELEMENT_SPACE).newChild(
                                                                                                                      ReservationInfoVisitor.PATH_ELEMENT_FREE),
                              new IntegerStateValue( value));

        _exhibitor.visitState( _visitor);

        Map<String, ReservationInfo> info = _visitor.getReservations();

        assertEquals( "check visit contains one entry", 1, info.size());

        assertTrue( "check visit result has reservation",
                    info.containsKey( resvId));

        ReservationInfo resv = info.get( resvId);

        assertTrue( "checking hasFree", resv.hasFree());
        assertEquals( "checking free value", value, resv.getFree());
        assertFalse( "checking hasUsed", resv.hasUsed());
        assertFalse( "checking hasAllocated", resv.hasAllocated());
        assertFalse( "checking hasTotal", resv.hasTotal());

        assertFalse( "checking hasLifetime", resv.hasLifetime());

        assertFalse( "checking hasVo", resv.hasVo());

        assertFalse( "checking description", resv.hasDescription());
        assertFalse( "checking access-latency", resv.hasAccessLatency());
        assertFalse( "checking retention-policy", resv.hasRetentionPolicy());
        assertFalse( "checking state", resv.hasState());
    }

    @Test
    public void testReservationWithDescription() {
        String resvId = "aabbcc";

        String description = "the description";

        _exhibitor.addMetric(
                              reservationPath( resvId).newChild(
                                                                 ReservationInfoVisitor.PATH_ELEMENT_DESCRIPTION),
                              new StringStateValue( description));

        _exhibitor.visitState( _visitor);

        Map<String, ReservationInfo> info = _visitor.getReservations();

        assertEquals( "check visit contains one entry", 1, info.size());

        assertTrue( "check visit result has reservation",
                    info.containsKey( resvId));

        ReservationInfo resv = info.get( resvId);

        assertFalse( "checking hasFree", resv.hasFree());
        assertFalse( "checking hasUsed", resv.hasUsed());
        assertFalse( "checking hasAllocated", resv.hasAllocated());
        assertFalse( "checking hasTotal", resv.hasTotal());

        assertFalse( "checking hasLifetime", resv.hasLifetime());

        assertFalse( "checking hasVo", resv.hasVo());

        assertEquals( "checking description", description,
                      resv.getDescription());
        assertFalse( "checking access-latency", resv.hasAccessLatency());
        assertFalse( "checking retention-policy", resv.hasRetentionPolicy());
        assertFalse( "checking state", resv.hasState());
    }

    @Test
    public void testReservationWithLifetime() {
        String resvId = "aabbcc";

        int lifetime = 100;

        _exhibitor.addMetric(
                              reservationPath( resvId).newChild(
                                                                 ReservationInfoVisitor.PATH_ELEMENT_LIFETIME),
                              new IntegerStateValue( lifetime));

        _exhibitor.visitState( _visitor);

        Map<String, ReservationInfo> info = _visitor.getReservations();

        assertEquals( "check visit contains one entry", 1, info.size());

        assertTrue( "check visit result has reservation",
                    info.containsKey( resvId));

        ReservationInfo resv = info.get( resvId);

        assertFalse( "checking hasFree", resv.hasFree());
        assertFalse( "checking hasUsed", resv.hasUsed());
        assertFalse( "checking hasAllocated", resv.hasAllocated());
        assertFalse( "checking hasTotal", resv.hasTotal());

        assertTrue( "checking hasLifetime", resv.hasLifetime());
        assertEquals( "checking lifetime value", lifetime, resv.getLifetime());

        assertFalse( "checking hasVo", resv.hasVo());

        assertFalse( "checking description", resv.hasDescription());
        assertFalse( "checking access-latency", resv.hasAccessLatency());
        assertFalse( "checking retention-policy", resv.hasRetentionPolicy());
        assertFalse( "checking state", resv.hasState());
    }

    @Test
    public void testReservationWithAccessLatencyNearline() {
        assertAccessLatency( ReservationInfo.AccessLatency.NEARLINE);
    }

    @Test
    public void testReservationWithAccessLatencyOnline() {
        assertAccessLatency( ReservationInfo.AccessLatency.ONLINE);
    }

    @Test
    public void testReservationWithAccessLatencyOffline() {
        assertAccessLatency( ReservationInfo.AccessLatency.OFFLINE);
    }

    @Test
    public void testReservationWithRetentionPolicyCustodial() {
        assertRetentionPolicy( ReservationInfo.RetentionPolicy.CUSTODIAL);
    }

    @Test
    public void testReservationWithRetentionPolicyOutput() {
        assertRetentionPolicy( ReservationInfo.RetentionPolicy.OUTPUT);
    }

    @Test
    public void testReservationWithRetentionPolicyReplica() {
        assertRetentionPolicy( ReservationInfo.RetentionPolicy.REPLICA);
    }

    @Test
    public void testReservationWithStateReserved() {
        assertState( ReservationInfo.State.RESERVED);
    }

    @Test
    public void testReservationWithStateExpired() {
        assertState( ReservationInfo.State.EXPIRED);
    }

    @Test
    public void testReservationWithStateReleased() {
        assertState( ReservationInfo.State.RELEASED);
    }

    private void assertAccessLatency( ReservationInfo.AccessLatency al) {
        String resvId = "aabbcc";

        _exhibitor.addMetric(
                              reservationPath( resvId).newChild(
                                                                 ReservationInfoVisitor.PATH_ELEMENT_AL),
                              new StringStateValue( al.getMetricValue()));

        _exhibitor.visitState( _visitor);

        Map<String, ReservationInfo> info = _visitor.getReservations();

        assertEquals( "check visit contains one entry", 1, info.size());

        assertTrue( "check visit result has reservation",
                    info.containsKey( resvId));

        ReservationInfo resv = info.get( resvId);

        assertFalse( "checking hasFree", resv.hasFree());
        assertFalse( "checking hasUsed", resv.hasUsed());
        assertFalse( "checking hasAllocated", resv.hasAllocated());
        assertFalse( "checking hasTotal", resv.hasTotal());

        assertFalse( "checking hasLifetime", resv.hasLifetime());

        assertFalse( "checking hasVo", resv.hasVo());

        assertFalse( "checking description", resv.hasDescription());
        assertEquals( "checking access-latency", al, resv.getAccessLatency());
        assertFalse( "checking retention-policy", resv.hasRetentionPolicy());
        assertFalse( "checking state", resv.hasState());
    }

    private void assertRetentionPolicy( ReservationInfo.RetentionPolicy rp) {
        String resvId = "aabbcc";

        _exhibitor.addMetric(
                              reservationPath( resvId).newChild(
                                                                 ReservationInfoVisitor.PATH_ELEMENT_RP),
                              new StringStateValue( rp.getMetricValue()));

        _exhibitor.visitState( _visitor);

        Map<String, ReservationInfo> info = _visitor.getReservations();

        assertEquals( "check visit contains one entry", 1, info.size());

        assertTrue( "check visit result has reservation",
                    info.containsKey( resvId));

        ReservationInfo resv = info.get( resvId);

        assertFalse( "checking hasFree", resv.hasFree());
        assertFalse( "checking hasUsed", resv.hasUsed());
        assertFalse( "checking hasAllocated", resv.hasAllocated());
        assertFalse( "checking hasTotal", resv.hasTotal());

        assertFalse( "checking hasLifetime", resv.hasLifetime());

        assertFalse( "checking hasVo", resv.hasVo());

        assertFalse( "checking description", resv.hasDescription());
        assertFalse( "checking access-latency", resv.hasAccessLatency());
        assertEquals( "checking retention-policy", rp,
                      resv.getRetentionPolicy());
        assertFalse( "checking state", resv.hasState());
    }

    private void assertState( ReservationInfo.State state) {
        String resvId = "aabbcc";

        _exhibitor.addMetric(
                              reservationPath( resvId).newChild(
                                                                 ReservationInfoVisitor.PATH_ELEMENT_STATE),
                              new StringStateValue( state.getMetricValue()));

        _exhibitor.visitState( _visitor);

        Map<String, ReservationInfo> info = _visitor.getReservations();

        assertEquals( "check visit contains one entry", 1, info.size());

        assertTrue( "check visit result has reservation",
                    info.containsKey( resvId));

        ReservationInfo resv = info.get( resvId);

        assertFalse( "checking hasFree", resv.hasFree());
        assertFalse( "checking hasUsed", resv.hasUsed());
        assertFalse( "checking hasAllocated", resv.hasAllocated());
        assertFalse( "checking hasTotal", resv.hasTotal());

        assertFalse( "checking hasLifetime", resv.hasLifetime());

        assertFalse( "checking hasVo", resv.hasVo());

        assertFalse( "checking description", resv.hasDescription());
        assertFalse( "checking access-latency", resv.hasAccessLatency());
        assertFalse( "checking retention-policy", resv.hasRetentionPolicy());
        assertEquals( "checking state", state, resv.getState());
    }

    /**
     * Provide the StatePath of a reservation.
     *
     * @param id the ID of the reservation.
     * @return a StatePath object pointing to the top-most part of this
     *         reservation.
     */
    private StatePath reservationPath( String id) {
        return PATH_RESERVATIONS.newChild( id);
    }
}
