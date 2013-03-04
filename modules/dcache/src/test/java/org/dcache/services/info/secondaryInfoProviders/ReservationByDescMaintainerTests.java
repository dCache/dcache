package org.dcache.services.info.secondaryInfoProviders;

import org.junit.Before;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

import org.dcache.services.info.base.IntegerStateValue;
import org.dcache.services.info.base.MalleableStateTransition;
import org.dcache.services.info.base.PostTransitionStateExhibitor;
import org.dcache.services.info.base.QueuingStateUpdateManager;
import org.dcache.services.info.base.StateComposite;
import org.dcache.services.info.base.StateExhibitor;
import org.dcache.services.info.base.StatePath;
import org.dcache.services.info.base.StateUpdate;
import org.dcache.services.info.base.StateWatcher;
import org.dcache.services.info.base.StringStateValue;
import org.dcache.services.info.base.TestStateExhibitor;
import org.dcache.services.info.stateInfo.ReservationInfo;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ReservationByDescMaintainerTests {

    static final StatePath SUMMARY_RESERVATIONS_BY_VO = StatePath.parsePath( "summary.reservations.by-VO");

    StateWatcher _watcher;
    QueuingStateUpdateManager _sum;
    TestStateExhibitor _exhibitor;
    StateUpdate _update;
    MalleableStateTransition _transition;

    @Before
    public void setUp() {
        _exhibitor = new TestStateExhibitor();
        _watcher = new ReservationByDescMaintainer();
        _sum = new QueuingStateUpdateManager();
        _update = new StateUpdate();
        _transition = new MalleableStateTransition();
    }


    /**
     * State is empty.  We add a reservation.
     */
    @Test
    public void testEmptyNewReservationId() {
        String id = "id";

        StateLocation.transitionAddsReservation( _transition, id, 0);

        triggerWatcher();

        assertEquals( "Check number of purges", 0, _update.countPurges());
        assertEquals( "Check number of updates", 0, _update.count());
    }


    /**
     * State is empty.  We add a complete reservation.
     */
    @Test
    public void testEmptyNewReservation() {
        String id = "id";
        String vo = "atlas";
        String group = "/" + vo;
        String role = "";
        String FQAN = group;
        String description = "MCDISK";

        long total = 10;
        long used = 2;
        long allocated = 1;
        long free = 7;

        StateLocation.transitionAddsReservation( _transition, id, 0);
        StateLocation.transitionAddsReservationDescription( _transition, 2, id, description);
        StateLocation.transitionAddsReservationAuth( _transition, 2, id, FQAN, group, role);
        StateLocation.transitionAddsReservationSpace( _transition, 2, id, total, used, allocated, free);
        StateLocation.transitionAddsReservationState( _transition, 2, id, ReservationInfo.State.RESERVED);

        triggerWatcher();

        assertEquals( "Check number of purges", 0, _update.countPurges());

        StatePath expectedSummary = SUMMARY_RESERVATIONS_BY_VO.newChild( vo).newChild( "by-description").newChild( description);

        assertStringMetric( "vo metric", expectedSummary.newChild("vo"), vo);

        StatePath expectedSummarySpace = expectedSummary.newChild( "space");

        assertIntegerMetric( "total metric", expectedSummarySpace.newChild("total"), total);
        assertIntegerMetric( "used metric", expectedSummarySpace.newChild("used"), used);
        assertIntegerMetric( "free metric", expectedSummarySpace.newChild("free"), free);
        assertIntegerMetric( "allocated metric", expectedSummarySpace.newChild("allocated"), allocated);

        Set<String> ids = new HashSet<>();
        ids.add( id);
        assertList( "reservations list", expectedSummary.newChild( "reservations"), ids);
    }

    @Test
    public void testReservationTransitionUpdatesVo() {
        String id = "id";

        String oldVo = "atlas";
        String oldGroup = "/" + oldVo;
        String oldRole = "";
        String oldFQAN = oldGroup;

        String description = "SCRATCH";

        long total = 10;
        long used = 2;
        long allocated = 1;
        long free = 7;

        StateLocation.putReservationAuth( _exhibitor, id, oldFQAN, oldGroup, oldRole);
        StateLocation.putReservationDescription( _exhibitor, id, description);
        StateLocation.putReservationSpace( _exhibitor, id, total, used, allocated, free);
        StateLocation.putReservationState( _exhibitor, id, ReservationInfo.State.RESERVED);

        String newVo = "cms";
        String newGroup = "/" + newVo;
        String newFQAN = newGroup;
        String newRole = "";

        StateLocation.transitionAddsReservationAuth( _transition, 4, id, newFQAN, newGroup, newRole);

        triggerWatcher();

        // SIP should purge the old atlas information
        assertEquals( "Check number of purges", 1, _update.countPurges());

        // Check that new summary is created with the new VO.

        StatePath expectedSummaryBase = SUMMARY_RESERVATIONS_BY_VO.newChild( newVo).newChild( "by-description").newChild( description);

        assertStringMetric( "checking new vo summary", expectedSummaryBase.newChild( "vo"), newVo);

        StatePath expectedSummarySpace = expectedSummaryBase.newChild( "space");

        assertIntegerMetric( "checking total", expectedSummarySpace.newChild( "total"), total);
        assertIntegerMetric( "checking used", expectedSummarySpace.newChild( "used"), used);
        assertIntegerMetric( "checking allocated", expectedSummarySpace.newChild( "allocated"), allocated);
        assertIntegerMetric( "checking free", expectedSummarySpace.newChild( "free"), free);
    }

    @Test
    public void testReservationTransitionUpdatesSpaceMetric() {
        String id = "id";
        String vo = "atlas";
        String group = "/" + vo;
        String role = "";
        String FQAN = group;
        String description = "SCRATCH";

        long oldTotal = 10;
        long newTotal = 20;
        long used = 2;
        long allocated = 1;
        long free = 7;

        StateLocation.putReservationAuth( _exhibitor, id, FQAN, group, role);
        StateLocation.putReservationDescription( _exhibitor, id, description);
        StateLocation.putReservationSpace( _exhibitor, id, oldTotal, used, allocated, free);
        StateLocation.putReservationState( _exhibitor, id, ReservationInfo.State.RESERVED);

        StateLocation.transitionAddsReservationSpaceTotal( _transition, 4, id, newTotal);

        triggerWatcher();

        assertEquals( "Check number of purges", 0, _update.countPurges());

        StatePath expectedSummarySpace = SUMMARY_RESERVATIONS_BY_VO.newChild( vo).newChild( "by-description").newChild( description).newChild( "space");

        assertIntegerMetric( "total metric", expectedSummarySpace.newChild("total"), newTotal);
    }

    @Test
    public void testReservationTransitionAddsReservationWithSameDescription() {
        /* Common info */
        String resvDesc = "MCDISK";
        String vo = "atlas";
        String group = "/" + vo;
        String role = "";
        String FQAN = group;

        String resv1Id = "id-1";
        int resv1Total = 10;
        int resv1Used = 1;
        int resv1Allocated = 1;
        int resv1Free = 8;

        String resv2Id = "id-2";
        int resv2Total = 20;
        int resv2Used = 2;
        int resv2Allocated = 3;
        int resv2Free = 15;

        StateLocation.putReservationDescription( _exhibitor, resv1Id, resvDesc);
        StateLocation.putReservationAuth( _exhibitor, resv1Id, FQAN, group, role);
        StateLocation.putReservationSpace( _exhibitor, resv1Id, resv1Total, resv1Used, resv1Allocated, resv1Free);
        StateLocation.putReservationState( _exhibitor, resv1Id, ReservationInfo.State.RESERVED);

        StateLocation.transitionAddsReservation( _transition, resv2Id, 1);
        StateLocation.transitionAddsReservationDescription( _transition, 2, resv2Id, resvDesc);
        StateLocation.transitionAddsReservationAuth( _transition, 2, resv2Id, FQAN, group, role);
        StateLocation.transitionAddsReservationSpace( _transition, 2, resv2Id, resv2Total, resv2Used,
                                                      resv2Allocated, resv2Free);
        StateLocation.transitionAddsReservationState( _transition, 2, resv2Id, ReservationInfo.State.RESERVED);

        triggerWatcher();

        assertEquals( "Check number of purges", 0, _update.countPurges());

        StatePath expectedSummary = SUMMARY_RESERVATIONS_BY_VO.newChild( vo).newChild( "by-description").newChild( resvDesc);

        StatePath expectedSummarySpace = expectedSummary.newChild( "space");

        assertIntegerMetric( "total metric", expectedSummarySpace.newChild("total"), resv1Total + resv2Total);
        assertIntegerMetric( "used metric", expectedSummarySpace.newChild("used"), resv1Used + resv2Used);
        assertIntegerMetric( "free metric", expectedSummarySpace.newChild("free"), resv1Free + resv2Free);
        assertIntegerMetric( "allocated metric", expectedSummarySpace.newChild("allocated"), resv1Allocated + resv2Allocated);

        Set<String> ids = new HashSet<>();
        ids.add( resv2Id);
        assertList( "reservations list", expectedSummary.newChild( "reservations"), ids);
    }


    @Test
    public void testReservationTransitionAddsReservationWithDifferentDescription() {
        /* Common info */
        String vo = "atlas";
        String group = "/" + vo;
        String role = "";
        String FQAN = group;

        String resv1Desc = "MCDISK";
        String resv1Id = "id-1";
        int resv1Total = 10;
        int resv1Used = 1;
        int resv1Allocated = 1;
        int resv1Free = 8;

        String resv2Desc = "SCRATCH";
        String resv2Id = "id-2";
        int resv2Total = 20;
        int resv2Used = 2;
        int resv2Allocated = 3;
        int resv2Free = 15;

        StateLocation.putReservationDescription( _exhibitor, resv1Id, resv1Desc);
        StateLocation.putReservationAuth( _exhibitor, resv1Id, FQAN, group, role);
        StateLocation.putReservationSpace( _exhibitor, resv1Id, resv1Total, resv1Used, resv1Allocated, resv1Free);
        StateLocation.putReservationState( _exhibitor, resv1Id, ReservationInfo.State.RESERVED);

        StateLocation.transitionAddsReservation( _transition, resv2Id, 1);
        StateLocation.transitionAddsReservationDescription( _transition, 2, resv2Id, resv2Desc);
        StateLocation.transitionAddsReservationAuth( _transition, 2, resv2Id, FQAN, group, role);
        StateLocation.transitionAddsReservationSpace( _transition, 2, resv2Id, resv2Total, resv2Used,
                                                      resv2Allocated, resv2Free);
        StateLocation.transitionAddsReservationState( _transition, 2, resv2Id, ReservationInfo.State.RESERVED);

        triggerWatcher();

        assertEquals( "Check number of purges", 0, _update.countPurges());

        StatePath expectedSummary = SUMMARY_RESERVATIONS_BY_VO.newChild( vo).newChild( "by-description").newChild( resv2Desc);

        StatePath expectedSummarySpace = expectedSummary.newChild( "space");

        assertIntegerMetric( "total metric", expectedSummarySpace.newChild("total"), resv2Total);
        assertIntegerMetric( "used metric", expectedSummarySpace.newChild("used"), resv2Used);
        assertIntegerMetric( "free metric", expectedSummarySpace.newChild("free"), resv2Free);
        assertIntegerMetric( "allocated metric", expectedSummarySpace.newChild("allocated"), resv2Allocated);

        Set<String> ids = new HashSet<>();
        ids.add( resv2Id);
        assertList( "reservations list", expectedSummary.newChild( "reservations"), ids);
    }


    @Test
    public void testReservationTransitionAddsReservationWithSameDescriptionDifferentVO() {
        /* Common info */
        String resvDesc = "MCDISK";
        String role = "";

        String resv1Id = "id-1";
        String resv1Vo = "atlas";
        String resv1Group = "/" + resv1Vo;
        String resv1FQAN = resv1Group;
        int resv1Total = 10;
        int resv1Used = 1;
        int resv1Allocated = 1;
        int resv1Free = 8;

        String resv2Id = "id-2";
        String resv2Vo = "cms";
        String resv2Group = "/" + resv2Vo;
        String resv2FQAN = resv2Group;
        int resv2Total = 20;
        int resv2Used = 2;
        int resv2Allocated = 3;
        int resv2Free = 15;

        StateLocation.putReservationDescription( _exhibitor, resv1Id, resvDesc);
        StateLocation.putReservationAuth( _exhibitor, resv1Id, resv1FQAN, resv1Group, role);
        StateLocation.putReservationSpace( _exhibitor, resv1Id, resv1Total, resv1Used, resv1Allocated, resv1Free);
        StateLocation.putReservationState( _exhibitor, resv1Id, ReservationInfo.State.RESERVED);

        StateLocation.transitionAddsReservation( _transition, resv2Id, 1);
        StateLocation.transitionAddsReservationDescription( _transition, 2, resv2Id, resvDesc);
        StateLocation.transitionAddsReservationAuth( _transition, 2, resv2Id, resv2FQAN, resv2Group, role);
        StateLocation.transitionAddsReservationSpace( _transition, 2, resv2Id, resv2Total, resv2Used,
                                                      resv2Allocated, resv2Free);
        StateLocation.transitionAddsReservationState( _transition, 2, resv2Id, ReservationInfo.State.RESERVED);

        triggerWatcher();

        assertEquals( "Check number of purges", 0, _update.countPurges());

        StatePath expectedSummary = SUMMARY_RESERVATIONS_BY_VO.newChild( resv2Vo).newChild( "by-description").newChild( resvDesc);

        StatePath expectedSummarySpace = expectedSummary.newChild( "space");

        assertIntegerMetric( "total metric", expectedSummarySpace.newChild("total"), resv2Total);
        assertIntegerMetric( "used metric", expectedSummarySpace.newChild("used"), resv2Used);
        assertIntegerMetric( "free metric", expectedSummarySpace.newChild("free"), resv2Free);
        assertIntegerMetric( "allocated metric", expectedSummarySpace.newChild("allocated"), resv2Allocated);

        Set<String> ids = new HashSet<>();
        ids.add( resv2Id);
        assertList( "reservations list", expectedSummary.newChild( "reservations"), ids);
    }

    @Test
    public void testTwoReservationsSameVoTransitionRemovesReservation() {
        /* Common info */
        String role = "";
        String vo = "atlas";
        String group = "/" + vo;
        String FQAN = group;

        String resv1Desc = "MCDISK";
        String resv1Id = "id-1";
        int resv1Total = 10;
        int resv1Used = 1;
        int resv1Allocated = 1;
        int resv1Free = 8;

        String resv2Desc = "SCRATCH";
        String resv2Id = "id-2";
        int resv2Total = 20;
        int resv2Used = 2;
        int resv2Allocated = 3;
        int resv2Free = 15;

        StateLocation.putReservationDescription( _exhibitor, resv1Id, resv1Desc);
        StateLocation.putReservationAuth( _exhibitor, resv1Id, FQAN, group, role);
        StateLocation.putReservationSpace( _exhibitor, resv1Id, resv1Total, resv1Used, resv1Allocated, resv1Free);
        StateLocation.putReservationState( _exhibitor, resv1Id, ReservationInfo.State.RESERVED);

        StateLocation.putReservationDescription( _exhibitor, resv2Id, resv2Desc);
        StateLocation.putReservationAuth( _exhibitor, resv2Id, FQAN, group, role);
        StateLocation.putReservationSpace( _exhibitor, resv2Id, resv2Total, resv2Used, resv2Allocated, resv2Free);
        StateLocation.putReservationState( _exhibitor, resv2Id, ReservationInfo.State.RESERVED);

        StateLocation.transitionRemovesReservation( _transition, resv2Id);

        triggerWatcher();

        StatePath expectedSummary = SUMMARY_RESERVATIONS_BY_VO.newChild( vo);

        assertPurge( "check all of reservation-2 is removed", expectedSummary.newChild( "by-description").newChild( resv2Desc));
    }

    @Test
    public void testTwoReservationsSameVoAndDescriptionTransitionRemovesReservation() {
        /* Common info */
        String resvDesc = "MCDISK";
        String role = "";
        String vo = "atlas";
        String group = "/" + vo;
        String FQAN = group;

        String resv1Id = "id-1";
        int resv1Total = 10;
        int resv1Used = 1;
        int resv1Allocated = 1;
        int resv1Free = 8;

        String resv2Id = "id-2";
        int resv2Total = 20;
        int resv2Used = 2;
        int resv2Allocated = 3;
        int resv2Free = 15;

        StateLocation.putReservationDescription( _exhibitor, resv1Id, resvDesc);
        StateLocation.putReservationAuth( _exhibitor, resv1Id, FQAN, group, role);
        StateLocation.putReservationSpace( _exhibitor, resv1Id, resv1Total, resv1Used, resv1Allocated, resv1Free);
        StateLocation.putReservationState( _exhibitor, resv1Id, ReservationInfo.State.RESERVED);

        StateLocation.putReservationDescription( _exhibitor, resv2Id, resvDesc);
        StateLocation.putReservationAuth( _exhibitor, resv2Id, FQAN, group, role);
        StateLocation.putReservationSpace( _exhibitor, resv2Id, resv2Total, resv2Used, resv2Allocated, resv2Free);
        StateLocation.putReservationState( _exhibitor, resv2Id, ReservationInfo.State.RESERVED);

        StateLocation.transitionRemovesReservation( _transition, resv2Id);

        triggerWatcher();

        StatePath expectedSummary = SUMMARY_RESERVATIONS_BY_VO.newChild( vo).newChild( "by-description").newChild( resvDesc);

        StatePath expectedReservations = expectedSummary.newChild( "reservations");

        assertPurge( "check reservation 2 is removed", expectedReservations.newChild( resv2Id));

        /* Should update to reflect new space info */
        StatePath expectedSummarySpace = expectedSummary.newChild( "space");
        assertIntegerMetric( "total metric", expectedSummarySpace.newChild("total"), resv1Total);
        assertIntegerMetric( "used metric", expectedSummarySpace.newChild("used"), resv1Used);
        assertIntegerMetric( "free metric", expectedSummarySpace.newChild("free"), resv1Free);
        assertIntegerMetric( "allocated metric", expectedSummarySpace.newChild("allocated"), resv1Allocated);
    }


    @Test
    public void testTwoReservationsTransitionRemovesReservation() {
        /* Common info */
        String role = "";

        String resv1Desc = "MCDISK";
        String resv1Id = "id-1";
        String resv1Vo = "atlas";
        String resv1Group = "/" + resv1Vo;
        String resv1FQAN = resv1Group;
        int resv1Total = 10;
        int resv1Used = 1;
        int resv1Allocated = 1;
        int resv1Free = 8;

        String resv2Desc = "SCRATCH";
        String resv2Id = "id-2";
        String resv2Vo = "cms";
        String resv2Group = "/" + resv2Vo;
        String resv2FQAN = resv2Group;
        int resv2Total = 20;
        int resv2Used = 2;
        int resv2Allocated = 3;
        int resv2Free = 15;

        StateLocation.putReservationDescription( _exhibitor, resv1Id, resv1Desc);
        StateLocation.putReservationAuth( _exhibitor, resv1Id, resv1FQAN, resv1Group, role);
        StateLocation.putReservationSpace( _exhibitor, resv1Id, resv1Total, resv1Used, resv1Allocated, resv1Free);
        StateLocation.putReservationState( _exhibitor, resv1Id, ReservationInfo.State.RESERVED);

        StateLocation.putReservationDescription( _exhibitor, resv2Id, resv2Desc);
        StateLocation.putReservationAuth( _exhibitor, resv2Id, resv2FQAN, resv2Group, role);
        StateLocation.putReservationSpace( _exhibitor, resv2Id, resv2Total, resv2Used, resv2Allocated, resv2Free);
        StateLocation.putReservationState( _exhibitor, resv2Id, ReservationInfo.State.RESERVED);

        StateLocation.transitionRemovesReservation( _transition, resv2Id);

        triggerWatcher();

        StatePath vo2BasePath = SUMMARY_RESERVATIONS_BY_VO.newChild( resv2Vo);

        assertPurge( "check all of VO-2 is removed", vo2BasePath);
    }



    private void assertStringMetric( String msg, StatePath path, String value) {
        assertTrue( msg, _update.hasUpdate( path, new StringStateValue(value)));
    }

    private void assertIntegerMetric( String msg, StatePath path, long value) {
        assertTrue( msg, _update.hasUpdate( path, new IntegerStateValue( value)));
    }

    private void assertList( String msg, StatePath path, Set<String> items) {
        for( String item : items) {
            assertTrue(msg, _update
                    .hasUpdate(path.newChild(item), new StateComposite(true)));
        }
    }

    private void assertPurge( String msg, StatePath path)  {
        assertTrue( msg + " [" + path.toString() + "]", _update.hasPurge( path));
    }

    private void triggerWatcher()
    {
        StateExhibitor futureState = new PostTransitionStateExhibitor( _exhibitor, _transition);
        _watcher.trigger( _update, _exhibitor, futureState);
    }
}
