package org.dcache.services.info.secondaryInfoProviders;

import org.dcache.services.info.base.IntegerStateValue;
import org.dcache.services.info.base.MalleableStateTransition;
import org.dcache.services.info.base.StateComposite;
import org.dcache.services.info.base.StatePath;
import org.dcache.services.info.base.StateUpdate;
import org.dcache.services.info.base.StateValue;
import org.dcache.services.info.base.StringStateValue;
import org.dcache.services.info.base.TestStateExhibitor;
import org.dcache.services.info.stateInfo.LinkInfo;
import org.dcache.services.info.stateInfo.ReservationInfo;
import org.dcache.services.info.stateInfo.SpaceInfo;

import static org.junit.Assert.assertTrue;

/**
 * The StateLocation class contains a collection of static methods and fields
 * that describe how dCache uses state.
 * <p>
 * The class includes support for updating TestStateExhibitor (i.e., adding
 * metrics to a fake dCache state) and for updating MalleableStateTransition
 * (i.e., faking addition, updating or removal of metrics).
 */
public class StateLocation {

    static private final StatePath PATH_POOLS = StatePath.parsePath( "pools");
    static private final StatePath PATH_POOLGROUPS =
            StatePath.parsePath( "poolgroups");
    static private final StatePath PATH_LINKS = StatePath.parsePath( "links");
    static private final StatePath PATH_RESERVATIONS =
            StatePath.parsePath( "reservations");

    static private final String PATH_ELEMENT_POOLGROUPS_POOLS = "pools";
    static private final String PATH_ELEMENT_LINKS_POOLS = "pools";
    static private final String PATH_ELEMENT_SPACE = "space";
    static private final String PATH_ELEMENT_LINKS_PREF = "prefs";

    static private final String PATH_ELEMENT_READ = "read";
    static private final String PATH_ELEMENT_WRITE = "write";
    static private final String PATH_ELEMENT_P2P = "p2p";
    static private final String PATH_ELEMENT_CACHE = "cache";

    public static final String PATH_ELEMENT_LINKS_UNITS = "units";

    public static final String PATH_ELEMENT_AUTHORISATION_BRANCH =
            "authorisation";
    public static final String PATH_ELEMENT_FQAN_METRIC = "FQAN";
    public static final String PATH_ELEMENT_GROUP_METRIC = "group";
    public static final String PATH_ELEMENT_ROLE_METRIC = "role";

    public static final String PATH_ELEMENT_RESV_DESCRIPTION_METRIC =
            "description";
    public static final String PATH_ELEMENT_RESV_STATE_METRIC = "state";

    public static final String PATH_ELEMENT_RESV_SPACE_BRANCH = "space";

    public static final String PATH_ELEMENT_RESV_TOTAL_METRIC = "total";
    public static final String PATH_ELEMENT_RESV_USED_METRIC = "used";
    public static final String PATH_ELEMENT_RESV_ALLOCATED_METRIC = "allocated";
    public static final String PATH_ELEMENT_RESV_FREE_METRIC = "free";

    /*
     * SUPPORT FOR MalleableStateTransition CLASS
     */

    static public void transitionAddsPool( MalleableStateTransition transition,
                                           String poolName, int existing) {
        StatePath poolPath = poolPath( poolName);
        transition.updateTransitionForNewBranch( poolPath, existing);
    }

    static public void transitionRemovesPool(
                                              MalleableStateTransition transition,
                                              String poolName) {
        StatePath poolPath = poolPath( poolName);
        transition.updateTransitionForRemovingElement( poolPath);
    }

    static public void transitionAddsPoolgroup(
                                                MalleableStateTransition transition,
                                                String poolgroupName,
                                                int existing) {
        StatePath poolgroupPath = poolgroupPath( poolgroupName);
        transition.updateTransitionForNewBranch( poolgroupPath, existing);
    }

    static public void transitionAddsPoolMetrics(
                                                  MalleableStateTransition transition,
                                                  String poolName,
                                                  int existing,
                                                  SpaceInfo spaceInfo) {

        StatePath poolPath = poolPath( poolName).newChild( PATH_ELEMENT_SPACE);

        int existingAfterMetric = existing < 3 ? 3 : 4;

        transition.updateTransitionForNewMetric(
                                                 poolPath.newChild( SpaceInfo.PATH_ELEMENT_TOTAL),
                                                 new IntegerStateValue(
                                                                        spaceInfo.getTotal()),
                                                 existing < 4 ? existing : 4);
        transition.updateTransitionForNewMetric(
                                                 poolPath.newChild( SpaceInfo.PATH_ELEMENT_FREE),
                                                 new IntegerStateValue(
                                                                        spaceInfo.getFree()),
                                                 existingAfterMetric);
        transition.updateTransitionForNewMetric(
                                                 poolPath.newChild( SpaceInfo.PATH_ELEMENT_USED),
                                                 new IntegerStateValue(
                                                                        spaceInfo.getUsed()),
                                                 existingAfterMetric);
        transition.updateTransitionForNewMetric(
                                                 poolPath.newChild( SpaceInfo.PATH_ELEMENT_PRECIOUS),
                                                 new IntegerStateValue(
                                                                        spaceInfo.getPrecious()),
                                                 existingAfterMetric);
        transition.updateTransitionForNewMetric(
                                                 poolPath.newChild( SpaceInfo.PATH_ELEMENT_REMOVABLE),
                                                 new IntegerStateValue(
                                                                        spaceInfo.getRemovable()),
                                                 existingAfterMetric);
    }

    static public void transitionAddsPoolgroupMembership(
                                                          MalleableStateTransition transition,
                                                          String poolgroupName,
                                                          String poolName,
                                                          int existing) {
        StatePath poolMbshipPath = poolgroupPoolPath( poolgroupName, poolName);
        transition.updateTransitionForNewBranch( poolMbshipPath, existing);
    }

    static public void transitionRemovesPoolgroupMembership(
                                                             MalleableStateTransition transition,
                                                             String poolgroupName,
                                                             String poolName) {
        StatePath poolMbshipPath = poolgroupPoolPath( poolgroupName, poolName);
        transition.updateTransitionForRemovingElement( poolMbshipPath);
    }

    static public void transitionAddsLink( MalleableStateTransition transition,
                                           String linkName, int existing) {
        StatePath linkPath = linkPath( linkName);
        transition.updateTransitionForNewBranch( linkPath, existing);
    }

    static public void transitionAddsPoolInLink(
                                                 MalleableStateTransition transition,
                                                 String linkName,
                                                 String poolName, int existing) {
        StatePath poolLinkPath = linkPoolPath( linkName, poolName);
        transition.updateTransitionForNewBranch( poolLinkPath, existing);
    }

    static public void transitionAddsLinkPrefs(
                                                MalleableStateTransition transition,
                                                String linkName, int existing,
                                                long readPref, long writePref,
                                                long p2pPref, long cachePref) {
        StatePath linkPrefPath =
                linkPath( linkName).newChild( PATH_ELEMENT_LINKS_PREF);
        transition.updateTransitionForNewMetric(
                                                 linkPrefPath.newChild( PATH_ELEMENT_READ),
                                                 new IntegerStateValue(
                                                                        readPref),
                                                 existing);
        transition.updateTransitionForNewMetric(
                                                 linkPrefPath.newChild( PATH_ELEMENT_WRITE),
                                                 new IntegerStateValue(
                                                                        writePref),
                                                 existing);
        transition.updateTransitionForNewMetric(
                                                 linkPrefPath.newChild( PATH_ELEMENT_P2P),
                                                 new IntegerStateValue( p2pPref),
                                                 existing);
        transition.updateTransitionForNewMetric(
                                                 linkPrefPath.newChild( PATH_ELEMENT_CACHE),
                                                 new IntegerStateValue(
                                                                        cachePref),
                                                 existing);
    }

    static public void transitionAddsReservation(
                                                  MalleableStateTransition transition,
                                                  String id, int existing) {
        transition.updateTransitionForNewBranch( reservationPath( id), existing);
    }

    static public void transitionAddsReservationAuth(
                                                      MalleableStateTransition transition,
                                                      int existing, String id,
                                                      String FQAN,
                                                      String group, String role) {

        StatePath resvAuthPath =
                reservationPath( id).newChild(
                                               PATH_ELEMENT_AUTHORISATION_BRANCH);

        int existingAfterMetric = existing < 3 ? 3 : 4;
        transition.updateTransitionForNewMetric(
                                                 resvAuthPath.newChild( PATH_ELEMENT_FQAN_METRIC),
                                                 new StringStateValue( FQAN),
                                                 existing < 4 ? existing : 4);
        transition.updateTransitionForNewMetric(
                                                 resvAuthPath.newChild( PATH_ELEMENT_GROUP_METRIC),
                                                 new StringStateValue( group),
                                                 existingAfterMetric);
        transition.updateTransitionForNewMetric(
                                                 resvAuthPath.newChild( PATH_ELEMENT_ROLE_METRIC),
                                                 new StringStateValue( role),
                                                 existingAfterMetric);
    }

    static public void transitionAddsReservationSpace(
                                                       MalleableStateTransition transition,
                                                       int existing, String id,
                                                       long total, long used,
                                                       long allocated, long free) {
        transitionAddsReservationSpaceTotal( transition, existing, id, total);

        int existingAfterMetric = existing < 3 ? 3 : 4;
        transitionAddsReservationSpaceUsed( transition, existingAfterMetric,
                                            id, used);
        transitionAddsReservationSpaceAllocated( transition,
                                                 existingAfterMetric, id,
                                                 allocated);
        transitionAddsReservationSpaceFree( transition, existingAfterMetric,
                                            id, free);
    }

    static public void transitionAddsReservationSpaceTotal(
                                                            MalleableStateTransition transition,
                                                            int existing,
                                                            String id,
                                                            long total) {
        StatePath resvSpacePath =
                reservationPath( id).newChild( PATH_ELEMENT_RESV_SPACE_BRANCH);

        transition.updateTransitionForNewMetric(
                                                 resvSpacePath.newChild( PATH_ELEMENT_RESV_TOTAL_METRIC),
                                                 new IntegerStateValue( total),
                                                 existing);
    }

    static public void transitionAddsReservationSpaceUsed(
                                                           MalleableStateTransition transition,
                                                           int existing,
                                                           String id, long used) {
        StatePath resvSpacePath =
                reservationPath( id).newChild( PATH_ELEMENT_RESV_SPACE_BRANCH);

        transition.updateTransitionForNewMetric(
                                                 resvSpacePath.newChild( PATH_ELEMENT_RESV_USED_METRIC),
                                                 new IntegerStateValue( used),
                                                 existing);
    }

    static public void transitionAddsReservationSpaceAllocated(
                                                                MalleableStateTransition transition,
                                                                int existing,
                                                                String id,
                                                                long allocated) {
        StatePath resvSpacePath =
                reservationPath( id).newChild( PATH_ELEMENT_RESV_SPACE_BRANCH);

        transition.updateTransitionForNewMetric(
                                                 resvSpacePath.newChild( PATH_ELEMENT_RESV_ALLOCATED_METRIC),
                                                 new IntegerStateValue(
                                                                        allocated),
                                                 existing);
    }

    static public void transitionAddsReservationSpaceFree(
                                                           MalleableStateTransition transition,
                                                           int existing,
                                                           String id, long free) {
        StatePath resvSpacePath =
                reservationPath( id).newChild( PATH_ELEMENT_RESV_SPACE_BRANCH);

        transition.updateTransitionForNewMetric(
                                                 resvSpacePath.newChild( PATH_ELEMENT_RESV_FREE_METRIC),
                                                 new IntegerStateValue( free),
                                                 existing);
    }

    static public void transitionAddsReservationState(
                                                       MalleableStateTransition transition,
                                                       int existing,
                                                       String id,
                                                       ReservationInfo.State state) {
        StatePath resvPath = reservationPath( id);

        transition.updateTransitionForNewMetric(
                                                 resvPath.newChild( PATH_ELEMENT_RESV_STATE_METRIC),
                                                 new StringStateValue(
                                                                       state.getMetricValue()),
                                                 existing);
    }

    static public void transitionAddsReservationDescription(
                                                             MalleableStateTransition transition,
                                                             int existing,
                                                             String id,
                                                             String description) {
        StatePath resvPath = reservationPath( id);

        transition.updateTransitionForNewMetric(
                                                 resvPath.newChild( PATH_ELEMENT_RESV_DESCRIPTION_METRIC),
                                                 new StringStateValue(
                                                                       description),
                                                 existing);
    }

    static public void transitionRemovesReservation(
                                                     MalleableStateTransition transition,
                                                     String id) {
        transition.updateTransitionForRemovingElement( reservationPath( id));
    }

    /*
     * SUPPORT FOR TestStateExhibitor CLASS
     */

    /**
     * Add space metrics underneath given path.
     */
    static public void putSpaceMetrics( TestStateExhibitor exhibitor,
                                        StatePath spacePath, SpaceInfo info) {
        exhibitor.addMetric( spacePath.newChild( SpaceInfo.PATH_ELEMENT_TOTAL),
                             new IntegerStateValue( info.getTotal()));
        exhibitor.addMetric( spacePath.newChild( SpaceInfo.PATH_ELEMENT_FREE),
                             new IntegerStateValue( info.getFree()));
        exhibitor.addMetric( spacePath.newChild( SpaceInfo.PATH_ELEMENT_USED),
                             new IntegerStateValue( info.getUsed()));
        exhibitor.addMetric(
                             spacePath.newChild( SpaceInfo.PATH_ELEMENT_REMOVABLE),
                             new IntegerStateValue( info.getRemovable()));
        exhibitor.addMetric(
                             spacePath.newChild( SpaceInfo.PATH_ELEMENT_PRECIOUS),
                             new IntegerStateValue( info.getPrecious()));
    }

    /**
     * Add space metrics for a pool
     */
    static public void putPoolSpaceMetrics( TestStateExhibitor exhibitor,
                                            String poolName, SpaceInfo info) {
        StatePath poolSpacePath =
                poolPath( poolName).newChild( PATH_ELEMENT_SPACE);
        putSpaceMetrics( exhibitor, poolSpacePath, info);
    }

    /**
     * Add the space metrics for a poolgroup.
     */
    static public void putPoolgroupSpaceMetrics( TestStateExhibitor exhibitor,
                                                 String poolgroupName,
                                                 SpaceInfo info) {
        StatePath poolgroupSpacePath =
                poolgroupPath( poolgroupName).newChild( PATH_ELEMENT_SPACE);
        putSpaceMetrics( exhibitor, poolgroupSpacePath, info);
    }

    /**
     * Add a pool.
     */
    static public void putPool( TestStateExhibitor exhibitor, String poolName) {
        exhibitor.addBranch( poolPath( poolName));
    }

    /**
     * Add a pool to a poolgroup
     */
    static public void putPoolInPoolgroup( TestStateExhibitor exhibitor,
                                           String poolgroupName, String poolName) {
        exhibitor.addBranch( poolgroupPoolPath( poolgroupName, poolName));
    }

    static public void putUnitInLink( TestStateExhibitor exhibitor,
                                      String linkName,
                                      LinkInfo.UNIT_TYPE unitType,
                                      String unitName) {
        exhibitor.addBranch( linkUnitPath( linkName, unitType, unitName));
    }

    /**
     * Add a pool to a link
     */
    static public void putPoolInLink( TestStateExhibitor exhibitor,
                                      String linkName, String poolName) {
        exhibitor.addBranch( linkPoolPath( linkName, poolName));
    }

    /**
     * Add a link
     */
    static public void putLink( TestStateExhibitor exhibitor, String linkName) {
        exhibitor.addBranch( linkPath( linkName));
    }

    /**
     * Add space metrics for named link.
     */
    static public void putLinkSpaceMetrics( TestStateExhibitor exhibitor,
                                            String linkName, SpaceInfo info) {
        putSpaceMetrics( exhibitor, linkPath( linkName).newChild( PATH_ELEMENT_SPACE), info);
    }

    /**
     * Add reservation metrics
     */
    static public void putReservationDescription( TestStateExhibitor exhibitor,
                                                  String id, String resvDesc) {
        exhibitor.addMetric(
                             reservationPath( id).newChild(
                                                            PATH_ELEMENT_RESV_DESCRIPTION_METRIC),
                             new StringStateValue( resvDesc));
    }

    static public void putReservationAuth( TestStateExhibitor exhibitor,
                                           String id, String FQAN,
                                           String group, String role) {
        StatePath authPath =
                reservationPath( id).newChild(
                                               PATH_ELEMENT_AUTHORISATION_BRANCH);

        exhibitor.addMetric( authPath.newChild( PATH_ELEMENT_FQAN_METRIC),
                             new StringStateValue( FQAN));
        exhibitor.addMetric( authPath.newChild( PATH_ELEMENT_GROUP_METRIC),
                             new StringStateValue( group));
        exhibitor.addMetric( authPath.newChild( PATH_ELEMENT_ROLE_METRIC),
                             new StringStateValue( role));
    }

    static public void putReservationSpace( TestStateExhibitor exhibitor,
                                            String id, long total, long used,
                                            long allocated, long free) {
        StatePath spacePath =
                reservationPath( id).newChild( PATH_ELEMENT_RESV_SPACE_BRANCH);

        exhibitor.addMetric(
                             spacePath.newChild( PATH_ELEMENT_RESV_TOTAL_METRIC),
                             new IntegerStateValue( total));
        exhibitor.addMetric(
                             spacePath.newChild( PATH_ELEMENT_RESV_FREE_METRIC),
                             new IntegerStateValue( free));
        exhibitor.addMetric(
                             spacePath.newChild( PATH_ELEMENT_RESV_USED_METRIC),
                             new IntegerStateValue( used));
        exhibitor.addMetric(
                             spacePath.newChild( PATH_ELEMENT_RESV_ALLOCATED_METRIC),
                             new IntegerStateValue( allocated));
    }

    static public void putReservationState( TestStateExhibitor exhibitor,
                                            String id,
                                            ReservationInfo.State state) {
        exhibitor.addMetric(
                             reservationPath( id).newChild(
                                                            PATH_ELEMENT_RESV_STATE_METRIC),
                             new StringStateValue( state.getMetricValue()));
    }

    /*
     * Support methods for building a StatePath for key objects
     */

    static public StatePath linkUnitPath( String linkName,
                                          LinkInfo.UNIT_TYPE unitType,
                                          String unitName) {
        return linkPath( linkName).newChild( PATH_ELEMENT_LINKS_UNITS).newChild(
                                                                                 unitType.getPathElement()).newChild(
                                                                                                                      unitName);
    }

    /**
     * The StatePath describing a pool's membership of a link.
     */
    static public StatePath linkPoolPath( String linkName, String poolName) {
        return linkPath( linkName).newChild( PATH_ELEMENT_LINKS_POOLS).newChild(
                                                                                 poolName);
    }

    /**
     * The StatePath describing a pool's membership of a poolgroup.
     */
    static public StatePath poolgroupPoolPath( String poolgroupName,
                                               String poolName) {
        return poolgroupPath( poolgroupName).newChild(
                                                       PATH_ELEMENT_POOLGROUPS_POOLS).newChild(
                                                                                                poolName);
    }

    /**
     * The StatePath of a poolgroup.
     */
    static public StatePath poolgroupPath( String poolgroupName) {
        return PATH_POOLGROUPS.newChild( poolgroupName);
    }

    /**
     * The StatePath of a pool
     */
    static public StatePath poolPath( String poolName) {
        return PATH_POOLS.newChild( poolName);
    }

    /**
     * The StatePath of a link.
     */
    static public StatePath linkPath( String linkName) {
        return PATH_LINKS.newChild( linkName);
    }

    static public StatePath reservationPath( String id) {
        return PATH_RESERVATIONS.newChild( id);
    }

    /*
     * SUPPORT FOR log4j asserts ON StateUpdate OBJECT
     */

    static public void assertUpdatePoolgroupSpaceMetrics( StateUpdate update,
                                                          String poolgroupName,
                                                          SpaceInfo spaceInfo) {
        StatePath spacePath =
                poolgroupPath( poolgroupName).newChild( PATH_ELEMENT_SPACE);
        assertSpaceMetrics( update, spacePath, spaceInfo);
    }

    static public void assertSpaceMetrics( StateUpdate update,
                                           StatePath spacePath,
                                           SpaceInfo spaceInfo) {
        assertIntegerMetric( update, spacePath, "total", spaceInfo.getTotal());
        assertIntegerMetric( update, spacePath, "free", spaceInfo.getFree());
        assertIntegerMetric( update, spacePath, "used", spaceInfo.getUsed());
        assertIntegerMetric( update, spacePath, "precious",
                             spaceInfo.getPrecious());
        assertIntegerMetric( update, spacePath, "removable",
                             spaceInfo.getRemovable());
    }

    static public void assertUpdateHasBranch( String msg, StateUpdate update,
                                              StatePath path) {
        assertTrue( msg, update.hasUpdate( path, new StateComposite()));
    }

    static public void assertIntegerMetric( StateUpdate update,
                                            StatePath spacePath,
                                            String metricName, long metricValue) {
        StateValue metric = new IntegerStateValue( metricValue);
        assertTrue( "expected space." + metricName +
                    " metric is missing or has wrong value",
                    update.hasUpdate( spacePath.newChild( metricName), metric));
    }

}
