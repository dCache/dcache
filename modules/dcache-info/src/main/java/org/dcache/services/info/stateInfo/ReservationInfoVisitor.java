package org.dcache.services.info.stateInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.dcache.services.info.base.IntegerStateValue;
import org.dcache.services.info.base.StateExhibitor;
import org.dcache.services.info.base.StatePath;
import org.dcache.services.info.base.StringStateValue;

/**
 * The ReservationInfoVisitor class is a StateVisitor that builds up
 * information about SRM reservations by visiting some StateExhibitor. The
 * result of visiting dCache state is a Map between the reservation ID and a
 * corresponding ReservationInfo object describing the SRM reservation.
 */
public class ReservationInfoVisitor extends SkeletonListVisitor {

    private static Logger _log =
            LoggerFactory.getLogger(ReservationInfoVisitor.class);

    public static final String PATH_ELEMENT_SPACE = "space";
    public static final String PATH_ELEMENT_LIFETIME = "lifetime";
    public static final String PATH_ELEMENT_TOTAL = "total";
    public static final String PATH_ELEMENT_FREE = "free";
    public static final String PATH_ELEMENT_ALLOCATED = "allocated";
    public static final String PATH_ELEMENT_USED = "used";
    public static final String PATH_ELEMENT_AL = "access-latency";
    public static final String PATH_ELEMENT_RP = "retention-policy";
    public static final String PATH_ELEMENT_STATE = "state";
    public static final String PATH_ELEMENT_DESCRIPTION = "description";
    public static final String PATH_ELEMENT_AUTHORISATION = "authorisation";
    public static final String PATH_ELEMENT_GROUP = "group";

    public static final StatePath RESERVATIONS_PATH =
            StatePath.parsePath("reservations");

    /**
     * Match Strings like "/vo", "/vo/group" or "vo" allowing the extraction
     * of "vo" in all cases as group 1 of the resulting pattern.
     */
    public static final Pattern VO_EXTRACTOR_PATTERN =
            Pattern.compile("^/?([^/]*).*");

    /**
     * Obtain information about the current reservations in dCache
     *
     * @return a Mapping between a reservation's ID and a corresponding
     *         ReservationInfo object.
     */
    public static Map<String, ReservationInfo> getDetails(
            StateExhibitor exhibitor) {
        _log.debug("Gathering reservation information.");

        ReservationInfoVisitor visitor = new ReservationInfoVisitor();
        exhibitor.visitState(visitor);

        return visitor.getReservations();
    }

    private final Map<String, ReservationInfo> _reservations =
            new HashMap<>();

    /*
     * Per-item state
     */
    private ReservationInfo _thisResv;
    private StatePath _thisResvPath;
    private StatePath _thisResvSpacePath;
    private StatePath _thisResvAuthPath;

    public ReservationInfoVisitor() {
        super(RESERVATIONS_PATH);
    }

    /**
     * Provide an unmodifiable view of the result of the visitor.
     *
     * @return a Map between a reservation's ID and a corresponding
     *         ReservationInfo object.
     */
    public Map<String, ReservationInfo> getReservations() {
        return Collections.unmodifiableMap(_reservations);
    }

    @Override
    protected void newListItem(String listItemName) {
        super.newListItem(listItemName);

        _thisResv = new ReservationInfo(listItemName);

        _reservations.put(listItemName, _thisResv);

        /**
         * Build up the various StatePaths where we expect data to appear for
         * this reservation.
         */
        _thisResvPath = RESERVATIONS_PATH.newChild(listItemName);
        _thisResvSpacePath = _thisResvPath.newChild(PATH_ELEMENT_SPACE);
        _thisResvAuthPath = _thisResvPath.newChild(PATH_ELEMENT_AUTHORISATION);
    }

    /**
     * Called when an integer metric is encountered.
     */
    @Override
    public void visitInteger(StatePath path, IntegerStateValue value) {
        if (!isInListItem()) {
            return;
        }

        String metricName = path.getLastElement();

        if (_thisResvPath.isParentOf(path)) {
            if (metricName.equals(PATH_ELEMENT_LIFETIME)) {
                _thisResv.setLifetime(value.getValue());
            }

            return;
        }

        if (_thisResvSpacePath.isParentOf(path)) {
            switch (metricName) {
            case PATH_ELEMENT_TOTAL:
                _thisResv.setTotal(value.getValue());
                break;
            case PATH_ELEMENT_FREE:
                _thisResv.setFree(value.getValue());
                break;
            case PATH_ELEMENT_ALLOCATED:
                _thisResv.setAllocated(value.getValue());
                break;
            case PATH_ELEMENT_USED:
                _thisResv.setUsed(value.getValue());
                break;
            default:
                _log.warn("Seen unexpected reservation metric at path " + path);
                break;
            }
        }
    }

    @Override
    public void visitString(StatePath path, StringStateValue value) {
        if (!isInListItem()) {
            return;
        }

        if (!_thisResvPath.isParentOf(path) &&
            !_thisResvSpacePath.isParentOf(path) &&
            !_thisResvAuthPath.isParentOf(path)) {
            return;
        }

        String metricName = path.getLastElement();

        if (metricName.equals(PATH_ELEMENT_AL)) {
            ReservationInfo.AccessLatency al =
                    ReservationInfo.AccessLatency.parseMetricValue(value.toString());
            if (al != null) {
                _thisResv.setAccessLatency(al);
            } else {
                _log.error("Unknown access-latency value " + value.toString());
            }
            return;
        }

        if (metricName.equals(PATH_ELEMENT_RP)) {
            ReservationInfo.RetentionPolicy rp =
                    ReservationInfo.RetentionPolicy.parseMetricValue(value.toString());
            if (rp != null) {
                _thisResv.setRetentionPolicy(rp);
            } else {
                _log.error("Unknown retention-policy value " +
                        value.toString());
            }
            return;
        }

        if (metricName.equals(PATH_ELEMENT_STATE)) {
            ReservationInfo.State state =
                    ReservationInfo.State.parseMetricValue(value.toString());
            if (state != null) {
                _thisResv.setState(state);
            } else {
                _log.error("Unknown state value " + value.toString());
            }
            return;
        }

        if (metricName.equals(PATH_ELEMENT_DESCRIPTION)) {
            _thisResv.setDescription(value.toString());
            return;
        }

        if (metricName.equals(PATH_ELEMENT_GROUP)) {
            Matcher matcher = VO_EXTRACTOR_PATTERN.matcher(value.toString());
            if (matcher.matches()) {
                String voName = matcher.group(1);
                _thisResv.setVo(voName);
            } else {
                _log.error("authorisation.group doesn't match expected pattern " +
                            value.toString());
            }
        }
    }
}
