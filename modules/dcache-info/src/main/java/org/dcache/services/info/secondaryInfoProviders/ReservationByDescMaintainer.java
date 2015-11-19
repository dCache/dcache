package org.dcache.services.info.secondaryInfoProviders;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.dcache.services.info.base.IntegerStateValue;
import org.dcache.services.info.base.StateComposite;
import org.dcache.services.info.base.StateExhibitor;
import org.dcache.services.info.base.StatePath;
import org.dcache.services.info.base.StateUpdate;
import org.dcache.services.info.base.StringStateValue;
import org.dcache.services.info.stateInfo.ReservationInfo;
import org.dcache.services.info.stateInfo.ReservationInfoVisitor;

/**
 * The ReservationByDescMaintainer class implements StateWatcher. It
 * maintains an aggregation of SRM reservations by (description, VO) ordered
 * pairs: the space statics of SRM reservations that have the same
 * reservation description are aggregated.
 */
public class ReservationByDescMaintainer extends AbstractStateWatcher
{
    private static final Logger LOGGER =
            LoggerFactory.getLogger(ReservationByDescMaintainer.class);

    public static final String PATH_ELEMENT_BY_DESCRIPTION_BRANCH =
            "by-description";

    public static final String PATH_ELEMENT_VO_NAME_METRIC = "vo";

    public static final String PATH_ELEMENT_SPACE_BRANCH = "space";

    public static final String PATH_ELEMENT_TOTAL_METRIC = "total";
    public static final String PATH_ELEMENT_ALLOCATED_METRIC = "allocated";
    public static final String PATH_ELEMENT_USED_METRIC = "used";
    public static final String PATH_ELEMENT_FREE_METRIC = "free";

    public static final String PATH_ELEMENT_RESERVATIONS_BRANCH =
            "reservations";

    public static final StatePath RESERVATIONS_BASE_PATH =
            StatePath.parsePath("summary.reservations.by-VO");

    private static final String PREDICATE_PATHS[] =
            { "reservations.*",
              "reservations.*.space.*",
              "reservations.*.description",
              "reservations.*.state",
              "reservations.*.authorisation.group"};

    /**
     * Information aggregated over all reservations with the same description
     * for the same VO.
     */
    private class ReservationSummaryInfo
    {
        private long _total;
        private long _free;
        private long _allocated;
        private long _used;
        private final Set<String> _ids = new HashSet<>();

        @Override
        public int hashCode()
        {
            return (int) _total ^ (int) _free ^ (int) _allocated ^ (int) _used ^
                   _ids.hashCode();
        }

        @Override
        public boolean equals(Object other)
        {
            if (this == other) {
                return true;
            }

            if (!(other instanceof ReservationSummaryInfo)) {
                return false;
            }

            ReservationSummaryInfo otherSummary =
                    (ReservationSummaryInfo) other;

            if (otherSummary._total != _total || otherSummary._free != _free ||
                otherSummary._allocated != _allocated ||
                otherSummary._used != _used) {
                return false;
            }

            return otherSummary._ids.equals(_ids);
        }

        /**
         * Add a SRM reservation to this Reservation summary.
         *
         * @param reservationId the ID of this reservation.
         * @param info the information about this reservation.
         */
        public void addReservationInfo(String reservationId, ReservationInfo info)
        {
            if (info.hasTotal()) {
                _total += info.getTotal();
            }
            if (info.hasUsed()) {
                _used += info.getUsed();
            }
            if (info.hasFree()) {
                _free += info.getFree();
            }
            if (info.hasAllocated()) {
                _allocated += info.getAllocated();
            }
            _ids.add(reservationId);
        }

        public boolean hasId(String ID)
        {
            return _ids.contains(ID);
        }

        public long getTotal()
        {
            return _total;
        }

        public long getUsed()
        {
            return _used;
        }

        public long getFree()
        {
            return _free;
        }

        public long getAllocated()
        {
            return _allocated;
        }

        public boolean totalNeedsUpdating(ReservationSummaryInfo oldInfo)
        {
            return oldInfo == null || oldInfo.getTotal() != getTotal();
        }

        public boolean usedNeedsUpdating(ReservationSummaryInfo oldInfo)
        {
            return oldInfo == null || oldInfo.getUsed() != getUsed();
        }

        public boolean freeNeedsUpdating(ReservationSummaryInfo oldInfo)
        {
            return oldInfo == null || oldInfo.getFree() != getFree();
        }

        public boolean allocatedNeedsUpdating(ReservationSummaryInfo oldInfo)
        {
            return oldInfo == null || oldInfo.getAllocated() != getAllocated();
        }

        /**
         * Purge all IDs that are not present in this ReservationSummaryInfo
         * but are missing from a new version of ReservationSummaryInfo
         *
         * @param update the StateUpdate to add adjust.
         * @param basePath the base path of the IDs
         * @param newInfo the new version of this ReservationSummaryInfo
         */
        public void purgeMissingIds(StateUpdate update, StatePath basePath,
                ReservationSummaryInfo newInfo)
        {
            for (String id : _ids) {
                if (!newInfo.hasId(id)) {
                    update.purgeUnder(basePath.newChild(id));
                }
            }
        }

        /**
         * Update the provided StateUpdate object so that later processing
         * this StateUpdate will alter dCache state such that it reflects the
         * information held in this object.
         *
         * @param update the StateUpdate to use
         * @param voName the name of the VO
         * @param basePath the StatePath under which metrics will be added
         * @param oldInfo the previous ReservationSummaryInfo or null if this
         *            description is new.
         */
        public void updateMetrics(StateUpdate update, String voName,
                StatePath basePath, ReservationSummaryInfo oldInfo)
        {
            StatePath spacePath = basePath.newChild(PATH_ELEMENT_SPACE_BRANCH);

            if (totalNeedsUpdating(oldInfo)) {
                update.appendUpdate(spacePath
                        .newChild(PATH_ELEMENT_TOTAL_METRIC), new IntegerStateValue(getTotal(), true));
            }

            if (freeNeedsUpdating(oldInfo)) {
                update.appendUpdate(spacePath
                        .newChild(PATH_ELEMENT_FREE_METRIC), new IntegerStateValue(getFree(), true));
            }

            if (allocatedNeedsUpdating(oldInfo)) {
                update.appendUpdate(spacePath
                        .newChild(PATH_ELEMENT_ALLOCATED_METRIC), new IntegerStateValue(getAllocated(), true));
            }

            if (usedNeedsUpdating(oldInfo)) {
                update.appendUpdate(spacePath
                        .newChild(PATH_ELEMENT_USED_METRIC), new IntegerStateValue(getUsed(), true));
            }

            StatePath resvPath =
                    basePath.newChild(PATH_ELEMENT_RESERVATIONS_BRANCH);

            /* Activity if there was an existing summary information */
            if (oldInfo != null) {
                oldInfo.purgeMissingIds(update, resvPath, this);

                /* Add those entries that are new */
                for (String newId : _ids) {
                    if (!oldInfo.hasId(newId)) {
                        update.appendUpdate(resvPath
                                .newChild(newId), new StateComposite(true));
                    }
                }
            } else {
                /* Add the VO name (we need to this only once) */
                update.appendUpdate(basePath.newChild(PATH_ELEMENT_VO_NAME_METRIC), new StringStateValue(voName, true));

                /* Add all IDs */
                for (String id : _ids) {
                    update.appendUpdate(resvPath
                            .newChild(id), new StateComposite(true));
                }
            }
        }
    }

    @Override
    protected String[] getPredicates()
    {
        return PREDICATE_PATHS;
    }

    @Override
    public void trigger(StateUpdate update, StateExhibitor currentState, StateExhibitor futureState)
    {
        Map<String, ReservationInfo> currentResv =
                ReservationInfoVisitor.getDetails(currentState);
        Map<String, ReservationInfo> futureResv =
                ReservationInfoVisitor.getDetails(futureState);

        // build mapping from description to summary data
        Map<String, Map<String, ReservationSummaryInfo>> currentSummary =
                buildSummaryInfo(currentResv);
        Map<String, Map<String, ReservationSummaryInfo>> futureSummary =
                buildSummaryInfo(futureResv);

        purgeMissingSummaries(update, currentSummary, futureSummary);
        addMetrics(update, currentSummary, futureSummary);
    }

    /**
     * Build a Map between VO name and a collection of reservation summary
     * information. This collection of reservation summary information is
     * itself a Map between the reservation description and a summary of all
     * reservations with that same description. Those reservations without a
     * description are ignored.
     *
     * @param reservations a Map between reservation ID and a corresponding
     *            ReservationInfo object describing that reservation.
     * @return a Map of VO to reservation-description summaries.
     */
    private Map<String, Map<String, ReservationSummaryInfo>> buildSummaryInfo(Map<String, ReservationInfo> reservations)
    {
        Map<String, Map<String, ReservationSummaryInfo>> summary = new HashMap<>();

        for (Map.Entry<String, ReservationInfo> entry : reservations.entrySet()) {
            String reservationId = entry.getKey();
            ReservationInfo info = entry.getValue();

            /*
             * Ignore those reservations that don't have a state or that
             * state is a final one
             */
            if (!info.hasState() || info.getState().isFinalState()) {
                LOGGER.trace("ignoring reservation {} as state is undefined or final",
                        reservationId);
                continue;
            }

            /* Skip those reservations that don't have a description */
            if (!info.hasDescription() || info.getDescription().isEmpty()) {
                LOGGER.trace("ignoring reservation {} as description is undefined or empty",
                        reservationId);
                continue;
            }

            /* Skip all those reservations that don't have a well-defined VO */
            if (!info.hasVo() || info.getVo().isEmpty()) {
                LOGGER.trace("ignoring reservation {} as VO is undefined or empty",
                        reservationId);
                continue;
            }

            String voName = info.getVo();

            ReservationSummaryInfo thisSummary;

            Map<String, ReservationSummaryInfo> thisVoSummary;

            thisVoSummary = summary.get(voName);

            if (thisVoSummary == null) {
                thisVoSummary = new HashMap<>();
                summary.put(voName, thisVoSummary);
            }

            thisSummary = thisVoSummary.get(info.getDescription());

            if (thisSummary == null) {
                thisSummary = new ReservationSummaryInfo();
                thisVoSummary.put(info.getDescription(), thisSummary);
            }

            // update summary with this reservation.
            thisSummary.addReservationInfo(reservationId, info);
        }

        return summary;
    }

    /**
     * Adjust the StateUpdate object so that those descriptions that have
     * disappeared are purged.
     *
     * @param update
     * @param currentDescriptions
     * @param futureDescriptions
     */
    private void purgeMissingSummaries(StateUpdate update,
            Map<String, Map<String, ReservationSummaryInfo>> currentVoInfo,
            Map<String, Map<String, ReservationSummaryInfo>> futureVoInfo)
    {
        for (Map.Entry<String, Map<String, ReservationSummaryInfo>> voEntry : currentVoInfo.entrySet()) {
            String voName = voEntry.getKey();
            Set<String> currentDescriptions = voEntry.getValue().keySet();

            StatePath voBasePath = buildVoPath(voName);

            Map<String, ReservationSummaryInfo> futureDescriptions =
                    futureVoInfo.get(voName);

            // If this VO is gone completely, purge everything and move on.
            if (futureDescriptions == null) {
                update.purgeUnder(voBasePath);
                continue;
            }

            // Otherwise, purge those descriptions that have gone.
            for (String thisDescription : currentDescriptions) {
                if (!futureDescriptions.containsKey(thisDescription)) {
                    update.purgeUnder(buildDescriptionPath(voBasePath, thisDescription));
                }
            }
        }
    }

    /**
     * Add metrics that update dCache state to reflect the changes. We assume
     * those elements that should be removed have been purged
     *
     * @param update
     * @param currentSummary
     * @param futureSummary
     */
    private void addMetrics (StateUpdate update,
            Map<String, Map<String, ReservationSummaryInfo>> currentSummary,
            Map<String, Map<String, ReservationSummaryInfo>> futureSummary)
    {
        for (Map.Entry<String, Map<String, ReservationSummaryInfo>> voEntry : futureSummary.entrySet()) {
            String voName = voEntry.getKey();
            LOGGER.trace("Checking vo {}", voName);

            Map<String, ReservationSummaryInfo> futureDescriptions =
                    voEntry.getValue();
            Map<String, ReservationSummaryInfo> currentDescriptions =
                    currentSummary.get(voName);

            StatePath voPath = buildVoPath(voName);

            /* Scan through descriptions after transition is applied */
            for (Map.Entry<String, ReservationSummaryInfo> entry : futureDescriptions.entrySet()) {
                String description = entry.getKey();
                ReservationSummaryInfo futureInfo = entry.getValue();

                /*
                 * Try to establish the corresponding current
                 * ReservationSummaryInfo for this description
                 */
                ReservationSummaryInfo currentInfo =
                        currentDescriptions != null
                                ? currentDescriptions.get(description) : null;

                if (!futureInfo.equals(currentInfo)) {
                    futureInfo
                            .updateMetrics(update, voName, buildDescriptionPath(voPath, description), currentInfo);
                }
            }
        }
    }

    /**
     * Build a StatePath for a given VO to the summary of that VO's SRM
     * reservations; for example,
     * <code>summary.reservations.by-vo.atlas</code>
     *
     * @param voName The name of the VO
     * @return a StatePath pointing to the "by-description" part of this VO's
     *         reservation summary.
     */
    private static StatePath buildVoPath(String voName)
    {
        if (voName == null) {
            throw new IllegalArgumentException("voName is null");
        }

        return RESERVATIONS_BASE_PATH.newChild(voName);
    }

    /**
     * Build a path to a description's summary information based on a
     * voBasePath (something like
     * <code>summary.reservations.by-vo.atlas</code>. The returned path is
     * something like:
     * <code>summary.reservations.by-vo.atlas.by-description.MCDISK</code>
     *
     * @param voBasePath
     * @param description
     * @return
     */
    private static StatePath buildDescriptionPath(StatePath voBasePath, String description)
    {
        return voBasePath.newChild(PATH_ELEMENT_BY_DESCRIPTION_BRANCH).newChild(description);
    }
}
