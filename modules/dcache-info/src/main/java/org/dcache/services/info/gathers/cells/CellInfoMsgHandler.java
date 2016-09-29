package org.dcache.services.info.gathers.cells;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import dmg.cells.nucleus.CellInfo;
import dmg.cells.nucleus.CellVersion;
import dmg.cells.nucleus.UOID;

import org.dcache.services.info.base.IntegerStateValue;
import org.dcache.services.info.base.StatePath;
import org.dcache.services.info.base.StateUpdate;
import org.dcache.services.info.base.StateUpdateManager;
import org.dcache.services.info.base.StringStateValue;
import org.dcache.services.info.gathers.CellMessageHandlerSkel;
import org.dcache.services.info.gathers.MessageMetadataRepository;

/**
 * Process an incoming message from issuing the command "getcellinfos" on the System
 * cell within a domain.
 *
 * @author Paul Millar <paul.millar@desy.de>
 */
public class CellInfoMsgHandler extends CellMessageHandlerSkel
{
    private static final Logger LOGGER = LoggerFactory.getLogger(CellInfoMsgHandler.class);

    private static final StatePath DOMAINS_PATH = new StatePath("domains");

    public CellInfoMsgHandler(StateUpdateManager sum, MessageMetadataRepository<UOID> msgMetaRepo)
    {
        super(sum, msgMetaRepo);
    }

    @Override
    public void process(Object msgPayload, long metricLifetime)
    {
        // Should never be null
        if (msgPayload == null) {
            LOGGER.error("received null payload from getcellinfos");
            return;
        }

        if (!msgPayload.getClass().isArray()) {
            LOGGER.error("received a message that isn't an array");
            return;
        }

        Class<?> arrayClass = msgPayload.getClass().getComponentType();

        if (arrayClass == null) {
            LOGGER.error("unable to figure out what array type is.");
            return;
        }

        if (!arrayClass.equals(CellInfo.class)) {
            LOGGER.error("received array is not an array of CellInfo");
            return;
        }

        StateUpdate update = new StateUpdate();

        CellInfo cells[] = (CellInfo[]) msgPayload;

        for (CellInfo thisCellInfo : cells) {
            String domain = thisCellInfo.getDomainName();
            String cellName = thisCellInfo.getCellName();

            StatePath thisCellPath = DOMAINS_PATH.newChild(domain)
                    .newChild("cells").newChild(cellName);

            addCellInfo(update, thisCellPath, thisCellInfo, metricLifetime);
        }

        applyUpdates(update);
    }


    /**
     * Add some information about a specific cell
     * @param update  the StateUpdate that metrics will be added
     * @param thisCellPath the StatePath for metrics for this branch
     * @param thisCell the CellInfo for the specific cell
     * @param lifetime how long the metrics should last.
     */
    private void addCellInfo(StateUpdate update, StatePath thisCellPath,
            CellInfo thisCell, long lifetime)
    {
        update.appendUpdate(thisCellPath.newChild("class"),
                new StringStateValue(thisCell.getCellClass(), lifetime));

        update.appendUpdate(thisCellPath.newChild("type"),
                new StringStateValue(thisCell.getCellType(), lifetime));

        CellVersion cellVersion = thisCell.getCellVersion();
        if (cellVersion != null) {
            addVersionInfo(update, thisCellPath, cellVersion, lifetime);
        }

        CellMessageHandlerSkel.addTimeMetrics(update, thisCellPath.newChild("created"), thisCell.getCreationTime(), lifetime);

        update.appendUpdate(thisCellPath.newChild("event-queue-size"),
                new IntegerStateValue(thisCell.getEventQueueSize(), lifetime));

        update.appendUpdate(thisCellPath.newChild("thread-count"),
                new IntegerStateValue(thisCell.getThreadCount(), lifetime));
    }

    /**
     * Add version information within a branch "version", parent of the supplied path.
     * @param update  the StateUpdate to append metrics
     * @param parentPath the path under which the version branch will be created.
     * @param version the CellVersion information
     * @param lifetime how long the metric should live for.
     */
    private void addVersionInfo(StateUpdate update, StatePath parentPath,
            CellVersion version, long lifetime)
    {
        StatePath versionPath = parentPath.newChild("version");

        update.appendUpdate(versionPath.newChild("revision"),
                new StringStateValue(version.getRevision(), lifetime));

        update.appendUpdate(versionPath.newChild("release"),
                new StringStateValue(version.getRelease(), lifetime));
    }
}
