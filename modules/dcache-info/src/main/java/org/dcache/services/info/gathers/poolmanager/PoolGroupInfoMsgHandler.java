package org.dcache.services.info.gathers.poolmanager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import dmg.cells.nucleus.UOID;

import org.dcache.services.info.base.StateComposite;
import org.dcache.services.info.base.StatePath;
import org.dcache.services.info.base.StateUpdate;
import org.dcache.services.info.base.StateUpdateManager;
import org.dcache.services.info.gathers.CellMessageHandlerSkel;
import org.dcache.services.info.gathers.MessageMetadataRepository;

public class PoolGroupInfoMsgHandler extends CellMessageHandlerSkel
{
    private static final Logger LOGGER = LoggerFactory.getLogger(PoolGroupInfoMsgHandler.class);
    private static final StatePath POOLGROUPS_PATH = new StatePath("poolgroups");

    public PoolGroupInfoMsgHandler(StateUpdateManager sum,
            MessageMetadataRepository<UOID> msgMetaRepo)
    {
        super(sum, msgMetaRepo);
    }

    @Override
    public void process(Object msgPayload, long metricLifetime)
    {
        LOGGER.trace("processing new poolgroup information");

        if (!msgPayload.getClass().isArray()) {
            LOGGER.error("Pool group info, received a message that isn't an array");
            return;
        }

        Object array[] = (Object []) msgPayload;

	if (array.length != 4) {
            LOGGER.error("Pool group info, unexpected array size: {}", array.length);
		    return;
	}

        // Map the array into slightly more meaningful components.
        String poolgroupName = (String) array[0];
        Object poolNames[] = (Object []) array[1];
        Object linkNames[] = (Object []) array[2];
        Object resilient[] = (Object []) array[3];

        StateUpdate update = new StateUpdate();

        StatePath thisPoolGroupPath = POOLGROUPS_PATH.newChild(poolgroupName);

        if (poolNames.length + linkNames.length == 0) {
            // Add an entry, even though this poolgroup is empty.
            update.appendUpdate(thisPoolGroupPath, new StateComposite(metricLifetime));
        } else {
            addItems(update, thisPoolGroupPath.newChild("pools"), poolNames, metricLifetime);
            addItems(update, thisPoolGroupPath.newChild("links"), linkNames, metricLifetime);
            addItems(update, thisPoolGroupPath.newChild("resilient"), resilient, metricLifetime);
        }

        applyUpdates(update);
    }
}
