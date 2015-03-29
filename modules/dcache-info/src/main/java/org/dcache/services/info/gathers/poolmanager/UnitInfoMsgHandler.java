package org.dcache.services.info.gathers.poolmanager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import dmg.cells.nucleus.UOID;

import org.dcache.services.info.base.StatePath;
import org.dcache.services.info.base.StateUpdate;
import org.dcache.services.info.base.StateUpdateManager;
import org.dcache.services.info.base.StringStateValue;
import org.dcache.services.info.gathers.CellMessageHandlerSkel;
import org.dcache.services.info.gathers.MessageMetadataRepository;

/**
 * Process incoming Object array (*shudder*) and update state.
 *
 * @author Paul Millar <paul.millar@desy.de>
 */
public class UnitInfoMsgHandler extends CellMessageHandlerSkel
{
    private static final Logger LOGGER = LoggerFactory.getLogger(UnitInfoMsgHandler.class);
    private static final StatePath UNITS_PATH = new StatePath("units");

    public UnitInfoMsgHandler(StateUpdateManager sum,
            MessageMetadataRepository<UOID> msgMetaRepo)
    {
        super(sum, msgMetaRepo);
    }

    @Override
    public void process(Object msgPayload, long metricLifetime)
    {
        if (!msgPayload.getClass().isArray()) {
            LOGGER.error("unexpected received non-array payload");
            return;
        }

        Object array[] = (Object []) msgPayload;

        if (array.length != 3) {
            LOGGER.error("Unexpected array size: {}", array.length);
            return;
        }

        /*
         * array[0] = name
         * array[1] = type
         * array[2] = list of unitgroups.
         */

        String unitName = array[0].toString();
        String unitType = array[1].toString();

        StatePath thisUnitPath = UNITS_PATH.newChild(unitName);

        StateUpdate update = new StateUpdate();

        update.appendUpdate(thisUnitPath.newChild("type"),
                new StringStateValue(unitType, metricLifetime));

        addItems(update, thisUnitPath.newChild("unitgroups"), (Object []) array [2], metricLifetime);

        applyUpdates(update);
    }
}
