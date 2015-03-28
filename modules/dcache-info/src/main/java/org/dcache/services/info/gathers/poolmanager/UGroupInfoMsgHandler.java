package org.dcache.services.info.gathers.poolmanager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import dmg.cells.nucleus.UOID;

import org.dcache.services.info.base.StatePath;
import org.dcache.services.info.base.StateUpdate;
import org.dcache.services.info.base.StateUpdateManager;
import org.dcache.services.info.gathers.CellMessageHandlerSkel;
import org.dcache.services.info.gathers.MessageMetadataRepository;

/**
 * Process an incoming message from PoolManager about a specific UnitGroup.
 *
 * @author Paul Millar <paul.millar@desy.de>
 */
public class UGroupInfoMsgHandler extends CellMessageHandlerSkel {

    private static Logger _log = LoggerFactory.getLogger(UGroupInfoMsgHandler.class);

    private static final StatePath UNITGROUP_PATH = new StatePath("unitgroups");

    public UGroupInfoMsgHandler(StateUpdateManager sum, MessageMetadataRepository<UOID> msgMetaRepo) {
        super(sum, msgMetaRepo);
    }

    @Override
    public void process(Object msgPayload, long metricLifetime) {

        if (!msgPayload.getClass().isArray()) {
            _log.error("unexpected received non-array payload");
            return;
        }

        Object array[] = (Object []) msgPayload;

        if (array.length != 3) {
            _log.error("unexpected array size: "+array.length);
            return;
        }

        /**
         * array[0] = group name
         * array[1] = unit list
         * array[2] = link list
         */
        String unitGroupName = (String) array[0];

        StatePath thisUGroupPath = UNITGROUP_PATH.newChild(unitGroupName);

        StateUpdate update = new StateUpdate();

        addItems(update, thisUGroupPath.newChild("units"), (Object []) array [1], metricLifetime);
        addItems(update, thisUGroupPath.newChild("links"), (Object []) array [2], metricLifetime);

        applyUpdates(update);
    }
}
