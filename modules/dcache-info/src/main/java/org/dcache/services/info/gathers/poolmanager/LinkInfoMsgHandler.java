package org.dcache.services.info.gathers.poolmanager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;

import dmg.cells.nucleus.UOID;

import org.dcache.services.info.base.IntegerStateValue;
import org.dcache.services.info.base.StateComposite;
import org.dcache.services.info.base.StatePath;
import org.dcache.services.info.base.StateUpdate;
import org.dcache.services.info.base.StateUpdateManager;
import org.dcache.services.info.base.StringStateValue;
import org.dcache.services.info.gathers.CellMessageHandlerSkel;
import org.dcache.services.info.gathers.MessageMetadataRepository;

/**
 * A class to handle reply messages from PoolManager's "psux ls -x -resolve" command.
 *
 * @author Paul Millar <paul.millar@desy.de>
 */
public class LinkInfoMsgHandler extends CellMessageHandlerSkel
{
    private static Logger _log = LoggerFactory.getLogger(LinkInfoMsgHandler.class);

    private static final int EXPECTED_ARRAY_SIZE=13;

    public LinkInfoMsgHandler(StateUpdateManager sum,
            MessageMetadataRepository<UOID> msgMetaRepo)
    {
        super(sum, msgMetaRepo);
    }

    @Override
    public void process(Object msgPayload, long metricLifetime)
    {
        StateUpdate update = null;

        Iterable<?> linkInfoArray = (ArrayList<?>) msgPayload;

        StatePath linksPath = new StatePath("links");

        for (Object o : linkInfoArray) {
            if (!o.getClass().isArray()) {
                _log.error("Link information not an array.");
                continue;
            }
            Object[] array = (Object[]) o;
            if (array.length != EXPECTED_ARRAY_SIZE) {
                _log.error("Unexpected array size: "+array.length);
                continue;
            }

            if (update == null) {
                update = new StateUpdate();
            }

            processInfo(update, linksPath, (Object[]) o, metricLifetime);
        }

        if (update != null) {
            applyUpdates(update);
        }
    }


    /**
     * Append updates to the supplied StateUpdate object containing new data based on
     * the supplied information.
     * @param update the StateUpdate object
     * @param linksPath the path under which data will be added ("links").
     * @param o the array of information for this link
     * @param lifetime how long, in seconds, this data should survive.
     */
    private void processInfo(StateUpdate update, StatePath linksPath, Object[] o, long lifetime)
    {
        String name = (String) o[0];
        int readPref = (Integer) o[1];
        int cachePref = (Integer) o[2];
        int writePref = (Integer) o[3];

        Object[] uGroups = (Object[]) o[4];
        Object[] pools = (Object[]) o[5];
        Object[] groups = (Object[]) o[6];
        int p2pPref = (Integer) o[7];
        String tag = (String) o[8];

        Object[] store = (Object[]) o[9];
        Object[] net = (Object[]) o[10];
        Object[] dcache = (Object[]) o[11];
        Object[] protocol = (Object[]) o[12];

        StatePath thisLinkPath = linksPath.newChild(name);

        StatePath prefPath = thisLinkPath.newChild("prefs");

        update.appendUpdate(prefPath.newChild("read"), new IntegerStateValue(readPref, lifetime));
        update.appendUpdate(prefPath.newChild("cache"), new IntegerStateValue(cachePref, lifetime));
        update.appendUpdate(prefPath.newChild("write"), new IntegerStateValue(writePref, lifetime));
        update.appendUpdate(prefPath.newChild("p2p"), new IntegerStateValue(p2pPref, lifetime));

        if (uGroups != null) {
            addItems(update, thisLinkPath
                    .newChild("unitgroups"), uGroups, lifetime);
        }
        addItems(update, thisLinkPath.newChild("pools"), pools, lifetime);
        addItems(update, thisLinkPath.newChild("poolgroups"), groups, lifetime);

        update.appendUpdate(thisLinkPath.newChild("selection"), new StringStateValue(tag != null ? tag : "None", lifetime));

        StatePath unitPath = thisLinkPath.newChild("units");

        addItems(update, unitPath.newChild("store"), store, lifetime);
        addItems(update, unitPath.newChild("net"), net, lifetime);
        addItems(update, unitPath.newChild("dcache"), dcache, lifetime);
        addItems(update, unitPath.newChild("protocol"), protocol, lifetime);

        /**
         * We must add the space branch explicitly as it must be mortal.  This is to prevent the
         * state engine from killing the automatically created (ephemeral) branch when adding the
         * ephemeral space children: the space metrics calculated by the LinkSpaceMainter SIP.
         * TODO: come up with a better solution!
         */
        update.appendUpdate(thisLinkPath.newChild("space"), new StateComposite(lifetime));
    }
}
