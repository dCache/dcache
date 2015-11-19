package org.dcache.services.info.gathers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

import diskCacheV111.services.space.message.GetLinkGroupNamesMessage;
import diskCacheV111.vehicles.Message;

import org.dcache.services.info.base.StateComposite;
import org.dcache.services.info.base.StatePath;
import org.dcache.services.info.base.StateUpdate;
import org.dcache.services.info.base.StateUpdateManager;

/**
 * Instances of this class will interpret an incoming reply CellMessages
 * that have instances of GetLinkGroupNamesMessage payload.  It uploads
 * the gathered information into the dCache state.
 *
 * @author Paul Millar <paul.millar@desy.de>
 */
public class LinkgroupListMsgHandler implements MessageHandler
{
    private static final Logger LOGGER = LoggerFactory.getLogger(LinkgroupListMsgHandler.class);
    private static final StatePath LINKGROUPS_PATH = new StatePath("linkgroups");

    private final StateUpdateManager _sum;

    public LinkgroupListMsgHandler(StateUpdateManager sum)
    {
        _sum = sum;
    }

    @Override
    public boolean handleMessage(Message messagePayload, long metricLifetime)
    {
        if (!(messagePayload instanceof GetLinkGroupNamesMessage)) {
            return false;
        }

        LOGGER.trace("received linkgroup list msg.");

        GetLinkGroupNamesMessage msg = (GetLinkGroupNamesMessage) messagePayload;

        Collection<String> names = msg.getLinkGroupNames();

        StateUpdate update = null;

        for (String name : names) {
            if (update == null) {
                update = new StateUpdate();
            }

            LOGGER.trace("adding linkgroup: {} lifetime: {}", name, metricLifetime);
            update.appendUpdate(LINKGROUPS_PATH
                    .newChild(name), new StateComposite(metricLifetime));
        }

        if (update != null) {
            _sum.enqueueUpdate(update);
        } else {
            LOGGER.trace("received GetLinkGroupNamesMessage with no linkgroups listed");
        }

        return true;
    }
}
