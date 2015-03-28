package org.dcache.services.info.gathers.topo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import dmg.cells.network.CellDomainNode;
import dmg.cells.nucleus.UOID;

import org.dcache.services.info.base.StatePath;
import org.dcache.services.info.base.StateUpdate;
import org.dcache.services.info.base.StateUpdateManager;
import org.dcache.services.info.base.StringStateValue;
import org.dcache.services.info.gathers.CellMessageHandlerSkel;
import org.dcache.services.info.gathers.MessageMetadataRepository;

/**
 * This class handles reply messages from the TopoCell when issuing a
 * "gettopomap" command.  In fact, it does little beyond maintaining a
 * simple list of domains.
 *
 * @author Paul Millar <paul.millar@desy.de>
 */
public class TopoMapHandler extends CellMessageHandlerSkel
{
    private static Logger _log = LoggerFactory.getLogger(TopoMapHandler.class);

    private static final StatePath DOMAINS_PATH = new StatePath("domains");

    public TopoMapHandler(StateUpdateManager sum, MessageMetadataRepository<UOID> msgMetaRepo)
    {
        super(sum, msgMetaRepo);
    }

    @Override
    public void process(Object msgPayload, long metricLifetime)
    {
        // The TopoCell may return null whilst starting up.
        if (msgPayload == null) {
            return;
        }

        if (!msgPayload.getClass().isArray()) {
            _log.error("received a message that isn't an array");
            return;
        }

        Class<?> arrayClass = msgPayload.getClass().getComponentType();

        if (arrayClass == null) {
            _log.error("unable to figure out what array type is.");
            return;
        }

        if (!arrayClass.equals(CellDomainNode.class)) {
            _log.error("received array is not instance of CellDomainNode[]");
            return;
        }

        StateUpdate update = new StateUpdate();

        CellDomainNode domains[] = (CellDomainNode[]) msgPayload;

        for (CellDomainNode domain : domains) {
            addDomain(update, domain, metricLifetime);
        }

        applyUpdates(update);
    }


    /**
     * Add information about a domain to the list of pending updates.
     * @param update the StateUpdate to append new metric requests,
     * @param domain the information to add.
     */
    private void addDomain(StateUpdate update, CellDomainNode domain, long lifetime)
    {
        StatePath thisDomainPath = DOMAINS_PATH.newChild(domain.getName());

        update.appendUpdate(thisDomainPath.newChild("address"),
                new StringStateValue(domain.getAddress(), lifetime));

        /*
         * We could also record tunnel information here...
         *
         *    CellTunnelInfo tunnels[] = domain.getLinks();
         */
    }
}
