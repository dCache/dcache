package org.dcache.services.info.gathers.topo;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import dmg.cells.nucleus.EnvironmentAware;
import dmg.cells.nucleus.Environments;
import dmg.cells.nucleus.UOID;

import org.dcache.services.info.base.StateExhibitor;
import org.dcache.services.info.base.StateUpdateManager;
import org.dcache.services.info.gathers.DgaFactoryService;
import org.dcache.services.info.gathers.MessageMetadataRepository;
import org.dcache.services.info.gathers.MessageSender;
import org.dcache.services.info.gathers.Schedulable;
import org.dcache.services.info.gathers.SingleMessageDga;

import static org.dcache.services.info.Configuration.PROPERTY_NAME_SERVICE_TOPO;

/**
 * This class provides monitoring activity against the topo cell.
 *
 * @author Paul Millar <paul.millar@desy.de>
 */
public class TopoDgaFactoryService implements DgaFactoryService, EnvironmentAware
{
    private String _topo;

    @Override
    public Set<Schedulable> createDgas(StateExhibitor exhibitor, MessageSender sender,
            StateUpdateManager sum, MessageMetadataRepository<UOID> msgMetaRepo)
    {
        Set<Schedulable> activity = new HashSet<>();

        activity.add(new SingleMessageDga(sender, _topo, "gettopomap",
                new TopoMapHandler(sum, msgMetaRepo), 120));

        return activity;
    }

    @Override
    public void setEnvironment(Map<String, Object> environment)
    {
        _topo = Environments.getValue(environment, PROPERTY_NAME_SERVICE_TOPO);
    }
}
