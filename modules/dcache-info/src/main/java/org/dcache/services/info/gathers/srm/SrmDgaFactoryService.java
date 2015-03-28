package org.dcache.services.info.gathers.srm;

import java.util.HashSet;
import java.util.Set;

import dmg.cells.nucleus.UOID;

import org.dcache.services.info.base.StateExhibitor;
import org.dcache.services.info.base.StateUpdateManager;
import org.dcache.services.info.gathers.DgaFactoryService;
import org.dcache.services.info.gathers.MessageMetadataRepository;
import org.dcache.services.info.gathers.MessageSender;
import org.dcache.services.info.gathers.Schedulable;

/**
 * This class provides monitoring activity for SpaceManager
 *
 * @author Paul Millar <paul.millar@desy.de>
 */
public class SrmDgaFactoryService implements DgaFactoryService
{

    @Override
    public Set<Schedulable> createDgas(StateExhibitor exhibitor,
                                       MessageSender sender,
                                       StateUpdateManager sum,
                                       MessageMetadataRepository<UOID> msgMetaRepo)
    {
        Set<Schedulable> activity = new HashSet<>();

        // We don't use LinkgroupListDga as it provides the wrong information, and isn't needed as
        // LinkgroupDetailsDga (mistakenly) provides all information about all linkgroups.
        //addActivity(new LinkgroupListDga(60));
        activity.add(new LinkgroupDetailsDga(sender, 300)); // every five minutes, as this may be a heavy-weight operation.
        activity.add(new SrmSpaceDetailsDga(sender, 300)); // every five minutes, as this may be a heavy-weight operation.

        return activity;
    }
}
