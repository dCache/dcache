package org.dcache.services.info.gathers.cells;

import java.util.Set;

import org.dcache.services.info.base.StateExhibitor;
import org.dcache.services.info.base.StateUpdateManager;
import org.dcache.services.info.gathers.DgaFactoryService;
import org.dcache.services.info.gathers.MessageMetadataRepository;
import org.dcache.services.info.gathers.MessageSender;
import org.dcache.services.info.gathers.Schedulable;

import com.google.common.collect.ImmutableSet;

import dmg.cells.nucleus.UOID;

/**
 * This class is a registered service that instantiates a
 * standard set of DGAs for monitoring dCache.
 *
 * @author Paul Millar <paul.millar@desy.de>
 */
public class CellsDgaFactoryService implements DgaFactoryService
{

    @Override
    public Set<Schedulable> createDgas(StateExhibitor exhibitor,
                                       MessageSender sender,
                                       StateUpdateManager sum,
                                       MessageMetadataRepository<UOID> msgMetaRepo)
    {
    	return ImmutableSet.of((Schedulable)new CellInfoDga(exhibitor, sender,
                new CellInfoMsgHandler(sum, msgMetaRepo)));
    }
}
