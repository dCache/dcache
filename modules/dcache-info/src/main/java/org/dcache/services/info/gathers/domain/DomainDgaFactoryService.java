package org.dcache.services.info.gathers.domain;

import com.google.common.collect.ImmutableSet;

import java.util.Set;

import dmg.cells.nucleus.UOID;

import org.dcache.services.info.base.StateExhibitor;
import org.dcache.services.info.base.StateUpdateManager;
import org.dcache.services.info.gathers.DgaFactoryService;
import org.dcache.services.info.gathers.MessageMetadataRepository;
import org.dcache.services.info.gathers.MessageSender;
import org.dcache.services.info.gathers.Schedulable;

/**
 *  A factory for creating all domain-orientated DGA
 */
public class DomainDgaFactoryService implements DgaFactoryService
{
    @Override
    public Set<Schedulable> createDgas(StateExhibitor exhibitor,
            MessageSender sender, StateUpdateManager sum,
            MessageMetadataRepository<UOID> msgMetaRepo)
    {
        return ImmutableSet.of((Schedulable)new StaticDomainDga(exhibitor,
                sender, new StaticDomainMsgHandler(sum, msgMetaRepo)));
    }
}
