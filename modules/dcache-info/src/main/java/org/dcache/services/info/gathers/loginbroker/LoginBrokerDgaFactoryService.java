package org.dcache.services.info.gathers.loginbroker;

import com.google.common.collect.ImmutableSet;

import java.util.Set;
import java.util.concurrent.TimeUnit;

import dmg.cells.nucleus.CellMessageAnswerable;
import dmg.cells.nucleus.UOID;

import org.dcache.services.info.base.StateExhibitor;
import org.dcache.services.info.base.StateUpdateManager;
import org.dcache.services.info.gathers.DgaFactoryService;
import org.dcache.services.info.gathers.MessageMetadataRepository;
import org.dcache.services.info.gathers.MessageSender;
import org.dcache.services.info.gathers.Schedulable;
import org.dcache.services.info.gathers.SingleMessageDga;

/**
 * This class is a registered service that instantiates a
 * standard set of DGAs for monitoring dCache.
 *
 * @author Paul Millar <paul.millar@desy.de>
 */
public class LoginBrokerDgaFactoryService implements DgaFactoryService
{
    @Override
    public Set<Schedulable> createDgas(StateExhibitor exhibitor, MessageSender sender,
            StateUpdateManager sum, MessageMetadataRepository<UOID> msgMetaRepo)
    {
        CellMessageAnswerable lbHandler = new LoginBrokerLsMsgHandler(sum, msgMetaRepo);

        return ImmutableSet.of((Schedulable)new SingleMessageDga(sender, "LoginBroker",
                "ls -binary", lbHandler, TimeUnit.MINUTES.toSeconds(1)));
    }
}
