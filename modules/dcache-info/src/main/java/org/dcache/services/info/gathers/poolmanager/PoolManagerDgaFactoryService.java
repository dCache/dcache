package org.dcache.services.info.gathers.poolmanager;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import dmg.cells.nucleus.CellMessageAnswerable;
import dmg.cells.nucleus.UOID;

import org.dcache.services.info.base.StateExhibitor;
import org.dcache.services.info.base.StatePath;
import org.dcache.services.info.base.StateUpdateManager;
import org.dcache.services.info.gathers.DgaFactoryService;
import org.dcache.services.info.gathers.ListBasedMessageDga;
import org.dcache.services.info.gathers.MessageMetadataRepository;
import org.dcache.services.info.gathers.MessageSender;
import org.dcache.services.info.gathers.Schedulable;
import org.dcache.services.info.gathers.SingleMessageDga;
import org.dcache.services.info.gathers.StringListMsgHandler;

/**
 * This DgaFactoryService creates DGAs for monitoring the PoolManager.
 *
 * @author Paul Millar <paul.millar@desy.de>
 */
public class PoolManagerDgaFactoryService implements DgaFactoryService
{

    @Override
    public Set<Schedulable> createDgas(StateExhibitor exhibitor,
                                       MessageSender sender,
                                       StateUpdateManager sum,
                                       MessageMetadataRepository<UOID> msgMetaRepo)
    {
        return new DgaFactory(exhibitor, sender, sum, msgMetaRepo).get();
    }


    /**
     * Instances of this class will build the DGA with the provided
     * StateExhibitor, MessageSender, StateUpdateManager and
     * MessageMetadataRepository.  Please note this builder is not
     * thread-safe and must be used only once.
     *
     * @author Paul Millar <paul.millar@desy.de>
     */
    private static class DgaFactory
    {
        private final StateExhibitor _exhibitor;
        private final MessageSender _sender;
        private final StateUpdateManager _sum;
        private final MessageMetadataRepository<UOID> _msgMetaRepo;
        private final Set<Schedulable> _activity = new HashSet<>();

        DgaFactory(StateExhibitor exhibitor,
                   MessageSender sender,
                   StateUpdateManager sum,
                   MessageMetadataRepository<UOID> msgMetaRepo)
        {
            _exhibitor = exhibitor;
            _sender = sender;
            _sum = sum;
            _msgMetaRepo = msgMetaRepo;

            addListCommandAsDga("pools", "psux ls pool");
            addListCommandAsDga("poolgroups", "psux ls pgroup");
            addListCommandAsDga("units", "psux ls unit");
            addListCommandAsDga("unitgroups", "psux ls ugroup");

            addSingleMessageDga("xcm ls",
            		new PoolCostMsgHandler(_sum, _msgMetaRepo));

            addSingleMessageDga("psux ls link -x -resolve",
                    new LinkInfoMsgHandler(_sum, _msgMetaRepo));

            addListBasedDgaForChildrenOf("pools", "psux ls pool",
                    new PoolInfoMsgHandler(_sum, _msgMetaRepo));

            addListBasedDgaForChildrenOf("poolgroups", "psux ls pgroup",
                    new PoolGroupInfoMsgHandler(_sum, _msgMetaRepo));

            addListBasedDgaForChildrenOf("units", "psux ls unit",
                    new UnitInfoMsgHandler(_sum, _msgMetaRepo));

            addListBasedDgaForChildrenOf("unitgroups", "psux ls ugroup",
                    new UGroupInfoMsgHandler(_sum, _msgMetaRepo));
        }

        public Set<Schedulable> get()
        {
            return _activity;
        }

        /* For each child of path, query cell with an ASCII command like: prefix + " " + child; process the response */
        private void addListBasedDgaForChildrenOf(String path, String prefix,
                                CellMessageAnswerable response)
        {
            _activity.add(new ListBasedMessageDga(_exhibitor, _sender,
                    new StatePath(path), "PoolManager", prefix,
                    response));
        }

        /* A DGA that queries PoolManager for a list and populate the info under some path */
        private void addListCommandAsDga(String path, String command)
        {
            addSingleMessageDga(command, new StringListMsgHandler(_sum, _msgMetaRepo, path));
        }


        private void addSingleMessageDga(String command, CellMessageAnswerable response)
        {
            _activity.add(new SingleMessageDga(_sender, "PoolManager", command,
                    response, TimeUnit.MINUTES.toSeconds(5)));
        }
    }
}
