package org.dcache.webadmin.model.dataaccess.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.dcache.admin.webadmin.datacollector.datatypes.MoverInfo;
import org.dcache.webadmin.model.businessobjects.RestoreInfo;
import org.dcache.webadmin.model.dataaccess.MoverDAO;
import org.dcache.webadmin.model.dataaccess.communication.CellMessageGenerator;
import org.dcache.webadmin.model.dataaccess.communication.CellMessageGenerator.CellMessageRequest;
import org.dcache.webadmin.model.dataaccess.communication.CommandSender;
import org.dcache.webadmin.model.dataaccess.communication.CommandSenderFactory;
import org.dcache.webadmin.model.dataaccess.communication.ContextPaths;
import org.dcache.webadmin.model.dataaccess.communication.impl.PageInfoCache;
import org.dcache.webadmin.model.dataaccess.communication.impl.PoolMoverKillMessageGenerator;
import org.dcache.webadmin.model.exceptions.DAOException;
import org.dcache.webadmin.model.exceptions.NoSuchContextException;

/**
 *
 * @author jans
 */
public class StandardMoverDAO implements MoverDAO {

    private static final Logger _log = LoggerFactory.getLogger(StandardDomainsDAO.class);
    private PageInfoCache _pageCache;
    private CommandSenderFactory _commandSenderFactory;

    public StandardMoverDAO(PageInfoCache pageCache,
            CommandSenderFactory commandSenderFactory) {
        _pageCache = pageCache;
        _commandSenderFactory = commandSenderFactory;
    }

    @Override
    public List<MoverInfo> getActiveTransfers()
    {
        try {
            List<MoverInfo> transfers = (List<MoverInfo>) _pageCache.getCacheContent(
                    ContextPaths.MOVER_LIST);
            return transfers;
        } catch (NoSuchContextException e) {
            return Collections.emptyList();
        }
    }

    @Override
    public void killMoversOnSinglePool(Set<Integer> jobids, String targetPool)
            throws DAOException {
        try {
            _log.debug("kill movers {} on pool {}", jobids, targetPool);
            if (!jobids.isEmpty()) {
                PoolMoverKillMessageGenerator messageGenerator =
                        new PoolMoverKillMessageGenerator(targetPool, jobids);
                CommandSender commandSender =
                        _commandSenderFactory.createCommandSender(messageGenerator);
                commandSender.sendAndWait();
                checkForErrors(commandSender, messageGenerator);
            }
            _log.debug("killed movers successfully");
        } catch (InterruptedException e) {
            _log.warn("interrupted");
        }
    }

    private void checkForErrors(CommandSender commandSender,
            CellMessageGenerator<?> messageGenerator) throws DAOException {
        if (!commandSender.allSuccessful()) {
            Set<String> failedIds = extractFailedIds(messageGenerator);
            throw new DAOException(failedIds.toString());
        }
    }

    private Set<String> extractFailedIds(CellMessageGenerator<?> messageGenerator) {
        Set<String> failedIds = new HashSet<>();
        for (CellMessageRequest request : messageGenerator) {
            if (!request.isSuccessful()) {
                String destination = request.getDestination().toString();
                failedIds.add(destination);
            }
        }
        return failedIds;
    }

    @Override
    public Set<RestoreInfo> getRestores()
    {
        try {
            Set<RestoreInfo> restores = (Set<RestoreInfo>) _pageCache.getCacheContent(
                    ContextPaths.RESTORE_INFOS);
            return restores;
        } catch (NoSuchContextException e) {
            return Collections.emptySet();
        }
    }
}
