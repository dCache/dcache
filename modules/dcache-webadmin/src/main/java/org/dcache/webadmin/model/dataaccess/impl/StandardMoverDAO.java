package org.dcache.webadmin.model.dataaccess.impl;

import com.google.common.collect.Iterables;
import com.google.common.primitives.Ints;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;

import diskCacheV111.util.CacheException;
import diskCacheV111.vehicles.PoolMoverKillMessage;

import dmg.cells.nucleus.CellPath;

import org.dcache.cells.CellStub;
import org.dcache.pool.classic.IoRequestState;
import org.dcache.webadmin.model.businessobjects.RestoreInfo;
import org.dcache.webadmin.model.dataaccess.MoverDAO;
import org.dcache.webadmin.model.dataaccess.communication.CommandSenderFactory;
import org.dcache.webadmin.model.dataaccess.communication.ContextPaths;
import org.dcache.webadmin.model.dataaccess.communication.impl.PageInfoCache;
import org.dcache.webadmin.model.exceptions.DAOException;
import org.dcache.webadmin.model.exceptions.NoSuchContextException;
import org.dcache.webadmin.view.beans.ActiveTransfersBean;

/**
 *
 * @author jans
 */
public class StandardMoverDAO implements MoverDAO {

    private static final Logger _log = LoggerFactory.getLogger(StandardDomainsDAO.class);
    private final PageInfoCache _pageCache;
    private final CellStub _cellStub;

    public StandardMoverDAO(PageInfoCache pageCache,
            CommandSenderFactory commandSenderFactory)
    {
        _pageCache = pageCache;
        _cellStub = commandSenderFactory.getCellStub();
    }

    @Override
    public List<ActiveTransfersBean> getActiveTransfers()
    {
        try {
            return (List<ActiveTransfersBean>) _pageCache.getCacheContent(ContextPaths.MOVER_LIST);
        } catch (NoSuchContextException e) {
            return Collections.emptyList();
        }
    }

    @Override
    public void killMovers(Iterable<ActiveTransfersBean> transfers)
            throws DAOException
    {
        try {
            if (!Iterables.isEmpty(transfers)) {
                Map<ActiveTransfersBean.Key, Future<PoolMoverKillMessage>> futures = new HashMap<>();
                for (ActiveTransfersBean transfer : transfers) {
                    futures.put(transfer.getKey(),
                                _cellStub.send(new CellPath(transfer.getPool()),
                                              new PoolMoverKillMessage(transfer.getPool(),
                                                                       Ints.checkedCast(transfer.getMoverId()))));
                }

                Collection<Long> failed = new ArrayList<>();
                for (ActiveTransfersBean transfer : transfers) {
                    try {
                        CellStub.getMessage(futures.get(transfer.getKey()));
                        transfer.setMoverStatus(IoRequestState.CANCELED.toString());
                    } catch (CacheException e) {
                        if (e.getRc() != 1) {
                            failed.add(transfer.getMoverId());
                        }
                    }
                }
                if (!failed.isEmpty()) {
                    throw new DAOException(failed.toString());
                }
            }
            _log.debug("killed movers successfully");
        } catch (InterruptedException e) {
            _log.warn("interrupted");
        }
    }

    @Override
    public Set<RestoreInfo> getRestores()
    {
        try {
            return (Set<RestoreInfo>) _pageCache.getCacheContent(ContextPaths.RESTORE_INFOS);
        } catch (NoSuchContextException e) {
            return Collections.emptySet();
        }
    }
}
