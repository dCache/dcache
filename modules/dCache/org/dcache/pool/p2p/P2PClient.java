// $Id: P2PClient.java,v 1.21 2007-10-31 17:27:11 radicke Exp $

package org.dcache.pool.p2p;

import java.io.PrintWriter;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.Collections;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;

import org.apache.log4j.Logger;
import org.dcache.pool.classic.ChecksumModuleV1;
import org.dcache.pool.repository.Repository;
import org.dcache.pool.repository.StickyRecord;
import org.dcache.pool.repository.EntryState;
import org.dcache.pool.repository.WriteHandle;
import org.dcache.pool.repository.CacheEntry;
import org.dcache.pool.repository.StickyRecord;
import org.dcache.cells.AbstractCellComponent;
import org.dcache.cells.CellMessageReceiver;
import org.dcache.cells.CellCommandListener;
import org.dcache.cells.CellStub;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.CacheFileAvailable;
import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.DCapProtocolInfo;
import diskCacheV111.vehicles.DoorTransferFinishedMessage;
import diskCacheV111.vehicles.StorageInfo;
import dmg.cells.nucleus.CellEndpoint;
import dmg.util.Args;
import dmg.util.CommandSyntaxException;

public class P2PClient
    extends AbstractCellComponent
    implements CellMessageReceiver,
               CellCommandListener
{
    private final static Logger _log = Logger.getLogger(P2PClient.class);
    private final static Acceptor _acceptor = new Acceptor();

    private final Map<Integer, Companion> _sessions =
        new ConcurrentHashMap<Integer, Companion>();
    private final ExecutorService _executor =
        Executors.newSingleThreadExecutor();
    private Repository _repository;
    private ChecksumModuleV1 _checksumModule;

    private int _maxActive = 0;

    private CellStub _pnfs;
    private CellStub _pool;

    public P2PClient()
    {
    }

    public void setRepository(Repository repository)
    {
        _repository = repository;
    }

    public void setChecksumModule(ChecksumModuleV1 csm)
    {
        _checksumModule = csm;
    }

    public void setPnfs(CellStub pnfs)
    {
        _pnfs = pnfs;
    }

    public void setPool(CellStub pool)
    {
        _pool = pool;
    }

    public void shutdown()
    {
        _executor.shutdown();
    }

    public int getActiveJobs()
    {
        return (_sessions.size() <= _maxActive) ? _sessions.size() : _maxActive;
    }

    public int getMaxActiveJobs()
    {
        return _maxActive;
    }

    public int getQueueSize()
    {
        return
            (_sessions.size() > _maxActive)
            ? (_sessions.size() - _maxActive)
            : 0;
    }

    public void messageArrived(DoorTransferFinishedMessage message)
    {
        DCapProtocolInfo pinfo = (DCapProtocolInfo)message.getProtocolInfo();
        int sessionId = pinfo.getSessionId();
        Companion companion = _sessions.get(sessionId);
        if (companion != null) {
            companion.messageArrived(message);
        }
    }

    /**
     * Small wrapper for the real callback. Will remove the companion
     * from the <code>_sessions</code> map.
     */
    private class Callback implements CacheFileAvailable
    {
        private CacheFileAvailable _callback;
        private int _id;

        Callback(CacheFileAvailable callback)
        {
            _callback = callback;
            _id = -1;
        }

        synchronized void setId(int id)
        {
            _id = id;
            notifyAll();
        }

        synchronized int getId()
            throws InterruptedException
        {
            while (_id == -1)
                wait();
            return _id;
        }

        public void cacheFileAvailable(String pnfsId, Throwable t)
        {
            try {
                if (_callback != null) {
                    _callback.cacheFileAvailable(pnfsId, t);
                }
                _sessions.remove(getId());
            } catch (InterruptedException e) {
                // Ignored, typically happens at cell shutdown
            }
        }
    }

    public int newCompanion(PnfsId pnfsId,
                            String poolName,
                            StorageInfo storageInfo,
                            EntryState targetState,
                            List<StickyRecord> stickyRecords,
                            CacheFileAvailable callback)
        throws UnknownHostException
    {
        if (getCellEndpoint() == null)
            throw new IllegalStateException("Endpoint must be set");

        Callback cb = new Callback(callback);

        Companion companion =
            new Companion(_executor, _acceptor, _repository,
                          _checksumModule,
                          _pnfs, _pool,
                          pnfsId, storageInfo,
                          poolName, targetState, stickyRecords,
                          cb);

        int id = companion.getId();
        _sessions.put(id, companion);
        cb.setId(id);

        return id;
    }

    /**
     * Cancels a transfer. Returns true unless the transfer is already
     * completed.
     */
    public boolean cancel(int id)
    {
        Companion companion = _sessions.get(id);
        return (companion == null)
                ? false
                : companion.cancel("Transfer was cancelled");
    }

    public void getInfo(PrintWriter pw)
    {
        pw.println("  Listener   : " + _acceptor);
        pw.println("  Max Active : " + _maxActive);
        pw.println("Pnfs Timeout : " + (_pnfs.getTimeout() / 1000L) + " seconds ");
    }

    public void printSetup(PrintWriter pw)
    {
        pw.println("#\n#  Pool to Pool (P2P) [$Id: P2PClient.java,v 1.21 2007-10-31 17:27:11 radicke Exp $]\n#");
        pw.println("pp set port " + _acceptor.getPort());
        pw.println("pp set max active " + _maxActive);
        pw.println("pp set pnfs timeout " + (_pnfs.getTimeout() / 1000L));
    }

    public String hh_pp_set_pnfs_timeout = "<Timeout/sec>";
    public String ac_pp_set_pnfs_timeout_$_1(Args args)
    {
        long timeout = Long.parseLong(args.argv(0));
        _pnfs.setTimeout(timeout * 1000L);
        return "Pnfs timeout set to " + timeout + " seconds";
    }

    public String hh_pp_set_max_active = "<normalization>";
    public String ac_pp_set_max_active_$_1(Args args)
    {
        _maxActive = Integer.parseInt(args.argv(0));
        return "";
    }

    public String hh_pp_set_port = "<listenPort>";
    public String ac_pp_set_port_$_1(Args args)
    {
        _acceptor.setPort(Integer.parseInt(args.argv(0)));
        return "";
    }

    public String hh_pp_get_file = "<pnfsId> <pool>";
    public String ac_pp_get_file_$_2(Args args)
        throws CacheException, UnknownHostException
    {
        PnfsId pnfsId = new PnfsId(args.argv(0));
        String pool = args.argv(1);
        List<StickyRecord> stickyRecords = Collections.emptyList();
        newCompanion(pnfsId, pool, null, EntryState.CACHED, stickyRecords, null);
        return "Transfer Initiated";
    }

    public String hh_pp_remove = "<id>";
    public String ac_pp_remove_$_1(Args args)
        throws NumberFormatException
    {
        Companion companion = _sessions.remove(Integer.valueOf(args.argv(0)));
        if (companion == null || !companion.cancel("Cancelled by user"))
            throw new IllegalArgumentException("Id not found: " + args.argv(0));
        return "";
    }

    public String hh_pp_ls = " # get the list of companions";
    public String ac_pp_ls(Args args)
    {
        StringBuilder sb = new StringBuilder();

        for (Companion c : _sessions.values()) {
            sb.append(c.toString()).append("\n");
        }
        return sb.toString();
    }
}
