// $Id: P2PClient.java,v 1.21 2007-10-31 17:27:11 radicke Exp $

package org.dcache.pool.p2p;

import java.io.PrintWriter;
import java.io.IOException;
import java.net.UnknownHostException;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Collections;
import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ScheduledExecutorService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
    private final static Logger _log = LoggerFactory.getLogger(P2PClient.class);
    private final static Acceptor _acceptor = new Acceptor();

    private final Map<Integer, Companion> _companions = new HashMap();
    private ScheduledExecutorService _executor;
    private Repository _repository;
    private ChecksumModuleV1 _checksumModule;

    private int _maxActive = 0;

    private CellStub _pnfs;
    private CellStub _pool;

    public P2PClient()
    {
    }

    public synchronized void setExecutor(ScheduledExecutorService executor)
    {
        _executor = executor;
    }

    public synchronized void setRepository(Repository repository)
    {
        _repository = repository;
    }

    public synchronized void setChecksumModule(ChecksumModuleV1 csm)
    {
        _checksumModule = csm;
    }

    public synchronized void setPnfs(CellStub pnfs)
    {
        _pnfs = pnfs;
    }

    public synchronized void setPool(CellStub pool)
    {
        _pool = pool;
    }

    public synchronized int getActiveJobs()
    {
        return (_companions.size() <= _maxActive) ? _companions.size() : _maxActive;
    }

    public synchronized int getMaxActiveJobs()
    {
        return _maxActive;
    }

    public synchronized int getQueueSize()
    {
        return
            (_companions.size() > _maxActive)
            ? (_companions.size() - _maxActive)
            : 0;
    }

    public synchronized void messageArrived(DoorTransferFinishedMessage message)
    {
        DCapProtocolInfo pinfo = (DCapProtocolInfo)message.getProtocolInfo();
        int sessionId = pinfo.getSessionId();
        Companion companion = _companions.get(sessionId);
        if (companion != null) {
            companion.messageArrived(message);
        }
    }

    /**
     * Adds a companion to the _companions map.
     */
    private synchronized int addCompanion(Companion companion)
    {
        int sessionId = companion.getId();
        _companions.put(sessionId, companion);
        return sessionId;
    }

    /**
     * Removes a companion from the _companions map.
     */
    private synchronized void removeCompanion(int sessionId)
    {
        _companions.remove(sessionId);
        notifyAll();
    }

    /**
     * Cancels all companions for a given file.
     */
    private synchronized void cancelCompanions(PnfsId pnfsId, String cause)
    {
        for (Companion companion: _companions.values()) {
            if (pnfsId.equals(companion.getPnfsId())) {
                companion.cancel(cause);
            }
        }
    }

    /**
     * Small wrapper for the real callback. Will remove the companion
     * from the <code>_companions</code> map.
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

        public void cacheFileAvailable(PnfsId pnfsId, Throwable t)
        {
            try {
                if (_callback != null) {
                    _callback.cacheFileAvailable(pnfsId, t);
                }
                removeCompanion(getId());

                /* In case of a successfull transfer, there is no
                 * reason to keep other companions on the same file
                 * around.
                 */
                if (t == null) {
                    cancelCompanions(pnfsId,
                                     "Replica already exists");
                }
            } catch (InterruptedException e) {
                // Ignored, typically happens at cell shutdown
            }
        }
    }

    public synchronized int newCompanion(PnfsId pnfsId,
                                         String poolName,
                                         StorageInfo storageInfo,
                                         EntryState targetState,
                                         List<StickyRecord> stickyRecords,
                                         CacheFileAvailable callback)
        throws IOException, CacheException, InterruptedException
    {
        if (getCellEndpoint() == null)
            throw new IllegalStateException("Endpoint not initialized");
        if (_pool == null)
            throw new IllegalStateException("Pool stub not initialized");
        if (_executor == null)
            throw new IllegalStateException("Executor not initialized");
        if (_repository == null)
            throw new IllegalStateException("Repository not initialized");
        if (_checksumModule == null)
            throw new IllegalStateException("Checksum module not initialized");
        if (_pnfs == null)
            throw new IllegalStateException("PNFS stub not initialized");
        if (_repository.getState(pnfsId) != EntryState.NEW)
            throw new IllegalStateException("Replica already exists");

        Callback cb = new Callback(callback);

        Companion companion =
            new Companion(_executor, _acceptor, _repository,
                          _checksumModule,
                          _pnfs, _pool,
                          pnfsId, storageInfo,
                          poolName, targetState, stickyRecords,
                          cb);

        int id = addCompanion(companion);
        cb.setId(id);
        return id;
    }

    /**
     * Cancels a transfer. Returns true if the transfer was
     * cancelled. Returns false if the transfer was already completed
     * or did not exist.
     */
    public synchronized boolean cancel(int id)
    {
        Companion companion = _companions.get(id);
        return (companion == null)
                ? false
                : companion.cancel("Transfer was cancelled");
    }

    /**
     * Cancels all transfers.
     */
    public synchronized void shutdown()
        throws InterruptedException
    {
        for (Companion companion: _companions.values()) {
            companion.cancel("Pool is going down");
        }
        while (!_companions.isEmpty()) {
            wait();
        }
    }

    public synchronized void getInfo(PrintWriter pw)
    {
        pw.println("  Listen     : " + _acceptor);
        pw.println("  Max Active : " + _maxActive);
        pw.println("Pnfs Timeout : " + (_pnfs.getTimeout() / 1000L) + " seconds ");
    }

    public synchronized void printSetup(PrintWriter pw)
    {
        pw.println("#\n#  Pool to Pool (P2P) [$Revision$]\n#");
        InetSocketAddress address = _acceptor.getAddress();
        if (address.getAddress().isAnyLocalAddress()) {
            pw.println("pp set port " + address.getPort());
        } else {
            pw.println("pp set listen " +
                       address.getHostName() + " " + address.getPort());
        }
        pw.println("pp set max active " + _maxActive);
        pw.println("pp set pnfs timeout " + (_pnfs.getTimeout() / 1000L));
    }

    public static final String hh_pp_set_pnfs_timeout = "<Timeout/sec>";
    public synchronized String ac_pp_set_pnfs_timeout_$_1(Args args)
    {
        long timeout = Long.parseLong(args.argv(0));
        _pnfs.setTimeout(timeout * 1000L);
        return "Pnfs timeout set to " + timeout + " seconds";
    }

    public static final String hh_pp_set_max_active = "<normalization>";
    public synchronized String ac_pp_set_max_active_$_1(Args args)
    {
        _maxActive = Integer.parseInt(args.argv(0));
        return "";
    }

    public static final String fh_pp_set_port =
        "Equivalent to calling 'pp set listen * <listenPort>'";
    public static final String hh_pp_set_port = "<listenPort>";
    public synchronized String ac_pp_set_port_$_1(Args args)
    {
        _acceptor.setAddress(new InetSocketAddress(Integer.parseInt(args.argv(0))));
        return "";
    }

    public static final String fh_pp_set_listen =
        "Specifies the interface and port on which to listen for connections\n"+
        "from other pools. Use * to select the wildcard address. If port is\n"+
        "ommitted or set to 0, then a free port is selected from the range\n"+
        "defined by org.dcache.net.tcp.portrange. If the range is not\n"+
        "defined then a free port is selected by the OS.\n\n"+
        "Changes will not have any effect until the pool is idle.";
    public static final String hh_pp_set_listen = "<address> [<port>]";
    public synchronized String ac_pp_set_listen_$_1_2(Args args)
        throws UnknownHostException
    {
        String host = args.argv(0);
        int port = (args.argc() == 2) ? Integer.parseInt(args.argv(1)) : 0;
        InetSocketAddress address = host.equals("*")
            ? new InetSocketAddress(port)
            : new InetSocketAddress(host, port);
        _acceptor.setAddress(address);
        return "";
    }

    public static final String hh_pp_get_file = "<pnfsId> <pool>";
    public synchronized String ac_pp_get_file_$_2(Args args)
        throws CacheException, IOException, InterruptedException
    {
        PnfsId pnfsId = new PnfsId(args.argv(0));
        String pool = args.argv(1);
        List<StickyRecord> stickyRecords = Collections.emptyList();
        newCompanion(pnfsId, pool, null, EntryState.CACHED, stickyRecords, null);
        return "Transfer Initiated";
    }

    public static final String hh_pp_remove = "<id>";
    public synchronized String ac_pp_remove_$_1(Args args)
        throws NumberFormatException
    {
        int id = Integer.valueOf(args.argv(0));
        if (!cancel(id))
            throw new IllegalArgumentException("Id not found: " + id);
        return "";
    }

    public static final String hh_pp_ls = " # get the list of companions";
    public synchronized String ac_pp_ls(Args args)
    {
        StringBuilder sb = new StringBuilder();

        for (Companion c : _companions.values()) {
            sb.append(c.toString()).append("\n");
        }
        return sb.toString();
    }
}
