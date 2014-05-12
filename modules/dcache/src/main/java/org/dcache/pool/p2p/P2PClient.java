// $Id: P2PClient.java,v 1.21 2007-10-31 17:27:11 radicke Exp $

package org.dcache.pool.p2p;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.CacheFileAvailable;
import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.DoorTransferFinishedMessage;
import diskCacheV111.vehicles.HttpDoorUrlInfoMessage;
import diskCacheV111.vehicles.HttpProtocolInfo;

import dmg.cells.nucleus.AbstractCellComponent;
import dmg.cells.nucleus.CellCommandListener;
import dmg.cells.nucleus.CellMessageReceiver;

import org.dcache.cells.CellStub;
import org.dcache.pool.classic.ChecksumModule;
import org.dcache.pool.repository.EntryState;
import org.dcache.pool.repository.Repository;
import org.dcache.pool.repository.StickyRecord;
import org.dcache.util.Args;
import org.dcache.vehicles.FileAttributes;

public class P2PClient
    extends AbstractCellComponent
    implements CellMessageReceiver,
               CellCommandListener
{
    private final static Logger _log = LoggerFactory.getLogger(P2PClient.class);

    private final Map<Integer, Companion> _companions = new HashMap();
    private ScheduledExecutorService _executor;
    private Repository _repository;
    private ChecksumModule _checksumModule;

    private int _maxActive;

    private CellStub _pnfs;
    private CellStub _pool;
    private InetAddress _interface;

    public synchronized void setExecutor(ScheduledExecutorService executor)
    {
        _executor = executor;
    }

    public synchronized void setRepository(Repository repository)
    {
        _repository = repository;
    }

    public synchronized void setChecksumModule(ChecksumModule csm)
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

    public synchronized InetAddress getInterface()
        throws UnknownHostException
    {
        return (_interface == null) ? InetAddress.getLocalHost() : _interface;
    }

    public synchronized void messageArrived(DoorTransferFinishedMessage message)
    {
        HttpProtocolInfo pinfo = (HttpProtocolInfo)message.getProtocolInfo();
        int sessionId = pinfo.getSessionId();
        Companion companion = _companions.get(sessionId);
        if (companion != null) {
            companion.messageArrived(message);
        }
    }

    public synchronized void messageArrived(HttpDoorUrlInfoMessage message)
    {
        int sessionId = (int) message.getId();
        Companion companion = _companions.get(sessionId);
        if (companion != null) {
            companion.messageArrived(message);
        } else {
            /* The original p2p is no longer around, but maybe we can use the redirect
             * for another p2p transfer.
             */
            String pnfsId = message.getPnfsId();
            for (Companion c : _companions.values()) {
                if (c.getPnfsId().equals(pnfsId)) {
                    c.messageArrived(message);
                    return;
                }
            }

            /* TODO: We should kill the mover, but at the moment we don't
             * know the mover id here.
             */
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
            while (_id == -1) {
                wait();
            }
            return _id;
        }

        @Override
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

    public synchronized int newCompanion(String sourcePoolName,
                                         FileAttributes fileAttributes,
                                         EntryState targetState,
                                         List<StickyRecord> stickyRecords,
                                         CacheFileAvailable callback,
                                         boolean forceSourceMode)
        throws IOException, CacheException, InterruptedException
    {
        if (getCellEndpoint() == null) {
            throw new IllegalStateException("Endpoint not initialized");
        }
        if (_pool == null) {
            throw new IllegalStateException("Pool stub not initialized");
        }
        if (_executor == null) {
            throw new IllegalStateException("Executor not initialized");
        }
        if (_repository == null) {
            throw new IllegalStateException("Repository not initialized");
        }
        if (_checksumModule == null) {
            throw new IllegalStateException("Checksum module not initialized");
        }
        if (_pnfs == null) {
            throw new IllegalStateException("PNFS stub not initialized");
        }
        if (_repository.getState(fileAttributes.getPnfsId()) != EntryState.NEW) {
            throw new IllegalStateException("Replica already exists");
        }

        Callback cb = new Callback(callback);

        Companion companion =
            new Companion(_executor, getInterface(), _repository,
                          _checksumModule,
                          _pnfs, _pool,
                          fileAttributes,
                          sourcePoolName,
                          getCellName(),
                          getCellDomainName(),
                          targetState, stickyRecords,
                          cb, forceSourceMode);

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

    @Override
    public synchronized void getInfo(PrintWriter pw)
    {
        try {
            pw.println("  Interface  : " + getInterface());
        } catch (UnknownHostException e) {
            pw.println("  Interface  : " + e.getMessage());
        }
        pw.println("  Max Active : " + _maxActive);
        pw.println("Pnfs Timeout : " + _pnfs.getTimeout() + " " + _pnfs.getTimeoutUnit());
    }

    @Override
    public synchronized void printSetup(PrintWriter pw)
    {
        pw.println("#\n#  Pool to Pool (P2P) [$Revision$]\n#");
        pw.println("pp set max active " + _maxActive);
        pw.println("pp set pnfs timeout " + (_pnfs.getTimeoutInMillis() / 1000L));
        if (_interface != null) {
            pw.println("pp interface " + _interface.getHostAddress());
        }
    }

    public static final String hh_pp_set_pnfs_timeout = "<Timeout/sec>";
    public synchronized String ac_pp_set_pnfs_timeout_$_1(Args args)
    {
        long timeout = Long.parseLong(args.argv(0));
        _pnfs.setTimeout(timeout);
        _pnfs.setTimeoutUnit(TimeUnit.SECONDS);
        return "Pnfs timeout set to " + timeout + " seconds";
    }

    public static final String hh_pp_set_max_active = "<normalization>";
    public synchronized String ac_pp_set_max_active_$_1(Args args)
    {
        _maxActive = Integer.parseInt(args.argv(0));
        return "";
    }

    public static final String hh_pp_set_port = "<port> # Obsolete";
    public synchronized String ac_pp_set_port_$_1(Args args)
    {
        return "'pp set port' is obsolete";
    }

    public static final String fh_pp_set_listen =
        "The command is deprecated. Use 'pp interface' instead.";
    public static final String hh_pp_set_listen = "<address> # Deprecated";
    public synchronized String ac_pp_set_listen_$_1_2(Args args)
        throws UnknownHostException
    {
        return ac_pp_interface_$_0_1(new Args(args.argv(0)));
    }

    public static final String fh_pp_interface =
        "Specifies the interface used when connecting to other pools.\n\n" +
        "For pool to pool transfers, the destination creates a TCP\n" +
        "conection to the source pool. For this to work the source pool\n" +
        "must select one of its network interfaces to which the destination\n" +
        "pool can connect. For compatibility reasons this interface is\n" +
        "not specified explicitly on the source pool. Instead an interface\n" +
        "on the target pool is specified and the source pool selects an\n" +
        "interface facing the target interface.\n\n" +
        "If * is provided then an interface is selected automatically.";
    public static final String hh_pp_interface = "[<address>]";
    public synchronized String ac_pp_interface_$_0_1(Args args)
        throws UnknownHostException
    {
        if (args.argc() == 1) {
            String host = args.argv(0);
            _interface =  host.equals("*") ? null : InetAddress.getByName(host);
        }
        return "PP interface is " + getInterface();
    }

    public static final String hh_pp_get_file = "<pnfsId> <pool>";
    public synchronized String ac_pp_get_file_$_2(Args args)
        throws CacheException, IOException, InterruptedException
    {
        FileAttributes fileAttributes = new FileAttributes();
        fileAttributes.setPnfsId(new PnfsId(args.argv(0)));
        String pool = args.argv(1);
        List<StickyRecord> stickyRecords = Collections.emptyList();
        newCompanion(pool, fileAttributes, EntryState.CACHED, stickyRecords, null,
                false);
        return "Transfer Initiated";
    }

    public static final String hh_pp_remove = "<id>";
    public synchronized String ac_pp_remove_$_1(Args args)
        throws NumberFormatException
    {
        int id = Integer.valueOf(args.argv(0));
        if (!cancel(id)) {
            throw new IllegalArgumentException("Id not found: " + id);
        }
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
