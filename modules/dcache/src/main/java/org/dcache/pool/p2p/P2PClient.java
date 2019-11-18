// $Id: P2PClient.java,v 1.21 2007-10-31 17:27:11 radicke Exp $

package org.dcache.pool.p2p;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetAddress;
import javax.net.ssl.SSLContext;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.CacheFileAvailable;
import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.DoorTransferFinishedMessage;
import diskCacheV111.vehicles.HttpDoorUrlInfoMessage;
import diskCacheV111.vehicles.HttpProtocolInfo;
import dmg.cells.nucleus.AbstractCellComponent;
import dmg.cells.nucleus.CellCommandListener;
import dmg.cells.nucleus.CellInfoProvider;
import dmg.cells.nucleus.CellMessageReceiver;
import dmg.cells.nucleus.CellSetupProvider;
import dmg.util.command.Argument;
import dmg.util.command.Command;
import org.dcache.cells.CellStub;
import org.dcache.pool.PoolDataBeanProvider;
import org.dcache.pool.classic.ChecksumModule;
import org.dcache.pool.repository.ReplicaState;
import org.dcache.pool.repository.Repository;
import org.dcache.pool.repository.StickyRecord;
import org.dcache.vehicles.FileAttributes;
import org.dcache.pool.p2p.json.P2PData;

import static java.util.stream.Collectors.joining;

 enum TlsMode {
    NEVER,
    ALWAYS,
    CROSSZONES

}

public class P2PClient
    extends AbstractCellComponent
    implements CellMessageReceiver, CellCommandListener, CellSetupProvider, CellInfoProvider,
                PoolDataBeanProvider<P2PData>
{
    private final Map<Integer, Companion> _companions = new HashMap<>();
    private ScheduledExecutorService _executor;
    private Repository _repository;
    private ChecksumModule _checksumModule;

    private CellStub _pnfs;
    private CellStub _pool;
    private InetAddress _interface;
    private TlsMode _p2pTlsMode;

    private SSLContext _sslContext;

    // TODO: cross zone behaves as ALYWAYS as long as we can't distinct zones
    private Supplier<SSLContext> getContextIfNeeded = () -> {

        return _p2pTlsMode == TlsMode.NEVER ? null :  _sslContext;
    };


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
        return _companions.size();
    }

    public synchronized void setSslContext(SSLContext sslContext)
    {
        _sslContext = sslContext;
    }

    public synchronized void setTlsMode(TlsMode p2pTlslMode)
    {
        _p2pTlsMode = p2pTlslMode;

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
            PnfsId pnfsId = new PnfsId(message.getPnfsId());
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
        private final CacheFileAvailable _callback;
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
                    cancelCompanions(pnfsId, "Replicated by another transfer");
                }
            } catch (InterruptedException e) {
                // Ignored, typically happens at cell shutdown
            }
        }
    }

    public synchronized int newCompanion(String sourcePoolName,
                                         FileAttributes fileAttributes,
                                         ReplicaState targetState,
                                         List<StickyRecord> stickyRecords,
                                         CacheFileAvailable callback,
                                         boolean forceSourceMode,
                                         Long atime)
        throws IOException, CacheException, InterruptedException, IllegalStateException
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
        ReplicaState state = _repository.getState(fileAttributes.getPnfsId());
        if (state != ReplicaState.NEW) {
            throw new IllegalStateException("Replica exists with state: " + state);
        }



        Callback cb = new Callback(callback);

        Companion companion =
                new Companion(_executor, _interface, _repository,
                        _checksumModule,
                        _pnfs, _pool,
                        fileAttributes,
                        sourcePoolName,
                        getCellName(),
                        getCellDomainName(),
                        targetState, stickyRecords,
                        cb, forceSourceMode,
                        atime,
                        getContextIfNeeded
                        );

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
        return (companion != null) && companion.cancel("Transfer was cancelled");
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
        getDataObject().print(pw);
    }

    @Override
    public synchronized P2PData getDataObject() {
        P2PData info = new P2PData();
        info.setLabel("Pool to Pool");
        info.setPpInterface(_interface);
        return info;
    }

    @Override
    public synchronized void printSetup(PrintWriter pw)
    {
        pw.println("#\n#  Pool to Pool (P2P)\n#");
        if (_interface != null) {
            pw.println("pp interface " + _interface.getHostAddress());
        }
    }

    @Command(name="pp set pnfs timeout",
            hint = "Obsolete Command",
            description = "This command is obsolete.")
    public class PpSetPnfsTimeoutCommand implements Callable<String>
    {
        @Argument
        int timeout;

        @Override
        public String call()
        {
            return "This command is obsolete.";
        }
    }

    @Command(name="pp set max active")
    @Deprecated
    public class PpSetMaxActiveCommand implements Callable<String>
    {
        @Argument
        int maxActiveAllowed;

        @Override
        public String call() throws IllegalArgumentException
        {
            return "";
        }
    }

    @Command(name = "pp set listen",
            hint = "Obsolete Command",
            description = "The command is Obsolete. Use 'pp interface' instead.")
    public class PpSetListenCommand implements Callable<String>
    {
        @Override
        public String call()
        {
            return "This command is obsolete. Use 'pp interface' instead.";
        }
    }

    @AffectsSetup
    @Command(name = "pp interface",
            hint = "Specifies the interface used when connecting to other pools.",
            description = "For pool to pool transfers, the destination creates a TCP " +
                    "connection to the source pool. For this to work the source pool " +
                    "must select one of its network interfaces to which the destination " +
                    "pool can connect. For compatibility reasons this interface is " +
                    "not specified explicitly on the source pool. Instead an interface " +
                    "on the target pool is specified and the source pool selects an " +
                    "interface facing the target interface.\n\n" +
                    "If * is provided then an interface is selected automatically.")
    public class PpInterfaceCommand implements Callable<String>
    {
        @Argument(required = false,
                usage = "Specify the address to which the destination pool can connect.")
        String address;

        @Override
        public String call() throws UnknownHostException
        {
            synchronized (P2PClient.this) {
                if (address != null) {
                    _interface = address.equals("*") ? null : InetAddress.getByName(address);
                }
                return "PP interface is " + ((_interface == null) ? "selected automatically" : _interface) + ".";
            }
        }
    }

    @Command(name = "pp get file",
            hint = "initiate pool-to-pool client transfer request of a file",
            description = "Transfer a file from a specified pool to this pool through " +
                    "pool-to-pool client transfer request. The transferred file will " +
                    "be marked cached.")
    public class PpGetFileCommand implements Callable<String>
    {
        @Argument(index = 0,
                usage = "Specify the pnfsID of the file to transfer.")
        PnfsId pnfsId;

        @Argument(index = 1, metaVar = "sourcePoolName",
                usage = "Specify the source pool name where the file reside.")
        String pool;

        @Override
        public String call() throws
                IOException, CacheException, InterruptedException, IllegalStateException
        {
            List<StickyRecord> stickyRecords = Collections.emptyList();
            newCompanion(pool, FileAttributes.ofPnfsId(pnfsId), ReplicaState.CACHED,
                    stickyRecords, null, false, null);
            return "Transfer Initiated";
        }
    }

    @Command(name = "pp remove",
            hint = "cancel a pool-to-pool client transfer request",
            description = "Terminate a specific pool-to-pool client transfer request by " +
                    "specifying the session ID. This stop the transfer from completion. " +
                    "An error is thrown if the file session ID is not found and " +
                    "this might be due to either the file transfer is completed or " +
                    "the session ID doesn't exist at all.")
    public class PpRemoveCommand implements Callable<String>
    {
        @Argument(metaVar = "sessionID",
                usage = "Specify the session ID identifying the transfer.")
        int id;

        @Override
        public String call() throws IllegalArgumentException
        {
            if (!cancel(id)) {
                throw new IllegalArgumentException("Session ID not found: " + id);
            }
            return "";
        }
    }

    @Command(name = "pp ls",
            hint = "list pool-to-pool client transfer request",
            description = "Get the list of all active and waiting pool-to-pool client " +
                    "transfer request. The return list comprise of: the session ID " +
                    "identifying the transfer; the pnfsID of the file; " +
                    "and the state of the state machine (which is driving the transfer).")
    public class PpLsCommand implements Callable<String>
    {
        @Override
        public String call()
        {
            synchronized (P2PClient.this) {
                return _companions.values().stream().map(Object::toString).collect(joining("\n"));
            }
        }
    }
}