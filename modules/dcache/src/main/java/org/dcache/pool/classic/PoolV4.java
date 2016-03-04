// $Id: MultiProtocolPoolV3.java,v 1.16 2007-10-26 11:17:06 behrmann Exp $

package org.dcache.pool.classic;

import com.google.common.base.Throwables;
import com.google.common.net.InetAddresses;
import dmg.util.command.Argument;
import dmg.util.command.Command;
import dmg.util.command.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.nio.channels.CompletionHandler;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import diskCacheV111.pools.PoolCellInfo;
import diskCacheV111.pools.PoolCostInfo;
import diskCacheV111.pools.PoolV2Mode;
import diskCacheV111.repository.CacheRepositoryEntryInfo;
import diskCacheV111.repository.RepositoryCookie;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.CacheFileAvailable;
import diskCacheV111.util.FileCorruptedCacheException;
import diskCacheV111.util.FileInCacheException;
import diskCacheV111.util.FileNotFoundCacheException;
import diskCacheV111.util.FileNotInCacheException;
import diskCacheV111.util.PnfsHandler;
import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.DCapProtocolInfo;
import diskCacheV111.vehicles.IoJobInfo;
import diskCacheV111.vehicles.JobInfo;
import diskCacheV111.vehicles.Message;
import diskCacheV111.vehicles.Pool2PoolTransferMsg;
import diskCacheV111.vehicles.PoolAcceptFileMessage;
import diskCacheV111.vehicles.PoolCheckFreeSpaceMessage;
import diskCacheV111.vehicles.PoolCheckable;
import diskCacheV111.vehicles.PoolDeliverFileMessage;
import diskCacheV111.vehicles.PoolFetchFileMessage;
import diskCacheV111.vehicles.PoolFileCheckable;
import diskCacheV111.vehicles.PoolIoFileMessage;
import diskCacheV111.vehicles.PoolManagerPoolUpMessage;
import diskCacheV111.vehicles.PoolMgrReplicateFileMsg;
import diskCacheV111.vehicles.PoolModifyModeMessage;
import diskCacheV111.vehicles.PoolModifyPersistencyMessage;
import diskCacheV111.vehicles.PoolMoverKillMessage;
import diskCacheV111.vehicles.PoolQueryRepositoryMsg;
import diskCacheV111.vehicles.PoolRemoveFilesFromHSMMessage;
import diskCacheV111.vehicles.PoolRemoveFilesMessage;
import diskCacheV111.vehicles.PoolSetStickyMessage;
import diskCacheV111.vehicles.PoolUpdateCacheStatisticsMessage;
import diskCacheV111.vehicles.ProtocolInfo;
import diskCacheV111.vehicles.RemoveFileInfoMessage;
import diskCacheV111.vehicles.StorageInfo;

import dmg.cells.nucleus.AbstractCellComponent;
import dmg.cells.nucleus.CellCommandListener;
import dmg.cells.nucleus.CellInfo;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellMessageReceiver;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.CellVersion;
import dmg.cells.nucleus.DelayedReply;
import dmg.cells.nucleus.Reply;
import dmg.util.CommandSyntaxException;

import org.dcache.alarms.AlarmMarkerFactory;
import org.dcache.alarms.PredefinedAlarm;
import org.dcache.cells.CellStub;
import org.dcache.pool.FaultEvent;
import org.dcache.pool.FaultListener;
import org.dcache.pool.movers.Mover;
import org.dcache.pool.movers.MoverFactory;
import org.dcache.pool.nearline.HsmSet;
import org.dcache.pool.nearline.NearlineStorageHandler;
import org.dcache.pool.p2p.P2PClient;
import org.dcache.pool.repository.AbstractStateChangeListener;
import org.dcache.pool.repository.Account;
import org.dcache.pool.repository.CacheEntry;
import org.dcache.pool.repository.EntryState;
import org.dcache.pool.repository.IllegalTransitionException;
import org.dcache.pool.repository.ReplicaDescriptor;
import org.dcache.pool.repository.Repository;
import org.dcache.pool.repository.SpaceRecord;
import org.dcache.pool.repository.StateChangeEvent;
import org.dcache.pool.repository.StickyRecord;
import org.dcache.pool.repository.v5.CacheRepositoryV5;
import org.dcache.util.Args;
import org.dcache.util.IoPriority;
import org.dcache.util.Version;
import org.dcache.vehicles.FileAttributes;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

public class PoolV4
    extends AbstractCellComponent
    implements FaultListener,
               CellCommandListener,
               CellMessageReceiver
{
    private static final int DUP_REQ_NONE = 0;
    private static final int DUP_REQ_IGNORE = 1;
    private static final int DUP_REQ_REFRESH = 2;

    private static final int P2P_CACHED = 1;
    private static final int P2P_PRECIOUS = 2;
    private static final int HEARTBEAT = 30;

    private static final double DEFAULT_BREAK_EVEN = 0.7;

    private static final Pattern TAG_PATTERN =
        Pattern.compile("([^=]+)=(\\S*)\\s*");

    /**
     * The name of a queue used by pool-to-pool transfers.
     */
    private static final String P2P_QUEUE_NAME= "p2p";

    private static final Logger _log = LoggerFactory.getLogger(PoolV4.class);

    private final String _poolName;

    /**
     * pool start time identifier.
     * used by PoolManager to recognize pool restarts
     */
    private final long _serialId = System.currentTimeMillis();
    private static final CellVersion VERSION = new CellVersion(Version.of(PoolV4.class));
    private PoolV2Mode _poolMode;
    private boolean _reportOnRemovals;
    private boolean _suppressHsmLoad;
    private boolean _cleanPreciousFiles;
    private String     _poolStatusMessage = "OK";
    private int        _poolStatusCode;

    private PnfsHandler _pnfs;
    private StorageClassContainer _storageQueue;
    private CacheRepositoryV5 _repository;

    private Account _account;

    private String _poolupDestination;

    private int _version = 4;
    private CellStub _billingStub;
    private final Map<String, String> _tags = new HashMap<>();
    private String _baseDir;

    private final PoolManagerPingThread _pingThread ;
    private HsmFlushController _flushingThread;
    private IoQueueManager _ioQueue ;
    private HsmSet _hsmSet;
    private NearlineStorageHandler _storageHandler;
    private int _p2pFileMode = P2P_CACHED;
    private int _dupRequest = DUP_REQ_IGNORE;
    private P2PClient _p2pClient;

    private boolean _isVolatile;
    private boolean _hasTapeBackend = true;

    private int _cleaningInterval = 60;

    private final Object _hybridInventoryLock = new Object();
    private boolean _hybridInventoryActive;
    private int _hybridCurrent;

    private ChecksumModule _checksumModule;
    private final ReplicationHandler _replicationHandler = new ReplicationHandler();

    private ReplicaStatePolicy _replicaStatePolicy;

    private CellPath _replicationManager;
    private InetAddress _replicationIp;

    private boolean _running;
    private double _breakEven = DEFAULT_BREAK_EVEN;
    private double _moverCostFactor = 0.5;
    private TransferServices _transferServices;

    public PoolV4(String poolName)
    {
        _poolName = poolName;

        _log.info("Pool " + poolName + " starting");

        //
        // repository and ping thread must exist BEFORE the setup
        // file is scanned. PingThread will be started after all
        // the setup is done.
        //
        _pingThread = new PoolManagerPingThread();
    }

    protected void assertNotRunning(String error)
    {
        checkState(!_running, error);
    }

    @Required
    public void setBaseDir(String baseDir)
    {
        assertNotRunning("Cannot change base dir after initialisation");
        _baseDir = baseDir;
    }

    @Required
    public void setVersion(int version)
    {
        _version = version;
    }

    @Required
    public void setReplicationNotificationDestination(String address)
    {
        _replicationManager = (!address.isEmpty()) ? new CellPath(address) : null;
    }

    @Required
    public void setReplicationIp(String address)
    {
        if (!address.isEmpty()) {
            _replicationIp = InetAddresses.forString(address);
        } else {
            try {
                _replicationIp = InetAddress.getLocalHost();
            } catch (UnknownHostException ee) {
                _replicationIp = InetAddress.getLoopbackAddress();

            }
        }
    }

    @Required
    public void setAllowCleaningPreciousFiles(boolean allow)
    {
        _cleanPreciousFiles = allow;
    }

    @Required
    public void setVolatile(boolean isVolatile)
    {
        _isVolatile = isVolatile;
    }

    public boolean isVolatile()
    {
        return _isVolatile;
    }

    @Required
    public void setHasTapeBackend(boolean hasTapeBackend)
    {
        _hasTapeBackend = hasTapeBackend;
    }

    public boolean getHasTapeBackend()
    {
        return _hasTapeBackend;
    }

    public void setP2PMode(String mode)
    {
        if (mode == null) {
            _p2pFileMode = P2P_CACHED;
        } else if (mode.equals("precious")) {
            _p2pFileMode = P2P_PRECIOUS;
        } else if (mode.equals("cached")) {
            _p2pFileMode = P2P_CACHED;
        } else {
            throw new IllegalArgumentException("p2p=precious|cached");
        }
    }

    public void setDuplicateRequestMode(String mode)
    {
        if (mode == null || mode.equals("none")) {
            _dupRequest = DUP_REQ_NONE;
        } else if (mode.equals("ignore")) {
            _dupRequest = DUP_REQ_IGNORE;
        } else if (mode.equals("refresh")) {
            _dupRequest = DUP_REQ_REFRESH;
        } else {
            throw new IllegalArgumentException("Illegal 'dupRequest' value");
        }
    }

    @Required
    public void setPoolUpDestination(String name)
    {
        _poolupDestination = name;
    }

    @Required
    public void setBillingStub(CellStub stub)
    {
        assertNotRunning("Cannot set billing stub after initialization");
        _billingStub = stub;
    }

    @Required
    public void setPnfsHandler(PnfsHandler pnfs)
    {
        assertNotRunning("Cannot set PNFS handler after initialization");
        _pnfs = pnfs;
    }

    @Required
    public void setRepository(CacheRepositoryV5 repository)
    {
        assertNotRunning("Cannot set repository after initialization");
        if (_repository != null) {
            _repository.removeFaultListener(this);
        }
        _repository = repository;
        _repository.addFaultListener(this);
        _repository.addListener(new RepositoryLoader());
        _repository.addListener(new NotifyBillingOnRemoveListener());
        _repository.addListener(new HFlagMaintainer());
        _repository.addListener(_replicationHandler);
    }

    @Required
    public void setAccount(Account account)
    {
        assertNotRunning("Cannot set account after initialization");
        _account = account;
    }

    @Required
    public void setChecksumModule(ChecksumModule module)
    {
        assertNotRunning("Cannot set checksum module after initialization");
        _checksumModule = module;
    }

    @Required
    public void setStorageQueue(StorageClassContainer queue)
    {
        assertNotRunning("Cannot set storage queue after initialization");
        _storageQueue = queue;
    }

    @Required
    public void setStorageHandler(NearlineStorageHandler handler)
    {
        assertNotRunning("Cannot set storage handler after initialization");
        _storageHandler = handler;
    }

    @Required
    public void setHSMSet(HsmSet set)
    {
        assertNotRunning("Cannot set HSM set after initialization");
        _hsmSet = set;
    }

    @Required
    public void setFlushController(HsmFlushController controller)
    {
        assertNotRunning("Cannot set flushing controller after initialization");
        _flushingThread = controller;
    }

    @Required
    public void setPPClient(P2PClient client)
    {
        assertNotRunning("Cannot set P2P client after initialization");
        _p2pClient = client;
    }

    @Required
    public void setReplicaStatePolicy(ReplicaStatePolicy replicaStatePolicy)
    {
        assertNotRunning("Cannot set replica state policy after initialization");
        _replicaStatePolicy = replicaStatePolicy;
    }

    @Required
    public void setTags(String tags)
    {
        Map<String,String> newTags = new HashMap<>();
        Matcher matcher = TAG_PATTERN.matcher(tags);
        while (matcher.lookingAt()) {
            String tag = matcher.group(1);
            String value = matcher.group(2);
            newTags.put(tag, value);
            matcher.region(matcher.end(), matcher.regionEnd());
        }

        if (matcher.regionStart() != matcher.regionEnd()) {
            String msg = "Cannot parse '" + tags.substring(matcher.regionStart()) + "'";
            throw new IllegalArgumentException(msg);
        }

        for (Map.Entry<String,String> e: newTags.entrySet()) {
            _tags.put(e.getKey(), e.getValue());
            _log.info("Tag: " + e.getKey() + "="+ e.getValue());
        }
    }

    @Required
    public void setIoQueueManager(IoQueueManager ioQueueManager)
    {
        assertNotRunning("Cannot set I/O queue manager after initialization");
        _ioQueue = ioQueueManager;
    }

    @Required
    public void setPoolMode(PoolV2Mode mode)
    {
        _poolMode = mode;
    }

    @Required
    public void setTransferServices(TransferServices transferServices)
    {
        assertNotRunning("Cannot set transfer services after initialization");
        _transferServices = transferServices;
    }

    public void init()
    {
        assertNotRunning("Cannot initialize several times");
        checkState(!_isVolatile || !_hasTapeBackend, "Volatile pool cannot have a tape backend");
        _running = true;
    }

    @Override
    public void afterStart()
    {
        disablePool(PoolV2Mode.DISABLED_STRICT, 1, "Awaiting initialization");
        _pingThread.start();
        new Thread() {
            @Override
            public void run() {
                try {
                    _repository.init();
                    disablePool(PoolV2Mode.DISABLED_RDONLY, 1, "Loading...");
                    _repository.load();
                    enablePool();
                    _flushingThread.start();
                } catch (RuntimeException e) {
                    _log.error(AlarmMarkerFactory.getMarker
                                    (PredefinedAlarm.POOL_DISABLED, _poolName),
                                     "Pool {} initialization failed, repository "
                                     + "reported a problem."
                                     + "Please report this to support@dcache.org.",
                                     _poolName, e);
                    _log.warn("Pool not enabled {}", _poolName);
                    disablePool(PoolV2Mode.DISABLED_DEAD | PoolV2Mode.DISABLED_STRICT,
                            666, "Init failed: " + e.getMessage());
                } catch (Throwable e) {
                    _log.error(AlarmMarkerFactory.getMarker
                                    (PredefinedAlarm.POOL_DISABLED, _poolName),
                                     "Pool {} initialization failed, repository "
                                     + "reported a problem ({}).",
                                     _poolName, e.getMessage());
                    _log.warn("Pool not enabled {}", _poolName);
                    disablePool(PoolV2Mode.DISABLED_DEAD | PoolV2Mode.DISABLED_STRICT,
                                666, "Init failed: " + e.getMessage());
                }

                _log.info("Repository finished");
            }
        }.start();
    }

    @Override
    public void beforeStop()
    {
        _flushingThread.stop();
        _pingThread.stop();

        /*
         * No need for alarm here.
         */
        disablePool(PoolV2Mode.DISABLED_DEAD | PoolV2Mode.DISABLED_STRICT,
                666, "Shutdown");
    }

    /**
     * Called by subsystems upon serious faults.
     */
    @Override
    public void faultOccurred(FaultEvent event)
    {
        Throwable cause = event.getCause();
        String poolState;
        switch (event.getAction()) {
        case READONLY:
            poolState = "Pool read-only: ";
            disablePool(PoolV2Mode.DISABLED_RDONLY, 99,
                        poolState + event.getMessage());
            break;

        case DISABLED:
            poolState = "Pool disabled: ";
            disablePool(PoolV2Mode.DISABLED_STRICT, 99,
                        poolState + event.getMessage());
            break;

        default:
            poolState = "Pool restart required: ";
            disablePool(PoolV2Mode.DISABLED_STRICT | PoolV2Mode.DISABLED_DEAD, 666,
                        poolState + event.getMessage());
            break;
        }

        String message = "Fault occurred in " + event.getSource() + ": "
                        + event.getMessage() +". " + poolState;

        if (cause != null) {
            _log.error(AlarmMarkerFactory.getMarker(PredefinedAlarm.POOL_DISABLED,
                            _poolName),
                            message,
                            cause);
        } else {
            _log.error(AlarmMarkerFactory.getMarker(PredefinedAlarm.POOL_DISABLED,
                            _poolName),
                            message);
        }
    }

    /**
     * Sets the h-flag in PNFS.
     */
    private class HFlagMaintainer extends AbstractStateChangeListener
    {
        @Override
        public void stateChanged(StateChangeEvent event)
        {
            if (event.getOldState() == EntryState.FROM_CLIENT) {
                PnfsId id = event.getPnfsId();
                if (_hasTapeBackend) {
                    _pnfs.putPnfsFlag(id, "h", "yes");
                } else {
                    _pnfs.putPnfsFlag(id, "h", "no");
                }
            }
        }
    }

    /**
     * Interface between the repository and the StorageQueueContainer.
     */
    private class RepositoryLoader extends AbstractStateChangeListener
    {
        @Override
        public void stateChanged(StateChangeEvent event)
        {
            PnfsId id = event.getPnfsId();
            EntryState from = event.getOldState();
            EntryState to = event.getNewState();

            if (from == to) {
                return;
            }

            if (to == EntryState.PRECIOUS) {
                _log.debug("Adding {} to flush queue", id);

                if (_hasTapeBackend) {
                    try {
                        _storageQueue.addCacheEntry(event.getNewEntry());
                    } catch (FileNotInCacheException e) {
                        /* File was deleted before we got a chance to do
                         * anything with it. We don't care about deleted
                         * files so we ignore this.
                         */
                        _log.info("Failed to flush " + id + ": Replica is no longer in the pool", e);
                    } catch (CacheException | InterruptedException e) {
                        _log.error("Error adding {} to flush queue: {}", id, e.getMessage());
                    }
                }
            } else if (from == EntryState.PRECIOUS) {
                _log.debug("Removing {} from flush queue", id);
                if (!_storageQueue.removeCacheEntry(id)) {
                    _log.info("File {} not found in flush queue", id);
                }
            }
        }
    }

    private class NotifyBillingOnRemoveListener
        extends AbstractStateChangeListener
    {
        @Override
        public void stateChanged(StateChangeEvent event)
        {
            if (_reportOnRemovals && event.getNewState() == EntryState.REMOVED) {
                CacheEntry entry = event.getNewEntry();
                RemoveFileInfoMessage msg =
                    new RemoveFileInfoMessage(getCellAddress().toString(), entry.getPnfsId());
                msg.setFileSize(entry.getReplicaSize());
                msg.setStorageInfo(entry.getFileAttributes().getStorageInfo());
                _billingStub.notify(msg);
            }
        }
    }

    @Override
    public void printSetup(PrintWriter pw)
    {
        pw.println("set heartbeat " + _pingThread.getHeartbeat());
        pw.println("set report remove " + (_reportOnRemovals ? "on" : "off"));
        pw.println("set breakeven " + _breakEven);
        pw.println("set mover cost factor " + _moverCostFactor);
        if (_suppressHsmLoad) {
            pw.println("pool suppress hsmload on");
        }
        pw.println("set duplicate request "
                + ((_dupRequest == DUP_REQ_NONE)
                ? "none"
                : (_dupRequest == DUP_REQ_IGNORE)
                ? "ignore"
                      : "refresh"));
        _ioQueue.printSetup(pw);
    }

    @Override
    public CellInfo getCellInfo(CellInfo info)
    {
        PoolCellInfo poolinfo = new PoolCellInfo(info);
        poolinfo.setPoolCostInfo(getPoolCostInfo());
        poolinfo.setTagMap(_tags);
        poolinfo.setErrorStatus(_poolStatusCode, _poolStatusMessage);
        poolinfo.setCellVersion(VERSION);
        return poolinfo;
    }

    @Override
    public void getInfo(PrintWriter pw)
    {
        pw.println("Base directory    : " + _baseDir);
        pw.println("Version           : " + VERSION + " (Sub="
                + _version + ")");
        pw.println("Report remove     : " + (_reportOnRemovals ? "on" : "off"));
        pw.println("Pool Mode         : " + _poolMode);
        if (_poolMode.isDisabled()) {
            pw.println("Detail            : [" + _poolStatusCode + "] "
                       + _poolStatusMessage);
        }
        pw.println("Clean prec. files : "
                   + (_cleanPreciousFiles ? "on" : "off"));
        pw.println("Hsm Load Suppr.   : " + (_suppressHsmLoad ? "on" : "off"));
        pw.println("Ping Heartbeat    : " + _pingThread.getHeartbeat()
                   + " seconds");
        pw.println("Breakeven         : " + getBreakEven());
        pw.println("ReplicationMgr    : " + _replicationHandler);
        if (_hasTapeBackend) {
            pw.println("LargeFileStore    : None");
        } else if (_isVolatile) {
            pw.println("LargeFileStore    : Volatile");
        } else {
            pw.println("LargeFileStore    : Precious");
        }
        pw.println("DuplicateRequests : "
                   + ((_dupRequest == DUP_REQ_NONE)
                      ? "None"
                      : (_dupRequest == DUP_REQ_IGNORE)
                      ? "Ignored"
                      : "Refreshed"));
        pw.println("P2P File Mode     : "
                   + ((_p2pFileMode == P2P_PRECIOUS) ? "Precious" : "Cached"));

        if (_hybridInventoryActive) {
            pw.println("Inventory         : " + _hybridCurrent);
        }

        for (MoverRequestScheduler js : _ioQueue.getQueues()) {
            pw.println("Mover Queue (" + js.getName() + ") "
                       + js.getActiveJobs() + "(" + js.getMaxActiveJobs()
                       + ")/" + js.getQueueSize());
        }
    }

    // //////////////////////////////////////////////////////////////
    //
    // The io File Part
    //
    //

    public Mover<?> createMover(CellMessage envelop, PoolIoFileMessage message) throws CacheException
    {
        CellPath source = envelop.getSourcePath().revert();
        FileAttributes attributes = message.getFileAttributes();
        PnfsId pnfsId = attributes.getPnfsId();
        ProtocolInfo pi = message.getProtocolInfo();

        MoverFactory moverFactory = _transferServices.getMoverFactory(pi);
        ReplicaDescriptor handle;
        try {
            if (message instanceof PoolAcceptFileMessage) {
                List<StickyRecord> stickyRecords =
                        _replicaStatePolicy.getStickyRecords(attributes);
                EntryState targetState =
                        _replicaStatePolicy.getTargetState(attributes);
                handle = _repository.createEntry(attributes,
                        EntryState.FROM_CLIENT,
                        targetState,
                        stickyRecords,
                        EnumSet.of(Repository.OpenFlags.CREATEFILE));
            } else {
                Set<Repository.OpenFlags> openFlags =
                        message.isPool2Pool()
                                ? EnumSet.of(Repository.OpenFlags.NOATIME)
                                : EnumSet.noneOf(Repository.OpenFlags.class);
                handle = _repository.openEntry(pnfsId, openFlags);
            }
        } catch (FileNotInCacheException e) {
            throw new FileNotInCacheException("File " + pnfsId + " does not exist in " + _poolName, e);
        } catch (FileInCacheException e) {
            throw new FileInCacheException("File " + pnfsId + " already exists in " + _poolName, e);
        } catch (InterruptedException e) {
            throw new CacheException("Pool is shutting down", e);
        }
        try {
            return moverFactory.createMover(handle, message, source);
        } catch (Throwable t) {
            handle.close();
            throw Throwables.propagate(t);
        }
    }

    private int queueIoRequest(CellMessage envelope, PoolIoFileMessage message) throws CacheException
    {
        String queueName = message.getIoQueueName();
        String doorUniqueId = envelope.getSourceAddress().toString() + message.getId();

        if (message instanceof PoolAcceptFileMessage) {
            return _ioQueue.getOrCreateMover(queueName, doorUniqueId, () -> createMover(envelope, message), IoPriority.HIGH);
        } else if (message.isPool2Pool()) {
            return _ioQueue.getOrCreateMover(P2P_QUEUE_NAME, doorUniqueId, () -> createMover(envelope, message), IoPriority.HIGH);
        } else {
            return _ioQueue.getOrCreateMover(queueName, doorUniqueId, () -> createMover(envelope, message), IoPriority.REGULAR);
        }
    }

    private void ioFile(CellMessage envelope, PoolIoFileMessage message)
    {
        try {
            message.setMoverId(queueIoRequest(envelope, message));
            message.setSucceeded();
        } catch (CacheException e) {
            _log.error(e.getMessage());
            message.setFailed(e.getRc(), e.getMessage());
        } catch (RuntimeException e) {
            _log.error("Possible bug found: " + e.getMessage(), e);
            message.setFailed(CacheException.DEFAULT_ERROR_CODE,
                              "Failed to enqueue mover: " + e.getMessage());
        }
        envelope.revertDirection();
        sendMessage(envelope);
    }

    // //////////////////////////////////////////////////////////////
    //
    // replication on data arrived
    //
    private class ReplicationHandler
        extends AbstractStateChangeListener
    {
        @Override
        public void stateChanged(StateChangeEvent event)
        {
            EntryState from = event.getOldState();
            EntryState to = event.getNewState();

            if (to == EntryState.CACHED || to == EntryState.PRECIOUS) {
                switch (from) {
                case FROM_CLIENT:
                    initiateReplication(event.getPnfsId(), "write");
                    break;
                case FROM_STORE:
                    initiateReplication(event.getPnfsId(), "restore");
                    break;
                }
            }
        }

        @Override
        public String toString()
        {
            if (_replicationManager != null) {
                return "{Mgr=" + _replicationManager + ",Host=" + _replicationIp + "}";
            } else {
                return "Disabled";
            }
        }

        private void initiateReplication(PnfsId id, String source)
        {
            if (_replicationManager != null) {
                try {
                    _initiateReplication(_repository.getEntry(id), source);
                } catch (InterruptedException e) {
                    _log.error("Problem in sending replication request: " + e);
                    Thread.currentThread().interrupt();
                } catch (CacheException e) {
                    _log.error("Problem in sending replication request: " + e);
                }
            }
        }

        private void _initiateReplication(CacheEntry entry, String source)
        {
            PnfsId pnfsId = entry.getPnfsId();
            FileAttributes fileAttributes = entry.getFileAttributes();
            StorageInfo storageInfo = fileAttributes.getStorageInfo().clone();

            storageInfo.setKey("replication.source", source);

            FileAttributes attributes = new FileAttributes();
            attributes.setPnfsId(pnfsId);
            attributes.setStorageInfo(storageInfo);
            attributes.setLocations(Collections.singleton(_poolName));
            attributes.setSize(fileAttributes.getSize());
            attributes.setAccessLatency(fileAttributes.getAccessLatency());
            attributes.setRetentionPolicy(fileAttributes.getRetentionPolicy());

            PoolMgrReplicateFileMsg req =
                new PoolMgrReplicateFileMsg(attributes,
                                            new DCapProtocolInfo("DCap", 3, 0,
                                                    new InetSocketAddress(_replicationIp, 2222)));
            req.setReplyRequired(false);
            sendMessage(new CellMessage(_replicationManager, req));
        }
    }


    // //////////////////////////////////////////////////////////////////////////
    //
    // interface to the HsmRestoreHandler
    //
    private class ReplyToPoolFetch
        extends DelayedReply
        implements CompletionHandler<Void, PnfsId>
    {
        private final PoolFetchFileMessage _message;

        private ReplyToPoolFetch(PoolFetchFileMessage message)
        {
            _message = message;
        }

        @Override
        public void completed(Void result, PnfsId pnfsId)
        {
            _message.setSucceeded();
            reply(_message);
        }

        @Override
        public void failed(Throwable exc, PnfsId pnfsId)
        {
            if (exc instanceof CacheException) {
                CacheException ce = (CacheException) exc;
                int errorCode = ce.getRc();
                switch (errorCode) {
                case CacheException.FILE_IN_CACHE:
                    _log.info("Pool already contains replica");
                    _message.setSucceeded();
                    break;
                case CacheException.ERROR_IO_DISK:
                case 41:
                case 42:
                case 43:
                    disablePool(PoolV2Mode.DISABLED_STRICT, errorCode, ce.getMessage());
                    _log.error(AlarmMarkerFactory.getMarker(PredefinedAlarm.POOL_DISABLED,
                                                            _poolName),
                                    "Error encountered during fetch of {}",
                                    pnfsId,
                                    ce.getMessage());
                    _message.setFailed(errorCode, ce.getMessage());
                    break;
                default:
                    _message.setFailed(errorCode, ce.getMessage());
                    break;
                }
            } else {
                _message.setFailed(1000, exc);
            }
            reply(_message);
        }
    }

    private void checkFile(PoolFileCheckable poolMessage)
        throws CacheException, InterruptedException
    {
        PnfsId id = poolMessage.getPnfsId();
        switch (_repository.getState(id)) {
        case PRECIOUS:
        case CACHED:
            poolMessage.setHave(true);
            poolMessage.setWaiting(false);
            break;
        case FROM_CLIENT:
        case FROM_STORE:
        case FROM_POOL:
            poolMessage.setHave(false);
            poolMessage.setWaiting(true);
            break;
        case BROKEN:
            throw new FileCorruptedCacheException(id.toString() + " is broken in " + _poolName);
        default:
            poolMessage.setHave(false);
            poolMessage.setWaiting(false);
            break;
        }
    }

    private class CompanionFileAvailableCallback
        extends DelayedReply
        implements CacheFileAvailable
    {
        private final Pool2PoolTransferMsg _message;

        private CompanionFileAvailableCallback(Pool2PoolTransferMsg message)
        {
            _message = message;
        }

        @Override
        public void cacheFileAvailable(PnfsId pnfsId, Throwable error)
        {
            if (_message.getReplyRequired()) {
                if (error == null) {
                    _message.setSucceeded();
                } else if (error instanceof FileInCacheException) {
                    _message.setReply(0, null);
                } else if (error instanceof CacheException) {
                    _message.setReply(((CacheException) error).getRc(), error);
                } else {
                    _message.setReply(CacheException.UNEXPECTED_SYSTEM_EXCEPTION, error);
                }

                reply(_message);
            }
        }
    }

    public PoolMoverKillMessage messageArrived(PoolMoverKillMessage kill)
    {
        if (kill.isReply()) {
            return null;
        }

        try {
            int id = kill.getMoverId();
            MoverRequestScheduler js = _ioQueue.getQueueByJobId(id);
            mover_kill(js, id);
            kill.setSucceeded();
        } catch (NoSuchElementException e) {
            _log.info(e.toString());
            kill.setReply(1, e);
        }
        return kill;
    }

    public void messageArrived(CellMessage envelope, PoolIoFileMessage msg)
        throws CacheException
    {
        if (msg.isReply()) {
            return;
        }

        if ((msg instanceof PoolAcceptFileMessage
             && _poolMode.isDisabled(PoolV2Mode.DISABLED_STORE))
            || (msg instanceof PoolDeliverFileMessage
                && _poolMode.isDisabled(PoolV2Mode.DISABLED_FETCH))) {

            if (!msg.isForceSourceMode()) {
                _log.warn("PoolIoFileMessage request rejected due to "
                          + _poolMode);
                throw new CacheException(CacheException.POOL_DISABLED, "Pool is disabled");
            }
        }

        msg.setReply();
        ioFile(envelope, msg);
    }

    public DelayedReply messageArrived(Pool2PoolTransferMsg msg)
        throws CacheException, IOException, InterruptedException
    {
        if (_poolMode.isDisabled(PoolV2Mode.DISABLED_P2P_CLIENT)) {
            _log.warn("Pool2PoolTransferMsg request rejected due to "
                       + _poolMode);
            throw new CacheException(CacheException.POOL_DISABLED, "Pool is disabled");
        }

        String poolName = msg.getPoolName();
        FileAttributes fileAttributes = msg.getFileAttributes();
        CompanionFileAvailableCallback callback =
            new CompanionFileAvailableCallback(msg);

        EntryState targetState = EntryState.CACHED;
        int fileMode = msg.getDestinationFileStatus();
        if (fileMode != Pool2PoolTransferMsg.UNDETERMINED) {
            if (fileMode == Pool2PoolTransferMsg.PRECIOUS) {
                targetState = EntryState.PRECIOUS;
            }
        } else if (!_hasTapeBackend && !_isVolatile
                   && (_p2pFileMode == P2P_PRECIOUS)) {
            targetState = EntryState.PRECIOUS;
        }

        List<StickyRecord> stickyRecords = Collections.emptyList();
        _p2pClient.newCompanion(poolName, fileAttributes,
                                targetState, stickyRecords, callback, false, null);
        return callback;
    }

    public Object messageArrived(PoolFetchFileMessage msg)
        throws CacheException
    {
        if (_poolMode.isDisabled(PoolV2Mode.DISABLED_STAGE)) {
            _log.warn("PoolFetchFileMessage request rejected due to "
                       + _poolMode);
            throw new CacheException(CacheException.POOL_DISABLED, "Pool is disabled");
        }
        if (!_hasTapeBackend) {
            _log.warn("PoolFetchFileMessage request rejected due to LFS mode");
            throw new CacheException(CacheException.POOL_DISABLED, "Pool has no tape backend");
        }

        FileAttributes fileAttributes = msg.getFileAttributes();
        String hsm = _hsmSet.getInstanceName(fileAttributes);
        _log.info("Pool {} asked to fetch file {} (hsm={})",
                _poolName, fileAttributes.getPnfsId(), hsm);
        ReplyToPoolFetch reply = new ReplyToPoolFetch(msg);
        _storageHandler.stage(hsm, fileAttributes, reply);
        return reply;
    }

    private class RemoveFileReply extends DelayedReply implements CompletionHandler<Void,URI>
    {
        private final PoolRemoveFilesFromHSMMessage msg;
        private final Collection<URI> succeeded;
        private final Collection<URI> failed;

        private RemoveFileReply(PoolRemoveFilesFromHSMMessage msg)
        {
            this.msg = msg;
            succeeded = new ArrayList<>(msg.getFiles().size());
            failed = new ArrayList<>();
        }

        @Override
        public void completed(Void nil, URI uri)
        {
            succeeded.add(uri);
            sendIfFinished();
        }

        @Override
        public void failed(Throwable exc, URI uri)
        {
            failed.add(uri);
            sendIfFinished();
        }

        private void sendIfFinished()
        {
            if (succeeded.size() + failed.size() >= msg.getFiles().size()) {
                msg.setResult(succeeded, failed);
                reply(msg);
            }
        }
    }

    public Reply messageArrived(PoolRemoveFilesFromHSMMessage msg)
        throws CacheException
    {
        if (_poolMode.isDisabled(PoolV2Mode.DISABLED_STAGE)) {
            _log.warn("PoolRemoveFilesFromHsmMessage request rejected due to "
                      + _poolMode);
            throw new CacheException(CacheException.POOL_DISABLED, "Pool is disabled");
        }
        if (!_hasTapeBackend) {
            _log.warn("PoolRemoveFilesFromHsmMessage request rejected due to LFS mode");
            throw new CacheException(CacheException.POOL_DISABLED, "Pool has no tape backend");
        }

        RemoveFileReply reply = new RemoveFileReply(msg);
        _storageHandler.remove(msg.getHsm(), msg.getFiles(), reply);
        return reply;
    }

    public PoolCheckFreeSpaceMessage
        messageArrived(PoolCheckFreeSpaceMessage msg)
    {
        msg.setFreeSpace(_account.getFree());
        msg.setSucceeded();
        return msg;
    }

    public Message messageArrived(Message msg)
        throws CacheException, InterruptedException
    {
        if (msg instanceof PoolCheckable) {
            if (msg instanceof PoolFileCheckable) {
                checkFile((PoolFileCheckable) msg);
            }

            msg.setSucceeded();
            return msg;
        }

        return null;
    }

    public PoolUpdateCacheStatisticsMessage
        messageArrived(PoolUpdateCacheStatisticsMessage msg)
    {
        msg.setSucceeded();
        return msg;
    }

    public PoolRemoveFilesMessage messageArrived(PoolRemoveFilesMessage msg)
        throws CacheException, InterruptedException
    {
        if (_poolMode.isDisabled(PoolV2Mode.DISABLED)) {
            _log.warn("PoolRemoveFilesMessage request rejected due to "
                      + _poolMode);
            throw new CacheException(CacheException.POOL_DISABLED, "Pool is disabled");
        }

        String[] fileList = msg.getFiles();
        int counter = 0;
        for (int i = 0; i < fileList.length; i++) {
            try {
                PnfsId pnfsId = new PnfsId(fileList[i]);
                if (!_cleanPreciousFiles && _hasTapeBackend
                    && (_repository.getState(pnfsId) == EntryState.PRECIOUS)) {
                    counter++;
                    _log.error("Replica " + fileList[i] + " kept (precious)");
                } else {
                    _repository.setState(pnfsId, EntryState.REMOVED);
                    fileList[i] = null;
                }
            } catch (IllegalTransitionException e) {
                _log.error("Replica " + fileList[i] + " not removed: "
                           + e.getMessage());
                counter++;
            }
        }
        if (counter > 0) {
            String[] replyList = new String[counter];
            for (int i = 0, j = 0; i < fileList.length; i++) {
                if (fileList[i] != null) {
                    replyList[j++] = fileList[i];
                }
            }
            msg.setFailed(1, replyList);
        } else {
            msg.setSucceeded();
        }

        return msg;
    }

    public PoolModifyPersistencyMessage
        messageArrived(PoolModifyPersistencyMessage msg)
    {
        try {
            PnfsId pnfsId = msg.getPnfsId();
            switch (_repository.getState(pnfsId)) {
            case PRECIOUS:
                if (msg.isCached()) {
                    _repository.setState(pnfsId, EntryState.CACHED);
                }
                msg.setSucceeded();
                break;

            case CACHED:
                if (msg.isPrecious()) {
                    _repository.setState(pnfsId, EntryState.PRECIOUS);
                }
                msg.setSucceeded();
                break;

            case FROM_CLIENT:
            case FROM_POOL:
            case FROM_STORE:
                msg.setFailed(101, "File still transient: " + pnfsId);
                break;

            case BROKEN:
                msg.setFailed(101, "File is broken: " + pnfsId);
                break;

            case NEW:
            case REMOVED:
            case DESTROYED:
                msg.setFailed(101, "File does not exist: " + pnfsId);
                break;
            }
        } catch (Exception e) { //FIXME
            msg.setFailed(100, e);
        }
        return msg;
    }

    public PoolModifyModeMessage messageArrived(PoolModifyModeMessage msg)
    {
        PoolV2Mode mode = msg.getPoolMode();
        if (mode != null) {
            if (mode.isEnabled()) {
                enablePool();
            } else {
                disablePool(mode.getMode(),
                            msg.getStatusCode(), msg.getStatusMessage());
            }
        }
        msg.setSucceeded();
        return msg;
    }

    public PoolSetStickyMessage messageArrived(PoolSetStickyMessage msg)
        throws CacheException, InterruptedException
    {
        if (_poolMode.isDisabled(PoolV2Mode.DISABLED_STRICT)) {
            _log.warn("PoolSetStickyMessage request rejected due to "
                      + _poolMode);
            throw new CacheException(CacheException.POOL_DISABLED, "Pool is disabled");
        }

        _repository.setSticky(msg.getPnfsId(),
                msg.getOwner(),
                msg.isSticky()
                        ? msg.getLifeTime()
                        : 0,
                true);
        msg.setSucceeded();
        return msg;
    }

    public PoolQueryRepositoryMsg messageArrived(PoolQueryRepositoryMsg msg)
        throws CacheException, InterruptedException
    {
        msg.setReply(new RepositoryCookie(), getRepositoryListing());
        return msg;
    }

    private List<CacheRepositoryEntryInfo> getRepositoryListing()
        throws CacheException, InterruptedException
    {
        List<CacheRepositoryEntryInfo> listing = new ArrayList<>();
        for (PnfsId pnfsid : _repository) {
            try {
                switch (_repository.getState(pnfsid)) {
                case PRECIOUS:
                case CACHED:
                case BROKEN:
                    listing.add(getCacheRepositoryEntryInfo(pnfsid));
                    break;
                default:
                    break;
                }
            } catch (FileNotInCacheException e) {
                /* The file was deleted before we got a chance to add
                 * it to the list. Since deleted files are not
                 * supposed to be on the list, the exception is not a
                 * problem.
                 */
            } catch (IllegalStateException e) {
                /*
                 * For the purposes of this listing, ignore files
                 * with incomplete metadata (accessing an undefined file
                 * attribute throws this exception).
                 *
                 * Otherwise the loading of the pool into the replica manager
                 * database will fail and the pool will be marked offline when
                 * it reality it is accessible (this method is only
                 * used by DCacheCoreControllerV2).
                 */
                _log.warn("Skipping {} when listing contents of pool {}: {}.",
                          pnfsid, _poolName, e.getMessage());
            }
        }
        return listing;
    }

    private CacheRepositoryEntryInfo getCacheRepositoryEntryInfo(PnfsId pnfsid)
            throws CacheException, InterruptedException
    {
        CacheEntry entry = _repository.getEntry(pnfsid);
        int bitmask;
        switch (entry.getState()) {
        case PRECIOUS:
            bitmask = 1 << CacheRepositoryEntryInfo.PRECIOUS_BIT;
            break;
        case CACHED:
            bitmask = 1 << CacheRepositoryEntryInfo.CACHED_BIT;
            break;
        case FROM_CLIENT:
            bitmask = 1 << CacheRepositoryEntryInfo.RECEIVINGFROMCLIENT_BIT;
            break;
        case FROM_POOL:
        case FROM_STORE:
            bitmask = 1 << CacheRepositoryEntryInfo.RECEIVINGFROMSTORE_BIT;
            break;
        case BROKEN:
            bitmask = 1 << CacheRepositoryEntryInfo.BAD_BIT;
            break;
        case REMOVED:
            bitmask = 1 << CacheRepositoryEntryInfo.REMOVED_BIT;
            break;
        default:
            throw new IllegalArgumentException("Bug. An entry should never be in " + entry.getState());
        }
        if (entry.isSticky()) {
            bitmask |= 1<< CacheRepositoryEntryInfo.STICKY_BIT;
        }
        return new CacheRepositoryEntryInfo(entry.getPnfsId(), bitmask,
                                            entry.getLastAccessTime(),
                                            entry.getCreationTime(),
                                            entry.getReplicaSize());
    }

    /**
     * Partially or fully disables normal operation of this pool.
     */
    private synchronized void disablePool(int mode, int errorCode, String errorString)
    {
        _poolStatusCode = errorCode;
        _poolStatusMessage =
            (errorString == null) ? "Requested by operator" : errorString;
        _poolMode.setMode(mode);

        _pingThread.sendPoolManagerMessage(true);
        _log.warn("Pool mode changed to {}: {}", _poolMode, _poolStatusMessage);
    }

    /**
     * Fully enables this pool. The status code is set to 0 and the
     * status message is cleared.
     */
    private synchronized void enablePool()
    {
        _poolMode.setMode(PoolV2Mode.ENABLED);
        _poolStatusCode = 0;
        _poolStatusMessage = "OK";

        _pingThread.sendPoolManagerMessage(true);
        _log.warn("Pool mode changed to " + _poolMode);
    }

    private class PoolManagerPingThread implements Runnable
    {
        private final Thread _worker;
        private int _heartbeat = HEARTBEAT;

        private PoolManagerPingThread()
        {
            _worker = new Thread(this, "ping");
        }

        public void start()
        {
            _worker.start();
        }

        public void stop()
        {
            _worker.interrupt();
        }

        @Override
        public void run()
        {
            _log.debug("Ping thread started");
            try {
                while (!Thread.interrupted()) {
                    sendPoolManagerMessage(true);
                    Thread.sleep(_heartbeat * 1000);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                _log.debug("Ping Thread finished");
            }
        }

        public void setHeartbeat(int seconds)
        {
            _heartbeat = seconds;
        }

        public int getHeartbeat()
        {
            return _heartbeat;
        }

        public synchronized void sendPoolManagerMessage(boolean forceSend)
        {
            if (forceSend || _storageQueue.poolStatusChanged()) {
                send(getPoolManagerMessage());
            }
        }

        private CellMessage getPoolManagerMessage()
        {
            boolean disabled =
                _poolMode.getMode() == PoolV2Mode.DISABLED ||
                _poolMode.isDisabled(PoolV2Mode.DISABLED_STRICT);
            PoolCostInfo info = disabled ? null : getPoolCostInfo();

            PoolManagerPoolUpMessage poolManagerMessage =
                new PoolManagerPoolUpMessage(_poolName, _serialId,
                                             _poolMode, info);

            poolManagerMessage.setTagMap(_tags);
            if (_hsmSet != null) {
                poolManagerMessage.setHsmInstances(new TreeSet<>(_hsmSet
                        .getHsmInstances()));
            }
            poolManagerMessage.setMessage(_poolStatusMessage);
            poolManagerMessage.setCode(_poolStatusCode);

            return new CellMessage(new CellPath(_poolupDestination),
                                   poolManagerMessage);
        }

        private void send(CellMessage msg)
        {
            sendMessage(msg);
        }
    }

    private PoolCostInfo getPoolCostInfo()
    {
        PoolCostInfo info = new PoolCostInfo(_poolName, IoQueueManager.DEFAULT_QUEUE);
        SpaceRecord space = _repository.getSpaceRecord();

        info.setSpaceUsage(space.getTotalSpace(), space.getFreeSpace(),
                           space.getPreciousSpace(), space.getRemovableSpace(),
                           space.getLRU());

        info.getSpaceInfo().setParameter(_breakEven, space.getGap());
        info.setMoverCostFactor(_moverCostFactor);

        for (MoverRequestScheduler js : _ioQueue.getQueues()) {
            /*
             * we skip p2p queue as it is handled differently
             * FIXME: no special cases
             */
            if(js.getName().equals(P2P_QUEUE_NAME)) {
                continue;
            }

            info.addExtendedMoverQueueSizes(js.getName(),
                                            js.getActiveJobs(),
                                            js.getMaxActiveJobs(),
                                            js.getQueueSize(),
                                            js.getCountByPriority(IoPriority.REGULAR),
                                            js.getCountByPriority(IoPriority.HIGH));
        }

        info.setP2pClientQueueSizes(_p2pClient.getActiveJobs(),
                                    _p2pClient.getMaxActiveJobs(),
                                    _p2pClient.getQueueSize());

        MoverRequestScheduler p2pQueue = _ioQueue.getQueue(P2P_QUEUE_NAME);
        info.setP2pServerQueueSizes(p2pQueue.getActiveJobs(),
                                    p2pQueue.getMaxActiveJobs(),
                                    p2pQueue.getQueueSize());

        info.setQueueSizes(_suppressHsmLoad ? 0 : _storageHandler.getActiveFetchJobs(),
                0,
                _suppressHsmLoad ? 0 : _storageHandler.getFetchQueueSize(),
                _suppressHsmLoad ? 0 : _storageHandler.getActiveStoreJobs(),
                0,
                _suppressHsmLoad ? 0 : _storageHandler.getStoreQueueSize());
        return info;
    }

    @Command(name = "set breakeven", hint = "set the space cost value of a week old file for this pool",
            description = "Set the breakeven parameter which is used within the space " +
                    "cost calculation scheme. This calculation is relevant for determining " +
                    "this pool space availability and selection for storing a file. The set " +
                    "parameter specifies the impact of the age of the least recently used " +
                    "(LRU) file on space cost.\n\n" +
                    "If the LRU file is one week old, the space cost will be equal to " +
                    "(1 + costForMinute), where costForMinute =  breakeven parameter * 7 * 24 * 60. " +
                    "Note that, if the breakeven parameter of a pool is set to equal or greater " +
                    "than 1.0, the default value of " + DEFAULT_BREAK_EVEN + " is used.")
    public class SetBreakevenCommand implements Callable<String>
    {
        @Argument(usage = "Specify the breakeven value. This value has to be a positive and " +
                "less than 1.0.")
        double parameter = _breakEven;

        @Override
        public String call() throws IllegalArgumentException
        {
            checkArgument(parameter > 0, "The breakeven parameter must be a positive number.");

            if (parameter >= 1){
                parameter = DEFAULT_BREAK_EVEN;
            }
            _breakEven = parameter;
            return "BreakEven = " + getBreakEven();
        }

    }

    public double getBreakEven()
    {
        return _breakEven;
    }

    @Command(name = "set mover cost factor", hint = "set the selectivity of this pool by movers",
            description = "The mover cost factor controls how much the number of movers " +
                    "affects proportional pool selection.\n\n" +
                    "Intuitively, for every 1/f movers, where f is the mover cost " +
                    "factor, the probability of choosing this pools is halved. When " +
                    "set to zero, the number of movers does not affect pool selection.")
    public class SetMoverCostFactorCommand implements Callable<String>
    {
        @Argument(usage = "Specify the cost factor value. This value " +
                "must be greater than or equal to 0.0.", required = false)
        double value = _moverCostFactor;

        @Override
        public String call() throws IllegalArgumentException
        {
            checkArgument(value > 0, "Mover cost factor must be larger than or equal to 0.0");

            _moverCostFactor = value;
            return "Cost factor is " + _moverCostFactor;
        }
    }

    // /////////////////////////////////////////////////
    //
    // the hybrid inventory part
    //
    private class HybridInventory implements Runnable
    {
        private boolean _activate = true;

        public HybridInventory(boolean activate)
        {
            _activate = activate;
            new Thread(this, "HybridInventory").start();
        }

        private void addCacheLocation(PnfsId id)
        {
            try {
                _pnfs.addCacheLocation(id);
            } catch (FileNotFoundCacheException e) {
                try {
                    _repository.setState(id, EntryState.REMOVED);
                    _log.info("File not found in PNFS; removed " + id);
                } catch (InterruptedException | IllegalTransitionException | CacheException f) {
                    _log.error("File not found in PNFS, but failed to remove "
                               + id + ": " + f);
                }
            } catch (CacheException e) {
                _log.error("Cache location was not registered for "
                           + id + ": " + e.getMessage());
            }
        }

        private void clearCacheLocation(PnfsId id)
        {
            _pnfs.clearCacheLocation(id);
        }

        @Override
        public void run()
        {
            _hybridCurrent = 0;

            long startTime, stopTime;
            if (_activate) {
                _log.info("Registering all replicas in PNFS");
            } else {
                _log.info("Unregistering all replicas in PNFS");
            }
            startTime = System.currentTimeMillis();

            for (PnfsId pnfsid : _repository) {
                if (Thread.interrupted()) {
                    break;
                }
                try {
                    switch (_repository.getState(pnfsid)) {
                    case PRECIOUS:
                    case CACHED:
                    case BROKEN:
                        _hybridCurrent++;
                        if (_activate) {
                            addCacheLocation(pnfsid);
                        } else {
                            clearCacheLocation(pnfsid);
                        }
                        break;
                    default:
                        break;
                    }
                } catch (CacheException e) {
                    _log.warn(e.getMessage());
                } catch (InterruptedException e) {
                    break;
                }
            }
            stopTime = System.currentTimeMillis();
            synchronized (_hybridInventoryLock) {
                _hybridInventoryActive = false;
            }

            _log.info("Replica "
                      + (_activate ? "registration" : "deregistration" )
                      + " finished. " + _hybridCurrent
                      + " replicas processed in "
                      + (stopTime-startTime) + " msec");
        }
    }

    private void startHybridInventory(boolean register)
            throws IllegalArgumentException
    {
        synchronized (_hybridInventoryLock) {
            if (_hybridInventoryActive) {
                throw new IllegalArgumentException(
                        "Hybrid inventory still active");
            }
            _hybridInventoryActive = true;
            new HybridInventory(register);
        }
    }

    @Command(name = "pnfs register",
            hint = "add file locations in namespace",
            description = "Record all the file replicas in this pool with the namespace. " +
                    "This is achieved by registering all file replicas in this pool into " +
                    "the namespace, provided they have not been registered. If the replica " +
                    "is unknown to the namespace then local copy is removed.\n\n" +
                    "By default, dCache synchronously update the namespace whenever " +
                    "a file is uploaded or removed from the pool.")
    public class PnfsRegisterCommand implements Callable<String>
    {
        @Override
        public String call() throws IllegalArgumentException
        {
            startHybridInventory(true);
            return "";
        }
    }

    @Command(name = "pnfs unregister",
            hint = "remove file locations from namespace",
            description = "Unregister all file replicas of this pool from the namespace.")
    public class PnfsUnregisterCommand implements Callable<String>
    {
        @Override
        public String call() throws IllegalArgumentException
        {
            startHybridInventory(false);
            return "";
        }
    }

    @Command(name = "pf", hint = "return the path of a file",
            description = "Get the path corresponding to a particular file by specifying " +
                    "the pnfsid. In case of hard links, one of the possible path is returned. " +
                    "The pnfsid is the internal identifier of the file within dCache. This is " +
                    "unique within a single dCache instance and globally unique with a very " +
                    "high probability.")
    public class PfCommand implements Callable<String>
    {
        @Argument(usage = "Specify the pnfsid of the file.")
        PnfsId pnfsId;

        @Override
        public String call() throws CacheException, IllegalArgumentException
        {
            return _pnfs.getPathByPnfsId(pnfsId).toString();
        }
    }

    public static final String hh_set_replication = "[-off] [<mgr> [<host>]]";
    public String ac_set_replication_$_0_2(Args args)
    {
        if (args.hasOption("off")) {
            setReplicationNotificationDestination("");
        } else if (args.argc() > 0) {
            setReplicationNotificationDestination(args.argv(0));
            if (args.argc() > 1) {
                setReplicationIp(args.argv(1));
            }
        }
        return _replicationHandler.toString();
    }

    public static final String hh_pool_suppress_hsmload = "on|off";
    public String ac_pool_suppress_hsmload_$_1(Args args)
    {
        String mode = args.argv(0);
        switch (mode) {
        case "on":
            _suppressHsmLoad = true;
            break;
        case "off":
            _suppressHsmLoad = false;
            break;
        default:
            throw new IllegalArgumentException("Illegal syntax : pool suppress hsmload on|off");
        }

        return "hsm load suppression swithed : "
            + (_suppressHsmLoad ? "on" : "off");
    }

    public static final String hh_set_duplicate_request = "none|ignore|refresh";
    public String ac_set_duplicate_request_$_1(Args args)
        throws CommandSyntaxException
    {
        String mode = args.argv(0);
        switch (mode) {
        case "none":
            _dupRequest = DUP_REQ_NONE;
            break;
        case "ignore":
            _dupRequest = DUP_REQ_IGNORE;
            break;
        case "refresh":
            _dupRequest = DUP_REQ_REFRESH;
            break;
        default:
            throw new CommandSyntaxException("Not Found : ",
                    "Usage : pool duplicate request none|ignore|refresh");
        }
        return "";
    }

    public static final String hh_set_p2p = "integrated|separated; OBSOLETE";
    public String ac_set_p2p_$_1(Args args)
    {
        return "WARNING: this command is obsolete";
    }

    public static final String fh_pool_disable = "   pool disable [options] [ <errorCode> [<errorMessage>]]\n"
        + "      OPTIONS :\n"
        + "        -fetch    #  disallows fetch (transfer to client)\n"
        + "        -stage    #  disallows staging (from HSM)\n"
        + "        -store    #  disallows store (transfer from client)\n"
        + "        -p2p-client\n"
        + "        -rdonly   #  := store,stage,p2p-client\n"
        + "        -strict   #  := disallows everything\n";
    public static final String hh_pool_disable = "[options] [<errorCode> [<errorMessage>]] # suspend sending 'up messages'";
    public String ac_pool_disable_$_0_2(Args args)
    {
        if (_poolMode.isDisabled(PoolV2Mode.DISABLED_DEAD)) {
            return "The pool is dead and a restart is required to enable it";
        }

        int rc = (args.argc() > 0) ? Integer.parseInt(args.argv(0)) : 1;
        String rm = (args.argc() > 1) ? args.argv(1) : "Operator intervention";

        int modeBits = PoolV2Mode.DISABLED;
        if (args.hasOption("strict")) {
            modeBits |= PoolV2Mode.DISABLED_STRICT;
        }
        if (args.hasOption("stage")) {
            modeBits |= PoolV2Mode.DISABLED_STAGE;
        }
        if (args.hasOption("fetch")) {
            modeBits |= PoolV2Mode.DISABLED_FETCH;
        }
        if (args.hasOption("store")) {
            modeBits |= PoolV2Mode.DISABLED_STORE;
        }
        if (args.hasOption("p2p-client")) {
            modeBits |= PoolV2Mode.DISABLED_P2P_CLIENT;
        }
        if (args.hasOption("p2p-server")) {
            modeBits |= PoolV2Mode.DISABLED_P2P_SERVER;
        }
        if (args.hasOption("rdonly")) {
            modeBits |= PoolV2Mode.DISABLED_RDONLY;
        }

        /*
         * No need for alarm here.
         */

        disablePool(modeBits, rc, rm);

        return "Pool " + _poolName + " " + _poolMode;
    }

    public static final String hh_pool_enable = " # resume sending up messages'";
    public String ac_pool_enable(Args args)
    {
        if (_poolMode.isDisabled(PoolV2Mode.DISABLED_DEAD)) {
            return "The pool is dead and a restart is required to enable it";
        }
        enablePool();
        return "Pool " + _poolName + " enabled";
    }

    public static final String hh_set_max_movers = "!!! Please use 'mover|st|rh set max active <jobs>'";
    public String ac_set_max_movers_$_1(Args args)
        throws IllegalArgumentException
    {
        int num = Integer.parseInt(args.argv(0));
        if ((num < 0) || (num > 10000)) {
            throw new IllegalArgumentException("Not in range (0...10000)");
        }
        return "Please use 'mover|st|rh set max active <jobs>'";

    }

    public static final String hh_set_report_remove = "on|off";
    public String ac_set_report_remove_$_1(Args args)
        throws CommandSyntaxException
    {
        String onoff = args.argv(0);
        switch (onoff) {
        case "on":
            _reportOnRemovals = true;
            break;
        case "off":
            _reportOnRemovals = false;
            break;
        default:
            throw new CommandSyntaxException("Invalid value : " + onoff);
        }
        return "";
    }

    public static final String hh_set_sticky = "# Deprecated";
    public String ac_set_sticky_$_0_1(Args args)
    {
        return "The command is deprecated and has no effect";
    }

    public static final String hh_set_cleaning_interval = "<interval/sec>";
    public String ac_set_cleaning_interval_$_1(Args args)
    {
        _cleaningInterval = Integer.parseInt(args.argv(0));
        _log.info("set cleaning interval to " + _cleaningInterval);
        return "";
    }

    public static final String hh_mover_ls = "[-binary [jobId] ]";
    public static final String hh_p2p_ls = "[-binary [jobId] ]";

    @Command(name = "mover set max active",
            hint = "set the maximum number of active client transfers",
            description = "Set the maximum number of allowed concurrent transfers. " +
                    "If any further requests are send after the set maximum value is " +
                    "reach, these requests will be queued. A classic usage will be " +
                    "to set the maximum number of client concurrent transfer request " +
                    "allowed.\n\n" +
                    "Note that, this set maximum value will also be used by the cost " +
                    "module for calculating the performance cost.")
    public class MoverSetMaxActiveCommand implements Callable<String>
    {
        @Argument(metaVar = "maxActiveMovers",
                usage = "Specify the maximum number of active client transfers.")
        int maxActiveIoMovers;

        @Option(name = "queue", metaVar = "queueName",
                usage = "Specify the mover queue name to operate on. If unspecified, " +
                        "the default mover queue is assumed.")
        String queueName;

        @Override
        public String call() throws IllegalArgumentException
        {
            if (queueName == null) {
                return mover_set_max_active(_ioQueue.getDefaultQueue(), maxActiveIoMovers);
            }

            MoverRequestScheduler js = _ioQueue.getQueue(queueName);

            if (js == null) {
                return "Not found : " + queueName;
            }

            return mover_set_max_active(js, maxActiveIoMovers);
        }
    }

    @Command(name = "p2p set max active",
            hint = "set the maximum number of active pool-to-pool server transfers",
            description = "Set the maximum number of active pool-to-pool " +
                    "(server-side) concurrent transfers allowed. Any further " +
                    "requests will be queued. This value will also be used by " +
                    "the cost module for calculating the performance cost.")
    public class P2pSetMaxActiveCommand implements Callable<String>
    {
        @Argument(usage = "The maximum number of active pool-to-pool server transfers")
        int maxActiveP2PTransfers;

        @Override
        public String call() throws IllegalArgumentException
        {
            MoverRequestScheduler p2pQueue = _ioQueue.getQueue(P2P_QUEUE_NAME);
            return mover_set_max_active(p2pQueue, maxActiveP2PTransfers);
        }
    }

    private String mover_set_max_active(MoverRequestScheduler js, int active)
            throws IllegalArgumentException
    {
        checkArgument(active > 0, "<maxActiveMovers> must be >= 0");
        js.setMaxActiveJobs(active);

        return "Max Active Io Movers set to " + active;
    }

    @Command(name = "mover queue ls",
            hint = "list all mover queues in this pool",
            description = "List information about the mover queues in this pool. " +
                    "Only the names of the mover queues are listed if the option '-l' " +
                    "is not specified.")
    public class MoverQueueLsCommand implements Callable<Serializable>
    {
        @Option(name = "l",
                usage = "Get additional information on the mover queues. " +
                        "The returned information comprises of: the name of the " +
                        "mover queue, number of active transfer, maximum number " +
                        "of allowed transfer and the length of the queued transfer.")
        boolean l;

        @Override
        public Serializable call()
        {
            StringBuilder sb = new StringBuilder();

            if (l) {
                for (MoverRequestScheduler js : _ioQueue.getQueues()) {
                    sb.append(js.getName())
                            .append(" ").append(js.getActiveJobs())
                            .append(" ").append(js.getMaxActiveJobs())
                            .append(" ").append(js.getQueueSize()).append("\n");
                }
            } else {
                for (MoverRequestScheduler js : _ioQueue.getQueues()) {
                    sb.append(js.getName()).append("\n");
                }
            }
            return sb.toString();
        }
    }

    public Object ac_mover_ls_$_0_1(Args args)
            throws NoSuchElementException, NumberFormatException
    {
        String queueName = args.getOpt("queue");
        boolean binary = args.hasOption("binary");

        if (binary && args.argc() > 0) {
            int id = Integer.parseInt(args.argv(0));
            MoverRequestScheduler js = _ioQueue.getQueueByJobId(id);
            return js.getJobInfo(id);
        }

        if (queueName == null) {
            return mover_ls(_ioQueue.getQueues(), binary);
        }

        if (queueName.length() == 0) {
            StringBuilder sb = new StringBuilder();
            for (MoverRequestScheduler js : _ioQueue.getQueues()) {
                sb.append("[").append(js.getName()).append("]\n");
                sb.append(mover_ls(js, binary).toString());
            }
            return sb.toString();
        }

        MoverRequestScheduler js = _ioQueue.getQueue(queueName);
        if (js == null) {
            throw new NoSuchElementException(queueName);
        }

        return mover_ls(js, binary);

    }

    public Object ac_p2p_ls_$_0_1(Args args)
            throws NoSuchElementException, NumberFormatException
    {
        MoverRequestScheduler p2pQueue = _ioQueue.getQueue(P2P_QUEUE_NAME);

        boolean binary = args.hasOption("binary");
        if (binary && args.argc() > 0) {
            int id = Integer.parseInt(args.argv(0));
            MoverRequestScheduler js = _ioQueue.getQueueByJobId(id);
            return js.getJobInfo(id);
        }

        return mover_ls(p2pQueue, binary);
    }

    private Object mover_ls(MoverRequestScheduler js, boolean binary) {
        return mover_ls(Arrays.asList(js), binary);
    }

    private Object mover_ls(Collection<MoverRequestScheduler> jobSchedulers, boolean binary) {

        if (binary) {
            List<JobInfo> list = new ArrayList<>();
            for (MoverRequestScheduler js : jobSchedulers) {
                list.addAll(js.getJobInfos());
            }
            return list.toArray(new IoJobInfo[list.size()]);
        } else {
            StringBuffer sb = new StringBuffer();
            for (MoverRequestScheduler js : jobSchedulers) {
                js.printJobQueue(sb);
            }
            return sb.toString();
        }
    }

    @Command(name = "mover remove",
            hint = "#OBSOLETE command",
            description = "This command is obsolete, please use: \n" +
                    "\t mover kill <jobid>")
    public class MoverRemoveCommand implements Callable<String>
    {
        @Argument(metaVar = "jobId", required = false)
        String id;

        @Override
        public String call()
        {
            return "This command is obsolete. Please use: \n\tmover kill " +
                    (id==null ? "<jobid>":id);
        }
    }

    @Command(name = "mover kill",
            hint = "terminate a file transfer connection",
            description = "Interrupt a specified file transfer in progress by " +
                    "terminating the request. This is particularly useful when " +
                    "the transfer request is stuck and blocking other requests.")
    public class MoverKillCommand implements Callable<String>
    {
        @Argument(metaVar = "jobId",
                usage = "Specify the job number of the transfer request to kill.")
        int id;

        @Override
        public String call() throws NoSuchElementException, IllegalArgumentException
        {
            MoverRequestScheduler js = _ioQueue.getQueueByJobId(id);
            mover_kill(js, id);
            return "Kill initialized";
        }
    }

    private void mover_kill(MoverRequestScheduler js, int id)
        throws NoSuchElementException
    {

        _log.info("Killing mover " + id);
        js.cancel(id);
    }

    @Command(name = "set heartbeat",
            hint = "set time interval for sending this pool cost info",
            description = "Set the regular time interval at which this pool " +
                    "sent it cost information to the pool manager. The sent " +
                    "pool cost information are stored and will be use for load " +
                    "balancing and pool selection. However, this time interval " +
                    "is not guaranteed to be exact since this operation is limited " +
                    "by the underlying OS where this pool reside.")
    public class SetHeartbeatCommand implements Callable<String>
    {
        @Argument(usage = "Specify the time interval in seconds. This " +
                "value has to be a positive integer.")
        int value = HEARTBEAT;

        @Override
        public String call() throws IllegalArgumentException
        {
            checkArgument(value > 0, "The heartbeat value must be a positive integer.");
            _pingThread.setHeartbeat(value);

            return "Heartbeat set to " + (_pingThread.getHeartbeat()) + " seconds";
        }
    }
}
