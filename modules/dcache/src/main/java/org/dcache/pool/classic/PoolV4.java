// $Id: MultiProtocolPoolV3.java,v 1.16 2007-10-26 11:17:06 behrmann Exp $

package org.dcache.pool.classic;

import com.google.common.base.Throwables;
import com.google.common.net.InetAddresses;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;

import java.io.IOException;
import java.io.PrintWriter;
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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import diskCacheV111.pools.PoolCellInfo;
import diskCacheV111.pools.PoolCostInfo;
import diskCacheV111.pools.PoolV2Mode;
import diskCacheV111.repository.CacheRepositoryEntryInfo;
import diskCacheV111.repository.RepositoryCookie;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.CacheFileAvailable;
import diskCacheV111.util.FileInCacheException;
import diskCacheV111.util.FileNotFoundCacheException;
import diskCacheV111.util.FileNotInCacheException;
import org.dcache.pool.nearline.HsmSet;
import diskCacheV111.util.PnfsHandler;
import diskCacheV111.util.PnfsId;
import diskCacheV111.util.UnitInteger;
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
import dmg.cells.nucleus.NoRouteToCellException;
import dmg.cells.nucleus.Reply;
import dmg.util.CommandSyntaxException;

import org.dcache.cells.CellStub;
import org.dcache.pool.FaultEvent;
import org.dcache.pool.FaultListener;
import org.dcache.pool.nearline.NearlineStorageHandler;
import org.dcache.pool.movers.Mover;
import org.dcache.pool.movers.MoverFactory;
import org.dcache.pool.p2p.P2PClient;
import org.dcache.pool.repository.AbstractStateChangeListener;
import org.dcache.pool.repository.Account;
import org.dcache.pool.repository.CacheEntry;
import org.dcache.pool.repository.EntryChangeEvent;
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
import org.dcache.vehicles.PnfsSetFileAttributes;

import static com.google.common.base.Preconditions.checkState;

public class PoolV4
    extends AbstractCellComponent
    implements FaultListener,
               CellCommandListener,
               CellMessageReceiver
{
    private final static int DUP_REQ_NONE = 0;
    private final static int DUP_REQ_IGNORE = 1;
    private final static int DUP_REQ_REFRESH = 2;

    private final static int P2P_CACHED = 1;
    private final static int P2P_PRECIOUS = 2;

    private final static Pattern TAG_PATTERN =
        Pattern.compile("([^=]+)=(\\S*)\\s*");

    /**
     * The name of a queue used by pool-to-pool transfers.
     */
    private final static String P2P_QUEUE_NAME= "p2p";

    private final static Logger _log = LoggerFactory.getLogger(PoolV4.class);

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

    private String _poolupDestination = "PoolManager";

    private int _version = 4;
    private CellStub _billingStub;
    private final Map<String, String> _tags = new HashMap<>();
    private String _baseDir;

    private final PoolManagerPingThread _pingThread ;
    private HsmFlushController _flushingThread;
    private IoQueueManager _ioQueue ;
    private HsmSet _hsmSet;
    private NearlineStorageHandler _storageHandler;
    private boolean _crashEnabled;
    private String _crashType = "exception";
    private long _gap = 4L * 1024L * 1024L * 1024L;
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
    private ReplicationHandler _replicationHandler = new ReplicationHandler();

    private ReplicaStatePolicy _replicaStatePolicy;

    private CellPath _replicationManager;
    private InetAddress _replicationIp;

    private boolean _running;
    private double _breakEven = 0.7;
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
        _repository.addListener(new ATimeMaintainer());
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

    /**
     * Initialize remaining pieces.
     *
     * We cannot do these things in the constructor as they rely on
     * various properties being set first.
     */
    public void init()
    {
        checkState(!_isVolatile || !_hasTapeBackend, "Volatile pool cannot have a tape backend");
        disablePool(PoolV2Mode.DISABLED_STRICT, 1, "Initializing");
        _pingThread.start();
    }

    @Override
    public void afterStart()
    {
        assertNotRunning("Cannot initialize several times");
        _running = true;
        new Thread() {
            @Override
            public void run() {
                try {
                    _repository.init();
                    disablePool(PoolV2Mode.DISABLED_RDONLY, 1, "Initializing");
                    _repository.load();
                    enablePool();
                    _flushingThread.start();
                } catch (RuntimeException e) {
                    _log.error("Repository reported a problem. Please report this to support@dcache.org.", e);
                    _log.warn("Pool not enabled {}", _poolName);
                    disablePool(PoolV2Mode.DISABLED_DEAD | PoolV2Mode.DISABLED_STRICT,
                            666, "Init failed: " + e.getMessage());
                } catch (Throwable e) {
                    _log.error("Repository reported a problem: " + e.getMessage());
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
        if (cause != null) {
            _log.error("Fault occured in " + event.getSource() + ": "
                       + event.getMessage(), cause);
        } else {
            _log.error("Fault occured in " + event.getSource() + ": "
                       + event.getMessage());
        }

        switch (event.getAction()) {
        case READONLY:
            disablePool(PoolV2Mode.DISABLED_RDONLY, 99,
                        "Pool read-only: " + event.getMessage());
            break;

        case DISABLED:
            disablePool(PoolV2Mode.DISABLED_STRICT, 99,
                        "Pool disabled: " + event.getMessage());
            break;

        default:
            disablePool(PoolV2Mode.DISABLED_STRICT | PoolV2Mode.DISABLED_DEAD, 666,
                        "Pool disabled: " + event.getMessage());
            break;
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
     * Update file's atime in the namespace
     */
    private class ATimeMaintainer extends AbstractStateChangeListener {

        @Override
        public void accessTimeChanged(EntryChangeEvent event) {
            FileAttributes fileAttributes = new FileAttributes();
            fileAttributes.setAccessTime(System.currentTimeMillis());
            _pnfs.notify(new PnfsSetFileAttributes(event.getPnfsId(), fileAttributes));
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
                _log.debug("Adding " + id + " to flush queue");

                if (_hasTapeBackend) {
                    try {
                        _storageQueue.addCacheEntry(id);
                    } catch (FileNotInCacheException e) {
                        /* File was deleted before we got a chance to do
                         * anything with it. We don't care about deleted
                         * files so we ignore this.
                         */
                        _log.info("Failed to flush " + id + ": Replica is no longer in the pool", e);
                    } catch (CacheException | InterruptedException e) {
                        _log.error("Error adding " + id + " to flush queue: "
                                + e.getMessage());
                    }
                }
            } else if (from == EntryState.PRECIOUS) {
                _log.debug("Removing " + id + " from flush queue");
                if (!_storageQueue.removeCacheEntry(id)) {
                    _log.info("File " + id + " not found in flush queue");
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
                try {
                    CacheEntry entry = event.getNewEntry();
                    RemoveFileInfoMessage msg =
                        new RemoveFileInfoMessage(getCellAddress().toString(), entry.getPnfsId());
                    msg.setFileSize(entry.getReplicaSize());
                    msg.setStorageInfo(entry.getFileAttributes().getStorageInfo());
                    _billingStub.notify(msg);
                } catch (NoRouteToCellException e) {
                    _log.error("Failed to register removal in billing: {}", e.getMessage());
                }
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
        pw.println("set gap " + _gap);
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
        pw.println("Gap               : " + _gap);
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

        for (IoScheduler js : _ioQueue.getQueues()) {
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

    private boolean isDuplicateIoRequest(CellPath pathFromSource, PoolIoFileMessage message)
    {
        if (!(message instanceof PoolAcceptFileMessage)
                && !message.isPool2Pool()) {
            long id = message.getId();
            String door = pathFromSource.getSourceAddress().toString();
            JobInfo job = _ioQueue.findJob(door, id);
            if (job != null) {
                switch (_dupRequest) {
                case DUP_REQ_NONE:
                    _log.info("Dup Request : none <" + door + ":" + id + ">");
                    break;
                case DUP_REQ_IGNORE:
                    _log.info("Dup Request : ignoring <" + door + ":" + id + ">");
                    return true;
                case DUP_REQ_REFRESH:
                    long jobId = job.getJobId();
                    _log.info("Dup Request : refreshing <" + door + ":"
                            + id + "> old = " + jobId);
                    _ioQueue.cancel((int)jobId);
                    break;
                default:
                    throw new RuntimeException("Dup Request : PANIC (code corrupted) <"
                            + door + ":" + id + ">");
                }
            }
        }
        return false;
    }

    private Mover<?> createMover(CellPath source, PoolIoFileMessage message) throws CacheException
    {
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

    private int queueIoRequest(PoolIoFileMessage message, Mover<?> mover)
    {
        String queueName = message.getIoQueueName();

        if (message instanceof PoolAcceptFileMessage) {
            return _ioQueue.add(queueName, mover, IoPriority.HIGH);
        } else if (message.isPool2Pool()) {
            return _ioQueue.add(P2P_QUEUE_NAME, mover, IoPriority.HIGH);
        } else {
            return _ioQueue.add(queueName, mover, IoPriority.REGULAR);
        }
    }

    private void ioFile(CellMessage envelope, PoolIoFileMessage message)
    {
        try {
            if (isDuplicateIoRequest(envelope.getSourcePath(), message)) {
                return;
            }
            Mover<?> mover = createMover(envelope.getSourcePath().revert(), message);
            try {
                message.setMoverId(queueIoRequest(message, mover));
            } catch (Throwable t) {
                mover.postprocess(new NopCompletionHandler<Void, Void>());
                throw Throwables.propagate(t);
            }
            message.setSucceeded();
        } catch (CacheException e) {
            _log.error(e.getMessage());
            message.setFailed(e.getRc(), e.getMessage());
        } catch (RuntimeException e) {
            _log.error("Possible bug found: " + e.getMessage(), e);
            message.setFailed(CacheException.DEFAULT_ERROR_CODE,
                              "Failed to enqueue mover: " + e.getMessage());
        }
        try {
            envelope.revertDirection();
            sendMessage(envelope);
        } catch (NoRouteToCellException e) {
            _log.error(e.toString());
        }
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
                } catch (NoRouteToCellException e) {
                    _log.error("Problem in sending replication request: " + e.getMessage());
                }
            }
        }

        private void _initiateReplication(CacheEntry entry, String source)
            throws NoRouteToCellException
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
            throw new CacheException(CacheException.DEFAULT_ERROR_CODE,
                                     id.toString() + " is broken in " + _poolName);
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
            IoScheduler js = _ioQueue.getQueueByJobId(id);
            mover_kill(js, id, false);
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
                                targetState, stickyRecords, callback, false);
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
            } catch (IllegalArgumentException e) {
                _log.error("Invalid syntax in remove request ("
                           + fileList[i] + ")");
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
                    listing.add(new CacheRepositoryEntryInfo(_repository.getEntry(pnfsid)));
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
            }
        }
        return listing;
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
        private int _heartbeat = 30;

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
            try {
                sendMessage(msg);
            } catch (NoRouteToCellException e){
                _log.error("Failed to send ping message: " + e.getMessage());
            }
        }
    }

    private PoolCostInfo getPoolCostInfo()
    {
        PoolCostInfo info = new PoolCostInfo(_poolName);
        SpaceRecord space = _repository.getSpaceRecord();

        info.setSpaceUsage(space.getTotalSpace(), space.getFreeSpace(),
                           space.getPreciousSpace(), space.getRemovableSpace(),
                           space.getLRU());

        info.getSpaceInfo().setParameter(_breakEven, _gap);
        info.setMoverCostFactor(_moverCostFactor);

        for (IoScheduler js : _ioQueue.getQueues()) {
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

        IoScheduler p2pQueue = _ioQueue.getQueue(P2P_QUEUE_NAME);
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

    public final static String hh_set_breakeven =
        "[<breakEven>] # free and recoverable space";
    public String ac_set_breakeven_$_0_1(Args args)
    {
        if (args.argc() > 0) {
            _breakEven = Double.parseDouble(args.argv(0));
        }
        return "BreakEven = " + _breakEven;
    }

    public final static String hh_set_mover_cost_factor =
        "[<factor>]";
    public final static String fh_set_mover_cost_factor =
        "The mover cost factor controls how much the number of movers" +
        "affects proportional pool selection.\n\n" +
        "Intuitively, for every 1/f movers, where f is the mover cost" +
        "factor, the probability of choosing this pools is halfed. When" +
        "set to zero, the number of movers does not affect pool selection.";
    public String ac_set_mover_cost_factor_$_0_1(Args args)
    {
        if (args.argc() > 0) {
            double value = Double.parseDouble(args.argv(0));
            if (value < 0.0) {
                throw new IllegalArgumentException("Mover cost factor must be larger than or equal to 0.0");
            }
            _moverCostFactor = value;
        }
        return "Cost factor is " + _moverCostFactor;
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

    public static final String hh_pnfs_register = " # add entry of all files into pnfs";
    public static final String hh_pnfs_unregister = " # remove entry of all files from pnfs";

    public String ac_pnfs_register(Args args)
    {
        synchronized (_hybridInventoryLock) {
            if (_hybridInventoryActive) {
                throw new IllegalArgumentException(
                        "Hybrid inventory still active");
            }
            _hybridInventoryActive = true;
            new HybridInventory(true);
        }
        return "";
    }

    public String ac_pnfs_unregister(Args args)
    {
        synchronized (_hybridInventoryLock) {
            if (_hybridInventoryActive) {
                throw new IllegalArgumentException(
                        "Hybrid inventory still active");
            }
            _hybridInventoryActive = true;
            new HybridInventory(false);
        }
        return "";
    }

    public static final String hh_run_hybrid_inventory = " [-destroy]";

    public String ac_run_hybrid_inventory(Args args)
    {
        synchronized (_hybridInventoryLock) {
            if (_hybridInventoryActive) {
                throw new IllegalArgumentException(
                        "Hybrid inventory still active");
            }
            _hybridInventoryActive = true;
            new HybridInventory(!args.hasOption("destroy"));
        }
        return "";
    }

    public static final String hh_pf = "<pnfsId>";

    public String ac_pf_$_1(Args args) throws CacheException, IllegalArgumentException
    {
        return _pnfs.getPathByPnfsId(new PnfsId(args.argv(0))).toString();
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

    public static final String hh_set_gap = "<always removable gap>/size[<unit>] # unit = k|m|g";
    public String ac_set_gap_$_1(Args args)
    {
        _gap = UnitInteger.parseUnitLong(args.argv(0));
        return "Gap set to " + _gap;
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

    public static final String hh_crash = "disabled|shutdown|exception";
    public String ac_crash_$_0_1(Args args) throws IllegalArgumentException
    {
        if (args.argc() < 1) {
            return "Crash is " + (_crashEnabled ? _crashType : "disabled");

        } else if (args.argv(0).equals("shutdown")) {
            _crashEnabled = true;
            _crashType = "shutdown";
        } else if (args.argv(0).equals("exception")) {
            _crashEnabled = true;
            _crashType = "exception";
        } else if (args.argv(0).equals("disabled")) {
            _crashEnabled = false;
        } else {
            throw new IllegalArgumentException("crash disabled|shutdown|exception");
        }

        return "Crash is " + (_crashEnabled ? _crashType : "disabled");

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

    public static final String hh_mover_set_max_active = "<maxActiveIoMovers> -queue=<queueName>";
    public static final String hh_mover_queue_ls = "";
    public static final String hh_mover_ls = "[-binary [jobId] ]";
    public static final String hh_mover_remove = "<jobId>";
    public static final String hh_mover_kill = "<jobId> [-force]" ;
    public static final String hh_p2p_set_max_active = "<maxActiveIoMovers>";
    public static final String hh_p2p_ls = "[-binary [jobId] ]";
    public static final String hh_p2p_remove = "<jobId>; OBSOLETE: use: mover remove -queue=" + P2P_QUEUE_NAME;
    public static final String hh_p2p_kill = "<jobId> [-force]; OBSOLETE: use: mover kill -queue=" + P2P_QUEUE_NAME;

    public String ac_mover_set_max_active_$_1(Args args)
        throws NumberFormatException, IllegalArgumentException
    {
        String queueName = args.getOpt("queue");

        if (queueName == null) {
            return mover_set_max_active(_ioQueue.getDefaultQueue(), args);
        }

        IoScheduler js = _ioQueue.getQueue(queueName);

        if (js == null) {
            return "Not found : " + queueName;
        }

        return mover_set_max_active(js, args);

    }

    public String ac_p2p_set_max_active_$_1(Args args)
        throws NumberFormatException, IllegalArgumentException
    {
        IoScheduler p2pQueue = _ioQueue.getQueue(P2P_QUEUE_NAME);
        return mover_set_max_active(p2pQueue, args);
    }

    private String mover_set_max_active(IoScheduler js, Args args)
        throws NumberFormatException, IllegalArgumentException
    {
        int active = Integer.parseInt(args.argv(0));
        if (active < 0) {
            throw new IllegalArgumentException("<maxActiveMovers> must be >= 0");
        }
        js.setMaxActiveJobs(active);

        return "Max Active Io Movers set to " + active;
    }

    public Object ac_mover_queue_ls_$_0_1(Args args)
    {
        StringBuilder sb = new StringBuilder();

        if (args.hasOption("l")) {
            for (IoScheduler js : _ioQueue.getQueues()) {
                sb.append(js.getName())
                    .append(" ").append(js.getActiveJobs())
                    .append(" ").append(js.getMaxActiveJobs())
                    .append(" ").append(js.getQueueSize()).append("\n");
            }
        } else {
            for (IoScheduler js : _ioQueue.getQueues()) {
                sb.append(js.getName()).append("\n");
            }
        }
        return sb.toString();
    }

    public Object ac_mover_ls_$_0_1(Args args)
            throws NoSuchElementException, NumberFormatException
    {
        String queueName = args.getOpt("queue");
        boolean binary = args.hasOption("binary");

        if (binary && args.argc() > 0) {
            int id = Integer.parseInt(args.argv(0));
            IoScheduler js = _ioQueue.getQueueByJobId(id);
            return js.getJobInfo(id);
        }

        if (queueName == null) {
            return mover_ls(_ioQueue.getQueues(), binary);
        }

        if (queueName.length() == 0) {
            StringBuilder sb = new StringBuilder();
            for (IoScheduler js : _ioQueue.getQueues()) {
                sb.append("[").append(js.getName()).append("]\n");
                sb.append(mover_ls(js, binary).toString());
            }
            return sb.toString();
        }

        IoScheduler js = _ioQueue.getQueue(queueName);
        if (js == null) {
            throw new NoSuchElementException(queueName);
        }

        return mover_ls(js, binary);

    }

    public Object ac_p2p_ls_$_0_1(Args args)
            throws NoSuchElementException, NumberFormatException
    {
        IoScheduler p2pQueue = _ioQueue.getQueue(P2P_QUEUE_NAME);

        boolean binary = args.hasOption("binary");
        if (binary && args.argc() > 0) {
            int id = Integer.parseInt(args.argv(0));
            IoScheduler js = _ioQueue.getQueueByJobId(id);
            return js.getJobInfo(id);
        }

        return mover_ls(p2pQueue, binary);
    }

    private Object mover_ls(IoScheduler js, boolean binary) {
        return mover_ls(Arrays.asList(js), binary);
    }

    private Object mover_ls(Collection<IoScheduler> jobSchedulers, boolean binary) {

        if (binary) {
            List<JobInfo> list = new ArrayList<>();
            for (IoScheduler js : jobSchedulers) {
                list.addAll(js.getJobInfos());
            }
            return list.toArray(new IoJobInfo[list.size()]);
        } else {
            StringBuffer sb = new StringBuffer();
            for (IoScheduler js : jobSchedulers) {
                js.printJobQueue(sb);
            }
            return sb.toString();
        }
    }

    public String ac_mover_remove_$_1(Args args)
        throws NoSuchElementException, NumberFormatException
    {
        int id = Integer.parseInt(args.argv(0));
        IoScheduler js = _ioQueue.getQueueByJobId(id);
        js.cancel(id);
        return "Removed";
    }

    public String ac_p2p_remove_$_1(Args args)
        throws NoSuchElementException, NumberFormatException
    {
        return "OBSOLETE: use: mover remove -queue=" + P2P_QUEUE_NAME;
    }

    public String ac_mover_kill_$_1(Args args)
        throws NoSuchElementException, NumberFormatException
    {
        int id = Integer.parseInt(args.argv(0));
        boolean force = args.hasOption("force");
        IoScheduler js = _ioQueue.getQueueByJobId(id);
        mover_kill(js, id, force);
        return "Kill initialized";
    }

    public String ac_p2p_kill_$_1(Args args)
        throws NoSuchElementException, NumberFormatException
    {
        return "OBSOLETE: use: mover kill -queue=" + P2P_QUEUE_NAME;
    }

    private void mover_kill(IoScheduler js, int id, boolean force)
        throws NoSuchElementException
    {

        _log.info("Killing mover " + id);
        js.cancel(id);
    }

    public static final String hh_set_heartbeat = "<heartbeatInterval/sec>";
    public String ac_set_heartbeat_$_0_1(Args args)
        throws NumberFormatException
    {
        if (args.argc() > 0) {
            _pingThread.setHeartbeat(Integer.parseInt(args.argv(0)));
        }
        return "Heartbeat at " + (_pingThread.getHeartbeat());
    }
}
