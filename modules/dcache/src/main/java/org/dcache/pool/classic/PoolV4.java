// $Id: MultiProtocolPoolV3.java,v 1.16 2007-10-26 11:17:06 behrmann Exp $

package org.dcache.pool.classic;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.stream.Collectors.toList;

import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.net.InetAddresses;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFutureTask;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.RateLimiter;
import diskCacheV111.pools.PoolCellInfo;
import diskCacheV111.pools.PoolCostInfo;
import diskCacheV111.pools.PoolV2Mode;
import diskCacheV111.pools.json.PoolCostData;
import diskCacheV111.repository.CacheRepositoryEntryInfo;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.CacheFileAvailable;
import diskCacheV111.util.FileInCacheException;
import diskCacheV111.util.FileNotFoundCacheException;
import diskCacheV111.util.FileNotInCacheException;
import diskCacheV111.util.LockedCacheException;
import diskCacheV111.util.OutOfDateCacheException;
import diskCacheV111.util.PnfsHandler;
import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.DCapProtocolInfo;
import diskCacheV111.vehicles.Pool2PoolTransferMsg;
import diskCacheV111.vehicles.PoolAcceptFileMessage;
import diskCacheV111.vehicles.PoolCheckFreeSpaceMessage;
import diskCacheV111.vehicles.PoolDeliverFileMessage;
import diskCacheV111.vehicles.PoolFetchFileMessage;
import diskCacheV111.vehicles.PoolIoFileMessage;
import diskCacheV111.vehicles.PoolManagerPoolUpMessage;
import diskCacheV111.vehicles.PoolMgrReplicateFileMsg;
import diskCacheV111.vehicles.PoolModifyModeMessage;
import diskCacheV111.vehicles.PoolModifyPersistencyMessage;
import diskCacheV111.vehicles.PoolMoverKillMessage;
import diskCacheV111.vehicles.PoolRemoveFilesFromHSMMessage;
import diskCacheV111.vehicles.PoolRemoveFilesMessage;
import diskCacheV111.vehicles.PoolSetStickyMessage;
import diskCacheV111.vehicles.ProtocolInfo;
import diskCacheV111.vehicles.RemoveFileInfoMessage;
import dmg.cells.nucleus.AbstractCellComponent;
import dmg.cells.nucleus.CellCommandListener;
import dmg.cells.nucleus.CellInfo;
import dmg.cells.nucleus.CellInfoProvider;
import dmg.cells.nucleus.CellLifeCycleAware;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellMessageReceiver;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.CellSetupProvider;
import dmg.cells.nucleus.CellVersion;
import dmg.cells.nucleus.DelayedReply;
import dmg.cells.nucleus.Reply;
import dmg.util.CommandException;
import dmg.util.CommandSyntaxException;
import dmg.util.command.Argument;
import dmg.util.command.Command;
import dmg.util.command.Option;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.nio.channels.CompletionHandler;
import java.nio.file.OpenOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadFactory;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.dcache.alarms.AlarmMarkerFactory;
import org.dcache.alarms.PredefinedAlarm;
import org.dcache.auth.Subjects;
import org.dcache.cells.CellStub;
import org.dcache.cells.MessageReply;
import org.dcache.cells.ThreadCreator;
import org.dcache.cells.ZoneAware;
import org.dcache.pool.FaultEvent;
import org.dcache.pool.FaultListener;
import org.dcache.pool.PoolDataBeanProvider;
import org.dcache.pool.assumption.Assumption;
import org.dcache.pool.json.PoolDataDetails;
import org.dcache.pool.json.PoolDataDetails.Lsf;
import org.dcache.pool.json.PoolDataDetails.P2PMode;
import org.dcache.pool.movers.Mover;
import org.dcache.pool.movers.MoverFactory;
import org.dcache.pool.nearline.HsmSet;
import org.dcache.pool.nearline.NearlineStorageHandler;
import org.dcache.pool.p2p.P2PClient;
import org.dcache.pool.repository.AbstractStateChangeListener;
import org.dcache.pool.repository.Account;
import org.dcache.pool.repository.CacheEntry;
import org.dcache.pool.repository.IllegalTransitionException;
import org.dcache.pool.repository.ReplicaDescriptor;
import org.dcache.pool.repository.ReplicaState;
import org.dcache.pool.repository.Repository;
import org.dcache.pool.repository.SpaceRecord;
import org.dcache.pool.repository.StateChangeEvent;
import org.dcache.pool.repository.StickyRecord;
import org.dcache.util.IoPriority;
import org.dcache.util.NetworkUtils;
import org.dcache.util.Version;
import org.dcache.vehicles.FileAttributes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Required;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.core.KafkaTemplate;

public class PoolV4
      extends AbstractCellComponent
      implements FaultListener, CellCommandListener, CellMessageReceiver, CellSetupProvider,
      CellLifeCycleAware, CellInfoProvider,
      PoolDataBeanProvider<PoolDataDetails>, ZoneAware, ThreadCreator {

    private static final int P2P_CACHED = 1;
    private static final int P2P_PRECIOUS = 2;
    private static final int HEARTBEAT = 30;

    private static final double DEFAULT_BREAK_EVEN = 0.7;

    public static final String ZONE_TAG = "zone";

    /**
     * These are tags that may not be set by an admin.
     */
    private static final Set<String> RESERVED_TAGS = ImmutableSet.of(ZONE_TAG);

    private static final Logger LOGGER = LoggerFactory.getLogger(PoolV4.class);

    private String _poolName;

    /**
     * pool start time identifier. used by PoolManager to recognize pool restarts
     */
    private final long _serialId = System.currentTimeMillis();
    private static final CellVersion VERSION = new CellVersion(Version.of(PoolV4.class));
    private PoolV2Mode _poolMode;
    private volatile boolean _reportOnRemovals;
    private volatile boolean _suppressHsmLoad;
    private String _poolStatusMessage = "OK";
    private int _poolStatusCode;

    private PnfsHandler _pnfs;
    private StorageClassContainer _storageQueue;
    private Repository _repository;

    private Account _account;

    private String _poolupDestination;

    private int _version = 4;
    private CellStub _billingStub;
    private final Map<String, String> _tags = new HashMap<>();
    private String _baseDir;

    private final PoolManagerPingThread _pingThread = new PoolManagerPingThread();
    private HsmFlushController _flushingThread;
    private IoQueueManager _ioQueue;
    private HsmSet _hsmSet;
    private NearlineStorageHandler _storageHandler;
    private int _p2pFileMode = P2P_CACHED;
    private P2PClient _p2pClient;

    private boolean _isVolatile;
    private boolean _hasTapeBackend = true;

    private final Object _hybridInventoryLock = new Object();
    private boolean _hybridInventoryActive;
    private int _hybridCurrent;

    private final ReplicationHandler _replicationHandler = new ReplicationHandler();

    private ReplicaStatePolicy _replicaStatePolicy;

    private CellPath _replicationManager;
    private InetAddress _replicationIp;

    private boolean _running;
    private double _breakEven = DEFAULT_BREAK_EVEN;
    private double _moverCostFactor = 0.5;
    private TransferServices _transferServices;

    private final Assumption.Pool _poolInfo = new PoolInfo();

    private final RateLimiter _pingLimiter = RateLimiter.create(100);

    private Executor _executor;

    private boolean _enableHsmFlag;

    private Consumer<RemoveFileInfoMessage> _kafkaSender = (s) -> {
    };

    private ThreadFactory _threadFactory;


    protected void assertNotRunning(String error) {
        checkState(!_running, error);
    }


    @Autowired(required = false)
    @Qualifier("remove")
    public void setKafkaTemplate(KafkaTemplate kafkaTemplate) {
        _kafkaSender = kafkaTemplate::sendDefault;
    }

    @Required
    public void setPoolName(String name) {
        assertNotRunning("Cannot change pool name after initialisation");
        _poolName = name;
    }

    @Required
    public void setBaseDir(String baseDir) {
        assertNotRunning("Cannot change base dir after initialisation");
        _baseDir = baseDir;
    }

    @Required
    public void setVersion(int version) {
        _version = version;
    }

    @Required
    public void setReplicationNotificationDestination(String address) {
        _replicationManager = (!address.isEmpty()) ? new CellPath(address) : null;
    }

    @Required
    public void setReplicationIp(String address) {
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
    public void setVolatile(boolean isVolatile) {
        _isVolatile = isVolatile;
    }

    public boolean isVolatile() {
        return _isVolatile;
    }

    @Required
    public void setHasTapeBackend(boolean hasTapeBackend) {
        _hasTapeBackend = hasTapeBackend;
    }

    public boolean getHasTapeBackend() {
        return _hasTapeBackend;
    }

    public void setP2PMode(String mode) {
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

    @Required
    public void setExecutor(Executor executor) {
        _executor = executor;
    }

    @Required
    public void setPoolUpDestination(String name) {
        _poolupDestination = name;
    }

    @Required
    public void setBillingStub(CellStub stub) {
        assertNotRunning("Cannot set billing stub after initialization");
        _billingStub = stub;
    }

    @Required
    public void setPnfsHandler(PnfsHandler pnfs) {
        assertNotRunning("Cannot set PNFS handler after initialization");
        _pnfs = pnfs;
    }

    @Required
    public void setRepository(Repository repository) {
        assertNotRunning("Cannot set repository after initialization");
        _repository = repository;
    }

    @Required
    public void setAccount(Account account) {
        assertNotRunning("Cannot set account after initialization");
        _account = account;
    }

    @Required
    public void setStorageQueue(StorageClassContainer queue) {
        assertNotRunning("Cannot set storage queue after initialization");
        _storageQueue = queue;
    }

    @Required
    public void setStorageHandler(NearlineStorageHandler handler) {
        assertNotRunning("Cannot set storage handler after initialization");
        _storageHandler = handler;
    }

    @Required
    public void setHSMSet(HsmSet set) {
        assertNotRunning("Cannot set HSM set after initialization");
        _hsmSet = set;
    }

    @Required
    public void setFlushController(HsmFlushController controller) {
        assertNotRunning("Cannot set flushing controller after initialization");
        _flushingThread = controller;
    }

    @Required
    public void setPPClient(P2PClient client) {
        assertNotRunning("Cannot set P2P client after initialization");
        _p2pClient = client;
    }

    @Required
    public void setReplicaStatePolicy(ReplicaStatePolicy replicaStatePolicy) {
        assertNotRunning("Cannot set replica state policy after initialization");
        _replicaStatePolicy = replicaStatePolicy;
    }

    @Required
    public void setTags(String tags) {
        Map<String, String> newTags = Splitter.on(' ')
              .omitEmptyStrings()
              .trimResults()
              .withKeyValueSeparator('=')
              .split(tags);

        newTags.forEach(((key, value) -> {
            if (RESERVED_TAGS.contains(key)) {
                LOGGER.warn("Skipping reserved tag: {}={}", key, value);
            } else {
                _tags.put(key, value);
                LOGGER.info("Tag: {}={}", key, value);
            }
        }));
    }

    @Required
    public void setIoQueueManager(IoQueueManager ioQueueManager) {
        assertNotRunning("Cannot set I/O queue manager after initialization");
        _ioQueue = ioQueueManager;
    }

    @Required
    public void setPoolMode(PoolV2Mode mode) {
        _poolMode = mode;
    }

    public PoolV2Mode getPoolMode() {
        return new PoolV2Mode(_poolMode.getMode());
    }

    @Required
    public void setTransferServices(TransferServices transferServices) {
        assertNotRunning("Cannot set transfer services after initialization");
        _transferServices = transferServices;
    }

    @Required
    public void setEnableHsmFlag(boolean enable) {
        _enableHsmFlag = enable;
    }

    @Override
    public void setZone(Optional<String> zone) {
        zone.ifPresent(z -> _tags.put(ZONE_TAG, z));
    }

    @Override
    public void setThreadFactory(ThreadFactory factory) {
        _threadFactory = factory;
    }

    public void init() {
        assertNotRunning("Cannot initialize several times");
        checkState(!_isVolatile || !_hasTapeBackend, "Volatile pool cannot have a tape backend");

        LOGGER.info("Pool {} starting", _poolName);

        _repository.addFaultListener(this);
        _repository.addListener(new RepositoryLoader());
        _repository.addListener(new NotifyBillingOnRemoveListener());
        if (_enableHsmFlag) {
            _repository.addListener(new HFlagMaintainer());
        }
        _repository.addListener(_replicationHandler);

        _ioQueue.addFaultListener(this);

        _running = true;
    }

    @Override
    public void afterStart() {
        disablePool(PoolV2Mode.DISABLED_STRICT, 1, "Awaiting initialization");
        _pingThread.start();
        _threadFactory.newThread(() -> {
            int mode;
            try {
                _repository.init();
                disablePool(PoolV2Mode.DISABLED_RDONLY_REPOSITORY_LOADING, 1, "Loading...");
                _repository.load();
                enablePool(PoolV2Mode.ENABLED);
                _flushingThread.start();
            } catch (RuntimeException e) {
                LOGGER.error(AlarmMarkerFactory.getMarker
                            (PredefinedAlarm.POOL_DISABLED, _poolName),
                      "Pool {} initialization failed, repository "
                            + "reported a problem."
                            + "Please report this to support@dcache.org.",
                      _poolName, e);
                LOGGER.warn("Pool not enabled {}", _poolName);
                disablePool(PoolV2Mode.DISABLED_DEAD
                            | PoolV2Mode.DISABLED_STRICT,
                      666, "Init failed: " + e.getMessage());
            } catch (Throwable e) {
                LOGGER.error(AlarmMarkerFactory.getMarker
                            (PredefinedAlarm.POOL_DISABLED, _poolName),
                      "Pool {} initialization failed, repository "
                            + "reported a problem ({}).",
                      _poolName, e.getMessage());
                LOGGER.warn("Pool not enabled {}", _poolName);
                disablePool(PoolV2Mode.DISABLED_DEAD
                            | PoolV2Mode.DISABLED_STRICT,
                      666, "Init failed: " + e.getMessage());
            }

            LOGGER.info("Repository finished");
        }).start();
    }

    @Override
    public void beforeStop() {
        _flushingThread.stop();
        _pingThread.stop();

        /*
         * No need for alarm here.
         */
        disablePool(PoolV2Mode.DISABLED_DEAD
              | PoolV2Mode.DISABLED_STRICT, 666, "Shutdown");
    }

    /**
     * Called by subsystems upon serious faults.
     */
    @Override
    public void faultOccurred(FaultEvent event) {
        Throwable cause = event.getCause();
        String poolState;
        PredefinedAlarm alarm;
        switch (event.getAction()) {
            case READONLY:
                poolState = "Pool read-only: ";
                disablePool(PoolV2Mode.DISABLED_RDONLY,
                      99, poolState + event.getMessage());
                alarm = null;
                break;

            case DISABLED:
                poolState = "Pool disabled: ";
                disablePool(PoolV2Mode.DISABLED_STRICT, 99, poolState + event.getMessage());
                alarm = PredefinedAlarm.POOL_DISABLED;
                break;

            default:
                poolState = "Pool restart required: ";
                disablePool(PoolV2Mode.DISABLED_STRICT
                            | PoolV2Mode.DISABLED_DEAD,
                      666, poolState + event.getMessage());
                alarm = PredefinedAlarm.POOL_DEAD;
                break;
        }

        if (alarm != null) {
            if (cause != null) {
                LOGGER.error(AlarmMarkerFactory.getMarker(alarm, _poolName),
                      "Pool: {}, fault occurred in {}: {}. {}, cause: {}",
                      _poolName, event.getSource(), event.getMessage(), poolState,
                      cause.toString());
            } else {
                LOGGER.error(AlarmMarkerFactory.getMarker(alarm, _poolName),
                      "Pool: {}, fault occurred in {}: {}. {}",
                      _poolName, event.getSource(), event.getMessage(), poolState);
            }
        }
    }

    /**
     * Sets the h-flag in PNFS.
     */
    private class HFlagMaintainer extends AbstractStateChangeListener {

        @Override
        public void stateChanged(StateChangeEvent event) {
            if (event.getOldState() == ReplicaState.FROM_CLIENT) {
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
    private class RepositoryLoader extends AbstractStateChangeListener {

        @Override
        public void stateChanged(StateChangeEvent event) {
            PnfsId id = event.getPnfsId();
            ReplicaState from = event.getOldState();
            ReplicaState to = event.getNewState();

            if (from == to) {
                return;
            }

            if (to == ReplicaState.PRECIOUS) {
                LOGGER.debug("Adding {} to flush queue", id);

                if (_hasTapeBackend) {
                    try {
                        _storageQueue.addCacheEntry(event.getNewEntry());
                    } catch (FileNotInCacheException e) {
                        /* File was deleted before we got a chance to do
                         * anything with it. We don't care about deleted
                         * files so we ignore this.
                         */
                        LOGGER.info("Failed to flush {}: Replica is no longer in the pool", id);
                    } catch (CacheException e) {
                        LOGGER.error("Error adding {} to flush queue: {}", id, e.getMessage());
                    }
                }
            } else if (from == ReplicaState.PRECIOUS) {
                LOGGER.debug("Removing {} from flush queue", id);
                if (!_storageQueue.removeCacheEntry(id)) {
                    LOGGER.info("File {} not found in flush queue", id);
                }
            }
        }
    }

    private class NotifyBillingOnRemoveListener
          extends AbstractStateChangeListener {

        @Override
        public void stateChanged(StateChangeEvent event) {
            if (_reportOnRemovals && event.getNewState() == ReplicaState.REMOVED) {
                CacheEntry entry = event.getNewEntry();
                RemoveFileInfoMessage msg =
                      new RemoveFileInfoMessage(getCellAddress(), entry.getPnfsId());
                msg.setFileSize(entry.getReplicaSize());
                msg.setStorageInfo(entry.getFileAttributes().getStorageInfo());
                msg.setSubject(Subjects.ROOT);
                msg.setResult(0, event.getWhy());
                _billingStub.notify(msg);

                try {
                    _kafkaSender.accept(msg);
                } catch (KafkaException e) {
                    LOGGER.warn(Throwables.getRootCause(e).getMessage());

                }
            }
        }
    }

    @Override
    public void printSetup(PrintWriter pw) {
        pw.println("set heartbeat " + _pingThread.getHeartbeat());
        pw.println("set report remove " + (_reportOnRemovals ? "on" : "off"));
        pw.println("set breakeven " + _breakEven);
        pw.println("set mover cost factor " + _moverCostFactor);
        if (_suppressHsmLoad) {
            pw.println("pool suppress hsmload on");
        }
    }

    @Override
    public CellInfo getCellInfo(CellInfo info) {
        PoolCellInfo poolinfo = new PoolCellInfo(info);
        poolinfo.setPoolCostInfo(getPoolCostInfo());
        poolinfo.setTagMap(_tags);
        poolinfo.setErrorStatus(_poolStatusCode, _poolStatusMessage);
        poolinfo.setCellVersion(VERSION);
        return poolinfo;
    }

    public ImmutableMap<String, String> getTagMap() {
        return ImmutableMap.copyOf(_tags);
    }

    @Override
    public void getInfo(PrintWriter pw) {
        getDataObject().print(pw);
    }

    @Override
    public PoolDataDetails getDataObject() {
        PoolDataDetails info = new PoolDataDetails();
        info.setLabel("Pool Details");

        info.setBaseDir(_baseDir);
        info.setBreakEven(getBreakEven());

        if (_hybridInventoryActive) {
            info.setHybridInventory(_hybridCurrent);
        }

        info.setInetAddresses(NetworkUtils.getLocalAddresses().toArray(InetAddress[]::new));

        if (_hasTapeBackend) {
            info.setLargeFileStore(Lsf.NONE);
        } else if (_isVolatile) {
            info.setLargeFileStore(Lsf.VOLATILE);
        } else {
            info.setLargeFileStore(Lsf.PRECIOUS);
        }

        info.setPingHeartbeatInSecs(_pingThread.getHeartbeat());
        info.setP2pFileMode(_p2pFileMode == P2P_PRECIOUS ?
              P2PMode.PRECIOUS : P2PMode.CACHED);
        info.setPoolMode(_poolMode.toString());
        if (_poolMode.isDisabled()) {
            info.setPoolStatusCode(_poolStatusCode);
            info.setPoolStatusMessage(_poolStatusMessage);
        }

        info.setPoolVersion(VERSION + " (Sub=" + _version + ")");
        info.setRemovalReported(_reportOnRemovals);
        info.setHsmLoadSuppressed(_suppressHsmLoad);
        info.setTagMap(getTagMap());

        info.setErrorCode(_poolStatusCode);
        info.setErrorMessage(_poolStatusMessage);

        info.setCostData(new PoolCostData(getPoolCostInfo()));

        return info;
    }

    // //////////////////////////////////////////////////////////////
    //
    // The io File Part
    //
    //

    public Mover<?> createMover(CellMessage envelop, PoolIoFileMessage message)
          throws CacheException {
        CellPath source = envelop.getSourcePath().revert();
        FileAttributes attributes = message.getFileAttributes();
        PnfsId pnfsId = attributes.getPnfsId();
        ProtocolInfo pi = message.getProtocolInfo();

        MoverFactory moverFactory = _transferServices.getMoverFactory(pi);
        ReplicaDescriptor handle;
        try {
            if (message instanceof PoolAcceptFileMessage) {
                OptionalLong maximumSize = ((PoolAcceptFileMessage) message).getMaximumSize();
                List<StickyRecord> stickyRecords =
                      _replicaStatePolicy.getStickyRecords(attributes);
                ReplicaState targetState =
                      _replicaStatePolicy.getTargetState(attributes);
                handle = _repository.createEntry(attributes,
                      ReplicaState.FROM_CLIENT,
                      targetState,
                      stickyRecords,
                      moverFactory.getChannelCreateOptions(),
                      maximumSize);
            } else {
                Set<? extends OpenOption> openFlags =
                      message.isPool2Pool()
                            ? EnumSet.of(Repository.OpenFlags.NOATIME)
                            : EnumSet.noneOf(Repository.OpenFlags.class);
                handle = _repository.openEntry(pnfsId, openFlags);
            }
        } catch (FileNotInCacheException e) {
            throw new FileNotInCacheException("File " + pnfsId + " does not exist in " + _poolName,
                  e);
        } catch (FileInCacheException e) {
            throw new FileInCacheException("File " + pnfsId + " already exists in " + _poolName, e);
        }
        try {
            return moverFactory.createMover(handle, message, source);
        } catch (Throwable t) {
            handle.close();
            throw t;
        }
    }

    private int queueIoRequest(CellMessage envelope, PoolIoFileMessage message)
          throws CacheException {
        message.getAssumption().check(_poolInfo);

        String queueName = message.getIoQueueName();
        String doorUniqueId = envelope.getSourceAddress().toString() + message.getId();

        if (message instanceof PoolAcceptFileMessage) {
            return _ioQueue.getOrCreateMover(queueName, doorUniqueId,
                  () -> createMover(envelope, message), IoPriority.HIGH);
        } else if (message.isPool2Pool()) {
            return _ioQueue.getOrCreateMover(IoQueueManager.P2P_QUEUE_NAME, doorUniqueId,
                  () -> createMover(envelope, message), IoPriority.HIGH);
        } else {
            return _ioQueue.getOrCreateMover(queueName, doorUniqueId,
                  () -> createMover(envelope, message), IoPriority.REGULAR);
        }
    }

    private void ioFile(CellMessage envelope, PoolIoFileMessage message) {
        try {
            message.setMoverId(queueIoRequest(envelope, message));
            message.setSucceeded();
        } catch (OutOfDateCacheException e) {
            if (_pingLimiter.tryAcquire()) {
                _pingThread.sendPoolManagerMessage();
            }
            message.setFailed(e.getRc(), e.getMessage());
        } catch (FileNotInCacheException | FileInCacheException e) {
            LOGGER.warn(e.getMessage());
            message.setFailed(e.getRc(), e.getMessage());
        } catch (LockedCacheException e) {
            LOGGER.info(e.getMessage());
            message.setFailed(e.getRc(), e.getMessage());
        } catch (CacheException e) {
            LOGGER.error(e.getMessage());
            message.setFailed(e.getRc(), e.getMessage());
        } catch (RuntimeException e) {
            LOGGER.error("Please report the following stack-trace to <support@dcache.org>", e);
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
          extends AbstractStateChangeListener {

        @Override
        public void stateChanged(StateChangeEvent event) {
            ReplicaState from = event.getOldState();
            ReplicaState to = event.getNewState();

            if (to == ReplicaState.CACHED || to == ReplicaState.PRECIOUS) {
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
        public String toString() {
            if (_replicationManager != null) {
                return "{Mgr=" + _replicationManager + ",Host=" + _replicationIp + "}";
            } else {
                return "Disabled";
            }
        }

        private void initiateReplication(PnfsId id, String source) {
            if (_replicationManager != null) {
                try {
                    _initiateReplication(_repository.getEntry(id), source);
                } catch (InterruptedException e) {
                    LOGGER.warn("Replication request was interrupted");
                    Thread.currentThread().interrupt();
                } catch (CacheException e) {
                    LOGGER.error("Problem in sending replication request: {}", e.getMessage());
                }
            }
        }

        private void _initiateReplication(CacheEntry entry, String source) {
            FileAttributes attributes = entry.getFileAttributes().clone();
            attributes.setLocations(Collections.singleton(_poolName));
            attributes.getStorageInfo().setKey("replication.source", source);

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
          implements CompletionHandler<Void, PnfsId> {

        private final PoolFetchFileMessage _message;

        private ReplyToPoolFetch(PoolFetchFileMessage message) {
            _message = message;
        }

        @Override
        public void completed(Void result, PnfsId pnfsId) {
            _message.setSucceeded();
            reply(_message);
        }

        @Override
        public void failed(Throwable exc, PnfsId pnfsId) {
            if (exc instanceof CacheException) {
                CacheException ce = (CacheException) exc;
                int errorCode = ce.getRc();
                switch (errorCode) {
                    case CacheException.FILE_IN_CACHE:
                        LOGGER.info("Pool already contains replica");
                        _message.setSucceeded();
                        break;
                    case CacheException.ERROR_IO_DISK:
                    case 41:
                    case 42:
                    case 43:
                        disablePool(PoolV2Mode.DISABLED_STRICT, errorCode, ce.getMessage());
                        LOGGER.error(AlarmMarkerFactory.getMarker(PredefinedAlarm.POOL_DISABLED,
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

    private static class CompanionFileAvailableCallback
          extends DelayedReply
          implements CacheFileAvailable {

        private final Pool2PoolTransferMsg _message;

        private CompanionFileAvailableCallback(Pool2PoolTransferMsg message) {
            _message = message;
        }

        @Override
        public void cacheFileAvailable(PnfsId pnfsId, Throwable error) {
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

    public PoolMoverKillMessage messageArrived(PoolMoverKillMessage kill) {
        if (kill.isReply()) {
            return null;
        }

        try {
            int id = kill.getMoverId();
            MoverRequestScheduler js = _ioQueue.getQueueByJobId(id)
                  .orElseThrow(() -> new NoSuchElementException(
                        "Id doesn't belong to any known scheduler."));
            String explanation = kill.getExplanation();
            LOGGER.info("Killing mover {}: {}", id, explanation);
            if (!js.cancel(id, explanation)) {
                throw new NoSuchElementException("Job " + id + " not found");
            }
            kill.setSucceeded();
        } catch (NoSuchElementException e) {
            LOGGER.info(e.toString());
            kill.setReply(CacheException.MOVER_NOT_FOUND, e);
        }
        return kill;
    }

    public void messageArrived(CellMessage envelope, PoolIoFileMessage msg)
          throws CacheException {
        if (msg.isReply()) {
            return;
        }

        if ((msg instanceof PoolAcceptFileMessage
              && _poolMode.isDisabled(PoolV2Mode.DISABLED_STORE))
              || (msg instanceof PoolDeliverFileMessage
              && _poolMode.isDisabled(PoolV2Mode.DISABLED_FETCH))) {

            if (!msg.isForceSourceMode()) {
                LOGGER.warn("PoolIoFileMessage request rejected due to {}", _poolMode);
                throw new CacheException(CacheException.POOL_DISABLED, "Pool is disabled");
            }
        }

        msg.setReply();
        ioFile(envelope, msg);
    }

    public DelayedReply messageArrived(Pool2PoolTransferMsg msg)
          throws CacheException, IOException, InterruptedException {
        if (_poolMode.isDisabled(PoolV2Mode.DISABLED_P2P_CLIENT)) {
            LOGGER.warn("Pool2PoolTransferMsg request rejected due to {}", _poolMode);
            throw new CacheException(CacheException.POOL_DISABLED, "Pool is disabled");
        }

        String poolName = msg.getPoolName();
        FileAttributes fileAttributes = msg.getFileAttributes();
        CompanionFileAvailableCallback callback =
              new CompanionFileAvailableCallback(msg);

        ReplicaState targetState = ReplicaState.CACHED;
        int fileMode = msg.getDestinationFileStatus();
        if (fileMode != Pool2PoolTransferMsg.UNDETERMINED) {
            if (fileMode == Pool2PoolTransferMsg.PRECIOUS) {
                targetState = ReplicaState.PRECIOUS;
            }
        } else if (!_hasTapeBackend && !_isVolatile
              && (_p2pFileMode == P2P_PRECIOUS)) {
            targetState = ReplicaState.PRECIOUS;
        }

        List<StickyRecord> stickyRecords = Collections.emptyList();
        _p2pClient.newCompanion(poolName, fileAttributes,
              targetState, stickyRecords, callback, false, null);
        return callback;
    }

    public Object messageArrived(PoolFetchFileMessage msg)
          throws CacheException {
        if (_poolMode.isDisabled(PoolV2Mode.DISABLED_STAGE)) {
            LOGGER.warn("PoolFetchFileMessage request rejected due to {}", _poolMode);
            throw new CacheException(CacheException.POOL_DISABLED, "Pool is disabled");
        }
        if (!_hasTapeBackend) {
            LOGGER.warn("PoolFetchFileMessage request rejected due to LFS mode");
            throw new CacheException(CacheException.POOL_DISABLED, "Pool has no tape backend");
        }

        FileAttributes fileAttributes = msg.getFileAttributes();
        String hsm = _hsmSet.getInstanceName(fileAttributes);
        LOGGER.info("Pool {} asked to fetch file {} (hsm={})",
              _poolName, fileAttributes.getPnfsId(), hsm);
        ReplyToPoolFetch reply = new ReplyToPoolFetch(msg);
        _storageHandler.stage(hsm, fileAttributes, reply);
        return reply;
    }

    private static class RemoveFileReply extends DelayedReply implements
          CompletionHandler<Void, URI> {

        private final PoolRemoveFilesFromHSMMessage msg;
        private final Collection<URI> succeeded;
        private final Collection<URI> failed;

        private RemoveFileReply(PoolRemoveFilesFromHSMMessage msg) {
            this.msg = msg;
            succeeded = new ArrayList<>(msg.getFiles().size());
            failed = new ArrayList<>();
        }

        @Override
        public void completed(Void nil, URI uri) {
            succeeded.add(uri);
            sendIfFinished();
        }

        @Override
        public void failed(Throwable exc, URI uri) {
            failed.add(uri);
            sendIfFinished();
        }

        private void sendIfFinished() {
            if (succeeded.size() + failed.size() >= msg.getFiles().size()) {
                msg.setResult(succeeded, failed);
                msg.setSucceeded();
                reply(msg);
            }
        }
    }

    public Reply messageArrived(PoolRemoveFilesFromHSMMessage msg)
          throws CacheException {
        if (_poolMode.isDisabled(PoolV2Mode.DISABLED_STAGE)) {
            LOGGER.warn("PoolRemoveFilesFromHsmMessage request rejected due to {}", _poolMode);
            throw new CacheException(CacheException.POOL_DISABLED, "Pool is disabled");
        }
        if (!_hasTapeBackend) {
            LOGGER.warn("PoolRemoveFilesFromHsmMessage request rejected due to LFS mode");
            throw new CacheException(CacheException.POOL_DISABLED, "Pool has no tape backend");
        }

        RemoveFileReply reply = new RemoveFileReply(msg);
        _storageHandler.remove(msg.getHsm(), msg.getFiles(), reply);
        return reply;
    }

    public PoolCheckFreeSpaceMessage
    messageArrived(PoolCheckFreeSpaceMessage msg) {
        msg.setFreeSpace(_account.getFree());
        msg.setSucceeded();
        return msg;
    }

    public Reply messageArrived(CellMessage envelope, PoolRemoveFilesMessage msg)
          throws CacheException {
        if (_poolMode.isDisabled(PoolV2Mode.DISABLED)) {
            LOGGER.warn("PoolRemoveFilesMessage request rejected due to {}", _poolMode);
            throw new CacheException(CacheException.POOL_DISABLED, "Pool is disabled");
        }

        String why = envelope.getSourceAddress() + " [" + msg.getDiagnosticContext() + "]";

        List<ListenableFutureTask<String>> tasks = Stream.of(msg.getFiles())
              .map(file -> ListenableFutureTask.create(() -> remove(file, why))).collect(toList());
        tasks.forEach(_executor::execute);
        MessageReply<PoolRemoveFilesMessage> reply = new MessageReply<>();
        Futures.addCallback(Futures.allAsList(tasks),
              new FutureCallback<List<String>>() {
                  @Override
                  public void onSuccess(List<String> files) {
                      String[] replyList = files.stream().filter(Objects::nonNull)
                            .toArray(String[]::new);
                      if (replyList.length > 0) {
                          msg.setFailed(1, replyList);
                      } else {
                          msg.setSucceeded();
                      }
                      reply.reply(msg);
                  }

                  @Override
                  public void onFailure(Throwable t) {
                      reply.fail(msg, t);
                  }
              },
              MoreExecutors.directExecutor());
        return reply;
    }

    private String remove(String file, String why) throws CacheException, InterruptedException {
        try {
            PnfsId pnfsId = new PnfsId(file);
            _repository.setState(pnfsId, ReplicaState.REMOVED, why);
            return null;
        } catch (IllegalTransitionException e) {
            LOGGER.error("Replica {} not removed: {}", file, e.getMessage());
            return file;
        }
    }

    public PoolModifyPersistencyMessage messageArrived(CellMessage envelope,
          PoolModifyPersistencyMessage msg) {
        try {
            PnfsId pnfsId = msg.getPnfsId();
            switch (_repository.getState(pnfsId)) {
                case PRECIOUS:
                    if (msg.isCached()) {
                        _repository.setState(pnfsId, ReplicaState.CACHED,
                              "At request of " + envelope.getSourceAddress());
                    }
                    msg.setSucceeded();
                    break;

                case CACHED:
                    if (msg.isPrecious()) {
                        _repository.setState(pnfsId, ReplicaState.PRECIOUS,
                              "At request of " + envelope.getSourceAddress());
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

    public PoolModifyModeMessage messageArrived(PoolModifyModeMessage msg) {
        PoolV2Mode mode = msg.getPoolMode();
        boolean isRepositoryLoading =
              _poolMode.isDisabled(PoolV2Mode.REPOSITORY_LOADING);
        if (mode != null) {
            if (mode.isEnabled()) {
                if (isRepositoryLoading) {
                    msg.setFailed(CacheException.DEFAULT_ERROR_CODE,
                          "The pool repository is loading, cannot enable pool");
                    return msg;
                }
                enablePool(mode.getMode());
            } else {
                int targetMode = (mode.getMode() & ~PoolV2Mode.REPOSITORY_LOADING) |
                      (_poolMode.getMode() & PoolV2Mode.REPOSITORY_LOADING);
                /**
                 if Repository is loading only two states are allowd -
                 PoolV2Mode.DISABLED_STRICT or PoolV2Mode.DISABLED_RDONLY
                 */
                if (isRepositoryLoading &&
                      !((targetMode & PoolV2Mode.DISABLED_STRICT) ==
                            PoolV2Mode.DISABLED_STRICT) &&
                      !((targetMode & PoolV2Mode.DISABLED_RDONLY) ==
                            PoolV2Mode.DISABLED_RDONLY)) {
                    msg.setFailed(CacheException.DEFAULT_ERROR_CODE,
                          "The pool repository is loading, pool cannot be set to " +
                                mode + " state");
                    return msg;
                }
                disablePool(targetMode, msg.getStatusCode(),
                      msg.getStatusMessage());
            }
        }
        msg.setSucceeded();
        return msg;
    }

    public PoolSetStickyMessage messageArrived(PoolSetStickyMessage msg)
          throws CacheException, InterruptedException {
        if (_poolMode.isDisabled(PoolV2Mode.DISABLED_STRICT)) {
            LOGGER.warn("PoolSetStickyMessage request rejected due to {}", _poolMode);
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

    public CacheRepositoryEntryInfo getCacheRepositoryEntryInfo(PnfsId pnfsid)
          throws CacheException, InterruptedException {
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
                throw new IllegalArgumentException(
                      "Bug. An entry should never be in " + entry.getState());
        }
        if (entry.isSticky()) {
            bitmask |= 1 << CacheRepositoryEntryInfo.STICKY_BIT;
        }
        return new CacheRepositoryEntryInfo(entry.getPnfsId(), bitmask,
              entry.getLastAccessTime(),
              entry.getCreationTime(),
              entry.getReplicaSize());
    }

    /**
     * Partially or fully disables normal operation of this pool.
     */
    private synchronized void disablePool(int mode,
          int errorCode, String errorString) {
        _poolStatusCode = errorCode;
        _poolStatusMessage =
              (errorString == null) ? "Requested by operator" : errorString;
        _poolMode.setMode(mode);

        _pingThread.sendPoolManagerMessage();
        LOGGER.warn("Pool mode changed to {}: {}", _poolMode, _poolStatusMessage);
    }

    /**
     * Fully enables this pool. The status code is set to 0 and the status message is cleared.
     */
    private synchronized void enablePool(int mode) {
        _poolMode.setMode(mode);
        _poolStatusCode = 0;
        _poolStatusMessage = "OK";

        _pingThread.sendPoolManagerMessage();
        LOGGER.warn("Pool mode changed to {}", _poolMode);
    }

    private class PoolManagerPingThread implements Runnable {

        private final Thread _worker;
        private int _heartbeat = HEARTBEAT;

        private PoolManagerPingThread() {
            _worker = new Thread(this, "ping");
        }

        public void start() {
            _worker.start();
        }

        public void stop() {
            _worker.interrupt();
        }

        @Override
        public void run() {
            LOGGER.debug("Ping thread started");
            try {
                while (!Thread.interrupted()) {
                    sendPoolManagerMessage();
                    Thread.sleep(_heartbeat * 1000);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                LOGGER.debug("Ping Thread finished");
            }
        }

        public void setHeartbeat(int seconds) {
            _heartbeat = seconds;
        }

        public int getHeartbeat() {
            return _heartbeat;
        }

        public synchronized void sendPoolManagerMessage() {
            send(getPoolManagerMessage());
        }

        private CellMessage getPoolManagerMessage() {
            boolean disabled =
                  _poolMode.getMode() == PoolV2Mode.DISABLED ||
                        _poolMode.isDisabled(PoolV2Mode.DISABLED_STRICT);
            PoolCostInfo info = disabled ? null : getPoolCostInfo();

            PoolManagerPoolUpMessage poolManagerMessage =
                  new PoolManagerPoolUpMessage(_poolName, _serialId,
                        _poolMode, info);

            poolManagerMessage.setHostName(NetworkUtils.getCanonicalHostName());
            poolManagerMessage.setTagMap(_tags);
            if (_hasTapeBackend && (_hsmSet != null)) {
                poolManagerMessage.setHsmInstances(new TreeSet<>(_hsmSet
                      .getHsmInstances()));
            }
            poolManagerMessage.setMessage(_poolStatusMessage);
            poolManagerMessage.setCode(_poolStatusCode);

            return new CellMessage(new CellPath(_poolupDestination),
                  poolManagerMessage);
        }

        private void send(CellMessage msg) {
            sendMessage(msg);
        }
    }

    public PoolCostInfo getPoolCostInfo() {
        return new PoolCostInfo(IoQueueManager.DEFAULT_QUEUE, _poolInfo);
    }

    @AffectsSetup
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
    public class SetBreakevenCommand implements Callable<String> {

        @Argument(usage = "Specify the breakeven value. This value has to be greater than or " +
              "equal zero")
        double parameter = _breakEven;

        @Override
        public String call() throws IllegalArgumentException {
            checkArgument(parameter >= 0,
                  "The breakeven parameter must be greater than or equal to zero.");

            if (parameter >= 1) {
                parameter = DEFAULT_BREAK_EVEN;
            }
            _breakEven = parameter;
            return "BreakEven = " + getBreakEven();
        }

    }

    public double getBreakEven() {
        return _breakEven;
    }

    @AffectsSetup
    @Command(name = "set mover cost factor", hint = "set the selectivity of this pool by movers",
          description = "The mover cost factor controls how much the number of movers " +
                "affects proportional pool selection.\n\n" +
                "Intuitively, for every 1/f movers, where f is the mover cost " +
                "factor, the probability of choosing this pools is halved. When " +
                "set to zero, the number of movers does not affect pool selection.")
    public class SetMoverCostFactorCommand implements Callable<String> {

        @Argument(usage = "Specify the cost factor value. This value " +
              "must be greater than or equal to 0.0.", required = false)
        double value = _moverCostFactor;

        @Override
        public String call() throws IllegalArgumentException {
            checkArgument(value > 0, "Mover cost factor must be larger than or equal to 0.0");

            _moverCostFactor = value;
            return "Cost factor is " + _moverCostFactor;
        }
    }

    // /////////////////////////////////////////////////
    //
    // the hybrid inventory part
    //
    private class HybridInventory implements Runnable {

        private boolean _activate = true;

        public HybridInventory(boolean activate) {
            _activate = activate;
            new Thread(this, "HybridInventory").start();
        }

        private void addCacheLocation(PnfsId id) {
            try {
                _pnfs.addCacheLocation(id);
            } catch (FileNotFoundCacheException e) {
                try {
                    _repository.setState(id, ReplicaState.REMOVED,
                          "PnfsManager claimed file not found during 'pnfs register' command");
                    LOGGER.info("File not found in PNFS; removed {}", id);
                } catch (InterruptedException | CacheException f) {
                    LOGGER.error("File not found in PNFS, but failed to remove {}: {}", id, f);
                }
            } catch (CacheException e) {
                LOGGER.error("Cache location was not registered for {}: {}", id, e.getMessage());
            }
        }

        private void clearCacheLocation(PnfsId id) {
            _pnfs.clearCacheLocation(id);
        }

        @Override
        public void run() {
            _hybridCurrent = 0;

            long startTime, stopTime;
            if (_activate) {
                LOGGER.info("Registering all replicas in PNFS");
            } else {
                LOGGER.info("Unregistering all replicas in PNFS");
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
                    LOGGER.warn(e.getMessage());
                } catch (InterruptedException e) {
                    break;
                }
            }
            stopTime = System.currentTimeMillis();
            synchronized (_hybridInventoryLock) {
                _hybridInventoryActive = false;
            }

            LOGGER.info("Replica {} finished. {} replicas processed in {} msec",
                  (_activate ? "registration" : "deregistration"),
                  _hybridCurrent,
                  (stopTime - startTime));
        }
    }

    private void startHybridInventory(boolean register)
          throws IllegalArgumentException {
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
    public class PnfsRegisterCommand implements Callable<String> {

        @Override
        public String call() throws IllegalArgumentException {
            startHybridInventory(true);
            return "";
        }
    }

    @Command(name = "pnfs unregister",
          hint = "remove file locations from namespace",
          description = "Unregister all file replicas of this pool from the namespace.")
    public class PnfsUnregisterCommand implements Callable<String> {

        @Override
        public String call() throws IllegalArgumentException {
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
    public class PfCommand implements Callable<String> {

        @Argument(usage = "Specify the pnfsid of the file.")
        PnfsId pnfsId;

        @Override
        public String call() throws CacheException, IllegalArgumentException {
            return _pnfs.getPathByPnfsId(pnfsId).toString();
        }
    }

    @Command(name = "set replication")
    class SetReplicationCommand implements Callable<String> {

        @Option(name = "off")
        boolean off;

        @Argument(required = false, index = 0)
        String mgr;

        @Argument(required = false, index = 1)
        String host;

        @Override
        public String call() {
            if (off) {
                setReplicationNotificationDestination("");
            } else if (mgr != null) {
                setReplicationNotificationDestination(mgr);
                if (host != null) {
                    setReplicationIp(host);
                }
            }
            return _replicationHandler.toString();
        }
    }

    @Command(name = "pool suppress hsmload")
    @AffectsSetup
    class SetPoolSuppressCommand implements Callable<String> {

        @Argument(valueSpec = "on|off")
        String mode;

        @Override
        public String call() throws CommandSyntaxException {
            switch (mode) {
                case "on":
                    _suppressHsmLoad = true;
                    break;
                case "off":
                    _suppressHsmLoad = false;
                    break;
                default:
                    throw new CommandSyntaxException(
                          "Illegal syntax : pool suppress hsmload on|off");
            }

            return "hsm load suppression switched : " + (_suppressHsmLoad ? "on" : "off");
        }
    }

    @Command(name = "set duplicate request")
    class SetDuplicateRequestCommand implements Callable<String> {

        @Argument(valueSpec = "none|ignore|refresh")
        String mode;

        @Override
        public String call() throws CommandSyntaxException {
            // command remove. keep this for backward compatibility
            switch (mode) {
                case "none":
                case "ignore":
                case "refresh":
                    break;
                default:
                    throw new CommandSyntaxException("Not Found : ",
                          "Usage : pool duplicate request none|ignore|refresh");
            }
            return "Support for 'set duplicate request' has been removed. If you see this command on pool startup, please update the pool configuration.";
        }
    }

    @Command(name = "pool disable")
    class PoolDisableCommand implements Callable<String> {

        @Option(name = "fetch", usage = "disallows fetch (transfer to client)")
        boolean fetch;

        @Option(name = "stage", usage = "disallows staging (from HSM)")
        boolean stage;

        @Option(name = "store", usage = "disallows store (transfer from client)")
        boolean store;

        @Option(name = "p2p-client", usage = "disallows pool to pool transfers to this pool")
        boolean p2pClient;

        @Option(name = "p2p-server", usage = "disallows pool to pool transfers from this pool")
        boolean p2pServer;

        @Option(name = "rdonly", usage = "equivalent to -store -stage -p2p-client")
        boolean rdonly;

        @Option(name = "draining", usage = "equivalent to -store -stage -p2p-client, "
              + "with an additional flag to indicate to QoS to treat this pool as "
              + "offline (and thus to make an additional replica of all files on it).")
        boolean draining;

        @Option(name = "strict", usage = "disallows everything")
        boolean strict;

        @Argument(required = false, index = 0)
        int errorCode = 1;

        @Argument(required = false, index = 1)
        String errorMessage = "Operator intervention";

        @Override
        public String call() {
            if (_poolMode.isDisabled(PoolV2Mode.DISABLED_DEAD)) {
                return "The pool is dead and a restart is required to enable it";
            }

            int modeBits = PoolV2Mode.DISABLED;
            if (strict) {
                modeBits |= PoolV2Mode.DISABLED_STRICT;
            }
            if (draining) {
                modeBits |= PoolV2Mode.DRAINING;
            }
            if (stage) {
                modeBits |= PoolV2Mode.DISABLED_STAGE;
            }
            if (fetch) {
                modeBits |= PoolV2Mode.DISABLED_FETCH;
            }
            if (store) {
                modeBits |= PoolV2Mode.DISABLED_STORE;
            }
            if (p2pClient) {
                modeBits |= PoolV2Mode.DISABLED_P2P_CLIENT;
            }
            if (p2pServer) {
                modeBits |= PoolV2Mode.DISABLED_P2P_SERVER;
            }
            if (rdonly) {
                modeBits |= PoolV2Mode.DISABLED_RDONLY;
            }
            if (_poolMode.isDisabled(PoolV2Mode.REPOSITORY_LOADING)) {
                modeBits |= PoolV2Mode.REPOSITORY_LOADING;
            }

            if ((modeBits & PoolV2Mode.REPOSITORY_LOADING) ==
                  PoolV2Mode.REPOSITORY_LOADING &&
                  !((modeBits & PoolV2Mode.DISABLED_STRICT) ==
                        PoolV2Mode.DISABLED_STRICT) &&
                  !((modeBits & PoolV2Mode.DISABLED_RDONLY) ==
                        PoolV2Mode.DISABLED_RDONLY)) {
                PoolV2Mode mode = new PoolV2Mode(modeBits);
                return "The pool repository is loading, pool cannot be set to " +
                      mode + " state";
            }

            disablePool(modeBits, errorCode, errorMessage);

            return "Pool " + _poolName + " " + _poolMode;
        }
    }

    @Command(name = "pool enable")
    class PoolEnableCommand implements Callable<String> {

        @Override
        public String call() throws CommandException {
            if (_poolMode.isDisabled(PoolV2Mode.DISABLED_DEAD)) {
                throw new
                      CommandException("The pool is dead and a restart is required to enable it");
            }
            if (_poolMode.isDisabled(PoolV2Mode.REPOSITORY_LOADING)) {
                throw new
                      CommandException("The pool repository is loading, cannot enable the pool");
            }

            enablePool(PoolV2Mode.ENABLED);
            return "Pool " + _poolName + " " + _poolMode;
        }
    }

    @AffectsSetup
    @Command(name = "set report remove")
    class SetReportRemoveCommand implements Callable<String> {

        @Argument(valueSpec = "on|off")
        String onoff;

        @Override
        public String call() throws CommandSyntaxException {
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
    }

    @AffectsSetup
    @Command(name = "set heartbeat",
          hint = "set time interval for sending this pool cost info",
          description = "Set the regular time interval at which this pool " +
                "sent it cost information to the pool manager. The sent " +
                "pool cost information are stored and will be use for load " +
                "balancing and pool selection. However, this time interval " +
                "is not guaranteed to be exact since this operation is limited " +
                "by the underlying OS where this pool reside.")
    public class SetHeartbeatCommand implements Callable<String> {

        @Argument(usage = "Specify the time interval in seconds. This " +
              "value has to be a positive integer.")
        int value = HEARTBEAT;

        @Override
        public String call() throws IllegalArgumentException {
            checkArgument(value > 0, "The heartbeat value must be a positive integer.");
            _pingThread.setHeartbeat(value);

            return "Heartbeat set to " + (_pingThread.getHeartbeat()) + " seconds";
        }
    }

    private static PoolCostInfo.NamedPoolQueueInfo toNamedPoolQueueInfo(
          MoverRequestScheduler queue) {
        return queue.getQueueInfo();
    }

    /**
     * Provides access to various performance metrics of the pool.
     */
    private class PoolInfo implements Assumption.Pool {

        @Override
        public String name() {
            return _poolName;
        }

        @Override
        public PoolCostInfo.PoolSpaceInfo space() {
            SpaceRecord space = _repository.getSpaceRecord();
            return new PoolCostInfo.PoolSpaceInfo(space.getTotalSpace(), space.getFreeSpace(),
                  space.getPreciousSpace(),
                  space.getRemovableSpace(), space.getLRU(), _breakEven, space.getGap());
        }

        @Override
        public double moverCostFactor() {
            return _moverCostFactor;
        }

        @Override
        public PoolCostInfo.NamedPoolQueueInfo getMoverQueue(String name) {
            return toNamedPoolQueueInfo(_ioQueue.getQueueByNameOrDefault(name));
        }

        @Override
        public Collection<PoolCostInfo.NamedPoolQueueInfo> movers() {
            return _ioQueue.queues().stream()
                  .filter(q -> !q.getName().equals(IoQueueManager.P2P_QUEUE_NAME))
                  .map(PoolV4::toNamedPoolQueueInfo)
                  .collect(toList());
        }

        @Override
        public PoolCostInfo.PoolQueueInfo p2PClient() {
            int active = _p2pClient.getActiveJobs();
            return new PoolCostInfo.PoolQueueInfo(active, 0, 0, 0, active);
        }

        @Override
        public PoolCostInfo.PoolQueueInfo p2pServer() {
            return toNamedPoolQueueInfo(_ioQueue.getPoolToPoolQueue());
        }

        @Override
        public PoolCostInfo.PoolQueueInfo restore() {
            int active = _storageHandler.getActiveFetchJobs();
            int queued = _storageHandler.getFetchQueueSize();
            return new PoolCostInfo.PoolQueueInfo(active, 0, queued, 0, active + queued);
        }

        @Override
        public PoolCostInfo.PoolQueueInfo store() {
            int active = _storageHandler.getActiveStoreJobs();
            int queued = _storageHandler.getStoreQueueSize();
            return new PoolCostInfo.PoolQueueInfo(active, 0, queued, 0, active + queued);
        }
    }
}
