// $Id: MultiProtocolPoolV3.java,v 1.16 2007-10-26 11:17:06 behrmann Exp $

package org.dcache.pool.classic;

import java.io.IOException;
import java.io.PrintWriter;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.TreeSet;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.dcache.pool.FaultListener;
import org.dcache.pool.FaultEvent;
import org.dcache.pool.repository.v5.CacheRepositoryV5;
import org.dcache.pool.repository.IllegalTransitionException;
import org.dcache.pool.repository.SpaceRecord;
import org.dcache.pool.repository.EntryState;
import org.dcache.pool.repository.CacheEntry;
import org.dcache.pool.repository.AbstractStateChangeListener;
import org.dcache.pool.repository.StateChangeEvent;
import org.dcache.pool.repository.StickyRecord;
import org.dcache.pool.repository.Account;
import org.dcache.pool.p2p.P2PClient;
import org.dcache.pool.movers.MoverProtocol;
import org.dcache.cells.CellCommandListener;
import org.dcache.cells.CellMessageReceiver;
import org.dcache.cells.AbstractCellComponent;
import diskCacheV111.pools.PoolV2Mode;
import diskCacheV111.pools.PoolCostInfo;
import diskCacheV111.pools.PoolCellInfo;
import diskCacheV111.repository.CacheRepositoryEntryInfo;
import diskCacheV111.repository.RepositoryCookie;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.CacheFileAvailable;
import diskCacheV111.util.FileInCacheException;
import diskCacheV111.util.FileNotInCacheException;
import diskCacheV111.util.HsmSet;
import diskCacheV111.util.JobScheduler;
import diskCacheV111.util.PnfsHandler;
import diskCacheV111.util.PnfsId;
import diskCacheV111.util.SimpleJobScheduler;
import diskCacheV111.util.UnitInteger;
import diskCacheV111.vehicles.DCapProtocolInfo;
import diskCacheV111.vehicles.InfoMessage;
import diskCacheV111.vehicles.IoJobInfo;
import diskCacheV111.vehicles.JobInfo;
import diskCacheV111.vehicles.Message;
import diskCacheV111.vehicles.PnfsMapPathMessage;
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
import dmg.cells.nucleus.DelayedReply;
import dmg.cells.nucleus.CellInfo;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.CellVersion;
import dmg.cells.nucleus.NoRouteToCellException;
import dmg.util.Args;
import dmg.util.CommandSyntaxException;
import java.util.Arrays;

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
    private final Args _args;

    private final Map<String, Class<?>> _moverHash =
        new ConcurrentHashMap<String, Class<?>>();

    /**
     * pool start time identifier.
     * used by PoolManager to recognize pool restarts
     */
    private final long _serialId = System.currentTimeMillis();
    private final PoolV2Mode _poolMode = new PoolV2Mode();
    private boolean _reportOnRemovals = false;
    private boolean _suppressHsmLoad = false;
    private boolean _cleanPreciousFiles = false;
    private String     _poolStatusMessage = "OK";
    private int        _poolStatusCode  = 0;

    private PnfsHandler _pnfs;
    private StorageClassContainer _storageQueue;
    private CacheRepositoryV5 _repository;

    private Account _account;

    private String _poolManagerName = "PoolManager";
    private String _poolupDestination = "PoolManager";

    private int _version = 4;
    private CellPath _billingCell = new CellPath("billing");
    private final Map<String, String> _tags = new HashMap<String, String>();
    private String _baseDir;

    private final PoolManagerPingThread _pingThread ;
    private HsmFlushController _flushingThread;
    private IoQueueManager _ioQueue ;
    private JobTimeoutManager _timeoutManager;
    private HsmSet _hsmSet;
    private HsmStorageHandler2 _storageHandler;
    private boolean _crashEnabled = false;
    private String _crashType = "exception";
    private long _gap = 4L * 1024L * 1024L * 1024L;
    private int _p2pFileMode = P2P_CACHED;
    private int _dupRequest = DUP_REQ_IGNORE;
    private P2PClient _p2pClient = null;

    private boolean _isVolatile = false;
    private boolean _hasTapeBackend = true;

    private int _cleaningInterval = 60;

    private Object _hybridInventoryLock = new Object();
    private boolean _hybridInventoryActive = false;
    private int _hybridCurrent = 0;

    private ChecksumModuleV1 _checksumModule;
    private ReplicationHandler _replicationHandler = new ReplicationHandler();

    private ReplicaStatePolicy _replicaStatePolicy;

    private boolean _running = false;
    private double _breakEven = 250.0;

    public PoolV4(String poolName, String args)
    {
        _poolName = poolName;
        _args = new Args(args);

        _log.info("Pool " + poolName + " starting");

        //
        // repository and ping thread must exist BEFORE the setup
        // file is scanned. PingThread will be started after all
        // the setup is done.
        //
        _pingThread = new PoolManagerPingThread();

        //
        // get additional tags
        //
        for (Map.Entry<String,String> option: _args.options().entrySet()) {
            String key = option.getKey();
            if ((key.length() > 4) && key.startsWith("tag.")) {
                _tags.put(key.substring(4), option.getValue());
            }
        }

        for (Map.Entry<String, String> e: _tags.entrySet() ) {
            _log.info("Tag: " + e.getKey() + "="+ e.getValue());
        }
    }

    protected void assertNotRunning(String error)
    {
        if (_running)
            throw new IllegalStateException(error);
    }

    public void setBaseDir(String baseDir)
    {
        assertNotRunning("Cannot change base dir after initialisation");
        _baseDir = baseDir;
    }

    public void setVersion(int version)
    {
        _version = version;
    }

    public void setReplicateOnArrival(String replicate)
    {
        _replicationHandler.init(replicate.equals("") ? "on" : replicate);
    }

    public void setAllowCleaningPreciousFiles(boolean allow)
    {
        _cleanPreciousFiles = allow;
    }

    public void setVolatile(boolean isVolatile)
    {
        _isVolatile = isVolatile;
    }

    public boolean isVolatile()
    {
        return _isVolatile;
    }

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

    public void setPoolManagerName(String name)
    {
        _poolManagerName = name;
    }

    public void setPoolUpDestination(String name)
    {
        _poolupDestination = name;
    }

    public void setBillingCellName(String name)
    {
        _billingCell = new CellPath(name);
    }

    public void setPnfsHandler(PnfsHandler pnfs)
    {
        _pnfs = pnfs;
    }

    public void setRepository(CacheRepositoryV5 repository)
    {
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

    public void setAccount(Account account)
    {
        _account = account;
    }

    public void setChecksumModule(ChecksumModuleV1 module)
    {
        assertNotRunning("Cannot set checksum module after initialization");
        _checksumModule = module;
    }

    public void setStorageQueue(StorageClassContainer queue)
    {
        assertNotRunning("Cannot set storage queue after initialization");
        _storageQueue = queue;
    }

    public void setStorageHandler(HsmStorageHandler2 handler)
    {
        _storageHandler = handler;
    }

    public void setHSMSet(HsmSet set)
    {
        assertNotRunning("Cannot set HSM set after initialization");
        _hsmSet = set;
    }

    public void setTimeoutManager(JobTimeoutManager manager)
    {
        assertNotRunning("Cannot set timeout manager after initialization");
        _timeoutManager = manager;
        _timeoutManager.start();
    }

    public void setFlushController(HsmFlushController controller)
    {
        assertNotRunning("Cannot set flushing controller after initialization");
        _flushingThread = controller;
    }

    public void setPPClient(P2PClient client)
    {
        assertNotRunning("Cannot set P2P client after initialization");
        _p2pClient = client;
    }

    public void setReplicaStatePolicy(ReplicaStatePolicy replicaStatePolicy)
    {
        _replicaStatePolicy = replicaStatePolicy;
    }

    public void setTags(String tags)
    {
        Map<String,String> newTags = new HashMap<String,String>();
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

    /**
     * Initialize remaining pieces.
     *
     * We cannot do these things in the constructor as they rely on
     * various properties being set first.
     */
    public void init()
    {
        assert _baseDir != null : "Base directory must be set";
        assert _pnfs != null : "PNFS handler must be set";
        assert _repository != null : "Repository must be set";
        assert _checksumModule != null : "Checksum module must be set";
        assert _storageQueue != null : "Storage queue must be set";
        assert _storageHandler != null : "Storage handler must be set";
        assert _hsmSet != null : "HSM set must be set";
        assert _timeoutManager != null : "Timeout manager must be set";
        assert _flushingThread != null : "Flush controller must be set";
        assert _p2pClient != null : "P2P client must be set";
        assert _account != null : "Account must be set";

        if (_isVolatile && _hasTapeBackend) {
            throw new IllegalStateException("Volatile pool cannot have a tape backend");
        }

        String ioQueues = _args.getOpt("io-queues");
        String queues[];
        if(ioQueues != null || !ioQueues.isEmpty()) {
            queues = ioQueues.split(",");
        }else{
            queues = new String[0];
        }
        queues = Arrays.copyOf(queues, queues.length +1);
        queues[queues.length -1] = P2P_QUEUE_NAME;

        _ioQueue = new IoQueueManager(_timeoutManager, queues);

        disablePool(PoolV2Mode.DISABLED_STRICT, 1, "Initializing");
        _pingThread.start();
    }

    @Override
    public void afterStart()
    {
        assertNotRunning("Cannot initialize several times");

        _running = true;

        _log.info("Running repository");
        try {
            _repository.init();
            enablePool();
            _flushingThread.start();
        } catch (Throwable e) {
            _log.error("Repository reported a problem : " + e.getMessage());
            _log.warn("Pool not enabled " + _poolName);
            disablePool(PoolV2Mode.DISABLED_DEAD | PoolV2Mode.DISABLED_STRICT,
                        666, "Init failed: " + e.getMessage());
        }
        _log.info("Repository finished");
    }

    @Override
    public void beforeStop()
    {
        disablePool(PoolV2Mode.DISABLED_DEAD | PoolV2Mode.DISABLED_STRICT,
                    666, "Shutdown");
    }

    public void cleanUp()
    {
        _ioQueue.shutdown();
    }

    /**
     * Called by subsystems upon serious faults.
     */
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

    public CellVersion getCellVersion()
    {
        return new CellVersion(diskCacheV111.util.Version.getVersion(),
                               "$Revision$");
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

            if (from == to)
                return;

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
                    } catch (CacheException e) {
                        _log.error("Error adding " + id + " to flush queue: "
                                   + e.getMessage());
                    }
                }
            } else if (from == EntryState.PRECIOUS) {
                _log.debug("Removing " + id + " from flush queue");
                try {
                    if (!_storageQueue.removeCacheEntry(id))
                        _log.info("File " + id + " not found in flush queue");
                } catch (CacheException e) {
                    _log.error("Error removing " + id + " from flush queue: " + e);
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
                PnfsId id = event.getPnfsId();
                try {
                    String source = getCellName() + "@" + getCellDomainName();
                    InfoMessage msg =
                        new RemoveFileInfoMessage(source, id);
                    sendMessage(new CellMessage(_billingCell, msg));
                } catch (NoRouteToCellException e) {
                    _log.error("Failed to send message to " + _billingCell + ": "
                         + e.getMessage());
                }
            }
        }
    }

    public void printSetup(PrintWriter pw)
    {
        pw.println("set heartbeat " + _pingThread.getHeartbeat());
        pw.println("set report remove " + (_reportOnRemovals ? "on" : "off"));
        pw.println("set breakeven " + _breakEven);
        if (_suppressHsmLoad)
            pw.println("pool suppress hsmload on");
        pw.println("set gap " + _gap);
        pw.println("set duplicate request "
                   + ((_dupRequest == DUP_REQ_NONE)
                      ? "none"
                      : (_dupRequest == DUP_REQ_IGNORE)
                      ? "ignore"
                      : "refresh"));
        _flushingThread.printSetup(pw);
        _ioQueue.printSetup(pw);
    }

    public CellInfo getCellInfo(CellInfo info)
    {
        PoolCellInfo poolinfo = new PoolCellInfo(info);
        poolinfo.setPoolCostInfo(getPoolCostInfo());
        poolinfo.setTagMap(_tags);
        poolinfo.setErrorStatus(_poolStatusCode, _poolStatusMessage);
        poolinfo.setCellVersion(getCellVersion());
        return poolinfo;
    }

    public void getInfo(PrintWriter pw)
    {
        pw.println("Base directory    : " + _baseDir);
        pw.println("Revision          : [$Revision$]");
        pw.println("Version           : " + getCellVersion() + " (Sub="
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

        for (JobScheduler js : _ioQueue.getSchedulers()) {
            pw.println("Mover Queue (" + js.getSchedulerName() + ") "
                       + js.getActiveJobs() + "(" + js.getMaxActiveJobs()
                       + ")/" + js.getQueueSize());
        }
    }

    // //////////////////////////////////////////////////////////////
    //
    // The io File Part
    //
    //

    private int queueIoRequest(PoolIoFileMessage message,
                               PoolIORequest request)
        throws InvocationTargetException
    {
        String queueName = message.getIoQueueName();

        if (message instanceof PoolAcceptFileMessage) {
            return _ioQueue.add(queueName, request, SimpleJobScheduler.HIGH);
        } else if (message.isPool2Pool()) {
            return _ioQueue.add(P2P_QUEUE_NAME, request, SimpleJobScheduler.HIGH);
        } else {
            return _ioQueue.add(queueName, request, SimpleJobScheduler.REGULAR);
        }
    }

    private void ioFile(CellMessage envelope, PoolIoFileMessage message)
    {
        PnfsId pnfsId = message.getPnfsId();
        try {
            long id = message.getId();
            ProtocolInfo pi = message.getProtocolInfo();
            StorageInfo si = message.getStorageInfo();
            String initiator = message.getInitiator();
            String pool = message.getPoolName();
            String queueName = message.getIoQueueName();
            CellPath source = (CellPath)envelope.getSourcePath().clone();
            String door =
                source.getCellName() + "@" + source.getCellDomainName();

            /* Eliminate duplicate requests.
             */
            if (!(message instanceof PoolAcceptFileMessage)
                && !message.isPool2Pool()) {

                JobInfo job = _ioQueue.findJob(door, id);
                if (job != null) {
                    switch (_dupRequest) {
                    case DUP_REQ_NONE:
                        _log.info("Dup Request : none <" + door + ":" + id + ">");
                        break;
                    case DUP_REQ_IGNORE:
                        _log.info("Dup Request : ignoring <" + door + ":" + id + ">");
                        return;
                    case DUP_REQ_REFRESH:
                        long jobId = job.getJobId();
                        _log.info("Dup Request : refresing <" + door + ":"
                            + id + "> old = " + jobId);
                        _ioQueue.kill((int)jobId, true);
                        break;
                    default:
                        throw new RuntimeException("Dup Request : PANIC (code corrupted) <"
                                                   + door + ":" + id + ">");
                    }
                }
            }

            /* Queue new request.
             */
            MoverProtocol mover = getProtocolHandler(pi);
            if (mover == null)
                throw new CacheException(27,
                                         "PANIC : Could not get handler for " +
                                         pi);

            PoolIOTransfer transfer;
            if (message instanceof PoolAcceptFileMessage) {
                List<StickyRecord> stickyRecords =
                    _replicaStatePolicy.getStickyRecords(si);
                EntryState targetState =
                    _replicaStatePolicy.getTargetState(si);
                transfer =
                    new PoolIOWriteTransfer(pnfsId, pi, si, mover, _repository,
                                            _checksumModule,
                                            targetState, stickyRecords);
            } else {
                transfer =
                    new PoolIOReadTransfer(pnfsId, pi, si, mover, _repository);
            }
            try {
                source.revert();
                PoolIORequest request =
                    new PoolIORequest(transfer, id, initiator,
                                      source, pool, queueName, getCellEndpoint(), _billingCell,  this);
                message.setMoverId(queueIoRequest(message, request));
                transfer = null;
            } catch (InvocationTargetException e) {
                throw e.getTargetException();
            } finally {
                if (transfer != null) {
                    /* This is only executed if enqueuing the request
                     * failed. Therefore we only log failures and
                     * propagate the original error to the client.
                     */
                    try {
                        transfer.close();
                    } catch (NoRouteToCellException e) {
                        _log.error("Communication failure while closing entry: "
                                   + e.getMessage());
                    } catch (IOException e) {
                        _log.error("IO error while closing entry: "
                                   + e.getMessage());
                    } catch (InterruptedException e) {
                        _log.error("Interrupted while closing entry: "
                                   + e.getMessage());
                    }
                }
            }
            message.setSucceeded();
        } catch (FileInCacheException e) {
            _log.warn("Pool already contains replica");
            message.setFailed(e.getRc(), "Pool already contains " + pnfsId);
        } catch (FileNotInCacheException e) {
            _log.warn("Pool does not contain replica");
            message.setFailed(e.getRc(), "Pool does not contain " + pnfsId);
        } catch (CacheException e) {
            _log.error(e.getMessage());
            message.setFailed(e.getRc(), e.getMessage());
        } catch (Throwable e) {
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
        private boolean _enabled = false;
        private CellPath _replicationManager = new CellPath("PoolManager");
        private String _destinationHostName = null;
        private String _destinationMode = "keep";
        private boolean _replicateOnRestore = false;

        //
        // replicationManager,Hostname,modeOfDestFile
        //
        private ReplicationHandler()
        {
        }

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

        public void init(String vars)
        {
            if (_destinationHostName == null) {
                try {
                    _destinationHostName = InetAddress.getLocalHost()
                        .getHostAddress();
                } catch (UnknownHostException ee) {
                    _destinationHostName = "localhost";
                }
            }
            if ((vars == null) || vars.equals("off")) {
                _enabled = false;
                return;
            } else if (vars.equals("on")) {
                _enabled = true;
                return;
            }
            _enabled = true;

            String[] args = vars.split(",");
            if (args.length > 0 && !args[0].equals("")) {
                _replicationManager = new CellPath(args[0]);
            }
            _destinationHostName = ((args.length > 1) && !args[1].equals("")) ? args[1]
                : _destinationHostName;
            _destinationMode = ((args.length > 2) && !args[2].equals("")) ? args[2]
                : _destinationMode;

            if (_destinationHostName.equals("*")) {
                try {
                    _destinationHostName = InetAddress.getLocalHost()
                        .getHostAddress();
                } catch (UnknownHostException ee) {
                    _destinationHostName = "localhost";
                }
            }
        }

        @Override
        public String toString()
        {
            StringBuilder sb = new StringBuilder();

            if (_enabled) {
                sb.append("{Mgr=").append(_replicationManager).append(",Host=")
                    .append(_destinationHostName).append(",DestMode=")
                    .append(_destinationMode).append("}");
            } else {
                sb.append("Disabled");
            }
            return sb.toString();
        }

        private void initiateReplication(PnfsId id, String source)
        {
            if ((!_enabled)
                || (source.equals("restore") && !_replicateOnRestore))
                return;
            try {
                _initiateReplication(_repository.getEntry(id), source);
            } catch (CacheException e) {
                _log.error("Problem in sending replication request: " + e);
            } catch (NoRouteToCellException e) {
                _log.error("Problem in sending replication request: " + e.getMessage());
            }
        }

        private void _initiateReplication(CacheEntry entry, String source)
            throws CacheException, NoRouteToCellException
        {
            PnfsId pnfsId = entry.getPnfsId();
            StorageInfo storageInfo = entry.getStorageInfo().clone();

            storageInfo.setKey("replication.source", source);

            PoolMgrReplicateFileMsg req =
                new PoolMgrReplicateFileMsg(pnfsId,
                                            storageInfo,
                                            new DCapProtocolInfo("DCap", 3, 0,
                                                                 _destinationHostName, 2222),
                                            storageInfo.getFileSize());
            req.setReplyRequired(false);
            sendMessage(new CellMessage(_replicationManager, req));
        }
    }

    // ///////////////////////////////////////////////////////////
    //
    // The mover class loader
    //
    //
    private Map<String, Class<?>> _handlerClasses =
        new Hashtable<String, Class<?>>();

    private MoverProtocol getProtocolHandler(ProtocolInfo info)
    {
        Class<?>[] argsClass = { dmg.cells.nucleus.CellEndpoint.class };
        String moverClassName = info.getProtocol() + "-"
            + info.getMajorVersion();
        Class<?> mover = _moverHash.get(moverClassName);

        try {
            if (mover == null) {
                moverClassName = "org.dcache.pool.movers." + info.getProtocol()
                    + "Protocol_" + info.getMajorVersion();

                mover = _handlerClasses.get(moverClassName);

                if (mover == null) {
                    mover = Class.forName(moverClassName);
                    _handlerClasses.put(moverClassName, mover);
                }

            }
            Constructor<?> moverCon = mover.getConstructor(argsClass);
            Object[] args = { getCellEndpoint() };
            return (MoverProtocol) moverCon.newInstance(args);
        } catch (Exception e) {
            _log.error("Could not create mover for " + moverClassName, e);
            return null;
        }
    }


    // //////////////////////////////////////////////////////////////////////////
    //
    // interface to the HsmRestoreHandler
    //
    private class ReplyToPoolFetch
        extends DelayedReply
        implements CacheFileAvailable
    {
        private final PoolFetchFileMessage _message;

        private ReplyToPoolFetch(PoolFetchFileMessage message)
        {
            _message = message;
        }

        public void cacheFileAvailable(PnfsId pnfsId, Throwable ee)
        {
            try {
                if (ee == null) {
                    _message.setSucceeded();
                } else if (ee instanceof CacheException) {
                    CacheException ce = (CacheException) ee;
                    int errorCode = ce.getRc();
                    _message.setFailed(errorCode, ce.getMessage());

                    switch (errorCode) {
                    case 41:
                    case 42:
                    case 43:
                        disablePool(PoolV2Mode.DISABLED_STRICT, errorCode,
                                    ce.getMessage());
                    }
                } else {
                    _message.setFailed(1000, ee);
                }
            } finally {
                if (_message.getReturnCode() != 0) {
                    _log.error("Fetch failed: " +
                               _message.getErrorObject());

                    /* Something went wrong. We delete the file to be
                     * on the safe side (better waste tape bandwidth
                     * than risk leaving a broken file).
                     */
                    try {
                        _repository.setState(pnfsId, EntryState.REMOVED);
                    } catch (IllegalTransitionException e) {
                        /* Most likely indicates that the file was
                         * removed before we could do it. Log the
                         * problem, but otherwise ignore it.
                         */
                        _log.warn("Failed to remove replica: " + e.getMessage());
                    }
                }

                try {
                    send(_message);
                } catch (NoRouteToCellException e) {
                    _log.error("Failed to send reply: " + e.getMessage());
                } catch (InterruptedException e) {
                    _log.error("Failed to send reply: " + e.getMessage());
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    private void checkFile(PoolFileCheckable poolMessage)
        throws CacheException
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

                try {
                    send(_message);
                } catch (NoRouteToCellException e) {
                    _log.error("Cannot send P2P reply: " + e.getMessage());
                } catch (InterruptedException e) {
                    _log.error("Cannot send P2P reply: " + e.getMessage());
                    Thread.currentThread().interrupt();
                }
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
            JobScheduler js = _ioQueue.getQueueByJobId(id);
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

            _log.warn("PoolIoFileMessage request rejected due to "
                      + _poolMode);
            throw new CacheException(104, "Pool is disabled");
        }

        msg.setReply();
        ioFile(envelope, msg);
    }

    public DelayedReply messageArrived(Pool2PoolTransferMsg msg)
        throws CacheException, IOException
    {
        if (_poolMode.isDisabled(PoolV2Mode.DISABLED_P2P_CLIENT)) {
            _log.warn("Pool2PoolTransferMsg request rejected due to "
                       + _poolMode);
            throw new CacheException(104, "Pool is disabled");
        }

        String poolName = msg.getPoolName();
        PnfsId pnfsId = msg.getPnfsId();
        StorageInfo storageInfo = msg.getStorageInfo();
        CompanionFileAvailableCallback callback =
            new CompanionFileAvailableCallback(msg);

        EntryState targetState = EntryState.CACHED;
        int fileMode = msg.getDestinationFileStatus();
        if (fileMode != Pool2PoolTransferMsg.UNDETERMINED) {
            if (fileMode == Pool2PoolTransferMsg.PRECIOUS)
                targetState = EntryState.PRECIOUS;
        } else if (!_hasTapeBackend && !_isVolatile
                   && (_p2pFileMode == P2P_PRECIOUS)) {
            targetState = EntryState.PRECIOUS;
        }

        List<StickyRecord> stickyRecords = Collections.emptyList();
        _p2pClient.newCompanion(pnfsId, poolName, storageInfo,
                                targetState, stickyRecords, callback);
        return callback;
    }

    public Object messageArrived(PoolFetchFileMessage msg)
        throws CacheException
    {
        if (_poolMode.isDisabled(PoolV2Mode.DISABLED_STAGE)) {
            _log.warn("PoolFetchFileMessage request rejected due to "
                       + _poolMode);
            throw new CacheException(104, "Pool is disabled");
        }
        if (!_hasTapeBackend) {
            _log.warn("PoolFetchFileMessage request rejected due to LFS mode");
            throw new CacheException(104, "Pool has no tape backend");
        }

        PnfsId pnfsId = msg.getPnfsId();
        StorageInfo storageInfo = msg.getStorageInfo();
        _log.info("Pool " + _poolName + " asked to fetch file "
                  + pnfsId + " (hsm=" + storageInfo.getHsm() + ")");

        try {
            ReplyToPoolFetch reply = new ReplyToPoolFetch(msg);
            _storageHandler.fetch(pnfsId, storageInfo, reply);
            return reply;
        } catch (FileInCacheException e) {
            _log.warn("Pool already contains replica");
            msg.setSucceeded();
            return msg;
        } catch (CacheException e) {
            _log.error(e.toString());
            if (e.getRc() == CacheException.ERROR_IO_DISK)
                disablePool(PoolV2Mode.DISABLED_STRICT,
                            e.getRc(), e.getMessage());
            throw e;
        }
    }

    public void messageArrived(CellMessage envelope,
                               PoolRemoveFilesFromHSMMessage msg)
        throws CacheException
    {
        if (_poolMode.isDisabled(PoolV2Mode.DISABLED_STAGE)) {
            _log.warn("PoolRemoveFilesFromHsmMessage request rejected due to "
                      + _poolMode);
            throw new CacheException(104, "Pool is disabled");
        }
        if (!_hasTapeBackend) {
            _log.warn("PoolRemoveFilesFromHsmMessage request rejected due to LFS mode");
            throw new CacheException(104, "Pool has no tape backend");
        }

        _storageHandler.remove(envelope);
    }

    public PoolCheckFreeSpaceMessage
        messageArrived(PoolCheckFreeSpaceMessage msg)
        throws CacheException
    {
        msg.setFreeSpace(_account.getFree());
        msg.setSucceeded();
        return msg;
    }

    public Message messageArrived(Message msg)
        throws CacheException
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
        throws CacheException
    {
        if (_poolMode.isDisabled(PoolV2Mode.DISABLED)) {
            _log.warn("PoolRemoveFilesMessage request rejected due to "
                      + _poolMode);
            throw new CacheException(104, "Pool is disabled");
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
            for (int i = 0, j = 0; i < fileList.length; i++)
                if (fileList[i] != null)
                    replyList[j++] = fileList[i];
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
                if (msg.isCached())
                    _repository.setState(pnfsId, EntryState.CACHED);
                msg.setSucceeded();
                break;

            case CACHED:
                if (msg.isPrecious())
                    _repository.setState(pnfsId, EntryState.PRECIOUS);
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
        throws CacheException
    {
        try {
            _repository.setSticky(msg.getPnfsId(),
                                  msg.getOwner(),
                                  msg.isSticky()
                                  ? msg.getLifeTime()
                                  : 0,
                                  true);
            msg.setSucceeded();
        } catch (FileNotInCacheException e) {
            msg.setFailed(e.getRc(), e);
        }
        return msg;
    }

    public PoolQueryRepositoryMsg messageArrived(PoolQueryRepositoryMsg msg)
    {
        msg.setReply(new RepositoryCookie(), getRepositoryListing());
        return msg;
    }

    private List<CacheRepositoryEntryInfo> getRepositoryListing()
    {
        List<CacheRepositoryEntryInfo> listing = new ArrayList();
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
            (errorString == null) ? "Requested By Operator" : errorString;
        _poolMode.setMode(mode);

        _pingThread.sendPoolManagerMessage(true);
        _log.warn("Pool mode changed to " + _poolMode);
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

        private void start()
        {
            _worker.start();
        }

        public void run()
        {
            _log.debug("Ping thread started");
            try {
                while (!Thread.interrupted()) {
                    sendPoolManagerMessage(true);
                    Thread.sleep(_heartbeat * 1000);
                }
            } catch (InterruptedException e) {
                _log.debug("Ping thread was interrupted");
            }

            _log.info("Ping thread sending pool down message");
            disablePool(PoolV2Mode.DISABLED_DEAD | PoolV2Mode.DISABLED_STRICT,
                        666, "PingThread terminated");
            _log.debug("Ping Thread finished");
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
            if (forceSend || _storageQueue.poolStatusChanged())
                send(getPoolManagerMessage());
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
            if (_hsmSet != null)
                poolManagerMessage.setHsmInstances(new TreeSet<String>(_hsmSet.getHsmInstances()));
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

        info.setQueueSizes(_ioQueue.getActiveJobs(), _ioQueue
                           .getMaxActiveJobs(), _ioQueue.getQueueSize(), _storageHandler
                            .getFetchScheduler().getActiveJobs(), _suppressHsmLoad ? 0
                            : _storageHandler.getFetchScheduler().getMaxActiveJobs(),
                            _storageHandler.getFetchScheduler().getQueueSize(),
                            _storageHandler.getStoreScheduler().getActiveJobs(),
                            _suppressHsmLoad ? 0 : _storageHandler.getStoreScheduler()
                            .getMaxActiveJobs(), _storageHandler
                            .getStoreScheduler().getQueueSize()

                           );

        for (JobScheduler js : _ioQueue.getSchedulers()) {
            info.addExtendedMoverQueueSizes(js.getSchedulerName(),
                    js.getActiveJobs(), js.getMaxActiveJobs(), js.getQueueSize());
        }

        info.setP2pClientQueueSizes(_p2pClient.getActiveJobs(), _p2pClient
                                    .getMaxActiveJobs(), _p2pClient.getQueueSize());

        JobScheduler p2pQueue = _ioQueue.getQueue(P2P_QUEUE_NAME);
        info.setP2pServerQueueSizes(p2pQueue.getActiveJobs(),
                p2pQueue.getMaxActiveJobs(), p2pQueue.getQueueSize());

        return info;
    }

    public String hh_set_breakeven = "<breakEven> # free and recovable space";

    public String ac_set_breakeven_$_0_1(Args args)
    {
        if (args.argc() > 0)
            _breakEven = Double.parseDouble(args.argv(0));
        return "BreakEven = " + _breakEven;
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
            } catch (CacheException e) {
                if (e.getRc() == CacheException.FILE_NOT_FOUND) {
                    try {
                        _repository.setState(id, EntryState.REMOVED);
                        _log.info("File not found in PNFS; removed " + id);
                    } catch (IllegalTransitionException f) {
                        _log.error("File not found in PNFS, but failed to remove "
                                   + id + ": " + f);
                    }
                } else {
                    _log.error("Cache location was not registered for "
                               + id + ": " + e.getMessage());
                }
            }
        }

        private void clearCacheLocation(PnfsId id)
        {
            _pnfs.clearCacheLocation(id);
        }

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
                if (Thread.interrupted())
                    break;
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

    public String hh_pnfs_register = " # add entry of all files into pnfs";
    public String hh_pnfs_unregister = " # remove entry of all files from pnfs";

    public String ac_pnfs_register(Args args)
    {
        synchronized (_hybridInventoryLock) {
            if (_hybridInventoryActive)
                throw new IllegalArgumentException(
                                                   "Hybrid inventory still active");
            _hybridInventoryActive = true;
            new HybridInventory(true);
        }
        return "";
    }

    public String ac_pnfs_unregister(Args args)
    {
        synchronized (_hybridInventoryLock) {
            if (_hybridInventoryActive)
                throw new IllegalArgumentException(
                                                   "Hybrid inventory still active");
            _hybridInventoryActive = true;
            new HybridInventory(false);
        }
        return "";
    }

    public String hh_run_hybrid_inventory = " [-destroy]";

    public String ac_run_hybrid_inventory(Args args)
    {
        synchronized (_hybridInventoryLock) {
            if (_hybridInventoryActive)
                throw new IllegalArgumentException(
                                                   "Hybrid inventory still active");
            _hybridInventoryActive = true;
            new HybridInventory(args.getOpt("destroy") == null);
        }
        return "";
    }

    public String hh_pf = "<pnfsId>";

    public String ac_pf_$_1(Args args) throws Exception
    {
        PnfsId pnfsId = new PnfsId(args.argv(0));
        PnfsMapPathMessage info = new PnfsMapPathMessage(pnfsId);
        CellPath path = new CellPath("PnfsManager");
        CellMessage m = sendAndWait(new CellMessage(path, info), 10000);
        if (m == null)
            throw new CacheException("No reply from PnfsManager");

        info = ((PnfsMapPathMessage) m.getMessageObject());
        if (info.getReturnCode() != 0) {
            Object o = info.getErrorObject();
            if (o instanceof Exception)
                throw (Exception) o;
            else
                throw new CacheException(o.toString());
        }
        return info.getGlobalPath();
    }

    public String hh_set_replication = "off|on|<mgr>,<host>,<destMode>";
    public String ac_set_replication_$_1(Args args)
    {
        setReplicateOnArrival(args.argv(0));
        return _replicationHandler.toString();
    }

    public String hh_pool_suppress_hsmload = "on|off";
    public String ac_pool_suppress_hsmload_$_1(Args args)
    {
        String mode = args.argv(0);
        if (mode.equals("on")) {
            _suppressHsmLoad = true;
        } else if (mode.equals("off")) {
            _suppressHsmLoad = false;
        } else
            throw new IllegalArgumentException("Illegal syntax : pool suppress hsmload on|off");

        return "hsm load suppression swithed : "
            + (_suppressHsmLoad ? "on" : "off");
    }

    public String hh_movermap_define = "<protocol>-<major> <moverClassName>";
    public String ac_movermap_define_$_2(Args args) throws Exception
    {
        _moverHash.put(args.argv(0), Class.forName(args.argv(1)));
        return "";
    }

    public String hh_movermap_undefine = "<protocol>-<major>";
    public String ac_movermap_undefine_$_1(Args args)
    {
        _moverHash.remove(args.argv(0));
        return "";
    }

    public String hh_movermap_ls = "";
    public String ac_movermap_ls(Args args)
    {
        StringBuilder sb = new StringBuilder();

        for (Map.Entry<String, Class<?>> entry: _moverHash.entrySet()) {
            sb.append(entry.getKey()).append(" -> ").append(entry.getValue().getName()).append("\n");
        }
        return sb.toString();
    }

    public String hh_set_duplicate_request = "none|ignore|refresh";
    public String ac_set_duplicate_request_$_1(Args args)
        throws CommandSyntaxException
    {
        String mode = args.argv(0);
        if (mode.equals("none")) {
            _dupRequest = DUP_REQ_NONE;
        } else if (mode.equals("ignore")) {
            _dupRequest = DUP_REQ_IGNORE;
        } else if (mode.equals("refresh")) {
            _dupRequest = DUP_REQ_REFRESH;
        } else {
            throw new CommandSyntaxException("Not Found : ",
                                             "Usage : pool duplicate request none|ignore|refresh");
        }
        return "";
    }

    public String hh_set_p2p = "integrated|separated; OBSOLETE";
    public String ac_set_p2p_$_1(Args args) throws CommandSyntaxException
    {
        return "WARNING: this command is obsolete";
    }

    public String fh_pool_disable = "   pool disable [options] [ <errorCode> [<errorMessage>]]\n"
        + "      OPTIONS :\n"
        + "        -fetch    #  disallows fetch (transfer to client)\n"
        + "        -stage    #  disallows staging (from HSM)\n"
        + "        -store    #  disallows store (transfer from client)\n"
        + "        -p2p-client\n"
        + "        -rdonly   #  := store,stage,p2p-client\n"
        + "        -strict   #  := disallows everything\n";
    public String hh_pool_disable = "[options] [<errorCode> [<errorMessage>]] # suspend sending 'up messages'";
    public String ac_pool_disable_$_0_2(Args args)
    {
        if (_poolMode.isDisabled(PoolV2Mode.DISABLED_DEAD))
            return "The pool is dead and a restart is required to enable it";

        int rc = (args.argc() > 0) ? Integer.parseInt(args.argv(0)) : 1;
        String rm = (args.argc() > 1) ? args.argv(1) : "Operator intervention";

        int modeBits = PoolV2Mode.DISABLED;
        if (args.getOpt("strict") != null)
            modeBits |= PoolV2Mode.DISABLED_STRICT;
        if (args.getOpt("stage") != null)
            modeBits |= PoolV2Mode.DISABLED_STAGE;
        if (args.getOpt("fetch") != null)
            modeBits |= PoolV2Mode.DISABLED_FETCH;
        if (args.getOpt("store") != null)
            modeBits |= PoolV2Mode.DISABLED_STORE;
        if (args.getOpt("p2p-client") != null)
            modeBits |= PoolV2Mode.DISABLED_P2P_CLIENT;
        if (args.getOpt("p2p-server") != null)
            modeBits |= PoolV2Mode.DISABLED_P2P_SERVER;
        if (args.getOpt("rdonly") != null)
            modeBits |= PoolV2Mode.DISABLED_RDONLY;

        disablePool(modeBits, rc, rm);

        return "Pool " + _poolName + " " + _poolMode;
    }

    public String hh_pool_enable = " # resume sending up messages'";
    public String ac_pool_enable(Args args)
    {
        if (_poolMode.isDisabled(PoolV2Mode.DISABLED_DEAD))
            return "The pool is dead and a restart is required to enable it";
        enablePool();
        return "Pool " + _poolName + " enabled";
    }

    public String hh_set_max_movers = "!!! Please use 'mover|st|rh set max active <jobs>'";
    public String ac_set_max_movers_$_1(Args args)
        throws IllegalArgumentException
    {
        int num = Integer.parseInt(args.argv(0));
        if ((num < 0) || (num > 10000))
            throw new IllegalArgumentException("Not in range (0...10000)");
        return "Please use 'mover|st|rh set max active <jobs>'";

    }

    public String hh_set_gap = "<always removable gap>/size[<unit>] # unit = k|m|g";
    public String ac_set_gap_$_1(Args args)
    {
        _gap = UnitInteger.parseUnitLong(args.argv(0));
        return "Gap set to " + _gap;
    }

    public String hh_set_report_remove = "on|off";
    public String ac_set_report_remove_$_1(Args args)
        throws CommandSyntaxException
    {
        String onoff = args.argv(0);
        if (onoff.equals("on"))
            _reportOnRemovals = true;
        else if (onoff.equals("off"))
            _reportOnRemovals = false;
        else
            throw new CommandSyntaxException("Invalid value : " + onoff);
        return "";
    }

    public String hh_crash = "disabled|shutdown|exception";
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
        } else
            throw new IllegalArgumentException("crash disabled|shutdown|exception");

        return "Crash is " + (_crashEnabled ? _crashType : "disabled");

    }

    public String hh_set_sticky = "# Deprecated";
    public String ac_set_sticky_$_0_1(Args args)
    {
        return "The command is deprecated and has no effect";
    }

    public String hh_set_cleaning_interval = "<interval/sec>";
    public String ac_set_cleaning_interval_$_1(Args args)
    {
        _cleaningInterval = Integer.parseInt(args.argv(0));
        _log.info("set cleaning interval to " + _cleaningInterval);
        return "";
    }

    public String hh_flush_class = "<hsm> <storageClass> [-count=<count>]";
    public String ac_flush_class_$_2(Args args)
    {
        String tmp = args.getOpt("count");
        int count = ((tmp == null) || tmp.equals("")) ? 0 : Integer
            .parseInt(tmp);
        long id = _flushingThread.flushStorageClass(args.argv(0), args.argv(1),
                                                    count);
        return "Flush Initiated (id=" + id + ")";
    }

    public String hh_flush_pnfsid = "<pnfsid> # flushs a single pnfsid";
    public String ac_flush_pnfsid_$_1(Args args)
        throws CacheException
    {
        _storageHandler.store(new PnfsId(args.argv(0)), null);
        return "Flush Initiated";
    }

    public String hh_mover_set_max_active = "<maxActiveIoMovers> -queue=<queueName>";
    public String hh_mover_queue_ls = "";
    public String hh_mover_ls = "[-binary [jobId] ]";
    public String hh_mover_remove = "<jobId>";
    public String hh_mover_kill = "<jobId> [-force]" ;
    public String hh_p2p_set_max_active = "<maxActiveIoMovers>";
    public String hh_p2p_ls = "[-binary [jobId] ]";
    public String hh_p2p_remove = "<jobId>; OBSOLETE: use: mover remove -queue=" + P2P_QUEUE_NAME;
    public String hh_p2p_kill = "<jobId> [-force]; OBSOLETE: use: mover kill -queue=" + P2P_QUEUE_NAME;

    public String ac_mover_set_max_active_$_1(Args args)
        throws NumberFormatException, IllegalArgumentException
    {
        String queueName = args.getOpt("queue");

        if (queueName == null)
            return mover_set_max_active(_ioQueue.getDefaultScheduler(), args);

        JobScheduler js = _ioQueue.getQueue(queueName);

        if (js == null)
            return "Not found : " + queueName;

        return mover_set_max_active(js, args);

    }

    public String ac_p2p_set_max_active_$_1(Args args)
        throws NumberFormatException, IllegalArgumentException
    {
        JobScheduler p2pQueue = _ioQueue.getQueue(P2P_QUEUE_NAME);
        return mover_set_max_active(p2pQueue, args);
    }

    private String mover_set_max_active(JobScheduler js, Args args)
        throws NumberFormatException, IllegalArgumentException
    {
        int active = Integer.parseInt(args.argv(0));
        if (active < 0)
            throw new IllegalArgumentException("<maxActiveMovers> must be >= 0");
        js.setMaxActiveJobs(active);

        return "Max Active Io Movers set to " + active;
    }

    public Object ac_mover_queue_ls_$_0_1(Args args)
    {
        StringBuilder sb = new StringBuilder();

        if (args.getOpt("l") != null) {
            for (JobScheduler js : _ioQueue.getSchedulers()) {
                sb.append(js.getSchedulerName())
                    .append(" ").append(js.getActiveJobs())
                    .append(" ").append(js.getMaxActiveJobs())
                    .append(" ").append(js.getQueueSize()).append("\n");
            }
        } else {
            for (JobScheduler js : _ioQueue.getSchedulers()) {
                sb.append(js.getSchedulerName()).append("\n");
            }
        }
        return sb.toString();
    }

    public Object ac_mover_ls_$_0_1(Args args) throws NoSuchElementException {
        String queueName = args.getOpt("queue");
        boolean binary = args.getOpt("binary") != null;

        if (binary && args.argc() > 0) {
            int id = Integer.parseInt(args.argv(0));
            JobScheduler js = _ioQueue.getQueueByJobId(id);
            return js.getJobInfo(id);
        }

        if (queueName == null) {
            return mover_ls(_ioQueue.getQueues(), binary);
        }

        if (queueName.length() == 0) {
            StringBuilder sb = new StringBuilder();
            for (JobScheduler js : _ioQueue.getSchedulers()) {
                sb.append("[").append(js.getSchedulerName()).append("]\n");
                sb.append(mover_ls(js, binary).toString());
            }
            return sb.toString();
        }

        JobScheduler js = _ioQueue.getQueue(queueName);
        if (js == null) {
            throw new NoSuchElementException(queueName);
        }

        return mover_ls(js, binary);

    }

    public Object ac_p2p_ls_$_0_1(Args args)
    {
        JobScheduler p2pQueue = _ioQueue.getQueue(P2P_QUEUE_NAME);
        return mover_ls(p2pQueue, args.getOpt("binary") != null);
    }

    private Object mover_ls(JobScheduler js, boolean binary) {
        return mover_ls(Arrays.asList(js), binary);
    }

    private Object mover_ls(List<JobScheduler> jobSchedulers, boolean binary) {        

        if (binary) {
            List<JobInfo> list = new ArrayList<JobInfo>();
            for (JobScheduler js : jobSchedulers) {
                list.addAll(js.getJobInfos());
            }
            return list.toArray(new IoJobInfo[0]);
        } else {
            StringBuffer sb = new StringBuffer();
            for (JobScheduler js : jobSchedulers) {
                js.printJobQueue(sb);
            }
            return sb.toString();
        }
    }

    public String ac_mover_remove_$_1(Args args)
        throws NoSuchElementException, NumberFormatException
    {
        int id = Integer.parseInt(args.argv(0));
        JobScheduler js = _ioQueue.getQueueByJobId(id);
        js.remove(id);
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
        boolean force = args.getOpt("force") != null;
        JobScheduler js = _ioQueue.getQueueByJobId(id);
        mover_kill(js, id, force);
        return "Kill initialized";
    }

    public String ac_p2p_kill_$_1(Args args)
        throws NoSuchElementException, NumberFormatException
    {
        return "OBSOLETE: use: mover kill -queue=" + P2P_QUEUE_NAME;
    }

    private void mover_kill(JobScheduler js, int id, boolean force)
        throws NoSuchElementException
    {
        if (force) {
            _log.warn("Forcefully killing mover " + id);
        } else {
            _log.info("Killing mover " + id);
        }

        js.kill(id, force);
    }

    public String hh_set_heartbeat = "<heartbeatInterval/sec>";
    public String ac_set_heartbeat_$_0_1(Args args)
        throws NumberFormatException
    {
        if (args.argc() > 0) {
            _pingThread.setHeartbeat(Integer.parseInt(args.argv(0)));
        }
        return "Heartbeat at " + (_pingThread.getHeartbeat());
    }
}
