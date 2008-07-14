// $Id: MultiProtocolPoolV3.java,v 1.16 2007-10-26 11:17:06 behrmann Exp $

package org.dcache.pool.classic;

import java.io.BufferedReader;
import java.io.EOFException;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.io.NotSerializableException;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.io.StringWriter;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.Collections;
import java.util.ArrayList;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.MissingResourceException;
import java.util.NoSuchElementException;
import java.util.StringTokenizer;
import java.util.TreeSet;
import java.util.concurrent.ThreadFactory;

import org.apache.log4j.Logger;

import org.dcache.services.AbstractCell;
import org.dcache.pool.FaultListener;
import org.dcache.pool.FaultEvent;
import org.dcache.pool.repository.v3.RepositoryException;
import org.dcache.pool.repository.v5.CacheRepositoryV5;
import org.dcache.pool.repository.v5.IllegalTransitionException;
import org.dcache.pool.repository.SpaceRecord;
import org.dcache.pool.repository.EntryState;
import org.dcache.pool.repository.WriteHandle;
import org.dcache.pool.repository.ReadHandle;
import org.dcache.pool.repository.CacheEntry;
import org.dcache.pool.repository.StateChangeListener;
import org.dcache.pool.repository.StateChangeEvent;
import diskCacheV111.pools.PoolV2Mode;
import diskCacheV111.pools.SpaceSweeper;
import diskCacheV111.pools.JobTimeoutManager;
import diskCacheV111.pools.PoolCostInfo;
import diskCacheV111.pools.PoolCellInfo;
import diskCacheV111.movers.ChecksumMover;
import diskCacheV111.movers.MoverProtocol;
import diskCacheV111.repository.CacheRepository;
import diskCacheV111.repository.CacheRepositoryEntryInfo;
import diskCacheV111.repository.RepositoryCookie;
import diskCacheV111.util.AccessLatency;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.CacheFileAvailable;
import diskCacheV111.util.Checksum;
import diskCacheV111.util.ChecksumFactory;
import diskCacheV111.util.FileInCacheException;
import diskCacheV111.util.FileNotInCacheException;
import diskCacheV111.util.HsmSet;
import diskCacheV111.util.IoBatchable;
import diskCacheV111.util.JobScheduler;
import diskCacheV111.util.PnfsHandler;
import diskCacheV111.util.PnfsId;
import diskCacheV111.util.RetentionPolicy;
import diskCacheV111.util.SimpleJobScheduler;
import diskCacheV111.util.SysTimer;
import diskCacheV111.util.UnitInteger;
import diskCacheV111.util.event.CacheEvent;
import diskCacheV111.util.event.CacheNeedSpaceEvent;
import diskCacheV111.vehicles.DCapProtocolInfo;
import diskCacheV111.vehicles.InfoMessage;
import diskCacheV111.vehicles.DoorTransferFinishedMessage;
import diskCacheV111.vehicles.IoJobInfo;
import diskCacheV111.vehicles.JobInfo;
import diskCacheV111.vehicles.Message;
import diskCacheV111.vehicles.MoverInfoMessage;
import diskCacheV111.vehicles.PnfsMapPathMessage;
import diskCacheV111.vehicles.Pool2PoolTransferMsg;
import diskCacheV111.vehicles.PoolAcceptFileMessage;
import diskCacheV111.vehicles.PoolCheckFreeSpaceMessage;
import diskCacheV111.vehicles.PoolCheckable;
import diskCacheV111.vehicles.PoolDeliverFileMessage;
import diskCacheV111.vehicles.PoolFetchFileMessage;
import diskCacheV111.vehicles.PoolFileCheckable;
import diskCacheV111.vehicles.PoolFlushControlMessage;
import diskCacheV111.vehicles.PoolIoFileMessage;
import diskCacheV111.vehicles.PoolManagerPoolUpMessage;
import diskCacheV111.vehicles.PoolMgrReplicateFileMsg;
import diskCacheV111.vehicles.PoolModifyModeMessage;
import diskCacheV111.vehicles.PoolModifyPersistencyMessage;
import diskCacheV111.vehicles.PoolMoverKillMessage;
import diskCacheV111.vehicles.PoolQueryRepositoryMsg;
import diskCacheV111.vehicles.PoolRemoveFilesFromHSMMessage;
import diskCacheV111.vehicles.PoolRemoveFilesMessage;
import diskCacheV111.vehicles.PoolReserveSpaceMessage;
import diskCacheV111.vehicles.PoolSetStickyMessage;
import diskCacheV111.vehicles.PoolUpdateCacheStatisticsMessage;
import diskCacheV111.vehicles.ProtocolInfo;
import diskCacheV111.vehicles.RemoveFileInfoMessage;
import diskCacheV111.vehicles.StorageInfo;
import dmg.cells.nucleus.CellAdapter;
import dmg.cells.nucleus.CellInfo;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellNucleus;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.CellVersion;
import dmg.cells.nucleus.NoRouteToCellException;
import dmg.cells.services.SetupInfoMessage;
import dmg.util.Args;
import dmg.util.CommandException;
import dmg.util.CommandSyntaxException;
import dmg.util.Logable;

public class PoolV4 extends AbstractCell
    implements Logable,
               FaultListener
{
    private static final String MAX_SPACE = "use-max-space";
    private static final String PREALLOCATED_SPACE = "use-preallocated-space";

    private final static int LFS_NONE = 0;
    private final static int LFS_PRECIOUS = 1;
    private final static int LFS_VOLATILE = 2;

    private final static int DUP_REQ_NONE = 0;
    private final static int DUP_REQ_IGNORE = 1;
    private final static int DUP_REQ_REFRESH = 2;

    private final static int P2P_INTEGRATED = 0;
    private final static int P2P_SEPARATED = 1;

    private final static int P2P_CACHED = 1;
    private final static int P2P_PRECIOUS = 2;

    private final String _poolName;
    private final Args _args;
    private final CellNucleus _nucleus;

    private final Map<?,?> _moverAttributes = new HashMap();
    private final Map<String, Class<?>> _moverHash = new HashMap<String, Class<?>>();
    /**
     * pool start time identifier.
     * used by PoolManager to recognize pool restarts
     */
    private final long _serialId = System.currentTimeMillis();
    private int _recoveryFlags = 0;
    private final PoolV2Mode _poolMode = new PoolV2Mode();
    private boolean _reportOnRemovals = false;
    private boolean _suppressHsmLoad = false;
    private boolean _cleanPreciousFiles = false;
    private String     _poolStatusMessage = "OK";
    private int        _poolStatusCode  = 0;

    private PnfsHandler _pnfs;
    private StorageClassContainer _storageQueue;
    private CacheRepositoryV5 _repository;

    private String _setupManager;
    private String _pnfsManagerName = "PnfsManager";
    private String _poolManagerName = "PoolManager";
    private String _poolupDestination = "PoolManager";

    private int _version = 4;
    private CellPath _billingCell = new CellPath("billing");
    private final Map<String, String> _tags = new HashMap<String, String>();
    private String _baseDir;
    private File _base;
    private File _setup;

    private final PoolManagerPingThread _pingThread ;
    private HsmFlushController _flushingThread;
    private JobScheduler _ioQueue ;
    private JobScheduler _p2pQueue;
    private JobTimeoutManager _timeoutManager;
    private HsmSet _hsmSet;
    private HsmStorageHandler2 _storageHandler;
    private Logable _logClass = this;
    private boolean _crashEnabled = false;
    private String _crashType = "exception";
    private boolean _allowSticky = false;
    private long _gap = 4L * 1024L * 1024L * 1024L;
    private int _lfsMode = LFS_NONE;
    private int _p2pFileMode = P2P_CACHED;
    private int _dupRequest = DUP_REQ_IGNORE;
    private int _p2pMode = P2P_SEPARATED;
    private P2PClient _p2pClient = null;

    private int _cleaningInterval = 60;

    private double _simCpuCost = -1.;
    private double _simSpaceCost = -1.;

    private Object _hybridInventoryLock = new Object();
    private boolean _hybridInventoryActive = false;
    private int _hybridCurrent = 0;

    private ChecksumModuleV1 _checksumModule;
    private ReplicationHandler _replicationHandler = new ReplicationHandler();

    private boolean _running = false;
    private double _breakEven = 250.0;

    //
    // arguments :
    // MPP2 <poolBasePath> no default
    // [-version=<version>] : default : 4
    // [-sticky=allowed|denied] ; default : denied
    // [-recover-space[=no]] : default : no
    // [-recover-control[=no]] : default : no
    // [-lfs=none|precious|volatile] : default : none
    // [-p2p=<p2pFileMode>] : default : cached
    // [-poolManager=<name>] : default : PoolManager
    // [-poolupDestination=<name>] : default : PoolManager
    // [-billing=<name>] : default : billing
    // [-setupManager=<name>] : default : none
    // [-dupRequest=none|ignore|refresh]: default : ignore
    // [-allowCleaningPreciousFiles] : default : false
    // [-checkRepository] : default : true
    // [-replicateOnArrival[=[Manager],[host],[mode]]] : default :
    // PoolManager,thisHost,keep
    //
    public PoolV4(String poolName, String args)
        throws Exception
    {
        super(poolName, args, false);

        _poolName = poolName;
        _args = getArgs();
        _nucleus = getNucleus();

        //
        // the export is convenient but not really necessary, because
        // we send our path along with the 'alive' message.
        //
        getNucleus().export();

        int argc = _args.argc();
        say("Pool " + poolName + " starting");

        try {
            //
            // repository and ping thread must exist BEFORE the setup
            // file is scanned. PingThread will be started after all
            // the setup is done.
            //
            _pingThread = new PoolManagerPingThread();

            disablePool(PoolV2Mode.DISABLED_STRICT, 1, "Initializing");

            String recover = _args.getOpt("recover-control");
            if ((recover != null) && (!recover.equals("no"))) {
                _recoveryFlags |= CacheRepository.ALLOW_CONTROL_RECOVERY;
                say("Enabled : recover-control");
            }
            recover = _args.getOpt("recover-space");
            if ((recover != null) && (!recover.equals("no"))) {
                _recoveryFlags |= CacheRepository.ALLOW_SPACE_RECOVERY;
                say("Enabled : recover-space");
            }

            recover = _args.getOpt("recover-anyway");
            if ((recover != null) && (!recover.equals("no"))) {
                _recoveryFlags |= CacheRepository.ALLOW_RECOVER_ANYWAY;
                say("Enabled : recover-anyway");
            }

            //
            // get additional tags
            //
            for (Enumeration<String> options = _args.options().keys(); options
                     .hasMoreElements();) {

                String key = options.nextElement();
                say("Tag scanning : " + key);
                if ((key.length() > 4) && key.startsWith("tag.")) {
                    _tags.put(key.substring(4), _args.getOpt(key));
                }
            }
            for (Map.Entry<String, String> e: _tags.entrySet() ) {

                say(" Extra Tag Option : " + e.getKey() + " -> "+ e.getValue());
            }
        } catch (Exception e) {
            say("Exception occurred on startup: " + e);
            start();
            kill();
            throw e;
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
        _base = new File(_baseDir);
        _setup = new File(_base, "setup");
    }

    public void setVersion(int version)
    {
        _version = version;
    }

    public void setStickyAllowed(boolean sticky)
    {
        _allowSticky = sticky;
        if (_storageHandler != null)
            _storageHandler.setStickyAllowed(sticky);
    }

    public void setStickyAllowed(String sticky)
    {
        setStickyAllowed("allowed".equals(sticky));
    }

    public void setReplicateOnArrival(String replicate)
    {
        _replicationHandler.init(replicate.equals("") ? "on" : replicate);
    }

    public void setAllowCleaningPreciousFiles(boolean allow)
    {
        _cleanPreciousFiles = allow;
    }

    public void setLFSMode(String lfs)
    {
        if (lfs == null || lfs.equals("none")) {
            _lfsMode = LFS_NONE;
        } else if (lfs.equals("precious") || lfs.equals("")){
            _lfsMode = LFS_PRECIOUS;
        } else if (lfs.equals("volatile") || lfs.equals("transient")) {
            _lfsMode = LFS_PRECIOUS;
        } else {
            throw new IllegalArgumentException("lfs=[none|precious|volatile]");
        }
        if (_repository != null)
            _repository.setVolatile(_lfsMode == LFS_VOLATILE);
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

    public void setSetupMananger(String name)
    {
        _setupManager = name;
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
        _repository.setVolatile(_lfsMode == LFS_VOLATILE);
    }

    public void setChecksumModule(ChecksumModuleV1 module)
    {
        assertNotRunning("Cannot set checksum module after initialization");
        _checksumModule = module;
        addCommandListener(_checksumModule);
    }

    public void setStorageQueue(StorageClassContainer queue)
    {
        assertNotRunning("Cannot set storage queue after initialization");
        _storageQueue = queue;
        addCommandListener(_storageQueue);
    }

    public void setStorageHandler(HsmStorageHandler2 handler)
    {
        _storageHandler = handler;
        _storageHandler.setStickyAllowed(_allowSticky);
    }

    public void setHSMSet(HsmSet set)
    {
        assertNotRunning("Cannot set HSM set after initialization");
        _hsmSet = set;
        addCommandListener(_hsmSet);
    }

    public void setTimeoutManager(JobTimeoutManager manager)
    {
        assertNotRunning("Cannot set timeout manager after initialization");
        _timeoutManager = manager;
        addCommandListener(_timeoutManager);
    }

    public void setFlushController(HsmFlushController controller)
    {
        assertNotRunning("Cannot set flushing controller after initialization");
        _flushingThread = controller;
        addCommandListener(_flushingThread);
    }

    public void setPPClient(P2PClient client)
    {
        assertNotRunning("Cannot set P2P client after initialization");
        _p2pClient = client;
        addCommandListener(_p2pClient);
    }


    public void init()
        throws InterruptedException, IOException,
               RepositoryException, CommandException
    {
        assertNotRunning("Cannot initialize several times");

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

        try {
            while (!_setup.canRead()) {
                disablePool(PoolV2Mode.DISABLED_STRICT,1,"Initializing : Repository seems not to be ready - setup file does not exist or not readable");
                esay("Can't read setup file: exists? " +
                     Boolean.toString(_setup.exists()) + " can read? " + Boolean.toString(_setup.canRead()) );
                try {
                    Thread.sleep(30000);
                } catch (InterruptedException ie) {
                    esay("Waiting for repository was interrupted");
                    throw new InterruptedException("Waiting for repository was interrupted");
                }
            }

            _ioQueue = new IoQueueManager(getNucleus(), _args.getOpt("io-queues"));

            _p2pQueue = new SimpleJobScheduler(getNucleus(), "P2P");

            _timeoutManager.addScheduler("p2p", _p2pQueue);
            _timeoutManager.start();

            //
            // add the command listeners before we execute the setupFile.
            //

            addCommandListener(new HsmStorageInterpreter(this, _storageHandler, _pnfs));

            execFile(_setup);

            _running = true;

        } finally {
            if (!_running) {
                start();
                kill();
            }
        }

        _pingThread.start();

        start();

        Object weAreDone = new Object();
        synchronized (weAreDone) {
            _nucleus.newThread(new InventoryScanner(weAreDone), "inventory")
                .start();
            try {
                weAreDone.wait();
            } catch (InterruptedException e) {
                kill();
                throw e;
            }
            _flushingThread.start();
        }
    }

    /**
     * Called by subsystems upon serious faults.
     */
    public void faultOccurred(FaultEvent event)
    {
        esay("Fault occured in " + event.getSource() + ": " + event.getMessage());
        if (event.getCause() != null)
            esay(event.getCause());

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

    @Override
    public CellVersion getCellVersion()
    {
        return new CellVersion(diskCacheV111.util.Version.getVersion(),
                               "$Revision$");
    }

    private class IoQueueManager implements JobScheduler
    {
        private ArrayList<JobScheduler> _list = new ArrayList<JobScheduler>();
        private HashMap<String, JobScheduler> _hash = new HashMap<String, JobScheduler>();
        private boolean _isConfigured = false;

        private IoQueueManager(ThreadFactory factory, String ioQueueList)
        {
            _isConfigured = (ioQueueList != null) && (ioQueueList.length() > 0);
            if (!_isConfigured) {
                ioQueueList = "regular";
            }

            for (String queueName : ioQueueList.split(",")) {
                boolean fifo = !queueName.startsWith("-");
                if (!fifo) {
                    queueName = queueName.substring(1);
                }

                if (_hash.get(queueName) != null) {
                    esay("Duplicated queue name (ignored) : " + queueName);
                    continue;
                }
                int id = _list.size();
                JobScheduler job =
                    new SimpleJobScheduler(factory, "IO-" + id, fifo);
                _list.add(job);
                _hash.put(queueName, job);
                job.setSchedulerId(queueName, id);
                _timeoutManager.addScheduler(queueName, job);
            }
            if (!_isConfigured) {
                say("IoQueueManager : not configured");
            } else {
                say("IoQueueManager : " + _hash.toString());
            }
        }

        private boolean isConfigured()
        {
            return _isConfigured;
        }

        private JobScheduler getDefaultScheduler()
        {
            return _list.get(0);
        }

        private Collection<JobScheduler> getSchedulers()
        {
            return Collections.unmodifiableCollection(_list);
        }

        private JobScheduler getSchedulerByName(String queueName)
        {
            return _hash.get(queueName);
        }

        private JobScheduler getSchedulerById(int id)
        {
            int pos = id % 10;
            if (pos >= _list.size()) {
                throw new IllegalArgumentException(
                                                   "Invalid id (doesn't below to any known scheduler)");
            }
            return  _list.get(pos);
        }

        public JobInfo getJobInfo(int id)
        {
            return getSchedulerById(id).getJobInfo(id);
        }

        public int add(String queueName, Runnable runnable, int priority)
            throws InvocationTargetException
        {
            JobScheduler js = queueName == null ? null : (JobScheduler) _hash
                .get(queueName);

            return js == null ? add(runnable, priority) : js.add(runnable,
                                                                 priority);
        }

        public int add(Runnable runnable) throws InvocationTargetException
        {
            return getDefaultScheduler().add(runnable);
        }

        public int add(Runnable runnable, int priority)
            throws InvocationTargetException
        {
            return getDefaultScheduler().add(runnable, priority);
        }

        public void kill(int jobId, boolean force)
            throws NoSuchElementException
        {
            getSchedulerById(jobId).kill(jobId, force);
        }

        public void remove(int jobId) throws NoSuchElementException
        {
            getSchedulerById(jobId).remove(jobId);
        }

        public StringBuffer printJobQueue(StringBuffer sb)
        {
            if (sb == null) {
                sb = new StringBuffer();
            }
            for (JobScheduler s : _list) {
                s.printJobQueue(sb);
            }
            return sb;
        }

        public int getMaxActiveJobs()
        {
            int sum = 0;
            for (JobScheduler s : _list) {
                sum += s.getMaxActiveJobs();
            }
            return sum;
        }

        public int getActiveJobs()
        {
            int sum = 0;
            for (JobScheduler s : _list) {
               sum += s.getActiveJobs();
            }
            return sum;
        }

        public int getQueueSize()
        {
            int sum = 0;
            for (JobScheduler s : _list) {
                sum += s.getQueueSize();
            }
            return sum;
        }

        public void setMaxActiveJobs(int maxJobs)
        {
        }

        public List<JobInfo>  getJobInfos()
        {
            List<JobInfo> list = new ArrayList<JobInfo>();
            for (JobScheduler s : _list) {
                list.addAll(s.getJobInfos());
            }
            return list;
        }

        public void setSchedulerId(String name, int id)
        {
        }

        public String getSchedulerName()
        {
            return "Manager";
        }

        public int getSchedulerId()
        {
            return -1;
        }

        public void dumpSetup(PrintWriter pw)
        {
            for (JobScheduler s : _list) {
                pw.println("mover set max active -queue="
                           + s.getSchedulerName() + " " + s.getMaxActiveJobs());
            }
        }

        public JobInfo findJob(String client, long id)
        {
            for (JobInfo info : getJobInfos()) {
                if (client.equals(info.getClientName())
                    && id == info.getClientId()) {
                    return info;
                }
            }
            return null;
        }
    }

    private class InventoryScanner implements Runnable
    {
        private final Object _notifyMe;

        private InventoryScanner(Object notifyMe)
        {
            _notifyMe = notifyMe;
        }

        public void run()
        {
            _logClass.log("Running Repository (Cell is locked)");
            _logClass.log("Repository seems to be ok");
            try {
                _repository.init(_recoveryFlags);
                enablePool();
            } catch (Throwable e) {
                _logClass.elog("Repository reported a problem : "
                               + e.getMessage());
                _logClass.elog("Pool not enabled " + _poolName);
                disablePool(PoolV2Mode.DISABLED_DEAD | PoolV2Mode.DISABLED_STRICT,
                            666, "Init failed: " + e.getMessage());
            }
            _logClass.elog("Repository finished");
            if (_notifyMe != null) {
                synchronized (_notifyMe) {
                    _notifyMe.notifyAll();
                }
            }
        }
    }

    @Override
    public void cleanUp()
    {
        disablePool(PoolV2Mode.DISABLED_DEAD | PoolV2Mode.DISABLED_STRICT,
                    666, "Shutdown");
        if (_repository != null)
            _repository.shutdown();
    }

    /**
     * Sets the h-flag in PNFS.
     */
    private class HFlagMaintainer implements StateChangeListener
    {
        public void stateChanged(StateChangeEvent event)
        {
            if (event.getOldState() == EntryState.FROM_CLIENT) {
                PnfsId id = event.getPnfsId();
                if (_lfsMode == LFS_NONE) {
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
    private class RepositoryLoader implements StateChangeListener
    {
        public void stateChanged(StateChangeEvent event)
        {
            PnfsId id = event.getPnfsId();
            EntryState from = event.getOldState();
            EntryState to = event.getNewState();

            if (from == to)
                return;

            if (to == EntryState.PRECIOUS) {
                say("Adding " + id + " to flush queue");

                if (_lfsMode == LFS_NONE) {
                    try {
                        _storageQueue.addCacheEntry(id);
                    } catch (FileNotInCacheException e) {
                        /* File was deleted before we got a chance to do
                         * anything with it. We don't care about deleted
                         * files so we ignore this.
                         */
                        say("Failed to flush " + id + ": Replica is no longer in the pool");
                    } catch (CacheException e) {
                        esay("Error adding " + id + " to flush queue: "
                             + e.getMessage());
                    }
                }
            } else if (from == EntryState.PRECIOUS) {
                say("Removing " + id + " from flush queue");
                try {
                    if (!_storageQueue.removeCacheEntry(id))
                        say("File " + id + " not found in flush queue");
                } catch (CacheException e) {
                    esay("Error removing " + id + " from flush queue: " + e);
                }
            }
        }
    }

    private class NotifyBillingOnRemoveListener implements StateChangeListener
    {
        public void stateChanged(StateChangeEvent event)
        {
            if (_reportOnRemovals && event.getNewState() == EntryState.REMOVED) {
                PnfsId id = event.getPnfsId();
                try {
                    String source = getCellName() + "@" + getCellDomainName();
                    InfoMessage msg =
                        new RemoveFileInfoMessage(source, id);
                    sendMessage(new CellMessage(_billingCell, msg));
                } catch (NotSerializableException e) {
                    throw new RuntimeException("Bug detected: Unserializable vehicle", e);
                } catch (NoRouteToCellException e) {
                    esay("Failed to send message to " + _billingCell + ": "
                         + e.getMessage());
                }
            }
        }
    }

    private void execFile(File setup)
        throws IOException, CommandException
    {
        BufferedReader br = new BufferedReader(new FileReader(setup));
        String line;
        try {
            int lineCount = 0;
            while ((line = br.readLine()) != null) {
                ++lineCount;

                line = line.trim();
                if (line.length() == 0)
                    continue;
                if (line.charAt(0) == '#')
                    continue;
                say("Execute setup : " + line);
                try {
                    command(new Args(line));
                } catch (CommandException ce) {
                    esay("Error executing line " + lineCount + " : " + ce);
                    throw ce;
                }
            }
        } finally {
            try {
                br.close();
            } catch (IOException dummy) {
                // ignored
            }
        }
    }

    private void dumpSetup(PrintWriter pw)
    {
        SpaceRecord space = _repository.getSpaceRecord();

        pw.println("#\n# Created by " + getCellName() + "("
                   + this.getClass().getName() + ") at " + (new Date()).toString()
                   + "\n#");
        pw.println("set max diskspace " + space.getTotalSpace());
        pw.println("set heartbeat " + _pingThread.getHeartbeat());
        pw.println("set sticky " + (_allowSticky ? "allowed" : "denied"));
        pw.println("set report remove " + (_reportOnRemovals ? "on" : "off"));
        pw.println("set breakeven " + _breakEven);
        if (_suppressHsmLoad)
            pw.println("pool suppress hsmload on");
        pw.println("set gap " + _gap);
        pw
            .println("set duplicate request "
                     + (_dupRequest == DUP_REQ_NONE ? "none"
                        : _dupRequest == DUP_REQ_IGNORE ? "ignore"
                        : "refresh"));
        pw.println("set p2p "
                   + (_p2pMode == P2P_INTEGRATED ? "integrated" : "separated"));
        _flushingThread.printSetup(pw);
        if (_storageQueue != null)
            _storageQueue.printSetup(pw);
        if (_storageHandler != null)
            _storageHandler.printSetup(pw);
        if (_hsmSet != null)
            _hsmSet.printSetup(pw);
        if (_repository != null)
            _repository.printSetup(pw);
        if (_ioQueue != null)
            ((IoQueueManager) _ioQueue).dumpSetup(pw);
        if (_p2pQueue != null) {
            pw.println("p2p set max active " + _p2pQueue.getMaxActiveJobs());
        }
        if (_p2pClient != null)
            _p2pClient.printSetup(pw);
        if (_timeoutManager != null)
            _timeoutManager.printSetup(pw);
        _checksumModule.dumpSetup(pw);
    }

    private void dumpSetup()
        throws IOException
    {
        String name = _setup.getName();
        String parent = _setup.getParent();
        File tempFile =
            parent == null
            ? new File("." + name)
            : new File(parent, "." + name);

        PrintWriter pw = new PrintWriter(new FileWriter(tempFile));
        try {
            dumpSetup(pw);
        } finally {
            pw.close();
        }
        if (!tempFile.renameTo(_setup))
            throw new IOException("Rename failed (" + tempFile + " -> "
                                  + _setup + ")");
    }


    @Override
    public CellInfo getCellInfo()
    {
        PoolCellInfo info = new PoolCellInfo(super.getCellInfo());
        info.setPoolCostInfo(getPoolCostInfo());
        info.setTagMap(_tags);
        info.setErrorStatus(_poolStatusCode, _poolStatusMessage);
        return info;
    }

    public void log(String str)
    {
        say(str);
    }

    public void elog(String str)
    {
        esay(str);
    }

    public void plog(String str)
    {
        esay("PANIC : " + str);
    }

    @Override
    public void getInfo(PrintWriter pw)
    {
        pw.println("Base directory    : " + _baseDir);
        pw.println("Revision          : [$Revision$]");
        pw.println("Version           : " + getCellVersion() + " (Sub="
                   + _version + ")");
        pw.println("StickyFiles       : "
                   + (_allowSticky ? "allowed" : "denied"));
        pw.println("Gap               : " + _gap);
        pw.println("Report remove     : " + (_reportOnRemovals ? "on" : "off"));
        pw.println("Recovery          : "
                   + ((_recoveryFlags & CacheRepository.ALLOW_CONTROL_RECOVERY) > 0 ? "CONTROL "
                      : "")
                   + ((_recoveryFlags & CacheRepository.ALLOW_SPACE_RECOVERY) > 0 ? "SPACE "
                      : "")
                   + ((_recoveryFlags & CacheRepository.ALLOW_RECOVER_ANYWAY) > 0 ? "ANYWAY "
                      : ""));
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
        switch (_lfsMode) {
        case LFS_NONE:
            pw.println("LargeFileStore    : None");
            break;
        case LFS_PRECIOUS:
            pw.println("LargeFileStore    : Precious");
            break;
        case LFS_VOLATILE:
            pw.println("LargeFileStore    : Volatile");
            break;
        }
        pw.println("DuplicateRequests : "
                   + (_dupRequest == DUP_REQ_NONE ? "None"
                      : _dupRequest == DUP_REQ_IGNORE ? "Ignored"
                      : "Refreshed"));
        pw.println("P2P Mode          : "
                   + (_p2pMode == P2P_INTEGRATED ? "Integrated" : "Separated"));
        pw.println("P2P File Mode     : "
                   + (_p2pFileMode == P2P_PRECIOUS ? "Precious" : "Cached"));

        if (_hybridInventoryActive) {
            pw.println("Inventory         : " + _hybridCurrent);
        }

        _repository.getInfo(pw);

        if (_flushingThread != null)
            _flushingThread.getInfo(pw);

        pw.println("Storage Queue     : ");
        if (_storageQueue != null) {
            pw.println("   Classes  : " + _storageQueue.getStorageClassCount());
            pw.println("   Requests : " + _storageQueue.getRequestCount());
        } else {
            pw.println("   Not Yet known");
        }
        if (_ioQueue != null) {
            IoQueueManager manager = (IoQueueManager) _ioQueue;
            pw.println("Mover Queue Manager : "
                       + (manager.isConfigured() ? "Active" : "Not Configured"));
            for (JobScheduler js : manager.getSchedulers()) {
                pw.println("Mover Queue (" + js.getSchedulerName() + ") "
                           + js.getActiveJobs() + "(" + js.getMaxActiveJobs()
                           + ")/" + js.getQueueSize());
            }
        }
        if (_p2pQueue != null)
            pw.println("P2P   Queue " + _p2pQueue.getActiveJobs() + "("
                       + _p2pQueue.getMaxActiveJobs() + ")/"
                       + _p2pQueue.getQueueSize());
        if (_storageHandler != null)
            _storageHandler.getInfo(pw);

        _p2pClient.getInfo(pw);
        _timeoutManager.getInfo(pw);
        _checksumModule.getInfo(pw);
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
        IoQueueManager queue = (IoQueueManager) _ioQueue;
        if (message instanceof PoolAcceptFileMessage) {
            return queue.add(queueName, request, SimpleJobScheduler.HIGH);
        } else if (message.isPool2Pool()) {
            if (_p2pMode == P2P_INTEGRATED) {
                return queue.add(request, SimpleJobScheduler.HIGH);
            } else {
                return _p2pQueue.add(request, SimpleJobScheduler.HIGH);
            }
        } else {
            return queue.add(queueName, request, SimpleJobScheduler.REGULAR);
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
                IoQueueManager queue = (IoQueueManager) _ioQueue;

                JobInfo job = queue.findJob(door, id);
                if (job != null) {
                    switch (_dupRequest) {
                    case DUP_REQ_NONE:
                        say("Dup Request : none <" + door + ":" + id + ">");
                        break;
                    case DUP_REQ_IGNORE:
                        say("Dup Request : ignoring <" + door + ":" + id + ">");
                        return;
                    case DUP_REQ_REFRESH:
                        long jobId = job.getJobId();
                        say("Dup Request : refresing <" + door + ":"
                            + id + "> old = " + jobId);
                        queue.kill((int)jobId, true);
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
                transfer =
                    new PoolIOWriteTransfer(pnfsId, pi, si, mover, _repository,
                                            _checksumModule);
            } else {
                transfer =
                    new PoolIOReadTransfer(pnfsId, pi, si, mover, _repository);
            }
            try {
                source.revert();
                PoolIORequest request =
                    new PoolIORequest(transfer, id, initiator,
                                      source, pool, queueName);
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
                        esay("Communication failure while closing " + pnfsId
                             + ": " + e.getMessage());
                    } catch (IOException e) {
                        esay("IO error while closing " + pnfsId
                             + ": " + e.getMessage());
                    } catch (InterruptedException e) {
                        esay("Interrupted while closing " + pnfsId
                             + ": " + e.getMessage());
                    }
                }
            }
            message.setSucceeded();
        } catch (FileInCacheException e) {
            warn("Attempted to create existing replica " + pnfsId);
            message.setFailed(e.getRc(), "Pool already contains " + pnfsId);
        } catch (FileNotInCacheException e) {
            warn("Attempted to open non-existing replica " + pnfsId);
            message.setFailed(e.getRc(), "Pool does not contain " + pnfsId);
        } catch (CacheException e) {
            error(e.getMessage());
            message.setFailed(e.getRc(), e.getMessage());
        } catch (Throwable e) {
            fatal("Possible bug found: " + e.getMessage());
            debug(e);
            message.setFailed(CacheException.DEFAULT_ERROR_CODE,
                              "Failed to enqueue mover: " + e.getMessage());
        }

        try {
            envelope.revertDirection();
            sendMessage(envelope);
        } catch (NotSerializableException e) {
            throw new RuntimeException("Bug detected: Unserializable vehicle", e);
        } catch (NoRouteToCellException e) {
            esay(e);
        }
    }

    /**
     * PoolIORequest encapsulates queuing, execution and notification
     * of a file transfer.
     *
     * The transfer is represented by a PoolIOTransfer instance, and
     * PoolIORequest manages the lifetime of the transfer object.
     *
     * Billing and door notifications are send after completed or
     * failed transfer, or upon dequeuing the request.
     */
    private class PoolIORequest implements IoBatchable
    {
        private final PoolIOTransfer _transfer;
        private final long _id;
        private final String _queue;
        private final String _pool;
        private final CellPath _door;
        private final String _initiator;

        private Thread _thread;

        /**
         * @param transfer the read or write transfer to execute
         * @param id the client id of the request
         * @param initiator the initiator string identifying who
         * requested the transfer
         * @param door the cell path to the cell requesting the
         * transfer
         * @param pool the name of the pool
         * @param queue the name of the queue used for the request
         */
        public PoolIORequest(PoolIOTransfer transfer,
                             long id, String initiator,
                             CellPath door, String pool, String queue)
        {
            _transfer = transfer;
            _id = id;
            _initiator = initiator;
            _door = door;
            _pool = pool;
            _queue = queue;
        }

        private void sendBillingMessage(int rc, String message)
        {
            MoverInfoMessage info =
                new MoverInfoMessage(getCellName() + "@" + getCellDomainName(),
                                     getPnfsId());
            info.setInitiator(_initiator);
            info.setFileCreated(_transfer instanceof PoolIOWriteTransfer);
            info.setStorageInfo(getStorageInfo());
            info.setFileSize(_transfer.getFileSize());
            info.setResult(rc, message);
            info.setTransferAttributes(getBytesTransferred(),
                                       getTransferTime(),
                                       getProtocolInfo());

            try {
                sendMessage(new CellMessage(_billingCell, info));
            } catch (NotSerializableException e) {
                throw new RuntimeException("Bug: Unserializable vehicle detected", e);
            } catch (NoRouteToCellException e) {
                esay("Cannot send message to " + _billingCell + ": No route to cell");
            }
        }

        private void sendFinished(int rc, String msg)
        {
            DoorTransferFinishedMessage finished =
                new DoorTransferFinishedMessage(getClientId(),
                                                getPnfsId(),
                                                getProtocolInfo(),
                                                getStorageInfo(),
                                                _pool);
            finished.setIoQueueName(_queue);
            if (rc == 0) {
                finished.setSucceeded();
            } else {
                finished.setReply(rc, msg);
            }

            try {
                sendMessage(new CellMessage(_door, finished));
            } catch (NotSerializableException e) {
                throw new RuntimeException("Bug: Unserializable vehicle detected", e);
            } catch (NoRouteToCellException e) {
                esay("Cannot send message to " + _door + ": No route to cell");
            }
        }

        protected ProtocolInfo getProtocolInfo()
        {
            return _transfer.getProtocolInfo();
        }

        protected StorageInfo getStorageInfo()
        {
            return _transfer.getStorageInfo();
        }

        public long getTransferTime()
        {
            return _transfer.getTransferTime();
        }

        public long getBytesTransferred()
        {
            return _transfer.getBytesTransferred();
        }

        public double getTransferRate()
        {
            return _transfer.getTransferRate();
        }

        public long getLastTransferred()
        {
            return _transfer.getLastTransferred();
        }

        public PnfsId getPnfsId()
        {
            return _transfer.getPnfsId();
        }

        public void queued(int id)
        {
        }

        public void unqueued()
        {
            /* Closing the transfer object should not throw an
             * exception when the transfer has not begun yet. If it
             * does, we log the error, but otherwise there is not much
             * we can do. REVISIT: Consider to disable the pool.
             */
            try {
                _transfer.close();
            } catch (NoRouteToCellException e) {
                esay("Failed to cancel transfer: " + e);
            } catch (CacheException e) {
                esay("Failed to cancel transfer: " + e);
            } catch (IOException e) {
                esay("Failed to cancel transfer: " + e);
            } catch (InterruptedException e) {
                esay("Failed to cancel transfer: " + e);
            }

            sendFinished(CacheException.DEFAULT_ERROR_CODE,
                         "Transfer was killed");
        }

        public String getClient()
        {
            return _door.getDestinationAddress().toString();
        }

        public long getClientId()
        {
            return _id;
        }

        private synchronized void setThread(Thread thread)
        {
            _thread = thread;
        }

        public synchronized boolean kill()
        {
            if (_thread == null) {
                return false;
            }
            _thread.interrupt();
            return true;
        }

        public void run()
        {
            int rc;
            String msg;
            try {
                setThread(Thread.currentThread());
                try {
                    _transfer.transfer();
                } finally {
                    setThread(null);
                    _transfer.close();
                }

                rc = 0;
                msg = "";
            } catch (InterruptedException e) {
                rc = 37;
                msg = "Transfer was killed";
            } catch (CacheException e) {
                if (e.getRc() == CacheRepository.ERROR_IO_DISK) {
                    disablePool(PoolV2Mode.DISABLED_STRICT,
                                CacheRepository.ERROR_IO_DISK,
                                e.getMessage());
                }
                rc = e.getRc();
                msg = e.getMessage();
            } catch (Exception e) {
                rc = 37;
                msg = "Unexpected exception: " + e.getMessage();
            }

            sendFinished(rc, msg);
            sendBillingMessage(rc, msg);
        }

        @Override
        public String toString()
        {
            return _transfer.toString();
        }

    }

    // //////////////////////////////////////////////////////////////
    //
    // replication on data arrived
    //
    private class ReplicationHandler implements StateChangeListener
    {
        private boolean _enabled = false;
        private String _replicationManager = "PoolManager";
        private String _destinationHostName = null;
        private String _destinationMode = "keep";
        private boolean _replicateOnRestore = false;

        //
        // replicationManager,Hostname,modeOfDestFile
        //
        private ReplicationHandler()
        {
        }

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
            _replicationManager = (args.length > 0) && (!args[0].equals("")) ? args[0]
                : _replicationManager;
            _destinationHostName = (args.length > 1) && (!args[1].equals("")) ? args[1]
                : _destinationHostName;
            _destinationMode = (args.length > 2) && (!args[2].equals("")) ? args[2]
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
                esay("Problem in sending replication request : " + e);
            } catch (NoRouteToCellException e) {
                esay("Problem in sending replication request : " + e.getMessage());
            }
        }

        private void _initiateReplication(CacheEntry entry, String source)
            throws CacheException, NoRouteToCellException
        {
            PnfsId pnfsId = entry.getPnfsId();
            StorageInfo storageInfo = entry.getStorageInfo();

            storageInfo.setKey("replication.source", source);

            PoolMgrReplicateFileMsg req =
                new PoolMgrReplicateFileMsg(pnfsId,
                                            storageInfo,
                                            new DCapProtocolInfo("DCap", 3, 0,
                                                                 _destinationHostName, 2222),
                                            storageInfo.getFileSize());
            req.setReplyRequired(false);
            try {
                sendMessage(new CellMessage(new CellPath(_replicationManager), req));
            } catch (NotSerializableException e) {
                throw new RuntimeException("Bug detected: Unserializable vehicle", e);
            }

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
                moverClassName = "diskCacheV111.movers." + info.getProtocol()
                    + "Protocol_" + info.getMajorVersion();

                mover = _handlerClasses.get(moverClassName);

                if (mover == null) {
                    mover = Class.forName(moverClassName);
                    _handlerClasses.put(moverClassName, mover);
                }

            }
            Constructor<?> moverCon = mover.getConstructor(argsClass);
            Object[] args = { this };
            MoverProtocol instance = (MoverProtocol) moverCon.newInstance(args);

            for (Map.Entry<?,?> attribute : _moverAttributes.entrySet()) {
                try {
                    Object key = attribute.getKey();
                    Object value = attribute.getValue();
                    instance.setAttribute(key.toString(), value);
                } catch (IllegalArgumentException e) {
                    esay("setAttribute : " + e.getMessage());
                }
            }

            return instance;
        } catch (Exception e) {
            esay("Couldn't get Handler Class" + moverClassName);
            esay(e);
            return null;
        }
    }


    // //////////////////////////////////////////////////////////////////////////
    //
    // interface to the HsmRestoreHandler
    //
    private class ReplyToPoolFetch implements CacheFileAvailable
    {
        private CellMessage _cellMessage = null;

        private ReplyToPoolFetch(CellMessage cellMessage)
        {
            _cellMessage = cellMessage;
        }

        public void cacheFileAvailable(String pnfsId, Throwable ee)
        {
            Message msg = (Message) _cellMessage.getMessageObject();
            PnfsId id = new PnfsId(pnfsId);
            try {
                if (ee == null) {
                    msg.setSucceeded();
                } else if (ee instanceof CacheException) {
                    CacheException ce = (CacheException) ee;
                    int errorCode = ce.getRc();
                    msg.setFailed(errorCode, ce.getMessage());

                    switch (errorCode) {
                    case 41:
                    case 42:
                    case 43:
                        disablePool(PoolV2Mode.DISABLED_STRICT, errorCode, ce
                                    .getMessage());
                    }
                } else {
                    msg.setFailed(1000, ee);
                }
            } finally {
                if (msg.getReturnCode() != 0) {
                    esay("Fetch of " + id + " failed: " + msg.getErrorObject().toString());

                    /* Something went wrong. We delete the file to be
                     * on the safe side (better waste tape bandwidth
                     * than risk leaving a broken file).
                     */
                    try {
                        _repository.setState(id, EntryState.REMOVED);
                    } catch (IllegalTransitionException e) {
                        /* Most likely indicate that the file was removed
                         * before we could do it. Log the problem, but
                         * otherwise ignore it.
                         */
                        esay("Failed to remove " + pnfsId +  ": "
                             + e.getMessage());
                    }
                }

                try {
                    _cellMessage.revertDirection();
                    sendMessage(_cellMessage);
                } catch (NotSerializableException e) {
                    throw new RuntimeException("Bug detected: Unserializable vehicle", e);
                } catch (NoRouteToCellException e) {
                    esay("Failed to send reply to " + _cellMessage.getDestinationAddress() + ": " + e.getMessage());
                }
            }
        }
    }

    private boolean fetchFile(PoolFetchFileMessage poolMessage,
                              CellMessage cellMessage)
    {
        PnfsId pnfsId = poolMessage.getPnfsId();
        StorageInfo storageInfo = poolMessage.getStorageInfo();
        say("Pool " + _poolName + " asked to fetch file " + pnfsId + " (hsm="
            + storageInfo.getHsm() + ")");

        try {
            ReplyToPoolFetch reply = new ReplyToPoolFetch(cellMessage);
            _storageHandler.fetch(pnfsId, storageInfo, reply);
            return false;
        } catch (FileInCacheException ce) {
            esay("Fetch failed: Repository already contains " + pnfsId);
            poolMessage.setSucceeded();
            return true;
        } catch (CacheException ce) {
            esay(ce);
            poolMessage.setFailed(ce.getRc(), ce);
            if (ce.getRc() == CacheRepository.ERROR_IO_DISK)
                disablePool(PoolV2Mode.DISABLED_STRICT, ce.getRc(), ce
                            .getMessage());
            return true;
        } catch (Exception ui) {
            esay(ui);
            poolMessage.setFailed(100, ui);
            return true;
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

    private void setSticky(PoolSetStickyMessage stickyMessage)
    {
        if (stickyMessage.isSticky() && !_allowSticky) {
            stickyMessage.setFailed(101, "making sticky denied by pool : "
                                    + _poolName);
            return;
        }

        try {
            _repository.setSticky(stickyMessage.getPnfsId(),
                                  stickyMessage.getOwner(),
                                  stickyMessage.isSticky()
                                  ? stickyMessage.getLifeTime()
                                  : 0);
        } catch (FileNotInCacheException e) {
            stickyMessage.setFailed(e.getRc(), e);
        }
    }

    private void modifyPersistency(PoolModifyPersistencyMessage persistencyMessage)
    {
        try {
            PnfsId pnfsId = persistencyMessage.getPnfsId();
            switch (_repository.getState(pnfsId)) {
            case PRECIOUS:
                if (persistencyMessage.isCached())
                    _repository.setState(pnfsId, EntryState.CACHED);
                break;

            case CACHED:
                if (persistencyMessage.isPrecious())
                    _repository.setState(pnfsId, EntryState.PRECIOUS);
                break;

            case FROM_CLIENT:
            case FROM_POOL:
            case FROM_STORE:
                persistencyMessage.setFailed(101, "File still transient: "
                                             + pnfsId);
                break;

            case BROKEN:
                persistencyMessage.setFailed(101, "File is broken: "
                                             + pnfsId);
                break;

            case NEW:
            case REMOVED:
            case DESTROYED:
                persistencyMessage.setFailed(101, "File does not exist: "
                                             + pnfsId);
                break;
            }
        } catch (Exception ee) {
            persistencyMessage.setFailed(100, ee);
        }
    }

    private void modifyPoolMode(PoolModifyModeMessage modeMessage)
    {
        PoolV2Mode mode = modeMessage.getPoolMode();
        if (mode == null)
            return;

        if (mode.isEnabled()) {
            enablePool();
        } else {
            disablePool(mode.getMode(), modeMessage.getStatusCode(),
                        modeMessage.getStatusMessage());
        }
    }

    private void checkFreeSpace(PoolCheckFreeSpaceMessage poolMessage)
    {
        // long freeSpace = _repository.getFreeSpace() ;
        long freeSpace = 1024L * 1024L * 1024L * 100L;
        say("XChecking free space [ result = " + freeSpace + " ] ");
        poolMessage.setFreeSpace(freeSpace);
        poolMessage.setSucceeded();
    }

    private void updateCacheStatistics(PoolUpdateCacheStatisticsMessage poolMessage)
    {
        // /
    }

    private class CompanionFileAvailableCallback implements CacheFileAvailable
    {
        private final CellMessage _envelope;
        private final Pool2PoolTransferMsg _message;

        private CompanionFileAvailableCallback(CellMessage envelope,
                                               Pool2PoolTransferMsg message)
        {
            _envelope = envelope;
            _message = message;
        }

        public void cacheFileAvailable(String pnfsIdString, Throwable error)
        {
            if (_message.getReplyRequired()) {
                if (error != null) {
                    if (error instanceof FileInCacheException) {
                        _message.setReply(0, null);
                    } else if (error instanceof CacheException) {
                        _message.setReply(((CacheException) error).getRc(), error);
                    } else {
                        _message.setReply(CacheException.UNEXPECTED_SYSTEM_EXCEPTION, error);
                    }
                }

                say("Sending p2p reply " + _message);
                try {
                    _envelope.revertDirection();
                    sendMessage(_envelope);
                } catch (NotSerializableException e) {
                    throw new RuntimeException("Bug detected: Unserializable vehicle", e);
                } catch (NoRouteToCellException e) {
                    esay("Cannot reply p2p message : " + e.getMessage());
                }
            }
        }
    }

    private void runPool2PoolClient(final CellMessage cellMessage,
                                    final Pool2PoolTransferMsg poolMessage)
    {
        String poolName = poolMessage.getPoolName();
        PnfsId pnfsId = poolMessage.getPnfsId();
        StorageInfo storageInfo = poolMessage.getStorageInfo();
        CacheFileAvailable callback =
            new CompanionFileAvailableCallback(cellMessage, poolMessage);

        EntryState targetState = EntryState.CACHED;
        int fileMode = poolMessage.getDestinationFileStatus();
        if (fileMode != Pool2PoolTransferMsg.UNDETERMINED) {
            if (fileMode == Pool2PoolTransferMsg.PRECIOUS)
                targetState = EntryState.PRECIOUS;
        } else if (_lfsMode == LFS_PRECIOUS && _p2pFileMode == P2P_PRECIOUS) {
            targetState = EntryState.PRECIOUS;
        }

        _p2pClient.newCompanion(pnfsId, poolName, storageInfo, targetState, callback);
    }

    @Override
    public void messageArrived(CellMessage cellMessage)
    {
        Object messageObject = cellMessage.getMessageObject();

        if (!(messageObject instanceof Message)) {
            say("Unexpected message class 1 " + messageObject.getClass());
            return;
        }

        Message poolMessage = (Message) messageObject;

        boolean replyRequired = poolMessage.getReplyRequired();
        if (poolMessage instanceof PoolMoverKillMessage) {
            PoolMoverKillMessage kill = (PoolMoverKillMessage) poolMessage;
            say("PoolMoverKillMessage for mover id " + kill.getMoverId());
            try {
                mover_kill(kill.getMoverId(), false);
            } catch (NoSuchElementException e) {
                esay(e);
                kill.setReply(1, e);
            }
        } else if (poolMessage instanceof PoolFlushControlMessage) {

            _flushingThread.messageArrived((PoolFlushControlMessage)poolMessage, cellMessage);
            return;

        } else if (poolMessage instanceof DoorTransferFinishedMessage) {

            _p2pClient.messageArrived(poolMessage, cellMessage);

            return;

        } else if (poolMessage instanceof PoolIoFileMessage) {

            PoolIoFileMessage msg = (PoolIoFileMessage) poolMessage;

            if (((poolMessage instanceof PoolAcceptFileMessage)
                 && _poolMode.isDisabled(PoolV2Mode.DISABLED_STORE))
                || ((poolMessage instanceof PoolDeliverFileMessage)
                    && _poolMode.isDisabled(PoolV2Mode.DISABLED_FETCH))) {

                esay("PoolIoFileMessage Request rejected due to "
                     + _poolMode);
                sentNotEnabledException(poolMessage, cellMessage);
                return;

            }

            msg.setReply();
            ioFile(cellMessage, (PoolIoFileMessage) poolMessage);
            return;

        } else if (poolMessage instanceof Pool2PoolTransferMsg) {

            if (_poolMode.isDisabled(PoolV2Mode.DISABLED_P2P_CLIENT)) {

                esay("Pool2PoolTransferMsg Request rejected due to "
                     + _poolMode);
                sentNotEnabledException( poolMessage, cellMessage);
                return;

            }

            runPool2PoolClient(cellMessage, (Pool2PoolTransferMsg) poolMessage);

            poolMessage.setReply();

            return;

        } else if (poolMessage instanceof PoolFetchFileMessage) {

            if (_poolMode.isDisabled(PoolV2Mode.DISABLED_STAGE )
                || (_lfsMode != LFS_NONE)) {

                esay("PoolFetchFileMessage  Request rejected due to "
                     + _poolMode);
                sentNotEnabledException(poolMessage, cellMessage);
                return;

            }

            replyRequired = fetchFile((PoolFetchFileMessage) poolMessage,
                                      cellMessage);

        } else if (poolMessage instanceof PoolRemoveFilesFromHSMMessage) {

            if (_poolMode.isDisabled(PoolV2Mode.DISABLED_STAGE) ||
                (_lfsMode != LFS_NONE)) {

                esay("PoolRemoveFilesFromHsmMessage request rejected due to "
                     + _poolMode);
                sentNotEnabledException(poolMessage, cellMessage);
                return;
            }

            _storageHandler.remove(cellMessage);
            replyRequired = false;

        } else if (poolMessage instanceof PoolCheckFreeSpaceMessage) {

            if (_poolMode.isDisabled(PoolV2Mode.DISABLED)) {

                esay("PoolCheckFreeSpaceMessage Request rejected due to "
                     + _poolMode);
                sentNotEnabledException(poolMessage, cellMessage);
                return;

            }

            checkFreeSpace((PoolCheckFreeSpaceMessage) poolMessage);

        } else if (poolMessage instanceof PoolCheckable) {
            try {
                if (_poolMode.isDisabled(PoolV2Mode.DISABLED) ||
                    _poolMode.isDisabled(PoolV2Mode.DISABLED_FETCH)) {

                    esay("PoolCheckable Request rejected due to " + _poolMode);
                    sentNotEnabledException(poolMessage, cellMessage);
                    return;
                }

                if (poolMessage instanceof PoolFileCheckable) {
                    checkFile((PoolFileCheckable) poolMessage);
                }
                poolMessage.setSucceeded();
            } catch (CacheException e) {
                poolMessage.setFailed(e.getRc(), e.getMessage());
            }
        } else if (poolMessage instanceof PoolUpdateCacheStatisticsMessage) {

            updateCacheStatistics((PoolUpdateCacheStatisticsMessage) poolMessage);

        } else if (poolMessage instanceof PoolRemoveFilesMessage) {

            if (_poolMode.isDisabled(PoolV2Mode.DISABLED)) {

                esay("PoolRemoveFilesMessage Request rejected due to "
                     + _poolMode);
                sentNotEnabledException(poolMessage, cellMessage);
                return;

            }
            removeFiles((PoolRemoveFilesMessage) poolMessage);

        } else if (poolMessage instanceof PoolModifyPersistencyMessage) {

            modifyPersistency((PoolModifyPersistencyMessage) poolMessage);

        } else if (poolMessage instanceof PoolModifyModeMessage) {

            modifyPoolMode((PoolModifyModeMessage) poolMessage);

        } else if (poolMessage instanceof PoolSetStickyMessage) {

            setSticky((PoolSetStickyMessage) poolMessage);

        } else if (poolMessage instanceof PoolQueryRepositoryMsg) {

            getRepositoryListing((PoolQueryRepositoryMsg) poolMessage);
            replyRequired = true;

        } else {
            say("Unexpected message class 2" + poolMessage.getClass());
            say(" isReply = " + ( poolMessage).isReply()); // REMOVE
            say(" source = " + cellMessage.getSourceAddress());
            return;
        }
        if (!replyRequired)
            return;
        try {
            cellMessage.revertDirection();
            sendMessage(cellMessage);
        } catch (NotSerializableException e) {
            throw new RuntimeException("Bug detected: Unserializable vehicle", e);
        } catch (NoRouteToCellException e) {
            esay("Cannot reply message : " + e.getMessage());
        }
    }

    private void getRepositoryListing(PoolQueryRepositoryMsg queryMessage)
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
        queryMessage.setReply(new RepositoryCookie(), listing);
    }

    private void sentNotEnabledException(Message poolMessage,
                                         CellMessage cellMessage)
    {
        try {
            poolMessage.setFailed(104, "Pool is disabled");
            cellMessage.revertDirection();
            sendMessage(cellMessage);
        } catch (NotSerializableException e) {
            throw new RuntimeException("Bug detected: Unserializable vehicle", e);
        } catch (NoRouteToCellException e) {
            esay("Cannot reply message : " + e.getMessage());
        }
    }

    public String hh_simulate_cost = "[-cpu=<cpuCost>] [-space=<space>]";

    public String ac_simulate_cost(Args args)
        throws NumberFormatException
    {
        String tmp = args.getOpt("cpu");
        if (tmp != null)
            _simCpuCost = Double.parseDouble(tmp);
        tmp = args.getOpt("space");
        if (tmp != null)
            _simSpaceCost = Double.parseDouble(tmp);

        return "Costs : cpu = " + _simCpuCost + " , space = " + _simSpaceCost;
    }


    /**
     * Partially or fully disables normal operation of this pool.
     */
    private synchronized void disablePool(int mode, int errorCode, String errorString)
    {
        _poolStatusCode = errorCode;
        _poolStatusMessage =
            (errorString == null ? "Requested By Operator" : errorString);
        _poolMode.setMode(mode);

        _pingThread.sendPoolManagerMessage(true);
        esay("Pool mode changed to " + _poolMode);
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
        esay("Pool mode changed to " + _poolMode);
    }

    private class PoolManagerPingThread implements Runnable
    {
        private final Thread _worker;
        private int _heartbeat = 30;

        private PoolManagerPingThread()
        {
            _worker = _nucleus.newThread(this, "ping");
        }

        private void start()
        {
            _worker.start();
        }

        public void run()
        {
            say("Ping Thread started");
            try {
                while (!Thread.interrupted()) {
                    sendPoolManagerMessage(true);
                    Thread.sleep(_heartbeat * 1000);
                }
            } catch (InterruptedException e) {
                esay("Ping Thread was interrupted");
            }

            esay("Ping Thread sending Pool Down message");
            disablePool(PoolV2Mode.DISABLED_DEAD | PoolV2Mode.DISABLED_STRICT,
                        666, "PingThread terminated");
            esay("Ping Thread finished");
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
            } catch (NotSerializableException e) {
                throw new RuntimeException("Bug detected: Unserializable vehicle", e);
            } catch (NoRouteToCellException e){
                esay("Failed to send ping message: " + e.getMessage());
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

        IoQueueManager manager = (IoQueueManager) _ioQueue;
        if (manager.isConfigured()) {
            for (JobScheduler js : manager.getSchedulers()) {
                info.addExtendedMoverQueueSizes(js.getSchedulerName(), js
						.getActiveJobs(), js.getMaxActiveJobs(), js
						.getQueueSize());
            }
        }
        info.setP2pClientQueueSizes(_p2pClient.getActiveJobs(), _p2pClient
                                    .getMaxActiveJobs(), _p2pClient.getQueueSize());

        if (_p2pMode == P2P_SEPARATED) {

            info.setP2pServerQueueSizes(_p2pQueue.getActiveJobs(), _p2pQueue
					.getMaxActiveJobs(), _p2pQueue.getQueueSize());

        }

        return info;
    }

    // //////////////////////////////////////////////////////////
    //
    // Check cost
    //
    public String hh_set_breakeven = "<breakEven> # free and recovable space";

    public String ac_set_breakeven_$_0_1(Args args)
    {
        if (args.argc() > 0)
            _breakEven = Double.parseDouble(args.argv(0));
        return "BreakEven = " + _breakEven;
    }

    private synchronized void removeFiles(PoolRemoveFilesMessage poolMessage)
    {
        String[] fileList = poolMessage.getFiles();
        int counter = 0;
        for (int i = 0; i < fileList.length; i++) {
            try {
                PnfsId pnfsId = new PnfsId(fileList[i]);
                if (!_cleanPreciousFiles && _lfsMode == LFS_NONE
                    && _repository.getState(pnfsId) == EntryState.PRECIOUS) {
                    counter++;
                    esay("removeFiles : File " + fileList[i] + " kept. (precious)");
                } else {
                    _repository.setState(pnfsId, EntryState.REMOVED);
                    fileList[i] = null;
                }
            } catch (IllegalTransitionException e) {
                esay("removeFiles : File " + fileList[i] + " delete CE : "
                     + e.getMessage());
                counter++;
            } catch (IllegalArgumentException e) {
                esay("removeFiles : invalid syntax in remove filespec ("
                     + fileList[i] + ")");
                counter++;
            }
        }
        if (counter > 0) {
            String[] replyList = new String[counter];
            for (int i = 0, j = 0; i < fileList.length; i++)
                if (fileList[i] != null)
                    replyList[j++] = fileList[i];
            poolMessage.setFailed(1, replyList);
        } else {
            poolMessage.setSucceeded();
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
            _nucleus.newThread(this, "HybridInventory").start();
        }

        public void run()
        {
            _hybridCurrent = 0;

            long startTime, stopTime;
            say("HybridInventory started. _activate="+_activate);
            startTime = System.currentTimeMillis();

            for (PnfsId pnfsid : _repository) {
                if (Thread.interrupted())
                    break;
                switch (_repository.getState(pnfsid)) {
                case PRECIOUS:
                case CACHED:
                case BROKEN:
                    _hybridCurrent++;
                    if (_activate)
                        _pnfs.addCacheLocation(pnfsid.toString());
                    else
                        _pnfs.clearCacheLocation(pnfsid.toString());
                    break;
                default:
                    break;
                }
            }
            stopTime = System.currentTimeMillis();
            synchronized (_hybridInventoryLock) {
                _hybridInventoryActive = false;
            }

            say("HybridInventory finished. Number of pnfsids " +
                ((_activate) ? "" : "un" )
                +"registered="
                +_hybridCurrent +" in " + (stopTime-startTime) +" msec");
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

    // //////////////////////////////////////////////////////////////////////////////////
    //
    // the interpreter set/get functions
    //
    public String hh_pf = "<pnfsId>";

    public String ac_pf_$_1(Args args) throws Exception
    {
        PnfsId pnfsId = new PnfsId(args.argv(0));
        PnfsMapPathMessage info = new PnfsMapPathMessage(pnfsId);
        CellPath path = new CellPath("PnfsManager");
        say("Sending : " + info);
        CellMessage m = sendAndWait(new CellMessage(path, info), 10000);
        say("Reply arrived : " + m);
        if (m == null)
            throw new Exception("No reply from PnfsManager");

        info = ((PnfsMapPathMessage) m.getMessageObject());
        if (info.getReturnCode() != 0) {
            Object o = info.getErrorObject();
            if (o instanceof Exception)
                throw (Exception) o;
            else
                throw new Exception(o.toString());
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

    public String hh_pool_lfs = "none|precious # FOR DEBUG ONLY";
    public String ac_pool_lfs_$_1(Args args) throws CommandSyntaxException
    {
        setLFSMode(args.argv(0));
        return "";
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

    public String hh_set_p2p = "integrated|separated";
    public String ac_set_p2p_$_1(Args args) throws CommandSyntaxException
    {
        String mode = args.argv(0);
        if (mode.equals("integrated")) {
            _p2pMode = P2P_INTEGRATED;
        } else if (mode.equals("separated")) {
            _p2pMode = P2P_SEPARATED;
        } else {
            throw new CommandSyntaxException("Not Found : ",
                                             "Usage : set p2p ntegrated|separated");
        }
        return "";
    }

    public String hh_pool_disablemode = "strict|fuzzy # DEPRICATED, use pool disable [options]";
    public String ac_pool_disablemode_$_1(Args args)
    {
        return "# DEPRICATED, use pool disable [options]";
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

        int rc = args.argc() > 0 ? Integer.parseInt(args.argv(0)) : 1;
        String rm = args.argc() > 1 ? args.argv(1) : "Operator intervention";

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

    public String hh_set_sticky = "allowed|denied";
    public String ac_set_sticky_$_0_1(Args args)
    {
        if (args.argc() > 0) {
            String mode = args.argv(0);
            if (mode.equals("allowed")) {
                setStickyAllowed(true);
            } else if (mode.equals("denied")) {
                setStickyAllowed(false);
            } else
                throw new IllegalArgumentException("set sticky allowed|denied");
        }
        return "Sticky Bit " + (_allowSticky ? "allowed" : "denied");
    }

    public String hh_set_max_diskspace = "<space>[<unit>] # unit = k|m|g";
    public String ac_set_max_diskspace_$_1(Args args)
    {
        long maxDisk = UnitInteger.parseUnitLong(args.argv(0));
        _repository.setSize(maxDisk);
        say("set maximum diskspace =" + UnitInteger.toUnitString(maxDisk));
        return "";
    }

    public String hh_set_cleaning_interval = "<interval/sec>";
    public String ac_set_cleaning_interval_$_1(Args args)
    {
        _cleaningInterval = Integer.parseInt(args.argv(0));
        say("_cleaningInterval=" + _cleaningInterval);
        return "";
    }

    public String hh_set_flushing_interval = "DEPRECATED (use flush set interval <time/sec>)";
    public String ac_set_flushing_interval_$_1(Args args)
    {
        return "DEPRECATED (use flush set interval <time/sec>)";
    }

    public String hh_flush_class = "<hsm> <storageClass> [-count=<count>]";
    public String ac_flush_class_$_2(Args args)
    {
        String tmp = args.getOpt("count");
        int count = (tmp == null) || (tmp.equals("")) ? 0 : Integer
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

    /*
     * public String hh_mover_set_attr = "default|*|<moverId> <attrKey>
     * <attrValue>" ; public String ac_mover_set_attr_$_3( Args args )throws
     * Exception { String moverId = args.argv(0) ; String key = args.argv(1) ;
     * String value = args.argv(2) ;
     *
     * if( moverId.equals("default") ){ _moverAttributes.put( key , value ) ;
     * return "" ; }else if( moverId.equals("*") ){ StringBuffer sb = new
     * StringBuffer() ; synchronized( _ioMovers ){ Iterator i =
     * _ioMovers.values().iterator() ; while( i.hasNext() ){ RepositoryIoHandler
     * h = (RepositoryIoHandler)i.next() ; try{ h.setAttribute( key , value ) ;
     * sb.append( ""+h.getId()+" OK\n" ) ; }catch(Exception ee ){ sb.append(
     * ""+h.getId()+" ERROR : "+ee.getMessage()+"\n" ) ; } } } return
     * sb.toString() ; }else{ Integer id = new Integer(moverId) ; synchronized(
     * _ioMovers ){ RepositoryIoHandler h =
     * (RepositoryIoHandler)_ioMovers.get(id) ; h.setAttribute( key , value ) ; }
     * return "" ; } }
     */
    public String hh_mover_set_max_active = "<maxActiveIoMovers> -queue=<queueName>";
    public String hh_mover_queue_ls = "";
    public String hh_mover_ls = "[-binary [jobId] ]";
    public String hh_mover_remove = "<jobId>";
    public String hh_mover_kill = "<jobId> [-force]" ;
    public String hh_p2p_set_max_active = "<maxActiveIoMovers>";
    public String hh_p2p_ls = "[-binary [jobId] ]";
    public String hh_p2p_remove = "<jobId>";
    public String hh_p2p_kill = "<jobId> [-force]" ;

    public String ac_mover_set_max_active_$_1(Args args)
        throws NumberFormatException, IllegalArgumentException
    {
        String queueName = args.getOpt("queue");

        IoQueueManager ioManager = (IoQueueManager) _ioQueue;

        if (queueName == null)
            return mover_set_max_active(ioManager.getDefaultScheduler(), args);

        JobScheduler js = ioManager.getSchedulerByName(queueName);

        if (js == null)
            return "Not found : " + queueName;

        return mover_set_max_active(js, args);

    }

    public String ac_p2p_set_max_active_$_1(Args args)
        throws NumberFormatException, IllegalArgumentException
    {
        return mover_set_max_active(_p2pQueue, args);
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
        IoQueueManager manager = (IoQueueManager) _ioQueue;

        if (args.getOpt("l") != null) {
            for (JobScheduler js : manager.getSchedulers()) {
                sb.append(js.getSchedulerName())
                    .append(" ").append(js.getActiveJobs())
                    .append(" ").append(js.getMaxActiveJobs())
                    .append(" ").append(js.getQueueSize()).append("\n");
            }
        } else {
            for (JobScheduler js : manager.getSchedulers()) {
                sb.append(js.getSchedulerName()).append("\n");
            }
        }
        return sb.toString();
    }

    public Object ac_mover_ls_$_0_1(Args args)
        throws NoSuchElementException
    {
        String queueName = args.getOpt("queue");
        if (queueName == null)
            return mover_ls(_ioQueue, args);

        if (queueName.length() == 0) {
            IoQueueManager manager = (IoQueueManager) _ioQueue;
            StringBuilder sb = new StringBuilder();
            for (JobScheduler js : manager.getSchedulers()) {
                sb.append("[").append(js.getSchedulerName()).append("]\n");
                sb.append(mover_ls(js, args).toString());
            }
            return sb.toString();
        }
        IoQueueManager manager = (IoQueueManager) _ioQueue;

        JobScheduler js = manager.getSchedulerByName(queueName);

        if (js == null)
            throw new NoSuchElementException(queueName);

        return mover_ls(js, args);

    }

    public Object ac_p2p_ls_$_0_1(Args args)
    {
        return mover_ls(_p2pQueue, args);
    }

    /*
     * private Object mover_ls( IoQueueManager queueManager , int id , boolean
     * binary ){ Iterator queues = queueManager.scheduler() ;
     *
     * if( binary ){ if( id > 0 ){ ArrayList list = new ArrayList() ; while(
     * queues.hasNext() ){ list.addAll(
     * ((JobScheduler)queues.next()).getJobInfos() ) ; } return list.toArray(
     * new IoJobInfo[0] ) ; }else{ return queueManager.getJobInfo(id) ; } }else{
     * StringBuffer sb = new StringBuffer() ; while( queues.hasNext() ){
     * JobScheduler js = (JobScheduler)queues.next() ; js.printJobQueue(sb); }
     * return sb.toString() ; } }
     */
    private Object mover_ls(JobScheduler js, Args args)
        throws NumberFormatException
    {
        boolean binary = args.getOpt("binary") != null;
        try {
            if (binary) {
                if (args.argc() > 0) {
                    return js.getJobInfo(Integer.parseInt(args.argv(0)));
                } else {
                    List<JobInfo> list = js.getJobInfos();
                    return list.toArray(new IoJobInfo[list.size()]);
                }
            } else {
                return js.printJobQueue(null).toString();
            }
        } catch (NumberFormatException ee) {
            esay(ee);
            throw ee;
        }
    }

    public String ac_mover_remove_$_1(Args args)
        throws NoSuchElementException, NumberFormatException
    {
        return mover_remove(_ioQueue, args);
    }

    public String ac_p2p_remove_$_1(Args args)
        throws NoSuchElementException, NumberFormatException
    {
        return mover_remove(_p2pQueue, args);
    }

    private String mover_remove(JobScheduler js, Args args)
        throws NoSuchElementException, NumberFormatException
    {
        int id = Integer.parseInt(args.argv(0));
        js.remove(id);
        return "Removed";
    }

    public String ac_mover_kill_$_1(Args args)
        throws NoSuchElementException, NumberFormatException
    {
        return mover_kill(_ioQueue, args);
    }

    public String ac_p2p_kill_$_1(Args args)
        throws NoSuchElementException, NumberFormatException
    {
        return mover_kill(_p2pQueue, args);
    }

    private void mover_kill(int id, boolean force)
        throws NoSuchElementException
    {
        mover_kill(_ioQueue, id, force);
    }

    private String mover_kill(JobScheduler js, Args args)
        throws NoSuchElementException, NumberFormatException
    {
        int id = Integer.parseInt(args.argv(0));
        mover_kill(js, id, args.getOpt("force") != null);
        return "Kill initialized";
    }

    private void mover_kill(JobScheduler js, int id, boolean force)
        throws NoSuchElementException
    {

        js.kill(id, force);
    }

    // ////////////////////////////////////////////////////
    //
    // queue stuff
    //
    public String hh_set_heartbeat = "<heartbeatInterval/sec>";

    public String ac_set_heartbeat_$_0_1(Args args)
        throws NumberFormatException
    {
        if (args.argc() > 0) {
            _pingThread.setHeartbeat(Integer.parseInt(args.argv(0)));
        }
        return "Heartbeat at " + (_pingThread.getHeartbeat());
    }

    public String fh_update = "  update [-force] [-perm] !!! DEPRECATED  \n"
        + "     sends relevant data to the PoolManager if this information has been\n"
        + "     changed recently.\n\n"
        + "    -force : forces the cell to send the information regardless whether it\n"
        + "             changed or not.\n"
        + "    -perm  : writes the current parameter setup back to the setupFile\n";
    public String hh_update = "[-force] [-perm] !!! DEPRECATED";

    public String ac_update(Args args)
        throws IOException
    {
        boolean forced = args.getOpt("force") != null;
        _pingThread.sendPoolManagerMessage(forced);
        if (args.getOpt("perm") != null) {
            dumpSetup();
        }
        return "";
    }

    public String hh_save = "[-sc=<setupController>|none] # saves setup to disk or SC";

    public String ac_save(Args args)
        throws IOException, IllegalArgumentException, NoRouteToCellException
    {
        String setupManager = args.getOpt("sc");

        if (_setupManager == null) {

            if ((setupManager != null) && setupManager.equals(""))
                throw new IllegalArgumentException("setupManager needs to be specified");

        } else {

            if ((setupManager == null) || setupManager.equals("")) {

                setupManager = _setupManager;
            }
        }
        if ((setupManager != null) && !setupManager.equals("none")) {
            try {
                StringWriter sw = new StringWriter();
                PrintWriter pw = new PrintWriter(sw);
                dumpSetup(pw);
                SetupInfoMessage info = new SetupInfoMessage("put", this
                                                             .getCellName(), "pool", sw.toString());

                sendMessage(new CellMessage(new CellPath(setupManager), info));
            } catch (NotSerializableException e) {
                throw new RuntimeException("Bug detected: Unserializable vehicle", e);
            } catch (NoRouteToCellException e) {
                esay("Problem sending setup to >" + setupManager + "< : " + e.getMessage());
                throw e;
            }
        }
        dumpSetup();
        return "";
    }
}
