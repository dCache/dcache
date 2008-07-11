// $Id: MultiProtocolPoolV3.java,v 1.16 2007-10-26 11:17:06 behrmann Exp $

package diskCacheV111.pools;

import java.io.BufferedReader;
import java.io.EOFException;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.NotSerializableException;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.io.StringWriter;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.InetAddress;
import java.net.UnknownHostException;
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
import org.dcache.pool.repository.v4.CacheRepositoryV4;

import diskCacheV111.movers.ChecksumMover;
import diskCacheV111.movers.MoverProtocol;
import diskCacheV111.repository.CacheRepository;
import diskCacheV111.repository.CacheRepositoryEntry;
import diskCacheV111.repository.PreallocationSpaceMonitor;
import diskCacheV111.repository.ReadOnlySpaceMonitor;
import diskCacheV111.repository.RepositoryCookie;
import diskCacheV111.repository.RepositoryInterpreter;
import diskCacheV111.repository.SpaceMonitor;
import diskCacheV111.repository.SpaceMonitorWatch;
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
import diskCacheV111.util.event.CacheRepositoryEvent;
import diskCacheV111.util.event.CacheRepositoryListener;
import diskCacheV111.vehicles.DCapProtocolInfo;
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
import diskCacheV111.vehicles.PoolFreeSpaceReservationMessage;
import diskCacheV111.vehicles.PoolIoFileMessage;
import diskCacheV111.vehicles.PoolManagerPoolUpMessage;
import diskCacheV111.vehicles.PoolMgrReplicateFileMsg;
import diskCacheV111.vehicles.PoolModifyModeMessage;
import diskCacheV111.vehicles.PoolModifyPersistencyMessage;
import diskCacheV111.vehicles.PoolMoverKillMessage;
import diskCacheV111.vehicles.PoolQueryRepositoryMsg;
import diskCacheV111.vehicles.PoolQuerySpaceReservationMessage;
import diskCacheV111.vehicles.PoolRemoveFilesFromHSMMessage;
import diskCacheV111.vehicles.PoolRemoveFilesMessage;
import diskCacheV111.vehicles.PoolReserveSpaceMessage;
import diskCacheV111.vehicles.PoolSetStickyMessage;
import diskCacheV111.vehicles.PoolSpaceReservationMessage;
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

public class MultiProtocolPoolV3 extends CellAdapter implements Logable {

    private final static Logger _logPoolMonitor = Logger.getLogger("logger.org.dcache.poolmonitor." + MultiProtocolPoolV3.class.getName());

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
    private boolean _flushZeroSizeFiles = false;
    private boolean _suppressHsmLoad = false;
    private boolean _cleanPreciousFiles = false;
    private String     _poolStatusMessage = "OK";
    private int        _poolStatusCode  = 0;

    private final PnfsHandler _pnfs;
    private final CacheRepository _repository;
    private final StorageClassContainer _storageQueue;
    private final SpaceSweeper _sweeper;

    private String _setupManager = null;
    private String _pnfsManagerName = "PnfsManager";
    private String _poolManagerName = "PoolManager";
    private String _poolupDestination = "PoolManager";
    private String _hsmPoolManagerName = "HsmPoolManager";
    private String _sweeperClass = "diskCacheV111.pools.SpaceSweeper0";
    private static final String _dummySweeperClass = "diskCacheV111.pools.DummySpaceSweeper";

    private int _version = 4;
    private final CellPath _billingCell ;
    private final Map<String, String> _tags = new HashMap<String, String>();
    private final String _baseDir;
    private File _base;
    private File _setup;

    private final PoolManagerPingThread _pingThread ;
    private final HsmFlushController _flushingThread;
    private final JobScheduler _ioQueue ;
    private final JobScheduler _p2pQueue;
    private final JobTimeoutManager _timeoutManager;
    private final HsmSet _hsmSet = new HsmSet();
    private final HsmStorageHandler2 _storageHandler;
    private Logable _logClass = this;
    private boolean _crashEnabled = false;
    private String _crashType = "exception";
    private boolean _isPermanent = false;
    private boolean _allowSticky = false;
    private boolean _isHsmPool = false;
    private boolean _blockOnNoSpace = true;
    private boolean _checkRepository = true;
    private boolean _waitForRepositoryOk = false;
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

    private ChecksumModuleV1 _checksumModule = null;
    private ReplicationHandler _replicationHandler = null;

    //
    // arguments :
    // MPP2 <poolBasePath> no default
    // [-permanent] : default : dynamic
    // [-version=<version>] : default : 4
    // [-sticky=allowed|denied] ; default : denied
    // [-recover-space[=no]] : default : no
    // [-recover-control[=no]] : default : no
    // [-lfs=precious]
    // [-p2p=<p2pFileMode>] : default : cached
    // [-poolManager=<name>] : default : PoolManager
    // [-poolupDestination=<name>] : default : PoolManager
    // [-billing=<name>] : default : billing
    // [-setupManager=<name>] : default : none
    // [-dupRequest=none|ignore|refresh]: default : ignore
    // [-flushZeroSizeFiles=yes|no] : default : no
    // [-blockOnNoSpace=yes|no|auto] : default : auto
    // [-allowCleaningPreciousFiles] : default : false
    // [-checkRepository] : default : true
    // [-waitForRepositoryOk] : default : false
    // [-replicateOnArrival[=[Manager],[host],[mode]]] : default :
    // PoolManager,thisHost,keep
    //
    public MultiProtocolPoolV3(String poolName, String args)
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

            if (argc < 1) {
                throw new IllegalArgumentException("no base dir specified");
            }

            _baseDir = _args.argv(0);

            String versionString = _args.getOpt("version");
            if (versionString != null) {
                try {
                    _version = Integer.parseInt(versionString);
                } catch (NumberFormatException e) { /* bad string, ignored */}
            }

            _isPermanent = _args.getOpt("permanent") != null;

            String stickyString = _args.getOpt("sticky");
            if (stickyString != null)
                _allowSticky = stickyString.equals("allowed");
            say("Sticky files : " + (_allowSticky ? "allowed" : "denied"));

            String sweeperClass = _args.getOpt("sweeper");
            if (sweeperClass != null)
                _sweeperClass = sweeperClass;
            if (_isPermanent)
                _sweeperClass = _dummySweeperClass;
            say("Using sweeper : " + _sweeperClass);

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

            recover = _args.getOpt("checkRepository");
            if (recover != null) {
                if (recover.equals("yes") || recover.equals("true")) {
                    _checkRepository = true;
                } else if (recover.equals("no") || recover.equals("false")) {
                    _checkRepository = false;
                }
            }
            say("CheckRepository : " + _checkRepository);

            recover = _args.getOpt("waitForRepositoryReady");
            if (recover != null) {
                if (recover.equals("yes") || recover.equals("true")) {
                    _waitForRepositoryOk = true;
                } else if (recover.equals("no") || recover.equals("false")) {
                    _waitForRepositoryOk = false;
                }
            }
            say("waitForRepositoryReady : " + _waitForRepositoryOk);

            recover = _args.getOpt("recover-anyway");
            if ((recover != null) && (!recover.equals("no"))) {
                _recoveryFlags |= CacheRepository.ALLOW_RECOVER_ANYWAY;
                say("Enabled : recover-anyway");
            }

            recover = _args.getOpt("replicateOnArrival");
            if (recover == null) {
                _replicationHandler = new ReplicationHandler();
            } else {
                _replicationHandler = new ReplicationHandler(
                                                             recover.equals("") ? "on" : recover);
            }
            say("ReplicationHandler : " + _replicationHandler);

            /**
             * If cleaner sends its remove list, do we allow to remove precious
             * files for HSM connected pools ?
             */
            recover = _args.getOpt("allowCleaningPreciousFiles");

            _cleanPreciousFiles = (recover != null)
                && (recover.equalsIgnoreCase("yes") || recover.equalsIgnoreCase("true"));

            say("allowCleaningPreciousFiles : " + _cleanPreciousFiles);

            String lfsModeString = _args.getOpt("lfs");
            lfsModeString = lfsModeString == null ? _args
                .getOpt("largeFileStore") : lfsModeString;
            if (lfsModeString != null) {
                if (lfsModeString.equals("precious")
                    || lfsModeString.equals("hsm")
                    || lfsModeString.equals("")) {

                    _lfsMode = LFS_PRECIOUS;
                    _isHsmPool = lfsModeString.equals("hsm");

                } else if (lfsModeString.equals("volatile")
                           || lfsModeString.equals("transient")) {

                    _lfsMode = LFS_VOLATILE;
                } else {
                    throw new IllegalArgumentException(
                                                       "lfs[=volatile|precious|transient]");
                }
            } else {
                _lfsMode = LFS_NONE;
            }
            say("LargeFileStore Mode : "
                + (_lfsMode == LFS_NONE ? "None"
                   : _lfsMode == LFS_VOLATILE ? "Volatile"
                   : "Precious"));
            say("isHsmPool = " + _isHsmPool);

            lfsModeString = _args.getOpt("p2p");
            lfsModeString = lfsModeString == null ? _args.getOpt("p2pFileMode")
                : lfsModeString;
            if (lfsModeString != null) {
                if (lfsModeString.equals("precious")) {

                    _p2pFileMode = P2P_PRECIOUS;

                } else if (lfsModeString.equals("cached")) {

                    _p2pFileMode = P2P_CACHED;

                } else {
                    throw new IllegalArgumentException("p2p=precious|cached");
                }
            } else {
                _p2pFileMode = P2P_CACHED;
            }
            say("Pool2Pool File Mode : "
                + (_p2pFileMode == P2P_CACHED ? "cached" : "precious"));

            String dupString = _args.getOpt("dupRequest");
            if ((dupString == null) || dupString.equals("none")) {
                _dupRequest = DUP_REQ_NONE;
            } else if (dupString.equals("ignore")) {
                _dupRequest = DUP_REQ_IGNORE;
            } else if (dupString.equals("refresh")) {
                _dupRequest = DUP_REQ_REFRESH;
            } else {
                esay("Illegal 'dupRequest' value : " + dupString
                     + " (using 'none')");
            }
            say("DuplicateRequest Mode : "
                + (_dupRequest == DUP_REQ_NONE ? "None"
                   : _dupRequest == DUP_REQ_IGNORE ? "Ignore"
                   : "Refresh"));

            String tmp = _args.getOpt("poolManager");
            _poolManagerName = tmp == null ? (_isHsmPool ? _hsmPoolManagerName
                                              : _poolManagerName) : tmp;

            say("PoolManagerName : " + _poolManagerName);

            tmp = _args.getOpt("poolupDestination");
            if (tmp != null)
                _poolupDestination = tmp;
            else
                _poolupDestination = _poolManagerName;
            say("Pool up destination: " + _poolupDestination);

            tmp = _args.getOpt("billing");
            if (tmp != null) {
                _billingCell = new CellPath(tmp);
            }else{
                _billingCell = new CellPath("billing");
            }

            say("Billing Cell : " + _billingCell);

            tmp = _args.getOpt("flushZeroSizeFiles");
            if (tmp != null) {
                if (tmp.equals("yes") || tmp.equals("true")) {
                    _flushZeroSizeFiles = true;
                } else if (tmp.equals("no") || tmp.equals("false")) {
                    _flushZeroSizeFiles = false;
                }
            }
            say("flushZeroSizeFiles = " + _flushZeroSizeFiles);

            _setupManager = _args.getOpt("setupManager");
            say("SetupManager set to "
                + (_setupManager == null ? "none" : _setupManager));

            //
            // block 'reserve space' only if we have a chance to
            // make space available. So not for LFS_PRECIOUS.
            //
            _blockOnNoSpace = _lfsMode != LFS_PRECIOUS;
            //
            // and allow overwriting
            //
            tmp = _args.getOpt("blockOnNoSpace");
            if ((tmp != null) && !tmp.equals("auto"))
                _blockOnNoSpace = tmp.equals("yes");
            say("BlockOnNoSpace : " + _blockOnNoSpace);
            //
            // get additional tags
            //
            {
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
            }
            //
            // repository and ping thread must exist BEFORE the
            // setup file is scanned. PingThread will be start
            // after all the setup is done.
            //
            _pingThread = new PoolManagerPingThread();

            disablePool(PoolV2Mode.DISABLED_STRICT, 1, "Initializing");

            say("Checking base directory ( reading setup) " + _baseDir);
            _base = new File(_baseDir);
            _setup = new File(_base, "setup");

            while (!_setup.canRead()) {
                disablePool(PoolV2Mode.DISABLED_STRICT,1,"Initializing : Repository seems not to be ready - setup file does not exist or not readble");
                esay("Can't read setup file: exists? " +
                     Boolean.toString(_setup.exists()) + " can read? " + Boolean.toString(_setup.canRead()) );
                try {
                    Thread.sleep(30000);
                } catch (InterruptedException ie) {
                    esay("Waiting for repository was interrupted");
                    throw new InterruptedException("Waiting for repository was interrupted");
                }
            }
            say("Base dir ok");

            _storageQueue = new StorageClassContainer(poolName);
            _repository = new CacheRepositoryV4(_base, _args);
            _repository.setLogable(this);

            _pnfs = new PnfsHandler(this, new CellPath(_pnfsManagerName), _poolName);

            _storageHandler = new HsmStorageHandler2(this, _repository, _hsmSet, _pnfs);

            _storageHandler.setStickyAllowed(_allowSticky);

            _sweeper = getSweeperHandler();

            //
            // transfer queue management
            //
            _timeoutManager = new JobTimeoutManager(this);
            //
            // _ioQueue = new SimpleJobScheduler( getNucleus().getThreadGroup()
            // , "IO" ) ;
            // _ioQueue.setSchedulerId( "regular" , 2 ) ;
            // _timeoutManager.addScheduler( "io" , _ioQueue ) ;
            //
            // _ioQueueManager = new IoQueueManager(
            // getNucleus().getThreadGroup() ,
            // _args.getOpt("io-queues" ) ) ;
            // _ioQueue = _ioQueueManager.getDefaultScheduler() ;

            _ioQueue = new IoQueueManager(getNucleus(), _args.getOpt("io-queues"));

            _p2pQueue = new SimpleJobScheduler(getNucleus(), "P2P");

            _flushingThread = new HsmFlushController(this, _storageQueue,
                                                     _storageHandler);

            _checksumModule = new ChecksumModuleV1(this, _repository, _pnfs);

            _p2pClient = new P2PClient(this, _repository, _checksumModule);

            _timeoutManager.addScheduler("p2p", _p2pQueue);
            _timeoutManager.start();
            addCommandListener(_timeoutManager);

            //
            // add the command listeners before we execute the setupFile.
            //

            addCommandListener(_sweeper);
            addCommandListener(_hsmSet);
            addCommandListener(new RepositoryInterpreter(_repository));
            addCommandListener(_storageQueue);
            addCommandListener(new HsmStorageInterpreter(this, _storageHandler));
            addCommandListener(_flushingThread);
            addCommandListener(_p2pClient);
            addCommandListener(_checksumModule);

            execFile(_setup);

        } catch (Exception e) {
            say("Exception occurred on startup: " + e);
            start();
            kill();
            throw e;
        }

        _pingThread.start();

        _repository.addCacheRepositoryListener(new RepositoryLoader());

        start();

        Object weAreDone = new Object();
        synchronized (weAreDone) {
            _nucleus.newThread(new InventoryScanner(weAreDone), "inventory")
                .start();
            try {
                weAreDone.wait();
            } catch (InterruptedException ee) {
                kill();
                throw ee;
            }
            _logClass.elog("Starting Flushing Thread");
            _flushingThread.start();
        }
        esay("Constructor done (still waiting for 'inventory')");
    }

    @Override
    public CellVersion getCellVersion() {
        return new CellVersion(diskCacheV111.util.Version.getVersion(),
                               "$Revision$");
    }

    private class IoQueueManager implements JobScheduler {
        private ArrayList<JobScheduler> _list = new ArrayList<JobScheduler>();
        private HashMap<String, JobScheduler> _hash = new HashMap<String, JobScheduler>();
        private boolean _isConfigured = false;

        private IoQueueManager(ThreadFactory factory, String ioQueueList) {
            _isConfigured = (ioQueueList != null) && (ioQueueList.length() > 0);
            if( !_isConfigured ) {
                ioQueueList = "regular";
            }

            StringTokenizer st = new StringTokenizer(ioQueueList, ",");
            while (st.hasMoreTokens()) {
                boolean fifo = true;
                String queueName = st.nextToken();
                if (queueName.startsWith("-")) {
                    queueName = queueName.substring(1);
                    fifo = false;
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

        private boolean isConfigured() {
            return _isConfigured;
        }

        private JobScheduler getDefaultScheduler() {
            return _list.get(0);
        }

        private Iterator<JobScheduler> scheduler() {
            return new ArrayList<JobScheduler>(_list).iterator();
        }

        private JobScheduler getSchedulerByName(String queueName) {
            return _hash.get(queueName);
        }

        private JobScheduler getSchedulerById(int id) {
            int pos = id % 10;
            if (pos >= _list.size()) {
                throw new IllegalArgumentException(
                                                   "Invalid id (doesn't below to any known scheduler)");
            }
            return  _list.get(pos);
        }

        public JobInfo getJobInfo(int id) {
            return getSchedulerById(id).getJobInfo(id);
        }

        public int add(String queueName, Runnable runnable, int priority)
            throws InvocationTargetException {

            JobScheduler js = queueName == null ? null : (JobScheduler) _hash
                .get(queueName);

            return js == null ? add(runnable, priority) : js.add(runnable,
                                                                 priority);

        }

        public int add(Runnable runnable) throws InvocationTargetException {
            return getDefaultScheduler().add(runnable);
        }

        public int add(Runnable runnable, int priority)
            throws InvocationTargetException {
            return getDefaultScheduler().add(runnable, priority);
        }

        public void kill(int jobId, boolean force)
            throws NoSuchElementException
        {
            getSchedulerById(jobId).kill(jobId, force);
        }

        public void remove(int jobId) throws NoSuchElementException {
            getSchedulerById(jobId).remove(jobId);
        }

        public StringBuffer printJobQueue(StringBuffer sbin) {
            StringBuffer sb = sbin == null ? new StringBuffer() : sbin;
            for (Iterator<JobScheduler> it = scheduler(); it.hasNext();) {
                (it.next()).printJobQueue(sb);
            }
            return sb;
        }

        public int getMaxActiveJobs() {
            int sum = 0;
            for (Iterator<JobScheduler> it = scheduler(); it.hasNext();) {
                sum += (it.next()).getMaxActiveJobs();
            }
            return sum;
        }

        public int getActiveJobs() {
            int sum = 0;
            for (Iterator<JobScheduler> it = scheduler(); it.hasNext();) {
                sum += (it.next()).getActiveJobs();
            }
            return sum;
        }

        public int getQueueSize() {
            int sum = 0;
            for (Iterator<JobScheduler> it = scheduler(); it.hasNext();) {
                sum += (it.next()).getQueueSize();
            }
            return sum;
        }

        public void setMaxActiveJobs(int maxJobs) {
        }

        public List<JobInfo>  getJobInfos() {
            List<JobInfo> list = new ArrayList<JobInfo> ();
            for (Iterator<JobScheduler> it = scheduler(); it.hasNext();) {
                list.addAll((it.next()).getJobInfos());
            }
            return list;
        }

        public void setSchedulerId(String name, int id) {
            return;
        }

        public String getSchedulerName() {
            return "Manager";
        }

        public int getSchedulerId() {
            return -1;
        }

        public void dumpSetup(PrintWriter pw) {
            for (Iterator<JobScheduler> it = scheduler(); it.hasNext();) {
                JobScheduler js = it.next();
                pw.println("mover set max active -queue="
                           + js.getSchedulerName() + " " + js.getMaxActiveJobs());
            }
        }
    }

    private class InventoryScanner implements Runnable {
        private Object _notifyMe = null;

        private InventoryScanner(Object notifyMe) {
            _notifyMe = notifyMe;
        }

        public void run() {

            _logClass.log("Running Repository (Cell is locked)");
            _logClass.log("Repository seems to be ok");
            try {

                _repository.runInventory(_logClass, _pnfs, _recoveryFlags);
                enablePool();
                _logClass.elog("Pool enabled " + _poolName);

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
    public void cleanUp() {
        disablePool(PoolV2Mode.DISABLED_DEAD | PoolV2Mode.DISABLED_STRICT,
                    666, "Shutdown");
    }

    // public void setMaxActiveIOs( int ios ){ _ioQueue.setMaxActiveJobs( ios) ;
    // }
    //
    // The sweeper class loader
    //
    //
    private SpaceSweeper getSweeperHandler()
        throws InstantiationException,
               ClassNotFoundException,
               IllegalAccessException,
               InvocationTargetException,
               NoSuchMethodException
    {
        Class<?>[] argClass = { diskCacheV111.util.PnfsHandler.class,
				diskCacheV111.repository.CacheRepository.class };
        Class<?> sweeperClass = Class.forName(_sweeperClass);
        Constructor<?> sweeperCon = sweeperClass.getConstructor(argClass);
        Object[] args = { _pnfs, _repository };

        return (SpaceSweeper) sweeperCon.newInstance(args);
    }

    private void setDummyStorageInfo(CacheRepositoryEntry entry) {
        try {

            StorageInfo storageInfo = entry.getStorageInfo();
            storageInfo.setBitfileId("*");

            _pnfs.setStorageInfoByPnfsId(entry.getPnfsId(), storageInfo, 0 // write
                                         // (don't overwrite)
                                         );
        } catch (CacheException e) {
            //
            // for now we just ignore this exception (its dummy only)
            //
            esay("Problem in setDummyStorageInfo of : " + entry + " : " + e);
        }
    }

    //
    // interface between the repository and the StorageQueueContainer
    // (we do the pnfs stuff here as well)
    //
    private class RepositoryLoader implements CacheRepositoryListener {
        public void actionPerformed(CacheEvent event) {
            // forced by interface
        }

        public void sticky(CacheRepositoryEvent event) {
            say("RepositoryLoader : sticky : " + event);
        }

        public void precious(CacheRepositoryEvent event) {
            say("RepositoryLoader : precious : " + event);
            CacheRepositoryEntry entry = event.getRepositoryEntry();
            long size = 0L;
            try {
                size = entry.getSize();
            } catch (CacheException ee) {
                esay("RepositoryLoader : can't get filesize : " + ee);
                //
                // if we can't get the size, so to be on the save side.
                //
                size = 1;
            }

            try {
                if ((size == 0) && !_flushZeroSizeFiles) {

                    say("RepositoryLoader : 0 size file set cached (no HSM flush)");
                    if ((_lfsMode == LFS_NONE) || (_lfsMode == LFS_VOLATILE)) {

                        entry.setCached();
                        setDummyStorageInfo(entry);
                    }
                } else {

                    if (_lfsMode == LFS_NONE)
                        _storageQueue.addCacheEntry(entry);
                    if (_lfsMode == LFS_VOLATILE)
                        entry.setCached();

                }
            } catch (CacheException ce) {
                esay("RepositoryLoader : Cache Exception in addCacheEntry ["
                     + entry.getPnfsId() + "] : " + ce.getMessage());
            }
        }

        public void cached(CacheRepositoryEvent event) {
            say("RepositoryLoader : cached : " + event);
            //
            // the remove will fail if the file
            // is coming FROM the store.
            //
            CacheRepositoryEntry entry = event.getRepositoryEntry();
            try {
                CacheRepositoryEntry e = _storageQueue.removeCacheEntry(entry
                                                                        .getPnfsId());
                say("RepositoryLoader : removing " + entry.getPnfsId()
                    + (e == null ? " failed" : " ok"));
            } catch (CacheException ce) {
                esay("RepositoryLoader : remove " + entry.getPnfsId()
                     + " failed : " + ce);
            }
        }

        public void available(CacheRepositoryEvent event) {
            say("RepositoryLoader : available : " + event);
            _pnfs.addCacheLocation(event.getRepositoryEntry().getPnfsId());
            event.getRepositoryEntry().lock(false);
        }

        public void created(CacheRepositoryEvent event) {
            say("RepositoryLoader : created : " + event);
        }

        public void touched(CacheRepositoryEvent event) {
            say("RepositoryLoader : touched : " + event);
        }

        public void removed(CacheRepositoryEvent event) {
            say("RepositoryLoader : removed : " + event);
            CacheRepositoryEntry entry = event.getRepositoryEntry();
            try {
                _storageQueue.removeCacheEntry(entry.getPnfsId());
            } catch (CacheException ce) {
                esay("RepositoryLoader : PANIC : " + entry.getPnfsId()
                     + " removing from storage queue failed : " + ce);
            }
            //
            // although we may be inconsistent now, we clear the database
            // (volatile systems remove the entry from pnfs if this was the last
            // copy)
            //
            _pnfs.clearCacheLocation(entry.getPnfsId(),
                                     _lfsMode == LFS_VOLATILE);
            //
            //
            if (_reportOnRemovals) {
                try {
                    sendMessage(new CellMessage(_billingCell,
                                                new RemoveFileInfoMessage(getCellName() + "@"
                                                                          + getCellDomainName(), entry.getPnfsId())));
                } catch (NotSerializableException e) {
                    throw new RuntimeException("Bug detected: Unserializable vehicle", e);
                } catch (NoRouteToCellException e) {
                    esay("Could not report removal of : " + entry.getPnfsId()
                         + " : " + e.getMessage());
                }
            }
        }

        public void destroyed(CacheRepositoryEvent event) {
            say("RepositoryLoader : destroyed : " + event);
        }

        public void needSpace(CacheNeedSpaceEvent event) {
            say("RepositoryLoader : needSpace : " + event);
        }

        public void scanned(CacheRepositoryEvent event) {
            CacheRepositoryEntry entry = event.getRepositoryEntry();

            try {
                if (entry.isPrecious() && (!entry.isBad())
                    && (_lfsMode == LFS_NONE)) {

                    _storageQueue.addCacheEntry(entry);
                    say("RepositoryLoader : Scanning " + entry.getPnfsId()
                        + " ok");

                }
            } catch (CacheException ce) {
                esay("RepositoryLoader : Scanning " + entry.getPnfsId()
                     + " failed " + ce.getMessage());
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

    private void dumpSetup(PrintWriter pw) {
        pw.println("#\n# Created by " + getCellName() + "("
                   + this.getClass().getName() + ") at " + (new Date()).toString()
                   + "\n#");
        pw.println("set max diskspace " + _repository.getTotalSpace());
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
        if (_sweeper != null)
            _sweeper.printSetup(pw);
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

    public void log(String str) {
        say(str);
    }

    public void elog(String str) {
        esay(str);
    }

    public void plog(String str) {
        esay("PANIC : " + str);
    }

    @Override
    public void getInfo(PrintWriter pw) {
        pw.println("Base directory    : " + _baseDir);
        pw
            .println("Revision          : [$Revision$]");
        pw.println("Version           : " + getCellVersion() + " (Sub="
                   + _version + ")");
        pw.println("StickyFiles       : "
                   + (_allowSticky ? "allowed" : "denied"));
        pw.println("Gap               : " + _gap);
        pw.println("Report remove     : " + (_reportOnRemovals ? "on" : "off"));
        pw
            .println("Recovery          : "
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
        pw.println("Storage Mode      : "
                   + (_isPermanent ? "Static" : "Dynamic"));
        pw.println("ReplicationMgr    : " + _replicationHandler);
        pw.println("Check Repository  : " + _checkRepository);
        pw.println("LargeFileStore    : "
                   + (_lfsMode == LFS_NONE ? "None"
                      : _lfsMode == LFS_VOLATILE ? "Volatile" : "Precious"));
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
        pw.println("Diskspace usage   : ");
        if (_repository != null) {
            long total = _repository.getTotalSpace();
            long used = total - _repository.getFreeSpace();
            long precious = _repository.getPreciousSpace();

            pw.println("    Total    : " + UnitInteger.toUnitString(total));
            pw.println("    Used     : " + used + "    ["
                       + (((float) used) / ((float) total)) + "]");
            pw.println("    Free     : " + (total - used));
            pw.println("    Precious : " + precious + "    ["
                       + (((float) precious) / ((float) total)) + "]");
            pw
                .println("    Removable: "
                         + _sweeper.getRemovableSpace()
                         + "    ["
                         + (((float) _sweeper.getRemovableSpace()) / ((float) total))
                         + "]");
            pw.println("    Reserved : " + _repository.getReservedSpace());
        } else {
            pw.println("No Yet known");
        }

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
            for (Iterator<JobScheduler> it = manager.scheduler(); it.hasNext();) {
                JobScheduler js = it.next();
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

    @Override
    public void say(String str) {
        pin(str);
        super.say(str);
    }

    @Override
    public void esay(String str) {
        pin(str);
        super.esay(str);
    }

    // //////////////////////////////////////////////////////////////
    //
    // The io File Part
    //
    //
    private void ioFile(PoolIoFileMessage poolMessage, CellMessage cellMessage) {

        cellMessage.revertDirection();

        RepositoryIoHandler io = null;
        try {
            io = new RepositoryIoHandler(poolMessage, cellMessage);
        } catch (CacheException ce) {
            poolMessage.setFailed(ce.getRc(), ce.getMessage());
            esay(ce.getMessage());
            try {
                sendMessage(cellMessage);
            } catch (NotSerializableException e) {
                throw new RuntimeException("Bug detected: Unserializable vehicle", e);
            } catch (NoRouteToCellException e) {
                esay(e);
            }
            return;
        }

        for ( Map.Entry<?,?> attribute : _moverAttributes.entrySet() ) {
            try {
                Object key = attribute.getKey();
                Object value = attribute.getValue();
                say("Setting mover " + key.toString() + " -> " + value);
                io.setAttribute(key.toString(), value);
            } catch (IllegalArgumentException iae) {
                esay("setAttribute : " + iae.getMessage());
            }
        }
        String queueName = poolMessage.getIoQueueName();
        IoQueueManager queueManager = (IoQueueManager) _ioQueue;
        try {
            if (io.isWrite()) {

                queueManager.add(queueName, io, SimpleJobScheduler.HIGH);

            } else if (poolMessage.isPool2Pool()) {

                (_p2pMode == P2P_INTEGRATED ? _ioQueue : _p2pQueue).add(io,
                                                                        SimpleJobScheduler.HIGH);

            } else {
                String newClient = io.getClient();
                long newId = io.getClientId();

                if (_dupRequest != DUP_REQ_NONE) {

                    JobInfo job = null;
                    List<JobInfo> list = _ioQueue.getJobInfos();
                    boolean found = false;

                    for (int l = 0; l < list.size(); l++) {

                        job = list.get(l);
                        if (newClient.equals(job.getClientName())
                            && (newId == job.getClientId())) {

                            found = true;
                            break;

                        }
                    }
                    if (!found) {
                        say("Dup Request : regular <" + newClient + ":" + newId
                            + ">");
                        queueManager.add(queueName, io,
                                         SimpleJobScheduler.REGULAR);
                    } else if (_dupRequest == DUP_REQ_IGNORE) {
                        say("Dup Request : ignoring <" + newClient + ":"
                            + newId + ">");
                    } else if (_dupRequest == DUP_REQ_REFRESH) {
                        long jobId = job.getJobId();
                        say("Dup Request : refresing <" + newClient + ":"
                            + newId + "> old = " + jobId);
                        queueManager.kill((int) jobId, true);
                        queueManager.add(queueName, io,
                                         SimpleJobScheduler.REGULAR);
                    } else {
                        say("Dup Request : PANIC (code corrupted) <"
                            + newClient + ":" + newId + ">");
                    }

                } else {

                    say("Dup Request : none <" + newClient + ":" + newId + ">");
                    queueManager.add(queueName, io, SimpleJobScheduler.REGULAR);

                }
            }
        } catch (InvocationTargetException ite) {
            esay("" + io + " not added due to : " + ite.getTargetException());
            esay(ite);
        }
        return;
    }

    private class RepositoryIoHandler implements IoBatchable {

        private PoolIoFileMessage _command = null;
        private CellMessage _message = null;
        private String _clientPath = null;
        private PnfsId _pnfsId = null;
        private MoverProtocol _handler = null;
        private ProtocolInfo _protocolInfo = null;
        private StorageInfo _storageInfo = null;
        private CellPath _destination = null;
        private boolean _rdOnly = true;
        private boolean _create = false;
        private CacheRepositoryEntry _entry = null;
        private boolean _preparationDone = false;
        private DoorTransferFinishedMessage _finished = null;
        private MoverInfoMessage _info = null;
        private boolean _started = false;
        private boolean _protected = false;
        private Thread _thread;

        public RepositoryIoHandler(PoolIoFileMessage poolMessage,
                                   CellMessage originalCellMessage)
            throws CacheException
        {
            _message = originalCellMessage;
            _command = poolMessage;
            _destination = _message.getDestinationPath();
            _pnfsId = _command.getPnfsId();
            _protocolInfo = _command.getProtocolInfo();
            _storageInfo = _command.getStorageInfo();
            _info = new MoverInfoMessage(getCellName() + "@"
                                         + getCellDomainName(), _pnfsId);
            _info.setInitiator(_command.getInitiator());
            CellPath tmp = (CellPath) _destination.clone();
            tmp.revert();
            _clientPath = tmp.getCellName() + "@" + tmp.getCellDomainName();
            //
            // prepare the final reply
            //
            _finished = new DoorTransferFinishedMessage(_command.getId(),
                                                        _pnfsId, _protocolInfo, _storageInfo, poolMessage
							.getPoolName());
            _finished.setIoQueueName(poolMessage.getIoQueueName());

            //
            // we need to change the next two lines as soon
            // as we allow 'transient files'.
            //
            _rdOnly = poolMessage instanceof PoolDeliverFileMessage;
            _create = !_rdOnly;

            _info.setFileCreated(_create);
            _info.setStorageInfo(_storageInfo);

            // check for file existence
            if (_rdOnly && !_repository.contains(_pnfsId)) {
                // remove 'BAD' cachelocation in pnfs
                _pnfs.clearCacheLocation(_pnfsId);
                throw new FileNotInCacheException("Entry not in repository : "
                                                  + _pnfsId);
            }

        }

        private boolean isWrite() {
            return _create;
        }

        protected synchronized void protect()
            throws InterruptedException
        {
            if (Thread.interrupted()) {
                throw new InterruptedException("IO Job was killed");
            }
            _protected = true;
        }

        protected synchronized void setThread(Thread value)
        {
            _thread = value;
        }

        public synchronized boolean kill()
        {
            if (_protected || _thread == null)
                return false;

            _thread.interrupt();
            return true;
        }

        @Override
        public String toString() {

            StringBuffer sb = new StringBuffer();
            sb.append(_pnfsId.toString());
            if (_handler == null) {
                sb.append(" h={NoHandlerYet}");
            } else {
                sb.append(" h={").append(_handler.toString())
                    .append("} bytes=").append(
                                               _handler.getBytesTransferred()).append(
                                                                                      " time/sec=").append(getTransferTime() / 1000L)
                    .append(" LM=");

                long lastTransferTime = getLastTransferred();
                if (lastTransferTime == 0L) {
                    sb.append(0);
                } else {
                    sb.append((System.currentTimeMillis() - lastTransferTime) / 1000L);
                }
            }
            return sb.toString();
        }

        private void setAttribute(String name, Object attribute) {

            if (_handler == null) {
                throw new IllegalArgumentException("Handler not yet installed");
            }

            _handler.setAttribute(name, attribute);
        }

        private Object getAttribute(String name) {
            if (_handler == null) {
                throw new IllegalArgumentException("Handler not yet installed");
            }

            return _handler.getAttribute(name);
        }

        private synchronized boolean prepare() {
            if (_preparationDone)
                return false;
            _preparationDone = true;
            boolean failed = false;
            say("JOB prepare " + _pnfsId);
            //
            // do all the preliminary stuff
            // - load Protocol Handler
            // - check File is ok
            // - increment the link count (to prevent the file from been
            // removed)
            //
            PnfsId pnfsId = _pnfsId;
            try {
                _handler = getProtocolHandler(_protocolInfo);
                if (_handler == null)
                    throw new CacheException(27,
                                             "PANIC : Couldn't get handler for " + _protocolInfo);

                if (_create && _rdOnly)
                    throw new IllegalArgumentException("Can't read and create");

                synchronized (_repository) {
                    if (_create) {
                        _entry = _repository.createEntry(pnfsId);
                        _entry.lock(true);
                        _entry.setReceivingFromClient();
                    } else {
                        _entry = _repository.getEntry(pnfsId);
                        say("Entry for " + pnfsId + " found and is " + _entry);
                        if ((!_entry.isCached()) && (!_entry.isPrecious()))
                            throw new CacheException(301,
                                                     "File is still transient : " + pnfsId);
                    }
                    _entry.incrementLinkCount();
                }
                _command.setSucceeded();

            } catch (FileNotInCacheException fce) {
                esay(_pnfsId + " not in repository");
                // probably bad entry in cacheInfo, clean it
                _pnfs.clearCacheLocation(_pnfsId);
                failed = true;
                _command.setFailed(fce.getRc(), fce.getMessage());
            } catch (CacheException ce) {
                esay("Io Thread Cache Exception : " + ce);
                esay(ce);
                if (ce.getRc() == CacheRepository.ERROR_IO_DISK)
                    disablePool(PoolV2Mode.DISABLED_STRICT, ce.getRc(), ce
                                .getMessage());
                failed = true;
                _command.setFailed(ce.getRc(), ce.getMessage());
            } catch (Exception exc) {
                esay("Thread Exception : " + exc);
                esay(exc);
                failed = true;
                _command.setFailed(11, exc.toString());
            }
            //
            // send first acknowledge.
            //
            try {
                sendMessage(_message);
            } catch (NotSerializableException e) {
                throw new RuntimeException("Bug detected: Unserializable vehicle", e);
            } catch (NoRouteToCellException e) {
                esay("Can't send message back to door : " + e.getMessage());
                esay(e);
                failed = true;
            }
            if (failed) {
                try {
                    if (_create) {
                        _entry.lock(false);
                        _repository.removeEntry(_entry);
                    }
                } catch (CacheException ce) {
                    esay("PANIC : couldn't remove entry : " + _entry);
                }
                esay("IoFile thread finished (request failure)");
            }
            return failed;
        }

        //
        // the IoBatchable Interface
        //
        public PnfsId getPnfsId() {
            return _pnfsId;
        }

        public long getLastTransferred() {
            synchronized (this) {
                if (!_started)
                    return 0L;
            }
            if (_handler == null) {
                return 0;
            } else {
                return _handler.getLastTransferred();
            }

        }

        public long getTransferTime() {
            synchronized (this) {
                if (!_started)
                    return 0L;
            }
            if (_handler == null) {
                return 0;
            } else {
                return _handler.getTransferTime();
            }
        }

        public long getBytesTransferred() {
            if (_handler == null) {
                return 0;
            } else {
                return _handler.getBytesTransferred();
            }
        }

        public double getTransferRate() {
            if (_handler == null) {
                return 10.000;
            } else {
                long bt = _handler.getBytesTransferred();
                long tm = _handler.getTransferTime();
                return tm == 0L ? (double) 10.00 : ((double) bt / (double) tm);
            }
        }

        //
        // the Batchable Interface
        //
        public String getClient() {
            return _clientPath;
        }

        public long getClientId() {
            return _command.getId();
        }

        public String getIoQueueName() {
            String name = _command.getIoQueueName();
            if (name == null) {
                IoQueueManager manager = (IoQueueManager)_ioQueue;
                return manager.getDefaultScheduler().getSchedulerName();
            } else {
                return name;
            }
        }

        public void queued(int id) {
            say("JOB queued " + _pnfsId);
            _command.setMoverId(id);
            if (prepare())
                throw new IllegalArgumentException("prepare failed");
        }

        public void unqueued() {
            say("JOB unqueued " + _pnfsId);
            //
            // TBD: have to send 'failed' as last message
            //
            return;
        }

        //
        // the Runnable Interface
        //
        public void run() {

            setThread(Thread.currentThread());

            say("JOB run " + _pnfsId);
            if (prepare())
                return;

            synchronized (this) {
                _started = true;
            }
            SysTimer sysTimer = new SysTimer();
            long transferTimer = System.currentTimeMillis();

            SpaceMonitor monitor = _repository;
            File cacheFile = null;
            ChecksumMover csmover = null;
            ChecksumFactory clientChecksumFactory = null;

            if (!_crashEnabled) {
                _storageInfo.setKey("crash", null);
            } else {
                _storageInfo.setKey("crashType", _crashType);
            }

            try {
                cacheFile = _entry.getDataFile();

                say("Trying to open " + cacheFile);
                if (_create) {

                    sysTimer.getDifference();

                    String tmp = null;
                    long preallocatedSpace = 0L;
                    long maxAllocatedSpace = 0L;
                    if ((tmp = _storageInfo.getKey(PREALLOCATED_SPACE)) != null) {
                        try {
                            preallocatedSpace = Long.parseLong(tmp);
                        } catch (NumberFormatException ee) { /* ignore 'bad' values*/ }
                    }
                    if ((tmp = _storageInfo.getKey(MAX_SPACE)) != null) {
                        try {
                            maxAllocatedSpace = Long.parseLong(tmp);
                        } catch (NumberFormatException ee) {/* ignore 'bad' values*/ }
                    }

                    if ((preallocatedSpace > 0L) || (maxAllocatedSpace > 0L))
                        monitor = new PreallocationSpaceMonitor(_repository,
								preallocatedSpace, maxAllocatedSpace);

                    if( _handler instanceof ChecksumMover ){

                        csmover = (ChecksumMover)_handler;
                        say("Checksum mover is set");
                        clientChecksumFactory = csmover.getChecksumFactory(_protocolInfo);
                        Checksum checksum = null;
                        if ( clientChecksumFactory != null ){
                            say("Got checksum factory of "+clientChecksumFactory.getType());
                            checksum = clientChecksumFactory.create();
                        } else
                            checksum = _checksumModule.getDefaultChecksumFactory().create();

                        if ( _checksumModule.checkOnTransfer() ){
                            csmover.setDigest(checksum);
                        }
                    }

                    /* To protect against bugs in the mover, we
                     * decorate the space monitor and correct any
                     * discrepancies after the transfer.
                     */
                    SpaceMonitorWatch watcher =
                        new SpaceMonitorWatch(monitor);
                    try {
                        RandomAccessFile raf =
                            new RandomAccessFile(cacheFile, "rw");
                        try {
                            try {
                                _handler.runIO(raf,
                                               _protocolInfo,
                                               _storageInfo,
                                               _pnfsId,
                                               watcher,
                                               MoverProtocol.WRITE
                                               | MoverProtocol.READ);
                            } finally {
                                /* The remaining steps are not safe to
                                 * interrupt and we therefore block the
                                 * timeout manager from killing us. If we
                                 * have already been killed, we raise an
                                 * exception right away.
                                 */
                                protect();
                            }

                            /* Some movers perform checksum
                             * computation after the
                             * transfer. Therefore we cannot close the
                             * file until we have retrieved the
                             * checksum.
                             */
                            if (csmover != null) {
                                _checksumModule.setMoverChecksums(_entry,
                                                                  clientChecksumFactory,
                                                                  csmover.getClientChecksum(),
                                                                  _checksumModule.checkOnTransfer()
                                                                  ? csmover.getTransferChecksum()
                                                                  : null);
                            } else {
                                _checksumModule.setMoverChecksums(_entry,
                                                                  null,
                                                                  null,
                                                                  null);
                            }
                        } finally {
                            /* This may throw an IOException, although it
                             * is not clear when this would happen. If it
                             * does, we are probably better off
                             * propagating the exception, which is why we
                             * do not catch it here.
                             */
                            raf.close();
                        }
                    } finally {
                        long diff = watcher.correctSpace(cacheFile.length());
                        if (diff != 0) {
                            esay("Bug (please report this): Broken space allocation for "
                                 + _pnfsId + " with mover "
                                 + _handler.getClass().getName() +
                                 " and difference " + diff
                                 + " (" + watcher + ")");
                        }
                    }

                    long fileSize = cacheFile.length();
                    _storageInfo.setFileSize(fileSize);
                    _info.setFileSize(fileSize);

                    say(_pnfsId.toString()
                        + ";length=" + fileSize
                        + ";timer=" + sysTimer.getDifference().toString());

                    boolean overwrite = _storageInfo.getKey("overwrite") != null;

                    //
                    // store the storage info and set the file precious.
                    // ( first remove the lock, otherwise the
                    // state for the precious events is wrong.
                    //

                    /*
                     * Due to support of <AccessLatency> and <RetentionPolicy>
                     * the file state in the pool has changed has changed it's
                     * meaning:
                     *     precious: have to goto tape
                     *     cached: free to be removed by sweeper
                     *     cached+sticky: does not go to tape, isn't removed by sweeper
                     *
                     * new states depending on AL and RP:
                     *     Custodial+ONLINE   (T1D1) : precious+sticky  => cached+sticky
                     *     Custodial+NEARLINE (T1D0) : precious         => cached
                     *     Output+ONLINE      (T0D1) : cached+sticky    => cached+sticky
                     *
                     */

                    _entry.lock(false) ;
                    _entry.setStorageInfo( _storageInfo ) ;

                    AccessLatency accessLatency = _storageInfo.getAccessLatency();
                    if( accessLatency != null && accessLatency.equals( AccessLatency.ONLINE) ) {

                    	// TODO: probably, we have to notify PinManager
                    	// HopingManager have to copy file into a 'read' pool if
                    	// needed, set copy 'sticky' and remove sticky flag in the 'write' pool

                    	_entry.setSticky(true);
                    }else{
                    	_entry.setSticky(false);
                    }

                    // flush to tape only if the file defined as a 'tape file'( RP = Custodial) and the HSM is defined
                    String hsm = _storageInfo.getHsm();
                    RetentionPolicy retentionPolicy = _storageInfo.getRetentionPolicy();

                    if (overwrite) {
                        _entry.setCached();
                        say("Overwriting requested");
                    } else if( retentionPolicy != null && retentionPolicy.equals(RetentionPolicy.CUSTODIAL) ) {
                        _entry.setPrecious() ;
                    } else {
                    	_entry.setCached() ;
                    }


                    //
                    // setFileSize will throw an exception if the file is no
                    // longer in pnfs. As a result, the client will let the
                    // close fail and we will remove the entries from the
                    // the repository.
                    //
                    if ((!overwrite) && (!_isHsmPool)) {
                        _pnfs.setFileSize(_pnfsId, fileSize);
                        if (_lfsMode == MultiProtocolPoolV3.LFS_NONE) {
                            _pnfs.putPnfsFlag(_pnfsId, "h", "yes");
                        } else {
                            _pnfs.putPnfsFlag(_pnfsId, "h", "no");
                        }
                    }
                    //
                    _replicationHandler.initiateReplication(_entry, "write");
                    //
                    // cache location changed by event handler
                    //
                    //
                } else {
                    sysTimer.getDifference();
                    long fileSize = cacheFile.length();
                    _info.setFileSize(fileSize);

                    RandomAccessFile raf =
                        new RandomAccessFile(cacheFile, "r");
                    try {
                        try {
                            _handler.runIO(raf,
                                           _protocolInfo,
                                           _storageInfo,
                                           _pnfsId,
                                           new ReadOnlySpaceMonitor(_repository),
                                           MoverProtocol.READ);
                        } finally {
                            /* The remaining steps are not safe to
                             * interrupt and we therefore block the
                             * timeout manager from killing us. If we
                             * have already been killed, we raise an
                             * exception right away.
                             */
                            protect();
                        }
                    } finally {
                        /* This may throw an IOException, although it
                         * is not clear when this would happen. If it
                         * does, we are probably better off
                         * propagating the exception.
                         */
                        raf.close();
                    }

                    if (_handler.wasChanged()) {
                        throw new RuntimeException("Bug: Mover changed read-only file");
                    }

                    say(_pnfsId.toString() + ";length=" + fileSize + ";timer="
                        + sysTimer.getDifference().toString());

                }
                _finished.setSucceeded();

            } catch (Exception eofe) {

                esay("Exception in runIO for : " + _pnfsId + " " + eofe);

                if (!(eofe instanceof EOFException))
                    esay(eofe);

                if (eofe instanceof CacheException) {
                    int errorCode = ((CacheException) eofe).getRc();
                    if (errorCode == CacheRepository.ERROR_IO_DISK) {
                        disablePool(PoolV2Mode.DISABLED_STRICT, errorCode, eofe
                                    .getMessage());
                        fsay(eofe.getMessage());
                    }
                }

                try {
                    if (_create) {

                        long fileSize = cacheFile.length();
                        if (fileSize == 0) {
                            // remove newly created zero size files if
                            // there was a problem to write it
                            esay("removing empty file: " + _pnfsId);
                            _entry.lock(false);
                            _repository.removeEntry(_entry);
                            if (!_isHsmPool)
                                _pnfs.deletePnfsEntry(_pnfsId);
                        } else {

                            // FIXME: this part is not as elegant as it's should
                            // be - duplicated code

                            // set file size
                            esay("Storing incomplete file : " + _pnfsId
                                 + " with " + fileSize);

                            _storageInfo.setFileSize(fileSize);
                            _info.setFileSize(fileSize);
                            _entry.lock(false);
                            _entry.setStorageInfo(_storageInfo);
                            _entry.setPrecious();
                            if (!_isHsmPool) {
                                _pnfs.setFileSize(_pnfsId, fileSize);
                                if (_lfsMode == MultiProtocolPoolV3.LFS_NONE) {
                                    _pnfs.putPnfsFlag(_pnfsId, "h", "yes");
                                } else {
                                    _pnfs.putPnfsFlag(_pnfsId, "h", "no");
                                }
                            }
                            // set checksum
                            if (csmover != null) {
                                _checksumModule
                                    .setMoverChecksums(
                                                       _entry,  clientChecksumFactory,
                                                       csmover.getClientChecksum(),
                                                       _checksumModule
                                                       .checkOnTransfer() ? csmover
                                                       .getTransferChecksum()
                                                       : null);
                            } else {
                                _checksumModule.setMoverChecksums(_entry, null, null,
                                                                  null);
                            }

                        }
                    }

                } catch (Throwable e) {
                    esay("Stacked Exception (Original) for " + _pnfsId + " : "+ eofe);
                    esay("Stacked Throwable (Resulting) for " + _pnfsId + " : " + e);
                    esay(e);
                } finally {
                    String errorMessage = "Unexpected Exception : " + eofe;
                    int errorCode = 33;

                    if (eofe instanceof CacheException) {
                        errorCode = ((CacheException) eofe).getRc();
                        errorMessage = eofe.getMessage();
                    }
                    _finished.setReply(errorCode, errorMessage);
                    _info.setResult(errorCode, errorMessage);
                }

            } catch (Throwable e) {
                esay("Throwable in runIO() " + _pnfsId + " " + e);
                esay(e);

                _finished.setReply(34, e);
                _info.setResult(34, e.toString());

            } finally {
                try {
                    _entry.decrementLinkCount();
                } catch (CacheException e) {
                    // Exception never thrown
                    throw new RuntimeException("Bug: decrementLinkCount threw unexpected exception", e);
                }

                try {

                    if (monitor instanceof PreallocationSpaceMonitor) {
                        PreallocationSpaceMonitor m = (PreallocationSpaceMonitor) monitor;
                        long usedSpace = m.getUsedSpace();
                        say("Applying preallocated space : " + usedSpace);
                        if (usedSpace > 0L)
                            _repository.applyReservedSpace(usedSpace);
                    }

                } catch (CacheException e) {
                    esay("Problem  handling reserved space management : " + e);
                    esay(e);
                }

                transferTimer = System.currentTimeMillis() - transferTimer;
                long bytesTransferred = _handler.getBytesTransferred();
                _info.setTransferAttributes(bytesTransferred, transferTimer,
                                            _protocolInfo);

            }
            try {
                _message.setMessageObject(_finished);
                sendMessage(_message);
            } catch (Exception e) {
                esay("PANIC : Can't send message back to door : " + e);
                esay(e);
            }
            try {
                sendMessage(new CellMessage(_billingCell, _info));
            } catch (Exception e) {
                esay("PANIC : Can't report to 'billing cell' : " + e);
                esay(e);
            }
            say("IO thread finished : " + Thread.currentThread().toString());
        }

    }

    // //////////////////////////////////////////////////////////////
    //
    // replication on data arrived
    //
    private class ReplicationHandler {

        private boolean _enabled = false;
        private String _replicationManager = "PoolManager";
        private String _destinationHostName = null;
        private String _destinationMode = "keep";
        private boolean _replicateOnRestore = false;

        //
        // replicationManager,Hostname,modeOfDestFile
        //
        private ReplicationHandler() {
            init(null);
        }

        private ReplicationHandler(String vars) {
            init(vars);
        }

        public void init(String vars) {

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

            return;
        }

        public String getParameterString() {
            StringBuffer sb = new StringBuffer();
            if (_enabled) {
                sb.append(_replicationManager).append(_destinationHostName)
                    .append(_destinationMode);
            } else {
                sb.append("off");
            }
            return sb.toString();
        }

        @Override
        public String toString() {
            StringBuffer sb = new StringBuffer();

            if (_enabled) {
                sb.append("{Mgr=").append(_replicationManager).append(",Host=")
                    .append(_destinationHostName).append(",DestMode=")
                    .append(_destinationMode).append("}");
            } else {
                sb.append("Disabled");
            }
            return sb.toString();
        }

        private void initiateReplication(CacheRepositoryEntry entry,
                                         String source) {
            if ((!_enabled)
                || (source.equals("restore") && !_replicateOnRestore))
                return;
            try {
                _initiateReplication(entry, source);
            } catch (CacheException e) {
                esay("Problem in sending replication request : " + e);
            } catch (NoRouteToCellException e) {
                esay("Problem in sending replication request : " + e.getMessage());
            }
        }

        private void _initiateReplication(CacheRepositoryEntry entry,
                                          String source)
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
    private Map<String, Class<?>> _handlerClasses = new Hashtable<String, Class<?>>();

    private MoverProtocol getProtocolHandler(ProtocolInfo info) {

        Class<?>[] argsClass = { dmg.cells.nucleus.CellAdapter.class };
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
            return (MoverProtocol) moverCon.newInstance(args);

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
    private class ReplyToPoolFetch implements CacheFileAvailable {
        private CellMessage _cellMessage = null;

        private ReplyToPoolFetch(CellMessage cellMessage) {
            _cellMessage = cellMessage;
        }

        public void cacheFileAvailable(String pnfsId, Throwable ee) {

            Message msg = (Message) _cellMessage.getMessageObject();

            if (ee != null) {

                if (ee instanceof CacheException) {

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

            } else {

                try {

                    PnfsId id = new PnfsId(pnfsId);

                    CacheRepositoryEntry entry = _repository.getEntry(id);

                    msg.setSucceeded();

                    doChecksum(id, entry);

                    _replicationHandler.initiateReplication(entry,
                                                            "restore");


                } catch (CacheException ee2) {

                    msg.setFailed(1010, "Checksum calculation failed " + ee2);
                    esay("Checksum calculation failed : " + ee2);
                    esay(ee);
                }
            }

            _cellMessage.revertDirection();
            esay("cacheFileAvailable : Returning from restore : "
                 + _cellMessage.getMessageObject());
            try {
                sendMessage(_cellMessage);
            } catch (NotSerializableException e) {
                throw new RuntimeException("Bug detected: Unserializable vehicle", e);
            } catch (NoRouteToCellException e) {
                esay("Couldn't send ack to poolManager : " + e.getMessage());
            }

        }

        private void doChecksum(PnfsId pnfsId, CacheRepositoryEntry entry)
            throws CacheException
        {

            Message msg = (Message) _cellMessage.getMessageObject();

            if (_checksumModule.getCrcFromHsm())
                getChecksumFromHsm(pnfsId, entry);

            if (!_checksumModule.checkOnRestore())
                return;

            say("Calculating checksum of " + pnfsId);

            try {
                StorageInfo info = entry.getStorageInfo();
                String checksumString = info.getKey("flag-c");

                if (checksumString == null)
                    throw new CacheException("Checksum not in StorageInfo");

                long start = System.currentTimeMillis();

                Checksum infoChecksum = new Checksum(checksumString);
                Checksum fileChecksum = _checksumModule.calculateFileChecksum(
                                                                              entry, _checksumModule.getDefaultChecksum());

                say("Checksum for " + pnfsId + " info=" + infoChecksum
                    + ";file=" + fileChecksum + " in "
                    + (System.currentTimeMillis() - start));

                if (!infoChecksum.equals(fileChecksum)) {

                    esay("Checksum of " + pnfsId + " differs info="
                         + infoChecksum + ";file=" + fileChecksum);
                    try {
                        _repository.removeEntry(entry);
                    } catch (CacheException ee2) {
                        esay("Couldn't remove file : " + pnfsId + " : " + ee2);
                    }
                    msg.setFailed(1009, "Checksum error : info=" + infoChecksum
                                  + ";file=" + fileChecksum);
                }
            } catch (Exception ee3) {
                esay("Couldn't compare checksum of " + pnfsId + " : " + ee3);
                ee3.printStackTrace();
            }

        }

        private void getChecksumFromHsm(PnfsId pnfsId,
                                        CacheRepositoryEntry entry) {
            String line = null;
            try {
                File f = entry.getDataFile();
                f = new File(f.getCanonicalPath() + ".crcval");
                Checksum checksum = null;
                if (f.exists()) {
                    BufferedReader br = new BufferedReader(new FileReader(f));
                    try {
                        line = "1:" + br.readLine();
                        checksum = new Checksum(line);
                    } finally {
                        try {
                            br.close();
                        } catch (IOException ioio) {
                        }
                        f.delete();
                    }
                    say(pnfsId + " : sending adler32 to pnfs : " + line);
                    _checksumModule.storeChecksumInPnfs(pnfsId, checksum);
                }
            } catch (Exception waste) {
                esay("Couldn't send checksum (" + line + ") to pnfs : " + waste);
            }

        }
    }

    private boolean fetchFile(PoolFetchFileMessage poolMessage,
                              CellMessage cellMessage) {

        PnfsId pnfsId = poolMessage.getPnfsId();
        StorageInfo storageInfo = poolMessage.getStorageInfo();
        say("Pool " + _poolName + " asked to fetch file " + pnfsId + " (hsm="
            + storageInfo.getHsm() + ")");

        try {

            if ((storageInfo.getFileSize() == 0) && !_flushZeroSizeFiles) {

                CacheRepositoryEntry entry = _repository.createEntry(pnfsId);
                entry.setStorageInfo(storageInfo);
                entry.setReceivingFromStore();
                entry.setCached();
                return true;

            }
            ReplyToPoolFetch reply = new ReplyToPoolFetch(cellMessage);
            if (_storageHandler.fetch(pnfsId, storageInfo, reply)) {
                poolMessage.setSucceeded();
                return true;
            } else {
                return false;
            }

        } catch (FileInCacheException ce) {
            esay(ce);
            poolMessage.setFailed(0, null);
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

    private void checkFile(PoolFileCheckable poolMessage) {
        PnfsId pnfsId = poolMessage.getPnfsId();
        try {

            /*
             * logic is following:
             *
             *    in case of file  not in repository, FileNotInCacheException will be thrown
             *    if not, file is there. Check readiness - cache or precious.
             *    other wise - not available, but waiting.
             */

            poolMessage.setHave(false);
            poolMessage.setWaiting(false);

            CacheRepositoryEntry entry = _repository.getEntry(pnfsId);
            if ( entry.isCached() || entry.isPrecious() ) {
                poolMessage.setHave(true);
            } else {
                poolMessage.setWaiting(true);
            }
        } catch (FileNotInCacheException fe) {
            // default state is fine
        } catch (Exception e) {
            esay("Could get information about <" + pnfsId + "> : "
                 + e.getMessage());
            // default state is fine
        }
    }

    private void setSticky(PoolSetStickyMessage stickyMessage) {
        if (stickyMessage.isSticky() && (!_allowSticky)) {
            stickyMessage.setFailed(101, "making sticky denied by pool : "
                                    + _poolName);
            return;
        }
        PnfsId pnfsId = stickyMessage.getPnfsId();

        try {

            /*
             * add sticky owner and lifetime if repository supports it
             */
            CacheRepositoryEntry entry = _repository.getEntry(pnfsId);
            entry.setSticky(stickyMessage
                            .isSticky(), stickyMessage.getOwner(), stickyMessage
                            .getLifeTime());

        } catch (CacheException ce) {
            stickyMessage.setFailed(ce.getRc(), ce);
            return;
        } catch (Exception ee) {
            stickyMessage.setFailed(100, ee);
            return;
        }
        return;
    }

    private void modifyPersistency(
                                   PoolModifyPersistencyMessage persistencyMessage) {

        PnfsId pnfsId = persistencyMessage.getPnfsId();

        try {

            CacheRepositoryEntry entry = _repository.getEntry(pnfsId);
            if (entry.isPrecious()) {

                if (persistencyMessage.isCached())
                    entry.setCached();

            } else if (entry.isCached()) {

                if (persistencyMessage.isPrecious())
                    entry.setPrecious(true);

            } else {

                persistencyMessage.setFailed(101, "File still transient : "
                                             + entry);
                return;

            }

        } catch (CacheException ce) {
            persistencyMessage.setFailed(ce.getRc(), ce);
            return;
        } catch (Exception ee) {
            persistencyMessage.setFailed(100, ee);
            return;
        }
        return;
    }

    private void modifyPoolMode(PoolModifyModeMessage modeMessage) {
        PoolV2Mode mode = modeMessage.getPoolMode();
        if (mode == null)
            return;

        if (mode.isEnabled()) {
            enablePool();
        } else {
            disablePool(mode.getMode(), modeMessage.getStatusCode(),
                        modeMessage.getStatusMessage());
        }

        return;
    }

    private void checkFreeSpace(PoolCheckFreeSpaceMessage poolMessage) {
        // long freeSpace = _repository.getFreeSpace() ;
        long freeSpace = 1024L * 1024L * 1024L * 100L;
        say("XChecking free space [ result = " + freeSpace + " ] ");
        poolMessage.setFreeSpace(freeSpace);
        poolMessage.setSucceeded();
    }

    private void updateCacheStatistics(
                                       PoolUpdateCacheStatisticsMessage poolMessage) {
        // /
    }

    private class CompanionFileAvailableCallback implements CacheFileAvailable {

        private final CellMessage _cellMessage ;
        private final Pool2PoolTransferMsg _poolMessage ;
        private final PnfsId _pnfsId ;

        private CompanionFileAvailableCallback(CellMessage cellMessage,
                                               Pool2PoolTransferMsg poolMessage, PnfsId pnfsId) {

            this._cellMessage = cellMessage;
            this._poolMessage = poolMessage;
            this._pnfsId = pnfsId;
        }

        public void cacheFileAvailable(String pnfsIdString, Throwable e) {

            if (e != null) {
                if (e instanceof CacheException) {
                    _poolMessage.setReply(((CacheException) e).getRc(), e);
                } else {
                    _poolMessage.setReply(CacheException.UNEXPECTED_SYSTEM_EXCEPTION, e);
                }
            } else {
                try {
                    //
                    // the default mode of a p2p copy is 'cached'.
                    // So we only have to change the mode if the customer
                    // needs something different.
                    //
                    // ( API overwrites the configuration setting )
                    //
                    CacheRepositoryEntry entry = _repository.getEntry(_pnfsId);
                    try {
                        int fileMode = _poolMessage.getDestinationFileStatus();
                        if (fileMode != Pool2PoolTransferMsg.UNDETERMINED) {
                            if (fileMode == Pool2PoolTransferMsg.PRECIOUS)
                                entry.setPrecious(true);
                        } else {
                            if ((_lfsMode == LFS_PRECIOUS)
                                && (_p2pFileMode == P2P_PRECIOUS)) {

                                entry.setPrecious(true);

                            }
                        }
                    } catch (CacheException ee) {
                        esay("Couldn't set precious : " + entry + " : " + ee);
                        throw ee;
                    }
                } catch (CacheException ee) {
                    //
                    // we regard this transfer as OK, even if setting
                    // the file mode failed.
                    //
                    esay("Problems setting file mode : " + ee);
                }
            }
            if (_poolMessage.getReplyRequired()) {

                say("Sending p2p reply " + _poolMessage);
                try {
                    say("CellMessage before revert : " + _cellMessage);
                    _cellMessage.revertDirection();
                    say("CellMessage after revert : " + _cellMessage);
                    sendMessage(_cellMessage);
                } catch (NotSerializableException ee) {
                    throw new RuntimeException("Bug detected: Unserializable vehicle", ee);
                } catch (NoRouteToCellException ee) {
                    esay("Cannot reply p2p message : " + ee.getMessage());
                }
            }

        }

    }

    private void runPool2PoolClient(final CellMessage cellMessage,
                                    final Pool2PoolTransferMsg poolMessage) {

        String poolName = poolMessage.getPoolName();
        PnfsId pnfsId = poolMessage.getPnfsId();
        StorageInfo storageInfo = poolMessage.getStorageInfo();
        try {

            CacheFileAvailable callback = new CompanionFileAvailableCallback(
                                                                             cellMessage, poolMessage, pnfsId);

            _p2pClient.newCompanion(pnfsId, poolName, storageInfo, callback);

        } catch (Exception ee) {
            esay("Exception from _p2pClient.newCompanion of : " + pnfsId
                 + " : " + ee);
            if (ee instanceof FileInCacheException) {
                poolMessage.setReply(0, null);
            } else {
                poolMessage.setReply(102, ee);
            }
            try {
                say("Sending p2p reply " + poolMessage);
                cellMessage.revertDirection();
                sendMessage(cellMessage);
            } catch (Exception eee) {
                esay("Can't reply p2p message : " + eee);
            }
        }

    }

    @Override
    public void messageArrived(CellMessage cellMessage) {
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

            if (msg.isPool2Pool() && msg.isReply()) {

                say("Pool2PoolIoFileMsg delivered to p2p Client");
                _p2pClient.messageArrived(poolMessage, cellMessage);

            } else {

                say("PoolIoFileMessage delivered to ioFile (method)");
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
                ioFile((PoolIoFileMessage) poolMessage, cellMessage);

            }

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

            if( _poolMode.isDisabled(PoolV2Mode.DISABLED) ||
                _poolMode.isDisabled(PoolV2Mode.DISABLED_FETCH)){

                esay("PoolCheckable Request rejected due to " + _poolMode);
                sentNotEnabledException(poolMessage, cellMessage);
                return;

            }

            if (poolMessage instanceof PoolFileCheckable) {
                checkFile((PoolFileCheckable) poolMessage);
                poolMessage.setSucceeded();
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

        } else if (poolMessage instanceof PoolSpaceReservationMessage) {

            replyRequired = false;
            runSpaceReservation((PoolSpaceReservationMessage)poolMessage,
                                cellMessage);

        } else {
            say("Unexpected message class 2" + poolMessage.getClass());
            say(" isReply = " + ( poolMessage).isReply()); // REMOVE
            say(" source = " + cellMessage.getSourceAddress());
            return;
        }
        if (!replyRequired)
            return;
        try {
            say("Sending reply " + poolMessage);
            cellMessage.revertDirection();
            sendMessage(cellMessage);
        } catch (NotSerializableException e) {
            throw new RuntimeException("Bug detected: Unserializable vehicle", e);
        } catch (NoRouteToCellException e) {
            esay("Cannot reply message : " + e.getMessage());
        }
    }

    private void runSpaceReservation(final PoolSpaceReservationMessage spaceReservationMessage,
                                     final CellMessage cellMessage) {

        if (_blockOnNoSpace
            && (spaceReservationMessage instanceof PoolReserveSpaceMessage)) {
            getNucleus().newThread(new Runnable() {
                    public void run() {
                        say("Reservation job started");
                        spaceReservation(spaceReservationMessage, cellMessage);
                        say("Reservation job finished");
                    }
                }, "reservationThread").start();
        } else {

            spaceReservation(spaceReservationMessage, cellMessage);

        }

    }

    private void spaceReservation(
                                  PoolSpaceReservationMessage spaceReservationMessage,
                                  CellMessage cellMessage) {

        try {

            if (spaceReservationMessage instanceof PoolReserveSpaceMessage) {

                PoolReserveSpaceMessage reserve = (PoolReserveSpaceMessage) spaceReservationMessage;

                _repository.reserveSpace(reserve.getSpaceReservationSize(),
                                         _blockOnNoSpace);

            } else if (spaceReservationMessage instanceof PoolFreeSpaceReservationMessage) {

                _repository
                    .freeReservedSpace(((PoolFreeSpaceReservationMessage) spaceReservationMessage)
                                       .getFreeSpaceReservationSize());

            } else if (spaceReservationMessage instanceof PoolQuerySpaceReservationMessage) {
            }

            spaceReservationMessage.setReservedSpace(_repository
                                                     .getReservedSpace());

        } catch (CacheException ce) {
            spaceReservationMessage.setFailed(ce.getRc(), ce.getMessage());
        } catch (MissingResourceException mre) {
            spaceReservationMessage.setFailed(104, mre);
        } catch (Exception ee) {
            spaceReservationMessage.setFailed(101, ee.toString());
        }
        try {
            say("Sending reply " + spaceReservationMessage);
            cellMessage.revertDirection();
            sendMessage(cellMessage);
        } catch (NotSerializableException e) {
            throw new RuntimeException("Bug detected: Unserializable vehicle", e);
        } catch (NoRouteToCellException e) {
            esay("Cannot reply message : " + e.getMessage());
        }

    }

    private void getRepositoryListing(PoolQueryRepositoryMsg queryMessage) {
        queryMessage.setReply(new RepositoryCookie(),
                _repository.getValidCacheRepostoryEntryInfoList());
    }

    private void sentNotEnabledException(Message poolMessage,
                                         CellMessage cellMessage) {
        try {
            say("Sending reply " + poolMessage);
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
        esay("New Pool Mode : " + _poolMode);
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
        esay("New Pool Mode : " + _poolMode);
    }

    /**
     * Performs basic sanity checks on space accounting. Problems are
     * logged.
     *
     * @return true when all checks pass, false otherwise.
     */
    private boolean checkSpaceAccounting()
    {
        long removable = _sweeper.getRemovableSpace();
        long total = _repository.getTotalSpace();
        long free = _repository.getFreeSpace();
        long precious = _repository.getPreciousSpace();
        long used = total - free;

        if (removable < 0) {
            esay("Removable space is negative.");
            return false;
        }

        if (total < 0) {
            esay("Repository size is negative.");
            return false;
        }

        if (free < 0) {
            esay("Free space is negative.");
            return false;
        }

        if (precious < 0) {
            esay("Precious space is negative.");
            return false;
        }

        if (used < 0) {
            esay("Used space is negative.");
            return false;
        }

        /* The following check cannot be made consistently, since we
         * do not retrieve these values atomically. Therefore we log
         * the error, but do not return false.
         */
        if (precious + removable > used) {
            esay("Used space is less than the sum of precious and removable space (this may be a temporary problem - if it persists then please report it to support@dcache.org).");
        }

        return true;
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
            while (!Thread.interrupted()) {

                if (!_poolMode.isDisabled(PoolV2Mode.DISABLED_STRICT)
                    && _checkRepository) {

                    if (!_repository.isRepositoryOk()) {
                        esay("Pool disabled due to problems in repository") ;
                        disablePool(PoolV2Mode.DISABLED_STRICT, 99,
                                    "Pool disabled due to problems in repository");
                    }

                    if (!checkSpaceAccounting()) {
                        esay("Marking pool read-only due to accounting errors. This is a bug. Please report it to support@dcache.org.");
                        disablePool(PoolV2Mode.DISABLED_RDONLY, 99,
                                    "Pool is read-only due to accounting errors");
                    }
		}
                sendPoolManagerMessage(true);

                try {
                    Thread.sleep(_heartbeat*1000);
                } catch(InterruptedException e) {
                    esay("Ping Thread was interrupted");
                    break;
                }
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

            if( _logPoolMonitor.isDebugEnabled() ) {
                _logPoolMonitor.debug(_poolName + " - sending poolUpMessage (mode/serialId): " + _poolMode + " / " + _serialId);
            }
            poolManagerMessage.setTagMap( _tags ) ;
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

    private PoolCostInfo getPoolCostInfo() {

        PoolCostInfo info = new PoolCostInfo(_poolName);

        info.setSpaceUsage(_repository.getTotalSpace(), _repository
                           .getFreeSpace(), _repository.getPreciousSpace(), _sweeper
                           .getRemovableSpace(), _sweeper.getLRUSeconds());

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
            for (Iterator<JobScheduler> it = manager.scheduler(); it.hasNext();) {
                JobScheduler js = it.next();
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

    public String ac_set_breakeven_$_0_1(Args args) {
        if (args.argc() > 0)
            _breakEven = Double.parseDouble(args.argv(0));
        return "BreakEven = " + _breakEven;
    }

    public String hh_get_cost = " [filesize] # get space and performance cost";

    public String ac_get_cost_$_0_1(Args args) {
        return "DEPRICATED # cost now solely calculated in PoolManager";
        /*
         * long filesize = 0 ; if( args.argc() > 0 )filesize =
         * Long.parseLong(args.argv(0));
         *
         * PoolCheckCostMessage m = new PoolCheckCostMessage(
         * _nucleus.getCellName() , filesize ) ;
         *
         * checkCost( m ) ; return m.toString() ;
         */
    }

    private double _breakEven = 250.0;

    /*
     * private CostCalculationEngine _costCalculationEngine = new
     * CostCalculationEngine("V5") ;
     *
     * private void checkCost( PoolCostCheckable poolMessage ) {
     *
     * CostCalculatable cost = _costCalculationEngine.getCostCalculatable(
     * getPoolCostInfo() ) ;
     *
     * cost.recalculate( poolMessage.getFilesize() ) ;
     *
     *
     * if( _simSpaceCost > (double)(-1.0) ){
     *
     * poolMessage.setSpaceCost( _simSpaceCost ) ;
     *
     * }else if( ! _isPermanent ){
     *
     * poolMessage.setSpaceCost( cost.getSpaceCost() ) ;
     *
     * }else{
     *
     * poolMessage.setSpaceCost( (double)200000000.0 );
     *  }
     *
     * if( _simCpuCost > (double) (-1.0))
     *
     * poolMessage.setPerformanceCost( _simCpuCost ) ;
     *
     *
     * else
     *
     * poolMessage.setPerformanceCost( cost.getSpaceCost() );
     *
     * poolMessage.setSucceeded(); say("checking cost for
     * PoolCheckCostMessage["+poolMessage+"]"); }
     *
     *
     */

    private synchronized void removeFiles(PoolRemoveFilesMessage poolMessage) {

        String[] fileList = poolMessage.getFiles();
        int counter = 0;
        for (int i = 0; i < fileList.length; i++) {

            synchronized (_repository) {
                try {
                    String pnfsIdString = fileList[i];
                    if ((pnfsIdString == null) || (pnfsIdString.length() == 0)) {
                        esay("removeFiles : invalid syntax in remove filespec >"
                             + pnfsIdString + "<");
                        continue;
                    }
                    PnfsId pnfsId = new PnfsId(fileList[i]);
                    CacheRepositoryEntry entry = _repository.getEntry(pnfsId);
                    if ((!_cleanPreciousFiles) && (_lfsMode == LFS_NONE)
                        && (entry.isPrecious())) {
                        counter++;
                        say("removeFiles : File " + fileList[i]
                            + " kept. (precious)");
                    } else if (!_repository.removeEntry(_repository
							.getEntry(pnfsId))) {
                        //
                        // the entry couldn't be removed because the
                        // file is still in an unstable phase.
                        //
                        counter++;
                        say("removeFiles : File " + fileList[i]
                            + " kept. (locked)");
                    } else {
                        say("removeFiles : File " + fileList[i] + " deleted.");
                        fileList[i] = null;
                    }
                } catch (FileNotInCacheException fce) {
                    esay("removeFiles : File " + fileList[i] + " delete CE : "
                         + fce.getMessage());
                    fileList[i] = null; // let them remove it
                } catch (CacheException ce) {
                    counter++;
                    say("removeFiles : File " + fileList[i] + " kept. CE : "
                        + ce.getMessage());
                }
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
    private class HybridInventory implements Runnable {
        private boolean _activate = true;

        public HybridInventory(boolean activate) {
            _activate = activate;
            _nucleus.newThread(this, "HybridInventory").start();
        }

        public void run() {
            _hybridCurrent = 0;

            long startTime, stopTime;
            say("HybridInventory started. _activate="+_activate);
            startTime = System.currentTimeMillis();

            for (PnfsId pnfsid : _repository.getValidPnfsidList()) {
                if (Thread.interrupted())
                    break;
                _hybridCurrent++;
                if (_activate)
                    _pnfs.addCacheLocation(pnfsid.toString());
                else
                    _pnfs.clearCacheLocation(pnfsid.toString());
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

    public String ac_pnfs_register(Args args) {
        synchronized (_hybridInventoryLock) {
            if (_hybridInventoryActive)
                throw new IllegalArgumentException(
                                                   "Hybrid inventory still active");
            _hybridInventoryActive = true;
            new HybridInventory(true);
        }
        return "";
    }

    public String ac_pnfs_unregister(Args args) {
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

    public String ac_run_hybrid_inventory(Args args) {
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

    public String ac_pf_$_1(Args args) throws Exception {
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

    public String ac_set_replication_$_1(Args args) {
        String mode = args.argv(0);
        _replicationHandler.init(mode);
        return _replicationHandler.toString();
    }

    public String hh_pool_inventory = "DEBUG ONLY";

    public String ac_pool_inventory(Args args)
        throws CacheException
    {
        final StringBuffer sb = new StringBuffer();
        Logable l = new Logable() {
                public void log(String msg) {
                    _logClass.log(msg);
                };

                public void elog(String msg) {
                    sb.append(msg).append("\n");
                    _logClass.elog(msg);
                }

                public void plog(String msg) {
                    sb.append(msg).append("\n");
                    _logClass.plog(msg);
                }
            };
        _repository.runInventory(l, _pnfs, _recoveryFlags);
        return sb.toString();

    }

    public String hh_pool_suppress_hsmload = "on|off";

    public String ac_pool_suppress_hsmload_$_1(Args args) {
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
    public String hh_movermap_undefine = "<protocol>-<major>";
    public String hh_movermap_ls = "";

    public String ac_movermap_define_$_2(Args args) throws Exception {
        _moverHash.put(args.argv(0), Class.forName(args.argv(1)));
        return "";
    }

    public String ac_movermap_undefine_$_1(Args args) {
        _moverHash.remove(args.argv(0));
        return "";
    }

    public String ac_movermap_ls(Args args) {
        StringBuilder sb = new StringBuilder();

        for (Map.Entry<String, Class<?>> entry: _moverHash.entrySet()) {

            sb.append(entry.getKey()).append(" -> ").append(
                                                            entry.getValue().getName()).append("\n");
        }
        return sb.toString();
    }

    public String hh_pool_lfs = "none|precious|volatile # FOR DEBUG ONLY";

    public String ac_pool_lfs_$_1(Args args) throws CommandSyntaxException {
        String mode = args.argv(0);
        if (mode.equals("none")) {
            _lfsMode = LFS_NONE;
        } else if (mode.equals("precious")) {
            _lfsMode = LFS_PRECIOUS;
        } else if (mode.equals("volatile")) {
            _lfsMode = LFS_VOLATILE;
        } else {
            throw new CommandSyntaxException("Not Found : ",
                                             "Usage : pool lfs none|precious|volatile");
        }
        return "";
    }

    public String hh_set_duplicate_request = "none|ignore|refresh";

    public String ac_set_duplicate_request_$_1(Args args)
        throws CommandSyntaxException {
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

    public String ac_set_p2p_$_1(Args args) throws CommandSyntaxException {
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

    public String ac_pool_disablemode_$_1(Args args) {
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

    public String ac_pool_disable_$_0_2(Args args) {

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

    public String ac_pool_enable(Args args) {
        if (_poolMode.isDisabled(PoolV2Mode.DISABLED_DEAD))
            return "The pool is dead and a restart is required to enable it";
        enablePool();
        return "Pool " + _poolName + " enabled";
    }

    public String hh_set_max_movers = "!!! Please use 'mover|st|rh set max active <jobs>'";

    public String ac_set_max_movers_$_1(Args args)
        throws IllegalArgumentException {
        int num = Integer.parseInt(args.argv(0));
        if ((num < 0) || (num > 10000))
            throw new IllegalArgumentException("Not in range (0...10000)");
        return "Please use 'mover|st|rh set max active <jobs>'";

    }

    public String hh_set_gap = "<always removable gap>/size[<unit>] # unit = k|m|g";

    public String ac_set_gap_$_1(Args args) {
        _gap = UnitInteger.parseUnitLong(args.argv(0));
        return "Gap set to " + _gap;
    }

    public String hh_set_report_remove = "on|off";

    public String ac_set_report_remove_$_1(Args args)
        throws CommandSyntaxException {
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

    public String ac_crash_$_0_1(Args args) throws IllegalArgumentException {
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

    public String ac_set_sticky_$_0_1(Args args) {
        if (args.argc() > 0) {
            String mode = args.argv(0);
            if (mode.equals("allowed")) {
                _allowSticky = true;
            } else if (mode.equals("denied")) {
                _allowSticky = false;
            } else
                throw new IllegalArgumentException("set sticky allowed|denied");
        }
        _storageHandler.setStickyAllowed(_allowSticky);
        return "Sticky Bit " + (_allowSticky ? "allowed" : "denied");
    }

    public String hh_set_max_diskspace = "<space>[<unit>] # unit = k|m|g";

    public String ac_set_max_diskspace_$_1(Args args) {

        long maxDisk = UnitInteger.parseUnitLong(args.argv(0));
        _repository.setTotalSpace(maxDisk);
        say("set maximum diskspace =" + UnitInteger.toUnitString(maxDisk));
        return "";
    }

    public String hh_set_cleaning_interval = "<interval/sec>";

    public String ac_set_cleaning_interval_$_1(Args args) {
        _cleaningInterval = Integer.parseInt(args.argv(0));
        say("_cleaningInterval=" + _cleaningInterval);
        return "";
    }

    public String hh_set_flushing_interval = "DEPRECATED (use flush set interval <time/sec>)";

    public String ac_set_flushing_interval_$_1(Args args) {
        return "DEPRECATED (use flush set interval <time/sec>)";
    }

    public String hh_flush_class = "<hsm> <storageClass> [-count=<count>]";

    public String ac_flush_class_$_2(Args args) {
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
        CacheRepositoryEntry entry = _repository.getEntry(new PnfsId(args
                                                                     .argv(0)));
        _storageHandler.store(entry, null);
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
        StringBuffer sb = new StringBuffer();
        IoQueueManager manager = (IoQueueManager) _ioQueue;

        if (args.getOpt("l") != null) {
            for (Iterator<JobScheduler> it = manager.scheduler(); it.hasNext();) {
                JobScheduler js = it.next();
                sb.append(js.getSchedulerName()).append(" ").append(
                                                                    js.getActiveJobs()).append(" ").append(
                                                                                                           js.getMaxActiveJobs()).append(" ").append(
                                                                                                                                                     js.getQueueSize()).append("\n");
            }
        } else {
            for (Iterator<JobScheduler> it = manager.scheduler(); it.hasNext();) {
                sb.append((it.next()).getSchedulerName())
                    .append("\n");
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
            StringBuffer sb = new StringBuffer();
            for (Iterator<JobScheduler> it = manager.scheduler(); it.hasNext();) {
                JobScheduler js = it.next();
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
