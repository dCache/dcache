package diskCacheV111.services;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jdo.PersistenceManager;
import javax.jdo.PersistenceManagerFactory;
import javax.jdo.Transaction;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.DoorTransferFinishedMessage;
import diskCacheV111.vehicles.IpProtocolInfo;
import diskCacheV111.vehicles.transferManager.CancelTransferMessage;
import diskCacheV111.vehicles.transferManager.TransferManagerMessage;
import diskCacheV111.vehicles.transferManager.TransferStatusQueryMessage;

import dmg.cells.nucleus.AbstractCellComponent;
import dmg.cells.nucleus.CellCommandListener;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellMessageReceiver;
import dmg.cells.nucleus.SerializationException;
import dmg.util.TimebasedCounter;

import org.dcache.cells.CellStub;
import org.dcache.util.Args;
import org.dcache.util.CDCExecutorServiceDecorator;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;


/**
 * Base class for services that transfer files on behalf of SRM. Used to
 * implement server-side srmCopy.
 */
public abstract class TransferManager extends AbstractCellComponent
                                      implements CellCommandListener,
                                                 CellMessageReceiver
{
    private static final Logger log = LoggerFactory.getLogger(TransferManager.class);
    private final Map<Long, TransferManagerHandler> _activeTransfers =
            new ConcurrentHashMap<>();
    private int _maxTransfers;
    private int _numTransfers;
    private long _moverTimeout;
    private TimeUnit _moverTimeoutUnit;
    protected static long nextMessageID;
    private String _tLogRoot;
    private CellStub _pnfsManager;
    private CellStub _poolManager;
    private CellStub _poolStub;
    private CellStub _billingStub;
    private boolean _overwrite;
    private int _maxNumberOfDeleteRetries;
    // this is the timer which will timeout the
    // transfer requests
    private final Timer _moverTimeoutTimer = new Timer("Mover timeout timer", true);
    private final Map<Long, TimerTask> _moverTimeoutTimerTasks =
            new ConcurrentHashMap<>();
    private String _ioQueueName; // multi io queue option
    private TimebasedCounter idGenerator = new TimebasedCounter();
    public final Set<PnfsId> justRequestedIDs = new HashSet<>();
    private String _poolProxy;
    private ExecutorService executor =
            new CDCExecutorServiceDecorator<>(Executors.newCachedThreadPool());
    private PersistenceManagerFactory _pmf;

    public void cleanUp()
    {
        executor.shutdown();
    }

    @Override
    public void getInfo(PrintWriter pw)
    {
        pw.printf("    %s\n", getClass().getName());
        pw.println("---------------------------------");
        pw.printf("Name   : %s\n", getCellName());
        if (doDbLogging()) {
            pw.println("dblogging=true");
        } else {
            pw.println("dblogging=false");
        }
        if (idGenerator != null) {
            pw.println("TransferID is generated using Data Base");
        } else {
            pw.println("TransferID is generated w/o DB access");
        }
        pw.printf("number of active transfers : %d\n", _numTransfers);
        pw.printf("max number of active transfers  : %d\n", getMaxTransfers());
        pw.printf("PoolManager  : %s\n", _poolManager);
        pw.printf("PoolManager timeout : %d seconds\n", MILLISECONDS.toSeconds(_poolManager.getTimeoutInMillis()));
        pw.printf("PnfsManager timeout : %d seconds\n", MILLISECONDS.toSeconds(_pnfsManager.getTimeoutInMillis()));
        pw.printf("Pool timeout  : %d seconds\n", MILLISECONDS.toSeconds(_poolStub.getTimeoutInMillis()));
        pw.printf("next id  : %d seconds\n", nextMessageID);
        pw.printf("io-queue  : %s\n", _ioQueueName);
        pw.printf("maxNumberofDeleteRetries  : %d\n", _maxNumberOfDeleteRetries);
        pw.printf("Pool Proxy : %s\n",
                (_poolProxy == null ? "not set" : ("set to " + _poolProxy)));
    }

    public String ac_set_maxNumberOfDeleteRetries_$_1(Args args)
    {
        _maxNumberOfDeleteRetries = Integer.parseInt(args.argv(0));
        return "setting maxNumberOfDeleteRetries " + _maxNumberOfDeleteRetries;
    }

    public final static String hh_set_tlog = "<direcory for ftp logs or \"null\" for none>";

    public String ac_set_tlog_$_1(Args args)
    {
        _tLogRoot = args.argv(0);
        if (_tLogRoot.equals("null")) {
            _tLogRoot = null;
            return "remote ftp transaction logging is off";
        }
        return "remote ftp transactions will be logged to " + _tLogRoot;
    }

    public final static String hh_set_max_transfers = "<#max transfers>";

    public String ac_set_max_transfers_$_1(Args args)
    {
        int max = Integer.parseInt(args.argv(0));
        if (max <= 0) {
            return "Error, max transfers number should be greater then 0 ";
        }
        setMaxTransfers(max);
        return "set maximum number of active transfers to " + max;
    }

    public final static String hh_set_pool_timeout = "<#seconds>";

    public String ac_set_pool_timeout_$_1(Args args)
    {
        int timeout = Integer.parseInt(args.argv(0));
        if (timeout <= 0) {
            return "Error, pool timeout should be greater then 0 ";
        }
        _poolStub.setTimeout(timeout);
        _poolStub.setTimeoutUnit(SECONDS);
        return "set pool timeout to " + timeout + " seconds";
    }

    public final static String hh_set_pool_manager_timeout = "<#seconds>";

    public String ac_set_pool_manager_timeout_$_1(Args args)
    {
        int timeout = Integer.parseInt(args.argv(0));
        if (timeout <= 0) {
            return "Error, pool manger timeout should be greater then 0 ";
        }
        _poolManager.setTimeout(timeout);
        _poolManager.setTimeoutUnit(SECONDS);
        return "set pool manager timeout to " + timeout + " seconds";
    }

    public final static String hh_set_pnfs_manager_timeout = "<#seconds>";

    public String ac_set_pnfs_manager_timeout_$_1(Args args)
    {
        int timeout = Integer.parseInt(args.argv(0));
        if (timeout <= 0) {
            return "Error, pnfs manger timeout should be greater then 0 ";
        }
        _pnfsManager.setTimeout(timeout);
        _pnfsManager.setTimeoutUnit(SECONDS);
        return "set pnfs manager timeout to " + timeout + " seconds";
    }

    public final static String hh_ls = "[-l] [<#transferId>]";

    public String ac_ls_$_0_1(Args args)
    {
        boolean long_format = args.hasOption("l");
        if (args.argc() > 0) {
            long id = Long.parseLong(args.argv(0));
            TransferManagerHandler handler = _activeTransfers.get(id);
            if (handler == null) {
                return "ID not found : " + id;
            }
            return " transfer id=" + id + " : " + handler.toString(long_format);
        }
        StringBuilder sb = new StringBuilder();
        if (_activeTransfers.isEmpty()) {
            return "No Active Transfers";
        }
        sb.append("  Active Transfers ");
        for (Map.Entry<Long, TransferManagerHandler> e : _activeTransfers.entrySet()) {
            sb.append("\n#").append(e.getKey());
            sb.append(" ").append(e.getValue().toString(long_format));
        }
        return sb.toString();
    }

    public final static String hh_kill = " id";

    public String ac_kill_$_1(Args args)
    {
        long id = Long.parseLong(args.argv(0));
        TransferManagerHandler handler = _activeTransfers.get(id);
        if (handler == null) {
            return "transfer not found: " + id;
        }
        handler.cancel("triggered by admin");
        return "request sent to kill the mover on pool\n";
    }

    public final static String hh_killall = " [-p pool] pattern [pool] \n"
            + " for example killall .* ketchup will kill all transfers with movers on the ketchup pool";

    public String ac_killall_$_1_2(Args args)
    {
        try {
            Pattern p = Pattern.compile(args.argv(0));
            String pool = null;
            if (args.argc() > 1) {
                pool = args.argv(1);
            }
            List<TransferManagerHandler> handlersToKill =
                    new ArrayList<>();
            for (Map.Entry<Long, TransferManagerHandler> e : _activeTransfers.entrySet()) {
                long id = e.getKey();
                TransferManagerHandler handler = e.getValue();
                Matcher m = p.matcher(String.valueOf(id));
                if (m.matches()) {
                    log.debug("pattern: \"{}\" matches id=\"{}\"", args.argv(0), id);
                    if (pool != null && pool.equals(handler.getPool())) {
                        handlersToKill.add(handler);
                    } else if (pool == null) {
                        handlersToKill.add(handler);
                    }
                } else {
                    log.debug("pattern: \"{}\" does not match id=\"{}\"", args.argv(0), id);
                }
            }
            if (handlersToKill.isEmpty()) {
                return "no active transfers match the pattern and the pool";
            }
            StringBuilder sb = new StringBuilder("Killing these transfers: \n");
            for (TransferManagerHandler handler : handlersToKill) {
                handler.cancel("triggered by admin");
                sb.append(handler.toString(true)).append('\n');
            }
            return sb.toString();
        } catch (Exception e) {
            log.error(e.toString());
            return e.toString();
        }
    }

    public final static String hh_set_io_queue = "<io-queue name >";

    public String ac_set_io_queue_$_1(Args args)
    {
        String newIoQueueName = args.argv(0);
        if (newIoQueueName.equals("null")) {
            _ioQueueName = null;
            return "io-queue is set to null";
        }
        _ioQueueName = newIoQueueName;
        return "io_queue was set to " + _ioQueueName;
    }

    public void messageArrived(CellMessage envelope, DoorTransferFinishedMessage message)
    {
        long id = message.getId();
        TransferManagerHandler h = getHandler(id);
        if (h != null) {
            h.poolDoorMessageArrived(message);
        } else {
            log.error("cannot find handler with id={} for DoorTransferFinishedMessage", id);
        }
    }

    public CancelTransferMessage messageArrived(CellMessage envelope, CancelTransferMessage message)
    {
        long id = message.getId();
        TransferManagerHandler h = getHandler(id);
        if (h != null) {
            String explanation = message.getExplanation();
            h.cancel(explanation != null ? explanation : "at the request of door");
        } else {
            // FIXME: shouldn't this throw an exception?
            log.error("cannot find handler with id={} for CancelTransferMessage", id);
        }
        return message;
    }

    public TransferManagerMessage messageArrived(CellMessage envelope, TransferManagerMessage message)
            throws CacheException
    {
        if (!newTransfer()) {
            throw new CacheException(TransferManagerMessage.TOO_MANY_TRANSFERS, "too many transfers!");
        }
        new TransferManagerHandler(this, message, envelope.getSourcePath().revert(), executor).handle();
        return message;
    }

    public Object messageArrived(CellMessage envelope, TransferStatusQueryMessage message)
    {
        TransferManagerHandler handler = getHandler(message.getId());

        if (handler == null) {
            message.setState(TransferManagerHandler.UNKNOWN_ID);
            return message;
        }

        return handler.appendInfo(message);
    }

    public int getMaxTransfers()
    {
        return _maxTransfers;
    }

    public void setMaxTransfers(int max_transfers)
    {
        _maxTransfers = max_transfers;
    }

    private synchronized boolean newTransfer()
    {
        log.debug("newTransfer() num_transfers = {} max_transfers={}",
                _numTransfers, _maxTransfers);
        if (_numTransfers == _maxTransfers) {
            log.debug("newTransfer() returns false");
            return false;
        }
        log.debug("newTransfer() INCREMENT and return true");
        _numTransfers++;
        return true;
    }

    synchronized void finishTransfer()
    {
        log.debug("finishTransfer() num_transfers = {} DECREMENT", _numTransfers);
        _numTransfers--;
    }

    public synchronized long getNextMessageID()
    {
        if (idGenerator != null) {
            try {
                nextMessageID = idGenerator.next();
            } catch (Exception e) {
                log.error("Having trouble getting getNextMessageID from DB");
                log.error(e.toString());
                log.error("will nullify requestsPropertyStorage");
                idGenerator = null;
                getNextMessageID();
            }
        } else {
            if (nextMessageID == Long.MAX_VALUE) {
                nextMessageID = 0;
                return Long.MAX_VALUE;
            }
            return nextMessageID++;
        }
        return nextMessageID;
    }

    protected abstract IpProtocolInfo getProtocolInfo(TransferManagerMessage transferRequest);

    protected TransferManagerHandler getHandler(long handlerId)
    {
        return _activeTransfers.get(handlerId);
    }

    public void startTimer(final long id)
    {
        TimerTask task = new TimerTask()
        {
            @Override
            public void run()
            {
                log.error("timer for handler " + id + " has expired, killing");
                Object o = _moverTimeoutTimerTasks.remove(id);
                if (o == null) {
                    log.error("TimerTask.run(): timer task for handler Id={} not found in moverTimoutTimerTasks hashtable", id);
                    return;
                }
                TransferManagerHandler handler = getHandler(id);
                if (handler == null) {
                    log.error("TimerTask.run(): timer task for handler Id={} could not find handler !!!", id);
                    return;
                }
                handler.timeout();
            }
        };

        _moverTimeoutTimerTasks.put(id, task);

        // this is very approximate
        // but we do not need hard real time
        _moverTimeoutTimer.schedule(task, _moverTimeoutUnit.toMillis(_moverTimeout));
    }

    public void stopTimer(long id)
    {
        TimerTask tt = _moverTimeoutTimerTasks.remove(id);
        if (tt == null) {
            log.error("stopTimer(): timer not found for Id={}", id);
            return;
        }
        log.debug("canceling the mover timer for handler id {}", id);
        tt.cancel();
    }

    public void addActiveTransfer(long id, TransferManagerHandler handler) {
        _activeTransfers.put(id, handler);
        if (doDbLogging()) {
            PersistenceManager pm = _pmf.getPersistenceManager();
            try {
                Transaction tx = pm.currentTransaction();
                try {
                    tx.begin();
                    pm.makePersistent(handler);
                    tx.commit();
                    log.debug("Recording new handler into database.");
                } catch (Exception e) {
                    log.error(e.toString());
                } finally {
                        rollbackIfActive(tx);
                }
            } finally {
                    pm.close();
            }
        }
    }

    public void removeActiveTransfer(long id) {
        TransferManagerHandler handler = _activeTransfers.remove(id);
        if (doDbLogging()) {
            PersistenceManager pm = _pmf.getPersistenceManager();
            try {
                Transaction tx = pm.currentTransaction();
                TransferManagerHandlerBackup handlerBackup
                    = new TransferManagerHandlerBackup(handler);
                try {
                    tx.begin();
                    pm.makePersistent(handler);
                    pm.deletePersistent(handler);
                    pm.makePersistent(handlerBackup);
                    tx.commit();
                    log.debug("handler removed from db");
                } catch (Exception e) {
                    log.error(e.toString());
                } finally {
                    rollbackIfActive(tx);
                }
            } finally {
                    pm.close();
            }
        }
    }

    public CellStub getPoolStub()
    {
        return _poolStub;
    }

    public String getLogRootName()
    {
        return _tLogRoot;
    }

    public boolean isOverwrite()
    {
        return _overwrite;
    }

    public CellStub getPoolManagerStub()
    {
        return _poolManager;
    }

    public CellStub getPnfsManagerStub()
    {
        return _pnfsManager;
    }

    public CellStub getBillingStub()
    {
        return _billingStub;
    }

    public String getIoQueueName()
    {
        return _ioQueueName;
    }

    public static void rollbackIfActive(Transaction tx)
    {
        if (tx != null && tx.isActive()) {
            tx.rollback();
        }
    }

    public boolean doDbLogging()
    {
        return _pmf != null;
    }

    public int getMaxNumberOfDeleteRetries()
    {
        return _maxNumberOfDeleteRetries;
    }

    public void persist(Object o) {
        if (doDbLogging()) {
            PersistenceManager pm = _pmf.getPersistenceManager();
            try {
                Transaction tx = pm.currentTransaction();
                try {
                    tx.begin();
                    pm.makePersistent(o);
                    tx.commit();
                    log.debug("[{}]: Recording new state of handler into database.",
                                o);
                } catch (Exception e) {
                    log.error("[{}]: failed to persist object: {}.",
                                o, e.getMessage());
                } finally {
                    rollbackIfActive(tx);
                }
            } finally {
                pm.close();
            }
        }
    }

    public String getCellName() {
       return super.getCellName();
    }

    public String getCellDomainName() {
        return super.getCellDomainName();
    }

    public void sendMessage(CellMessage envelope) throws SerializationException
    {
        super.sendMessage(envelope);
    }

    public String getPoolProxy()
    {
        return _poolProxy;
    }

    public void setBilling(CellStub billingStub) {
        _billingStub = billingStub;
    }

    public void setPoolManager(CellStub poolManager) {
        _poolManager = poolManager;
    }

    public void setPnfsManager(CellStub pnfsManager) {
        _pnfsManager = pnfsManager;
    }

    public void setPool(CellStub pool) {
        _poolStub = pool;
    }

    public void setMoverTimeout(long moverTimeout) {
        _moverTimeout = moverTimeout;
    }

    public void setMoverTimeoutUnit(TimeUnit moverTimeoutUnit) {
        _moverTimeoutUnit = moverTimeoutUnit;
    }

    public void setIoQueueName(String ioQueueName) {
        _ioQueueName = ioQueueName;
    }

    public void setPoolProxy(String poolProxy) {
        _poolProxy = poolProxy;
    }

    public void setMaxNumberOfDeleteRetries(int maxNumberOfDeleteRetries) {
        _maxNumberOfDeleteRetries = maxNumberOfDeleteRetries;
    }

    public void setOverwrite(boolean overwrite) {
        _overwrite = overwrite;
    }

    public void setPersistenceManagerFactory(PersistenceManagerFactory pmf) {
        _pmf = pmf;
    }

    public void setTLogRoot(String tLogRoot) {
        _tLogRoot = tLogRoot;
    }
}
