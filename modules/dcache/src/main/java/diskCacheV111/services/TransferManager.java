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
import dmg.cells.nucleus.CellAddressCore;
import dmg.cells.nucleus.CellCommandListener;
import dmg.cells.nucleus.CellInfoProvider;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellMessageReceiver;
import dmg.cells.nucleus.SerializationException;
import dmg.util.TimebasedCounter;

import org.dcache.cells.CellStub;
import org.dcache.poolmanager.PoolManagerStub;
import org.dcache.util.Args;
import org.dcache.util.CDCExecutorServiceDecorator;

import static java.util.stream.Collectors.joining;


/**
 * Base class for services that transfer files on behalf of SRM. Used to
 * implement server-side srmCopy.
 */
public abstract class TransferManager extends AbstractCellComponent
                                      implements CellCommandListener,
                                                 CellMessageReceiver, CellInfoProvider
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
    private PoolManagerStub _poolManager;
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
    private final ExecutorService executor =
            new CDCExecutorServiceDecorator<>(Executors.newCachedThreadPool());
    private PersistenceManagerFactory _pmf;

    public void cleanUp()
    {
        executor.shutdown();
    }

    @Override
    public void getInfo(PrintWriter pw)
    {
        pw.printf("DB logging            : %b\n", doDbLogging());
        pw.printf("Transfer ID generated : %s\n", idGenerator == null ? "locally" : "from DB");
        pw.printf("Next Transfer ID      : %d\n", nextMessageID);
        pw.printf("Active transfers      : %d\n", _numTransfers);
        pw.printf("Max active transfers  : %d\n", getMaxTransfers());
        pw.printf("Pool manager          : %s\n", _poolManager);
        pw.printf("io-queue              : %s\n", _ioQueueName);
        pw.printf("Max delete retries    : %d\n", _maxNumberOfDeleteRetries);
    }

    public String ac_set_maxNumberOfDeleteRetries_$_1(Args args)
    {
        _maxNumberOfDeleteRetries = Integer.parseInt(args.argv(0));
        return "setting maxNumberOfDeleteRetries " + _maxNumberOfDeleteRetries;
    }

    public static final String hh_set_tlog = "<direcory for ftp logs or \"null\" for none>";

    public String ac_set_tlog_$_1(Args args)
    {
        _tLogRoot = args.argv(0);
        if (_tLogRoot.equals("null")) {
            _tLogRoot = null;
            return "remote ftp transaction logging is off";
        }
        return "remote ftp transactions will be logged to " + _tLogRoot;
    }

    public static final String hh_set_max_transfers_external = "<#max transfers>";

    public String ac_set_max_transfers_external_$_1(Args args)
    {
        int max = Integer.parseInt(args.argv(0));
        if (max <= 0) {
            return "Error, max transfers number should be greater then 0 ";
        }
        setMaxTransfers(max);
        return "set maximum number of active transfers to " + max;
    }

    public static final String hh_ls_external = "[-l] [<#transferId>]";

    public String ac_ls_external_$_0_1(Args args)
    {
        boolean long_format = args.hasOption("l");
        if (args.argc() > 0) {
            long id = Long.parseLong(args.argv(0));
            TransferManagerHandler handler = _activeTransfers.get(id);
            if (handler == null) {
                return "ID not found : " + id;
            }
            return handler.toString(long_format);
        }
        if (_activeTransfers.isEmpty()) {
            return "No active transfers.";
        }
        return _activeTransfers.values().stream()
                .map(h -> h.toString(long_format))
                .collect(joining("\n", "", "\n"));
    }

    public static final String hh_kill = " id";

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

    public static final String hh_killall = " [-p pool] pattern [pool] \n"
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
                    if (pool != null && pool.equals(handler.getPool().getName())) {
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

    public void messageArrived(DoorTransferFinishedMessage message)
    {
        long id = message.getId();
        TransferManagerHandler h = getHandler(id);
        if (h != null) {
            h.poolDoorMessageArrived(message);
        }
    }

    public CancelTransferMessage messageArrived(CancelTransferMessage message)
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

    // TransferStatusQueryMessage is a subclass of
    // TransferManagerMessage, so the code relies on the
    // messageArrived dispatch invoking the method with the most
    // specific signature.  The unused "CellMessage envelope" argument
    // of this method is required because two-argument methods are
    // called preferentially.
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
                TimerTask task = _moverTimeoutTimerTasks.remove(id);
                if (task != null) {
                    TransferManagerHandler handler = getHandler(id);
                    if (handler == null) {
                        log.warn("Transfer {} is (apparently) still ongoing"
                                + " after {} {} but unable to find the transfer"
                                + " to kill it.", id, _moverTimeout,
                                _moverTimeoutUnit);
                        return;
                    }
                    log.warn("Killing transfer {}, which is still ongoing after"
                            + " {} {}.", id, _moverTimeout, _moverTimeoutUnit);
                    handler.timeout();
                }
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
        if (tt != null) {
            log.debug("Cancelling the mover timer for handler id {}", id);
            tt.cancel();
        }
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
        if (handler == null) {
            return;
        }
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

    public PoolManagerStub getPoolManagerStub()
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

    @Override
    public CellAddressCore getCellAddress()
    {
        return super.getCellAddress();
    }

    public void sendMessage(CellMessage envelope) throws SerializationException
    {
        super.sendMessage(envelope);
    }

    public void setBilling(CellStub billingStub) {
        _billingStub = billingStub;
    }

    public void setPoolManager(PoolManagerStub poolManager) {
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
