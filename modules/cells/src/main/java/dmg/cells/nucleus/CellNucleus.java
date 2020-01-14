package dmg.cells.nucleus;

import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Monitor;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.common.util.concurrent.Uninterruptibles;
import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import javax.annotation.Nonnull;

import java.io.FileNotFoundException;
import java.io.Reader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

import dmg.cells.zookeeper.CellCuratorFramework;
import dmg.util.Pinboard;
import dmg.util.logback.FilterThresholdSet;
import dmg.util.logback.RootFilterThresholds;

import org.dcache.util.BoundedCachedExecutor;
import org.dcache.util.BoundedExecutor;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.consumingIterable;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static org.dcache.util.CompletableFutures.fromListenableFuture;
import static org.dcache.util.MathUtils.addWithInfinity;
import static org.dcache.util.MathUtils.subWithInfinity;

/**
 *
 *
 * @author Patrick Fuhrmann
 * @version 0.1, 15 Feb 1998
 */
public class CellNucleus implements ThreadFactory
{
    private static final Logger LOGGER =
            LoggerFactory.getLogger(CellNucleus.class);

    private enum State
    {
        NEW(CellInfo.INITIAL,         false, false, true),
        PRE_STARTUP(CellInfo.ACTIVE,  true,  false, true),
        POST_STARTUP(CellInfo.ACTIVE, true,  true,  true),
        RUNNING(CellInfo.ACTIVE,      false, true,  true),
        FAILED(CellInfo.REMOVING,     false, true,  true),
        STOPPING(CellInfo.REMOVING,   false, true,  false),
        TERMINATED(CellInfo.DEAD,     false, false, false);

        /** State included in CellInfo. */
        int externalState;

        /**
         * Whether the cell is currently processing startup callbacks.
         */
        boolean isStarting;

        /**
         * Whether it is legal for a cell to call {@link #sendMessage(CellMessage,
         * boolean, boolean, boolean, CellMessageAnswerable, Executor, long)}.
         */
        boolean isSendWithCallbackAllowed;

        /**
         * Whether callbacks are guaranteed to be called. At some point
         * during shutdown, the timeout mechanism is stopped and callbacks
         * are no longer called automatically.
         */
        boolean areCallbacksGuaranteed;

        State(int externalState, boolean isStarting, boolean isSendWithCallbackAllowed, boolean areCallbacksGuaranteed)
        {
            this.externalState = externalState;
            this.isStarting = isStarting;
            this.isSendWithCallbackAllowed = isSendWithCallbackAllowed;
            this.areCallbacksGuaranteed = areCallbacksGuaranteed;
        }
    }

    private static final int PINBOARD_DEFAULT_SIZE = 200;

    private static CellGlue __cellGlue;
    private final  String    _cellName;
    private final  String    _cellType;
    private final  ThreadGroup _threads;
    private final  AtomicInteger _threadCounter = new AtomicInteger();
    private final  Cell      _cell;
    private final  Date      _creationTime   = new Date();

    private volatile State _state = State.NEW;

    private final ConcurrentMap<UOID, CellLock> _waitHash = new ConcurrentHashMap<>();
    private String _cellClass;
    private String _cellSimpleClass;

    private final BoundedExecutor _messageExecutor;
    private final AtomicInteger _eventQueueSize = new AtomicInteger();

    /**
     * Timer for periodic low-priority maintenance tasks. Shared among
     * all cell instances. Since a Timer is single-threaded,
     * it is important that the timer is not used for long-running or
     * blocking tasks, nor for time critical tasks.
     */
    private static final ScheduledExecutorService _timer = Executors.newSingleThreadScheduledExecutor(
            new ThreadFactoryBuilder().setDaemon(true).setNameFormat("Cell maintenance task timer").build());

    /**
     * Task for calling the Cell nucleus message timeout mechanism.
     */
    private Future<?> _timeoutTask;

    /**
     * Task starting the cell.
     */
    private ListenableFuture<Void> _startup;

    private Pinboard _pinboard;

    private FilterThresholdSet _loggingThresholds;

    private final BlockingQueue<Runnable> _deferredTasks = new LinkedBlockingQueue<>();

    private volatile long _lastQueueTime;

    private final CellCuratorFramework _curatorFramework;

    private final Monitor _lifeCycleMonitor = new Monitor();

    private final List<CellEventListener> _cellEventListeners = new CopyOnWriteArrayList<>();

    private final Monitor.Guard isNotStarting = new Monitor.Guard(_lifeCycleMonitor)
    {
        @Override
        public boolean isSatisfied()
        {
            return !_state.isStarting;
        }
    };


    public CellNucleus(Cell cell, String name, String type, Executor executor)
    {
        String cellName = name.replace('@', '+');

        if (cellName.isEmpty()) {
            cellName = "*";
        }
        if (cellName.charAt(cellName.length() - 1) == '*') {
            if (cellName.length() == 1) {
                cellName = "$-" + getUnique();
            } else {
                cellName = cellName.substring(0, cellName.length() - 1) + '-' + getUnique();
            }
        }

        _cellName = cellName;
        _cellType    = type;

        _cell = cell;
        Class<? extends Cell> clazz = _cell.getClass();
        _cellClass = clazz.getName();
        if (clazz.isAnonymousClass()) {
            int dot = _cellClass.lastIndexOf('.');
            _cellSimpleClass = dot == -1 ? _cellClass : _cellClass.substring(dot+1);
        } else {
            _cellSimpleClass = clazz.getSimpleName();
        }

        setPinboard(new Pinboard(PINBOARD_DEFAULT_SIZE));

        _threads = new ThreadGroup(__cellGlue.getMasterThreadGroup(), _cellName + "-threads");

        __cellGlue.registerCell(this);

        /* Instantiate management component for log filtering.
         */
        CellNucleus parentNucleus =
                CellNucleus.getLogTargetForCell(MDC.get(CDC.MDC_CELL));
        FilterThresholdSet parentThresholds =
                (parentNucleus.isSystemNucleus() || parentNucleus == this)
                        ? RootFilterThresholds.getInstance()
                        : parentNucleus.getLoggingThresholds();
        setLoggingThresholds(new FilterThresholdSet(parentThresholds));

        _messageExecutor = (executor == null) ? new BoundedCachedExecutor(this, 1) : new BoundedExecutor(executor, 1);

        CuratorFramework curatorFramework = __cellGlue.getCuratorFramework();
        _curatorFramework = new CellCuratorFramework(curatorFramework, _messageExecutor);

        LOGGER.info("Created {}", cellName);
    }

    /**
     * Returns the CellNucleus to which log messages tagged with a
     * given cell are associated.
     */
    public static CellNucleus getLogTargetForCell(String cell)
    {
        CellNucleus nucleus = null;
        if (__cellGlue != null) {
            if (cell != null) {
                nucleus = __cellGlue.getCell(cell);
            }
            if (nucleus == null) {
                nucleus = __cellGlue.getSystemNucleus();
            }
        }
        return nucleus;
    }

    public static Optional<CellNucleus> findForThread(Thread thread)
    {
        return __cellGlue.findCellNucleus(thread);
    }

    public static void initCellGlue(String cellDomainName,
            CuratorFramework curatorFramework, Optional<String> zone, SerializationHandler.Serializer serializer)
    {
        checkState(__cellGlue == null);
        __cellGlue = new CellGlue(cellDomainName, curatorFramework, zone, serializer);
    }

    public static void startCurator()
    {
        __cellGlue.getCuratorFramework().start();
    }

    public static void shutdownCellGlue()
    {
        if (__cellGlue != null) {
            __cellGlue.shutdown();
        }
    }

    boolean isSystemNucleus() {
        return this == __cellGlue.getSystemNucleus();
    }

    public String getCellName() { return _cellName; }
    public String getCellType() { return _cellType; }

    public String getCellClass()
    {
        return _cellClass;
    }

    public void setCellClass(String cellClass)
    {
        _cellClass = cellClass;
    }

    public CellAddressCore getThisAddress() {
        return new CellAddressCore(_cellName, __cellGlue.getCellDomainName());
    }

    public String getCellDomainName() {
        return __cellGlue.getCellDomainName();
    }
    public List<String> getCellNames() { return __cellGlue.getCellNames(); }
    public CellInfo getCellInfo(String name) {
        return __cellGlue.getCellInfo(name);
    }
    public CellInfo getCellInfo() {
        return _getCellInfo();
    }

    public Map<String, Object> getDomainContext()
    {
        return __cellGlue.getCellContext();
    }

    public Reader getDomainContextReader(String contextName)
        throws FileNotFoundException  {
        Object o = __cellGlue.getCellContext(contextName);
        if (o == null) {
            throw new
                    FileNotFoundException("Context not found : " + contextName);
        }
        return new StringReader(o.toString());
    }
    public void   setDomainContext(String contextName, Object context) {
        __cellGlue.getCellContext().put(contextName, context);
    }
    public Object getDomainContext(String str) {
        return __cellGlue.getCellContext(str);
    }

    Cell getThisCell() { return _cell; }

    CellInfo _getCellInfo() {
        CellInfo info = new CellInfo();
        info.setCellName(getCellName());
        info.setDomainName(getCellDomainName());
        info.setCellType(getCellType());
        info.setCreationTime(_creationTime);
        try {
            info.setCellVersion(_cell.getCellVersion());
        } catch(Exception e) {}
        try {
            info.setPrivateInfo(_cell.getInfo());
        } catch(Exception e) {
            info.setPrivateInfo("Not yet/No more available\n");
        }
        try {
            // REVISIT -- many cells simply return their cell-name.  Since
            // this is already represented, in CellInfo, this is unnecessary
            // duplication.
            info.setShortInfo(_cell.toString());
        } catch(Exception e) {
            info.setShortInfo("Not yet/No more available");
        }
        info.setCellClass(_cellClass);
        info.setCellSimpleClass(_cellSimpleClass);
        try {
            int eventQueueSize = getEventQueueSize();
            info.setEventQueueSize(eventQueueSize);
            info.setExpectedQueueTime((eventQueueSize == 0) ? 0 : _lastQueueTime);
            info.setState(_state.externalState);
            info.setThreadCount(_threads.activeCount());
        } catch(Exception e) {
            info.setEventQueueSize(0);
            info.setState(0);
            info.setThreadCount(0);
        }
        return info;
    }

    public void setLoggingThresholds(FilterThresholdSet thresholds)
    {
        _loggingThresholds = thresholds;
    }

    public FilterThresholdSet getLoggingThresholds()
    {
        return _loggingThresholds;
    }

    public synchronized void setPinboard(Pinboard pinboard)
    {
        _pinboard = pinboard;
    }

    public synchronized Pinboard getPinboard()
    {
        return _pinboard;
    }

    public void setMaximumPoolSize(int size)
    {
        _messageExecutor.setMaximumPoolSize(size);
    }

    public int getMaximumPoolSize()
    {
        return _messageExecutor.getMaximumPoolSize();
    }

    public void setMaximumQueueSize(int size)
    {
        _messageExecutor.setMaximumQueueSize(size);
    }

    public int getMaximumQueueSize()
    {
        return _messageExecutor.getMaximumQueueSize();
    }

    public void  sendMessage(CellMessage msg, boolean locally, boolean remotely, boolean shouldAddSource)
        throws SerializationException
    {
        checkArgument(!msg.isFinalDestination(), "Message has no next destination: %s", msg.getDestinationPath());

        if (shouldAddSource) {
            msg.addSourceAddress(getThisAddress());
        }

        EventLogger.sendBegin(msg, "async");
        try {
            __cellGlue.sendMessage(msg, locally, remotely);
        } finally {
            EventLogger.sendEnd(msg);
        }
    }

    /**
     * Sends <code>envelope</code> and waits <code>timeout</code>
     * milliseconds for an answer to arrive.  The answer will bypass
     * the ordinary queuing mechanism and will be delivered before any
     * other asynchronous message.  The answer need to have the
     * getLastUOID set to the UOID of the message send with
     * sendAndWait. If the answer does not arrive withing the specified
     * time interval, the method returns <code>null</code> and the
     * answer will be handled as if it was an ordinary asynchronous
     * message.
     *
     * This method mostly exists for backwards compatibility. dCache code
     * should use CellStub or CellEndpoint.
     *
     * @param envelope the cell message to be sent.
     * @param timeout milliseconds to wait for an answer.
     * @return the answer or null if the timeout was reached.
     * @throws SerializationException if the payload object of this
     *         message is not serializable.
     * @throws NoRouteToCellException if the destination
     *         could not be reached.
     * @throws ExecutionException if an exception was returned.
     */
    public CellMessage sendAndWait(CellMessage envelope, long timeout)
            throws SerializationException, NoRouteToCellException, InterruptedException, ExecutionException
    {
        final SettableFuture<CellMessage> future = SettableFuture.create();
        sendMessage(envelope, true, true,
                    true, new CellMessageAnswerable()
                    {
                        @Override
                        public void answerArrived(CellMessage request, CellMessage answer)
                        {
                            future.set(answer);
                        }

                        @Override
                        public void exceptionArrived(CellMessage request, Exception exception)
                        {
                            future.setException(exception);
                        }

                        @Override
                        public void answerTimedOut(CellMessage request)
                        {
                            future.set(null);
                        }
                    }, directExecutor(), timeout);
        try {
            return future.get(timeout, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            return null;
        } catch (ExecutionException e) {
            Throwables.throwIfInstanceOf(e.getCause(), NoRouteToCellException.class);
            Throwables.throwIfInstanceOf(e.getCause(), SerializationException.class);
            throw e;
        }
    }

    public Map<UOID,CellLock > getWaitQueue()
    {
        return Collections.unmodifiableMap(_waitHash);
    }

    private void executeMaintenanceTasks()
    {
        long now = System.currentTimeMillis();
        _waitHash.entrySet().stream()
                .filter(e -> e.getValue().getTimeout() < now)
                .forEach(e -> timeOutMessage(e.getKey(), e.getValue(), this::reregisterCallback));

        // Execute delayed tasks; since those tasks may themselves add new deferred
        // tasks we limit the operation to the number of tasks we started out with
        // to avoid an infinite loop.
        Iterables.limit(consumingIterable(_deferredTasks), _deferredTasks.size()).forEach(Runnable::run);
    }

    /**
     * Sends <code>msg</code>.
     *
     * The <code>callback</code> argument specifies an object which is informed
     * as soon as an has answer arrived or if the timeout has expired.
     *
     * The callback is run in the supplied executor. The executor may
     * execute the callback inline, but such an executor must only be
     * used if the callback is non-blocking, and the callback should
     * refrain from CPU heavy operations. Care should be taken that
     * the executor isn't blocked by tasks waiting for the callback;
     * such tasks could lead to a deadlock.
     *
     * @param msg the cell message to be sent.
     * @param local whether to attempt delivery to cells in the same domain
     * @param remote whether to attempt delivery to cells in other domains
     * @param shouldAddSource whether to add this cell to the source path
     * @param callback specifies an object class which will be informed
     *                 as soon as the message arrives.
     * @param executor the executor to run the callback in
     * @param timeout  is the timeout in msec.
     * @exception SerializationException if the payload object of this
     *            message is not serializable.
     */
    public void sendMessage(CellMessage msg,
                            boolean local,
                            boolean remote,
                            boolean shouldAddSource,
                            CellMessageAnswerable callback,
                            Executor executor,
                            long timeout)
        throws SerializationException
    {
        checkState(_state.isSendWithCallbackAllowed, "Cannot send message with callback in state {}", _state);
        checkArgument(!msg.isFinalDestination(), "Message has no next destination: %s", msg.getDestinationPath());

        if (shouldAddSource) {
            msg.addSourceAddress(getThisAddress());
        } else {
            checkArgument(msg.getSourcePath().hops() > 0, "Message has no source address.");
        }

        msg.setTtl(timeout);

        UOID uoid = msg.getUOID();
        CellLock lock = new CellLock(msg, callback, executor, timeout);

        EventLogger.sendBegin(msg, "callback");

        /* Ordering here is important - need to insert into waitHash before checking the state
         * to avoid a race with shutdown.
         */
        _waitHash.put(uoid, lock);

        if (!_state.areCallbacksGuaranteed) {
            /* Cell is shutting down so timeout the message.
             */
            timeOutMessage(uoid, lock, (u, l) -> {});
            return;
        }

        try {
            __cellGlue.sendMessage(msg, local, remote);
        } catch (SerializationException e) {
            if (_waitHash.remove(uoid, lock)) {
                EventLogger.sendEnd(msg);
            }
            throw e;
        } catch (RuntimeException e) {
            if (_waitHash.remove(uoid, lock)) {
                try {
                    executor.execute(() -> {
                        try {
                            callback.exceptionArrived(msg, e);
                            EventLogger.sendEnd(msg);
                        } catch (RejectedExecutionException e1) {
                            /* May happen when the callback itself tries to schedule the call
                             * on an executor. Put the request back and let it time out.
                             */
                            LOGGER.error("Failed to invoke callback: {}", e1.toString());
                            reregisterCallback(uoid, lock);
                        }
                    });
                } catch (RejectedExecutionException e1) {
                    /* Put it back and let it time out.
                     */
                    LOGGER.error("Failed to invoke callback: {}", e1.toString());
                    reregisterCallback(uoid, lock);
                }
            } else {
                LOGGER.error("Failed to send message: {}", e.toString());
            }
        }
    }

    public void addCellEventListener(CellEventListener listener)
    {
        _cellEventListeners.add(listener);
    }

    void addToEventQueue(CellEvent ce)
    {
        try {
            _eventQueueSize.incrementAndGet();
            _messageExecutor.execute(new CellEventTask(ce));
        } catch (RejectedExecutionException e) {
            _eventQueueSize.decrementAndGet();
            LOGGER.error("Dropping event: {}", e.getMessage());
        }
    }


    public void consume(String queue) { __cellGlue.consume(this, queue);  }

    public void subscribe(String topic) { __cellGlue.subscribe(this, topic); }

    /**
     *
     * The kill method schedules the specified cell for deletion.
     * The actual remove operation will run in a different
     * thread. So on return of this method the cell may
     * or may not be alive.
     */
    public CompletableFuture<?> kill()
    {
        return __cellGlue.kill(this);
    }

    /**
     *
     * The kill method schedules this Cell for deletion.
     * The actual remove operation will run in a different
     * thread. So on return of this method the cell may
     * or may not be alive.
     */
    public CompletableFuture<?> kill(String cellName)
    {
        return __cellGlue.kill(this, cellName);
    }


    /**
     * Log the threads of some cell.  This is
     * intended for diagnostic information.
     */
    public static void listThreadGroupOf(String cellName) {
        __cellGlue.listThreadGroupOf(cellName);
    }

    /**
     * Log the killer threads.  This is
     * intended for diagnostic information.
     */
    public static void listKillerThreadGroup()
    {
        __cellGlue.listKillerThreadGroup();
    }

    /**
     * Print diagnostic information about currently running
     * threads at warn level.
     */
    public void  threadGroupList() {
        CellGlue.listThreadGroup(_threads);
    }

    /**
     * Blocks until the given cell is dead.
     *
     * @throws InterruptedException if another thread interrupted the
     * current thread before or while the current thread was waiting
     * for a notification. The interrupted status of the current
     * thread is cleared when this exception is thrown.
     * @return True if the cell died, false in case of a timeout.
     */
    public boolean join(String cellName)
        throws InterruptedException
    {
        return __cellGlue.join(cellName, 0);
    }

    /**
     * Blocks until the given cell is dead.
     *
     * @param timeout the maximum time to wait in milliseconds.
     * @throws InterruptedException if another thread interrupted the
     * current thread before or while the current thread was waiting
     * for a notification. The interrupted status of the current
     * thread is cleared when this exception is thrown.
     * @return True if the cell died, false in case of a timeout.
     */
    public boolean join(String cellName, long timeout)
        throws InterruptedException
    {
        return __cellGlue.join(cellName, timeout);
    }

    /**
     * Returns the non-daemon threads of a thread group.
     */
    private Collection<Thread> getNonDaemonThreads(ThreadGroup group)
    {
        Thread[] threads = new Thread[group.activeCount()];
        int count = group.enumerate(threads);
        Collection<Thread> nonDaemonThreads = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            Thread thread = threads[i];
            if (!thread.isDaemon()) {
                nonDaemonThreads.add(thread);
            }
        }
        return nonDaemonThreads;
    }

    /**
     * Waits for at most timeout milliseconds for the termination of a
     * set of threads.
     *
     * @return true if all threads terminated, false otherwise
     */
    private boolean joinThreads(Collection<Thread> threads, long timeout)
        throws InterruptedException
    {
        long deadline = addWithInfinity(System.currentTimeMillis(), timeout);
        for (Thread thread: threads) {
            if (thread.isAlive()) {
                long wait = subWithInfinity(deadline, System.currentTimeMillis());
                if (wait <= 0) {
                    return false;
                }
                thread.join(wait);
                if (thread.isAlive()) {
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * Interrupts a set of threads.
     */
    private void killThreads(Collection<Thread> threads)
    {
        for (Thread thread: threads) {
            if (thread.isAlive()) {
                LOGGER.warn("Forcefully interrupting thread {} during shutdown.", thread.getName());
                thread.interrupt();
            }
        }
    }

    private Runnable wrapLoggingContext(final Runnable runnable)
    {
        return () -> {
            try (CDC ignored = CDC.reset(CellNucleus.this)) {
                runnable.run();
            }
        };
    }

    private <T> Callable<T> wrapLoggingContext(final Callable<T> callable)
    {
        return () -> {
            try (CDC ignored = CDC.reset(CellNucleus.this)) {
                return callable.call();
            }
        };
    }

    /**
     * Submits a task for execution on the message thread.
     */
    <T> Future<T> invokeOnMessageThread(Callable<T> task)
    {
        return _messageExecutor.submit(wrapLoggingContext(task));
    }

    /**
     * Submits a task for execution on the message thread.
     */
    Future<?> invokeOnMessageThread(Runnable task)
    {
        return _messageExecutor.submit(wrapLoggingContext(task));
    }

    void invokeLater(Runnable runnable)
    {
        _deferredTasks.add(wrapLoggingContext(runnable));
    }

    void runDeferredTasksNow()
    {
        _timer.execute(() -> consumingIterable(_deferredTasks).forEach(Runnable::run));
    }

    @Override @Nonnull
    public Thread newThread(@Nonnull Runnable target)
    {
        return newThread(target, getCellName() + '-' + _threadCounter.getAndIncrement());
    }

    @Nonnull
    public Thread newThread(@Nonnull Runnable target, @Nonnull String name)
    {
        return CellGlue.newThread(_threads, wrapLoggingContext(target), name);
    }

    //
    //  package
    //
    Thread [] getThreads(String cellName) {
        return __cellGlue.getThreads(cellName);
    }
    public ThreadGroup getThreadGroup() { return _threads; }
    Thread [] getThreads() {
        if (_threads == null) {
            return new Thread[0];
        }

        int threadCount = _threads.activeCount();
        Thread [] list  = new Thread[threadCount];
        int rc = _threads.enumerate(list);
        if (rc == list.length) {
            return list;
        }
        Thread [] ret = new Thread[rc];
        System.arraycopy(list, 0, ret, 0, rc);
        return ret;
    }

    private String getUnique() {
        return __cellGlue.getUnique();
    }

    int getEventQueueSize()
    {
        return _eventQueueSize.get();
    }

    void addToEventQueue(MessageEvent ce)
    {
        CellMessage msg = ce.getMessage();
        LOGGER.trace("addToEventQueue : message arrived : {}", msg);

        CellLock lock = _waitHash.remove(msg.getLastUOID());
        if (lock != null) {
            //
            // we were waiting for you (sync or async)
            //
            LOGGER.trace("addToEventQueue : lock found for : {}", msg);
            try {
                _eventQueueSize.incrementAndGet();
                lock.getExecutor().execute(new CallbackTask(lock, msg));
            } catch (RejectedExecutionException e) {
                _eventQueueSize.decrementAndGet();
                /* Put it back; the timeout handler will eventually take care of it.
                 */
                LOGGER.error("Dropping reply: {}", e.getMessage());
                reregisterCallback(msg.getLastUOID(), lock);
            }
        } else {
            /* Fail fast for requests if the cell is busy. We consider the cell busy
             * if the last queue time exceeds the TTL of the request.
             */
            if (_eventQueueSize.get() == 0) {
                _lastQueueTime = 0;
            } else if (!msg.isReply()) {
                long queueTime = _lastQueueTime;
                if (msg.getTtl() < queueTime) {
                    CellMessage envelope = new CellMessage(msg.getSourcePath().revert(),
                            new NoRouteToCellException(msg, getCellName() + '@' + getCellDomainName() +
                                                            " is busy (its estimated response time of " +
                                                            queueTime + " ms is longer than the message TTL of " +
                                                            msg.getTtl() + " ms)."));
                    envelope.setLastUOID(msg.getUOID());
                    sendMessage(envelope, true, true, true);
                }
            }

            try {
                EventLogger.queueBegin(ce);
                _eventQueueSize.incrementAndGet();
                _messageExecutor.execute(new DeliverMessageTask(ce));
            } catch (RejectedExecutionException e) {
                EventLogger.queueEnd(ce);
                _eventQueueSize.decrementAndGet();
                LOGGER.error("Dropping message: {}", e.getMessage());
            }
        }
    }

    private void setState(State newState)
    {
        _lifeCycleMonitor.enter();
        try {
            _state = newState;
        } finally {
            _lifeCycleMonitor.leave();
        }
    }

    /**
     * Starts the cell asynchronously.
     *
     * Calls the startup callbacks of the cell, registers the cell with the cell glue and
     * initiates cell message delivery. If startup fails, the cell is torn down.
     *
     * Must only be called once.
     */
    public CompletableFuture<Void> start()
    {
        _lifeCycleMonitor.enter();
        try {
            checkState(_state == State.NEW);
            _state = State.PRE_STARTUP;
            _startup = _messageExecutor.submit(wrapLoggingContext(this::doStart));
        } finally {
            _lifeCycleMonitor.leave();
        }
        return fromListenableFuture(Futures.nonCancellationPropagating(_startup));
    }

    private Void doStart() throws Exception
    {
        try {
            checkState(_state == State.PRE_STARTUP);
            _timeoutTask = _timer.scheduleWithFixedDelay(wrapLoggingContext(this::executeMaintenanceTasks),
                                                         20, 20, TimeUnit.SECONDS);
            StartEvent event = new StartEvent(new CellPath(_cellName), 0);
            try {
                EventLogger.prepareSetupBegin(_cell, event);
                _cell.prepareStartup(event);
            } finally {
                EventLogger.prepareSetupEnd(_cell, event);
            }
            setState(State.POST_STARTUP);
            __cellGlue.publishCell(this);
            try {
                EventLogger.postStartupBegin(_cell, event);
                _cell.postStartup(event);
            } finally {
                EventLogger.postStartupEnd(_cell, event);
            }
            setState(State.RUNNING);
        } catch (Throwable e) {
            setState(State.FAILED);
            __cellGlue.kill(CellNucleus.this);
            throw e;
        }
        return null;
    }

    void shutdown(KillEvent event)
    {
        try (CDC ignored = CDC.reset(CellNucleus.this)) {
            LOGGER.trace("Received {}", event);

            /* Wait for cell initialization to complete to ensure sequential execution of callbacks.
             */
            boolean wasRunning = false;
            _lifeCycleMonitor.enter();
            try {
                if (!_lifeCycleMonitor.waitForUninterruptibly(isNotStarting, 2, TimeUnit.SECONDS)) {
                    _startup.cancel(true);
                    _lifeCycleMonitor.waitForUninterruptibly(isNotStarting);
                }
                State state = _state;
                checkState(state == State.NEW || state == State.RUNNING || state == State.FAILED);
                wasRunning = (state == State.RUNNING);
                _state = State.STOPPING;
            } finally {
                _lifeCycleMonitor.leave();
            }

            /* Stop executing deferred tasks.
             */
            if (_timeoutTask != null) {
                _timeoutTask.cancel(false);
                try {
                    Uninterruptibles.getUninterruptibly(_timeoutTask);
                } catch (CancellationException | ExecutionException ignore) {
                }
            }

            /* Only call prepareRemoval if prepareStartup completed successfully.
             */
            if (wasRunning) {
                try {
                    Uninterruptibles.getUninterruptibly(_messageExecutor.submit(() -> {
                        try {
                            EventLogger.prepareRemovalBegin(_cell, event);
                            _cell.prepareRemoval(event);
                        } finally {
                            EventLogger.prepareRemovalEnd(_cell, event);
                        }}));
                } catch (Throwable e) {
                    Thread t = Thread.currentThread();
                    t.getUncaughtExceptionHandler().uncaughtException(t, e);
                }
            }

            /* Cancel callbacks.
             */
            _waitHash.forEach((uoid, lock) -> timeOutMessage(uoid, lock, (u, l) -> {}));

            /* Shut down message executor.
             */
            if (!MoreExecutors.shutdownAndAwaitTermination(_messageExecutor, 2, TimeUnit.SECONDS)) {
                LOGGER.warn("Failed to flush message queue during shutdown.");
            }

            /* Shut down cell.
             */
            try {
                EventLogger.postRemovalBegin(_cell, event);
                _cell.postRemoval(event);
            } catch (Throwable e) {
                Thread t = Thread.currentThread();
                t.getUncaughtExceptionHandler().uncaughtException(t, e);
            } finally {
                EventLogger.postRemovalEnd(_cell, event);
            }

            /* Shut down remaining threads.
             */
            LOGGER.debug("Waiting for all threads in {} to finish", _threads);
            try {
                Collection<Thread> threads = getNonDaemonThreads(_threads);

                /* Some threads shut down asynchronously. Give them
                 * one second before we start to kill them.
                 */
                while (!joinThreads(threads, 1000)) {
                    threadGroupList();
                    killThreads(threads);
                }
                _threads.destroy();
            } catch (IllegalThreadStateException e) {
                _threads.setDaemon(true);
            } catch (InterruptedException e) {
                LOGGER.warn("Interrupted while waiting for threads");
            }

            /* Declare the cell as dead.
             */
            __cellGlue.destroy(CellNucleus.this);
            setState(State.TERMINATED);
        }
    }

    /**
     * Registers a callback, considering that the cell may have already shut down.
     */
    private void reregisterCallback(UOID uoid, CellLock lock)
    {
        /* Ordering here is important - need to insert into waitHash before checking the state
         * to avoid a race with shutdown.
         */
        _waitHash.put(uoid, lock);

        if (!_state.areCallbacksGuaranteed) {
            /* The cell is shutting down so we time out the message right away.
             */
            timeOutMessage(uoid, lock, (u, l) -> {});
        }
    }

    /**
     * Unregisters the callback and calls its timeout method. If scheduling
     * the callback fails that task is reregistered for later processing.
     */
    private void timeOutMessage(UOID uoid, CellLock lock, BiConsumer<UOID, CellLock> reregister)
    {
        if (_waitHash.remove(uoid, lock)) {
            try (CDC ignored = lock.getCdc().restore()) {
                try {
                    lock.getExecutor().execute(() -> {
                        try (CDC ignored2 = lock.getCdc().restore()) {
                            CellMessage envelope = lock.getMessage();
                            try {
                                lock.getCallback().answerTimedOut(envelope);
                                EventLogger.sendEnd(envelope);
                            } catch (RejectedExecutionException e) {
                                LOGGER.warn("Failed to invoke callback: {}", e.toString());
                                reregister.accept(uoid, lock);
                            } catch (RuntimeException e) {
                                Thread t = Thread.currentThread();
                                t.getUncaughtExceptionHandler().uncaughtException(t, e);
                            }
                        }
                    });
                } catch (RejectedExecutionException e) {
                    /* Put it back and deal with it later.
                     */
                    reregister.accept(uoid, lock);
                    LOGGER.warn("Failed to invoke callback: {}", e.toString());
                }
            }
        }
    }

    ////////////////////////////////////////////////////////////
    //
    //
    // the routing stuff
    //

    /**
     * Installs a new route in the routing table.
     *
     * @param route The route to add
     * @throws IllegalArgumentException If the route is a duplicate or if it routes through
     *                                  a non-existing local cell.
     */
    public void routeAdd(CellRoute route) throws IllegalArgumentException
    {
        __cellGlue.routeAdd(route);
    }

    public void routeDelete(CellRoute  route) throws IllegalArgumentException {
        __cellGlue.routeDelete(route);
    }
    CellRoute routeFind(CellAddressCore addr) {
        return __cellGlue.getRoutingTable().find(addr, getZone(), true);
    }
    public CellRoutingTable getRoutingTable() { return __cellGlue.getRoutingTable(); }
    public CellRoute [] getRoutingList() { return __cellGlue.getRoutingList(); }
    //
    public List<CellTunnelInfo> getCellTunnelInfos() { return __cellGlue.getCellTunnelInfos(); }

    public CuratorFramework getCuratorFramework()
    {
        return _curatorFramework;
    }

    @Nonnull
    public Optional<String> getZone()
    {
        return __cellGlue.getZone();
    }

    public SerializationHandler.Serializer getMsgSerialization()
    {
        return __cellGlue.getMessageSerializer();
    }

    //

    private class CallbackTask implements Runnable
    {
        private final CellLock _lock;
        private final CellMessage _message;

        public CallbackTask(CellLock lock, CellMessage message)
        {
            _lock = lock;
            _message = message;
        }

        @Override
        public void run()
        {
            _eventQueueSize.decrementAndGet();
            try (CDC ignored = _lock.getCdc().restore()) {
                try {
                    _message.getDestinationPath().next();
                    CellMessageAnswerable callback = _lock.getCallback();
                    CellMessage request = _lock.getMessage();
                    try {
                        Object obj = _message.getMessageObject();
                        if (obj instanceof Exception) {
                            callback.exceptionArrived(request, (Exception) obj);
                        } else {
                            callback.answerArrived(request, _message);
                        }
                        EventLogger.sendEnd(request);
                    } catch (RejectedExecutionException e) {
                        /* May happen when the callback itself tries to schedule the call
                         * on an executor. Put the request back and let it time out.
                         */
                        LOGGER.error("Failed to invoke callback: {}", e.toString());
                        reregisterCallback(request.getUOID(), _lock);
                    }
                    LOGGER.trace("addToEventQueue : callback done for : {}", _message);
                } catch (Throwable e) {
                    Thread t = Thread.currentThread();
                    t.getUncaughtExceptionHandler().uncaughtException(t, e);
                }
            }
        }

        @Override
        public String toString()
        {
            return "Delivery-of-" + _message;
        }
    }

    private class DeliverMessageTask implements Runnable
    {
        private final MessageEvent _event;

        public DeliverMessageTask(MessageEvent event)
        {
            _event = event;
        }

        @Override
        public void run()
        {
            try (CDC ignored = CDC.reset(CellNucleus.this)) {
                try {
                    EventLogger.queueEnd(_event);
                    _lastQueueTime = _event.getMessage().getLocalAge();
                    _eventQueueSize.decrementAndGet();

                    if (_event instanceof RoutedMessageEvent) {
                        _cell.messageArrived(_event);
                    } else {
                        CellMessage msg = _event.getMessage();
                        CDC.setMessageContext(msg);
                        msg.getDestinationPath().next();
                        try {
                            _cell.messageArrived(_event);
                        } catch (RuntimeException e) {
                            if (!msg.isReply()) {
                                msg.revertDirection();
                                msg.setMessageObject(e);
                                sendMessage(msg, true, true, true);
                            }
                            throw e;
                        }
                    }
                } catch (Throwable e) {
                    Thread t = Thread.currentThread();
                    t.getUncaughtExceptionHandler().uncaughtException(t, e);
                }
            }
        }

        @Override
        public String toString()
        {
            return "Delivery-of-" + _event;
        }
    }

    private class CellEventTask implements Runnable
    {
        private final CellEvent _event;

        public CellEventTask(CellEvent event)
        {
            this._event = event;
        }

        @Override
        public void run()
        {
            _eventQueueSize.decrementAndGet();
            try (CDC ignored = CDC.reset(CellNucleus.this)) {
                for (CellEventListener listener : _cellEventListeners) {
                    try {
                        switch (_event.getEventType()) {
                        case CellEvent.CELL_CREATED_EVENT:
                            listener.cellCreated(_event);
                            break;
                        case CellEvent.CELL_DIED_EVENT:
                            listener.cellDied(_event);
                            break;
                        case CellEvent.CELL_ROUTE_ADDED_EVENT:
                            listener.routeAdded(_event);
                            break;
                        case CellEvent.CELL_ROUTE_DELETED_EVENT:
                            listener.routeDeleted(_event);
                            break;
                        }
                    } catch (Throwable e) {
                        Thread t = Thread.currentThread();
                        t.getUncaughtExceptionHandler().uncaughtException(t, e);
                    }
                }

            }
        }
    }
}
