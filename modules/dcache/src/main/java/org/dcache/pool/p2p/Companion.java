package org.dcache.pool.p2p;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.io.SyncFailedException;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.CacheFileAvailable;
import diskCacheV111.util.ChecksumFactory;
import diskCacheV111.util.FileInCacheException;
import diskCacheV111.util.PnfsId;
import diskCacheV111.util.TimeoutCacheException;
import diskCacheV111.vehicles.DoorTransferFinishedMessage;
import diskCacheV111.vehicles.HttpDoorUrlInfoMessage;
import diskCacheV111.vehicles.HttpProtocolInfo;
import diskCacheV111.vehicles.IoJobInfo;
import diskCacheV111.vehicles.Pool2PoolTransferMsg;
import diskCacheV111.vehicles.PoolDeliverFileMessage;

import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.NoRouteToCellException;

import org.dcache.cells.AbstractMessageCallback;
import org.dcache.cells.CellStub;
import org.dcache.namespace.FileAttribute;
import org.dcache.pool.classic.ChecksumModule;
import org.dcache.pool.repository.EntryState;
import org.dcache.pool.repository.ReplicaDescriptor;
import org.dcache.pool.repository.Repository;
import org.dcache.pool.repository.StickyRecord;
import org.dcache.util.Checksum;
import org.dcache.util.FireAndForgetTask;
import org.dcache.vehicles.FileAttributes;
import org.dcache.vehicles.PnfsGetFileAttributes;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Encapsulates the tasks to be performed on the destination of a pool
 * to pool transfer.
 *
 * The companion will submit an HTTP download request to the source
 * pool, wait for the reply and then download the file through HTTP.
 *
 * The code is driven by a state machine, Companion.sm. Most of the
 * logic is encapsulated in the state machine.
 */
class Companion
{
    private final static Logger _log = LoggerFactory.getLogger(Companion.class);

    private final static long PING_PERIOD = TimeUnit.MINUTES.toMillis(5);
    private final static int BUFFER_SIZE = 65536;
    private final static String PROTOCOL_INFO_NAME = "Http";
    private final static int PROTOCOL_INFO_MAJOR_VERSION = 1;
    private final static int PROTOCOL_INFO_MINOR_VERSION = 1;

    private final static AtomicInteger _nextId = new AtomicInteger(100);
    private static final long CONNECT_TIMEOUT = TimeUnit.MINUTES.toMillis(5);
    private static final long READ_TIMEOUT = TimeUnit.MINUTES.toMillis(10);

    private final InetAddress _address;
    private final Repository _repository;
    private final ChecksumModule _checksumModule;
    private final String _sourcePoolName;
    private final String _destinationPoolCellname;
    private final String _destinationPoolCellDomainName;
    private final EntryState _targetState;
    private final List<StickyRecord> _stickyRecords;
    private final CacheFileAvailable _callback;
    private final ScheduledExecutorService _executor;
    private final CellStub _pnfs;
    private final CellStub _pool;
    private final boolean _forceSourceMode;

    /** State machine driving the transfer. */
    private final CompanionContext _fsm;

    /** Companion ID identifying the transfer. */
    private final int _id;

    /** Storage info for the file. */
    private FileAttributes _fileAttributes;

    /** Description of error condition, or null. */
    private Object _error;

    /** The thread performing the actual file transfer. */
    private Thread _thread;

    /** Used to implement the startTimer and stopTimer actions. */
    private ScheduledFuture<?> _timerTask;

    /** ID of the mover on the source pool. */
    private int _moverId;

    /**
     * Creates a new instance.
     *
     * @param executor    Executor used for state machine callbacks
     * @param address     Expected interface to connect to source pool
     * @param repository  Repository in which the replica is created
     * @param checksumModule Checksum module used to verify and
     *                    compute checksums
     * @param pnfs        Cell stub for PNFS communication
     * @param pool        Cell stub for pool communication
     * @param fileAttributes File attributes of the file. May be null.
     * @param sourcePoolName Name of source pool
     * @param destinationPoolCellname Cell name of the destination pool
     * @param destinationPoolCellDomainName Domain name of the destination pool
     * @param targetState The repository state used for the new replica
     * @param stickyRecords The sticky flags used for the new replica
     * @param callback    Callback to which success or failure is reported
     * @param forceSourceMode Ignores disabled state of pools
     */
    Companion(ScheduledExecutorService executor,
              InetAddress address,
              Repository repository,
              ChecksumModule checksumModule,
              CellStub pnfs,
              CellStub pool,
              FileAttributes fileAttributes,
              String sourcePoolName,
              String destinationPoolCellname,
              String destinationPoolCellDomainName,
              EntryState targetState,
              List<StickyRecord> stickyRecords,
              CacheFileAvailable callback,
              boolean forceSourceMode)
    {
        _fsm = new CompanionContext(this);

        _executor = executor;
        _address = address;
        _repository = repository;
        _checksumModule = checksumModule;
        _pnfs = pnfs;
        _pool = pool;
        _sourcePoolName = sourcePoolName;

        _destinationPoolCellname = checkNotNull(destinationPoolCellname, "Destination pool name is unknown.");
        _destinationPoolCellDomainName = checkNotNull(destinationPoolCellDomainName, "Destination domain name is unknown.");
        _fileAttributes = checkNotNull(fileAttributes, "File attributes is missing.");

        if (!_fileAttributes.isDefined(FileAttribute.PNFSID)) {
            throw new IllegalArgumentException("PNFSID is required, got " + _fileAttributes.getDefinedAttributes());
        }

        _callback = callback;
        _forceSourceMode = forceSourceMode;
        _targetState = targetState;
        _stickyRecords = new ArrayList<>(stickyRecords);

        _id = _nextId.getAndIncrement();

        synchronized (this) {
            _fsm.start();
        }
    }

    /**
     * Returns the session ID identifying the transfer.
     */
    synchronized public int getId()
    {
        return _id;
    }

    /**
     * Returns the PNFS ID of the file to be transfered.
     */
    synchronized public PnfsId getPnfsId()
    {
        return _fileAttributes.getPnfsId();
    }

    synchronized public long getPingPeriod()
    {
        return PING_PERIOD;
    }

    /**
     * Cancels the transfer. Returns true unless the transfer is
     * already completed.
     */
    synchronized public boolean cancel(Object cause)
    {
        _fsm.cancel(cause);
        return (_fsm.getState() != CompanionContext.FSM.Done);
    }

    synchronized public String toString()
    {
        return ""
            + _id
            + " "
            + getPnfsId()
            + " "
            + _fsm.getState();
    }

    /**
     * Delivers a DoorTransferFinishedMessage. Normally send by the
     * source pool.
     */
    synchronized public void messageArrived(DoorTransferFinishedMessage message)
    {
        _fsm.messageArrived(message);
    }

    /**
     * Message handler for redirect messages from the pools.
     */
    synchronized public void messageArrived(HttpDoorUrlInfoMessage message)
    {
        _fsm.messageArrived(message);
    }

    /**
     * Sets the thread used for the file transfer.
     */
    synchronized private void setThread(Thread thread)
    {
        _thread = thread;
    }

    private void transfer(String uri)
    {
        ReplicaDescriptor handle;
        synchronized (this) {
            try {
                handle = createReplicaEntry();
            } catch (FileInCacheException e) {
                _fsm.createEntryFailed();
                return;
            }
            setThread(Thread.currentThread());
        }

        Throwable error = null;
        try {
            try {
                File file = handle.getFile();
                long size = handle.getFileAttributes().getSize();

                handle.allocate(size);

                ChecksumFactory checksumFactory;
                MessageDigest digest;
                if (_checksumModule.hasPolicy(ChecksumModule.PolicyFlag.ON_TRANSFER)) {
                    checksumFactory = _checksumModule.getPreferredChecksumFactory(handle);
                    digest = checksumFactory.create();
                } else {
                    checksumFactory = null;
                    digest = null;
                }

                HttpURLConnection connection = createConnection(uri);
                try {
                    try (InputStream input = connection.getInputStream()) {
                        long total = copy(input, file, digest);
                        if (total != size) {
                            throw new IOException("Amount of received data does not match expected file size");
                        }
                    }
                } finally {
                    connection.disconnect();
                }

                Set<Checksum> actualChecksums =
                        (digest == null)
                                ? Collections.<Checksum>emptySet()
                                : Collections.singleton(checksumFactory.create(digest.digest()));
                _checksumModule.enforcePostTransferPolicy(handle, actualChecksums);
            } finally {
                setThread(null);
                Thread.interrupted();
            }
            handle.commit();
        } catch (Throwable e) {
            error = e;
        } finally {
            handle.close();
            synchronized (this) {
                _fsm.transferEnded(error);
            }
        }
    }

    private ReplicaDescriptor createReplicaEntry()
        throws FileInCacheException
    {
        return _repository.createEntry(
                _fileAttributes,
                EntryState.FROM_POOL,
                _targetState,
                _stickyRecords,
                EnumSet.of(Repository.OpenFlags.CREATEFILE));
    }

    private HttpURLConnection createConnection(String uri)
        throws MalformedURLException, IOException
    {
        URL url = new URL(uri);
        HttpURLConnection connection =
            (HttpURLConnection) url.openConnection();
        connection.setRequestProperty("Connection", "close");
        connection.setConnectTimeout((int) CONNECT_TIMEOUT);
        connection.setReadTimeout((int) READ_TIMEOUT);
        connection.connect();
        return connection;
    }

    private long copy(InputStream input, File file, MessageDigest digest)
        throws IOException
    {
        long total = 0L;
        try (RandomAccessFile dataFile = new RandomAccessFile(file, "rw")) {
            try {
                byte[] buffer = new byte[BUFFER_SIZE];
                int read;
                while ((read = input.read(buffer)) > -1) {
                    dataFile.write(buffer, 0, read);
                    total += read;
                    if (digest != null) {
                        digest.update(buffer, 0, read);
                    }
                }
            } finally {
                try {
                    dataFile.getFD().sync();
                } catch (SyncFailedException e) {
                    /* Data is not guaranteed to be on disk. Not a fatal
                     * problem, but better generate a warning.
                     */
                    _log.warn("Failed to synchronize file with storage device: {}",
                              e.getMessage());
                }
            }
        }
        return total;
    }

    //
    // The following methods are actions or helper methods used by the
    // state machine.
    /////////////////////////////////////////////////////////////////

    synchronized void setError(Object error)
    {
        if (_error == null) {
            _error = error;
        }
    }

    synchronized void clearError()
    {
        _error = null;
    }

    /** Returns true iff all required attributes are available. */
    synchronized boolean hasRequiredAttributes()
    {
        return _fileAttributes.isDefined(Pool2PoolTransferMsg.NEEDED_ATTRIBUTES);
    }

    /** Asynchronously retrieves the file attributes. */
    synchronized void fetchFileAttributes()
    {
        CellStub.addCallback(_pnfs.send(new PnfsGetFileAttributes(getPnfsId(), Pool2PoolTransferMsg.NEEDED_ATTRIBUTES)),
                             new Callback<PnfsGetFileAttributes>()
                             {
                                 @Override
                                 public void success(PnfsGetFileAttributes message)
                                 {
                                     setFileAttributes(message.getFileAttributes());
                                     super.success(message);
                                 }
                             }, _executor);
    }

    synchronized void setFileAttributes(FileAttributes fileAttributes)
    {
        _fileAttributes = fileAttributes;
    }

    /** FSM Action */
    synchronized void startTimer(long delay)
    {
        Runnable task =
            new Runnable()
            {
                @Override
                public void run()
                {
                    synchronized (Companion.this) {
                        if (_timerTask != null) {
                            _fsm.timer();
                            _timerTask = null;
                        }
                    }
                }
            };
        _timerTask =
            _executor.schedule(new FireAndForgetTask(task),
                               delay, TimeUnit.MILLISECONDS);
    }

    /** FSM Action */
    synchronized void stopTimer()
    {
        if (_timerTask != null) {
            _timerTask.cancel(false);
            _timerTask = null;
        }
    }

    /** Asynchronously requests delivery from the source pool. */
    synchronized void sendDeliveryRequest()
    {
        HttpProtocolInfo protocolInfo =
            new HttpProtocolInfo(PROTOCOL_INFO_NAME,
                                 PROTOCOL_INFO_MAJOR_VERSION,
                                 PROTOCOL_INFO_MINOR_VERSION,
                                 new InetSocketAddress(_address, 0),
                                 _destinationPoolCellname,
                                 _destinationPoolCellDomainName,
                                 "/" +  getPnfsId(),
                                 null);
        protocolInfo.setSessionId(_id);

        PoolDeliverFileMessage request =
                new PoolDeliverFileMessage(_sourcePoolName,
                                           protocolInfo,
                                           _fileAttributes);
        request.setPool2Pool();
        request.setInitiator(getInitiator());
        request.setId(_id);
        request.setForceSourceMode(_forceSourceMode);

        CellStub.addCallback(_pool.send(new CellPath(_sourcePoolName), request),
                             new Callback<PoolDeliverFileMessage>()
                             {
                                 @Override
                                 public void success(PoolDeliverFileMessage message)
                                 {
                                     setMoverId(message.getMoverId());
                                     super.success(message);
                                 }
                             }, _executor);
    }

    private String getInitiator()
    {
        return "pool:"  + _destinationPoolCellname + "@"
                        + _destinationPoolCellDomainName;
    }

    synchronized void setMoverId(int moverId)
    {
        _moverId = moverId;
    }

    synchronized void ping()
    {
        Futures.addCallback(_pool.send(new CellPath(_sourcePoolName),
                                       "p2p ls -binary " + _moverId, IoJobInfo.class),
                            new FutureCallback<IoJobInfo>()
                            {
                                @Override
                                public void onSuccess(IoJobInfo result)
                                {
                                    try {
                                        synchronized (Companion.this) {
                                            _fsm.success();
                                        }
                                    } catch (Throwable e) {
                                        Thread thisThread = Thread.currentThread();
                                        Thread.UncaughtExceptionHandler ueh = thisThread.getUncaughtExceptionHandler();
                                        ueh.uncaughtException( thisThread, e);
                                    }
                                }

                                @Override
                                public void onFailure(Throwable t)
                                {
                                    try {
                                        synchronized (Companion.this) {
                                            if (t instanceof NoRouteToCellException) {
                                                _fsm.noroute();
                                            } else if (t instanceof TimeoutCacheException) {
                                                _fsm.timeout();
                                            } else if (t instanceof CacheException) {
                                                _fsm.failure(((CacheException) t).getRc(), t.getMessage());
                                            } else {
                                                _fsm.failure(CacheException.UNEXPECTED_SYSTEM_EXCEPTION, t);
                                            }
                                        }
                                    } catch (Throwable e) {
                                        Thread thisThread = Thread.currentThread();
                                        Thread.UncaughtExceptionHandler ueh = thisThread.getUncaughtExceptionHandler();
                                        ueh.uncaughtException( thisThread, e);
                                    }
                                }
                            }, _executor);
    }

    /**
     * Starts a thread that transfers the file from the source pool.
     */
    synchronized void beginTransfer(final String uri)
    {
        new Thread("P2P Transfer - " + getPnfsId()) {
            @Override
            public void run()
            {
                transfer(uri);
            }
        }.start();
    }

    /**
     * Called at the end of the transfer to call callbacks and free
     * resources associated with the transfer.
     */
    synchronized void done()
    {
        if (_thread != null) {
            throw new IllegalStateException("Cannot close a companion while the transfer is in progress");
        }

        if (_error != null) {
            if (_error instanceof RuntimeException || _error instanceof Error) {
                _log.error(String.format("P2P for %s failed: %s", getPnfsId(), _error),
                           (Throwable) _error);
            } else {
                _log.error(String.format("P2P for %s failed: %s", getPnfsId(), _error));
            }
        } else {
            _log.info(String.format("P2P for %s completed", getPnfsId()));
        }

        if (_callback != null) {

            final Object error = _error;

            _executor.execute(new FireAndForgetTask(new Runnable() {
                    @Override
                    public void run() {
                        Throwable t;

                        if (error == null) {
                            t = null;
                        } else if (error instanceof Throwable) {
                            t = (Throwable)error;
                        } else {
                            t = new CacheException(error.toString());
                        }
                        _callback.cacheFileAvailable(getPnfsId(), t);
                    }
                }));
        }
    }

    /**
     * Interrupt an ongoing transfer.
     */
    synchronized void interrupt()
    {
        if (_thread != null) {
            _thread.interrupt();
        }
    }

    /**
     * Helper class implementing the MessageCallback interface,
     * forwarding all messages as events to the state machine. Events
     * are forwarded via an executor to guarantee asynchronous
     * delivery (SMC state machines do not allow transitions to be
     * triggered from within transitions).
     */
    class Callback<T> extends AbstractMessageCallback<T>
    {
        @Override
        public void success(T message)
        {
            try {
                synchronized (Companion.this) {
                    _fsm.success();
                }
            } catch (Throwable e) {
                Thread thisThread = Thread.currentThread();
                Thread.UncaughtExceptionHandler ueh = thisThread.getUncaughtExceptionHandler();
                ueh.uncaughtException( thisThread, e);
            }
        }

        @Override
        public void failure(final int rc, final Object cause)
        {
            try {
                synchronized (Companion.this) {
                    _fsm.failure(rc, cause);
                }
            } catch (Throwable e) {
                Thread thisThread = Thread.currentThread();
                Thread.UncaughtExceptionHandler ueh = thisThread.getUncaughtExceptionHandler();
                ueh.uncaughtException( thisThread, e);
            }
        }

        @Override
        public void timeout(String error)
        {
            try {
                synchronized (Companion.this) {
                    _fsm.timeout();
                }
            } catch (Throwable e) {
                Thread thisThread = Thread.currentThread();
                Thread.UncaughtExceptionHandler ueh = thisThread.getUncaughtExceptionHandler();
                ueh.uncaughtException( thisThread, e);
            }
        }

        @Override
        public void noroute(CellPath path)
        {
            try {
                synchronized (Companion.this) {
                    _fsm.noroute();
                }
            } catch (Throwable e) {
                Thread thisThread = Thread.currentThread();
                Thread.UncaughtExceptionHandler ueh = thisThread.getUncaughtExceptionHandler();
                ueh.uncaughtException( thisThread, e);
            }
        }
    }
}
