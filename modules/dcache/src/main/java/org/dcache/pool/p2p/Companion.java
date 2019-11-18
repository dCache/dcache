package org.dcache.pool.p2p;

import com.google.common.io.ByteStreams;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.StatusLine;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpResponseException;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.protocol.HTTP;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.io.SyncFailedException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import javax.net.ssl.SSLContext;
import java.net.UnknownHostException;
import java.nio.channels.Channels;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.OptionalLong;
import java.util.Set;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.CacheFileAvailable;
import diskCacheV111.util.FileInCacheException;
import diskCacheV111.util.PnfsId;
import diskCacheV111.util.TimeoutCacheException;
import diskCacheV111.vehicles.DoorTransferFinishedMessage;
import diskCacheV111.vehicles.HttpDoorUrlInfoMessage;
import diskCacheV111.vehicles.HttpProtocolInfo;
import diskCacheV111.vehicles.IoJobInfo;
import diskCacheV111.vehicles.Message;
import diskCacheV111.vehicles.Pool2PoolTransferMsg;
import diskCacheV111.vehicles.PoolDeliverFileMessage;

import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.NoRouteToCellException;

import org.dcache.cells.AbstractMessageCallback;
import org.dcache.cells.CellStub;
import org.dcache.namespace.FileAttribute;
import org.dcache.pool.assumption.Assumptions;
import org.dcache.pool.classic.ChecksumModule;
import org.dcache.pool.movers.ChecksumChannel;
import org.dcache.pool.repository.ReplicaState;
import org.dcache.pool.repository.ReplicaDescriptor;
import org.dcache.pool.repository.Repository;
import org.dcache.pool.repository.RepositoryChannel;
import org.dcache.pool.repository.StickyRecord;
import org.dcache.util.Checksum;
import org.dcache.util.ChecksumType;
import org.dcache.util.FireAndForgetTask;
import org.dcache.util.Version;
import org.dcache.vehicles.FileAttributes;
import org.dcache.vehicles.PnfsGetFileAttributes;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.dcache.util.ByteUnit.KiB;

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
    private static final Logger _log = LoggerFactory.getLogger(Companion.class);

    private static final long PING_PERIOD = TimeUnit.MINUTES.toMillis(5);
    private static final int BUFFER_SIZE = KiB.toBytes(64);
    private static final String PROTOCOL_INFO_NAME = "Http";
    private static final String PROTOCOL_INFO_SSL_NAME = "Https";
    private static final int PROTOCOL_INFO_MAJOR_VERSION = 1;
    private static final int PROTOCOL_INFO_MINOR_VERSION = 1;

    private static final AtomicInteger _nextId = new AtomicInteger(100);
    private static final long CONNECT_TIMEOUT = TimeUnit.MINUTES.toMillis(1);
    private static final long READ_TIMEOUT = TimeUnit.MINUTES.toMillis(1);

    private static final String USER_AGENT = "dCache/" + Version.of(Companion.class).getVersion();

    private final InetAddress _address;
    private final Repository _repository;
    private final ChecksumModule _checksumModule;
    private final String _sourcePoolName;
    private final String _destinationPoolCellname;
    private final String _destinationPoolCellDomainName;
    private final ReplicaState _targetState;
    private final List<StickyRecord> _stickyRecords;
    private final CacheFileAvailable _callback;
    private final ScheduledExecutorService _executor;
    private final CellStub _pnfs;
    private final CellStub _pool;
    private final boolean _forceSourceMode;
    private final PnfsId _pnfsId;

    /** State machine driving the transfer. */
    private final CompanionContext _fsm;

    /** Companion ID identifying the transfer. */
    private final int _id;

    /** Last access time to set the new replica to. */
    private final Long _atime;

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
    private HttpGet _request;

    private SSLContext _sslContext;

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
     * @param atime       Last access time for the new replica
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
              ReplicaState targetState,
              List<StickyRecord> stickyRecords,
              CacheFileAvailable callback,
              boolean forceSourceMode,
              Long atime,
              Supplier<SSLContext> getContextIfNeeded) {
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

        _sslContext = getContextIfNeeded.get();


        if (!_fileAttributes.isDefined(FileAttribute.PNFSID)) {
            throw new IllegalArgumentException("PNFSID is required, got " + _fileAttributes.getDefinedAttributes());
        }

        _pnfsId = _fileAttributes.getPnfsId();

        _callback = callback;
        _forceSourceMode = forceSourceMode;
        _atime = atime;
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
    public int getId()
    {
        return _id;
    }

    /**
     * Returns the PNFS ID of the file to be transfered.
     */
    public PnfsId getPnfsId()
    {
        return _pnfsId;
    }

    public long getPingPeriod()
    {
        return PING_PERIOD;
    }

    /**
     * Cancels the transfer. Returns true unless the transfer is
     * already completed.
     */
    public synchronized boolean cancel(Object cause)
    {
        _fsm.cancel(cause);
        return (_fsm.getState() != CompanionContext.FSM.Done);
    }

    public String toString()
    {
        // Unsynchronized access to the fsm state means we may show an old value, but it
        // avoids blocking in toString().
        return _id + " " + _pnfsId + " " + _fsm.getState();
    }

    /**
     * Delivers a DoorTransferFinishedMessage. Normally send by the
     * source pool.
     */
    public synchronized void messageArrived(DoorTransferFinishedMessage message)
    {
        _fsm.messageArrived(message);
    }

    /**
     * Message handler for redirect messages from the pools.
     */
    public synchronized void messageArrived(HttpDoorUrlInfoMessage message)
    {
        _fsm.messageArrived(message);
    }

    /**
     * Sets the thread used for the file transfer.
     */
    private synchronized void setThread(Thread thread)
    {
        _thread = thread;
    }

    /**
     * Sets the request used for the file transfer.
     */
    private synchronized void setRequest(HttpGet request)
    {
        _request = request;
    }

    private void transfer(String uri)
    {
        ReplicaDescriptor handle;
        synchronized (this) {
            try {
                handle = createReplicaEntry();
            } catch (FileInCacheException e) {
                _fsm.fileExists();
                return;
            } catch (CacheException e) {
                _fsm.createEntryFailed(e.getRc(), e.getMessage());
                return;
            }
            setThread(Thread.currentThread());
        }

        Throwable error = null;
        try {
            try {
                Set<Checksum> actualChecksums = copy(uri, handle);
                _checksumModule.enforcePostTransferPolicy(handle, actualChecksums);
            } finally {
                setThread(null);
                Thread.interrupted();
            }
            if (_atime != null) {
                handle.setLastAccessTime(_atime);
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

    private Set<Checksum> copy(String uri, ReplicaDescriptor handle) throws IOException
    {
        EnumSet<ChecksumType> knownChecksumTypes = EnumSet.noneOf(ChecksumType.class);
        try {
            handle.getChecksums().forEach(c -> knownChecksumTypes.add(c.getType()));
        } catch (CacheException e) {
            _log.warn("Failed to fetch checksum information: {}", e.getMessage());
        }

        try (RepositoryChannel channel = handle.createChannel()) {
            HttpGet get = new HttpGet(uri);
            get.addHeader(HttpHeaders.CONNECTION, HTTP.CONN_CLOSE);
            get.setConfig(RequestConfig.custom()
                                  .setConnectTimeout((int) CONNECT_TIMEOUT)
                                  .setSocketTimeout((int) READ_TIMEOUT)
                                  .build());
            setRequest(get);

            try (CloseableHttpClient client = HttpClients.custom()
                    .setSSLContext(_sslContext)
                    .setUserAgent(USER_AGENT).build();
                 CloseableHttpResponse response = client.execute(get)) {
                StatusLine statusLine = response.getStatusLine();
                if (statusLine.getStatusCode() >= 300) {
                    throw new HttpResponseException(statusLine.getStatusCode(), statusLine.getReasonPhrase());
                }

                HttpEntity entity = response.getEntity();
                if (entity == null) {
                    throw new ClientProtocolException("Response contains no content");
                }

                long contentLength = entity.getContentLength();
                if (contentLength >= 0 && contentLength != _fileAttributes.getSize()) {
                    /* Fail fast if the response is incomplete.
                     */
                    throw new EOFException("Received file does not match expected file size.");
                }

                ByteStreams.copy(entity.getContent(), Channels.newOutputStream(channel));

                try {
                    channel.sync();
                } catch (SyncFailedException e) {
                    /* Data is not guaranteed to be on disk. Not a fatal
                     * problem, but better generate a warning.
                     */
                    _log.warn("Failed to synchronize file with storage device: {}",
                              e.getMessage());
                }
            } finally {
                setRequest(null);
            }
            return channel.optionallyAs(ChecksumChannel.class)
                    .map(ChecksumChannel::getChecksums)
                    .orElseThrow(() -> new IllegalStateException("Missing ChecksumChannel"));
        }
    }

    private ReplicaDescriptor createReplicaEntry()
        throws CacheException
    {
        return _repository.createEntry(
                _fileAttributes,
                ReplicaState.FROM_POOL,
                _targetState,
                _stickyRecords,
                EnumSet.of(StandardOpenOption.CREATE),
                OptionalLong.empty());
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
    void fetchFileAttributes()
    {
        CellStub.addCallback(_pnfs.send(new PnfsGetFileAttributes(_pnfsId, Pool2PoolTransferMsg.NEEDED_ATTRIBUTES)),
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
                () -> {
                    synchronized (Companion.this) {
                        if (_timerTask != null) {
                            _fsm.timer();
                            _timerTask = null;
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
        try {
            InetAddress address = (_address == null) ? InetAddress.getLocalHost() : _address;
            HttpProtocolInfo protocolInfo =
                new HttpProtocolInfo(_sslContext != null ? PROTOCOL_INFO_SSL_NAME: PROTOCOL_INFO_NAME,
                                     PROTOCOL_INFO_MAJOR_VERSION,
                                     PROTOCOL_INFO_MINOR_VERSION,
                                     new InetSocketAddress(address, 0),
                                     _destinationPoolCellname,
                                     _destinationPoolCellDomainName,
                                     "/" + _pnfsId,
                                     null);
            protocolInfo.setSessionId(_id);

            PoolDeliverFileMessage request =
                    new PoolDeliverFileMessage(_sourcePoolName,
                                               protocolInfo,
                                               _fileAttributes,
                                               Assumptions.none());
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
        } catch (UnknownHostException e) {
            _executor.execute(() -> _fsm.failure(CacheException.UNEXPECTED_SYSTEM_EXCEPTION, e));
        }
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
    void beginTransfer(final String uri)
    {
        new Thread("P2P Transfer - " + _pnfsId + " " + _sourcePoolName) {
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
            if (_error instanceof Error) {
                _log.error("P2P for {} failed due to a serious problem in the JVM.",
                        _pnfsId, _error);
                throw (Error)_error; // We should not attempt to recover from this!
            } else if (_error instanceof RuntimeException) {
                _log.error("P2P for {} failed due to a bug.  Please report"
                        + " this to <support@dCache.org>", _pnfsId, _error);
            } else {
                _log.error("P2P for {} failed: {}", _pnfsId, _error.toString());
            }
        } else {
            _log.info("P2P for {} completed", _pnfsId);
        }

        if (_callback != null) {

            final Object error = _error;

            _executor.execute(new FireAndForgetTask(() -> {
                Throwable t;
                if (error == null) {
                    t = null;
                } else if (error instanceof Throwable) {
                    t = (Throwable)error;
                } else {
                    t = new CacheException(error.toString());
                }
                _callback.cacheFileAvailable(_pnfsId, t);
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
        if (_request != null) {
            _request.abort();
        }
    }

    /**
     * Helper class implementing the MessageCallback interface,
     * forwarding all messages as events to the state machine. Events
     * are forwarded via an executor to guarantee asynchronous
     * delivery (SMC state machines do not allow transitions to be
     * triggered from within transitions).
     */
    class Callback<T extends Message> extends AbstractMessageCallback<T>
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
