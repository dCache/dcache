package org.dcache.pool.p2p;

import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.net.InetSocketAddress;
import java.net.InetAddress;
import java.io.IOException;
import java.io.SyncFailedException;
import java.io.File;
import java.io.RandomAccessFile;
import java.security.MessageDigest;

import org.dcache.cells.CellStub;
import org.dcache.cells.AbstractMessageCallback;
import org.dcache.pool.repository.EntryState;
import org.dcache.pool.repository.StickyRecord;
import org.dcache.pool.repository.ReplicaDescriptor;
import org.dcache.pool.repository.Repository;
import org.dcache.pool.repository.CacheEntry;
import org.dcache.pool.classic.ChecksumModuleV1;
import diskCacheV111.util.PnfsId;
import diskCacheV111.util.CacheFileAvailable;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.FileInCacheException;
import org.dcache.util.Checksum;
import org.dcache.util.ChecksumType;
import org.dcache.util.FireAndForgetTask;
import diskCacheV111.util.Adler32;
import diskCacheV111.vehicles.StorageInfo;
import diskCacheV111.vehicles.PnfsGetStorageInfoMessage;
import diskCacheV111.vehicles.PoolDeliverFileMessage;
import diskCacheV111.vehicles.DoorTransferFinishedMessage;
import diskCacheV111.vehicles.HttpDoorUrlInfoMessage;
import diskCacheV111.vehicles.HttpProtocolInfo;
import diskCacheV111.vehicles.IoJobInfo;

import dmg.cells.nucleus.CellPath;

import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.MalformedURLException;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkArgument;

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

    private final InetAddress _address;
    private final Repository _repository;
    private final ChecksumModuleV1 _checksumModule;
    private final PnfsId _pnfsId;
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
    private StorageInfo _storageInfo;

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
     * @param pnfsId      PNFS ID of replica to copy
     * @param storageInfo Storage info of the file. May be null.
     * @param poolName    Name of source pool
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
              ChecksumModuleV1 checksumModule,
              CellStub pnfs,
              CellStub pool,
              PnfsId pnfsId,
              StorageInfo storageInfo,
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
        _pnfsId = pnfsId;
        _sourcePoolName = sourcePoolName;

        checkArgument(destinationPoolCellname != null, "Destination pool name is unknown. Aborting the request.");
        checkArgument(destinationPoolCellDomainName != null, "Destination domain name is unknown. Aborting the request");

        _destinationPoolCellname = destinationPoolCellname;
        _destinationPoolCellDomainName = destinationPoolCellDomainName;

        _callback = callback;
        _forceSourceMode = forceSourceMode;
        _targetState = targetState;
        _stickyRecords = new ArrayList<>(stickyRecords);
        if (storageInfo != null) {
            setStorageInfo(storageInfo);
        }

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
        return _pnfsId;
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
            + _pnfsId
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
                CacheEntry entry = handle.getEntry();
                long size = entry.getStorageInfo().getFileSize();

                handle.allocate(size);

                MessageDigest digest = createDigest();
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
                setChecksum(file, digest);
            } finally {
                setThread(null);
                Thread.interrupted();
            }
            handle.commit(null);
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
        return _repository.createEntry(_pnfsId,
                                       _storageInfo,
                                       EntryState.FROM_POOL,
                                       _targetState,
                                       _stickyRecords);
    }

    private MessageDigest createDigest()
    {
        return _checksumModule.checkOnTransfer() ? new Adler32() : null;
    }

    private HttpURLConnection createConnection(String uri)
        throws MalformedURLException, IOException
    {
        URL url = new URL(uri);
        HttpURLConnection connection =
            (HttpURLConnection) url.openConnection();
        connection.setRequestProperty("Connection", "close");
        connection.connect();
        return connection;
    }

    private void setChecksum(File file, MessageDigest digest)
        throws CacheException, InterruptedException, IOException
    {
        Checksum checksum = null;
        if (digest != null) {
            checksum = new Checksum(ChecksumType.ADLER32, digest.digest());
        }

        _checksumModule.setMoverChecksums(_pnfsId,
                                          file,
                                          _checksumModule.getDefaultChecksumFactory(),
                                          null,
                                          checksum);
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

    /** Returns true iff storage info is known. */
    synchronized boolean hasStorageInfo()
    {
        return _storageInfo != null;
    }

    /** Asynchronously retrieves the storage info. */
    synchronized void getStorageInfo()
    {
        _pnfs.send(new PnfsGetStorageInfoMessage(_pnfsId),
                   PnfsGetStorageInfoMessage.class,
                   new Callback<PnfsGetStorageInfoMessage>()
                   {
                       @Override
                       public void success(PnfsGetStorageInfoMessage message)
                       {
                           setStorageInfo(message.getStorageInfo());
                           super.success(message);
                       }
                   });
    }

    synchronized void setStorageInfo(StorageInfo info)
    {
        _storageInfo = info;
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
                                 "/" +  _pnfsId.toIdString(),
                                 null);
        protocolInfo.setSessionId(_id);

        PoolDeliverFileMessage request =
                new PoolDeliverFileMessage(_sourcePoolName,
                                           _pnfsId,
                                           protocolInfo,
                                           _storageInfo);
        request.setPool2Pool();
        request.setInitiator(getInitiator());
        request.setId(_id);
        request.setForceSourceMode(_forceSourceMode);

        _pool.send(new CellPath(_sourcePoolName),
                   request, PoolDeliverFileMessage.class,
                   new Callback<PoolDeliverFileMessage>()
                   {
                       @Override
                       public void success(PoolDeliverFileMessage message)
                       {
                           setMoverId(message.getMoverId());
                           super.success(message);
                       }
                   });
    }

    private String getInitiator()
    {
        return _destinationPoolCellname + "@" + _destinationPoolCellDomainName;
    }

    synchronized void setMoverId(int moverId)
    {
        _moverId = moverId;
    }

    synchronized void ping()
    {
        _pool.send(new CellPath(_sourcePoolName),
                   "p2p ls -binary " + _moverId, IoJobInfo.class,
                   new Callback<IoJobInfo>());
    }

    /**
     * Starts a thread that transfers the file from the source pool.
     */
    synchronized void beginTransfer(final String uri)
    {
        new Thread("P2P Transfer - " + _pnfsId) {
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
            if (_error instanceof RuntimeException) {
                _log.error(String.format("P2P for %s failed: %s", _pnfsId, _error),
                           (Exception) _error);
            } else {
                _log.error(String.format("P2P for %s failed: %s", _pnfsId, _error));
            }
        } else {
            _log.info(String.format("P2P for %s completed", _pnfsId));
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
                        _callback.cacheFileAvailable(_pnfsId, t);
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
            _executor.execute(new FireAndForgetTask(new Runnable() {
                    @Override
                    public void run() {
                        synchronized (Companion.this) {
                            _fsm.success();
                        }
                    }
                }));
        }

        @Override
        public void failure(final int rc, final Object cause)
        {
            _executor.execute(new FireAndForgetTask(new Runnable() {
                    @Override
                    public void run() {
                        synchronized (Companion.this) {
                            _fsm.failure(rc, cause);
                        }
                    }
                }));
        }

        @Override
        public void timeout(CellPath path)
        {
            _executor.execute(new FireAndForgetTask(new Runnable() {
                    @Override
                    public void run() {
                        synchronized (Companion.this) {
                            _fsm.timeout();
                        }
                    }
                }));
        }

        @Override
        public void noroute(CellPath path)
        {
            _executor.execute(new FireAndForgetTask(new Runnable() {
                    @Override
                    public void run() {
                        synchronized (Companion.this) {
                            _fsm.noroute();
                        }
                    }
                }));
        }
    }
}
