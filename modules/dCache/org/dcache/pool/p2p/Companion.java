package org.dcache.pool.p2p;

import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.net.InetSocketAddress;
import java.net.InetAddress;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.SyncFailedException;
import java.io.File;
import java.io.RandomAccessFile;
import java.net.BindException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.dcache.cells.CellStub;
import org.dcache.cells.MessageCallback;
import org.dcache.pool.repository.EntryState;
import org.dcache.pool.repository.StickyRecord;
import org.dcache.pool.repository.WriteHandle;
import org.dcache.pool.repository.Repository;
import org.dcache.pool.repository.CacheEntry;
import org.dcache.pool.classic.ChecksumModuleV1;
import diskCacheV111.util.PnfsId;
import diskCacheV111.util.CacheFileAvailable;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.FileInCacheException;
import diskCacheV111.util.Checksum;
import org.dcache.util.FireAndForgetTask;
import diskCacheV111.util.Adler32;
import diskCacheV111.vehicles.StorageInfo;
import diskCacheV111.vehicles.PnfsGetStorageInfoMessage;
import diskCacheV111.vehicles.PoolDeliverFileMessage;
import diskCacheV111.vehicles.DCapProtocolInfo;
import diskCacheV111.vehicles.DoorTransferFinishedMessage;
import diskCacheV111.vehicles.Message;
import diskCacheV111.vehicles.IoJobInfo;
import diskCacheV111.movers.DCapConstants;

import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.NoRouteToCellException;

import org.apache.log4j.Logger;

/**
 * Encapsulates the tasks to be performed on the destination of a pool
 * to pool transfer.
 *
 * The companion registers itself with an Acceptor upon creation.  The
 * Acceptor will invoke the transfer method of the Companion when a
 * connection from the source pool is received.
 *
 * The companion is driven by a state machine, Companion.sm. Most of
 * the logic is encapsulated in the state machine.
 */
class Companion
{
    private final static Logger _log = Logger.getLogger(Companion.class);

    private final static long PING_PERIOD = 5 * 60 * 1000; // 5 minutes

    private final Acceptor _acceptor;
    private final Repository _repository;
    private final ChecksumModuleV1 _checksumModule;
    private final PnfsId _pnfsId;
    private final String _poolName;
    private final EntryState _targetState;
    private final List<StickyRecord> _stickyRecords;
    private final CacheFileAvailable _callback;
    private final InetSocketAddress _address;
    private final ScheduledExecutorService _executor;
    private final CellStub _pnfs;
    private final CellStub _pool;

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
    private ScheduledFuture _timerTask;

    /** ID of the mover on the source pool. */
    private int _moverId;

    /**
     * Creates a new instance.
     *
     * @param executor    Executor used for state machine callbacks
     * @param acceptor    Acceptor used for registering the companion
     * @param repository  Repository in which the replica is created
     * @param checksumModule Checksum module used to verify and
     *                    compute checksums
     * @param pnfs        Cell stub for PNFS communication
     * @param pool        Cell stub for pool communication
     * @param pnfsId      PNFS ID of replica to copy
     * @param storageInfo Storage info of the file. May be null.
     * @param poolName    Name of source pool
     * @param targetState The repository state used for the new replica
     * @param stickyRecords The sticky flags used for the new replica
     * @param callback    Callback to which success or failure is reported
     * @throws IOException if the P2P socket could not be opened
     */
    Companion(ScheduledExecutorService executor,
              Acceptor acceptor,
              Repository repository,
              ChecksumModuleV1 checksumModule,
              CellStub pnfs,
              CellStub pool,
              PnfsId pnfsId,
              StorageInfo storageInfo,
              String poolName,
              EntryState targetState,
              List<StickyRecord> stickyRecords,
              CacheFileAvailable callback)
        throws IOException
    {
        _fsm = new CompanionContext(this);

        _executor = executor;
        _acceptor = acceptor;
        _repository = repository;
        _checksumModule = checksumModule;
        _pnfs = pnfs;
        _pool = pool;
        _pnfsId = pnfsId;
        _poolName = poolName;
        _callback = callback;
        _targetState = targetState;
        _stickyRecords = new ArrayList(stickyRecords);
        if (storageInfo != null) {
            setStorageInfo(storageInfo);
        }

        _id = _acceptor.register(this);

        boolean success = false;
        try {
            /* Determine address that source pool should connect to. If
             * the acceptor listens on a wildcard address, then we use the
             * local host name to select an address.
             */
            InetSocketAddress address = _acceptor.getSocketAddress();
            if (address == null) {
                throw new BindException("P2P socket is not bound");
            }

            if (address.getAddress().isAnyLocalAddress()) {
                address = new InetSocketAddress(InetAddress.getLocalHost(),
                                                address.getPort());
            }
            _address = address;
            success = true;
        } finally {
            if (!success) {
                _acceptor.unregister(_id);
            }
        }

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
     * Sets the thread used for the file transfer.
     */
    synchronized private void setThread(Thread thread)
    {
        _thread = thread;
    }

    /**
     * Transfer the file from the source pool using the given data
     * streams. Normally called by the Acceptor.
     */
    public void transfer(DataInputStream in, DataOutputStream out)
        throws IllegalStateException
    {
        WriteHandle handle;
        synchronized (this) {
            try {
                CompanionContext.CompanionState state = _fsm.getState();
                if (state != CompanionContext.FSM.CreatingMover &&
                    state != CompanionContext.FSM.WaitingForConnection)
                    throw new IllegalStateException("Wrong state [" + state + "]");

                handle = _repository.createEntry(_pnfsId,
                                                 _storageInfo,
                                                 EntryState.FROM_POOL,
                                                 _targetState,
                                                 _stickyRecords);
            } catch (FileInCacheException e) {
                _fsm.createEntryFailed();
                throw new IllegalStateException("File already exists", e);
            }
            setThread(Thread.currentThread());
            _fsm.connected();
        }

        Throwable error = null;
        try {
            try {
                File file = handle.getFile();
                CacheEntry entry = handle.getEntry();
                long size = entry.getStorageInfo().getFileSize();

                handle.allocate(size);
                runIO(in, out, file, size);
            } finally {
                setThread(null);
                Thread.interrupted();
            }
            handle.commit(null);
        } catch (Throwable e) {
            error = e;
        } finally {
            handle.close();
        }

        synchronized (this) {
            _fsm.disconnected(error);
        }
    }

    private void readReply(DataInputStream in, int minLength,
                           int expectedType, int expectedMode)
        throws IOException
    {
        int following = in.readInt();
        if (following < minLength)
            throw new IOException("Protocol Violation : ack too small : "
                                  + following);

        int type = in.readInt();
        if (type != expectedType)
            throw new IOException("Protocol Violation : NOT REQUEST_ACK : "
                                  + type);

        int mode = in.readInt();
        if (mode != expectedMode) // SEEK
            throw new IOException("Protocol Violation : NOT SEEK : " + mode);

        int returnCode = in.readInt();
        if (returnCode != 0) {
            String error = in.readUTF();
            throw new IOException("Request Failed : (" + returnCode
                                  + ") " + error);
        }
    }

    private long getFileSize(DataOutputStream out, DataInputStream in)
        throws IOException
    {
        out.writeInt(4); // bytes following
        out.writeInt(DCapConstants.IOCMD_LOCATE);

        readReply(in, 28, DCapConstants.IOCMD_ACK, DCapConstants.IOCMD_LOCATE);
        long filesize = in.readLong();
        in.readLong(); // file position
        return filesize;
    }

    private void runIO(DataInputStream in, DataOutputStream out,
                       File file, long filesize)
        throws IOException, CacheException, InterruptedException,
               NoSuchAlgorithmException, NoRouteToCellException
    {
        MessageDigest digest =
            _checksumModule.checkOnTransfer()
            ? new Adler32()
            : null;

        int challengeSize = in.readInt();
        while (challengeSize > 0) {
            challengeSize -= in.skipBytes(challengeSize);
        }

        if (filesize != getFileSize(out, in)) {
            throw new IOException("Remote file has incorrect size");
        }

        RandomAccessFile dataFile = new RandomAccessFile(file, "rw");
        try {
            //
            // request the full file
            //
            out.writeInt(12); // bytes following
            out.writeInt(DCapConstants.IOCMD_READ);
            out.writeLong(filesize);
            //
            // waiting for reply
            //
            readReply(in, 12, DCapConstants.IOCMD_ACK, DCapConstants.IOCMD_READ);
            //
            // expecting data chain
            //
            //
            // waiting for reply
            //
            int following = in.readInt();
            if (following < 4)
                throw new IOException("Protocol Violation : ack too small : "
                                      + following);

            int type = in.readInt();
            if (type != DCapConstants.IOCMD_DATA)
                throw new IOException("Protocol Violation : NOT DATA : " + type);

            byte[] data = new byte[256 * 1024];

            int nextPacket = 0;
            long total = 0L;
            while (true) {
                if ((nextPacket = in.readInt()) < 0)
                    break;

                int restPacket = nextPacket;

                while (restPacket > 0) {
                    int block = Math.min(restPacket, data.length);
                    //
                    // we collect a full block before we write it out
                    // (a block always fits into our buffer)
                    //
                    int position = 0;
                    for (int rest = block; rest > 0;) {
                        int rc = in.read(data, position, rest);
                        if (rc < 0)
                            throw new IOException("Premature EOF");

                        rest -= rc;
                        position += rc;
                    }
                    total += block;
                    dataFile.write(data, 0, block);
                    restPacket -= block;

                    if (digest != null) {
                        digest.update(data, 0, block);
                    }
                }
            }
            //
            // waiting for reply
            //
            readReply(in, 12, DCapConstants.IOCMD_FIN, DCapConstants.IOCMD_READ);
            //
            out.writeInt(4); // bytes following
            out.writeInt(DCapConstants.IOCMD_CLOSE);
            //
            // waiting for reply
            //
            readReply(in, 12, DCapConstants.IOCMD_ACK, DCapConstants.IOCMD_CLOSE);

            if (total != filesize) {
                throw new IOException("Amount of received data does not match expected file size");
            }
        } finally {
            try {
                dataFile.getFD().sync();
            } catch (SyncFailedException e) {
                /* Data is not guaranteed to be on disk. Not a fatal
                 * problem, but better generate a warning.
                 */
                _log.warn("Failed to synchronize file with storage device: "
                          + e.getMessage());
            }

            dataFile.close();
        }

        _checksumModule
            .setMoverChecksums(_pnfsId,
                               file,
                               _checksumModule.getDefaultChecksumFactory(),
                               null,
                               digest != null ? new Checksum(digest) : null);
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
        DCapProtocolInfo info =
            new DCapProtocolInfo("DCap", 3, 0,
                                 _address.getAddress().getHostAddress(),
                                 _address.getPort());
        info.setSessionId(_id);

        PoolDeliverFileMessage request =
            new PoolDeliverFileMessage(_poolName, _pnfsId, info, _storageInfo);
        request.setPool2Pool();

        _pool.send(new CellPath(_poolName),
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

    synchronized void setMoverId(int moverId)
    {
        _moverId = moverId;
    }

    synchronized void ping()
    {
        _pool.send(new CellPath(_poolName),
                   "p2p ls -binary " + _moverId, IoJobInfo.class,
                   new Callback());
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
                _log.fatal(String.format("P2P for %s failed: %s", _pnfsId, _error),
                           (Exception) _error);
            } else {
                _log.error(String.format("P2P for %s failed: %s", _pnfsId, _error));
            }
        } else {
            _log.info(String.format("P2P for %s completed", _pnfsId));
        }

        _acceptor.unregister(_id);

        if (_callback != null) {
            final String pnfsId = _pnfsId.toString();
            final Object error = _error;

            _executor.execute(new FireAndForgetTask(new Runnable() {
                    public void run() {
                        Throwable t;

                        if (error == null) {
                            t = null;
                        } else if (error instanceof Throwable) {
                            t = (Throwable)error;
                        } else {
                            t = new CacheException(error.toString());
                        }
                        _callback.cacheFileAvailable(pnfsId, t);
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
    class Callback<T> implements MessageCallback<T>
    {
        public void success(T message)
        {
            _executor.execute(new FireAndForgetTask(new Runnable() {
                    public void run() {
                        synchronized (Companion.this) {
                            _fsm.success();
                        }
                    }
                }));
        }

        public void failure(final int rc, final Object cause)
        {
            _executor.execute(new FireAndForgetTask(new Runnable() {
                    public void run() {
                        synchronized (Companion.this) {
                            _fsm.failure(rc, cause);
                        }
                    }
                }));
        }

        public void timeout()
        {
            _executor.execute(new FireAndForgetTask(new Runnable() {
                    public void run() {
                        synchronized (Companion.this) {
                            _fsm.timeout();
                        }
                    }
                }));
        }

        public void noroute()
        {
            _executor.execute(new FireAndForgetTask(new Runnable() {
                    public void run() {
                        synchronized (Companion.this) {
                            _fsm.noroute();
                        }
                    }
                }));
        }
    }
}
