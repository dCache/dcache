package org.dcache.pool.p2p;

import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.Executor;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.File;
import java.io.RandomAccessFile;
import java.net.UnknownHostException;
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
import diskCacheV111.util.Adler32;
import diskCacheV111.vehicles.StorageInfo;
import diskCacheV111.vehicles.PnfsGetStorageInfoMessage;
import diskCacheV111.vehicles.PoolDeliverFileMessage;
import diskCacheV111.vehicles.DCapProtocolInfo;
import diskCacheV111.vehicles.DoorTransferFinishedMessage;
import diskCacheV111.vehicles.Message;
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

    private final Acceptor _acceptor;
    private final Repository _repository;
    private final ChecksumModuleV1 _checksumModule;
    private final PnfsId _pnfsId;
    private final String _poolName;
    private final EntryState _targetState;
    private final List<StickyRecord> _stickyRecords;
    private final CacheFileAvailable _callback;
    private final InetSocketAddress _address;
    private final Executor _executor;
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
     * @throws UnknownHostException if the host address could not be found
     */
    Companion(Executor executor,
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
        throws UnknownHostException
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
        _address = _acceptor.getSocketAddress();

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
                if (_fsm.getState() != CompanionContext.FSM.WaitingForConnection)
                    throw new IllegalStateException("Connection denied");

                handle = _repository.createEntry(_pnfsId,
                                                 _storageInfo,
                                                 EntryState.FROM_POOL,
                                                 _targetState,
                                                 _stickyRecords);
                setThread(Thread.currentThread());
                _fsm.connected();
            } catch (FileInCacheException e) {
                _fsm.createEntryFailed();
                throw new IllegalStateException("File already exists", e);
            }
        }

        Throwable error = null;
        try {
            File file = handle.getFile();
            CacheEntry entry = handle.getEntry();
            long size = entry.getStorageInfo().getFileSize();

            handle.allocate(size);
            runIO(in, out, file, size);
            handle.commit(null);
        } catch (Throwable e) {
            error = e;
        } finally {
            setThread(null);
            Thread.interrupted();

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
               UnknownHostException, NoSuchAlgorithmException,
               NoRouteToCellException
    {
        MessageDigest digest =
            _checksumModule.checkOnTransfer()
            ? new Adler32()
            : null;

        int challengeSize = in.readInt();
        in.skipBytes(challengeSize);

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

    /**
     * Marks the transfer as failed.
     *
     * @param error Description of the cause of the failure.
     */
    synchronized void setError(Object error)
    {
        if (_error == null) {
            _error = error;
        }
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
        String value = _storageInfo.getKey("flag-s");
        if (value != null && value.length() > 0) {
            _stickyRecords.add(new StickyRecord("system", -1));
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
                   new Callback());
    }

    /**
     * Called at the end of the transfer to free call callbacks and
     * free resources associated with the transfer.
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

        if (_callback != null) {
            String pnfsId = _pnfsId.toString();
            Throwable t;

            if (_error == null) {
                t = null;
            } else if (_error instanceof Throwable) {
                t = (Throwable)_error;
            } else {
                t = new CacheException(_error.toString());
            }
            _callback.cacheFileAvailable(pnfsId, t);
        }

        _acceptor.unregister(_id);
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
     * a forwarded via an executor to guarantee asynchronous delivery
     * (SMC state machines do not allow transitions to be triggered
     * from within transitions).
     */
    class Callback<T extends Message> implements MessageCallback<T>
    {
        public void success(T message)
        {
            _executor.execute(new Runnable() {
                    public void run() {
                        synchronized (Companion.this) {
                            _fsm.success();
                        }
                    }
                });
        }

        public void failure(final int rc, final Object cause)
        {
            _executor.execute(new Runnable() {
                    public void run() {
                        synchronized (Companion.this) {
                            _fsm.failure(rc, cause);
                        }
                    }
                });
        }

        public void timeout()
        {
            _executor.execute(new Runnable() {
                    public void run() {
                        synchronized (Companion.this) {
                            _fsm.timeout();
                        }
                    }
                });
        }

        public void noroute()
        {
            _executor.execute(new Runnable() {
                    public void run() {
                        synchronized (Companion.this) {
                            _fsm.noroute();
                        }
                    }
                });
        }
    }
}
