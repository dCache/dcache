// $Id: P2PClient.java,v 1.21 2007-10-31 17:27:11 radicke Exp $

package org.dcache.pool.classic;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;
import org.dcache.pool.repository.v5.CacheRepositoryV5;
import org.dcache.pool.repository.StickyRecord;
import org.dcache.pool.repository.EntryState;
import org.dcache.pool.repository.WriteHandle;
import org.dcache.pool.repository.CacheEntry;
import org.dcache.cell.AbstractCellComponent;
import org.dcache.cell.CellMessageReceiver;
import org.dcache.cell.CellCommandListener;
import diskCacheV111.movers.DCapConstants;
import diskCacheV111.movers.DCapProtocol_3_nio;
import diskCacheV111.util.Adler32;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.FileInCacheException;
import diskCacheV111.util.CacheFileAvailable;
import diskCacheV111.util.Checksum;
import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.DCapProtocolInfo;
import diskCacheV111.vehicles.DoorTransferFinishedMessage;
import diskCacheV111.vehicles.Message;
import diskCacheV111.vehicles.PnfsGetStorageInfoMessage;
import diskCacheV111.vehicles.PoolDeliverFileMessage;
import diskCacheV111.vehicles.StorageInfo;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellMessageAnswerable;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.CellInfo;
import dmg.cells.nucleus.CellEndpoint;
import dmg.cells.nucleus.NoRouteToCellException;
import dmg.util.Args;
import dmg.util.CommandSyntaxException;

public class P2PClient
    extends AbstractCellComponent
    implements CellMessageReceiver,
               CellCommandListener
{
    private final static Logger _log = Logger.getLogger(P2PClient.class);

    private final CacheRepositoryV5 _repository;
    private final Acceptor _acceptor = new Acceptor();
    private final Map<Integer, P2PClient.Companion> _sessions =
        new HashMap<Integer, P2PClient.Companion>();
    private final AtomicInteger _nextId = new AtomicInteger(100);
    private final ChecksumModuleV1 _checksumModule;

    private int _maxActive = 0;

    private String _pnfsManager = "PnfsManager";
    private long _pnfsTimeout = 5L * 60L * 1000L;
    private long _poolTimeout = 5L * 60L * 1000L;

    private boolean _simulateIOFailure = false;

    private int getNextId()
    {
        return _nextId.getAndIncrement();
    }

    //
    // the connection listener
    //
    private class Acceptor implements Runnable
    {
        private ServerSocket _serverSocket = null;
        private int _listenPort = -1;
        private int _recommendedPort = 0;
        private Thread _worker = null;
        private String _error = null;

        private synchronized void setPort(int listenPort)
        {
            if (_serverSocket != null)
                throw new IllegalArgumentException("Port already listening");
            _recommendedPort = listenPort;
        }

        private synchronized int getPort()
        {
            return _listenPort;
        }

        private synchronized void start()
        {
            if (_serverSocket != null)
                return;
            try {
                _serverSocket = new ServerSocket(_recommendedPort);
                _listenPort = _serverSocket.getLocalPort();
            } catch (IOException ioe) {
                _error = ioe.getMessage();
                _log.error("Problem in opening Server Socket : " + ioe);
                return;
            }
            _error = null;
            _worker = new Thread(this, "Acceptor");
            _worker.start();
        }

        public void run()
        {
            try {
                while (true) {
                    new IOHandler(_serverSocket.accept());
                }
            } catch (IOException ioe) {
                _log.error("Problem in accepting connection : " + ioe);
            } catch (Exception ioe) {
                _log.error("Bug detected : " + ioe);
                _log.error(ioe);
            }
        }

        public String toString()
        {
            return "Listen port (recommended="
                    + _recommendedPort
                    + ") "
                    + (_error != null ? ("Error : " + _error)
                            : _listenPort < 0 ? "Inactive" : "" + _listenPort);
        }
    }

    public int getActiveJobs()
    {
        return _sessions.size() <= _maxActive ? _sessions.size() : _maxActive;
    }

    public int getMaxActiveJobs()
    {
        return _maxActive;
    }

    public int getQueueSize()
    {
        return _sessions.size() > _maxActive ? (_sessions.size() - _maxActive)
                : 0;
    }

    //
    // the io handler
    //
    private class IOHandler implements Runnable
    {
        private final Socket _socket;
        private final MessageDigest _digest;
        private String _status = "<Idle>";

        private IOHandler(Socket socket)
            throws IOException
        {
            _socket = socket;
            _socket.setKeepAlive(true);

            if (_checksumModule.checkOnTransfer()) {
                _digest = new Adler32();
            } else {
                _digest = null;
            }

            new Thread(this, "IOHandler").start();
        }

        private void setStatus(String status)
        {
            _status = status;
        }

        private void readReply(DataInputStream in, int minLength, int expectedType, int expectedMode)
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
            setStatus("<gettingFilesize>");
            out.writeInt(4); // bytes following
            out.writeInt(DCapConstants.IOCMD_LOCATE);

            readReply(in, 28, DCapConstants.IOCMD_ACK, DCapConstants.IOCMD_LOCATE);
            long filesize = in.readLong();
            setStatus("<WaitingForSpace-" + filesize + ">");
            in.readLong(); // file position
            return filesize;
        }

        private void runIO(DataOutputStream out, DataInputStream in,
                           File file, long filesize)
            throws IOException, CacheException, InterruptedException,
                   UnknownHostException, NoSuchAlgorithmException
        {
            int challengeSize = in.readInt();
            in.skipBytes(challengeSize);

            if (filesize != getFileSize(out, in)) {
                throw new IOException("Remote file has incorrect size");
            }

            RandomAccessFile dataFile = new RandomAccessFile(file, "rw");
            try {
                setStatus("<StartingIO>");

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
                setStatus("<RunningIO>");
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
                        _status = "<RunningIo=" + total + ">";
                        dataFile.write(data, 0, block);
                        restPacket -= block;

                        if (_digest != null) {
                            _digest.update(data, 0, block);
                        }
                    }
                }
                setStatus("<WaitingForReadAck>");
                //
                // waiting for reply
                //
                readReply(in, 12, DCapConstants.IOCMD_FIN, DCapConstants.IOCMD_READ);
                setStatus("<WaitingForCloseAck>");
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

            setStatus("<Done>");
        }

        public void run()
        {
            try {
                setStatus("<init>");

                DataInputStream in =
                    new DataInputStream(_socket.getInputStream());
                DataOutputStream out =
                    new DataOutputStream(_socket.getOutputStream());

                Companion companion = _sessions.get(in.readInt());
                if (companion == null) {
                    _log.error("Unsolicited connection from " +
                         _socket.getRemoteSocketAddress());
                    return;
                }

                try {
                    WriteHandle handle = companion.connected(this);
                    File file = handle.getFile();
                    CacheEntry entry = handle.getEntry();
                    long size = entry.getStorageInfo().getFileSize();

                    handle.allocate(size);

                    runIO(out, in, file, size);

                    _checksumModule
                        .setMoverChecksums(entry.getPnfsId(),
                                           file,
                                           _checksumModule.getDefaultChecksumFactory(),
                                           null,
                                           _digest != null ? new Checksum(_digest) : null);

                    if (_simulateIOFailure)
                        throw new IOException("Transfer failed (simulate)");
                } catch (Throwable e) {
                    setStatus("Error: " + e.getMessage());
                    companion.failed(e);
                }
                companion.clientSucceeded();
            } catch (IOException e) {
                /* This happens if we fail to read the session ID. Not
                 * much we can do about this except log the failure.
                 */
                _log.error("Failed to read from " +
                     _socket.getRemoteSocketAddress().toString() +
                     ": " + e.getMessage());
            } finally {
                try {
                    _socket.close();
                } catch (IOException e) {
                    // take it easy
                }
            }
        }

        public String toString()
        {
            return _status;
        }
    }

    public class Companion implements CellMessageAnswerable
    {
        private final int _id;
        private final PnfsId _pnfsId;
        private final String _poolName;
        private final CacheFileAvailable _callback;
        private final EntryState _targetState;

        private StorageInfo _storageInfo;
        private WriteHandle _handle;
        private String _status = "<idle>";
        private IOHandler _ioHandler;
        private boolean _failed = false;
        private boolean _serverSucceeded = false;
        private boolean _clientSucceeded = false;
        private boolean _connected = false;

        private Companion(int id, PnfsId pnfsId,
                          String poolName,
                          EntryState targetState,
                          CacheFileAvailable callback)
        {
            _id = id;
            _pnfsId = pnfsId;
            _poolName = poolName;
            _callback = callback;
            _targetState = targetState;

            _sessions.put(_id, this);

            sendStorageInfoRequest();
        }

        synchronized private void setStorageInfo(StorageInfo info)
        {
            _storageInfo = info;
        }

        synchronized private void createEntry()
            throws FileInCacheException
        {
            StickyRecord sticky;
            String value = _storageInfo.getKey("flag-s");
            if (value != null && value.length() > 0) {
                sticky = new StickyRecord("system", -1);
            } else {
                sticky = null;
            }

            _handle = _repository.createEntry(_pnfsId,
                                              _storageInfo,
                                              EntryState.FROM_POOL,
                                              _targetState,
                                              sticky);
        }

        synchronized private void sendMessage(String destination, Message message, long timeout)
        {
            P2PClient.this.sendMessage(new CellMessage(new CellPath(destination), message),
                                           this, timeout);
        }

        synchronized private void sendStorageInfoRequest()
        {
            _status = "Waiting for storage info";
            sendMessage(_pnfsManager,
                        new PnfsGetStorageInfoMessage(_pnfsId),
                        _pnfsTimeout);
        }

        synchronized private void messageArrived(PnfsGetStorageInfoMessage msg)
        {
            try {
                if (msg.getReturnCode() != 0) {
                    failed(msg.getErrorObject());
                    return;
                }

                setStorageInfo(msg.getStorageInfo());
                createEntry();
                sendDeliveryRequest();
            } catch (FileInCacheException e) {
                failed(e);
            }
        }

        synchronized private void sendDeliveryRequest()
        {
            try {
                _status = "Waiting for connect from pool";

                DCapProtocolInfo pinfo =
                    new DCapProtocolInfo("DCap", 3, 0,
                                         InetAddress.getLocalHost().getHostAddress(),
                                         _acceptor.getPort());
                pinfo.setSessionId(_id);

                PoolDeliverFileMessage request =
                    new PoolDeliverFileMessage(_poolName, _pnfsId, pinfo, _storageInfo);
                request.setPool2Pool();
                sendMessage(_poolName, request, _poolTimeout);
            } catch (UnknownHostException e) {
                failed(e);
            }
        }

        synchronized private void messageArrived(PoolDeliverFileMessage msg)
        {
            if (msg.getReturnCode() != 0) {
                failed(msg.getErrorObject());
            }
        }

        synchronized public WriteHandle connected(IOHandler handler)
        {
            if (_connected)
                throw new IllegalStateException("Already connected");

            _status = "Connected to pool";
            _connected = true;
            _ioHandler = handler;
            return _handle;
        }

        synchronized public void messageArrived(DoorTransferFinishedMessage msg)
        {
            if (msg.getReturnCode() != 0) {
                failed(msg.getErrorObject());
            } else {
                serverSucceeded();
            }
        }

        /**
         * If both sender and receiver have reported success, then the
         * callback is triggered and the companion is closed.
         *
         * Notice that we do not unlock the repository entry. The
         * unlock is done by the pool code as soon as the file is
         * marked as cached.
         */
        synchronized private void succeedIfDone()
        {
            if (_clientSucceeded && _serverSucceeded && !_failed) {
                _status = "Transfer done";
                done(null);
            }
        }

        /**
         * Reports that the receiver believes that the transfer
         * succeeded.
         */
        synchronized public void clientSucceeded()
        {
            if (_clientSucceeded)
                throw new IllegalStateException("Duplicate call not allowed");
            _clientSucceeded = true;
            succeedIfDone();
        }

        /**
         * Reports that the sender believes that the transfer
         * succeeded.
         */
        synchronized public void serverSucceeded()
        {
            if (_serverSucceeded)
                throw new IllegalStateException("Duplicate call not allowed");
            _serverSucceeded = true;
            succeedIfDone();
        }

        synchronized public String toString()
        {
            return ""
                    + _id
                    + " "
                    + _pnfsId
                    + " "
                    + _status
                    + " "
                    + (_ioHandler == null ? "" : _ioHandler.toString());
        }

        /**
         * Calls the callback with the given failure cause. The cause
         * may be <code>null</code>, in which case the callback
         * indicates a successful transfer.
         */
        synchronized private void done(Object cause)
        {
            if (_handle != null) {
                try {
                    _handle.close();
                } catch (InterruptedException e) {
                    // TODO
                } catch (CacheException e) {
                    // TODO
                }
            }

            if (_callback != null) {
                String pnfsId = _pnfsId.toString();
                Throwable t;

                if (cause == null) {
                    t = null;
                } else if (cause instanceof Throwable) {
                    t = (Throwable)cause;
                } else {
                    t = new CacheException(cause.toString());
                }
                _callback.cacheFileAvailable(pnfsId, t);
            }

            _sessions.remove(_id);
        }

        /**
         * Causes the callback to be called with the given failure
         * cause, the repository entry to be deleted, and the
         * companion to be closed.
         */
        synchronized public void failed(Object cause)
        {
            if (!_failed) {
                if (_clientSucceeded && _serverSucceeded)
                    throw new IllegalStateException("Cannot fail a finished transfer");
                _failed = true;
                _status = cause.toString();
                _log.error(String.format("%d -> %s", _id, cause));

                /* If we fail before creating an entry, e.g. because
                 * the entry already exists, then we cannot cancel the
                 * write handle.
                 */
                if (_handle != null)
                    _handle.cancel(false);

                done(cause);
            }
        }

        public void answerArrived(CellMessage request, CellMessage answer)
        {
            Object msg = answer.getMessageObject();
            if (msg instanceof PnfsGetStorageInfoMessage)
                messageArrived((PnfsGetStorageInfoMessage)msg);
            else if (msg instanceof DoorTransferFinishedMessage)
                messageArrived((DoorTransferFinishedMessage)msg);
            else if (msg instanceof PoolDeliverFileMessage)
                messageArrived((PoolDeliverFileMessage)msg);
        }

        public void answerTimedOut(CellMessage request)
        {
            failed("Timout waiting for message from " + request.getDestinationAddress());
        }

        public void exceptionArrived(CellMessage request, Exception exception)
        {
            failed(exception);
        }
    }

    public P2PClient(CacheRepositoryV5 repository, ChecksumModuleV1 csModule)
    {
        _repository = repository;
        _checksumModule = csModule;
    }

    public void messageArrived(DoorTransferFinishedMessage message, CellMessage envelope)
    {
        DCapProtocolInfo pinfo = (DCapProtocolInfo)message.getProtocolInfo();
        int sessionId = pinfo.getSessionId();
        Companion companion = _sessions.get(sessionId);
        if (companion != null) {
            companion.messageArrived(message);
        }
    }

    public void newCompanion(PnfsId pnfsId,
                             String poolName,
                             StorageInfo storageInfo,
                             EntryState targetState,
                             CacheFileAvailable callback)
    {
        if (getCellEndpoint() == null)
            throw new IllegalStateException("Endpoint must be set");

        //
        // start the listener (if not yet done)
        //
        _acceptor.start();

        //
        // create our companion
        //
        new Companion(getNextId(), pnfsId, poolName, targetState,callback);
    }

    public void getInfo(PrintWriter pw)
    {
        pw.println(" Pool to Pool (P2P) [$Id: P2PClient.java,v 1.21 2007-10-31 17:27:11 radicke Exp $]");
        pw.println("  Listener   : " + _acceptor);
        pw.println("  Max Active : " + _maxActive);
        pw.println("Pnfs Timeout : " + (_pnfsTimeout / 1000L) + " seconds ");
    }

    public void printSetup(PrintWriter pw)
    {
        pw.println("#\n#  Pool to Pool (P2P) [$Id: P2PClient.java,v 1.21 2007-10-31 17:27:11 radicke Exp $]\n#");
        pw.println("pp set port " + _acceptor._recommendedPort);
        pw.println("pp set max active " + _maxActive);
        pw.println("pp set pnfs timeout " + (_pnfsTimeout / 1000L));
    }

    public String hh_pp_set_pnfs_timeout = "<Timeout/sec>";
    public String ac_pp_set_pnfs_timeout_$_1(Args args)
    {
        _pnfsTimeout = Long.parseLong(args.argv(0)) * 1000L;
        return "Pnfs timeout set to " + (_pnfsTimeout / 1000L) + " seconds";
    }

    public String hh_pp_set_max_active = "<normalization>";
    public String ac_pp_set_max_active_$_1(Args args)
    {
        _maxActive = Integer.parseInt(args.argv(0));
        return "";
    }

    public String hh_pp_set_port = "<listenPort>";
    public String ac_pp_set_port_$_1(Args args)
    {
        _acceptor.setPort(Integer.parseInt(args.argv(0)));
        return "";
    }

    public String hh_pp_get_file = "<pnfsId> <pool>";
    public String ac_pp_get_file_$_2(Args args)
        throws CacheException, UnknownHostException
    {
        PnfsId pnfsId = new PnfsId(args.argv(0));
        String pool = args.argv(1);
        newCompanion(pnfsId, pool, null, EntryState.CACHED, null);
        return "Transfer Initiated";
    }

    public String hh_pp_remove = "<id>";
    public String ac_pp_remove_$_1(Args args)
        throws NumberFormatException
    {
        Companion companion = _sessions.remove(Integer.valueOf(args.argv(0)));
        if (companion == null)
            throw new IllegalArgumentException("Id not found : " + args.argv(0));
        companion.failed("Cancelled");
        return "";
    }

    public String hh_pp_keep = "on|off";
    public String ac_pp_keep_$_1(Args args)
    {
        return "Deprecated. Command ignored.";
    }

    public String hh_pp_ls = " # get the list of companions";
    public String ac_pp_ls(Args args)
    {
        StringBuilder sb = new StringBuilder();

        for (Companion c : _sessions.values()) {
            sb.append(c.toString()).append("\n");
        }
        return sb.toString();
    }

    public String hh_pp_fail = " on|off  # DEBUG ";
    public String ac_pp_fail_$_1(Args args)
        throws CommandSyntaxException
    {
        if (args.argv(0).equals("on")) {
            _simulateIOFailure = true;
        } else if (args.argv(0).equals("off")) {
            _simulateIOFailure = false;
        } else {
            throw new CommandSyntaxException("pp fail on|off # DEBUG ONLY");
        }
        return "Done";
    }
}
