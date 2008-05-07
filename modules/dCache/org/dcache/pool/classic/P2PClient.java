// $Id: P2PClient.java,v 1.21 2007-10-31 17:27:11 radicke Exp $

package org.dcache.pool.classic;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.io.NotSerializableException;
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

import diskCacheV111.pools.ChecksumModuleV1;
import diskCacheV111.movers.DCapConstants;
import diskCacheV111.movers.DCapProtocol_3_nio;
import diskCacheV111.repository.CacheRepository;
import diskCacheV111.repository.CacheRepositoryEntry;
import diskCacheV111.util.Adler32;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.CacheFileAvailable;
import diskCacheV111.util.Checksum;
import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.DCapProtocolInfo;
import diskCacheV111.vehicles.DoorTransferFinishedMessage;
import diskCacheV111.vehicles.DummyStorageInfo;
import diskCacheV111.vehicles.Message;
import diskCacheV111.vehicles.PnfsGetStorageInfoMessage;
import diskCacheV111.vehicles.PoolDeliverFileMessage;
import diskCacheV111.vehicles.StorageInfo;
import dmg.cells.nucleus.CellAdapter;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.NoRouteToCellException;
import dmg.util.Args;
import dmg.util.CommandSyntaxException;

public class P2PClient {

	private final static Logger _logSpaceAllocation = Logger.getLogger("logger.dev.org.dcache.poolspacemonitor." + P2PClient.class.getName());

    private final CacheRepository _repository;
    private final CellAdapter _cell;
    private final Acceptor _acceptor = new Acceptor();
    private final Map<Integer, P2PClient.Companion> _sessions = new HashMap<Integer, P2PClient.Companion>();
    private final AtomicInteger _nextId = new AtomicInteger(100);
    private boolean _removeOnExit = true;
    private int _maxActive = 0;
    private long _pnfsTimeout = 5L * 60L * 1000L;
    private boolean _simulateIOFailure = false;
    private final ChecksumModuleV1 _checksumModule;

    private int getNextId() {
        return _nextId.getAndIncrement();
    }

    //
    // the connection listener
    //
    private class Acceptor implements Runnable {
        private ServerSocket _serverSocket = null;
        private int _listenPort = -1;
        private int _recommendedPort = 0;
        private Thread _worker = null;
        private String _error = null;

        private synchronized void setPort(int listenPort) {
            if (_serverSocket != null)
                throw new IllegalArgumentException("Port already listening");
            _recommendedPort = listenPort;
        }

        private synchronized int getPort() {
            return _listenPort;
        }

        private synchronized void start() {
            if (_serverSocket != null)
                return;
            try {
                _serverSocket = new ServerSocket(_recommendedPort);
                _listenPort = _serverSocket.getLocalPort();
            } catch (IOException ioe) {
                _error = ioe.getMessage();
                esay("Problem in opening Server Socket : " + ioe);
                return;
            }
            _error = null;
            _worker = _cell.getNucleus().newThread(this, "Acceptor");
            _worker.start();
        }

        public void run() {
            try {
                while (true) {
                    new IOHandler(_serverSocket.accept());
                }
            } catch (IOException ioe) {
                esay("Problem in accepting connection : " + ioe);
            } catch (Exception ioe) {
                esay("Bug detected : " + ioe);
                esay(ioe);
            }
        }

        public String toString() {
            return "Listen port (recommended="
                    + _recommendedPort
                    + ") "
                    + (_error != null ? ("Error : " + _error)
                            : _listenPort < 0 ? "Inactive" : "" + _listenPort);
        }
    }

    private void say(String message) {
        _cell.say("PP : " + message);
    }

    private void esay(String message) {
        _cell.esay("PP : " + message);
    }

    private void esay(Exception e) {
        _cell.esay(e);
    }

    public int getActiveJobs() {
        return _sessions.size() <= _maxActive ? _sessions.size() : _maxActive;
    }

    public int getMaxActiveJobs() {
        return _maxActive;
    }

    public int getQueueSize() {
        return _sessions.size() > _maxActive ? (_sessions.size() - _maxActive)
                : 0;
    }

    //
    // the io handler
    //
    private class IOHandler implements Runnable {
        private Socket _socket = null;
        private int _sessionId = 0;
        private String _status = "<Idle>";
        private Companion _companion = null;
        private long _spaceAllocated = 0L;

        private IOHandler(Socket socket) {
            _socket = socket;
            _cell.getNucleus().newThread(this, "IOHandler").start();
        }

        private void setStatus(String status) {
            say("ID-" + _sessionId + " " + status);
            _status = status;
        }

        private void runIO()
            throws IOException, CacheException, InterruptedException,
                   UnknownHostException, NoSuchAlgorithmException
        {

            DataInputStream in = new DataInputStream(_socket.getInputStream());

            setStatus("<init>");
            _sessionId = in.readInt();

            //
            // find the session id
            //
            _companion = _sessions.get(_sessionId);
            if (_companion == null)
                throw new IOException("Unexpected Session Id : " + _sessionId);
            _companion.setIOHandler(this);

            RandomAccessFile dataFile =
                new RandomAccessFile(_companion.getDataFile(), "rw");

            boolean checksummingOn = _checksumModule.checkOnTransfer();
            MessageDigest digest = checksummingOn ? new Adler32() : null;

            try {
                int challengeSize = in.readInt();
                in.skipBytes(challengeSize);

                DataOutputStream out = new DataOutputStream(_socket
                        .getOutputStream());

                setStatus("<gettingFilesize>");
                out.writeInt(4); // bytes following
                out.writeInt(DCapConstants.IOCMD_LOCATE);
                //
                // waiting for reply
                //
                int following = in.readInt();
                if (following < 28)
                    throw new IOException("Protocol Violation : ack too small : "
                            + following);

                int type = in.readInt();
                if (type != DCapConstants.IOCMD_ACK)
                    throw new IOException("Protocol Violation : NOT REQUEST_ACK : "
                            + type);

                int mode = in.readInt();
                if (mode != DCapConstants.IOCMD_LOCATE) // SEEK
                    throw new IOException("Protocol Violation : NOT SEEK : " + mode);

                int returnCode = in.readInt();
                if (returnCode != 0) {
                    String error = in.readUTF();
                    throw new IOException("Seek Request Failed : (" + returnCode
                            + ") " + error);
                }
                long filesize = in.readLong();
                setStatus("<WaitingForSpace-" + filesize + ">");
                _logSpaceAllocation.debug("ALLOC: " + _companion.getEntry().getPnfsId() + " : " + filesize );
                _repository.allocateSpace(filesize);
                _spaceAllocated = filesize;
                //
                in.readLong(); // file position

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
                following = in.readInt();
                if (following < 12)
                    throw new IOException("Protocol Violation : ack too small : "
                            + following);

                type = in.readInt();
                if (type != DCapConstants.IOCMD_ACK)
                    throw new IOException("Protocol Violation : NOT REQUEST_ACK : "
                            + type);

                mode = in.readInt();
                if (mode != DCapConstants.IOCMD_READ)
                    throw new IOException("Protocol Violation : NOT READ : " + mode);

                returnCode = in.readInt();
                if (returnCode != 0) {
                    String error = in.readUTF();
                    throw new IOException("Read Request Failed : (" + returnCode
                            + ") " + error);
                }
                setStatus("<RunningIO>");
                //
                // expecting data chain
                //
                //
                // waiting for reply
                //
                following = in.readInt();
                if (following < 4)
                    throw new IOException("Protocol Violation : ack too small : "
                            + following);

                type = in.readInt();
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

                        if (checksummingOn) {
                            digest.update(data, 0, block);
                        }
                    }
                }
                setStatus("<WaitingForReadAck>");
                //
                // waiting for reply
                //
                following = in.readInt();
                if (following < 12)
                    throw new IOException("Protocol Violation : ack too small : "
                            + following);

                type = in.readInt();
                if (type != DCapConstants.IOCMD_FIN)
                    throw new IOException("Protocol Violation : NOT REQUEST_FIN : "
                            + type);

                mode = in.readInt();
                if (mode != DCapConstants.IOCMD_READ)
                    throw new IOException("Protocol Violation : NOT READ : " + mode);

                returnCode = in.readInt();
                if (returnCode != 0) {
                    String error = in.readUTF();
                    throw new IOException("Read Fin Failed : (" + returnCode + ") "
                            + error);
                }
                setStatus("<WaitingForCloseAck>");
                //
                out.writeInt(4); // bytes following
                out.writeInt(DCapConstants.IOCMD_CLOSE);
                //
                // waiting for reply
                //
                following = in.readInt();
                if (following < 12)
                    throw new IOException("Protocol Violation : ack too small : "
                            + following);

                type = in.readInt();
                if (type != DCapConstants.IOCMD_ACK)
                    throw new IOException("Protocol Violation : NOT REQUEST_ACK : "
                            + type);

                mode = in.readInt();
                if (mode != DCapConstants.IOCMD_CLOSE)
                    throw new IOException("Protocol Violation : NOT CLOSE : " + mode);

                returnCode = in.readInt();
                if (returnCode != 0) {
                    String error = in.readUTF();
                    throw new IOException("Close ack Failed : (" + returnCode
                            + ") " + error);
                }

                if (total != filesize) {
                    throw new IOException("Amount of received data does not match expected file size");
                }
            } finally {
                dataFile.close();
            }

            if (checksummingOn) {

                _companion.setTransferChecksum(new Checksum(digest));

                StringBuilder sb = new StringBuilder(
                        "Adler32 checksum computed for p2p transfer (");
                sb.append("SessionID=");
                sb.append(_sessionId);
                sb.append(" pnfsid=");
                sb.append(_companion.getEntry().getPnfsId());
                sb.append(" sourcePool=");
                sb.append(_companion.getSourcePool());
                sb.append(" checksum="
                        + _companion.getTransferChecksum().toHexString());
                sb.append(")");
                esay(sb.toString());
            }


                setStatus("<Done>");

        }

        private void adjustSpaceAllocation()
        {
            if (_companion != null) {
                try {
                    long size = _companion.getDataFile().length();
                    long overAllocation = _spaceAllocated - size;
                    PnfsId pnfsId = _companion.getEntry().getPnfsId();

                    if (overAllocation > 0) {
                        _logSpaceAllocation.debug("FREE: " + pnfsId + " : " + overAllocation);
                        _repository.freeSpace(overAllocation);
                    } else if (overAllocation < 0) {
                        esay("Bug detected, not enough space allocated for P2P");
                        _logSpaceAllocation.debug("ALLOCATE: " + pnfsId + " : " + -overAllocation);
                        _repository.allocateSpace(-overAllocation);
                    }
                    _spaceAllocated = size;
                } catch (CacheException e) {
                    throw new RuntimeException("Bug detected, unexpected exception", e);
                } catch (InterruptedException e) {
                    esay("Could not adjust space allocation. Expect it to be wrong!");
                    Thread.currentThread().interrupt();
                }
            }
        }

        public void run() {
            try {
                try {
                    runIO();

                    _checksumModule.setMoverChecksums(_companion.getEntry(), null,
                                                      null, _checksumModule.checkOnTransfer() ? _companion
                                                      .getTransferChecksum() : null);

                    if (_simulateIOFailure)
                        throw new IOException("Transfer failed (simulate)");
                } catch (Exception e) {
                    /* Not having a companion at this point means an
                     * unsolicited connect happened (e.g. a portscan).
                     */
                    if (_companion == null) {
                        esay("Unsolicited connection from " +
                             _socket.getRemoteSocketAddress());
                        return;
                    }
                    throw e;
                } finally {
                    try {
                        _socket.close();
                    } catch (IOException e) {
                        // take it easy
                    }
                    adjustSpaceAllocation();
                }
            } catch (Exception e) {
                setStatus("Error : " + e.getMessage());
                esay(e);
                _companion.transferFailed(e);
                return;
            }

            CacheRepositoryEntry entry = _companion.getEntry();

            /* Try to get the storage info (no problem if it
             * fails).
             */
            try {
                PnfsGetStorageInfoMessage storageInfoMsg =
                    new PnfsGetStorageInfoMessage(
                        _companion.getEntry().getPnfsId());

                CellMessage answer = _cell.sendAndWait(new CellMessage(
                        new CellPath("PnfsManager"), storageInfoMsg),
                        _pnfsTimeout);

                Message message = (Message)answer.getMessageObject();
                if (message.getReturnCode() != 0)
                    throw new CacheException(message.getReturnCode(), message
                            .getErrorObject().toString());

                StorageInfo info =
                    ((PnfsGetStorageInfoMessage)message).getStorageInfo();

                /* If the file size doesn't match, we fail the
                 * transfer.
                 */
                if (info.getFileSize() != entry.getDataFile().length()) {
                    esay("Incomplete file received (file size does not match storage information)");
                    _companion.transferFailed("Incomplete file received");
                    return;
                }

                entry.setStorageInfo(info);

                /* We mark the file sticky before making it available
                 * (marking it cached) to avoid a race condition in
                 * which the sweeper could delete the file between the
                 * two events.
                 */
                String value = info.getKey("flag-s");
                if (value != null && !value.equals("")) {
                    say("setting sticky bit of " + entry);
                    entry.setSticky(true);
                }
            } catch (NotSerializableException e) {
                esay("Bug detected: Unserializable vehicle: " + e.getMessage());
                esay(e);
            } catch (CacheException e) {
                esay("Failed to set storageinfo : " + e.getMessage());
            } catch (Exception e) {
                esay("Bug detected: " + e.getMessage());
                esay(e);
            }

            try {
                /* Make the file available. This action will unlock the
                 * file (the pool does this).
                 */
                entry.setCached();
            } catch (CacheException e) {
                esay("Bug detected: " + e.getMessage());
                esay(e);
            }

            _companion.clientSucceeded();
        }

        public String toString() {
            return "id=" + _sessionId + " " + _status;
        }
    }

    public class Companion
    {
        private final int _id ;
        private final CacheRepositoryEntry _entry;
        private final String _srcPoolName;
        private final CacheFileAvailable _callback;

        private String _status = "<idle>";
        private IOHandler _ioHandler;
        private Checksum _transferCS;
        private boolean _failed = false;
        private boolean _serverSucceeded = false;
        private boolean _clientSucceeded = false;

        private Companion(PnfsId pnfsId, String srcPoolName,
                          CacheFileAvailable callback)
            throws CacheException
        {
            _id = getNextId();
            _srcPoolName = srcPoolName;
            _callback = callback;

            synchronized (_repository) {
                _entry = _repository.createEntry(pnfsId);
                try {
                    _entry.lock(true);
                    _entry.setReceivingFromStore();
                } catch (CacheException e) {
                    removeEntry();
                    throw e;
                }
            }

            _sessions.put(_id, this);
        }

        /**
         * Deletes the repository entry associated with the companion.
         */
        private synchronized void removeEntry()
        {
            try {
                _entry.lock(false);
                _repository.removeEntry(_entry);
            } catch (CacheException e) {
                esay("Cannot remove entry on error: " + e.getMessage());
                esay(e);
            }
        }

        private synchronized void setTransferChecksum(Checksum checksum)
        {
            _transferCS = checksum;
        }

        private synchronized Checksum getTransferChecksum()
        {
            return _transferCS;
        }

        private synchronized int getSessionId()
        {
            return _id;
        }

        private synchronized CacheRepositoryEntry getEntry()
        {
            return _entry;
        }

        private synchronized File getDataFile() throws CacheException
        {
            return _entry.getDataFile();
        }

        private synchronized void setIOHandler(IOHandler ioHandler)
        {
            _ioHandler = ioHandler;
        }

        public synchronized String toString()
        {
            return ""
                    + _id
                    + " "
                    + _entry.getPnfsId()
                    + " "
                    + _status
                    + " "
                    + (_ioHandler == null ? "<noHandlerYet>" : _ioHandler
                            .toString());
        }

        private synchronized void pool2PoolIoFileMsgArrived(
                PoolDeliverFileMessage message)
        {
            say("" + _id + " : PoolDeliverFileMessage : " + message);
            if (message.getReturnCode() != 0) {
                transferFailed(message.getErrorObject());
                return;
            }
            _status = "Pool2Pool message ok";
        }

        private synchronized void poolTransferFinishedArrived(
                DoorTransferFinishedMessage message)
        {
            say("" + _id + " : poolTransferFinishedArrived : " + message);
            if (message.getReturnCode() != 0) {
                transferFailed(message.getErrorObject());
                return;
            }
            serverSucceeded();
        }

        private synchronized void remove()
        {
            if (_removeOnExit)
                _sessions.remove(_id);
        }

        public synchronized String getSourcePool()
        {
            return _srcPoolName;
        }

        /**
         * Calls the callback with the given failure cause. The cause
         * may be <code>null</code>, in which case the callback
         * indicates a successful transfer.
         */
        private void callCallback(Object cause)
        {
            if (_callback != null) {
                String pnfsId = _entry.getPnfsId().toString();
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
        }

        /**
         * Causes the callback to be called with the given failure
         * cause, the repository entry to be deleted, and the
         * companion to be closed.
         */
        public synchronized void transferFailed(Object cause)
        {
            if (!_failed) {
                if (_clientSucceeded && _serverSucceeded)
                    throw new IllegalStateException("Cannot fail a finished transfer");
                _failed = true;
                _status = cause.toString();
                esay(String.format("%d -> %s", _id, cause));

                remove();
                removeEntry();
                callCallback(cause);
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
        private void succeedIfDone()
        {
            if (_clientSucceeded && _serverSucceeded && !_failed) {
                _status = "Transfer done";
                remove();
                callCallback(null);
            }
        }

        /**
         * Reports that the receiver believes that the transfer
         * succeeded.
         */
        public synchronized void clientSucceeded()
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
        public synchronized void serverSucceeded()
        {
            if (_serverSucceeded)
                throw new IllegalStateException("Duplicate call not allowed");
            _serverSucceeded = true;
            succeedIfDone();
        }
    }

    public P2PClient(CellAdapter cell, CacheRepository repository,
            ChecksumModuleV1 csModule)
    {
        _repository = repository;
        _cell = cell;
        _checksumModule = csModule;
    }

    public Companion newCompanion(PnfsId pnfsId, String poolName,
            StorageInfo storageInfo, CacheFileAvailable callback)
        throws CacheException, UnknownHostException
    {
        //
        // start the listener (if not yet done)
        //
        _acceptor.start();

        //
        // create our companion
        //
        boolean success = false;
        Companion companion = new Companion(pnfsId, poolName, callback);
        try {
            //
            // construct the actual request to the remote pool.
            //
            DCapProtocolInfo pinfo =
                new DCapProtocolInfo("DCap", 3, 0,
                                     InetAddress.getLocalHost().getHostAddress(),
                                     _acceptor.getPort());
            pinfo.setSessionId(companion.getSessionId());

            StorageInfo sinfo =
                storageInfo != null ? storageInfo : new DummyStorageInfo();

            PoolDeliverFileMessage request =
                new PoolDeliverFileMessage(poolName, pnfsId, pinfo, sinfo);
            request.setPool2Pool();

            _cell.sendMessage(new CellMessage(new CellPath(poolName), request));
            success = true;
        } catch (NotSerializableException e) {
            throw new RuntimeException("Bug detected: Unserializable vehicle", e);
        } catch (NoRouteToCellException e) {
            esay("Problem in sending request to " + poolName + " : "
                 + e.getMessage());
        } finally {
            if (!success) {
                companion.remove();
                companion.removeEntry();
            }
        }

        return companion;
    }

    public void messageArrived(Message message, CellMessage cellMessage) {
        say("Message arrived (" + message.getClass().getName() + " : "
            + message);
        if (message instanceof PoolDeliverFileMessage) {

            PoolDeliverFileMessage msg = (PoolDeliverFileMessage) message;
            DCapProtocolInfo pinfo = (DCapProtocolInfo) msg.getProtocolInfo();
            int sessionId = pinfo.getSessionId();
            Companion companion = _sessions.get(sessionId);
            if (companion == null) {
                esay("companion not found for id " + sessionId);
                return;
            }
            companion.pool2PoolIoFileMsgArrived(msg);

        } else if (message instanceof DoorTransferFinishedMessage) {

            DoorTransferFinishedMessage msg = (DoorTransferFinishedMessage) message;
            DCapProtocolInfo pinfo = (DCapProtocolInfo) msg.getProtocolInfo();
            int sessionId = pinfo.getSessionId();
            Companion companion = _sessions.get(sessionId);
            if (companion == null) {
                esay("companion not found for id " + sessionId);
                return;
            }

            companion.poolTransferFinishedArrived(msg);

        } else {
            esay("Unexpected message arrived ("
                 + message.getClass().getName() + " : " + message);
        }
    }

    public void getInfo(PrintWriter pw) {
        pw.println(" Pool to Pool (P2P) [$Id: P2PClient.java,v 1.21 2007-10-31 17:27:11 radicke Exp $]");
        pw.println("  Listener   : " + _acceptor);
        pw.println("  Max Active : " + _maxActive);
        pw.println("Pnfs Timeout : " + (_pnfsTimeout / 1000L) + " seconds ");
    }

    public void printSetup(PrintWriter pw) {
        pw.println("#\n#  Pool to Pool (P2P) [$Id: P2PClient.java,v 1.21 2007-10-31 17:27:11 radicke Exp $]\n#");
        pw.println("pp set port " + _acceptor._recommendedPort);
        pw.println("pp set max active " + _maxActive);
        pw.println("pp set pnfs timeout " + (_pnfsTimeout / 1000L));
    }

    public String hh_pp_set_pnfs_timeout = "<Timeout/sec>";

    public String ac_pp_set_pnfs_timeout_$_1(Args args) {
        _pnfsTimeout = Long.parseLong(args.argv(0)) * 1000L;
        return "Pnfs timeout set to " + (_pnfsTimeout / 1000L) + " seconds";
    }

    public String hh_pp_set_max_active = "<normalization>";

    public String ac_pp_set_max_active_$_1(Args args) {
        _maxActive = Integer.parseInt(args.argv(0));
        return "";
    }

    public String hh_pp_set_port = "<listenPort>";

    public String ac_pp_set_port_$_1(Args args) {
        _acceptor.setPort(Integer.parseInt(args.argv(0)));
        return "";
    }

    public String hh_pp_get_file = "<pnfsId> <pool>";

    public String ac_pp_get_file_$_2(Args args)
        throws CacheException, UnknownHostException
    {
        PnfsId pnfsId = new PnfsId(args.argv(0));
        String pool = args.argv(1);
        newCompanion(pnfsId, pool, null, null);
        return "Transfer Initiated";
    }

    public String hh_pp_remove = "<id>";

    public String ac_pp_remove_$_1(Args args)
        throws NumberFormatException
    {
        Object o = _sessions.remove(Integer.valueOf(args.argv(0)));
        if (o == null)
            throw new IllegalArgumentException("Id not found : " + args.argv(0));
        return "";
    }

    public String hh_pp_keep = "on|off";

    public String ac_pp_keep_$_1(Args args)
    {
        String mode = args.argv(0);
        if (mode.equals("on")) {
            _removeOnExit = true;
        } else if (mode.equals("off")) {
            _removeOnExit = false;
        } else {
            throw new IllegalArgumentException("Usage : pp keep on|off");
        }
        return "";
    }

    public String hh_pp_ls = " # get the list of companions";

    public String ac_pp_ls(Args args)
    {
        StringBuffer sb = new StringBuffer();

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
