// $Id: P2PClient.java,v 1.21 2007-10-31 17:27:11 radicke Exp $

package diskCacheV111.pools;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import diskCacheV111.movers.DCapConstants;
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
import dmg.util.Args;
import dmg.util.CommandSyntaxException;

public class P2PClient {
    private final CacheRepository _repository;
    private final CellAdapter _cell;
    private final Acceptor _acceptor = new Acceptor();
    private final Map<Integer, P2PClient.Companion> _sessions = new HashMap<Integer, P2PClient.Companion>();
    private final AtomicInteger _nextId = new AtomicInteger(100);
    private boolean _removeOnExit = true;
    private int _maxActive = 0;
    private long _pnfsTimeout = 5L * 60L * 1000L;
    private boolean _fail = false;
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
                _cell.esay("Problem in opening Server Socket : " + ioe);
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
            } catch (Exception ioe) {
                _cell.esay("Problem in accepting connection : " + ioe);
                _cell.esay(ioe);
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
        private RandomAccessFile _dataFile = null;
        private long _spaceAllocated = 0L;

        private IOHandler(Socket socket) {
            _socket = socket;
            _cell.getNucleus().newThread(this, "IOHandler").start();
        }

        private void setStatus(String status) {
            _cell.say("ID-" + _sessionId + " " + status);
            _status = status;
        }

        private void runIO() throws Exception {

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
            _dataFile = new RandomAccessFile(_companion.getDataFile(), "rw");

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
            MessageDigest digest = new Adler32();
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
                    _dataFile.write(data, 0, block);
                    restPacket -= block;

                    digest.update(data, 0, block);
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
            _cell.esay(sb.toString());

            setStatus("<Done>");
        }

        public void run() {
            try {

                runIO();

                _checksumModule.setMoverChecksums(_companion.getEntry(), null,
                        null, _checksumModule.checkOnTransfer() ? _companion
                                .getTransferChecksum() : null);

                if (_fail)
                    throw new IOException("Transfer failed (simulate)");

            } catch (Exception ioe) {
                setStatus("Error : " + ioe.getMessage());
                _cell.esay(ioe);

                // clean up before exiting
                // not having a _companion at this point means an unsolicited
                // connect happened (e.g. a portscan)
                if (_companion != null) {

                    if (_spaceAllocated != 0L)
                        try {
                            _repository.freeSpace(_spaceAllocated
                                    - _companion.getEntry().getSize());
                        } catch (CacheException ignored) {
                            // exception is never thrown, we can ignore this
                            // safely
                        }

                    try {
                        _repository.removeEntry(_companion.getEntry());
                    } catch (Exception ee) {
                        esay(ee);
                    }
                    CacheFileAvailable callback = _companion.getCallback();
                    if (callback != null) {
                        callback.cacheFileAvailable(_companion.getEntry()
                                .getPnfsId().toString(), ioe);
                    }
                }
                return;
            } finally {
                try {
                    _socket.close();
                } catch (IOException ee) {
                }
                try {
                    if (_dataFile != null)
                        _dataFile.close();
                } catch (IOException ee) {
                }
            }
            CacheRepositoryEntry entry = _companion.getEntry();
            try {
                entry.setCached();
            } catch (CacheException ee) {
                esay(ee);
            }
            //
            // try to get the storage info (no problem if it fails)
            //
            try {
                PnfsGetStorageInfoMessage storageInfoMsg = new PnfsGetStorageInfoMessage(
                        _companion.getEntry().getPnfsId());

                CellMessage answer = _cell.sendAndWait(new CellMessage(
                        new CellPath("PnfsManager"), storageInfoMsg),
                        _pnfsTimeout);

                Message message = (Message) answer.getMessageObject();

                if (message.getReturnCode() != 0)
                    throw new CacheException(message.getReturnCode(), message
                            .getErrorObject().toString());

                StorageInfo info = ((PnfsGetStorageInfoMessage) message)
                        .getStorageInfo();
                entry.setStorageInfo(info);

                String value = info.getKey("flag-s");
                if ((value != null) && (!value.equals(""))) {
                    say("setting sticky bit of " + entry);
                    entry.setSticky(true);
                }

            } catch (Exception eee) {
                esay("Failed to set storageinfo : " + eee);
            }
            CacheFileAvailable callback = _companion.getCallback();
            if (callback != null) {
                callback.cacheFileAvailable(_companion.getEntry().getPnfsId()
                        .toString(), null);
            }
        }

        public String toString() {
            return "id=" + _sessionId + " " + _status;
        }
    }

    public class Companion {
        private final int _id ;
        private IOHandler _ioHandler = null;
        private String _status = "<idle>";
        private final CacheRepositoryEntry _entry;

        private PoolDeliverFileMessage _initOkMsg = null;
        private DoorTransferFinishedMessage _transferOkMsg = null;

        private CacheFileAvailable _callback = null;
        private Checksum transferCS = null;
        private final String _srcPoolName;

        private Companion(CacheRepositoryEntry entry, String srcPoolName) {
            _id = getNextId();
            _entry = entry;
            _sessions.put(_id, this);
            _srcPoolName = srcPoolName;
        }

        private void setTransferChecksum(Checksum checksum) {
            this.transferCS = checksum;
        }

        private Checksum getTransferChecksum() {
            return transferCS;
        }

        private void setCallback(CacheFileAvailable callback) {
            _callback = callback;
        }

        private CacheFileAvailable getCallback() {
            return _callback;
        }

        private int getSessionId() {
            return _id;
        }

        private CacheRepositoryEntry getEntry() {
            return _entry;
        }

        private File getDataFile() throws CacheException {
            return _entry.getDataFile();
        }

        private void setIOHandler(IOHandler ioHandler) {
            _ioHandler = ioHandler;
        }

        public String toString() {
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
                PoolDeliverFileMessage message) {
            _initOkMsg = message;
            say("" + _id + " : PoolDeliverFileMessage : " + message);
            if (message.getReturnCode() != 0) {
                _status = "" + message.getErrorObject();
                esay("" + _id + " -> " + _status);
                remove();
                removeFileEntry();
                return;
            }
            _status = "Pool2Pool message ok";
        }

        private synchronized void poolTransferFinishedArrived(
                DoorTransferFinishedMessage message) {
            _transferOkMsg = message;
            say("" + _id + " : poolTransferFinishedArrived : " + message);
            if (message.getReturnCode() != 0) {
                _status = "" + message.getErrorObject();
                esay("" + _id + " -> " + _status);
                remove();
                removeFileEntry();
                return;
            }
            _status = "Transfer Done";
            remove();
        }

        private void remove() {
            if (_removeOnExit)
                _sessions.remove(_id);
        }

        private void removeFileEntry() {
            try {
                _repository.removeEntry(_entry);
            } catch (Exception ee) {
                esay("Can't remove entry on Error : " + ee);
                esay(ee);
            }
        }

        public String getSourcePool() {
            return _srcPoolName;
        }
    }

    public P2PClient(CellAdapter cell, CacheRepository repository,
            ChecksumModuleV1 csModule) {
        _repository = repository;
        _cell = cell;
        _checksumModule = csModule;
    }

    public Companion newCompanion(PnfsId pnfsId, String poolName,
            StorageInfo storageInfo, CacheFileAvailable callback) throws Exception {

        //
        // make sure the entry doens't yet exist.
        // and create it.
        //
        CacheRepositoryEntry entry = _repository.createEntry(pnfsId);
        try {
            entry.setReceivingFromStore();
        } catch (CacheException ee) {
            _repository.removeEntry(entry);
            throw ee;
        }
        //
        // create our companion
        //
        Companion companion = new Companion(entry, poolName);
        companion.setCallback(callback);
        //
        // start the listener (if not yet done)
        //
        _acceptor.start();
        //
        // construct the actual request to the remote pool.
        //
        DCapProtocolInfo pinfo = new DCapProtocolInfo("DCap", 3, 0, InetAddress
                .getLocalHost().getHostAddress(), _acceptor.getPort());
        pinfo.setSessionId(companion.getSessionId());

        StorageInfo sinfo = storageInfo != null ? storageInfo
                : new DummyStorageInfo();

        PoolDeliverFileMessage request = new PoolDeliverFileMessage(poolName,
                pnfsId, pinfo, sinfo);
        request.setPool2Pool();

        try {
            _cell.sendMessage(new CellMessage(new CellPath(poolName), request));
        } catch (Exception ee) {
            _cell.esay("Problem in sending request to " + poolName + " : "
                    + ee.getMessage());
            _cell.esay(ee);
            _sessions.remove(companion.getSessionId());
            try {
                _repository.removeEntry(entry);
            } catch (Exception eee) {
                esay("Couldn't remove entry after failure : " + eee);
                esay(eee);
            }
            throw ee;
        }

        return companion;
    }

    public void messageArrived(Message message, CellMessage cellMessage) {
        _cell.say("Message arrived (" + message.getClass().getName() + " : "
                + message);
        if (message instanceof PoolDeliverFileMessage) {

            PoolDeliverFileMessage msg = (PoolDeliverFileMessage) message;
            DCapProtocolInfo pinfo = (DCapProtocolInfo) msg.getProtocolInfo();
            int sessionId = pinfo.getSessionId();
            Companion companion = _sessions.get(sessionId);
            if (companion == null) {
                _cell.esay("companion not found for id " + sessionId);
                return;
            }
            companion.pool2PoolIoFileMsgArrived(msg);

        } else if (message instanceof DoorTransferFinishedMessage) {

            DoorTransferFinishedMessage msg = (DoorTransferFinishedMessage) message;
            DCapProtocolInfo pinfo = (DCapProtocolInfo) msg.getProtocolInfo();
            int sessionId = pinfo.getSessionId();
            Companion companion = _sessions.get(sessionId);
            if (companion == null) {
                _cell.esay("companion not found for id " + sessionId);
                return;
            }

            companion.poolTransferFinishedArrived(msg);

        } else {
            _cell.esay("Unexpected message arrived ("
                    + message.getClass().getName() + " : " + message);
        }
    }

    public void getInfo(PrintWriter pw) {
        pw
                .println(" Pool to Pool (P2P) [$Id: P2PClient.java,v 1.21 2007-10-31 17:27:11 radicke Exp $]");
        pw.println("  Listener   : " + _acceptor);
        pw.println("  Max Active : " + _maxActive);
        pw.println("Pnfs Timeout : " + (_pnfsTimeout / 1000L) + " seconds ");
    }

    public void printSetup(PrintWriter pw) {
        pw
                .println("#\n#  Pool to Pool (P2P) [$Id: P2PClient.java,v 1.21 2007-10-31 17:27:11 radicke Exp $]\n#");
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

    public String ac_pp_get_file_$_2(Args args) throws Exception {
        PnfsId pnfsId = new PnfsId(args.argv(0));
        String pool = args.argv(1);
        newCompanion(pnfsId, pool, null, null);
        return "Transfer Initiated";
    }

    public String hh_pp_remove = "<id>";

    public String ac_pp_remove_$_1(Args args) throws Exception {
        Object o = _sessions.remove(Integer.valueOf(args.argv(0)));
        if (o == null)
            throw new IllegalArgumentException("Id not found : " + args.argv(0));
        return "";
    }

    public String hh_pp_keep = "on|off";

    public String ac_pp_keep_$_1(Args args) throws Exception {
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

    public String ac_pp_ls(Args args) throws Exception {

        StringBuffer sb = new StringBuffer();

        for (Companion c : _sessions.values()) {
            sb.append(c.toString()).append("\n");
        }
        return sb.toString();
    }

    public String hh_pp_fail = " on|off  # DEBUG ";

    public String ac_pp_fail_$_1(Args args) throws Exception {
        if (args.argv(0).equals("on")) {
            _fail = true;
        } else if (args.argv(0).equals("off")) {
            _fail = false;
        } else {
            throw new CommandSyntaxException("pp fail on|off # DEBUG ONLY");
        }
        return "Done";
    }
}
