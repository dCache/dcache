// $Id: DirectoryLookUpPool.java,v 1.7 2007-07-26 14:34:12 tigran Exp $

package diskCacheV111.pools;

import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;

import dmg.cells.nucleus.CellAdapter;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellNucleus;
import dmg.util.Args;

import diskCacheV111.movers.DCapConstants;
import diskCacheV111.movers.DCapDataOutputStream;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsFile;
import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.DCapProtocolInfo;
import diskCacheV111.vehicles.Message;
import diskCacheV111.vehicles.PoolIoFileMessage;

public class DirectoryLookUpPool extends CellAdapter {

    private final String _poolName;
    private final Args _args;
    private final CellNucleus _nucleus;

    private String _rootDir = null;

    public DirectoryLookUpPool(String poolName, String args) throws Exception {
        super(poolName, DirectoryLookUpPool.class.getName(), args, false);

        _poolName = poolName;
        _args = getArgs();
        _nucleus = getNucleus();

        int argc = _args.argc();
        say("Lookup Pool " + poolName + " starting");

        try {

            if (argc < 1)
                throw new IllegalArgumentException("no base dir specified");

            _rootDir = _args.argv(0);

        } catch (Exception e) {
            say("Exception occurred on startup: " + e);
            start();
            kill();
            throw e;
        }

        useInterpreter(true);
        _nucleus.export();
        start();

    }

    public void log(String str) {
        say(str);
    }

    public void elog(String str) {
        esay(str);
    }

    public void plog(String str) {
        esay("PANIC : " + str);
    }

    public void getInfo(PrintWriter pw) {
        pw.println("Root directory    : " + _rootDir);
        pw
            .println("Revision          : [$Id: DirectoryLookUpPool.java,v 1.7 2007-07-26 14:34:12 tigran Exp $]");
    }

    public void messageToForward(CellMessage cellMessage) {
        messageArrived(cellMessage);
    }

    public void messageArrived(CellMessage cellMessage) {

        esay("DirectoryLookUp: message arrived: " + cellMessage);

        Object messageObject = cellMessage.getMessageObject();

        if (!(messageObject instanceof Message)) {
            say("Unexpected message class 1 " + messageObject.getClass());
            return;
        }

        Message poolMessage = (Message) messageObject;

        if (poolMessage instanceof PoolIoFileMessage) {

            ioFile((PoolIoFileMessage) poolMessage, cellMessage);

            return;
        }

    }

    // commands
    public String hh_ls_$_1 = "ls <path>";

    public String ac_ls_$_1(Args args) {
        String path = _rootDir + args.argv(0);
        StringBuffer sb = new StringBuffer();
        File f = new File(path);

        if (!f.exists()) {
            sb.append("Path " + path + " do not exist.");
        } else {

            if (f.isDirectory()) {
                String[] list = f.list();
                if (list != null) {
                    for (int i = 0; i < list.length; i++) {
                        File ff = new File(path, list[i]);
                        try {
                            PnfsFile pnfsFile = new PnfsFile(f, list[i]);
                            sb.append(pnfsFile.getPnfsId().toString());
                        } catch (Exception e) {
                            continue;
                        }
                        if (ff.isDirectory()) {
                            sb.append(":d:");
                        } else {
                            sb.append(":f:");
                        }
                        sb.append(list[i].length()).append(':').append(list[i])
                            .append('\n');
                    }
                }
            } else {
                if (f.isFile()) {
                    sb.append(path).append(" : ").append(f.length());
                }
            }
        }

        return sb.toString();

    }

    // //////////////////////////////////////////////////////////////
    //
    // The io File Part
    //
    //
    private void ioFile(PoolIoFileMessage poolMessage, CellMessage cellMessage) {

        cellMessage.revertDirection();

        try {
            _nucleus.newThread(new DirectoryService(poolMessage, cellMessage),"dir").start();
        } catch (Exception e) {
            esay(e);
        }

    }

    // this is a actual mover
    private class DirectoryService implements Runnable {

        private DCapProtocolInfo dcap;
        private int sessionId;

        private DCapDataOutputStream ostream;
        private DataInputStream istream;
        private Socket dataSocket = null;
        private DCapDataOutputStream cntOut = null;
        private DataInputStream cntIn = null;
        private String _path = null;

        DirectoryService(PoolIoFileMessage poolMessage,
                         CellMessage originalCellMessage) throws IOException {

            dcap = (DCapProtocolInfo) poolMessage.getProtocolInfo();

            PnfsId pnfsId = poolMessage.getPnfsId();
            _path = PnfsFile.pathfinder(new File(_rootDir), pnfsId.toString());
            String rootPrefix = PnfsFile.pathfinder(new File(_rootDir),
                                                    PnfsFile.getMountId(new File(_rootDir)).toString());
            _path = _rootDir + _path.substring(rootPrefix.length());

            sessionId = dcap.getSessionId();

        }

        public void run() {

            boolean done = false;
            int commandSize;
            int commandCode;
            int minSize;
            String dirList = createDirEnt(_path);
            int index = 0;

            try {

                connectToClinet();

                while (!done && !Thread.currentThread().isInterrupted()) {

                    commandSize = cntIn.readInt();

                    if (commandSize < 4)
                        throw new CacheException(44,
                                                 "Protocol Violation (cl<4)");

                    commandCode = cntIn.readInt();
                    switch (commandCode) {
                        // -------------------------------------------------------------
                        //
                        // The IOCMD_CLOSE
                        //
                    case DCapConstants.IOCMD_CLOSE:
                        cntOut.writeACK(DCapConstants.IOCMD_CLOSE);
                        done = true;
                        break;

                        // -------------------------------------------------------------
                        //
                        // The ReadDir
                        //
                    case DCapConstants.IOCMD_READ:
                        //
                        //
                        minSize = 12;
                        if (commandSize < minSize)
                            throw new CacheException(45,
                                                     "Protocol Violation (clREAD<8)");

                        long numberOfEntries = cntIn.readLong();
                        esay("requested " + numberOfEntries + " bytes");

                        cntOut.writeACK(DCapConstants.IOCMD_READ);
                        index += doReadDir(cntOut, ostream, dirList, index,
                                           numberOfEntries);
                        cntOut.writeFIN(DCapConstants.IOCMD_READ);

                        break;
                    default:
                        cntOut.writeACK(1717, 9, "Invalid mover command : "
                                        + commandCode);
                        break;
                    }

                }

            } catch (Exception e) {
                esay(e);
            } finally {
                if (ostream != null) {
                    try {
                        ostream.close();
                    } catch (IOException e) {
                        // ignored
                    }
                }

                if (istream != null) {
                    try {
                        istream.close();
                    } catch (IOException e) {
                        // ignored
                    }
                }

                if (dataSocket != null) {
                    try {
                        dataSocket.close();
                    } catch (IOException e) {
                        // ignored
                    }
                }
            }

        }

        void connectToClinet() throws Exception {

            int port = dcap.getPort();
            String[] hosts = dcap.getHosts();
            String host = null;
            Exception se = null;

            //
            // try to connect to the client, scan the list.
            //
            for (int i = 0; i < hosts.length; i++) {
                try {
                    host = hosts[i];
                    dataSocket = new Socket(host, port);
                } catch (Exception e) {
                    se = e;
                    continue;
                }
                break;
            }

            if (dataSocket == null) {
                esay(se);
                throw se;
            }

            ostream = new DCapDataOutputStream(dataSocket.getOutputStream());
            istream = new DataInputStream(dataSocket.getInputStream());
            esay("Connected to " + host + "(" + port + ")");
            //
            // send the sessionId and our (for now) 0 byte security
            // challenge.
            //

            cntOut = ostream;
            cntIn = istream;

            cntOut.writeInt(sessionId);
            cntOut.writeInt(0);
            cntOut.flush();

        }

        private int doReadDir(DCapDataOutputStream cntOut,
                              DCapDataOutputStream ostream, String dirList, int index,
                              long len) throws Exception {

            long rc = 0;
            byte data[] = null;

            if (index > dirList.length()) {
                throw new ArrayIndexOutOfBoundsException(
                                                         " requested index greater then directory size");
            }

            data = dirList.getBytes();
            rc = len > dirList.length() - index ? dirList.length() - index : len;

            cntOut.writeDATA_HEADER();

            ostream.writeDATA_BLOCK(data, index, (int) rc);

            ostream.writeDATA_TRAILER();

            return (int) rc;
        }

        private String createDirEnt(String path) {

            File f = new File(path);
            StringBuffer sb = new StringBuffer();

            if (!f.exists()) {
                sb.append("Path " + path + " do not exist.");
            } else {

                if (f.isDirectory()) {
                    String[] list = f.list();
                    if (list != null) {
                        for (int i = 0; i < list.length; i++) {
                            File ff = new File(path, list[i]);
                            try {
                                PnfsFile pnfsFile = new PnfsFile(f, list[i]);
                                sb.append(pnfsFile.getPnfsId().toString());
                            } catch (Exception e) {
                                continue;
                            }
                            if (ff.isDirectory()) {
                                sb.append(":d:");
                            } else {
                                sb.append(":f:");
                            }
                            sb.append(list[i].length()).append(':').append(
                                                                           list[i]).append('\n');
                        }
                    }
                } else {
                    if (f.isFile()) {
                        sb.append(path).append(" : ").append(f.length());
                    }
                }
            }

            esay(sb.toString());
            return sb.toString();

        }

    } // end of private class

} // end of MultiProtocolPool
