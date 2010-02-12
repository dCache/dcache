// $Id: DirectoryLookUpPool.java,v 1.7 2007-07-26 14:34:12 tigran Exp $

package diskCacheV111.pools;

import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.Set;
import java.util.EnumSet;
import java.util.concurrent.ExecutionException;

import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.DelayedReply;
import dmg.cells.nucleus.NoRouteToCellException;
import dmg.util.Args;

import diskCacheV111.movers.DCapConstants;
import diskCacheV111.movers.DCapDataOutputStream;
import diskCacheV111.util.FileNotFoundCacheException;
import diskCacheV111.util.NotDirCacheException;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsId;
import diskCacheV111.util.PnfsHandler;
import diskCacheV111.vehicles.DCapProtocolInfo;
import diskCacheV111.vehicles.PoolIoFileMessage;

import org.dcache.cells.AbstractCell;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.dcache.namespace.FileAttribute;
import org.dcache.util.list.DirectoryListSource;
import org.dcache.util.list.ListDirectoryHandler;
import org.dcache.util.list.DirectoryEntry;
import org.dcache.util.list.DirectoryListPrinter;
import org.dcache.vehicles.FileAttributes;

import static org.dcache.namespace.FileAttribute.*;
import static org.dcache.namespace.FileType.*;

/**
 * Provides directory listing services for DCAP.
 */
public class DirectoryLookUpPool extends AbstractCell
{
    private final static Logger _log =
        LoggerFactory.getLogger(DirectoryLookUpPool.class);

    private final static CellPath PNFS_MANAGER = new CellPath("PnfsManager");

    private final String _poolName;
    private final Args _args;
    private PnfsHandler _pnfs;
    private DirectoryListSource _list;

    public DirectoryLookUpPool(String poolName, String args)
        throws InterruptedException, ExecutionException
    {
        super(poolName, args);

        _poolName = poolName;
        _args = getArgs();

        doInit();
    }

    protected void init()
        throws IllegalArgumentException
    {
        _log.info("Lookup Pool " + _poolName + " starting");

        _pnfs = new PnfsHandler(this, PNFS_MANAGER);
        _list = new ListDirectoryHandler(_pnfs);

        addMessageListener(_list);

        useInterpreter(true);
    }

    public void getInfo(PrintWriter pw)
    {
        pw.println("Revision          : [$Id: DirectoryLookUpPool.java,v 1.7 2007-07-26 14:34:12 tigran Exp $]");
    }

    public void messageToForward(CellMessage cellMessage)
    {
        messageArrived(cellMessage);
    }

    /**
     * List a directory.
     */
    private String list(File path)
        throws InterruptedException, CacheException
    {
        StringBuilder sb = new StringBuilder();
        try {
            _list.printDirectory(null, new DirectoryPrinter(sb),
                                 path, null, null);
        } catch (FileNotFoundCacheException e) {
            sb.append("Path " + path + " does not exist.");
        } catch (NotDirCacheException e) {
            _list.printFile(null, new FilePrinter(sb, path.getParentFile()),
                            path);
        }
        return sb.toString();
    }

    /**
     * Reply format for directory listings.
     */
    class DirectoryPrinter implements DirectoryListPrinter
    {
        private final StringBuilder _out;

        public DirectoryPrinter(StringBuilder out)
        {
            _out = out;
        }

        public Set<FileAttribute> getRequiredAttributes()
        {
            return EnumSet.of(PNFSID, TYPE, SIZE);
        }

        public void print(FileAttributes dirAttr, DirectoryEntry entry)
        {
            FileAttributes attr = entry.getFileAttributes();
            _out.append(attr.getPnfsId());
            switch (attr.getFileType()) {
            case DIR:
                _out.append(":d:");
                break;
            case REGULAR:
            case LINK:
            case SPECIAL:
                _out.append(":f:");
                break;
            }
            _out.append(attr.getSize()).append(':').append(entry.getName());
            _out.append('\n');
        }
    }

    /**
     * Reply format for single file listings.
     */
    class FilePrinter implements DirectoryListPrinter
    {
        private final StringBuilder _out;
        private final File _dir;

        public FilePrinter(StringBuilder out, File dir)
        {
            _out = out;
            _dir = dir;
        }

        public Set<FileAttribute> getRequiredAttributes()
        {
            return EnumSet.of(TYPE, SIZE);
        }

        public void print(FileAttributes dirAttr, DirectoryEntry entry)
        {
            FileAttributes attr = entry.getFileAttributes();
            if (attr.getFileType() == REGULAR) {
                _out.append(new File(_dir, entry.getName()));
                _out.append(" : ").append(attr.getSize());
            }
        }
    }

    /**
     * List task that can serve as a DelayedReply.
     */
    class ListThread extends DelayedReply implements Runnable
    {
        private final File _path;

        public ListThread(File path)
        {
            _path = path;
        }

        public void run()
        {
            try {
                try {
                    send(list(_path));
                } catch (CacheException e) {
                    send(e);
                }
            } catch (InterruptedException e) {
                // end of thread
            } catch (NoRouteToCellException e) {
                _log.warn("Failed to send list reply: " + e.getMessage());
            }
        }
    }

    // commands
    public final static String hh_ls_$_1 = "ls <path>";
    public DelayedReply ac_ls_$_1(Args args)
        throws CacheException, InterruptedException
    {
        File path = new File(args.argv(0));
        ListThread thread = new ListThread(path);
        new Thread(thread, "list[" + path + "]").start();
        return thread;
    }

    // //////////////////////////////////////////////////////////////
    //
    // The io File Part
    //
    //
    public PoolIoFileMessage messageArrived(PoolIoFileMessage message)
        throws IOException
    {
        DCapProtocolInfo dcap = (DCapProtocolInfo) message.getProtocolInfo();
        PnfsId pnfsId = message.getPnfsId();
        DirectoryService service = new DirectoryService(dcap, pnfsId);
        new Thread(service, "list[" + pnfsId + "]").start();
        message.setSucceeded();
        return message;
    }

    /**
     * A task which handles directory listing for a particular
     * client. We have an instance per client request.
     */
    private class DirectoryService implements Runnable
    {
        private DCapProtocolInfo dcap;
        private int sessionId;

        private DCapDataOutputStream ostream;
        private DataInputStream istream;
        private Socket dataSocket = null;
        private DCapDataOutputStream cntOut = null;
        private DataInputStream cntIn = null;
        private PnfsId pnfsId;

        DirectoryService(DCapProtocolInfo dcap, PnfsId pnfsId)
            throws IOException
        {
            this.dcap = dcap;
            this.pnfsId = pnfsId;
            this.sessionId = dcap.getSessionId();
        }

        public void run()
        {
            boolean done = false;
            int commandSize;
            int commandCode;
            int minSize;
            int index = 0;

            try {
                String path = _pnfs.getPathByPnfsId(pnfsId);
                String dirList = list(new File(path));

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
                        _log.debug("requested " + numberOfEntries + " bytes");

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

            } catch (CacheException e) {
                _log.error(e.toString());
            } catch (IOException e) {
                _log.warn(e.toString());
            } catch (InterruptedException e) {
                // end of thread
            } finally {
                if (ostream != null) {
                    try {
                        ostream.close();
                    } catch (IOException e) {
                        _log.warn(e.toString());
                    }
                }

                if (istream != null) {
                    try {
                        istream.close();
                    } catch (IOException e) {
                        _log.warn(e.toString());
                    }
                }

                if (dataSocket != null) {
                    try {
                        dataSocket.close();
                    } catch (IOException e) {
                        _log.warn(e.toString());
                    }
                }
            }
        }

        void connectToClinet()
            throws IOException
        {
            int port = dcap.getPort();
            String[] hosts = dcap.getHosts();
            String host = null;
            IOException se = null;

            //
            // try to connect to the client, scan the list.
            //
            for (int i = 0; i < hosts.length; i++) {
                try {
                    host = hosts[i];
                    dataSocket = new Socket(host, port);
                } catch (IOException e) {
                    se = e;
                    continue;
                }
                break;
            }

            if (dataSocket == null) {
                _log.error(se.toString());
                throw se;
            }

            ostream = new DCapDataOutputStream(dataSocket.getOutputStream());
            istream = new DataInputStream(dataSocket.getInputStream());
            _log.error("Connected to " + host + "(" + port + ")");
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
                              long len)
            throws IOException
        {
            long rc = 0;
            byte data[] = null;

            if (index > dirList.length()) {
                throw new ArrayIndexOutOfBoundsException("requested index greater then directory size");
            }

            data = dirList.getBytes();
            rc = len > dirList.length() - index ? dirList.length() - index : len;

            cntOut.writeDATA_HEADER();

            ostream.writeDATA_BLOCK(data, index, (int) rc);

            ostream.writeDATA_TRAILER();

            return (int) rc;
        }
    } // end of private class
} // end of MultiProtocolPool
