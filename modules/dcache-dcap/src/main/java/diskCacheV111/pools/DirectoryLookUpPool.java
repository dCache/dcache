// $Id: DirectoryLookUpPool.java,v 1.7 2007-07-26 14:34:12 tigran Exp $

package diskCacheV111.pools;

import static org.dcache.namespace.FileAttribute.PNFSID;
import static org.dcache.namespace.FileAttribute.SIZE;
import static org.dcache.namespace.FileAttribute.TYPE;
import static org.dcache.namespace.FileType.REGULAR;

import com.google.common.collect.Range;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.FileNotFoundCacheException;
import diskCacheV111.util.FsPath;
import diskCacheV111.util.NotDirCacheException;
import diskCacheV111.util.PnfsHandler;
import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.DCapProtocolInfo;
import diskCacheV111.vehicles.DirRequestMessage;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.DelayedReply;
import dmg.cells.nucleus.NoRouteToCellException;
import dmg.util.command.Argument;
import dmg.util.command.Command;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.EnumSet;
import java.util.Set;
import java.util.concurrent.Callable;
import javax.security.auth.Subject;
import org.dcache.auth.Subjects;
import org.dcache.auth.attributes.Restriction;
import org.dcache.auth.attributes.Restrictions;
import org.dcache.cells.AbstractCell;
import org.dcache.namespace.FileAttribute;
import org.dcache.pool.movers.DCapConstants;
import org.dcache.pool.movers.DCapDataOutputStream;
import org.dcache.util.Args;
import org.dcache.util.Option;
import org.dcache.util.list.DirectoryEntry;
import org.dcache.util.list.DirectoryListPrinter;
import org.dcache.util.list.DirectoryListSource;
import org.dcache.util.list.ListDirectoryHandler;
import org.dcache.vehicles.FileAttributes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides directory listing services for DCAP.
 */
public class DirectoryLookUpPool extends AbstractCell {

    private static final Logger _log =
          LoggerFactory.getLogger(DirectoryLookUpPool.class);

    private final String _poolName;
    private final Args _args;
    private PnfsHandler _pnfs;
    private DirectoryListSource _list;

    @Option(name = "pnfsManager",
          description = "Cell address of the PNFS manager",
          defaultValue = "PnfsManager")
    protected CellPath _pnfsManager;

    public DirectoryLookUpPool(String poolName, String args) {
        super(poolName, args);
        _poolName = poolName;
        _args = getArgs();
    }

    @Override
    protected void starting() throws Exception {
        _log.info("Lookup Pool {} starting", _poolName);
        super.starting();

        _pnfs = new PnfsHandler(this, _pnfsManager);
        ListDirectoryHandler listHandler = new ListDirectoryHandler(_pnfs);
        addMessageListener(listHandler);
        _list = listHandler;

        useInterpreter(true);
    }

    @Override
    public void getInfo(PrintWriter pw) {
        pw.println(
              "Revision          : [$Id: DirectoryLookUpPool.java,v 1.7 2007-07-26 14:34:12 tigran Exp $]");
    }

    @Override
    public void messageToForward(CellMessage cellMessage) {
        messageArrived(cellMessage);
    }

    /**
     * List a directory.
     */
    private String list(FsPath path, Subject subject, Restriction restriction)
          throws InterruptedException, CacheException {
        StringBuilder sb = new StringBuilder();
        try {
            _list.printDirectory(subject, restriction, new DirectoryPrinter(sb),
                  path, null, Range.<Integer>all());
        } catch (FileNotFoundCacheException e) {
            sb.append("Path ").append(path).append(" does not exist.");
        } catch (NotDirCacheException e) {
            _list.printFile(subject, restriction, new FilePrinter(sb), path);
        }
        return sb.toString();
    }

    /**
     * Reply format for directory listings.
     */
    class DirectoryPrinter implements DirectoryListPrinter {

        private final StringBuilder _out;

        public DirectoryPrinter(StringBuilder out) {
            _out = out;
        }

        @Override
        public Set<FileAttribute> getRequiredAttributes() {
            return EnumSet.of(PNFSID, TYPE, SIZE);
        }

        @Override
        public void print(FsPath dir, FileAttributes dirAttr, DirectoryEntry entry) {
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
            _out.append(attr.getSizeIfPresent().orElse(0L)).append(':').append(entry.getName());
            _out.append('\n');
        }
    }

    /**
     * Reply format for single file listings.
     */
    class FilePrinter implements DirectoryListPrinter {

        private final StringBuilder _out;

        public FilePrinter(StringBuilder out) {
            _out = out;
        }

        @Override
        public Set<FileAttribute> getRequiredAttributes() {
            return EnumSet.of(TYPE, SIZE);
        }

        @Override
        public void print(FsPath dir, FileAttributes dirAttr, DirectoryEntry entry) {
            FileAttributes attr = entry.getFileAttributes();
            if (attr.getFileType() == REGULAR) {
                _out.append(dir.child(entry.getName()));
                _out.append(" : ").append(attr.getSizeIfPresent().orElse(0L));
            }
        }
    }

    /**
     * List task that can serve as a DelayedReply.
     */
    class ListThread extends DelayedReply implements Runnable {

        private final FsPath _path;

        public ListThread(FsPath path) {
            _path = path;
        }

        @Override
        public void run() {
            try {
                try {
                    reply(list(_path, Subjects.ROOT, Restrictions.none()));
                } catch (CacheException e) {
                    reply(e);
                }
            } catch (InterruptedException e) {
                // end of thread
            }
        }
    }

    // commands
    @Command(name = "ls",
          hint = "list the contents of a directory",
          description = "Get the list of all the contents in the specified directory path. " +
                "The directory listing is displayed in this format:\n" +
                "\t\t<Pnfs-ID>:<TYPE>:<SIZE>:<NAME>\n" +
                "where" +
                "\n\tPnfs-ID: is a unique identifier of the file;" +
                "\n\tTYPE: file type has two denotations, 'd' denotes directory and 'f' " +
                "for other file type (like regular, link or special);" +
                "\n\tSIZE: the size of the file in bytes; and" +
                "\n\tNAME: the name of the file.")
    public class LsCommand implements Callable<DelayedReply> {

        @Argument(metaVar = "directory",
              usage = "An absolute directory path.")
        FsPath path;

        @Override
        public DelayedReply call()
              throws InterruptedException, CacheException {
            ListThread thread = new ListThread(path);
            new Thread(thread, "list[" + path + "]").start();
            return thread;
        }
    }

    // //////////////////////////////////////////////////////////////
    //
    // The io File Part
    //
    //
    public DirRequestMessage messageArrived(DirRequestMessage message) {
        DCapProtocolInfo dcap = (DCapProtocolInfo) message.getProtocolInfo();
        PnfsId pnfsId = message.getPnfsId();
        Restriction restriction = message.getRestriction();
        Subject subject = message.getSubject();
        DirectoryService service = new DirectoryService(subject, restriction, dcap, pnfsId);
        new Thread(service, "list[" + pnfsId + "]").start();
        message.setSucceeded();
        return message;
    }

    public void messageArrived(NoRouteToCellException e) {
        _log.warn(e.getMessage());
    }

    /**
     * A task which handles directory listing for a particular client. We have an instance per
     * client request.
     */
    private class DirectoryService implements Runnable {

        private final DCapProtocolInfo dcap;
        private final PnfsId pnfsId;
        private final int sessionId;
        private final Subject subject;
        private final Restriction restriction;

        private DCapDataOutputStream ostream;
        private DataInputStream istream;
        private Socket dataSocket;
        private DCapDataOutputStream cntOut;
        private DataInputStream cntIn;

        DirectoryService(Subject subject, Restriction restriction, DCapProtocolInfo dcap,
              PnfsId pnfsId) {
            this.dcap = dcap;
            this.pnfsId = pnfsId;
            this.sessionId = dcap.getSessionId();
            this.subject = subject;
            this.restriction = restriction;
        }

        @Override
        public void run() {
            boolean done = false;
            int commandSize;
            int commandCode;
            int minSize;
            int index = 0;

            try {
                FsPath path = _pnfs.getPathByPnfsId(pnfsId);
                String dirList = list(path, subject, restriction);

                connectToClinet();

                while (!done && !Thread.currentThread().isInterrupted()) {

                    commandSize = cntIn.readInt();

                    if (commandSize < 4) {
                        throw new CacheException(44,
                              "Protocol Violation (cl<4)");
                    }

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
                            if (commandSize < minSize) {
                                throw new CacheException(45,
                                      "Protocol Violation (clREAD<8)");
                            }

                            long numberOfEntries = cntIn.readLong();
                            _log.debug("requested {} bytes", numberOfEntries);

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
              throws IOException {

            dataSocket = new Socket(dcap.getSocketAddress().getAddress(),
                  dcap.getSocketAddress().getPort());

            ostream = new DCapDataOutputStream(dataSocket.getOutputStream());
            istream = new DataInputStream(dataSocket.getInputStream());
            _log.info("Connected to {}", dcap.getSocketAddress());
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
              throws IOException {
            long rc;
            byte data[];

            if (index > dirList.length()) {
                throw new ArrayIndexOutOfBoundsException(
                      "requested index greater then directory size");
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
