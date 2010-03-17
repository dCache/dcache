// $Id: AbstractFtpDoorV1.java,v 1.137 2007-10-29 13:29:24 behrmann Exp $

/*
COPYRIGHT STATUS:
  Dec 1st 2001, Fermi National Accelerator Laboratory (FNAL) documents and
  software are sponsored by the U.S. Department of Energy under Contract No.
  DE-AC02-76CH03000. Therefore, the U.S. Government retains a  world-wide
  non-exclusive, royalty-free license to publish or reproduce these documents
  and software for U.S. Government purposes.  All documents and software
  available from this server are protected under the U.S. and Foreign
  Copyright Laws, and FNAL reserves all rights.


 Distribution of the software available from this server is free of
 charge subject to the user following the terms of the Fermitools
 Software Legal Information.

 Redistribution and/or modification of the software shall be accompanied
 by the Fermitools Software Legal Information  (including the copyright
 notice).

 The user is asked to feed back problems, benefits, and/or suggestions
 about the software to the Fermilab Software Providers.


 Neither the name of Fermilab, the  URA, nor the names of the contributors
 may be used to endorse or promote products derived from this software
 without specific prior written permission.



  DISCLAIMER OF LIABILITY (BSD):

  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
  "AS IS" AND ANY EXPRESS OR IMPLIED  WARRANTIES, INCLUDING, BUT NOT
  LIMITED TO, THE IMPLIED  WARRANTIES OF MERCHANTABILITY AND FITNESS
  FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL FERMILAB,
  OR THE URA, OR THE U.S. DEPARTMENT of ENERGY, OR CONTRIBUTORS BE LIABLE
  FOR  ANY  DIRECT, INDIRECT,  INCIDENTAL, SPECIAL, EXEMPLARY, OR
  CONSEQUENTIAL DAMAGES  (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT
  OF SUBSTITUTE  GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR
  BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY  OF
  LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
  NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT  OF THE USE OF THIS
  SOFTWARE, EVEN IF ADVISED OF THE  POSSIBILITY OF SUCH DAMAGE.


  Liabilities of the Government:

  This software is provided by URA, independent from its Prime Contract
  with the U.S. Department of Energy. URA is acting independently from
  the Government and in its own private capacity and is not acting on
  behalf of the U.S. Government, nor as its contractor nor its agent.
  Correspondingly, it is understood and agreed that the U.S. Government
  has no connection to this software and in no manner whatsoever shall
  be liable for nor assume any responsibility or obligation for any claim,
  cost, or damages arising out of or resulting from the use of the software
  available from this server.


  Export Control:

  All documents and software available from this server are subject to U.S.
  export control laws.  Anyone downloading information from this server is
  obligated to secure any necessary Government licenses before exporting
  documents or software obtained from this server.
 */

package diskCacheV111.doors;

import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ExecutionException;
import java.util.Date;
import java.util.Queue;
import java.util.List;
import java.util.LinkedList;
import java.util.TimerTask;
import java.util.Map;
import java.util.HashMap;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Set;
import java.util.HashSet;
import java.util.EnumSet;
import java.util.Iterator;
import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.io.PrintWriter;
import java.io.BufferedReader;
import java.io.FilenameFilter;
import java.io.OutputStream;
import java.io.BufferedOutputStream;
import java.io.OutputStreamWriter;
import java.io.InputStreamReader;
import java.io.EOFException;
import java.net.Socket;
import java.net.InetSocketAddress;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Constructor;
import java.security.NoSuchAlgorithmException;
import java.security.Principal;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellMessageAnswerable;
import dmg.cells.nucleus.CellAdapter;
import dmg.cells.nucleus.NoRouteToCellException;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.CellVersion;
import dmg.util.CommandExitException;
import dmg.util.Args;
import dmg.util.StreamEngine;

import diskCacheV111.vehicles.DoorRequestInfoMessage;
import diskCacheV111.vehicles.IoDoorInfo;
import diskCacheV111.vehicles.IoDoorEntry;
import diskCacheV111.vehicles.GFtpTransferStartedMessage;
import diskCacheV111.vehicles.DoorTransferFinishedMessage;
import diskCacheV111.vehicles.Message;
import diskCacheV111.vehicles.StorageInfo;
import diskCacheV111.vehicles.ProtocolInfo;
import diskCacheV111.vehicles.GFtpProtocolInfo;
import diskCacheV111.vehicles.PnfsSetLengthMessage;
import diskCacheV111.vehicles.PoolMgrSelectPoolMsg;
import diskCacheV111.vehicles.PoolMgrSelectWritePoolMsg;
import diskCacheV111.vehicles.PoolMgrSelectReadPoolMsg;
import diskCacheV111.vehicles.PoolMoverKillMessage;
import diskCacheV111.vehicles.PoolIoFileMessage;
import diskCacheV111.vehicles.PoolDeliverFileMessage;
import diskCacheV111.vehicles.PoolAcceptFileMessage;
import diskCacheV111.vehicles.IoJobInfo;
import diskCacheV111.movers.GFtpPerfMarkersBlock;
import diskCacheV111.movers.GFtpPerfMarker;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.PermissionDeniedCacheException;
import diskCacheV111.util.CheckStagePermission;
import diskCacheV111.util.FileExistsCacheException;
import diskCacheV111.util.FileNotOnlineCacheException;
import diskCacheV111.util.FileNotFoundCacheException;
import diskCacheV111.util.NotDirCacheException;
import diskCacheV111.util.NotFileCacheException;
import diskCacheV111.util.PnfsId;
import diskCacheV111.util.FileMetaData;
import diskCacheV111.util.FsPath;
import diskCacheV111.util.VOInfo;
import diskCacheV111.util.ProxyAdapter;
import diskCacheV111.util.SocketAdapter;
import diskCacheV111.util.ActiveAdapter;
import diskCacheV111.util.ChecksumPersistence;
import diskCacheV111.util.ChecksumFactory;
import diskCacheV111.util.Checksum;
import diskCacheV111.util.PnfsHandler;
import diskCacheV111.services.acl.PermissionHandler;
import diskCacheV111.services.acl.DelegatingPermissionHandler;
import diskCacheV111.services.acl.GrantAllPermissionHandler;

import org.dcache.namespace.FileType;
import org.dcache.auth.FQANPrincipal;
import org.dcache.auth.Subjects;
import org.dcache.auth.UserAuthBase;
import org.dcache.auth.AuthorizationRecord;
import org.dcache.cells.AbstractCell;
import org.dcache.cells.Option;
import org.dcache.acl.ACLException;
import org.dcache.acl.Origin;
import org.dcache.acl.enums.AccessType;
import org.dcache.acl.enums.AuthType;
import org.dcache.acl.enums.FileAttribute;
import org.dcache.acl.enums.InetAddressType;
import org.dcache.acl.enums.AccessMask;
import org.dcache.vehicles.FileAttributes;
import org.dcache.vehicles.PnfsListDirectoryMessage;
import org.dcache.vehicles.PnfsGetFileAttributes;
import org.dcache.util.list.DirectoryListSource;
import org.dcache.util.list.DirectoryListPrinter;
import org.dcache.util.list.ListDirectoryHandler;
import org.dcache.util.list.DirectoryStream;
import org.dcache.util.list.DirectoryEntry;
import org.dcache.util.Glob;
import org.dcache.auth.Subjects;
import org.globus.gsi.jaas.GlobusPrincipal;

import dmg.cells.nucleus.CDC;

import org.apache.log4j.NDC;

import com.sun.security.auth.UnixNumericGroupPrincipal;
import com.sun.security.auth.UnixNumericUserPrincipal;
import javax.security.auth.Subject;

import diskCacheV111.poolManager.RequestContainerV5;

import static org.dcache.namespace.FileAttribute.*;

/**
 * Exception indicating an error during processing of an FTP command.
 */
class FTPCommandException extends Exception
{
    /** FTP reply code. */
    protected int    _code;

    /** Human readable part of FTP reply. */
    protected String _reply;

    /**
     * Constructs a command exception with the given ftp reply code and
     * message. The message will be used for both the public FTP reply
     * string and for the exception message.
     */
    public FTPCommandException(int code, String reply)
    {
        this(code, reply, reply);
    }

    /**
     * Constructs a command exception with the given ftp reply code,
     * public and internal message.
     */
    public FTPCommandException(int code, String reply, String msg)
    {
        super(msg);
        _code = code;
        _reply = reply;
    }

    /** Returns FTP reply code. */
    public int getCode()
    {
        return _code;
    }

    /** Returns the public FTP reply string. */
    public String getReply()
    {
        return _reply;
    }
}

/**
 * Exception indicating and error condition during a sendAndWait
 * operation.
 *
 * TODO: This should be refined to better convey the actual error.
 */
class SendAndWaitException extends Exception
{
    public SendAndWaitException(String msg)
    {
        super(msg);
    }

    public SendAndWaitException(String msg, Throwable cause)
    {
        super(msg, cause);
    }
}

/**
 * @author Charles G Waldman, Patrick, rich wellner, igor mandrichenko
 * @version 0.0, 15 Sep 1999
 */
public abstract class AbstractFtpDoorV1
    extends AbstractCell implements Runnable
{
    /**
     * Enumeration type for representing the connection mode.
     *
     * For PASSIVE transfers the client establishes the data
     * connection.
     *
     * For ACTIVE transfers dCache establishes the data connection.
     *
     * Depending on the values of _allowPassivePool and _allowRelay,
     * the data connection with the client will be established either
     * to an adapter (proxy) at the FTP door, or to the pool directly.
     */
    protected enum Mode
    {
        PASSIVE, ACTIVE;
    }


    /**
     * Enumeration type for representing RFC 3659 facts.
     */
    protected enum Fact
    {
        SIZE, MODIFY, TYPE, UNIQUE, PERM;

        public static Fact find(String s)
        {
            for (Fact fact: values()) {
                if (s.equalsIgnoreCase(fact.name())) {
                    return fact;
                }
            }
            return null;
        }
    }

    /**
     * Used for generating session IDs unique to this domain.
     */
    private static long   __counter = 10000;

    /**
     * Feature strings returned when the client sends the FEAT
     * command.
     */
    private static final String[] FEATURES = {
        "EOF", "PARALLEL", "MODE-E-PERF", "SIZE", "SBUF",
        "ERET", "ESTO", "GETPUT", "MDTM",
        "CKSUM " + buildChecksumList(),  "MODEX"
        /*
         * do not publish DCAU as supported feature. This will force
         * some clients to always encrypt data channel
         */
        // "DCAU"
    };

    /**
     * Time stamp format as defined in RFC 3659.
     */
    private final DateFormat TIMESTAMP_FORMAT =
        new SimpleDateFormat("yyyyMMddHHmmss");

    private static final String buildChecksumList(){
        String result = "";
        int mod = 0;
        for (String type : ChecksumFactory.getTypes()) {
            result += type + ",";
            mod = 1;
        }

        return result.substring(0, result.length() - mod);
    }

    /**
     * This reports an internal error as a fatal error to the log file and
     * then throws a RuntimeException to stop the program.
     * @param methodName The name of the method where the bug was detected.
     * @param message A message indicating the nature of the bug.
     * @param exc An exception that may have been caught that indicated the bug.
     *          This can be null if no exception was involved.
     * @throws RuntimeException This is always thrown to terminate the process.
     */
    protected void reportBug(String methodName,
                             String message,
                             Throwable exc)
        throws RuntimeException
    {
        String text = "BUG: " + getNucleus().getCellName() + "@" +
                       getNucleus().getCellDomainName() + ":" +
                       getClass().getName() + "::" + methodName +
                       ": " + message;
        fatal(text);
        if (exc != null) {
            fatal(exc);
        }
        throw new RuntimeException(text, exc);
    }


    /**
     * FTP door instances are created by the LoginManager. This is the
     * stream engine passed to us from the LoginManager upon
     * instantiation.
     */
    protected StreamEngine _engine;

    /**
     * Writer for control channel.
     */
    protected PrintWriter _out;

    /**
     * Stub object for talking to the PNFS manager.
     */
    protected PnfsHandler _pnfs;

    /**
     * Used for directory listing.
     */
    protected DirectoryListSource _listSource;

    /**
     * User's Origin
     */
    protected Origin _origin;

    /**
     * Permission Handler
     */
    protected PermissionHandler _permissionHandler;

    @Option(
        name = "permission-handler",
        description = "Permission handler class names",
        defaultValue = ""
    )
    protected String _permissionHandlerClasses;

    @Option(
        name = "poolManager",
        description = "Well known name of the pool manager",
        defaultValue = "PoolManager"
    )
    protected String _poolManager;

    @Option(
        name = "pnfsManager",
        description = "Well known name of the PNFS manager",
        defaultValue = "PnfsManager"
    )
    protected String _pnfsManager;

    @Option(
        name = "clientDataPortRange"
    )
    protected String _portRange;

    @Option(
        name = "poolProxy"
    )
    protected String _poolProxy;

    /**
     * Name or IP address of the interface on which we listen for
     * connections from the pool in case an adapter is used.
     */
    @Option(
        name = "ftp-adapter-internal-interface",
        description = "Interface to bind to"
    )
    protected String _local_host;

    @Option(
        name = "read-only",
        description = "Whether to mark the FTP door read only",
        defaultValue = "false"
    )
    protected boolean _readOnly;

    @Option(
        name = "maxRetries",
        defaultValue = "3"
    )
    protected int _maxRetries;

    @Option(
        name = "poolManagerTimeout",
        defaultValue = "1500",
        unit = "seconds"
    )
    protected int _poolManagerTimeout;

    @Option(
        name = "pnfsTimeout",
        defaultValue = "60",
        unit = "seconds"
    )
    protected int _pnfsTimeout;

    @Option(
        name = "poolTimeout",
        defaultValue = "300",
        unit = "seconds"
    )
    protected int _poolTimeout;

    @Option(
        name = "retryWait",
        defaultValue = "30",
        unit = "seconds"
    )
    protected int _retryWait;

    /**
     * Size of the largest block used in the socket adapter in mode
     * E. Blocks larger than this are divided into smaller blocks.
     */
    @Option(
        name = "maxBlockSize",
        defaultValue = "131072",
        unit = "bytes"
    )
    protected int _maxBlockSize;

    @Option(
        name = "deleteOnConnectionClosed",
        description = "Whether to remove files on incomplete transfers",
        defaultValue = "false"
    )
    protected boolean _removeFileOnIncompleteTransfer;

    /**
     * True if passive pools are allowed, i.e., the client connects
     * directly to the pool, bypassing the proxy at the door. Set to
     * false if any of the pools are not at least at version 1.8.
     */
    @Option(
        name = "allowPassivePool",
        description = "Whether to allow pools to be passive",
        defaultValue = "false"
    )
    protected boolean _allowPassivePool;

    /**
     * True if active adapter is allowed, i.e., the client connects to
     * the new proxy adapter at the door, when the pools are on the
     * private network, for example.  Has to be set via arguments.
     */
    @Option(
        name = "allow-relay",
        description = "Whether to allow the use of the relay adapter",
        defaultValue = "false"
    )
    protected boolean _allowRelay;

    /**
     * If use_gplazmaAuthzCell is true, the door will first contact
     * the GPLAZMA cell for authentification.
     */
    @Option(
        name = "use-gplazma-authorization-cell",
        description = "Whether to use gPlazma cell for authorization",
        defaultValue = "false"
    )
    protected boolean _use_gplazmaAuthzCell;

    @Option(
        name = "delegate-to-gplazma",
        description = "Whether to delegate credentials to gPlazma",
        defaultValue = "false"
    )
    protected boolean _delegate_to_gplazma;

    /**
     * If use_gplazmaAuthzModule is true, then the door will consult
     * the authorization module and use its policy configuration, else
     * door keeps using kpwd.
     */
    @Option(
        name = "use-gplazma-authorization-module",
        description = "Whether to use the gPlazma module for authorization",
        defaultValue = "false"
    )
    protected boolean _use_gplazmaAuthzModule;

    @Option(
        name = "gplazma-authorization-module-policy",
        description = "Path to gPlazma policy file"
    )
    protected String _gplazmaPolicyFilePath;

     /**
     * File (StageConfiguration.conf) containing DNs and FQANs whose owner are allowed to STAGE files
     * (i.e. allowed to copy file from dCache in case file is stored on tape but not on disk).
     * /opt/d-cache/config/StageConfiguration.conf
     * By default, such file does not exist, so that tape protection feature is not in use.
     */
    @Option(
        name = "stageConfigurationFilePath",
        description = "File containing DNs and FQANs for which STAGING is allowed",
        defaultValue = ""
    )
    protected String _stageConfigurationFilePath;

    /**
     * transferTimeout (in seconds)
     *
     * Is used for waiting for the end of transfer after the pool
     * already notified us that the file transfer is finished. This is
     * needed because we are using adapters.  If timeout is 0, there is
     * no timeout.
     */
    @Option(
        name = "transfer-timeout",
        description = "Transfer timeout",
        defaultValue = "0",
        unit = "seconds"
    )
    protected int _transferTimeout;

    @Option(
        name = "tlog",
        description = "Path to FTP transaction log"
    )
    protected String _tLogRoot;

    /**
     * wlcg demands that support for overwrite in srm and gridftp
     * be off by default.
     */
    @Option(
        name = "overwrite",
        defaultValue = "false"
    )
    protected boolean _overwrite;

    @Option(
        name = "io-queue"
    )
    private String _ioQueueName;

    @Option(
        name = "maxStreamsPerClient",
        description = "Maximum allowed streams per client in mode E",
        defaultValue = "-1",                   // -1 = unlimited
        unit = "streams"
    )
    protected int _maxStreamsPerClient;

    @Option(
        name = "defaultStreamsPerClient",
        description = "Default number of streams per client in mode E",
        defaultValue = "1",
        unit = "streams"
    )
    protected int _defaultStreamsPerClient;

    protected final int _sleepAfterMoverKill = 15; // seconds

    protected final int _spaceManagerTimeout = 5 * 60;

    /**
     * Lowest allowable port to use for the data channel when using an
     * adapter.
     */
    protected int _lowDataListenPort;

    /**
     * Highest allowable port to use for the data channel when using
     * an adapter.
     */
    protected int _highDataListenPort;

    private final CommandQueue        _commandQueue =
        new CommandQueue();
    private final CountDownLatch      _shutdownGate =
        new CountDownLatch(1);
    private final Map<String,Method>  _methodDict =
        new HashMap();

    /**
     * Shared executor for processing FTP commands.
     *
     * FIXME: This will be created within the thread group creating
     * the first FTP door. This will usually be the login manager and
     * works fine, but it isn't clean.
     */
    private final static ExecutorService _executor =
        Executors.newCachedThreadPool();

    protected String         _dnUser;
    private   Thread         _workerThread;
    protected int            _commandCounter = 0;
    protected String         _lastCommand    = "<init>";

    //XXX this should get set when we authenicate the user
    protected String         _user       = "nobody";

    protected String         _client_data_host;
    protected int            _client_data_port = 20;
    protected volatile Socket _dataSocket;

    // added for the support or ERET with partial retrieve mode
    protected long prm_offset = -1;
    protected long prm_size = -1;


    protected long   _skipBytes  = 0;

    protected boolean _needUser = true;
    protected boolean _needPass = true;

    protected boolean _confirmEOFs = false;

    protected UserAuthBase _pwdRecord;
    AuthorizationRecord authRecord=null;
    protected UserAuthBase _originalPwdRecord;
    protected String _pathRoot;
    protected String _curDirV;
    protected String _xferMode = "S";
    protected final CellPath _billingCellPath = new CellPath("billing");

    /** Tape Protection */
    protected CheckStagePermission _checkStagePermission;

    /**
     *   NEW
     */

    /** Generalized kpwd file path used by all flavors. */
    protected String _kpwdFilePath;

    /** Can be "mic", "conf", "enc", "clear". */
    protected String _gReplyType = "clear";

    protected Mode _mode = Mode.ACTIVE;
    protected ProxyAdapter _adapter;
    protected FTPTransactionLog _tLog;

    //These are the number of parallel streams to have
    //when doing mode e transfers
    protected int _parallelStart;
    protected int _parallelMin;
    protected int _parallelMax;
    protected int _bufSize = 0;

    protected String ftpDoorName = "FTP";
    protected Checksum _checkSum;
    protected ChecksumFactory _checkSumFactory;
    protected ChecksumFactory _optCheckSumFactory;
    protected long _allo;

    /** List of selected RFC 3659 facts. */
    protected Set<Fact> _currentFacts =
        new HashSet(Arrays.asList(new Fact[] {
                    Fact.SIZE, Fact.MODIFY, Fact.TYPE, Fact.UNIQUE }));

    /**
     * Configuration parameters related to performance markers.
     */
    protected PerfMarkerConf _perfMarkerConf = new PerfMarkerConf();

    protected static class PerfMarkerConf
    {
        protected boolean use;
        protected long    period;

        PerfMarkerConf()
        {
            use    = false;
            period = 3 * 60 * 1000L; // default - 3 minutes
        }
    }

    /**
     * Queue used to pass the address on which a pool listens when in
     * passive mode between threads. Under normal circumstances, this
     * queue will at most contain a single message.
     */
    private BlockingQueue<GFtpTransferStartedMessage> _transferStartedMessages
        = new LinkedBlockingQueue<GFtpTransferStartedMessage>();


    /**
     * Encapsulation of all parameters of a transfer.
     */
    protected class Transfer
    {
        /**
         * The session ID of this transfer. The session id is unique
         * for each transfer within the same AbstractFtpDoorV1.
         */
        final long sessionId;

        /**
         * Time when the transfer started, in milliseconds since the epoch.
         */
        final long startedAt;

        /** The name of the file being transferred. */
        final String  path;

        /**
         * Information for the billing cell.
         */
        final DoorRequestInfoMessage info;

        /** Description of the current state of the transfer. */
        String state;

        /** */
        Integer moverId;

        /** The name of the pool used for the transfer. */
        String pool;

        /** The PNFS id of the file being transferred. */
        PnfsId pnfsId;

        /** The host name of the client side of the data connection. */
        String client_host;

        /** The TCP port of the client side of the data connection. */
        int client_port;

        /**
         * True when a pnfs entry has been created for path, but the
         * transfer has not yet successfully completed.
         */
        boolean pnfsEntryIncomplete = false;

        /**
         * Socket adapter used for the transfer.
         */
        ProxyAdapter adapter;

        /**
         * Task that periodically generates performance markers. May
         * be null.
         */
        PerfMarkerTask perfMarkerTask;

        /**
         * True if the transfer was aborted.
         */
        boolean aborted = false;

        Transfer(String aPath)
        {
            sessionId = nextSessionId();
            startedAt = System.currentTimeMillis();
            path      = aPath;

            info =
                new DoorRequestInfoMessage(getNucleus().getCellName()+"@"+
                                           getNucleus().getCellDomainName());
            info.setTransactionTime(startedAt);
            info.setClient(_engine.getInetAddress().getHostName());
            // some requests do not have a pnfsId yet, fill it with dummy
            info.setPnfsId( new PnfsId("000000000000000000000000") );
            if (path != null) {
                info.setPath(path);
            }
        }

        /** Sends door request information to the billing cell. */
        void sendDoorRequestInfo(int code, String msg)
        {
            try {
                info.setResult(code, msg);
                sendMessage(new CellMessage(_billingCellPath , info));
            } catch (NoRouteToCellException e) {
                error("FTP Door: couldn't send door request data to " +
                      "billing database: " + e.getMessage());
            }
        }
    }

    protected Transfer _transfer;


    //
    // Use initializer to load up hashes.
    //
    {
        for (Method method : getClass().getMethods()) {
            String name = method.getName();
            if (name.regionMatches(false, 0, "ac_", 0, 3)){
                _methodDict.put(name.substring(3), method);
            }
        }
    }

    private synchronized static long nextSessionId()
    {
        return __counter++ ;
    }

    public static CellVersion getStaticCellVersion()
    {
        return new CellVersion(diskCacheV111.util.Version.getVersion(),
                               "$Revision$");
    }

    public void SetTLog(FTPTransactionLog tlog)
    {
        //XXX See IVM for how this is supposed to work
        // In some cases, passive retrieves are resetting tlog before it
        // can be used.
        if( tlog == null) {
            info("FTP Door: SetTLog isn't setting _tLog to " +
                 "null because it seems to screw things up");
        } else {
            info("FTP Door: SetTLog setting _tLog");
            _tLog = tlog;
        }
        //         try {
        //             throw new Exception();
        //         }
        //         catch(Exception ex) {
        //             ex.printStackTrace();
        //         }

    }

    public AbstractFtpDoorV1(String name, StreamEngine engine, Args args)
        throws InterruptedException, ExecutionException
    {
        super(name, args);

        try {
            _engine = engine;
            doInit();
            _workerThread.start();
        } catch (InterruptedException e) {
            reply("421 " + ftpDoorName + " door not ready");
            _shutdownGate.countDown();
        } catch (ExecutionException e) {
            reply("421 " + ftpDoorName + " door not ready");
            _shutdownGate.countDown();
        }
    }

    @Override
    protected void init()
        throws Exception
    {
        super.init();

        CDC.createSession();

        Args args = getArgs();
        _out      = new PrintWriter(_engine.getWriter());
        _client_data_host = _engine.getInetAddress().getHostName();

        debug("FTP Door: client hostname = " + _client_data_host);

        if (_local_host == null)
            _local_host = _engine.getLocalAddress().getHostName();

        if (!_use_gplazmaAuthzModule) {
            _gplazmaPolicyFilePath = null;
        } else if (_gplazmaPolicyFilePath == null) {
            String s = "FTP Door: -gplazma-authorization-module-policy " +
                "file argument wasn't specified";
            error(s);
            throw new IllegalArgumentException(s);
        }

        /* Use kpwd file if gPlazma is not enabled.
         */
        if (!(_use_gplazmaAuthzModule || _use_gplazmaAuthzCell)) {
            _kpwdFilePath = args.getOpt("kpwd-file");
            if ((_kpwdFilePath == null) ||
                (_kpwdFilePath.length() == 0) ||
                (!new File(_kpwdFilePath).exists())) {
                String s = "FTP Door: -kpwd-file file argument wasn't specified";
                error(s);
                throw new IllegalArgumentException(s);
            }
        }

        /* Data channel port range used when client issues PASV
         * command.
         */
        int low = 0;
        int high = 0;
        if (_portRange != null) {
            try {
                int ind = _portRange.indexOf(":");
                if ((ind <= 0) || (ind == (_portRange.length() - 1)))
                    throw new IllegalArgumentException("Not a port range");

                low  = Integer.parseInt(_portRange.substring(0, ind));
                high = Integer.parseInt(_portRange.substring(ind + 1));
                debug("Ftp Door: client data port range [" +
                      low + ":" + high + "]");
            } catch (NumberFormatException ee) {
                error("FTP Door: invalid port range string: " +
                      _portRange + ". Command ignored." );
                low = 0;
            }
        }
        _lowDataListenPort = low;
        _highDataListenPort = high;

        /* Parallelism for mode E transfers.
         */
        _parallelStart = _parallelMin = _parallelMax =
            _defaultStreamsPerClient;

        /* Permission handler.
         */
        Subject subject;
        _origin = new Origin(AuthType.ORIGIN_AUTHTYPE_STRONG,
                             _engine.getInetAddress());
        if (_permissionHandlerClasses != null) {
            _permissionHandler = new DelegatingPermissionHandler(this);
            subject = null;
        } else {
            _permissionHandler = new GrantAllPermissionHandler();
            subject = new Subject();
            subject.getPrincipals().add(_origin);
            subject.setReadOnly();
        }

        _pnfs = new PnfsHandler(this, new CellPath(_pnfsManager));
        _pnfs.setPnfsTimeout(_pnfsTimeout * 1000L);
        _pnfs.setSubject(subject);
        _listSource = new ListDirectoryHandler(_pnfs);

        adminCommandListener = new AdminCommandListener();
        addCommandListener(adminCommandListener);
        addMessageListener(_listSource);

        _checkStagePermission = new CheckStagePermission(_stageConfigurationFilePath);

        useInterpreter(true);

        _workerThread = new Thread(this);
    }

    protected AdminCommandListener adminCommandListener;
    public class AdminCommandListener
    {
        public String hh_get_door_info = "[-binary]";
        public Object ac_get_door_info(Args args)
        {
            IoDoorInfo doorInfo = new IoDoorInfo(getCellName(),
                                                 getCellDomainName());
            doorInfo.setProtocol("GFtp","1");
            doorInfo.setOwner(_pwdRecord == null ? "0" : Integer.toString(_pwdRecord.UID));
            //doorInfo.setProcess( _pid == null ? "0" : _pid ) ;
            doorInfo.setProcess("0");
            if (_transfer != null) {
                IoDoorEntry[] entries = {
                    new IoDoorEntry(_transfer.sessionId,
                                    _transfer.pnfsId,
                                    _transfer.pool,
                                    _transfer.state,
                                    _transfer.startedAt,
                                    _transfer.client_host)
                };
                doorInfo.setIoDoorEntries(entries);
            } else {
                IoDoorEntry[] entries = {};
                doorInfo.setIoDoorEntries(entries);
            }

            if (args.getOpt("binary") != null)
                return doorInfo;
            else
                return doorInfo.toString();
        }
    }

    private int spawn(String cmd, int errexit)
    {
        try {
            Process p = Runtime.getRuntime().exec(cmd);
            p.waitFor();
            int returnCode = p.exitValue();
            p.destroy();
            return returnCode;
        } catch (IOException e) {
            return errexit;
        } catch (InterruptedException e) {
            return errexit;
        }
    }


    public void ftpcommand(String cmdline)
        throws CommandExitException
    {
        int l = 4;
        // Every FTP command is 3 or 4 characters
        if (cmdline.length() < 3) {
            reply(err(cmdline, ""));
            return;
        }
        if (cmdline.length() == 3 || cmdline.charAt(3) == ' ') {
            l = 3;
        }

        String cmd = cmdline.substring(0,l);
        String arg = cmdline.length() > l + 1 ? cmdline.substring(l + 1) : "";
        Object args[] = {arg};

        cmd = cmd.toLowerCase();

        // most of the ic is handled in the ac_ functions but a few
        // commands need special handling
        if (cmd.equals("mic" ) || cmd.equals("conf") || cmd.equals("enc") ||
            cmd.equals("adat") || cmd.equals("pass")) {
            info("ftpcommand <" + cmd + " ... >");
        } else {
            _lastCommand = cmdline;
            info("ftpcommand <" + cmdline + ">");
        }

        // If a transfer is in progress, only permit ABORT and a few
        // other commands to be processed
        synchronized(this) {
            if (_transfer != null &&
                !(cmd.equals("abor") || cmd.equals("mic")
                  || cmd.equals("conf") || cmd.equals("enc")
                  || cmd.equals("quit") || cmd.equals("bye"))) {
                reply("503 Transfer in progress", false);
                return;
            }
        }

        if (!_methodDict.containsKey(cmd)) {
            _skipBytes = 0;
            reply(err(cmd,arg));
            return;
        }

        resetPwdRecord();

        Method m = _methodDict.get(cmd);
        try {
            // Most of this info is printed above.
            // Uncomment the next line for debugging.
            // debug("Going to invoke:" + m.getName() +"("+arg+")");
            m.invoke(this, args);
            if (!cmd.equals("rest"))
                _skipBytes = 0;
        } catch (InvocationTargetException ite) {
            //
            // is thrown if the underlying method
            // actively throws an exception.
            //
            Throwable te = ite.getTargetException();
            if (te instanceof CommandExitException) {
                throw (dmg.util.CommandExitException)te;
            }
            reply("500 Operation failed due to internal error: " +
                  te.getMessage());

            /* We don't want to call reportBug, since throwing a
             * RuntimeException at this point will lead to other error
             * messages being generated. Just log the exception as
             * fatal and continue.
             */
            fatal("FTP door: ftp command '" + cmd +
                  "' got exception: " + te.getMessage());
            fatal(te);
            _skipBytes = 0;
        } catch (IllegalAccessException e) {
            reportBug("ftpcommand", "got illegal access exception", e);
        }
    }

    private synchronized void shutdownInputStream()
    {
        try {
            Socket socket = _engine.getSocket();
            if (!socket.isClosed() && !socket.isInputShutdown())
                socket.shutdownInput();
        } catch (IOException e) {
            warn("FTP Door failed to shut down input stream of the " +
                 "control channel: " + e.getMessage());
        }
    }


    protected synchronized void closeAdapter()
    {
        if (_adapter != null) {
            info("Ftp Door closing adapter");
            _adapter.close();
            _adapter = null;
        }
    }

    /**
     * Main loop for FTP command processing.
     *
     * Commands are read from the socket and submitted to the command
     * queue for execution. Upon termination, most of the shutdown
     * logic is in this method, including:
     *
     * - Emergency shutdown of performance marker engine
     * - Shut down of socket adapter
     * - Abort and cleanup after failed transfers
     * - Cell shutdown initiation
     *
     * Notice that socket and thus input and output streams are not
     * closed here. See cleanUp() for details on this.
     */
    public void run()
    {
        NDC.push(CDC.getSession().toString());
        try {
            try {
                /* Notice that we do not close the input stream, as
                 * doing so would close the socket as well. We don't
                 * want to do that until cleanUp() is called.
                 *
                 * REVISIT: I hope that the StreamEngine does not
                 * maintain any ressources that do not get
                 * automatically freed when the socket is closed.
                 */
                BufferedReader in =
                    new BufferedReader(new InputStreamReader(_engine.getInputStream(), "UTF-8"));

                reply("220 " + ftpDoorName + " door ready");

                String s = in.readLine();
                while (s != null) {
                    _commandQueue.add(s);
                    s = in.readLine();
                }
            } catch (IOException e) {
                error("FTP Door: got error reading data: " + e.getMessage());
            } finally {
                /* This will block until command processing has
                 * finished.
                 */
                try {
                    _commandQueue.stop();
                } catch (InterruptedException e) {
                    error("FTP Door: failed to shut down command processing: "
                          + e.getMessage());
                }

                /* In case of failure, we may have a transfer hanging
                 * around.
                 */
                if (_transfer != null) {
                    abortTransfer(451, "Aborting transfer due to session termination");
                }

                closeAdapter();

                debug("FTP Door: end of stream encountered");
            }
        } finally {
            /* cleanUp() waits for us to open the gate.
             */
            _shutdownGate.countDown();

            /* Killing the cell will cause cleanUp() to be
             * called (although from a different thread).
             */
            kill();

            NDC.remove();
        }
    }

    /**
     * Called by the cell infrastructure when the cell has been killed.
     *
     * If the FTP session is still running, a shutdown of it will be
     * initiated. The method blocks until the FTP session has shut
     * down.
     *
     * The socket will be closed by this method. It is quite important
     * that this does not happen earlier, as several threads use the
     * output stream. This is the only place where we can be 100%
     * certain, that all the other threads are done with their job.
     */
    @Override
    public void cleanUp()
    {
        /* Closing the input stream will cause the FTP command
         * procesing thread to shut down. In case the shutdown was
         * initiated by the FTP client, this will already have
         * happened at this point. However if the cell is shut down
         * explicitly, then we have to shutdown the input stream here.
         */
        shutdownInputStream();

        /* The FTP command processing thread will open the gate after
         * shutdown.
         */
        try {
            _shutdownGate.await();
        } catch (InterruptedException e) {
            /* This should really not happen as nobody is supposed to
             * interrupt the cell thread, but if it does happen then
             * we better log it.
             */
            error("FTP Door: got interrupted exception shutting down input stream");
        }

        try {
            /* Closing the socket will also close the input and output
             * streams of the socket. This in turn will cause the
             * command poller thread to shut down.
             */
            _engine.getSocket().close();
        } catch (IOException e) {
            error("FTP Door: got I/O exception closing socket: " +
                  e.getMessage());
        }

        super.cleanUp();
    }

    public void println(String str)
    {
        synchronized (_out) {
            _out.println(str + "\r");
            _out.flush();
        }
        // This prints everything including encoded replies and
        // is not needed for normal logging.
        // It can be uncommented for debugging.
        //debug( "TO CLIENT : " + str ) ;
    }

    public void print(String str)
    {
        synchronized (_out) {
            _out.print(str);
            _out.flush();
        }
    }

    public void execute(String command)
    {
        try {
            if (command.equals("")) {
                reply(err("",""));
            } else {
                _commandCounter++;
                ftpcommand(command);
            }
        } catch (CommandExitException e) {
            shutdownInputStream();
        }
    }

    @Override
    public String toString()
    {
        return _user + "@" + _client_data_host;
    }

    @Override
    public void getInfo(PrintWriter pw)
    {
        pw.println( "            FTPDoor");
        pw.println( "         User  : " + _dnUser == null? _user : _dnUser);
        pw.println( "    User Host  : " + _client_data_host);
        pw.println( "   Local Host  : " + _local_host);
        pw.println( " Last Command  : " + _lastCommand);
        pw.println( " Command Count : " + _commandCounter);
        pw.println( "     I/O Queue : " + _ioQueueName);
        pw.println(adminCommandListener.ac_get_door_info(new Args("")));
    }

    /**
     * Handles post-transfer success/failure messages going back to
     * the client.
     */
    public void messageArrived(CellMessage envelope,
                               DoorTransferFinishedMessage reply)
    {
        String adapterError = null;
        boolean adapterClosed = false;
        ProxyAdapter adapter;

        /* The synchronization is required to ensure that the call
         * to transfer() has returned before we clean up after the
         * transfer. It is also needed to ensure that this block
         * completes before we start shutting down the cell.
         */
        synchronized (this) {
            debug("FTP Door received 'Door Transfer Finished' message");

            /* It may happen the transfer has been cancelled and
             * cleaned up after already. This is not a failure.
             */
            if (_transfer == null) {
                return;
            }

            /* The abortTransfer method may wait for moverId to be
             * reset.
             */
            _transfer.moverId = null;
            notifyAll();

            /* It may happen the transfer has been aborted
             * already. This is not a failure.
             */
            if (_transfer.aborted) {
                return;
            }

            /* Kill the adapter in case of errors or if it is an
             * ActiveAdapter (they have to be killed to shut down).
             */
            adapter = _transfer.adapter;
            if (adapter != null && (adapter instanceof ActiveAdapter
                                    || reply.getReturnCode() != 0)) {
                debug("FTP Door closing adapter after message arrived.");
                adapter.close();
                adapterClosed = true;
            }
        }

        /* Wait for adapter to shut down.
         *
         * We do this unsynchronized to avoid blocking while
         * holding the monitor. Concurrent access to the adapter
         * itself is safe and the following will be a noop if
         * another thread happens to close the adapter.
         */
        if (adapter != null) {
            info("FTP Door: Message arrived. Waiting for adapter to finish.");
            try {
                adapter.join(300000); // 5 minutes
                if (adapter.isAlive()) {
                    warn("FTP Door: Adapter didn't shut down. Killing adapter");
                    adapterClosed = true;
                    adapterError = "adapter did not shut down";
                    adapter.close();
                    adapter.join(10000); // 10 seconds
                    if (adapter.isAlive()) {
                        error("FTP Door: failed to kill adapter");
                    }
                } else if (adapter.hasError()) {
                    adapterError =_transfer.adapter.getError();
                }
            } catch (InterruptedException e) {
                error("FTP Door: thread join error: " + e.getMessage());
                adapterError = "adapter did not shut down";
            }

            /* With GridFTP v2 GET and PUT commands, we may
             * have a temporary socket adapter specific to
             * this transfer. If so, close it.
             */
            if (adapter != _adapter && !adapterClosed) {
                debug("FTP Door: Message arrived. Closing adapter");
                adapter.close();
                adapterClosed = true;
            }
        }

        synchronized (this) {
            /* It may happen that the transfer was aborted while we
             * killed the adapter. This is not a failure.
             */
            if (_transfer == null || _transfer.aborted) {
                return;
            }

            if (reply.getReturnCode() == 0 && adapterError == null) {
                if (_transfer.perfMarkerTask != null) {
                    ProtocolInfo pinfo = reply.getProtocolInfo();
                    if (pinfo != null && pinfo instanceof GFtpProtocolInfo) {
                        _transfer.perfMarkerTask.stop((GFtpProtocolInfo)reply.getProtocolInfo());
                    } else {
                        _transfer.perfMarkerTask.stop();
                        reportBug("messageArrived",
                    "DoorTransferFinishedMessage arrived and " +
                                  "ProtocolInfo is null or is not of type " +
                                  "GFtpProtocolInfo", null);
                    }
                }

                StorageInfo storageInfo = reply.getStorageInfo();
                if (_tLog != null) {
                    _tLog.middle(storageInfo.getFileSize());
                    _tLog.success();
                    SetTLog(null);
                }


                // RDK: Note that data/command channels both dropped (esp. ACTIVE mode) at same time
                //      can lead to a race. The transfer will be declared successful, this flag cleared,
                //      and THEN the command channel drop is reacted to. This is difficult to reproduce.
                //      Treat elsewhere to prevent a successful return code from being returned.
                //      Clear the _pnfsEntryIncomplete flag since transfer successful
                _transfer.sendDoorRequestInfo(0, "");
                _transfer = null;
                notifyAll();
                reply("226 Transfer complete.");
            } else {
                StringBuffer errMsg = new StringBuffer("Transfer aborted (");

                if (reply.getReturnCode() != 0) {
                    if (reply.getErrorObject() != null) {
                        errMsg.append(reply.getErrorObject());
                    } else {
                        errMsg.append("mover failure");
                    }
                    if (adapterError != null) {
                        errMsg.append("/");
                        errMsg.append(adapterError);
                    }
                } else {
                    errMsg.append(adapterError);
                }
                errMsg.append(")");

                abortTransfer(426, errMsg.toString());
            }
        }
    }

    public void messageArrived(CellMessage envelope,
                               GFtpTransferStartedMessage message)
    {
        try {
            _transferStartedMessages.put(message);
        } catch (InterruptedException e) {
            /* The queue is not bounded, thus this should never happen.
             */
            error("FTP Door: unexpected interrupted exception: " + e);
        }
    }

    //
    // GSS authentication
    //

    protected void reply(String answer, boolean resetReply)
    {
        if (answer.startsWith("335 ADAT=")) {
            info("REPLY(reset=" + resetReply + " GReplyType=" + _gReplyType + "): <335 ADAT=...>");
        } else {
            info("REPLY(reset=" + resetReply + " GReplyType=" + _gReplyType + "): <" + answer + ">");
        }
        if (_gReplyType.equals("clear"))
            println(answer);
        else if (_gReplyType.equals("mic"))
            secure_reply(answer, "631");
        else if (_gReplyType.equals("enc"))
            secure_reply(answer, "633");
        else if (_gReplyType.equals("conf"))
            secure_reply(answer, "632");
        if (resetReply)
            _gReplyType = "clear";
    }

    protected void reply(String answer)
    {
        reply(answer, true);
    }

    protected abstract void secure_reply(String answer, String code);

    public void ac_feat(String arg)
    {
        StringBuilder builder = new StringBuilder();
        builder.append("211-OK\r\n");
        for (String feature: FEATURES) {
            builder.append(' ').append(feature).append("\r\n");
        }

        /* RFC 3659 specifies the MLST feature. It is followed by the
         * list of supported facts. Currently active facts are
         * suffixed by an asterix.
         */
        builder.append(" MLST ");
        for (Fact fact: Fact.values()) {
            builder.append(fact);
            if (_currentFacts.contains(fact)) {
                builder.append('*');
            }
            builder.append(';');
        }
        builder.append("\r\n");

        builder.append("211 End");
        reply(builder.toString());
    }

    public void opts_retr(String opt)
    {
        String[] st = opt.split("=");
        String real_opt = st[0];
        String real_value= st[1];
        if (!real_opt.equalsIgnoreCase("Parallelism")) {
            reply("501 Unrecognized option: " + real_opt + " (" + real_value + ")");
            return;
        }

        st = real_value.split(",|;");
        _parallelStart = Integer.parseInt(st[0]);
        _parallelMin = Integer.parseInt(st[1]);
        _parallelMax = Integer.parseInt(st[2]);

        if (_maxStreamsPerClient > 0) {
            _parallelStart = Math.min(_parallelStart, _maxStreamsPerClient);
            _parallelMin = Math.min(_parallelMin, _maxStreamsPerClient);
            _parallelMax = Math.min(_parallelMax, _maxStreamsPerClient);
        }

        reply("200 Parallel streams set (" + opt + ")");
    }

    public void opts_stor(String opt, String val)
    {
        if (!opt.equalsIgnoreCase("EOF")) {
            reply("501 Unrecognized option: " + opt + " (" + val + ")");
            return;
        }
        if (!val.equals("1")) {
            _confirmEOFs = true;
            reply("200 EOF confirmation is ON");
            return;
        }
        if (!val.equals("0")) {
            _confirmEOFs = false;
            reply("200 EOF confirmation is OFF");
            return;
        }
        reply("501 Unrecognized option value: " + val);
    }

    private void opts_cksm(String algo)
    {
        if (algo ==  null) {
            reply("501 CKSM option command requires algorithm type");
            return;
        }

        try {
            if (!algo.equalsIgnoreCase("NONE")) {
                _optCheckSumFactory = ChecksumFactory.getFactory(algo);
            } else {
                _optCheckSumFactory = null;
            }
            reply("200 OK");
        } catch (NoSuchAlgorithmException e) {
            reply("504 Unsupported checksum type: " + algo);
        }
    }

    private void opts_mlst(String facts)
    {
        Set<Fact> newFacts = new HashSet();
        for (String s: facts.split(";")) {
            Fact fact = Fact.find(s);
            if (fact != null) {
                newFacts.add(fact);
            }
        }

        _currentFacts = newFacts;

        if (_currentFacts.isEmpty()) {
            reply("200 MLST");
        } else {
            StringBuffer s = new StringBuffer("200 MLST ");
            for (Fact fact: _currentFacts) {
                s.append(fact).append(';');
            }
            reply(s.toString());
        }
    }

    public void ac_opts(String arg)
    {
        String[] st = arg.split("\\s+");
        if (st.length == 2 && st[0].equalsIgnoreCase("RETR")) {
            opts_retr(st[1]);
        } else if (st.length == 3 && st[0].equalsIgnoreCase("STOR")) {
            opts_stor(st[1], st[2]);
        } else if (st.length == 2 && st[0].equalsIgnoreCase("CKSM")) {
            opts_cksm(st[1]);
        } else if (st.length == 1 && st[0].equalsIgnoreCase("MLST")) {
            opts_mlst("");
        } else if (st.length == 2 && st[0].equalsIgnoreCase("MLST")) {
            opts_mlst(st[1]);
        } else {
            reply("501 Unrecognized option: " + st[0] + " (" + arg + ")");
        }
    }

    public void ac_dele(String arg)
    {
        if (_readOnly) {
            reply("500 Command disabled");
            return;
        }

        if (_pwdRecord.isReadOnly()) {
            if(!setNextPwdRecord()) {
                reply("500 Command disabled");
                return;
            } else {
                ac_dele(arg);
                return;
            }
        }

        String pnfsPath = absolutePath(arg);

        try {
            if (_permissionHandler.canDeleteFile(pnfsPath, getSubject(), _origin) != AccessType.ACCESS_ALLOWED) {
                if(!setNextPwdRecord()) {
                    reply("550 Permission denied");
                    return;
                } else {
                    ac_dele(arg);
                    return;
                }
            }

            _pnfs.deletePnfsEntry(pnfsPath,
                                  EnumSet.of(FileType.REGULAR, FileType.LINK));

        }catch(ACLException e) {
            reply("550 Permission denied");
            error("FTP Door: DELE got AclException: " + e.getMessage());
            return;
        } catch (PermissionDeniedCacheException e) {
            if(!setNextPwdRecord()) {
                reply("550 Permission denied");
            } else {
                ac_dele(arg);
            }
            return;
        } catch (CacheException e) {
            error("FTP Door: DELE got CacheException: " + e.getMessage());
            if(!setNextPwdRecord()) {
                reply("550 Permission denied, reason: " + e);
            } else {
                ac_dele(arg);
            }
            return;
        }
        sendRemoveInfoToBilling(pnfsPath);
        reply("200 file deleted");
    }

    public abstract void ac_auth(String arg);


    public abstract void ac_adat(String arg);

    public void ac_mic(String arg)
        throws dmg.util.CommandExitException
    {
        secure_command(arg, "mic");
    }

    public void ac_enc(String arg)
        throws dmg.util.CommandExitException
    {
        secure_command(arg, "enc");
    }

    public void ac_conf(String arg)
        throws dmg.util.CommandExitException
    {
        secure_command(arg, "conf");
    }

    public abstract void secure_command(String arg, String sectype)
        throws dmg.util.CommandExitException;



    public void ac_ccc(String arg)
    {
        // We should never received this, only through MIC, ENC or CONF,
        // in which case it will be intercepted by secure_command()
        reply("533 CCC must be protected");
    }

    public abstract void ac_user(String arg);


    public abstract void ac_pass(String arg);




    public void ac_pbsz(String arg)
    {
        reply("200 OK");
    }

    public void ac_prot(String arg)
    {
        if (!arg.equals("C"))
            reply("534 Will accept only Clear protection level");
        else
            reply("200 OK");
    }

    ///////////////////////////////////////////////////////////////////////////
    //                                                                       //
    // the interpreter stuff                                                 //
    //                                                                       //


    private String absolutePath(String relCwdPath)
    {
        if (_pathRoot == null)
            return null;

        _curDirV = (_curDirV == null ? "/" : _curDirV);
        FsPath relativeToRootPath = new FsPath(_curDirV);
        relativeToRootPath.add(relCwdPath);


        FsPath absolutePath = new FsPath(_pathRoot);
        String rootPath = absolutePath.toString();
        absolutePath.add("./" + relativeToRootPath.toString());
        String absolutePathStr = absolutePath.toString();
        debug("Absolute Path is \"" + absolutePathStr +
              "\", root is " + _pathRoot);
        if (!absolutePathStr.startsWith(rootPath)) {
            debug("AbsolutePath didn't start with root");
            return null;
        }
        return absolutePathStr;
    }

    public void ac_rmd(String arg)
    {
        if (arg.equals("")) {
            reply(err("RMD",arg));
            return;
        }

        if (_pwdRecord == null) {
            reply("530 Not logged in.");
            return;
        }

        if (_readOnly) {
            reply("500 Command disabled");
            return;
        }

        if (_pwdRecord.isReadOnly()) {
            if(!setNextPwdRecord()) {
                reply("500 Command disabled");
                return;
            } else {
                ac_rmd(arg);
                return;
            }
        }

        if (_pwdRecord.isAnonymous()) {
            if(!setNextPwdRecord()) {
                reply("554 Anonymous write access not permitted");
                return;
            } else {
                ac_rmd(arg);
                return;
            }
        }

        String pnfsPath = absolutePath(arg);
        if (pnfsPath == null) {
            if(!setNextPwdRecord()) {
                reply("553 Cannot determine full directory pathname in PNFS: " + arg);
                return;
            } else {
                ac_rmd(arg);
                return;
            }
        }

        try {
            PnfsId pnfsId = _pnfs.getPnfsIdByPath(pnfsPath);
            if (_permissionHandler.canDeleteDir(pnfsId, getSubject(), _origin) != AccessType.ACCESS_ALLOWED) {
                if(!setNextPwdRecord()) {
                    reply("550 Permission denied");
                    return;
                } else {
                    ac_rmd(arg);
                    return;
                }
            }

            _pnfs.deletePnfsEntry(pnfsPath, EnumSet.of(FileType.DIR));

            reply("200 OK");
        }catch(ACLException e) {
            reply("550 Permission denied, reason (Acl) ");
            error("FTP Door: ACL module failed: " + e);
        } catch (FileNotFoundCacheException e) {
            reply("550 File not found");
        } catch (PermissionDeniedCacheException e) {
            if(!setNextPwdRecord()) {
                reply("550 Permission denied");
            } else {
                ac_rmd(arg);
            }
        } catch (CacheException ce) {
            if(!setNextPwdRecord()) {
                reply("550 Permission denied, reason: " + ce);
            } else {
                ac_rmd(arg);
            }
        }
    }


    public void ac_mkd(String arg)
    {
        if (_pwdRecord == null) {
            reply("530 Not logged in.");
            return;
        }

        if (arg.equals("")) {
            reply(err("MKD",arg));
            return;
        }

        if (_readOnly) {
            reply("500 Command disabled");
            return;
        }

        if (_pwdRecord.isReadOnly()) {
            if(!setNextPwdRecord()) {
                reply("500 Command disabled");
                return;
            } else {
                ac_mkd(arg);
                return;
            }
        }

        if (_pwdRecord.isAnonymous()) {
            if(!setNextPwdRecord()) {
                reply("554 Anonymous write access not permitted");
                return;
            } else {
                ac_mkd(arg);
                return;
            }
        }

        String pnfsPath = absolutePath(arg);
        if (pnfsPath == null) {
            if(!setNextPwdRecord()) {
                reply("553 Cannot create directory in PNFS: " + arg);
                return;
            } else {
                ac_mkd(arg);
                return;
            }
        }

        try {
            String parent = new File(pnfsPath).getParent();
            PnfsId parentId = _pnfs.getPnfsIdByPath(parent);
            if (_permissionHandler.canCreateDir(parentId, getSubject(), _origin) != AccessType.ACCESS_ALLOWED) {
                if(!setNextPwdRecord()) {
                    reply("553 Permission denied");
                    return;
                } else {
                    ac_mkd(arg);
                    return;
                }
            }

            _pnfs.createPnfsDirectory(pnfsPath, _pwdRecord.UID, _pwdRecord.GID, 0755);

        }catch(ACLException e) {
            reply("550 Permission denied, reason (Acl) ");
            error("FTP Door: ACL module failed: " + e);
            return;
        } catch (PermissionDeniedCacheException e) {
            if(!setNextPwdRecord()) {
                reply("550 Permission denied");
            } else {
                ac_mkd(arg);
            }
            return;
        } catch(CacheException ce) {
            if(!setNextPwdRecord()) {
                reply("553 Permission denied, reason: "+ce);
                return;
            } else {
                ac_mkd(arg);
                return;
            }
        }
        reply("200 OK");
    }

    public void ac_help(String arg)
    {
        reply("214 No help available");
    }

    public void ac_syst(String arg)
    {
        reply("215 UNIX Type: L8 Version: FTPDoor");
    }

    public void ac_type(String arg)
    {
        reply("200 Type set to I");
    }

    public void ac_noop(String arg)
    {
        reply(ok("NOOP"));
    }

    private static final Pattern ALLO_PATTERN =
        Pattern.compile("(\\d+)( R \\d+)?");

    public void ac_allo(String arg)
    {
        _allo = 0;

        Matcher matcher = ALLO_PATTERN.matcher(arg);
        if (!matcher.matches()) {
            reply("501 Invalid argument");
            return;
        }

        try {
            _allo = Long.parseLong(matcher.group(1));
        } catch (NumberFormatException e) {
            reply("501 Invalid argument");
            return;
        }

        reply(ok("ALLO"));
    }

    public void ac_pwd(String arg)
    {
        if (!arg.equals("")) {
            reply(err("PWD",arg));
            return;
        }
        reply("257 \"" + _curDirV + "\" is current directory");
    }

    public void ac_cwd(String arg)
    {
        try {
            String newcwd = absolutePath(arg);
            if (newcwd == null)
                newcwd = _pathRoot;

            FileAttributes attributes =
                _pnfs.getFileAttributes(newcwd, EnumSet.of(SIMPLE_TYPE));
            if (attributes.getFileType() != FileType.DIR) {
                reply("550 Not a directory: " + arg);
                return;
            }
            _curDirV = newcwd.substring(_pathRoot.length());
            if (_curDirV.length() == 0)
                _curDirV = "/";
            reply("250 CWD command succcessful. New CWD is <" + _curDirV + ">");
        } catch (FileNotFoundCacheException e) {
            reply("550 File not found");
        } catch (CacheException e) {
            reply("451 CWD failed: " + e.getMessage());
            error("Error in CWD: " + e);
        }
    }

    public void ac_cdup(String arg)
    {
        ac_cwd("..");
    }

    public void ac_port(String arg)
    {
        String[] st = arg.split(",");
        if (st.length != 6) {
            reply(err("PORT",arg));
            return;
        }

        int tok[] = new int[6];
        for (int i = 0; i < 6; ++i) {
            tok[i] = Integer.parseInt(st[i]);
        }
        String ip = tok[0] + "." + tok[1] + "." + tok[2] + "." + tok[3];
        _client_data_host = ip;

        // XXX if transfer in progress, abort
        _client_data_port = tok[4] * 256 + tok[5];

        // Switch to active mode
        closeAdapter();
        _mode = Mode.ACTIVE;

        reply(ok("PORT"));
    }

    public void ac_pasv(String arg)
    {
        try {
            closeAdapter();
            info("FTP Door creating adapter for passive mode");
            _adapter = new SocketAdapter(this, _lowDataListenPort , _highDataListenPort);
            _adapter.setMaxBlockSize(_maxBlockSize);
            int port = _adapter.getClientListenerPort();
            byte[] hostb = _engine.getLocalAddress().getAddress();
            int[] host = new int[4];
            for (int i = 0; i < 4; i++) {
                host[i] = hostb[i] & 0377;
            }
            _mode = Mode.PASSIVE;
            reply("227 OK (" +
                  host[0] + "," +
                  host[1] + "," +
                  host[2] + "," +
                  host[3] + "," +
                  port/256 + "," +
                  port % 256 + ")");
            //_host = host[0]+"."+host[1]+"."+host[2]+"."+host[3];
            //_port = 0;
            // will be set by retr/stor
        } catch (IOException e) {
            reply("500 Cannot enter passive mode: " + e);
            _mode = Mode.ACTIVE;
            closeAdapter();
        }
    }

    public void ac_mode(String arg)
    {
        if (arg.equalsIgnoreCase("S")) {
            _xferMode = "S";
            reply("200 Will use Stream mode");
        } else if (arg.equalsIgnoreCase("E")) {
            _xferMode = "E";
            reply("200 Will use Extended Block mode");
        } else if (arg.equalsIgnoreCase("X")) {
            _xferMode = "X";
            reply("200 Will use GridFTP 2 eXtended block mode");
        } else {
            reply("200 Unsupported transfer mode");
        }
    }

    public void ac_site(String arg)
    {
        if (arg.equals("")) {
            reply("500 must supply the site specific command");
            return;
        }

        String args[] = arg.split(" ");

        if (args[0].equalsIgnoreCase("BUFSIZE")) {
            if (args.length != 2) {
                reply("500 command must be in the form 'SITE BUFSIZE <number>'");
                return;
            }
            ac_sbuf(args[1]);
        } else if ( args[0].equalsIgnoreCase("CHKSUM")) {
            if (args.length != 2) {
                reply("500 command must be in the form 'SITE CHKSUM <value>'");
                return;
            }
            doCheckSum("adler32",args[1]);
        } else if (args[0].equalsIgnoreCase("CHMOD")) {
            if (args.length != 3) {
                reply("500 command must be in the form 'SITE CHMOD <octal perms> <file/dir>'");
                return;
            }
            doChmod(args[1], args[2]);
        } else {
            reply("500 Unknown SITE command");
            return;
        }
    }

    public void ac_cksm(String arg)
    {
        String[] st = arg.split("\\s+");
        if (st.length != 4) {
            reply("500 Unsupported CKSM command operands");
            return;
        }
        String algo = st[0];
        String offset = st[1];
        String length = st[2];
        String path = st[3];

        long offsetL = 0;
        long lengthL = -1;

        try {
            offsetL = Long.parseLong(offset);
        } catch (NumberFormatException ex){
            reply("501 Invalid offset format:"+ex);
            return;
        }

        try {
            lengthL = Long.parseLong(length);
        } catch (NumberFormatException ex){
            reply("501 Invalid length format:"+ex);
            return;
        }

        try {
            doCksm(algo,path,offsetL,lengthL);
        } catch (FTPCommandException e) {
            reply(String.valueOf(e.getCode()) + " " + e.getReply());
        }
    }

    public void doCksm(String algo, String path, long offsetL, long lengthL)
        throws FTPCommandException
    {
        if (lengthL != -1)
            throw new FTPCommandException(504, "Unsupported checksum over partial file length");

        if (offsetL != 0)
            throw new FTPCommandException(504, "Unsupported checksum over partial file offset");

        try {
            ChecksumFactory cf = ChecksumFactory.getFactory(algo);

            try {
                PnfsId pnfsId = _pnfs.getPnfsIdByPath(absolutePath(path));
                Checksum checksum = cf.createFromPersistentState(this, pnfsId);
                if (checksum == null) {
                    throw new FTPCommandException(504, "Checksum is not available, dynamic checksum calculation is not supported");
                } else {
                    reply("213 "+Checksum.toHexString(checksum.getDigest()));
                }

            } catch (CacheException ce) {
                throw new FTPCommandException(550, "Error retrieving " + path
                                              + ": " + ce.getMessage());
            }
        } catch (java.security.NoSuchAlgorithmException ex) {
            throw new FTPCommandException(504, "Unsupported checksum type:" + ex);
        }
    }

    public void ac_scks(String arg)
    {
        String[] st = arg.split("\\s+");
        if (st.length != 2) {
            reply("505 Unsupported SCKS command operands");
            return;
        }
        doCheckSum(st[0], st[1]);
    }


    public void doCheckSum(String type, String value)
    {
        try {
            _checkSumFactory = ChecksumFactory.getFactory(type);
            _checkSum = _checkSumFactory.create(value);
            reply("213 OK");
        } catch (NoSuchAlgorithmException e) {
            _checkSumFactory = null;
            _checkSum = null;
            reply("504 Unsupported checksum type:" + type);
        }
    }

    public void doChmod(String permstring, String path)
    {
        if (_pwdRecord == null) {
            reply("530 Not logged in.");
            return;
        }

        if (_readOnly) {
            reply("500 Command disabled");
            return;
        }

        if (_pwdRecord.isReadOnly()) {
            if(!setNextPwdRecord()) {
                reply("500 Command disabled");
                return;
            } else {
                doChmod(permstring, path);
                return;
            }
        }

        if (path.equals("")){
            reply(err("SITE CHMOD",path));
            return;
        }

        String pnfsPath = absolutePath(path);
        if (pnfsPath == null) {
            if(!setNextPwdRecord()) {
                reply("553 Cannot determine full directory pathname in PNFS: " + path);
                return;
            } else {
                doChmod(permstring, path);
                return;
            }
        }

        int newperms;
        try {
            newperms = Integer.parseInt(permstring, 8); // Assume octal regardless of string
        } catch (NumberFormatException ex) {
            reply("501 permissions argument must be an octal integer");
            return;
        }

        // Get meta-data for this file/directory
        FileAttributes attributes;
        try {
            attributes =
                _pnfs.getFileAttributes(pnfsPath,
                                        EnumSet.of(PNFSID, TYPE,
                                                   OWNER, OWNER_GROUP));
        } catch (PermissionDeniedCacheException e) {
            if(!setNextPwdRecord()) {
                reply("550 Permission denied");
            } else {
                doChmod(permstring, path);
            }
            return;
        } catch (CacheException ce) {
            if(!setNextPwdRecord()) {
                reply("550 Permission denied, reason: " + ce);
            } else {
                doChmod(permstring, path);
            }
            return;
        }

        // Extract fields of interest
        PnfsId       myPnfsId   = attributes.getPnfsId();
        boolean      isADir     = (attributes.getFileType() == FileType.DIR);
        boolean      isASymLink = (attributes.getFileType() == FileType.LINK);
        int          myUid      = attributes.getOwner();
        int          myGid      = attributes.getGroup();

        // Only file/directory owner can change permissions on that file/directory
        if (myUid != _pwdRecord.UID) {
            if(!setNextPwdRecord()) {
                reply("550 Permission denied. Only owner can change permissions.");
            } else {
                doChmod(permstring, path);
            }
            return;
        }

        // Chmod on symbolic links not yet supported (should change perms on file/dir pointed to)
        if (isASymLink) {
            if(!setNextPwdRecord()) {
                reply("502 chmod of symbolic links is not yet supported.");
            } else {
                doChmod(permstring, path);
            }
            return;
        }

        FileMetaData newMetaData = new FileMetaData(isADir,myUid,myGid,newperms);

        _pnfs.pnfsSetFileMetaData(myPnfsId,newMetaData);

        reply("200 OK");
    }

    public void ac_sbuf(String arg)
    {
        if (arg.equals("")) {
            reply("500 must supply a buffer size");
            return;
        }

        int bufsize;
        try {
            bufsize = Integer.parseInt(arg);
        } catch(NumberFormatException ex) {
            reply("500 bufsize argument must be integer");
            return;
        }

        if (bufsize < 1) {
            reply("500 bufsize must be positive.  Probably large, but at least positive");
            return;
        }

        _bufSize = bufsize;
        reply("200 bufsize set to " + arg);
    }

    public void ac_eret(String arg)
    {
        String[] st = arg.split("\\s+");
        if (st.length < 2) {
            reply(err("ERET", arg));
            return;
        }
        String extended_retrieve_mode = st[0];
        String cmd = "eret_" + extended_retrieve_mode.toLowerCase();
        Object args[] = { arg };
        if (_methodDict.containsKey(cmd)) {
            Method m = _methodDict.get(cmd);
            try {
                info("FTP Door: error return invoking:" + m.getName() +
                     "(" + arg + ")");
                m.invoke(this, args);
            } catch (IllegalAccessException e) {
                reply("500 " + e.toString());
                _skipBytes = 0;
            } catch (InvocationTargetException e) {
                reply("500 " + e.toString());
                _skipBytes = 0;
            }
        } else {
            reply("504 ERET is not implemented for retrieve mode: "
                  + extended_retrieve_mode);
        }
    }

    public void ac_esto(String arg)
    {
        String[] st = arg.split("\\s+");
        if (st.length < 2) {
            reply(err("ESTO",arg));
            return;
        }

        String extended_store_mode = st[0];
        String cmd = "esto_" + extended_store_mode.toLowerCase();
        Object args[] = { arg };
        if (_methodDict.containsKey(cmd)) {
            Method m = _methodDict.get(cmd);
            try {
                info("FTP Door: esto invoking:" + m.getName() +
                     "(" + arg + ")");
                m.invoke(this, args);
            } catch (IllegalAccessException e) {
                reply("500 " + e.toString());
                _skipBytes = 0;
            } catch (InvocationTargetException e) {
                reply("500 " + e.toString());
                _skipBytes = 0;
            }
        } else {
            reply("504 ESTO is not implemented for store mode: "
                  + extended_store_mode);
        }
    }

    //
    // this is the implementation for the ESTO with mode "a"
    // "a" is ajusted store mode
    // other modes identified by string "MODE" can be implemented by adding
    // void method ac_esto_"MODE"(String arg)
    //
    public void ac_esto_a(String arg)
    {
        String[] st = arg.split("\\s+");
        if (st.length != 3) {
            reply(err("ESTO", arg));
            return;
        }
        String extended_store_mode = st[0];
        if (!extended_store_mode.equalsIgnoreCase("a")) {
            reply("504 ESTO is not implemented for store mode: "
                  + extended_store_mode);
            return;
        }
        String offset = st[1];
        String filename = st[2];
        long asm_offset;
        try {
            asm_offset = Long.parseLong(offset);
        } catch (NumberFormatException e) {
            String err = "501 ESTO Adjusted Store Mode: invalid offset " + offset;
            error("FTP Door: " + err);
            reply(err);
            return;
        }
        if (asm_offset != 0) {
            reply("504 ESTO Adjusted Store Mode does not work with nonzero offset: " + offset);
            return;
        }
        info("FTP Door: performing esto in \"a\" mode with offset = " + offset);
        ac_stor(filename);
    }

    //
    // this is the implementation for the ERET with mode "p"
    // "p" is partiall retrieve mode
    // other modes identified by string "MODE" can be implemented by adding
    // void method ac_eret_"MODE"(String arg)
    //
    public void ac_eret_p(String arg)
    {
        String[] st = arg.split("\\s+");
        if (st.length != 4) {
            reply(err("ERET",arg));
            return;
        }
        String extended_retrieve_mode = st[0];
        if (!extended_retrieve_mode.equalsIgnoreCase("p")) {
            reply("504 ERET is not implemented for retrieve mode: "+extended_retrieve_mode);
            return;
        }
        String offset = st[1];
        String size = st[2];
        String filename = st[3];
        try {
            prm_offset = Long.parseLong(offset);
        } catch (NumberFormatException e) {
            String err = "501 ERET Partial Retrieve Mode: invalid offset " + offset;
            error("FTP Door: " + err);
            reply(err);
            return;
        }
        try {
            prm_size = Long.parseLong(size);
        } catch (NumberFormatException e) {
            String err = "501 ERET Partial Retrieve Mode: invalid size " + offset;
            error("FTP Door: " + err);
            reply(err);
            return;
        }
        info("FTP Door: Performing eret in \"p\" mode " +
             "with offset = " + offset + " size = " + size);
        ac_retr(filename);
    }

    public void ac_retr(String arg)
    {
        String        clientHost = _client_data_host;
        int           clientPort = _client_data_port;
        Mode          mode       = _mode;
        try {
            if (_skipBytes > 0){
                reply("504 RESTART not implemented");
                return;
            }
            retrieve(arg, prm_offset, prm_size, mode,
                     _xferMode, _parallelStart, _parallelMin, _parallelMax,
                     new InetSocketAddress(clientHost, clientPort),
                     _bufSize, false, 1);
        } finally {
            prm_offset=-1;
            prm_size=-1;
        }
    }

    /**
     * Sends a message to a cell and waits for the reply. The reply is
     * expected to contain a message object of the same type as the
     * message object that was sent, and the return code of that
     * message is expected to be zero. If either is not the case,
     * Exception is thrown.
     *
     * @param  path    the path to the cell to which to send the message
     * @param  msg     the message object to send
     * @param  timeout timeout in milliseconds
     * @return         the message object from the reply
     * @throws TimeoutException If no reply was received in time
     * @throws SendAndWaitException If the message could not be sent,
     *       the object in the reply was of the wrong type, or the
     *       return code was non-zero.
     * @throws FileNotOnlineException If file is not online and STAGING is NOT allowed for this user
     */
    private <T extends Message> T sendAndWait(CellPath path, T msg, int timeout)
        throws TimeoutException, SendAndWaitException, InterruptedException, FileNotOnlineCacheException
    {
        CellMessage replyMessage = null;
        try {
            replyMessage = sendAndWait(new CellMessage(path, msg), timeout);
        } catch (NoRouteToCellException e) {
            String errmsg = "FTP Door: cannot send message to " + path +
                            ". Got error: " + e.getMessage();
            error(errmsg);
            throw new SendAndWaitException(errmsg, e);
        }

        if (replyMessage == null) {
            String errmsg = "FTP Door got timeout sending message to " + path;
            throw new TimeoutException(errmsg);
        }

        Object replyObject = replyMessage.getMessageObject();
        if (!(msg.getClass().isInstance(replyObject))) {
            String errmsg = "FTP Door got unexpected message of class " +
                            replyObject.getClass() + " from " +
                            replyMessage.getSourceAddress();
            error(errmsg);
            throw new SendAndWaitException(errmsg);
        }

        T reply = (T)replyObject;
        if (reply.getReturnCode() != 0) {
            if (reply.getReturnCode() == CacheException.FILE_NOT_ONLINE) {
               throw new FileNotOnlineCacheException(reply.getErrorObject().toString());
            } else {
              String errmsg = "FTP Door: got response from '" +
                            replyMessage.getSourceAddress() + "' with error " +
                            reply.getErrorObject();
              warn(errmsg);
              throw new SendAndWaitException(errmsg);
            }
        }

        return reply;
    }

    /**
     * Transfers a file from a pool to the client.
     *
     * @param file          the LFN of the file to transfer
     * @param offset        the position at which to begin the transfer
     * @param size          the number of bytes to transfer (whole
     *                      file when -1).
     * @param mode          indicates the direction of connection
     *                      establishment
     * @param xferMode      the transfer mode to use
     * @param parallelStart number of simultaneous streams to use
     * @param parallelMin   number of simultaneous streams to use
     * @param parallelMax   number of simultaneous streams to use
     * @param client        address of the client (for active servers)
     * @param bufSize       TCP buffers size to use (send and receive),
     *                      or auto scaling when -1.
     * @param reply127      GridFTP v2 127 reply is generated when true
     *                      and client is active.
     * @param version       The mover version to use for the transfer
     */
    private
        void retrieve(String file, long offset, long size,
                      Mode mode, String xferMode,
                      int parallelStart, int parallelMin, int parallelMax,
                      InetSocketAddress client, int bufSize,
                      boolean reply127, int version)
    {
        /* Close incomplete log.
         */
        if (_tLog != null) {
            _tLog.error("incomplete transaction");
            SetTLog(null);
        }

        _transfer = new Transfer(absolutePath(file));
        try {
            /* Check preconditions.
             */
            if (file.equals("")) {
                throw new FTPCommandException(501, "Missing path");
            }
            if (xferMode.equals("E") && mode == Mode.PASSIVE) {
                throw new FTPCommandException(500, "Cannot do passive retrieve in E mode");
            }
            if (xferMode.equals("X") && mode == Mode.PASSIVE && !_allowPassivePool) {
                throw new FTPCommandException(504, "Cannot use passive X mode");
            }
            if (_pwdRecord == null) {
                throw new FTPCommandException(530, "Not logged in.");
            }

            if ( _checkSumFactory != null || _checkSum != null ){
                throw new FTPCommandException(503,"Expecting STOR ESTO PUT commands");
            }

            /* Set ownership and other information for transfer.
             */
            _transfer.info.setOwner(_dnUser == null? _user : _dnUser);
            _transfer.info.setGid(_pwdRecord.GID);
            _transfer.info.setUid(_pwdRecord.UID);
            _transfer.client_host = client.getHostName();
            _transfer.client_port = client.getPort();

            /* ?
             */
            _curDirV = (_curDirV == null ? "/" : _curDirV);
            FsPath relativeToRootPath = new FsPath(_curDirV);
            relativeToRootPath.add(file);

            /* Check file permissions.
             */
            //PnfsId pnfsId = _pnfs.getPnfsIdByPath(_transfer.path);
            try {
                if (_permissionHandler.canReadFile(_transfer.path, getSubject(), _origin) != AccessType.ACCESS_ALLOWED) {
                    if(setNextPwdRecord()) {
                        retrieve(file, offset, size,
                                 mode, xferMode,
                                 parallelStart, parallelMin, parallelMax,
                                 client, bufSize, reply127, version);
                        return;
                    }
                    throw new FTPCommandException(550, "Permission denied");
                }
            }catch(ACLException e) {
                error("FTP Door: ACL module failed: " + e);
                throw new FTPCommandException(550, "Permission denied, reason (Acl)");
            } catch(NotFileCacheException ce ) {
                throw new FTPCommandException(501, "Not a file");
            }

            info("FTP Door: retrieve user=" + _user);
            info("FTP Door: retrieve vpath=" + relativeToRootPath);
            info("FTP Door: retrieve addr=" + _engine.getInetAddress().toString());

            //XXX When we upgrade to the GSSAPI version of GSI
            //we need to revisit this code and put something more useful
            //in the userprincipal spotpoolManager
            if (_tLogRoot != null) {
                SetTLog(new FTPTransactionLog(_tLogRoot, this));
                info("FTP Door will log ftp transactions to " + _tLogRoot);
            } else{
                info("FTP Door: tlog is not specified, door will not log FTP transactions");
            }
            startTlog(_transfer.path, "read");
            debug("FTP Door: tLog begin done");

            /* Retrieve storage information for file.
             */
            _transfer.state = "waiting for storage info";
            FileAttributes attributes =
                _pnfs.getFileAttributes(_transfer.path,
                                        EnumSet.of(PNFSID,
                                                   STORAGEINFO,
                                                   SIZE,
                                                   SIMPLE_TYPE),
                                        EnumSet.of(AccessMask.READ_DATA));
            _transfer.state = "received storage info";

            FileType type = attributes.getFileType();
            if (type == FileType.DIR || type == FileType.SPECIAL) {
                throw new FTPCommandException(550, "Not a regular file");
            }

            StorageInfo storageInfo = attributes.getStorageInfo();
            _transfer.info.setPnfsId(attributes.getPnfsId());
            _transfer.pnfsId = attributes.getPnfsId();

            /* Sanity check offset and size parameters. We cannot do
             * this earlier, because we need the size of the file
             * first.
             */
            long fileSize = attributes.getSize();
            if (offset == -1) {
                offset = 0;
            }
            if (size == -1) {
                size = fileSize;
            }
            if (offset < 0) {
                throw new FTPCommandException(500, "prm offset is "+offset);
            }
            if (size < 0) {
                throw new FTPCommandException(500, "500 prm_size is " + size);
            }

            if (offset + size > fileSize) {
                throw new FTPCommandException(500,
                                              "invalid prm_offset=" + offset
                                              + " and prm_size " + size
                                              + " for file of size " + fileSize);
            }

            /* Transfer the file. As there is a delay between the
             * point when a pool goes offline and when the pool
             * manager updates its state, we will retry failed
             * transfer a few times.
             */
            int retry = 0;
            _commandQueue.enableInterrupt();
            try {
                for (;;) {
                    try {
                        transfer(mode, xferMode,
                                 parallelStart, parallelMin, parallelMax,
                                 client, bufSize, offset, size,
                                 storageInfo, reply127, false, version);
                        break;
                    } catch (TimeoutException e){
                        error("FTP Door got timeout: while retrieving: " +
                              e.getMessage());
                    } catch (SendAndWaitException e){
                        error("FTP Door: retrieve got SendAndWaitException: "
                              + e.getMessage());
                    }
                    retry++;
                    if (retry >= _maxRetries) {
                        throw new FTPCommandException(425, "Cannot open port: No pools available", "No pools available");
                    }
                    Thread.sleep(_retryWait*1000);
                    info("FTP Door: retrieve retry attempt " + retry);
                }  //end of retry loop
            } finally {
                _commandQueue.disableInterrupt();
            }

            // no perf markers on retry (REVISIT: why not?)
            //if ( _perfMarkerConf.use && _xferMode.equals("E") ) {
            //     /** @todo: done ### ac_retr - breadcrumb - performance markers */
            //    _perfMarkerEngine = new PerfMarkerEngine( protocolInfo.getMax() ) ;
            //    _perfMarkerEngine.startEngine();
            //}
        } catch (PermissionDeniedCacheException e) {
            abortTransfer(550, "Permission denied");
        } catch (CacheException e) {
            switch (e.getRc()) {
            case CacheException.FILE_NOT_FOUND:
            case CacheException.DIR_NOT_EXISTS:
                abortTransfer(550, "File not found");
                break;
            case CacheException.TIMEOUT:
                abortTransfer(451, "Internal timeout", e);
                break;
            case CacheException.NOT_DIR:
                abortTransfer(550, "Not a directory");
                break;
            default:
                abortTransfer(451, "Operation failed: " + e.getMessage(), e);
                break;
            }
        } catch (FTPCommandException e) {
            abortTransfer(e.getCode(), e.getReply());
        } catch (InterruptedException e) {
            abortTransfer(451, "Operation cancelled");
        } catch (IOException e) {
            abortTransfer(451, "Operation failed: " + e.getMessage());
        } finally {
            _checkSumFactory = null;
            _checkSum = null;
            _allo = 0;
        }
    }

    private void setLength(PnfsId pnfsId, long length)
    {
        CellPath pnfsCellPath;
        CellMessage pnfsCellMessage;
        PnfsSetLengthMessage pnfsMessage;

        pnfsCellPath = new CellPath(_pnfsManager);
        pnfsMessage = new PnfsSetLengthMessage(pnfsId,length);
        pnfsCellMessage = new CellMessage(pnfsCellPath, pnfsMessage);

        debug("FTP Door: setLength setting length of file " + pnfsId +
              " to " + length);

        try {
            sendMessage(pnfsCellMessage);
        } catch (NoRouteToCellException e) {
            error("FTP Door: setLength cannot send message " + e.getMessage());
        }
    }

    public abstract void startTlog(String path,String action);

    public void ac_stor(String arg)
    {
        if (_client_data_host == null) {
            reply("504 Host somehow not set");
            return;
        }
        if (_skipBytes > 0) {
            reply("504 RESTART not implemented for STORE");
            return;
        }

        store(arg, _mode, _xferMode,
              _parallelStart, _parallelMin, _parallelMax,
              new InetSocketAddress(_client_data_host, _client_data_port),
              _bufSize, false, 1);
    }

    /**
     * Transfers a file from the client to a pool.
     *
     * @param file          the LFN of the file to transfer
     * @param mode          indicates the direction of connection
     *                      establishment
     * @param xferMode      the transfer mode to use
     * @param parallelStart number of simultaneous streams to use
     * @param parallelMin   number of simultaneous streams to use
     * @param parallelMax   number of simultaneous streams to use
     * @param client        address of the client (for active servers)
     * @param bufSize       TCP buffers size to use (send and receive),
     *                      or auto scaling when -1.
     * @param reply127      GridFTP v2 127 reply is generated when true
     *                      and client is active.
     * @param version       The mover version to use for the transfer
     */
    private void store(String file, Mode mode, String xferMode,
                       int parallelStart, int parallelMin, int parallelMax,
                       InetSocketAddress client, int bufSize,
                       boolean reply127, int version)
    {
        _transfer = new Transfer(absolutePath(file));
        try {
            if (_readOnly) {
                throw new FTPCommandException(502, "Command disabled");
            }
            if (file.equals("")) {
                throw new FTPCommandException(501, "STOR command not understood");
            }

            if (_pwdRecord == null) {
                throw new FTPCommandException(530, "Not logged in.");
            }

            if (_pwdRecord.isReadOnly()) {
                if(!setNextPwdRecord()) {
                    throw new FTPCommandException(500, "Command disabled");
                } else {
                    store(file, mode, xferMode,
                          parallelStart, parallelMin, parallelMax,
                          client, bufSize, reply127, version);
                    return;
                }
            }

            if (_pwdRecord.isAnonymous()) {
                if(!setNextPwdRecord()) {
                    throw new FTPCommandException(554, "Anonymous write access not permitted");
                } else {
                    store(file, mode, xferMode,
                          parallelStart, parallelMin, parallelMax,
                          client, bufSize, reply127, version);
                    return;
                }
            }

            /* Set ownership and other information for transfer.
             */
            _transfer.info.setOwner(_dnUser == null? _user : _dnUser);
            _transfer.info.setGid(_pwdRecord.GID);
            _transfer.info.setUid(_pwdRecord.UID);
            _transfer.client_host = client.getHostName();
            _transfer.client_port = client.getPort();

            if (xferMode.equals("E") && mode == Mode.ACTIVE) {
                throw new FTPCommandException(504, "Cannot store in active E mode");
            }
            if (xferMode.equals("X") && mode == Mode.PASSIVE && !_allowPassivePool) {
                throw new FTPCommandException(504, "Cannot use passive X mode");
            }

            info("FTP Door: store receiving with mode " + xferMode);

            if (_tLogRoot != null) {
                SetTLog(new FTPTransactionLog(_tLogRoot, this));
                debug("FTP Door: store will log ftp transactions to " + _tLogRoot);
            } else {
                info("FTP Door: tlog is not specified. Store will not log FTP transactions");
            }

            // for monitoring
            _transfer.state = "waiting for storage info";

            info("FTP Door: store user=" + _user);
            info("FTP Door: store path=" + _transfer.path);
            info("FTP Door: store addr=" + _engine.getInetAddress().toString());
            //XXX When we upgrade to the GSSAPI version of GSI
            //we need to revisit this code and put something more useful
            //in the userprincipal spot
            startTlog(_transfer.path, "write");
            debug("FTP Door: store: _tLog begin done");

            /* Check if the user has permission to create the file.
             */
            _transfer.state = "checking permissions via permission handler";
            info("FTP Door: store: checking permissions via permission " +
                 "handler for path: " + _transfer.path);
            String parent = new File(_transfer.path).getParent();
            PnfsId parentId = _pnfs.getPnfsIdByPath(parent);
            if (_permissionHandler.canCreateFile(parentId, getSubject(), _origin) != AccessType.ACCESS_ALLOWED) {
                if(!setNextPwdRecord()) {
                    throw new FTPCommandException
                        (550,
                         "Permission denied",
                         "Permission denied for path: " + _transfer.path);
                } else {
                    store(file, mode, xferMode,
                          parallelStart, parallelMin, parallelMax,
                          client, bufSize, reply127, version);
                    return;
                }
            }

            /* Create PNFS entry.
             */
            _transfer.state = "creating pnfs entry";
            StorageInfo storageInfo;
            PnfsGetFileAttributes pnfsEntry;

            /* FIXME: There is a race condition here. In case we break
             * out, maybe due to a thread interrupt, or due to some
             * other failure, while we wait for the reply from the
             * PnfsManager, then we do not know whether the entry was
             * actually created at that point - and if the entry
             * exists, we cannot know whether we created it or whether
             * it already existed. This means we cannot reliably clean
             * up after ourself and thus we risk leaving empty files
             * behind, even though the transfer failed.
             */
            try {
                pnfsEntry = _pnfs.createPnfsEntry(_transfer.path,
                                                  _pwdRecord.UID,
                                                  _pwdRecord.GID,
                                                  0644);
            } catch (FileExistsCacheException fnfe) {
                if(_overwrite) {
                    warn("FTP Door: Overwrite is enabled. File \"" +
                         _transfer.path + "\" exists, and will be overwritten");
                    _pnfs.deletePnfsEntry( _transfer.path);
                    pnfsEntry = _pnfs.createPnfsEntry(_transfer.path,
                                                      _pwdRecord.UID,
                                                      _pwdRecord.GID,
                                                      0644);
                } else {
                    throw new FTPCommandException(553,
                                                  _transfer.path
                                                  + ": Cannot create file: "
                                                  + fnfe.getMessage());
                }
            } catch (CacheException ce) {
                // Unfortunately if file exists the regular file
                // exeption is still being thown. FIXME: we have to
                // distinguish between transient and permanent errors!
                if(_overwrite) {
                    warn("FTP Door: Overwrite is enabled. File \"" +
                         _transfer.path + "\" exists, and will be overwritten");
                    _pnfs.deletePnfsEntry(_transfer.path);
                    pnfsEntry = _pnfs.createPnfsEntry(_transfer.path,
                                                      _pwdRecord.UID,
                                                      _pwdRecord.GID,
                                                      0644);
                } else {
                    throw new FTPCommandException(553,
                                                  _transfer.path
                                                  + ": Cannot create file: "
                                                  + ce.getMessage());
                }
            }

            /* The PNFS entry has been created, but the file transfer
             * has not yet successfully completed. Setting
             * pnfsEntryIncomplete to true ensures that the entry will
             * be deleted if anything goes wrong.
             */
            _transfer.pnfsEntryIncomplete = true;
            _transfer.pnfsId = pnfsEntry.getPnfsId();
            _transfer.info.setPnfsId(_transfer.pnfsId);
            info("FTP Door: store created new pnfs entry: " +
                 _transfer.pnfsId);

            debug("FTP Door: store getting related StorageInfo");
            storageInfo = pnfsEntry.getFileAttributes().getStorageInfo();
            if (storageInfo == null) {
                throw new FTPCommandException
                    (533,
                     "Couldn't get StorageInfo for : " + _transfer.pnfsId);
            }
            info("FTP Door: store got storageInfo : " + storageInfo);

            /* Send checksum to PNFS manager.
             */
            if (_checkSum != null) {
                _transfer.state = "setting checksum in pnfs";

                try{
                    ChecksumPersistence.getPersistenceMgr().store(this, _transfer.pnfsId, _checkSum);
                } catch (Exception e) {
                    throw new FTPCommandException(451,
                                                  "Failed to store checksum: "
                                                  + e.getMessage());
                }
            }

            _commandQueue.enableInterrupt();
            try {
                transfer(mode, xferMode, parallelStart,
                         parallelMin, parallelMax,
                         client, bufSize, 0, 0, storageInfo,
                         reply127, true, version);
            } finally {
                _commandQueue.disableInterrupt();
            }

            if (_perfMarkerConf.use && xferMode.equals("E")) {
                /** @todo: done ### ac_stor - breadcrumb - performance markers
                 */
                _transfer.perfMarkerTask =
                    new PerfMarkerTask(_transfer.pool,
                                       _transfer.moverId,
                                       _perfMarkerConf.period / 2);
                _timer.schedule(_transfer.perfMarkerTask,
                                _perfMarkerConf.period,
                                _perfMarkerConf.period);
            }
        } catch (ACLException e) {
            abortTransfer(550, "Permission denied, ACL failure", e);
        } catch (FTPCommandException e) {
            abortTransfer(e.getCode(), e.getReply());
        } catch (InterruptedException e) {
            abortTransfer(451, "Operation cancelled");
        } catch (IOException e) {
            abortTransfer(451, "Operation failed: " + e.getMessage());
        } catch (PermissionDeniedCacheException e) {
            abortTransfer(550, "Permission denied");
        } catch (CacheException e) {
            switch (e.getRc()) {
            case CacheException.FILE_NOT_FOUND:
                abortTransfer(550, "File not found");
                break;
            case CacheException.DIR_NOT_EXISTS:
                abortTransfer(550, "Directory not found");
                break;
            case CacheException.FILE_EXISTS:
                abortTransfer(550, "File exists");
                break;
            case CacheException.TIMEOUT:
                abortTransfer(451, "Internal timeout", e);
                break;
            case CacheException.NOT_DIR:
                abortTransfer(501, "Not a directory");
                break;
            default:
                abortTransfer(451, "Operation failed: " + e.getMessage(), e);
                break;
            }
        } catch (TimeoutException e) {
            abortTransfer(451, "Internal timeout", e);
        } catch (SendAndWaitException e) {
            abortTransfer(451, "Operation failed: " + e.getMessage(), e);
        } finally {
            _checkSumFactory = null;
            _checkSum = null;
            _allo = 0;
        }
    }

    /**
     * Selects a pool and initiates a transfer between the client and
     * the pool.
     *
     * Unless space reservation is used, a pool is selected by
     * contacting the PoolMananger.
     *
     * Once the pool is known, the pool is asked to initiate a
     * transfer. If we can make the pool passive or if mode X is used,
     * then GFtp/2 is used to talk to the pool. Otherwise GFtp/1 is
     * used. If GFtp/2 is used, this method will block and await a
     * GFtpTransferStartedMessage from the pool.
     *
     * If an adapter (proxy) has to be used, the _transfer.adapter
     * field will become non-null. The pool may decline to be passive
     * and force the door to use an adapter.
     *
     * The method is synchronized to ensure that it completes before
     * any DoorTransferFinishedMessage is received from the pool (see
     * @link messageArrived).
     *
     * If replu127 is true, an FTP 127 response will be
     * generated. This is only valid if mode is set to PASSIVE.
     *
     * TODO: We can simplify the parameter list by including more of
     * this information into the _transfer object.
     *
     * @param mode          indicates the direction of connection
     *                      establishment
     * @param xferMode      the transfer mode to use
     * @param parallelStart number of simultaneous streams to use
     * @param parallelMin   number of simultaneous streams to use
     * @param parallelMax   number of simultaneous streams to use
     * @param client        address of the client.
     * @param bufSize       TCP buffers size to use (send and receive),
     * @param offset        the position at which to begin the transfer
     * @param size          the number of bytes to transfer (whole
     *                      file when -1).
     * @param storageInfo   Storage information about the file to transfer
     * @param reply127      GridFTP v2 127 reply is generated when true.
     *                      Requires mode to be PASSIVE.
     * @param isWrite       True writing to pool, false when reading.
     * @param version       The mover version to use for the transfer
     * @throws FileNotOnlineCacheException If file is not online and STAGING is NOT allowed for this user
     */
    private synchronized void transfer(Mode              mode,
                                       String            xferMode,
                                       int               parallelStart,
                                       int               parallelMin,
                                       int               parallelMax,
                                       InetSocketAddress client,
                                       int               bufSize,
                                       long              offset,
                                       long              size,
                                       StorageInfo       storageInfo,
                                       boolean           reply127,
                                       boolean           isWrite,
                                       int               version)
        throws InterruptedException,
               TimeoutException,
               SendAndWaitException,
               IOException,
               FTPCommandException,
               FileNotOnlineCacheException
    {
        /* reply127 implies passive mode.
         */
        assert !reply127 || mode == Mode.PASSIVE;

        /* We can only let the pool be passive if this has been
         * enabled and if we can provide the address to the client
         * using a 127 response.
         */
        boolean usePassivePool = _allowPassivePool && reply127;

        /*
         *
         */


        /* Set up an adapter, if needed. Since a pool may reject to be
         * passive, we need to set up an adapter even when we can use
         * passive pools.
         */
        switch (mode) {
        case PASSIVE:
            if (reply127) {
                info("FTP Door: transfer creating adapter for passive mode");
                _transfer.adapter =
                    new SocketAdapter(this, _lowDataListenPort, _highDataListenPort);
            } else {
                _transfer.adapter = _adapter;
            }
            break;

        case ACTIVE:
            if (_allowRelay) {
                info("FTP Door: transfer creating adapter for active mode");
                _transfer.adapter =
                    new ActiveAdapter(this, _lowDataListenPort , _highDataListenPort);
                ((ActiveAdapter)_transfer.adapter).setDestination(client.getAddress().getHostAddress(), client.getPort());
            }
            break;
        }

        if (_transfer.adapter != null) {
            _transfer.adapter.setMaxBlockSize(_maxBlockSize);
            _transfer.adapter.setModeE(xferMode.equals("E"));
            _transfer.adapter.setMaxStreams(_maxStreamsPerClient);
            if (isWrite) {
                _transfer.adapter.setDirClientToPool();
            } else {
                _transfer.adapter.setDirPoolToClient();
            }
        }

        // Find a pool suitable for the transfer.

        _transfer.state = "waiting for pool selection by PoolManager";
        VOInfo voInfo = null;
        if(_pwdRecord != null ) {
            String group = _pwdRecord.getGroup();
            String role = _pwdRecord.getRole();
            if(group != null && role != null) {
                voInfo =new VOInfo(group, role);
            }
        }
        GFtpProtocolInfo protocolInfo =
            new GFtpProtocolInfo("GFtp",
                                 version,
                                 0,
                                 client.getAddress().getHostAddress(),
                                 client.getPort(),
                                 parallelStart,
                                 parallelMin,
                                 parallelMax,
                                 bufSize, 0, 0,
                                 voInfo);

        PoolMgrSelectPoolMsg request;
        if (isWrite) {
            request = new PoolMgrSelectWritePoolMsg(_transfer.pnfsId,
                                                    storageInfo,
                                                    protocolInfo,
                                                    _allo);
        } else {   //transfer: 'retrieve'
            Subject subject = getSubject();
            int allowedStates =
                _checkStagePermission.canPerformStaging(subject)
                ? RequestContainerV5.allStates
                : RequestContainerV5.allStatesExceptStage;
            request = new PoolMgrSelectReadPoolMsg(_transfer.pnfsId,
                                                   storageInfo,
                                                   protocolInfo,
                                                   0L,
                                                   allowedStates);
            request.setSubject(subject);
        }
        request.setPnfsPath(_transfer.path);
        request = sendAndWait(new CellPath(_poolManager), request,
                              _poolManagerTimeout * 1000);

        // use the updated StorageInfo from the PoolManager/SpaceManager
        storageInfo = request.getStorageInfo();

        _transfer.pool = request.getPoolName();

        /* Construct protocol info. For backward compatibility, when
         * an adapter could be used we put the adapter address into
         * the protocol info (this behaviour is consistent with dCache
         * 1.7).
         */
        voInfo = null;
        if(_pwdRecord != null ) {
            String group = _pwdRecord.getGroup();
            String role = _pwdRecord.getRole();
            if(group != null && role != null) {
                voInfo =new VOInfo(group, role);
            }
        }
        if (_transfer.adapter != null) {
            protocolInfo =
                new GFtpProtocolInfo("GFtp",
                                      version, 0,
                                     _local_host,
                                     _transfer.adapter.getPoolListenerPort(),
                                     parallelStart,
                                     parallelMin,
                                     parallelMax,
                                     bufSize,
                                     offset,
                                     size,
                                     voInfo);
        } else {
            protocolInfo =
                new GFtpProtocolInfo("GFtp",
                                     version, 0,
                                     client.getAddress().getHostAddress(),
                                     client.getPort(),
                                     parallelStart,
                                     parallelMin,
                                     parallelMax,
                                     bufSize,
                                     offset,
                                     size,
                                     voInfo);
        }

        protocolInfo.setDoorCellName(getCellName());
        protocolInfo.setDoorCellDomainName(getCellDomainName());
        protocolInfo.setClientAddress(client.getAddress().getHostAddress());
        protocolInfo.setPassive(usePassivePool);
        protocolInfo.setMode(xferMode);

        if ( _optCheckSumFactory != null )
            protocolInfo.setChecksumType(_optCheckSumFactory.getType());

        if ( _checkSumFactory != null )
            protocolInfo.setChecksumType(_checkSumFactory.getType());

        /* Ask the pool to transfer the file.
         */
        askForFile(_transfer, storageInfo, protocolInfo, isWrite);

        /* When version 2 is used, then block until we got a 'transfer
         * started' message.
         */
        GFtpTransferStartedMessage message = null;
        if (version == 2) {
            do {
                message = _transferStartedMessages.take();
            } while (!message.getPnfsId().equals(_transfer.pnfsId.getId()));

            if (message.getPassive() && !reply127) {
                reportBug("transfer",
                          "internal error: pool unexpectedly volunteered to " +
                          "be passive", null);
            }

            /* If passive X mode was requested, but the pool rejected
             * it, then we have to fail for now. REVISIT: We should
             * use the other adapter in this case.
             */
            if (mode == Mode.PASSIVE && !message.getPassive() && xferMode.equals("X")) {
                throw new FTPCommandException(504, "Cannot use passive X mode");
            }
        }

        /* Determine the 127 response address to send back to the
         * client. When the pool is passive, this is the address of
         * the pool (and in this case we no longer need the
         * adapter). Otherwise this is the address of the adapter.
         */
        if (message != null && message.getPassive()) {
            assert reply127;

            reply127PORT(message.getPoolAddress());

            info("FTP Door: transfer closing adapter");
            _transfer.adapter.close();
            _transfer.adapter = null;
        } else if (reply127) {
            reply127PORT(new InetSocketAddress(_engine.getLocalAddress(),
                                               _transfer.adapter.getClientListenerPort()));
        }

        /* If an adapter is used, then start it now.
         */
        if (_transfer.adapter != null) {
            _transfer.adapter.start();
        }

        reply("150 Opening BINARY data connection for " + _transfer.path, false);
    }

    /**
     * Returns the number of milliseconds until a specified point in
     * time. May be negative if the point in time is in the past.
     *
     * TODO: Move to a utility class.
     *
     * @param time a point in time measured in milliseconds since
     *             midnight, January 1, 1970 UTC.
     * @return the difference, measured in milliseconds, between
     *         <code>time</code> and midnight, January 1, 1970 UTC.
     */
    private long timeUntil(long time)
    {
        return time - System.currentTimeMillis();
    }

    private void abortTransfer(int replyCode, String msg)
    {
        abortTransfer(replyCode, msg, null);
    }

    /**
     * Aborts a transfer and performs all necessary cleanup steps,
     * including killing movers and removing incomplete files. A
     * failure message is send to the client. Both the reply code and
     * reply message are logged as errors.
     *
     * If an exception is specified, then the error message in the
     * exception is logged too and the exception itself is logged at a
     * debug level. The intention is that an exception is only
     * specified for exceptional cases, i.e. errors we would not
     * expect to appear in normal use (potential bugs). Communication
     * errors and the like should not be logged with an exception.
     *
     * In case the caller knows that an exception is certainly a bug,
     * <code>reportBug</code> should be called. That method logs the
     * exception as fatal, meaning it will always be added to the
     * log. In most cases <code>reportBug</code> should be called
     * instead of <code>abortTransfer</code>, since the former will
     * throw a <code>RuntimeException</code>, which in turn causes
     * <code>abortTransfer</code> to be called.
     *
     * @param replyCode reply code to send the the client
     * @param replyMsg error message to send back to the client
     * @param exception exception to log or null
     */
    private synchronized void abortTransfer(int replyCode, String replyMsg,
                                             Exception exception)
    {
        if (_transfer != null) {
            _transfer.aborted = true;

            if (_transfer.perfMarkerTask != null) {
                _transfer.perfMarkerTask.stop();
            }

            if (_transfer.adapter != null && _transfer.adapter != _adapter) {
                _transfer.adapter.close();
            }

            if (_transfer.moverId != null) {
                assert _transfer.pool != null;

                warn("FTP Door: Transfer error. Sending kill to pool " +
                     _transfer.pool + " for mover " + _transfer.moverId);
                try {
                    PoolMoverKillMessage message =
                        new PoolMoverKillMessage(_transfer.pool,
                                                 _transfer.moverId);
                    message.setReplyRequired(false);
                    sendMessage(new CellMessage(new CellPath(_transfer.pool),
                                                message));

                    /* Since the mover doesn't register the file in
                     * the companion until the transfer is completed,
                     * we wait for some time to avoid orphaned
                     * files. This is an imperfect hack, as there is
                     * no upper bound on how long it could take to
                     * kill the mover.
                     */
                    long timeToWait = _sleepAfterMoverKill * 1000;
                    long deadline = System.currentTimeMillis() + timeToWait;
                    while (_transfer.moverId != null && timeToWait > 0) {
                        wait(timeToWait);
                        timeToWait = timeUntil(deadline);
                    }
                } catch (InterruptedException e) {
                    /* Bugger, something decided that we are in a
                     * hurry (most likely domain shutdown). We are
                     * shutting down anyway, so continue doing that...
                     */
                } catch (NoRouteToCellException e) {
                    error("FTP Door: Transfer error. Can't send message to " +
                          _transfer.pool + ": " + e.getMessage());
                }
            }

            if (_transfer.pnfsEntryIncomplete) {
                if (_removeFileOnIncompleteTransfer) {
                    warn("FTP Door: Transfer error. Removing incomplete file "
                         + _transfer.pnfsId + ": " + _transfer.path);
                    try {
                        _pnfs.deletePnfsEntry(_transfer.pnfsId);
                    } catch (CacheException e) {
                        error("FTP Door: Failed to delete " + _transfer.pnfsId
                              + ": " + e.getMessage());
                    }
                } else {
                    warn("FTP Door: Transfer error. Incomplete file was not removed:"
                         + _transfer.path);
                }
            }

            /* Report errors.
             */
            String msg = String.valueOf(replyCode) + " " + replyMsg;
            _transfer.sendDoorRequestInfo(replyCode, replyMsg);
            reply(msg);
            if (_tLog != null) {
                _tLog.error(msg);
            }
            if (exception == null) {
                error("FTP Door: Transfer error: " + msg);
            } else {
                error("FTP Door: Transfer error: " + msg
                      + " (" + exception.getMessage() + ")");
                debug(exception);
            }
            _transfer = null;
            notifyAll();
        }
    }

    public void ac_size(String arg)
    {
        if (arg.equals("")) {
            reply(err("SIZE",""));
            return;
        }

        if (_pwdRecord == null) {
            reply("530 Not logged in.");
            return;
        }

        String path = absolutePath(arg);
        long filelength = 0;
        try {
            FileAttributes attributes =
                _pnfs.getFileAttributes(path, EnumSet.of(PNFSID, SIZE));
            PnfsId pnfsId = attributes.getPnfsId();
            if (_permissionHandler.canGetAttributes(pnfsId, getSubject(), _origin, FileAttribute.FATTR4_SIZE) != AccessType.ACCESS_ALLOWED) {
                if(!setNextPwdRecord()) {
                    reply("553 Permission denied");
                } else {
                    ac_size(arg);
                }
                return;
            }
            filelength = attributes.getSize();
        } catch(ACLException e) {
            reply("550 Permission denied, reason (Acl) ");
            error("FTP Door: ACL module failed: " + e);
            return;
        } catch (PermissionDeniedCacheException e) {
            reply("550 Permission denied");
            return;
        } catch (CacheException ce) {
            reply("550 Permission denied, reason: " + ce);
            return;
        }
        reply("213 " + filelength);
    }

    public void ac_mdtm(String arg)
    {
        if (arg.equals("")) {
            reply(err("MDTM",""));
            return;
        }

        if (_pwdRecord == null) {
            reply("530 Not logged in.");
            return;
        }

        try {
            String path = absolutePath(arg);
            if (path == null) {
                reply("550 File not found");
                return;
            }

            long modification_time;
            FileAttributes attributes =
                _pnfs.getFileAttributes(path,
                                        EnumSet.of(PNFSID, MODIFICATION_TIME));
            PnfsId pnfsId = attributes.getPnfsId();
            if (_permissionHandler.canGetAttributes(pnfsId, getSubject(), _origin, FileAttribute.FATTR4_SUPPORTED_ATTRS) != AccessType.ACCESS_ALLOWED) {
                if(!setNextPwdRecord()) {
                    reply("550 Permission denied");
                } else {
                    ac_mdtm(arg);
                }
                return;
            }

            modification_time = attributes.getModificationTime();
            String time_val =
                TIMESTAMP_FORMAT.format(new Date(modification_time));
            reply("213 " + time_val);
        } catch(ACLException e) {
            reply("451 Internal permission check failure: " +
                  e.getMessage());
            error("Error in MDTM: " + e);
        } catch (PermissionDeniedCacheException e) {
            reply("550 Permission denied");
        } catch (CacheException e) {
            switch (e.getRc()) {
            case CacheException.FILE_NOT_FOUND:
                reply("550 File not found");
                break;
            case CacheException.TIMEOUT:
                reply("451 Internal timeout");
                warn("Timeout in MDTM: " + e);
                break;
            default:
                reply("451 Internal failure: " + e.getMessage());
                error("Error in MDTM: " + e);
            }
        }
    }

    private void openDataSocket()
        throws IOException
    {
        /* Mode being PASSIVE means the client did a PASV.  Otherwise
         * we establish the data connection to the client.
         */
        if (_mode == Mode.PASSIVE) {
            _dataSocket = _adapter.acceptOnClientListener();
        } else {
            _dataSocket = new Socket(_client_data_host, _client_data_port);
        }
    }

    private void closeDataSocket()
    {
        Socket socket = _dataSocket;
        if (socket != null) {
            try {
                socket.close();
            } catch (IOException e) {
                warn("FTP Door: got I/O exception closing socket: " +
                     e.getMessage());
            }
            _dataSocket = null;
        }
    }

    public void ac_list(String arg)
    {
        Args args = new Args(arg);
        boolean listLong =
            args.options().isEmpty() || (args.getOpt("l") != null);
        if (args.argc() == 0) {
            arg = "";
        } else {
            arg = args.argv(0);
        }

        String absolutepath = absolutePath(arg);
        if (absolutepath == null) {
            FsPath relativeToRootPath = new FsPath(_curDirV);
            relativeToRootPath.add(arg);
            reply("550 " + relativeToRootPath + " not found.");
            return;
        }
        File f = new File(absolutepath);

        try {

            reply("150 Opening ASCII data connection for file list", false);
            try {
                openDataSocket();
            } catch (IOException e) {
                reply("425 Cannot open connection");
                return;
            }

            int total;
            try {
                PrintWriter writer =
                    new PrintWriter(new OutputStreamWriter(new BufferedOutputStream(_dataSocket.getOutputStream()), "US-ASCII"));

                DirectoryListPrinter printer =
                    listLong
                    ? new LongListPrinter(writer)
                    : new ShortListPrinter(writer);

                try {
                    total = _listSource.printDirectory(null, printer, f, null, null);
                } catch (NotDirCacheException e) {
                    /* f exists, but it is not a directory.
                     */
                    _listSource.printFile(null, printer, f);
                    total = 1;
                } catch (FileNotFoundCacheException e) {
                    /* If f does not exist, then it could be a
                     * pattern; we move up one directory level and
                     * repeat the list.
                     */
                    total =
                        _listSource.printDirectory(null, printer, f.getParentFile(),
                                                   new Glob(f.getName()), null);
                }

                writer.close();
            } finally {
                closeDataSocket();
            }
            reply("226 " + total + " files");
        } catch (InterruptedException e) {
            reply("451 Operation cancelled");
        } catch (FileNotFoundCacheException e) {
            reply("550 File not found");
        } catch (NotDirCacheException e) {
            reply("550 Not a directory");
        } catch (PermissionDeniedCacheException e) {
            reply("550 Permission denied");
        } catch (CacheException e){
            reply("451 Local error in processing");
            warn("Error in LIST: " + e.getMessage());
        } catch (EOFException e) {
            reply("426 Connection closed; transfer aborted");
        } catch (IOException e) {
            reply("451 Local error in processing");
            warn("Error in LIST: " + e.getMessage());
        }
    }

    public void ac_nlst(String arg)
    {
        if (arg.equals("")) {
            arg = ".";
        }

        try {
            /* 550 is now a valid reply for NLST. However other FTP
             * servers use this return code for NLST. Gerd and Timur
             * decided to follow their example and violate the spec.
             */
            String path = absolutePath(arg);
            if (path == null) {
                reply("550 Access denied");
                return;
            }

            reply("150 Opening ASCII data connection for file list", false);
            try {
                openDataSocket();
            } catch (IOException e) {
                reply("425 Cannot open connection");
                return;
            }

            int total;
            try {
                PrintWriter writer =
                    new PrintWriter(new OutputStreamWriter(new BufferedOutputStream(_dataSocket.getOutputStream()), "US-ASCII"));

                total = _listSource.printDirectory(null, new ShortListPrinter(writer),
                                                   new File(path), null, null);
                writer.close();
            } finally {
                closeDataSocket();
            }
            reply("226 " + total + " files");
        } catch (InterruptedException e) {
            reply("451 Operation cancelled");
        } catch (FileNotFoundCacheException e) {
            reply("550 Directory not found");
        } catch (NotDirCacheException e) {
            reply("550 Directory not found");
        } catch (PermissionDeniedCacheException e) {
            reply("550 Permission denied");
        } catch (CacheException e) {
            reply("451 Local error in processing");
            warn("Error in NLST: " + e.getMessage());
        } catch (EOFException e) {
            reply("426 Connection closed; transfer aborted");
        } catch (IOException e) {
            reply("451 Local error in processing");
            warn("Error in NLST: " + e.getMessage());
        }
    }

    public void ac_mlst(String arg)
    {
        if (_pwdRecord == null) {
            reply("530 Not logged in.");
            return;
        }

        String path;
        if (arg.length() == 0) {
            path = absolutePath(".");
        } else {
            path = absolutePath(arg);
        }

        if (path == null) {
            reply("550 Access denied");
            return;
        }

        FsPath tvfsPath = new FsPath(_curDirV == null ? "/" : _curDirV);
        tvfsPath.add(arg);

        try {
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            File f = new File(path);
            pw.println("250- Listing " + arg + "\r\n");
            pw.print(' ');
            _listSource.printFile(null, new FactPrinter(pw), f);
            pw.print("250 End");
            reply(sw.toString());
        } catch (InterruptedException e) {
            reply("451 Operation cancelled");
        } catch (PermissionDeniedCacheException e) {
            reply("550 Permission denied");
        } catch (CacheException e) {
            reply("451 Local error in processing");
            warn("Error in MLST: " + e.getMessage());
        }
    }

    public void ac_mlsd(String arg)
    {
        if (_pwdRecord == null) {
            reply("530 Not logged in.");
            return;
        }

        try {
            String path;
            if (arg.length() == 0) {
                path = absolutePath(".") + "/";
            } else {
                path = absolutePath(arg) + "/";
            }

            if (path == null) {
                reply("501 Directory not found");
                return;
            }

            reply("150 Openening ASCII mode data connection for MLSD", false);
            try {
                openDataSocket();
            } catch (IOException e) {
                reply("425 Cannot open connection");
                return;
            }

            int total;
            try {
                PrintWriter writer =
                    new PrintWriter(new OutputStreamWriter(new BufferedOutputStream(_dataSocket.getOutputStream()), "UTF-8"));

                total = _listSource.printDirectory(null, new FactPrinter(writer),
                                                   new File(path), null, null);
                writer.close();
            } finally {
                closeDataSocket();
            }
            reply("226 MLSD completed for " + total + " files");
        } catch (InterruptedException e) {
            reply("451 Operation cancelled");
        } catch (FileNotFoundCacheException e) {
            reply("501 Directory not found");
        } catch (NotDirCacheException e) {
            reply("501 Directory not found");
        } catch (PermissionDeniedCacheException e) {
            reply("550 Permission denied");
        } catch (CacheException e) {
            reply("451 Local error in processing");
            warn("Error in MLSD: " + e.getMessage());
        } catch (EOFException e) {
            reply("426 Connection closed; transfer aborted");
        } catch (IOException e) {
            reply("451 Local error in processing");
            warn("Error in MLSD: " + e.getMessage());
        }
    }

    //----------------------------------------------
    // DCAU: data channel authtication
    // currentrly ( 07.04.2008 ) it's not supported
    //----------------------------------------------
    public void ac_dcau(String arg)
    {

        if(arg.equalsIgnoreCase("N")) {
            reply("200 data channel authtication switched off");
        }else{
            reply("202 data channel authtication not sopported");
        }

    }

    // ---------------------------------------------
    // QUIT: close command channel.
    // If transfer is in progress, wait for it to finish, so set pending_quit state.
    //      The delayed QUIT has not been directly implemented yet, instead...
    // Equivalent: let the data channel and pnfs entry clean-up code take care of clean-up.
    // ---------------------------------------------
    public synchronized void ac_quit(String arg)
        throws CommandExitException
    {
        reply("221 Goodbye");

        /* From RFC 959:
         *
         *    "This command terminates a USER and if file transfer is
         *     not in progress, the server closes the control
         *     connection.  If file transfer is in progress, the
         *     connection will remain open for result response and the
         *     server will then close it."
         *
         * In other words, we are supposed to wait until ongoing
         * transfers have completed.
         */
        try {
            _commandQueue.enableInterrupt();
            while (_transfer != null) {
                wait();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            _commandQueue.disableInterrupt();
        }

        throw new CommandExitException("", 0);
    }

    // --------------------------------------------
    // BYE: synonym for QUIT
    // ---------------------------------------------
    public void ac_bye( String arg ) throws CommandExitException
    {
        ac_quit(arg);
    }

    // --------------------------------------------
    // ABOR: close data channels, but leave command channel open
    // ---------------------------------------------
    public void ac_abor(String arg)
    {
        abortTransfer(426, "Transfer aborted");
        closeDataSocket();
        reply("226 Abort successful");
    }

    // --------------------------------------------
    public String err(String cmd, String arg)
    {
        String msg = "500 '" + cmd;
        if (arg.length() > 0)
            msg = msg + " " + arg;
        msg = msg + "': command not understood";
        return msg;
    }

    public String ok(String cmd)
    {
        return "200 "+cmd+" command successful";
    }

    /**
     * Asks a pool to perform a transfer.
     *
     * Information about which pool to ask and the file to ask for is
     * taken from the <tt>transfer</tt> parameter. On success, the
     * moverId field of <tt>transfer</tt> will be filled in. On
     * failure, Exception is thrown.
     *
     * @param transfer     The transfer to perform.
     * @param storageInfo  Storage information of the file to transfer
     * @param protocolInfo Protocol information for the transfer
     * @param isWrite      True when transfer is to the pool, otherwise false
     * @throws FileNotOnlineCacheException If file is not online and STAGING is NOT allowed for this user
     */
    private void askForFile(Transfer     transfer,
                            StorageInfo  storageInfo,
                            ProtocolInfo protocolInfo,
                            boolean      isWrite)
        throws SendAndWaitException, TimeoutException, InterruptedException, FileNotOnlineCacheException
    {
        info("FTP Door: trying pool " + transfer.pool +
             " for " + (isWrite ? "write" : "read"));
        transfer.state = "sending " + (isWrite ? "write" : "read")
            + " request to pool";

        PoolIoFileMessage poolMessage;
        if (isWrite) {
            poolMessage = new PoolAcceptFileMessage(transfer.pool,
                                                    transfer.pnfsId,
                                                    protocolInfo,
                                                    storageInfo);
        } else {
            poolMessage = new PoolDeliverFileMessage(transfer.pool,
                                                     transfer.pnfsId,
                                                     protocolInfo,
                                                     storageInfo);
        }

        if (_ioQueueName != null) {
            poolMessage.setIoQueueName(_ioQueueName);
        }

        poolMessage.setId(transfer.sessionId);
        // let the pool know which request triggered the transfer
        if (transfer.info != null) {
            poolMessage.setInitiator(transfer.info.getTransaction());
        }

        CellPath toPool = null;
        if (_poolProxy == null) {
            toPool = new CellPath(transfer.pool);
        } else {
            toPool = new CellPath(_poolProxy);
            toPool.add(transfer.pool);
        }

        PoolIoFileMessage poolReply =
            sendAndWait(toPool, poolMessage, _poolTimeout * 1000);
        transfer.moverId = poolReply.getMoverId();

        info("FTP Door: mover " + transfer.moverId + " at pool " +
             transfer.pool + " will " + (isWrite ? "receive" : "send") +
            " file " + transfer.pnfsId);
        _transfer.state = "mover " + transfer.moverId
            + (isWrite ? ": receiving" : ": sending");
    }

    abstract protected boolean setNextPwdRecord();

    abstract protected void resetPwdRecord();

    private class PerfMarkerTask
        extends TimerTask implements CellMessageAnswerable
    {
        private final GFtpPerfMarkersBlock _perfMarkersBlock
            = new GFtpPerfMarkersBlock(1);
        private final long _timeout;
        private final String _pool;
        private final int _moverId;
        private final CDC _cdc;
        private boolean _stopped = false;

        public PerfMarkerTask(String pool, int moverId, long timeout)
        {
            _pool = pool;
            _moverId = moverId;
            _timeout = timeout;
            _cdc = new CDC();

            /* For the first time, send markers with zero counts -
             * requirement of the standard
             */
            sendMarker();
        }

        /**
         * Stops the task, preventing it from sending any further
         * performance markers.
         *
         * Since the task obtains performance information
         * asynchronously, cancelling the task is not enough to
         * prevent it from sending further performance markers to the
         * client.
         */
        public synchronized void stop()
        {
            cancel();
            _stopped = true;
        }

        /**
         * Like stop() but sends a final performance marker.
         *
         * @param info Information about the completed transfer used
         * to generate the final performance marker.
         */
        public synchronized void stop(GFtpProtocolInfo info)
        {
            /* The protocol info does not contain a timestamp, so
             * we use the current time instead.
             */
            setProgressInfo(info.getBytesTransferred(),
                            System.currentTimeMillis());
            sendMarker();
            stop();
        }

        /**
         * Send markers to client.
         */
        protected synchronized void sendMarker()
        {
            if (!_stopped) {
                reply(_perfMarkersBlock.markers(0).getReply(), false);
            }
        }

        protected synchronized void setProgressInfo(long bytes, long timeStamp)
        {
            /* Since the timestamp in some cases is generated at the
             * pool and in some cases at the door, we need to ensure
             * that time stamps are never decreasing.
             */
            GFtpPerfMarker marker = _perfMarkersBlock.markers(0);
            timeStamp = Math.max(timeStamp, marker.getTimeStamp());
            marker.setBytesWithTime(bytes, timeStamp);
        }

        @Override
        public synchronized void run()
        {
            _cdc.apply();

            CellMessage msg =
                new CellMessage(new CellPath(_pool),
                                "mover ls -binary " + _moverId);
            sendMessage(msg, this, _timeout);
        }

        public synchronized void exceptionArrived(CellMessage request, Exception exception)
        {
            if (exception instanceof NoRouteToCellException) {
                /* Seems we lost connectivity to the pool. This is
                 * not fatal, but we send a new marker to the
                 * client to convince it that we are still alive.
                 */
                sendMarker();
            } else {
                error("FTP Door: PerfMarkerEngine got exception " +
                      exception.getMessage());
            }
        }

        public synchronized void answerTimedOut(CellMessage request)
        {
            sendMarker();
        }

        public synchronized void answerArrived(CellMessage req, CellMessage answer)
        {
            Object msg = answer.getMessageObject();
            if (msg instanceof IoJobInfo) {
                IoJobInfo ioJobInfo = (IoJobInfo)msg;
                String status = ioJobInfo.getStatus();

                if (status == null) {
                    sendMarker();
                } else if (status.equals("A")) {
                    // "Active" job
                    setProgressInfo(ioJobInfo.getBytesTransferred(),
                                    ioJobInfo.getLastTransferred());
                    sendMarker();
                } else if (status.equals("K") || status.equals("R")) {
                    // "Killed" or "Removed" job
                } else if (status.equals("W")) {
                    sendMarker();
                } else {
                    error("FTP Door: performance marker engine " +
                          "received unexcepted status from mover: " + status);
                }
            } else if (msg instanceof Exception) {
                warn("FTP Door: performance marker engine: "
                     + ((Exception)msg).getMessage());
            } else if (msg instanceof String) {
                /* Typically this is just an error message saying the
                 * mover is gone.
                 */
                info("FTP Door: performance marker engine: "
                     + msg.toString());
            } else {
                error("FTP Door: performance marker engine: "
                      + msg.getClass().getName());
            }
        }
    }

    /**
     * Support class to implement FTP command processing on shared
     * worker threads. Commands on the same queue are executed
     * sequentially.
     */
    class CommandQueue
    {
        /** Queue of FTP commands to execute.
         */
        private final Queue<String> _commands = new LinkedList<String>();

        /**
         * The thread to interrupt when the command poller is
         * closed. May be null if interrupts are disabled.
         */
        private Thread _thread;

        /**
         * True iff the command queue has been stopped.
         */
        private boolean _stopped = false;

        /**
         * True iff the command processing task is in the
         * ExecutorService queue or is currently running.
         */
        private boolean _running = false;

        /**
         * Adds a command to the command queue.
         */
        synchronized public void add(String command)
        {
            if (!_stopped) {
                _commands.add(command);
                if (!_running) {
                    final CDC cdc = new CDC();
                    _running = true;
                    _executor.submit(new Runnable() {
                            public void run() {
                                cdc.apply();
                                String command = get();
                                while (command != null) {
                                    execute(command);
                                    command = get();
                                }
                                done();
                            }
                        });
                }
            }
        }

        /**
         * Returns the next command, or null if the queue has been
         * stopped or if there is no command in the queue.
         */
        synchronized private String get()
        {
            return _stopped ? null : _commands.poll();
        }

        /**
         * Signals that the command processing loop was left.
         */
        synchronized private void done()
        {
            _running = false;
            notifyAll();
        }

        /**
         * Stops the command queue. After a call to this method,
         * get() will return null. If interrupts are currently
         * enabled, the target thread is interrupted.
         *
         * Does nothing if the command queue is already stopped.
         */
        synchronized public void stop()
            throws InterruptedException
        {
            if (!_stopped) {
                _stopped = true;

                if (_thread != null) {
                    _thread.interrupt();
                }

                if (_running) {
                    wait();
                }
            }
        }

        /**
         * Enables interrupt upon stop. Until the next call of
         * disableInterrupt(), a call to <code>stop</code> will cause
         * the calling thread to be interrupted.
         *
         * @throws InterruptedException if command poller is already
         * closed
         */
        synchronized void enableInterrupt()
            throws InterruptedException
        {
            if (_stopped) {
                throw new InterruptedException();
            }
            _thread = Thread.currentThread();
        }

        /**
         * Disables interrupt upon stop.
         */
        synchronized void disableInterrupt()
        {
            _thread = null;
        }
    }

    //////////////////////////////////////////////////////////////////////
    //                                                                  //
    // GRIDFTP V2 IMPLEMENTATION                                        //
    // =========================                                        //

    /**
     * Regular expression for parsing parameters of GET and PUT
     * commands. The GridFTP 2 specification is unclear on the format
     * of the keyword, the value and whether white space is
     * allowed. Here we assume keywords are limited to word
     * characters. Values do not contain semicolons.
     *
     * Although RFC 3659 does not cover the GET and PUT commands, we
     * use RFC 3659 as justification to consider white space to be
     * part of the value, that is, we do not strip any white space.
     */
    private static final Pattern _parameterPattern =
        Pattern.compile("(\\w+)(?:=([^;]+))?;");

    /**
     * Patterns for checking the format of values to parameters of GET
     * and PUT commands.
     */
    private static final Map<String,Pattern> _valuePatterns =
        new HashMap<String,Pattern>();

    static
    {
        _valuePatterns.put("mode",  Pattern.compile("[Ee]|[Ss]|[Xx]"));
        _valuePatterns.put("pasv",  null);
        _valuePatterns.put("cksum", Pattern.compile("NONE"));
        _valuePatterns.put("path",  Pattern.compile(".+"));
        _valuePatterns.put("port",  Pattern.compile("(\\d+)(,(\\d+)){5}"));

        //      tid is ignored until we implement mode X
        //      _valuePatterns.put("tid",   Pattern.compile("\\d+"));
    }

    /**
     * Parses parameters of GET and PUT commands. The result is
     * returned as a map from parameter keywords to values. The
     * GridFTP 2 specification does not specify if parameter keywords
     * are case sensitive or not. We assume that they are. The GridFTP
     * 2 specification is unclear whether unknown parameters should be
     * ignored. We silently ignore unknown parameters.
     *
     * @param s the parameter string of a GET or PUT command
     * @return  a map from parameter names to parameter values
     * @throws FTPCommandException If the parameter string cannot be
     *                             parsed.
     */
    protected Map<String,String> parseGetPutParameters(String s)
        throws FTPCommandException
    {
        Map<String,String> parameters = new HashMap<String,String>();

        /* For each parameter.
         */
        Matcher matcher = _parameterPattern.matcher(s);
        while (matcher.lookingAt()) {
            String keyword = matcher.group(1);
            String value   = matcher.group(2);
            if (_valuePatterns.containsKey(keyword)) {
                /* Check format of value.
                 */
                Pattern valuePattern = _valuePatterns.get(keyword);
                if (valuePattern == null && value != null
                    || valuePattern != null && !valuePattern.matcher(value != null ? value : "").matches()) {
                    String msg = "Illegal or unexpected value for " +
                                 keyword + "=" + value;
                    throw new FTPCommandException(501, msg);
                }
                parameters.put(keyword, value);
            }
            matcher.region(matcher.end(), matcher.regionEnd());
        }

        /* Detect trailing garbage.
         */
        if (matcher.regionStart() != matcher.regionEnd()) {
            String msg = "Cannot parse '" + s.substring(matcher.regionStart()) + "'";
            throw new FTPCommandException(501, msg);
        }
        return parameters;
    }


    /**
     * Generate '127 PORT (a,b,c,d,e,f)' command as specified in the
     * GridFTP v2 spec.
     *
     * The GridFTP v2 spec does not specify the reply code to
     * use. However, since the PASV command uses 227, it seems
     * reasonable to use 127 here.
     *
     * GFD.47 specifies the format to be 'PORT=a,b,c,d,e,f', however
     * after consultation with the authors of GFD.47, it was decided
     * to use the typical '(a,b,c,d,e,f)' format instead.
     *
     * @param address the address and port on which we listen
     */
    protected void reply127PORT(InetSocketAddress address)
    {
        int port = address.getPort();
        byte host[] = address.getAddress().getAddress();
        reply(String.format("127 PORT (%d,%d,%d,%d,%d,%d)",
                            (host[0] & 0377),
                            (host[1] & 0377),
                            (host[2] & 0377),
                            (host[3] & 0377),
                            (port / 256),
                            (port % 256)), false);
    }

    /**
     * Implements GridFTP v2 GET operation.
     *
     * @param arg the argument string of the GET command.
     */
    public void ac_get(String arg)
    {
        try {
            String        xferMode   = _xferMode;
            String        clientHost = _client_data_host;
            int           clientPort = _client_data_port;
            Mode          mode       = _mode;
            boolean       reply127   = false;

            if (_skipBytes > 0){
                throw new FTPCommandException(501, "RESTART not implemented");
            }

            Map<String,String> parameters = parseGetPutParameters(arg);

            if (parameters.containsKey("pasv") && parameters.containsKey("port")) {
                throw new FTPCommandException(501, "Cannot use both 'pasv' and 'port'");
            }

            if (!parameters.containsKey("path")) {
                throw new FTPCommandException(501, "Missing path");
            }

            if (parameters.containsKey("mode")) {
                xferMode = parameters.get("mode").toUpperCase();
            }

            if (parameters.containsKey("pasv")) {
                mode = Mode.PASSIVE;
                reply127 = true;
            }

            if (parameters.containsKey("port")) {
                String[] tok = parameters.get("port").split(",");
                String ip = tok[0]+"."+tok[1]+"."+tok[2]+"."+tok[3];
                clientHost = ip;
                clientPort =
                    Integer.valueOf(tok[4]) * 256 + Integer.valueOf(tok[5]);
                mode = Mode.ACTIVE;
            }

            /* Now do the transfer...
             */
            retrieve(parameters.get("path"), prm_offset, prm_size, mode,
                     xferMode, _parallelStart, _parallelMin, _parallelMax,
                     new InetSocketAddress(clientHost, clientPort),
                     _bufSize, reply127, 2);
        } catch (FTPCommandException e) {
            reply(String.valueOf(e.getCode()) + " " + e.getReply());
        } finally {
            prm_offset=-1;
            prm_size=-1;
        }
    }

    /**
     * Implements GridFTP v2 PUT operation.
     *
     * @param arg the argument string of the PUT command.
     */
    public void ac_put(String arg)
    {
        String        xferMode   = _xferMode;
        String        clientHost = _client_data_host;
        int           clientPort = _client_data_port;
        Mode          mode       = _mode;
        boolean       reply127   = false;
        try {
            Map<String,String> parameters = parseGetPutParameters(arg);

            if (parameters.containsKey("pasv") && parameters.containsKey("port")) {
                throw new FTPCommandException(501,
                                              "Cannot use both 'pasv' and 'port'");
            }

            if (!parameters.containsKey("path")) {
                throw new FTPCommandException(501, "Missing path");
            }

            if (parameters.containsKey("mode")) {
                xferMode = parameters.get("mode").toUpperCase();
            }

            if (parameters.containsKey("pasv")) {
                mode = Mode.PASSIVE;
                reply127 = true;
            }

            if (parameters.containsKey("port")) {
                String[] tok = parameters.get("port").split(",");
                String ip = tok[0]+"."+tok[1]+"."+tok[2]+"."+tok[3];
                clientHost = ip;
                clientPort =
                    Integer.valueOf(tok[4]) * 256 + Integer.valueOf(tok[5]);
                mode = Mode.ACTIVE;
            }

            /* Now do the transfer...
             */
            store(parameters.get("path"), mode, xferMode,
                  _parallelStart, _parallelMin, _parallelMax,
                  new InetSocketAddress(clientHost, clientPort),
                  _bufSize, reply127, 2);
        } catch (FTPCommandException e) {
            reply(String.valueOf(e.getCode()) + " " + e.getReply());
        }
    }

    private final Subject getSubject()
    {
        Subject subject = Subjects.getSubject(_pwdRecord, true);
        subject.getPrincipals().add(_origin);
        subject.setReadOnly();
        return subject;
    }

    private void sendRemoveInfoToBilling(String pathInPnfs) {
        try {
            DoorRequestInfoMessage infoRemove =
                new DoorRequestInfoMessage(getNucleus().getCellName()+"@"+
                                           getNucleus().getCellDomainName(), "remove");
            infoRemove.setOwner(_dnUser == null? _user : _dnUser);
            infoRemove.setGid(_pwdRecord.GID);
            infoRemove.setUid(_pwdRecord.UID);
            infoRemove.setPath(pathInPnfs);
            infoRemove.setClient(_client_data_host);

            sendMessage(new CellMessage(_billingCellPath, infoRemove));
            } catch (NoRouteToCellException e) {
                error("FTP Door: Can't send remove message to " +
                        "billing database: " + e.getMessage());
            }
     }

    /** A short format which only includes the file name. */
    static class ShortListPrinter implements DirectoryListPrinter
    {
        private final PrintWriter _out;

        public ShortListPrinter(PrintWriter writer)
        {
            _out = writer;
        }

        public Set<org.dcache.namespace.FileAttribute> getRequiredAttributes()
        {
            return EnumSet.noneOf(org.dcache.namespace.FileAttribute.class);
        }

        public void print(FileAttributes dirAttr, DirectoryEntry entry)
        {
            _out.append(entry.getName()).append("\r\n");
        }
    }

    /** A long format corresponding to the 'normal' FTP list format. */
    class LongListPrinter implements DirectoryListPrinter
    {
        private final Subject _subject;
        private final PrintWriter _out;
        private final org.dcache.namespace.PermissionHandler _pdp =
            new org.dcache.namespace.ChainedPermissionHandler
            (
                new org.dcache.namespace.ACLPermissionHandler(),
                new org.dcache.namespace.PosixPermissionHandler()
            );

        public LongListPrinter(PrintWriter writer)
        {
            _out = writer;
            _subject = getSubject();
        }

        public Set<org.dcache.namespace.FileAttribute> getRequiredAttributes()
        {
            Set<org.dcache.namespace.FileAttribute> attributes =
                EnumSet.of(SIMPLE_TYPE, MODIFICATION_TIME, SIZE);
            attributes.addAll(_pdp.getRequiredAttributes());
            return attributes;
        }

        public void print(FileAttributes dirAttr, DirectoryEntry entry)
        {
            StringBuilder mode = new StringBuilder();
            FileAttributes attr = entry.getFileAttributes();

            if (attr.getFileType() == FileType.DIR) {
                boolean canListDir =
                    _pdp.canListDir(_subject, attr) == AccessType.ACCESS_ALLOWED;
                boolean canLookup =
                    _pdp.canLookup(_subject, attr) == AccessType.ACCESS_ALLOWED;
                boolean canCreateFile =
                    _pdp.canCreateFile(_subject, attr) == AccessType.ACCESS_ALLOWED;
                boolean canCreateDir =
                    _pdp.canCreateSubDir(_subject, attr) == AccessType.ACCESS_ALLOWED;
                mode.append('d');
                mode.append(canListDir ? 'r' : '-');
                mode.append(canCreateFile || canCreateDir ? 'w' : '-');
                mode.append(canLookup || canListDir || canCreateFile || canCreateDir ? 'x' : '-');
                mode.append("------");
            } else {
                boolean canReadFile =
                    _pdp.canReadFile(_subject, attr)== AccessType.ACCESS_ALLOWED;
                mode.append('-');
                mode.append(canReadFile ? 'r' : '-');
                mode.append('-');
                mode.append('-');
                mode.append("------");
            }

            long modified = attr.getModificationTime();
            long age = System.currentTimeMillis() - modified;
            String format;
            if (age > (182L * 24 * 60 * 60 * 1000)) {
                format = "%1$s  1 %2$-10s %3$-10s %4$12d %5$tb %5$2te %5$5tY %6$s";
            } else {
                format = "%1$s  1 %2$-10s %3$-10s %4$12d %5$tb %5$2te %5$5tR %6$s";
            }

            _out.format(format,
                        mode,
                        _pwdRecord.Username,
                        _pwdRecord.Username,
                        attr.getSize(),
                        modified,
                        entry.getName());
            _out.append("\r\n");
        }
    }

    /**
     * ListPrinter using the RFC 3659 fact line format.
     */
    private class FactPrinter implements DirectoryListPrinter
    {
        private final PrintWriter _out;
        private final Subject _subject;
        private final org.dcache.namespace.PermissionHandler _pdp =
            new org.dcache.namespace.ChainedPermissionHandler
            (
                new org.dcache.namespace.ACLPermissionHandler(),
                new org.dcache.namespace.PosixPermissionHandler()
            );

        public FactPrinter(PrintWriter writer)
        {
            _out = writer;
            _subject = getSubject();
        }

        public Set<org.dcache.namespace.FileAttribute> getRequiredAttributes()
        {
            Set<org.dcache.namespace.FileAttribute> attributes =
                EnumSet.noneOf(org.dcache.namespace.FileAttribute.class);
            for (Fact fact: _currentFacts) {
                switch (fact) {
                case SIZE:
                    attributes.add(SIZE);
                    attributes.addAll(_pdp.getRequiredAttributes());
                    break;
                case MODIFY:
                    attributes.add(MODIFICATION_TIME);
                    attributes.addAll(_pdp.getRequiredAttributes());
                    break;
                case TYPE:
                    attributes.add(SIMPLE_TYPE);
                    break;
                case PERM:
                    attributes.add(SIMPLE_TYPE);
                    attributes.addAll(_pdp.getRequiredAttributes());
                    break;
                case UNIQUE:
                    attributes.add(PNFSID);
                    break;
                }
            }
            return attributes;
        }

        public void print(FileAttributes dirAttr, DirectoryEntry entry)
        {
            if (!_currentFacts.isEmpty()) {
                AccessType access;
                FileAttributes attr = entry.getFileAttributes();

                for (Fact fact: _currentFacts) {
                    switch (fact) {
                    case SIZE:
                        access =
                            _pdp.canGetAttributes(_subject, dirAttr, attr,
                                                  EnumSet.of(SIZE));
                        if (access == AccessType.ACCESS_ALLOWED) {
                            printSizeFact(attr);
                        }
                        break;
                    case MODIFY:
                        access =
                            _pdp.canGetAttributes(_subject, dirAttr, attr,
                                                  EnumSet.of(MODIFICATION_TIME));
                        if (access == AccessType.ACCESS_ALLOWED) {
                            printModifyFact(attr);
                        }
                        break;
                    case TYPE:
                        printTypeFact(attr);
                        break;
                    case UNIQUE:
                        printUniqueFact(attr);
                        break;
                    case PERM:
                        printPermFact(dirAttr, attr);
                        break;
                    }
                }
            }
            _out.print(' ');
            _out.print(entry.getName());
            _out.print("\r\n");
        }

        /** Writes an RFC 3659 fact to a writer. */
        private void printFact(Fact fact, Object value)
        {
            _out.print(fact);
            _out.print('=');
            _out.print(value);
            _out.print(';');
        }

        /** Writes a RFC 3659 modify fact to a writer. */
        private void printModifyFact(FileAttributes attr)
        {
            long time = attr.getModificationTime();
            printFact(Fact.MODIFY, TIMESTAMP_FORMAT.format(new Date(time)));
        }

        /** Writes a RFC 3659 size fact to a writer. */
        private void printSizeFact(FileAttributes attr)
        {
            printFact(Fact.SIZE, attr.getSize());
        }

        /** Writes a RFC 3659 type fact to a writer. */
        private void printTypeFact(FileAttributes attr)
        {
            switch (attr.getFileType()) {
            case DIR:
                printFact(Fact.TYPE, "dir");
                break;
            case REGULAR:
                printFact(Fact.TYPE, "file");
                break;
            }
        }

        /**
         * Writes a RFC 3659 unique fact to a writer. The value of the
         * unique fact is the PNFS ID.
         */
        private void printUniqueFact(FileAttributes attr)
        {
            printFact(Fact.UNIQUE, attr.getPnfsId());
        }

        /**
         * Writes a RFC 3659 perm fact to a writer. This operation is
         * rather expensive as the permission information must be
         * retrieved.
         */
        private void printPermFact(FileAttributes parentAttr, FileAttributes attr)
        {
            StringBuffer s = new StringBuffer();
            if (attr.getFileType() == FileType.DIR) {
                if (_pdp.canCreateFile(_subject, attr) == AccessType.ACCESS_ALLOWED) {
                    s.append('c');
                }
                if (_pdp.canDeleteDir(_subject, parentAttr, attr) == AccessType.ACCESS_ALLOWED) {
                    s.append('d');
                }
                s.append('e');
                if (_pdp.canListDir(_subject, attr) == AccessType.ACCESS_ALLOWED) {
                    s.append('l');
                }
                if (_pdp.canCreateSubDir(_subject, attr) == AccessType.ACCESS_ALLOWED) {
                    s.append('m');
                }
            } else {
                if (_pdp.canDeleteFile(_subject, parentAttr, attr) == AccessType.ACCESS_ALLOWED) {
                    s.append('d');
                }
                if (_pdp.canReadFile(_subject, attr) == AccessType.ACCESS_ALLOWED) {
                    s.append('r');
                }
            }
            printFact(Fact.PERM, s);
        }
    }
}
