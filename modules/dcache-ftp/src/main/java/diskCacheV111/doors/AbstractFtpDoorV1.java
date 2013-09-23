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

import com.google.common.base.Strings;
import com.google.common.collect.Range;

import javax.security.auth.Subject;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.channels.ServerSocketChannel;
import java.security.NoSuchAlgorithmException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.TimerTask;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import diskCacheV111.movers.GFtpPerfMarker;
import diskCacheV111.movers.GFtpPerfMarkersBlock;
import diskCacheV111.util.ActiveAdapter;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.CheckStagePermission;
import diskCacheV111.util.ChecksumFactory;
import diskCacheV111.util.FileExistsCacheException;
import diskCacheV111.util.FileMetaData;
import diskCacheV111.util.FileNotFoundCacheException;
import diskCacheV111.util.FsPath;
import diskCacheV111.util.NotDirCacheException;
import diskCacheV111.util.NotFileCacheException;
import diskCacheV111.util.PermissionDeniedCacheException;
import diskCacheV111.util.PnfsHandler;
import diskCacheV111.util.PnfsId;
import diskCacheV111.util.ProxyAdapter;
import diskCacheV111.util.SocketAdapter;
import diskCacheV111.util.TimeoutCacheException;
import diskCacheV111.vehicles.DoorRequestInfoMessage;
import diskCacheV111.vehicles.DoorTransferFinishedMessage;
import diskCacheV111.vehicles.GFtpProtocolInfo;
import diskCacheV111.vehicles.GFtpTransferStartedMessage;
import diskCacheV111.vehicles.IoDoorEntry;
import diskCacheV111.vehicles.IoDoorInfo;
import diskCacheV111.vehicles.IoJobInfo;
import diskCacheV111.vehicles.ProtocolInfo;

import dmg.cells.nucleus.CDC;
import dmg.cells.nucleus.CellAddressCore;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellMessageAnswerable;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.NoRouteToCellException;
import dmg.util.Args;
import dmg.util.CommandExitException;
import dmg.util.StreamEngine;

import org.dcache.acl.enums.AccessType;
import org.dcache.auth.LoginReply;
import org.dcache.auth.LoginStrategy;
import org.dcache.auth.Origin;
import org.dcache.auth.Subjects;
import org.dcache.auth.attributes.HomeDirectory;
import org.dcache.auth.attributes.LoginAttribute;
import org.dcache.auth.attributes.ReadOnly;
import org.dcache.auth.attributes.RootDirectory;
import org.dcache.cells.AbstractCell;
import org.dcache.cells.CellStub;
import org.dcache.cells.Option;
import org.dcache.namespace.ACLPermissionHandler;
import org.dcache.namespace.ChainedPermissionHandler;
import org.dcache.namespace.FileAttribute;
import org.dcache.namespace.FileType;
import org.dcache.namespace.PermissionHandler;
import org.dcache.namespace.PosixPermissionHandler;
import org.dcache.services.login.RemoteLoginStrategy;
import org.dcache.util.Checksum;
import org.dcache.util.ChecksumType;
import org.dcache.util.FireAndForgetTask;
import org.dcache.util.Glob;
import org.dcache.util.PortRange;
import org.dcache.util.Transfer;
import org.dcache.util.TransferRetryPolicy;
import org.dcache.util.list.DirectoryEntry;
import org.dcache.util.list.DirectoryListPrinter;
import org.dcache.util.list.DirectoryListSource;
import org.dcache.util.list.ListDirectoryHandler;
import org.dcache.vehicles.FileAttributes;

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
     * Depending on the values of _isProxyRequiredOnActive and
     * _isProxyRequiredOnPassive, the data connection with the client
     * will be established either to an adapter (proxy) at the FTP
     * door, or to the pool directly.
     */
    protected enum Mode
    {
        PASSIVE, ACTIVE
    }


    /**
     * Enumeration type for representing RFC 3659 facts.
     *
     * Note that the Globus Online service erroneously requires fact
     * names to be capitalised, whereas RFC 3659 makes no such
     * requirement.  This bug has been reported:
     *
     *    https://ggus.eu/tech/ticket_show.php?ticket=72601
     *
     * and, within Globus' internal tracker, as:
     *
     *    http://jira.globus.org/browse/OPS-1
     */
    protected enum Fact
    {
        SIZE("Size"),
        MODIFY("Modify"),
        TYPE("Type"),
        UNIQUE("Unique"),
        PERM("Perm"),
        OWNER("UNIX.owner"),
        GROUP("UNIX.group"),
        MODE("UNIX.mode");

        private final String _name;

        Fact(String name)
        {
            _name = name;
        }

        public String getName()
        {
            return _name;
        }

        public static Fact find(String s)
        {
            for (Fact fact: values()) {
                if (s.equalsIgnoreCase(fact.getName())) {
                    return fact;
                }
            }
            return null;
        }
    }

    /**
     * Feature strings returned when the client sends the FEAT
     * command.
     */
    private static final String[] FEATURES = {
        "EOF", "PARALLEL", "MODE-E-PERF", "SIZE", "SBUF",
        "ERET", "ESTO", "GETPUT", "MDTM",
        "CKSUM " + buildChecksumList(),  "MODEX",
        "TVFS"
        /*
         * do not publish DCAU as supported feature. This will force
         * some clients to always encrypt data channel
         */
        // "DCAU"
    };

    private final static int DEFAULT_DATA_PORT = 20;

    /**
     * The maximum number of retries done on write. Must be one to
     * avoid that empty replicas are left on pools.
     */
    private final static int MAX_RETRIES_WRITE = 1;

    /**
     * Time stamp format as defined in RFC 3659.
     */
    private final DateFormat TIMESTAMP_FORMAT =
        new SimpleDateFormat("yyyyMMddHHmmss");

    private static String buildChecksumList(){
        String result = "";
        int mod = 0;
        for (ChecksumType type: ChecksumType.values()) {
            result += type.getName() + ",";
            mod = 1;
        }

        return result.substring(0, result.length() - mod);
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

    @Option(name = "gplazma",
            description = "Cell path to gPlazma",
            defaultValue = "gPlazma")
    protected String _gPlazma;

    @Option(name = "billing",
            description = "Cell path to billing",
            defaultValue = "billing")
    protected String _billing;

    @Option(
        name = "clientDataPortRange"
    )
    protected String _portRange;

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
        defaultValue = "1500"
    )
    protected int _poolManagerTimeout;

    @Option(
            name = "poolManagerTimeoutUnit",
            defaultValue = "SECONDS"
    )
    protected TimeUnit _poolManagerTimeoutUnit;

    @Option(
        name = "pnfsTimeout",
        defaultValue = "60"
    )
    protected int _pnfsTimeout;

    @Option(
            name = "pnfsTimeoutUnit",
            defaultValue = "SECONDS"
    )
    protected TimeUnit _pnfsTimeoutUnit;

    @Option(
        name = "poolTimeout",
        defaultValue = "300"
    )
    protected int _poolTimeout;

    @Option(
            name = "poolTimeoutUnit",
            defaultValue = "SECONDS"
    )
    protected TimeUnit _poolTimeoutUnit;

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
     * True if passive transfers have to be relayed through the door,
     * i.e., the client must not connect directly to the pool.
     */
    @Option(
        name = "proxyPassive",
        description = "Whether proxy is required for passive transfers",
        required = true
    )
    protected boolean _isProxyRequiredOnPassive;

    /**
     * True if active transfers have to be relayed through the door.
     */
    @Option(
        name = "proxyActive",
        description = "Whether proxy is required for active transfers",
        required = true
    )
    protected boolean _isProxyRequiredOnActive;

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

    @Option(
        name = "perfMarkerPeriod",
        description = "Performance marker period",
        defaultValue = "90"
    )
    protected long _performanceMarkerPeriod;

    @Option(
            name = "perfMarkerPeriodUnit",
            description = "Performance marker period unit",
            defaultValue = "SECONDS"
    )
    protected TimeUnit _performanceMarkerPeriodUnit;

    protected final int _sleepAfterMoverKill = 1; // seconds

    protected final int _spaceManagerTimeout = 5 * 60;

    protected PortRange _passiveModePortRange;
    protected ServerSocketChannel _passiveModeServerSocket;

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

    private   Thread         _workerThread;
    protected int            _commandCounter;
    protected String         _lastCommand    = "<init>";

    protected InetSocketAddress _clientDataAddress;
    protected volatile Socket _dataSocket;

    // added for the support or ERET with partial retrieve mode
    protected long prm_offset = -1;
    protected long prm_size = -1;


    protected long   _skipBytes;

    protected boolean _confirmEOFs;

    protected Subject _subject;
    protected boolean _isUserReadOnly;
    protected FsPath _pathRoot = new FsPath();
    protected String _cwd = "/";    // Relative to _pathRoot
    protected FsPath _filepath; // Absolute filepath to the file to be renamed
    protected String _xferMode = "S";
    protected CellStub _billingStub;
    protected CellStub _poolManagerStub;
    protected CellStub _poolStub;
    protected CellStub _gPlazmaStub;
    protected TransferRetryPolicy _readRetryPolicy;
    protected TransferRetryPolicy _writeRetryPolicy;

    /** Tape Protection */
    protected CheckStagePermission _checkStagePermission;

    protected LoginStrategy _loginStrategy;

    /** Can be "mic", "conf", "enc", "clear". */
    protected String _gReplyType = "clear";

    protected Mode _mode = Mode.ACTIVE;

    //These are the number of parallel streams to have
    //when doing mode e transfers
    protected int _parallel;
    protected int _bufSize;

    protected String ftpDoorName = "FTP";
    protected Checksum _checkSum;
    protected ChecksumFactory _checkSumFactory;
    protected ChecksumFactory _optCheckSumFactory;
    protected long _allo;

    /** List of selected RFC 3659 facts. */
    protected Set<Fact> _currentFacts =
        new HashSet(Arrays.asList(new Fact[] {
                    Fact.SIZE, Fact.MODIFY, Fact.TYPE, Fact.UNIQUE, Fact.PERM,
                    Fact.OWNER, Fact.GROUP, Fact.MODE }));

    /**
     * Encapsulation of an FTP transfer.
     */
    protected class FtpTransfer extends Transfer
    {
        private final Mode _mode;
        private final String _xferMode;
        private final int _parallel;
        private final InetSocketAddress _client;
        private final int _bufSize;
        private final boolean _reply127;
        private final int _version;

        private long _offset;
        private long _size;

        protected FTPTransactionLog _tLog;

        /**
         * Socket adapter used for the transfer.
         */
        protected ProxyAdapter _adapter;

        /**
         * Task that periodically generates performance markers. May
         * be null.
         */
        protected PerfMarkerTask _perfMarkerTask;

        /**
         * True if the transfer was aborted.
         */
        protected boolean _aborted;

        /**
         * True if the transfer completed successfully.
         */
        protected boolean _completed;

        public FtpTransfer(FsPath path,
                           long offset,
                           long size,
                           Mode mode,
                           String xferMode,
                           int parallel,
                           InetSocketAddress client,
                           int bufSize,
                           boolean reply127,
                           int version)
        {
            super(AbstractFtpDoorV1.this._pnfs,
                  AbstractFtpDoorV1.this._subject, path);

            setDomainName(AbstractFtpDoorV1.this.getCellDomainName());
            setCellName(AbstractFtpDoorV1.this.getCellName());
            setClientAddress((InetSocketAddress) _engine.getSocket().getRemoteSocketAddress());
            setCheckStagePermission(_checkStagePermission);
            setOverwriteAllowed(_overwrite);
            setPoolManagerStub(_poolManagerStub);
            setPoolStub(_poolStub);
            setBillingStub(_billingStub);
            setAllocation(_allo);

            _offset = offset;
            _size = size;
            _mode = mode;
            _xferMode = xferMode;
            _parallel = parallel;
            _client = client;
            _bufSize = bufSize;
            _reply127 = reply127;
            _version = version;

            setTransfer(this);
        }

        public int getVersion()
        {
            return _version;
        }

        /**
         * Create an adapter, if needed.
         *
         * Since a pool may reject to be passive, we need to set up an
         * adapter even when we can use passive pools.
         */
        private synchronized void createAdapter()
            throws IOException
        {
            switch (_mode) {
            case PASSIVE:
                _adapter =
                    new SocketAdapter(AbstractFtpDoorV1.this,
                                      _passiveModeServerSocket);
                break;

            case ACTIVE:
                if (_isProxyRequiredOnActive) {
                    _logger.info("Creating adapter for active mode");
                    _adapter =
                        new ActiveAdapter(_passiveModePortRange,
                                          _client.getAddress().getHostAddress(),
                                          _client.getPort());
                }
                break;
            }

            if (_adapter != null) {
                _adapter.setMaxBlockSize(_maxBlockSize);
                _adapter.setModeE(_xferMode.equals("E"));
                if (isWrite()) {
                    _adapter.setDirClientToPool();
                } else {
                    _adapter.setDirPoolToClient();
                }
            }
        }

        /**
         * Sanity check offset and size parameters. Must be done after
         * the name space entry has been read because we need the size
         * of the file first.
         */
        public synchronized void checkAndDeriveOffsetAndSize()
            throws FTPCommandException
        {
            long fileSize = getFileAttributes().getSize();
            if (_offset == -1) {
                _offset = 0;
            }
            if (_size == -1) {
                _size = fileSize;
            }
            if (_offset < 0) {
                throw new FTPCommandException(500, "prm offset is " + _offset);
            }
            if (_size < 0) {
                throw new FTPCommandException(500, "prm_size is " + _size);
            }

            if (_offset + _size > fileSize) {
                throw new FTPCommandException(500,
                                              "invalid prm_offset=" + _offset
                                              + " and prm_size " + _size
                                              + " for file of size " + fileSize);
            }
        }

        @Override
        protected synchronized ProtocolInfo getProtocolInfoForPoolManager()
        {
            return new GFtpProtocolInfo("GFtp",
                                        _version,
                                        0,
                                        _client,
                                        _parallel,
                                        _parallel,
                                        _parallel,
                                        _bufSize, 0, 0);
        }

        @Override
        protected synchronized ProtocolInfo getProtocolInfoForPool()
        {
            /* We can only let the pool be passive if this has been
             * enabled and if we can provide the address to the client
             * using a 127 response.
             */
            boolean usePassivePool = !_isProxyRequiredOnPassive && _reply127;

            /* Construct protocol info. For backward compatibility, when
             * an adapter could be used we put the adapter address into
             * the protocol info.
             */
            GFtpProtocolInfo protocolInfo;
            if (_adapter != null) {
                protocolInfo =
                    new GFtpProtocolInfo("GFtp",
                                         _version, 0,
                                         new InetSocketAddress(_local_host, _adapter.getPoolListenerPort()),
                                         _parallel,
                                         _parallel,
                                         _parallel,
                                         _bufSize,
                                         _offset,
                                         _size);
            } else {
                protocolInfo =
                    new GFtpProtocolInfo("GFtp",
                                         _version, 0,
                                         _client,
                                         _parallel,
                                         _parallel,
                                         _parallel,
                                         _bufSize,
                                         _offset,
                                         _size);
            }

            protocolInfo.setDoorCellName(getCellName());
            protocolInfo.setDoorCellDomainName(getCellDomainName());
            protocolInfo.setClientAddress(_client.getAddress().getHostAddress());
            protocolInfo.setPassive(usePassivePool);
            protocolInfo.setMode(_xferMode);

            if (_optCheckSumFactory != null) {
                protocolInfo.setChecksumType(_optCheckSumFactory.getType().getName());
            }

            if (_checkSumFactory != null) {
                protocolInfo.setChecksumType(_checkSumFactory.getType().getName());
            }

            return protocolInfo;
        }

        public void createTransactionLog()
        {
            if (_tLogRoot != null) {
                _logger.info("Door will log ftp transactions to {}", _tLogRoot);
                _tLog = new FTPTransactionLog(_tLogRoot);
                startTlog(_tLog, _path.toString(), isWrite() ? "write" : "read");
            }
        }

        public void setChecksum(Checksum checksum)
            throws CacheException
        {
            if (checksum != null) {
                setStatus("PnfsManager: Setting checksum");
                try {
                    _pnfs.setChecksum(getPnfsId(), checksum);
                } finally {
                    setStatus(null);
                }
            }
        }

        protected synchronized void startTransfer()
        {
            if (_adapter != null) {
                _adapter.start();
            }

            setStatus("Mover " + getPool() + "/" + getMoverId() + ": " +
                      (isWrite() ? "Receiving" : "Sending"));

            reply("150 Opening BINARY data connection for " + _path, false);

            if (isWrite() && _xferMode.equals("E") && _performanceMarkerPeriod > 0) {
                long period = _performanceMarkerPeriodUnit.toMillis(_performanceMarkerPeriod);
                long timeout = period / 2;
                _perfMarkerTask =
                    new PerfMarkerTask(getPoolAddress(), getMoverId(), timeout);
                _timer.schedule(_perfMarkerTask, period, period);
            }
        }

        @Override
        public synchronized void startMover(String queue, long timeout)
            throws CacheException, InterruptedException
        {
            super.startMover(queue, timeout);
            setStatus("Mover " + getPool() + "/" + getMoverId());
            if (_version == 1) {
                startTransfer();
            }
        }

        public synchronized void transferStarted(CellMessage envelope,
                                                 GFtpTransferStartedMessage message)
        {
            try {
                if (_aborted || _completed) {
                    return;
                }

                if (_version != 2) {
                    _logger.error("Received unexpected GFtpTransferStartedMessage for {} from {}", message.getPnfsId(), envelope.getSourcePath());
                    return;
                }

                if (!message.getPnfsId().equals(getPnfsId().getId())) {
                    _logger.error("GFtpTransferStartedMessage has wrong ID, expected {} but got {}", getPnfsId(), message.getPnfsId());
                    throw new FTPCommandException(451, "Transient internal failure");
                }

                if (message.getPassive() && !_reply127) {
                    _logger.error("Pool {} unexpectedly volunteered to be passive", envelope.getSourcePath());
                    throw new FTPCommandException(451, "Transient internal failure");
                }

                /* If passive X mode was requested, but the pool rejected
                 * it, then we have to fail for now. REVISIT: We should
                 * use the other adapter in this case.
                 */
                if (_mode == Mode.PASSIVE && !message.getPassive() && _xferMode.equals("X")) {
                    throw new FTPCommandException(504, "Cannot use passive X mode");
                }

                /* Determine the 127 response address to send back to the
                 * client. When the pool is passive, this is the address of
                 * the pool (and in this case we no longer need the
                 * adapter). Otherwise this is the address of the adapter.
                 */
                if (message.getPassive()) {
                    assert _reply127;
                    assert _adapter != null;

                    reply127PORT(message.getPoolAddress());

                    _logger.info("Closing adapter");
                    _adapter.close();
                    _adapter = null;
                } else if (_reply127) {
                    reply127PORT(new InetSocketAddress(_engine.getLocalAddress(),
                                                       _adapter.getClientListenerPort()));
                }
                startTransfer();
            } catch (FTPCommandException e) {
                abort(e.getCode(), e.getReply());
            } catch (RuntimeException e) {
                abort(426, "Transient internal error", e);
            }
        }

        @Override
        public void finished(CacheException error)
        {
            super.finished(error);
            transferCompleted(error);
        }

        protected synchronized void transferCompleted(CacheException error)
        {
            try {
                /* It may happen the transfer has been aborted
                 * already. This is not a failure.
                 */
                if (_aborted) {
                    return;
                }

                if (_completed) {
                    throw new RuntimeException("DoorTransferFinished message received more than once");
                }

                if (error != null) {
                    throw error;
                }

                /* Wait for adapter to shut down.
                 */
                if (_adapter != null) {
                    _logger.info("Waiting for adapter to finish.");
                    _adapter.join(300000); // 5 minutes
                    if (_adapter.isAlive()) {
                        throw new FTPCommandException(426, "FTP proxy did not shut down");
                    } else if (_adapter.hasError()) {
                        throw new FTPCommandException(426, "FTP proxy failed: " + _adapter.getError());
                    }

                    _logger.debug("Closing adapter");
                    _adapter.close();
                    _adapter = null;
                }

                if (_perfMarkerTask != null) {
                    _perfMarkerTask.stop((GFtpProtocolInfo) getProtocolInfo());
                }

                if (_tLog != null) {
                    _tLog.middle(getFileAttributes().getSize());
                    _tLog.success();
                    _tLog = null;
                }

                notifyBilling(0, "");
                reply("226 Transfer complete.");
                _completed = true;
                setTransfer(null);
            } catch (CacheException e) {
                abort(426, e.getMessage());
            } catch (FTPCommandException e) {
                abort(e.getCode(), e.getReply());
            } catch (InterruptedException e) {
                abort(426, "FTP proxy was interrupted", e);
            } catch (RuntimeException e) {
                abort(426, "Transient internal error", e);
            }
        }

        public void abort(int replyCode, String msg)
        {
            abort(replyCode, msg, null);
        }

        /**
         * Aborts a transfer and performs all necessary cleanup steps,
         * including killing movers and removing incomplete files. A
         * failure message is send to the client. Both the reply code
         * and reply message are logged as errors.
         *
         * If an exception is specified, then the error message in the
         * exception is logged too and the exception itself is logged
         * at a debug level. The intention is that an exception is
         * only specified for exceptional cases, i.e. errors we would
         * not expect to appear in normal use (potential
         * bugs). Communication errors and the like should not be
         * logged with an exception.
         *
         * @param replyCode reply code to send the the client
         * @param replyMsg error message to send back to the client
         * @param exception exception to log or null
         */
        public synchronized void abort(int replyCode, String replyMsg,
                                       Exception exception)
        {
            if (_aborted) {
                return;
            }

            if (_completed) {
                _logger.error("Cannot abort transfer that already completed: {} {}",
                              replyCode, replyMsg);
                return;
            }

            if (_perfMarkerTask != null) {
                _perfMarkerTask.stop();
            }

            if (_adapter != null) {
                _adapter.close();
                _adapter = null;
            }

            killMover(_sleepAfterMoverKill * 1000);

            if (isWrite()) {
                if (_removeFileOnIncompleteTransfer) {
                    _logger.warn("Removing incomplete file {}: {}", getPnfsId(), _path);
                    deleteNameSpaceEntry();
                } else {
                    _logger.warn("Incomplete file was not removed: {}", _path);
                }
            }

            /* Report errors.
             */
            String msg = String.valueOf(replyCode) + " " + replyMsg;
            notifyBilling(replyCode, replyMsg);
            if (_tLog != null) {
                _tLog.error(msg);
                _tLog = null;
            }
            if (exception == null) {
                _logger.error("Transfer error: {}", msg);
            } else {
                _logger.error("Transfer error: {} ({})", msg, exception.getMessage());
                _logger.debug(exception.toString(), exception);
            }
            reply(msg);
            _aborted = true;
            setTransfer(null);
        }
    }

    protected FtpTransfer _transfer;

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

    public AbstractFtpDoorV1(String name, StreamEngine engine, Args args)
    {
        super(name, args);

        try {
            _engine = engine;
            doInit();
            _workerThread.start();
        } catch (InterruptedException | ExecutionException e) {
            reply("421 " + ftpDoorName + " door not ready");
            _shutdownGate.countDown();
        }
    }

    @Override
    protected void init()
        throws Exception
    {
        super.init();

        Transfer.initSession();

        _out      = new PrintWriter(_engine.getWriter());

        _clientDataAddress =
            new InetSocketAddress(_engine.getInetAddress(), DEFAULT_DATA_PORT);

        _logger.debug("Client host: {}",
                      _clientDataAddress.getAddress().getHostAddress());

        if (_local_host == null) {
            _local_host = _engine.getLocalAddress().getHostAddress();
        }

        _billingStub =
                new CellStub(this, new CellPath(_billing));
        _poolManagerStub =
                new CellStub(this, new CellPath(_poolManager),
                        _poolManagerTimeout, _poolManagerTimeoutUnit);
        _poolStub =
                new CellStub(this, null, _poolTimeout, _poolTimeoutUnit);

        _gPlazmaStub =
                new CellStub(this, new CellPath(_gPlazma), 30000);

        _loginStrategy = new RemoteLoginStrategy(_gPlazmaStub);

        /* Data channel port range used when client issues PASV
         * command.
         */
        if (_portRange != null) {
            _passiveModePortRange = PortRange.valueOf(_portRange);
        } else {
            _passiveModePortRange = new PortRange(0);
        }

        /* Parallelism for mode E transfers.
         */
        _parallel = _defaultStreamsPerClient;

	_origin = new Origin(Origin.AuthType.ORIGIN_AUTHTYPE_STRONG,
                             _engine.getInetAddress());

        _readRetryPolicy =
            new TransferRetryPolicy(_maxRetries, _retryWait * 1000,
                                    Long.MAX_VALUE, _poolTimeoutUnit.toMillis(_poolTimeout));
        _writeRetryPolicy =
            new TransferRetryPolicy(MAX_RETRIES_WRITE, 0,
                                    Long.MAX_VALUE, _poolTimeoutUnit.toMillis(_poolTimeout));

        adminCommandListener = new AdminCommandListener();
        addCommandListener(adminCommandListener);

        _checkStagePermission = new CheckStagePermission(_stageConfigurationFilePath);

        useInterpreter(true);

        _workerThread = new Thread(this);
    }

    /**
     * Subject is logged in using the current login strategy.
     */
    protected void login(Subject subject)
        throws CacheException
    {
        LoginReply login = _loginStrategy.login(subject);
        _subject = login.getSubject();

        /* The origin ought to be part of the subject sent to the
         * LoginStrategy, however due to the policy that
         * LoginStrategies only provide what they recognize, we cannot
         * rely on the Origin surviving. Hence we add it to the
         * result. We copy the subject because it could be read-only
         * resulting in failure to add origin.
         */

        _subject = new Subject(false,
                               _subject.getPrincipals(),
                               _subject.getPublicCredentials(),
                               _subject.getPrivateCredentials());
        _subject.getPrincipals().add(_origin);

        for (LoginAttribute attribute: login.getLoginAttributes()) {
            if (attribute instanceof RootDirectory) {
                _pathRoot = new FsPath(((RootDirectory) attribute).getRoot());
            } else if (attribute instanceof HomeDirectory) {
                _cwd = ((HomeDirectory) attribute).getHome();
            } else if (attribute instanceof ReadOnly) {
                _isUserReadOnly = ((ReadOnly) attribute).isReadOnly();
            }
        }

        _pnfs = new PnfsHandler(new CellStub(this, new CellPath(_pnfsManager), _pnfsTimeout, _pnfsTimeoutUnit));
        _pnfs.setSubject(_subject);
        ListDirectoryHandler listSource = new ListDirectoryHandler(_pnfs);
        addMessageListener(listSource);
        _listSource = listSource;
    }

    protected AdminCommandListener adminCommandListener;
    public class AdminCommandListener
    {
        public static final String hh_get_door_info = "[-binary]";
        public Object ac_get_door_info(Args args)
        {
            IoDoorInfo doorInfo = new IoDoorInfo(getCellName(),
                                                 getCellDomainName());
            long[] uids = (_subject != null) ? Subjects.getUids(_subject) : new long[0];
            doorInfo.setOwner((uids.length == 0) ? "0" : Long.toString(uids[0]));
            doorInfo.setProcess("0");
            FtpTransfer transfer = _transfer;
            if (transfer != null) {
                IoDoorEntry[] entries = { transfer.getIoDoorEntry() };
                doorInfo.setIoDoorEntries(entries);
                doorInfo.setProtocol("GFtp",
                                     String.valueOf(transfer.getVersion()));
            } else {
                IoDoorEntry[] entries = {};
                doorInfo.setIoDoorEntries(entries);
                doorInfo.setProtocol("GFtp", "1");
            }

            if (args.hasOption("binary")) {
                return doorInfo;
            } else {
                return doorInfo.toString();
            }
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
            _logger.info("ftpcommand <{} ...>", cmd);
        } else {
            _lastCommand = cmdline;
            _logger.info("ftpcommand <{}>", cmdline);
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

        Method m = _methodDict.get(cmd);
        try {
            m.invoke(this, args);
            if (!cmd.equals("rest")) {
                _skipBytes = 0;
            }
        } catch (InvocationTargetException ite) {
            //
            // is thrown if the underlying method
            // actively throws an exception.
            //
            Throwable te = ite.getTargetException();
            if (te instanceof FTPCommandException) {
                FTPCommandException failure = (FTPCommandException) te;
                reply(String.valueOf(failure.getCode()) + " " + failure.getReply());
            } else if (te instanceof CommandExitException) {
                throw (CommandExitException) te;
            } else {
                reply("500 Operation failed due to internal error: " +
                      te.getMessage());
                _logger.error("FTP command '{}' got exception", cmd, te);
            }

            _skipBytes = 0;
        } catch (IllegalAccessException e) {
            _logger.error("This is a bug. Please report it.", e);
        }
    }

    private synchronized void shutdownInputStream()
    {
        try {
            Socket socket = _engine.getSocket();
            if (!socket.isClosed() && !socket.isInputShutdown()) {
                socket.shutdownInput();
            }
        } catch (IOException e) {
            _logger.info("Failed to shut down input stream of the " +
                         "control channel: {}", e.getMessage());
        }
    }

    private synchronized void closePassiveModeServerSocket()
    {
        if (_passiveModeServerSocket != null) {
            try {
                _logger.info("Closing passive mode server socket");
                _passiveModeServerSocket.close();
            } catch (IOException e) {
                _logger.warn("Failed to close passive mode server socket: {}",
                             e.getMessage());
            }
            _passiveModeServerSocket = null;
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
     * - Shut down of passive mode server socket
     * - Abort and cleanup after failed transfers
     * - Cell shutdown initiation
     *
     * Notice that socket and thus input and output streams are not
     * closed here. See cleanUp() for details on this.
     */
    @Override
    public void run()
    {
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
                _logger.error("Got error reading data: {}", e.getMessage());
            } finally {
                /* This will block until command processing has
                 * finished.
                 */
                try {
                    _commandQueue.stop();
                } catch (InterruptedException e) {
                    _logger.error("Failed to shut down command processing: {}",
                                  e.getMessage());
                }

                /* In case of failure, we may have a transfer hanging
                 * around.
                 */
                FtpTransfer transfer = _transfer;
                if (transfer != null) {
                    transfer.abort(451, "Aborting transfer due to session termination");
                }

                closePassiveModeServerSocket();

                _logger.debug("End of stream encountered");
            }
        } finally {
            /* cleanUp() waits for us to open the gate.
             */
            _shutdownGate.countDown();

            /* Killing the cell will cause cleanUp() to be
             * called (although from a different thread).
             */
            kill();
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
            _logger.error("Got interrupted exception shutting down input stream");
        }

        try {
            /* Closing the socket will also close the input and output
             * streams of the socket. This in turn will cause the
             * command poller thread to shut down.
             */
            _engine.getSocket().close();
        } catch (IOException e) {
            _logger.error("Got I/O exception closing socket: {}",
                          e.getMessage());
        }

        super.cleanUp();
    }

    public void println(String str)
    {
        PrintWriter out = _out;
        synchronized (out) {
            out.println(str + "\r");
            out.flush();
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

    protected String getUser()
    {
        return Subjects.getUserName(_subject);
    }

    @Override
    public String toString()
    {
        String user = getUser();
        String address = _clientDataAddress.getAddress().getHostAddress();
        if (user == null) {
            return address;
        } else {
            return user + "@" + address;
        }
    }

    @Override
    public void getInfo(PrintWriter pw)
    {
        String user = getUser();
        pw.println( "            FTPDoor");
        if (user != null) {
            pw.println( "         User  : " + user);
        }
        pw.println( "    User Host  : " + _clientDataAddress.getAddress().getHostAddress());
        pw.println( "   Local Host  : " + _local_host);
        pw.println( " Last Command  : " + _lastCommand);
        pw.println( " Command Count : " + _commandCounter);
        pw.println( "     I/O Queue : " + _ioQueueName);
        pw.println(adminCommandListener.ac_get_door_info(new Args("")));
    }

     public void messageArrived(CellMessage envelope,
                                GFtpTransferStartedMessage message)
     {
         _logger.debug("Received TransferStarted message");
         FtpTransfer transfer = _transfer;
         if (transfer != null) {
             transfer.transferStarted(envelope, message);
         }
     }

    public void messageArrived(DoorTransferFinishedMessage message)
    {
        _logger.debug("Received TransferFinished message [rc={}]",
                      message.getReturnCode());
        FtpTransfer transfer = _transfer;
        if (transfer != null) {
            transfer.finished(message);
        }
    }

    //
    // GSS authentication
    //

    protected void reply(String answer, boolean resetReply)
    {
        if (answer.startsWith("335 ADAT=")) {
            _logger.info("REPLY(reset={} GReplyType={}): <335 ADAT=...>",
                         resetReply, _gReplyType);
        } else {
            _logger.info("REPLY(reset={} GReplyType={}): <{}>", resetReply,_gReplyType, answer);
        }
        switch (_gReplyType) {
        case "clear":
            println(answer);
            break;
        case "mic":
            secure_reply(answer, "631");
            break;
        case "enc":
            secure_reply(answer, "633");
            break;
        case "conf":
            secure_reply(answer, "632");
            break;
        }
        if (resetReply) {
            _gReplyType = "clear";
        }
    }

    protected void reply(String answer)
    {
        reply(answer, true);
    }

    protected abstract void secure_reply(String answer, String code);

    protected void checkLoggedIn()
        throws FTPCommandException
    {
        if (_subject == null) {
            throw new FTPCommandException(530, "Not logged in.");
        }
    }

    protected void checkWritable()
        throws FTPCommandException
    {
        if (_readOnly || _isUserReadOnly) {
            throw new FTPCommandException(500, "Command disabled");
        }

        if (Subjects.isNobody(_subject)) {
            throw new FTPCommandException(554, "Anonymous write access not permitted");
        }
    }

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
            builder.append(fact.getName());
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
        _parallel = Integer.parseInt(st[0]);
        if (_maxStreamsPerClient > 0) {
            _parallel = Math.min(_parallel, _maxStreamsPerClient);
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
                _optCheckSumFactory =
                    ChecksumFactory.getFactory(ChecksumType.getChecksumType(algo));
            } else {
                _optCheckSumFactory = null;
            }
            reply("200 OK");
        } catch (IllegalArgumentException | NoSuchAlgorithmException e) {
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
            StringBuilder s = new StringBuilder("200 MLST ");
            for (Fact fact: _currentFacts) {
                s.append(fact.getName()).append(';');
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
        throws FTPCommandException
    {
        /**
         * DELE
         *    250
         *    450, 550
         *    500, 501, 502, 421, 530
         */
        checkLoggedIn();
        checkWritable();

        FsPath path = absolutePath(arg);
        try {
            _pnfs.deletePnfsEntry(path.toString(),
                                  EnumSet.of(FileType.REGULAR, FileType.LINK));
            reply("250 OK");
            sendRemoveInfoToBilling(path);
        }
        catch (PermissionDeniedCacheException e) {
            throw new FTPCommandException(550,"Permission denied");
        }
        catch (FileNotFoundCacheException e) {
            throw new FTPCommandException(550,"No such file or directory");
        }
        catch (NotFileCacheException e) {
            throw new FTPCommandException(550,"Not a file: "+arg);
        }
        catch (TimeoutCacheException e) {
            throw new FTPCommandException(451,"Internal timeout, reason:"+e);
        }
        catch (CacheException e) {
            throw new FTPCommandException(550,"Cannot delete file, reason:"+e);
        }
    }

    public abstract void ac_auth(String arg);


    public abstract void ac_adat(String arg);

    public void ac_mic(String arg)
        throws CommandExitException
    {
        secure_command(arg, "mic");
    }

    public void ac_enc(String arg)
        throws CommandExitException
    {
        secure_command(arg, "enc");
    }

    public void ac_conf(String arg)
        throws CommandExitException
    {
        secure_command(arg, "conf");
    }

    public abstract void secure_command(String arg, String sectype)
        throws CommandExitException;



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
        if (!arg.equals("C")) {
            reply("534 Will accept only Clear protection level");
        } else {
            reply("200 OK");
        }
    }

    ///////////////////////////////////////////////////////////////////////////
    //                                                                       //
    // the interpreter stuff                                                 //
    //                                                                       //


    private FsPath absolutePath(String path)
    {
        FsPath relativePath = new FsPath(_cwd);
        relativePath.add(path);
        return new FsPath(_pathRoot, relativePath);
    }


    public void ac_rmd(String arg)
        throws FTPCommandException
    {
        /**
         * RMD
         *   250
         *   500, 501, 502, 421, 530, 550
         */
        checkLoggedIn();
        checkWritable();

        if (arg.equals("")) {
            reply(err("RMD",arg));
            return;
        }

        try {
            FsPath path = absolutePath(arg);
            _pnfs.deletePnfsEntry(path.toString(), EnumSet.of(FileType.DIR));
            reply("250 OK");
        }
        catch (PermissionDeniedCacheException e) {
            throw new FTPCommandException(550,"Permission denied");
        }
        catch (FileNotFoundCacheException e) {
            throw new FTPCommandException(550,"No such file or directory");
        }
        catch (NotDirCacheException e) {
            throw new FTPCommandException(550,"Not a directory: "+arg);
        }
        catch (TimeoutCacheException e) {
            throw new FTPCommandException(451,"Internal timeout, reason:"+e);
        }
        catch (CacheException e) {
            throw new FTPCommandException(550,"Cannot remove directory, reason:"+e);
        }
    }


    public void ac_mkd(String arg)
        throws FTPCommandException
    {
        /**
         * MKD
         *   257
         *   500, 501, 502, 421, 530, 550
         */
        checkLoggedIn();
        checkWritable();

        if (arg.equals("")) {
            reply(err("MKD",arg));
            return;
        }
        FsPath path = absolutePath(arg);
        FsPath relativePath = new FsPath(_cwd);
        relativePath.add(arg);
        String properDirectoryStringReply = relativePath.toString().replaceAll("\"","\"\"");
        try {
            _pnfs.createPnfsDirectory(path.toString());
            /*
               From RFC 959
               ....., upon successful completion of an MKD
               command, the server should return a line of the form:

               257<space>"<directory-name>"<space><commentary>

                That is, the server will tell the user what string to use when
                referring to the created  directory.  The directory name can
                contain any character; embedded double-quotes should be escaped by
                double-quotes (the "quote-doubling" convention).

                For example, a user connects to the directory /usr/dm, and creates
                a subdirectory, named pathname:

                CWD /usr/dm
                200 directory changed to /usr/dm
                MKD pathname
                257 "/usr/dm/pathname" directory created

                An example with an embedded double quote:

                MKD foo"bar
                257 "/usr/dm/foo""bar" directory created
                CWD /usr/dm/foo"bar
                200 directory changed to /usr/dm/foo"bar
            */
            reply("257 \"" +properDirectoryStringReply+"\" directory created");
        }
        catch (PermissionDeniedCacheException e) {
            throw new FTPCommandException(550,"Permission denied");
        }
        catch (FileExistsCacheException e) {
            throw new FTPCommandException(550,"\""+properDirectoryStringReply+
                                          "\" directory already exists");
        }
        catch (TimeoutCacheException e) {
            throw new FTPCommandException(451,"Internal timeout, reason:"+e);
        }
        catch (CacheException e) {
            throw new FTPCommandException(550,"Cannot create directory, reason:"+e);
        }
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
        throws FTPCommandException
    {
        checkLoggedIn();
        checkWritable();

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
        throws FTPCommandException
    {
        checkLoggedIn();

        if (!arg.equals("")) {
            reply(err("PWD",arg));
            return;
        }
        reply("257 \"" + _cwd + "\" is current directory");
    }

    public void ac_cwd(String arg)
        throws FTPCommandException
    {
        checkLoggedIn();

        try {
            FsPath newcwd = absolutePath(arg);
            checkIsDirectory(newcwd);
            _cwd = _pathRoot.relativize(newcwd).toString();
            reply("250 CWD command succcessful. New CWD is <" + _cwd + ">");
        } catch (NotDirCacheException e) {
            reply("550 Not a directory: " + arg);
        } catch (FileNotFoundCacheException e) {
            reply("550 File not found");
        } catch (CacheException e) {
            reply("451 CWD failed: " + e.getMessage());
            _logger.error("Error in CWD: {}", e);
        }
    }

    public void ac_cdup(String arg)
        throws FTPCommandException
    {
        ac_cwd("..");
    }

    private InetSocketAddress getAddressOf(String[] s)
    {
        try {
            byte address[] = new byte[4];
            for (int i = 0; i < 4; ++i) {
                address[i] = (byte) Integer.parseInt(s[i]);
            }
            int port = Integer.parseInt(s[4]) * 256 + Integer.parseInt(s[5]);
            return new InetSocketAddress(InetAddress.getByAddress(address), port);
        } catch (UnknownHostException e) {
            throw new RuntimeException("Bug detected (UnknownHostException should only be thrown if address has wrong length): " + e.toString());
        }
    }

    private void setActive(InetSocketAddress address)
    {
        _mode = Mode.ACTIVE;
        _clientDataAddress = address;
        closePassiveModeServerSocket();
    }

    private InetSocketAddress setPassive()
        throws FTPCommandException
    {
        try {
            if (_passiveModeServerSocket == null) {
                _logger.info("Opening server socket for passive mode");
                _passiveModeServerSocket = ServerSocketChannel.open();
                _passiveModePortRange.bind(_passiveModeServerSocket.socket(),
                                           _engine.getLocalAddress());
                _mode = Mode.PASSIVE;
            }
            return (InetSocketAddress) _passiveModeServerSocket.socket().getLocalSocketAddress();
        } catch (IOException e) {
            _mode = Mode.ACTIVE;
            closePassiveModeServerSocket();
            throw new FTPCommandException(500, "Cannot enter passive mode: " + e);
        }
    }

    public void ac_port(String arg)
        throws FTPCommandException
    {
        checkLoggedIn();

        String[] st = arg.split(",");
        if (st.length != 6) {
            reply(err("PORT",arg));
            return;
        }

        setActive(getAddressOf(st));

        reply(ok("PORT"));
    }

    public void ac_pasv(String arg)
        throws FTPCommandException
    {
        checkLoggedIn();

        /* If already in passive mode then we close the previous
         * socket and allocate a new one. This is a defensive move to
         * recover from the server socket having been closed by some
         * error condition.
         */
        closePassiveModeServerSocket();
        InetSocketAddress address = setPassive();
        int port = address.getPort();
        byte[] hostb = address.getAddress().getAddress();
        int[] host = new int[4];
        for (int i = 0; i < 4; i++) {
            host[i] = hostb[i] & 0377;
        }
        reply("227 OK (" +
              host[0] + "," +
              host[1] + "," +
              host[2] + "," +
              host[3] + "," +
              port/256 + "," +
              port % 256 + ")");
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
        throws FTPCommandException
    {
        checkLoggedIn();

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
        }
    }

    public void ac_cksm(String arg)
        throws FTPCommandException
    {
        checkLoggedIn();

        String[] st = arg.split("\\s+");
        if (st.length != 4) {
            reply("500 Unsupported CKSM command operands");
            return;
        }
        String algo = st[0];
        String offset = st[1];
        String length = st[2];
        String path = st[3];

        long offsetL;
        long lengthL;

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
        if (lengthL != -1) {
            throw new FTPCommandException(504, "Unsupported checksum over partial file length");
        }

        if (offsetL != 0) {
            throw new FTPCommandException(504, "Unsupported checksum over partial file offset");
        }

        try {
            ChecksumFactory cf =
                ChecksumFactory.getFactory(ChecksumType.getChecksumType(algo));
            FileAttributes attributes =
                _pnfs.getFileAttributes(absolutePath(path).toString(),
                                        EnumSet.of(CHECKSUM));
            Checksum checksum = cf.find(attributes.getChecksums());
            if (checksum == null) {
                throw new FTPCommandException(504, "Checksum is not available, dynamic checksum calculation is not supported");
            }
            reply("213 " + checksum.getValue());
        } catch (CacheException ce) {
            throw new FTPCommandException(550, "Error retrieving " + path
                                          + ": " + ce.getMessage());
        } catch (IllegalArgumentException | NoSuchAlgorithmException e) {
            throw new FTPCommandException(504, "Unsupported checksum type:" + e);
        }
    }

    public void ac_scks(String arg)
        throws FTPCommandException
    {
        checkLoggedIn();

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
            _checkSumFactory =
                ChecksumFactory.getFactory(ChecksumType.getChecksumType(type));
            _checkSum = _checkSumFactory.create(value);
            reply("213 OK");
        } catch (NoSuchAlgorithmException | IllegalArgumentException e) {
            _checkSumFactory = null;
            _checkSum = null;
            reply("504 Unsupported checksum type:" + type);
        }
    }

    public void doChmod(String permstring, String path)
        throws FTPCommandException
    {
        checkLoggedIn();
        checkWritable();

        if (path.equals("")){
            reply(err("SITE CHMOD",path));
            return;
        }

        FileAttributes attributes;
        try {
            // Assume octal regardless of string
            int newperms = Integer.parseInt(permstring, 8);

            // Get meta-data for this file/directory
            attributes =
                _pnfs.getFileAttributes(absolutePath(path).toString(),
                                        EnumSet.of(PNFSID, TYPE,
                                                   OWNER, OWNER_GROUP));

            // Extract fields of interest
            PnfsId       myPnfsId   = attributes.getPnfsId();
            boolean      isADir     = (attributes.getFileType() == FileType.DIR);
            boolean      isASymLink = (attributes.getFileType() == FileType.LINK);
            int          myUid      = attributes.getOwner();
            int          myGid      = attributes.getGroup();

            // Chmod on symbolic links not yet supported (should change perms on file/dir pointed to)
            if (isASymLink) {
                reply("502 chmod of symbolic links is not yet supported.");
                return;
            }

            FileMetaData newMetaData =
                new FileMetaData(isADir,myUid,myGid,newperms);
            _pnfs.pnfsSetFileMetaData(myPnfsId, newMetaData);

            reply("250 OK");
        } catch (NumberFormatException ex) {
            reply("501 permissions argument must be an octal integer");
        } catch (PermissionDeniedCacheException e) {
            reply("550 Permission denied");
        } catch (CacheException ce) {
            reply("550 Permission denied, reason: " + ce);
        }
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
                _logger.info("Error return invoking: {}({})", m.getName(), arg);
                m.invoke(this, args);
            } catch (IllegalAccessException | InvocationTargetException e) {
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
                _logger.info("Esto invoking: {} ({})", m.getName(), arg);
                m.invoke(this, args);
            } catch (IllegalAccessException | InvocationTargetException e) {
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
        throws FTPCommandException
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
            _logger.error(err);
            reply(err);
            return;
        }
        if (asm_offset != 0) {
            reply("504 ESTO Adjusted Store Mode does not work with nonzero offset: " + offset);
            return;
        }
        _logger.info("Performing esto in \"a\" mode with offset = {}", offset);
        ac_stor(filename);
    }

    //
    // this is the implementation for the ERET with mode "p"
    // "p" is partiall retrieve mode
    // other modes identified by string "MODE" can be implemented by adding
    // void method ac_eret_"MODE"(String arg)
    //
    public void ac_eret_p(String arg)
        throws FTPCommandException
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
            _logger.error(err);
            reply(err);
            return;
        }
        try {
            prm_size = Long.parseLong(size);
        } catch (NumberFormatException e) {
            String err = "501 ERET Partial Retrieve Mode: invalid size " + offset;
            _logger.error(err);
            reply(err);
            return;
        }
        _logger.info("Performing eret in \"p\" mode with offset = {} size = {}",
                     offset, size);
        ac_retr(filename);
    }

    public void ac_retr(String arg)
        throws FTPCommandException
    {
        try {
            if (_skipBytes > 0){
                reply("504 RESTART not implemented");
                return;
            }
            retrieve(arg, prm_offset, prm_size, _mode,
                     _xferMode, _parallel, _clientDataAddress,
                     _bufSize, false, 1);
        } finally {
            prm_offset=-1;
            prm_size=-1;
        }
    }

    protected synchronized Transfer setTransfer(FtpTransfer transfer)
    {
        _transfer = transfer;
        notifyAll();
        return transfer;
    }

    protected synchronized void joinTransfer()
        throws InterruptedException
    {
        while (_transfer != null) {
            wait();
        }
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
     * @param parallel      number of simultaneous streams to use
     * @param client        address of the client (for active servers)
     * @param bufSize       TCP buffers size to use (send and receive),
     *                      or auto scaling when -1.
     * @param reply127      GridFTP v2 127 reply is generated when true
     *                      and client is active.
     * @param version       The mover version to use for the transfer
     */
    private void retrieve(String file, long offset, long size,
                          Mode mode, String xferMode,
                          int parallel,
                          InetSocketAddress client, int bufSize,
                          boolean reply127, int version)
        throws FTPCommandException
    {
        /* Check preconditions.
         */
        checkLoggedIn();

        if (file.equals("")) {
            throw new FTPCommandException(501, "Missing path");
        }
        if (xferMode.equals("E") && mode == Mode.PASSIVE) {
            throw new FTPCommandException(500, "Cannot do passive retrieve in E mode");
        }
        if (xferMode.equals("X") && mode == Mode.PASSIVE && _isProxyRequiredOnPassive) {
            throw new FTPCommandException(504, "Cannot use passive X mode");
        }
        if (_checkSumFactory != null || _checkSum != null) {
            throw new FTPCommandException(503,"Expecting STOR ESTO PUT commands");
        }

        FtpTransfer transfer =
            new FtpTransfer(absolutePath(file),
                            offset, size,
                            mode,
                            xferMode,
                            parallel,
                            client,
                            bufSize,
                            reply127,
                            version);
        try {
            _logger.info("retrieve user={}", getUser());
            _logger.info("retrieve addr={}", _engine.getInetAddress());

            transfer.readNameSpaceEntry();
            transfer.createTransactionLog();
            transfer.checkAndDeriveOffsetAndSize();

            /* Transfer the file. As there is a delay between the
             * point when a pool goes offline and when the pool
             * manager updates its state, we will retry failed
             * transfer a few times.
             */
            enableInterrupt();
            try {
                transfer.createAdapter();
                transfer.selectPoolAndStartMover(_ioQueueName, _readRetryPolicy);
            } finally {
                disableInterrupt();
            }
        } catch (PermissionDeniedCacheException e) {
            transfer.abort(550, "Permission denied");
        } catch (CacheException e) {
            switch (e.getRc()) {
            case CacheException.FILE_NOT_FOUND:
                transfer.abort(550, "File not found");
                break;
            case CacheException.TIMEOUT:
                transfer.abort(451, "Internal timeout", e);
                break;
            case CacheException.NOT_DIR:
                transfer.abort(550, "Not a directory");
                break;
            default:
                transfer.abort(451, "Operation failed: " + e.getMessage(), e);
                break;
            }
        } catch (FTPCommandException e) {
            transfer.abort(e.getCode(), e.getReply());
        } catch (InterruptedException e) {
            transfer.abort(451, "Operation cancelled");
        } catch (IOException e) {
            transfer.abort(451, "Operation failed: " + e.getMessage());
        } catch (RuntimeException e) {
            _logger.error("Retrieve failed", e);
            transfer.abort(451, "Transient internal failure");
        } finally {
            _allo = 0;
        }
    }

    public abstract void startTlog(FTPTransactionLog log, String path, String action);

    public void ac_stor(String arg)
        throws FTPCommandException
    {
        if (_clientDataAddress == null) {
            reply("504 Host somehow not set");
            return;
        }
        if (_skipBytes > 0) {
            reply("504 RESTART not implemented for STORE");
            return;
        }

        store(arg, _mode, _xferMode, _parallel, _clientDataAddress,
              _bufSize, false, 1);
    }

    /**
     * Transfers a file from the client to a pool.
     *
     * @param file          the LFN of the file to transfer
     * @param mode          indicates the direction of connection
     *                      establishment
     * @param xferMode      the transfer mode to use
     * @param parallel      number of simultaneous streams to use
     * @param client        address of the client (for active servers)
     * @param bufSize       TCP buffers size to use (send and receive),
     *                      or auto scaling when -1.
     * @param reply127      GridFTP v2 127 reply is generated when true
     *                      and client is active.
     * @param version       The mover version to use for the transfer
     */
    private void store(String file, Mode mode, String xferMode,
                       int parallel,
                       InetSocketAddress client, int bufSize,
                       boolean reply127, int version)
        throws FTPCommandException
    {
        checkLoggedIn();
        checkWritable();

        if (file.equals("")) {
            throw new FTPCommandException(501, "STOR command not understood");
        }
        if (xferMode.equals("E") && mode == Mode.ACTIVE) {
            throw new FTPCommandException(504, "Cannot store in active E mode");
        }
        if (xferMode.equals("X") && mode == Mode.PASSIVE && _isProxyRequiredOnPassive) {
            throw new FTPCommandException(504, "Cannot use passive X mode");
        }

        FtpTransfer transfer =
            new FtpTransfer(absolutePath(file),
                            0, 0,
                            mode,
                            xferMode,
                            parallel,
                            client,
                            bufSize,
                            reply127,
                            version);
        try {
            _logger.info("store receiving with mode {}", xferMode);

            transfer.createNameSpaceEntry();
            transfer.createTransactionLog();
            transfer.setChecksum(_checkSum);

            enableInterrupt();
            try {
                transfer.createAdapter();
                transfer.selectPoolAndStartMover(_ioQueueName, _writeRetryPolicy);
            } finally {
                disableInterrupt();
            }
        } catch (InterruptedException e) {
            transfer.abort(451, "Operation cancelled");
        } catch (IOException e) {
            transfer.abort(451, "Operation failed: " + e.getMessage());
        } catch (PermissionDeniedCacheException e) {
            transfer.abort(550, "Permission denied");
        } catch (CacheException e) {
            switch (e.getRc()) {
            case CacheException.FILE_NOT_FOUND:
                transfer.abort(550, "File not found");
                break;
            case CacheException.FILE_EXISTS:
                transfer.abort(550, "File exists");
                break;
            case CacheException.TIMEOUT:
                transfer.abort(451, "Internal timeout", e);
                break;
            case CacheException.NOT_DIR:
                transfer.abort(501, "Not a directory");
                break;
            default:
                transfer.abort(451, "Operation failed: " + e.getMessage(), e);
                break;
            }
        } catch (RuntimeException e) {
            _logger.error("Store failed", e);
            transfer.abort(451, "Transient internal failure");
        } finally {
            _checkSumFactory = null;
            _checkSum = null;
            _allo = 0;
        }
    }

    public void ac_size(String arg)
        throws FTPCommandException
    {
        checkLoggedIn();

        if (arg.equals("")) {
            reply(err("SIZE",""));
            return;
        }

        FsPath path = absolutePath(arg);
        long filelength;
        try {
            FileAttributes attributes =
                _pnfs.getFileAttributes(path.toString(), EnumSet.of(SIZE));
            filelength = attributes.getSize();
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
        throws FTPCommandException
    {
        checkLoggedIn();

        if (arg.equals("")) {
            reply(err("MDTM",""));
            return;
        }

        try {
            FsPath path = absolutePath(arg);

            long modification_time;
            FileAttributes attributes =
                _pnfs.getFileAttributes(path.toString(),
                                        EnumSet.of(MODIFICATION_TIME));
            modification_time = attributes.getModificationTime();
            String time_val =
                TIMESTAMP_FORMAT.format(new Date(modification_time));
            reply("213 " + time_val);
        } catch (PermissionDeniedCacheException e) {
            reply("550 Permission denied");
        } catch (CacheException e) {
            switch (e.getRc()) {
            case CacheException.FILE_NOT_FOUND:
                reply("550 File not found");
                break;
            case CacheException.TIMEOUT:
                reply("451 Internal timeout");
                _logger.warn("Timeout in MDTM: {}", e);
                break;
            default:
                reply("451 Internal failure: " + e.getMessage());
                _logger.error("Error in MDTM: {}", e);
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
            _dataSocket = _passiveModeServerSocket.accept().socket();
        } else {
            _dataSocket = new Socket();
            _dataSocket.connect(_clientDataAddress);
        }
    }

    private void closeDataSocket()
    {
        Socket socket = _dataSocket;
        if (socket != null) {
            try {
                socket.close();
            } catch (IOException e) {
                _logger.warn("Got I/O exception closing socket: {}",
                             e.getMessage());
            }
            _dataSocket = null;
        }
    }

    public void ac_list(String arg)
        throws FTPCommandException
    {
        checkLoggedIn();

        Args args = new Args(arg);
        boolean listLong =
            args.options().isEmpty() || args.hasOption("l");
        if (args.argc() == 0) {
            arg = "";
        } else {
            arg = args.argv(0);
        }

        FsPath path = absolutePath(arg);

        try {
            enableInterrupt();
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
                    total = _listSource.printDirectory(null, printer, path, null,
                                                       Range.<Integer>all());
                } catch (NotDirCacheException e) {
                    /* path exists, but it is not a directory.
                     */
                    _listSource.printFile(null, printer, path);
                    total = 1;
                } catch (FileNotFoundCacheException e) {
                    /* If f does not exist, then it could be a
                     * pattern; we move up one directory level and
                     * repeat the list.
                     */
                    total =
                        _listSource.printDirectory(null, printer, path.getParent(),
                                                   new Glob(path.getName()), Range.<Integer>all());
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
        } catch (EOFException e) {
            reply("426 Connection closed; transfer aborted");
        } catch (CacheException | IOException e){
            reply("451 Local error in processing");
            _logger.warn("Error in LIST: {}", e.getMessage());
        } finally {
            disableInterrupt();
        }
    }


    private static final Pattern GLOB_PATTERN =
        Pattern.compile("[*?]");

    public void ac_nlst(String arg)
        throws FTPCommandException
    {
        checkLoggedIn();

        if (arg.equals("")) {
            arg = ".";
        }

        try {
            enableInterrupt();

            FsPath path = absolutePath(arg);

            /* RFC 3659 seems to imply that we have to report on
             * illegal arguments (ie attempts to list files) before
             * opening the data connection. We are therefore forced to
             * query the file type first. We allow path to be a pattern though,
             * to allow mget functionality.
             */
            Matcher m = GLOB_PATTERN.matcher(path.getName());
            boolean pathIsPattern = m.find();
            if ( !pathIsPattern ) {
                checkIsDirectory(path);
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
                DirectoryListPrinter printer = new ShortListPrinter(writer);
                if ( pathIsPattern ) {
                    total =
                        _listSource.printDirectory(null, printer, path.getParent(),
                                                   new Glob(path.getName()),
                                                   Range.<Integer>all());
                } else {
                    total = _listSource.printDirectory(null, printer,
                                                       path, null,
                                                       Range.<Integer>all());
                }
                writer.close();
            } finally {
                closeDataSocket();
            }
            reply("226 " + total + " files");
        } catch (InterruptedException e) {
            reply("451 Operation cancelled");
        } catch (FileNotFoundCacheException e) {
            /* 550 is not a valid reply for NLST. However other FTP
             * servers use this return code for NLST. Gerd and Timur
             * decided to follow their example and violate the spec.
             */
            reply("550 Directory not found");
        } catch (NotDirCacheException e) {
            reply("550 Not a directory");
        } catch (PermissionDeniedCacheException e) {
            reply("550 Permission denied");
        } catch (EOFException e) {
            reply("426 Connection closed; transfer aborted");
        } catch (CacheException | IOException e) {
            reply("451 Local error in processing");
            _logger.warn("Error in NLST: {}", e.getMessage());
        } finally {
            disableInterrupt();
        }
    }

    public void ac_mlst(String arg)
        throws FTPCommandException
    {
        checkLoggedIn();

        try {
            FsPath path = absolutePath(arg);
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            pw.print("250- Listing " + arg + "\r\n");
            pw.print(' ');
            _listSource.printFile(null, new MlstFactPrinter(pw), path);
            pw.print("250 End");
            reply(sw.toString());
        } catch (InterruptedException e) {
            reply("451 Operation cancelled");
        } catch (FileNotFoundCacheException e) {
            /**
             * see https://github.com/JasonAlt/UberFTP/issues/2
             * reply "No such file or directory" to make
             * uberftp client happy.
             */
            reply("550 No such file or directory");
        } catch (PermissionDeniedCacheException e) {
            reply("550 Permission denied");
        } catch (CacheException e) {
            reply("451 Local error in processing");
            _logger.warn("Error in MLST: {}", e.getMessage());
        }
    }

    public void ac_mlsd(String arg)
        throws FTPCommandException
    {
        checkLoggedIn();

        try {
            enableInterrupt();

            FsPath path;
            if (arg.length() == 0) {
                path = absolutePath(".");
            } else {
                path = absolutePath(arg);
            }

            /* RFC 3659 seems to imply that we have to report on
             * illegal arguments (ie attempts to list files) before
             * opening the data connection. We are therefore forced to
             * query the file type first.
             */
            checkIsDirectory(path);

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

                total = _listSource.printDirectory(null, new MlsdFactPrinter(writer),
                                                   path, null,
                                                   Range.<Integer>all());
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
            reply("501 Not a directory");
        } catch (PermissionDeniedCacheException e) {
            reply("550 Permission denied");
        } catch (EOFException e) {
            reply("426 Connection closed; transfer aborted");
        } catch (CacheException | IOException e) {
            reply("451 Local error in processing");
            _logger.warn("Error in MLSD: {}", e.getMessage());
        } finally {
            disableInterrupt();
        }
    }

    public void ac_rnfr(String arg) throws FTPCommandException {

        checkLoggedIn();

        try {
            enableInterrupt();
            _filepath = null;

            if (Strings.isNullOrEmpty(arg)) {
                throw new FTPCommandException(500, "Missing file name for RNFR");
            }

            FsPath path = absolutePath(arg);
            _pnfs.getPnfsIdByPath(path.toString());
            _filepath = path;

            reply("350 File exists, ready for destination name RNTO");
        }
        catch (InterruptedException e) {
            throw new FTPCommandException(451,"Operation cancelled");
        }
        catch (CacheException e) {
            throw new FTPCommandException(550, "File not found");
        }
        finally {
            disableInterrupt();
        }
    }

    public void ac_rnto(String arg) throws FTPCommandException {

        checkLoggedIn();

        try {
            enableInterrupt();
            if (_filepath == null) {
                throw new FTPCommandException(503, "RNTO must be preceeded by RNFR");
            }
            if (Strings.isNullOrEmpty(arg)) {
                throw new FTPCommandException(500, "missing destination name for RNTO");
            }

            FsPath newName = absolutePath(arg);
            _pnfs.renameEntry(_filepath.toString(), newName.toString(), true);

            reply("250 File renamed");
        }
        catch (InterruptedException e) {
            throw new FTPCommandException(451, "Operation cancelled");
        }
        catch (CacheException e) {
            throw new FTPCommandException(550, "Permission denied");
        }
        finally {
            _filepath = null;
            disableInterrupt();
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
    public void ac_quit(String arg)
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
            enableInterrupt();
            joinTransfer();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            disableInterrupt();
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
        throws FTPCommandException
    {
        checkLoggedIn();

        FtpTransfer transfer = _transfer;
        if (transfer != null) {
            transfer.abort(426, "Transfer aborted");
        }
        closeDataSocket();
        reply("226 Abort successful");
    }

    // --------------------------------------------
    public String err(String cmd, String arg)
    {
        String msg = "500 '" + cmd;
        if (arg.length() > 0) {
            msg = msg + " " + arg;
        }
        msg = msg + "': command not understood";
        return msg;
    }

    public String ok(String cmd)
    {
        return "200 "+cmd+" command successful";
    }

    /**
     * Throws NotDirCacheException if the given path is not a
     * directory. Throws FileNotFoundCacheException if the path does
     * not exist.
     */
    private void checkIsDirectory(FsPath path)
        throws CacheException
    {
        FileAttributes attributes =
            _pnfs.getFileAttributes(path.toString(), EnumSet.of(SIMPLE_TYPE));
        if (attributes.getFileType() != FileType.DIR) {
            throw new NotDirCacheException("Not a directory");
        }
    }

    /**
     * Allow command processing to be interrupted when the control
     * channel is closed. Should be called from the command processing
     * thread.
     *
     * @throw InterruptedException if command processing was already
     * interrupted.
     */
    protected void enableInterrupt()
        throws InterruptedException
    {
        _commandQueue.enableInterrupt();
    }

    /**
     * Disallow command procesing to be interupted when the control
     * channel is closed.
     */
    protected void disableInterrupt()
    {
        _commandQueue.disableInterrupt();
    }

    private class PerfMarkerTask
        extends TimerTask implements CellMessageAnswerable
    {
        private final GFtpPerfMarkersBlock _perfMarkersBlock
            = new GFtpPerfMarkersBlock(1);
        private final long _timeout;
        private final CellAddressCore _pool;
        private final int _moverId;
        private final CDC _cdc;
        private boolean _stopped;

        public PerfMarkerTask(CellAddressCore pool, int moverId, long timeout)
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
            try (CDC ignored = _cdc.restore()) {
                CellMessage msg =
                        new CellMessage(new CellPath(_pool),
                                "mover ls -binary " + _moverId);
                sendMessage(msg, this, _timeout);
            }
        }

        @Override
        public synchronized void exceptionArrived(CellMessage request, Exception exception)
        {
            if (exception instanceof NoRouteToCellException) {
                /* Seems we lost connectivity to the pool. This is
                 * not fatal, but we send a new marker to the
                 * client to convince it that we are still alive.
                 */
                sendMarker();
            } else {
                _logger.error("PerfMarkerEngine got exception {}",
                              exception.getMessage());
            }
        }

        @Override
        public synchronized void answerTimedOut(CellMessage request)
        {
            sendMarker();
        }

        @Override
        public synchronized void answerArrived(CellMessage req, CellMessage answer)
        {
            Object msg = answer.getMessageObject();
            if (msg instanceof IoJobInfo) {
                IoJobInfo ioJobInfo = (IoJobInfo)msg;
                String status = ioJobInfo.getStatus();

                if (status == null) {
                    sendMarker();
                } else if (status.equals("A") || status.equals("RUNNING")) {
                    // "Active" job
                    setProgressInfo(ioJobInfo.getBytesTransferred(),
                                    ioJobInfo.getLastTransferred());
                    sendMarker();
                } else if (status.equals("K") || status.equals("R")) {
                    // "Killed" or "Removed" job
                } else if (status.equals("W") || status.equals("QUEUED")) {
                    sendMarker();
                } else {
                    _logger.error("Performance marker engine received unexcepted status from mover: {}",
                                  status);
                }
            } else if (msg instanceof Exception) {
                _logger.warn("Performance marker engine: {}",
                             ((Exception) msg).getMessage());
            } else if (msg instanceof String) {
                /* Typically this is just an error message saying the
                 * mover is gone.
                 */
                _logger.info("Performance marker engine: {}", msg);
            } else {
                _logger.error("Performance marker engine: {}",
                              msg.getClass().getName());
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
        private final Queue<String> _commands = new LinkedList<>();

        /**
         * The thread to interrupt when the command poller is
         * closed. May be null if interrupts are disabled.
         */
        private Thread _thread;

        /**
         * True iff the command queue has been stopped.
         */
        private boolean _stopped;

        /**
         * True iff the command processing task is in the
         * ExecutorService queue or is currently running.
         */
        private boolean _running;

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
                    _executor.submit(new FireAndForgetTask(new Runnable() {
                            @Override
                            public void run() {
                                try (CDC ignored = cdc.restore()) {
                                    String command = getOrDone();
                                    while (command != null) {
                                        try {
                                            execute(command);
                                        } catch (RuntimeException e) {
                                            _logger.error("Bug detected", e);
                                        }
                                        command = getOrDone();
                                    }
                                }
                            }
                        }));
                }
            }
        }

        /**
         * Returns the next command.
         *
         * Returns null and signals that the command processing loop
         * was left if the CommandQueue was stopped or the queue is
         * empty.
         */
        synchronized private String getOrDone()
        {
            if (_stopped || _commands.isEmpty()) {
                _running = false;
                notifyAll();
                return null;
            } else {
                return _commands.remove();
            }
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
        new HashMap<>();

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
        Map<String,String> parameters = new HashMap<>();

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
            boolean reply127 = false;

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
                _xferMode = parameters.get("mode").toUpperCase();
            }

            if (parameters.containsKey("pasv")) {
                reply127 = true;
                setPassive();
            }

            if (parameters.containsKey("port")) {
                setActive(getAddressOf(parameters.get("port").split(",")));
            }

            /* Now do the transfer...
             */
            retrieve(parameters.get("path"), prm_offset, prm_size, _mode,
                     _xferMode, _parallel, _clientDataAddress,
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
        boolean reply127 = false;
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
                _xferMode = parameters.get("mode").toUpperCase();
            }

            if (parameters.containsKey("pasv")) {
                reply127 = true;
                setPassive();
            }

            if (parameters.containsKey("port")) {
                setActive(getAddressOf(parameters.get("port").split(",")));
            }

            /* Now do the transfer...
             */
            store(parameters.get("path"), _mode, _xferMode,
                  _parallel, _clientDataAddress, _bufSize, reply127, 2);
        } catch (FTPCommandException e) {
            reply(String.valueOf(e.getCode()) + " " + e.getReply());
        }
    }

    private void sendRemoveInfoToBilling(FsPath path) {
        try {
            DoorRequestInfoMessage infoRemove =
                new DoorRequestInfoMessage(getNucleus().getCellName()+"@"+
                                           getNucleus().getCellDomainName(), "remove");
            infoRemove.setSubject(_subject);
            infoRemove.setPath(path.toString());
            infoRemove.setClient(_clientDataAddress.getAddress().getHostAddress());

            _billingStub.send(infoRemove);
        } catch (NoRouteToCellException e) {
            _logger.error("Can't send remove message to billing database: {}",
                          e.getMessage());
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

        @Override
        public Set<FileAttribute> getRequiredAttributes()
        {
            return EnumSet.noneOf(FileAttribute.class);
        }

        @Override
        public void print(FsPath dir, FileAttributes dirAttr, DirectoryEntry entry)
        {
            _out.append(entry.getName()).append("\r\n");
        }
    }

    /** A long format corresponding to the 'normal' FTP list format. */
    class LongListPrinter implements DirectoryListPrinter
    {
        private final String _userName;
        private final PrintWriter _out;
        private final PermissionHandler _pdp =
            new ChainedPermissionHandler
            (
                new ACLPermissionHandler(),
                new PosixPermissionHandler()
            );

        public LongListPrinter(PrintWriter writer)
        {
            _out = writer;
            _userName = Subjects.getUserName(_subject);
        }

        @Override
        public Set<FileAttribute> getRequiredAttributes()
        {
            Set<FileAttribute> attributes =
                EnumSet.of(SIMPLE_TYPE, MODIFICATION_TIME, SIZE);
            attributes.addAll(_pdp.getRequiredAttributes());
            return attributes;
        }

        @Override
        public void print(FsPath dir, FileAttributes dirAttr, DirectoryEntry entry)
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
                        _userName,
                        _userName,
                        attr.getSize(),
                        modified,
                        entry.getName());
            _out.append("\r\n");
        }
    }

    /**
     * ListPrinter using the RFC 3659 fact line format.
     */
    private abstract class FactPrinter implements DirectoryListPrinter
    {
        private final static int MODE_MASK = 07777;

        protected final PrintWriter _out;

        private final PermissionHandler _pdp =
            new ChainedPermissionHandler
            (
                new ACLPermissionHandler(),
                new PosixPermissionHandler()
            );

        public FactPrinter(PrintWriter writer)
        {
            _out = writer;
        }

        @Override
        public Set<FileAttribute> getRequiredAttributes()
        {
            Set<FileAttribute> attributes =
                EnumSet.noneOf(FileAttribute.class);
            for (Fact fact: _currentFacts) {
                switch (fact) {
                case SIZE:
                    attributes.add(SIMPLE_TYPE);
                    attributes.add(SIZE);
                    attributes.addAll(_pdp.getRequiredAttributes());
                    break;
                case MODIFY:
                    attributes.add(MODIFICATION_TIME);
                    attributes.addAll(_pdp.getRequiredAttributes());
                    break;
                case TYPE:
                    attributes.add(SIMPLE_TYPE);
                    attributes.addAll(_pdp.getRequiredAttributes());
                    break;
                case PERM:
                    attributes.add(SIMPLE_TYPE);
                    attributes.addAll(_pdp.getRequiredAttributes());
                    break;
                case UNIQUE:
                    attributes.add(PNFSID);
                    break;
                case OWNER:
                    attributes.add(OWNER);
                    attributes.addAll(_pdp.getRequiredAttributes());
                    break;
                case GROUP:
                    attributes.add(OWNER_GROUP);
                    attributes.addAll(_pdp.getRequiredAttributes());
                    break;
                case MODE:
                    attributes.add(MODE);
                    attributes.addAll(_pdp.getRequiredAttributes());
                    break;
                }
            }
            return attributes;
        }

        @Override
        public void print(FsPath dir, FileAttributes dirAttr, DirectoryEntry entry)
        {
            if (!_currentFacts.isEmpty()) {
                AccessType access;
                FileAttributes attr = entry.getFileAttributes();

                for (Fact fact: _currentFacts) {
                    switch (fact) {
                    case SIZE:
                        if (attr.getFileType() != FileType.DIR) {
                            access =
                                _pdp.canGetAttributes(_subject, dirAttr, attr,
                                                      EnumSet.of(SIZE));
                            if (access == AccessType.ACCESS_ALLOWED) {
                                printSizeFact(attr);
                            }
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
                        access =
                            _pdp.canGetAttributes(_subject, dirAttr, attr,
                                                  EnumSet.of(TYPE));
                        if (access == AccessType.ACCESS_ALLOWED) {
                            printTypeFact(attr);
                        }
                        break;
                    case UNIQUE:
                        printUniqueFact(attr);
                        break;
                    case PERM:
                        access =
                            _pdp.canGetAttributes(_subject, dirAttr, attr,
                                                  EnumSet.of(MODE, ACL));
                        if (access == AccessType.ACCESS_ALLOWED) {
                            printPermFact(dirAttr, attr);
                        }
                        break;
                    case OWNER:
                        access =
                            _pdp.canGetAttributes(_subject, dirAttr, attr,
                                                  EnumSet.of(OWNER));
                        if (access == AccessType.ACCESS_ALLOWED) {
                            printOwnerFact(attr);
                        }
                        break;
                    case GROUP:
                        access =
                            _pdp.canGetAttributes(_subject, dirAttr, attr,
                                                  EnumSet.of(OWNER_GROUP));
                        if (access == AccessType.ACCESS_ALLOWED) {
                            printGroupFact(attr);
                        }
                        break;
                    case MODE:
                        access =
                            _pdp.canGetAttributes(_subject, dirAttr, attr,
                                                  EnumSet.of(MODE));
                        if (access == AccessType.ACCESS_ALLOWED) {
                            printModeFact(attr);
                        }
                        break;
                    }
                }
            }
            _out.print(' ');
            printName(dir, entry);
            _out.print("\r\n");
        }

        /** Writes an RFC 3659 fact to a writer. */
        private void printFact(Fact fact, Object value)
        {
            _out.print(fact.getName());
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

        /** Writes a RFC 3659 size fact to a writer. */
        private void printOwnerFact(FileAttributes attr)
        {
            printFact(Fact.OWNER, attr.getOwner());
        }

        /** Writes a RFC 3659 size fact to a writer. */
        private void printGroupFact(FileAttributes attr)
        {
            printFact(Fact.GROUP, attr.getGroup());
        }

        /** Writes a RFC 3659 size fact to a writer. */
        private void printModeFact(FileAttributes attr)
        {
            printFact(Fact.MODE,
                      Integer.toOctalString(attr.getMode() & MODE_MASK));
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
            case LINK:
                printFact(Fact.TYPE, "OS.UNIX=slink");
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
            StringBuilder s = new StringBuilder();
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

        protected abstract void printName(FsPath dir, DirectoryEntry entry);
    }

    private class MlsdFactPrinter extends FactPrinter
    {
        public MlsdFactPrinter(PrintWriter writer)
        {
            super(writer);
        }

        @Override
        protected void printName(FsPath dir, DirectoryEntry entry)
        {
            _out.print(entry.getName());
        }
    }

    private class MlstFactPrinter extends FactPrinter
    {
        public MlstFactPrinter(PrintWriter writer)
        {
            super(writer);
        }

        @Override
        protected void printName(FsPath dir, DirectoryEntry entry)
        {
            String name = entry.getName();
            FsPath path = (dir == null) ? new FsPath(name) : new FsPath(dir, name);
            _out.print(_pathRoot.relativize(path));
        }
    }
}
