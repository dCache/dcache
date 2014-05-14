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

package org.dcache.ftp.door;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.CharMatcher;
import com.google.common.base.Predicate;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import com.google.common.net.InetAddresses;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.security.auth.Subject;

import java.io.BufferedOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.InterfaceAddress;
import java.net.NetworkInterface;
import java.net.ProtocolFamily;
import java.net.Socket;
import java.net.SocketException;
import java.net.StandardProtocolFamily;
import java.net.UnknownHostException;
import java.nio.channels.ServerSocketChannel;
import java.security.NoSuchAlgorithmException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import diskCacheV111.doors.FTPTransactionLog;
import diskCacheV111.doors.LineBasedDoor.LineBasedInterpreter;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.CheckStagePermission;
import diskCacheV111.util.ChecksumFactory;
import diskCacheV111.util.FileExistsCacheException;
import diskCacheV111.util.FileNotFoundCacheException;
import diskCacheV111.util.FsPath;
import diskCacheV111.util.NotDirCacheException;
import diskCacheV111.util.NotFileCacheException;
import diskCacheV111.util.PermissionDeniedCacheException;
import diskCacheV111.util.PnfsHandler;
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
import dmg.cells.nucleus.CellCommandListener;
import dmg.cells.nucleus.CellEndpoint;
import dmg.cells.nucleus.CellInfo;
import dmg.cells.nucleus.CellInfoProvider;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellMessageAnswerable;
import dmg.cells.nucleus.CellMessageReceiver;
import dmg.cells.nucleus.CellMessageSender;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.NoRouteToCellException;
import dmg.util.CommandExitException;

import org.dcache.acl.enums.AccessType;
import org.dcache.auth.LoginReply;
import org.dcache.auth.LoginStrategy;
import org.dcache.auth.Origin;
import org.dcache.auth.Subjects;
import org.dcache.auth.attributes.HomeDirectory;
import org.dcache.auth.attributes.LoginAttribute;
import org.dcache.auth.attributes.ReadOnly;
import org.dcache.auth.attributes.RootDirectory;
import org.dcache.cells.CellStub;
import org.dcache.cells.Option;
import org.dcache.ftp.proxy.ActiveAdapter;
import org.dcache.ftp.proxy.ProxyAdapter;
import org.dcache.ftp.proxy.SocketAdapter;
import org.dcache.namespace.ACLPermissionHandler;
import org.dcache.namespace.ChainedPermissionHandler;
import org.dcache.namespace.FileAttribute;
import org.dcache.namespace.FileType;
import org.dcache.namespace.PermissionHandler;
import org.dcache.namespace.PosixPermissionHandler;
import org.dcache.services.login.RemoteLoginStrategy;
import org.dcache.util.Args;
import org.dcache.util.AsynchronousRedirectedTransfer;
import org.dcache.util.Checksum;
import org.dcache.util.ChecksumType;
import org.dcache.util.Glob;
import org.dcache.util.NetLoggerBuilder;
import org.dcache.util.PortRange;
import org.dcache.util.TransferRetryPolicy;
import org.dcache.util.list.DirectoryEntry;
import org.dcache.util.list.DirectoryListPrinter;
import org.dcache.util.list.ListDirectoryHandler;
import org.dcache.vehicles.FileAttributes;
import org.dcache.vehicles.PnfsListDirectoryMessage;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Math.min;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;
import static org.dcache.namespace.FileAttribute.*;
import static org.dcache.util.NetLoggerBuilder.Level.INFO;

@Inherited
@Retention(RUNTIME)
@Target(METHOD)
@interface Help
{
    String value();
}

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
     * Constructs a command exception with the given ftp reply code and
     * message. The message will be used for both the public FTP reply
     * string and for the exception message.
     */
    public FTPCommandException(int code, String reply, Exception cause)
    {
        super(reply, cause);
        _code = code;
        _reply = reply;
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

public abstract class AbstractFtpDoorV1
        implements LineBasedInterpreter, CellMessageReceiver, CellCommandListener, CellInfoProvider, CellMessageSender
{
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractFtpDoorV1.class);
    private static final Timer TIMER = new Timer("Performance marker timer", true);
    private static final Logger ACCESS_LOGGER = LoggerFactory.getLogger("org.dcache.access.ftp");

    protected InetSocketAddress _localAddress;
    protected InetSocketAddress _remoteAddress;
    protected CellAddressCore _cellAddress;
    protected CellEndpoint _cellEndpoint;
    protected Executor _executor;

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
        CREATE("Create"),
        TYPE("Type"),
        UNIQUE("Unique"),
        PERM("Perm"),
        OWNER("UNIX.owner"),
        GROUP("UNIX.group"),
        MODE("UNIX.mode"),
        // See http://www.iana.org/assignments/os-specific-parameters
        CHANGE("UNIX.ctime"),
        ACCESS("UNIX.atime");

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
     * Enumeration type for representing RFC 2428 protocol families.
     *
     * EPSV and EPRT commands
     */
    protected enum Protocol {

        IPV4(Inet4Address.class, 1, StandardProtocolFamily.INET),
        IPV6(Inet6Address.class, 2, StandardProtocolFamily.INET6);

        private Class<? extends InetAddress> _class;
        private int _code;
        private ProtocolFamily _protocolFamily;

        Protocol(Class<? extends InetAddress> addressClass, int code, ProtocolFamily protocolFamily) {
            _class = addressClass;
            _code = code;
            _protocolFamily = protocolFamily;
        }

        public static Protocol fromAddress(InetAddress c) {
            if (c.getClass().equals(Inet4Address.class)) {
               return IPV4;
            } else {
               return IPV6;
            }
        }

        /**
         * find finds the matching enum element for a given protocol code
         * @param code protocol code as defined in RFC2428: 1 for IPv4, 2 for IPv6
         * @return Protocol (IPV4 or IPV6)
         * @throws FTPCommandException if a code other than 1 or 2 is passed as argument
         */
        public static Protocol find(String code) throws FTPCommandException {
            switch (code) {
                case "1": return IPV4;
                case "2": return IPV6;
                default: throw new FTPCommandException(522, "Unknown protocol family '"+code+". "+
                        "Known protocol families are 1: IPv4 and 2: IPv6, use one of (1,2)");
            }
        }

        /**
         * getAddressClass gets the address class associated with this enum element
         * @return class type (Inet4Address or Inet6Address)
         */
        public Class<? extends InetAddress> getAddressClass() {
            return _class;
        }

        public int getCode() {
            return _code;
        }

        public ProtocolFamily getProtocolFamily() {
            return _protocolFamily;
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
        "TVFS",
        "MFMT",
        "MFCT",
        "MFF " + buildSemiColonList(Fact.MODIFY, Fact.CREATE, Fact.MODE),
        "PASV AllowDelayed"
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

    private static String buildSemiColonList(Fact... facts)
    {
        StringBuilder sb = new StringBuilder();
        for (Fact fact : facts) {
            sb.append(fact.getName()).append(';');
        }
        return sb.toString();
    }

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
    protected ListDirectoryHandler _listSource;

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
    protected String _ioQueueName;

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

    @Option(name = "root",
            description = "Root path")
    protected String _root;

    @Option(
            name = "upload",
            description = "Upload directory"
    )
    protected FsPath _uploadPath;

    protected PortRange _passiveModePortRange;
    protected ServerSocketChannel _passiveModeServerSocket;

    private final Map<String,Method>  _methodDict =
        new HashMap<>();
    private final Map<String,Help>  _helpDict = new HashMap<>();

    protected int            _commandCounter;
    protected String         _lastCommand    = "<init>";
    protected String _currentCmdLine;
    private boolean _isHello = true;

    protected InetSocketAddress _clientDataAddress;
    protected volatile Socket _dataSocket;

    // added for the support or ERET with partial retrieve mode
    protected long prm_offset = -1;
    protected long prm_size = -1;


    protected long   _skipBytes;

    protected boolean _confirmEOFs;

    protected Subject _subject;
    protected boolean _isUserReadOnly;
    protected FsPath _userRootPath = new FsPath();
    protected FsPath _doorRootPath = new FsPath();
    protected String _cwd = "/";    // Relative to _doorRootPath
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

    protected Protocol _preferredProtocol;
    /**
     * if _sessionAllPassive is set to true, all
     * future transfers in the session will use
     * passive mode. The flag is set by ftp_epsv.
     */
    protected boolean _sessionAllPassive = false;

    private enum DelayedPassiveReply
    {
        NONE, PASV, EPSV
    }

    private boolean _allowDelayed;
    private DelayedPassiveReply _delayedPassive = DelayedPassiveReply.NONE;

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
    protected Set<Fact> _currentFacts = Sets.newHashSet(
            Fact.SIZE, Fact.MODIFY, Fact.TYPE, Fact.UNIQUE, Fact.PERM,
            Fact.OWNER, Fact.GROUP, Fact.MODE );

    /**
     * Encapsulation of an FTP transfer.
     */
    protected class FtpTransfer extends AsynchronousRedirectedTransfer<GFtpTransferStartedMessage>
    {
        private final Mode _mode;
        private final String _xferMode;
        private final int _parallel;
        private final InetSocketAddress _client;
        private final int _bufSize;
        private final DelayedPassiveReply _delayedPassive;
        private final ProtocolFamily _protocolFamily;
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

        public FtpTransfer(FsPath path,
                           long offset,
                           long size,
                           Mode mode,
                           String xferMode,
                           int parallel,
                           InetSocketAddress client,
                           int bufSize,
                           DelayedPassiveReply delayedPassive,
                           ProtocolFamily protocolFamily,
                           int version)
        {
            super(AbstractFtpDoorV1.this._pnfs,
                  AbstractFtpDoorV1.this._subject, path);

            setDomainName(_cellAddress.getCellDomainName());
            setCellName(_cellAddress.getCellName());
            setClientAddress(_remoteAddress);
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
            _delayedPassive = delayedPassive;
            _protocolFamily = protocolFamily;
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
                    new SocketAdapter(_passiveModeServerSocket);
                break;

            case ACTIVE:
                if (_isProxyRequiredOnActive) {
                    LOGGER.info("Creating adapter for active mode");
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
            boolean usePassivePool = !_isProxyRequiredOnPassive && _delayedPassive != DelayedPassiveReply.NONE;

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
            protocolInfo.setDoorCellDomainName(getDomainName());
            protocolInfo.setClientAddress(_client.getAddress().getHostAddress());
            protocolInfo.setPassive(usePassivePool);
            protocolInfo.setMode(_xferMode);
            protocolInfo.setProtocolFamily(_protocolFamily);

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
                LOGGER.info("Door will log ftp transactions to {}", _tLogRoot);
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

        @Override
        public synchronized void startMover(String queue, long timeout)
            throws CacheException, InterruptedException
        {
            super.startMover(queue, timeout);
            setStatus("Mover " + getPool() + "/" + getMoverId());
            if (_version == 1) {
                redirect(null);
            }
        }

        public void abort(int replyCode, String msg)
        {
            doAbort(new FTPCommandException(replyCode, msg));
        }

        public void abort(int replyCode, String msg, Exception exception)
        {
            doAbort(new FTPCommandException(replyCode, msg, exception));
        }

        @Override
        protected void onQueued()
        {
            setStatus("Mover " + getPool() + "/" + getMoverId());
        }

        @Override
        protected synchronized void onRedirect(GFtpTransferStartedMessage redirect)
        {
            try {
                if (redirect != null) {
                    if (_version != 2) {
                        LOGGER.error("Received unexpected GFtpTransferStartedMessage for {}", redirect.getPnfsId());
                        return;
                    }

                    if (!redirect.getPnfsId().equals(getPnfsId().getId())) {
                        LOGGER.error("GFtpTransferStartedMessage has wrong ID, expected {} but got {}", getPnfsId(), redirect.getPnfsId());
                        throw new FTPCommandException(451, "Transient internal failure");
                    }

                    if (redirect.getPassive() && _delayedPassive == DelayedPassiveReply.NONE) {
                        LOGGER.error("Pool unexpectedly volunteered to be passive");
                        throw new FTPCommandException(451, "Transient internal failure");
                    }

                    /* If passive X mode was requested, but the pool rejected
                     * it, then we have to fail for now. REVISIT: We should
                     * use the other adapter in this case.
                     */
                    if (_mode == Mode.PASSIVE && !redirect.getPassive() && _xferMode.equals("X")) {
                        throw new FTPCommandException(504, "Cannot use passive X mode");
                    }

                    /* Determine the 127 response address to send back to the
                     * client. When the pool is passive, this is the address of
                     * the pool (and in this case we no longer need the
                     * adapter). Otherwise this is the address of the adapter.
                     */
                    if (redirect.getPassive()) {
                        assert _delayedPassive != DelayedPassiveReply.NONE;
                        assert _mode == Mode.PASSIVE;
                        assert _adapter != null;

                        replyDelayedPassive(_delayedPassive, redirect.getPoolAddress());

                        LOGGER.info("Closing adapter");
                        _adapter.close();
                        _adapter = null;
                    } else if (_mode == Mode.PASSIVE) {
                        replyDelayedPassive(_delayedPassive,
                                            new InetSocketAddress(_localAddress.getAddress(),
                                                                  _adapter.getClientListenerPort()));
                    }
                }

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
                    TIMER.schedule(_perfMarkerTask, period, period);
                }
            } catch (FTPCommandException e) {
                abort(e.getCode(), e.getReply());
            } catch (RuntimeException e) {
                _log.error("Possible bug detected.", e);
                abort(451, "Transient internal error", e);
            }
        }

        @Override
        protected synchronized void onFinish()
        {
            try {
                /* Wait for adapter to shut down.
                 */
                if (_adapter != null) {
                    LOGGER.info("Waiting for adapter to finish.");
                    _adapter.join(300000); // 5 minutes
                    if (_adapter.isAlive()) {
                        throw new FTPCommandException(451, "FTP proxy did not shut down");
                    } else if (_adapter.hasError()) {
                        throw new FTPCommandException(451, "FTP proxy failed: " + _adapter.getError());
                    }

                    LOGGER.debug("Closing adapter");
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
                setTransfer(null);
                reply("226 Transfer complete.");
            } catch (FTPCommandException e) {
                abort(e.getCode(), e.getReply());
            } catch (InterruptedException e) {
                abort(451, "FTP proxy was interrupted", e);
            } catch (RuntimeException e) {
                _log.error("Possible bug detected.", e);
                abort(451, "Transient internal error", e);
            }
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
         */
        @Override
        protected synchronized void onFailure(Exception exception)
        {
            if (_perfMarkerTask != null) {
                _perfMarkerTask.stop();
            }

            if (_adapter != null) {
                _adapter.close();
                _adapter = null;
            }

            if (isWrite()) {
                if (_removeFileOnIncompleteTransfer) {
                    LOGGER.warn("Removing incomplete file {}: {}", getPnfsId(), _path);
                    deleteNameSpaceEntry();
                } else {
                    LOGGER.warn("Incomplete file was not removed: {}", _path);
                }
            }

            /* Report errors.
             */
            int replyCode;
            String replyMsg;
            if (exception instanceof FTPCommandException) {
                replyCode = ((FTPCommandException) exception).getCode();
                replyMsg = ((FTPCommandException) exception).getReply();
            } else {
                replyCode = 451;
                replyMsg = exception.getMessage();
            }

            String msg = String.valueOf(replyCode) + " " + replyMsg;
            notifyBilling(replyCode, replyMsg);
            if (_tLog != null) {
                _tLog.error(msg);
                _tLog = null;
            }
            LOGGER.error("Transfer error: {}", msg);
            if (!(exception instanceof FTPCommandException)) {
                LOGGER.debug(exception.toString(), exception);
            }
            setTransfer(null);
            reply(msg);
        }

        public void getInfo(PrintWriter pw)
        {
            pw.println( "  Data channel  : " + _mode + "; mode " + _xferMode + "; " + _parallel + " streams");
            PerfMarkerTask perfMarkerTask = _perfMarkerTask;
            long size = getLength();
            if (size > 0) {
                pw.println("     File size  : " + size);
            }
            if (!isWrite() && _size > -1 && _offset > -1) {
                pw.println("  File segment  : " + _offset + '-' + (_offset + _size));
            }
            if (perfMarkerTask != null) {
                pw.println("   Transferred  : " + perfMarkerTask.getBytesTransferred());
            }
            ProxyAdapter adapter = _adapter;
            if (adapter != null) {
                pw.println("         Proxy  : " + adapter);
            }
        }
    }

    protected FtpTransfer _transfer;

    //
    // Use initializer to load up hashes.
    //
    {
        for (Method method : getClass().getMethods()) {
            String name = method.getName();
            if (name.startsWith("ftp_")) {
                String command = name.substring(4);
                _methodDict.put(command, method);
                Help help = method.getAnnotation(Help.class);
                if (help != null) {
                    _helpDict.put(command, help);
                }
            }
        }
    }

    @Override
    public void setCellEndpoint(CellEndpoint endpoint)
    {
        _cellEndpoint = endpoint;
        CellInfo cellInfo = _cellEndpoint.getCellInfo();
        _cellAddress = new CellAddressCore(cellInfo.getCellName(), cellInfo.getDomainName());
    }

    @Override
    public void setWriter(Writer writer)
    {
        _out = new PrintWriter(writer);
    }

    @Override
    public void setRemoteAddress(InetSocketAddress remoteAddress)
    {
        _remoteAddress = remoteAddress;
    }

    @Override
    public void setLocalAddress(InetSocketAddress localAddress)
    {
        _localAddress = localAddress;
    }

    @Override
    public void setExecutor(Executor executor)
    {
        _executor = executor;
    }

    @Override
    public void init()
    {
        _clientDataAddress =
            new InetSocketAddress(_remoteAddress.getAddress(), DEFAULT_DATA_PORT);

        if (_local_host == null) {
            _local_host = _localAddress.getAddress().getHostAddress();
        }

        _preferredProtocol = Protocol.fromAddress(_clientDataAddress.getAddress());

        _billingStub =
                new CellStub(_cellEndpoint, new CellPath(_billing));
        _poolManagerStub =
                new CellStub(_cellEndpoint, new CellPath(_poolManager),
                        _poolManagerTimeout, _poolManagerTimeoutUnit);
        _poolStub =
                new CellStub(_cellEndpoint, null, _poolTimeout, _poolTimeoutUnit);

        _gPlazmaStub =
                new CellStub(_cellEndpoint, new CellPath(_gPlazma), 30000);

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

        _origin = new Origin(Origin.AuthType.ORIGIN_AUTHTYPE_STRONG, _remoteAddress.getAddress());

        _readRetryPolicy =
            new TransferRetryPolicy(_maxRetries, _retryWait * 1000,
                                    Long.MAX_VALUE, _poolTimeoutUnit.toMillis(_poolTimeout));
        _writeRetryPolicy =
            new TransferRetryPolicy(MAX_RETRIES_WRITE, 0,
                                    Long.MAX_VALUE, _poolTimeoutUnit.toMillis(_poolTimeout));

        _checkStagePermission = new CheckStagePermission(_stageConfigurationFilePath);

        _doorRootPath = new FsPath(_root);

        reply("220 " + ftpDoorName + " door ready");
    }

    /**
     * Subject is logged in using the current login strategy.
     */
    protected void login(Subject subject)
        throws CacheException
    {
        LoginReply login = _loginStrategy.login(subject);

        Subject mappedSubject = login.getSubject();

        /* The origin ought to be part of the subject sent to the
         * LoginStrategy, however due to the policy that
         * LoginStrategies only provide what they recognize, we cannot
         * rely on the Origin surviving. Hence we add it to the
         * result. We copy the subject because it could be read-only
         * resulting in failure to add origin.
         */
        mappedSubject = new Subject(false,
                                    mappedSubject.getPrincipals(),
                                    mappedSubject.getPublicCredentials(),
                                    mappedSubject.getPrivateCredentials());
        mappedSubject.getPrincipals().add(_origin);

        boolean isUserReadOnly = false;
        FsPath userRootPath = new FsPath();
        FsPath userHomePath = new FsPath();
        for (LoginAttribute attribute: login.getLoginAttributes()) {
            if (attribute instanceof RootDirectory) {
                userRootPath = new FsPath(((RootDirectory) attribute).getRoot());
            } else if (attribute instanceof HomeDirectory) {
                userHomePath = new FsPath(((HomeDirectory) attribute).getHome());
            } else if (attribute instanceof ReadOnly) {
                isUserReadOnly = ((ReadOnly) attribute).isReadOnly();
            }
        }
        FsPath doorRootPath;
        if (_root == null) {
            doorRootPath = userRootPath;
        } else {
            doorRootPath = new FsPath(_root);
            if (!userRootPath.startsWith(doorRootPath) && (_uploadPath == null || !_uploadPath.startsWith(doorRootPath))) {
                throw new PermissionDeniedCacheException("User's files are not visible through this FTP service.");
            }
        }
        String cwd = doorRootPath.relativize(new FsPath(userRootPath, userHomePath)).toString();

        _pnfs = new PnfsHandler(new CellStub(_cellEndpoint, new CellPath(_pnfsManager), _pnfsTimeout, _pnfsTimeoutUnit));
        _pnfs.setSubject(mappedSubject);
        _listSource = new ListDirectoryHandler(_pnfs);

        _subject = mappedSubject;
        _cwd = cwd;
        _doorRootPath = doorRootPath;
        _userRootPath = userRootPath;
        _isUserReadOnly = isUserReadOnly;
    }

    public static final String hh_get_door_info = "[-binary]";
    public Object ac_get_door_info(Args args)
    {
        IoDoorInfo doorInfo = new IoDoorInfo(_cellAddress.getCellName(), _cellAddress.getCellDomainName());
        long[] uids = (_subject != null) ? Subjects.getUids(_subject) : new long[0];
        doorInfo.setOwner((uids.length == 0) ? "0" : Long.toString(uids[0]));
        doorInfo.setProcess("0");
        FtpTransfer transfer = getTransfer();
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

        // most of the ic is handled in the ftp_ functions but a few
        // commands need special handling
        if (!cmd.equals("mic" ) && !cmd.equals("conf") && !cmd.equals("enc") &&
            !cmd.equals("adat") && !cmd.equals("pass")) {
            _lastCommand = cmdline;
        }

        // If a transfer is in progress, only permit ABORT and a few
        // other commands to be processed
        if (getTransfer() != null &&
                !(cmd.equals("abor") || cmd.equals("mic")
                        || cmd.equals("conf") || cmd.equals("enc")
                        || cmd.equals("quit") || cmd.equals("bye"))) {
            reply("503 Transfer in progress", false);
            return;
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
                LOGGER.error("FTP command '{}' got exception", cmd, te);
            }

            _skipBytes = 0;
        } catch (IllegalAccessException e) {
            LOGGER.error("This is a bug. Please report it.", e);
        }
    }

    private synchronized void closePassiveModeServerSocket()
    {
        if (_passiveModeServerSocket != null) {
            try {
                LOGGER.info("Closing passive mode server socket");
                _passiveModeServerSocket.close();
            } catch (IOException e) {
                LOGGER.warn("Failed to close passive mode server socket: {}",
                        e.getMessage());
            }
            _passiveModeServerSocket = null;
            _sessionAllPassive = false;
        }
    }

    @Override
    public void shutdown()
    {
        /* In case of failure, we may have a transfer hanging around.
         */
        FtpTransfer transfer = getTransfer();
        if (transfer != null) {
            transfer.abort(451, "Aborting transfer due to session termination");
        }

        closePassiveModeServerSocket();

        if (ACCESS_LOGGER.isInfoEnabled()) {
            NetLoggerBuilder log = new NetLoggerBuilder(INFO, "org.dcache.ftp.disconnect").omitNullValues();
            log.add("host.remote", _remoteAddress);
            log.add("session", CDC.getSession());
            log.toLogger(ACCESS_LOGGER);
        }
    }

    protected void println(String str)
    {
        PrintWriter out = _out;
        synchronized (out) {
            out.println(str + "\r");
            out.flush();
        }
    }

    @Override
    public void execute(String command)
            throws CommandExitException
    {
        _currentCmdLine = command;
        try {
            if (command.equals("")) {
                reply(err("",""));
            } else {
                _commandCounter++;
                ftpcommand(command);
            }
        } finally {
            _currentCmdLine = null;
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
        if (user != null) {
            pw.println( "          User  : " + user);
        }
        pw.println( "    Local Host  : " + _local_host);
        pw.println( "  Last Command  : " + _lastCommand);
        pw.println( " Command Count  : " + _commandCounter);
        pw.println( "     I/O Queue  : " + _ioQueueName);
        FtpTransfer transfer = _transfer;
        if (transfer != null) {
            transfer.getInfo(pw);
        }
        pw.println(ac_get_door_info(new Args("")));
    }

    @Override
    public CellInfo getCellInfo(CellInfo info)
    {
        return info;
    }

    public void messageArrived(CellMessage envelope,
                                GFtpTransferStartedMessage message)
     {
         LOGGER.debug("Received TransferStarted message");
         FtpTransfer transfer = getTransfer();
         if (transfer != null) {
             transfer.redirect(message);
         }
     }

    public void messageArrived(DoorTransferFinishedMessage message)
    {
        LOGGER.debug("Received TransferFinished message [rc={}]",
                message.getReturnCode());
        FtpTransfer transfer = getTransfer();
        if (transfer != null) {
            transfer.finished(message);
        }
    }

    public void messageArrived(PnfsListDirectoryMessage message)
    {
        ListDirectoryHandler listSource = _listSource;
        if (listSource != null) {
            listSource.messageArrived(message);
        }
    }

    //
    // GSS authentication
    //

    protected void reply(String answer, boolean resetReply)
    {
        logReply(answer);
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

    private void logReply(String response)
    {
        if (ACCESS_LOGGER.isInfoEnabled()) {
            String event = _isHello ? "org.dcache.ftp.hello" :
                    "org.dcache.ftp.response";

            String commandLine = _currentCmdLine;
            if (commandLine != null) {
                // For some commands we don't want to log the arguments.
                String command = commandLine.substring(0, min(commandLine.length(), 4)).trim();
                if (command.equalsIgnoreCase("ADAT") ||
                        command.equalsIgnoreCase("PASS")) {
                    commandLine = command + " ...";
                }
            }

            if (response.startsWith("335 ADAT=")) {
                response = "335 ADAT=...";
            }

            if (!_gReplyType.equals("clear")) {
                response = _gReplyType.toUpperCase() + "{" + response + "}";
            }

            NetLoggerBuilder log = new NetLoggerBuilder(INFO, event).omitNullValues();
            log.add("host.remote", _remoteAddress);
            log.add("session", CDC.getSession());
            log.addInQuotes("command", commandLine);
            log.addInQuotes("reply", response);
            log.toLogger(ACCESS_LOGGER);

            _isHello = false;
            _currentCmdLine = null;
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

    @Help("FEAT - List available features.")
    public void ftp_feat(String arg)
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
        Set<Fact> newFacts = new HashSet<>();
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

    public void opts_pasv(String s)
    {
        Map<String, String> options = Splitter.on(';').omitEmptyStrings().withKeyValueSeparator('=').split(s);
        for (Map.Entry<String, String> option : options.entrySet()) {
            if (option.getKey().equalsIgnoreCase("AllowDelayed")) {
                _allowDelayed = option.getValue().equals("1");
            } else {
                reply("501 Unrecognized option: " + option.getKey() + " (" + option.getValue() + ')');
                return;
            }
        }
        reply("200 OK");
    }

    @Help("OPTS <SP> <feat> [<SP> <arg>] - Select desired behaviour for a feature.")
    public void ftp_opts(String arg)
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
        } else if (st.length == 2 && st[0].equalsIgnoreCase("PASV")) {
            opts_pasv(st[1]);
        } else {
            reply("501 Unrecognized option: " + st[0] + " (" + arg + ")");
        }
    }

    @Help("DELE <SP> <pathname> - Delete a file or symbolic link.")
    public void ftp_dele(String arg)
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

    @Help("AUTH <SP> <arg> - Initiate secure context negotiation.")
    public abstract void ftp_auth(String arg);


    @Help("ADAT <SP> <arg> - Supply context negotation data.")
    public abstract void ftp_adat(String arg);

    @Help("MIC <SP> <arg> - Integrity protected command.")
    public void ftp_mic(String arg)
        throws CommandExitException
    {
        secure_command(arg, "mic");
    }

    @Help("ENC <SP> <arg> - Privacy protected command.")
    public void ftp_enc(String arg)
        throws CommandExitException
    {
        secure_command(arg, "enc");
    }

    @Help("CONF <SP> <arg> - Confidentiality protection command.")
    public void ftp_conf(String arg)
        throws CommandExitException
    {
        secure_command(arg, "conf");
    }

    public abstract void secure_command(String arg, String sectype)
        throws CommandExitException;



    @Help("CCC - Switch control channel to cleartext.")
    public void ftp_ccc(String arg)
    {
        // We should never received this, only through MIC, ENC or CONF,
        // in which case it will be intercepted by secure_command()
        reply("533 CCC must be protected");
    }

    @Help("USER <SP> <name> - Authentication username.")
    public abstract void ftp_user(String arg);


    @Help("PASS <SP> <password> - Authentication password.")
    public abstract void ftp_pass(String arg);



    @Help("PBSZ <SP> <size> - Protection buffer size.")
    public void ftp_pbsz(String arg)
    {
        reply("200 OK");
    }

    @Help("PROT <SP> <level> - Set data channel protection level.")
    public void ftp_prot(String arg)
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


    private FsPath absolutePath(String path) throws FTPCommandException
    {
        FsPath relativePath = new FsPath(_cwd);
        relativePath.add(path);
        FsPath absolutePath = new FsPath(_doorRootPath, relativePath);
        if (!absolutePath.startsWith(_userRootPath) &&
                (_uploadPath == null || !absolutePath.startsWith(_uploadPath))) {
            throw new FTPCommandException(550, "Permission denied");
        }
        return absolutePath;
    }


    @Help("RMD <SP> <path> - Remove an empty directory.")
    public void ftp_rmd(String arg)
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


    @Help("MKD <SP> <path> - Create a directory.")
    public void ftp_mkd(String arg)
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

    @Help("HELP [<SP> <string>] - Help about a command, or all commands if <string> isn't specified.")
    public void ftp_help(String arg)
    {
        StringWriter sr = new StringWriter();
        PrintWriter pw = new PrintWriter(sr);

        if (arg.isEmpty()) {
            pw.print("214-The following commands are supported:\r\n");

            List<String> commands =
                    Ordering.natural().sortedCopy(_methodDict.keySet());

            StringBuilder sb = new StringBuilder();
            for (String command : commands) {
                if (command.indexOf('_') != -1) {
                    continue;
                }
                if (sb.length() != 0) {
                    sb.append(' ');
                }
                sb.append(command.toUpperCase());
                if (sb.length() > 65) {
                    pw.print(sb.append("\r\n"));
                    sb = new StringBuilder();
                } else if (command.length() != 4) {
                    sb.append(' ');
                }
            }
            if (sb.length() != 0) {
                pw.print(sb.append("\r\n"));
            }
            pw.print("214 Direct comments to support@dcache.org.");
        } else {
            String command = arg.toUpperCase();
            String lowerCaseCmd = arg.toLowerCase();

            if (arg.indexOf('_') == -1 && _methodDict.containsKey(lowerCaseCmd)) {
                Help help = _helpDict.get(lowerCaseCmd);
                String message = help == null ? command : help.value();

                Iterator<String> i = Splitter.on("\r\n").split(message).iterator();
                boolean isFirstLine = true;
                while (i.hasNext()) {
                    String line = i.next();

                    if (isFirstLine) {
                        if (i.hasNext()) {
                            pw.print("214-" + line + "\r\n");
                        } else {
                            pw.print("214 " + line);
                        }
                    } else if (i.hasNext()) {
                        pw.print(line + "\r\n");
                    } else {
                        pw.print("214 " + line);
                    }
                }
            } else {
                pw.print("501 Unknown command " + command);
            }
        }

        reply(sr.toString());
    }

    @Help("SYST - Return system type.")
    public void ftp_syst(String arg)
    {
        reply("215 UNIX Type: L8 Version: FTPDoor");
    }

    @Help("TYPE - Sets the transfer mode.")
    public void ftp_type(String arg)
    {
        reply("200 Type set to I");
    }

    @Help("NOOP - Does nothing.")
    public void ftp_noop(String arg)
    {
        reply(ok("NOOP"));
    }

    private static final Pattern ALLO_PATTERN =
        Pattern.compile("(\\d+)( R \\d+)?");

    @Help("ALLO <SP> <size> [<SP> R <SP> <size>] - Allocate sufficient disk space to receive a file.")
    public void ftp_allo(String arg)
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

    @Help("PWD - Returns the current directory of the host.")
    public void ftp_pwd(String arg)
        throws FTPCommandException
    {
        checkLoggedIn();

        if (!arg.equals("")) {
            reply(err("PWD",arg));
            return;
        }
        reply("257 \"" + _cwd + "\" is current directory");
    }

    @Help("CWD <SP> <path> - Change working directory.")
    public void ftp_cwd(String arg)
        throws FTPCommandException
    {
        checkLoggedIn();

        try {
            FsPath newcwd = absolutePath(arg);
            checkIsDirectory(newcwd);
            _cwd = _doorRootPath.relativize(newcwd).toString();
            reply("250 CWD command succcessful. New CWD is <" + _cwd + ">");
        } catch (NotDirCacheException e) {
            reply("550 Not a directory: " + arg);
        } catch (FileNotFoundCacheException e) {
            reply("550 File not found");
        } catch (CacheException e) {
            reply("451 CWD failed: " + e.getMessage());
            LOGGER.error("Error in CWD: {}", e);
        }
    }

    @Help("CDUP - Change to parent directory.")
    public void ftp_cdup(String arg)
        throws FTPCommandException
    {
        ftp_cwd("..");
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

    protected InetSocketAddress getExtendedAddressOf(String arg) throws FTPCommandException
    {
        try {
            if (arg.isEmpty()) {
                throw new FTPCommandException(501, "Syntax error: empty arguments.");
            }
            ArrayList<String> splitted = Lists.newArrayList(Splitter.on(arg.charAt(0)).split(arg));
            if (splitted.size() != 5) {
                throw new FTPCommandException(501, "Syntax error: Wrong number of arguments in '"+arg+"'.");
            }
            Protocol protocol = Protocol.find(splitted.get(1));
            if (!InetAddresses.isInetAddress(splitted.get(2))) {
                throw new FTPCommandException(501, "Syntax error: '"+splitted.get(2)+"' is no valid address.");
            }
            InetAddress address = InetAddresses.forString(splitted.get(2));
            if (!protocol.getAddressClass().equals(address.getClass())) {
                throw new FTPCommandException(501, "Protocol code does not match address: '"+arg+"'.");
            }
            int port = Integer.parseInt(splitted.get(3));
            if (port < 1 || port > 65536) {
                throw new FTPCommandException(501, "Port number '"+port+"' out of range [1,65536].");
            }
            return new InetSocketAddress(address, port);

        } catch (NumberFormatException nfe) {
            throw new FTPCommandException(501, "Syntax error: no valid port number in '"+arg+"'.");
        }
    }

    protected void setActive(InetSocketAddress address) throws FTPCommandException
    {
        if (_sessionAllPassive) {
            throw new FTPCommandException(503, "PORT and EPRT not allowed after EPSV ALL.");
        }

        _mode = Mode.ACTIVE;
        _clientDataAddress = address;
        closePassiveModeServerSocket();
    }

    @VisibleForTesting
    InetSocketAddress setPassive()
        throws FTPCommandException
    {
        try {
            if (_passiveModeServerSocket == null) {
                LOGGER.info("Opening server socket for passive mode");
                if (!Protocol.fromAddress(_localAddress.getAddress()).equals(_preferredProtocol)) {
                    Iterable<InterfaceAddress> addresses = getLocalAddressInterfaces();
                    int port = _localAddress.getPort();
                    InterfaceAddress newAddress = Iterables.find(addresses, new Predicate<InterfaceAddress>() {
                        @Override
                        public boolean apply(@Nullable InterfaceAddress input) {
                            return Protocol.fromAddress(input.getAddress()).equals(_preferredProtocol);
                        }
                    });
                    _localAddress = new InetSocketAddress((newAddress).getAddress(), port);
                }
                _passiveModeServerSocket = ServerSocketChannel.open();
                _passiveModePortRange.bind(_passiveModeServerSocket.socket(), _localAddress.getAddress());
                _mode = Mode.PASSIVE;
            }
            return (InetSocketAddress) _passiveModeServerSocket.getLocalAddress();
        } catch (NoSuchElementException e) {
            _mode = Mode.ACTIVE;
            closePassiveModeServerSocket();
            throw new FTPCommandException(522, "Protocol family not supported");
        } catch (IOException e) {
            _mode = Mode.ACTIVE;
            closePassiveModeServerSocket();
            throw new FTPCommandException(500, "Cannot enter passive mode: " + e);
        }
    }

    /**
     * Provides the addresses of (logical) interfaces that share the same
     * network interface (often a physical NIC socket) as the (logical)
     * interface to which the client connected.  The order of the addresses is
     * not guaranteed.  Typically a "single stack" machine will return a single
     * address (either IPv4 or IPv6 address) and a "dual stack" machine will
     * return both an IPv4 and an IPv6 address.
     *
     * This method exists to allow a mock of this class to isolate itself
     * from the testing machine's network configuration.
     */
    protected Iterable<InterfaceAddress> getLocalAddressInterfaces()
            throws SocketException
    {
        return NetworkInterface.getByInetAddress(_localAddress.getAddress())
                .getInterfaceAddresses();
    }

    @Help("PORT <SP> <target> - The address and port to which the server should connect.")
    public void ftp_port(String arg)
        throws FTPCommandException
    {
        checkLoggedIn();

        String[] st = arg.split(",");
        if (st.length != 6) {
            reply(err("PORT",arg));
            return;
        }

        setActive(getAddressOf(st));
        _allowDelayed = false;
        _delayedPassive = DelayedPassiveReply.NONE;
        reply(ok("PORT"));
    }

    @Help("PASV - Enter passive mode.")
    public void ftp_pasv(String arg)
        throws FTPCommandException
    {
        checkLoggedIn();
        if (_sessionAllPassive) {
            throw new FTPCommandException(503, "PASV not allowed after EPSV ALL");
        }

        /* PASV can only return IPv4 addresses.
         */
        _preferredProtocol = Protocol.IPV4;

        /* If already in passive mode then we close the previous
         * socket and allocate a new one. This is a defensive move to
         * recover from the server socket having been closed by some
         * error condition.
         */
        closePassiveModeServerSocket();
        InetSocketAddress address = setPassive();

        if (_allowDelayed) {
            _delayedPassive = DelayedPassiveReply.PASV;
            reply("200 Passive delayed.");
        } else {
            _delayedPassive = DelayedPassiveReply.NONE;
            int port = address.getPort();
            byte[] hostb = address.getAddress().getAddress();
            int[] host = new int[4];
            for (int i = 0; i < 4; i++) {
                host[i] = hostb[i] & 0_377;
            }
            reply("227 OK (" +
                          host[0] + ',' +
                          host[1] + ',' +
                          host[2] + ',' +
                          host[3] + ',' +
                          port / 256 + ',' +
                          port % 256 + ')');
        }
    }

    @Help("EPRT <SP> <target> - The extended address and port to which the server should connect.")
    public void ftp_eprt(String arg)
            throws FTPCommandException
    {
        checkIpV6();
        checkLoggedIn();

        setActive(getExtendedAddressOf(arg));
        _allowDelayed = false;
        _delayedPassive = DelayedPassiveReply.NONE;
        reply(ok("EPRT"));
    }

    @Help("EPSV - Enter extended passive mode.")
    public void ftp_epsv(String arg)
        throws FTPCommandException
    {
        checkIpV6();
        checkLoggedIn();

        if  ("ALL".equalsIgnoreCase(arg)) {
            _sessionAllPassive = true;
            reply(ok("EPSV ALL"));
            return;
        }
        if (arg.isEmpty()) {
            /* If already in passive mode then we close the previous
             * socket and allocate a new one. This is a defensive move to
             * recover from the server socket having been closed by some
             * error condition.
             */
            closePassiveModeServerSocket();
            InetSocketAddress address = setPassive();
            if (_allowDelayed) {
                _delayedPassive = DelayedPassiveReply.EPSV;
                reply("200 Passive delayed.");
            } else {
                _delayedPassive = DelayedPassiveReply.NONE;
                reply("229 Entering Extended Passive Mode (|||" + address.getPort() + "|)");
            }
        } else {
            try {
                _preferredProtocol = Protocol.find(arg);
                reply(ok("EPSV" +arg));
            } catch (NumberFormatException nfe) {
                throw new FTPCommandException(501, "Syntax error: '"+
                        arg+"' is not a valid argument for EPSV.");
            } catch (IllegalArgumentException e) {
                throw new FTPCommandException(522, "Protocol family '"+
                        arg+"'is not supported, use one of (1,2)");
            }
        }
    }

    private void checkIpV6() throws FTPCommandException
    {
        if (!_localAddress.getAddress().getClass().equals(Inet6Address.class)) {
            throw new FTPCommandException(502, "Command only supported for IPv6");
        }
    }

    @Help("MODE <SP> <mode> - Sets the transfer mode.")
    public void ftp_mode(String arg)
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


    /*
     * The MFMT MFCT and MFF commands are not standards but are described in
     * the 'draft-somers-ftp-mfxx-04' document, currently available here:
     *
     *     http://tools.ietf.org/html/draft-somers-ftp-mfxx-04
     */


    @Help("MFMT <SP> <time-val> <SP> <path> - Adjust modify timestamp")
    public void ftp_mfmt(String arg) throws FTPCommandException
    {
        checkLoggedIn();

        int spaceIndex = arg.indexOf(' ');
        if (spaceIndex == -1) {
            reply("500 missing time-val and pathname");
            return;
        }

        String pathname = arg.substring(spaceIndex + 1);
        String timeval = arg.substring(0, spaceIndex);
        long when = parseTimeval(timeval, "");

        FileAttributes updates = new FileAttributes();
        updates.setModificationTime(when);
        FileAttributes updated = updateAttributesFromPath(pathname, updates);

        String updatedTimeval =
                TIMESTAMP_FORMAT.format(new Date(updated.getModificationTime()));

        reply("213 Modify=" + updatedTimeval + "; " + pathname);
    }


    @Help("MFCT <SP> <time-val> <SP> <path> - Adjust creation timestamp")
    public void ftp_mfct(String arg) throws FTPCommandException
    {
        checkLoggedIn();

        int spaceIndex = arg.indexOf(' ');
        if (spaceIndex == -1) {
            reply("500 missing time-val and pathname");
            return;
        }

        String pathname = arg.substring(spaceIndex + 1);
        String timeval = arg.substring(0, spaceIndex);
        long when = parseTimeval(timeval, "");

        FileAttributes updates = new FileAttributes();
        updates.setCreationTime(when);
        FileAttributes updated = updateAttributesFromPath(pathname, updates);

        String updatedTimeval =
                TIMESTAMP_FORMAT.format(new Date(updated.getCreationTime()));

        reply("213 Create=" + updatedTimeval + "; " + pathname);
    }


    @Help("MFF <SP> <fact> = <value> ; [<fact> = <value> ; ...] <SP> <path> - Update facts about file or directory")
    public void ftp_mff(String arg) throws FTPCommandException
    {
        checkLoggedIn();

        int spaceIndex = arg.indexOf(' ');
        if (spaceIndex == -1) {
            reply("500 missing mff-facts and pathname");
            return;
        }

        String pathname = arg.substring(spaceIndex + 1);
        String facts = arg.substring(0, spaceIndex);

        FileAttributes updates = new FileAttributes();
        Map<String,String> changes = Splitter.on(';').omitEmptyStrings().
                withKeyValueSeparator('=').split(facts);
        for (Map.Entry<String,String> change : changes.entrySet()) {
            Fact fact = Fact.find(change.getKey());
            if (fact == null) {
                reply("504 Unsupported fact " + change.getKey());
                return;
            }
            switch (fact) {
            case MODE:
                try {
                    updates.setMode(Integer.parseInt(change.getValue(), 8));
                } catch (NumberFormatException e) {
                    reply("504 value not in octal for UNIX.mode");
                    return;
                }
                break;
            case MODIFY:
                updates.setModificationTime(parseTimeval(change.getValue(),
                        " for MODIFY"));
                break;
            case CREATE:
                updates.setCreationTime(parseTimeval(change.getValue(),
                        " for CREATE"));
                break;
            case ACCESS:
                updates.setAccessTime(parseTimeval(change.getValue(),
                        " for UNIX.atime"));
                break;
            default:
                reply("504 Unmodifable fact " + change.getKey());
                return;
            }
        }

        FileAttributes updated = updateAttributesFromPath(pathname, updates);

        StringBuilder sb = new StringBuilder("213 ");
        for (Map.Entry<String,String> change : changes.entrySet()) {
            Fact fact = Fact.find(change.getKey());
            sb.append(fact.getName()).append('=');
            switch (fact) {
            case MODE:
                sb.append(Integer.toOctalString(updated.getMode() & 0777));
                break;
            case MODIFY:
                sb.append(TIMESTAMP_FORMAT.format(new Date(updated.getModificationTime())));
                break;
            case CREATE:
                sb.append(TIMESTAMP_FORMAT.format(new Date(updated.getCreationTime())));
                break;
            case ACCESS:
                sb.append(TIMESTAMP_FORMAT.format(new Date(updated.getAccessTime())));
                break;
            }
            sb.append(';');
        }
        reply(sb.append(' ').append(pathname).toString());
    }


    private FileAttributes updateAttributesFromPath(String path, FileAttributes updates)
            throws FTPCommandException
    {
        FsPath absolutePath = absolutePath(path);
        try {
            return _pnfs.setFileAttributes(absolutePath, updates,
                    updates.getDefinedAttributes());
        } catch (FileNotFoundCacheException e) {
            throw new FTPCommandException(550, "file not found");
        } catch (CacheException e) {
            /* FIXME: we should distinguish between transitory and permanent
             *        failures; however, the CacheException hierachy doesn't
             *        make this easy.  So we mark all such failures as permanent.
             */
            throw new FTPCommandException(501, "internal problem: " +
                    e.toString());
        }
    }

    private long parseTimeval(String timeval, String errorSuffix)
            throws FTPCommandException
    {
        String fractionalPart = null;
        int dotIndex = timeval.indexOf('.');
        if (dotIndex != -1) {
            fractionalPart = timeval.substring(dotIndex);
            timeval = timeval.substring(0, dotIndex);
        }

        long when;

        try {
            when = TIMESTAMP_FORMAT.parse(timeval).getTime();

            if (dotIndex != -1) {
                float seconds = Float.parseFloat(fractionalPart);
                when += Math.floor(seconds * 1000);
            }
        } catch (NumberFormatException | ParseException e) {
            throw new FTPCommandException(501, "bad timeval" + errorSuffix);
        }

        return when;
    }


    /* The ncftp client checks for supported site commands by
     * parsing the response to the HELP SITE command.  Each line is
     * checked for the client-supported site command names, in
     * capitals, as a simple string [see strstr(3)].
     */

    @Help("The following site-specific commands are supported:\r\n" +
            "SITE <SP> HELP - Information about SITE commands\r\n" +
            "SITE <SP> BUFSIZE <SP> <size> - Set network buffer to <size>\r\n" +
            "SITE <SP> CHKSUM <SP> <value> - Fail upload if ADLER32 checksum isn't <value>\r\n" +
            "SITE <SP> CHMOD <SP> <perm> <SP> <path> - Change permission of <path> to octal value <perm>")
    public void ftp_site(String arg)
        throws FTPCommandException
    {
        checkLoggedIn();

        if (arg.equals("")) {
            reply("500 must supply the site specific command");
            return;
        }

        String args[] = arg.split(" ");

        if (args[0].equalsIgnoreCase("HELP")) {
            /* The Globus FTP client uses the SITE HELP command to discover
             * which commands are available (globus_l_ftp_client_parse_site_help
             * in globus_ftp_client_state.c).  The algorithm parses each line
             * looking for expected commands in upper-case as a simple substring
             * [see strstr(3)].  For some commands it checks, if there's a
             * match, that the preceding character is not upper-case.
             *
             * We provide the same output as the HELP SITE command.
             */
            ftp_help("SITE");
        } else if (args[0].equalsIgnoreCase("BUFSIZE")) {
            if (args.length != 2) {
                reply("500 command must be in the form 'SITE BUFSIZE <number>'");
                return;
            }
            ftp_sbuf(args[1]);
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
        } else if (args[0].equalsIgnoreCase("CLIENTINFO")) {
            if (args.length < 2) {
                reply("500 command must be in the form 'SITE CLIENTINFO <info>'");
                return;
            }
            doClientinfo(arg.substring(11));
        } else {
            reply("500 Unknown SITE command");
        }
    }

    @Help("CKSM <SP> <alg> <SP> <off> <SP> <len> <SP> <path> - Return checksum of file.")
    public void ftp_cksm(String arg)
        throws FTPCommandException
    {
        checkLoggedIn();

        List<String> st = Splitter.on(' ').limit(4).splitToList(arg);
        if (st.size() != 4) {
            reply("500 Unsupported CKSM command operands");
            return;
        }
        String algo = st.get(0);
        String offset = st.get(1);
        String length = st.get(2);
        String path = st.get(3);

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

    @Help("SCKS <SP> <alg> <SP> <value> - Fail next upload if checksum does not match.")
    public void ftp_scks(String arg)
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

            attributes =
                _pnfs.getFileAttributes(absolutePath(path).toString(), EnumSet.of(PNFSID, TYPE));

            if (attributes.getFileType() == FileType.LINK) {
                reply("502 chmod of symbolic links is not yet supported.");
                return;
            }

            FileAttributes newAttributes = new FileAttributes();
            newAttributes.setMode(newperms);
            _pnfs.setFileAttributes(attributes.getPnfsId(), newAttributes);

            reply("250 OK");
        } catch (NumberFormatException ex) {
            reply("501 permissions argument must be an octal integer");
        } catch (PermissionDeniedCacheException e) {
            reply("550 Permission denied");
        } catch (CacheException ce) {
            reply("550 Permission denied, reason: " + ce);
        }
    }


    public void doClientinfo(String description)
    {
        LOGGER.debug("client-info: {}", description);
        Map<String,String> items = Splitter.on(';').omitEmptyStrings().
                withKeyValueSeparator(Splitter.on('=').trimResults(CharMatcher.is('\"'))).
                split(description);
        String appname = items.get("appname");
        if (appname != null && appname.equals("globusonline-fxp")) {
            /* GlobusOnline transfer client expects an upload to have a
             * MD5 checksum available, without explicitly saying this, see:
             *
             *     https://support.globus.org/entries/23563241
             *
             * As a work-around, we do the equivalent to the 'OPTS CKSM MD5'
             * command.  Note that this requires on-transfer=yes on the
             * target pool as on-write will ignore this setting.
             */
            try {
                _optCheckSumFactory =
                        ChecksumFactory.getFactory(ChecksumType.MD5_TYPE);
            } catch (NoSuchAlgorithmException e) {
                throw new RuntimeException(e.getMessage(), e);
            }
        }

        reply("250 OK");
    }

    @Help("SBUF <SP> <size> - Set buffer size.")
    public void ftp_sbuf(String arg)
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

    @Help("ERET <SP> <mode> <SP> <path> - Extended file retrieval.")
    public void ftp_eret(String arg)
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
                LOGGER.info("Error return invoking: {}({})", m.getName(), arg);
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

    @Help("ESTO <SP> <mode> <SP> <path> - Extended store.")
    public void ftp_esto(String arg)
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
                LOGGER.info("Esto invoking: {} ({})", m.getName(), arg);
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
    // void method ftp_esto_"MODE"(String arg)
    //
    public void ftp_esto_a(String arg)
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
            LOGGER.error(err);
            reply(err);
            return;
        }
        if (asm_offset != 0) {
            reply("504 ESTO Adjusted Store Mode does not work with nonzero offset: " + offset);
            return;
        }
        LOGGER.info("Performing esto in \"a\" mode with offset = {}", offset);
        ftp_stor(filename);
    }

    //
    // this is the implementation for the ERET with mode "p"
    // "p" is partiall retrieve mode
    // other modes identified by string "MODE" can be implemented by adding
    // void method ftp_eret_"MODE"(String arg)
    //
    public void ftp_eret_p(String arg)
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
            LOGGER.error(err);
            reply(err);
            return;
        }
        try {
            prm_size = Long.parseLong(size);
        } catch (NumberFormatException e) {
            String err = "501 ERET Partial Retrieve Mode: invalid size " + offset;
            LOGGER.error(err);
            reply(err);
            return;
        }
        LOGGER.info("Performing eret in \"p\" mode with offset = {} size = {}",
                offset, size);
        ftp_retr(filename);
    }

    @Help("RETR <SP> <path> - Retrieve a copy of the file.")
    public void ftp_retr(String arg)
        throws FTPCommandException
    {
        try {
            if (_skipBytes > 0){
                reply("504 RESTART not implemented");
                return;
            }
            retrieve(arg, prm_offset, prm_size, _mode,
                     _xferMode, _parallel, _clientDataAddress, _bufSize,
                     _delayedPassive, _preferredProtocol.getProtocolFamily(),
                     _delayedPassive == DelayedPassiveReply.NONE ? 1 : 2);
        } finally {
            prm_offset=-1;
            prm_size=-1;
        }
    }

    protected synchronized FtpTransfer getTransfer()
    {
        return _transfer;
    }

    protected synchronized void setTransfer(FtpTransfer transfer)
    {
        _transfer = transfer;
        notifyAll();
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
     * @param delayedPassive whether to generate delayed passive reply in passive mode
     * @param protocolFamily Protocol family to use for passive mode
     * @param version       The mover version to use for the transfer
     */
    private void retrieve(String file, long offset, long size,
                          Mode mode, String xferMode,
                          int parallel,
                          InetSocketAddress client, int bufSize,
                          DelayedPassiveReply delayedPassive,
                          ProtocolFamily protocolFamily, int version)
        throws FTPCommandException
    {
        /* Check preconditions.
         */
        checkLoggedIn();

        if (file.isEmpty()) {
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
                            delayedPassive,
                            protocolFamily,
                            version);
        try {
            LOGGER.info("retrieve user={}", getUser());
            LOGGER.info("retrieve addr={}", _remoteAddress);

            transfer.readNameSpaceEntry();
            transfer.createTransactionLog();
            transfer.checkAndDeriveOffsetAndSize();

            /* Transfer the file. As there is a delay between the
             * point when a pool goes offline and when the pool
             * manager updates its state, we will retry failed
             * transfer a few times.
             */
            transfer.createAdapter();
            transfer.selectPoolAndStartMover(_ioQueueName, _readRetryPolicy);
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
            case CacheException.NO_POOL_CONFIGURED:
                transfer.abort(552, "No read pool configured for this transfer", e);
                break;
            case CacheException.FILE_NOT_IN_REPOSITORY:
            case CacheException.NO_POOL_ONLINE:
                transfer.abort(452, "File is unavailable", e);
                break;
            case CacheException.INVALID_ARGS:
                transfer.abort(501, "Invalid request: " + e.getMessage(), e);
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
            LOGGER.error("Retrieve failed", e);
            transfer.abort(451, "Transient internal failure");
        } finally {
            _allo = 0;
        }
    }

    public abstract void startTlog(FTPTransactionLog log, String path, String action);

    @Help("STOR <SP> <path> - Tell server to start accepting data.")
    public void ftp_stor(String arg)
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

        store(arg, _mode, _xferMode, _parallel, _clientDataAddress, _bufSize,
              _delayedPassive, _preferredProtocol.getProtocolFamily(),
              _delayedPassive == DelayedPassiveReply.NONE ? 1 : 2);
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
     * @param delayedPassive whether to generate delayed passive reply in passive mode
     * @param protocolFamily Protocol family to use for passive mode
     * @param version       The mover version to use for the transfer
     */
    private void store(String file, Mode mode, String xferMode,
                       int parallel,
                       InetSocketAddress client, int bufSize,
                       DelayedPassiveReply delayedPassive,
                       ProtocolFamily protocolFamily, int version)
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
                            delayedPassive,
                            protocolFamily,
                            version);
        try {
            LOGGER.info("store receiving with mode {}", xferMode);

            transfer.createNameSpaceEntry();
            transfer.createTransactionLog();
            transfer.setChecksum(_checkSum);

            transfer.createAdapter();
            transfer.selectPoolAndStartMover(_ioQueueName, _writeRetryPolicy);
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
            case CacheException.NO_POOL_CONFIGURED:
                transfer.abort(552, "No write pool configured for this transfer", e);
                break;
            case CacheException.NO_POOL_ONLINE:
                transfer.abort(452, "No write pool available", e);
                break;
            case CacheException.INVALID_ARGS:
                transfer.abort(501, "Invalid request: " + e.getMessage(), e);
                break;
            default:
                transfer.abort(451, "Operation failed: " + e.getMessage(), e);
                break;
            }
        } catch (RuntimeException e) {
            LOGGER.error("Store failed", e);
            transfer.abort(451, "Transient internal failure");
        } finally {
            _checkSumFactory = null;
            _checkSum = null;
            _allo = 0;
        }
    }

    @Help("SIZE <SP> <path> - Return the size of a file.")
    public void ftp_size(String arg)
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

    @Help("MDTM <SP> <path> - Return the last-modified time of a specified file.")
    public void ftp_mdtm(String arg)
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
                LOGGER.warn("Timeout in MDTM: {}", e);
                break;
            default:
                reply("451 Internal failure: " + e.getMessage());
                LOGGER.error("Error in MDTM: {}", e);
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
            replyDelayedPassive(_delayedPassive, (InetSocketAddress) _passiveModeServerSocket.getLocalAddress());
            reply("150 Openening ASCII mode data connection", false);
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
                LOGGER.warn("Got I/O exception closing socket: {}",
                        e.getMessage());
            }
            _dataSocket = null;
        }
    }

    @Help("LIST [<SP> <path>] - Returns information on <path> or the current working directory.")
    public void ftp_list(String arg)
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
            LOGGER.warn("Error in LIST: {}", e.getMessage());
        }
    }


    private static final Pattern GLOB_PATTERN =
        Pattern.compile("[*?]");

    @Help("NLST [<SP> <path>] - Returns a list of file names in a specified directory.")
    public void ftp_nlst(String arg)
        throws FTPCommandException
    {
        checkLoggedIn();

        if (arg.equals("")) {
            arg = ".";
        }

        try {
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
            LOGGER.warn("Error in NLST: {}", e.getMessage());
        }
    }

    @Help("MLST [<SP> <path>] - Returns data about exactly one object.")
    public void ftp_mlst(String arg)
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
            LOGGER.warn("Error in MLST: {}", e.getMessage());
        }
    }

    @Help("MLSD [<SP> <path>] - Lists the contents of a directory.")
    public void ftp_mlsd(String arg)
        throws FTPCommandException
    {
        checkLoggedIn();

        try {
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
            LOGGER.warn("Error in MLSD: {}", e.getMessage());
        }
    }

    @Help("RNFR <SP> <path> - Rename from <path>.")
    public void ftp_rnfr(String arg) throws FTPCommandException {
        checkLoggedIn();

        try {
            _filepath = null;

            if (Strings.isNullOrEmpty(arg)) {
                throw new FTPCommandException(500, "Missing file name for RNFR");
            }

            FsPath path = absolutePath(arg);
            _pnfs.getPnfsIdByPath(path.toString());
            _filepath = path;

            reply("350 File exists, ready for destination name RNTO");
        } catch (FileNotFoundCacheException e) {
            throw new FTPCommandException(550, "File not found");
        } catch (CacheException e) {
            throw new FTPCommandException(451, "Transient error: " + e.getMessage());
        }
    }

    @Help("RNTO <SP> <path> - Rename file specified by RNTO to <path>.")
    public void ftp_rnto(String arg) throws FTPCommandException {
        checkLoggedIn();

        try {
            if (_filepath == null) {
                throw new FTPCommandException(503, "RNTO must be preceeded by RNFR");
            }
            if (Strings.isNullOrEmpty(arg)) {
                throw new FTPCommandException(500, "missing destination name for RNTO");
            }

            FsPath newName = absolutePath(arg);
            _pnfs.renameEntry(_filepath.toString(), newName.toString(), true);

            reply("250 File renamed");
        } catch (PermissionDeniedCacheException e) {
            throw new FTPCommandException(550, "Permission denied");
        } catch (CacheException e) {
            throw new FTPCommandException(451, "Transient error: " + e.getMessage());
        } finally {
            _filepath = null;
        }
    }
    //----------------------------------------------
    // DCAU: data channel authtication
    // currentrly ( 07.04.2008 ) it's not supported
    //----------------------------------------------
    @Help("DCAU <SP> <enable> - Data channel authentication.")
    public void ftp_dcau(String arg)
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
    @Help("QUIT - Disconnect.")
    public void ftp_quit(String arg)
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
            joinTransfer();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        throw new CommandExitException("", 0);
    }

    // --------------------------------------------
    // BYE: synonym for QUIT
    // ---------------------------------------------
    @Help("BYE - Disconnect.")
    public void ftp_bye( String arg ) throws CommandExitException
    {
        ftp_quit(arg);
    }

    // --------------------------------------------
    // ABOR: close data channels, but leave command channel open
    // ---------------------------------------------
    @Help("ABOR - Abort transfer.")
    public void ftp_abor(String arg)
        throws FTPCommandException
    {
        checkLoggedIn();

        FtpTransfer transfer = getTransfer();
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
                _cellEndpoint.sendMessage(msg, this, _executor, _timeout);
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
                LOGGER.error("PerfMarkerEngine got exception {}",
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
                    LOGGER.error("Performance marker engine received unexcepted status from mover: {}",
                            status);
                }
            } else if (msg instanceof Exception) {
                LOGGER.warn("Performance marker engine: {}",
                        ((Exception) msg).getMessage());
            } else if (msg instanceof String) {
                /* Typically this is just an error message saying the
                 * mover is gone.
                 */
                LOGGER.info("Performance marker engine: {}", msg);
            } else {
                LOGGER.error("Performance marker engine: {}",
                        msg.getClass().getName());
            }
        }

        public long getBytesTransferred()
        {
            return _perfMarkersBlock.getBytesTransferred();
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
     * @param socketAddress the address and port on which we listen
     */
    protected void replyDelayedPassive(DelayedPassiveReply format, InetSocketAddress socketAddress)
    {
        InetAddress address = socketAddress.getAddress();
        Protocol protocol = Protocol.fromAddress(address);
        switch (format) {
        case NONE:
            break;
        case PASV:
            checkArgument(protocol == Protocol.IPV4, "PASV required IPv4 data channel.");
            int port = socketAddress.getPort();
            byte[] host = address.getAddress();
            reply(String.format("127 PORT (%d,%d,%d,%d,%d,%d)",
                                (host[0] & 0377),
                                (host[1] & 0377),
                                (host[2] & 0377),
                                (host[3] & 0377),
                                (port / 256),
                                (port % 256)), false);
            break;
        case EPSV:
            reply(String.format("129 Entering Extended Passive Mode (|%d|%s|%d|)",
                                protocol.getCode(), InetAddresses.toAddrString(address), socketAddress.getPort()));
            break;
        }
    }

    /**
     * Implements GridFTP v2 GET operation.
     *
     * @param arg the argument string of the GET command.
     */
    @Help("GET <SP> <args> - Flexible transfer of data to client.")
    public void ftp_get(String arg)
    {
        try {
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
                _preferredProtocol = Protocol.IPV4;
                _delayedPassive = DelayedPassiveReply.PASV;
                setPassive();
            }

            if (parameters.containsKey("port")) {
                _delayedPassive = DelayedPassiveReply.NONE;
                setActive(getAddressOf(parameters.get("port").split(",")));
            }

            /* Now do the transfer...
             */
            retrieve(parameters.get("path"), prm_offset, prm_size, _mode,
                     _xferMode, _parallel, _clientDataAddress, _bufSize,
                     _delayedPassive, _preferredProtocol.getProtocolFamily(), 2);
        } catch (FTPCommandException e) {
            reply(String.valueOf(e.getCode()) + ' ' + e.getReply());
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
    @Help("PUT <SP> <args> - Flexible transfer of data to server.")
    public void ftp_put(String arg)
    {
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
                _preferredProtocol = Protocol.IPV4;
                _delayedPassive = DelayedPassiveReply.PASV;
                setPassive();
            }

            if (parameters.containsKey("port")) {
                _delayedPassive = DelayedPassiveReply.NONE;
                setActive(getAddressOf(parameters.get("port").split(",")));
            }

            /* Now do the transfer...
             */
            store(parameters.get("path"), _mode, _xferMode, _parallel,  _clientDataAddress,
                  _bufSize, _delayedPassive, _preferredProtocol.getProtocolFamily(), 2);
        } catch (FTPCommandException e) {
            reply(String.valueOf(e.getCode()) + ' ' + e.getReply());
        }
    }

    private void sendRemoveInfoToBilling(FsPath path) {
        try {
            DoorRequestInfoMessage infoRemove =
                new DoorRequestInfoMessage(_cellAddress.toString(), "remove");
            infoRemove.setSubject(_subject);
            infoRemove.setPath(path);
            infoRemove.setClient(_clientDataAddress.getAddress().getHostAddress());

            _billingStub.notify(infoRemove);
        } catch (NoRouteToCellException e) {
            LOGGER.error("Can't send remove message to billing database: {}",
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
                case CREATE:
                    attributes.add(CREATION_TIME);
                    attributes.addAll(_pdp.getRequiredAttributes());
                    break;
                case MODIFY:
                    attributes.add(MODIFICATION_TIME);
                    attributes.addAll(_pdp.getRequiredAttributes());
                    break;
                case CHANGE:
                    attributes.add(CHANGE_TIME);
                    attributes.addAll(_pdp.getRequiredAttributes());
                    break;
                case ACCESS:
                    attributes.add(ACCESS_TIME);
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
                    case CREATE:
                        access =
                            _pdp.canGetAttributes(_subject, dirAttr, attr,
                                                  EnumSet.of(CREATION_TIME));
                        if (access == AccessType.ACCESS_ALLOWED) {
                            printCreateFact(attr);
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
                    case CHANGE:
                        access =
                            _pdp.canGetAttributes(_subject, dirAttr, attr,
                                                  EnumSet.of(CHANGE_TIME));
                        if (access == AccessType.ACCESS_ALLOWED) {
                            printChangeFact(attr);
                        }
                        break;
                    case ACCESS:
                        access =
                            _pdp.canGetAttributes(_subject, dirAttr, attr,
                                                  EnumSet.of(ACCESS_TIME));
                        if (access == AccessType.ACCESS_ALLOWED) {
                            printAccessFact(attr);
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

        /** Writes a RFC 3659 create fact to a writer. */
        private void printCreateFact(FileAttributes attr)
        {
            long time = attr.getCreationTime();
            printFact(Fact.CREATE, TIMESTAMP_FORMAT.format(new Date(time)));
        }

        /** Writes a RFC 3659 modify fact to a writer. */
        private void printModifyFact(FileAttributes attr)
        {
            long time = attr.getModificationTime();
            printFact(Fact.MODIFY, TIMESTAMP_FORMAT.format(new Date(time)));
        }

        /** Writes UNIX.ctime fact to a writer. */
        private void printChangeFact(FileAttributes attr)
        {
            long time = attr.getChangeTime();
            printFact(Fact.CHANGE, TIMESTAMP_FORMAT.format(new Date(time)));
        }

        /** Writes UNIX.atime fact to a writer. */
        private void printAccessFact(FileAttributes attr)
        {
            long time = attr.getAccessTime();
            printFact(Fact.ACCESS, TIMESTAMP_FORMAT.format(new Date(time)));
        }

        /** Writes a RFC 3659 size fact to a writer. */
        private void printSizeFact(FileAttributes attr)
        {
            printFact(Fact.SIZE, attr.getSize());
        }

        /** Writes a RFC 3659 UNIX.Owner fact to a writer. */
        private void printOwnerFact(FileAttributes attr)
        {
            printFact(Fact.OWNER, attr.getOwner());
        }

        /** Writes a RFC 3659 UNIX.group fact to a writer. */
        private void printGroupFact(FileAttributes attr)
        {
            printFact(Fact.GROUP, attr.getGroup());
        }

        /** Writes a RFC 3659 UNIX.mode fact to a writer. */
        private void printModeFact(FileAttributes attr)
        {
            /* ncftp client v3.2.5 requires that the mode fact starts with a '0'
             * otherwise the value is interpreted as a base-10 value.
             *
             * This seems to be consistent with what Globus server does.
             */
            printFact(Fact.MODE,
                      "0" + Integer.toOctalString(attr.getMode() & MODE_MASK));
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
            _out.print(_doorRootPath.relativize(path));
        }
    }
}
