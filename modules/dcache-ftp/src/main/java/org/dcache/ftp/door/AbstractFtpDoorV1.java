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
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import com.google.common.net.InetAddresses;
import com.google.common.primitives.Ints;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;

import java.io.BufferedOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.StringWriter;
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
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.StandardProtocolFamily;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;
import java.security.Principal;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Queue;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TimeZone;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import diskCacheV111.doors.LineBasedInterpreter;
import diskCacheV111.services.space.Space;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.CheckStagePermission;
import diskCacheV111.util.FileExistsCacheException;
import diskCacheV111.util.FileNotFoundCacheException;
import diskCacheV111.util.FsPath;
import diskCacheV111.util.MissingResourceCacheException;
import diskCacheV111.util.NotDirCacheException;
import diskCacheV111.util.NotFileCacheException;
import diskCacheV111.util.PermissionDeniedCacheException;
import diskCacheV111.util.PnfsHandler;
import diskCacheV111.util.PnfsId;
import diskCacheV111.util.TimeoutCacheException;
import diskCacheV111.vehicles.DoorCancelledUploadNotificationMessage;
import diskCacheV111.vehicles.DoorRequestInfoMessage;
import diskCacheV111.vehicles.DoorTransferFinishedMessage;
import diskCacheV111.vehicles.GFtpProtocolInfo;
import diskCacheV111.vehicles.GFtpTransferStartedMessage;
import diskCacheV111.vehicles.IoDoorEntry;
import diskCacheV111.vehicles.IoDoorInfo;
import diskCacheV111.vehicles.IoJobInfo;
import diskCacheV111.vehicles.Pool;
import diskCacheV111.vehicles.ProtocolInfo;

import dmg.cells.nucleus.CDC;
import dmg.cells.nucleus.CellAddressCore;
import dmg.cells.nucleus.CellCommandListener;
import dmg.cells.nucleus.CellEndpoint;
import dmg.cells.nucleus.CellIdentityAware;
import dmg.cells.nucleus.CellInfo;
import dmg.cells.nucleus.CellInfoProvider;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellMessageAnswerable;
import dmg.cells.nucleus.CellMessageReceiver;
import dmg.cells.nucleus.CellMessageSender;
import dmg.cells.nucleus.NoRouteToCellException;
import dmg.util.CommandExitException;
import dmg.util.LineWriter;

import org.dcache.acl.enums.AccessType;
import org.dcache.auth.GidPrincipal;
import org.dcache.auth.GroupNamePrincipal;
import org.dcache.auth.LoginReply;
import org.dcache.auth.LoginStrategy;
import org.dcache.auth.Origin;
import org.dcache.auth.Subjects;
import org.dcache.util.ColumnWriter;
import org.dcache.util.ColumnWriter.DateStyle;

import static org.dcache.auth.attributes.Activity.*;

import org.dcache.auth.attributes.Activity;
import org.dcache.auth.attributes.HomeDirectory;
import org.dcache.auth.attributes.LoginAttribute;
import org.dcache.auth.attributes.MaxUploadSize;
import org.dcache.auth.attributes.Restrictions;
import org.dcache.auth.attributes.Restriction;
import org.dcache.auth.attributes.RootDirectory;
import org.dcache.cells.CellStub;
import org.dcache.ftp.proxy.ActiveAdapter;
import org.dcache.ftp.proxy.PassiveConnectionHandler;
import org.dcache.ftp.proxy.ProxyAdapter;
import org.dcache.ftp.proxy.ProxyAdapter.Direction;
import org.dcache.ftp.TransferMode;
import org.dcache.ftp.proxy.SocketAdapter;
import org.dcache.namespace.ACLPermissionHandler;
import org.dcache.namespace.ChainedPermissionHandler;
import org.dcache.namespace.FileAttribute;
import org.dcache.namespace.FileType;
import org.dcache.namespace.PermissionHandler;
import org.dcache.namespace.PosixPermissionHandler;
import org.dcache.poolmanager.PoolManagerHandler;
import org.dcache.poolmanager.PoolManagerStub;
import org.dcache.services.login.IdentityResolverFactory;
import org.dcache.services.login.IdentityResolverFactory.IdentityResolver;
import org.dcache.services.login.RemoteLoginStrategy;
import org.dcache.space.ReservationCaches.GetSpaceTokensKey;
import org.dcache.util.Args;
import org.dcache.util.AsynchronousRedirectedTransfer;
import org.dcache.util.ByteUnit;
import org.dcache.util.CDCExecutorDecorator;
import org.dcache.util.Checksum;
import org.dcache.util.ChecksumType;
import org.dcache.util.Glob;
import org.dcache.util.LineIndentingPrintWriter;
import org.dcache.util.NetLoggerBuilder;
import org.dcache.util.PortRange;
import org.dcache.util.TimeUtils;
import org.dcache.util.TimeUtils.TimeUnitFormat;
import org.dcache.util.Transfer;
import org.dcache.util.TransferRetryPolicy;
import org.dcache.util.list.DirectoryEntry;
import org.dcache.util.list.DirectoryListPrinter;
import org.dcache.util.list.ListDirectoryHandler;
import org.dcache.vehicles.FileAttributes;
import org.dcache.vehicles.PnfsListDirectoryMessage;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;
import static java.util.Objects.requireNonNull;
import static org.dcache.acl.enums.AccessType.ACCESS_ALLOWED;
import static org.dcache.ftp.door.AnonymousPermission.ALLOW_ANONYMOUS_USER;
import static org.dcache.ftp.door.AnonymousPermission.FORBID_ANONYMOUS_USER;
import static org.dcache.namespace.FileAttribute.*;
import static org.dcache.namespace.FileType.LINK;
import static org.dcache.util.ByteUnit.MiB;
import static org.dcache.util.Exceptions.genericCheck;
import static org.dcache.util.NetLoggerBuilder.Level.INFO;
import static org.dcache.util.Strings.describe;
import static org.dcache.util.Strings.describeSize;

@Inherited
@Retention(RUNTIME)
@Target(METHOD)
@interface Help
{
    String value();
}

@Retention(RUNTIME)
@Target(METHOD)
@interface ConcurrentWithTransfer
{
}

enum AnonymousPermission
{
    ALLOW_ANONYMOUS_USER, FORBID_ANONYMOUS_USER
}

/**
 * Cancelling a transfer by some other dCache component.
 * @see ClientAbortException
 */
class CancelledUploadException extends Exception
{
}

/**
 * Exception indicating an error during processing of an FTP command.
 */
class FTPCommandException extends Exception
{
    /** FTP reply code. */
    protected final int _code;

    /**
     * Constructs a command exception with the given ftp reply code and
     * message. The message will be used for both the public FTP reply
     * string and for the exception message.
     */
    public FTPCommandException(int code, String reply)
    {
        super(requireNonNull(reply));
        _code = code;
    }

    /**
     * Constructs a command exception with the given ftp reply code and
     * message. The message will be used for both the public FTP reply
     * string and for the exception message.
     */
    public FTPCommandException(int code, String reply, Exception cause)
    {
        super(requireNonNull(reply), cause);
        _code = code;
    }

    /** Returns FTP reply code. */
    public int getCode()
    {
        return _code;
    }
}

/**
 * An FTPCommandException that indicates some request is failing due to the
 * client's behaviour after making the request.  This is distinct from requests
 * that fail for dCache-internal reasons, bad syntax, etc.  An example where
 * ClientAbortException may be used is aborting a transfer due to the client
 * tearing down the control channel.
 */
class ClientAbortException extends FTPCommandException
{
    public ClientAbortException(int code, String reply)
    {
        super(code, reply);
    }
}

public abstract class AbstractFtpDoorV1
        implements LineBasedInterpreter, CellMessageReceiver, CellCommandListener,
        CellInfoProvider, CellMessageSender, CellIdentityAware
{
    private static final long MINIMUM_PERFORMANCE_MARKER_PERIOD = 2;
    private static final long MAXIMUM_PERFORMANCE_MARKER_PERIOD = TimeUnit.MINUTES.toSeconds(5);
    private static final ImmutableMap<ProtocolFamily,String> PROTOCOLFAMILY_TO_STRING = ImmutableMap.<ProtocolFamily,String>builder()
            .put(StandardProtocolFamily.INET, "IP v4")
            .put(StandardProtocolFamily.INET6, "IP v6")
            .build();

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractFtpDoorV1.class);
    private static final Timer TIMER = new Timer("Performance marker timer", true);
    private static final Logger ACCESS_LOGGER = LoggerFactory.getLogger("org.dcache.access.ftp");
    protected FtpDoorSettings _settings;

    protected InetSocketAddress _localSocketAddress;
    protected InetSocketAddress _proxySocketAddress;
    protected InetSocketAddress _remoteSocketAddress;
    protected CellAddressCore _cellAddress;
    protected CellEndpoint _cellEndpoint;
    private LoadingCache<GetSpaceTokensKey, long[]> _spaceDescriptionCache;
    private LoadingCache<String,Optional<Space>> _spaceLookupCache;
    protected Executor _executor;
    private IdentityResolverFactory _identityResolverFactory;
    private IdentityResolver _identityResolver;
    private EnumSet<WorkAround> _activeWorkarounds = EnumSet.noneOf(WorkAround.class);
    private String _clientInfo;
    private boolean _logAbortedTransfers;

    private enum WorkAround
    {
        /* If globus-url-copy is organising a third-party copy then it will
         * issue a QUIT command to the destination endpoint before
         * disconnecting.  For the source endpoint and all second-party copies
         * no QUIT command is issued; it simply closes the control channel's
         * TCP connection.
         *
         * The support in globus-url-copy support for QUIT is broken: it does
         * not wait for dCache's response, but closes the TCP connection
         * (almost) immediately after issuing the command.  Therefore, when
         * dCache sends the QUIT command response, the client always sends a
         * RST packet in response.
         *
         * There is a race condition between dCache receiving the FIN packet
         * and receiving the RST packet (from dCache sending the QUIT command
         * response).
         *
         * If dCache receives the FIN before the RST then the connection is in
         * CLOSE_WAIT when receiving the RST packet and the connection moves to
         * CLOSED without any issue.
         *
         * If the RST is received first then the connection is in state
         * ESTABLISHED when receiving the RST packet, which causes any
         * subsequent read to trigger an IOException.
         *
         * As a work-around, we suppress any output from the QUIT command and
         * close our end of the TCP connection once any pending transfers have
         * completed.
         */
        NO_REPLY_ON_QUIT
    }


    /**
     * Simple class to allow easy accumulation of space usage.
     */
    private class SpaceAccount
    {
        private final long used;
        private final long available;
        private final long allocated;
        private final long total;

        public SpaceAccount(Space space)
        {
            used = space.getUsedSizeInBytes();
            available = space.getAvailableSpaceInBytes();
            allocated = space.getAllocatedSpaceInBytes();
            total = space.getSizeInBytes();
        }

        public SpaceAccount(long used, long available, long allocated, long total)
        {
            this.used = used;
            this.available = available;
            this.allocated = allocated;
            this.total = total;
        }

        public SpaceAccount()
        {
            used = 0L;
            available = 0L;
            allocated = 0L;
            total = 0L;
        }

        public SpaceAccount plus(SpaceAccount other)
        {
            long combinedUsed = used + other.used;
            long combinedAvailable = available + other.available;
            long combinedAllocated = allocated + other.allocated;
            long combinedTotal = total + other.total;
            return new SpaceAccount(combinedUsed, combinedAvailable, combinedAllocated, combinedTotal);
        }

        public long getUsed()
        {
            return used;
        }

        public long getAvailable()
        {
            return available;
        }

        public long getAllocated()
        {
            return allocated;
        }

        public long getTotal()
        {
            return total;
        }
    }

    public enum ReplyType
    {
        CLEAR, MIC, ENC, CONF, TLS
    }

    protected class CommandRequest
    {
        private final String commandLine;
        private final String commandLineDescription;
        private final String name;
        private final String arg;
        private final Method method;
        private final Object commandContext;
        private final ReplyType replyType;
        private final CommandRequest originalRequest = _currentRequest;
        private boolean captureReply;
        private List<String> delayedReplies;
        private boolean hasProxyRequest;

        public CommandRequest(String commandLine, ReplyType replyType, Object commandContext)
        {
            this.replyType = replyType;
            this.commandContext = commandContext;

            // Every FTP command is 3 or 4 characters
            if (commandLine.length() < 3) {
                this.commandLine = commandLine;
                commandLineDescription = commandLine;
                name = null;
                arg = null;
                method = null;
            } else {
                int l = (commandLine.length() == 3 || commandLine.charAt(3) == ' ') ? 3 : 4;
                name = commandLine.substring(0,l).toLowerCase();
                arg = commandLine.length() > l + 1 ? commandLine.substring(l + 1) : "";
                method = _methodDict.get(name);

                this.commandLine = (name.equals("adat") || name.equals("pass")) && !arg.isEmpty()
                        ? commandLine.substring(0, 4) + " ..."
                        : commandLine;

                if (replyType == ReplyType.CLEAR) {
                    commandLineDescription = this.commandLine;
                } else {
                    commandLineDescription = replyType.name() + "{" + this.commandLine + "}";
                }
            }

            if (originalRequest != null) {
                originalRequest.setHasProxyRequest();
            }
        }

        public void setHasProxyRequest()
        {
            hasProxyRequest = true;
            captureReply = false;
        }

        public boolean hasProxyRequest()
        {
            return hasProxyRequest;
        }

        public boolean isConcurrentCommand()
        {
            return method != null && method.isAnnotationPresent(ConcurrentWithTransfer.class);
        }

        public String getCommandLineDescription()
        {
            return commandLineDescription;
        }

        public String getName()
        {
            return name;
        }

        public ReplyType getReplyType()
        {
            return replyType;
        }

        public CommandRequest getOriginalRequest()
        {
            return originalRequest;
        }

        public boolean isReplyCapturing()
        {
            return captureReply;
        }

        public void storeReply(String reply)
        {
            if (delayedReplies == null) {
                delayedReplies = new ArrayList<>();
            }

            delayedReplies.add(reply);
        }

        /**
         * Run the command and capture the output.  Subsequent call to
         * {@code #run} returns output to client.
         */
        public void runCapturingReply() throws CommandExitException
        {
            if (!captureReply) {
                captureReply = true;
                runCommand();
            }
        }

        /**
         * Emit captured output if command has captured output, otherwise
         * run the command and return output to client.
         */
        public void run() throws CommandExitException
        {
            if (hasProxyRequest) {
                // do nothing: the proxy handles all output
            } else if (captureReply) {
                captureReply = false;
                if (delayedReplies != null) {
                    for (String reply : delayedReplies) {
                        reply(this, reply);
                    }
                }
            } else {
                runCommand();
            }
        }

        private void runCommand() throws CommandExitException
        {
            _lastCommand = commandLine;
            _commandCounter++;

            try {
                checkFTPCommand(method != null, 500, "'%s': command not understood", commandLine);

                try {
                    _currentRequest = this;
                    checkCommandAllowed(this, commandContext);
                    method.invoke(AbstractFtpDoorV1.this, new Object[]{arg});
                } catch (InvocationTargetException ite) {
                    //
                    // is thrown if the underlying method
                    // actively throws an exception.
                    //
                    Throwable te = ite.getTargetException();
                    Throwables.throwIfInstanceOf(te, FTPCommandException.class);
                    Throwables.throwIfInstanceOf(te, CommandExitException.class);
                    LOGGER.error("FTP command '{}' got exception", commandLine, te);
                    throw new FTPCommandException(500, "Operation failed due to internal error: " +
                          te.getMessage());
                } catch (RuntimeException | IllegalAccessException e) {
                    LOGGER.error("This is a bug. Please report it.", e);
                    throw new FTPCommandException(500, "Operation failed due to internal error: " + e.getMessage());
                } finally {
                    _currentRequest = null;
                }
            } catch (FTPCommandException e) {
                reply(this, e.getCode() + " " + e.getMessage());
            } finally {
                if (name == null || !name.equals("rest")) {
                    _skipBytes = 0;
                }
            }
        }

        @Override
        public String toString()
        {
            return commandLine;
        }
    }


    /**
     * Enumeration type for representing the connection mode.
     *
     * For PASSIVE transfers the client establishes the data
     * connection.
     *
     * For ACTIVE transfers dCache establishes the data connection.
     *
     * When INVALID, the client most reset the mode.
     *
     * Depending on the values of _isProxyRequiredOnActive and
     * _isProxyRequiredOnPassive, the data connection with the client
     * will be established either to an adapter (proxy) at the FTP
     * door, or to the pool directly.
     */
    protected enum Mode
    {
        PASSIVE, ACTIVE, INVALID
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
        UID("UNIX.uid"),
        GID("UNIX.gid"),
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
    public enum Protocol {

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
        "PASV AllowDelayed",
        "MLSC"
        /*
         * do not publish DCAU as supported feature. This will force
         * some clients to always encrypt data channel
         */
        // "DCAU"
    };

    private static final int DEFAULT_DATA_PORT = 20;

    /**
     * The maximum number of retries done on write. Must be one to
     * avoid that empty replicas are left on pools.
     */
    private static final int MAX_RETRIES_WRITE = 1;

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
    protected LineWriter _out;

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

    protected InetAddress _internalInetAddress;

    protected PassiveConnectionHandler _clientConnectionHandler;

    private final Map<String,Method>  _methodDict =
        new HashMap<>();
    private final Map<String,Help>  _helpDict = new HashMap<>();
    private final Queue<CommandRequest> _pendingCommands = new ArrayDeque<>();

    protected int            _commandCounter;
    protected String         _lastCommand    = "<init>";
    protected CommandRequest _currentRequest;
    private boolean _isHello = true;

    protected InetSocketAddress _clientDataAddress;
    protected volatile Socket _dataSocket;

    // added for the support or ERET with partial retrieve mode
    protected long prm_offset = -1;
    protected long prm_size = -1;


    protected long   _skipBytes;

    protected boolean _confirmEOFs;

    private boolean _subjectLogged;
    protected Subject _subject;
    protected Restriction _doorRestriction;
    private OptionalLong _maximumUploadSize = OptionalLong.empty();
    protected Restriction _authz = Restrictions.denyAll();
    protected FsPath _userRootPath = FsPath.ROOT;
    protected FsPath _doorRootPath = FsPath.ROOT;
    protected FsPath _userHomePath = FsPath.ROOT;
    protected String _cwd = "/";    // Relative to _doorRootPath
    protected FsPath _filepath; // Absolute filepath to the file to be renamed
    protected PnfsId _fileId; // Id of the file to be renamed
    private String _symlinkPath; // User-supplied path of new symlink
    protected TransferMode _xferMode = TransferMode.MODE_S;
    protected PoolManagerHandler _poolManagerHandler;
    protected PoolManagerStub _poolManagerStub;
    protected CellStub _billingStub;
    protected CellStub _poolStub;
    protected CellStub _gPlazmaStub;
    protected TransferRetryPolicy _readRetryPolicy;
    protected TransferRetryPolicy _writeRetryPolicy;

    protected KafkaProducer _kafkaProducer;
    private volatile boolean _sendToKafka;

    /** Tape Protection */
    protected CheckStagePermission _checkStagePermission;

    protected LoginStrategy _loginStrategy;

    protected Mode _mode = Mode.ACTIVE;

    /**
     * if _sessionAllPassive is set to true, all
     * future transfers in the session will use
     * passive mode. The flag is set by ftp_epsv.
     */
    protected boolean _sessionAllPassive = false;

    /**
     * Defines passive replies that have been delayed.
     */
    private enum DelayedPassiveReply
    {
        /** No passive reply was delayed. */
        NONE,

        /** A 127/227 reply was delayed. */
        PASV,

        /** A 129/229 reply was delayed. */
        EPSV
    }

    /**
     * Whether the FTP client has enabled delayed passive.
     */
    private boolean _allowDelayed;

    /**
     * Indicates whether and which passive reply has been delayed.
     */
    private DelayedPassiveReply _delayedPassive = DelayedPassiveReply.NONE;

    //These are the number of parallel streams to have
    //when doing mode e transfers
    protected int _parallel;
    protected int _bufSize;
    private long _performanceMarkerPeriod = 0;
    private long _checksumProgressPeriod = 0;

    private final String _ftpDoorName;
    protected Checksum _checkSum;
    protected ChecksumType _optCheckSumType;
    protected OptionalLong _allo = OptionalLong.empty();

    /** List of selected RFC 3659 facts. */
    protected Set<Fact> _currentFacts = Sets.newHashSet(
            Fact.SIZE, Fact.MODIFY, Fact.TYPE, Fact.UNIQUE, Fact.PERM,
            Fact.OWNER, Fact.GROUP, Fact.UID, Fact.GID, Fact.MODE );

    /**
     * Encapsulation of an FTP transfer.
     */
    protected class FtpTransfer extends AsynchronousRedirectedTransfer<GFtpTransferStartedMessage>
    {
        private final Mode _mode;
        private final TransferMode _xferMode;
        private final int _parallel;
        private final InetSocketAddress _client;
        private final int _bufSize;
        private final DelayedPassiveReply _delayedPassive;
        private final ProtocolFamily _protocolFamily;
        private final int _version;
        private final CommandRequest _request = AbstractFtpDoorV1.this._currentRequest;
        private final Instant whenCreated = Instant.now();

        private Optional<Instant> whenMoverStarted = Optional.empty();
        private long _offset;
        private long _size;


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
                           TransferMode xferMode,
                           int parallel,
                           InetSocketAddress client,
                           int bufSize,
                           DelayedPassiveReply delayedPassive,
                           ProtocolFamily protocolFamily,
                           int version)
        {
            super(AbstractFtpDoorV1.this._executor,
                  AbstractFtpDoorV1.this._pnfs,
                  AbstractFtpDoorV1.this._subject,
                  AbstractFtpDoorV1.this._authz, path);

            setCellAddress(_cellAddress);
            setClientAddress(_remoteSocketAddress);
            setCheckStagePermission(_checkStagePermission);
            setOverwriteAllowed(_settings.isOverwrite());
            setPoolManagerStub(_poolManagerStub);
            setPoolStub(AbstractFtpDoorV1.this._poolStub);
            setBillingStub(_billingStub);

            if (_sendToKafka) {
                setKafkaSender(m -> {
                   _kafkaProducer.send(new ProducerRecord<String, DoorRequestInfoMessage>(_settings.getKafkaTopic(), m));
                });
            }

            _allo.ifPresent(this::setAllocation);
            setIoQueue(_settings.getIoQueueName());

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
                    new SocketAdapter(_clientConnectionHandler, _internalInetAddress);
                break;

            case ACTIVE:
                if (_settings.isProxyRequiredOnActive()) {
                    LOGGER.info("Creating adapter for active mode");
                    _adapter =
                        new ActiveAdapter(_internalInetAddress,
                                          _client.getAddress().getHostAddress(),
                                          _client.getPort());
                }
                break;
            }

            if (_adapter != null) {
                _adapter.setMaxBlockSize(_settings.getMaxBlockSize());
                _adapter.setMode(_xferMode);
                _adapter.setDataDirection(isWrite() ? Direction.UPLOAD : Direction.DOWNLOAD);
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
            checkFTPCommand(_offset >= 0, 500, "prm offset is %d", _offset);
            checkFTPCommand(_size >= 0, 500, "prm_size is  %d", _size);
            checkFTPCommand(_offset + _size <= fileSize,
                    500, "invalid prm_offset=%d and prm_size %d for file of size %d",
                    _offset, _size, fileSize);
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
            boolean usePassivePool = !_settings.isProxyRequiredOnPassive() && _delayedPassive != DelayedPassiveReply.NONE;

            /* Construct protocol info. For backward compatibility, when
             * an adapter could be used we put the adapter address into
             * the protocol info.
             */
            GFtpProtocolInfo protocolInfo;
            if (_adapter != null) {
                protocolInfo =
                    new GFtpProtocolInfo("GFtp",
                                         _version, 0,
                                         _adapter.getInternalAddress(),
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

            if (_optCheckSumType != null) {
                protocolInfo.setChecksumType(_optCheckSumType.getName());
            }

            if (_checkSum != null) {
                protocolInfo.setChecksumType(_checkSum.getType().getName());
            }

            return protocolInfo;
        }


        public void abort(int replyCode, String msg)
        {
            abort(new FTPCommandException(replyCode, msg));
        }

        public void abort(int replyCode, String msg, Exception exception)
        {
            abort(new FTPCommandException(replyCode, msg, exception));
        }

        @Override
        protected void onQueued()
        {
            setStatus("Mover " + getPool() + "/" + getMoverId());
        }

        @Override
        protected synchronized void onRedirect(GFtpTransferStartedMessage redirect) throws FTPCommandException
        {
            if (redirect != null) {
                if (_version != 2) {
                    LOGGER.error("Received unexpected GFtpTransferStartedMessage for {}", redirect.getPnfsId());
                    return;
                }

                if (!redirect.getPnfsId().equals(getPnfsId().toString())) {
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
                checkFTPCommand(_mode != Mode.PASSIVE || redirect.getPassive() || _xferMode != TransferMode.MODE_X,
                        504, "Cannot use passive X mode");

                /* Determine the 127 response address to send back to the
                 * client. When the pool is passive, this is the address of
                 * the pool (and in this case we no longer need the
                 * adapter). Otherwise this is the address of the adapter.
                 */
                if (redirect.getPassive()) {
                    assert _delayedPassive != DelayedPassiveReply.NONE;
                    assert _mode == Mode.PASSIVE;
                    assert _adapter != null;

                    replyDelayedPassive(_request, _delayedPassive,
                                        redirect.getPoolAddress());

                    LOGGER.info("Closing adapter");
                    _adapter.close();
                    _adapter = null;
                } else if (_mode == Mode.PASSIVE) {
                    replyDelayedPassive(_request, _delayedPassive,
                                        _clientConnectionHandler.getLocalAddress());
                }
            }

            if (_adapter != null) {
                _adapter.start();
            }

            setStatus("Mover " + getPool() + "/" + getMoverId() + ": " +
                      (isWrite() ? "Receiving" : "Sending"));
            whenMoverStarted = Optional.of(Instant.now());

            reply(_request, "150 Opening BINARY data connection for " + _path);

            if (isWrite() && _xferMode == TransferMode.MODE_E && _performanceMarkerPeriod > 0) {
                _perfMarkerTask = new PerfMarkerTask(_request, getPool().getAddress(),
                        getMoverId(), _performanceMarkerPeriod / 2);
                TIMER.schedule(_perfMarkerTask, _performanceMarkerPeriod, _performanceMarkerPeriod);
            }
        }

        @Override
        protected void onFinish() throws FTPCommandException
        {
            try {
                ProxyAdapter adapter;
                synchronized (this) {
                    adapter = _adapter;
                }

                if (adapter != null) {
                    try {
                        LOGGER.info("Waiting for adapter to finish.");
                        adapter.join(300000); // 5 minutes
                        checkFTPCommand(!adapter.isAlive(), 451, "FTP proxy did not shut down");
                        checkFTPCommand(!adapter.hasError(), 451, "FTP proxy failed: %s", adapter.getError());
                    } finally {
                        LOGGER.debug("Closing adapter");
                        adapter.close();
                    }
                }

                synchronized (this) {
                    _adapter = null;
                    if (_perfMarkerTask != null) {
                        _perfMarkerTask.stop((GFtpProtocolInfo) getProtocolInfo());
                    }
                }

                notifyBilling(0, "");
                reply(_request, "226 Transfer complete.");
                setTransfer(null);
                _executor.execute(AbstractFtpDoorV1.this::runPendingCommands);
            } catch (InterruptedException e) {
                throw new FTPCommandException(451, "FTP proxy was interrupted", e);
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
        protected synchronized void onFailure(Throwable t)
        {
            if (_logAbortedTransfers && t instanceof ClientAbortException) {
                StringWriter sw = new StringWriter();
                PrintWriter pw = new LineIndentingPrintWriter(sw, "    ");
                pw.println("Control channel: remote " + describe(_remoteSocketAddress) + "; local " + describe(_localSocketAddress));
                pw.println("Work-arounds: " + _activeWorkarounds);
                if (_clientInfo != null) {
                    pw.println("Client info: " + _clientInfo);
                }
                getInfo(pw);
                String info = sw.toString();
                _log.warn("Client aborted transfer, details follow:\n{}",
                        info.substring(0, info.length()-1)); // remove trailing '\n'
            }

            if (_perfMarkerTask != null) {
                _perfMarkerTask.stop();
            }

            if (_adapter != null) {
                _adapter.close();
                _adapter = null;

                if (_mode == Mode.PASSIVE) {
                    _clientConnectionHandler.close();
                    _sessionAllPassive = false; // REVISIT see RFC 2428 Section 4.
                    AbstractFtpDoorV1.this._mode = Mode.INVALID;
                }
            }

            if (isWrite() && !(t.getCause() instanceof CancelledUploadException)) {
                if (_settings.isRemoveFileOnIncompleteTransfer()) {
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
            if (t instanceof FTPCommandException) {
                replyCode = ((FTPCommandException) t).getCode();
                replyMsg = t.getMessage();
            } else if (t instanceof MissingResourceCacheException) {
                replyCode = 452;
                replyMsg = t.getMessage();
            } else if (t instanceof RuntimeException) {
                _log.error("Possible bug detected.", t);
                replyCode = 451;
                replyMsg = "Transient internal error";
            } else {
                replyCode = 451;
                replyMsg = t.getMessage();
            }

            String msg = String.valueOf(replyCode) + " " + replyMsg;
            notifyBilling(replyCode, replyMsg);
            LOGGER.error("Transfer error: {}", msg);
            if (!(t instanceof FTPCommandException)) {
                LOGGER.debug(t.toString(), t);
            }
            reply(_request, msg);
            setTransfer(null);
            _executor.execute(AbstractFtpDoorV1.this::runPendingCommands);
        }

        @Override
        protected String explain(Throwable t)
        {
            if (t instanceof FTPCommandException) {
                return t.getMessage();
            }

            return super.explain(t);
        }

        public void getInfo(PrintWriter pw)
        {
            pw.println("Transaction: " + getTransaction());
            pw.println("Transfer command: " + _request);
            pw.println("Transfer direction: " + (isWrite() ? "UPLOAD" : "DOWNLOAD"));
            pw.println("Transfer started: " + TimeUtils.relativeTimestamp(whenCreated));
            pw.println("Mover started: " + whenMoverStarted.map(TimeUtils::relativeTimestamp).orElse("not started"));
            String dataChannel = _mode + "; mode " + _xferMode.getLabel() + "; ";
            if (_mode == Mode.ACTIVE) {
                // _parallel only has an effect if the pool is active and is not using a proxy.
                dataChannel = dataChannel + _parallel + " stream" + (_parallel == 1 ? "" : "s") + "; connecting to";
            } else {
                dataChannel = dataChannel + "expecting connections from";
            }
            pw.println("Data channel: " + dataChannel + " " + describe(_client));
            Pool pool = getPool();
            if (pool != null) {
                pw.println("Pool: " + pool.getName() + " (" + pool.getAddress() + ")");
            }
            Integer moverId = getMoverId();
            if (moverId != null) {
                pw.println("Mover ID: " + moverId);
            }
            String status = getStatus();
            if (status != null) {
                pw.println("Transfer status: " + status);
            }
            pw.println("Protocol version: " + _version);
            if (_mode == Mode.PASSIVE) {
                pw.println("IP family: " + _protocolFamily);
                if (_delayedPassive != DelayedPassiveReply.NONE) {
                    pw.println("Delayed passive: " + _delayedPassive);
                }
            }

            FileAttributes fileAttributes = getFileAttributes();
            if (fileAttributes.isDefined(SIZE)) {
                pw.println("File size: " + describeSize(fileAttributes.getSize()));
            }
            if (!isWrite() && _size > -1 && _offset > -1) {
                pw.println("File segment: " + _offset + '-' + (_offset + _size));
            }

            if (_perfMarkerTask != null) {
                pw.println("Performance markers:");
                _perfMarkerTask.getInfo(new LineIndentingPrintWriter(pw, "    "));
            }

            ProxyAdapter adapter = _adapter;
            if (adapter != null) {
                adapter.getInfo(pw);
            }
        }
    }

    protected interface CommandMethodVisitor
    {
        void acceptCommand(Method method, String name);
    }

    protected Transfer _transfer;

    public AbstractFtpDoorV1(String ftpDoorName)
    {
        _ftpDoorName = ftpDoorName;
        /**
         * RFC 3659 requires GMT
         */
        TIMESTAMP_FORMAT.setTimeZone(TimeZone.getTimeZone("GMT"));

        visitFtpCommands((Method method, String command) -> {
            _methodDict.put(command, method);
            Help help = method.getAnnotation(Help.class);
            if (help != null) {
                _helpDict.put(command, help);
            }
        });
    }

    final protected void visitFtpCommands(CommandMethodVisitor visitor)
    {
        for (Method method : getClass().getMethods()) {
            String name = method.getName();
            if (name.startsWith("ftp_")) {
                visitor.acceptCommand(method, name.substring(4));
            }
        }
    }

    protected void checkFTPCommand(boolean isOK, int code, String format, Object... arguments) throws FTPCommandException
    {
        genericCheck(isOK, m -> new FTPCommandException(code, m), format, arguments);
    }

    /**
     * Get current user's name, as used by the transaction log.
     */
    protected String getUserName()
    {
        return Subjects.getUserName(_subject);
    }

    public void setSettings(FtpDoorSettings settings)
    {
        _settings = settings;
    }

    @Override
    public void setCellEndpoint(CellEndpoint endpoint)
    {
        _cellEndpoint = endpoint;
    }

    @Override
    public void setCellAddress(CellAddressCore address)
    {
        _cellAddress = address;
    }

    public void setWriter(LineWriter writer)
    {
        _out = writer;
    }

    public void setRemoteSocketAddress(InetSocketAddress remoteAddress)
    {
        _remoteSocketAddress = remoteAddress;
    }

    public void setLocalSocketAddress(InetSocketAddress localAddress)
    {
        _localSocketAddress = localAddress;
    }

    public void setProxySocketAddress(InetSocketAddress localAddress)
    {
        _proxySocketAddress = localAddress;
    }

    public void setExecutor(Executor executor)
    {
        _executor = new CDCExecutorDecorator<>(executor);
    }

    public void setSpaceDescriptionCache(LoadingCache<GetSpaceTokensKey, long[]> cache)
    {
        _spaceDescriptionCache = cache;
    }

    public void setSpaceLookupCache(LoadingCache<String,Optional<Space>> cache)
    {
        _spaceLookupCache = cache;
    }

    public void setPoolManagerHandler(PoolManagerHandler poolManagerHandler)
    {
        _poolManagerHandler = poolManagerHandler;
    }

    public void setIdentityResolverFactory(IdentityResolverFactory factory)
    {
        _identityResolverFactory = factory;
        _identityResolver = factory.withoutSubject();
    }

    public void init() throws Exception
    {
        if (_settings.getPerformanceMarkerPeriod() > 0) {
            _performanceMarkerPeriod = _settings.getPerformanceMarkerPeriodUnit().toMillis(_settings.getPerformanceMarkerPeriod());
        }

        _logAbortedTransfers = _settings.logAbortedTransfers();

        _clientDataAddress =
            new InetSocketAddress(_remoteSocketAddress.getAddress(), DEFAULT_DATA_PORT);

        _internalInetAddress =
                (_settings.getInternalAddress() == null)
                ? InetAddress.getLocalHost()
                : InetAddress.getByName(_settings.getInternalAddress());

        _billingStub = _settings.createBillingStub(_cellEndpoint);

        if (_settings.isKafkaEnabled()) {
            _kafkaProducer = _settings.createKafkaProducer(_settings.getKafkaBootstrapServer(),
                                                           _cellAddress.toString(),
                                                           _settings.getKafkaMaxBlockMs(),
                                                           _settings.getKafkaRetries());
            _sendToKafka = _settings.isKafkaEnabled();

        }
        _poolManagerStub = _settings.createPoolManagerStub(_cellEndpoint, _cellAddress, _poolManagerHandler);
        _poolStub = _settings.createPoolStub(_cellEndpoint);
        _gPlazmaStub = _settings.createGplazmaStub(_cellEndpoint);

        _doorRestriction = _settings.isReadOnly() ? Restrictions.readOnly() : Restrictions.none();

        _loginStrategy = new RemoteLoginStrategy(_gPlazmaStub);


        /* Parallelism for mode E transfers.
         */
        _parallel = _settings.getDefaultStreamsPerClient();

        _origin = new Origin(_remoteSocketAddress.getAddress());

        _readRetryPolicy =
            new TransferRetryPolicy(_settings.getMaxRetries(), _settings.getRetryWait() * 1000, Long.MAX_VALUE);
        _writeRetryPolicy =
            new TransferRetryPolicy(MAX_RETRIES_WRITE, 0, Long.MAX_VALUE);

        _checkStagePermission = new CheckStagePermission(_settings.getStageConfigurationFilePath());
        _checkStagePermission.setAllowAnonymousStaging(_settings.isAnonymousStagingAllowed());
        buildClientConnectionHandler();

        reply("220 " + _ftpDoorName + " door ready");

        _isHello = false;
    }

    @VisibleForTesting
    protected void buildClientConnectionHandler()
    {
        checkState(_clientConnectionHandler == null);

        _clientConnectionHandler = new PassiveConnectionHandler(_localSocketAddress.getAddress(), _settings.getPortRange());
        _clientConnectionHandler.setAddressSupplier(() -> {
                    try {
                        return getLocalAddressInterfaces();
                    } catch (SocketException e) {
                        LOGGER.warn("Problem listing local interfaces: {}", e.toString());
                        return Collections.emptyList();
                    }});
        _clientConnectionHandler.setPreferredProtocol(Protocol.fromAddress(_remoteSocketAddress.getAddress()));
    }

    /**
     * Subject is logged in using the current login strategy.
     */
    protected void login(Subject subject) throws CacheException
    {
        LoginReply login = _loginStrategy.login(subject);
        acceptLogin(login.getSubject(), login.getLoginAttributes(), login.getRestriction(),
                _settings.getRoot() == null ? null : FsPath.create(_settings.getRoot()));
    }

    protected void acceptLogin(Subject mappedSubject, Set<LoginAttribute> loginAttributes,
            Restriction restriction, FsPath doorRootPath)
    {
        FsPath userRootPath = FsPath.ROOT;
        String userHomePath = "/";
        for (LoginAttribute attribute: loginAttributes) {
            if (attribute instanceof RootDirectory) {
                userRootPath = FsPath.create(((RootDirectory) attribute).getRoot());
            } else if (attribute instanceof HomeDirectory) {
                userHomePath = ((HomeDirectory) attribute).getHome();
            } else if (attribute instanceof MaxUploadSize) {
                long max = ((MaxUploadSize) attribute).getMaximumSize();
                if (!_maximumUploadSize.isPresent() || max < _maximumUploadSize.getAsLong()) {
                    _maximumUploadSize = OptionalLong.of(max);
                }
            }
        }
        _authz = Restrictions.concat(_doorRestriction, restriction);
        String cwd;
        if (doorRootPath == null) {
            doorRootPath = userRootPath;
            cwd = userHomePath;
        } else {
            if (userRootPath.hasPrefix(doorRootPath)) {
                cwd = userRootPath.chroot(userHomePath).stripPrefix(doorRootPath);
            } else {
                cwd = "/";
            }
        }

        _pnfs = _settings.createPnfsHandler(_cellEndpoint);
        _pnfs.setSubject(mappedSubject);
        _pnfs.setRestriction(_authz);
        _listSource = new ListDirectoryHandler(_pnfs);

        _subject = mappedSubject;
        _cwd = cwd;
        _doorRootPath = doorRootPath;
        _userRootPath = userRootPath;
        _userHomePath = FsPath.create(userHomePath);
        _identityResolver = _identityResolverFactory.withSubject(mappedSubject);
    }

    public static final String hh_get_door_info = "[-binary]";
    public Object ac_get_door_info(Args args)
    {
        IoDoorInfo doorInfo = new IoDoorInfo(_cellAddress);
        long[] uids = (_subject != null) ? Subjects.getUids(_subject) : new long[0];
        doorInfo.setOwner((uids.length == 0) ? "0" : Long.toString(uids[0]));
        doorInfo.setProcess("0");
        Transfer transfer = getTransfer();
        if (transfer instanceof FtpTransfer) {
            IoDoorEntry[] entries = { transfer.getIoDoorEntry() };
            doorInfo.setIoDoorEntries(entries);
            doorInfo.setProtocol("GFtp",
                    String.valueOf(((FtpTransfer)transfer).getVersion()));
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

    protected void checkCommandAllowed(CommandRequest command, Object commandContext)
            throws FTPCommandException
    {
        // all commands are allowed by default.
    }

    public void ftpcommand(String cmdline, Object commandContext, ReplyType replyType)
        throws CommandExitException
    {
        CommandRequest request = new CommandRequest(cmdline, replyType, commandContext);

        synchronized (_pendingCommands) {
            if (isTransferring() || !_pendingCommands.isEmpty()) {
                if (request.isConcurrentCommand()) {
                    request.runCapturingReply();
                }

                _pendingCommands.add(request);
                return;
            }
        }

        request.run();
    }

    void runPendingCommands()
    {
        synchronized (_pendingCommands) {
            boolean queuingCommands = isTransferring();

            List<CommandRequest> todo = new ArrayList<>(_pendingCommands);
            _pendingCommands.clear();

            for (CommandRequest request : todo) {
                if (queuingCommands) {
                    _pendingCommands.add(request);
                } else {
                    try {
                        request.run();
                    } catch (CommandExitException e) {
                        LOGGER.error("Bug detected: blocking command issued CommandExitException", e);
                    }
                    queuingCommands |= isTransferring();
                }
            }
        }
    }

    @Override
    public void shutdown()
    {
        try {
            /* In case of failure, we may have a transfer hanging around.
             */
            Transfer transfer = getTransfer();
            if (transfer instanceof FtpTransfer) {
                ((FtpTransfer)transfer).abort(new ClientAbortException(451, "Aborting transfer due to session termination"));
            }

            /*The producer consists of a pool of buffer space that holds records that haven't yet been
              transmitted to the server as well as a background I/O thread
              that is responsible for turning these records into requests and transmitting them to the cluster.
              Failure to close the producer after use will leak these resources. Hence we need to  and close Kafka Producer
             */
            //TODO _sendToKafka checks if the shutdown() method has been called and whether the producer has been closed or not.
            // currently  there is no method isClosed() in kafka API
            if (_sendToKafka) {
                _sendToKafka = false;
                _kafkaProducer.close();
            }

        } finally {
            _clientConnectionHandler.close();
            _sessionAllPassive = false; // REVISIT see RFC 2428 Section 4.

            if (ACCESS_LOGGER.isInfoEnabled()) {
                NetLoggerBuilder log = new NetLoggerBuilder(INFO, "org.dcache.ftp.disconnect").omitNullValues();
                log.add("host.remote", _remoteSocketAddress);
                log.add("session", CDC.getSession());
                log.toLogger(ACCESS_LOGGER);
            }
        }
    }

    protected void println(String str)
    {
        _out.writeLine(str);
    }

    @Override
    public void execute(String command)
            throws CommandExitException
    {
        ftpcommand(command, null, ReplyType.CLEAR);
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
            pw.println("          User  : " + user);
        }
        pw.println("Control channel : remote " + describe(_remoteSocketAddress) + "; local " + describe(_localSocketAddress));
        if (_clientInfo != null) {
            pw.println("   Client info  : " + _clientInfo);
        }
        pw.println("    Local Host  : " + _internalInetAddress);
        pw.println("  Last Command  : " + _lastCommand);
        pw.println(" Command Count  : " + _commandCounter);
        pw.println("     I/O Queue  : " + _settings.getIoQueueName());
        pw.println("  Work-arounds  : " + _activeWorkarounds);
        Transfer transfer = _transfer;
        if (transfer instanceof FtpTransfer) {
            ((FtpTransfer)transfer).getInfo(pw);
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
         Transfer transfer = getTransfer();
         if (transfer instanceof FtpTransfer) {
             ((FtpTransfer)transfer).redirect(message);
         }
     }

    public void messageArrived(DoorTransferFinishedMessage message)
    {
        LOGGER.debug("Received TransferFinished message [rc={}]",
                message.getReturnCode());
        Transfer transfer = getTransfer();
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

    public void messageArrived(DoorCancelledUploadNotificationMessage message)
    {
        Transfer transfer = getTransfer();
        if (transfer instanceof FtpTransfer && transfer.isWrite() &&
                message.getPnfsId().equals(transfer.getPnfsId())) {
            ((FtpTransfer)transfer).abort(555, message.getExplanation(),
                    new CancelledUploadException());
        }
    }

    //
    // GSS authentication
    //
    protected void reply(CommandRequest request, String answer)
    {
        if (!_isHello && request.isReplyCapturing()) {
            request.storeReply(answer);
        } else {
            logReply(request, answer);

            switch (_isHello ? ReplyType.CLEAR : request.getReplyType()) {
            case CLEAR:
            case TLS:
                println(answer);
                break;
            case MIC:
                secure_reply(request, answer, "631");
                break;
            case ENC:
                secure_reply(request, answer, "633");
                break;
            case CONF:
                secure_reply(request, answer, "632");
                break;
            }
        }
    }

    private void logReply(CommandRequest request, String response)
    {
        if (ACCESS_LOGGER.isInfoEnabled()) {
            String event = _isHello ? "org.dcache.ftp.hello" :
                    "org.dcache.ftp.response";

            if (response.startsWith("335 ADAT=")) {
                response = "335 ADAT=...";
            }

            NetLoggerBuilder log = new NetLoggerBuilder(INFO, event).omitNullValues();
            log.add("session", CDC.getSession());
            if (_isHello) {
                log.add("socket.remote", _remoteSocketAddress);
                if (_proxySocketAddress != null && !_proxySocketAddress.equals(_localSocketAddress)) {
                    log.add("socket.proxy", _proxySocketAddress);
                }
                log.add("socket.local", _localSocketAddress);
            } else {
                if (request.getReplyType() != ReplyType.CLEAR) {
                    response = request.getReplyType().name() + "{" + response + "}";
                }
                log.addInQuotes("command", request.getCommandLineDescription());
            }
            if (_subject != null && !_subjectLogged) {
                logSubject(log, _subject);
                log.add("user.mapped", _subject);
                _subjectLogged = true;
            }
            log.addInQuotes("reply", response);
            log.toLogger(ACCESS_LOGGER);
        }
    }

    protected abstract void logSubject(NetLoggerBuilder log, Subject subject);

    protected void reply(String answer)
    {
        reply(_currentRequest, answer);
    }

    protected abstract void secure_reply(CommandRequest request, String answer, String code);

    protected void checkLoggedIn(AnonymousPermission mode) throws FTPCommandException
    {
        checkFTPCommand(_subject != null, 530, "Not logged in.");
        checkFTPCommand(mode != FORBID_ANONYMOUS_USER || !Subjects.isNobody(_subject),
                554, "Anonymous usage not permitted.");
    }

    @Help("FEAT - List available features.")
    public void ftp_feat(String arg)
    {
        StringBuilder builder = new StringBuilder();
        builder.append("211-OK\r\n");
        buildFeatList(builder);
        builder.append("211 End");
        reply(builder.toString());
    }

    protected StringBuilder buildFeatList(StringBuilder builder)
    {
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
        return builder;
    }

    public void opts_retr(String opt) throws FTPCommandException
    {
        String[] st = opt.split("=");
        checkFTPCommand(st.length == 2, 500, "OPTS failed.");

        String key = st[0];
        String value = st[1].split(",|;") [0];
        switch (key.toLowerCase()) {
        case "parallelism":
            _parallel = Integer.parseInt(value);
            if (_settings.getMaxStreamsPerClient() > 0) {
                _parallel = Math.min(_parallel, _settings.getMaxStreamsPerClient());
            }
            reply("200 Parallel streams set (" + opt + ")");
            break;

        case "markers":
            try {
                long period = Integer.parseInt(value);
                checkFTPCommand(period >= MINIMUM_PERFORMANCE_MARKER_PERIOD && period <= MAXIMUM_PERFORMANCE_MARKER_PERIOD,
                        500, "Value \"%s\" not acceptable", value);
                _performanceMarkerPeriod = TimeUnit.SECONDS.toMillis(period);
                reply("200 OPTS Command Successful.");
            } catch (NumberFormatException e) {
                throw new FTPCommandException(500, "Value \"" + value + "\" not an integer");
            }
            break;

        default:
            throw new FTPCommandException(501, "Unrecognized RETR option: " + key);
        }
    }

    public void opts_stor(String opt, String val) throws FTPCommandException
    {
        checkFTPCommand(opt.equalsIgnoreCase("EOF"),
                501, "Unrecognized option: %s (%s)", opt, val);

        switch (val) {
        case "0":
            _confirmEOFs = false;
            reply("200 EOF confirmation is OFF");
            break;
        case "1":
            _confirmEOFs = true;
            reply("200 EOF confirmation is ON");
            break;
        default:
            throw new FTPCommandException(501, "Unrecognized option value: " + val);
        }
    }

    private void opts_cksm(String algo) throws FTPCommandException
    {
        checkFTPCommand(algo != null, 501, "CKSM option command requires algorithm type");

        if (algo.startsWith("markers=")) {
            _checksumProgressPeriod = Long.parseLong(algo.substring(8).split(";") [0]);
            reply("200 OK");
            return;
        }

        if (algo.equalsIgnoreCase("NONE")) {
            _optCheckSumType = null;
        } else if (ChecksumType.isValid(algo)) {
            _optCheckSumType = ChecksumType.getChecksumType(algo);
        } else {
            throw new FTPCommandException(504, "Unsupported checksum type: " + algo);
        }
        reply("200 OK");
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

    public void opts_pasv(String s) throws FTPCommandException
    {
        Map<String, String> options = Splitter.on(';').omitEmptyStrings().withKeyValueSeparator('=').split(s);
        for (Map.Entry<String, String> option : options.entrySet()) {
            checkFTPCommand(option.getKey().equalsIgnoreCase("AllowDelayed"),
                    501, "Unrecognized option: %s (%s)", option.getKey(), option.getValue());
            _allowDelayed = option.getValue().equals("1");
        }
        reply("200 OK");
    }

    @Help("OPTS <SP> <feat> [<SP> <arg>] - Select desired behaviour for a feature.")
    public void ftp_opts(String arg) throws FTPCommandException
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
            throw new FTPCommandException(501, "Unrecognized option: " + st[0] + " (" + arg + ")");
        }
    }

    @Help("DELE <SP> <pathname> - Delete a file or symbolic link.")
    public void ftp_dele(String arg) throws FTPCommandException
    {
        /**
         * DELE
         *    250
         *    450, 550
         *    500, 501, 502, 421, 530
         */
        checkLoggedIn(FORBID_ANONYMOUS_USER);

        FsPath path = absolutePath(arg);
        try {
            PnfsId pnfsId =
                    _pnfs.deletePnfsEntry(path.toString(), EnumSet.of(FileType.REGULAR, FileType.LINK));
            reply("250 OK");
            sendRemoveInfoToBilling(pnfsId, path);
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

    @Help("USER <SP> <name> - Authentication username.")
    public abstract void ftp_user(String arg) throws FTPCommandException;

    @Help("PASS <SP> <password> - Authentication password.")
    public abstract void ftp_pass(String arg) throws FTPCommandException;

    @Help("PBSZ <SP> <size> - Protection buffer size.")
    public void ftp_pbsz(String arg)
    {
        reply("200 OK");
    }

    @Help("PROT <SP> <level> - Set data channel protection level.")
    public void ftp_prot(String arg) throws FTPCommandException
    {
        checkFTPCommand(arg.equals("C"), 534, "Will accept only Clear protection level");

        reply("200 OK");
    }

    ///////////////////////////////////////////////////////////////////////////
    //                                                                       //
    // the interpreter stuff                                                 //
    //                                                                       //


    private FsPath absolutePath(String path) throws FTPCommandException
    {
        String absPath;

        if (path.startsWith("/")) {
            absPath = path;
        } else if (path.equals("~")) {
            absPath = _userHomePath.toString();
        } else if (path.startsWith("~/")) {
            absPath = _userHomePath + path.substring(1);
        } else {
            absPath = _cwd + "/" + path;
        }

        return _doorRootPath.chroot(absPath);
    }


    @Help("RMD <SP> <path> - Remove an empty directory.")
    public void ftp_rmd(String arg) throws FTPCommandException
    {
        /**
         * RMD
         *   250
         *   500, 501, 502, 421, 530, 550
         */
        checkLoggedIn(FORBID_ANONYMOUS_USER);
        checkFTPCommand(!arg.isEmpty(), 500, "Missing path argument");

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
    public void ftp_mkd(String arg) throws FTPCommandException
    {
        /**
         * MKD
         *   257
         *   500, 501, 502, 421, 530, 550
         */
        checkLoggedIn(FORBID_ANONYMOUS_USER);
        checkFTPCommand(!arg.isEmpty(), 500, "Missing path argument");

        FsPath path = absolutePath(arg);
        String properDirectoryStringReply = path.stripPrefix(_doorRootPath).replaceAll("\"","\"\"");
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
    public void ftp_help(String arg) throws FTPCommandException
    {
        String lowerCaseCmd = arg.toLowerCase();
        checkFTPCommand(arg.indexOf('_') == -1 && (arg.isEmpty() || _methodDict.containsKey(lowerCaseCmd)),
                501, "Unknown command %s", arg.toUpperCase());

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
            Help help = _helpDict.get(lowerCaseCmd);
            String message = help == null ? arg.toUpperCase() : help.value();

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
                isFirstLine = false;
            }
        }

        reply(sr.toString());
    }

    /**
     * Apache Commons FTPClient uses the output of SYST to determine how
     * to parse the output from the LIST command.  Any response with the
     * keyword "UNIX" ensures the client parses LIST output as if it is the
     * output from "ls -l", as will including the phrase "Type: L8".
     */
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
        checkLoggedIn(FORBID_ANONYMOUS_USER);

        _allo = OptionalLong.empty();

        Matcher matcher = ALLO_PATTERN.matcher(arg);
        checkFTPCommand(matcher.matches(), 501, "Invalid argument");

        try {
            long size = Long.parseLong(matcher.group(1));
            _allo = OptionalLong.of(size);
        } catch (NumberFormatException e) {
            throw new FTPCommandException(501, "Invalid argument");
        }

        reply(ok("ALLO"));
    }

    @Help("PWD - Returns the current directory of the host.")
    public void ftp_pwd(String arg) throws FTPCommandException
    {
        checkLoggedIn(ALLOW_ANONYMOUS_USER);
        checkFTPCommand(arg.isEmpty(), 500, "No argument expected");
        reply("257 \"" + _cwd + "\" is current directory");
    }

    @Help("CWD <SP> <path> - Change working directory.")
    public void ftp_cwd(String arg) throws FTPCommandException
    {
        checkLoggedIn(ALLOW_ANONYMOUS_USER);

        try {
            FsPath newcwd = absolutePath(arg);
            checkIsDirectory(newcwd);
            _cwd = newcwd.stripPrefix(_doorRootPath);
            reply("250 CWD command succcessful. New CWD is <" + _cwd + ">");
        } catch (NotDirCacheException e) {
            throw new FTPCommandException(550, "Not a directory: " + arg);
        } catch (FileNotFoundCacheException e) {
            throw new FTPCommandException(550, "File not found");
        } catch (CacheException e) {
            LOGGER.error("Error in CWD: {}", e);
            throw new FTPCommandException(451, "CWD failed: " + e.getMessage());
        }
    }

    @Help("CDUP - Change to parent directory.")
    public void ftp_cdup(String arg) throws FTPCommandException
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
            checkFTPCommand(!arg.isEmpty(), 501, "Syntax error: empty arguments.");
            ArrayList<String> splitted = Lists.newArrayList(Splitter.on(arg.charAt(0)).split(arg));
            checkFTPCommand(splitted.size() == 5, 501, "Syntax error: Wrong number of arguments in '%s'.", arg);
            Protocol protocol = Protocol.find(splitted.get(1));
            checkFTPCommand(InetAddresses.isInetAddress(splitted.get(2)), 501,
                    "Syntax error: '%s' is no valid address.", splitted.get(2));
            InetAddress address = InetAddresses.forString(splitted.get(2));
            checkFTPCommand(protocol.getAddressClass().equals(address.getClass()),
                    501, "Protocol code does not match address: '%s'.", arg);
            int port = Integer.parseInt(splitted.get(3));
            checkFTPCommand(port >= 1 && port <= 65536, 501,
                    "Port number '%d' out of range [1,65536].", port);
            return new InetSocketAddress(address, port);

        } catch (NumberFormatException nfe) {
            throw new FTPCommandException(501, "Syntax error: no valid port number in '"+arg+"'.");
        }
    }

    protected void setActive(InetSocketAddress address) throws FTPCommandException
    {
        checkFTPCommand(!_sessionAllPassive, 503, "PORT and EPRT not allowed after EPSV ALL.");

        _mode = Mode.ACTIVE;
        _clientDataAddress = address;
        _clientConnectionHandler.close();
        _sessionAllPassive = false; // REVISIT see RFC 2428 Section 4.
    }

    @VisibleForTesting
    InetSocketAddress setPassive()
        throws FTPCommandException
    {
        try {
            _clientConnectionHandler.open();
            _mode = Mode.PASSIVE;
            return _clientConnectionHandler.getLocalAddress();
        } catch (NoSuchElementException e) {
            InetAddress address = _localSocketAddress.getAddress();
            String iface;
            try {
                iface = "Interface " + NetworkInterface.getByInetAddress(address).getName();
            } catch (SocketException se) {
                LOGGER.warn("Unable to discover interface for address {}: {}",
                        InetAddresses.toUriString(address), se.toString());
                iface = "Interface";
            }
            ProtocolFamily family = _clientConnectionHandler.getPreferredProtocolFamily();
            String ipVersion = PROTOCOLFAMILY_TO_STRING.getOrDefault(family, family.name());
            _mode = Mode.ACTIVE;
            _clientConnectionHandler.close();
            _sessionAllPassive = false; // REVISIT see RFC 2428 Section 4.
            throw new FTPCommandException(522, iface + " does not support " + ipVersion + " addresses");
        } catch (IOException e) {
            _mode = Mode.ACTIVE;
            _clientConnectionHandler.close();
            _sessionAllPassive = false; // REVISIT see RFC 2428 Section 4.
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
    protected Collection<InterfaceAddress> getLocalAddressInterfaces()
            throws SocketException
    {
        return NetworkInterface.getByInetAddress(_localSocketAddress.getAddress())
                .getInterfaceAddresses();
    }

    @Help("PORT <SP> <target> - The address and port to which the server should connect.")
    public void ftp_port(String arg)
        throws FTPCommandException
    {
        checkLoggedIn(ALLOW_ANONYMOUS_USER);

        String[] st = arg.split(",");
        checkFTPCommand(st.length == 6, 500, "Badly formatted argument: %s", arg);

        setActive(getAddressOf(st));
        _allowDelayed = false;
        _delayedPassive = DelayedPassiveReply.NONE;
        reply(ok("PORT"));
    }

    @Help("PASV - Enter passive mode.")
    public void ftp_pasv(String arg)
        throws FTPCommandException
    {
        checkLoggedIn(ALLOW_ANONYMOUS_USER);
        checkFTPCommand(!_sessionAllPassive, 503, "PASV not allowed after EPSV ALL");

        /* PASV can only return IPv4 addresses.
         */
        _clientConnectionHandler.setPreferredProtocol(Protocol.IPV4);

        /* If already in passive mode then we close the previous
         * socket and allocate a new one. This is a defensive move to
         * recover from the server socket having been closed by some
         * error condition.
         */
        _clientConnectionHandler.close();
        _sessionAllPassive = false; // REVISIT see RFC 2428 Section 4.
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
        checkLoggedIn(ALLOW_ANONYMOUS_USER);

        setActive(getExtendedAddressOf(arg));
        _allowDelayed = false;
        _delayedPassive = DelayedPassiveReply.NONE;
        reply(ok("EPRT"));
    }

    @Help("EPSV - Enter extended passive mode.")
    public void ftp_epsv(String arg)
        throws FTPCommandException
    {
        checkFTPCommand(_allowDelayed || _remoteSocketAddress.getAddress().getClass().equals(Inet6Address.class),
                502, "Command only supported for IPv6");
        checkLoggedIn(ALLOW_ANONYMOUS_USER);

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
            _clientConnectionHandler.close();
            _sessionAllPassive = false; // REVISIT see RFC 2428 Section 4.
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
                _clientConnectionHandler.setPreferredProtocol(Protocol.find(arg));
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

    private static TransferMode asTransferMode(String label) throws FTPCommandException
    {
        return TransferMode.forLabel(label.toUpperCase())
                .orElseThrow((Supplier<FTPCommandException>)()
                        -> new FTPCommandException(501, "Unsupported transfer mode"));
    }



    @Help("MODE <SP> <mode> - Sets the transfer mode.")
    public void ftp_mode(String arg) throws FTPCommandException
    {
        _xferMode = asTransferMode(arg);
        reply("200 Will use " + _xferMode.getDescription());
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
        checkLoggedIn(ALLOW_ANONYMOUS_USER);

        int spaceIndex = arg.indexOf(' ');
        checkFTPCommand(spaceIndex != -1, 500, "missing time-val and pathname");

        String pathname = arg.substring(spaceIndex + 1);
        String timeval = arg.substring(0, spaceIndex);
        long when = parseTimeval(timeval, "");

        FileAttributes updated = updateAttributesFromPath(pathname,
                FileAttributes.ofModificationTime(when));

        String updatedTimeval =
                TIMESTAMP_FORMAT.format(new Date(updated.getModificationTime()));

        reply("213 Modify=" + updatedTimeval + "; " + pathname);
    }


    @Help("MFCT <SP> <time-val> <SP> <path> - Adjust creation timestamp")
    public void ftp_mfct(String arg) throws FTPCommandException
    {
        checkLoggedIn(ALLOW_ANONYMOUS_USER);

        int spaceIndex = arg.indexOf(' ');
        checkFTPCommand(spaceIndex != -1, 500, "missing time-val and pathname");

        String pathname = arg.substring(spaceIndex + 1);
        String timeval = arg.substring(0, spaceIndex);
        long when = parseTimeval(timeval, "");

        FileAttributes updated = updateAttributesFromPath(pathname,
                FileAttributes.ofCreationTime(when));

        String updatedTimeval =
                TIMESTAMP_FORMAT.format(new Date(updated.getCreationTime()));

        reply("213 Create=" + updatedTimeval + "; " + pathname);
    }


    @Help("MFF <SP> <fact> = <value> ; [<fact> = <value> ; ...] <SP> <path> - Update facts about file or directory")
    public void ftp_mff(String arg) throws FTPCommandException
    {
        checkLoggedIn(ALLOW_ANONYMOUS_USER);

        int spaceIndex = arg.indexOf(' ');
        checkFTPCommand(spaceIndex != -1, 500, "missing mff-facts and pathname");

        String pathname = arg.substring(spaceIndex + 1);
        String facts = arg.substring(0, spaceIndex);

        FileAttributes updates = new FileAttributes();
        Map<String,String> changes = Splitter.on(';').omitEmptyStrings().
                withKeyValueSeparator('=').split(facts);
        for (Map.Entry<String,String> change : changes.entrySet()) {
            Fact fact = Fact.find(change.getKey());
            checkFTPCommand(fact != null, 504, "Unsupported fact %s", change.getKey());
            switch (fact) {
            case MODE:
                try {
                    updates.setMode(Integer.parseInt(change.getValue(), 8));
                } catch (NumberFormatException e) {
                    throw new FTPCommandException(504, "value not in octal for UNIX.mode");
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
                throw new FTPCommandException(504, "Unmodifable fact " + change.getKey());
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

    private long parseTimeval(String timeval, String errorSuffix) throws FTPCommandException
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
            "SITE <SP> CHGRP <SP> <group> <SP> <path> - Change group-owner of <path> to group <group>\r\n" +
            "SITE <SP> CHMOD <SP> <perm> <SP> <path> - Change permission of <path> to octal value <perm>\r\n" +
            "SITE <SP> CLIENTINFO <SP> <id> - Provide server with information about the client\r\n" +
            "SITE <SP> SYMLINKFROM <SP> <path> - Register symlink location; SYMLINKTO must follow\r\n" +
            "SITE <SP> SYMLINKTO <SP> <path> - Create symlink to <path>; SYMLINKFROM must be earlier command\r\n" +
            "SITE <SP> TASKID <SP> <id> - Provide server with an identifier\r\n" +
            "SITE <SP> USAGE <SP> [TOKEN <SP> <token> <SP> ] <path>\r\n" +
            "SITE <SP> WHOAMI - Provides the username or uid of the user")
    public void ftp_site(String arg) throws FTPCommandException
    {
        checkLoggedIn(ALLOW_ANONYMOUS_USER);
        checkFTPCommand(!arg.isEmpty(), 500, "must supply the site specific command");

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
            checkFTPCommand(args.length == 2, 500, "command must be in the form 'SITE BUFSIZE <number>'");
            ftp_sbuf(args[1]);
        } else if ( args[0].equalsIgnoreCase("CHKSUM")) {
            checkFTPCommand(args.length == 2, 500, "command must be in the form 'SITE CHKSUM <value>'");
            doCheckSum("adler32",args[1]);
        } else if (args[0].equalsIgnoreCase("CHGRP")) {
            checkFTPCommand(args.length == 3, 504, "command must be in the form 'SITE CHGRP <group/gid> <file/dir>'");
            doChgrp(args[1], args[2]);
        } else if (args[0].equalsIgnoreCase("CHMOD")) {
            checkFTPCommand(args.length == 3, 500, "command must be in the form 'SITE CHMOD <octal perms> <file/dir>'");
            doChmod(args[1], args[2]);
        } else if (args[0].equalsIgnoreCase("CLIENTINFO")) {
            checkFTPCommand(args.length >= 2, 500, "command must be in the form 'SITE CLIENTINFO <info>'");
            doClientinfo(arg.substring(11));
        } else if (args[0].equalsIgnoreCase("SYMLINKFROM")) {
            checkFTPCommand(args.length == 2, 500, "command must be in the form 'SITE SYMLINKFROM <path>'");
            doSymlinkFrom(args[1]);
        } else if (args[0].equalsIgnoreCase("SYMLINKTO")) {
            checkFTPCommand(args.length == 2, 500, "command must be in the form 'SITE SYMLINKTO <path>'");
            doSymlinkTo(args[1]);
        } else if (args[0].equalsIgnoreCase("TASKID")) {
            checkFTPCommand(args.length >= 2, 501, "Syntax error in parameters or arguments.");
            doTaskid(arg.substring(6));
        } else if (args[0].equalsIgnoreCase("USAGE")) {
            checkFTPCommand(args.length >= 2, 501, "Missing arguments in command.");
            boolean hasToken = args[1].equalsIgnoreCase("TOKEN");
            doUsage(hasToken ? arg.substring(12) : arg.substring(6), hasToken);
        } else if (args[0].equalsIgnoreCase("WHOAMI")) {
            checkFTPCommand(args.length == 1, 501, "Invalid command arguments.");
            doWhoami();
        } else {
            throw new FTPCommandException(500, "Unknown SITE command");
        }
    }

    @Help("CKSM <SP> <alg> <SP> <off> <SP> <len> <SP> <path> - Return checksum of file.")
    public void ftp_cksm(String arg) throws FTPCommandException
    {
        checkLoggedIn(ALLOW_ANONYMOUS_USER);

        List<String> st = Splitter.on(' ').limit(4).splitToList(arg);
        checkFTPCommand(st.size() == 4, 500, "Unsupported CKSM command operands");

        String algo = st.get(0);
        String offset = st.get(1);
        String length = st.get(2);
        String path = st.get(3);

        long offsetL;
        long lengthL;

        try {
            offsetL = Long.parseLong(offset);
        } catch (NumberFormatException e) {
            throw new FTPCommandException(501, "Invalid offset format: " + e);
        }

        try {
            lengthL = Long.parseLong(length);
        } catch (NumberFormatException e) {
            throw new FTPCommandException(501, "Invalid length format: " + e);
        }

        doCksm(algo, path, offsetL, lengthL);
    }

    public void doCksm(String algo, String path, long offsetL, long lengthL)
            throws FTPCommandException
    {
        checkFTPCommand(lengthL == -1, 504, "Unsupported checksum over partial file length");
        checkFTPCommand(offsetL == 0, 504, "Unsupported checksum over partial file offset");

        try {
            FsPath absPath = absolutePath(path);
            if (!ChecksumType.isValid(algo)) {
                throw new NoSuchAlgorithmException(algo);
            }
            ChecksumType type = ChecksumType.getChecksumType(algo);
            FileAttributes attributes =
                _pnfs.getFileAttributes(absPath, EnumSet.of(CHECKSUM));
            Checksum checksum = attributes.getChecksums().stream()
                    .filter(c -> c.getType() == type)
                    .findFirst()
                    .orElse(null);

            if (checksum == null) {
                ChecksumCalculatingTransfer cct = new ChecksumCalculatingTransfer(_pnfs, _subject, _authz, absPath, type, new PortRange(0,0));
                setTransfer(cct);
                TimerTask sendProgress = null;
                try {
                    cct.setPoolManagerStub(_poolManagerStub);
                    cct.setPoolStub(_poolStub);
                    cct.setAllowStaging(false);
                    if (_checksumProgressPeriod > 0) {
                        sendProgress = new TimerTask(){
                                    @Override
                                    public void run()
                                    {
                                        reply(cct.getReply());
                                    }
                                };
                        long period = TimeUnit.SECONDS.toMillis(_checksumProgressPeriod);
                        TIMER.schedule(sendProgress, period, period);
                    }
                    checksum = cct.calculateChecksum();
                } finally {
                    if (sendProgress != null) {
                        sendProgress.cancel();
                    }
                    setTransfer(null);
                }

                /* The client may be downloading a file that it does not have
                 * permission to add a new checksum value, nevertheless we want
                 * to avoid recalculating the checksum if this file is
                 * downloaded again.  Therefore, we add the freshly
                 * calculated checksum value as user ROOT with no restrictions.
                 */
                new PnfsHandler(_pnfs, Subjects.ROOT, Restrictions.none())
                        .setFileAttributes(absPath, FileAttributes.ofChecksum(checksum));
            }

            reply("213 " + checksum.getValue());
        } catch (InterruptedException | IOException | CacheException e) {
            throw new FTPCommandException(550, "Error retrieving " + path
                                          + ": " + e.getMessage());
        } catch (NoSuchAlgorithmException e) {
            throw new FTPCommandException(504, "Unsupported checksum type:" + e);
        }
    }

    private Checksum calculateChecksum(String file) throws FTPCommandException, IOException
    {
        ServerSocket ss = new ServerSocket();

        FtpTransfer transfer = new FtpTransfer(absolutePath(file),
                            -1, -1, Mode.PASSIVE, TransferMode.MODE_S, 1, (InetSocketAddress) ss.getLocalSocketAddress(),
                            MiB.toBytes(1), DelayedPassiveReply.NONE, null, 1);
        return null;
    }

    @Help("SCKS <SP> <alg> <SP> <value> - Fail next upload if checksum does not match.")
    public void ftp_scks(String arg) throws FTPCommandException
    {
        checkLoggedIn(ALLOW_ANONYMOUS_USER);

        String[] st = arg.split("\\s+");
        checkFTPCommand(st.length == 2, 505, "Unsupported SCKS command operands");
        doCheckSum(st[0], st[1]);
    }


    public void doCheckSum(String type, String value) throws FTPCommandException
    {
        checkFTPCommand(ChecksumType.isValid(type), 504, "Unsupported checksum type");

        try {
            _checkSum = new Checksum(ChecksumType.getChecksumType(type), value);
            reply("213 OK");
        } catch (RuntimeException e) {
            _checkSum = null;
            throw e;
        }
    }

    public void doChmod(String permstring, String path) throws FTPCommandException
    {
        checkLoggedIn(FORBID_ANONYMOUS_USER);
        checkFTPCommand(!path.isEmpty(), 500, "Missing path");

        FileAttributes attributes;
        try {
            // Assume octal regardless of string
            int newperms = Integer.parseInt(permstring, 8);

            FsPath absPath = absolutePath(path);
            attributes =
                _pnfs.getFileAttributes(absPath, EnumSet.of(TYPE));

            checkFTPCommand(attributes.getFileType() != FileType.LINK,
                    502, "chmod of symbolic links is not yet supported.");

            _pnfs.setFileAttributes(absPath, FileAttributes.ofMode(newperms));

            reply("250 OK");
        } catch (NumberFormatException ex) {
            throw new FTPCommandException(501, "permissions argument must be an octal integer");
        } catch (PermissionDeniedCacheException e) {
            throw new FTPCommandException(550, "Permission denied");
        } catch (CacheException ce) {
            throw new FTPCommandException(550, "Permission denied, reason: " + ce);
        }
    }

    /*
     *  If the return code is (>=500 && <= 509 && !504) || 202 then UberFTP
     *  disables support for this comand.
     */
    public void doChgrp(String group, String path) throws FTPCommandException
    {
        checkLoggedIn(FORBID_ANONYMOUS_USER);
        checkFTPCommand(!path.isEmpty(), 500, "Missing path");

        int gid;

        Integer result = Ints.tryParse(group);
        if (result == null) {
            try {
                Principal principal = _loginStrategy.map(new GroupNamePrincipal(group));
                checkFTPCommand(principal != null, 504, "Unknown group '%s'", group);
                if (!(principal instanceof GidPrincipal)) {
                    LOGGER.warn("Received non-GID {} principal from map request",
                            principal.getClass().getCanonicalName());
                    throw new FTPCommandException(431, "Internal error " +
                            "identifying group '" + group + "'");
                }
                gid = (int)((GidPrincipal)principal).getGid();
            } catch (CacheException e) {
                LOGGER.warn("Unable to map group '{}' to gid: {}", group, e.toString());
                throw new FTPCommandException(451, "Unable to process: " + e, e);
            }
        } else {
            gid = result;
        }

        FileAttributes attributes;
        try {
            FsPath absPath = absolutePath(path);
            attributes = _pnfs.getFileAttributes(absPath, EnumSet.of(TYPE));

            checkFTPCommand(attributes.getFileType() != FileType.LINK,
                    504, "chgrp of symbolic links is not yet supported.");

            _pnfs.setFileAttributes(absPath, FileAttributes.ofGid(gid));

            reply("250 OK");
        } catch (PermissionDeniedCacheException e) {
            throw new FTPCommandException(550, "Permission denied", e);
        } catch (FileNotFoundCacheException e) {
            throw new FTPCommandException(504, "No such file", e);
        } catch (CacheException e) {
            throw new FTPCommandException(451, "Unable to process: " + e, e);
        }
    }

    public void doSymlinkFrom(String path) throws FTPCommandException
    {
        checkLoggedIn(FORBID_ANONYMOUS_USER);
        checkFTPCommand(!path.isEmpty(), 501, "Command requires path argument.");

        _symlinkPath = path;

        reply("350 Send SITE SYMLINKTO to continue.");
    }

    public void doSymlinkTo(String target) throws FTPCommandException
    {
        checkLoggedIn(FORBID_ANONYMOUS_USER);
        checkFTPCommand(!target.isEmpty(), 501, "Command requires path.");
        checkFTPCommand(_symlinkPath != null, 503, "Command must follow SITE SYMLINKFROM command.");

        // NB. if we get here then user has satisfied conditions of
        // {@code #doSymlinkFrom}.

        try {
            _pnfs.createSymLink(absolutePath(_symlinkPath).toString(), target,
                    FileAttributes.of().uid(_subject).gid(_subject).build());
            reply ("257 symlink '" + _symlinkPath + "' created.");
        } catch (PermissionDeniedCacheException e) {
            throw new FTPCommandException(550, "Permission denied.", e);
        } catch (NotDirCacheException e) {
            throw new FTPCommandException(550, "Not a directory.", e);
        } catch (FileNotFoundCacheException e) {
            throw new FTPCommandException(550, "File not found.", e);
        } catch (FileExistsCacheException e) {
            throw new FTPCommandException(550, "File exists.", e);
        } catch (CacheException e) {
            LOGGER.warn("Unable to create symlink: {}", e.toString());
            throw new FTPCommandException(451, "Unexpected problem: " + e, e);
        } finally {
            _symlinkPath = null;
        }
    }

    /**
     * Create a map from a semi-colon list of chunks, with each chunk
     * having the form "key=value", "key=\"value\"", or "value".  For the
     * last type, we assume the key should be "appname".
     */
    private static Map<String,String> splitToMap(String info)
    {
        Map<String,String> items = new HashMap<>();
        for (String chunk : Splitter.on(';').omitEmptyStrings().split(info)) {
            int index = chunk.indexOf('=');
            if (index == -1) {
                items.put("appname", chunk.trim());
            } else {
                String value = chunk.substring(index+1).trim();
                if (value.charAt(0) == '\"' && value.charAt(value.length()-1) == '\"') {
                    value = value.substring(1, value.length()-1);
                }
                items.put(chunk.substring(0, index).trim(), value);
            }
        }
        return items;
    }

    public void doClientinfo(String description)
    {
        LOGGER.debug("client-info: {}", description);
        _clientInfo = description;

        Map<String,String> items = splitToMap(description);

        // If items.get("appname") is "globusonline-fxp" then client is the
        // Globus transfer service agent responsible for coordinating
        // transfers.  This may be used to enable any necessary work-arounds.
        //
        // REVISIT: FTS stores task-related information in the CLIENTINFO
        //     argument.  In future, we may want to record client-supplied
        //     identifiers in billing. (See doTaskid)

        switch (Strings.nullToEmpty(items.get("appname"))) {
        case "globus-url-copy":
            _activeWorkarounds.add(WorkAround.NO_REPLY_ON_QUIT);
            break;
        }
        reply("250 OK");
    }

    public void doTaskid(String arg)
    {
        // REVISIT: the task id is recorded in the access log, so may be
        //     discoverable, provided this file still exists.  In future, we
        //     may want to record client-supplied identifiers in billing.
        reply("250 OK");
    }

    /**
     * Process a "SITE USAGE" command.  The format is described here:
     *
     *     https://github.com/bbockelm/globus-gridftp-osg-extensions/
     */
    public void doUsage(String arg, boolean hasToken) throws FTPCommandException
    {
        String token;
        String requestPath;

        if (hasToken) {
            List<String> args = Splitter.on(' ').limit(2).splitToList(arg);
            checkFTPCommand(args.size() == 2, 501, "Missing path after TOKEN value");
            token = args.get(0);
            requestPath = args.get(1);
        } else {
            token = "default";
            requestPath = arg;
        }

        try {
            List<String> spaceIds;

            if (token.equalsIgnoreCase("default")) {
                FileAttributes attr = _pnfs.getFileAttributes(absolutePath(requestPath), EnumSet.of(FileAttribute.STORAGEINFO));
                String id = attr.getStorageInfo().getMap().get("writeToken");
                checkFTPCommand(id != null, 501, "Path is not under space management");
                spaceIds = Collections.singletonList(id);
            } else {
                long[] ids = _spaceDescriptionCache.get(new GetSpaceTokensKey(_subject.getPrincipals(), token));
                checkFTPCommand(ids.length > 0, 501, "Unknown TOKEN " + token);
                spaceIds = Arrays.stream(ids).mapToObj(Long::toString).collect(Collectors.toList());
            }

            SpaceAccount combined = _spaceLookupCache.getAll(spaceIds).values().stream()
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .map(SpaceAccount::new)
                    .reduce(new SpaceAccount(), (a,b) -> a.plus(b));

            reply("250 USAGE " + (combined.getUsed() + combined.getAllocated()) +
                    " FREE " + combined.getAvailable() +
                    " TOTAL " + combined.getTotal());
        } catch (CacheException e) {
            switch (e.getRc()) {
            case CacheException.FILE_NOT_FOUND:
                throw new FTPCommandException(550, "File not found", e);
            case CacheException.TIMEOUT:
                throw new FTPCommandException(451, "Internal timeout", e);
            case CacheException.NOT_DIR:
                throw new FTPCommandException(550, "Not a directory", e);
            default:
                throw new FTPCommandException(451, "Operation failed: " + e.getMessage(), e);
            }
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            Throwables.throwIfUnchecked(cause);
            throw new FTPCommandException(451, "Failed to fetch space details: " + cause.getMessage(), e);
        }
    }

    public void doWhoami()
    {
        String name = Subjects.getUserName(_subject);
        if (name == null) {
            name = Long.toString(Subjects.getUid(_subject));
        }
        reply("200 " + name);
    }

    @Help("SBUF <SP> <size> - Set buffer size.")
    public void ftp_sbuf(String arg) throws FTPCommandException
    {
        checkFTPCommand(!arg.isEmpty(), 500, "must supply a buffer size");

        int bufsize;
        try {
            bufsize = Integer.parseInt(arg);
        } catch (NumberFormatException e) {
            throw new FTPCommandException(500, "bufsize argument must be integer");
        }

        checkFTPCommand(bufsize > 0, 500, "bufsize must be positive.  Probably large, but at least positive");

        _bufSize = bufsize;
        reply("200 bufsize set to " + arg);
    }

    @Help("ERET <SP> <mode> <SP> <path> - Extended file retrieval.")
    public void ftp_eret(String arg) throws FTPCommandException
    {
        String[] st = arg.split("\\s+");
        checkFTPCommand(st.length >= 2, 500, "Missing argument");

        String extended_retrieve_mode = st[0];
        String cmd = "eret_" + extended_retrieve_mode.toLowerCase();
        Object args[] = { arg };
        checkFTPCommand(_methodDict.containsKey(cmd),
                504, "ERET is not implemented for retrieve mode: %s", extended_retrieve_mode);
        Method m = _methodDict.get(cmd);
        try {
            LOGGER.info("Error return invoking: {}({})", m.getName(), arg);
            m.invoke(this, args);
        } catch (IllegalAccessException | InvocationTargetException e) {
            _skipBytes = 0;
            Throwables.throwIfInstanceOf(e.getCause(), FTPCommandException.class);
            throw new FTPCommandException(500, e.toString());
        }
    }

    @Help("ESTO <SP> <mode> <SP> <path> - Extended store.")
    public void ftp_esto(String arg) throws FTPCommandException
    {
        String[] st = arg.split("\\s+");
        checkFTPCommand(st.length >= 2, 500, "Missing argument");

        String extended_store_mode = st[0];
        String cmd = "esto_" + extended_store_mode.toLowerCase();
        Object args[] = { arg };
        checkFTPCommand(_methodDict.containsKey(cmd),
                504, "ESTO is not implemented for store mode: %s", extended_store_mode);
        Method m = _methodDict.get(cmd);
        try {
            LOGGER.info("Esto invoking: {} ({})", m.getName(), arg);
            m.invoke(this, args);
        } catch (IllegalAccessException | InvocationTargetException e) {
            _skipBytes = 0;
            Throwables.throwIfInstanceOf(e.getCause(), FTPCommandException.class);
            throw new FTPCommandException(500, e.toString());
        }
    }

    //
    // this is the implementation for the ESTO with mode "a"
    // "a" is ajusted store mode
    // other modes identified by string "MODE" can be implemented by adding
    // void method ftp_esto_"MODE"(String arg)
    //
    public void ftp_esto_a(String arg) throws FTPCommandException
    {
        String[] st = arg.split("\\s+");
        checkFTPCommand(st.length == 3, 500, "Missing argument");

        String extended_store_mode = st[0];
        checkFTPCommand(extended_store_mode.equalsIgnoreCase("a"),
                504, "ESTO is not implemented for store mode: %s", extended_store_mode);
        String offset = st[1];
        String filename = st[2];
        long asm_offset;
        try {
            asm_offset = Long.parseLong(offset);
        } catch (NumberFormatException e) {
            throw new FTPCommandException(501, "Adjusted Store Mode: invalid offset " + offset);
        }
        checkFTPCommand(asm_offset == 0,
                504, "ESTO Adjusted Store Mode does not work with nonzero offset: %s", offset);
        LOGGER.info("Performing esto in \"a\" mode with offset = {}", offset);
        ftp_stor(filename);
    }

    //
    // this is the implementation for the ERET with mode "p"
    // "p" is partiall retrieve mode
    // other modes identified by string "MODE" can be implemented by adding
    // void method ftp_eret_"MODE"(String arg)
    //
    public void ftp_eret_p(String arg) throws FTPCommandException
    {
        String[] st = arg.split("\\s+");
        checkFTPCommand(st.length == 4, 500, "Missing argument");

        String extended_retrieve_mode = st[0];
        checkFTPCommand(extended_retrieve_mode.equalsIgnoreCase("p"),
                504, "ERET is not implemented for retrieve mode: %s", extended_retrieve_mode);
        String offset = st[1];
        String size = st[2];
        String filename = st[3];
        try {
            prm_offset = Long.parseLong(offset);
        } catch (NumberFormatException e) {
            throw new FTPCommandException(501, "invalid offset " + offset + ": " + e);
        }
        try {
            prm_size = Long.parseLong(size);
        } catch (NumberFormatException e) {
            throw new FTPCommandException(501, "invalid size " + size + ": " + e);
        }
        LOGGER.info("Performing eret in \"p\" mode with offset = {} size = {}",
                offset, size);
        ftp_retr(filename);
    }

    @Help("RETR <SP> <path> - Retrieve a copy of the file.")
    public void ftp_retr(String arg) throws FTPCommandException
    {
        try {
            checkFTPCommand(_skipBytes <= 0, 504, "RESTART not implemented");
            retrieve(arg, prm_offset, prm_size, _mode,
                     _xferMode, _parallel, _clientDataAddress, _bufSize,
                     _delayedPassive, _clientConnectionHandler.getPreferredProtocolFamily(),
                     _delayedPassive == DelayedPassiveReply.NONE ? 1 : 2);
        } finally {
            prm_offset=-1;
            prm_size=-1;
        }
    }

    protected synchronized Transfer getTransfer()
    {
        return _transfer;
    }

    private synchronized boolean isTransferring()
    {
        return _transfer != null;
    }

    protected synchronized void setTransfer(Transfer transfer)
    {
        _transfer = transfer;
        notifyAll();
    }

    protected synchronized void joinTransfer() throws InterruptedException
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
                          Mode mode, TransferMode xferMode,
                          int parallel,
                          InetSocketAddress client, int bufSize,
                          DelayedPassiveReply delayedPassive,
                          ProtocolFamily protocolFamily, int version) throws FTPCommandException
    {
        /* Check preconditions.
         */
        checkLoggedIn(ALLOW_ANONYMOUS_USER);
        checkFTPCommand(!file.isEmpty(), 501, "Missing path");
        checkFTPCommand(xferMode != TransferMode.MODE_E || mode != Mode.PASSIVE,
                500, "Cannot do passive retrieve in E mode");
        checkFTPCommand(xferMode != TransferMode.MODE_X || mode != Mode.PASSIVE || !_settings.isProxyRequiredOnPassive(),
                504, "Cannot use passive X mode");
        checkFTPCommand(mode != Mode.INVALID, 425, "Issue PASV or PORT to reset data channel.");
        checkFTPCommand(_checkSum == null, 503, "Expecting STOR ESTO PUT commands");

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
            LOGGER.info("retrieve addr={}", _remoteSocketAddress);

            if (version == 1) {
                transfer.redirect(null);
            }
            transfer.readNameSpaceEntry(false);
            transfer.checkAndDeriveOffsetAndSize();

            /* Transfer the file. As there is a delay between the
             * point when a pool goes offline and when the pool
             * manager updates its state, we will retry failed
             * transfer a few times.
             */
            transfer.createAdapter();
            transfer.selectPoolAndStartMoverAsync(_readRetryPolicy);
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
                transfer.abort(500, "Invalid request: " + e.getMessage(), e);
                break;
            case CacheException.RESOURCE:
                transfer.abort(452, e.getMessage(), e);
                break;
            default:
                transfer.abort(451, "Operation failed: " + e.getMessage(), e);
                break;
            }
        } catch (FTPCommandException e) {
            transfer.abort(e);
        } catch (InterruptedException e) {
            transfer.abort(451, "Operation cancelled");
        } catch (IOException e) {
            transfer.abort(451, "Operation failed: " + e.getMessage());
        } catch (RuntimeException e) {
            LOGGER.error("Retrieve failed", e);
            transfer.abort(451, "Transient internal failure");
        } finally {
            _allo = OptionalLong.empty();
        }
    }

    @Help("STOR <SP> <path> - Tell server to start accepting data.")
    public void ftp_stor(String arg) throws FTPCommandException
    {
        checkFTPCommand(_clientDataAddress != null, 504, "Host somehow not set");
        checkFTPCommand(_skipBytes == 0, 504, "RESTART not implemented for STORE");

        store(arg, _mode, _xferMode, _parallel, _clientDataAddress, _bufSize,
              _delayedPassive, _clientConnectionHandler.getPreferredProtocolFamily(),
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
    private void store(String file, Mode mode, TransferMode xferMode,
                       int parallel,
                       InetSocketAddress client, int bufSize,
                       DelayedPassiveReply delayedPassive,
                       ProtocolFamily protocolFamily, int version) throws FTPCommandException
    {
        checkLoggedIn(FORBID_ANONYMOUS_USER);

        checkFTPCommand(!file.equals(""), 501, "STOR command not understood");
        checkFTPCommand(xferMode != TransferMode.MODE_E || mode != Mode.ACTIVE,
                504, "Cannot store in active E mode");
        checkFTPCommand(xferMode != TransferMode.MODE_X || mode != Mode.PASSIVE || !_settings.isProxyRequiredOnPassive(),
                504, "Cannot use passive X mode");
        checkFTPCommand(mode != Mode.INVALID, 425, "Issue PASV or PORT to reset data channel.");
        checkFTPCommand(!_maximumUploadSize.isPresent() || !_allo.isPresent() || _allo.getAsLong() <= _maximumUploadSize.getAsLong(),
                552, "File exceeds allowed size");

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

            if (version == 1) {
                transfer.redirect(null);
            }
            transfer.createNameSpaceEntry();
            if (_checkSum != null) {
                transfer.setChecksum(_checkSum);
            }
            _maximumUploadSize.ifPresent(transfer::setMaximumLength);

            transfer.createAdapter();
            transfer.selectPoolAndStartMoverAsync(_writeRetryPolicy);
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
                transfer.abort(500, "Invalid request: " + e.getMessage(), e);
                break;
            case CacheException.RESOURCE:
                transfer.abort(452, e.getMessage(), e);
                break;
            default:
                transfer.abort(451, "Operation failed: " + e.getMessage(), e);
                break;
            }
        } catch (RuntimeException e) {
            LOGGER.error("Store failed", e);
            transfer.abort(451, "Transient internal failure");
        } finally {
            _checkSum = null;
            _allo = OptionalLong.empty();
        }
    }

    @Help("SIZE <SP> <path> - Return the size of a file.")
    public void ftp_size(String arg) throws FTPCommandException
    {
        checkLoggedIn(ALLOW_ANONYMOUS_USER);
        checkFTPCommand(!arg.isEmpty(), 500, "Missing argument");

        FsPath path = absolutePath(arg);
        long filelength;
        try {
            FileAttributes attributes =
                _pnfs.getFileAttributes(path.toString(), EnumSet.of(SIZE));
            filelength = attributes.getSizeIfPresent().orElse(0L);
        } catch (PermissionDeniedCacheException e) {
            throw new FTPCommandException(550, "Permission denied");
        } catch (CacheException e) {
            throw new FTPCommandException(550, "Permission denied, reason: " + e);
        }
        reply("213 " + filelength);
    }

    @Help("MDTM <SP> <path> - Return the last-modified time of a specified file.")
    public void ftp_mdtm(String arg) throws FTPCommandException
    {
        checkLoggedIn(ALLOW_ANONYMOUS_USER);
        checkFTPCommand(!arg.isEmpty(), 500, "Missing argument");

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
            throw new FTPCommandException(550, "Permission denied");
        } catch (CacheException e) {
            switch (e.getRc()) {
            case CacheException.FILE_NOT_FOUND:
                throw new FTPCommandException(550, "File not found");
            case CacheException.TIMEOUT:
                LOGGER.warn("Timeout in MDTM: {}", e);
                throw new FTPCommandException(451, "Internal timeout");
            default:
                LOGGER.error("Error in MDTM: {}", e);
                throw new FTPCommandException(451, "Internal failure: " + e.getMessage());
            }
        }
    }

    private void openDataSocket() throws IOException, FTPCommandException
    {
        /* Mode being PASSIVE means the client did a PASV.  Otherwise
         * we establish the data connection to the client.
         */
        switch (_mode) {
        case PASSIVE:
            replyDelayedPassive(_delayedPassive, _clientConnectionHandler.getLocalAddress());
            reply("150 Ready to accept ASCII mode data connection");
            _dataSocket = _clientConnectionHandler.accept().socket();
            break;
        case ACTIVE:
            reply("150 Opening ASCII mode data connection");
            _dataSocket = new Socket();
            _dataSocket.connect(_clientDataAddress);
            break;
        default:
            throw new FTPCommandException(425, "Issue PASV or PORT to reset data channel.");
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

    /**
     * Provide a directory listing in some unspecified format.  Historically
     * Unix-like systems returned the output from "ls -l" and some clients
     * attempt to parse the output on this basis.  Below we document the
     * format expectations of various clients.
     * <p>
     * <b>Apache Commons FTPClient</b> Although FTPClient supports MLSD & MLST,
     * it doesn't provide this transparently; therefore clients using FTPClient
     * may well issue a LIST command and attempt to parse the response.
     * FTPClient has an option to request the server shows all files; enabling
     * this option results in the client issuing the non-standard option "-a";
     * e.g., "LIST -a". FTPClient uses the output from the SYST command to
     * determine how to parse the LIST response.
     * @see ftp_syst
     */
    @Help("LIST [<SP> <path>] - Returns information on <path> or the current working directory.")
    public void ftp_list(String arg) throws FTPCommandException
    {
        checkLoggedIn(ALLOW_ANONYMOUS_USER);

        Args args = new Args(arg);

        args.removeOptions("a"); // Remove any '-a', dCache always shows all files.

        // REVISIT: do any clients require shortList output?
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
                throw new FTPCommandException(425, "Cannot open connection");
            }

            int total;
            try {
                PrintWriter writer =
                    new PrintWriter(new OutputStreamWriter(new BufferedOutputStream(_dataSocket.getOutputStream()), StandardCharsets.UTF_8));

                DirectoryListPrinter printer;
                if (listLong) {
                    printer = _settings.getListFormat().equals("legacy")
                            ? new LegacyListPrinter(writer)
                            : new LongListPrinter(writer);
                } else {
                    printer = new ShortListPrinter(writer);
                }

                try {
                    total = _listSource.printDirectory(_subject, _authz, printer,
                            path, null, Range.<Integer>all());
                } catch (NotDirCacheException e) {
                    /* path exists, but it is not a directory.
                     */
                    _listSource.printFile(_subject, _authz, printer, path);
                    total = 1;
                } catch (FileNotFoundCacheException e) {
                    /* If f does not exist, then it could be a
                     * pattern; we move up one directory level and
                     * repeat the list.
                     */
                    total =
                        _listSource.printDirectory(_subject, _authz, printer,
                                path.parent(), new Glob(path.name()),
                                Range.<Integer>all());
                }

                writer.close();
            } finally {
                closeDataSocket();
            }
            reply("226 " + total + " files");
        } catch (InterruptedException e) {
            throw new FTPCommandException(451, "Operation cancelled");
        } catch (FileNotFoundCacheException e) {
            throw new FTPCommandException(550, "File not found");
        } catch (NotDirCacheException e) {
            throw new FTPCommandException(550, "Not a directory");
        } catch (PermissionDeniedCacheException e) {
            throw new FTPCommandException(550, "Permission denied");
        } catch (EOFException e) {
            throw new FTPCommandException(426, "Connection closed; transfer aborted");
        } catch (CacheException | IOException e) {
            LOGGER.warn("Error in LIST: {}", e.getMessage());
            throw new FTPCommandException(451, "Local error in processing");
        }
    }


    private static final Pattern GLOB_PATTERN = Pattern.compile("[*?]");

    @Help("NLST [<SP> <path>] - Returns a list of file names in a specified directory.")
    public void ftp_nlst(String arg) throws FTPCommandException
    {
        checkLoggedIn(ALLOW_ANONYMOUS_USER);

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
            Matcher m = GLOB_PATTERN.matcher(path.name());
            boolean pathIsPattern = m.find();
            if ( !pathIsPattern ) {
                checkIsDirectory(path);
            }
            try {
                openDataSocket();
            } catch (IOException e) {
                throw new FTPCommandException(425, "Cannot open connection");
            }

            int total;
            try {
                PrintWriter writer =
                    new PrintWriter(new OutputStreamWriter(new BufferedOutputStream(_dataSocket.getOutputStream()), "US-ASCII"));
                DirectoryListPrinter printer = new ShortListPrinter(writer);
                if ( pathIsPattern ) {
                    total = _listSource.printDirectory(_subject, _authz,
                            printer, path.parent(), new Glob(path.name()),
                            Range.<Integer>all());
                } else {
                    total = _listSource.printDirectory(_subject, _authz,
                            printer, path, null, Range.<Integer>all());
                }
                writer.close();
            } finally {
                closeDataSocket();
            }
            reply("226 " + total + " files");
        } catch (InterruptedException e) {
            throw new FTPCommandException(451, "Operation cancelled");
        } catch (FileNotFoundCacheException e) {
            /* 550 is not a valid reply for NLST. However other FTP
             * servers use this return code for NLST. Gerd and Timur
             * decided to follow their example and violate the spec.
             */
            throw new FTPCommandException(550, "Directory not found");
        } catch (NotDirCacheException e) {
            throw new FTPCommandException(550, "Not a directory");
        } catch (PermissionDeniedCacheException e) {
            throw new FTPCommandException(550, "Permission denied");
        } catch (EOFException e) {
            throw new FTPCommandException(426, "Connection closed; transfer aborted");
        } catch (CacheException | IOException e) {
            LOGGER.warn("Error in NLST: {}", e.getMessage());
            throw new FTPCommandException(451, "Local error in processing");
        }
    }

    @Help("MLST [<SP> <path>] - Returns data about exactly one object.")
    public void ftp_mlst(String arg) throws FTPCommandException
    {
        checkLoggedIn(ALLOW_ANONYMOUS_USER);

        try {
            FsPath path = absolutePath(arg);
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            pw.print("250- Listing " + arg + "\r\n");
            pw.print(' ');
            _listSource.printFile(_subject, _authz, new MlstFactPrinter(pw), path);
            pw.print("250 End");
            reply(sw.toString());
        } catch (InterruptedException e) {
            throw new FTPCommandException(451, "Operation cancelled");
        } catch (FileNotFoundCacheException e) {
            /**
             * see https://github.com/JasonAlt/UberFTP/issues/2
             * reply "No such file or directory" to make
             * uberftp client happy.
             */
            throw new FTPCommandException(550, "No such file or directory");
        } catch (PermissionDeniedCacheException e) {
            throw new FTPCommandException(550, "Permission denied");
        } catch (CacheException e) {
            LOGGER.warn("Error in MLST: {}", e.getMessage());
            throw new FTPCommandException(451, "Local error in processing");
        }
    }

    @Help("MLSC [<SP> <path>] - List the contents of a directory on control channel")
    public void ftp_mlsc(String arg) throws FTPCommandException
    {
        checkLoggedIn(ALLOW_ANONYMOUS_USER);

        try {
            FsPath path = absolutePath(arg.isEmpty() ? "." : arg);

            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            pw.print("250- Contents of " + path + "\r\n");
            int total = _listSource.printDirectory(_subject, _authz,
                    new MlscFactPrinter(pw), path, null, Range.<Integer>all());
            pw.print("250 MLSC completed for " + total + " files");
            reply(sw.toString());
        } catch (InterruptedException e) {
            throw new FTPCommandException(451, "Operation cancelled");
        } catch (FileNotFoundCacheException e) {
            throw new FTPCommandException(501, "Directory not found");
        } catch (NotDirCacheException e) {
            throw new FTPCommandException(501, "Not a directory");
        } catch (PermissionDeniedCacheException e) {
            throw new FTPCommandException(550, "Permission denied");
        } catch (CacheException e) {
            LOGGER.warn("Error in MLSC: {}", e.getMessage());
            throw new FTPCommandException(451, "Local error in processing");
        }
    }

    @Help("MLSD [<SP> <path>] - Lists the contents of a directory.")
    public void ftp_mlsd(String arg)
        throws FTPCommandException
    {
        checkLoggedIn(ALLOW_ANONYMOUS_USER);

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
                throw new FTPCommandException(425, "Cannot open connection");
            }

            int total;
            try {
                PrintWriter writer =
                    new PrintWriter(new OutputStreamWriter(new BufferedOutputStream(_dataSocket.getOutputStream()), "UTF-8"));

                total = _listSource.printDirectory(_subject, _authz,
                        new MlsdFactPrinter(writer), path, null,
                        Range.<Integer>all());
                writer.close();
            } finally {
                closeDataSocket();
            }
            reply("226 MLSD completed for " + total + " files");
        } catch (InterruptedException e) {
            throw new FTPCommandException(451, "Operation cancelled");
        } catch (FileNotFoundCacheException e) {
            throw new FTPCommandException(501, "Directory not found");
        } catch (NotDirCacheException e) {
            throw new FTPCommandException(501, "Not a directory");
        } catch (PermissionDeniedCacheException e) {
            throw new FTPCommandException(550, "Permission denied");
        } catch (EOFException e) {
            throw new FTPCommandException(426, "Connection closed; transfer aborted");
        } catch (CacheException | IOException e) {
            LOGGER.warn("Error in MLSD: {}", e.getMessage());
            throw new FTPCommandException(451, "Local error in processing");
        }
    }

    @Help("RNFR <SP> <path> - Rename from <path>.")
    public void ftp_rnfr(String arg) throws FTPCommandException {
        checkLoggedIn(ALLOW_ANONYMOUS_USER);

        try {
            _filepath = null;
            _fileId = null;

            checkFTPCommand(!Strings.isNullOrEmpty(arg), 500, "Missing file name for RNFR");

            FsPath path = absolutePath(arg);
            _fileId = _pnfs.getPnfsIdByPath(path.toString());
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
        checkLoggedIn(ALLOW_ANONYMOUS_USER);

        try {
            checkFTPCommand(_filepath != null, 503, "RNTO must be preceeded by RNFR");
            checkFTPCommand(!arg.isEmpty(), 500, "missing destination name for RNTO");

            FsPath newName = absolutePath(arg);
            _pnfs.renameEntry(_fileId, _filepath.toString(), newName.toString(), true);

            reply("250 File renamed");
        } catch (PermissionDeniedCacheException e) {
            throw new FTPCommandException(550, "Permission denied");
        } catch (CacheException e) {
            throw new FTPCommandException(451, "Transient error: " + e.getMessage());
        } finally {
            _filepath = null;
            _fileId = null;
        }
    }
    //----------------------------------------------
    // DCAU: data channel authtication
    // currently ( 07.04.2008 ) it's not supported
    //----------------------------------------------
    @Help("DCAU <SP> <enable> - Data channel authentication.")
    public void ftp_dcau(String arg) throws FTPCommandException
    {
        checkFTPCommand(arg.equalsIgnoreCase("N"), 202, "data channel authentication not supported");

        reply("200 data channel authentication switched off");
    }

    // ---------------------------------------------
    // QUIT: close command channel.
    // If transfer is in progress, wait for it to finish, so set pending_quit state.
    //      The delayed QUIT has not been directly implemented yet, instead...
    // Equivalent: let the data channel and pnfs entry clean-up code take care of clean-up.
    // ---------------------------------------------
    @ConcurrentWithTransfer
    @Help("QUIT - Disconnect.")
    public void ftp_quit(String arg)
        throws CommandExitException
    {

        if (!_activeWorkarounds.contains(WorkAround.NO_REPLY_ON_QUIT)) {
            reply("221 Goodbye");
        }

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
    @ConcurrentWithTransfer
    @Help("BYE - Disconnect.")
    public void ftp_bye( String arg ) throws CommandExitException
    {
        ftp_quit(arg);
    }

    // --------------------------------------------
    // ABOR: close data channels, but leave command channel open
    // ---------------------------------------------
    @ConcurrentWithTransfer
    @Help("ABOR - Abort transfer.")
    public void ftp_abor(String arg)
        throws FTPCommandException
    {
        checkLoggedIn(ALLOW_ANONYMOUS_USER);

        Transfer transfer = getTransfer();
        if (transfer instanceof FtpTransfer) {
            ((FtpTransfer)transfer).abort(new ClientAbortException(426, "Transfer aborted"));
        }
        closeDataSocket();
        reply("226 Abort successful");
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
    private void checkIsDirectory(FsPath path) throws CacheException
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
        private final CommandRequest _request;
        private final CDC _cdc;
        private boolean _stopped;
        private int _markerSendCount;
        private Optional<Instant> _lastMarkerSent = Optional.empty();
        private int _querySendCount;
        private int _queryFailCount;
        private String _lastQueryErrorMessage;
        private Optional<Instant> _lastQueryError = Optional.empty();
        private Optional<Instant> _lastQuerySent = Optional.empty();

        public PerfMarkerTask(CommandRequest request, CellAddressCore pool, int moverId, long timeout)
        {
            _pool = pool;
            _moverId = moverId;
            _timeout = timeout;
            _cdc = new CDC();
            _request = request;

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
                reply(_request, _perfMarkersBlock.markers(0).getReply());
                _lastMarkerSent = Optional.of(Instant.now());
                _markerSendCount++;
            }
        }

        private void recordQueryError(String message)
        {
            _lastQueryErrorMessage = message;
            _lastQueryError = Optional.of(Instant.now());
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
                try {
                    CellMessage msg = new CellMessage(_pool, "mover ls -binary " + _moverId);
                    _cellEndpoint.sendMessage(msg, this, _executor, _timeout);
                    _lastQuerySent = Optional.of(Instant.now());
                    _querySendCount++;
                } catch (RuntimeException e) {
                    LOGGER.error("Bug detected, please report this to <support@dcache.org>", e);
                }
            }
        }

        @Override
        public synchronized void exceptionArrived(CellMessage request, Exception exception)
        {
            _queryFailCount++;
            recordQueryError("Received exception: " + exception);
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
            _queryFailCount++;
            recordQueryError("Answer timed out");
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
                    _queryFailCount++;
                    recordQueryError("Mover has status " + status);
                } else if (status.equals("W") || status.equals("QUEUED")) {
                    sendMarker();
                } else {
                    LOGGER.error("Performance marker engine received unexcepted status from mover: {}",
                            status);
                    _queryFailCount++;
                    recordQueryError("Mover has unknown status " + status);
                }
            } else if (msg instanceof Exception) {
                LOGGER.warn("Performance marker engine: {}",
                        ((Exception) msg).getMessage());
                _queryFailCount++;
                recordQueryError("Pool responded exceptionally: " + msg);
            } else if (msg instanceof String) {
                /* Typically this is just an error message saying the
                 * mover is gone.
                 */
                LOGGER.info("Performance marker engine: {}", msg);
                _queryFailCount++;
                recordQueryError("Pool responded with message \"" + msg + "\"");
            } else {
                LOGGER.error("Performance marker engine: {}",
                        msg.getClass().getName());
                _queryFailCount++;
                recordQueryError("Unexpected pool responded: " + msg);
            }
        }

        public long getBytesTransferred()
        {
            return _perfMarkersBlock.getBytesTransferred();
        }

        public void getInfo(PrintWriter pw)
        {
            pw.println("Status: " + (_stopped ? "stopped" : "active"));
            CharSequence period = TimeUtils.duration(_performanceMarkerPeriod, TimeUnit.MILLISECONDS, TimeUnitFormat.SHORT);
            pw.println("Period: " + _performanceMarkerPeriod + " ms (" + period + ")");
            pw.println("Mover status queries:");
            pw.println("    Sent: " + _querySendCount);
            if (_querySendCount > 0) {
                pw.println("    Last sent: " + describe(_lastQuerySent));
            }
            if (_queryFailCount > 0) {
                pw.println("    Failures: " + _queryFailCount);
                pw.println("    Last failure: " + describe(_lastQueryError)
                        + " " + _lastQueryErrorMessage);
            }
            pw.println("Markers sent to client:");
            pw.println("    Sent: " + _markerSendCount);
            if (_markerSendCount > 0) {
                pw.println("    Last sent: " + describe(_lastMarkerSent));
            }
            pw.println("Latest marker information:");
            _perfMarkersBlock.getInfo(new LineIndentingPrintWriter(pw, "    "));
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
                checkFTPCommand(value == null || valuePattern != null,
                        501, "Unexpected value '%s' for %s", value, keyword);
                checkFTPCommand(valuePattern == null || valuePattern.matcher(value != null ? value : "").matches(),
                        501, "Illegal value for %s=%s", keyword, value);
                parameters.put(keyword, value);
            }
            matcher.region(matcher.end(), matcher.regionEnd());
        }

        /* Detect trailing garbage.
         */
        checkFTPCommand(matcher.regionStart() == matcher.regionEnd(),
                501, "Cannot parse '%s'", s.substring(matcher.regionStart()));
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
        replyDelayedPassive(_currentRequest, format, socketAddress);
    }

    protected void replyDelayedPassive(CommandRequest request, DelayedPassiveReply format, InetSocketAddress socketAddress)
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
            reply(request, String.format("127 PORT (%d,%d,%d,%d,%d,%d)",
                                (host[0] & 0377),
                                (host[1] & 0377),
                                (host[2] & 0377),
                                (host[3] & 0377),
                                (port / 256),
                                (port % 256)));
            break;
        case EPSV:
            reply(request, String.format("129 Entering Extended Passive Mode (|%d|%s|%d|)",
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
    public void ftp_get(String arg) throws FTPCommandException
    {
        try {
            checkFTPCommand(_skipBytes == 0, 501, "RESTART not implemented");

            Map<String,String> parameters = parseGetPutParameters(arg);

            checkFTPCommand(!parameters.containsKey("pasv") || !parameters.containsKey("port"),
                    501, "Cannot use both 'pasv' and 'port'");
            checkFTPCommand(parameters.containsKey("path"), 501, "Missing path");

            if (parameters.containsKey("mode")) {
                _xferMode = asTransferMode(parameters.get("mode"));
            }

            if (parameters.containsKey("pasv")) {
                _clientConnectionHandler.setPreferredProtocol(Protocol.IPV4);
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
                     _delayedPassive, _clientConnectionHandler.getPreferredProtocolFamily(), 2);
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
    public void ftp_put(String arg) throws FTPCommandException
    {
        Map<String,String> parameters = parseGetPutParameters(arg);

        checkFTPCommand(!parameters.containsKey("pasv") || !parameters.containsKey("port"),
                501, "Cannot use both 'pasv' and 'port'");
        checkFTPCommand(parameters.containsKey("path"), 501, "Missing path");

        if (parameters.containsKey("mode")) {
            _xferMode = asTransferMode(parameters.get("mode"));
        }

        if (parameters.containsKey("pasv")) {
            _clientConnectionHandler.setPreferredProtocol(Protocol.IPV4);
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
              _bufSize, _delayedPassive, _clientConnectionHandler.getPreferredProtocolFamily(), 2);
    }

    private void sendRemoveInfoToBilling(PnfsId pnfsId, FsPath path) {
        DoorRequestInfoMessage infoRemove = new DoorRequestInfoMessage(_cellAddress, "remove");
        infoRemove.setSubject(_subject);
        infoRemove.setBillingPath(path.toString());
        infoRemove.setPnfsId(pnfsId);
        infoRemove.setClient(_clientDataAddress.getAddress().getHostAddress());

        _billingStub.notify(infoRemove);

        if (_sendToKafka) {
            _kafkaProducer.send(new ProducerRecord<String, DoorRequestInfoMessage>("billing", infoRemove), (rm, e) -> {
                if (e != null) {
                    LOGGER.error("Unable to send message to topic {} on  partition {}: {}",
                            rm.topic(), rm.partition(), e.getMessage());
                }
            });
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

    /** The output previously used for FTP LIST command. */
    class LegacyListPrinter implements DirectoryListPrinter
    {
        private final String _userName;
        private final PrintWriter _out;
        private final PermissionHandler _pdp =
            new ChainedPermissionHandler
            (
                new ACLPermissionHandler(),
                new PosixPermissionHandler()
            );

        public LegacyListPrinter(PrintWriter writer)
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
            FsPath path = (dir == null) ? FsPath.ROOT : dir.child(entry.getName());

            if (attr.getFileType() == FileType.DIR) {
                boolean canListDir = _pdp.canListDir(_subject, attr) == ACCESS_ALLOWED &&
                        !isRestricted(LIST, path);
                boolean canLookup = _pdp.canLookup(_subject, attr) == ACCESS_ALLOWED &&
                        !isRestricted(READ_METADATA, path);
                boolean canCreateFile = _pdp.canCreateFile(_subject, attr) == ACCESS_ALLOWED &&
                        !isRestricted(UPLOAD, path);
                boolean canCreateDir = _pdp.canCreateSubDir(_subject, attr) == ACCESS_ALLOWED &&
                        !isRestricted(MANAGE, path);
                mode.append('d');
                mode.append(canListDir ? 'r' : '-');
                mode.append(canCreateFile || canCreateDir ? 'w' : '-');
                mode.append(canLookup || canListDir || canCreateFile || canCreateDir ? 'x' : '-');
                mode.append("------");
            } else {
                boolean canReadFile = _pdp.canReadFile(_subject, attr) == ACCESS_ALLOWED &&
                        !isRestricted(DOWNLOAD, path);
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
                        attr.getSizeIfPresent().orElse(0L),
                        modified,
                        entry.getName());
            _out.append("\r\n");
        }
    }

    /** A list printer that provides output as close to 'ls -l' as possible. */
    class LongListPrinter implements DirectoryListPrinter
    {
        private final PrintWriter _out;
        private final ColumnWriter _columns = new ColumnWriter()
                    .left("type")
                    .left("mode")
                    .space().right("ncount")
                    .space().left("owner")
                    .space().left("group")
                    .space().bytes("size", ByteUnit.Type.DECIMAL)
                    .space().date("time", DateStyle.LS)
                    .space().left("name");

        public LongListPrinter(PrintWriter writer)
        {
            _out = writer;
        }

        @Override
        public Set<FileAttribute> getRequiredAttributes()
        {
            Set<FileAttribute> attributes =
                EnumSet.of(TYPE, MODIFICATION_TIME, SIZE, MODE, NLINK,
                        OWNER_GROUP, OWNER, MODIFICATION_TIME);
            return attributes;
        }

        private void appendPermissions(StringBuilder sb, int mode)
        {
            sb.append((mode & 4) == 4 ? 'r' : '-');
            sb.append((mode & 2) == 2 ? 'w' : '-');
            sb.append((mode & 1) == 1 ? 'x' : '-');
        }

        private String typeFor(FileType type)
        {
            switch (type) {
            case REGULAR:
                return "-";
            case DIR:
                return "d";
            case LINK:
                return "l";
            case SPECIAL:
                return "s";
            }
            return "?";
        }

        private String permissionsFor(int mode)
        {
            StringBuilder sb = new StringBuilder();
            appendPermissions(sb, mode >> 6 & 7);
            appendPermissions(sb, mode >> 3 & 7);
            appendPermissions(sb, mode & 7);
            return sb.toString();
        }

        @Override
        public void print(FsPath dir, FileAttributes dirAttr, DirectoryEntry entry)
        {
            FileAttributes attr = entry.getFileAttributes();
            _columns.row()
                    .value("type", typeFor(attr.getFileType()))
                    .value("mode", permissionsFor(attr.getMode()))
                    .value("ncount", attr.getNlink())
                    .value("owner", _identityResolver.uidToName(attr.getOwner())
                                    .orElseGet(() -> Integer.toString(attr.getOwner())))
                    .value("group", _identityResolver.gidToName(attr.getGroup())
                                    .orElseGet(() -> Integer.toString(attr.getGroup())))
                    .value("size", attr.getSizeIfPresent().orElse(0L))
                    .value("time", new Date(attr.getModificationTime()))
                    .value("name", entry.getName());
        }

        @Override
        public void close()
        {
            String output = _columns.toString("\r\n");
            _out.print(output);
            if (!output.isEmpty()) {
                _out.print("\r\n");
            }
        }
    }

    /**
     * ListPrinter using the RFC 3659 fact line format.
     */
    private abstract class FactPrinter implements DirectoryListPrinter
    {
        private static final int MODE_MASK = 07777;

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
                case UID:
                case OWNER:
                    attributes.add(OWNER);
                    attributes.addAll(_pdp.getRequiredAttributes());
                    break;
                case GID:
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
            FsPath path = (dir == null) ? FsPath.ROOT : dir.child(entry.getName());

            if (!_currentFacts.isEmpty()) {
                AccessType access;
                FileAttributes attr = entry.getFileAttributes();

                for (Fact fact: _currentFacts) {
                    switch (fact) {
                    case SIZE:
                        if (attr.isDefined(SIZE)) {
                            access = _pdp.canGetAttributes(_subject, attr, EnumSet.of(SIZE));
                            if (access == AccessType.ACCESS_ALLOWED) {
                                printSizeFact(attr);
                            }
                        }
                        break;
                    case CREATE:
                        if (attr.isDefined(CREATION_TIME)) {
                            access = _pdp.canGetAttributes(_subject, attr, EnumSet.of(CREATION_TIME));
                            if (access == AccessType.ACCESS_ALLOWED) {
                                printCreateFact(attr);
                            }
                        }
                        break;
                    case MODIFY:
                        if (attr.isDefined(MODIFICATION_TIME)) {
                            access = _pdp.canGetAttributes(_subject, attr, EnumSet.of(MODIFICATION_TIME));
                            if (access == AccessType.ACCESS_ALLOWED) {
                                printModifyFact(attr);
                            }
                        }
                        break;
                    case CHANGE:
                        if (attr.isDefined(CHANGE_TIME)) {
                            access = _pdp.canGetAttributes(_subject, attr, EnumSet.of(CHANGE_TIME));
                            if (access == AccessType.ACCESS_ALLOWED) {
                                printChangeFact(attr);
                            }
                        }
                        break;
                    case ACCESS:
                        if (attr.isDefined(ACCESS_TIME)) {
                            access = _pdp.canGetAttributes(_subject, attr, EnumSet.of(ACCESS_TIME));
                            if (access == AccessType.ACCESS_ALLOWED) {
                                printAccessFact(attr);
                            }
                        }
                        break;
                    case TYPE:
                        if (attr.isDefined(TYPE)) {
                            access = _pdp.canGetAttributes(_subject, attr, EnumSet.of(TYPE));
                            if (access == AccessType.ACCESS_ALLOWED) {
                                printTypeFact(attr);
                            }
                        }
                        break;
                    case UNIQUE:
                        printUniqueFact(attr);
                        break;
                    case PERM:
                        access = _pdp.canGetAttributes(_subject, attr, EnumSet.of(MODE, ACL));
                        if (access == AccessType.ACCESS_ALLOWED) {
                            printPermFact(dirAttr, attr, path);
                        }
                        break;
                    case OWNER:
                        if (attr.isDefined(OWNER)) {
                            access = _pdp.canGetAttributes(_subject, attr, EnumSet.of(OWNER));
                            if (access == AccessType.ACCESS_ALLOWED) {
                                printOwnerFact(attr);
                            }
                        }
                        break;
                    case GROUP:
                        if (attr.isDefined(OWNER_GROUP)) {
                            access = _pdp.canGetAttributes(_subject, attr, EnumSet.of(OWNER_GROUP));
                            if (access == AccessType.ACCESS_ALLOWED) {
                                printGroupFact(attr);
                            }
                        }
                        break;
                    case UID:
                        if (attr.isDefined(OWNER)) {
                            access = _pdp.canGetAttributes(_subject, attr, EnumSet.of(OWNER));
                            if (access == AccessType.ACCESS_ALLOWED) {
                                printUidFact(attr);
                            }
                        }
                        break;
                    case GID:
                        if (attr.isDefined(OWNER_GROUP)) {
                            access = _pdp.canGetAttributes(_subject, attr, EnumSet.of(OWNER_GROUP));
                            if (access == AccessType.ACCESS_ALLOWED) {
                                printGidFact(attr);
                            }
                        }
                        break;
                    case MODE:
                        if (attr.isDefined(MODE)) {
                            access = _pdp.canGetAttributes(_subject, attr, EnumSet.of(MODE));
                            if (access == AccessType.ACCESS_ALLOWED) {
                                printModeFact(attr);
                            }
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
            long uid = attr.getOwner();
            printFact(Fact.OWNER, _identityResolver.uidToName(uid).orElseGet(() -> Long.toString(uid)));
        }

        /** Writes a RFC 3659 UNIX.group fact to a writer. */
        private void printGroupFact(FileAttributes attr)
        {
            long gid = attr.getGroup();
            printFact(Fact.GROUP,  _identityResolver.gidToName(gid).orElseGet(() -> Long.toString(gid)));
        }

        /** Writes a numerical uid fact to a writer. */
        private void printUidFact(FileAttributes attr)
        {
            printFact(Fact.UID, attr.getOwner());
        }

        /** Writes a numerical gid fact to a writer. */
        private void printGidFact(FileAttributes attr)
        {
            printFact(Fact.GID, attr.getGroup());
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
        private void printPermFact(FileAttributes parentAttr, FileAttributes attr, FsPath path)
        {
            StringBuilder s = new StringBuilder();
            if (attr.getFileType() == FileType.DIR) {
                if (_pdp.canCreateFile(_subject, attr) == ACCESS_ALLOWED
                        && !isRestricted(UPLOAD, path)) {
                    s.append('c');
                }
                if (_pdp.canDeleteDir(_subject, parentAttr, attr) == ACCESS_ALLOWED
                        && !isRestricted(DELETE, path)) {
                    s.append('d');
                }
                s.append('e');
                if (_pdp.canListDir(_subject, attr) == ACCESS_ALLOWED
                        && !isRestricted(LIST, path)) {
                    s.append('l');
                }
                if (_pdp.canCreateSubDir(_subject, attr) == ACCESS_ALLOWED
                        && !isRestricted(MANAGE, path)) {
                    s.append('m');
                }
            } else {
                if (_pdp.canDeleteFile(_subject, parentAttr, attr) == ACCESS_ALLOWED
                        && !isRestricted(DELETE, path)) {
                    s.append('d');
                }
                if (_pdp.canReadFile(_subject, attr) == ACCESS_ALLOWED
                        && !isRestricted(DOWNLOAD, path)) {
                    s.append('r');
                }
            }
            printFact(Fact.PERM, s);
        }

        protected abstract void printName(FsPath dir, DirectoryEntry entry);
    }

    private class MlscFactPrinter extends MlsdFactPrinter
    {
        public MlscFactPrinter(PrintWriter writer)
        {
            super(writer);
        }

        @Override
        public void print(FsPath dir, FileAttributes dirAttr, DirectoryEntry entry)
        {
            _out.print(' ');
            super.print(dir, dirAttr, entry);
        }
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
            FsPath path = (dir == null) ? FsPath.ROOT : dir.child(name);
            _out.print(path.stripPrefix(_doorRootPath));
        }
    }

    private boolean isRestricted(Activity activity, FsPath path)
    {
        return !Subjects.isRoot(_subject) && path != null &&
            _authz.isRestricted(activity, path);
    }
}
