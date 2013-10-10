// $Id$

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



package diskCacheV111.srm.dcache;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Range;
import org.apache.axis.types.UnsignedLong;
import org.globus.gsi.gssapi.GlobusGSSCredentialImpl;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;
import org.springframework.dao.DataAccessException;

import javax.annotation.Nonnull;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;
import javax.security.auth.Subject;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import diskCacheV111.poolManager.CostModule;
import diskCacheV111.poolManager.PoolMonitorV5;
import diskCacheV111.pools.PoolCostInfo;
import diskCacheV111.services.space.Space;
import diskCacheV111.services.space.SpaceState;
import diskCacheV111.services.space.message.ExtendLifetime;
import diskCacheV111.services.space.message.GetFileSpaceTokensMessage;
import diskCacheV111.services.space.message.GetSpaceMetaData;
import diskCacheV111.services.space.message.GetSpaceTokens;
import diskCacheV111.srm.StorageElementInfo;
import diskCacheV111.util.AccessLatency;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.FileExistsCacheException;
import diskCacheV111.util.FileLocality;
import diskCacheV111.util.FileNotFoundCacheException;
import diskCacheV111.util.FsPath;
import diskCacheV111.util.NotDirCacheException;
import diskCacheV111.util.NotInTrashCacheException;
import diskCacheV111.util.PermissionDeniedCacheException;
import diskCacheV111.util.PnfsHandler;
import diskCacheV111.util.PnfsId;
import diskCacheV111.util.RetentionPolicy;
import diskCacheV111.util.ThreadManager;
import diskCacheV111.util.TimeoutCacheException;
import diskCacheV111.vehicles.CopyManagerMessage;
import diskCacheV111.vehicles.IpProtocolInfo;
import diskCacheV111.vehicles.PoolManagerGetPoolMonitor;
import diskCacheV111.vehicles.RemoteHttpDataTransferProtocolInfo;
import diskCacheV111.vehicles.transferManager.CancelTransferMessage;
import diskCacheV111.vehicles.transferManager.RemoteGsiftpTransferProtocolInfo;
import diskCacheV111.vehicles.transferManager.RemoteTransferManagerMessage;
import diskCacheV111.vehicles.transferManager.TransferCompleteMessage;
import diskCacheV111.vehicles.transferManager.TransferFailedMessage;
import diskCacheV111.vehicles.transferManager.TransferManagerMessage;

import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.NoRouteToCellException;
import dmg.cells.services.login.LoginBrokerInfo;
import dmg.util.Args;

import org.dcache.acl.enums.AccessMask;
import org.dcache.acl.enums.AccessType;
import org.dcache.auth.Subjects;
import org.dcache.cells.AbstractCellComponent;
import org.dcache.cells.AbstractMessageCallback;
import org.dcache.cells.CellCommandListener;
import org.dcache.cells.CellMessageReceiver;
import org.dcache.cells.CellStub;
import org.dcache.namespace.ACLPermissionHandler;
import org.dcache.namespace.ChainedPermissionHandler;
import org.dcache.namespace.FileAttribute;
import org.dcache.namespace.FileType;
import org.dcache.namespace.PermissionHandler;
import org.dcache.namespace.PosixPermissionHandler;
import org.dcache.pinmanager.PinManagerExtendPinMessage;
import org.dcache.poolmanager.PoolMonitor;
import org.dcache.srm.AbstractStorageElement;
import org.dcache.srm.AdvisoryDeleteCallbacks;
import org.dcache.srm.CopyCallbacks;
import org.dcache.srm.FileMetaData;
import org.dcache.srm.PinCallbacks;
import org.dcache.srm.PrepareToPutCallbacks;
import org.dcache.srm.PrepareToPutInSpaceCallbacks;
import org.dcache.srm.RemoveFileCallbacks;
import org.dcache.srm.SRM;
import org.dcache.srm.SRMAuthorizationException;
import org.dcache.srm.SRMDuplicationException;
import org.dcache.srm.SRMException;
import org.dcache.srm.SRMInternalErrorException;
import org.dcache.srm.SRMInvalidPathException;
import org.dcache.srm.SRMInvalidRequestException;
import org.dcache.srm.SRMUser;
import org.dcache.srm.SrmCancelUseOfSpaceCallbacks;
import org.dcache.srm.SrmReleaseSpaceCallbacks;
import org.dcache.srm.SrmReserveSpaceCallbacks;
import org.dcache.srm.SrmUseSpaceCallbacks;
import org.dcache.srm.UnpinCallbacks;
import org.dcache.srm.request.Job;
import org.dcache.srm.request.RequestCredential;
import org.dcache.srm.scheduler.IllegalStateTransition;
import org.dcache.srm.util.Configuration;
import org.dcache.srm.util.Permissions;
import org.dcache.srm.util.Tools;
import org.dcache.srm.v2_2.TAccessLatency;
import org.dcache.srm.v2_2.TMetaDataSpace;
import org.dcache.srm.v2_2.TRetentionPolicy;
import org.dcache.srm.v2_2.TRetentionPolicyInfo;
import org.dcache.srm.v2_2.TReturnStatus;
import org.dcache.srm.v2_2.TStatusCode;
import org.dcache.util.LoginBrokerHandler;
import org.dcache.util.Version;
import org.dcache.util.list.DirectoryEntry;
import org.dcache.util.list.DirectoryListPrinter;
import org.dcache.util.list.DirectoryListSource;
import org.dcache.vehicles.FileAttributes;

import static com.google.common.net.InetAddresses.isInetAddress;
import static org.dcache.namespace.FileAttribute.*;

/**
 * The Storage class bridges between the SRM server and dCache.
 *
 * @author Timur Perelmutov
 * @author FNAL,CD/ISD
 */
public final class Storage
    extends AbstractCellComponent
    implements AbstractStorageElement, Runnable,
               CellCommandListener, CellMessageReceiver
{
    private final static Logger _log = LoggerFactory.getLogger(Storage.class);

    private final static String INFINITY = "infinity";

    private static final String SPACEMANAGER_DISABLED_MESSAGE =
            "space reservation is disabled";

    /* these are the  protocols
     * that are not suitable for either put or get */
    private String[] srmPutNotSupportedProtocols;
    private String[] srmGetNotSupportedProtocols;
    private String[] srmPreferredProtocols;

    private final static String SFN_STRING = "SFN=";

    /**
     * The delay we use after transient failures that should be
     * retried immediately. The small delay prevents tight retry
     * loops.
     */
    private final static long TRANSIENT_FAILURE_DELAY =
        TimeUnit.MILLISECONDS.toMillis(10);
    private static final Version VERSION = Version.of(Storage.class);

    private CellStub _pnfsStub;
    private CellStub _poolManagerStub;
    private CellStub _spaceManagerStub;
    private CellStub _copyManagerStub;
    private CellStub _transferManagerStub;
    private CellStub _pinManagerStub;
    private CellStub _loginBrokerStub;
    private CellStub _gplazmaStub;

    private PnfsHandler _pnfs;
    private final PermissionHandler permissionHandler =
            new ChainedPermissionHandler(new ACLPermissionHandler(),
                                         new PosixPermissionHandler());

    private PoolMonitor _poolMonitor;

    private SRM srm;
    private Configuration config;
    private Thread storageInfoUpdateThread;
    private boolean customGetHostByAddr; //falseByDefault

    private FsPath _xrootdRootPath;
    private FsPath _httpRootPath;

    private DirectoryListSource _listSource;

    private boolean _isOnlinePinningEnabled = true;
    private boolean _isSpaceManagerEnabled;

    @Required
    public void setLoginBrokerStub(CellStub loginBrokerStub)
    {
        _loginBrokerStub = loginBrokerStub;
    }

    @Required
    public void setPnfsStub(CellStub pnfsStub)
    {
        _pnfsStub = pnfsStub;
    }

    @Required
    public void setIsSpaceManagerEnabled(boolean isEnabled)
    {
        _isSpaceManagerEnabled = isEnabled;
    }

    public void setSpaceManagerStub(CellStub spaceManagerStub)
    {
        _spaceManagerStub = spaceManagerStub;
    }

    @Required
    public void setPoolManagerStub(CellStub poolManagerStub)
    {
        _poolManagerStub = poolManagerStub;
    }

    @Required
    public void setTransferManagerStub(CellStub transferManagerStub)
    {
        _transferManagerStub = transferManagerStub;
    }

    @Required
    public void setCopyManagerStub(CellStub copyManagerStub)
    {
        _copyManagerStub = copyManagerStub;
    }

    @Required
    public void setPinManagerStub(CellStub pinManagerStub)
    {
        _pinManagerStub = pinManagerStub;
    }

    @Required
    public void setGplazmaStub(CellStub gplazmaStub)
    {
        _gplazmaStub = gplazmaStub;
    }

    @Required
    public void setPnfsHandler(PnfsHandler pnfs)
    {
        _pnfs = pnfs;
    }

    @Required
    public void setHttpRootPath(String path)
    {
        _httpRootPath = new FsPath(path);
    }

    @Required
    public void setXrootdRootPath(String path)
    {
        _xrootdRootPath = new FsPath(path);
    }

    @Required
    public void setConfiguration(Configuration config)
    {
        this.config = config;
    }

    @Required
    public void setSrm(SRM srm)
    {
        this.srm = srm;
    }

    public String[] getSrmPutNotSupportedProtocols()
    {
        return srmPutNotSupportedProtocols;
    }

    @Required
    public void setSrmPutNotSupportedProtocols(String[] srmPutNotSupportedProtocols)
    {
        this.srmPutNotSupportedProtocols = srmPutNotSupportedProtocols;
    }

    public String[] getSrmGetNotSupportedProtocols()
    {
        return srmGetNotSupportedProtocols;
    }

    @Required
    public void setSrmGetNotSupportedProtocols(String[] srmGetNotSupportedProtocols)
    {
        this.srmGetNotSupportedProtocols = srmGetNotSupportedProtocols;
    }

    public String[] getSrmPreferredProtocols()
    {
        return srmPreferredProtocols;
    }

    @Required
    public void setSrmPreferredProtocols(String[] srmPreferredProtocols)
    {
        this.srmPreferredProtocols = srmPreferredProtocols;
    }

    public void setPinOnlineFiles(boolean value)
    {
        _isOnlinePinningEnabled = value;
    }

    public void setLoginBrokerUpdatePeriod(long period)
    {
        LOGINBROKERINFO_VALIDITYSPAN = period;
    }

    public void setNumberOfDoorsInRandomSelection(int value)
    {
        numDoorInRanSelection = value;
    }

    public void setUseCustomGetHostByAddress(boolean value)
    {
        customGetHostByAddr = value;
    }

    public void start() throws CacheException, IOException,
            InterruptedException, IllegalStateTransition
    {
        _log.info("Starting SRM");

        while (_poolMonitor == null) {
            try {
                _poolMonitor =
                    _poolManagerStub.sendAndWait(new PoolManagerGetPoolMonitor()).getPoolMonitor();
            } catch (CacheException e) {
                _log.error(e.toString());
                Thread.sleep(TRANSIENT_FAILURE_DELAY);
            }
        }

        storageInfoUpdateThread = new Thread(this);
        storageInfoUpdateThread.start();
    }

    public void stop()
    {
        storageInfoUpdateThread.interrupt();
    }

    @Required
    public void setDirectoryListSource(DirectoryListSource source)
    {
        _listSource = source;
    }

    public static long parseTime(String s)
    {
        return s.equals(INFINITY) ? Long.MAX_VALUE : Long.parseLong(s);
    }

    public static long parseTime(String s, TimeUnit unit)
    {
        return s.equals(INFINITY) ? Long.MAX_VALUE : TimeUnit.MILLISECONDS.convert(Long.parseLong(s),unit);
    }

    @Required
    public void setLoginBrokerHandler(LoginBrokerHandler handler)
        throws UnknownHostException
    {
        handler.setAddresses(Arrays.asList(InetAddress.getAllByName(InetAddress.getLocalHost().getHostName())));
        handler.setLoad(new LoginBrokerHandler.LoadProvider() {
                @Override
                public double getLoad() {
                    return (srm == null) ? 0 : srm.getLoad();
                }
            });
    }

    @Override
    public void getInfo(PrintWriter pw)
    {
        StorageElementInfo info = getStorageElementInfo();
        if (info != null) {
            pw.println(info);
        }

        pw.println(config);

        StringBuilder sb = new StringBuilder();
        srm.printGetSchedulerInfo(sb);
        srm.printPutSchedulerInfo(sb);
        srm.printCopySchedulerInfo(sb);
        srm.printBringOnlineSchedulerInfo(sb);
        srm.printLsSchedulerInfo(sb);
        pw.println(sb);
    }

    public final static String hh_set_switch_to_async_mode_delay_get =
        "<milliseconds>";
    public final static String fh_set_switch_to_async_mode_delay_get =
        "Sets the time after which get requests are processed asynchronously.\n" +
        "Use 'infinity' to always use synchronous replies and use 0 to\n" +
        "always use asynchronous replies.";
    public String ac_set_switch_to_async_mode_delay_get_$_1(Args args)
    {
        config.setGetSwitchToAsynchronousModeDelay(parseTime(args.argv(0)));
        return "";
    }

    public final static String hh_set_switch_to_async_mode_delay_put =
        "<milliseconds>";
    public final static String fh_set_switch_to_async_mode_delay_put =
        "Sets the time after which put requests are processed asynchronously.\n" +
        "Use 'infinity' to always use synchronous replies and use 0 to\n" +
        "always use asynchronous replies.";
    public String ac_set_switch_to_async_mode_delay_put_$_1(Args args)
    {
        config.setPutSwitchToAsynchronousModeDelay(parseTime(args.argv(0)));
        return "";
    }

    public final static String hh_set_switch_to_async_mode_delay_ls =
        "<milliseconds>";
    public final static String fh_set_switch_to_async_mode_delay_ls =
        "Sets the time after which ls requests are processed asynchronously.\n" +
        "Use 'infinity' to always use synchronous replies and use 0 to\n" +
        "always use asynchronous replies.";
    public String ac_set_switch_to_async_mode_delay_ls_$_1(Args args)
    {
        config.setLsSwitchToAsynchronousModeDelay(parseTime(args.argv(0)));
        return "";
    }

    public final static String hh_set_switch_to_async_mode_delay_bring_online =
        "<milliseconds>";
    public final static String fh_set_switch_to_async_mode_delay_bring_online =
        "Sets the time after which bring online requests are processed\n" +
        "asynchronously. Use 'infinity' to always use synchronous replies\n" +
        "and use 0 to always use asynchronous replies.";
    public String ac_set_switch_to_async_mode_delay_bring_online_$_1(Args args)
    {
        config.setBringOnlineSwitchToAsynchronousModeDelay(parseTime(args.argv(0)));
        return "";
    }

    private static final ImmutableMap<String,String> OPTION_TO_PARAMETER_SET =
        new ImmutableMap.Builder<String,String>()
        .put("get", Configuration.GET_PARAMETERS)
        .put("put", Configuration.PUT_PARAMETERS)
        .put("ls", Configuration.LS_PARAMETERS)
        .put("bringonline", Configuration.BRINGONLINE_PARAMETERS)
        .put("reserve", Configuration.RESERVE_PARAMETERS)
        .build();

    public final static String fh_db_history_log= " Syntax: db history log [on|off] "+
        "# show status or enable db history log ";
    public final static String hh_db_history_log= "[-get] [-put] [-bringonline] [-ls] [-copy] [-reserve] [on|off] " +
        "# show status or enable db history log ";
    public String ac_db_history_log_$_0_1(Args args)
    {
        Collection<String> sets = new ArrayList<>();
        for (Map.Entry<String,String> e: OPTION_TO_PARAMETER_SET.entrySet()) {
            if (args.hasOption(e.getKey())) {
                sets.add(e.getValue());
            }
        }

        if (sets.isEmpty()) {
            sets = OPTION_TO_PARAMETER_SET.values();
        }

        if (args.argc() > 0) {
            String arg = args.argv(0);
            if (!arg.equals("on") && !arg.equals("off")){
                return "syntax error";
            }
            for (String set: sets) {
                config.getDatabaseParameters(set).setRequestHistoryDatabaseEnabled(arg.equals("on"));
            }
        }

        StringBuilder s = new StringBuilder();
        for (String set: sets) {
            Configuration.DatabaseParameters parameters = config.getDatabaseParameters(set);
            s.append("db history logging for ").append(set).append(" is ")
                .append((parameters.isRequestHistoryDatabaseEnabled()
                         ? "enabled"
                         : "disabled")).append("\n");
        }
        return s.toString();
    }

    public final static String fh_cancel= " Syntax: cancel <id> ";
    public final static String hh_cancel= " <id> ";
    public String ac_cancel_$_1(Args args) {
        try {
            Long id = Long.valueOf(args.argv(0));
            StringBuilder sb = new StringBuilder();
            srm.cancelRequest(sb, id);
            return sb.toString();
        } catch (SRMInvalidRequestException ire) {
            return "Invalid request: "+ire.getMessage();
        } catch (NumberFormatException e) {
            return e.toString();
        }
    }

    public final static String fh_cancelall= " Syntax: cancel [-get] [-put] [-copy] [-bring] [-reserve] <pattern> ";
    public final static String hh_cancelall= " [-get] [-put] [-copy] [-bring] [-reserve] <pattern> ";
    public String ac_cancelall_$_1(Args args) {
        try {
            boolean get=args.hasOption("get");
            boolean put=args.hasOption("put");
            boolean copy=args.hasOption("copy");
            boolean bring=args.hasOption("bring");
            boolean reserve=args.hasOption("reserve");
            boolean ls=args.hasOption("ls");
            if( !get && !put && !copy && !bring && !reserve && !ls ) {
                get=true;
                put=true;
                copy=true;
                bring=true;
                reserve=true;
                ls=true;
            }
            String pattern = args.argv(0);
            StringBuilder sb = new StringBuilder();
            if(get) {
                _log.debug("calling srm.cancelAllGetRequest(\""+pattern+"\")");
                srm.cancelAllGetRequest(sb, pattern);
            }
            if(bring) {
                _log.debug("calling srm.cancelAllBringOnlineRequest(\""+pattern+"\")");
                srm.cancelAllBringOnlineRequest(sb, pattern);
            }
            if(put) {
                _log.debug("calling srm.cancelAllPutRequest(\""+pattern+"\")");
                srm.cancelAllPutRequest(sb, pattern);
            }
            if(copy) {
                _log.debug("calling srm.cancelAllCopyRequest(\""+pattern+"\")");
                srm.cancelAllCopyRequest(sb, pattern);
            }
            if(reserve) {
                _log.debug("calling srm.cancelAllReserveSpaceRequest(\""+pattern+"\")");
                srm.cancelAllReserveSpaceRequest(sb, pattern);
            }
            if(ls) {
                _log.debug("calling srm.cancelAllLsRequests(\""+pattern+"\")");
                srm.cancelAllLsRequests(sb, pattern);
            }
            return sb.toString();
        } catch (DataAccessException | SRMException e) {
            _log.warn("Failure in cancelall: " + e.getMessage());
            return e.toString();
        }
    }
    public final static String fh_ls= " Syntax: ls [-get] [-put] [-copy] [-bring] [-reserve] [-ls] [-l] [<id>] "+
            "#will list all requests";
    public final static String hh_ls= " [-get] [-put] [-copy] [-bring] [-reserve] [-ls] [-l] [<id>]";
    public String ac_ls_$_0_1(Args args) throws SRMInvalidRequestException, DataAccessException
    {
        boolean get=args.hasOption("get");
        boolean put=args.hasOption("put");
        boolean copy=args.hasOption("copy");
        boolean bring=args.hasOption("bring");
        boolean reserve=args.hasOption("reserve");
        boolean ls=args.hasOption("ls");
        boolean longformat = args.hasOption("l");
        StringBuilder sb = new StringBuilder();
        if(args.argc() == 1) {
            try {
                Long reqId = Long.valueOf(args.argv(0));
                srm.listRequest(sb, reqId, longformat);
            } catch( NumberFormatException nfe) {
                return "id must be an integer, you gave id="+args.argv(0);
            }
        } else {
            if( !get && !put && !copy && !bring && !reserve && !ls) {
                get=true;
                put=true;
                copy=true;
                bring=true;
                reserve=true;
                ls=true;
            }
            if(get) {
                sb.append("Get Requests:\n");
                srm.listGetRequests(sb);
            }
            if(put) {
                sb.append("Put Requests:\n");
                srm.listPutRequests(sb);
            }
            if(copy) {
                sb.append("Copy Requests:\n");
                srm.listCopyRequests(sb);
            }
            if(copy) {
                sb.append("Bring Online Requests:\n");
                srm.listBringOnlineRequests(sb);
            }
            if(reserve) {
                sb.append("Reserve Space Requests:\n");
                srm.listReserveSpaceRequests(sb);
            }
            if(ls) {
                sb.append("Ls Requests:\n");
                srm.listLsRequests(sb);
            }
        }
        return sb.toString();
    }
    public final static String fh_ls_queues= " Syntax: ls queues " +
        "[-get] [-put] [-copy] [-bring] [-ls] [-l]  "+
            "#will list schedule queues";
    public final static String hh_ls_queues= " [-get] [-put] [-copy] [-bring] [-ls] [-l] ";
    public String ac_ls_queues_$_0(Args args) {
        boolean get=args.hasOption("get");
        boolean put=args.hasOption("put");
        boolean ls=args.hasOption("ls");
        boolean copy=args.hasOption("copy");
        boolean bring=args.hasOption("bring");
        StringBuilder sb = new StringBuilder();

        if( !get && !put && !copy && !bring && !ls ) {
            get=true;
            put=true;
            copy=true;
            bring=true;
            ls=true;
        }
        if(get) {
            sb.append("Get Request Scheduler:\n");
            srm.printGetSchedulerThreadQueue(sb);
            srm.printGetSchedulerPriorityThreadQueue(sb);
            srm.printCopySchedulerReadyThreadQueue(sb);
            sb.append('\n');
        }
        if(put) {
            sb.append("Put Request Scheduler:\n");
            srm.printPutSchedulerThreadQueue(sb);
            srm.printPutSchedulerPriorityThreadQueue(sb);
            srm.printPutSchedulerReadyThreadQueue(sb);
            sb.append('\n');
        }
        if(copy) {
            sb.append("Copy Request Scheduler:\n");
            srm.printCopySchedulerThreadQueue(sb);
            srm.printCopySchedulerPriorityThreadQueue(sb);
            srm.printCopySchedulerReadyThreadQueue(sb);
            sb.append('\n');
        }
        if(bring) {
            sb.append("Bring Online Request Scheduler:\n");
            srm.printBringOnlineSchedulerThreadQueue(sb);
            srm.printBringOnlineSchedulerPriorityThreadQueue(sb);
            srm.printBringOnlineSchedulerReadyThreadQueue(sb);
            sb.append('\n');
        }
        if(ls) {
            sb.append("Ls Request Scheduler:\n");
            srm.printLsSchedulerThreadQueue(sb);
            srm.printLsSchedulerPriorityThreadQueue(sb);
            srm.printLsSchedulerReadyThreadQueue(sb);
            sb.append('\n');
        }
        return sb.toString();
    }

    public final static String fh_ls_completed= " Syntax: ls completed [-get] [-put]" +
        " [-copy] [max_count]"+
            " #will list completed (done, failed or canceled) requests, " +
        "if max_count is not specified, it is set to 50";
    public final static String hh_ls_completed= " [-get] [-put] [-copy] [max_count]";
    public String ac_ls_completed_$_0_1(Args args) throws DataAccessException
    {
        boolean get=args.hasOption("get");
        boolean put=args.hasOption("put");
        boolean copy=args.hasOption("copy");
        int max_count=50;
        if(args.argc() == 1) {
            max_count = Integer.parseInt(args.argv(0));
        }

        if( !get && !put && !copy ) {
            get=true;
            put=true;
            copy=true;

        }
        StringBuilder sb = new StringBuilder();
        if(get) {
            sb.append("Get Requests:\n");
            srm.listLatestCompletedGetRequests(sb, max_count);
            sb.append('\n');
        }
        if(put) {
            sb.append("Put Requests:\n");
            srm.listLatestCompletedPutRequests(sb, max_count);
            sb.append('\n');
        }
        if(copy) {
            sb.append("Copy Requests:\n");
            srm.listLatestCompletedCopyRequests(sb, max_count);
            sb.append('\n');
        }
        return sb.toString();
    }

    public final static String fh_set_job_priority= " Syntax: set priority <requestId> <priority>"+
            "will set priority for the requestid";
    public final static String hh_set_job_priority=" <requestId> <priority>";

    public String ac_set_job_priority_$_2(Args args) {
        String s1 = args.argv(0);
        String s2 = args.argv(1);
        long requestId;
        int priority;
        try {
            requestId = Integer.parseInt(s1);
        } catch (NumberFormatException e) {
            return "Failed to parse request id: " + s1;
        }
        try {
            priority = Integer.parseInt(s2);
        } catch (Exception e) {
            return "Failed to parse priority: "+s2;
        }
        try {
            Job job = Job.getJob(requestId, Job.class);
            job.setPriority(priority);
            job.setPriority(priority);
            StringBuilder sb = new StringBuilder();
            srm.listRequest(sb, requestId, true);
            return sb.toString();
        } catch (SRMInvalidRequestException e) {
            return e.getMessage() + "\n";
        } catch (DataAccessException e) {
            _log.warn("Failure in set job priority: " + e.getMessage());
            return e.toString();
        }
    }


    public final static String fh_set_max_ready_put= " Syntax: set max ready put <count>"+
            " #will set a maximum number of put requests in the ready state";
    public final static String hh_set_max_ready_put= " <count>";
    public String ac_set_max_ready_put_$_1(Args args) throws Exception{
        if(args.argc() != 1) {
            throw new IllegalArgumentException("count is not specified");
        }
        int value = Integer.parseInt(args.argv(0));
        config.setPutMaxReadyJobs(value);
        srm.getPutRequestScheduler().setMaxReadyJobs(value);
        _log.info("put-req-max-ready-requests="+value);
        return "put-req-max-ready-requests="+value;
    }

    public final static String fh_set_max_ready_get= " Syntax: set max ready get <count>"+
            " #will set a maximum number of get requests in the ready state";
    public final static String hh_set_max_ready_get= " <count>";
    public String ac_set_max_ready_get_$_1(Args args) throws Exception{
        if(args.argc() != 1) {
            throw new IllegalArgumentException("count is not specified");
        }
        int value = Integer.parseInt(args.argv(0));
        config.setGetMaxReadyJobs(value);
        srm.getGetRequestScheduler().setMaxReadyJobs(value);
        _log.info("get-req-max-ready-requests="+value);
        return "get-req-max-ready-requests="+value;
    }

    public final static String fh_set_max_ready_bring_online= " Syntax: set max ready bring online <count>"+
            " #will set a maximum number of bring online requests in the ready state";
    public final static String hh_set_max_ready_bring_online= " <count>";
    public String ac_set_max_ready_bring_online_$_1(Args args) throws Exception{
        if(args.argc() != 1) {
            throw new IllegalArgumentException("count is not specified");
        }
        int value = Integer.parseInt(args.argv(0));
        config.setBringOnlineMaxReadyJobs(value);
        srm.getBringOnlineRequestScheduler().setMaxReadyJobs(value);
        _log.info("bring-online-req-max-ready-requests="+value);
        return "bring-online-req-max-ready-requests="+value;
    }

    public final static String fh_set_max_read_ls_= " Syntax: set max read ls <count>\n"+
            " #will set a maximum number of ls requests in the ready state\n"+
            " #\"set max read ls\" is an alias for \"set max ready ls\" preserved for compatibility ";
    public final static String hh_set_max_read_ls= " <count>";
    public String ac_set_read_ls_$_1(Args args) throws Exception{
        return ac_set_max_ready_ls_$_1(args);
    }

    public final static String fh_set_max_ready_ls= " Syntax: set max ready ls <count>\n"+
            " #will set a maximum number of ls requests in the ready state";
    public final static String hh_set_max_ready_ls= " <count>";
    public String ac_set_max_ready_ls_$_1(Args args) throws Exception{
        if(args.argc() != 1) {
            throw new IllegalArgumentException("count is not specified");
        }
        int value = Integer.parseInt(args.argv(0));
        config.setLsMaxReadyJobs(value);
        srm.getLsRequestScheduler().setMaxReadyJobs(value);
        _log.info("ls-request-max-ready-requests="+value);
        return "ls-request-max-ready-requests="+value;
    }

      public final static String fh_dir_creators_ls= " Syntax: dir creators ls [-l]  "+
         "#will list all put companion waiting for the dir creation ";
      public final static String hh_dir_creators_ls= " [-l] ";
      public String ac_dir_creators_ls_$_0(Args args) {
        try {
            boolean longformat = args.hasOption("l");
            StringBuilder sb = new StringBuilder();
            PutCompanion.listDirectoriesWaitingForCreation(sb,longformat);
            return sb.toString();
         } catch(Throwable t) {
            t.printStackTrace();
            return t.toString();
         }
      }
      public final static String fh_cancel_dir_creation= " Syntax:cancel dir creation <path>  "+
         "#will fail companion waiting for the dir creation on <path> ";
      public final static String hh_cancel_dir_creation= " <path>";
      public String ac_cancel_dir_creation_$_1(Args args) {
        try {
            String pnfsPath = args.argv(0);
            StringBuilder sb = new StringBuilder();
            PutCompanion.failCreatorsForPath(pnfsPath,sb);
            return sb.toString();
         } catch(Throwable t) {
            t.printStackTrace();
            return t.toString();
         }
      }

      public final static String hh_print_srm_counters= "# prints the counters for all srm operations";
      public String ac_print_srm_counters_$_0(Args args) {
            return srm.getSrmServerV1Counters().toString()+
                    '\n'+
                   srm.getSrmServerV2Counters().toString()+
                   '\n'+
                   srm.getAbstractStorageElementCounters().toString()+
                   '\n'+
                   srm.getSrmServerV1Gauges().toString()+
                   '\n'+
                   srm.getSrmServerV2Gauges().toString()+
                   '\n'+
                   srm.getAbstractStorageElementGauges().toString();
      }


    /**
     * Starts new threads for processing TransferManagerMessage.
     */
    public void messageArrived(final TransferManagerMessage msg)
    {
        ThreadManager.execute(new Runnable() {
                @Override
                public void run() {
                    handleTransferManagerMessage(msg);
                }
            });
    }

    @Override
    public void pinFile(SRMUser user,
                        URI surl,
                        String clientHost,
                        long pinLifetime,
                        String requestToken,
                        PinCallbacks callbacks)
    {
        try {
            PinCompanion.pinFile(((DcacheUser) user).getSubject(),
                                 getPath(surl),
                                 clientHost,
                                 callbacks,
                                 pinLifetime,
                                 requestToken,
                                 _isOnlinePinningEnabled,
                                 _poolMonitor,
                                 _pnfsStub,
                                 _poolManagerStub,
                                 _pinManagerStub);
        } catch (SRMInvalidPathException e) {
            callbacks.FileNotFound(e.getMessage());
        }
    }

    @Override
    public void unPinFile(SRMUser user, String fileId,
                          UnpinCallbacks callbacks,
                          String pinId)
    {
        if (PinCompanion.isFakePinId(pinId)) {
            return;
        }

        UnpinCompanion.unpinFile(((DcacheUser) user).getSubject(),
                                 new PnfsId(fileId), Long.parseLong(pinId), callbacks,_pinManagerStub);
    }

    @Override
    public void unPinFileBySrmRequestId(SRMUser user, String fileId,
                                        UnpinCallbacks callbacks,
                                        String requestToken)
    {
        UnpinCompanion.unpinFileBySrmRequestId(((DcacheUser) user).getSubject(),
                new PnfsId(fileId), requestToken, callbacks, _pinManagerStub);
    }

    @Override
    public void unPinFile(SRMUser user, String fileId, UnpinCallbacks callbacks)
    {
        UnpinCompanion.unpinFile(((DcacheUser) user).getSubject(),
                                 new PnfsId(fileId), callbacks, _pinManagerStub);
    }

    public String selectGetProtocol(String[] protocols)
            throws SRMException {
        return selectProtocolFor(protocols, srmGetNotSupportedProtocols);
    }

    public String selectPutProtocol(String[] protocols)
            throws SRMException {
        return selectProtocolFor(protocols, srmPutNotSupportedProtocols);
    }

    private String selectProtocolFor(String[] protocols, String[] excludes)
    throws SRMException {
        Set<String> available_protocols = listAvailableProtocols();
        available_protocols.retainAll(Arrays.asList(protocols));
        available_protocols.removeAll(Arrays.asList(excludes));
        if(available_protocols.isEmpty()) {
            _log.error("can not find sutable protocol");
            throw new SRMException("can not find sutable put protocol");
        }

        for (String protocol : srmPreferredProtocols) {
            if (available_protocols.contains(protocol)) {
                return protocol;
            }
        }

        for (String protocol : protocols) {
            if (available_protocols.contains(protocol)) {
                return protocol;
            }
        }

        // we should never get here
        throw new SRMException("can not find sutable put protocol");
    }

    @Override
    public String[] supportedGetProtocols()
    throws SRMException {
        Set<String> protocols = listAvailableProtocols();
        return protocols.toArray(new String[protocols.size()]);
    }

    @Override
    public String[] supportedPutProtocols()
    throws SRMException {
        Set<String> protocols = listAvailableProtocols();
        // "http" is for getting only
        if(protocols.contains("http")) {
            protocols.remove("http");
        }
        return protocols.toArray(new String[protocols.size()]);
    }

    @Override
    public URI getGetTurl(SRMUser user, URI surl, String[] protocols)
        throws SRMException
    {
        FsPath path = getPath(surl);
        String protocol = selectGetProtocol(protocols);
        return getTurl(path, protocol, user);
    }

    @Override
    public URI getGetTurl(SRMUser user, URI surl, URI previous_turl)
        throws SRMException
    {
        FsPath actualFilePath = getPath(surl);
        String host = previous_turl.getHost();
        int port = previous_turl.getPort();
        return getTurl(actualFilePath, previous_turl.getScheme(), host, port, user);
    }

    @Override
    public URI getPutTurl(SRMUser user, URI surl, String[] protocols)
        throws SRMException
    {
        FsPath path = getPath(surl);
        String protocol = selectPutProtocol(protocols);
        return getTurl(path, protocol, user);
    }

    @Override
    public URI getPutTurl(SRMUser user, URI surl, URI previous_turl)
        throws SRMException
    {
        FsPath path = getPath(surl);
        String host = previous_turl.getHost();
        int port = previous_turl.getPort();
        return getTurl(path, previous_turl.getScheme(), host, port, user);
    }

    private URI getTurl(FsPath path,String protocol,SRMUser user)
        throws SRMException
    {
        if (protocol == null) {
            throw new IllegalArgumentException("protocol is null");
        }
        String hostPort = selectHost(protocol);
        int index = hostPort.indexOf(':');
        if (index > -1) {
            String host = hostPort.substring(0, index);
            int port = Integer.parseInt(hostPort.substring(index + 1));
            return getTurl(path, protocol, host, port, user);
        } else {
            return getTurl(path, protocol, hostPort, 0, user);
        }
    }

    private static boolean isHostAndPortNeeded(String protocol) {
        return !protocol.equalsIgnoreCase("file");
    }

    private URI getTurl(FsPath path, String protocol,
                        String host, int port, SRMUser user)
        throws SRMException
    {
        if (path == null) {
            throw new IllegalArgumentException("path is null");
        }
        if (protocol == null) {
            throw new IllegalArgumentException("protocol is null");
        }
        if (host == null) {
            throw new IllegalArgumentException("host is null");
        }
        String transfer_path = getTurlPath(path,protocol,user);
        if (transfer_path == null) {
            throw new SRMException("cannot get transfer path");
        }
        try {
            if (port == 0) {
                port = -1;
            }
            URI turl = isHostAndPortNeeded(protocol) ?
                    new URI(protocol, null, host, port, transfer_path, null, null):
                    new URI(protocol, null, transfer_path, null);

            _log.debug("getTurl() returns turl={}", turl);
            return turl;
        } catch (URISyntaxException e) {
            throw new SRMInternalErrorException(e.getMessage());
        }
    }

    private boolean verifyUserPathIsRootSubpath(FsPath absolutePath, SRMUser user) {
        if (absolutePath == null) {
            return false;
        }
        FsPath user_root = null;
        if (user != null) {
            DcacheUser duser = (DcacheUser) user;
            user_root = duser.getRoot();
        }
        if (user_root!= null) {
            _log.trace("getTurl() user root is {}", user_root);
            if (!absolutePath.startsWith(user_root)) {
                _log.warn("verifyUserPathIsInTheRoot error: user's path {} is not subpath of the user's root {}",
                        absolutePath, user_root);
                return false;
            }
        }
        return true;
    }

    private String stripRootPath(FsPath root, FsPath path)
        throws SRMAuthorizationException
    {
        if (!path.startsWith(root)) {
            throw new SRMAuthorizationException(String.format("Access denied for path [%s]", path));
        }

        List<String> l = path.getPathItemsList();
        return FsPath.toString(l.subList(root.getPathItemsList().size(),
                                         l.size()));
    }

    private String getTurlPath(FsPath path, String protocol, SRMUser user)
        throws SRMException
    {
        FsPath userRoot = new FsPath();
        if (user != null) {
            userRoot = ((DcacheUser) user).getRoot();
        }

        if (!verifyUserPathIsRootSubpath(path, user)) {
            throw new SRMAuthorizationException(String.format("Access denied: Path [%s] is outside user's root [%s]", path, userRoot));
        }

        String transferPath;
        switch (protocol) {
        case "gsiftp":
            transferPath = stripRootPath(userRoot, path);
            break;
        case "http":
        case "https":
            transferPath = stripRootPath(_httpRootPath, path);
            break;
        case "root":
            transferPath = stripRootPath(_xrootdRootPath, path);
            break;
        default:
            transferPath = path.toString();
            break;
        }

        _log.debug("getTurlPath(path=" + path + ",protocol=" + protocol +
            ",user=" + user + ") = " + transferPath);

        return transferPath;
    }

    public LoginBrokerInfo[] getLoginBrokerInfos()
        throws SRMException
    {
        return getLoginBrokerInfos(null);
    }

    // These hashtables are used as a caching mechanizm for the login
    // broker infos. Here we asume that no protocol called "null" is
    // going to be ever used.
    private final Map<String,LoginBrokerInfo[]> latestLoginBrokerInfos =
        new HashMap<>();
    private final Map<String,Long> latestLoginBrokerInfosTimes =
        new HashMap<>();
    private long LOGINBROKERINFO_VALIDITYSPAN = 30 * 1000;
    private static final int MAX_LOGIN_BROKER_RETRIES=5;

    public LoginBrokerInfo[] getLoginBrokerInfos(String protocol)
        throws SRMException
    {
        String key = (protocol == null) ? "null" : protocol;

        synchronized (latestLoginBrokerInfosTimes) {
            Long timestamp = latestLoginBrokerInfosTimes.get(key);
            if (timestamp !=null) {
                long age = System.currentTimeMillis() - timestamp;
                if (age < LOGINBROKERINFO_VALIDITYSPAN) {
                    LoginBrokerInfo[] infos = latestLoginBrokerInfos.get(key);
                    if (infos != null) {
                        return infos;
                    }
                }
            }
        }

        String brokerMessage = "ls -binary";
        if (protocol != null) {
            brokerMessage = brokerMessage + " -protocol=" + protocol;
        }

        String error;
        try {
            int retry = 0;
            do {
                _log.debug("getLoginBrokerInfos sending \"" + brokerMessage +
                           "\"  to LoginBroker");
                try {
                    LoginBrokerInfo[] infos =
                        _loginBrokerStub.sendAndWait(brokerMessage,
                                                     LoginBrokerInfo[].class);
                    synchronized (latestLoginBrokerInfosTimes) {
                        latestLoginBrokerInfosTimes.put(key, System.currentTimeMillis());
                        latestLoginBrokerInfos.put(key, infos);
                    }
                    return infos;
                } catch (TimeoutCacheException e) {
                    error = "LoginBroker is unavailable";
                } catch (CacheException e) {
                    error = e.getMessage();
                }
                Thread.sleep(5 * 1000);
            } while (++retry < MAX_LOGIN_BROKER_RETRIES);
        } catch (InterruptedException e) {
            throw new SRMException("Request was interrupted", e);
        }

        throw new SRMException(error);
    }

    public Set<String> listAvailableProtocols()
        throws SRMException
    {
        Set<String> protocols = new HashSet<>();
        for (LoginBrokerInfo info: getLoginBrokerInfos()) {
            protocols.add(info.getProtocolFamily());
        }
        return protocols;
    }

    @Override
    public boolean isLocalTransferUrl(URI url)
        throws SRMException
    {
        String protocol = url.getScheme();
        String host = url.getHost();
        int port = url.getPort();
        for (LoginBrokerInfo info: getLoginBrokerInfos(protocol)) {
            if (info.getHost().equals(host) && info.getPort() == port) {
                return true;
            }
        }
        return false;
    }


    private String selectHost(String protocol)
        throws SRMException
    {
        _log.debug("selectHost("+protocol+")");
        LoginBrokerInfo[]loginBrokerInfos = getLoginBrokerInfos(protocol);
        return selectHost(loginBrokerInfos);
    }

    private final Random rand = new Random();

    int numDoorInRanSelection=3;

   /**
     *  HostnameCacheRecord TTL in millis
     */
    private static final long doorToHostnameCacheTTL = 600000L; // ten minutes

    private static final class HostnameCacheRecord {
        private String hostname;
        /**
         *  in millis
         */
        private long  creationTime;
        public HostnameCacheRecord( String hostname ) {
            this.hostname = hostname;
            this.creationTime = System.currentTimeMillis();
        }
        /**
         * @return the hostname
         */
        public String getHostname() {
            return hostname;
        }

        public boolean expired() {
            return
                System.currentTimeMillis() - creationTime >
                doorToHostnameCacheTTL;
        }

    }
     private Map<String,HostnameCacheRecord> doorToHostnameMap =
            new HashMap<>();
    private String lbiToDoor(LoginBrokerInfo lbi) throws SRMException {

            String thehost =lbi.getHost();
            String resolvedHost;
            HostnameCacheRecord resolvedHostRecord = doorToHostnameMap.get(thehost);
            if(resolvedHostRecord == null || resolvedHostRecord.expired() ) {
                try {

                    InetAddress address = InetAddress.getByName(thehost);
                    resolvedHost = address.getHostName();
                    if ( customGetHostByAddr && isInetAddress(resolvedHost) ) {
                        resolvedHost = getHostByAddr( address.getAddress() );
                    }
                } catch (IOException e) {
                    throw new SRMException("selectHost " + e, e);
                }
                // cache record
                doorToHostnameMap.put(thehost,
                        new HostnameCacheRecord(resolvedHost));
            } else {
                resolvedHost = resolvedHostRecord.getHostname();
            }


            return resolvedHost+":"+ lbi.getPort();

    }

    private final static Comparator<LoginBrokerInfo> LOAD_ORDER =
        new Comparator<LoginBrokerInfo>() {
            @Override
            public int compare(LoginBrokerInfo info1, LoginBrokerInfo info2)
            {
                return (int)Math.signum(info1.getLoad() - info2.getLoad());
            }
        };

    public String selectHost(LoginBrokerInfo[]loginBrokerInfos)
        throws SRMException
    {
        Arrays.sort(loginBrokerInfos, LOAD_ORDER);
        int len = loginBrokerInfos.length;
        if (len <=0){
            return null;
        }

        int selected_indx;
        synchronized (rand) {
            selected_indx = rand.nextInt(Math.min(len, numDoorInRanSelection));
        }
        String doorHostPort = lbiToDoor(loginBrokerInfos[selected_indx]);

        _log.debug("selectHost returns "+doorHostPort);
        return doorHostPort;
    }


    /**
     * Next two functions are
     * BNL's contribution
     */

        private static Map<String,List<String>> resolve(String name, String[] attrIds)
            throws NamingException {

            Map<String,List<String>> map = new HashMap<>();
            DirContext ctx = new InitialDirContext();
            Attributes attrs =
                    ctx.getAttributes(name, attrIds);

            if (attrs == null) {
                return null;
            } else {
                /* get each attribute */
                NamingEnumeration<? extends Attribute> ae = attrs.getAll();
                while (ae != null && ae.hasMoreElements()) {
                   Attribute attr = ae.next();
                   String attrID = attr.getID();
                   List<String> l = new ArrayList<>();
                   for (NamingEnumeration<?> e = attr.getAll();
                        e.hasMoreElements();) {
                       String literalip = (String)e.nextElement();
                       l.add(literalip);
                   }
                   map.put(attrID, l);
               }
            }
            return map;
        }

        private static final int IPv4_SIZE = 4;
        private static final int IPv6_SIZE = 16;

        private static String getHostByAddr(byte[] addr)
        throws UnknownHostException {
            try {
                StringBuilder literalip = new StringBuilder();
                if (addr.length == IPv4_SIZE) {
                    for (int i = addr.length-1; i >= 0; i--) {
                        literalip.append(addr[i] & 0xff).append(".");
                    }
                    literalip.append("IN-ADDR.ARPA.");
                } else if (addr.length == IPv6_SIZE) {
                    for (int i = addr.length-1; i >= 0; i--) {
                        literalip.append(addr[i] & 0x0f).append(".").append(addr[i] & 0xf0).append(".");
                    }
                    literalip.append("IP6.INT.");
                }

                String[] ids = new String[1];
                ids[0] = "PTR"; // PTR record
                Map<String,List<String>> map =
                    resolve("dns:///" + literalip, ids);
                String host = "";
                for (List<String> hosts: map.values()) {
                    host = hosts.get(0);
                }
                return host;
            } catch (NamingException e) {
                throw new UnknownHostException(e.getMessage());
            }
        }

    @Override
    public void prepareToPut(SRMUser user,
                             URI surl,
                             PrepareToPutCallbacks callbacks,
                             boolean overwrite)
    {
        try {
            FsPath actualPnfsPath = getPath(surl);
            PutCompanion.PrepareToPutFile(((DcacheUser) user).getSubject(),
                                          permissionHandler,
                                          actualPnfsPath.toString(),
                                          callbacks,
                                          _pnfsStub,
                                          config.isRecursiveDirectoryCreation(),
                                          overwrite);
        } catch (SRMInvalidPathException e) {
            callbacks.InvalidPathError(e.getMessage());
        }
    }

    @Override
    public void setFileMetaData(SRMUser user, FileMetaData fmd)
        throws SRMException
    {
        PnfsHandler handler =
            new PnfsHandler(_pnfs, ((DcacheUser) user).getSubject());

        try {
            if (!(fmd instanceof DcacheFileMetaData)) {
                throw new SRMException("Storage.setFileMetaData: " +
                                       "metadata in not dCacheMetaData");
            }
            DcacheFileMetaData dfmd = (DcacheFileMetaData) fmd;
            FileAttributes updatedAttributes = new FileAttributes();
            updatedAttributes.setMode(dfmd.permMode);
            handler.setFileAttributes(dfmd.getPnfsId(), updatedAttributes);
        } catch (TimeoutCacheException e) {
            throw new SRMInternalErrorException("PnfsManager is unavailable: "
                                                + e.getMessage(), e);
        } catch (NotInTrashCacheException | FileNotFoundCacheException e) {
            throw new SRMInvalidPathException("No such file or directory", e);
        } catch (PermissionDeniedCacheException e) {
            throw new SRMAuthorizationException("Permission denied");
        } catch (CacheException e) {
            throw new SRMException("SetFileMetaData failed for " + fmd.SURL +
                                   "; return code=" + e.getRc() +
                                   " reason=" + e.getMessage());
        }
    }

    @Override @Nonnull
    public FileMetaData getFileMetaData(SRMUser user, URI surl, boolean read)
        throws SRMException
    {
        _log.debug("getFileMetaData(" + surl + ")");
        FsPath path = getPath(surl);
        PnfsHandler handler =
            new PnfsHandler(_pnfs, ((DcacheUser) user).getSubject());
        try {
            /* Fetch file attributes.
             */
            Set<FileAttribute> requestedAttributes =
                EnumSet.of(TYPE, LOCATIONS);
            requestedAttributes.addAll(DcacheFileMetaData.getKnownAttributes());
            requestedAttributes.addAll(PoolMonitorV5.getRequiredAttributesForFileLocality());

            Set<AccessMask> accessMask =
                read
                ? EnumSet.of(AccessMask.READ_DATA)
                : EnumSet.noneOf(AccessMask.class);

            FileAttributes attributes =
                handler.getFileAttributes(path.toString(),
                                          requestedAttributes,
                                          accessMask);
            FileMetaData fmd = new DcacheFileMetaData(attributes);

            /* Determine file locality.
             */
            if (attributes.getFileType() != FileType.DIR) {
                FileLocality locality =
                    _poolMonitor.getFileLocality(attributes,
                                                 config.getSrmHost());
                fmd.locality = locality.toTFileLocality();
                fmd.isCached = locality.isCached();
            }

            /* Determine space tokens.
             */
            if(_isSpaceManagerEnabled) {
                try {
                    GetFileSpaceTokensMessage msg =
                        new GetFileSpaceTokensMessage(attributes.getPnfsId());
                    msg = _spaceManagerStub.sendAndWait(msg);

                    if (msg.getSpaceTokens() != null) {
                        fmd.spaceTokens = new long[msg.getSpaceTokens().length];
                        System.arraycopy(msg.getSpaceTokens(), 0,
                                         fmd.spaceTokens, 0,
                                         msg.getSpaceTokens().length);
                    }
                } catch (TimeoutCacheException e) {
                    /* SpaceManager is optional, so we don't clasify this
                     * as an error.
                     */
                    _log.info(e.getMessage());
                }
            }

            return fmd;
        } catch (TimeoutCacheException e) {
            throw new SRMInternalErrorException(e.getMessage(), e);
        } catch (PermissionDeniedCacheException e) {
            throw new SRMAuthorizationException(e.getMessage(), e);
        } catch (FileNotFoundCacheException e) {
            throw new SRMInvalidPathException(e.getMessage(), e);
        } catch (CacheException e) {
            throw new SRMException("Could not get storage info by path: " +
                                   e.getMessage(), e);
        } catch (InterruptedException e) {
            throw new SRMInternalErrorException("Operation interrupted", e);
        }
    }

    private static AtomicLong nextMessageID = new AtomicLong(20000);

    private static synchronized long getNextMessageID()
    {
        return nextMessageID.getAndIncrement();
    }

    @Override
    public void localCopy(SRMUser user, URI fromSurl, URI toSurl)
        throws SRMException
    {
        FsPath actualFromFilePath = getPath(fromSurl);
        FsPath actualToFilePath = getPath(toSurl);
        long id = getNextMessageID();
        _log.debug("localCopy for user " + user +
                   "from actualFromFilePath to actualToFilePath");
        try {
            CopyManagerMessage copyRequest =
                new CopyManagerMessage(actualFromFilePath.toString(),
                                       actualToFilePath.toString(),
                                       id,
                                       config.getBuffer_size(),
                                       config.getTcp_buffer_size());
            copyRequest.setSubject(((DcacheUser) user).getSubject());
            _copyManagerStub.sendAndWait(copyRequest);
        } catch (TimeoutCacheException e) {
            _log.error("CopyManager is unavailable");
            throw new SRMInternalErrorException("CopyManager is unavailable: " +
                                                e.getMessage(), e);
        } catch (CacheException e) {
            String msg = " local copy failed with code =" + e.getRc() +
                " details: " + e.getMessage();
            _log.warn(msg);
            throw new SRMException(msg, e);
        } catch (InterruptedException e) {
            throw new SRMException("Request to CopyManager was interrupted", e);
        }
    }

    @Override
    public void prepareToPutInReservedSpace(SRMUser user, String path, long size,
        long spaceReservationToken, PrepareToPutInSpaceCallbacks callbacks) {
        throw new UnsupportedOperationException("NotImplementedException");
    }

    @Override
    public void advisoryDelete(final SRMUser user, final URI surl,
                               final AdvisoryDeleteCallbacks callback)
    {
        _log.debug("Storage.advisoryDelete");

        /* If not enabled, we are allowed to silently ignore the call.
         */
        if (!config.isAdvisoryDelete()) {
            if (callback != null) {
                callback.AdvisoryDeleteSuccesseded();
            }
            return;
        }

        RemoveFileCallbacks removeFileCallback = new RemoveFileCallbacks() {
                @Override
                public void RemoveFileSucceeded()
                {
                    callback.AdvisoryDeleteSuccesseded();
                }

                @Override
                public void RemoveFileFailed(String reason)
                {
                    callback.AdvisoryDeleteFailed(reason);
                }

                @Override
                public void FileNotFound(String error)
                {
                    callback.AdvisoryDeleteFailed(error);
                }

                @Override
                public void Exception(Exception e)
                {
                    callback.Exception(e);
                }

                @Override
                public void Timeout()
                {
                    callback.Timeout();
                }

                @Override
                public void PermissionDenied()
                {
                    callback.AdvisoryDeleteFailed("Permission denied");
                }
            };

        try {
            RemoveFileCompanion.removeFile(((DcacheUser) user).getSubject(),
                                           getPath(surl).toString(),
                                           removeFileCallback,
                                           _pnfsStub,
                                           getCellEndpoint());
        } catch (SRMInvalidPathException e) {
            callback.AdvisoryDeleteFailed(e.getMessage());
        }
    }

    @Override
    public void removeFile(final SRMUser user,
                           final URI surl,
                           RemoveFileCallbacks callbacks)
    {
        _log.debug("Storage.removeFile");

        try {
            RemoveFileCompanion.removeFile(((DcacheUser) user).getSubject(),
                                           getPath(surl).toString(),
                                           callbacks,
                                           _pnfsStub,
                                           getCellEndpoint());
        } catch (SRMInvalidPathException e) {
            callbacks.FileNotFound(e.getMessage());
        }
    }

    @Override
    public void removeDirectory(SRMUser user, List<URI> surls)
        throws SRMException
    {
        _log.debug("Storage.removeDirectory");
        for (URI surl: surls) {
            FsPath path = getPath(surl);
            try {
                _pnfs.deletePnfsEntry(path.toString());
            } catch (TimeoutCacheException e) {
                _log.error("Failed to delete " + path + " due to timeout");
                throw new SRMInternalErrorException("Internal name space timeout while deleting " + surl);
            } catch (FileNotFoundCacheException | NotInTrashCacheException e) {
                throw new SRMException("File does not exist: " + surl);
            } catch (CacheException e) {
                _log.error("Failed to delete " + path + ": " + e.getMessage());
                throw new SRMException("Failed to delete " + surl + ": "
                                       + e.getMessage());
            }
        }
    }

    @Override
    public void createDirectory(SRMUser user, URI surl)
        throws SRMException
    {
        _log.debug("Storage.createDirectory");

        Subject subject = ((DcacheUser) user).getSubject();
        PnfsHandler handler = new PnfsHandler(_pnfs, subject);

        try {
            handler.createPnfsDirectory(getPath(surl).toString());
        } catch (TimeoutCacheException e) {
            throw new SRMInternalErrorException("Internal name space timeout", e);
        } catch (NotDirCacheException e) {
            throw new SRMInvalidPathException("Parent path is not a directory", e);
        } catch (NotInTrashCacheException | FileNotFoundCacheException e) {
            throw new SRMInvalidPathException("Parent path does not exist", e);
        } catch (FileExistsCacheException e) {
            throw new SRMDuplicationException("File exists");
        } catch (PermissionDeniedCacheException e) {
            throw new SRMAuthorizationException("Permission denied");
        } catch (CacheException e) {
            _log.error("Failed to create directory " + surl + ": "
                       + e.getMessage());
            throw new SRMException(String.format("Failed to create directory [rc=%d,msg=%s]",
                                                 e.getRc(), e.getMessage()));
        }
    }

    @Override
    public void moveEntry(SRMUser user, URI from, URI to)
        throws SRMException
    {
        Subject subject = ((DcacheUser) user).getSubject();
        PnfsHandler handler = new PnfsHandler(_pnfs, subject);
        FsPath fromPath = getPath(from);
        FsPath toPath = getPath(to);

        try {
            try {
                FileAttributes attr =
                    handler.getFileAttributes(toPath.toString(), EnumSet.of(TYPE));

                /* We now know the destination exists. In case the
                 * source and destination names are identical, we
                 * silently ignore the request.
                 */
                if (fromPath.equals(toPath)) {
                    return;
                }

                if (attr.getFileType() != FileType.DIR) {
                    throw new SRMDuplicationException("Destination exists");
                }

                toPath = new FsPath(toPath, fromPath.getName());
            } catch (FileNotFoundCacheException e) {
                /* Destination name does not exist; not a problem.
                 */
            }

            handler.renameEntry(fromPath.toString(), toPath.toString(), false);
        } catch (FileNotFoundCacheException e) {
            throw new SRMInvalidPathException("No such file or directory", e);
        } catch (FileExistsCacheException e) {
            throw new SRMDuplicationException("Destination exists", e);
        } catch (NotDirCacheException e) {
            /* The parent of the target name did not exist or was not
             * a directory.
             */
            FsPath parent = toPath.getParent();
            throw new SRMInvalidPathException("No such directory: " +
                                              parent, e);
        } catch (PermissionDeniedCacheException e) {
            throw new SRMException("Permission denied");
        } catch (TimeoutCacheException e) {
            _log.error("Failed to rename " + fromPath + " due to timeout");
            throw new SRMInternalErrorException("Internal name space timeout");
        } catch (CacheException e) {
            _log.error("Failed to rename " + fromPath + ": " + e.getMessage());
            throw new SRMException(String.format("Rename failed [rc=%d,msg=%s]",
                                                 e.getRc(), e.getMessage()));
        }
    }


    // To do:  extract common functionality from this and _canRead
    //         into another method.
    public boolean canRead(SRMUser user, FileMetaData fmd) {
        int uid = Integer.parseInt(fmd.owner);
        int gid = Integer.parseInt(fmd.group);
        int permissions = fmd.permMode;

        if(permissions == 0 ) {
            return false;
        }

        if(Permissions.worldCanRead(permissions)) {
            return true;
        }

        if(uid == -1 || gid == -1) {
            return false;
        }

        if(user == null || (!(user instanceof DcacheUser))) {
            return false;
        }
        Subject subject = ((DcacheUser) user).getSubject();

        if (Subjects.hasGid(subject, gid) && Permissions.groupCanRead(permissions)) {
            return true;
        }

        if (Subjects.hasUid(subject, uid) && Permissions.userCanRead(permissions)) {
            return true;
        }

        return false;
    }




    public static boolean _canWrite(SRMUser user,
            String fileId,
            FileMetaData fmd,
            String parentFileId,
            FileMetaData parentFmd,
            boolean overwrite) {
        // we can not overwrite file in dcache (at least for now)
        if(! overwrite) {
            if(fileId != null ) {
                // file exists and we can't overwrite
                return false;
            }
        }

        if( parentFileId == null) {
            return false;
        }

        DcacheUser duser = (DcacheUser) user;
        Subject subject = duser.getSubject();
        boolean canWrite;
        if(fileId == null) {
            canWrite = true;
        } else {
            int uid = Integer.parseInt(fmd.owner);
            int gid = Integer.parseInt(fmd.group);
            int permissions = fmd.permMode;

            if(permissions == 0 ) {
               canWrite = false;
            } else if(Permissions.worldCanWrite(permissions) ) {
               canWrite = true;
            } else if(uid == -1 || gid == -1) {
               canWrite = false;
            } else  if(Subjects.hasGid(subject, gid) &&
                    Permissions.groupCanWrite(permissions) ) {
                canWrite = true;
            } else  if(Subjects.hasUid(subject, uid) &&
                    Permissions.userCanWrite(permissions)) {
                canWrite = true;
            } else {
                canWrite = false;
            }
        }

        int parentUid = Integer.parseInt(parentFmd.owner);
        int parentGid = Integer.parseInt(parentFmd.group);
        int parentPermissions = parentFmd.permMode;

        boolean parentCanWrite;
        if(parentPermissions == 0 ) {
           parentCanWrite = false;
        } else if(Permissions.worldCanWrite(parentPermissions) &&
                Permissions.worldCanExecute(parentPermissions)) {
           parentCanWrite = true;
        } else if(parentUid == -1 || parentGid == -1) {
           parentCanWrite = false;
        } else  if(Subjects.hasGid(subject, parentGid) &&
                Permissions.groupCanWrite(parentPermissions) &&
                Permissions.groupCanExecute(parentPermissions)) {
            parentCanWrite = true;
        } else  if(Subjects.hasUid(subject, parentUid) &&
                Permissions.userCanWrite(parentPermissions) &&
                Permissions.userCanExecute(parentPermissions)) {
            parentCanWrite = true;
        } else {
            parentCanWrite = false;
        }
        return canWrite && parentCanWrite;

    }

    /**
     * @param user User ID
     * @param remoteTURL
     * @param surl
     * @param remoteUser
     * @param remoteCredetial
     * @param callbacks
     * @throws SRMException
     * @return copy handler id
     */
    @Override
    public String getFromRemoteTURL(SRMUser user,
                                    URI remoteTURL,
                                    URI surl,
                                    SRMUser remoteUser,
                                    Long remoteCredentialId,
                                    String spaceReservationId,
                                    long size,
                                    CopyCallbacks callbacks)
        throws SRMException
    {
        FsPath path = getPath(surl);
        _log.debug(" getFromRemoteTURL from "+remoteTURL+" to " +path);
        return performRemoteTransfer(user,remoteTURL,path,true,
                remoteUser,
                remoteCredentialId,
                spaceReservationId,
                size,
                callbacks);

    }

    @Override
    public String getFromRemoteTURL(SRMUser user,
                                    URI remoteTURL,
                                    URI surl,
                                    SRMUser remoteUser,
                                    Long remoteCredentialId,
                                    CopyCallbacks callbacks)
        throws SRMException
    {
        FsPath path = getPath(surl);
        _log.debug(" getFromRemoteTURL from "+remoteTURL+" to " +path);
        return performRemoteTransfer(user,remoteTURL,path,true,
                remoteUser,
                remoteCredentialId,
                null,
                null,
                callbacks);

    }

    /**
     * @param user
     * @param surl
     * @param remoteTURL
     * @param remoteUser
     * @param remoteCredetial
     * @param callbacks
     * @throws SRMException
     * @return copy handler id
     */
    @Override
    public String putToRemoteTURL(SRMUser user,
                                  URI surl,
                                  URI remoteTURL,
                                  SRMUser remoteUser,
                                  Long remoteCredentialId,
                                  CopyCallbacks callbacks)
        throws SRMException
    {
        FsPath path = getPath(surl);
        _log.debug(" putToRemoteTURL from "+path+" to " +surl);
        return performRemoteTransfer(user,remoteTURL,path,false,
                remoteUser,
                remoteCredentialId,
                null,
                null,
                callbacks);


    }

    @Override
    public void killRemoteTransfer(String transferId) {

        try {
            long callerId = Long.parseLong(transferId);
            TransferInfo info = callerIdToHandler.get(callerId);
            if (info != null) {
                CancelTransferMessage cancel =
                    new
                    CancelTransferMessage(info.transferId, callerId);
                sendMessage(new CellMessage(info.cellPath,cancel));
            }
        } catch (NoRouteToCellException e) {
            _log.error("Failed to kill remote transfer: " + e.getMessage());
        } catch (NumberFormatException e) {
            _log.error("Failed to kill remote transfer: Cannot parse transfer ID");
        }
    }

    private static int portFor(URI target) throws SRMException
    {
        if(target.getPort() != -1) {
            return target.getPort();
        }

        String scheme = target.getScheme();

        if(scheme == null) {
            throw new SRMException("No scheme in URI " + target.toString());
        }

        // REVISIT consider taking default port numbers from /etc/services

        switch(scheme.toLowerCase()) {
            case "http":
                return 80;
            case "https":
                return 443;
            case "gsiftp":
                return 2811;
            default:
                throw new SRMException("No default port number for " +
                        target.toString());
        }
    }

    private String performRemoteTransfer(SRMUser user,
                                         URI remoteTURL,
                                         FsPath actualFilePath,
                                         boolean store,
                                         SRMUser remoteUser,
                                         Long remoteCredentialId,
                                         String spaceReservationId,
                                         Long size,
                                         CopyCallbacks callbacks)
        throws SRMException
    {
        Subject subject = ((DcacheUser) user).getSubject();

        _log.debug("performRemoteTransfer performing "+(store?"store":"restore"));
        if (!verifyUserPathIsRootSubpath(actualFilePath,user)) {
            throw new SRMAuthorizationException("user's path "+actualFilePath+
                                                " is not subpath of the user's root");
        }

        IpProtocolInfo protocolInfo;

        int port = portFor(remoteTURL);

        if (remoteTURL.getScheme().equals("gsiftp")) {
            RequestCredential credential =
                RequestCredential.getRequestCredential(remoteCredentialId);
            if (credential == null) {
                throw new SRMAuthorizationException("Cannot authenticate with remote gsiftp service; credential delegation required.");
            }
            GSSCredential delegatedCredential =
                credential.getDelegatedCredential();

            if (!(delegatedCredential instanceof GlobusGSSCredentialImpl)) {
                throw new SRMException("Delegated credential is not compatible with Globus");
            }

            try {
                RemoteGsiftpTransferProtocolInfo gsiftpProtocolInfo =
                        new RemoteGsiftpTransferProtocolInfo(
                                "RemoteGsiftpTransfer",
				1, 1,
                                new InetSocketAddress(remoteTURL.getHost(), port),
				remoteTURL.toString(),
				getCellName(),
                                getCellDomainName(),
                                config.getBuffer_size(),
                                config.getTcp_buffer_size(),
                                (GlobusGSSCredentialImpl) delegatedCredential);
                gsiftpProtocolInfo.setEmode(true);
                gsiftpProtocolInfo.setNumberOfStreams(config.getParallel_streams());
                protocolInfo = gsiftpProtocolInfo;
            } catch (GSSException e) {
                throw new SRMException("Credential failure: " + e.getMessage(), e);
            }
        } else if (remoteTURL.getScheme().equals("http")) {

            protocolInfo =
                new RemoteHttpDataTransferProtocolInfo("RemoteHttpDataTransfer",
                                                       1, 1,
                                                       new InetSocketAddress(remoteTURL.getHost(), port),
                                                       config.getBuffer_size(),
                                                       remoteTURL.toString());
        } else {
            throw new SRMException("not implemented");
        }

        RemoteTransferManagerMessage request;
        if (store && spaceReservationId != null && size != null) {
            // space reservation was performed for a file of known size
            request =
                new RemoteTransferManagerMessage(remoteTURL,
                                                 actualFilePath,
                                                 store,
                                                 remoteCredentialId,
                                                 spaceReservationId,
                                                 config.isSpace_reservation_strict(),
                                                 size,
                                                 protocolInfo);
        } else {
            request =
                new RemoteTransferManagerMessage(remoteTURL,
                                                 actualFilePath,
                                                 store,
                                                 remoteCredentialId,
                                                 protocolInfo);
        }
        request.setSubject(subject);
        try {
            RemoteTransferManagerMessage reply =
                _transferManagerStub.sendAndWait(request);
            long id = reply.getId();
            _log.debug("received first RemoteGsiftpTransferManagerMessage "
                       + "reply from transfer manager, id ="+id);
            TransferInfo info =
                new TransferInfo(id, callbacks,
                                 _transferManagerStub.getDestinationPath());
            _log.debug("storing info for callerId = {}", id);
            callerIdToHandler.put(id, info);
            return String.valueOf(id);
        } catch (TimeoutCacheException e) {
            throw new SRMInternalErrorException("Transfer manager is unavailable: " +
                                                e.getMessage(), e);
        } catch (CacheException e) {
            throw new SRMException("TransferManager error: "+
                                   e.getMessage(), e);
        } catch (InterruptedException e) {
            throw new SRMException("Request to transfer manager got interruptd", e);
        }
    }

    private final Map<Long,TransferInfo> callerIdToHandler =
        new ConcurrentHashMap<>();

    private static class TransferInfo
    {
        final long transferId;
        final CopyCallbacks callbacks;
        final CellPath cellPath;

        public TransferInfo(long transferId,
                            CopyCallbacks callbacks,
                            CellPath cellPath)
        {
            this.transferId = transferId;
            this.callbacks = callbacks;
            this.cellPath = cellPath;
        }
    }

    private void handleTransferManagerMessage(TransferManagerMessage message) {
        Long callerId = message.getId();
        _log.debug("handleTransferManagerMessage for callerId="+callerId);

        TransferInfo info = callerIdToHandler.get(callerId);
        if (info == null) {
            _log.error("TransferInfo for callerId="+callerId+"not found");
            return;
        }

        if (message instanceof TransferCompleteMessage ) {
            info.callbacks.copyComplete(null);
            _log.debug("removing TransferInfo for callerId="+callerId);
            callerIdToHandler.remove(callerId);
        } else if (message instanceof TransferFailedMessage) {
            Object error =  message.getErrorObject();
            if (error instanceof CacheException) {
                error = ((CacheException) error).getMessage();
            }
            SRMException e;
            switch (message.getReturnCode()) {
            case CacheException.PERMISSION_DENIED:
                e = new SRMAuthorizationException(String.format("Access denied: %s", error));
                break;
            case CacheException.FILE_NOT_FOUND:
                e = new SRMInvalidPathException(String.valueOf(error));
                break;
            default:
                e = new SRMException(String.format("Transfer failed: %s [%d]",
                                                   error, message.getReturnCode()));
            }
            info.callbacks.copyFailed(e);

            _log.debug("removing TransferInfo for callerId="+callerId);
            callerIdToHandler.remove(callerId);
        }
    }


    private volatile StorageElementInfo storageElementInfo =
        new StorageElementInfo();

    @Override
    public StorageElementInfo getStorageElementInfo(SRMUser user)
    {
        return storageElementInfo;
    }

    private StorageElementInfo getStorageElementInfo()
    {
        return storageElementInfo;
    }

    private void updateStorageElementInfo()
        throws CacheException, InterruptedException
    {
        _poolMonitor =
            _poolManagerStub.sendAndWait(new PoolManagerGetPoolMonitor()).getPoolMonitor();

        String[] pools =
            _poolMonitor.getPoolSelectionUnit().getActivePools();
        if (pools.length == 0) {
            _log.debug("Pool manager provided empty list of pools; assuming pool manager was restarted");
            return;
        }

        CostModule cm = _poolMonitor.getCostModule();
        StorageElementInfo info = new StorageElementInfo();
        for (String pool: pools) {
            PoolCostInfo.PoolSpaceInfo poolInfo =
                cm.getPoolCostInfo(pool).getSpaceInfo();

            if (poolInfo != null) {
                /* FIXME: Removable space is added to both used and
                 * available. The logic is copied from the old code.
                 * It also seems like we don't account for CACHED +
                 * STICKY files.
                 */
                info.availableSpace += poolInfo.getFreeSpace();
                info.availableSpace += poolInfo.getRemovableSpace();
                info.totalSpace += poolInfo.getTotalSpace();
                info.usedSpace += poolInfo.getPreciousSpace();
                info.usedSpace += poolInfo.getRemovableSpace();
            }
        }

        storageElementInfo = info;
    }

    /**
     * we use run method to update the storage info structure periodically
     */
    @Override
    public void run()
    {
        try {
            while (true) {
                try {
                    updateStorageElementInfo();
                } catch (CacheException e) {
                    _log.error("Pool monitor update failed: {} [{}]",
                               e.getMessage(), e.getRc());
                }
                Thread.sleep(config.getStorage_info_update_period());
            }
        } catch (InterruptedException e) {
            _log.debug("Storage info update thread shut down");
        }
    }

    /**
     * Provides a directory listing of surl if and only if surl is not
     * a symbolic link. As a side effect, the method checks that surl
     * can be deleted by the user.
     *
     * @param user The SRMUser performing the operation; this must be
     * of type AuthorizationRecord
     * @param surl The directory to delete
     * @return The array of directory entries or null if directoryName
     * is a symbolic link
     */
    @Override
    public List<URI> listNonLinkedDirectory(SRMUser user, URI surl)
        throws SRMException
    {
        Subject subject = ((DcacheUser) user).getSubject();

        FsPath path = getPath(surl);
        try {
            Set<FileAttribute> requestedAttributes = EnumSet.of(TYPE);
            requestedAttributes.addAll(permissionHandler.getRequiredAttributes());
            FileAttributes parentAttr =
                _pnfs.getFileAttributes(path.getParent().toString(), requestedAttributes);
            FileAttributes childAttr =
                _pnfs.getFileAttributes(path.toString(), requestedAttributes);

            AccessType canDelete =
                permissionHandler.canDeleteDir(subject, parentAttr, childAttr);
            if (canDelete != AccessType.ACCESS_ALLOWED) {
                _log.warn("Cannot delete directory " + path +
                          ": Permission denied");
                throw new SRMAuthorizationException("Permission denied");
            }

            if (childAttr.getFileType() == FileType.LINK)  {
                return null;
            }
        } catch (FileNotFoundCacheException | NotInTrashCacheException e) {
            throw new SRMInvalidPathException("No such file or directory", e);
        } catch (TimeoutCacheException e) {
            throw new SRMInternalErrorException("Internal name space timeout", e);
        } catch (CacheException e) {
            _log.error("Failed to list directory " + path + ": "
                       + e.getMessage());
            throw new SRMException(String.format("Failed delete directory [rc=%d,msg=%s]",
                                                 e.getRc(), e.getMessage()));
        }

        return listDirectory(user, surl, null);
    }

    @Override
    public List<URI> listDirectory(SRMUser user, URI surl,
                                   FileMetaData fileMetaData)
        throws SRMException
    {
        final FsPath path = getPath(surl);
        final List<URI> result = new ArrayList<>();
        final String base = addTrailingSlash(surl.toString());
        Subject subject = ((DcacheUser) user).getSubject();
        DirectoryListPrinter printer =
            new DirectoryListPrinter()
            {
                @Override
                public Set<FileAttribute> getRequiredAttributes()
                {
                    return EnumSet.noneOf(FileAttribute.class);
                }

                @Override
                public void print(FsPath dir, FileAttributes dirAttr, DirectoryEntry entry)
                {
                    result.add(URI.create(base + entry.getName()));
                }
            };

        try {
            _listSource.printDirectory(subject, printer, path, null,
                                       Range.<Integer>all());
            return result;
        } catch (TimeoutCacheException e) {
            throw new SRMInternalErrorException("Internal name space timeout", e);
        } catch (InterruptedException e) {
            throw new SRMInternalErrorException("List aborted by administrator", e);
        } catch (NotDirCacheException e) {
            throw new SRMInvalidPathException("Not a directory", e);
        } catch (FileNotFoundCacheException | NotInTrashCacheException e) {
            throw new SRMInvalidPathException("No such file or directory", e);
        } catch (PermissionDeniedCacheException e) {
            throw new SRMAuthorizationException("Permission denied", e);
        } catch (CacheException e) {
            throw new SRMException(String.format("List failed [rc=%d,msg=%s]",
                                                 e.getRc(), e.getMessage()));
        }
    }

    @Override
    public List<FileMetaData>
        listDirectory(SRMUser user, URI surl, final boolean verbose,
                      int offset, int count)
        throws SRMException
    {
        try {
            FsPath path = getPath(surl);
            Subject subject = ((DcacheUser) user).getSubject();
            FmdListPrinter printer =
                verbose ? new VerboseListPrinter() : new FmdListPrinter();
            _listSource.printDirectory(subject, printer, path, null,
                                       Range.closedOpen(offset, offset + count));
            return printer.getResult();
        } catch (TimeoutCacheException e) {
            throw new SRMInternalErrorException("Internal name space timeout", e);
        } catch (InterruptedException e) {
            throw new SRMInternalErrorException("List aborted by administrator", e);
        } catch (NotDirCacheException e) {
            throw new SRMInvalidPathException("Not a directory", e);
        } catch (FileNotFoundCacheException | NotInTrashCacheException e) {
            throw new SRMInvalidPathException("No such file or directory", e);
        } catch (PermissionDeniedCacheException e) {
            throw new SRMAuthorizationException("Permission denied", e);
        } catch (CacheException e) {
            throw new SRMException(String.format("List failed [rc=%d,msg=%s]",
                                                 e.getRc(), e.getMessage()));
        }
    }

    /**
     * Custom DirectoryListPrinter that collects the list result as a
     * list of FileMetaData.
     */
    private class FmdListPrinter implements DirectoryListPrinter
    {
        protected final List<FileMetaData> _result =
            new ArrayList<>();
        protected final FsPath _root =
            new FsPath(config.getSrm_root());

        @Override
        public Set<FileAttribute> getRequiredAttributes()
        {
            return EnumSet.of(SIZE, SIMPLE_TYPE);
        }

        protected DcacheFileMetaData toFmd(FsPath dir, DirectoryEntry entry)
            throws InterruptedException
        {
            FileAttributes attributes = entry.getFileAttributes();
            DcacheFileMetaData fmd = new DcacheFileMetaData(attributes);
            String name = entry.getName();
            FsPath path = (dir == null) ? new FsPath(name) : new FsPath(dir, name);
            fmd.SURL = _root.relativize(path).toString();
            return fmd;
        }

        @Override
        public void print(FsPath dir, FileAttributes dirAttr, DirectoryEntry entry)
            throws InterruptedException
        {
            _result.add(toFmd(dir, entry));
        }

        public List<FileMetaData> getResult()
            throws InterruptedException
        {
            return _result;
        }
    }


    /**
     * Custom DirectoryListPrinter that collects the list result as a
     * list of FileMetaData.
     */
    private class VerboseListPrinter extends FmdListPrinter
    {
        private final static int PIPELINE_DEPTH = 40;

        private final Semaphore _available =
            new Semaphore(PIPELINE_DEPTH);
        private final Set<FileAttribute> _required;

        public VerboseListPrinter()
        {
            _required = DcacheFileMetaData.getKnownAttributes();
            _required.addAll(PoolMonitorV5.getRequiredAttributesForFileLocality());
        }

        @Override
        public Set<FileAttribute> getRequiredAttributes()
        {
            return _required;
        }

        @Override
        protected DcacheFileMetaData toFmd(FsPath dir, DirectoryEntry entry)
            throws InterruptedException
        {
            DcacheFileMetaData fmd = super.toFmd(dir, entry);
            if (!fmd.isDirectory) {
                lookupLocality(entry.getFileAttributes(), fmd);

                if (_isSpaceManagerEnabled) {
                    lookupTokens(entry.getFileAttributes(), fmd);
                }
            }
            return fmd;
        }

        @Override
        public List<FileMetaData> getResult()
            throws InterruptedException
        {
            _available.acquire(PIPELINE_DEPTH);
            try {
                return _result;
            } finally {
                _available.release(PIPELINE_DEPTH);
            }
        }

        private void lookupLocality(FileAttributes attributes,
                                    final DcacheFileMetaData fmd)
            throws InterruptedException
        {
            FileLocality locality =
                _poolMonitor.getFileLocality(attributes, config.getSrmHost());
            fmd.locality = locality.toTFileLocality();
            fmd.isCached = locality.isCached();
        }

        private void lookupTokens(FileAttributes attributes,
                                  final DcacheFileMetaData fmd)
            throws InterruptedException
        {
            _available.acquire();
            _spaceManagerStub.send(
                 new GetFileSpaceTokensMessage(attributes.getPnfsId()),
                 GetFileSpaceTokensMessage.class,
                 new AbstractMessageCallback<GetFileSpaceTokensMessage>() {
                      @Override
                      public void success(GetFileSpaceTokensMessage message)
                      {
                           _available.release();
                           fmd.spaceTokens = message.getSpaceTokens();
                      }

                      @Override
                      public void failure(int rc, Object error)
                      {
                           _available.release();
                           _log.error("Locality lookup failed: {} [{}]",
                                      error, rc);
                      }
                 });
        }
    }

    @Override
    public void srmReserveSpace(SRMUser user,
            long sizeInBytes,
            long spaceReservationLifetime,
            String retentionPolicy,
            String accessLatency,
            String description,
            SrmReserveSpaceCallbacks callbacks) {

        if (_isSpaceManagerEnabled) {
            SrmReserveSpaceCompanion.reserveSpace(((DcacheUser) user).getSubject(),
                    sizeInBytes, spaceReservationLifetime, retentionPolicy,
                    accessLatency, description, callbacks, _spaceManagerStub);
        } else {
            callbacks.ReserveSpaceFailed(SPACEMANAGER_DISABLED_MESSAGE);
        }
    }

    @Override
    public void srmReleaseSpace(SRMUser user,
            String spaceToken,
            Long releaseSizeInBytes, // everything is null
            SrmReleaseSpaceCallbacks callbacks) {
        if (_isSpaceManagerEnabled) {
            try {
                long token = Long.parseLong(spaceToken);

                SrmReleaseSpaceCompanion.releaseSpace(((DcacheUser) user).getSubject(),
                    token, releaseSizeInBytes, callbacks, _spaceManagerStub);
            } catch(NumberFormatException e){
                callbacks.ReleaseSpaceFailed("invalid space token="+spaceToken);
            }
        } else {
            callbacks.ReleaseSpaceFailed(SPACEMANAGER_DISABLED_MESSAGE);
        }
    }

    @Override
    public void srmMarkSpaceAsBeingUsed(SRMUser user,
                                        String spaceToken,
                                        URI surl,
                                        long sizeInBytes,
                                        long useLifetime,
                                        boolean overwrite,
                                        SrmUseSpaceCallbacks callbacks)
    {
        if (_isSpaceManagerEnabled) {
            try {
                SrmMarkSpaceAsBeingUsedCompanion.markSpace(((DcacheUser) user).getSubject(),
                        Long.parseLong(spaceToken), getPath(surl).toString(),
                        sizeInBytes, useLifetime, overwrite, callbacks,
                        _spaceManagerStub);
            } catch (SRMInvalidPathException e) {
                callbacks.SrmUseSpaceFailed("Invalid path: " + e.getMessage());
            } catch (NumberFormatException ignored){
                callbacks.SrmUseSpaceFailed("invalid space token=" + spaceToken);
            }
        } else {
            callbacks.SrmUseSpaceFailed(SPACEMANAGER_DISABLED_MESSAGE);
        }
    }

    @Override
    public void srmUnmarkSpaceAsBeingUsed(SRMUser user,
                                          String spaceToken,
                                          URI surl,
                                          SrmCancelUseOfSpaceCallbacks callbacks)
    {
        if (_isSpaceManagerEnabled) {
            try {
                SrmUnmarkSpaceAsBeingUsedCompanion.unmarkSpace(((DcacheUser) user).getSubject(),
                        Long.parseLong(spaceToken), getPath(surl).toString(),
                        callbacks, _spaceManagerStub);
            } catch (SRMInvalidPathException e) {
                callbacks.CancelUseOfSpaceFailed("Invalid path: " + e.getMessage());
            } catch (NumberFormatException ignored){
                callbacks.CancelUseOfSpaceFailed("invalid space token="+spaceToken);
            }
        } else {
            callbacks.CancelUseOfSpaceFailed(SPACEMANAGER_DISABLED_MESSAGE);
        }
    }

    private void guardSpaceManagerEnabled() throws SRMException
    {
        if (!_isSpaceManagerEnabled) {
            throw new SRMException(SPACEMANAGER_DISABLED_MESSAGE);
        }
    }

    /**
     *
     * @param spaceTokens
     * @throws SRMException
     * @return
     */
    @Override
    public TMetaDataSpace[] srmGetSpaceMetaData(SRMUser user,
                                                String[] spaceTokens)
        throws SRMException
    {
        _log.debug("srmGetSpaceMetaData");
        guardSpaceManagerEnabled();
        if(spaceTokens == null) {
            throw new SRMException("null array of space tokens");
        }
        long[] tokens = new long[spaceTokens.length];
        for(int i = 0; i<spaceTokens.length; ++i) {
            try{
                tokens[i] = Long.parseLong(spaceTokens[i]);
            } catch (Exception e) {
                throw new SRMException("invalid token: "+spaceTokens[i]);
            }
        }

        GetSpaceMetaData getSpaces = new GetSpaceMetaData(tokens);
        try {
            getSpaces = _spaceManagerStub.sendAndWait(getSpaces);
        } catch (TimeoutCacheException e) {
            throw new SRMInternalErrorException("SrmSpaceManager is unavailable: " +
                                                e.getMessage(), e);
        } catch (CacheException e) {
            _log.warn("GetSpaceMetaData failed with rc=" + e.getRc()+
                      " error="+e.getMessage());
            throw new SRMException("GetSpaceMetaData failed with rc="+
                                   e.getRc() + " error=" + e.getMessage(), e);
        } catch (InterruptedException e) {
            throw new SRMException("Request to SrmSpaceManaget got interrupted", e);
        }

        Space[] spaces = getSpaces.getSpaces();
        tokens =  getSpaces.getSpaceTokens();
        TMetaDataSpace[] spaceMetaDatas = new TMetaDataSpace[spaces.length];
        for(int i = 0; i<spaceMetaDatas.length; ++i){
            if(spaces[i] != null) {
                Space space = spaces[i];
                spaceMetaDatas[i] = new TMetaDataSpace();
                spaceMetaDatas[i].setSpaceToken(Long.toString(space.getId()));
                long lifetime = space.getLifetime();
                int lifetimeleft;
                if ( lifetime == -1) {  // -1 corresponds to infinite lifetime
                    lifetimeleft = -1;
                    spaceMetaDatas[i].setLifetimeAssigned(-1);
                    spaceMetaDatas[i].setLifetimeLeft(-1);
                } else {
			lifetimeleft = (int)((space.getCreationTime()+lifetime - System.currentTimeMillis())/1000);
                    lifetimeleft= lifetimeleft < 0? 0: lifetimeleft;
                    spaceMetaDatas[i].setLifetimeAssigned((int) (lifetime / 1000));
                    spaceMetaDatas[i].setLifetimeLeft(lifetimeleft);
                }
                TRetentionPolicy policy =
                    space.getRetentionPolicy().equals( RetentionPolicy.CUSTODIAL)?
                      TRetentionPolicy.CUSTODIAL :
                        space.getRetentionPolicy().equals(RetentionPolicy.OUTPUT)?
                            TRetentionPolicy.OUTPUT:TRetentionPolicy.REPLICA;
                TAccessLatency latency =
                    space.getAccessLatency().equals(AccessLatency.ONLINE) ?
                            TAccessLatency.ONLINE: TAccessLatency.NEARLINE;
                spaceMetaDatas[i].setRetentionPolicyInfo(
                    new TRetentionPolicyInfo(policy,latency));
                spaceMetaDatas[i].setTotalSize(
                    new UnsignedLong(
                        space.getSizeInBytes()));
                spaceMetaDatas[i].setGuaranteedSize(
                    spaceMetaDatas[i].getTotalSize());
                spaceMetaDatas[i].setUnusedSize(
                    new UnsignedLong(
                        space.getSizeInBytes() - space.getUsedSizeInBytes()));
                SpaceState spaceState =space.getState();
                if(SpaceState.RESERVED.equals(spaceState)) {
                    if(lifetimeleft == 0 ) {
                        spaceMetaDatas[i].setStatus(
                                new TReturnStatus(
                                TStatusCode.SRM_SPACE_LIFETIME_EXPIRED,"expired"));
                    }
                    else {
                        spaceMetaDatas[i].setStatus(
                                new TReturnStatus(TStatusCode.SRM_SUCCESS,"ok"));
                    }
                } else if(SpaceState.EXPIRED.equals(
                    spaceState)) {
                    spaceMetaDatas[i].setStatus(
                        new TReturnStatus(
                        TStatusCode.SRM_SPACE_LIFETIME_EXPIRED,"expired"));
                } else {
                    spaceMetaDatas[i].setStatus(
                            new TReturnStatus(TStatusCode.SRM_FAILURE,
                            "space has been released "));
                }
                spaceMetaDatas[i].setOwner("VoGroup="+space.getVoGroup()+"" +
                    " VoRole="+space.getVoRole());
            } else {
                spaceMetaDatas[i] = new TMetaDataSpace();
                spaceMetaDatas[i].setSpaceToken(Long.toString(tokens[i]));
                spaceMetaDatas[i].setStatus(new TReturnStatus(
						   TStatusCode.SRM_INVALID_REQUEST,"space not found"));
            }
        }
        return spaceMetaDatas;
    }

    /**
     *
     * @param description
     * @throws SRMException
     * @return
     */
    @Override
    public String[] srmGetSpaceTokens(SRMUser user, String description)
        throws SRMException
    {
        _log.debug("srmGetSpaceTokens ("+description+")");
        guardSpaceManagerEnabled();
        DcacheUser duser = (DcacheUser) user;
        GetSpaceTokens getTokens = new GetSpaceTokens(description);
        getTokens.setSubject(duser.getSubject());
        try {
            getTokens = _spaceManagerStub.sendAndWait(getTokens);
        } catch (TimeoutCacheException e) {
            throw new SRMInternalErrorException("SrmSpaceManager is unavailable: " +
                                                e.getMessage(), e);
        } catch (CacheException e) {
            _log.warn("GetSpaceTokens failed with rc=" + e.getRc() +
                      " error="+e.getMessage());
            throw new SRMException("GetSpaceTokens failed with rc="+
                                   e.getRc() + " error=" + e.getMessage(), e);
        } catch (InterruptedException e) {
            throw new SRMException("Request to SrmSpaceManager got interrupted", e);
        }
        long tokens[] = getTokens.getSpaceTokens();
        String tokenStrings[] = new String[tokens.length];
        for(int i = 0; i < tokens.length; ++i) {
            tokenStrings[i] = Long.toString(tokens[i]);
            _log.debug("srmGetSpaceTokens returns token#"+i+" : "+tokenStrings[i]);
        }

        return tokenStrings;
    }

    @Override
    public String[] srmGetRequestTokens(SRMUser user,String description)
        throws SRMException {
        try {
            Set<Long> tokens = srm.getBringOnlineRequestIds(user,
                    description);
            tokens.addAll(srm.getGetRequestIds(user,
                    description));
            tokens.addAll(srm.getPutRequestIds(user,
                    description));
            tokens.addAll(srm.getCopyRequestIds(user,
                    description));
            tokens.addAll(srm.getLsRequestIds(user,
                    description));
            Long[] tokenLongs = tokens
                    .toArray(new Long[tokens.size()]);
            String[] tokenStrings = new String[tokenLongs.length];
            for(int i=0;i<tokenLongs.length;++i) {
                tokenStrings[i] = tokenLongs[i].toString();
            }
            return tokenStrings;
        } catch (DataAccessException e) {
            _log.error("srmGetRequestTokens failed: {}", e.getMessage());
            throw new SRMException("srmGetRequestTokens failed: " +
                                   e.getMessage(), e);
        }
    }

    /**
     * Ensures that the user has write privileges for a path. That
     * includes checking lookup privileges. The file must exist for
     * the call to succeed.
     *
     * @param user The user ID
     * @param path The path to the file
     * @throws SRMAuthorizationException if the user lacks write privileges
     *         for this path.
     * @throws SRMInvalidPathException if the file does not exist
     * @throws SRMInternalErrorException for transient errors
     * @throws SRMException for other errors
     */
    private void checkWritePrivileges(SRMUser user, URI surl)
        throws SRMException
    {
        try {
            Subject subject = ((DcacheUser) user).getSubject();
            FsPath path = getPath(surl);
            PnfsHandler handler = new PnfsHandler(_pnfs, subject);
            handler.getFileAttributes(path.toString(),
                                      EnumSet.noneOf(FileAttribute.class),
                                      EnumSet.of(AccessMask.WRITE_DATA));
        } catch (TimeoutCacheException e) {
            throw new SRMInternalErrorException("Internal name space timeout", e);
        } catch (NotInTrashCacheException | FileNotFoundCacheException e) {
            throw new SRMInvalidPathException("Parent path does not exist", e);
        } catch (PermissionDeniedCacheException e) {
            throw new SRMAuthorizationException("Permission denied");
        } catch (CacheException e) {
            throw new SRMException(String.format("Operation failed [rc=%d,msg=%s]",
                                                 e.getRc(), e.getMessage()));
        }
    }

    /**
     *
     * we support only permanent file, lifetime is always -1
     *
     * @param newLifetime SURL lifetime in seconds
     *   -1 stands for infinite lifetime
     * @return long lifetime left in seconds
     *   -1 stands for infinite lifetime
     *
     */
    @Override
    public long srmExtendSurlLifetime(SRMUser user, URI surl, long newLifetime)
        throws SRMException
    {
        checkWritePrivileges(user, surl);
        return -1;
    }

    /**
     *
     *
     * @param user User ID
     * @param spaceToken of a valid space reservation
     * @param newReservationLifetime new lifetime
     * in millis to assign to space reservation
     * @return long lifetime of spacereservation left in milliseconds
     */
    @Override
    public long srmExtendReservationLifetime(SRMUser user, String spaceToken,
                                             long newReservationLifetime)
        throws SRMException
    {
        guardSpaceManagerEnabled();
        try {
            long longSpaceToken = Long.parseLong(spaceToken);
            ExtendLifetime extendLifetime =
                new ExtendLifetime(longSpaceToken, newReservationLifetime);
            extendLifetime = _spaceManagerStub.sendAndWait(extendLifetime);
            return extendLifetime.getNewLifetime();
        } catch (NumberFormatException e){
            throw new SRMException("Cannot parse space token: " +
                                   e.getMessage(), e);
        } catch (TimeoutCacheException e) {
            throw new SRMInternalErrorException("SrmSpaceManager is unavailable: " + e.getMessage(), e);
        } catch (CacheException e) {
            throw new SRMException("srmExtendReservationLifetime failed, " +
                                   "ExtendLifetime.returnCode="+
                                   e.getRc()+" errorObject = "+
                                   e.getMessage());
        } catch (InterruptedException e) {
            throw new SRMInternalErrorException("Request to SrmSpaceManager got interrupted", e);
        }
    }

    /**
     *
     *
     * @param user User ID
     * @param pinId Id of a valid pin
     * @param newPinLifetime new lifetime in millis to assign to pin
     * @return long lifetime left for pin in millis
     */
    @Override
    public long extendPinLifetime(SRMUser user,
        String fileId, String pinId, long newPinLifetime)
        throws SRMException
    {
        try {
            if (PinCompanion.isFakePinId(pinId)) {
                return newPinLifetime;
            }

            PnfsId pnfsId = new PnfsId(fileId);
            FileAttributes attributes = new FileAttributes();
            attributes.setPnfsId(pnfsId);
            PinManagerExtendPinMessage extendLifetime =
                new PinManagerExtendPinMessage(attributes, Long.parseLong(pinId), newPinLifetime);
            extendLifetime.setSubject(((DcacheUser) user).getSubject());
            extendLifetime = _pinManagerStub.sendAndWait(extendLifetime);
            return extendLifetime.getLifetime();
        } catch (IllegalArgumentException e) {
            throw new SRMException("Invalid PNFS ID: " + fileId, e);
        } catch (TimeoutCacheException e) {
            throw new SRMInternalErrorException("PinManager is unavailable: " +
                                                e.getMessage(), e);
        } catch (CacheException e) {
            throw new SRMException("extendPinLifetime failed, PinManagerExtendLifetimeMessage.returnCode="+ e.getRc() + " errorObject = " + e.getMessage());
        } catch (InterruptedException e) {
            throw new SRMInternalErrorException("Request to PinManager got interrupted", e);
        }
    }

    @Override
    public String getStorageBackendVersion() {
        return VERSION.getVersion();
    }

    @Override
    public boolean exists(SRMUser user, URI surl)  throws SRMException
    {
        FsPath path = getPath(surl);
        try {
            return _pnfs.getPnfsIdByPath(path.toString()) != null;
        } catch (FileNotFoundCacheException e) {
            return false;
        } catch (NotInTrashCacheException e) {
            return false;
        } catch (CacheException e) {
            _log.error("Failed to find file by path : " + e.getMessage());
            throw new SRMException("Failed to find file by path due to internal system failure or timeout: " + e.getMessage());
        }
    }

    /**
     * Adds a trailing slash to a string unless the string already has
     * a trailing slash.
     */
    private String addTrailingSlash(String s)
    {
        if (!s.endsWith("/")) {
            s = s + "/";
        }
        return s;
    }

    /**
     * Given a path relative to the root path, this method returns a
     * full PNFS path.
     */
    private FsPath getPath(String path)
    {
        return new FsPath(new FsPath(config.getSrm_root()), new FsPath(path));
    }

    /**
     * Given a surl, this method returns a full PNFS path.
     */
    private FsPath getPath(URI surl)
        throws SRMInvalidPathException
    {
        return getPath(getPathOfSurl(surl));
    }

    /**
     * Given a surl, this method returns the path in the surl.
     */
    private String getPathOfSurl(URI surl)
        throws SRMInvalidPathException
    {
        try {
            String scheme = surl.getScheme();
            if (scheme != null && !scheme.equalsIgnoreCase("srm")) {
                throw new SRMInvalidPathException("Invalid scheme: " + scheme);
            }

            String host = surl.getHost();
            if (host != null && !Tools.sameHost(config.getSrmHosts(), host)) {
                throw new SRMInvalidPathException("SURL is not local: " + surl);
            }

            String path = surl.getPath();
            String query = surl.getQuery();
            if (query != null) {
                int i = query.indexOf(SFN_STRING);
                if (i != -1) {
                    path = query.substring(i + SFN_STRING.length());
                }
            }
            return path;
        } catch (UnknownHostException e) {
            throw new SRMInvalidPathException(e.getMessage());
        }
    }
}
