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

import diskCacheV111.poolManager.PoolSelectionUnit.DirectionType;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellVersion;
import dmg.cells.nucleus.CellInfo;
import dmg.cells.nucleus.NoRouteToCellException;
import org.dcache.cells.CellStub;
import org.dcache.cells.AbstractMessageCallback;
import org.dcache.cells.AbstractCellComponent;
import org.dcache.cells.CellMessageReceiver;
import org.dcache.cells.CellCommandListener;
import dmg.util.Args;
import dmg.cells.services.login.LoginBrokerInfo;
import diskCacheV111.util.PnfsHandler;
import diskCacheV111.util.PnfsId;
import diskCacheV111.util.FsPath;
import diskCacheV111.util.Version;
import diskCacheV111.util.FileLocality;
import diskCacheV111.vehicles.StorageInfo;
import diskCacheV111.vehicles.RemoteHttpDataTransferProtocolInfo;
import diskCacheV111.vehicles.IpProtocolInfo;
import diskCacheV111.vehicles.PoolManagerGetPoolMonitor;
import diskCacheV111.poolManager.PoolMonitorV5;
import org.dcache.auth.LoginStrategy;
import org.dcache.auth.AuthorizationRecord;
import org.dcache.services.login.RemoteLoginStrategy;
import org.dcache.auth.KauthFileLoginStrategy;
import org.dcache.srm.SrmCancelUseOfSpaceCallbacks;
import org.dcache.srm.SrmReleaseSpaceCallbacks;
import org.dcache.srm.SrmUseSpaceCallbacks;
import org.dcache.srm.util.Configuration;
import org.dcache.srm.util.Tools;
import org.dcache.srm.security.SslGsiSocketFactory;
import org.dcache.srm.scheduler.IllegalStateTransition;
import org.dcache.pinmanager.PinManagerExtendPinMessage;
import diskCacheV111.util.CacheException;
import diskCacheV111.vehicles.transferManager.
    RemoteTransferManagerMessage;
import diskCacheV111.vehicles.transferManager.
    RemoteGsiftpTransferProtocolInfo;
import diskCacheV111.vehicles.transferManager.
    RemoteGsiftpDelegateUserCredentialsMessage;
import diskCacheV111.vehicles.transferManager.TransferManagerMessage;
import diskCacheV111.vehicles.transferManager.CancelTransferMessage;
import diskCacheV111.vehicles.transferManager.TransferCompleteMessage;
import diskCacheV111.vehicles.transferManager.TransferFailedMessage;
import diskCacheV111.vehicles.CopyManagerMessage;
import diskCacheV111.vehicles.PoolManagerGetPoolListMessage;
import diskCacheV111.services.space.message.ExtendLifetime;
import diskCacheV111.services.space.message.GetFileSpaceTokensMessage;
import diskCacheV111.pools.PoolCellInfo;
import diskCacheV111.pools.PoolCostInfo;
import org.globus.util.GlobusURL;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.UnknownHostException;
import java.net.Socket;
import java.net.URI;
import java.net.URISyntaxException;
import java.io.IOException;
import java.io.OutputStream;
import java.sql.SQLException;
import java.util.Random;
import java.util.Vector;
import java.util.List;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Set;
import java.util.HashMap;
import java.util.Map;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Collection;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.Semaphore;
import static java.util.concurrent.TimeUnit.*;
import org.dcache.namespace.PermissionHandler;
import org.dcache.namespace.ChainedPermissionHandler;
import org.dcache.namespace.PosixPermissionHandler;
import org.dcache.namespace.ACLPermissionHandler;
import org.dcache.srm.AbstractStorageElement;
import org.dcache.srm.SRMException;
import org.dcache.srm.SRMDuplicationException;
import org.dcache.srm.SRMInternalErrorException;
import org.dcache.srm.SRMInvalidPathException;
import org.dcache.srm.SRMAuthorizationException;
import org.dcache.srm.SRMInvalidRequestException;
import org.dcache.srm.FileMetaData;
import org.dcache.srm.PrepareToPutCallbacks;
import org.dcache.srm.PrepareToPutInSpaceCallbacks;
import org.dcache.srm.SrmReserveSpaceCallbacks;
import diskCacheV111.srm.StorageElementInfo;
import org.dcache.srm.AdvisoryDeleteCallbacks;
import org.dcache.srm.RemoveFileCallbacks;
import org.dcache.srm.PinCallbacks;
import org.dcache.srm.UnpinCallbacks;
import org.dcache.srm.util.Permissions;
import org.dcache.srm.CopyCallbacks;
import org.dcache.srm.scheduler.Job;
import org.dcache.srm.SRM;
import org.dcache.srm.v2_2.TMetaDataSpace;
import org.dcache.srm.v2_2.TRetentionPolicyInfo;
import org.dcache.srm.v2_2.TRetentionPolicy;
import org.dcache.srm.v2_2.TAccessLatency;
import org.dcache.srm.v2_2.TStatusCode;
import org.dcache.srm.v2_2.TReturnStatus;
import org.dcache.srm.v2_2.TFileLocality;
import org.dcache.util.LoginBrokerHandler;
import org.dcache.util.Interval;
import org.dcache.util.list.DirectoryListSource;
import org.dcache.util.list.DirectoryListPrinter;
import org.dcache.util.list.DirectoryEntry;
import org.dcache.util.list.ListDirectoryHandler;
import diskCacheV111.util.RetentionPolicy;
import diskCacheV111.util.AccessLatency;
import diskCacheV111.services.space.message.GetSpaceMetaData;
import diskCacheV111.services.space.message.GetSpaceTokens;
import diskCacheV111.util.TimeoutCacheException;
import diskCacheV111.util.NotInTrashCacheException;
import diskCacheV111.util.FileNotFoundCacheException;
import diskCacheV111.util.NotDirCacheException;
import diskCacheV111.util.PermissionDeniedCacheException;
import diskCacheV111.util.FileExistsCacheException;
import org.dcache.auth.persistence.AuthRecordPersistenceManager;
import org.dcache.auth.Subjects;
import org.dcache.vehicles.FileAttributes;
import org.dcache.namespace.FileAttribute;
import org.dcache.namespace.FileType;
import org.dcache.acl.enums.AccessType;
import org.dcache.acl.enums.AccessMask;
import org.ietf.jgss.GSSCredential;
import org.dcache.srm.SRMUser;
import org.dcache.srm.request.RequestCredential;
import java.io.File;
import javax.naming.NamingException;
import javax.naming.NamingEnumeration;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;
import javax.security.auth.Subject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;

import org.springframework.beans.factory.annotation.Required;

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

    private static boolean kludgeDomainMainWasRun = false;

    /* these are the  protocols
     * that are not sutable for either put or get */
    private static final String[] SRM_PUT_NOT_SUPPORTED_PROTOCOLS
        = { "http" };
    private static final String[] SRM_GET_NOT_SUPPORTED_PROTOCOLS
        = {};
    private static final String[] SRM_PREFERED_PROTOCOLS
        = { "gsiftp", "gsidcap" };

    private final static String SFN_STRING = "SFN=";

    /**
     * The delay we use after transient failures that should be
     * retried immediately. The small delay prevents tight retry
     * loops.
     */
    private final static long TRANSIENT_FAILURE_DELAY =
        MILLISECONDS.toMillis(10);

    private CellStub _pnfsStub;
    private CellStub _poolManagerStub;
    private CellStub _poolStub;
    private CellStub _spaceManagerStub;
    private CellStub _copyManagerStub;
    private CellStub _transferManagerStub;
    private CellStub _pinManagerStub;
    private CellStub _loginBrokerStub;

    private PnfsHandler _pnfs;
    private final PermissionHandler permissionHandler =
            new ChainedPermissionHandler(new ACLPermissionHandler(),
                                         new PosixPermissionHandler());

    private PoolMonitorV5 _poolMonitor;

    private SRM srm;
    private Configuration config;
    private Thread storageInfoUpdateThread;
    private boolean ignoreClientProtocolOrder; //falseByDefault
    private boolean customGetHostByAddr; //falseByDefault

    private FsPath _xrootdRootPath;
    private FsPath _httpRootPath;

    private LoginBrokerHandler _loginBrokerHandler;
    private DirectoryListSource _listSource;

    private boolean _isOnlinePinningEnabled = true;

    // public static SRM getSRMInstance(String xmlConfigPath)
    public static SRM getSRMInstance(final String[] dCacheParams,
            long timeout)
            throws InterruptedException,
            TimeoutException
    {
        _log.info("Here are the params/args to go to dCache: " + Arrays.toString(dCacheParams));

        _log.debug("entering Storage.getSRMInstance");
        SRM srmInstance = SRM.getSRM();
        if (srmInstance != null) {
            _log.debug("in Storage.getSRMInstance(), about to " +
                       "return existing srmInstance");
            return srmInstance;
        } else {
            // TODO:  Here is the kludge to keep from calling Domain.main
            //        twice, and therefore trying to create 2 instances
            //        of SRM.  We need a better solution than this...

            if (!kludgeDomainMainWasRun) {

                _log.debug(
                        "in Storage.getSRMInstance(),  " +
                        "srmInstance is null, " +
                        "about to call Domain.main()");
                new Thread() {

                    @Override
                    public void run() {

                        // Calling the main method and passing some
                        // arguments is kludgey.  But, we have no other
                        // way of calling Domain; we
                        // cannot modify anything in cells.
                        // ToDo:  Work with DESY to improve class Domain
                        // so that we can activate it without just
                        // calling the main.
                        dmg.cells.services.Domain.main(dCacheParams);
                    }
                }.start();

                _log.debug(
                    "in Storage.getSRMInstance(), " +
                    "started thread that will call " +
                    " Domain.main()");
            } else {
                _log.debug(
                        "in Storage.getSRMInstance(), Domain.main has " +
                        "already been run.");
            }
        }
        srmInstance = SRM.getInstance(timeout);
        _log.debug("about to return the instance of srm");
        return srmInstance;
    }

    public Storage()
    {
    }

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
    public void setSpaceManagerStub(CellStub spaceManagerStub)
    {
        _spaceManagerStub = spaceManagerStub;
    }

    @Required
    public void setPoolStub(CellStub poolStub)
    {
        _poolStub = poolStub;
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

    public void setIgnoreClientProtocolOrder(boolean ignore)
    {
        ignoreClientProtocolOrder = ignore;
    }

    public void start()
        throws Exception
    {
        _log.info("Starting SRM");

        if (config.getJdbcPass() == null && config.getJdbcPwdfile() == null) {
            String error = "database parameters are not specified; use options " +
                "-jdbcUrl, -jdbcDriver, -dbUser and -dbPass/-pgPass";
            _log.error(error);
            throw new Exception(error);
        }

        if (config.isGsissl()) {
            config.setWebservice_protocol("https");

            LoginStrategy loginStrategy;
            if (config.getUseGplazmaAuthzCellFlag() ||
                config.getUseGplazmaAuthzModuleFlag()) {
                loginStrategy =
                    new RemoteLoginStrategy(new CellStub(getCellEndpoint(), new CellPath("gPlazma"), 30000));
            } else {
                loginStrategy =
                    new KauthFileLoginStrategy(new File(config.getKpwdfile()));
            }

            DCacheAuthorization authorization =
                new DCacheAuthorization(loginStrategy,
                                        (AuthRecordPersistenceManager) config.getSrmUserPersistenceManager());
            authorization.setCacheLifetime(config.getAuthzCacheLifetime());
            config.setAuthorization(authorization);
        } else {
            config.setWebservice_protocol("http");
        }

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

        srm = SRM.getSRM(config, getCellName());
    }

    public void stop()
    {
        srm.stop();
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

    @Required
    public void setLoginBrokerHandler(LoginBrokerHandler handler)
        throws UnknownHostException
    {
        handler.setAddresses(Arrays.asList(InetAddress.getAllByName(InetAddress.getLocalHost().getHostName())));
        handler.setLoad(new LoginBrokerHandler.LoadProvider() {
                public double getLoad() {
                    return (srm == null) ? 0 : srm.getLoad();
                }
            });
        _loginBrokerHandler = handler;
    }

    @Override
    public void getInfo(java.io.PrintWriter pw)
    {
        StorageElementInfo info = getStorageElementInfo();
        if (info != null) {
            pw.println(info);
        }

        pw.println(config);

        try {
            StringBuilder sb = new StringBuilder();
            srm.printGetSchedulerInfo(sb);
            srm.printPutSchedulerInfo(sb);
            srm.printCopySchedulerInfo(sb);
            srm.printBringOnlineSchedulerInfo(sb);
            srm.printLsSchedulerInfo(sb);
            pw.println(sb);
        } catch (SQLException e) {
            _log.error(e.toString());
        }
    }

    @Override
    public CellInfo getCellInfo(CellInfo info)
    {
        info.setCellVersion(new CellVersion(Version.getVersion(),
                                            "$Revision$"));
        return info;
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
        Collection<String> sets = new ArrayList<String>();
        for (Map.Entry<String,String> e: OPTION_TO_PARAMETER_SET.entrySet()) {
            if (args.getOpt(e.getKey()) != null) {
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
        } catch (SQLException e) {
            _log.warn(e.toString());
            return e.toString();
        }
    }

    public final static String fh_cancelall= " Syntax: cancel [-get] [-put] [-copy] [-bring] [-reserve] <pattern> ";
    public final static String hh_cancelall= " [-get] [-put] [-copy] [-bring] [-reserve] <pattern> ";
    public String ac_cancelall_$_1(Args args) {
        try {
            boolean get=args.getOpt("get") != null;
            boolean put=args.getOpt("put") != null;
            boolean copy=args.getOpt("copy") != null;
            boolean bring=args.getOpt("bring") != null;
            boolean reserve=args.getOpt("reserve") != null;
            boolean ls=args.getOpt("ls") != null;
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
        } catch (RuntimeException e) {
            _log.error("Failure in cancelall", e);
            return e.toString();
        } catch (Exception e) {
            _log.warn("Failure in cancelall: " + e.getMessage());
            return e.toString();
        }
    }
    public final static String fh_ls= " Syntax: ls [-get] [-put] [-copy] [-bring] [-reserve] [-ls] [-l] [<id>] "+
            "#will list all requests";
    public final static String hh_ls= " [-get] [-put] [-copy] [-bring] [-reserve] [-ls] [-l] [<id>]";
    public String ac_ls_$_0_1(Args args) {
        try {
            boolean get=args.getOpt("get") != null;
            boolean put=args.getOpt("put") != null;
            boolean copy=args.getOpt("copy") != null;
            boolean bring=args.getOpt("bring") != null;
            boolean reserve=args.getOpt("reserve") != null;
            boolean ls=args.getOpt("ls") != null;
            boolean longformat = args.getOpt("l") != null;
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
        } catch(Throwable t) {
            t.printStackTrace();
            return t.toString();
        }
    }
    public final static String fh_ls_queues= " Syntax: ls queues " +
        "[-get] [-put] [-copy] [-bring] [-ls] [-l]  "+
            "#will list schedule queues";
    public final static String hh_ls_queues= " [-get] [-put] [-copy] [-bring] [-ls] [-l] ";
    public String ac_ls_queues_$_0(Args args) {
        try {
            boolean get=args.getOpt("get") != null;
            boolean put=args.getOpt("put") != null;
            boolean ls=args.getOpt("ls") != null;
            boolean copy=args.getOpt("copy") != null;
            boolean bring=args.getOpt("bring") != null;
            boolean longformat = args.getOpt("l") != null;
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
        } catch(Throwable t) {
            t.printStackTrace();
            return t.toString();
        }
    }

    public final static String fh_ls_completed= " Syntax: ls completed [-get] [-put]" +
        " [-copy] [-l] [max_count]"+
            " #will list completed (done, failed or canceled) requests, " +
        "if max_count is not specified, it is set to 50";
    public final static String hh_ls_completed= " [-get] [-put] [-copy] [-l] [max_count]";
    public String ac_ls_completed_$_0_1(Args args) throws Exception{
        boolean get=args.getOpt("get") != null;
        boolean put=args.getOpt("put") != null;
        boolean copy=args.getOpt("copy") != null;
        boolean longformat = args.getOpt("l") != null;
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
            Job job = Job.getJob(requestId);
            if(job == null ) {
                return "request with reqiest id "+requestId+" is not found\n";
            }
            job.setPriority(priority);
            job.setPriority(priority);
            StringBuilder sb = new StringBuilder();
            srm.listRequest(sb, requestId, true);
            return sb.toString();
        } catch (RuntimeException e) {
            _log.error("Failure in set job priority", e);
            return e.toString();
        } catch (Exception e) {
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
            boolean longformat = args.getOpt("l") != null;
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
        diskCacheV111.util.ThreadManager.execute(new Runnable() {
                public void run() {
                    handleTransferManagerMessage(msg);
                }
            });
    }

    public void pinFile(SRMUser user,
                        URI surl,
                        String clientHost,
                        long pinLifetime,
                        long requestId,
                        PinCallbacks callbacks)
    {
        try {
            PinCompanion.pinFile(Subjects.getSubject((AuthorizationRecord)user),
                                 getPath(surl),
                                 clientHost,
                                 callbacks,
                                 pinLifetime,
                                 requestId,
                                 _isOnlinePinningEnabled,
                                 _poolMonitor,
                                 _pnfsStub,
                                 _poolManagerStub,
                                 _pinManagerStub);
        } catch (SRMInvalidPathException e) {
            callbacks.FileNotFound(e.getMessage());
        }
    }

    public void unPinFile(SRMUser user, String fileId,
                          UnpinCallbacks callbacks,
                          String pinId)
    {
        if (PinCompanion.isFakePinId(pinId)) {
            return;
        }

        UnpinCompanion.unpinFile(Subjects.getSubject((AuthorizationRecord) user),
                                 new PnfsId(fileId), Long.parseLong(pinId), callbacks,_pinManagerStub);
    }

    public void unPinFileBySrmRequestId(SRMUser user, String fileId,
                                        UnpinCallbacks callbacks,
                                        long srmRequestId)
    {
        UnpinCompanion.unpinFileBySrmRequestId(Subjects.getSubject((AuthorizationRecord) user), new PnfsId(fileId), srmRequestId, callbacks, _pinManagerStub);
    }

    public void unPinFile(SRMUser user, String fileId, UnpinCallbacks callbacks)
    {
        UnpinCompanion.unpinFile(Subjects.getSubject((AuthorizationRecord) user),
                                 new PnfsId(fileId), callbacks, _pinManagerStub);
    }


    public String selectGetProtocol(String[] protocols)
    throws SRMException {
        Set<String> available_protocols = listAvailableProtocols();
        available_protocols.retainAll(Arrays.asList(protocols));
        available_protocols.removeAll(Arrays.asList(SRM_GET_NOT_SUPPORTED_PROTOCOLS));
        if(available_protocols.size() == 0) {
            _log.error("can not find sutable get protocol");
            throw new SRMException("can not find sutable get protocol");
        }

         /*
          * this is incorrect, need to select on basis of client's preferences
          * But we need to continue doing this while old srmcp clients
          * are out there in the wild
          */
         if(ignoreClientProtocolOrder) {
              for(int i = 0; i<SRM_PREFERED_PROTOCOLS.length; ++i) {
                if(available_protocols.contains(SRM_PREFERED_PROTOCOLS[i])) {
                   return SRM_PREFERED_PROTOCOLS[i];
                }
             }
         }

         for(int i = 0; i<protocols.length; ++i) {
            if(available_protocols.contains(protocols[i])) {
                return protocols[i];
            }
        }

        // we should never get here
        throw new SRMException("can not find sutable get protocol");
    }

    public String selectPutProtocol(String[] protocols)
    throws SRMException {
        Set<String> available_protocols = listAvailableProtocols();
        available_protocols.retainAll(Arrays.asList(protocols));
        available_protocols.removeAll(Arrays.asList(SRM_PUT_NOT_SUPPORTED_PROTOCOLS));
        if(available_protocols.size() == 0) {
            _log.error("can not find sutable put protocol");
            throw new SRMException("can not find sutable put protocol");
        }

         /*
          *this is incorrect, need to select on basis of client's preferences
          * But we need to continue doing this while old srmcp clients
          * are out there in the wild
          */
         if(ignoreClientProtocolOrder) {
             for(int i = 0; i<SRM_PREFERED_PROTOCOLS.length; ++i) {
                if(available_protocols.contains(SRM_PREFERED_PROTOCOLS[i])) {
                   return SRM_PREFERED_PROTOCOLS[i];
                }
             }
         }


        for(int i = 0; i<protocols.length; ++i) {
            if(available_protocols.contains(protocols[i])) {
                return protocols[i];
            }
        }

        // we should never get here
        throw new SRMException("can not find sutable put protocol");
    }

    public String[] supportedGetProtocols()
    throws SRMException {
        Set<String> protocols = listAvailableProtocols();
        return protocols.toArray(new String[0]);
    }

    public String[] supportedPutProtocols()
    throws SRMException {
        Set<String> protocols = listAvailableProtocols();
        // "http" is for getting only
        if(protocols.contains("http")) {
            protocols.remove("http");
        }
        return protocols.toArray(new String[0]);
    }

    public String selectGetHost(String protocol,String fileId)
    throws SRMException {
        return this.selectHost(protocol);
    }

    public String selectPutHost(String protocol)
    throws SRMException {
        return this.selectHost(protocol);
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
            throw new SRMException("cab not get transfer path");
        }
        try {
            if (port == 0) {
                port = -1;
            }
            URI turl =
                new URI(protocol, null, host, port, transfer_path, null, null);
            _log.debug("getTurl() returns turl="+turl);
            return turl;
        } catch (URISyntaxException e) {
            throw new SRMInternalErrorException(e.getMessage());
        }
    }

    private boolean verifyUserPathIsRootSubpath(FsPath absolutePath, SRMUser user) {

        if(absolutePath == null) {
            return false;
        }
        String user_root = null;
        if(user != null) {
            AuthorizationRecord duser = (AuthorizationRecord) user;
            user_root = duser.getRoot();
            if(user_root != null) {
                user_root =new FsPath(user_root).toString();
            }
        }


        if(user_root!= null) {
            String path = absolutePath.toString();
            _log.debug("getTurl() user root is "+user_root);
            if(!path.startsWith(user_root)) {
                String error = "verifyUserPathIsInTheRoot error:"+
                        "user's path "+absolutePath+
                        " is not subpath of the user's root" +user_root;
                _log.warn(error);
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
            AuthorizationRecord duser = (AuthorizationRecord) user;
            String root = duser.getRoot();
            if (root != null) {
                userRoot = new FsPath(root);
            }
        }

        if (!verifyUserPathIsRootSubpath(path, user)) {
            throw new SRMAuthorizationException(String.format("Access denied: Path [%s] is outside user's root [%s]", path, userRoot));
        }

        String transferPath;
        if (protocol.equals("gsiftp")) {
            transferPath = stripRootPath(userRoot, path);
        } else if (protocol.equals("http") || protocol.equals("https")) {
            transferPath = stripRootPath(_httpRootPath, path);
        } else if (protocol.equals("root")) {
            transferPath = stripRootPath(_xrootdRootPath, path);
        } else {
            transferPath = path.toString();
        }

        _log.debug("getTurlPath(path=" + path + ",protocol=" + protocol +
            ",user=" + user + ") = " + transferPath);

        return transferPath;
    }

    public LoginBrokerInfo[] getLoginBrokerInfos()
        throws SRMException
    {
        return getLoginBrokerInfos((String)null);
    }

    // These hashtables are used as a caching mechanizm for the login
    // broker infos. Here we asume that no protocol called "null" is
    // going to be ever used.
    private final Map<String,LoginBrokerInfo[]> latestLoginBrokerInfos =
        new HashMap<String,LoginBrokerInfo[]>();
    private final Map<String,Long> latestLoginBrokerInfosTimes =
        new HashMap<String,Long>();
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
        Set<String> protocols = new HashSet<String>();
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


    public String selectHost(String protocol)
        throws SRMException
    {
        _log.debug("selectHost("+protocol+")");
        boolean tryFile = false;
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
            new HashMap<String,HostnameCacheRecord>();
    private String lbiToDoor(LoginBrokerInfo lbi) throws SRMException {

            String thehost =lbi.getHost();
            String resolvedHost;
            HostnameCacheRecord resolvedHostRecord = doorToHostnameMap.get(thehost);
            if(resolvedHostRecord == null || resolvedHostRecord.expired() ) {
                try {

                    InetAddress address = InetAddress.getByName(thehost);
                    resolvedHost = address.getHostName();
                    if ( customGetHostByAddr && resolvedHost.toUpperCase().equals(
                        resolvedHost.toLowerCase() )  ) {// must be an IP address
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

            Map<String,List<String>> map = new HashMap<String,List<String>>();
            DirContext ctx = new InitialDirContext();
            javax.naming.directory.Attributes attrs =
                    ctx.getAttributes(name, attrIds);

            if (attrs == null) {
                return null;
            } else {
                /* get each attribute */
                for (NamingEnumeration ae = attrs.getAll();
                     ae != null && ae.hasMoreElements();) {
                    javax.naming.directory.Attribute attr =
                            (javax.naming.directory.Attribute)ae.next();
                    String attrID = attr.getID();
                    List<String> l = new ArrayList<String>();
                    for (NamingEnumeration e = attr.getAll();
                         e.hasMoreElements();) {
                        String literalip = (String)e.nextElement();
                        l.add(literalip);
                    }
                    map.put(attrID, l);
                }
            }
            return map;
        }

        private static String getHostByAddr(byte[] addr)
        throws java.net.UnknownHostException {
            try {
                StringBuilder literalip = new StringBuilder();
                if (addr.length == 4) {
                    for (int i = addr.length-1; i >= 0; i--) {
                        literalip.append(addr[i] & 0xff).append(".");
                    }
                } else if (addr.length == 16) {
                    for (int i = addr.length-1; i >= 0; i--) {
                        literalip.append(addr[i] & 0x0f).append(".").append(addr[i] & 0xf0).append(".");
                    }
                }
                if (addr.length == 4) { // ipv4 addr
                    literalip.append("IN-ADDR.ARPA.");
                } else if (addr.length == 16) { // ipv6 addr
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
            } catch (Exception e) {
                throw new java.net.UnknownHostException(e.getMessage());
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
            PutCompanion.PrepareToPutFile((AuthorizationRecord) user,
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
        AuthorizationRecord duser = (AuthorizationRecord) user;
        PnfsHandler handler =
            new PnfsHandler(_pnfs, Subjects.getSubject(duser));

        try {
            if (!(fmd instanceof DcacheFileMetaData)) {
                throw new SRMException("Storage.setFileMetaData: " +
                                       "metadata in not dCacheMetaData");
            }
            DcacheFileMetaData dfmd = (DcacheFileMetaData) fmd;
            FileAttributes updatedAttributes = new FileAttributes();
            updatedAttributes.setMode(dfmd.permMode);
            handler.setFileAttributes(dfmd.getPnfsId(), updatedAttributes);

            FileAttributes attributes = dfmd.getFileAttributes();
            attributes.setMode(dfmd.permMode);
        } catch (TimeoutCacheException e) {
            throw new SRMInternalErrorException("PnfsManager is unavailable: "
                                                + e.getMessage(), e);
        } catch (NotInTrashCacheException e) {
            throw new SRMInvalidPathException("No such file or directory", e);
        } catch (FileNotFoundCacheException e) {
            throw new SRMInvalidPathException("No such file or directory", e);
        } catch (PermissionDeniedCacheException e) {
            throw new SRMAuthorizationException("Permission denied");
        } catch (CacheException e) {
            throw new SRMException("SetFileMetaData failed for " + fmd.SURL +
                                   "; return code=" + e.getRc() +
                                   " reason=" + e.getMessage());
        }
    }

    @Override
    public FileMetaData getFileMetaData(SRMUser user, URI surl, boolean read)
        throws SRMException
    {
        _log.debug("getFileMetaData(" + surl + ")");
        FsPath path = getPath(surl);
        AuthorizationRecord duser = (AuthorizationRecord) user;
        PnfsHandler handler =
            new PnfsHandler(_pnfs, Subjects.getSubject(duser));
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
                switch (locality) {
                case ONLINE:
                case ONLINE_AND_NEARLINE:
                    fmd.isCached = true;
                }
            }

            /* Determine space tokens.
             */
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

    private final Map<Long,String> idToUserMap =
        new HashMap<Long,String>();
    private final Map<Long,GSSCredential> idToCredentialMap =
        new HashMap<Long,GSSCredential>();

    private String getUserById(long id) {
        _log.debug("getDcacheUserById("+id+")");
        synchronized(idToUserMap) {
            return idToUserMap.get(id);

        }
    }

    private GSSCredential getCredentialById(long id) {
        _log.debug("getDcacheUserById("+id+")");
        synchronized(idToUserMap) {
            return idToCredentialMap.get(id);

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
        AuthorizationRecord duser = (AuthorizationRecord)user;
        try {
            CopyManagerMessage copyRequest =
                new CopyManagerMessage(actualFromFilePath.toString(),
                                       actualToFilePath.toString(),
                                       id,
                                       config.getBuffer_size(),
                                       config.getTcp_buffer_size());
            copyRequest.setSubject(Subjects.getSubject((AuthorizationRecord) user));
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

    public void prepareToPutInReservedSpace(SRMUser user, String path, long size,
        long spaceReservationToken, PrepareToPutInSpaceCallbacks callbacks) {
        throw new java.lang.UnsupportedOperationException("NotImplementedException");
    }

    private final Map<String,StorageElementInfo> poolInfos =
        new HashMap<String,StorageElementInfo>();
    private final Map<String,Long> poolInfosTimestamps =
        new HashMap<String,Long>();

    public StorageElementInfo getPoolInfo(String pool)
        throws SRMException, InterruptedException
    {
        synchronized (poolInfosTimestamps) {
            Long timestamp = poolInfosTimestamps.get(pool);
            if (timestamp != null &&
                (System.currentTimeMillis() - timestamp) < 3 * 1000 * 60) {
                return poolInfos.get(pool);
            }
        }

        try {
            PoolCellInfo poolCellInfo =
                _poolStub.sendAndWait(new CellPath(pool),
                                      "xgetcellinfo", PoolCellInfo.class);

            PoolCostInfo.PoolSpaceInfo info = poolCellInfo.getPoolCostInfo().getSpaceInfo() ;
            long total     = info.getTotalSpace() ;
            long freespace = info.getFreeSpace() ;
            long precious  = info.getPreciousSpace() ;
            long removable = info.getRemovableSpace() ;
            StorageElementInfo poolInfo = new StorageElementInfo();
            poolInfo.totalSpace = total;
            poolInfo.availableSpace = freespace+removable;
            poolInfo.usedSpace = precious+removable;

            synchronized (poolInfosTimestamps) {
                poolInfos.put(pool, poolInfo);
                poolInfosTimestamps.put(pool, System.currentTimeMillis());
            }
            return poolInfo;
        } catch (TimeoutCacheException e) {
            throw new SRMInternalErrorException("Pool is unavailable: " + e.getMessage(), e);
        } catch (CacheException e) {
            _log.error("Pool returned [" + e + "] for xgetcellinfo");
            throw new SRMException(e.getMessage(), e);
        }
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
                public void RemoveFileSucceeded()
                {
                    callback.AdvisoryDeleteSuccesseded();
                }

                public void RemoveFileFailed(String reason)
                {
                    callback.AdvisoryDeleteFailed(reason);
                }

                public void FileNotFound(String error)
                {
                    callback.AdvisoryDeleteFailed(error);
                }

                public void Exception(Exception e)
                {
                    callback.Exception(e);
                }

                public void Timeout()
                {
                    callback.Timeout();
                }

                public void PermissionDenied()
                {
                    callback.AdvisoryDeleteFailed("Permission denied");
                }
            };

        try {
            RemoveFileCompanion.removeFile((AuthorizationRecord) user,
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
            RemoveFileCompanion.removeFile((AuthorizationRecord)user,
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
            } catch (FileNotFoundCacheException e) {
                throw new SRMException("File does not exist: " + surl);
            } catch (NotInTrashCacheException e) {
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

        Subject subject = Subjects.getSubject((AuthorizationRecord) user);
        PnfsHandler handler = new PnfsHandler(_pnfs, subject);

        try {
            handler.createPnfsDirectory(getPath(surl).toString());
        } catch (TimeoutCacheException e) {
            throw new SRMInternalErrorException("Internal name space timeout", e);
        } catch (NotDirCacheException e) {
            throw new SRMInvalidPathException("Parent path is not a directory", e);
        } catch (NotInTrashCacheException e) {
            throw new SRMInvalidPathException("Parent path does not exist", e);
        } catch (FileNotFoundCacheException e) {
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
        Subject subject = Subjects.getSubject((AuthorizationRecord) user);
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

        if(user == null || (!(user instanceof AuthorizationRecord))) {
            return false;
        }
        AuthorizationRecord duser = (AuthorizationRecord) user;

        if(duser.getGid() == gid && Permissions.groupCanRead(permissions)) {
            return true;
        }

        if(duser.getUid() == uid && Permissions.userCanRead(permissions)) {
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

        AuthorizationRecord duser = (AuthorizationRecord) user;
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
            } else  if(user == null || (!(user instanceof AuthorizationRecord))) {
               canWrite = false;
            } else  if(duser.getGid() == gid &&
                    Permissions.groupCanWrite(permissions) ) {
                canWrite = true;
            } else  if(duser.getUid() == uid &&
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
        } else  if(user == null || (!(user instanceof AuthorizationRecord))) {
           parentCanWrite = false;
        } else  if(duser.getGid() == parentGid &&
                Permissions.groupCanWrite(parentPermissions) &&
                Permissions.groupCanExecute(parentPermissions)) {
            parentCanWrite = true;
        } else  if(duser.getUid() == parentUid &&
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
                Long.valueOf(size),
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
                    new diskCacheV111.vehicles.transferManager.
                    CancelTransferMessage(info.transferId, callerId);
                sendMessage(new CellMessage(info.cellPath,cancel));
            }
        } catch (NoRouteToCellException e) {
            _log.error("Failed to kill remote transfer: " + e.getMessage());
        } catch (NumberFormatException e) {
            _log.error("Failed to kill remote transfer: Cannot parse transfer ID");
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
        Subject subject = Subjects.getSubject((AuthorizationRecord) user);

        _log.debug("performRemoteTransfer performing "+(store?"store":"restore"));
        if (!verifyUserPathIsRootSubpath(actualFilePath,user)) {
            throw new SRMAuthorizationException("user's path "+actualFilePath+
                                                " is not subpath of the user's root");
        }

        IpProtocolInfo protocolInfo;

        if (remoteTURL.getScheme().equals("gsiftp")) {
            //call this for the sake of checking that user is reading
            // from the "root" of the user
            String path = getTurlPath(actualFilePath,"gsiftp",user);
            if (path == null) {
                throw new SRMException("user is not authorized to access path: "+
                                       actualFilePath);
            }

            String[] hosts = new String[] { remoteTURL.getHost() };
            RemoteGsiftpTransferProtocolInfo gsiftpProtocolInfo =
                new RemoteGsiftpTransferProtocolInfo("RemoteGsiftpTransfer",
                                                     1, 1, hosts, 0,
                                                     remoteTURL.toString(),
                                                     getCellName(),
                                                     getCellDomainName(),
                                                     config.getBuffer_size(),
                                                     config.getTcp_buffer_size(),
                                                     remoteCredentialId);
            gsiftpProtocolInfo.setEmode(true);
            gsiftpProtocolInfo.setStreams_num(config.getParallel_streams());
            protocolInfo = gsiftpProtocolInfo;
        } else if (remoteTURL.getScheme().equals("http")) {
            String[] hosts = new String[] { remoteTURL.getHost() };
            protocolInfo =
                new RemoteHttpDataTransferProtocolInfo("RemoteHttpDataTransfer",
                                                       1, 1, hosts, 0,
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
                new TransferInfo(id, remoteCredentialId, callbacks,
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
        new ConcurrentHashMap<Long,TransferInfo>();

    private static class TransferInfo
    {
        final long transferId;
        final Long remoteCredentialId;
        final CopyCallbacks callbacks;
        final CellPath cellPath;

        public TransferInfo(long transferId, Long remoteCredentialId,
                            CopyCallbacks callbacks, CellPath cellPath)
        {
            this.transferId = transferId;
            this.remoteCredentialId = remoteCredentialId;
            this.callbacks = callbacks;
            this.cellPath = cellPath;
        }
    }

    private void handleTransferManagerMessage(TransferManagerMessage message) {
        Long callerId = Long.valueOf(message.getId());
        _log.debug("handleTransferManagerMessage for callerId="+callerId);

        if(message instanceof RemoteGsiftpDelegateUserCredentialsMessage) {
            RemoteGsiftpDelegateUserCredentialsMessage delegate =
                    (RemoteGsiftpDelegateUserCredentialsMessage)message;
            Long remoteCredentialId = delegate.getRequestCredentialId();
            final String host = delegate.getHost();
            final int port = delegate.getPort();
            RequestCredential remoteCredential =
                    RequestCredential.getRequestCredential(remoteCredentialId);
            if(remoteCredential != null) {
                final  GSSCredential gssRemoteCredential =
                    remoteCredential.getDelegatedCredential();
                new Thread(new Runnable() {public void run() {
                    delegate(gssRemoteCredential,host,port);
                }}, "credentialDelegator" ).start() ;
            } else {
                new Thread(new Runnable() {public void run() {
                    delegate(null,host,port);
                }}, "credentialDelegator" ).start() ;
            }
            return;

        }

        TransferInfo info = callerIdToHandler.get(callerId);
        if (info == null) {
            _log.error("TransferInfo for callerId="+callerId+"not found");
            return;
        }

        if (message instanceof TransferCompleteMessage ) {
            TransferCompleteMessage complete =
                    (TransferCompleteMessage)message;
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
            default:
                e = new SRMException(String.format("Transfer failed: %s [%d]",
                                                   error, message.getReturnCode()));
            }
            info.callbacks.copyFailed(e);

            _log.debug("removing TransferInfo for callerId="+callerId);
            callerIdToHandler.remove(callerId);
        }
    }

    private void delegate(GSSCredential credential, String host, int port) {
        if(credential == null) {
            _log.warn("cannot delegate,  user credential is null");
            try {
                //we can not deligate so we make this fail on the door side
                Socket s = new Socket(host,port);
                OutputStream sout= s.getOutputStream();
                sout.close();
                s.close();
            } catch(IOException ioe) {
                _log.error(ioe.toString());
            }
        } else {
            try {
                String credname = credential.getName().toString();
                _log.info("SRMCell.Delegator, delegating credentials :"+
                        credential+   " to mover at "+host+
                        " listening on port "+port);

            }catch(org.ietf.jgss.GSSException gsse) {
                _log.error("invalid credentials :" + gsse.getMessage());
                try {
                    Socket s = new Socket(host,port);
                    OutputStream sout= s.getOutputStream();
                    sout.close();
                    s.close();
                } catch(IOException ioe) {
                    _log.error(ioe.toString());
                }
                return;
            }

            try {
                SslGsiSocketFactory.delegateCredential(
                        InetAddress.getByName(host),
                        port,
                        credential,false);
                _log.info("delegation appears to have succeeded");
            } catch (RuntimeException e) {
                _log.error("delegation failed", e);
            } catch (Exception e) {
                _log.error("delegation failed: " + e.getMessage());
            }
        }
    }


    private volatile StorageElementInfo storageElementInfo =
        new StorageElementInfo();

    public StorageElementInfo getStorageElementInfo(SRMUser user)
    {
        return storageElementInfo;
    }

    private StorageElementInfo getStorageElementInfo()
    {
        return storageElementInfo;
    }

    private List<String> pools = Collections.emptyList();

    private void updateStorageElementInfo()
        throws SRMException, InterruptedException
    {
        try {
            _poolMonitor =
                _poolManagerStub.sendAndWait(new PoolManagerGetPoolMonitor()).getPoolMonitor();

            String[] newPools =
                _poolMonitor.getPoolSelectionUnit().getActivePools();
            if (newPools.length != 0) {
                pools = Arrays.asList(newPools);
            } else {
                _log.info("received an empty pool list from the pool manager," +
                          "using the previous list");
            }
        } catch (TimeoutCacheException e) {
            _log.error("poolManager timeout, using previously saved pool list");
        } catch (CacheException e) {
            _log.error("poolManager returned [" + e + "]" +
                       ", using previously saved pool list");
        }

        StorageElementInfo info = new StorageElementInfo();
        for (String pool: pools) {
            try {
                StorageElementInfo poolInfo = getPoolInfo(pool);
                info.availableSpace += poolInfo.availableSpace;
                info.totalSpace += poolInfo.totalSpace;
                info.usedSpace += poolInfo.usedSpace;
            } catch (SRMException e) {
                _log.error("Cannot get info from pool [" + pool +
                           "], continuing with the rest of the pools: " +
                           e.getMessage());
            } catch (RuntimeException e) {
                _log.error("Cannot get info from pool [" + pool +
                           "], continuing with the rest of the pools", e);
            }
        }

        storageElementInfo = info;
    }

    /**
     * we use run method to update the storage info structure periodically
     */
    public void run()
    {
        try {
            while (true) {
                try {
                    updateStorageElementInfo();
                } catch(SRMException e){
                    _log.warn(e.toString());
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
     * @param user The SRMUser performing the operation; this myst be
     * of type AuthorizationRecord
     * @param surl The directory to delete
     * @return The array of directory entries or null if directoryName
     * is a symbolic link
     */
    @Override
    public List<URI> listNonLinkedDirectory(SRMUser user, URI surl)
        throws SRMException
    {
        AuthorizationRecord duser = (AuthorizationRecord) user;
        Subject subject = Subjects.getSubject(duser);

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
        } catch (FileNotFoundCacheException e) {
            throw new SRMInvalidPathException("No such file or directory", e);
        } catch (NotInTrashCacheException e) {
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
        final List<URI> result = new ArrayList<URI>();
        final String base = addTrailingSlash(surl.toString());
        Subject subject = Subjects.getSubject((AuthorizationRecord) user);
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
            _listSource.printDirectory(subject, printer, path, null, null);
            return result;
        } catch (TimeoutCacheException e) {
            throw new SRMInternalErrorException("Internal name space timeout", e);
        } catch (InterruptedException e) {
            throw new SRMInternalErrorException("List aborted by administrator", e);
        } catch (NotDirCacheException e) {
            throw new SRMInvalidPathException("Not a directory", e);
        } catch (FileNotFoundCacheException e) {
            throw new SRMInvalidPathException("No such file or directory", e);
        } catch (NotInTrashCacheException e) {
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
                      long offset, long count)
        throws SRMException
    {
        try {
            FsPath path = getPath(surl);
            Subject subject = Subjects.getSubject((AuthorizationRecord) user);
            FmdListPrinter printer =
                verbose ? new VerboseListPrinter() : new FmdListPrinter();
            _listSource.printDirectory(subject, printer, path, null,
                                       new Interval(offset, offset + count - 1));
            return printer.getResult();
        } catch (TimeoutCacheException e) {
            throw new SRMInternalErrorException("Internal name space timeout", e);
        } catch (InterruptedException e) {
            throw new SRMInternalErrorException("List aborted by administrator", e);
        } catch (NotDirCacheException e) {
            throw new SRMInvalidPathException("Not a directory", e);
        } catch (FileNotFoundCacheException e) {
            throw new SRMInvalidPathException("No such file or directory", e);
        } catch (NotInTrashCacheException e) {
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
            new ArrayList<FileMetaData>();
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
            fmd.SURL = _root.relativize(new FsPath(dir, entry.getName())).toString();
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
                lookupTokens(entry.getFileAttributes(), fmd);
            }
            return fmd;
        }

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
            switch (locality) {
            case ONLINE:
            case ONLINE_AND_NEARLINE:
                fmd.isCached = true;
            }
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
        AuthorizationRecord duser = (AuthorizationRecord) user;

        SrmReserveSpaceCompanion.reserveSpace(
                duser,
                sizeInBytes,
                spaceReservationLifetime,
                retentionPolicy,
                accessLatency,
                description,
                callbacks,
                _spaceManagerStub);
    }

    @Override
    public void srmReleaseSpace(SRMUser user,
            String spaceToken,
            Long releaseSizeInBytes, // everything is null
            SrmReleaseSpaceCallbacks callbacks) {
        long longSpaceToken;
        try {
            longSpaceToken = Long.parseLong(spaceToken);
        } catch(Exception e){
            callbacks.ReleaseSpaceFailed("invalid space token="+spaceToken);
            return;
        }

        AuthorizationRecord duser = (AuthorizationRecord) user;
        SrmReleaseSpaceCompanion.releaseSpace(duser,
                longSpaceToken,
                releaseSizeInBytes,
                callbacks,
                _spaceManagerStub);
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
        try {
            long longSpaceToken = Long.parseLong(spaceToken);
            AuthorizationRecord duser = (AuthorizationRecord) user;
            FsPath fsPath = getPath(surl);
            SrmMarkSpaceAsBeingUsedCompanion.markSpace(duser,
                                                       longSpaceToken,
                                                       fsPath.toString(),
                                                       sizeInBytes,
                                                       useLifetime,
                                                       overwrite,
                                                       callbacks,
                                                       _spaceManagerStub);
        } catch (SRMInvalidPathException e) {
            callbacks.SrmUseSpaceFailed("Invalid path: " + e.getMessage());
        } catch (NumberFormatException e){
            callbacks.SrmUseSpaceFailed("invalid space token=" + spaceToken);
        }
    }

    @Override
    public void srmUnmarkSpaceAsBeingUsed(SRMUser user,
                                          String spaceToken,
                                          URI surl,
                                          SrmCancelUseOfSpaceCallbacks callbacks)
    {
        try {
            long longSpaceToken = Long.parseLong(spaceToken);
            AuthorizationRecord duser = (AuthorizationRecord) user;
            FsPath fsPath = getPath(surl);
            SrmUnmarkSpaceAsBeingUsedCompanion.unmarkSpace(duser,
                                                           longSpaceToken,
                                                           fsPath.toString(),
                                                           callbacks,
                                                           _spaceManagerStub);
        } catch (SRMInvalidPathException e) {
            callbacks.CancelUseOfSpaceFailed("Invalid path: " + e.getMessage());
        } catch (NumberFormatException e){
            callbacks.CancelUseOfSpaceFailed("invalid space token="+spaceToken);
        }
    }

    /**
     *
     * @param spaceTokens
     * @throws org.dcache.srm.SRMException
     * @return
     */
    @Override
    public TMetaDataSpace[] srmGetSpaceMetaData(SRMUser user,
                                                String[] spaceTokens)
        throws SRMException
    {
        _log.debug("srmGetSpaceMetaData");
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

        diskCacheV111.services.space.Space[] spaces = getSpaces.getSpaces();
        tokens =  getSpaces.getSpaceTokens();
        TMetaDataSpace[] spaceMetaDatas = new TMetaDataSpace[spaces.length];
        for(int i = 0; i<spaceMetaDatas.length; ++i){
            if(spaces[i] != null) {
                diskCacheV111.services.space.Space space = spaces[i];
                spaceMetaDatas[i] = new TMetaDataSpace();
                spaceMetaDatas[i].setSpaceToken(Long.toString(space.getId()));
                long lifetime = space.getLifetime();
                int lifetimeleft;
                if ( lifetime == -1) {  // -1 corresponds to infinite lifetime
                    lifetimeleft = -1;
                    spaceMetaDatas[i].setLifetimeAssigned(Integer.valueOf(-1));
                    spaceMetaDatas[i].setLifetimeLeft(Integer.valueOf(-1));
                } else {
			lifetimeleft = (int)((space.getCreationTime()+lifetime - System.currentTimeMillis())/1000);
                    lifetimeleft= lifetimeleft < 0? 0: lifetimeleft;
                    spaceMetaDatas[i].setLifetimeAssigned(Integer.valueOf((int)(lifetime/1000)));
                    spaceMetaDatas[i].setLifetimeLeft(Integer.valueOf(lifetimeleft));
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
                    new org.apache.axis.types.UnsignedLong(
                        space.getSizeInBytes()));
                spaceMetaDatas[i].setGuaranteedSize(
                    spaceMetaDatas[i].getTotalSize());
                spaceMetaDatas[i].setUnusedSize(
                    new org.apache.axis.types.UnsignedLong(
                        space.getSizeInBytes() - space.getUsedSizeInBytes()));
                diskCacheV111.services.space.SpaceState spaceState =space.getState();
                if(diskCacheV111.services.space.SpaceState.RESERVED.equals(spaceState)) {
                    if(lifetimeleft == 0 ) {
                        spaceMetaDatas[i].setStatus(
                                new TReturnStatus(
                                TStatusCode.SRM_SPACE_LIFETIME_EXPIRED,"expired"));
                    }
                    else {
                        spaceMetaDatas[i].setStatus(
                                new TReturnStatus(TStatusCode.SRM_SUCCESS,"ok"));
                    }
                } else if(diskCacheV111.services.space.SpaceState.EXPIRED.equals(
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
     * @throws org.dcache.srm.SRMException
     * @return
     */
    @Override
    public String[] srmGetSpaceTokens(SRMUser user, String description)
        throws SRMException
    {
        AuthorizationRecord duser = (AuthorizationRecord) user;
        _log.debug("srmGetSpaceTokens ("+description+")");
        GetSpaceTokens getTokens =
            new GetSpaceTokens(duser,
                               description);
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
            Set<Long> tokens = srm.getBringOnlineRequestIds((SRMUser) user,
                    description);
            tokens.addAll(srm.getGetRequestIds((SRMUser) user,
                    description));
            tokens.addAll(srm.getPutRequestIds((SRMUser) user,
                    description));
            tokens.addAll(srm.getCopyRequestIds((SRMUser) user,
                    description));
            tokens.addAll(srm.getLsRequestIds((SRMUser) user,
                    description));
            Long[] tokenLongs = (Long[]) tokens.toArray(new Long[0]);
            String[] tokenStrings = new String[tokenLongs.length];
            for(int i=0;i<tokenLongs.length;++i) {
                tokenStrings[i] = tokenLongs[i].toString();
            }
            return tokenStrings;
        } catch (SQLException e) {
            _log.error("srmGetRequestTokens failed: " + e.getMessage());
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
     * @throws SRMAuthorizationException if the user lacks write priviledges
     *         for this path.
     * @throws SRMInvalidPathException if the file does not exist
     * @throws SRMInternalErrorException for transient errors
     * @throws SRMException for other errors
     */
    private void checkWritePrivileges(SRMUser user, URI surl)
        throws SRMException
    {
        try {
            Subject subject = Subjects.getSubject((AuthorizationRecord) user);
            FsPath path = getPath(surl);
            PnfsHandler handler = new PnfsHandler(_pnfs, subject);
            handler.getFileAttributes(path.toString(),
                                      EnumSet.noneOf(FileAttribute.class),
                                      EnumSet.of(AccessMask.WRITE_DATA));
        } catch (TimeoutCacheException e) {
            throw new SRMInternalErrorException("Internal name space timeout", e);
        } catch (NotInTrashCacheException e) {
            throw new SRMInvalidPathException("Parent path does not exist", e);
        } catch (FileNotFoundCacheException e) {
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
     * @param newLifetime SURL lifetime in seconds
     *   -1 stands for infinite lifetime
     * @return long lifetime left in seconds
     *   -1 stands for infinite lifetime
     *
     */
    public int srmExtendSurlLifetime(SRMUser user, URI surl, int newLifetime)
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
    public long srmExtendReservationLifetime(SRMUser user, String spaceToken,
                                             long newReservationLifetime)
        throws SRMException
    {
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
            throw new SRMException("Request to SrmSpaceManager got interrupted", e);
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
            extendLifetime.setSubject(Subjects.getSubject((AuthorizationRecord) user));
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
            throw new SRMException("Request to PinManager got interrupted", e);
        }
    }

    public String getStorageBackendVersion() {
        return diskCacheV111.util.Version.getVersion();
    }

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
