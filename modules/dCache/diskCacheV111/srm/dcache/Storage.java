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
import org.dcache.cells.AbstractCell;
import org.dcache.cells.Option;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellVersion;
import dmg.cells.nucleus.NoRouteToCellException;
import org.dcache.cells.CellStub;
import dmg.util.Args;
import dmg.cells.services.login.LoginBrokerInfo;
import diskCacheV111.util.PnfsHandler;
import diskCacheV111.util.PnfsId;
import diskCacheV111.util.FsPath;
import diskCacheV111.vehicles.StorageInfo;
import diskCacheV111.vehicles.PoolMgrQueryPoolsMsg;
import org.dcache.auth.AuthorizationRecord;
import org.dcache.srm.SrmCancelUseOfSpaceCallbacks;
import org.dcache.srm.SrmReleaseSpaceCallbacks;
import org.dcache.srm.SrmUseSpaceCallbacks;
import org.dcache.srm.util.Configuration;
import org.dcache.srm.security.SslGsiSocketFactory;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.DirNotExistsCacheException;
import diskCacheV111.vehicles.transferManager.
    RemoteGsiftpTransferManagerMessage;
import diskCacheV111.vehicles.transferManager.
    RemoteGsiftpDelegateUserCredentialsMessage;
import diskCacheV111.vehicles.transferManager.TransferManagerMessage;
import diskCacheV111.vehicles.transferManager.CancelTransferMessage;
import diskCacheV111.vehicles.transferManager.TransferCompleteMessage;
import diskCacheV111.vehicles.transferManager.TransferFailedMessage;
import diskCacheV111.vehicles.CopyManagerMessage;
import diskCacheV111.vehicles.PoolManagerGetPoolListMessage;
import diskCacheV111.vehicles.PinManagerExtendLifetimeMessage;
import diskCacheV111.services.space.message.ExtendLifetime;
import diskCacheV111.services.space.message.GetFileSpaceTokensMessage;
import diskCacheV111.pools.PoolCellInfo;
import diskCacheV111.pools.PoolCostInfo;
import org.globus.util.GlobusURL;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.UnknownHostException;
import java.net.Socket;
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
import org.dcache.srm.GetFileInfoCallbacks;
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

import static org.dcache.namespace.FileAttribute.*;

/**
 * SRMCell is the key class that performes communication
 * between SRM server and dcache
 * SRMCell can be run in a separate domain, on
 * a machine that has no knowledge about pnfs
 *
 * @author Timur Perelmutov
 * @author FNAL,CD/ISD
 * @version 	0.9, 20 June 2002
 */

public class Storage
        extends AbstractCell
        implements AbstractStorageElement, Runnable
{
    private final static Logger _log = LoggerFactory.getLogger(Storage.class);

    private static boolean kludgeDomainMainWasRun = false;

    /* these are the  protocols
     * that are not sutable for either put or get */
    private static final String[] SRM_PUT_NOT_SUPPORTED_PROTOCOLS
        = { "http" };
    private static final String[] SRM_GET_NOT_SUPPORTED_PROTOCOLS
        = {};
    private static final String[] SRM_PREFERED_PROTOCOLS
        = { "gsiftp", "gsidcap" };

    private String srm_root = "";
    private Args           _args;
    private String _poolManagerName;
    private String _pnfsManagerName;
    private CellPath _poolMgrPath;
    private CellStub _pnfsStub;
    private CellStub _poolManagerStub;
    private CellStub _poolStub;
    private CellStub _spaceManagerStub;
    private CellStub _copyManagerStub;
    private CellStub _gridftpTransferManagerStub;
    private CellStub _pinManagerStub;
    private CellStub _loginBrokerStub;
    private ScheduledExecutorService _executor =
        Executors.newSingleThreadScheduledExecutor();

    private PnfsHandler _pnfs;
    private PermissionHandler permissionHandler;
    private int __pnfsTimeout = 60 ;
    private String loginBrokerName="LoginBroker";
    private CellPath loginBrokerPath;
    private SRM srm;
    private int __poolManagerTimeout = 60;
    private String remoteGridftpTransferManagerName =
        "RemoteGsiftpTransferManager";
    private final Configuration config = new Configuration();
    private Thread storageInfoUpdateThread;
    private static final Object syncObj = new Object();
    private boolean ignoreClientProtocolOrder; //falseByDefault
    private boolean customGetHostByAddr; //falseByDefault

    private final FsPath _xrootdRootPath;
    private final FsPath _httpRootPath;

    private final LoginBrokerHandler _loginBrokerHandler;


    private final DirectoryListSource _listSource;

    @Option(
        name = "localSrmHosts",
        description = "comma separated list of hosts"
    )
    protected String localSrmHosts;

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



    /**
     * Creates the instance of the CRMCell
     *
     * @param  name
     *         The Name of this cell
     * @param  argString
     *         arguments
     */

    public Storage(final String name, String argString) throws Exception {

        super(name , argString );

        _log.debug("In Storage constructor, back from super constructor.");
        _log.info("Starting SRM cell named " + name);

        _args      = getArgs() ;

        __poolManagerTimeout =
            getIntOption("pool-manager-timeout", __poolManagerTimeout);
        _poolManagerName = getOption("poolManager", "PoolManager" );
        _poolMgrPath     = new CellPath( _poolManagerName ) ;
        _poolManagerStub = new CellStub();
        _poolManagerStub.setDestination(_poolManagerName);
        _poolManagerStub.setCellEndpoint(this);
        _poolManagerStub.setTimeout(__poolManagerTimeout * 1000);

        _poolStub = new CellStub();
        _poolStub.setCellEndpoint(this);
        _poolStub.setTimeout(__poolManagerTimeout * 1000);

        _spaceManagerStub = new CellStub();
        _spaceManagerStub.setCellEndpoint(this);
        _spaceManagerStub.setDestination("SrmSpaceManager");
        _spaceManagerStub.setTimeout(3 * 60 * 1000);

        _gridftpTransferManagerStub = new CellStub();
        _gridftpTransferManagerStub.setCellEndpoint(this);
        _gridftpTransferManagerStub.setDestination(remoteGridftpTransferManagerName);
        _gridftpTransferManagerStub.setTimeout(24 * 60 * 60 * 1000);

        _copyManagerStub = new CellStub();
        _copyManagerStub.setCellEndpoint(this);
        _copyManagerStub.setDestination("CopyManager");
        _copyManagerStub.setTimeout(24 * 60 * 60 * 1000);

        _pinManagerStub = new CellStub();
        _pinManagerStub.setCellEndpoint(this);
        _pinManagerStub.setDestination("PinManager");
        _pinManagerStub.setTimeout(60 * 60 * 1000);

        __pnfsTimeout = getIntOption("pnfs-timeout",__pnfsTimeout);
        _pnfsManagerName = getOption("pnfsManager" , "PnfsManager") ;
        _pnfsStub = new CellStub();
        _pnfsStub.setDestination(_pnfsManagerName);
        _pnfsStub.setCellEndpoint(this);
        _pnfsStub.setTimeout(__pnfsTimeout * 1000);
        _pnfsStub.setRetryOnNoRouteToCell(true);

        _loginBrokerStub = new CellStub();
        _loginBrokerStub.setCellEndpoint(this);
        _loginBrokerStub.setDestination(loginBrokerName);
        _loginBrokerStub.setTimeout(__pnfsTimeout * 1000);

        _pnfs = new PnfsHandler(_pnfsStub);
        permissionHandler =
            new ChainedPermissionHandler(new ACLPermissionHandler(), new PosixPermissionHandler());

        _httpRootPath = new FsPath(getOption("httpRootPath", "/"));
        _xrootdRootPath = new FsPath(getOption("xrootdRootPath", "/"));

        _listSource = new ListDirectoryHandler(_pnfs);
        addMessageListener(_listSource);

        String tmpstr = _args.getOpt("config");
        if(tmpstr != null) {
            try {
                config.read(_args.getOpt("config"));
            } catch (Exception e) {
                _log.error("can't read config from file: "+_args.getOpt("config"));
                throw e;
            }
        }

        config.setPort(getIntOption("srmport",config.getPort()));
        config.setSizeOfSingleRemoveBatch(getIntOption("size-of-single-remove-batch",config.getSizeOfSingleRemoveBatch()));
        config.setAsynchronousLs(isOptionSetToTrueOrYes("use-asynchronous-ls",config.isAsynchronousLs()));
	config.setGlue_mapfile(getOption("srmmap",config.getGlue_mapfile()));

        config.setMaxNumberOfLsEntries(getIntOption("max-number-of-ls-entries",config.getMaxNumberOfLsEntries()));
        config.setMaxNumberOfLsLevels(getIntOption("max-number-of-ls-levels",config.getMaxNumberOfLsLevels()));

        config.setKpwdfile( getOption("kpwd-file",config.getKpwdfile()) );
        config.setUseGplazmaAuthzCellFlag(isOptionSetToTrueOrYes(
            "use-gplazma-authorization-cell",
            config.getUseGplazmaAuthzCellFlag()));

        config.setDelegateToGplazmaFlag(isOptionSetToTrueOrYes(
            "delegate-to-gplazma",
            config.getDelegateToGplazmaFlag()));

        config.setUseGplazmaAuthzModuleFlag(isOptionSetToTrueOrYes(
            "use-gplazma-authorization-module",
            config.getUseGplazmaAuthzModuleFlag()));

        config.setAuthzCacheLifetime( getOption("srm-authz-cache-lifetime",
            config.getAuthzCacheLifetime()) );
        config.setGplazmaPolicy(getOption("gplazma-authorization-module-policy",
            config.getGplazmaPolicy()) );

        srm_root = getOption("pnfs-srm-path",srm_root);
        config.setSrm_root(srm_root);

        config.setProxies_directory(getOption("proxies-directory",
            config.getProxies_directory()) );

        config.setUrlcopy(getOption("url-copy-command",
            config.getUrlcopy()));

        config.setTimeout_script(getOption("timeout-command",
            config.getTimeout_script()));

        config.setTimeout(getIntOption("timout",config.getTimeout()));

        config.setBuffer_size(getIntOption("buffer_size",
            config.getBuffer_size()));

        config.setTcp_buffer_size(getIntOption("tcp_buffer_size",
            config.getTcp_buffer_size()));

        config.setParallel_streams(getIntOption("parallel_streams",
            config.getParallel_streams()));

        config.setSrmHost(getOption("srmHost",
                java.net.InetAddress.getLocalHost().getCanonicalHostName()));

        if(localSrmHosts != null &&
           localSrmHosts.trim().length() >0 ) {

            for(String aHost:localSrmHosts.split(",")) {
             config.addSrmHost(aHost);
            }
        } else {
            config.addSrmHost(config.getSrmHost());
        }
        config.setDebug(isOptionSetToTrueOrYes("debug", config.isDebug()));
        tmpstr =  _args.getOpt("usekftp");
        if(tmpstr != null && tmpstr.equalsIgnoreCase("true")) {
            config.setGsiftpclinet("kftp");
        } else {
            config.setGsiftpclinet("globus-url-copy");
        }

        config.setUseUrlcopyScript(isOptionSetToTrueOrYes("use-urlcopy-script",
            config.isUseUrlcopyScript()));

        config.setUseDcapForSrmCopy(
            isOptionSetToTrueOrYes("use-dcap-for-srm-copy",
            config.isUseDcapForSrmCopy()));

        config.setUseGsiftpForSrmCopy(
            isOptionSetToTrueOrYes("use-gsiftp-for-srm-copy",
            config.isUseGsiftpForSrmCopy()));

        config.setUseHttpForSrmCopy(isOptionSetToTrueOrYes("use-http-for-srm-copy",
            config.isUseHttpForSrmCopy()));

        config.setUseFtpForSrmCopy(isOptionSetToTrueOrYes("use-ftp-for-srm-copy",
            config.isUseFtpForSrmCopy()));

        config.setGetLifetime(getLongOption(
            "get-lifetime",config.getGetLifetime()));
        config.setBringOnlineLifetime(getLongOption(
            "bring-online-lifetime",config.getBringOnlineLifetime()));
        config.setLsLifetime(getLongOption(
                                     "ls-request-lifetime",config.getLsLifetime()));
        config.setPutLifetime(getLongOption(
            "put-lifetime",config.getPutLifetime()));
        config.setCopyLifetime(getLongOption("copy-lifetime",
            config.getCopyLifetime()));

        config.setRecursiveDirectoryCreation(isOptionSetToTrueOrYes(
            "recursive-dirs-creation",config.isRecursiveDirectoryCreation()));

        config.setAdvisoryDelete(isOptionSetToTrueOrYes("advisory-delete",
            config.isAdvisoryDelete()));

        config.setRemoveFile(isOptionSetToTrueOrYes("rm",config.isRemoveFile()));

        config.setRemoveDirectory(isOptionSetToTrueOrYes("rmdir",
            config.isRemoveDirectory()));

        config.setCreateDirectory(isOptionSetToTrueOrYes("mkdir",
            config.isCreateDirectory()));

        config.setMoveEntry(isOptionSetToTrueOrYes("mv",config.isMoveEntry()));

        config.setReserve_space_implicitely(isOptionSetToTrueOrYes(
            "reserve-space-implicitly",config.isReserve_space_implicitely()));

        config.setSpace_reservation_strict(isOptionSetToTrueOrYes(
            "space-reservation-strict",config.isSpace_reservation_strict()));


        config.setGetPriorityPolicyPlugin(getOption("get-priority-policy",
            config.getGetPriorityPolicyPlugin()));
        config.setBringOnlinePriorityPolicyPlugin(getOption("bring-online-priority-policy",
            config.getBringOnlinePriorityPolicyPlugin()));
        config.setLsPriorityPolicyPlugin(getOption("ls-request-priority-policy",
                                                   config.getLsPriorityPolicyPlugin()));
        config.setPutPriorityPolicyPlugin(getOption("put-priority-policy",
            config.getPutPriorityPolicyPlugin()));
        config.setCopyPriorityPolicyPlugin(getOption("copy-priority-policy",
            config.getCopyPriorityPolicyPlugin()));

        String jdbcPass = _args.getOpt("dbPass");
        String jdbcPwdfile = _args.getOpt("pgPass");
        if((jdbcPass==null && jdbcPwdfile==null)) {
            String error = "database parameters are not specified; use options " +
                "-jdbcUrl, -jdbcDriver, -dbUser and -dbPass/-pgPass";
            _log.error(error.toString());
            throw new Exception(error);
        }
        config.setJdbcUrl(getOption("jdbcUrl"));
        config.setJdbcClass(getOption("jdbcDriver"));
        config.setJdbcUser(getOption("dbUser"));
        config.setJdbcPass( jdbcPass);
        if(jdbcPwdfile != null && jdbcPwdfile.trim().length() > 0 ) {
            config.setJdbcPwdfile(jdbcPwdfile);
            _log.info("jdbc info : JDBC Password file:"+jdbcPwdfile);
        }

        // scheduler parameters

        config.setGetReqTQueueSize( getIntOption("get-req-thread-queue-size",
            config.getGetReqTQueueSize()));
        config.setGetThreadPoolSize(getIntOption("get-req-thread-pool-size",
            config.getGetThreadPoolSize()));
        config.setGetMaxWaitingRequests(getIntOption("get-req-max-waiting-requests",
            config.getGetMaxWaitingRequests()));
        config.setGetReadyQueueSize(getIntOption("get-req-ready-queue-size",
            config.getGetReadyQueueSize()));
        config.setGetMaxReadyJobs(getIntOption("get-req-max-ready-requests",
            config.getGetMaxReadyJobs()));
        config.setGetMaxNumOfRetries(getIntOption("get-req-max-number-of-retries",
            config.getGetMaxNumOfRetries()));
        config.setGetRetryTimeout(getLongOption("get-req-retry-timeout",
            config.getGetRetryTimeout()));
        config.setGetMaxRunningBySameOwner(
            getIntOption("get-req-max-num-of-running-by-same-owner",
            config.getGetMaxRunningBySameOwner()));

        config.setBringOnlineReqTQueueSize( getIntOption("bring-online-req-thread-queue-size",
            config.getBringOnlineReqTQueueSize()));
        config.setBringOnlineThreadPoolSize(getIntOption("bring-online-req-thread-pool-size",
            config.getBringOnlineThreadPoolSize()));
        config.setBringOnlineMaxWaitingRequests(getIntOption("bring-online-req-max-waiting-requests",
            config.getBringOnlineMaxWaitingRequests()));
        config.setBringOnlineReadyQueueSize(getIntOption("bring-online-req-ready-queue-size",
            config.getBringOnlineReadyQueueSize()));
        config.setBringOnlineMaxReadyJobs(getIntOption("bring-online-req-max-ready-requests",
            config.getBringOnlineMaxReadyJobs()));
        config.setBringOnlineMaxNumOfRetries(getIntOption("bring-online-req-max-number-of-retries",
            config.getBringOnlineMaxNumOfRetries()));
        config.setBringOnlineRetryTimeout(getLongOption("bring-online-req-retry-timeout",
            config.getBringOnlineRetryTimeout()));
        config.setBringOnlineMaxRunningBySameOwner(
            getIntOption("bring-online-req-max-num-of-running-by-same-owner",
            config.getBringOnlineMaxRunningBySameOwner()));


        config.setLsReqTQueueSize( getIntOption("ls-request-thread-queue-size",
            config.getLsReqTQueueSize()));
        config.setLsThreadPoolSize(getIntOption("ls-request-thread-pool-size",
            config.getLsThreadPoolSize()));
        config.setLsMaxWaitingRequests(getIntOption("ls-request-max-waiting-requests",
            config.getLsMaxWaitingRequests()));
        config.setLsReadyQueueSize(getIntOption("ls-request-ready-queue-size",
            config.getLsReadyQueueSize()));
        config.setLsMaxReadyJobs(getIntOption("ls-request-max-ready-requests",
            config.getLsMaxReadyJobs()));
        config.setLsMaxNumOfRetries(getIntOption("ls-request-max-number-of-retries",
            config.getLsMaxNumOfRetries()));
        config.setLsRetryTimeout(getLongOption("ls-request-retry-timeout",
            config.getLsRetryTimeout()));
        config.setLsMaxRunningBySameOwner(
            getIntOption("ls-request-max-num-of-running-by-same-owner",
            config.getLsMaxRunningBySameOwner()));


        config.setPutReqTQueueSize(getIntOption("put-req-thread-queue-size",
            config.getPutReqTQueueSize()));
        config.setPutThreadPoolSize(getIntOption("put-req-thread-pool-size",
            config.getPutThreadPoolSize()));
        config.setPutMaxWaitingRequests(getIntOption("put-req-max-waiting-requests",
            config.getPutMaxWaitingRequests()));
        config.setPutReadyQueueSize(getIntOption("put-req-ready-queue-size",
            config.getPutReadyQueueSize()));
        config.setPutMaxReadyJobs(getIntOption("put-req-max-ready-requests",
            config.getPutMaxReadyJobs()));
        config.setPutMaxNumOfRetries(getIntOption("put-req-max-number-of-retries",
            config.getPutMaxNumOfRetries()));
        config.setPutRetryTimeout(getLongOption("put-req-retry-timeout",
            config.getPutRetryTimeout()));
        config.setPutMaxRunningBySameOwner(
             getIntOption("put-req-max-num-of-running-by-same-owner",
            config.getPutMaxRunningBySameOwner()));
        config.setCopyReqTQueueSize(getIntOption("copy-req-thread-queue-size",
            config.getCopyReqTQueueSize()));
        config.setCopyThreadPoolSize(getIntOption("copy-req-thread-pool-size",
            config.getCopyThreadPoolSize()));
        config.setCopyMaxWaitingRequests(getIntOption("copy-req-max-waiting-requests",
            config.getCopyMaxWaitingRequests()));
        config.setCopyMaxNumOfRetries(getIntOption("copy-req-max-number-of-retries",
            config.getCopyMaxNumOfRetries()));
        config.setCopyRetryTimeout(getLongOption("copy-req-retry-timeout",
            config.getCopyRetryTimeout()));
        config.setCopyMaxRunningBySameOwner(
            getIntOption("copy-req-max-num-of-running-by-same-owner",
            config.getCopyMaxRunningBySameOwner()));

        config.setStorage_info_update_period( getLongOption(
            "storage-info-update-period",
            config.getStorage_info_update_period()));

        config.setVacuum(isOptionSetToTrueOrYes( "vacuum",
            config.isVacuum()));
        config.setVacuum_period_sec( getLongOption("vacuum-period",
            config.getVacuum_period_sec()));

        config.setGetRequestRestorePolicy(getOption("get-request-restore-policy",
            config.getGetRequestRestorePolicy()));
        config.setBringOnlineRequestRestorePolicy(getOption("bring-online-request-restore-policy",
            config.getBringOnlineRequestRestorePolicy()));
        config.setLsRequestRestorePolicy(getOption("ls-request-restore-policy",
            config.getLsRequestRestorePolicy()));
        config.setPutRequestRestorePolicy(getOption("put-request-restore-policy",
            config.getPutRequestRestorePolicy()));
        config.setCopyRequestRestorePolicy(getOption("copy-request-restore-policy",
            config.getCopyRequestRestorePolicy()));

        LOGINBROKERINFO_VALIDITYSPAN = getLongOption("login-broker-update-period",
            LOGINBROKERINFO_VALIDITYSPAN);

        numDoorInRanSelection = getIntOption("num-doors-in-rand-selection",
            numDoorInRanSelection);

        config.setNumDaysHistory(getIntOption("num-days-history",
            config.getNumDaysHistory()));
        config.setOldRequestRemovePeriodSecs(
            getLongOption("old-request-remove-period-secs",
            config.getOldRequestRemovePeriodSecs()));

        if( _args.getOpt("max-queued-jdbc-tasks-num") != null) {
            config.setMaxQueuedJdbcTasksNum(Integer.valueOf(getIntOption(
                "max-queued-jdbc-tasks-num")));
        }

        if( _args.getOpt("jdbc-execution-thread-num") != null) {
            config.setJdbcExecutionThreadNum(Integer.valueOf(getIntOption(
                "jdbc-execution-thread-num")));
        }

        config.setCredentialsDirectory(getOption("credentials-dir",
            config.getCredentialsDirectory()));

        config.setJdbcLogRequestHistoryInDBEnabled(isOptionSetToTrueOrYes("log-request-history-in-db-enabled",
            config.isJdbcLogRequestHistoryInDBEnabled())); // false by default

        config.setCleanPendingRequestsOnRestart(isOptionSetToTrueOrYes("clean-pending-requests-on-restart",
            config.isCleanPendingRequestsOnRestart())); // false by default

        config.setOverwrite(isOptionSetToTrueOrYes("overwrite",
            config.isOverwrite())); //false by default

        config.setOverwrite_by_default(isOptionSetToTrueOrYes("overwrite_by_default",
            config.isOverwrite_by_default())); //false by default

        customGetHostByAddr = isOptionSetToTrueOrYes("custom-get-host-by-addr",
            customGetHostByAddr);

        ignoreClientProtocolOrder = isOptionSetToTrueOrYes(
            "ignore-client-protocol-order",ignoreClientProtocolOrder);


        config.setCredentialsDirectory(getOption("credentials-dir",
            config.getCredentialsDirectory()));

		config.setQosPluginClass(getOption("qosPluginClass",config.getQosPluginClass()));
		config.setQosConfigFile(getOption("qosConfigFile",config.getQosConfigFile()));

        config.setClientDNSLookup(isOptionSetToTrueOrYes("client-dns-lookup",
        config.isClientDNSLookup())); // false by default

        config.setCounterRrdDirectory(getOption("counterRrdDirectory",config.getCounterRrdDirectory()));
        config.setGaugeRrdDirectory(getOption("gaugeRrdDirectory",config.getGaugeRrdDirectory()));

        config.setSrmCounterRrdDirectory(getOption("srmCounterRrdDirectory",config.getSrmCounterRrdDirectory()));
        config.setSrmGaugeRrdDirectory(getOption("srmGaugeRrdDirectory",config.getSrmGaugeRrdDirectory()));


        _log.info("scheduler parameter read, starting");
        this.useInterpreter(true);
        this.getNucleus().export();

        this.start();
        try {
            Thread.sleep(5000);
        } catch(InterruptedException ie) {
        }
        AuthRecordPersistenceManager authRecordPersistenceManager =
            new AuthRecordPersistenceManager(
                config.getJdbcUrl(),
                config.getJdbcClass(),
                config.getJdbcUser(),
                config.getJdbcPass());
        config.setSrmUserPersistenceManager(authRecordPersistenceManager);

        tmpstr = _args.getOpt("gsissl");
        if(tmpstr !=null) {
            config.setGsissl(tmpstr.equalsIgnoreCase("true"));
            if(config.isGsissl()) {
                config.setWebservice_protocol("https");

                config.setAuthorization(
                    DCacheAuthorization.getDCacheAuthorization(
                    config.getUseGplazmaAuthzCellFlag(),
                    config.getDelegateToGplazmaFlag(),
                    config.getUseGplazmaAuthzModuleFlag(),
                    config.getGplazmaPolicy(),
                    config.getAuthzCacheLifetime(),
                    config.getKpwdfile(),
                    this,
                    authRecordPersistenceManager));
            } else {
                config.setWebservice_protocol("http");
            }

        } else {
            config.setWebservice_protocol("http");
        }

        config.setStorage(this);

        //getNucleus().newThread( new Runnable(){
        //   public void run() {
        try {
            _log.debug("In constructor of Storage, about " +
                       "to instantiate SRM...");

            srm = SRM.getSRM(config,name);
            _log.debug("In anonymous inner class, srm instantiated.");
        } catch (Throwable t) {
            _log.warn("Aborted anonymous inner class, error starting srm", t);
            start();
            kill();
        }
        //   }
        //}
        //).start();
        _log.debug("starting storage info update  thread ...");

        storageInfoUpdateThread = getNucleus().newThread(this);
        storageInfoUpdateThread.start();

        _loginBrokerHandler = createLoginBrokerHandler();

        }

    private String getOption(String value) {
        String tmpstr = _args.getOpt(value);
        if(tmpstr == null || tmpstr.length() == 0)  {
            throw new IllegalArgumentException("option "+value+" is not set");
        }
        return tmpstr;

    }

    private String getOption(String value, String default_value) {
        String tmpstr = _args.getOpt(value);
        if(tmpstr == null || tmpstr.length() == 0)  {
            return default_value;
        }
       return tmpstr;
    }

    private boolean isOptionSetToTrueOrYes(String value) {
        String tmpstr = _args.getOpt(value);
        return tmpstr != null &&
            (tmpstr.equalsIgnoreCase("true") ||
             tmpstr.equalsIgnoreCase("on")   ||
             tmpstr.equalsIgnoreCase("yes")  ||
             tmpstr.equalsIgnoreCase("enabled") ) ;
    }

    private boolean isOptionSetToTrueOrYes(String value, boolean default_value) {
        String tmpstr = _args.getOpt(value);
       if( tmpstr != null && tmpstr.length() > 0) {
            return
             tmpstr.equalsIgnoreCase("true") ||
             tmpstr.equalsIgnoreCase("on")   ||
             tmpstr.equalsIgnoreCase("yes")  ||
             tmpstr.equalsIgnoreCase("enabled") ;
       } else {
            return default_value;
       }
    }

    private double getDoubleOption(String value) throws IllegalArgumentException {
        String tmpstr = _args.getOpt(value);
        if(tmpstr == null || tmpstr.length() == 0)  {
            throw new IllegalArgumentException("option "+value+" is not set");
        }
        return Double.parseDouble(tmpstr);
    }

    private double getDoubleOption(String value, double default_value) {
        String tmpstr = _args.getOpt(value);
        if(tmpstr == null || tmpstr.length() == 0)  {
            return default_value;
        }
        return Double.parseDouble(tmpstr);
    }

    private long getLongOption(String value) throws IllegalArgumentException {
        String tmpstr = _args.getOpt(value);
        if(tmpstr == null || tmpstr.length() == 0)  {
            throw new IllegalArgumentException("option "+value+" is not set");
        }
        return Long.parseLong(tmpstr);
    }

    private long getLongOption(String value, long default_value) {
        String tmpstr = _args.getOpt(value);
        if(tmpstr == null || tmpstr.length() == 0)  {
            return default_value;
        }
        return Long.parseLong(tmpstr);
    }

    private int getIntOption(String value) throws IllegalArgumentException {
        String tmpstr = _args.getOpt(value);
        if(tmpstr == null || tmpstr.length() == 0)  {
            throw new IllegalArgumentException("option "+value+" is not set");
        }
        return Integer.parseInt(tmpstr);
    }

    private int getIntOption(String value, int default_value) {
        String tmpstr = _args.getOpt(value);
        if(tmpstr == null || tmpstr.length() == 0)  {
            return default_value;
        }
       return Integer.parseInt(tmpstr);
    }

    /**
     * Creates a new LoginBrokerHandler based on the current cell
     * options.
     */
    private LoginBrokerHandler createLoginBrokerHandler()
        throws UnknownHostException
    {
        LoginBrokerHandler handler = new LoginBrokerHandler();

        String broker = _args.getOpt("srmLoginBroker");
        if (broker != null) {
            handler.setLoginBroker(new CellPath(broker));
        }
        handler.setProtocolFamily(getOption("protocolFamily", "SRM"));
        handler.setProtocolVersion(getOption("protocolVersion", "0.1"));
        handler.setProtocolEngine(Storage.this.getClass().getName());
        try {
            handler.setUpdateTime(getLongOption("brokerUpdateTime", 5*60));
        } catch (NumberFormatException e) {
            _log.error("Failed to parse brokerUpdateTime: " +
                       e.getMessage());
        }
        try {
            handler.setUpdateThreshold(getDoubleOption("brokerUpdateOffset", 0.1));
        } catch (NumberFormatException e) {
            _log.error("Failed to parse brokerUpdateOffset: " +
                       e.getMessage());
        }

        handler.setAddresses(Arrays.asList(InetAddress.getAllByName(InetAddress.getLocalHost().getHostName())));
        handler.setPort(config.getPort());

        handler.setLoad(new LoginBrokerHandler.LoadProvider() {
                public double getLoad() {
                    return srm.getLoad();
                }
            });
        handler.setExecutor(_executor);
        handler.setCellEndpoint(this);
        handler.start();

        addCommandListener(handler);

        return handler;
    }

    @Override
    public void getInfo( java.io.PrintWriter printWriter ) {
        StringBuilder sb = new StringBuilder();
        sb.append("SRM Cell");
        sb.append(" storage info : ");
        StorageElementInfo info = getStorageElementInfo();
        if(info != null) {
            sb.append(info.toString());
        } else {
            sb.append("  info is not yet available !!!");
        }
        sb.append('\n');
        sb.append(config.toString()).append('\n');
        try {
            srm.printGetSchedulerInfo(sb);
            srm.printPutSchedulerInfo(sb);
            srm.printCopySchedulerInfo(sb);
            srm.printBringOnlineSchedulerInfo(sb);
            srm.printLsSchedulerInfo(sb);

        } catch (java.sql.SQLException sqle) {
            sqle.printStackTrace(printWriter);
        }
        printWriter.println( sb.toString()) ;
        printWriter.println( "  LoginBroker Info :" ) ;
        _loginBrokerHandler.getInfo( printWriter ) ;
    }

    @Override
    public CellVersion getCellVersion(){
        return new CellVersion(
        diskCacheV111.util.Version.getVersion(),"$Revision$" );
    }

    public String fh_set_async_ls= " Syntax : set async ls [on|off]  # turn on/off asynchronous srmls execution ";
    public String hh_set_async_ls= " [on|off]  # turn on/off asynchronous srmls execution ";

    public String ac_set_async_ls_$_0_1(Args args) {
        boolean yes = false;
        boolean no  = false;
        for (String s : new String[] { "on", "true", "t", "yes"}) {
            if (s.equalsIgnoreCase(args.argv(0))) {
                yes = true;
                break;
            }
        }
        if (yes == false) {
            for (String s : new String[] { "off", "false", "f", "no"}) {
                if (s.equalsIgnoreCase(args.argv(0))) {
                    no = true;
                    break;
                }
            }
        }
        if (no==false && yes==false) {
            if (args.argc()==0) {
                yes = true;
            }
            else {
                return "Syntax error : "+args.argv(0)+" is unsupported value";
            }
        }
        config.setAsynchronousLs(yes);
        return "asynchronous ls is "+(config.isAsynchronousLs()?"enabled":"disabled");
    }
    public String fh_db_history_log= " Syntax: db history log [on|off] "+
        "# show status or enable db history log ";
    public String hh_db_history_log= " [on|off] " +
        "# show status or enable db history log ";
    public String ac_db_history_log_$_0_1(Args args) {
        if (args.argc()==0) {
            return "db history logging is " +(
                config.isJdbcLogRequestHistoryInDBEnabled()?
                    " enabled":
                    " disabled");
        }
        String on_off= args.argv(0);
        if(!on_off.equals("on") &&
            !on_off.equals("off")) {
            return "syntax error";
        }

        config.setJdbcLogRequestHistoryInDBEnabled(on_off.equals("on"));
        return "db history logging is " +(
                config.isJdbcLogRequestHistoryInDBEnabled()?
                    " enabled":
                    " disabled");
    }

    public String fh_db_debug_history_log= " Syntax: db debug history log [on|off] "+
        "# show status or enable db history log ";
    public String hh_db_debug_history_log= " [on|off] " +
        "# show status or enable db history log ";
    public String ac_db_debug_history_log_$_0_1(Args args) {
        if(args.argc() == 0) {
            return "db debug history logging is " +(
                config.isJdbcLogRequestHistoryInDBEnabled()?
                    " enabled":
                    " disabled");
        }
        String on_off= args.argv(0);
        if(!on_off.equals("on") &&
            !on_off.equals("off")) {
            return "syntax error";
        }

        config.setJdbcLogRequestHistoryInDBEnabled(on_off.equals("on"));
        return "db debug history logging is " +(
                config.isJdbcLogRequestHistoryInDBEnabled()?
                    " enabled":
                    " disabled");
    }

    public String fh_cancel= " Syntax: cancel <id> ";
    public String hh_cancel= " <id> ";
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

    public String fh_cancelall= " Syntax: cancel [-get] [-put] [-copy] [-bring] [-reserve] <pattern> ";
    public String hh_cancelall= " [-get] [-put] [-copy] [-bring] [-reserve] <pattern> ";
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
    public String fh_ls= " Syntax: ls [-get] [-put] [-copy] [-bring] [-reserve] [-ls] [-l] [<id>] "+
            "#will list all requests";
    public String hh_ls= " [-get] [-put] [-copy] [-bring] [-reserve] [-ls] [-l] [<id>]";
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
    public String fh_ls_queues= " Syntax: ls queues " +
        "[-get] [-put] [-copy] [-bring] [-ls] [-l]  "+
            "#will list schedule queues";
    public String hh_ls_queues= " [-get] [-put] [-copy] [-bring] [-ls] [-l] ";
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

    public String fh_ls_completed= " Syntax: ls completed [-get] [-put]" +
        " [-copy] [-l] [max_count]"+
            " #will list completed (done, failed or canceled) requests, " +
        "if max_count is not specified, it is set to 50";
    public String hh_ls_completed= " [-get] [-put] [-copy] [-l] [max_count]";
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

    public String fh_set_job_priority= " Syntax: set priority <requestId> <priority>"+
            "will set priority for the requestid";
    public String hh_set_job_priority=" <requestId> <priority>";

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


    public String fh_set_max_ready_put= " Syntax: set max ready put <count>"+
            " #will set a maximum number of put requests in the ready state";
    public String hh_set_max_ready_put= " <count>";
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

    public String fh_set_max_ready_get= " Syntax: set max ready get <count>"+
            " #will set a maximum number of get requests in the ready state";
    public String hh_set_max_ready_get= " <count>";
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

    public String fh_set_max_ready_bring_online= " Syntax: set max ready bring online <count>"+
            " #will set a maximum number of bring online requests in the ready state";
    public String hh_set_max_ready_bring_online= " <count>";
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

    public String fh_set_max_read_ls_= " Syntax: set max read ls <count>\n"+
            " #will set a maximum number of ls requests in the ready state\n"+
            " #\"set max read ls\" is an alias for \"set max ready ls\" preserved for compatibility ";
    public String hh_set_max_read_ls= " <count>";
    public String ac_set_read_ls_$_1(Args args) throws Exception{
        return ac_set_max_ready_ls_$_1(args);
    }

    public String fh_set_max_ready_ls= " Syntax: set max ready ls <count>\n"+
            " #will set a maximum number of ls requests in the ready state";
    public String hh_set_max_ready_ls= " <count>";
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

      public String fh_dir_creators_ls= " Syntax: dir creators ls [-l]  "+
         "#will list all put companion waiting for the dir creation ";
      public String hh_dir_creators_ls= " [-l] ";
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
      public String fh_cancel_dir_creation= " Syntax:cancel dir creation <path>  "+
         "#will fail companion waiting for the dir creation on <path> ";
      public String hh_cancel_dir_creation= " <path>";
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

      public String hh_print_srm_counters= "# prints the counters for all srm operations";
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

    private boolean isCached(FileAttributes attributes)
    {
         try {
             StorageInfo storage_info = attributes.getStorageInfo();
             PoolMgrQueryPoolsMsg query =
                 new PoolMgrQueryPoolsMsg(DirectionType.READ,
                                          "*/*",
                                          config.getSrmHost(),
                                          storage_info);

             _log.debug("isCached: Waiting for PoolMgrQueryPoolsMsg reply from PoolManager");
             query = _poolManagerStub.sendAndWait(query);
             Set<String> readPools = new HashSet<String>();
             for (List<String> list: query.getPools()) {
                 readPools.addAll(list);
             }

             return !Collections.disjoint(readPools, attributes.getLocations());
         } catch (CacheException e) {
            _log.warn("isCached(): error receiving message back from PoolManager : " + e);
             return false;
         } catch (InterruptedException e) {
             Thread.currentThread().interrupt();
             return false;
         }
     }

    public void log(String s)
    {
        _log.info(s.toString());
    }

    public void elog(String s)
    {
        _log.error(s.toString());
    }

    public void elog(Throwable t)
    {
        _log.error(t.toString(),t);
    }

    public void pinFile(SRMUser user,
        String fileId,
        String clientHost,
        FileMetaData fmd,
        long pinLifetime,
        long requestId,
        PinCallbacks callbacks) {
        DcacheFileMetaData dfmd = (DcacheFileMetaData) fmd;
        PinCompanion.pinFile((AuthorizationRecord)user,
            fileId,
            clientHost,
            callbacks, dfmd, pinLifetime, requestId, _pinManagerStub);
    }

    public void unPinFile(SRMUser user,String fileId,
            UnpinCallbacks callbacks,
            String pinId) {
        UnpinCompanion.unpinFile((AuthorizationRecord)user, fileId, pinId, callbacks,this);
    }


    public void unPinFileBySrmRequestId(SRMUser user,String fileId,
            UnpinCallbacks callbacks,
            long srmRequestId) {
        UnpinCompanion.unpinFileBySrmRequestId((AuthorizationRecord)user, fileId, srmRequestId, callbacks,this);
    }

    public void unPinFile(SRMUser user,String fileId,
            UnpinCallbacks callbacks) {
        UnpinCompanion.unpinFile((AuthorizationRecord)user, fileId, callbacks,this);
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


    public String getGetTurl(SRMUser user,String path,String[] protocols)
    throws SRMException {
        path = srm_root+"/"+path;
        String protocol = selectGetProtocol(protocols);
        return getTurl(path,protocol,user);
    }

    public String getGetTurl(SRMUser user,String filePath,
            String previous_turl)
            throws SRMException {
        String actualFilePath = srm_root+"/"+filePath;
        GlobusURL prev_turl;
        try {
            prev_turl= new GlobusURL(previous_turl);
        } catch (java.net.MalformedURLException e) {
            _log.warn(e.toString());
            throw new SRMException("Illegal previous turl: " + e.getMessage(),
                                   e);
        }

        String host = prev_turl.getHost();
        int port = prev_turl.getPort();
        if(port > 0) {
            host = host+":"+port;
        }
        return getTurl(actualFilePath, prev_turl.getProtocol(),host, user);
    }

    public String getPutTurl(SRMUser user,String path,String[] protocols)
    throws SRMException {
        path=srm_root+"/"+path;
        String protocol = this.selectPutProtocol(protocols);
        return getTurl(path,protocol,user);
    }


    public String getPutTurl(SRMUser user, String filePath, String previous_turl)
    throws SRMException {
        String actualFilePath = srm_root+"/"+filePath;
        GlobusURL prev_turl;
        try {
            prev_turl= new GlobusURL(previous_turl);
        } catch (java.net.MalformedURLException e) {
            throw
                new SRMException("illegal previous turl:" + e.getMessage(), e);
        }
        String host = prev_turl.getHost();
        int port = prev_turl.getPort();
        if(port > 0) {
            host = host+":"+port;
        }
        return getTurl(actualFilePath, prev_turl.getProtocol(),host, user);
    }



    private String getTurl(String path,String protocol,SRMUser user)
    throws SRMException {
        if(path == null) {
            throw new IllegalArgumentException("path is null");
        }
        if(protocol == null) {
            throw new IllegalArgumentException("protocol is null");
        }
        String transfer_path = getTurlPath(path,protocol,user);
        if(transfer_path == null) {
            throw new SRMException("cab not get transfer path");
        }
        String host = selectHost(protocol);
        if(host == null) {
            throw new SRMException(" failed to get host for "+protocol);
        }
        String turl = protocol+"://"+host+"/"+transfer_path;
        _log.debug("getTurl() returns turl="+turl);
        return turl;

    }

    private String getTurl(String path,String protocol,String host,SRMUser user)
    throws SRMException {
        if(path == null) {
            throw new IllegalArgumentException("path is null");
        }
        if(protocol == null) {
            throw new IllegalArgumentException("protocol is null");
        }
        if(host == null) {
            throw new IllegalArgumentException("host is null");
        }
        String transfer_path = getTurlPath(path,protocol,user);
        if(transfer_path == null) {
            throw new SRMException("cab not get transfer path");
        }
        String turl = protocol+"://"+host+"/"+transfer_path;
        _log.debug("getTurl() returns turl="+turl);
        return turl;
    }

    private boolean verifyUserPathIsRootSubpath(String absolutePath, SRMUser user) {

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


        absolutePath = new FsPath(absolutePath).toString();
        if(user_root!= null) {
            _log.debug("getTurl() user root is "+user_root);
            if(!absolutePath.startsWith(user_root)) {
                String error = "verifyUserPathIsInTheRoot error:"+
                        "user's path "+absolutePath+
                        " is not subpath of the user's root" +user_root;
                _log.warn(error.toString());
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

    private String getTurlPath(String path, String protocol, SRMUser user)
        throws SRMException
    {
        FsPath fullPath = new FsPath(path);
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
            transferPath = stripRootPath(userRoot, fullPath);
        } else if (protocol.equals("http") || protocol.equals("https")) {
            transferPath = stripRootPath(_httpRootPath, fullPath);
        } else if (protocol.equals("root")) {
            transferPath = stripRootPath(_xrootdRootPath, fullPath);
        } else {
            transferPath = fullPath.toString();
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

    public boolean isLocalTransferUrl(String url)
        throws SRMException
    {
        try {
            GlobusURL gurl = new GlobusURL(url);
            String protocol = gurl.getProtocol();
            String host = gurl.getHost();
            int port = gurl.getPort();
            for (LoginBrokerInfo info: getLoginBrokerInfos(protocol)) {
                if (info.getHost().equals(host) && info.getPort() == port) {
                    return true;
                }
            }
            return false;
        } catch (MalformedURLException e) {
            return false;
        }
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

        private static Map resolve(String name, String[] attrIds)
            throws NamingException {

            Map map = new HashMap();
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
                    List l = new ArrayList();
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
                Map map = resolve("dns:///" + literalip, ids);
                Set entrySet = map.entrySet();
                String host = "";
                for (Iterator itr = entrySet.iterator(); itr.hasNext();) {
                    Map.Entry e = (Map.Entry)itr.next();
                    host = (String)((ArrayList)e.getValue()).get(0);
                }
                return host;
            } catch (Exception e) {
                throw new java.net.UnknownHostException(e.getMessage());
            }
        }

    public void getFileInfo(SRMUser user, String filePath, boolean read,
        GetFileInfoCallbacks callbacks) {
        String actualPnfsPath= srm_root+"/"+filePath;
        if(!verifyUserPathIsRootSubpath(actualPnfsPath,user)) {
            callbacks.GetStorageInfoFailed("user's path ["+actualPnfsPath+
                    "] is not a subpath of user's root ");
        }
        GetFileInfoCompanion.getFileInfo(
                (AuthorizationRecord)user,
                actualPnfsPath,
                read,
                callbacks,
                _pnfsStub);
    }

    public void prepareToPut(SRMUser user,
            String filePath,
            PrepareToPutCallbacks callbacks,
            boolean overwrite) {
        String actualPnfsPath = srm_root+"/"+filePath;
        PutCompanion.PrepareToPutFile(
                (AuthorizationRecord)user,
                permissionHandler,
                actualPnfsPath,
                callbacks,
                _pnfsStub,
                config.isRecursiveDirectoryCreation(),
                overwrite);
    }

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

    public FileMetaData getFileMetaData(SRMUser user, String path, boolean read)
        throws SRMException
    {
        _log.debug("getFileMetaData(" + path + ")");
        String fullPath = getFullPath(path);
        FileAttributes attributes;
        PnfsId pnfsId;
        AuthorizationRecord duser = (AuthorizationRecord) user;
        PnfsHandler handler =
            new PnfsHandler(_pnfs, Subjects.getSubject(duser));
        try {
            Set<FileAttribute> requestedAttributes =
                EnumSet.of(TYPE, LOCATIONS);
            requestedAttributes.addAll(DcacheFileMetaData.getKnownAttributes());
            attributes = handler.getFileAttributes(fullPath, requestedAttributes, read ? EnumSet.of(AccessMask.READ_DATA) : EnumSet.noneOf(AccessMask.class));
            pnfsId = attributes.getPnfsId();
        }
        catch (TimeoutCacheException e) {
            throw new SRMInternalErrorException(e.getMessage(), e);
        }
        catch (PermissionDeniedCacheException e) {
            throw new SRMAuthorizationException(e.getMessage(),e);
        }
        catch (DirNotExistsCacheException e) {
            throw new SRMInvalidPathException(e.getMessage(),e);
        }
        catch (FileNotFoundCacheException e) {
            throw new SRMInvalidPathException(e.getMessage(),e);
        }
        catch (CacheException e) {
            throw new SRMException("could not get storage info by path: " +
                                   e.getMessage(), e);
        }
        FileMetaData fmd = new DcacheFileMetaData(attributes);
        if (attributes.getFileType() != FileType.DIR) {
            fmd.isCached = isCached(attributes);
        }
	try {
	    GetFileSpaceTokensMessage msg =
                new GetFileSpaceTokensMessage(pnfsId);
            msg = _spaceManagerStub.sendAndWait(msg);

            if (msg.getSpaceTokens() != null) {
                fmd.spaceTokens = new long[msg.getSpaceTokens().length];
                System.arraycopy(msg.getSpaceTokens(), 0,
                                 fmd.spaceTokens, 0,
                                 msg.getSpaceTokens().length);
            }
        } catch (TimeoutCacheException e) {
            _log.error("Failed to retrieve space reservation tokens for file "+
                       fullPath+" ("+pnfsId+"): SrmSpaceManager timed out");

        } catch (CacheException e) {
            if (e.getRc()!=0) {
                _log.error("Failed to retrieve space reservation tokens for file "+
                           fullPath+"("+pnfsId+"): " + e.getMessage());
            }
            else {
                if (_log.isDebugEnabled()) {
                    _log.debug("Failed to retrieve space reservation tokens for file "+
                               fullPath+"("+pnfsId+"): " + e.getMessage());
                }
            }
        } catch (RuntimeException e) {
	    _log.error("getFileMetaData failed", e);
        } catch (Exception e) {
	    _log.warn("getFileMetaData failed: " + e);
	}
        return fmd;
    }

    private final Map idToUserMap = new HashMap();
    private final Map idToCredentialMap = new HashMap();

    private String getUserById(long id) {
        _log.debug("getDcacheUserById("+id+")");
        synchronized(idToUserMap) {
            return (String) idToUserMap.get(Long.valueOf(id));

        }
    }

    private GSSCredential getCredentialById(long id) {
        _log.debug("getDcacheUserById("+id+")");
        synchronized(idToUserMap) {
            return (GSSCredential) idToCredentialMap.get(Long.valueOf(id));

        }
    }

    private static AtomicLong nextMessageID = new AtomicLong(20000);

    private static synchronized long getNextMessageID()
    {
        return nextMessageID.getAndIncrement();
    }


    public void localCopy(SRMUser user,String fromFilePath, String toFilePath)
        throws SRMException
    {
        String actualFromFilePath = getFullPath(fromFilePath);
        String actualToFilePath = getFullPath(toFilePath);
        long id = getNextMessageID();
        _log.debug("localCopy for user " + user +
                   "from actualFromFilePath to actualToFilePath");
        AuthorizationRecord duser = (AuthorizationRecord)user;
        try {
            CopyManagerMessage copyRequest =
                new CopyManagerMessage(duser.getUid(),
                                       duser.getGid(),
                                       actualFromFilePath,
                                       actualToFilePath,
                                       id,
                                       config.getBuffer_size(),
                                       config.getTcp_buffer_size());
            _copyManagerStub.sendAndWait(copyRequest);
        } catch (TimeoutCacheException e) {
            _log.error("CopyManager is unavailable");
            throw new SRMInternalErrorException("CopyManager is unavailable: " +
                                                e.getMessage(), e);
        } catch (CacheException e) {
            String msg = " local copy failed with code =" + e.getRc() +
                " details: " + e.getMessage();
            _log.warn(msg.toString());
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

    public StorageElementInfo getPoolInfo(String pool) throws SRMException
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
        } catch (InterruptedException e) {
            throw new SRMException("Request to pool was interrupted", e);
        }
    }

    public void advisoryDelete(final SRMUser user, final String path,
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

        RemoveFileCallbacks removeFileCallback;
        if (callback != null) {
            removeFileCallback = new RemoveFileCallbacks() {
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
        } else {
            removeFileCallback = new RemoveFileCallbacks() {
                    public void RemoveFileSucceeded()
                    {
                        _log.debug("advisoryDelete("+user+","+path+") succeeded");
                    }

                    public void RemoveFileFailed(String reason)
                    {
                        _log.error("advisoryDelete("+user+","+path+") failed: "+reason);
                    }

                    public void FileNotFound(String error)
                    {
                        _log.info("advisoryDelete("+user+","+path+") failed: "+error);
                    }

                    public void Exception(Exception e)
                    {
                        _log.error("advisoryDelete("+user+","+path+") Exception :"+e);
                    }

                    public void Timeout()
                    {
                        _log.error("advisoryDelete("+user+","+path+") timeout");
                    }

                    public void PermissionDenied()
                    {
                        _log.warn("advisoryDelete("+user+","+path+"): Permission denied");
                    }
                };
        }

        RemoveFileCompanion.removeFile((AuthorizationRecord) user,
                                       getFullPath(path),
                                       removeFileCallback,
                                       _pnfsStub,
                                       this);
    }

    public void removeFile(final SRMUser user,
                           final String path,
                           RemoveFileCallbacks callbacks)
    {
        _log.debug("Storage.removeFile");

        RemoveFileCompanion.removeFile((AuthorizationRecord)user,
                                       getFullPath(path),
                                       callbacks,
                                       _pnfsStub,
                                       this);
    }

    public void removeDirectory(SRMUser user, Vector tree)
        throws SRMException
    {
        _log.debug("Storage.removeDirectory");
        for (Object path: tree) {
            String actualPath = getFullPath(path.toString());
            try {
                _pnfs.deletePnfsEntry(actualPath);
            } catch (TimeoutCacheException e) {
                _log.error("Failed to delete " + actualPath + " due to timeout");
                throw new SRMInternalErrorException("Internal name space timeout while deleting " + path);
            } catch (FileNotFoundCacheException e) {
                throw new SRMException("File does not exist: " + path);
            } catch (NotInTrashCacheException e) {
                throw new SRMException("File does not exist: " + path);
            } catch (CacheException e) {
                _log.error("Failed to delete " + actualPath + ": "
                           + e.getMessage());
                throw new SRMException("Failed to delete " + path + ": "
                                       + e.getMessage());
            }
        }
    }

    public void createDirectory(SRMUser user, String directory)
        throws SRMException
    {
        _log.debug("Storage.createDirectory");

        try {
            /* We copy the mode (permissions) of the parent directory,
             * so we start by querying it. REVISIT: It's a bit strange
             * that PNFS doesn't provide an option to copy the parent
             * mode.
             */
            String path = getFullPath(directory);
            String parent = FsPath.getParent(path);
            FileAttributes attr =
                _pnfs.getFileAttributes(parent, EnumSet.of(MODE));

            /* Permission checks are performed by PnfsManager because
             * we specify a Subject for the request.
             */
            AuthorizationRecord duser = (AuthorizationRecord) user;
            PnfsHandler handler =
                new PnfsHandler(_pnfs, Subjects.getSubject(duser));
            handler.createPnfsDirectory(path,
                                        duser.getUid(), duser.getGid(),
                                        attr.getMode());
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
            _log.error("Failed to create directory " + directory + ": "
                       + e.getMessage());
            throw new SRMException(String.format("Failed to create directory [rc=%d,msg=%s]",
                                                 e.getRc(), e.getMessage()));
        }
    }

    public void moveEntry(SRMUser user, String from, String to)
        throws SRMException
    {
        Subject subject = Subjects.getSubject((AuthorizationRecord) user);
        PnfsHandler handler = new PnfsHandler(_pnfs, subject);
        String fromPath = getFullPath(from);
        String toPath = getFullPath(to);

        try {
            try {
                FileAttributes attr =
                    handler.getFileAttributes(toPath, EnumSet.of(TYPE));

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

                File fromFile = new File(fromPath);
                File toFile = new File(new File(toPath), fromFile.getName());
                toPath = toFile.toString();
                to = to + "/" + fromFile.getName();
            } catch (FileNotFoundCacheException e) {
                /* Destination name does not exist; not a problem.
                 */
            }

            handler.renameEntry(fromPath, toPath, false);
        } catch (FileNotFoundCacheException e) {
            throw new SRMInvalidPathException("No such file or directory", e);
        } catch (FileExistsCacheException e) {
            throw new SRMDuplicationException("Destination exists", e);
        } catch (NotDirCacheException e) {
            /* The parent of the target name did not exist or was not
             * a directory.
             */
            String parent = (new File(to)).getParent();
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
     * @param actualFilePath
     * @param remoteUser
     * @param remoteCredetial
     * @param callbacks
     * @throws SRMException
     * @return copy handler id
     */
    public String getFromRemoteTURL(SRMUser user,
            String remoteTURL,
            String actualFilePath,
            SRMUser remoteUser,
            Long remoteCredentialId,
            String spaceReservationId,
            long size,
            CopyCallbacks callbacks) throws SRMException{
        actualFilePath = srm_root+"/"+actualFilePath;
        _log.debug(" getFromRemoteTURL from "+remoteTURL+" to " +actualFilePath);
        return performRemoteTransfer(user,remoteTURL,actualFilePath,true,
                remoteUser,
                remoteCredentialId,
                spaceReservationId,
                Long.valueOf(size),
                callbacks);

    }

    public String getFromRemoteTURL(SRMUser user,
            String remoteTURL,
            String actualFilePath,
            SRMUser remoteUser,
            Long remoteCredentialId,
            CopyCallbacks callbacks) throws SRMException{
        actualFilePath = srm_root+"/"+actualFilePath;
        _log.debug(" getFromRemoteTURL from "+remoteTURL+" to " +actualFilePath);
        return performRemoteTransfer(user,remoteTURL,actualFilePath,true,
                remoteUser,
                remoteCredentialId,
                null,
                null,
                callbacks);

    }

    /**
     * @param user
     * @param filePath
     * @param remoteTURL
     * @param remoteUser
     * @param remoteCredetial
     * @param callbacks
     * @throws SRMException
     * @return copy handler id
     */
    public String putToRemoteTURL(SRMUser user,
            String filePath,
            String remoteTURL,
            SRMUser remoteUser,
            Long remoteCredentialId,
            CopyCallbacks callbacks)
            throws SRMException{
        String actualFilePath = srm_root+"/"+filePath;
        _log.debug(" putToRemoteTURL from "+actualFilePath+" to " +remoteTURL);
        return performRemoteTransfer(user,remoteTURL,actualFilePath,false,
                remoteUser,
                remoteCredentialId,
                null,
                null,
                callbacks);


    }

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
                                         String remoteTURL,
                                         String actualFilePath,
                                         boolean store,
                                         SRMUser remoteUser,
                                         Long remoteCredentialId,
                                         String spaceReservationId,
                                         Long size,
                                         CopyCallbacks callbacks)
        throws SRMException
    {
        _log.debug("performRemoteTransfer performing "+(store?"store":"restore"));
        if (!verifyUserPathIsRootSubpath(actualFilePath,user)) {
            throw new SRMAuthorizationException("user's path "+actualFilePath+
                                                " is not subpath of the user's root");
        }

        if (remoteTURL.startsWith("gsiftp://")) {
            AuthorizationRecord duser = (AuthorizationRecord)user;

            //call this for the sake of checking that user is reading
            // from the "root" of the user
            String path = getTurlPath(actualFilePath,"gsiftp",user);
            if (path == null) {
                throw new SRMException("user is not authorized to access path: "+
                                       actualFilePath);
            }


            RemoteGsiftpTransferManagerMessage gsiftpTransferRequest;

            // if space reservation was performed for a file of known size

            RequestCredential remoteCredential =
                RequestCredential.getRequestCredential(remoteCredentialId);
            String credentialName = "Unknown";
            if (remoteCredential != null)
            	credentialName = remoteCredential.getCredentialName();

            if (store && spaceReservationId != null && size != null) {
                gsiftpTransferRequest =
                    new RemoteGsiftpTransferManagerMessage(duser,
                                                           remoteTURL,
                                                           actualFilePath,
                                                           store,
                                                           remoteCredentialId,
                                                           credentialName,
                                                           config.getBuffer_size(),
                                                           config.getTcp_buffer_size(),
                                                           spaceReservationId,
                                                           config.isSpace_reservation_strict(),
                                                           size
                                                           );
            } else {
                gsiftpTransferRequest =
                    new RemoteGsiftpTransferManagerMessage(duser,
                                                           remoteTURL,
                                                           actualFilePath,
                                                           store,
                                                           remoteCredentialId,
                                                           credentialName,
                                                           config.getBuffer_size(),
                                                           config.getTcp_buffer_size()
                                                           );
            }
            gsiftpTransferRequest.setStreams_num(config.getParallel_streams());
            try {
                RemoteGsiftpTransferManagerMessage reply =
                    _gridftpTransferManagerStub.sendAndWait(gsiftpTransferRequest);
                long id = reply.getId();
                _log.debug("received first RemoteGsiftpTransferManagerMessage "
                           + "reply from transfer manager, id ="+id);
                GridftpTransferInfo info =
                    new GridftpTransferInfo(id, remoteCredentialId, callbacks,
                                            _gridftpTransferManagerStub.getDestinationPath());
                _log.debug("storing info for callerId = " + id);
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
        throw new SRMException("not implemented");
    }

    private final Map<Long, GridftpTransferInfo> callerIdToHandler =
        new ConcurrentHashMap<Long, GridftpTransferInfo>();

    private class TransferInfo {
        private long transferId;
        private CopyCallbacks callbacks;
        private CellPath cellPath;

        public TransferInfo(long transferId,CopyCallbacks callbacks,
            CellPath cellPath ) {
            this.transferId = transferId;
            this.callbacks = callbacks;
            this.cellPath = cellPath;
        }
    }

    private class GridftpTransferInfo extends TransferInfo {
        private Long remoteCredentialId;

        public GridftpTransferInfo(long transferId,Long remoteCredentialId,
            CopyCallbacks callbacks,CellPath cellPath) {
            super( transferId,callbacks,cellPath);
            this.remoteCredentialId = remoteCredentialId;
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
                getNucleus().newThread(new Runnable() {public void run() {
                    delegate(gssRemoteCredential,host,port);
                }}, "credentialDelegator" ).start() ;
            } else {
                getNucleus().newThread(new Runnable() {public void run() {
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
            info.callbacks.copyComplete(null,null);
            _log.debug("removing TransferInfo for callerId="+callerId);
            callerIdToHandler.remove(callerId);
        } else if(message instanceof TransferFailedMessage) {
            Object error =                 message.getErrorObject();
            if(error != null ) {
                if(error instanceof Exception) {
                    info.callbacks.copyFailed((Exception)error);
                } else {
                    info.callbacks.copyFailed(new CacheException(error.toString()));
                }

            } else {
                info.callbacks.copyFailed(new CacheException("transfer failed: "+
                    message.toString()));
            }

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

    private void updateStorageElementInfo() throws SRMException
    {
        try {
            PoolManagerGetPoolListMessage getPoolsQuery =
                _poolManagerStub.sendAndWait(new PoolManagerGetPoolListMessage());
            List<String> newPools = getPoolsQuery.getPoolList();
            if (!newPools.isEmpty()) {
                pools = newPools;
            } else {
                _log.info("receieved an empty pool list from the pool manager," +
                          "using the previous list");
            }
        } catch (TimeoutCacheException e) {
            _log.error("poolManager timeout, using previously saved pool list");
        } catch (CacheException e) {
            _log.error("poolManager returned [" + e + "]" +
                       ", using previously saved pool list");
        } catch (InterruptedException e) {
            _log.error("Request to PoolManager got interrupted");
            Thread.currentThread().interrupt();
            return;
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
    public void run() {
        while(true) {
            try {
                updateStorageElementInfo();
            } catch(SRMException srme){
                _log.warn(srme.toString());
            }
            try {
                Thread.sleep(config.getStorage_info_update_period());
            } catch(InterruptedException ie) {
                return;
            }
        }
    }

    /**
     * Provides a directory listing of directoryName if and only if
     * directoryName is not a symbolic link. As a side effect, the
     * method checks that directoryName can be deleted by the user.
     *
     * @param user The SRMUser performing the operation; this myst be
     * of type AuthorizationRecord
     * @param directoryName The directory to delete
     * @return The array of directory entries or null if directoryName
     * is a symbolic link
     */
    public String[] listNonLinkedDirectory(SRMUser user, String directoryName)
        throws SRMException
    {
        AuthorizationRecord duser = (AuthorizationRecord) user;
        Subject subject = Subjects.getSubject(duser);

        String path = getFullPath(directoryName);
        try {
            Set<FileAttribute> requestedAttributes = EnumSet.of(TYPE);
            requestedAttributes.addAll(permissionHandler.getRequiredAttributes());
            FileAttributes parentAttr =
                _pnfs.getFileAttributes(FsPath.getParent(path), requestedAttributes);
            FileAttributes childAttr =
                _pnfs.getFileAttributes(path, requestedAttributes);

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

        return listDirectory(user, directoryName, null);
    }

    public File[] listDirectoryFiles(SRMUser user, String directoryName,
                                     FileMetaData fileMetaData)
        throws SRMException
    {
        final File path = new File(getFullPath(directoryName));
        final Collection<File> result = new ArrayList<File>();
        Subject subject = Subjects.getSubject((AuthorizationRecord) user);
        DirectoryListPrinter printer =
            new DirectoryListPrinter()
            {
                public Set<FileAttribute> getRequiredAttributes()
                {
                    return EnumSet.noneOf(FileAttribute.class);
                }

                public void print(FileAttributes dirAttr, DirectoryEntry entry)
                {
                    result.add(new File(path, entry.getName()));
                }
            };

        try {
            _listSource.printDirectory(subject, printer, path, null, null);
            return result.toArray(new File[0]);
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
            _log.error("Failed to list directory " + path + ": "
                       + e.getMessage());
            throw new SRMException(String.format("List failed [rc=%d,msg=%s]",
                                                 e.getRc(), e.getMessage()));
        }
    }

    public String[] listDirectory(SRMUser user, String directoryName,
                                  FileMetaData fileMetaData)
        throws SRMException
    {
        final File path = new File(getFullPath(directoryName));
        final Collection<String> result = new ArrayList<String>();
        Subject subject = Subjects.getSubject((AuthorizationRecord) user);
        DirectoryListPrinter printer =
            new DirectoryListPrinter()
            {
                public Set<FileAttribute> getRequiredAttributes()
                {
                    return EnumSet.noneOf(FileAttribute.class);
                }

                public void print(FileAttributes dirAttr, DirectoryEntry entry)
                {
                    result.add(entry.getName());
                }
            };

        try {
            _listSource.printDirectory(subject, printer, path, null, null);
            return result.toArray(new String[0]);
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
        listDirectory(SRMUser user, String directory, final boolean verbose,
                      long offset, long count)
        throws SRMException
    {
        final Set<FileAttribute> required =
            (verbose
             ? DcacheFileMetaData.getKnownAttributes()
             : EnumSet.of(SIZE, SIMPLE_TYPE));
        if (verbose) {
            required.add(LOCATIONS);
        }
        final String prefix =
            !directory.endsWith("/") ? directory + "/" : directory;
        final List<FileMetaData> result = new ArrayList<FileMetaData>();
        DirectoryListPrinter printer =
            new DirectoryListPrinter()
            {
                public Set<FileAttribute> getRequiredAttributes()
                {
                    return required;
                }

                public void print(FileAttributes dirAttr, DirectoryEntry entry)
                {
                    FileAttributes attributes = entry.getFileAttributes();
                    DcacheFileMetaData fmd = new DcacheFileMetaData(attributes);
                    fmd.SURL = prefix + entry.getName();

                    if (verbose && attributes.getFileType() != FileType.DIR) {
                        PnfsId pnfsId = attributes.getPnfsId();
                        fmd.isCached = isCached(attributes);

                        try {
                            GetFileSpaceTokensMessage msg =
                                new GetFileSpaceTokensMessage(pnfsId);
                            msg = _spaceManagerStub.sendAndWait(msg);
                            fmd.spaceTokens = msg.getSpaceTokens();
                        } catch (TimeoutCacheException e) {
                            _log.error("Failed to retrieve space tokens for "
                                       + pnfsId + ": SrmSpaceManager timed out");
                        } catch (CacheException e) {
                            _log.error("Failed to retrieve space tokens for "
                                       + pnfsId + ": " + e.getMessage());
                        } catch (InterruptedException e) {
                            /* Propagate the interrupt to the caller.
                             */
                            Thread.currentThread().interrupt();
                        }
                    }

                    result.add(fmd);
                }
            };

        try {
            File path = new File(getFullPath(directory));
            Subject subject = Subjects.getSubject((AuthorizationRecord) user);
            _listSource.printDirectory(subject, printer, path, null,
                                       new Interval(offset, offset + count - 1));
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

    public void srmMarkSpaceAsBeingUsed(SRMUser user,
            String spaceToken,
            String fileName,
            long sizeInBytes,
            long useLifetime,
            boolean overwrite,
            SrmUseSpaceCallbacks callbacks) {
        long longSpaceToken;
        try {
            longSpaceToken = Long.parseLong(spaceToken);
        } catch(Exception e){
            callbacks.SrmUseSpaceFailed("invalid space token="+spaceToken);
            return;
        }

        AuthorizationRecord duser = (AuthorizationRecord) user;
        String actualFilePath = srm_root+"/"+fileName;
        FsPath fsPath           = new FsPath(actualFilePath);
        SrmMarkSpaceAsBeingUsedCompanion.markSpace(
                duser,
                longSpaceToken,
                fsPath.toString(),
                sizeInBytes,
                useLifetime,
                overwrite,
                callbacks,
                _spaceManagerStub);
    }

    public void srmUnmarkSpaceAsBeingUsed(
            SRMUser user,
            String spaceToken,
            String fileName,
            SrmCancelUseOfSpaceCallbacks callbacks) {
        long longSpaceToken;
        try {
            longSpaceToken = Long.parseLong(spaceToken);
        } catch(Exception e){
            callbacks.CancelUseOfSpaceFailed("invalid space token="+spaceToken);
            return;
        }
        AuthorizationRecord duser = (AuthorizationRecord) user;
        String actualFilePath = srm_root+"/"+fileName;
        FsPath fsPath           = new FsPath(actualFilePath);
        SrmUnmarkSpaceAsBeingUsedCompanion.unmarkSpace(
                duser,
                longSpaceToken,
                fsPath.toString(),
                callbacks,
                _spaceManagerStub);
    }

    /**
     *
     * @param spaceTokens
     * @throws org.dcache.srm.SRMException
     * @return
     */
    public TMetaDataSpace[] srmGetSpaceMetaData(SRMUser user,
        String[] spaceTokens)
    throws SRMException {
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
    public String[] srmGetSpaceTokens(SRMUser user, String description)
        throws SRMException
    {
        AuthorizationRecord duser = (AuthorizationRecord) user;
        _log.debug("srmGetSpaceTokens ("+description+")");
        GetSpaceTokens getTokens =
            new GetSpaceTokens(duser.getVoGroup(), duser.getVoRole(),
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
    public void checkWritePrivileges(SRMUser user, String path)
        throws SRMException
    {
        try {
            Subject subject = Subjects.getSubject((AuthorizationRecord) user);
            String fullPath = getFullPath(path);
            PnfsHandler handler = new PnfsHandler(_pnfs, subject);
            handler.getFileAttributes(fullPath,
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
    public int srmExtendSurlLifetime(SRMUser user, String fileName, int newLifetime)
        throws SRMException
    {
        checkWritePrivileges(user, fileName);
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
            PnfsId pnfsId = new PnfsId(fileId);
            PinManagerExtendLifetimeMessage extendLifetime =
                new PinManagerExtendLifetimeMessage(pnfsId, pinId,
                                                    newPinLifetime);
            extendLifetime.setAuthorizationRecord((AuthorizationRecord) user);
            extendLifetime = _pinManagerStub.sendAndWait(extendLifetime);
            return extendLifetime.getNewLifetime();
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

    public boolean exists(SRMUser user,
                          String path)  throws SRMException {
            String absolute_path = srm_root + "/" + path;
            try {
                return _pnfs.getPnfsIdByPath(absolute_path) != null;
            }
            catch (CacheException e) {
                if (e.getRc() == CacheException.FILE_NOT_FOUND ||
                    e.getRc() == CacheException.NOT_IN_TRASH) {
                    return false;
                }
                _log.warn("Failed to find file by path : "+e.getMessage());
                throw new SRMException("Failed to find file by path due to internal system failure or timeout : "+e.getMessage());
            }
            catch (RuntimeException e) {
                _log.error("Unexpected Exception ",e);
                throw new SRMException("Failed to find file by path due to internal system failure or timeout, unexpected exception thrown : "+e.getMessage());
            }
    }

    /**
     * Given a path relative to the root path, this method returns a
     * full PNFS path.
     */
    private String getFullPath(String path)
    {
        FsPath fullPath = new FsPath(srm_root + "/" + path);
        return fullPath.toString();
    }

}
