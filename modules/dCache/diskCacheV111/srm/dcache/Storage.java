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

import dmg.cells.nucleus.CellAdapter;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.ExceptionEvent;
import dmg.cells.nucleus.CellMessageAnswerable;
import dmg.cells.nucleus.CellVersion;

import dmg.util.Args;

import dmg.cells.services.login.LoginBrokerInfo;

import diskCacheV111.util.PnfsHandler;
import diskCacheV111.util.PnfsId;
import diskCacheV111.util.FsPath;

import diskCacheV111.vehicles.PnfsGetStorageInfoMessage;
import diskCacheV111.vehicles.PnfsGetFileMetaDataMessage;
import diskCacheV111.vehicles.StorageInfo;
import diskCacheV111.vehicles.PoolMgrQueryPoolsMsg;
import diskCacheV111.vehicles.PnfsSetFileMetaDataMessage;
import org.dcache.srm.SrmCancelUseOfSpaceCallbacks;
import org.dcache.srm.SrmReleaseSpaceCallbacks;
import org.dcache.srm.SrmUseSpaceCallbacks;

import org.dcache.srm.util.Configuration;
//import org.dcache.srm.util.PnfsFileId;
//import org.dcache.srm.security.DCacheAuthorization;
//import org.dcache.srm.security.DCacheUser;
import org.dcache.srm.security.SslGsiSocketFactory;
import diskCacheV111.util.CacheException;
import diskCacheV111.vehicles.PnfsDeleteEntryMessage;
import diskCacheV111.vehicles.PnfsCreateDirectoryMessage;
import diskCacheV111.vehicles.PnfsRenameMessage;
import diskCacheV111.vehicles.transferManager.
    RemoteGsiftpTransferManagerMessage;
import diskCacheV111.vehicles.transferManager.
    RemoteGsiftpDelegateUserCredentialsMessage;
import diskCacheV111.vehicles.transferManager.TransferManagerMessage;
import diskCacheV111.vehicles.transferManager.CancelTransferMessage;
import diskCacheV111.vehicles.transferManager.TransferCompleteMessage;
import diskCacheV111.vehicles.transferManager.TransferFailedMessage;
import diskCacheV111.vehicles.CopyManagerMessage;
import diskCacheV111.vehicles.PnfsFlagMessage;
import diskCacheV111.vehicles.PoolManagerGetPoolListMessage;
import diskCacheV111.vehicles.PinManagerExtendLifetimeMessage;
import diskCacheV111.services.space.message.ExtendLifetime;
import diskCacheV111.services.space.message.GetFileSpaceTokensMessage;
import diskCacheV111.pools.PoolCellInfo;
import diskCacheV111.pools.PoolCostInfo;
import org.globus.util.GlobusURL;

import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.Socket;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;


import java.util.Random;
import java.util.Vector;
import java.util.List;
import java.util.Arrays;
import java.util.Set;
import java.util.HashMap;
import java.util.Map;
import java.util.HashSet;
import java.util.Iterator;
import diskCacheV111.services.PermissionHandler;
import org.dcache.srm.AbstractStorageElement;
import org.dcache.srm.SRMException;
import org.dcache.srm.SRMDuplicationException;
import org.dcache.srm.SRMInternalErrorException;
import org.dcache.srm.SRMInvalidPathException;
import org.dcache.srm.SRMAuthorizationException;
import org.dcache.srm.FileMetaData;
import org.dcache.srm.GetFileInfoCallbacks;
import org.dcache.srm.PrepareToPutCallbacks;
import org.dcache.srm.ReleaseSpaceCallbacks;
import org.dcache.srm.ReserveSpaceCallbacks;
import org.dcache.srm.PrepareToPutInSpaceCallbacks;
import org.dcache.srm.SrmReserveSpaceCallbacks;
import org.dcache.srm.SRMUser;
import diskCacheV111.srm.StorageElementInfo;
import org.dcache.srm.AdvisoryDeleteCallbacks;
import org.dcache.srm.RemoveFileCallbacks;
import org.dcache.srm.PinCallbacks;
import org.dcache.srm.UnpinCallbacks;
import org.dcache.srm.util.Permissions;
import org.dcache.srm.CopyCallbacks;
//import org.dcache.srm.PutCompanion;
//import org.dcache.srm.AdvisoryDeleteCompanion;
import org.dcache.srm.scheduler.Job;
import org.dcache.srm.SRM;
import org.dcache.srm.BadSRMObjectException;
import org.dcache.srm.v2_2.TMetaDataSpace;
import org.dcache.srm.v2_2.TRetentionPolicyInfo;
import org.dcache.srm.v2_2.TRetentionPolicy;
import org.dcache.srm.v2_2.TAccessLatency;
import org.dcache.srm.v2_2.TStatusCode;
import org.dcache.srm.v2_2.TReturnStatus;
import diskCacheV111.util.RetentionPolicy;
import diskCacheV111.util.AccessLatency;
import diskCacheV111.services.space.message.GetSpaceMetaData;
import diskCacheV111.services.space.message.GetSpaceTokens;


import org.ietf.jgss.GSSCredential;


import org.dcache.srm.request.RequestUser;
import org.dcache.srm.request.RequestCredential;
// End of imports to pull out once we pull out srmLs stuff

// Beginning of JDOM imports.  We may not need them all.
import java.io.File;

import javax.naming.NamingException;
import javax.naming.NamingEnumeration;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;


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
        extends CellAdapter
        implements AbstractStorageElement, Runnable {
    protected int poolTimeout        = 5 * 60 ;
    public static final int TIMEOUT = 112 ;
    public static String srm_root = "";
    public static String kAuthFileName="dcache.kpwd";
    private static boolean kludgeDomainMainWasRun = false;
    
    /* these are the  protocols
     * that are not sutable for either put or get */
    public static final String[] SRM_PUT_NOT_SUPPORTED_PROTOCOLS = {
        "http"};
    
    public static final String[] SRM_GET_NOT_SUPPORTED_PROTOCOLS = {
    };
    
    public static final String[] SRM_PREFERED_PROTOCOLS =
    { "gsiftp","gsidcap"};
    
    private Args           _args;
    //    private ManagerImpl srm_manager;
    //    private SRMServerV1 srm_manager_v1;
    private String _poolManagerName;
    private String _pnfsManagerName;
    private CellPath _poolMgrPath;
    private PnfsHandler _pnfs;
    private PermissionHandler permissionHandler;
    String _hosts[];
    private CellPath _pnfsPath;
    private int __pnfsTimeout = 60 ;
    private String loginBrokerName="LoginBroker";
    private CellPath loginBrokerPath;
    private SRM srm;
    private int __poolManagerTimeout = 60;
    private String remoteGridftpTransferManagerName = 
        "RemoteGsiftpTransferManager";
    private final Configuration config = new Configuration();
    private Thread storageInfoUpdateThread;
    private static SRM srmInstance = null;
    private static final Object syncObj = new Object();
    private boolean ignoreClientProtocolOrder; //falseByDefault
    private boolean customGetHostByAddr; //falseByDefault
      
    
    private LoginBrokerHandler _loginBrokerHandler = null ;
    
    // public static SRM getSRMInstance(String xmlConfigPath)
    public static SRM getSRMInstance(final String[] dCacheParams,
            long timeout)
            throws BadSRMObjectException {
        
        System.out.println("Here are the params/args to go to dCache:\n");
        for(int i = 0 ; i < dCacheParams.length; ++i)
            System.out.println(dCacheParams[i]);
        
        System.out.println(new java.util.Date() +
                ":  entering Storage.getSRMInstance");
        if (srmInstance != null) {
            System.out.println(new java.util.Date() +
                    ":  in Storage.getSRMInstance(), about to " +
                    "return existing srmInstance");
            return srmInstance;
        } 
	else {
            // TODO:  Here is the kludge to keep from calling Domain.main
            //        twice, and therefore trying to create 2 instances
            //        of SRM.  We need a better solution than this...
            
            if (!kludgeDomainMainWasRun) {
		
                System.out.println(new java.util.Date() +
                        ":  in Storage.getSRMInstance(),  " +
                        "srmInstance is null, " +
                        "about to call Domain.main()");
                new Thread() {
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
                
                System.out.println(new java.util.Date() +
                    ":  in Storage.getSRMInstance(), " +
                    "started thread that will call " +
                    " Domain.main()");
            } else {
                System.out.println(new java.util.Date() +
                        ":  in Storage.getSRMInstance(), Domain.main has " +
                        "already been run.");
            }
        }
        long time_expired = 0;
        long wait_period = 1000;
        synchronized (syncObj) {
            while(srmInstance == null ) {
                System.out.println(new java.util.Date() +
                        " Waiting for srm initialization to complete.");
                try {
                    syncObj.wait(wait_period);
                    time_expired += wait_period;
                } catch (InterruptedException ie) {
                    throw new BadSRMObjectException(
                            "Failed to get srmInstance");
                }
                if(time_expired > timeout) {
                    throw new BadSRMObjectException(
                            "startup takes longer then timeout");
                }
            }
        }
        System.out.println(new java.util.Date().toString() +
                " about to return the instance of srm");
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
        
        super(name , Storage.class.getName(), argString , false );
        
        String tmsg = "";
        
        tmsg = "In Storage constructor, back from super constructor.";
        System.out.println(new java.util.Date() + " " + tmsg);
        
        say(tmsg);
        tmsg = "Starting SRM cell named " + name;
        System.out.println(new java.util.Date() + " " + tmsg);
        say(tmsg);
        
        _args      = getArgs() ;
        
        _poolManagerName = getOption("poolManager", "PoolManager" );
        _pnfsManagerName = getOption("pnfsManager" , "PnfsManager") ;
        _poolMgrPath     = new CellPath( _poolManagerName ) ;
        _pnfsPath = new CellPath( _pnfsManagerName );
        _pnfs = new PnfsHandler( this, _pnfsPath ) ;
        permissionHandler = new PermissionHandler(this,_pnfsPath);
        InetAddress[] addresses = InetAddress.getAllByName(
                InetAddress.getLocalHost().getHostName());
        _hosts = new String[addresses.length];
        for(int i = 0; i<addresses.length; ++i) {
            _hosts[i] = addresses[i].getHostName();
        }
        String tmpstr = _args.getOpt("config");
        if(tmpstr != null) {
            try {
                config.read(_args.getOpt("config"));
            } catch (Exception e) {
                esay("can't read config from file: "+_args.getOpt("config"));
                esay(e);
                throw e;
            }
        }
        __pnfsTimeout =getIntOption("pnfs-timeout",__pnfsTimeout);
        _pnfs.setPnfsTimeout(__pnfsTimeout*1000);
        __poolManagerTimeout =getIntOption("pool-manager-timeout",
            __poolManagerTimeout);
        
        config.setPort(getIntOption("srmport",config.getPort()));
        config.setGlue_mapfile(getOption("srmmap",config.getGlue_mapfile()));
        
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
        
        config.setSrmhost(getOption("srmhost",config.getSrmhost()));
        
        config.setStart_server(isOptionSetToTrueOrYes("start_server",
            config.isStart_server()) );
        
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
        
        config.setSaveMemory(isOptionSetToTrueOrYes("save-memory",
            config.isSaveMemory()));
        
        config.setReserve_space_implicitely(isOptionSetToTrueOrYes(
            "reserve-space-implicitly",config.isReserve_space_implicitely()));
        
        config.setSpace_reservation_strict(isOptionSetToTrueOrYes(
            "space-reservation-strict",config.isSpace_reservation_strict()));
        
        
        config.setGetPriorityPolicyPlugin(getOption("get-priority-policy",
            config.getGetPriorityPolicyPlugin()));
        config.setPutPriorityPolicyPlugin(getOption("put-priority-policy",
            config.getPutPriorityPolicyPlugin()));
        config.setCopyPriorityPolicyPlugin(getOption("copy-priority-policy",
            config.getCopyPriorityPolicyPlugin()));
        
        String jdbcPass = _args.getOpt("dbPass");
        String jdbcPwdfile = _args.getOpt("pgPass");
        if((jdbcPass==null && jdbcPwdfile==null)) {
            String error = "database parameters are not specified; use options " +
                "-jdbcUrl, -jdbcDriver, -dbUser and -dbPass/-pgPass";
            esay(error);
            throw new Exception(error);
        }
        config.setJdbcUrl(getOption("jdbcUrl"));
        config.setJdbcClass(getOption("jdbcDriver"));
        config.setJdbcUser(getOption("dbUser"));
        config.setJdbcPass( jdbcPass);
        if(jdbcPwdfile != null && jdbcPwdfile.trim().length() > 0 ) {
            config.setJdbcPwdfile(jdbcPwdfile);
             say("jdbc info : JDBC Password file:"+jdbcPwdfile);
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
        
        config.setConnect_to_wsdl(isOptionSetToTrueOrYes("connect-to-wsdl",
            config.isConnect_to_wsdl()));
        config.setStorage_info_update_period( getLongOption(
            "storage-info-update-period",
            config.getStorage_info_update_period()));
        
        config.setVacuum(isOptionSetToTrueOrYes( "vacuum",
            config.isVacuum()));        
        config.setVacuum_period_sec( getLongOption("vacuum-period",
            config.getVacuum_period_sec()));
        
        config.setGetRequestRestorePolicy(getOption("get-request-restore-policy",
            config.getGetRequestRestorePolicy()));
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
            config.setMaxQueuedJdbcTasksNum(new Integer(getIntOption(
                "max-queued-jdbc-tasks-num")));
        }

        if( _args.getOpt("jdbc-execution-thread-num") != null) {
            config.setJdbcExecutionThreadNum(new Integer(getIntOption(
                "jdbc-execution-thread-num")));
        }

        config.setCredentialsDirectory(getOption("credentials-dir",
            config.getCredentialsDirectory()));
        
        config.setJdbcMonitoringEnabled(isOptionSetToTrueOrYes("jdbc-monitoring-log",
            config.isJdbcMonitoringEnabled())); // false by default
        config.setJdbcMonitoringDebugLevel(isOptionSetToTrueOrYes("jdbc-monitoring-debug",
            config.isJdbcMonitoringDebugLevel())); // false by default
        
        config.setOverwrite(isOptionSetToTrueOrYes("overwrite",
            config.isOverwrite())); //false by default
        
        config.setOverwrite_by_default(isOptionSetToTrueOrYes("overwrite_by_default",
            config.isOverwrite_by_default())); //false by default
        
        customGetHostByAddr = isOptionSetToTrueOrYes("custom-get-host-by-addr",
            customGetHostByAddr);

        ignoreClientProtocolOrder = isOptionSetToTrueOrYes(
            "ignore-client-protocol-order",ignoreClientProtocolOrder);

        say("scheduler parameter read, starting");
        this.useInterpreter(true);
        this.getNucleus().export();
        
        _loginBrokerHandler = new LoginBrokerHandler() ;
        addCommandListener( _loginBrokerHandler ) ;
        
        this.start();
        try {
            Thread.sleep(5000);
        } catch(InterruptedException ie) {
        }
        
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
                    this));
            } else {
                config.setWebservice_protocol("http");
            }
            
        } else {
            config.setWebservice_protocol("http");
        }
        config.setStorage(this);
        
        //getNucleus().newThread( new Runnable(){
        //   public void run() {
        String ttmsg;
        try {
            ttmsg = "In constructor of Storage, about " +
                    "to instantiate SRM...";
            say(ttmsg);
            System.out.println(
                    new java.util.Date() + " " + ttmsg);
            
            srm = new SRM(config,name);
            ttmsg = "In anonymous inner class, srm instantiated.";
            say(ttmsg);
            System.out.println(
                    new java.util.Date() + " " + ttmsg);
        } catch (Throwable t) {
            ttmsg =
                    "Aborted anonymous inner class, error starting srm";
            esay(ttmsg);
            esay(t);
            System.out.println(
                    new java.util.Date() + " " + ttmsg + " "
                    + t);
            start();
            kill();
        }
        //   }
        //}
        //).start();
        tmsg = "starting storage info update  thread ...";
        say(tmsg);
        System.out.println(new java.util.Date() + " " + tmsg);
        
        storageInfoUpdateThread = getNucleus().newThread(this);
        storageInfoUpdateThread.start();
        
        tmsg = "In Storage constructor, about to get/set srmInstance.";
        say(tmsg);
        System.out.println(new java.util.Date() + " " + tmsg);
        
        synchronized(syncObj) {
            srmInstance = srm;
            System.out.println("srmInstance is not null, srmInstance="+srmInstance);
            syncObj.notifyAll();
        }
        
        tmsg =
                "srmInstance was set, about to exit Storage constructor.";
        say(tmsg);
        System.out.println(new java.util.Date() + " " + tmsg);
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
    
    public void getInfo( java.io.PrintWriter printWriter ) {
        StringBuffer sb = new StringBuffer();
        sb.append("SRM Cell");
        sb.append(" storage info : ");
        try {
            StorageElementInfo info = getStorageElementInfo();
            if(info != null) {
                sb.append(info.toString());
            } else {
                sb.append("  info is not yet available !!!");
            }
        } catch(SRMException srme) {
            sb.append("cannot get storage info :");
            sb.append(srme.getMessage());
        }
        sb.append('\n');
        sb.append(config.toString()).append('\n');
        try {
            srm.printGetSchedulerInfo(sb);
            srm.printPutSchedulerInfo(sb);
            srm.printCopySchedulerInfo(sb);
        } catch (java.sql.SQLException sqle) {
            sqle.printStackTrace(printWriter);
        }
        printWriter.println( sb.toString()) ;
        if( _loginBrokerHandler != null ){
            printWriter.println( "  LoginBroker Info :" ) ;
            _loginBrokerHandler.getInfo( printWriter ) ;
        }
    }
    
    public CellVersion getCellVersion(){ 
        return new CellVersion(
        diskCacheV111.util.Version.getVersion(),"$Revision: 1.151 $" ); 
    }
    
    public String fh_db_history_log= " Syntax: db history log [on|off] "+
        "# show status or enable db history log ";
    public String hh_db_history_log= " [on|off] " +
        "# show status or enable db history log ";
    public String ac_db_history_log_$_0_1(Args args) {
        if(args.argc() == 0) {
            return "db history logging is " +(
                config.isJdbcMonitoringEnabled()?
                    " enabled":
                    " disabled");
        }
        String on_off= args.argv(0);
        if(!on_off.equals("on") && 
            !on_off.equals("off")) {
            return "syntax error";
        }
        
        config.setJdbcMonitoringEnabled(on_off.equals("on"));
        return "db history logging is " +(
                config.isJdbcMonitoringEnabled()?
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
                config.isJdbcMonitoringDebugLevel()?
                    " enabled":
                    " disabled");
        }
        String on_off= args.argv(0);
        if(!on_off.equals("on") && 
            !on_off.equals("off")) {
            return "syntax error";
        }
        
        config.setJdbcMonitoringDebugLevel(on_off.equals("on"));
        return "db debug history logging is " +(
                config.isJdbcMonitoringDebugLevel()?
                    " enabled":
                    " disabled");
    }

    public String fh_cancel= " Syntax: cancel <id> ";
    public String hh_cancel= " <id> ";
    public String ac_cancel_$_1(Args args) {
        try {
            Long id = new Long(args.argv(0));
            StringBuffer sb = new StringBuffer();
            srm.cancelRequest(sb, id);
            return sb.toString();
        }catch (Exception e) {
            esay(e);
            return e.toString();
        }
    }
    
    public String fh_cancelall= " Syntax: cancel [-get] [-put] [-copy] <pattern> ";
    public String hh_cancelall= " [-get] [-put] [-copy] <pattern> ";
    public String ac_cancelall_$_1(Args args) {
        try {
            boolean get=args.getOpt("get") != null;
            boolean put=args.getOpt("put") != null;
            boolean copy=args.getOpt("copy") != null;
            if( !get && !put && !copy ) {
                get=true;
                put=true;
                copy=true;
                
            }
            String pattern = args.argv(0);
            StringBuffer sb = new StringBuffer();
            if(get) {
                say("calling srm.cancelAllGetRequest(\""+pattern+"\")");
                srm.cancelAllGetRequest(sb, pattern);
            }
            if(put) {
                say("calling srm.cancelAllPutRequest(\""+pattern+"\")");
                srm.cancelAllPutRequest(sb, pattern);
            }
            if(copy) {
                say("calling srm.cancelAllCopyRequest(\""+pattern+"\")");
                srm.cancelAllCopyRequest(sb, pattern);
            }
            return sb.toString();
        }catch (Exception e) {
            esay(e);
            return e.toString();
        }
    }
    public String fh_ls= " Syntax: ls [-get] [-put] [-copy] [-bring] [-l] [<id>] "+
            "#will list all requests";
    public String hh_ls= " [-get] [-put] [-copy] [-bring] [-l] [<id>]";
    public String ac_ls_$_0_1(Args args) {
        try {
            boolean get=args.getOpt("get") != null;
            boolean put=args.getOpt("put") != null;
            boolean copy=args.getOpt("copy") != null;
            boolean bring=args.getOpt("bring") != null;
            boolean longformat = args.getOpt("l") != null;
            StringBuffer sb = new StringBuffer();
            if(args.argc() == 1) {
                try {
                    Long reqId = new Long(args.argv(0));
                    srm.listRequest(sb, reqId, longformat);
                } catch( NumberFormatException nfe) {
                    return "id must be an integer, you gave id="+args.argv(0);
                }
            } else {
                if( !get && !put && !copy && !bring) {
                    get=true;
                    put=true;
                    copy=true;
                    bring=true;
                    
                }
                if(get) {
                    sb.append("Get Requests:\n");
                    srm.listGetRequests(sb);
                    sb.append('\n');
                }
                if(put) {
                    sb.append("Put Requests:\n");
                    srm.listPutRequests(sb);
                    sb.append('\n');
                }
                if(copy) {
                    sb.append("Copy Requests:\n");
                    srm.listCopyRequests(sb);
                    sb.append('\n');
                }
                if(copy) {
                    sb.append("Bring Online Requests:\n");
                    srm.listBringOnlineRequests(sb);
                    sb.append('\n');
                }
            }
            return sb.toString();
        } catch(Throwable t) {
            t.printStackTrace();
            return t.toString();
        }
    }
    public String fh_ls_queues= " Syntax: ls queues " +
        "[-get] [-put] [-copy] [-bring] [-l]  "+
            "#will list schedule queues";
    public String hh_ls_queues= " [-get] [-put] [-copy] [-bring] [-l] ";
    public String ac_ls_queues_$_0(Args args) {
        try {
            boolean get=args.getOpt("get") != null;
            boolean put=args.getOpt("put") != null;
            boolean copy=args.getOpt("copy") != null;
            boolean bring=args.getOpt("bring") != null;
            boolean longformat = args.getOpt("l") != null;
            StringBuffer sb = new StringBuffer();
            
            if( !get && !put && !copy && !bring ) {
                get=true;
                put=true;
                copy=true;
                bring=true;
                
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
        StringBuffer sb = new StringBuffer();
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
    
    public String fh_reserve= " This is a function for testing space reservation\n"+
            " it will be removed when space reservation client becomes available"+
            " Syntax: reserve <voGroup> <voRole> <size> <lifetime> <accessLatency>" +
            " <retentionPolicy> <description> ";
    public String hh_reserve= " <voGroup> <voRole> <size> <lifetime> <accessLatency>" +
            " <retentionPolicy> <description> ";
    public String ac_reserve_$_7(Args args) {
        String voGroup=args.argv(0);
        String voRole = args.argv(1);
        DCacheUser duser = new DCacheUser("timur",voGroup,
                voRole,
                "/pnfs/fnal.gov/usr/cms/WAX",
                10401,
                1530);
        long sizeInBytes = Long.parseLong(args.argv(2));
        long reservationLifetime = Long.parseLong(args.argv(3));
        String accessLatency = args.argv(4);
        String retentionPolicy = args.argv(5);
        String description = args.argv(6);
        srmReserveSpace(duser,
                sizeInBytes,
                reservationLifetime,
                retentionPolicy,
                accessLatency,
                description,
                new org.dcache.srm.SrmReserveSpaceCallbacks(){
            public void ReserveSpaceFailed(String reason){
                esay("admin command SrmReserveSpace failed: "+reason);
                pin("admin command SrmReserveSpace failed: "+reason);
            }
            
            public void NoFreeSpace(String reason){
                esay("admin command SrmReserveSpace failed: NoFreeSpace: "+reason);
                pin("admin command SrmReserveSpace failed: NoFreeSpace:"+reason);
            }
            
            public void SpaceReserved(String spaceReservationToken,
                long reservedSpaceSize){
                esay("admin command SrmReserveSpace succeded:");
                esay("token ="+spaceReservationToken+" reservationSize="+
                    reservedSpaceSize);
                pin("admin command SrmReserveSpace succeded:");
                pin("token ="+spaceReservationToken+" reservationSize="+
                    reservedSpaceSize);
                
            }
            
            public void ReserveSpaceFailed(Exception e){
                esay("admin command SrmReserveSpace failed: ");
                esay(e);
                pin("admin command SrmReserveSpace failed: ");
                pin(e.toString());
            }
            
        });
        return " request submitted, watch logs and pins";
    }
    
    public String fh_set_job_priority= " Syntax: set priority <requestId> <priority>"+
            "will set priority for the requestid";
    public String hh_set_job_priority=" <requestId> <priority>";
    
    public String ac_set_job_priority_$_2(Args args) {
        StringBuffer sb = new StringBuffer();
        String s1 = args.argv(0);
        String s2 = args.argv(1);
        long requestId;
        int priority;
        try {
            requestId = Integer.parseInt(s1);
        } catch (Exception e) {
            esay("Failed to parse request id "+s1);
            esay(e);
            sb.append("Failed to parse request id "+s1+"\n");
            sb.append(e.getMessage());
            return sb.toString();
        }
        try {
            priority = Integer.parseInt(s2);
        } catch (Exception e) {
            esay("Failed to parse priority  "+s2);
            esay(e);
            sb.append("Failed to parse priority "+s2+"\n");
            sb.append(e.getMessage());
            return sb.toString();
        }
        try {
            Job job = Job.getJob(requestId);
            if(job == null ) {
                sb.append("request with reqiest id "+requestId+" is not found\n");
                return sb.toString();
            }
            job.getCreator().setPriority(priority);
            job.setPriority(priority);
            srm.listRequest(sb, requestId, true);
            return sb.toString();
        } catch(Throwable t) {
            t.printStackTrace();
            return t.toString();
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
        say("put-req-max-ready-requests="+value);
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
        say("get-req-max-ready-requests="+value);
        return "get-req-max-ready-requests="+value;
    }
    
      public String fh_dir_creators_ls= " Syntax: dir creators ls [-l]  "+
         "#will list all put companion waiting for the dir creation ";
      public String hh_dir_creators_ls= " [-l] ";
      public String ac_dir_creators_ls_$_0(Args args) {
        try {
            boolean longformat = args.getOpt("l") != null;         
            StringBuffer sb = new StringBuffer();   
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
            StringBuffer sb = new StringBuffer();   
            PutCompanion.failCreatorsForPath(pnfsPath,sb);
            return sb.toString();
         } catch(Throwable t) {
            t.printStackTrace();
            return t.toString();
         }
      }
        /*
        public String fh_rc_running_ls= " Syntax: rc running ls "+
         "[-get] [-put] [-copy] [-l]"+
        " #will list running requests";
        public String hh_rc_running_ls= " [-get] [-put] [-copy] [-l]";
        public String ac_rc_running_ls(Args args) {
            boolean get=args.getOpt("get") != null;
            boolean put=args.getOpt("put") != null;
            boolean copy=args.getOpt("copy") != null;
            boolean longformat = args.getOpt("l") != null;
            if( (!get && !put && !copy ) ||
            get && put && copy) {
                return null;//RequestScheduler.listAllRunningRequests(longformat);
            }
            StringBuffer sb = new StringBuffer();
            if(get) {
                //Scheduler scheduler = srm.getGetRequestScheduler();
                //sb.append(scheduler.listRunningRequests(longformat));
                sb.append('\n');
            }
            if(put) {
                //Scheduler scheduler = srm.getPutRequestScheduler();
                //sb.append(scheduler.listRunningRequests(longformat));
                sb.append('\n');
            }
            if(copy) {
                //Scheduler scheduler = srm.getCopyRequestScheduler();
                //sb.append(scheduler.listRunningRequests(longformat));
                sb.append('\n');
            }
            return sb.toString();
        }
         
        public String fh_rc_queued_ls= " Syntax: rc queued ls "+
         "[-get] [-put] [-copy] [-l]"+
        "#will list queued requests";
        public String hh_rc_queued_ls= " [-get] [-put] [-copy] [-l]";
        public String ac_rc_queued_ls(Args args) {
            boolean get=args.getOpt("get") != null;
            boolean put=args.getOpt("put") != null;
            boolean copy=args.getOpt("copy") != null;
            boolean longformat = args.getOpt("l") != null;
            if( (!get && !put && !copy ) ||
            get && put && copy) {
                return null;//Scheduler.listAllQueuedRequests(longformat);
            }
            StringBuffer sb = new StringBuffer();
            if(get) {
                //Scheduler scheduler = srm.getGetRequestScheduler();
                //sb.append(scheduler.listQueuedRequests(longformat));
                sb.append('\n');
            }
            if(put) {
                //Scheduler scheduler = srm.getPutRequestScheduler();
                //sb.append(scheduler.listQueuedRequests(longformat));
                sb.append('\n');
            }
            if(copy) {
                //Scheduler scheduler = srm.getCopyRequestScheduler();
                //sb.append(scheduler.listQueuedRequests(longformat));
                sb.append('\n');
            }
            return sb.toString();
        }
         
        public String fh_rc_pending_ls= " Syntax: rc pending ls "+
         "[-get] [-put] [-copy] [-l]"+
        "#will list pending requests";
        public String hh_rc_pending_ls= " [-get] [-put] [-copy] [-l]";
        public String ac_rc_pending_ls(Args args) {
            boolean get=args.getOpt("get") != null;
            boolean put=args.getOpt("put") != null;
            boolean copy=args.getOpt("copy") != null;
            boolean longformat = args.getOpt("l") != null;
            if( (!get && !put && !copy ) ||
            get && put && copy) {
                return null;//RequestScheduler.listAllPendingRequests(longformat);
            }
            StringBuffer sb = new StringBuffer();
            if(get) {
                //Scheduler scheduler = srm.getGetRequestScheduler();
                //sb.append(scheduler.listPendingRequests(longformat));
                sb.append('\n');
            }
            if(put) {
                //Scheduler scheduler = srm.getPutRequestScheduler();
                //sb.append(scheduler.listPendingRequests(longformat));
                sb.append('\n');
            }
            if(copy) {
                //Scheduler scheduler = srm.getCopyRequestScheduler();
                //sb.append(scheduler.listPendingRequests(longformat));
                sb.append('\n');
            }
            return sb.toString();
        }
         
        public String fh_rc_ready_ls= " Syntax: rc ready ls "+
         "[-get] [-put] [-copy] [-l]"+
        " #will list ready requests";
        public String hh_rc_ready_ls= " [-get] [-put] [-copy] [-l]";
        public String ac_rc_ready_ls(Args args) {
            boolean get=args.getOpt("get") != null;
            boolean put=args.getOpt("put") != null;
            boolean copy=args.getOpt("copy") != null;
            boolean longformat = args.getOpt("l") != null;
            if( (!get && !put && !copy ) ||
            get && put && copy) {
                return null;//RequestScheduler.listAllReadyRequests();
            }
            StringBuffer sb = new StringBuffer();
            if(get) {
                //Scheduler scheduler = srm.getGetRequestScheduler();
                //sb.append(scheduler.listReadyRequests(longformat));
                sb.append('\n');
            }
            if(put) {
                //Scheduler scheduler = srm.getPutRequestScheduler();
                //sb.append(scheduler.listReadyRequests(longformat));
                sb.append('\n');
            }
            if(copy) {
                //Scheduler scheduler = srm.getCopyRequestScheduler();
                //sb.append(scheduler.listReadyRequests(longformat));
                sb.append('\n');
            }
            return sb.toString();
        }
         
        public String fh_rc_failed_ls= " Syntax: rc failed ls "+
         "[-get] [-put] [-copy] [-l]"+
        "#will list failed requests";
        public String hh_rc_failed_ls= " [-get] [-put] [-copy] [-l]";
        public String ac_rc_failed_ls(Args args) {
            boolean get=args.getOpt("get") != null;
            boolean put=args.getOpt("put") != null;
            boolean copy=args.getOpt("copy") != null;
            boolean longformat = args.getOpt("l") != null;
            if( (!get && !put && !copy ) ||
            get && put && copy) {
                return null;//RequestScheduler.listAllFailedRequests(longformat);
            }
            StringBuffer sb = new StringBuffer();
            if(get) {
                //Scheduler scheduler = srm.getGetRequestScheduler();
                //sb.append(scheduler.listFailedRequests(longformat));
                sb.append('\n');
            }
            if(put) {
                //Scheduler scheduler = srm.getPutRequestScheduler();
                //sb.append(scheduler.listFailedRequests(longformat));
                sb.append('\n');
            }
            if(copy) {
                //Scheduler scheduler = srm.getCopyRequestScheduler();
                //sb.append(scheduler.listFailedRequests(longformat));
                sb.append('\n');
            }
            return sb.toString();
        }
         
        public String fh_rc_done_ls= " Syntax: rc done ls "+
         "[-get] [-put] [-copy] [-l]"+
        " #will list done requests";
        public String hh_rc_done_ls= " [-get] [-put] [-copy] [-l]";
        public String ac_rc_done_ls(Args args) {
            boolean get=args.getOpt("get") != null;
            boolean put=args.getOpt("put") != null;
            boolean copy=args.getOpt("copy") != null;
            boolean longformat = args.getOpt("l") != null;
            if( (!get && !put && !copy ) ||
            get && put && copy) {
                return null;//Scheduler.listAllDoneRequests(longformat);
            }
            StringBuffer sb = new StringBuffer();
            if(get) {
                //Scheduler scheduler = srm.getGetRequestScheduler();
                //sb.append(scheduler.listDoneRequests(longformat));
                sb.append('\n');
            }
            if(put) {
                //Scheduler scheduler = srm.getPutRequestScheduler();
                //sb.append(scheduler.listDoneRequests(longformat));
                sb.append('\n');
            }
            if(copy) {
                //Scheduler scheduler = srm.getCopyRequestScheduler();
                //sb.append(scheduler.listDoneRequests(longformat));
                sb.append('\n');
            }
            return sb.toString();
        }
         
        public String hh_set_max_running_get_requests = "<max-get-requests>";
        public String ac_set_max_running_get_requests_$_1(Args args) {
            int req_num = Integer.parseInt(args.argv(0)) ;
            if(req_num <= 0) {
                return "Error, maximum number of active get requests "+
                  " should be greater then 0 ";
            }
        //RequestScheduler sheduler = srm.getGetRequestScheduler();
        //sheduler.setmax_active_requests(req_num);
        //config.setMaxActiveGet(req_num);
            return "set max number of active get requests to "+req_num;
        }
         
        public String hh_set_max_running_put_requests = "<max-put-requests>";
        public String ac_set_max_running_put_requests_$_1(Args args) {
            int req_num = Integer.parseInt(args.argv(0)) ;
            if(req_num <= 0) {
                return "Error, maximum number of active put requests "+
                   "should be greater then 0 ";
            }
            //Scheduler sheduler = srm.getPutRequestScheduler();
            //sheduler.setmax_active_requests(req_num);
           // config.setMaxActivePut(req_num);
            return "set max number of active put requests to "+req_num;
        }
         
        public String hh_set_max_running_copy_requests = "<max-copy-requests>";
        public String ac_set_max_running_copy_requests_$_1(Args args) {
            int req_num = Integer.parseInt(args.argv(0)) ;
            if(req_num <= 0) {
                return "Error, maximum number of active copy requests should "+
         " be greater then 0 ";
            }
            //Scheduler sheduler = srm.getCopyRequestScheduler();
            //sheduler.setmax_active_requests(req_num);
            //config.setMaxActiveCopy(req_num);
            return "set max number of active copy requests to "+req_num;
        }
         
        public String hh_set_max_done_get_requests =
        "<max-done-get-requests> #set the maximum number "+
        "of completed request to be stored for reference purposes";
        public String ac_set_max_done_get_requests_$_1(Args args) {
            int req_num = Integer.parseInt(args.argv(0)) ;
            if(req_num <= 0) {
                return "Error, maximum number of done get requests should be"+
         " greater then 0 ";
            }
        //RequestScheduler sheduler = srm.getGetRequestScheduler();
        //sheduler.setMax_done_reqs(req_num);
        //config.setMaxDoneGet(req_num);
            return "set max number of done get requests to "+req_num;
        }
         
        public String hh_set_max_done_put_requests =
        "<max-done-put-requests> #set the maximum number "+
        "of completed request to be stored for reference purposes";
        public String ac_set_max_done_put_requests_$_1(Args args) {
            int req_num = Integer.parseInt(args.argv(0)) ;
            if(req_num <= 0) {
                return "Error, maximum number of done put requests should be"+
         " greater then 0 ";
            }
            //Scheduler sheduler = srm.getPutRequestScheduler();
            //sheduler.setMax_done_reqs(req_num);
            //config.setMaxDonePut(req_num);
            return "set max number of done put requests to "+req_num;
        }
         
        public String hh_set_max_done_copy_requests =
        "<max-done-copy-requests> #set the maximum number "+
        "of completed request to be stored for reference purposes";
        public String ac_set_max_done_copy_requests_$_1(Args args) {
            int req_num = Integer.parseInt(args.argv(0)) ;
            if(req_num <= 0) {
                return "Error, maximum number of done copy requests should be "+
         "greater then 0 ";
            }
            //Scheduler sheduler = srm.getCopyRequestScheduler();
            //sheduler.setMax_done_reqs(req_num);
            //config.setMaxDoneCopy(req_num);
            return "set max number of done copy requests to "+req_num;
        }
         
        public String hh_set_get_lifetime =
        "<get-request-lifetime (sec)> #set the lifetime "+
        "of get request";
        public String ac_set_get_lifetime_$_1(Args args) {
            long lifetime = Long.parseLong(args.argv(0)) ;
            if(lifetime <= 0) {
                return "Error, lifetime should be greater then 0 ";
            }
        //equestScheduler sheduler = srm.getGetRequestScheduler();
        //sheduler.setLifetime(lifetime);
        //config.setGetLifetime(lifetime);
            return "set lifetime of future get requests to "+lifetime+" seconds";
        }
         
        public String hh_set_put_lifetime =
        "<put-request-lifetime (sec)> #set the lifetime "+
        "of put request";
        public String ac_set_put_lifetime_$_1(Args args) {
            long lifetime = Long.parseLong(args.argv(0)) ;
            if(lifetime <= 0) {
                return "Error, lifetime should be greater then 0 ";
            }
            //Scheduler sheduler = srm.getPutRequestScheduler();
            //sheduler.setLifetime(lifetime);
            config.setPutLifetime(lifetime);
            return "set lifetime of future put requests to "+lifetime+" seconds";
        }
         
        public String hh_set_copy_lifetime =
        "<copy-request-lifetime (sec)> #set the lifetime "+
        "of copy request";
        public String ac_set_copy_lifetime_$_1(Args args) {
            long lifetime = Long.parseLong(args.argv(0)) ;
            if(lifetime <= 0) {
                return "Error, lifetime should be greater then 0 ";
            }
            //Scheduler sheduler = srm.getCopyRequestScheduler();
            //sheduler.setLifetime(lifetime);
            config.setCopyLifetime(lifetime);
            return "set lifetime of future copy requests to "+lifetime+" seconds";
        }
         
        public String hh_set_gsiftp_streams = "<num-of-gsiftp-parallel-streams>";
        public String ac_set_gsiftp_streams_$_1(Args args) {
            int streams = Integer.parseInt(args.argv(0)) ;
            if(streams <= 0) {
                return
                "Error, number of parallel gsiftp streams should be greater then 0 ";
            }
            config.setParallel_streams(streams);
            return "set number of parallel gsiftp streams to "+streams;
        }
         *
         */
    
    
    /**
     * Receives the Cell Messages
     * we currently process messages received as the
     * responce to something we sent
     * to other cells
     *
     * @param  cellMessage
     *         cellMessage Object containing the actual message
     *
     */
    
    public void messageArrived( CellMessage cellMessage ) {
        final Object o = cellMessage.getMessageObject();
            /*if(o instanceof RemoteGsiftpDelegateUserCredentialsMessage ) {
                say( " RemoteGsiftpDelegateUserCredentialsMessage From   : "+
                cellMessage.getSourceAddress() ) ;
                RemoteGsiftpDelegateUserCredentialsMessage delegate =
                    (RemoteGsiftpDelegateUserCredentialsMessage)o;
                 final String host = delegate.getHost();
                 final int port = delegate.getPort();
                 final  GSSCredential remoteCredential = getCredentialById(
                  delegate.getId());
                 new Thread( new Runnable() {public void run()
                 {
                     delegate(remoteCredential,host,port);
                 }}).start();
             
                return;
            }
            else*/
        if(o instanceof TransferManagerMessage){
            diskCacheV111.util.ThreadManager.execute(new Runnable() {
                public void run() {
                    handleTransferManagerMessage((TransferManagerMessage)o);
                }
            });
        }
        
        super.messageArrived(cellMessage);
    }
    
    public void exceptionArrived(ExceptionEvent ee) {
        say("Exception Arrived: "+ee);
        super.exceptionArrived(ee);
    }

    
    private boolean isCached(StorageInfo storage_info, PnfsId _pnfsId) {
        PoolMgrQueryPoolsMsg query =
                new PoolMgrQueryPoolsMsg( "read" ,
                      storage_info.getStorageClass()+"@"+storage_info.getHsm() ,
                      storage_info.getCacheClass(),
                      "*/*",
                      config.getSrmhost(),
                      null);
	
        CellMessage checkMessage = new CellMessage( _poolMgrPath , query ) ;
        say("isCached: Waiting for PoolMgrQueryPoolsMsg reply from PoolManager");
        try {
            checkMessage = sendAndWait(  checkMessage , __poolManagerTimeout*1000 ) ;
            if(checkMessage == null) {
                esay("isCached(): timeout expired");
                return false;
            }
            query = (PoolMgrQueryPoolsMsg) checkMessage.getMessageObject() ;
        } 
	catch(Exception ee ) {
            esay("isCached(): error receiving message back from PoolManager : "+ee);
            return false;
        }
        
        if( query.getReturnCode() != 0 ) {
            say( "storageInfo Available") ;
        }
        try {
            List assumedLocations = _pnfs.getCacheLocations(_pnfsId) ;
            List<String> [] lists = query.getPools() ;
            HashMap hash = new HashMap() ;
            
            for( int i = 0 ; i < lists.length ; i++ ) {
                Iterator nn = lists[i].iterator() ;
                while( nn.hasNext() ) {
                    hash.put( nn.next() , "" ) ;
                }
            }
            
            Iterator nn = assumedLocations.iterator() ;
            
            while( nn.hasNext() ) {
                if( hash.get( nn.next() ) != null ) {
                    return true;
                }
            }
        } 
	catch(Exception e) {
            say("isCached exception : "+ e);
	    e.printStackTrace();
        }
        return false;
    }
    
    
    
    
    public void log(String s) {
        say(s);
    }
    
    public void elog(String s) {
        esay(s);
    }
    
    public void elog(Throwable t) {
        esay(t);
    }
    
    public void pinFile(SRMUser user,
        String fileId, 
        FileMetaData fmd, 
        long pinLifetime,
        long requestId, 
        PinCallbacks callbacks) {
        DcacheFileMetaData dfmd = (DcacheFileMetaData) fmd;
        PinCompanion.pinFile((DCacheUser)user, 
            fileId, callbacks, dfmd, pinLifetime, requestId, this);
        
    }
    public void unPinFile(SRMUser user,String fileId,
            UnpinCallbacks callbacks,
            String pinId) {
        UnpinCompanion.unpinFile((DCacheUser)user, fileId, pinId, callbacks,this);
    }
    
    
    public String selectGetProtocol(String[] protocols)
    throws SRMException {
        //say("selectGetProtocol("+protocols+")");
        HashSet available_protocols = listAvailableProtocols();
        available_protocols.retainAll(Arrays.asList(protocols));
        available_protocols.removeAll(Arrays.asList(SRM_GET_NOT_SUPPORTED_PROTOCOLS));
        if(available_protocols.size() == 0) {
            esay("can not find sutable get protocol");
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
        //say("selectPutProtocol("+protocols+")");
        HashSet available_protocols = listAvailableProtocols();
        available_protocols.retainAll(Arrays.asList(protocols));
        available_protocols.removeAll(Arrays.asList(SRM_PUT_NOT_SUPPORTED_PROTOCOLS));
        if(available_protocols.size() == 0) {
            esay("can not find sutable put protocol");
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
        HashSet protocols = this.listAvailableProtocols();
        return (String[]) protocols.toArray(new String[0]);
    }
    
    public String[] supportedPutProtocols()
    throws SRMException {
        HashSet protocols = this.listAvailableProtocols();
        // "http" is for getting only
        if(protocols.contains("http")) {
            protocols.remove("http");
        }
        return (String[]) protocols.toArray(new String[0]);
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
        } catch ( java.net.MalformedURLException mue) {
            esay(mue);
            throw new SRMException("illegal previous turl :"+mue);
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
        if(actualFilePath == null) {
            esay("actualFilePath is null");
            throw new IllegalArgumentException("actualFilePath is null");
        }
        GlobusURL prev_turl;
        try {
            prev_turl= new GlobusURL(previous_turl);
        } catch ( java.net.MalformedURLException mue) {
            throw new SRMException("illegal previous turl :"+mue);
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
            esay("path is null");
            throw new IllegalArgumentException("path is null");
        }
        if(protocol == null) {
            esay("protocol is null");
            throw new IllegalArgumentException("protocol is null");
        }
        String transfer_path = getTurlPath(path,protocol,user);
        if(transfer_path == null) {
            esay("cab not get transfer path");
            throw new SRMException("cab not get transfer path");
        }
        String host = selectHost(protocol);
        if(host == null) {
            esay(" failed to get host for protocol:"+protocol);
            throw new SRMException(" failed to get host for "+protocol);
        }
        String turl = protocol+"://"+host+"/"+transfer_path;
        log("getTurl() returns turl="+turl);
        return turl;
        
    }
    
    private String getTurl(String path,String protocol,String host,SRMUser user)
    throws SRMException {
        if(path == null) {
            esay("path is null");
            throw new IllegalArgumentException("path is null");
        }
        if(protocol == null) {
            esay("protocol is null");
            throw new IllegalArgumentException("protocol is null");
        }
        if(host == null) {
            esay("host is null");
            throw new IllegalArgumentException("host is null");
        }
        String transfer_path = getTurlPath(path,protocol,user);
        if(transfer_path == null) {
            esay("cab not get transfer path");
            throw new SRMException("cab not get transfer path");
        }
        String turl = protocol+"://"+host+"/"+transfer_path;
        log("getTurl() returns turl="+turl);
        return turl;
    }
    
    private boolean verifyUserPathIsRootSubpath(String absolutePath, SRMUser user) {
        
        if(absolutePath == null) {
            return false;
        }
        String user_root = null;
        if(user != null) {
            DCacheUser duser = (DCacheUser) user;
            user_root = duser.getRoot();
            if(user_root != null) {
                user_root =new FsPath(user_root).toString();
            }
        }
        
        
        absolutePath = new FsPath(absolutePath).toString();
        if(user_root!= null) {
            log("getTurl() user root is "+user_root);
            if(!absolutePath.startsWith(user_root)) {
                String error = "verifyUserPathIsInTheRoot error:"+
                        "user's path "+absolutePath+
                        " is not subpath of the user's root" +user_root;
                elog(error);
                return false;
            }
            
        }
        return true;
    }
    
    private String getTurlPath(String path,String protocol,SRMUser user)
    throws SRMException {
        
        say("getTurlPath(path="+path+",protocol="+protocol+",user="+user);
        if(!verifyUserPathIsRootSubpath(path,user)) {
            throw new SRMAuthorizationException("user's path "+path+
                    " is not subpath of the user's root");
        }
        String user_root = null;
        if(user != null) {
            DCacheUser duser = (DCacheUser) user;
            user_root = duser.getRoot();
            if(user_root != null) {
                user_root =new FsPath(user_root).toString();
            }
        }
        
        String transfer_path = new FsPath(path).toString();
        if(protocol.equals("gsiftp") && user_root != null) {
            transfer_path = "/".concat(
                    transfer_path.substring(user_root.length()));
        }
        
        log("getTurl()  transfer_path = "+transfer_path);
        return transfer_path;
    }
    
    public LoginBrokerInfo[] getLoginBrokerInfos()
    throws SRMException {
        return getLoginBrokerInfos((String)null);
    }
    
    public static final int MAX_LOGIN_BROKER_RETRIES=5;
    
    private class LoginBrokerCompanion implements CellMessageAnswerable {
        public boolean answered = false;
        public LoginBrokerInfo[] loginBrokerInfos ; //null by default
        public String error; //null by default, error is indicated by error being nonnull
        
        public void answerArrived( CellMessage request ,
                CellMessage answer    ) {
            int i = 0;
            if(answer == null) {
                failedNotify("getLoginBrokerInfos: answer == null");
                return;
            }
            Object o = answer.getMessageObject();
            if(o == null ) {
                failedNotify("getLoginBrokerInfos: loginBroker answer is null");
                return;
            }
            
            if(!(o instanceof  dmg.cells.services.login.LoginBrokerInfo[]) ) {
                failedNotify("getLoginBrokerInfos: login broker returned o ="+o);
                return;
            }
            LoginBrokerInfo[] _loginBrokerInfos = (LoginBrokerInfo[]) o;
            
            successNotify(_loginBrokerInfos);
        }
        
        public void exceptionArrived( CellMessage request ,
                Exception   exception ) {
            esay(exception);
            failedNotify("request failed"+exception);
        }
        
        public void answerTimedOut( CellMessage request ) {
            esay("LoginBrokerCompanion.answerTimedOut()");
            failedNotify("request timed out");
        }
        
        public synchronized void successNotify(LoginBrokerInfo[] loginBrokerInfos ) {
            this.loginBrokerInfos =loginBrokerInfos ;
            answered = true;
            notifyAll();
        }
        
        public synchronized void failedNotify(String error) {
            say("LoginBrokerCompanion.failedNotify("+error+")");
            this.error =error ;
            answered = true;
            notifyAll();
        }
        
        public void waitForAnswer() throws InterruptedException {
            while(true) {
                synchronized(this) {
                    wait(1000);
                    if(answered) {
                        return;
                    }
                }
                getNucleus().updateWaitQueue();
                if(answered) {
                    return;
                }
            }
        }
    }
    //these hashtables will be used as a caching mechanizm for the login broker infos
    // here we asume that no protocol called "null" is going to be ever used
    private java.util.Hashtable latestLoginBrokerInfos = new java.util.Hashtable();
    private java.util.Hashtable latestLoginBrokerInfosTimes =
            new java.util.Hashtable();
    // 30 secs in millisecond
    private long LOGINBROKERINFO_VALIDITYSPAN =30*1000;
    
    public LoginBrokerInfo[] getLoginBrokerInfos(String protocol)
    throws SRMException {
        String key = protocol == null ? "null" : protocol;
        Object o = latestLoginBrokerInfosTimes.get(key);
        if(o !=null) {
            Long timestamp = (Long) o;
            if(System.currentTimeMillis() - timestamp.longValue() <
                    LOGINBROKERINFO_VALIDITYSPAN) {
                o = latestLoginBrokerInfos.get(key);
                if(o != null) {
                    return (LoginBrokerInfo[]) o;
                }
            }
        }
        
        if(loginBrokerPath == null) {
            loginBrokerPath = new CellPath(loginBrokerName);
        }
        
        String brokerMessage = "ls -binary";
        if(protocol != null) {
            brokerMessage = brokerMessage+" -protocol="+protocol;
        }
        LoginBrokerCompanion companion = null;
        for(int retry=0;retry<MAX_LOGIN_BROKER_RETRIES;++retry) {
            companion = new LoginBrokerCompanion();
            try {
                say("getLoginBrokerInfos sending \""+brokerMessage+
                        "\"  to LoginBroker");
                sendMessage(
                        new CellMessage(loginBrokerPath,brokerMessage) ,
                        companion,
                        __pnfsTimeout*1000) ;
            } catch(java.io.NotSerializableException nse) {
                esay("getLoginBrokerInfos error" + nse);
                throw new SRMException("getLoginBrokerInfos error" + nse);
            }
            try {
                companion.waitForAnswer();
            } catch (InterruptedException ie) {
                // ignore
            }
            if(companion.error != null) {
                try {
                    Thread.sleep(5*1000);
                } catch (InterruptedException ie) {
                    // ignore
                }
                continue;
            }
            break;
        }
        
        if(companion.error != null) {
            throw new SRMException(" communication with Login Broker failed with"+
                companion.error);
        }
        latestLoginBrokerInfosTimes.put( key,new Long(System.currentTimeMillis()));
        latestLoginBrokerInfos.put(key,  companion.loginBrokerInfos);
        return companion.loginBrokerInfos;
    }
    
    public HashSet listAvailableProtocols()
    throws SRMException {
        HashSet protocols = new HashSet();
        LoginBrokerInfo[] loginBrokerInfos = getLoginBrokerInfos();
        int len = loginBrokerInfos.length;
        for(int i = 0; i<len; ++i) {
            String protocol = loginBrokerInfos[i].getProtocolFamily();
            if(!protocols.contains(protocol)) {
                protocols.add(protocol);
            }
            
        }
        
        return protocols;
    }
    
    public boolean isLocalTransferUrl(String url)
    throws SRMException {
        
        GlobusURL gurl;
        try {
            gurl = new GlobusURL(url);
        } catch(MalformedURLException mue) {
            return false;
        }
        String protocol = gurl.getProtocol();
        LoginBrokerInfo[] loginBrokerInfos = getLoginBrokerInfos(protocol);
        if(loginBrokerInfos.length == 0) {
            return false;
        }
        String host = gurl.getHost();
        int port = gurl.getPort();
        
        for(int i=0;i<loginBrokerInfos.length;++i) {
            if(loginBrokerInfos[i].getHost().equals(host)  &&
                    loginBrokerInfos[i].getPort() == port) {
                return true;
            }
        }
        
        return false;
    }
    
    
    public String selectHost(String protocol)
    throws SRMException {
        say("selectHost("+protocol+")");
        boolean tryFile = false;
        LoginBrokerInfo[]loginBrokerInfos = getLoginBrokerInfos(protocol);
        return selectHost(loginBrokerInfos);
    }
    
    private Random rand = new Random();
    
    int numDoorInRanSelection=3;
    
    public String selectHost(LoginBrokerInfo[]loginBrokerInfos)
    throws SRMException {
        java.util.Arrays.sort(loginBrokerInfos,new java.util.Comparator(){
            public int compare(java.lang.Object o1,java.lang.Object o2) {
                LoginBrokerInfo loginBrokerInfo1 = (LoginBrokerInfo)o1;
                LoginBrokerInfo loginBrokerInfo2 = (LoginBrokerInfo)o2;
                double load1 = loginBrokerInfo1.getLoad();
                double load2 = loginBrokerInfo2.getLoad();
                if(load1<load2) { return -1  ;}
                if(load1==load2) { return 0  ;}
                return 1;
            } });
            int len = loginBrokerInfos.length;
            
            if(len <=0){
                return null;
            }
            
            int selected_indx = rand.nextInt(java.lang.Math.min(len,
                numDoorInRanSelection));
            LoginBrokerInfo selectedDoor = loginBrokerInfos[selected_indx];
            String thehost =selectedDoor.getHost();
            try {
                InetAddress address = InetAddress.getByName(thehost);
                thehost = address.getHostName();
                if ( customGetHostByAddr && thehost.toUpperCase().equals( 
                    thehost.toLowerCase() )  ) {// must be an IP address
                        thehost = getHostByAddr( address.getAddress() );
                }
                
            } catch(IOException ioe) {
                esay("selectHost "+ioe);
                throw new SRMException("selectHost "+ioe);
            }
            
            say("selectHost returns "+
                    thehost+":"+ selectedDoor.getPort());
            return thehost+":"+ selectedDoor.getPort();
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
                    java.util.List l = new java.util.ArrayList();
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
                String literalip = "";
                if (addr.length == 4) {
                    for (int i = addr.length-1; i >= 0; i--) {
                        literalip += (addr[i] & 0xff) +".";
                    }
                } else if (addr.length == 16) {
                    for (int i = addr.length-1; i >= 0; i--) {
                        literalip += (addr[i] & 0x0f) +"." +(addr[i] & 0xf0) +".";
                    }
                }
                if (addr.length == 4) { // ipv4 addr
                    literalip += "IN-ADDR.ARPA.";
                } else if (addr.length == 16) { // ipv6 addr
                    literalip += "IP6.INT.";
                }

                String[] ids = new String[1];
                ids[0] = "PTR"; // PTR record
                Map map = resolve("dns:///" + literalip, ids);
                Set entrySet = map.entrySet();
                String host = "";
                for (Iterator itr = entrySet.iterator(); itr.hasNext();) {
                    Map.Entry e = (Map.Entry)itr.next();
                    host = (String)((java.util.ArrayList)e.getValue()).get(0);
                }
                return host;
            } catch (Exception e) {
                throw new java.net.UnknownHostException(e.getMessage());
            }
        }
      

    //
    // create a RequestStatus with state set to "Failed"
    //
    
    
    
    
    
    
    
    public void getFileInfo(SRMUser user, String filePath, 
        GetFileInfoCallbacks callbacks) {
        String actualPnfsPath= srm_root+"/"+filePath;
        if(!verifyUserPathIsRootSubpath(actualPnfsPath,user)) {
            callbacks.GetStorageInfoFailed("user's path ["+actualPnfsPath+
                    "] is not a subpath of user's root ");
        }
        GetFileInfoCompanion.getFileInfo(
                (DCacheUser)user,
                actualPnfsPath,
                callbacks,
                this);
    }
    
    public void unPin(String pnfsPath, String pinId) {
        
    }
    
    public void prepareToPut(SRMUser user, 
            String filePath, 
            PrepareToPutCallbacks callbacks,
            boolean overwrite) {
        String actualPnfsPath = srm_root+"/"+filePath;
        PutCompanion.PrepareToPutFile(
                (DCacheUser)user,
                actualPnfsPath,
                callbacks,
                this,
                config.isRecursiveDirectoryCreation(),
                overwrite);
    }
    
	
	
    public void setFileMetaData(SRMUser user, FileMetaData fmd) 
        throws SRMException {
        DcacheFileMetaData dfmd=null;
        if(!(fmd instanceof DcacheFileMetaData)) {
                throw new SRMException("Storage.setFileMetaData: " +
                        "metadata in not dCacheMetaData");
        }
        dfmd = (DcacheFileMetaData) fmd;
        say("Storage.setFileMetaData("+dfmd.getFmd()+
                " , size="+dfmd.getFmd().getFileSize()+")");


        dfmd.getFmd().setUserPermissions( 
                new diskCacheV111.util.FileMetaData.Permissions ( 
                (dfmd.permMode >> 6 ) & 0x7 ) ) ;
        dfmd.getFmd().setGroupPermissions(
                new diskCacheV111.util.FileMetaData.Permissions ( 
                (dfmd.permMode >> 3 ) & 0x7 )) ;
        dfmd.getFmd().setWorldPermissions( 
                new diskCacheV111.util.FileMetaData.Permissions( 
                dfmd.permMode  & 0x7 ) ) ;


        long time = System.currentTimeMillis()/1000L;
        dfmd.getFmd().setLastAccessedTime(time);
        dfmd.getFmd().setLastModifiedTime(time);

        DCacheUser duser = null;
        if (user != null && user instanceof DCacheUser) {
                duser = (DCacheUser) user;
        }

// 		_pnfs.pnfsSetFileMetaData(dfmd.getPnfsId(),dfmd.getFmd());

        PnfsSetFileMetaDataMessage msg = 
            new PnfsSetFileMetaDataMessage(dfmd.getPnfsId());
        msg.setMetaData(dfmd.getFmd());
        msg.setReplyRequired(true);
        CellMessage answer=null;
        Object o=null;
        try {
                answer = sendAndWait(new CellMessage(
                                             new CellPath("PnfsManager") ,
                                             msg) ,
                                     __pnfsTimeout*1000);
        } 
        catch (Exception e) {
                String problem  = "Exception sending pnfs request : "+ e;
                esay(problem);
                esay(e) ;
                throw new
                        SRMException(problem);
        }
        if (answer == null ||
            (o = answer.getMessageObject()) == null ||
            !(o instanceof PnfsSetFileMetaDataMessage)) {
                esay("sent PnfsSetFileMetaDataMessage, received "+o+" back");
                throw new SRMException("can set metadata "+fmd.SURL);
        } 
        else {
            PnfsSetFileMetaDataMessage reply  = 
                (PnfsSetFileMetaDataMessage) answer.getMessageObject();
            if (reply.getReturnCode() != 0) {
                    esay("SetFileMetaData  failed : "+fmd.SURL+
                         " PnfsSetFileMetaDataMessage return code="+
                        reply.getReturnCode()+
                         " reason : "+reply.getErrorObject());
                    throw new SRMException("SetFileMetaData Failed : "+fmd.SURL+
                                           "SetFileMetaData  return code="+
                        reply.getReturnCode()+
                                           " reason : "+reply.getErrorObject());
            }
        }
    }
	

    public FileMetaData getFileMetaData(SRMUser user, 
					String path)
        throws SRMException  {
        return getFileMetaData(user, path, null);
    }
    
    public FileMetaData getFileMetaData(SRMUser user, 
					String path, 
                                        FileMetaData parentFMD) 
        throws SRMException {        
        say("getFileMetaData(" + path + ")");
        String absolute_path = srm_root + "/" + path;
        diskCacheV111.util.FileMetaData parent_util_fmd = null;
        if (parentFMD != null && parentFMD instanceof DcacheFileMetaData) {
            DcacheFileMetaData dfmd = (DcacheFileMetaData)parentFMD;
            parent_util_fmd = dfmd.getFmd();
        }
        DCacheUser duser = null;
        if (user != null && user instanceof DCacheUser) {
            duser = (DCacheUser) user;
        }
        FsPath parent_path = new FsPath(absolute_path);
        parent_path.add("..");
        String parent = parent_path.toString();
        diskCacheV111.util.FileMetaData util_fmd = null;
        StorageInfo storage_info = null;
        PnfsId pnfsId;
        try {
            PnfsGetStorageInfoMessage storage_info_msg = null;
            PnfsGetFileMetaDataMessage filemetadata_msg = null;
            if (parent_util_fmd == null) {
                PnfsGetFileMetaDataMessage parent_filemetadata_msg = 
                    _pnfs.getFileMetaDataByPath(parent);
                parent_util_fmd = parent_filemetadata_msg.getMetaData();
            }
            try {
                storage_info_msg = _pnfs.getStorageInfoByPath(absolute_path);
            } 
	    catch (CacheException e) {
                filemetadata_msg = _pnfs.getFileMetaDataByPath(absolute_path);
            }
            if (storage_info_msg != null) {
                storage_info = storage_info_msg.getStorageInfo();
                util_fmd = storage_info_msg.getMetaData();
                pnfsId = storage_info_msg.getPnfsId();
            } 
	    else if(filemetadata_msg != null) {
                util_fmd = filemetadata_msg.getMetaData();
                pnfsId = filemetadata_msg.getPnfsId();
            } 
	    else {
                esay("could not get storage info or file metadata by path ");
                throw new SRMException(
                    "could not get storage info or file metadata by path ");
                
            }
            if (duser == null) {
                if (!permissionHandler.worldCanRead(
                    absolute_path, parent_util_fmd, util_fmd)) {
                    esay("getFileMetaData have no read permission " +
                        "(or file does not exists) ");
                    throw new SRMException("getFileMetaData have no read " +
                        "permission (or file does not exists) ");
                }
            } 
        } 
	catch (CacheException e) {
            esay("could not get storage info by path : ");
            esay(e);
            throw new SRMException("could not get storage info by path : "+e);
        }
	
        PnfsFlagMessage flag =
            new PnfsFlagMessage(pnfsId, "c", "get");
        try {
            flag.setReplyRequired(true);
            CellMessage answer = 
                sendAndWait(new CellMessage(new CellPath("PnfsManager"),flag), 
			    __pnfsTimeout * 1000);
            Object o = null;
            if (answer == null ||
                (o = answer.getMessageObject()) == null|| 
                !(o instanceof PnfsFlagMessage)) {
                esay("sent PnfsFlagMessage to pnfs, received "+o+" back");
                flag = null;
            } 
	    else {
                flag = (PnfsFlagMessage)o;
            }
        } 
	catch (Exception e) {
            esay("Failed to get crc from PnfsManager : " + e);
            flag = null;
        }
        FileMetaData fmd =
            getFileMetaData(user, absolute_path, pnfsId, 
                            storage_info, util_fmd, flag);
        if (storage_info != null) {
            fmd.isCached = isCached(storage_info, pnfsId);
        }
	
	try { 
	    GetFileSpaceTokensMessage getSpaceTokensMessage = 
                new GetFileSpaceTokensMessage(pnfsId);
	    CellMessage answer =  sendAndWait(
                new CellMessage(
                new CellPath("SrmSpaceManager"),getSpaceTokensMessage),
					      60*60*1000);
            if(answer==null) {
                say("Failed to retrieve space reservation tokens for file "+
                    absolute_path+"("+pnfsId+")");
            }
	    else { 
		getSpaceTokensMessage = 
                    (GetFileSpaceTokensMessage)answer.getMessageObject();
		if (getSpaceTokensMessage.getReturnCode() != 0) {
		    say("Failed to retrieve space reservation tokens for file "+
                        absolute_path+"("+pnfsId+")");
		}
		else {
		    if (getSpaceTokensMessage.getSpaceTokens()!=null) { 
			fmd.spaceTokens = 
                            new long[getSpaceTokensMessage.getSpaceTokens().length];
			System.arraycopy(getSpaceTokensMessage.getSpaceTokens(),0,
                             fmd.spaceTokens,0,
                            getSpaceTokensMessage.getSpaceTokens().length);
		    }
		}
	    }
	}
	catch (Exception ee) { 
	    esay(ee);
	}
        return fmd;
    }
    
    public static FileMetaData 
        getFileMetaData(SRMUser user, 
                        String absolute_path,
                        PnfsId pnfsId, 
                        StorageInfo storage_info,
                        diskCacheV111.util.FileMetaData util_fmd,
                        PnfsFlagMessage flag) 
    {
        boolean isRegular = false;
        boolean isLink = false;
        boolean isDirectory = false;
        boolean isPinned = false;
        boolean isPermanent = true;
        long creationTime = 0;
        long lastModificationTime = 0;
        long lastAccessTime = 0;
        long size = 0;
        String group = null;
        String owner = null;        
        String checksum_type = null;
        String checksum_value = null;
        int permissions = 0;
	boolean isStored=false;
        DcacheFileMetaData fmd = new DcacheFileMetaData(pnfsId);

        if (flag != null) {
            String adler32 = flag.getValue();
            if ((adler32 != null) && adler32.startsWith("1:")) {
                checksum_type = "adler32";
                checksum_value = adler32.substring(2);
            }
        }

        if (util_fmd != null) {        
            owner=Integer.toString(util_fmd.getUid());
            group=Integer.toString(util_fmd.getGid());
            diskCacheV111.util.FileMetaData.Permissions perms =
                util_fmd.getUserPermissions();
            permissions = (perms.canRead()    ? 4 : 0) |
                (perms.canWrite()   ? 2 : 0) |
                (perms.canExecute() ? 1 : 0) ;
            permissions <<= 3;
            perms = util_fmd.getGroupPermissions();
            permissions |=    (perms.canRead()    ? 4 : 0) |
                (perms.canWrite()   ? 2 : 0) |
                (perms.canExecute() ? 1 : 0) ;
            permissions <<= 3;
            perms = util_fmd.getWorldPermissions();
            permissions |=    (perms.canRead()    ? 4 : 0) |
                (perms.canWrite()   ? 2 : 0) |
                (perms.canExecute() ? 1 : 0) ;        
            isRegular = util_fmd.isRegularFile();
            isDirectory = util_fmd.isDirectory();
            isLink = util_fmd.isSymbolicLink();
            creationTime=util_fmd.getCreationTime();
            lastModificationTime=util_fmd.getLastModifiedTime();
            lastAccessTime=util_fmd.getLastAccessedTime();
            size = util_fmd.getFileSize();            
            fmd.setFmd(util_fmd);
        }
        if (storage_info != null) {
            size = storage_info.getFileSize();
	    TRetentionPolicy retention = null;
	    TAccessLatency latency = null;
	    if (storage_info.isSetRetentionPolicy() && 
                storage_info.getRetentionPolicy() != null) {
                if(storage_info.getRetentionPolicy().equals(RetentionPolicy.CUSTODIAL)) { 
                    retention = TRetentionPolicy.CUSTODIAL;
                } else if (storage_info.getRetentionPolicy().equals(RetentionPolicy.REPLICA)) { 
                    retention = TRetentionPolicy.REPLICA;
                } else if (storage_info.getRetentionPolicy().equals(RetentionPolicy.OUTPUT)) { 
                    retention = TRetentionPolicy.OUTPUT;
                } 
            }
            if (storage_info.isSetRetentionPolicy() && 
                storage_info.getAccessLatency() != null) {
                if(storage_info.getAccessLatency().equals(AccessLatency.ONLINE)) { 
                    latency = TAccessLatency.ONLINE;
                } else if (storage_info.getAccessLatency().equals(AccessLatency.NEARLINE)) { 
                    latency = TAccessLatency.NEARLINE;
                }
            }
            // RetentionPolicy is non-nillable element of the 
            // TRetentionPolicyInfo, if retetion is null, we shold leave
            // the whole retentionPolicyInfo null
            if(retention != null) {
                fmd.retentionPolicyInfo = 
                    new TRetentionPolicyInfo(retention, latency);
            }
            fmd.setStorageInfo(storage_info);
	    isStored=storage_info.isStored();
        }
        fmd.isPinned = isPinned;
        fmd.isPermanent = isPermanent;
        fmd.permMode = permissions;
        fmd.size = size;
        fmd.SURL = null;
        fmd.group = group;
        fmd.owner = owner;
        fmd.checksumType = checksum_type;
        fmd.checksumValue = checksum_value;
        fmd.isDirectory = isDirectory;
        fmd.isRegular = isRegular;
        fmd.isLink = isLink;
        fmd.creationTime = creationTime;
        fmd.lastAccessTime= lastAccessTime;
        fmd.lastModificationTime = lastModificationTime;
	fmd.isStored=isStored;
        return fmd;
    }
    
    
    private HashMap idToUserMap = new HashMap();
    private HashMap idToCredentialMap = new HashMap();
    
    private String getUserById(long id) {
        say("getDcacheUserById("+id+")");
        synchronized(idToUserMap) {
            return (String) idToUserMap.get(new Long(id));
            
        }
    }
    
    private GSSCredential getCredentialById(long id) {
        say("getDcacheUserById("+id+")");
        synchronized(idToUserMap) {
            return (GSSCredential) idToCredentialMap.get(new Long(id));
            
        }
    }
    
    protected static long   nextMessageID = 20000 ;
    
    private static synchronized long getNextMessageID() {
        if(nextMessageID == Long.MAX_VALUE) {
            nextMessageID = 20000;
            return Long.MAX_VALUE;
        }
        return nextMessageID++;
        
    }
    
    
    public void localCopy(SRMUser user,String fromFilePath, String toFilePath)
    throws SRMException {
        String actualFromFilePath = srm_root+"/"+fromFilePath;
        String actualToFilePath = srm_root+"/"+toFilePath;
        long id = getNextMessageID();
        say("localCopy for user "+ user+"from actualFromFilePath to actualToFilePath");
        DCacheUser duser = (DCacheUser)user;
        CopyManagerMessage copyRequest =
                new CopyManagerMessage(
                duser.getUid(),
                duser.getGid(),
                actualFromFilePath,
                actualToFilePath,
                id,
                config.getBuffer_size(),
                config.getTcp_buffer_size());
        
        CellMessage answer;
        try {
            answer = sendAndWait(new CellMessage(
                    new CellPath("CopyManager"),
                    copyRequest),1000*60*60*24);
        } catch(Exception e) {
            String emsg ="failed to send/receive the"+
                    " CopyMessage back :"+e.toString();
            esay(emsg);
            throw new SRMException(emsg);
        }
        
        if(answer == null) {
            String emsg = "timeout expired while waiting for answer from CopyManager";
            esay(emsg);
            throw new SRMException(emsg);
        }
        Object object = answer.getMessageObject();
        if(object == null ||
                !(object instanceof CopyManagerMessage)) {
            String emsg ="failed to recive the "+
                    "CopyMessage back";
            esay(emsg);
            throw new SRMException(emsg);
        }
        CopyManagerMessage copyResponce =
                (CopyManagerMessage) object;
        int rc = copyResponce.getReturnCode();
        if( rc != 0) {
            String emsg =" local copy failed with code ="+ rc +
                    " details: "+copyResponce.getDescription();
            esay(emsg);
            throw new SRMException(emsg);
        }
        return;
    }
    
    
    public void prepareToPutInReservedSpace(SRMUser user, String path, long size, 
        long spaceReservationToken, PrepareToPutInSpaceCallbacks callbacks) {
        throw new java.lang.UnsupportedOperationException("NotImplementedException");
    }
    
    
    
    public HashMap poolInfos = new HashMap();
    public HashMap poolInfosTimestamps = new HashMap();
    
    public StorageElementInfo getPoolInfo(String pool) throws SRMException {
        synchronized(poolInfosTimestamps) {
            if(poolInfosTimestamps.containsKey(pool)) {
                long poolInfoTimestamp =
                        ((Long)(poolInfosTimestamps.get(pool))).longValue();
                if((System.currentTimeMillis() - poolInfoTimestamp) < 3*1000*60) {
                    return (StorageElementInfo) poolInfos.get(pool);
                }
            }
        }
        
        CellMessage poolInfoMessage = new CellMessage(
                new CellPath((String)( pool)) ,
                "xgetcellinfo" ) ;
        //say("getPoolInfo("+pool+"): Waiting for Pool reply for \"xgetcellinfo\"");
        try {
            poolInfoMessage = sendAndWait(poolInfoMessage,
                    __poolManagerTimeout*1000);
        }catch(Exception e) {
            esay(e);
            String error = "getPoolInfo("+pool+"): sendAndWait() failed "+
                    "with "+e;
            say(error);
            throw new SRMException(error);
            
        }
        
        if(poolInfoMessage == null) {
            String error = "pool timeout expired" ;
            esay(error);
            throw new SRMException(error);
        }
        
        Object o = poolInfoMessage.getMessageObject();
        if(o == null || !(o instanceof  PoolCellInfo)) {
            String error = "pool returned o="+o ;
            esay(error);
            throw new SRMException(error);
        }
        PoolCellInfo  poolCellInfo = (PoolCellInfo)o;
        PoolCostInfo.PoolSpaceInfo info = poolCellInfo.getPoolCostInfo().getSpaceInfo() ;
        long total     = info.getTotalSpace() ;
        long freespace = info.getFreeSpace() ;
        long precious  = info.getPreciousSpace() ;
        long removable = info.getRemovableSpace() ;
        StorageElementInfo poolInfo = new StorageElementInfo();
        poolInfo.totalSpace = total;
        poolInfo.availableSpace = freespace+removable;
        poolInfo.usedSpace = precious+removable;
        synchronized(poolInfosTimestamps) {
            poolInfos.put(pool,poolInfo);
            poolInfosTimestamps.put(pool, new Long(System.currentTimeMillis()));
        }
        return poolInfo;
        
    }
    
    public void advisoryDelete(final SRMUser user, final String path, 
        AdvisoryDeleteCallbacks callbacks) {
        say("Storage.advisoryDelete");
        
        if(callbacks == null) {
            callbacks =
                    new AdvisoryDeleteCallbacks() {
                public void AdvisoryDeleteFailed(String reason) {
                    esay(" advisoryDelete("+user+","+path+") GetStorageInfoFailed: "+reason);
                }
                
                
                public void AdvisoryDeleteSuccesseded(){
                    say(" advisoryDelete("+user+","+path+") AdvisoryDeleteSuccesseded");
                }
                
                public void Exception(Exception e){
                    esay(" advisoryDelete("+user+","+path+") Exception :"+e);
                }
                
                public void Timeout(){
                    esay(" advisoryDelete("+user+","+path+") timeout");
                }
                
                public void Error(String error){
                    esay(" advisoryDelete("+user+","+path+") Error:" + error);
                }
            };
        }
        
        String actualPnfsPath= srm_root+"/"+path;
        AdvisoryDeleteCompanion.advisoryDelete(
                (DCacheUser)user,
                actualPnfsPath,
                callbacks,
                this,
                config.isAdvisoryDelete());
        
    }
    
    public void removeFile(final SRMUser user,
            final String path,
            RemoveFileCallbacks callbacks) {
        say("Storage.removeFile");
        String actualPnfsPath= srm_root+"/"+path;
        RemoveFileCompanion.removeFile(
                (DCacheUser)user,
                actualPnfsPath,
                callbacks,
                this,
                config.isRemoveFile());
    }
    
    
    public void removeDirectory(final SRMUser user,
            final Vector tree)  throws SRMException {
        say("Storage.removeDirectory");
        DCacheUser duser = (DCacheUser) user;
        for (Iterator i = tree.iterator(); i.hasNext();) {
            String path= (String)i.next();
            String actualPnfsPath= srm_root+"/"+path;
            PnfsGetStorageInfoMessage storage_info_msg =null;
            PnfsGetStorageInfoMessage filemetadata_msg =null;
            
            PnfsId pnfsId;
            try {
                storage_info_msg = _pnfs.getStorageInfoByPath(actualPnfsPath);
            } catch(CacheException ce1 ) {
                esay("could not get storage info or file metadata by path "+
                    actualPnfsPath);
                throw new SRMException("could not get storage info or file " +
                    "metadata by path "+
                    ce1.toString());
            }
            if ( storage_info_msg != null) {
                pnfsId = storage_info_msg.getPnfsId();
            } else if (filemetadata_msg != null) {
                pnfsId = filemetadata_msg.getPnfsId();
            } else {
                esay("could not get storage info or file metadata by path "+
                    actualPnfsPath);
                throw new SRMException("could not get storage info or file " +
                    "metadata by path "+actualPnfsPath);
            }
            
            PnfsDeleteEntryMessage delete_request =
                    new PnfsDeleteEntryMessage(actualPnfsPath);
            delete_request.setReplyRequired(true);
            CellMessage answer=null;
            Object o=null;
            
            try {
                answer = sendAndWait( new CellMessage(
                        new CellPath("PnfsManager") ,
                        delete_request) ,
                        __pnfsTimeout*1000);
            } catch (Exception e) {
                String problem  = "Exception sending pnfs request : "+ e;
                esay( problem ) ;
                esay(e) ;
                throw new
                        SRMException(problem);
            }
            if (answer == null ||
                    (o = answer.getMessageObject()) == null ||
                    !(o instanceof PnfsDeleteEntryMessage)) {
                esay("sent PnfsDeleteEntryMessage pnfs, received "+o+" back");
                throw new SRMException("can not delete "+actualPnfsPath);
            } else {
                PnfsDeleteEntryMessage delete_reply = 
                    (PnfsDeleteEntryMessage) answer.getMessageObject();
                if (delete_reply.getReturnCode() != 0) {
                    esay("Delete failed : "+actualPnfsPath+
                            " PnfsDeleteEntryMessage return code="+
                        delete_reply.getReturnCode()+
                            " reason : "+delete_reply.getErrorObject());
                    throw new SRMException("Delete Failed : "+actualPnfsPath+
                            " PnfsDeleteEntryMessage return code="+
                        delete_reply.getReturnCode()+
                            " reason : "+delete_reply.getErrorObject());
                }
            }
        }
    }
    
    public void createDirectory(final SRMUser user,
            final String directory)  throws SRMException {
        say("Storage.createDirectory");
        //
        // checks are done in this order:
        //   - directory exists  NO : YES
        //                       |     +-- exception
        //                       +-- parent exists YES : NO
        //                                          |    +-- exception
        //                                          +-- can write YES : NO
        //                                                         |     +-- exception
        //                                                         +-- create PnfsEntry
        DCacheUser duser = (DCacheUser) user;
        String actualPnfsPath= srm_root+"/"+directory;
        PnfsGetStorageInfoMessage  storageInfoMessage  =null;
        PnfsGetFileMetaDataMessage fileMetadataMessage =null;
        diskCacheV111.util.FileMetaData parentFmd      =null;
        StorageInfo storageInfo = null;
        PnfsId pnfsId;
        try {
            fileMetadataMessage = _pnfs.getFileMetaDataByPath(actualPnfsPath);
            throw new SRMDuplicationException(" already exists");
        } catch( CacheException ce) {
            say("createDirectory "+actualPnfsPath+" does not exist, " +
                "proceeding to create ");
        }
        if ( fileMetadataMessage != null ) {
            esay("createDirectory: "+actualPnfsPath+" already exists");
            throw new SRMDuplicationException(" already exists");
        }
        //
        // checking parent
        //
        FsPath parentFsPath = new FsPath(actualPnfsPath);
        parentFsPath.add("..");
        String parent = parentFsPath.toString();
        
        try {
            fileMetadataMessage = _pnfs.getFileMetaDataByPath(parent);
        } catch( CacheException ce) {
            esay("failed to get metadata for "+actualPnfsPath+" parent "+parent);
            esay(ce);
            throw new SRMInvalidPathException("parent path or a component of the " +
                "parent path does not exist");
        }
        if (fileMetadataMessage != null) {
            parentFmd = fileMetadataMessage.getMetaData();
            pnfsId    = fileMetadataMessage.getPnfsId();
        } else {
            esay("createDirectory: "+actualPnfsPath+" creation failed, " +
                "parent path does not exist");
            throw new SRMInvalidPathException("parent path or a component " +
                "of the parent path does not exist");
        }
        
        int uid = parentFmd.getUid();
        int gid = parentFmd.getGid();
        diskCacheV111.util.FileMetaData.Permissions perms =
                parentFmd.getUserPermissions();
        int permissions = (perms.canRead()    ? 4 : 0) |
                (perms.canWrite()   ? 2 : 0) |
                (perms.canExecute() ? 1 : 0);
        permissions <<=3;
        perms = parentFmd.getGroupPermissions();
        permissions |=    (perms.canRead()    ? 4 : 0) |
                (perms.canWrite()   ? 2 : 0) |
                (perms.canExecute() ? 1 : 0);
        permissions <<=3;
        perms = parentFmd.getWorldPermissions();
        permissions |=    (perms.canRead()    ? 4 : 0) |
                (perms.canWrite()   ? 2 : 0) |
                (perms.canExecute() ? 1 : 0);
        
        if (permissions == 0 ) {
            esay("createDirectory: cannot create directory \""+actualPnfsPath+
                "\": Permission denied");
            throw new SRMAuthorizationException(" can't write into parent path," +
                " permission denied");
        }
        
        if (duser.getUid() == uid ) {
            if (!Permissions.userCanWrite(permissions) || 
                !Permissions.userCanExecute(permissions)) {
                esay("createDirectory: cannot create directory \""+
                    actualPnfsPath+"\": Permission denied");
                throw new SRMAuthorizationException(" Permission denied");
            }
        } else if ( duser.getGid() == gid ) {
            if (!Permissions.groupCanWrite(permissions) || 
                !Permissions.groupCanExecute(permissions)) {
                esay("createDirectory: cannot create directory \""+
                    actualPnfsPath+"\": Permission denied");
                throw new SRMAuthorizationException(" Permission denied");
            }
        } else {
            if (!Permissions.worldCanWrite(permissions) || 
                !Permissions.worldCanExecute(permissions)) {
                esay("createDirectory: cannot create directory \""+
                    actualPnfsPath+"\": Permission denied");
                throw new SRMAuthorizationException(" Permission denied");
            }
        }
        
        PnfsGetStorageInfoMessage createRequest = 
            new PnfsCreateDirectoryMessage(actualPnfsPath,duser.getUid(),
            duser.getGid(),permissions); // was 0755
        CellMessage answer=null;
        Object o=null;
        try {
            answer = sendAndWait( new CellMessage(
                    new CellPath("PnfsManager") ,
                    createRequest) ,
                    __pnfsTimeout*1000);
        } catch (Exception e) {
            String problem  = "createDirectory: Exception sending create pnfs request: "+e;
            esay( problem );
            esay(e);
            throw new SRMException(problem);
        }
        if (answer == null ||
                (o = answer.getMessageObject()) == null ||
                !(o instanceof PnfsCreateDirectoryMessage)) {
            esay("sent PnfsCreateDirectoryMessage to pnfs, received "+o+" back");
            throw new SRMException("Can't create, communication with pnfs failure");
        } else {
		PnfsCreateDirectoryMessage createReply = 
			(PnfsCreateDirectoryMessage) answer.getMessageObject();
		if (createReply.getReturnCode() != 0) {
			esay("createDirectory: directory creation failed, got error " +
			     "return code from pnfs");
			if (createReply.getReturnCode()== CacheException.FILE_EXISTS) {
				throw new SRMDuplicationException(" already exists");
			}
			Object error = createReply.getErrorObject();
                if(error instanceof Throwable) {
		    esay((Throwable)error);
                   throw new SRMException("Failed to create, got error return " +
                       "code from pnfs",(Throwable)error);
                }
                else 
		{
                    throw new SRMException("Failed to create, got error return " +
                        "code from pnfs: "+error);
                }
            }
        }
    }
    
    
    public void moveEntry(final SRMUser user,
			  final String from,
			  final String to)  throws SRMException {
        say("Storage.moveEntry");
        DCacheUser duser = (DCacheUser) user;
        String actualFromPnfsPath= srm_root+"/"+from;
        String actualToPnfsPath= srm_root+"/"+to;
        FsPath fromFsPath           = new FsPath(actualFromPnfsPath);
        String fromPnfsPath         = fromFsPath.toString();
        PnfsGetStorageInfoMessage  storageInfoMessage  =null;
        PnfsGetFileMetaDataMessage fileMetadataMessage =null;
        diskCacheV111.util.FileMetaData toFmd          =null;
        diskCacheV111.util.FileMetaData fromFmd        =null;
        diskCacheV111.util.FileMetaData parentFmd      =null;
        StorageInfo storageInfo = null;
        PnfsId pnfsId;
        FsPath toFsPath           = new FsPath(actualToPnfsPath);
        String toPnfsPath         = toFsPath.toString();

	try {
            storageInfoMessage = _pnfs.getStorageInfoByPath(actualFromPnfsPath);
        } 
	catch(CacheException ce1 ) {
            try {
                fileMetadataMessage = _pnfs.getFileMetaDataByPath(actualFromPnfsPath);
            } 
	    catch(CacheException e2) {
                esay("moveEntry: \"from\" path does not exist");
                esay(e2);
                throw new SRMException(actualFromPnfsPath+" (source) does not exist");
            }
        }
        if ( storageInfoMessage  != null) {
            storageInfo = storageInfoMessage.getStorageInfo();
            fromFmd     = storageInfoMessage.getMetaData();
            pnfsId      = storageInfoMessage.getPnfsId();
        } 
	else if ( fileMetadataMessage != null) {
            fromFmd = fileMetadataMessage.getMetaData();
            pnfsId  = fileMetadataMessage.getPnfsId();
        } 
	else {
            esay("moveEntry: \"from\" path, failed to get metadata - file does not exist");
            throw new SRMInvalidPathException(actualFromPnfsPath+
                " (source) failed to get metadata - file does not exist");
        }
        int uid = fromFmd.getUid();
        int gid = fromFmd.getGid();
        diskCacheV111.util.FileMetaData.Permissions perms =
                fromFmd.getUserPermissions();
        int permissions = (perms.canRead()    ? 4 : 0) |
	    (perms.canWrite()   ? 2 : 0) |
	    (perms.canExecute() ? 1 : 0);
        permissions <<=3;
        perms = fromFmd.getGroupPermissions();
        permissions |=    (perms.canRead()    ? 4 : 0) |
	    (perms.canWrite()   ? 2 : 0) |
	    (perms.canExecute() ? 1 : 0);
        permissions <<=3;
        perms = fromFmd.getWorldPermissions();
        permissions |=    (perms.canRead()    ? 4 : 0) |
	    (perms.canWrite()   ? 2 : 0) |
	    (perms.canExecute() ? 1 : 0);
        
        if (permissions == 0 ) {
            esay("moveEntry: cannot move source \""+
                actualFromPnfsPath+"\": Permission denied ");
            throw new SRMException(" don't have write access to source ");
        }
        
        if (duser.getUid() == uid ) {
            if (!Permissions.userCanWrite(permissions)) {
                esay("moveEntry: cannot move source  \""+actualFromPnfsPath+
                    "\": Permission denied");
                throw new SRMException(" don't have write access to source");
            }
        } else if ( duser.getGid() == gid ) {
            if (!Permissions.groupCanWrite(permissions) ) {
                esay("moveEntry: cannot move source \""+actualFromPnfsPath+
                    "\": Permission denied");
                throw new SRMException(" don't have group write access to source");
            }
        } else {
            if (!Permissions.worldCanWrite(permissions) ) {
                esay("moveEntry: cannot move source \""+actualFromPnfsPath+
                    "\": Permission denied");
                throw new SRMException(" don't have world write access to source");
            }
        }
        
        if ( fromPnfsPath.equals(toPnfsPath) ) {
            say("moveEntry: \"to\" is identical to \"from\", return success");
            return;
        }

        storageInfoMessage  = null;
        fileMetadataMessage = null;
        storageInfo = null;
        PnfsId toPnfsId;
        PnfsId parentPnfsId;
        boolean toExists=true;
        
       
        try {
            storageInfoMessage = _pnfs.getStorageInfoByPath(actualToPnfsPath);
        } 
	catch(CacheException ce1 ) {
            try {
                fileMetadataMessage = _pnfs.getFileMetaDataByPath(actualToPnfsPath);
            } 
	    catch(CacheException e2) {
            }
        }
        if ( storageInfoMessage  != null) {
            storageInfo = storageInfoMessage.getStorageInfo();
            toFmd       = storageInfoMessage.getMetaData();
            toPnfsId    = storageInfoMessage.getPnfsId();
        } 
	else if ( fileMetadataMessage != null) {
            toFmd   = fileMetadataMessage.getMetaData();
            toPnfsId = fileMetadataMessage.getPnfsId();
        } 
	else {
            toExists=false;
        }
        
        //
        // Logic is this :
        //                 destination may be non-existing file with valid path
        //                 destination may be existing directory
        //
        
        if (toExists) {
            if (toFmd.isRegularFile()) {
                esay("moveEntry: cannot move to existing file \""+
                    actualToPnfsPath+"\"");
                throw new SRMDuplicationException(" cannot move to existing file \""+
                    actualToPnfsPath+"\"");
            }
        } 
	else {
            //
            // check parent
            //
            FsPath toParentFsPath = new FsPath(actualToPnfsPath);
            toParentFsPath.add("..");
            String toParent = toParentFsPath.toString();
            fileMetadataMessage=null;
            storageInfoMessage=null;
            try 
	    {
                storageInfoMessage = _pnfs.getStorageInfoByPath(toParent);
            } 
	    catch(CacheException ce1 ) {
                try {
                    fileMetadataMessage = _pnfs.getFileMetaDataByPath(toParent);
                } 
		catch(CacheException e2) {
                    esay("moveEntry: neither destination no its parent path exist "+
                        actualToPnfsPath);
                    esay(e2);
                    throw new SRMInvalidPathException("neither destination no its " +
                        "parent path exist "+actualToPnfsPath);
                }
            }
            if (storageInfoMessage != null) {
                storageInfo  = storageInfoMessage.getStorageInfo();
                toFmd        = storageInfoMessage.getMetaData();
                toPnfsId     = storageInfoMessage.getPnfsId();
            } 
	    else if (fileMetadataMessage != null) {
                toFmd    = fileMetadataMessage.getMetaData();
                toPnfsId = fileMetadataMessage.getPnfsId();
            } 
	    else {
                esay("moveEntry: "+actualToPnfsPath+" does not exist and its path " +
                    "does not exist");
                throw new SRMInvalidPathException(" path or a component of the " +
                    "destination path does not exist");
            }
            if (toFmd.isRegularFile()) {
                esay("moveEntry: cannot move to existing file \""+actualToPnfsPath+"\"");
                throw new SRMException(" cannot move to existing file \""+
                    actualToPnfsPath+"\"");
            }
            
        }
        //
        // check we can write into destination
        //
        
        int to_uid = fromFmd.getUid();
        int to_gid = fromFmd.getGid();
        diskCacheV111.util.FileMetaData.Permissions to_perms =
                toFmd.getUserPermissions();
        permissions = (to_perms.canRead()    ? 4 : 0) |
                (to_perms.canWrite()   ? 2 : 0) |
                (to_perms.canExecute() ? 1 : 0);
        permissions <<=3;
        to_perms = toFmd.getGroupPermissions();
        permissions |=    (to_perms.canRead()    ? 4 : 0) |
                (to_perms.canWrite()   ? 2 : 0) |
                (to_perms.canExecute() ? 1 : 0);
        permissions <<=3;
        to_perms = toFmd.getWorldPermissions();
        permissions |=    (to_perms.canRead()    ? 4 : 0) |
                (to_perms.canWrite()   ? 2 : 0) |
                (to_perms.canExecute() ? 1 : 0);
        
        if (permissions == 0 ) {
            esay("moveEntry: cannot move to directory \""+actualToPnfsPath+
                "\": Permission denied");
            throw new SRMException(" don't have write access to destination");
        }
        
        if (duser.getUid() == uid ) {
            if (!Permissions.userCanWrite(permissions)) {
                esay("moveEntry: cannot move directory to  \""+actualToPnfsPath+
                    "\": Permission denied");
                throw new SRMException(" don't have write access to destination");
            }
        } 
	else if ( duser.getGid() == gid ) {
            if (!Permissions.groupCanWrite(permissions)) {
                esay("moveEntry: cannot move to directory \""+actualToPnfsPath+
                    "\": Permission denied");
                throw new SRMException(" don't have group write access to destination");
            }
        } 
	else {
            if (!Permissions.worldCanWrite(permissions)) {
                esay("moveEntry: cannot move to directory \""+actualToPnfsPath+
                    "\": Permission denied");
                throw new SRMException(" don't have world write access to destination");
            }
        }
        java.io.File fromFile = new File(actualFromPnfsPath);
        java.io.File toFile   = new File(actualToPnfsPath);
        String newName = actualToPnfsPath;
        String[] dirlist;
        if ( toExists ) {
            //
            // the output is directory and contains the same file
            //
            dirlist = toFile.list();
            for (int i=0; i<dirlist.length;i++) {
                if (dirlist[i].compareTo(fromFile.getName())==0)  {
                    esay("moveEntry: cannot overwrite existing entry \""+
                        actualToPnfsPath+"/"+fromFile.getName());
                    throw new SRMException(" cannot overwrite existing entry \""+
                        actualToPnfsPath+"/"+fromFile.getName());
                }
            }
        }
        if (toExists) {
            newName += "/"+fromFile.getName();
        }
        PnfsRenameMessage renameRequest = new PnfsRenameMessage(pnfsId,newName);
        CellMessage answer=null;
        Object o=null;
        try {
            answer = sendAndWait( new CellMessage(
				      new CellPath("PnfsManager") ,
				      renameRequest) ,
				  __pnfsTimeout*1000);
        } 
	catch (Exception e) {
            String problem  = "createDirectory: Exception sending rename " +
                "pnfs request: "+e;
            esay( problem );
            esay(e);
            throw new SRMException(problem);
        }
        if (answer == null ||
                (o = answer.getMessageObject()) == null ||
                !(o instanceof PnfsRenameMessage )) {
            esay("moveEntry: sent  PnfsRenameMessage to pnfs, received "+o+
                " back");
            throw new SRMInternalErrorException(
                "Can't create, communication with pnfs failure");
        } 
	else {
            PnfsRenameMessage reply = (PnfsRenameMessage) answer.getMessageObject();
            if (reply.getReturnCode() != 0) {
                esay("moveEntry: mv failed, got error return code from pnfs");
                esay("MoveEntry: "+reply.getPnfsPath()+" "+reply.getPnfsId()+" "+
                    reply.getErrorObject());
                throw new SRMInternalErrorException(
                    "Failed to move, got error return code from pnfs"+
                    reply.getErrorObject());
            }
        }
    }
    
    
    public boolean canRead(SRMUser user, String fileId, FileMetaData fmd) {
        return _canRead(user,fileId,fmd);
    }
    
    public static boolean _canRead(SRMUser user, String fileId, FileMetaData fmd) {
        PnfsId pnfsId = new PnfsId(fileId);
        int uid = Integer.parseInt(fmd.owner);
        int gid = Integer.parseInt(fmd.group);
        int permissions = fmd.permMode;
        
        if(pnfsId == null) {
            return false;
        }
        
        if(permissions == 0 ) {
            return false;
        }
        
        if(Permissions.worldCanRead(permissions)) {
            return true;
        }
        
        if(uid == -1 || gid == -1) {
            return false;
        }
        
        if(user == null || (!(user instanceof DCacheUser))) {
            return false;
        }
        DCacheUser duser = (DCacheUser) user;
        
        if(duser.getGid() == gid && Permissions.groupCanRead(permissions)) {
            return true;
        }
        
        if(duser.getUid() == uid && Permissions.userCanRead(permissions)) {
            return true;
        }
        
        return false;
        
        
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
        
        if(user == null || (!(user instanceof DCacheUser))) {
            return false;
        }
        DCacheUser duser = (DCacheUser) user;
        
        if(duser.getGid() == gid && Permissions.groupCanRead(permissions)) {
            return true;
        }
        
        if(duser.getUid() == uid && Permissions.userCanRead(permissions)) {
            return true;
        }
        
        return false;
    }
    
    
    
    public boolean canWrite(SRMUser user, String fileId, FileMetaData fmd, 
        String parentFileId, FileMetaData parentFmd, boolean overwrite) {
        return _canWrite(user,fileId,fmd,parentFileId,parentFmd,overwrite);
    }
    
    public static boolean _canWrite(SRMUser user, 
            String fileId,
            FileMetaData fmd,
            String parentFileId, 
            FileMetaData parentFmd, 
            boolean overwrite) {
        // we can not overwrite file in dcache (at least for now)
        //System.out.println("_canWrite user="+user+
        //        " fileId="+fileId+
        //        " fmd="+fmd+
        //        " parentFileId="+parentFileId+
        //        " parentFmd="+parentFmd+
        //        " overwrite="+overwrite);
        if(! overwrite) {
            if(fileId != null ) {
                // file exists and we can't overwrite
                return false;
            }
        }
        
        if( parentFileId == null) {
            return false;
        }
        
        DCacheUser duser = (DCacheUser) user;
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
            } else  if(user == null || (!(user instanceof DCacheUser))) {
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
        } else  if(user == null || (!(user instanceof DCacheUser))) {
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
    
    public static boolean _canDelete(SRMUser user, String fileId,
        FileMetaData fmd) {
        // we can not overwrite file in dcache (at least for now)
        if(fileId == null ) {
            return false;
        }
        
        PnfsId pnfsId = new PnfsId(fileId);
        int uid = Integer.parseInt(fmd.owner);
        int gid = Integer.parseInt(fmd.group);
        int permissions = fmd.permMode;
        
        if(permissions == 0 ) {
            return false;
        }
        
        if(Permissions.worldCanWrite(permissions)) {
            return true;
        }
        
        if(uid == -1 || gid == -1) {
            return false;
        }
        
        if(user == null || (!(user instanceof DCacheUser))) {
            return false;
        }
        DCacheUser duser = (DCacheUser) user;
        
        
        if(duser.getGid() == gid &&
                Permissions.groupCanWrite(permissions) ) {
            return true;
        }
        
        if(duser.getUid() == uid &&
                Permissions.userCanWrite(permissions) ) {
            return true;
        }
        
        return false;
        
        
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
            String remoteUser,
            Long remoteCredentialId,
            String spaceReservationId,
            long size,
            CopyCallbacks callbacks) throws SRMException{
        actualFilePath = srm_root+"/"+actualFilePath;
        say(" getFromRemoteTURL from "+remoteTURL+" to " +actualFilePath);
        return performRemoteTransfer(user,remoteTURL,actualFilePath,true,
                remoteUser,
                remoteCredentialId,
                spaceReservationId,
                new Long(size),
                callbacks);
        
    }
    
    public String getFromRemoteTURL(SRMUser user,
            String remoteTURL,
            String actualFilePath,
            String remoteUser,
            Long remoteCredentialId,
            CopyCallbacks callbacks) throws SRMException{
        actualFilePath = srm_root+"/"+actualFilePath;
        say(" getFromRemoteTURL from "+remoteTURL+" to " +actualFilePath);
        return performRemoteTransfer(user,remoteTURL,actualFilePath,true,
                remoteUser,
                remoteCredentialId,
                null,
                null,
                callbacks);
        
    }
    
    /**
     * @param user User ID
     * @param actualFilePath
     * @param remoteTURL
     * @param remoteUser
     * @param callbacks
     * @param remoteCredetial
     * @throws SRMException
     * @return copy handler id
     */
    public String putToRemoteTURL(SRMUser user,
            String filePath,
            String remoteTURL,
            String remoteUser,
            Long remoteCredentialId,
            CopyCallbacks callbacks)
            throws SRMException{
        String actualFilePath = srm_root+"/"+filePath;
        say(" putToRemoteTURL from "+actualFilePath+" to " +remoteTURL);
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
            Long longCallerId = new Long(callerId);
            TransferInfo info;
            synchronized(callerIdToHandler) {
                Object o =  callerIdToHandler.get(longCallerId);
                if( o == null) {
                    return;
                }
                info = (TransferInfo) o;
            }
            CancelTransferMessage cancel =
                    new diskCacheV111.vehicles.transferManager.
                CancelTransferMessage(info.transferId, callerId);
            sendMessage(new CellMessage(info.cellPath,cancel));
        } catch(Exception e) {
            esay(e);
        }
        
    }
    
    
    private String performRemoteTransfer(
            SRMUser user,
            String remoteTURL,
            String actualFilePath,
            boolean store,
            String remoteUser,
            Long remoteCredentialId,
            String spaceReservationId,
            Long size,
            CopyCallbacks callbacks
            )
            throws SRMException {
        say("performRemoteTransfer performing "+(store?"store":"restore"));
        if(!verifyUserPathIsRootSubpath(actualFilePath,user)) {
            throw new SRMAuthorizationException("user's path "+actualFilePath+
                    " is not subpath of the user's root");
        }
        
        if(remoteTURL.startsWith("gsiftp://")) {
            DCacheUser duser = (DCacheUser)user;
            
            //call this for the sake of checking that user is reading
            // from the "root" of the user
            String path = getTurlPath(actualFilePath,"gsiftp",user);
            if(path == null) {
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

            if(store && spaceReservationId != null  && size != null) {
                gsiftpTransferRequest = new RemoteGsiftpTransferManagerMessage(
                        duser.getName(),
                        duser.getUid(),
                        duser.getGid(),
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
                gsiftpTransferRequest = new RemoteGsiftpTransferManagerMessage(
                        duser.getName(),
                        duser.getUid(),
                        duser.getGid(),
                        remoteTURL,
                        actualFilePath,
                        store,
                        remoteCredentialId,
                        credentialName,
                        config.getBuffer_size(),
                        config.getTcp_buffer_size()
                        );
            }
            gsiftpTransferRequest.setReplyRequired(true);
            gsiftpTransferRequest.setStreams_num(config.getParallel_streams());
            CellMessage answer;
            try {
                CellPath cellPath = new CellPath(remoteGridftpTransferManagerName);
                answer = sendAndWait(new CellMessage(
                        cellPath,
                        gsiftpTransferRequest),1000*60*60*24);
                RemoteGsiftpTransferManagerMessage
                        answer_message =
                        (RemoteGsiftpTransferManagerMessage)
                        answer.getMessageObject();
                if(answer_message.getReturnCode() != 0) {
                    if(answer_message.getErrorObject() != null) {
                        throw new SRMException("TransferManager error"+
                                answer_message.getErrorObject().toString());
                    } else {
                        throw new SRMException("TransferManager error");
                        
                    }
                }
                long id = answer_message.getId();
                say("received first RemoteGsiftpTransferManagerMessage reply from" +
                    " transfer manager, id ="+id);
                GridftpTransferInfo info = new GridftpTransferInfo(id,
                    remoteCredentialId,callbacks,cellPath);
                Long longCallerId = new Long(id);
                say("strorring info for callerId = "+longCallerId);
                synchronized(callerIdToHandler) {
                    callerIdToHandler.put(longCallerId,info);
                }
                return longCallerId.toString();
            }catch(SRMException srme){
                throw srme;
            }catch(Exception e) {
                String emsg ="failed to send/receive the"+
                        " RemoteGsiftpTransferManagerMessage back";
                esay(emsg);
                throw new SRMException(emsg);
            }
        }
        throw new SRMException("not implemented");
    }
    
    private Map callerIdToHandler = new HashMap();
    
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
        Long callerId = new Long(message.getId());
        say("handleTransferManagerMessage for callerId="+callerId);
        
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
        
        Object o;
        synchronized(callerIdToHandler) {
            o = callerIdToHandler.get(callerId);
            if(o == null) {
                esay("TransferInfo for callerId="+callerId+"not found");
                return;
            }
        }
        TransferInfo info = (TransferInfo)o;
        
        if (message instanceof TransferCompleteMessage ) {
            TransferCompleteMessage complete =
                    (TransferCompleteMessage)message;
            info.callbacks.copyComplete(null,null);
            esay("removing TransferInfo for callerId="+callerId);
            synchronized(callerIdToHandler) {
                callerIdToHandler.remove(callerId);
            }
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
            
            esay("removing TransferInfo for callerId="+callerId);
            synchronized(callerIdToHandler) {
                callerIdToHandler.remove(callerId);
            }
            
        }
        
    }
    
    private void delegate(GSSCredential credential, String host, int port) {
        if(credential == null) {
            esay("cannot delegate,  user credential is null");
            try {
                //we can not deligate so we make this fail on the door side
                say("we can not deligate so we make this fail on the door side");
                Socket s = new Socket(host,port);
                OutputStream sout= s.getOutputStream();
                sout.close();
                s.close();
            } catch(IOException ioe) {
                esay(ioe);
            }
        } else {
            try {
                String credname = credential.getName().toString();
                say("SRMCell.Delegator, delegating credentials :"+
                        credential+   " to mover at "+host+
                        " listening on port "+port);
                
            }catch(org.ietf.jgss.GSSException gsse) {
                esay("invalid credentials :");
                esay(gsse);
                say("we can not deligate so we make this fail on the door side");
                try {
                    Socket s = new Socket(host,port);
                    OutputStream sout= s.getOutputStream();
                    sout.close();
                    s.close();
                } catch(IOException ioe) {
                    esay(ioe);
                }
                return;
            }
            
            try {
                SslGsiSocketFactory.delegateCredential(
                        InetAddress.getByName(host),
                        port,
                        credential,false);
                say("delegation appears to have succeeded");
            } catch(Exception e) {
                esay("delegation failed:");
                esay(e);
            }
        }
    }
    
    /**
     * Reserves spaceSize bytes of the space for 
     * storage of file with the path filename
     * for future transfer from the host. <br>
     * The storage will invoke methods of the callback interface
     * to syncronously notify of the srm about the results of the reservation.
     * @param user User ID
     * @param spaceSize size of the space to be released
     * @see org.dcache.srm.ReserveSpaceCallbacks
     */
    public void reserveSpace(SRMUser user, 
            long spaceSize, 
            long reservationLifetime, 
            String filename, 
            String host, 
            ReserveSpaceCallbacks callbacks){
        DCacheUser duser = (DCacheUser) user;
        String absolute_path = srm_root+"/"+filename;
        ReserveSpaceCompanion.reserveSpace(duser,
                absolute_path,
                callbacks,
                host,
                spaceSize,
                reservationLifetime,
                this);
        
    }
    
    
    /**
     * Release spaceSize bytes of the reserved space identified with the token
     * This method returns via callbacks the size of the
     * space released in the "pool" (pool is a space storage part that can be
     * utilized in a continuous manner. In case of dcache pool is a
     * dcache pool) and the name (unique string  id) of the pool.
     * @param user User ID
     * @param spaceSize size of the space to be released
     * @param reservationToken identifier of the space
     * @param callbacks This interface is used for 
     * asyncronous notification of SRM of the
     * various actions performed to release space in the storage
     */
    public void releaseSpace( SRMUser user, long spaceSize, String spaceToken,
        ReleaseSpaceCallbacks callbacks){
        DCacheUser duser = (DCacheUser) user;
        ReleaseSpaceCompanion.releaseSpace(spaceToken,spaceSize,callbacks,this);
        
    }
    
    /**
     * Release all of the space identified with the token
     * This method returns via callbacks the size of the
     * space released in the "pool" (pool is a space storage part that can be
     * utilized in a continuous manner. In case of dcache pool is a
     * dcache pool) and the name (unique string  id) of the pool.
     * @param user User ID
     * @param reservationToken identifier of the space
     * @param callbacks This interface is used for asyncronous notification of SRM of the
     * various actions performed to release space in the storage
     */
    public void releaseSpace( SRMUser user,  String spaceToken, 
        ReleaseSpaceCallbacks callbacks){
        DCacheUser duser = (DCacheUser) user;
        ReleaseSpaceCompanion.releaseSpace(spaceToken,callbacks,this);
        
    }
    
    
    private StorageElementInfo storageElementInfo= new StorageElementInfo();
    
    public StorageElementInfo getStorageElementInfo(SRMUser user) throws SRMException {
        synchronized (storageElementInfo) {
            return storageElementInfo;
        }
    }
    java.util.List pools = new java.util.LinkedList();
    private StorageElementInfo getStorageElementInfo() throws SRMException      {
        synchronized (storageElementInfo) {
            return storageElementInfo;
        }
    }
    
    
    private void updateStorageElementInfo() throws SRMException      {
        try{
            PoolManagerGetPoolListMessage getPoolsQuery = new
                    PoolManagerGetPoolListMessage();
            getPoolsQuery.setReplyRequired(true);
            CellMessage getPoolMessage = new CellMessage( _poolMgrPath , getPoolsQuery ) ;
            getPoolMessage = sendAndWait(  getPoolMessage ,
                    __poolManagerTimeout*1000 ) ;
            
            if(getPoolMessage != null) {
                Object o = getPoolMessage.getMessageObject();
                if(o != null && o instanceof  PoolManagerGetPoolListMessage) {
                    getPoolsQuery = (PoolManagerGetPoolListMessage)o;
                    
                    StorageElementInfo info = new StorageElementInfo();
                    List newPools = getPoolsQuery.getPoolList();
                    if(!newPools.isEmpty() ) {
                        pools = newPools;
                    } else {
                        esay("receieved an empty pool list from the pool manager," +
                            "using the prevois list");
                    }
                } else {
                    esay("poolManager returned o="+o+
                        ", using previosly saved pool list");
                }
            } else {
                esay("poolManager timeout expired, using previosly saved pool list");
            }
        } catch(Exception e) {
            esay(e);
            esay( "poolManager error, using previosly saved pool list") ;
        }
        
        StorageElementInfo info = new StorageElementInfo();
        for( Iterator i = pools.iterator(); i.hasNext();) {
            try {
                StorageElementInfo poolInfo = getPoolInfo((String)(i.next()));
                info.availableSpace += poolInfo.availableSpace;
                info.totalSpace += poolInfo.totalSpace;
                info.usedSpace += poolInfo.usedSpace;
            } catch(Exception e) {
                esay(e);
                esay("can not get info from pool "+i.next()+
                    " , contunue with the rest of the pools");
            }
        }
        
        synchronized (storageElementInfo) {
            storageElementInfo = info;
        }
    }
    
    
    
    /**
     * we use run method to update the storage info structure periodically
     */
    public void run() {
        while(true) {
            try {
                updateStorageElementInfo();
            } catch(SRMException srme){
                esay(srme);
            }
            try {
                Thread.sleep(config.getStorage_info_update_period());
            } catch(InterruptedException ie) {
                esay(ie);
                esay("exiting");
                return;
            }
        }
    }
    
    
    public String[] listNonLinkedDirectory(SRMUser user,
            String directoryName) throws SRMException {
        
        String actualPath       = srm_root+"/"+directoryName;
        FsPath fsPath           = new FsPath(actualPath);
        String pnfsPath         = fsPath.toString();
        diskCacheV111.util.FileMetaData pathFmd    = null;
        StorageInfo storageInfo = null;
        PnfsGetStorageInfoMessage storageInfoMessage =null;
        PnfsGetFileMetaDataMessage metadataMessage = null;
        PnfsId pnfsId;
        DCacheUser duser = (DCacheUser) user;
        
        try {
            storageInfoMessage = _pnfs.getStorageInfoByPath(actualPath);
        } catch ( CacheException ce1 ) {
            try {
                metadataMessage  = _pnfs.getFileMetaDataByPath(actualPath);
            } catch (CacheException ce) {
                esay("getFileMetaDataByPath failed"+ce.getMessage());
                throw new SRMInvalidPathException(pnfsPath+" : "+ce.getMessage());
            } catch (Exception e) {
                esay(e);
                throw new SRMInvalidPathException(pnfsPath+" : "+e.toString());
            }
        }
        
        if ( storageInfoMessage != null ) {
            storageInfo = storageInfoMessage.getStorageInfo();
            pathFmd     = storageInfoMessage.getMetaData();
            pnfsId      = storageInfoMessage.getPnfsId();
        } else if ( metadataMessage != null) {
            pathFmd = metadataMessage.getMetaData();
            pnfsId  = metadataMessage.getPnfsId();
        } else {
            esay("could not get storage info or file metadata by path ");
            throw new SRMInvalidPathException(
                    "could not get storage info or file metadata by path ");
        }
        
        if(!pathFmd.isDirectory())  {
            throw new SRMInvalidPathException("pnfsPath is not a directory!");
        }
        
        boolean canDelete=false;
        try {
            canDelete=permissionHandler.canDeleteDir(duser.getUid(), 
                duser.getGid(), pnfsPath);
        } catch( CacheException ce) {
            esay(ce);
            throw new SRMAuthorizationException("can't delete :"+ce.getMessage());
        }
        
        if ( !canDelete ) {
            esay("can't delete directory "+pnfsPath);
            throw new SRMAuthorizationException("can't delete");
        }
        say(pnfsPath+" dir: "+pathFmd.isDirectory()+" link: "+
            pathFmd.isSymbolicLink()+" regular: "+pathFmd.isRegularFile());
        if(!pathFmd.isSymbolicLink())  {
            java.io.File dirFile = new java.io.File(pnfsPath);
            return dirFile.list();
        } else {
            return null;
        }
        
    }
    
    public java.io.File[] listDirectoryFiles(SRMUser user, String directoryName,
            FileMetaData fileMetaData) throws SRMException {
        
        String actualFilePath = srm_root+"/"+directoryName;
        FsPath pnfsPathFile = new FsPath(actualFilePath);
        String pnfsPath = pnfsPathFile.toString();
        diskCacheV111.util.FileMetaData util_fmd = null;
        if(fileMetaData != null && fileMetaData instanceof DcacheFileMetaData) {
            util_fmd =  ((DcacheFileMetaData)fileMetaData).getFmd();
        }
        
        if(util_fmd == null) {
            diskCacheV111.vehicles.PnfsGetFileMetaDataMessage metadataMessage;
            try {
                metadataMessage =
                        _pnfs.getFileMetaDataByPath(pnfsPath);
            }catch(CacheException ce) {
                throw new SRMException(ce);
            }
            
            if(metadataMessage.getReturnCode() != 0) {
                throw new SRMException(
                        "listDirectory("+pnfsPath+
                    "): can't get pnfs metadata: " +
                    "metadataMessage.getReturnCode() != 0, error="+
                        metadataMessage.getErrorObject());
            }
            util_fmd =metadataMessage.getMetaData();
        }
        if(!util_fmd.isDirectory())  {
            throw new SRMException("pnfsPath is not a directory!");
        }
        DCacheUser duser = (DCacheUser) user;
        try {
            if(!permissionHandler.dirCanRead(
                    duser.getUid(),
                    duser.getGid(),
                    actualFilePath, util_fmd)) {
                throw new SRMException("listDirectory("+pnfsPath+"): directory " +
                    "listing is not allowed ");
            }
        } catch(CacheException ce) {
            throw new SRMException("listDirectory("+pnfsPath+")",ce);
        }
        java.io.File dirFile = new java.io.File(pnfsPath);
	if (!dirFile.exists()) {
	    throw new SRMInternalErrorException("path="+pnfsPath+
                " does not exist, possibly pnfs is not mounted on SRM server " +
                "host, contact SRM administrator \n" );
	}
        return dirFile.listFiles();
    }
    
    public String[] listDirectory(SRMUser user, String directoryName,
            FileMetaData fileMetaData) throws SRMException {
        
        String actualFilePath = srm_root+"/"+directoryName;
        FsPath pnfsPathFile = new FsPath(actualFilePath);
        String pnfsPath = pnfsPathFile.toString();
        diskCacheV111.util.FileMetaData util_fmd = null;
        if(fileMetaData != null && fileMetaData instanceof DcacheFileMetaData) {
            util_fmd =  ((DcacheFileMetaData)fileMetaData).getFmd();
        }
        
        if(util_fmd == null) {
            diskCacheV111.vehicles.PnfsGetFileMetaDataMessage metadataMessage;
            try {
                metadataMessage =
                        _pnfs.getFileMetaDataByPath(pnfsPath);
            }catch(CacheException ce) {
                throw new SRMException(ce);
            }
            
            if(metadataMessage.getReturnCode() != 0) {
                throw new SRMException(
                        "listDirectory("+pnfsPath+"): can't get pnfs metadata: " +
                    "metadataMessage.getReturnCode() != 0, error="+
                        metadataMessage.getErrorObject());
            }
            util_fmd =metadataMessage.getMetaData();
        }
        if(!util_fmd.isDirectory())  {
            throw new SRMException("pnfsPath is not a directory!");
        }
        DCacheUser duser = (DCacheUser) user;
        try {
            if(!permissionHandler.dirCanRead(
                    duser.getUid(),
                    duser.getGid(),
                    actualFilePath, util_fmd)) {
                throw new SRMException("listDirectory("+pnfsPath+"): directory " +
                    "listing is not allowed ");
            }
        } catch(CacheException ce) {
            throw new SRMException("listDirectory("+pnfsPath+")",ce);
        }
        java.io.File dirFile = new java.io.File(pnfsPath);
	if (!dirFile.exists()) {
	    throw new SRMInternalErrorException("path="+pnfsPath+
                " does not exist, possibly pnfs is not mounted on SRM server " +
                "host, contact SRM administrator \n");
	}
        return dirFile.list();
    }
    
    public void srmReserveSpace(SRMUser user,
            long sizeInBytes,
            long spaceReservationLifetime,
            String retentionPolicy,
            String accessLatency,
            String description,
            SrmReserveSpaceCallbacks callbacks) {
        DCacheUser duser = (DCacheUser) user;
        
        SrmReserveSpaceCompanion.reserveSpace(
                duser,
                sizeInBytes,
                spaceReservationLifetime,
                retentionPolicy,
                accessLatency,
                description,
                callbacks,
                this);
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
        
        DCacheUser duser = (DCacheUser) user;
        SrmReleaseSpaceCompanion.releaseSpace(duser,
                longSpaceToken,
                releaseSizeInBytes,
                callbacks,
                this);
    }
    
    public void srmMarkSpaceAsBeingUsed(SRMUser user,
            String spaceToken,
            String fileName,
            long sizeInBytes,
            long useLifetime,
            SrmUseSpaceCallbacks callbacks) {
        long longSpaceToken;
        try {
            longSpaceToken = Long.parseLong(spaceToken);
        } catch(Exception e){
            callbacks.SrmUseSpaceFailed("invalid space token="+spaceToken);
            return;
        }
        
        DCacheUser duser = (DCacheUser) user;
        String actualFilePath = srm_root+"/"+fileName;
        SrmMarkSpaceAsBeingUsedCompanion.markSpace(
                duser,
                longSpaceToken,
                actualFilePath,
                sizeInBytes,
                useLifetime,
                callbacks,
                this);
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
        DCacheUser duser = (DCacheUser) user;
        String actualFilePath = srm_root+"/"+fileName;
        SrmUnmarkSpaceAsBeingUsedCompanion.unmarkSpace(
                duser,
                longSpaceToken,
                actualFilePath,
                callbacks,
                this);
    }
    
    public class LoginBrokerHandler implements Runnable {
        private String _srmLoginBroker        = null ;
        private String _protocolFamily     = null ;
        private String _protocolVersion    = null ;
        private long   _brokerUpdateTime   = 5*60*1000 ;
        private double _brokerUpdateOffset = (double)0.1 ;
        private LoginBrokerInfo _info      = null ;
        private double _currentLoad        = 0.0 ;
        private LoginBrokerHandler(){
            
            _srmLoginBroker = _args.getOpt( "srmLoginBroker" ) ;
            if( _srmLoginBroker == null )return;
            
            _protocolFamily    = _args.getOpt("protocolFamily" ) ;
            if( _protocolFamily == null )_protocolFamily = "SRM" ;
            _protocolVersion = _args.getOpt("protocolVersion") ;
            if( _protocolVersion == null )_protocolVersion = "0.1" ;
            String tmp = _args.getOpt("brokerUpdateTime") ;
            if(tmp != null) {
                try{
                    _brokerUpdateTime = Long.parseLong(tmp) * 1000 ;
                }catch(Exception e )
                { 
                    esay(e);
                }
            }
            tmp = _args.getOpt("brokerUpdateOffset") ;
            if(tmp != null) {
                try{
                    _brokerUpdateOffset = Double.parseDouble(tmp) ;
                }catch(Exception e ){ 
                     esay(e);   
                }
            }
            
            _info = new LoginBrokerInfo(
                    getNucleus().getCellName() ,
                    getNucleus().getCellDomainName() ,
                    _protocolFamily ,
                    _protocolVersion ,
                    Storage.this.getClass().getName() ) ;
            
            _info.setUpdateTime( _brokerUpdateTime ) ;
            
            getNucleus().newThread( this , "loginBrokerHandler" ).start() ;
            
        }
        public void run(){
            try{
                synchronized(this){
                    while( ! Thread.interrupted() ){
                        try{
                            runUpdate() ;
                        }catch(Exception ie){
                            esay("Login Broker Thread reports : ");
                            esay(ie);
                        }
                        wait( _brokerUpdateTime ) ;
                    }
                }
            }catch( Exception io ){
                say( "Login Broker Thread terminated due to "+io ) ;
            }
        }
        
        public String hh_lb_set_update = "<updateTime/sec>" ;
        public String ac_lb_set_update_$_1( Args args ){
            long update = Long.parseLong( args.argv(0) )*1000 ;
            if( update < 2000 )
                throw new
                        IllegalArgumentException("Update time out of range") ;
            
            synchronized(this){
                _brokerUpdateTime = update ;
                _info.setUpdateTime(update) ;
                notifyAll() ;
            }
            return "" ;
        }
        private synchronized void runUpdate(){
            
            _info.setHosts(_hosts);
            _info.setPort(config.getPort());
            if(srm != null) {
                _currentLoad = srm.getLoad();
            }
            _info.setLoad(_currentLoad);
            try{
                sendMessage(new CellMessage(new CellPath(_srmLoginBroker),_info));
//           say("Updated : "+_info);
            }catch(Exception ee){
                esay(ee);
            }
        }
        
        public void getInfo( PrintWriter pw ){
            if( _srmLoginBroker == null ){
                pw.println( "    Login Broker : DISABLED" ) ;
                return ;
            }
            pw.println( "    LoginBroker      : "+_srmLoginBroker ) ;
            pw.println( "    Protocol Family  : "+_protocolFamily ) ;
            pw.println( "    Protocol Version : "+_protocolVersion ) ;
            pw.println( "    Update Time      : "+
                (_brokerUpdateTime/1000)+" seconds" ) ;
            pw.println( "    Update Offset    : "+
                    ((int)(_brokerUpdateOffset*100.))+" %" ) ;
            
        }
        private boolean isActive(){ return _srmLoginBroker != null ; }
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
        say("srmGetSpaceMetaData");
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
        CellMessage cellMessage = new CellMessage(
                new CellPath("SrmSpaceManager"),
                getSpaces);
        try {
            cellMessage = sendAndWait(
                    cellMessage,
                    3*60*1000);
        }
        catch(Exception e){
            esay(e);
            throw new SRMException(e.getMessage());
        }
        
        if(cellMessage == null ||
                    cellMessage.getMessageObject() ==null ||
                    !(cellMessage.getMessageObject()  instanceof GetSpaceMetaData)) {
            String error = "sent GetSpaceMetaData to SrmSpaceManager, received "+
                    cellMessage == null?"null":cellMessage.getMessageObject()
                    +" back";
                esay(error );
                throw new SRMException(error);
        } 
        getSpaces = (GetSpaceMetaData)cellMessage.getMessageObject() ;
        if(getSpaces.getReturnCode() != 0) {
            esay("GetSpaceMetaData failed with rc="+getSpaces.getReturnCode()+ 
                " error="+getSpaces.getErrorObject());
            throw new SRMException("GetSpaceMetaData failed with rc="+
                getSpaces.getReturnCode()+ " error="+getSpaces.getErrorObject());
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
                    lifetimeleft = (int)(space.getCreationTime() +
                                lifetime - System.currentTimeMillis())/1000;
                    lifetimeleft= lifetimeleft < 0? 0: lifetimeleft;
                    spaceMetaDatas[i].setLifetimeAssigned(new Integer((int)(
                            lifetime/1000)));
                    spaceMetaDatas[i].setLifetimeLeft(new Integer
                            (lifetimeleft));
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
                    TStatusCode.SRM_FAILURE,"space not found"));
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
        throws SRMException {
        
        say("srmGetSpaceTokens ("+description+")");
       GetSpaceTokens getTokens = new GetSpaceTokens(user.getVoGroup(), 
           user.getVoRole(),description);
        CellMessage cellMessage = new CellMessage(
                new CellPath("SrmSpaceManager"),
                getTokens);
        try {
            cellMessage = sendAndWait(
                    cellMessage,
                    3*60*1000);
        }
        catch(Exception e){
            esay(e);
            throw new SRMException(e.getMessage());
        }
         if(cellMessage == null ||
            cellMessage.getMessageObject() ==null ||
            !(cellMessage.getMessageObject()  instanceof GetSpaceTokens)) {
            String error = "sent GetSpaceTokens to SrmSpaceManager, received "+
                    cellMessage==null?"null":cellMessage.getMessageObject() +" back";
                esay(error);
                throw new SRMException(error);
        } 
        getTokens = (GetSpaceTokens)cellMessage.getMessageObject() ;
        if(getTokens.getReturnCode() != 0) {
            esay("GetSpaceTokens failed with rc="+getTokens.getReturnCode()+ 
                " error="+getTokens.getErrorObject());
            throw new SRMException("GetSpaceTokens failed with rc="+
                getTokens.getReturnCode()+ " error="+getTokens.getErrorObject());
        }
       long tokens[] =  getTokens.getSpaceTokens();
       String tokenStrings[] =new String[tokens.length];
       for(int i =0; i<tokens.length; ++i) {
           tokenStrings[i] = Long.toString(tokens[i]);
           say("srmGetSpaceTokens returns token#"+i+" : "+tokenStrings[i]);
       }
        
       return tokenStrings;
    }
    
    public String[] srmGetRequestTokens(SRMUser user,String description)
        throws SRMException {
        try {
            Set tokens = srm.getBringOnlineRequestIds((RequestUser) user,
                    description);
            tokens.addAll(srm.getGetRequestIds((RequestUser) user,
                    description));
            tokens.addAll(srm.getPutRequestIds((RequestUser) user,
                    description));
            tokens.addAll(srm.getCopyRequestIds((RequestUser) user,
                    description));
            Long[] tokenLongs = (Long[]) tokens.toArray(new Long[0]);
            String[] tokenStrings = new String[tokenLongs.length];
            for(int i=0;i<tokenLongs.length;++i) {
                tokenStrings[i] = tokenLongs[i].toString();
            }
            return tokenStrings;    
        } catch (Exception e) {
            esay("srmGetRequestTokens failed:");
            esay(e);
            throw new SRMException(e);
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
    public int srmExtendSurlLifetime(SRMUser user, 
        String fileName, int newLifetime) throws SRMException {
        FileMetaData fmd = getFileMetaData(user,fileName);
        int uid = Integer.parseInt(fmd.owner);
        int gid = Integer.parseInt(fmd.group);
        int permissions = fmd.permMode;
        
        if(Permissions.worldCanWrite(permissions)) {
            return -1;
        }
        
        if(uid == -1 || gid == -1) {
            throw new SRMAuthorizationException(
                "User is not authorized to modify this file");
        }
        
        if(user == null || (!(user instanceof DCacheUser))) {
            throw new SRMAuthorizationException(
                "User is not authorized to modify this file");
        }
        DCacheUser duser = (DCacheUser) user;
        
        if(duser.getGid() == gid && Permissions.groupCanWrite(permissions)) {
            return -1;
        }
        
        if(duser.getUid() == uid && Permissions.userCanWrite(permissions)) {
            return -1;
        }
        
        throw new SRMAuthorizationException(
            "User is not authorized to modify this file");

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
    public long srmExtendReservationLifetime(SRMUser user, 
        String spaceToken, long newReservationLifetime) throws SRMException {
        long longSpaceToken;
        try {
            longSpaceToken = Long.parseLong(spaceToken);
        } catch(Exception e){
            throw new SRMException("srmExtendReservationLifetime failed",e);
        }
        ExtendLifetime extendLifetime =
                new ExtendLifetime( longSpaceToken, newReservationLifetime);
        
        try {
            CellMessage response =  sendAndWait( new CellMessage(
                    new CellPath( "SrmSpaceManager") ,
                    extendLifetime ) ,
                    60*60*1000
                    );
            if(response == null) {
                throw new SRMException("srmExtendReservationLifetime response " +
                    "lifetime expired");
            }
            extendLifetime = (ExtendLifetime) response.getMessageObject();
            //say("StageAndPinCompanion: recordAsPinned");
            //rr.recordAsPinned (_fr,true);
        }catch(Exception ee ) {
            esay(ee);
            throw new SRMException("srmExtendReservationLifetime failed",ee);
        }
        
        if(extendLifetime.getReturnCode() != 0) {
            throw new SRMException("srmExtendReservationLifetime failed, " +
                "ExtendLifetime.returnCode="+
                    extendLifetime.getReturnCode()+" errorObject = "+
                extendLifetime.getErrorObject());
        }
        return extendLifetime.getNewLifetime();
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
        String fileId, String pinId, long newPinLifetime) throws SRMException {
        PnfsId pnfsId = null;
        try {
            pnfsId = new PnfsId(fileId);
        } catch(Exception e) {
            esay(e);
            throw new SRMException("extendPinLifetime failed",e);
        }

        
        PinManagerExtendLifetimeMessage extendLifetime =
            new PinManagerExtendLifetimeMessage( 
            pnfsId, pinId,newPinLifetime);
        
        try {
            CellMessage response =  sendAndWait( new CellMessage(
                    new CellPath( "PinManager") ,
                    extendLifetime ) ,
                    60*60*1000
                    );
            if(response == null) {
                throw new SRMException(
                    "PinManagerExtendLifetimeMessage response lifetime expired");
            }
            extendLifetime = 
                (PinManagerExtendLifetimeMessage) response.getMessageObject();
            //say("StageAndPinCompanion: recordAsPinned");
            //rr.recordAsPinned (_fr,true);
        }catch(Exception ee ) {
            esay(ee);
            throw new SRMException("extendPinLifetime failed",ee);
        }
        
        if(extendLifetime.getReturnCode() != 0) {
            throw new SRMException(
                "extendPinLifetime failed, PinManagerExtendLifetimeMessage.returnCode="+
                extendLifetime.getReturnCode()+
                " errorObject = "+extendLifetime.getErrorObject());
        }
        return extendLifetime.getNewLifetime();
    }

    public String getStorageBackendVersion() { 
        return diskCacheV111.util.Version.getVersion(); 
    } 
}

// $Log: not supported by cvs2svn $
// Revision 1.150  2007/10/19 20:57:04  tdh
// Merge of caching of gPlazma authorization in srm.
//
// Revision 1.149  2007/10/17 22:10:08  litvinse
// modify getFileMetadata so that it does not throw exception if user do
// not have permission to read its own file. This led to situation when
// user can remove permissions from file and can't modify them at all
//
// Revision 1.148  2007/10/17 21:34:46  litvinse
// fix createDirectory method to inherit permissions mask from parent directory
//
// Revision 1.147  2007/10/03 20:29:14  litvinse
// throw SRMAuthorisation exeption in getTurl
//
// Revision 1.146  2007/09/28 20:05:30  litvinse
// better text of exception message
//
// Revision 1.145  2007/09/28 19:03:31  litvinse
// make sure we test dirFile.exists()
//
// Revision 1.144  2007/09/14 21:12:47  timur
// rename srmSpaceManager option into srmImplicitSpaceManagerEnabled, make its
// value set to yes by default is srmSpaceManagerEnabled=yes and always set to 
// no if srmImplicitSpaceManagerEnabled=no
//
// Revision 1.143  2007/09/13 19:48:02  timur
// return SRM AUTHORIZATION or INVALID PATH errors instead of generic 
// SRM_FAILURE in several instances
//
// Revision 1.142  2007/08/28 17:00:14  timur
// commiting Gerd's patch that unifies and simplifies getFileMetaData code
//
// Revision 1.141  2007/08/22 23:06:15  timur
// make srmMkDir and SRMRmDir return SRM_AUTHORIZATION_FAILURE, 
// SRM_DUPLICATION_ERROR and SRM_INVALID_PATH when appropriate
//
// Revision 1.140  2007/08/22 20:29:53  timur
// space manager understand lifetime=-1 as infinite, get-space-tokens 
// does not check ownership, reserve space admin command takes lifetime 
// in seconds, or -1 for infinite
//
// Revision 1.139  2007/08/16 14:19:14  tigran
// removed obsolete try{} catch block
//
// Revision 1.138  2007/08/16 14:17:06  tigran
// prevent null pointer exception in case of CRC information not available
//
// Revision 1.137  2007/08/08 22:05:44  timur
// better parameter handling
//
// Revision 1.136  2007/08/03 20:20:02  timur
// implementing some of the findbug bugs and recommendations, 
// avoid selfassignment, possible nullpointer exceptions, 
// syncronization issues, etc
//
// Revision 1.135  2007/08/03 15:46:03  timur
// closing sql statement, implementing hashCode functions, not 
// passing null args, resing classes etc, per findbug recommendations
//
// Revision 1.134  2007/07/16 21:56:02  timur
// make sure the empty pgpass is ignored
//
// Revision 1.133  2007/06/18 21:37:48  timur
// better reporting of the expired space reservations, better singletons
//
// Revision 1.132  2007/05/24 13:51:09  tigran
// merge of 1.7.1 and the head
//
// Revision 1.131  2007/05/22 21:24:03  timur
// commiting the BNL workaround for the multihomed machine reverse DNS 
// lookup that standard INetAddress does not always handle. 
// The new procedure is not used if the flag srmCustomGetHostByAddr 
// is disabled (default). Also it is used only if standard procedure has failed
//
// Revision 1.130  2007/05/15 01:53:50  timur
// return 0 instead of negative number, in case of expired space reservation, 
// more debug info for getSpaceTokens
//
// Revision 1.129  2007/04/13 17:02:58  litvinse
// *** empty log message ***
//
// Revision 1.128  2007/04/09 22:48:28  timur
// added a flag that controls default overwrite behaviour, ouside of srmV2 
// specific reaction to the overwrite mode
//
// Revision 1.127  2007/04/06 22:07:30  timur
// options to enable srm v2.2 type space reservations and overwrites
//
// Revision 1.126  2007/04/03 21:42:57  timur
// select transfer url protocol on basis of the order of the protocols 
// supplied by the clients
//
// Revision 1.125  2007/04/02 21:53:57  litvinse
// *** empty log message ***
//
// Revision 1.124  2007/03/27 23:42:08  timur
// fixed overwrite mode support related bug
//
// Revision 1.123  2007/03/26 21:21:24  litvinse
// changes to getFileMetadata to extract and fill retention policy and
// access latency
//
// Revision 1.122  2007/03/23 23:17:29  litvinse
// fill retentionpolicy and accesslatency field
//
// Revision 1.121  2007/03/10 00:13:21  timur
// started work on adding support for optional overwrite
//
// Revision 1.120  2007/03/08 23:37:17  timur
// merging changes from the 1-7 branch related to database performance and 
// reduced usage of database when monitoring is not used
//
// Revision 1.119  2007/03/07 01:22:03  timur
// adding options to control maximum length of jdbs tasks queue and number 
// of threads for execution of these tasksdiskCacheV111/srm/dcache/Storage.java
//
// Revision 1.118  2007/03/03 00:44:17  timur
//  make srm reserve space and space get metadata return correct values
//
// Revision 1.117  2007/03/01 00:38:04  timur
// make mv of file into itself  always successful
//
// Revision 1.116  2007/02/23 17:05:23  timur
// changes to comply with the spec and appear green on various tests, 
// mostly propogating the errors as correct SRM Status Codes, filling in correct 
// fields in srm ls, etc
//
// Revision 1.115  2007/02/20 01:47:00  timur
// removed duplicate
//
// Revision 1.114  2007/02/17 05:49:28  timur
// propagate SRM_NO_FREE_SPACE to reserveSpace
//
// Revision 1.113  2007/02/10 04:48:15  timur
//  first version of SrmExtendFileLifetime
//
// Revision 1.111  2007/01/19 00:47:03  timur
// ArrayStoreException problem resolved
//
// Revision 1.110  2007/01/10 23:32:40  timur
// eliminate NullPointerException on startup
//
// Revision 1.109  2007/01/10 23:05:53  timur
// implemented srmGetRequestTokens, store request description in database, 
// fixed several srmv2 issues
//
// Revision 1.108  2007/01/06 00:25:02  timur
// merging production branch changes to database layer to improve performance 
// and reduce number of updates
//
// Revision 1.107  2006/12/27 21:41:07  litvinse
// add stacktrace to log file if pnfs operation fails
//
// Revision 1.106  2006/12/15 16:08:44  tdh
// Added code to make delegation from cell to gPlazma optional, through the 
// batch file parameter "delegate-to-gplazma". Default is to not delegate.
//
// Revision 1.105  2006/11/16 16:52:45  litvinse
// bug fix
//
// Revision 1.104  2006/11/16 00:15:14  litvinse
// bug fix in setFileMetadata
//
// Revision 1.103  2006/11/14 22:39:05  timur
// started getSpaceTokens implementation
//
// Revision 1.102  2006/11/13 18:45:17  litvinse
// implemented SetPermission
//
// Revision 1.101  2006/11/10 22:58:46  litvinse
// introduced setFileMetadata function to prapare for SrmSetPermission
//
// Revision 1.100  2006/11/09 22:32:54  timur
// implementation of SrmGetSpaceMetaData function
//
// Revision 1.99  2006/10/24 07:47:51  litvinse
// moved JobAppraiser interface into separate package
// added handles to select scheduler priority policies
//
// Revision 1.98  2006/10/10 21:04:36  timur
// srmBringOnline related changes
//
// Revision 1.97  2006/10/02 23:31:42  timur
// import change
//
// Revision 1.96  2006/09/25 20:30:58  timur
// modify srm companions and srm cell to use ThreadManager
//
// Revision 1.95  2006/09/12 16:03:06  timur
// make the maximum number of get/put request in ready state configurable 
// during the runtime
//
// Revision 1.94  2006/08/25 00:16:56  timur
// first complete version of space reservation working with srmPrepareToPut 
// and gridftp door
//
// Revision 1.93  2006/08/22 23:21:55  timur
// srmReleaseSpace is implemented
//
// Revision 1.92  2006/08/18 22:06:43  timur
// srm usage of space by srmPrepareToPut implemented
//
// Revision 1.91  2006/08/07 02:49:22  timur
// more space reservation code
//
// Revision 1.90  2006/08/02 22:09:54  timur
// more work for srm space reservation, included voGroup and voRole support
//
// Revision 1.89  2006/08/01 00:19:09  timur
// more space reservation code
//
// Revision 1.88  2006/07/04 22:23:37  timur
// Use Credential Id to reffer to the remote credential in delegation step, 
// reformated some classes
//
// Revision 1.87  2006/06/23 21:16:03  timur
// use correct transfer request ids in srm copy file request, use request 
// credential id  to refernce delegated credential
//
// Revision 1.86  2006/06/21 20:42:33  timur
// removed unused imports
//
// Revision 1.85  2006/06/09 15:33:51  tdh
// Added flag for using gplazma cell for authentification: use-gplazma-authorization-cell.
//
// Revision 1.84  2006/04/19 20:03:01  timur
// randomization and customization of the door selection mechanizm
//
// Revision 1.83  2006/04/04 17:41:54  litvinse
// fixed srm_root problem
// Revision 1.55.2.6  2006/04/18 22:14:27  timur
// making host selection more customizable
//
// Revision 1.55.2.5  2005/12/01 01:44:37  timur
// merging trunk changes to getInfo into production branch
//
// Revision 1.82  2006/03/31 23:28:11  timur
// fixed mkdir bug
//
// Revision 1.81  2006/03/16 23:15:34  timur
// remove dependency on axis classes
//
// Revision 1.80  2006/02/23 22:53:59  moibenko
// add ability to read in configuration from config.xml
//
// Revision 1.79  2006/02/02 01:32:02  timur
// check the user's root dir before attempt to copy
//
// Revision 1.78  2006/01/31 21:21:13  timur
// spelling fix
//
// Revision 1.77  2006/01/26 00:42:08  timur
// more debug info
//
// Revision 1.76  2006/01/25 18:54:10  litvinse
// fix logic
//
// Revision 1.75  2005/12/14 10:04:10  tigran
// setting cell type to class name
//
// Revision 1.74  2005/12/01 01:40:46  timur
// do not throw nullPointerException in getInfo if storageInfo is not available
//
// Revision 1.73  2005/12/01 00:38:12  timur
// do not allow execution of the blocking SendAndWait from inside 
// getStorageElementInfo, which in its turn is called in getInfo which is 
// ultinately called by CellAdapter.messageArrived
//
// Revision 1.72  2005/11/22 11:02:19  patrick
// versioning enabled.
//
// Revision 1.71  2005/11/17 01:20:03  litvinse
// tested and fixed what problems where uncovered
//
// Revision 1.70  2005/11/16 22:39:32  litvinse
// implemented mv
//
// Revision 1.69  2005/11/16 22:07:10  litvinse
// implemented Mv
//
// Revision 1.68  2005/11/15 17:08:56  litvinse
// make code a bit nicer
//
// Revision 1.67  2005/11/15 01:08:56  litvinse
// implemented SrmMkdir function
//
// Revision 1.66  2005/11/14 20:17:38  litvinse
// remove extraneous code
//
// Revision 1.65  2005/11/14 02:16:03  litvinse
// redo removeDirectory function so it is not asynchronous
//
// Revision 1.64  2005/11/12 22:10:21  litvinse
// implemented SrmRmDir
//
// WARNING: if directory is sym-link or recursion level is specified an
// 	 a subdirectory contains sym-link - it will follow it. Do not
// 	 use if there are symbolic link.
//
// Revision 1.63  2005/11/10 23:00:19  timur
// better faster srm ls in non verbose mode
//
// Revision 1.62  2005/11/10 00:02:25  timur
// srm ls related improvments
//
// Revision 1.61  2005/11/01 17:03:19  litvinse
// implemented SrmRm
//
// Revision 1.60  2005/10/25 20:22:49  timur
// parse start_server option
//
// Revision 1.59  2005/10/14 23:41:42  timur
// more code for srm v2 and axis server for srm v1
//
// Revision 1.58  2005/10/11 18:01:59  timur
// srm cell reports its load to LoginBroker specified by srmLoginBroker option
//
// Revision 1.57  2005/10/07 22:59:46  timur
// work towards v2
//
// Revision 1.56  2005/09/20 23:16:53  timur
// srm v2 deployment
//
// Revision 1.55  2005/08/30 22:09:55  timur
// fixing space path translation problem which was fixed in production tag a while ago
//
// Revision 1.54  2005/08/27 00:31:40  timur
// better error message
//
// Revision 1.53  2005/08/27 00:09:43  timur
// fixed the bug related to error handling when performing remotegridftp transfers
//
// Revision 1.52  2005/08/25 00:05:03  podstvkv
// MD5 password authentication added
//
// Revision 1.41  2005/06/02 06:16:58  timur
// changes to advisory delete behavior
//
// Revision 1.40  2005/05/20 16:50:39  timur
// adding optional usage of vo authorization module
//
// Revision 1.39  2005/05/13 22:18:53  timur
// imporved and more reliable gathering of storage info
//
// Revision 1.38  2005/05/04 21:56:48  timur
// options for new scheduling policies on startup
//
// Revision 1.37  2005/04/28 13:14:04  timur
// added options for starting vacuum thread
//
// Revision 1.36  2005/04/22 16:05:52  timur
// fixed some message timeout issues, nomalized paths in space manager
//
// Revision 1.35  2005/03/31 22:55:17  timur
// storage info is updated in a background thread
//
// Revision 1.34  2005/03/23 18:17:20  timur
// SpaceReservation working for the copy case as well
//
// Revision 1.33  2005/03/11 21:17:28  timur
// making srm compatible with cern tools again
//
// Revision 1.32  2005/03/09 23:22:29  timur
// more space reservation code
//
// Revision 1.31  2005/03/07 22:59:26  timur
// more work on space reservation
//
// Revision 1.30  2005/03/01 23:12:09  timur
// Modified the database scema to increase database operations performance and to 
// account for reserved space"and to account for reserved space
//
// Revision 1.29  2005/01/25 16:52:39  timur
// removed logging grom getStorageElementInfo function
//
// Revision 1.28  2005/01/25 05:17:31  timur
// moving general srm stuff into srm repository
//
// Revision 1.27  2005/01/07 20:55:30  timur
// changed the implementation of the built in client to use apache axis soap toolkit
//
// Revision 1.26  2004/12/14 19:37:56  timur
// fixed advisory delete permissions and client issues
//
// Revision 1.25  2004/12/10 22:14:37  timur
// fixed the cancelall commands
//
// Revision 1.24  2004/12/09 19:26:26  timur
// added new updated jglobus libraries, new group commands for canceling srm requests
//
// Revision 1.23  2004/12/02 05:30:20  timur
// new GsiftpTransferManager
//
// Revision 1.22  2004/11/17 21:56:48  timur
// adding the option which allows to store the pending or running requests in memory, 
// fixed a restore from database bug
//
// Revision 1.21  2004/11/08 23:02:40  timur
// remote gridftp manager kills the mover when the mover thread is killed,  
// further modified the srm database handling
//
// Revision 1.20  2004/11/01 22:40:34  timur
// prevent the remoteGsiftp mover to block indefinetely if the delegation is 
// not performed
//
// Revision 1.19  2004/09/15 16:49:27  timur
// create all threads in the cell's thread group
//
// Revision 1.18  2004/09/08 21:27:39  timur
// remote gsiftp transfer manager will now use ftp logger too
//
// Revision 1.17  2004/08/10 22:17:15  timur
// added indeces creation for state field, update postgres driver
//
// Revision 1.16  2004/08/10 17:04:16  timur
// more monitoring, change file request state, when request is complete
//
// Revision 1.15  2004/08/10 17:03:47  timur
// more monitoring, change file request state, when request is complete
//
// Revision 1.14  2004/08/06 19:35:23  timur
// merging branch srm-branch-12_May_2004 into the trunk
//
// Revision 1.13.2.12  2004/07/12 21:52:05  timur
// remote srm error handling is improved, minor issues fixed
//
// Revision 1.13.2.11  2004/07/02 20:10:23  timur
// fixed the leak of sql connections, added propogation of srm errors
//
// Revision 1.13.2.10  2004/06/30 20:37:23  timur
// added more monitoring functions, added retries to the srm client part, 
// adapted the srmclientv1 for usage in srmcp
//
// Revision 1.13.2.9  2004/06/28 21:54:10  timur
// added configuration options for the schedulers
//
// Revision 1.13.2.8  2004/06/25 21:39:58  timur
// first version where everything works, need much more thorough testing and 
// ability to configure scheduler better
//
// Revision 1.13.2.7  2004/06/18 22:20:51  timur
// adding sql database storage for requests
//
// Revision 1.13.2.6  2004/06/16 22:14:31  timur
// copy works for mulfile request
//
// Revision 1.13.2.5  2004/06/15 22:15:42  timur
// added cvs logging tags and fermi copyright headers at the top
//

