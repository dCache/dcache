
// $Id: SRM.java,v 1.51 2007-03-10 00:13:19 timur Exp $
// $Log: not supported by cvs2svn $
// Revision 1.50  2007/03/08 23:36:54  timur
// merging changes from the 1-7 branch related to database performance and reduced usage of database when monitoring is not used
//
// Revision 1.49  2007/03/06 23:12:53  timur
// limit queue of jdbc requests, allow multiple threads for sql request execution
//
// Revision 1.48  2007/01/10 23:00:22  timur
// implemented srmGetRequestTokens, store request description in database, fixed several srmv2 issues
//
// Revision 1.47  2006/11/11 01:16:32  timur
// propagate the StorageType from copy to srmPrepareToPut
//
// Revision 1.46  2006/10/24 07:44:32  litvinse
// moved JobAppraiser interface into separate package
// added handles to select scheduler priority policies
//
// Revision 1.45  2006/10/16 19:50:03  litvinse
// add casting to calls to ContainerRequest.getRequest() where necessary
//
// Revision 1.44  2006/10/02 23:29:15  timur
// implemented srmPing and srmBringOnline (not tested yet), refactored Request.java
//
// Revision 1.43  2006/09/15 22:39:15  timur
// work to make srmCopy  use new type of SpaceReservation
//
// Revision 1.42  2006/08/18 22:05:31  timur
// srm usage of space by srmPrepareToPut implemented
//
// Revision 1.41  2006/07/29 18:10:40  timur
// added schedulable requests for execution reserve space requests
//
// Revision 1.40  2006/06/30 15:32:13  timur
// reduced and generalized srm v2 server java code, using reflection
//
// Revision 1.39  2006/06/20 15:42:15  timur
// initial v2.2 commit, code is based on a week old wsdl, will update the wsdl and code next
//
// Revision 1.38  2006/04/18 00:53:46  timur
// added the job execution history storage for better diagnostics and profiling
//
// Revision 1.37  2006/03/30 23:17:19  timur
// added srmAbortRequest
//
// Revision 1.36  2006/03/28 00:20:47  timur
// added srmAbortFiles support
//
// Revision 1.35  2006/01/12 23:38:09  timur
// first working version of srmCopy
//
// Revision 1.34  2005/12/21 22:29:54  litvinse
// implemented srmCopy V2
//
// Revision 1.33  2005/12/14 01:49:14  litvinse
// moving towards working srmCopy
//
// Revision 1.32  2005/12/13 22:17:40  litvinse
// working on srmCopy implementation for SRM V2
//
// Revision 1.31  2005/12/12 22:35:46  timur
// more work on srmPrepareToGet and related srm v2 functions
//
// Revision 1.30  2005/12/10 00:04:01  timur
// working on srmPrepareToPut
//
// Revision 1.29  2005/12/08 01:01:12  timur
// working on srmPrepereToGet
//
// Revision 1.28  2005/12/05 23:53:18  litvinse
// minor change - override filemetadata SURL with original SURL, hence
// the "original" SURL is returned
//
// Revision 1.27  2005/12/02 22:20:51  timur
// working on srmReleaseFiles
//
// Revision 1.26  2005/11/20 02:40:10  timur
// SRM PrepareToGet and srmStatusOfPrepareToGet functions
//
// Revision 1.25  2005/11/18 20:28:18  timur
// use correct parameter for put request timeout
//
// Revision 1.24  2005/11/17 20:45:55  timur
// started work on srmPrepareToGet functions
//
// Revision 1.23  2005/11/16 22:13:37  litvinse
// implemented Mv
//
// Revision 1.22  2005/11/15 01:10:42  litvinse
// implemented SrmMkdir function
//
// Revision 1.21  2005/11/12 22:15:35  litvinse
// implemented SrmRmDir
//
// WARNING: if directory is sym-link or recursion level is specified an
// 	 a subdirectory contains sym-link - it will follow it. Do not
// 	 use if there are symbolic link.
//
// Revision 1.20  2005/11/01 17:07:16  litvinse
// implemented SrmRm
//
// Revision 1.19  2005/10/26 20:30:18  litvinse
// make sure it compiles
//
// Revision 1.18  2005/10/26 20:12:15  litvinse
// changes related to srRm
//
// Revision 1.17  2005/10/25 00:47:25  timur
// make startup of glue http server optional
//
// Revision 1.16  2005/10/18 21:03:55  timur
// modified how axis server passes credentials to srm
//
// Revision 1.15  2005/10/11 18:00:08  timur
// added load calculation
//
// Revision 1.14  2005/10/07 22:57:15  timur
// work for srm v2
//
// Revision 1.9  2005/06/02 20:48:04  timur
// better error propagation in case of advisory delete, less error printing in client
//
// Revision 1.8  2005/06/02 06:15:57  timur
// some changes to advisory delete
//
// Revision 1.7  2005/04/28 13:23:07  timur
// actually start the vacuum thread
//
// Revision 1.6  2005/04/28 13:09:58  timur
// added postgres vacuum thread
//
// Revision 1.5  2005/03/23 18:10:37  timur
// more space reservation related changes, need to support it in case of "copy"
//
// Revision 1.4  2005/03/13 21:56:28  timur
// more changes to restore compatibility
//
// Revision 1.3  2005/03/11 21:16:24  timur
// making srm compatible with cern tools again
//
// Revision 1.2  2005/03/01 23:10:38  timur
// Modified the database scema to increase database operations performance and to account for reserved space"and to account for reserved space
//
// Revision 1.1  2005/01/14 23:07:13  timur
// moving general srm code in a separate repository
//
// Revision 1.36  2004/12/14 19:37:56  timur
// fixed advisory delete permissions and client issues
//
// Revision 1.35  2004/12/10 22:14:37  timur
// fixed the cancelall commands
//
// Revision 1.34  2004/12/09 19:26:23  timur
// added new updated jglobus libraries, new group commands for canceling srm requests
//
// Revision 1.33  2004/11/17 21:56:48  timur
// adding the option which allows to store the pending or running requests in memory, fixed a restore from database bug
//
// Revision 1.32  2004/10/30 04:19:07  timur
// Fixed a problem related to the restoration of the job from database
//
// Revision 1.31  2004/10/28 02:41:30  timur
// changed the database scema a little bit, fixed various synchronization bugs in the scheduler, added interactive shell to the File System srm
//
// Revision 1.30  2004/08/10 17:03:47  timur
// more monitoring, change file request state, when request is complete
//
// Revision 1.29  2004/08/06 19:35:21  timur
// merging branch srm-branch-12_May_2004 into the trunk
//
// Revision 1.28.2.19  2004/07/29 22:17:29  timur
// Some functionality for disk srm is working
//
// Revision 1.28.2.18  2004/07/12 21:52:05  timur
// remote srm error handling is improved, minor issues fixed
//
// Revision 1.28.2.17  2004/07/02 20:10:23  timur
// fixed the leak of sql connections, added propogation of srm errors
//
// Revision 1.28.2.16  2004/06/30 20:37:22  timur
// added more monitoring functions, added retries to the srm client part, adapted the srmclientv1 for usage in srmcp
//
// Revision 1.28.2.15  2004/06/28 21:54:10  timur
// added configuration options for the schedulers
//
// Revision 1.28.2.14  2004/06/25 21:39:58  timur
// first version where everything works, need much more thorough testing and ability to configure scheduler better
//
// Revision 1.28.2.13  2004/06/24 23:03:07  timur
// put requests, put file requests and copy file requests are now stored in database, copy requests need more work
//
// Revision 1.28.2.12  2004/06/23 21:55:59  timur
// Get Requests are now stored in database, ContainerRequest Credentials are now stored in database too
//
// Revision 1.28.2.11  2004/06/22 01:38:06  timur
// working on the database part, created persistent storage for getFileRequests, for the next requestId
//
// Revision 1.28.2.10  2004/06/18 22:20:51  timur
// adding sql database storage for requests
//
// Revision 1.28.2.9  2004/06/16 22:14:31  timur
// copy works for mulfile request
//
// Revision 1.28.2.8  2004/06/15 22:15:41  timur
// added cvs logging tags and fermi copyright headers at the top
//

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

/*
 * SRM.java
 *
 * Created on January 10, 2003, 12:34 PM
 */

package org.dcache.srm;

import electric.registry.Registry;
import electric.server.http.HTTP;
import electric.net.socket.SocketFactories;
import electric.util.Context;
import electric.net.http.HTTPContext;
import org.dcache.srm.SRMAuthorization;
import org.globus.util.GlobusURL;
import org.dcache.srm.request.RequestUser;
import org.dcache.srm.request.RequestCredential;
import org.dcache.srm.request.ContainerRequest;
import org.dcache.srm.request.FileRequest;
import org.dcache.srm.util.Configuration;
import org.dcache.srm.request.ContainerRequest;
import org.dcache.srm.request.PutRequest;
import org.dcache.srm.request.GetRequest;
import org.dcache.srm.request.CopyRequest;
import org.dcache.srm.request.BringOnlineRequest;
import org.dcache.srm.request.sql.GetFileRequestStorage;
import org.dcache.srm.request.sql.GetRequestStorage;
import org.dcache.srm.request.sql.BringOnlineFileRequestStorage;
import org.dcache.srm.request.sql.BringOnlineRequestStorage;
import org.dcache.srm.request.sql.PutFileRequestStorage;
import org.dcache.srm.request.sql.PutRequestStorage;
import org.dcache.srm.request.sql.ReserveSpaceRequestStorage;
import org.dcache.srm.request.sql.CopyFileRequestStorage;
import org.dcache.srm.request.sql.CopyRequestStorage;
import org.dcache.srm.scheduler.HashtableJobCreatorStorage;
import org.dcache.srm.request.sql.DatabaseRequestCredentialStorage;
import org.dcache.srm.util.Tools;
import java.util.Set;
import java.util.Iterator;
import java.io.IOException;
import java.net.InetAddress;

import org.dcache.srm.scheduler.Scheduler;
import org.dcache.srm.scheduler.State;
import org.dcache.srm.scheduler.IllegalStateTransition;
import org.dcache.srm.scheduler.Job;
import org.dcache.srm.scheduler.JobStorage;
import org.dcache.srm.scheduler.JobCreator;
import org.dcache.srm.scheduler.JobCreatorStorage;
import org.dcache.srm.request.RequestCredential;
import org.dcache.srm.request.RequestCredentialStorage;
import diskCacheV111.srm.server.SRMServerV1;

import diskCacheV111.srm.FileMetaData;
import diskCacheV111.srm.RequestStatus;
import diskCacheV111.srm.RequestFileStatus;
import diskCacheV111.srm.StorageElementInfo;

import org.dcache.srm.v2_2.SrmLsRequest;
import org.dcache.srm.v2_2.SrmLsResponse;

import org.dcache.srm.v2_2.SrmRmRequest;
import org.dcache.srm.v2_2.SrmRmResponse;

import org.dcache.srm.v2_2.SrmPrepareToGetRequest;
import org.dcache.srm.v2_2.SrmPrepareToGetResponse;

import org.dcache.srm.v2_2.SrmMvRequest;
import org.dcache.srm.v2_2.SrmMvResponse;

import org.dcache.srm.v2_2.SrmRmdirRequest;
import org.dcache.srm.v2_2.SrmRmdirResponse;

import org.dcache.srm.v2_2.SrmMkdirRequest;
import org.dcache.srm.v2_2.SrmMkdirResponse;

import org.dcache.srm.v2_2.SrmStatusOfGetRequestRequest;
import org.dcache.srm.v2_2.SrmStatusOfGetRequestResponse;

import org.dcache.srm.v2_2.SrmStatusOfCopyRequestRequest;
import org.dcache.srm.v2_2.SrmStatusOfCopyRequestResponse;

import org.dcache.srm.v2_2.SrmReleaseFilesRequest;
import org.dcache.srm.v2_2.SrmReleaseFilesResponse;

import org.dcache.srm.v2_2.SrmPrepareToPutRequest;
import org.dcache.srm.v2_2.SrmPrepareToPutResponse;

import org.dcache.srm.v2_2.SrmCopyRequest;
import org.dcache.srm.v2_2.SrmCopyResponse;

import org.dcache.srm.v2_2.SrmStatusOfPutRequestRequest;
import org.dcache.srm.v2_2.SrmStatusOfPutRequestResponse;

import org.dcache.srm.v2_2.SrmPutDoneRequest;
import org.dcache.srm.v2_2.SrmPutDoneResponse;

import org.dcache.srm.v2_2.SrmAbortFilesRequest;
import org.dcache.srm.v2_2.SrmAbortFilesResponse;

import org.dcache.srm.v2_2.SrmAbortRequestRequest;
import org.dcache.srm.v2_2.SrmAbortRequestResponse;

/**
 * SRM class creates an instance of SRM client class and publishes it on a
 * given port as a storage element.
 *
 * @author  timur
 */
public class SRM {
    private String name;
    private InetAddress  host;
    private SRMAuthorization authorization;
    private SRMServerV1 server;
    private Configuration configuration;
    private Scheduler bringOnlineRequestScheduler;
    private Scheduler getRequestScheduler;
    private Scheduler putRequestScheduler;
    private Scheduler copyRequestScheduler;
    private Scheduler reserveSpaceScheduler;
    private RequestCredentialStorage requestCredentialStorage;
    //    private RequestScheduler putRequestScheduler;
    //    private RequestScheduler copyRequestScheduler;
    
    /** Creates a new instance of SRM
     *
     * @param  srcSURLS
     *         array of source SURL (Site specific URL) strings
     * @param  storage
     *         implemntation of abstract storage gives the methods
     *         to communicate to underlying storage
     * @param  port
     *         what port to publish the Web Service interface to SRM
     * @param  pathPrefix
     *         all path given by SRM users will be prepandes with this string
     * @param  proxies_directory
     *         directory where the user proxies will be stored temporarily
     * @param  url_copy_command
     *         path to the script or program with the same functionality and
     *         semantics as globus-url-copy. It will be used for performing
     *         MSS-to-MSS transfers
     * @param  wantPerm
     *         array of boolean values indicating if permonent copies are
     *         desired
     */
    
    /** Creates a new instance of SRM
     *
     * @param  config
     *         all srm config options
     */
    private BringOnlineRequestStorage bringOnlineStorage;
    private GetRequestStorage getStorage;
    private PutRequestStorage putStorage;
    private CopyRequestStorage copyStorage;
    private BringOnlineFileRequestStorage bringOnlineFileRequestStorage;
    private GetFileRequestStorage getFileRequestStorage;
    private PutFileRequestStorage putFileRequestStorage;
    private CopyFileRequestStorage copyFileRequestStorage;
    private ReserveSpaceRequestStorage reserveSpaceRequestStorage;
    private AbstractStorageElement storage;
    /**
     * if multiple srm installations live within same storage, they should have different names
     */
    public SRM(Configuration config,String name) 
        throws java.io.IOException,
        java.sql.SQLException,
        InterruptedException,
        org.dcache.srm.scheduler.IllegalStateTransition,
        electric.xml.ParseException
    {
        this.configuration = config;
        this.storage  = config.getStorage();
        this.name = name;

        // these jdbc parameters need to be set before the 
        // first jdbc instance is created
        // so we do it before everything else
        
        if(configuration.getMaxQueuedJdbcTasksNum() != null ) {
            say("setMaxJdbcTasksNum to "+
                    configuration.getMaxQueuedJdbcTasksNum().intValue());
            org.dcache.srm.request.sql.JdbcConnectionPool.setMaxQueuedJdbcTasksNum(
                    configuration.getMaxQueuedJdbcTasksNum().intValue());
        }
        
        if(configuration.getJdbcExecutionThreadNum() != null ) {
            say("set JDBC ExecutionThreadNum to "+
                    configuration.getJdbcExecutionThreadNum().intValue());
            org.dcache.srm.request.sql.JdbcConnectionPool.setExecutionThreadNum(
                    configuration.getJdbcExecutionThreadNum().intValue());
        }
        
        if(config.isGsissl()) {
            String protocol_property=System.getProperty("java.protocol.handler.pkgs");
            if(protocol_property == null) {
                protocol_property = "org.globus.net.protocol";
            }
            else {
                protocol_property = protocol_property+"|org.globus.net.protocol";
            }
            System.setProperty("java.protocol.handler.pkgs",protocol_property);
        }
        
        //config.setLocalSRM(this);
        requestCredentialStorage = new DatabaseRequestCredentialStorage(config);
        RequestCredential.registerRequestCredentialStorage(requestCredentialStorage);
        
        getRequestScheduler = new Scheduler("get_"+name,storage);
        // scheduler parameters
        getRequestScheduler.setMaxThreadQueueSize(config.getGetReqTQueueSize());
        getRequestScheduler.setThreadPoolSize(config.getGetThreadPoolSize());
        getRequestScheduler.setMaxWaitingJobNum(config.getGetMaxWaitingRequests());
        getRequestScheduler.setMaxReadyQueueSize(config.getGetReadyQueueSize());
        getRequestScheduler.setMaxReadyJobs(config.getGetMaxReadyJobs());
        getRequestScheduler.setMaxNumberOfRetries(config.getGetMaxNumOfRetries());
        getRequestScheduler.setRetryTimeout(config.getGetRetryTimeout());
        getRequestScheduler.setMaxRunningByOwner(config.getGetMaxRunningBySameOwner());
	getRequestScheduler.setPriorityPolicyPlugin(config.getGetPriorityPolicyPlugin());
        getRequestScheduler.start();

        
        bringOnlineRequestScheduler = new Scheduler("bring_online_"+name,storage);
        // scheduler parameters
        bringOnlineRequestScheduler.setMaxThreadQueueSize(config.getGetReqTQueueSize());
        bringOnlineRequestScheduler.setThreadPoolSize(config.getGetThreadPoolSize());
        bringOnlineRequestScheduler.setMaxWaitingJobNum(config.getGetMaxWaitingRequests());
        bringOnlineRequestScheduler.setMaxReadyQueueSize(config.getGetReadyQueueSize());
        bringOnlineRequestScheduler.setMaxReadyJobs(config.getGetMaxReadyJobs());
        bringOnlineRequestScheduler.setMaxNumberOfRetries(config.getGetMaxNumOfRetries());
        bringOnlineRequestScheduler.setRetryTimeout(config.getGetRetryTimeout());
        bringOnlineRequestScheduler.setMaxRunningByOwner(config.getGetMaxRunningBySameOwner());
        bringOnlineRequestScheduler.setPriorityPolicyPlugin(config.getGetPriorityPolicyPlugin());
        bringOnlineRequestScheduler.start();
        
        
        putRequestScheduler = new Scheduler("put_"+name,storage);
        // scheduler parameters
        putRequestScheduler.setMaxThreadQueueSize(config.getPutReqTQueueSize());
        putRequestScheduler.setThreadPoolSize(config.getPutThreadPoolSize());
        putRequestScheduler.setMaxWaitingJobNum(config.getPutMaxWaitingRequests());
        putRequestScheduler.setMaxReadyQueueSize(config.getPutReadyQueueSize());
        putRequestScheduler.setMaxReadyJobs(config.getPutMaxReadyJobs());
        putRequestScheduler.setMaxNumberOfRetries(config.getPutMaxNumOfRetries());
        putRequestScheduler.setRetryTimeout(config.getPutRetryTimeout());
        putRequestScheduler.setMaxRunningByOwner(config.getPutMaxRunningBySameOwner());
        putRequestScheduler.setPriorityPolicyPlugin(config.getPutPriorityPolicyPlugin());
        putRequestScheduler.start();
        
        copyRequestScheduler = new Scheduler("copy_"+name,storage);
        // scheduler parameters
        copyRequestScheduler.setMaxThreadQueueSize(config.getCopyReqTQueueSize());
        copyRequestScheduler.setThreadPoolSize(config.getCopyThreadPoolSize());
        copyRequestScheduler.setMaxWaitingJobNum(config.getCopyMaxWaitingRequests());
        copyRequestScheduler.setMaxNumberOfRetries(config.getCopyMaxNumOfRetries());
        copyRequestScheduler.setRetryTimeout(config.getCopyRetryTimeout());
        copyRequestScheduler.setMaxRunningByOwner(config.getCopyMaxRunningBySameOwner());
        copyRequestScheduler.setPriorityPolicyPlugin(config.getCopyPriorityPolicyPlugin());
        copyRequestScheduler.start();
        
        reserveSpaceScheduler = new Scheduler("reserve_space"+name,storage);
        reserveSpaceScheduler.start();
        //config.getMaxActiveGet(),config.getMaxDoneGet(),config.getGetLifetime(),config);
       /* putRequestScheduler = new RequestScheduler("put ContainerRequest Scheduler",
        config.getMaxActivePut(),config.getMaxDonePut(),config.getPutLifetime(),config);
        copyRequestScheduler = new RequestScheduler("copy ContainerRequest Scheduler",
        config.getMaxActiveCopy(),config.getMaxDoneCopy(),config.getCopyLifetime(),config);
        
        */
        bringOnlineStorage = new BringOnlineRequestStorage(configuration);
        getStorage = new GetRequestStorage(configuration);
        putStorage = new PutRequestStorage(configuration);
        copyStorage = new CopyRequestStorage(configuration);
        bringOnlineFileRequestStorage = new BringOnlineFileRequestStorage(configuration);
        getFileRequestStorage = new GetFileRequestStorage(configuration);
        putFileRequestStorage = new PutFileRequestStorage(configuration);
        copyFileRequestStorage = new CopyFileRequestStorage(configuration);
        reserveSpaceRequestStorage = new ReserveSpaceRequestStorage(configuration);
        bringOnlineStorage.storeUncompleteRequestsInMemory(config.isSaveMemory());
        getStorage.storeUncompleteRequestsInMemory(config.isSaveMemory());
        putStorage.storeUncompleteRequestsInMemory(config.isSaveMemory());
        copyStorage.storeUncompleteRequestsInMemory(config.isSaveMemory());
        bringOnlineFileRequestStorage.storeUncompleteRequestsInMemory(config.isSaveMemory());
        getFileRequestStorage.storeUncompleteRequestsInMemory(config.isSaveMemory());
        putFileRequestStorage.storeUncompleteRequestsInMemory(config.isSaveMemory());
        copyFileRequestStorage.storeUncompleteRequestsInMemory(config.isSaveMemory());
        reserveSpaceRequestStorage.storeUncompleteRequestsInMemory(config.isSaveMemory());
        Job.registerJobStorage(bringOnlineFileRequestStorage);
        Job.registerJobStorage(getFileRequestStorage);
        Job.registerJobStorage(putFileRequestStorage);
        Job.registerJobStorage(copyFileRequestStorage);
        Job.registerJobStorage(bringOnlineStorage);
        Job.registerJobStorage(getStorage);
        Job.registerJobStorage(putStorage);
        Job.registerJobStorage(copyStorage);
        Job.registerJobStorage(reserveSpaceRequestStorage);
        
        bringOnlineStorage.updatePendingJobs();
        getStorage.updatePendingJobs();
        copyStorage.updatePendingJobs();
        putStorage.updatePendingJobs();
        bringOnlineFileRequestStorage.updatePendingJobs();
        getFileRequestStorage.updatePendingJobs();
        putFileRequestStorage.updatePendingJobs();
        copyFileRequestStorage.updatePendingJobs();
        reserveSpaceRequestStorage.updatePendingJobs();
        
        bringOnlineRequestScheduler.jobStorageAdded(bringOnlineFileRequestStorage);
        bringOnlineFileRequestStorage.schedulePendingJobs(getRequestScheduler);
        getRequestScheduler.jobStorageAdded(getFileRequestStorage);
        getFileRequestStorage.schedulePendingJobs(getRequestScheduler);
        putRequestScheduler.jobStorageAdded(putFileRequestStorage);
        putFileRequestStorage.schedulePendingJobs(putRequestScheduler);
        copyRequestScheduler.jobStorageAdded(copyFileRequestStorage);
        copyRequestScheduler.jobStorageAdded(copyStorage);
        copyStorage.schedulePendingJobs(copyRequestScheduler);
        reserveSpaceScheduler.jobStorageAdded(reserveSpaceRequestStorage);
        reserveSpaceRequestStorage.schedulePendingJobs(reserveSpaceScheduler);
        JobCreatorStorage jobCreatorStorage = new HashtableJobCreatorStorage();
        JobCreator.registerJobCreatorStorage(jobCreatorStorage);
        reserveSpaceScheduler.jobStorageAdded(reserveSpaceRequestStorage);
        if(configuration.isVacuum()) {
           say("starting vacuum thread");
           org.dcache.srm.request.sql.JdbcConnectionPool pool = org.dcache.srm.request.sql.JdbcConnectionPool.getPool(configuration.getJdbcUrl(), 
            configuration.getJdbcClass(),configuration.getJdbcUser(),
            configuration.getJdbcPass());
            pool.startVacuumThread(configuration.getVacuum_period_sec()*1000);
        }
                
        if( config.isStart_server() ) {
            say("starting http server from the GLUE Soap toolkit");
            say("if you are ranning srm under Tomcat/Axis, this should be disabled");
            say("by specification of -start_server=false in srm batch");
            this.server = new SRMServerV1(this,config.getPort(),
            config.getAuthorization(), config,requestCredentialStorage);
            host = server.getHost();
        } else {
            say("option -start_server=false is specified");
            say("it is assumed srm soap server is running under Tomcat/Axis");
            host = java.net.InetAddress.getLocalHost();
        }
        if(configuration.getSrmhost() == null) {
            configuration.setSrmhost(host.getHostName());
        }
        try {
            Thread.sleep(5);
        }
        catch(InterruptedException ie) {
        }
        
        this.say("srm started :\n\t"+configuration.toString());
        
    }
    /**
     * @return this host InetAddress
     */
    public InetAddress getHost() {
        return host;
    }
    
    /**
     * @return this srm Web Servises Interface port
     */
    public int  getPort() {
        return configuration.getPort();
    }
    
    
    /**
     * logs a message
     */
    public void say(String words) {
        storage.log("srm: "+words);
    }
    
    /**
     * logs an error message
     */
    public void esay(String words) {
        storage.elog("srm: "+words);
    }
    
    /**
     * logs an exception
     */
    public void esay(Throwable t) {
        storage.elog("srm exception:");
        storage.elog(t);
    }
    private class TheAdvisoryDeleteCallbacks implements AdvisoryDeleteCallbacks {

        private boolean done = false;
        private boolean success  = true;
        RequestUser user;
        String path;
        String error;
        public TheAdvisoryDeleteCallbacks(RequestUser user,
        String path) {
            this.user = user;
            this.path = path;
        }
        public void AdvisoryDeleteFailed(String reason) {
            error=" advisoryDelete("+user+","+path+") AdvisoryDeleteFailed: "+reason;
            success = false;
            esay(error);
            done();
        }


        public void AdvisoryDeleteSuccesseded(){
            say(" advisoryDelete("+user+","+path+") AdvisoryDeleteSuccesseded");
            done();
        }

        public void Exception(Exception e){
            error = " advisoryDelete("+user+","+path+") Exception :"+e;
            esay(error);
            success = false;
            done();
        }

        public void Timeout(){
            error = " advisoryDelete("+user+","+path+") Timeout ";
            esay(error);
            success = false;
            done();
        }

        public void Error(String error){
            this.error = " advisoryDelete("+user+","+path+") Error "+ error;
            esay(this.error);
            success = false;
            done();
        }

       public  boolean waitCompleteion(long timeout) throws InterruptedException {
           long starttime = System.currentTimeMillis();
            while(true) {
                synchronized(this) {
                    wait(1000);
                    if(done) {
                        return success;
                    }
                    else
                    {
                        if((System.currentTimeMillis() - starttime)>timeout) {
                            error = " advisoryDelete("+user+","+path+") Timeout";
                            return false;
                        }
                    }
                }
            }
        }

        public  synchronized void done() {
            done = true;
            notifyAll();
        }

        public java.lang.String getError() {
            return error;
        }        

        
    };


    /**
     * this srm method is not implemented
     */
    public void advisoryDelete(final RequestUser user,RequestCredential credential,String[] SURLS)  {
        say("SRM.advisoryDelete");
        if(user == null) {
            String error ="advisoryDelete: user is unknown,"+
            " user needs authorization to delete ";
            esay(error);
            throw new IllegalArgumentException(error);
        }
        
        for(int i = 0 ; i<SURLS.length; ++i) {
            try {
                GlobusURL gurl = new GlobusURL(SURLS[i]);
                if(!Tools.sameHost(configuration.getSrmhost(),
                gurl.getHost())) {
                    String error ="advisoryDelete: surl is not local : "+gurl.getURL();
                    esay(error);
                    throw new IllegalArgumentException(error);
                }
            }
            catch(RuntimeException re) {
                esay(re);
                throw re;
            }
            catch (Exception e) {
                esay(e);
            }
        }
        
        final StringBuffer sb = new StringBuffer();
        TheAdvisoryDeleteCallbacks callabacks_array[] = 
        new TheAdvisoryDeleteCallbacks[SURLS.length];
        for(int i = 0 ; i<SURLS.length; ++i) {
            try {
                GlobusURL gurl = new GlobusURL(SURLS[i]);
                String surlpath = gurl.getPath();
                int indx=surlpath.indexOf(SFN_STRING);
                if( indx != -1) {
                    
                    surlpath=surlpath.substring(indx+SFN_STRING.length());
                }
                
                callabacks_array[i] = new TheAdvisoryDeleteCallbacks(user, surlpath);
                storage.advisoryDelete(user, surlpath,callabacks_array[i]
                );
                
                
            }
            catch(RuntimeException re) {
                esay(re);
                throw re;
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        boolean failed = false;
        StringBuffer errorsb= new StringBuffer();
        try
        {
            for(int i = 0 ; i<SURLS.length; ++i) {
                if(!callabacks_array[i].waitCompleteion(3*60*1000)) {
                    failed = true;
                    errorsb.append(callabacks_array[i].getError()).append('\n');
                }
                
            }
        }catch(InterruptedException ie) {
                throw new RuntimeException(ie);
            
        }
        
        if(failed) {
             throw new RuntimeException(errorsb.toString());
        }
    }
    

    
    /**
     * The implementation of SRM Copy method.
     *
     * @param  user
     *         an instance of the RequestUser or null if unknown
     * @param  srcSURLS
     *         array of source SURL (Site specific URL) strings
     * @param  destSURLS
     *         array of destination SURL (Site specific URL) strings
     * @param  wantPerm
     *         array of boolean values indicating if permonent copies are
     *         desired
     * @return  request status assosiated with this request
     */
    public RequestStatus copy(RequestUser user,
    RequestCredential credential,
    String[] srcSURLS,
    String[] destSURLS,
    boolean[] wantPerm,
    String client_host) {
        try {
            //require at least 10 minutes
            long cred_lifetime = 
                credential.getDelegatedCredentialRemainingLifetime()-600000;  
            if(cred_lifetime <0) {
                return createFailedRequestStatus(
                "delegated credentials lifetime is too short:"+ credential.getDelegatedCredentialRemainingLifetime()+" ms");
                
            }
            if(srcSURLS == null || srcSURLS.length == 0 ||
               destSURLS == null || destSURLS.length ==0 ) {
                   String error = " number of source or destination SURLs is zero";
                   esay(error);
                   return createFailedRequestStatus(error);
      
            }
            String[] from_urls = srcSURLS;
            String[] to_urls = destSURLS;
            int src_num = from_urls.length;
            int dst_num = to_urls.length;
            // this is for loggin
            StringBuffer sb = new StringBuffer(" copy (");
            for(int j=0; j<src_num;j++) {
                sb.append("from_urls[").append(j).append("]=")
                .append(from_urls[j]).append(",");
            }
            for(int j=0; j<dst_num;j++) {
                sb.append("to_urls[").append(j).append("]=")
                .append(to_urls[j]).append(",");
            }
            sb.append(")");
            say(sb.toString());
            
            if(src_num != dst_num) {
                return createFailedRequestStatus(
                "number of from and to urls do not match");
            }
            
            for(int i = 0; i<dst_num; ++i) {
                for(int j = 0;j<dst_num; ++j) {
                    if(i != j) {
                        if(to_urls[i].equals(to_urls[j])) {
                            return createFailedRequestStatus(
                            "list of sources contains the same url twice "+
                            "url#"+i+" is "+to_urls[i] +
                            " and url#"+j+" is "+to_urls[j]);
                        }
                    }
                }
            }
            long lifetime = configuration.getCopyLifetime();
            if(cred_lifetime < lifetime  )
            {
                say("credential lifetime is less than default lifetime, using credential lifetime ="+cred_lifetime);
                lifetime = cred_lifetime;
            }
            // create a request object
            say("calling Request.createCopyRequest()");
            ContainerRequest r = new CopyRequest(
            user.getId(),
            credential.getId(),
            copyStorage,
            from_urls,
            to_urls,
            null, // no space reservation in v1
            configuration,
            lifetime,
            copyFileRequestStorage,
            configuration.getCopyRetryTimeout(),
            configuration.getCopyMaxNumOfRetries(),
            SRMProtocol.V1_1,
            org.dcache.srm.v2_2.TFileStorageType.PERMANENT,
            null,
            null,null,
            client_host,
            null);
            say(" Copy Request = "+r);
            copyStorage.saveJob(r,true);
            // RequesScheduler will take care of the rest
            r.schedule(copyRequestScheduler);
            
            // Return the request status
            RequestStatus rs = r.getRequestStatus();
            say(" copy returns RequestStatus = "+rs);
            return rs;
        }
        catch(Exception e) {
            esay(e);
            return createFailedRequestStatus("copy request generated error : "+e);
        }
    }
    
   /**
     * The implementation of SRM get method.
     * Checks the protocols, if it contains at least one supported then it
     * creates the request and places it in a request repository
     * and starts request handler
     *
     * @param  user
     *         an instance of the RequestUser or null if unknown
     * @param  surls
     *         array of SURL (Site specific URL) strings
     * @param  protocols
     *         array of protocols understood by SRM client
     * @return  request status assosiated with this request
     */
    
    
    public RequestStatus get(RequestUser user,
    RequestCredential credential,
    String[] surls,
    String[] protocols,
    String client_host) {
        int len = protocols.length;
        int i = 0;
        // create a request object
        try {
            say("get(): user = "+user);
            ContainerRequest r =
            new  GetRequest(user.getId(),credential.getId(),
            getStorage,
            surls,protocols,configuration,
            configuration.getGetLifetime(),
            getFileRequestStorage,
            configuration.getGetRetryTimeout(),
            configuration.getGetMaxNumOfRetries(),
            null,
            client_host);
            r.setScheduler(getRequestScheduler.getId(),0);
            getStorage.saveJob(r,true);
            
            r.schedule(getRequestScheduler);
            // RequestScheduler will take care of the rest
            //getRequestScheduler.add(r);
            // Return the request status
            RequestStatus rs = r.getRequestStatus();
            say("get() initial RequestStatus = "+rs);
            return rs;
        }
        catch(Exception e) {
            esay(e);
            return  createFailedRequestStatus("get error "+e);
        }
        
    }
    
    /**
     * this srm method is not implemented
     */
    public RequestStatus getEstGetTime(RequestUser user,RequestCredential credential,String[] SURLS, String[] protocols) {
        return createFailedRequestStatus("time is unknown");
    }
    
    /**
     * this srm method is not implemented
     */
    public RequestStatus getEstPutTime(RequestUser user,RequestCredential credential,
    String[] src_names,
    String[] dest_names,
    long[] sizes,
    boolean[] wantPermanent,
    String[] protocols) {
        return  createFailedRequestStatus("time is unknown");
    }
    
    /**
     * The implementation of SRM getFileMetaData method.
     * Not really used by anyone.
     *
     * @param  user
     *         an instance of the RequestUser or null if unknown
     * @param  SURLS
     *         the array of SURLs of files of interest
     *
     * @return  FileMetaData array assosiated with these SURLs
     */
    private final static String SFN_STRING="?SFN=";
    public FileMetaData[] getFileMetaData(RequestUser user,RequestCredential credential,String[] SURLS) {
        StringBuffer sb = new StringBuffer();
        sb.append("getFileMetaData(");
        if(SURLS == null) {
            sb.append("SURLS are null)");
            say(sb.toString());
            throw new IllegalArgumentException(sb.toString());
        }
        
        int len = SURLS.length;
        for(int i =0; i<len; ++ i) {
            sb.append(SURLS[i]+",");
        }
        sb.append(")");
        say(sb.toString());
        
        FileMetaData[] fmds = new FileMetaData[len];
        // call getFileMetaData(String path) for each SURL in array
        for(int i =0; i<len; ++ i) {
            try {
                GlobusURL url = new GlobusURL(SURLS[i]);
                if(!Tools.sameHost(configuration.getSrmhost(),
                url.getHost())) {
                    String error ="getFileMetaData: surl is not local : "+url.getURL();
                    esay(error);
                    throw new IllegalArgumentException(error);
                }
                
                String surlpath = url.getPath();
                int indx=surlpath.indexOf(SFN_STRING);
                if( indx != -1) {
                    
                    surlpath=surlpath.substring(indx+SFN_STRING.length());
                }
                
                say("getFileMetaData(String[]) calling FileMetaData("+surlpath+")");
                
                 FileMetaData fmd = storage.getFileMetaData(user,surlpath);
		 fmd.SURL = SURLS[i];
                 fmds[i] = new FileMetaData(fmd);
                say("FileMetaData["+i+"]="+fmds[i]);
            }
            catch(Exception e) {
                esay("getFileMetaData failed to parse SURL: " +e);
                throw new IllegalArgumentException("getFileMetaData failed to parse SURL: " +e);
            }
        }
        
        return fmds;
    }
    
    /**
     * not implemented
     */
    public String[] getProtocols(RequestUser user,RequestCredential credential) {
        try {
            return storage.supportedGetProtocols();
        }
        catch( SRMException srme) {
            return new String[0];
        }
    }
    
    
    /**
     * The implementation of SRM getRequestStatus method.
     *
     * @param  user
     *         an instance of the RequestUser or null if unknown
     * @param  requestId
     *         the id of the previously issued request
     *
     * @return  request status assosiated with this request
     */
    
    public RequestStatus getRequestStatus(RequestUser user,RequestCredential credential,int requestId) {
        say(" getRequestStatus("+user+","+requestId+")");
        try {
            // Try to get the request with such id
            say("getRequestStatus() Request.getRequest("+requestId+");");
            ContainerRequest r =(ContainerRequest) ContainerRequest.getRequest(requestId);
            say("getRequestStatus() received Request  ");
            if(r != null) {
                // we found one make sure it is the same  user
                RequestUser req_user = r.getRequestUser();
                if(req_user == null || req_user.equals(user)) {
                    // say(" getRequestStatus() request found, returns request file status");
                    // and return the request status
                    RequestStatus rs = r.getRequestStatus();
                    say("obtained request status, returning rs for request id="+requestId);
                    return rs;
                }
                else {
                    return createFailedRequestStatus("getRequestStatus(): request #"+requestId+
                    " does not belong to user "+user,requestId);
                }
            }
            return  createFailedRequestStatus("getRequestStatus() request #"+requestId+
            " was not found",requestId);
        }
        catch(Exception e) {
            esay(e);
            return createFailedRequestStatus("getting request #"+requestId+
            " generated error : "+e,requestId);
        }
    }
    
    public RequestStatus mkPermanent(RequestUser user,RequestCredential credential,String[] SURLS) {
        return  createFailedRequestStatus("not supported, all files are already permanent");
    }
    
    
    /**
     * The implementation of SRM pin method.
     * Currenly Not Implemented
     *
     * @param  user
     *         an instance of the RequestUser or null if unknown
     * @param  turls
     *         array of TURL (Transfer URL) strings
     *
     * @return  request status assosiated with this request
     */
    
    public RequestStatus pin(RequestUser user,RequestCredential credential, String[] TURLS) {
        return createFailedRequestStatus("pins by users are not supported, use get instead");
    }
    /**
     * used for testing only
     * @param  user
     *         an instance of the RequestUser or null if unknown
     */
    public boolean ping(RequestUser user,RequestCredential credential) {
        return true;
    }
    
    /**
     * The implementation of SRM put method.
     *
     * @param  user
     *         an instance of the RequestUser or null if unknown
     * @param  requestId
     *         the id of the previously issued pin request
     * @param  fileId
     *         the id of the file within pin request
     * @param  state
     *         the new state of the request
     *
     * @return  request status assosiated with this request
     */
    
    public RequestStatus put(RequestUser user,
    RequestCredential credential,
    String[] sources,
    String[] dests,
    long[] sizes,
    boolean[] wantPerm,
    String[] protocols,
    String clientHost
    ) {
        int len = dests.length;
        String[] dests_urls = new String[len];
        
        String srmprefix;
        // we do this to support implementations that
        // supply paths instead of the whole urls
        // this is not part of the spec
        
        for(int i = 0; i<len; ++i) {
            for(int j = 0;j<len; ++j) {
                if(i != j) {
                    if(dests[i].equals(dests[j])) {
                        return createFailedRequestStatus(
                        "put(): list of sources contains the same url twice "+
                        "url#"+i+" is "+dests[i] +
                        " and url#"+j+" is "+dests[j]);
                    }
                }
            }
        }
        
        srmprefix = "srm://"+configuration.getSrmhost()+
        ":"+configuration.getPort()+"/";
        
        for(int i=0;i<len;++i) {
            if(dests[i].startsWith("srm://")) {
                dests_urls[i] = dests[i];
            }
            else {
                dests_urls[i] = srmprefix+dests[i];
            }
        }
        try {
            // create a new put request
            ContainerRequest r = new PutRequest(user.getId(),
            credential.getId(),
            putStorage,
            sources, dests_urls, sizes,
            wantPerm, protocols,configuration,configuration.getPutLifetime(),
            putFileRequestStorage,
            configuration.getPutRetryTimeout(),
            configuration.getPutMaxNumOfRetries(),
            clientHost,
            null,
            null,
            null,
            null
            );
            r.setScheduler(putRequestScheduler.getId(),0);
            putStorage.saveJob(r,true);
            //requestScheduler will pick up from here
            r.schedule(putRequestScheduler);
            // return status
            return r.getRequestStatus();
        }
        catch(Exception e) {
            esay(e);
            return  createFailedRequestStatus("put(): error "+e);
        }
    }

     /**
     * The implementation of SRM setFileStatus method.
     *  the only status that user can set file request into
     *  is "Done" status
     *
     * @param  user
     *         an instance of the RequestUser or null if unknown
     * @param  requestId
     *         the id of the previously issued pin request
     * @param  fileId
     *         the id of the file within pin request
     * @param  state
     *         the new state of the request
     *
     * @return  request status assosiated with this request
     */
    
    public RequestStatus setFileStatus(RequestUser user,RequestCredential credential,
    int requestId, int fileRequestId, String state) {
        try {
            say(" setFileStatus("+requestId+","+fileRequestId+","+state+");");
            if(!state.equalsIgnoreCase("done") && !state.equalsIgnoreCase("running") ) {
                return createFailedRequestStatus("setFileStatus(): incorrect state "+state);
            }
            
            //try to get the request
            ContainerRequest r =(ContainerRequest) ContainerRequest.getRequest(requestId);
            if(r == null) {
                return createFailedRequestStatus("setFileStatus(): request #"+requestId+" was not found");
            }
            // check that user is the same
            RequestUser req_user = r.getUser();
            if(req_user != null && !req_user.equals(user)) {
                return createFailedRequestStatus(
                "request #"+requestId+" does not belong to user "+user);
            }
            // get file request from request
            FileRequest fr = r.getFileRequest(fileRequestId);
            if(fr == null) {
                return createFailedRequestStatus("request #"+requestId+
                " does not contain file request #"+fileRequestId);
            }
            synchronized(fr)
            {
                State s = fr.getState();
                if(s == State.CANCELED || s == State.DONE || s == State.FAILED)
                {
                    say("can not set status, the file status is already "+s);
                }
                else
                {
                    // process request
                    say(" calling fr.setStatus(\""+state+"\")");
                    fr.setStatus(state);
                }
            }
            
            // return request status
            return r.getRequestStatus();
        }catch(Exception e) {
            throw new RuntimeException(e);
        }
        
    }
    
    
    /**
     * The implementation of SRM unPin method.
     * Currently unimplemented
     *
     * @param  user
     *         an instance of the RequestUser or null if unknown
     * @param  turls
     *         array of TURL (Transfer URL) strings
     * @param  requestId
     *         the id of the previously issued pin request
     *
     * @return  request status assosiated with this request
     */
    
    public RequestStatus unPin(RequestUser user,RequestCredential credential,
    String[] TURLS, int requestId) {
        return createFailedRequestStatus("pins by users are not supported, use get instead");
    }
    
    
    /**
     * @return iterator for set of ids of all current srm requests
     */
    public Iterator getRequestIds() {
        return null;//RequestScheduler.getRequestIds();
    }
    
    /**
     * @return request corresponding ot id
     */
    public ContainerRequest getRequest(Integer id) {
        try {
            return (ContainerRequest) ContainerRequest.getRequest(id.intValue());
        }catch(Exception e) {
            throw new RuntimeException(e);
        }
        
    }
    
    
    private RequestStatus createFailedRequestStatus(String error) {
        esay("creating a failed request status with a message: "+error);
        RequestStatus rs = new RequestStatus();
        rs.requestId = -1;
        rs.errorMessage = error;
        rs.state = "Failed";
        return rs;
    }
    private RequestStatus createFailedRequestStatus(String error,int  requestId) {
        esay("creating a failed request status with a message: "+error);
        RequestStatus rs = new RequestStatus();
        rs.requestId = requestId;
        rs.errorMessage = error;
        rs.state = "Failed";
        return rs;
    }
    
    
    public StorageElementInfo getStorageElementInfo(
    RequestUser user,
    RequestCredential credential) throws SRMException {
        return storage.getStorageElementInfo(user);
    }
    
    public void listGetRequests(StringBuffer sb)  throws java.sql.SQLException {
        Set activeRequestIds = 
        getStorage.getActiveRequestIds(getRequestScheduler.getId());
        for(Iterator i = activeRequestIds.iterator(); i.hasNext();) {
            Long requestId = (Long)i.next();
            GetRequest gr = (GetRequest)ContainerRequest.getRequest(requestId);
            sb.append(gr).append('\n');
        }
    }
    
    public Set getGetRequestIds(RequestUser user, String description)  throws java.sql.SQLException {
        return 
            getStorage.getActiveRequestIds(getRequestScheduler.getId(),user.getId(),description);
    }
    
    public void listLatestCompletedGetRequests(StringBuffer sb,int maxCount)  throws java.sql.SQLException {
        Set activeRequestIds = getStorage.getLatestCompletedRequestIds(maxCount);
        for(Iterator i = activeRequestIds.iterator(); i.hasNext();) {
            Long requestId = (Long)i.next();
            GetRequest gr = (GetRequest)ContainerRequest.getRequest(requestId);
            sb.append(gr).append('\n');
        }
        
    }
    
    public void listLatestFailedGetRequests(StringBuffer sb,int maxCount)  throws java.sql.SQLException {
        Set activeRequestIds = getStorage.getLatestFailedRequestIds(maxCount);
        for(Iterator i = activeRequestIds.iterator(); i.hasNext();) {
            Long requestId = (Long)i.next();
            GetRequest gr = (GetRequest)ContainerRequest.getRequest(requestId);
            sb.append(gr).append('\n');
        }
        
    }
    
    public void listLatestDoneGetRequests(StringBuffer sb,int maxCount)  throws java.sql.SQLException {
        Set activeRequestIds = getStorage.getLatestDoneRequestIds(maxCount);
        for(Iterator i = activeRequestIds.iterator(); i.hasNext();) {
            Long requestId = (Long)i.next();
            GetRequest gr = (GetRequest)ContainerRequest.getRequest(requestId);
            sb.append(gr).append('\n');
        }
        
    }
    
    public void listLatestCanceledGetRequests(StringBuffer sb,int maxCount)  throws java.sql.SQLException {
        Set activeRequestIds = getStorage.getLatestCanceledRequestIds(maxCount);
        for(Iterator i = activeRequestIds.iterator(); i.hasNext();) {
            Long requestId = (Long)i.next();
            GetRequest gr = (GetRequest)ContainerRequest.getRequest(requestId);
            sb.append(gr).append('\n');
        }
    }

    public void printGetSchedulerInfo(StringBuffer sb) throws java.sql.SQLException {
        getRequestScheduler.getInfo(sb);
    }
    
    public void printGetSchedulerThreadQueue(StringBuffer sb) throws java.sql.SQLException {
        getRequestScheduler.printThreadQueue(sb);
        
    }
    public void printGetSchedulerPriorityThreadQueue(StringBuffer sb) throws java.sql.SQLException {
        getRequestScheduler.printPriorityThreadQueue(sb);
    }
    public void printGetSchedulerReadyThreadQueue(StringBuffer sb) throws java.sql.SQLException {
        getRequestScheduler.printReadyQueue(sb);
        
    }
    
    public void listPutRequests(StringBuffer sb)  throws java.sql.SQLException {
        Set activeRequestIds = 
            putStorage.getActiveRequestIds(putRequestScheduler.getId());
        for(Iterator i = activeRequestIds.iterator(); i.hasNext();) {
            Long requestId = (Long)i.next();
            PutRequest pr = (PutRequest)ContainerRequest.getRequest(requestId);
            sb.append(pr).append('\n');
        }
    }
    
    public Set getPutRequestIds(RequestUser user, String description)  throws java.sql.SQLException {
        return 
            putStorage.getActiveRequestIds(putRequestScheduler.getId(),user.getId(),description);
    }
     
   public void listLatestCompletedPutRequests(StringBuffer sb,int maxCount)  throws java.sql.SQLException {
        Set activeRequestIds = putStorage.getLatestCompletedRequestIds(maxCount);
        for(Iterator i = activeRequestIds.iterator(); i.hasNext();) {
            Long requestId = (Long)i.next();
            PutRequest pr = (PutRequest)ContainerRequest.getRequest(requestId);
            sb.append(pr).append('\n');
        }
    }
    
    public void listLatestFailedPutRequests(StringBuffer sb,int maxCount)  throws java.sql.SQLException {
        Set activeRequestIds = putStorage.getLatestFailedRequestIds(maxCount);
        for(Iterator i = activeRequestIds.iterator(); i.hasNext();) {
            Long requestId = (Long)i.next();
            PutRequest pr = (PutRequest)ContainerRequest.getRequest(requestId);
            sb.append(pr).append('\n');
        }
    }
    
    public void listLatestCanceledPutRequests(StringBuffer sb,int maxCount)  throws java.sql.SQLException {
        Set activeRequestIds = putStorage.getLatestCanceledRequestIds(maxCount);
        for(Iterator i = activeRequestIds.iterator(); i.hasNext();) {
             Long requestId = (Long)i.next();
            PutRequest pr = (PutRequest)ContainerRequest.getRequest(requestId);
            sb.append(pr).append('\n');
        }
    }
    
    public void listLatestDonePutRequests(StringBuffer sb,int maxCount)  throws java.sql.SQLException {
        Set activeRequestIds = putStorage.getLatestDoneRequestIds(maxCount);
        for(Iterator i = activeRequestIds.iterator(); i.hasNext();) {
            Long requestId = (Long)i.next();
            PutRequest pr = (PutRequest)ContainerRequest.getRequest(requestId);
            sb.append(pr).append('\n');
        }
    }
    
    public void printPutSchedulerInfo(StringBuffer sb) throws java.sql.SQLException {
        putRequestScheduler.getInfo(sb);
    }
    
    public void printPutSchedulerThreadQueue(StringBuffer sb) throws java.sql.SQLException {
        putRequestScheduler.printThreadQueue(sb);
        
    }
    public void printPutSchedulerPriorityThreadQueue(StringBuffer sb) throws java.sql.SQLException {
        putRequestScheduler.printPriorityThreadQueue(sb);
    }
    public void printPutSchedulerReadyThreadQueue(StringBuffer sb) throws java.sql.SQLException {
        putRequestScheduler.printReadyQueue(sb);
        
    }
    public void listCopyRequests(StringBuffer sb)  throws java.sql.SQLException {
        Set activeRequestIds = 
        copyStorage.getActiveRequestIds(copyRequestScheduler.getId());
        for(Iterator i = activeRequestIds.iterator(); i.hasNext();) {
             Long requestId = (Long)i.next();
            CopyRequest cr = (CopyRequest)ContainerRequest.getRequest(requestId);
            sb.append(cr).append('\n');
        }
    }

    public Set getCopyRequestIds(RequestUser user, String description)  throws java.sql.SQLException {
        return 
            copyStorage.getActiveRequestIds(copyRequestScheduler.getId(),user.getId(),description);
    }
    
    public void listLatestCompletedCopyRequests(StringBuffer sb,int maxCount)  throws java.sql.SQLException {
        Set activeRequestIds = copyStorage.getLatestCompletedRequestIds(maxCount);
        for(Iterator i = activeRequestIds.iterator(); i.hasNext();) {
            Long requestId = (Long)i.next();
            CopyRequest cr = (CopyRequest)ContainerRequest.getRequest(requestId);
            sb.append(cr).append('\n');
        }
    }
    
    public void listLatestFailedCopyRequests(StringBuffer sb,int maxCount)  throws java.sql.SQLException {
        Set activeRequestIds = copyStorage.getLatestFailedRequestIds(maxCount);
        for(Iterator i = activeRequestIds.iterator(); i.hasNext();) {
            Long requestId = (Long)i.next();
            CopyRequest cr = (CopyRequest)ContainerRequest.getRequest(requestId);
            sb.append(cr).append('\n');
        }
    }
    public void listLatestCanceledCopyRequests(StringBuffer sb,int maxCount)  throws java.sql.SQLException {
        Set activeRequestIds = copyStorage.getLatestCanceledRequestIds(maxCount);
        for(Iterator i = activeRequestIds.iterator(); i.hasNext();) {
            Long requestId = (Long)i.next();
            CopyRequest cr = (CopyRequest)ContainerRequest.getRequest(requestId);
            sb.append(cr).append('\n');
        }
    }
    public void listLatestDoneCopyRequests(StringBuffer sb,int maxCount)  throws java.sql.SQLException {
        Set activeRequestIds = copyStorage.getLatestDoneRequestIds(maxCount);
        for(Iterator i = activeRequestIds.iterator(); i.hasNext();) {
            Long requestId = (Long)i.next();
            CopyRequest cr = (CopyRequest)ContainerRequest.getRequest(requestId);
            sb.append(cr).append('\n');
        }
    }
    
    public void printCopySchedulerInfo(StringBuffer sb) throws java.sql.SQLException {
        copyRequestScheduler.getInfo(sb);
    }
    
    public void printCopySchedulerThreadQueue(StringBuffer sb) throws java.sql.SQLException {
        copyRequestScheduler.printThreadQueue(sb);
        
    }
    public void printCopySchedulerPriorityThreadQueue(StringBuffer sb) throws java.sql.SQLException {
        copyRequestScheduler.printPriorityThreadQueue(sb);
    }
    public void printCopySchedulerReadyThreadQueue(StringBuffer sb) throws java.sql.SQLException {
        copyRequestScheduler.printReadyQueue(sb);
        
    }
    
    public void listBringOnlineRequests(StringBuffer sb)  throws java.sql.SQLException {
        Set activeRequestIds = 
            bringOnlineStorage.getActiveRequestIds(bringOnlineRequestScheduler.getId());
        for(Iterator i = activeRequestIds.iterator(); i.hasNext();) {
            Long requestId = (Long)i.next();
            BringOnlineRequest pr = (BringOnlineRequest)ContainerRequest.getRequest(requestId);
            sb.append(pr).append('\n');
        }
    }
    
    public Set getBringOnlineRequestIds(RequestUser user, String description)  throws java.sql.SQLException {
        return 
            bringOnlineStorage.getActiveRequestIds(bringOnlineRequestScheduler.getId(),user.getId(),description);
    }
    
    public void listLatestCompletedBringOnlineRequests(StringBuffer sb,int maxCount)  throws java.sql.SQLException {
        Set activeRequestIds = bringOnlineStorage.getLatestCompletedRequestIds(maxCount);
        for(Iterator i = activeRequestIds.iterator(); i.hasNext();) {
            Long requestId = (Long)i.next();
            BringOnlineRequest pr = (BringOnlineRequest)ContainerRequest.getRequest(requestId);
            sb.append(pr).append('\n');
        }
    }
    
    public void listLatestFailedBringOnlineRequests(StringBuffer sb,int maxCount)  throws java.sql.SQLException {
        Set activeRequestIds = bringOnlineStorage.getLatestFailedRequestIds(maxCount);
        for(Iterator i = activeRequestIds.iterator(); i.hasNext();) {
            Long requestId = (Long)i.next();
            BringOnlineRequest pr = (BringOnlineRequest)ContainerRequest.getRequest(requestId);
            sb.append(pr).append('\n');
        }
    }
    
    public void listLatestCanceledBringOnlineRequests(StringBuffer sb,int maxCount)  throws java.sql.SQLException {
        Set activeRequestIds = bringOnlineStorage.getLatestCanceledRequestIds(maxCount);
        for(Iterator i = activeRequestIds.iterator(); i.hasNext();) {
             Long requestId = (Long)i.next();
            BringOnlineRequest pr = (BringOnlineRequest)ContainerRequest.getRequest(requestId);
            sb.append(pr).append('\n');
        }
    }
    
    public void listLatestDoneBringOnlineRequests(StringBuffer sb,int maxCount)  throws java.sql.SQLException {
        Set activeRequestIds = bringOnlineStorage.getLatestDoneRequestIds(maxCount);
        for(Iterator i = activeRequestIds.iterator(); i.hasNext();) {
            Long requestId = (Long)i.next();
            BringOnlineRequest pr = (BringOnlineRequest)ContainerRequest.getRequest(requestId);
            sb.append(pr).append('\n');
        }
    }
    
    public void printBringOnlineSchedulerInfo(StringBuffer sb) throws java.sql.SQLException {
        bringOnlineRequestScheduler.getInfo(sb);
    }
    
    public void printBringOnlineSchedulerThreadQueue(StringBuffer sb) throws java.sql.SQLException {
        bringOnlineRequestScheduler.printThreadQueue(sb);
        
    }
    public void printBringOnlineSchedulerPriorityThreadQueue(StringBuffer sb) throws java.sql.SQLException {
        bringOnlineRequestScheduler.printPriorityThreadQueue(sb);
    }
    public void printBringOnlineSchedulerReadyThreadQueue(StringBuffer sb) throws java.sql.SQLException {
        bringOnlineRequestScheduler.printReadyQueue(sb);
        
    }

    public double getLoad() {
        int copyRunning = copyRequestScheduler.getTotalRunningThreads();
        int maxCopyRunning = copyRequestScheduler.getThreadPoolSize();
        int getRunning = getRequestScheduler.getTotalRunningThreads();
        int maxGetRunning = getRequestScheduler.getThreadPoolSize();
        int putRunning = putRequestScheduler.getTotalRunningThreads();
        int maxPutRunning = putRequestScheduler.getThreadPoolSize();
        
        double load = (double)copyRunning/(double)maxCopyRunning/3.0d +
        (double)getRunning/(double)maxGetRunning/3.0d +
        (double)putRunning/(double)maxPutRunning/3.0d ;
        return load;
    }
    public void listRequest(StringBuffer sb,Long requestId,boolean longformat)  throws java.sql.SQLException {
        Job job = Job.getJob(requestId);
        if(job == null ) {
            sb.append("request with reqiest id "+requestId+" is not found\n");
            return;
        }
        else
        {
           sb.append("Job # "+requestId + " is in the state "+job.getState()+"\n");
            if (job instanceof ContainerRequest)
            {
               sb.append("Job is a Request:\n");
                ContainerRequest r = (ContainerRequest)job;
                sb.append(r.toString(longformat)).append('\n');
            }
            else if(job instanceof FileRequest)
            {
               FileRequest fr = (FileRequest) job;
               sb.append("Job is a FileRequest from Request #"+fr.getRequestId()+" \n");
               sb.append(fr.toString(longformat));
            }
        }
    }
    
    public void cancelRequest(StringBuffer sb,Long requestId)  throws java.sql.SQLException {
        Job job = Job.getJob(requestId);
        if(job == null || !(job instanceof ContainerRequest)) {
            sb.append("request with reqiest id "+requestId+" is not found\n");
            return;
        }
        ContainerRequest r = (ContainerRequest)job;
        try {
            synchronized(job)
            {
                State s = job.getState();
                if(State.isFinalState(s))
                {
                    sb.append("job state is already "+s+" can not cancel\n");
                }
                else {
                    r.setState(State.CANCELED,"Canceled by admin through cancel command");
                    sb.append("state changed, no warranty that the proccess will end immediately\n");
                    sb.append(r.toString(false)).append('\n');
                }
            }
        }
        catch(IllegalStateTransition ist) {
            sb.append(ist);
            esay(ist);
        }
    }

    public void cancelAllGetRequest(StringBuffer sb,String pattern)  throws java.sql.SQLException {
        
        java.util.Set jobsToKill = new java.util.HashSet();
        java.util.regex.Pattern p = java.util.regex.Pattern.compile(pattern);
        Set activeRequestIds = 
        getStorage.getActiveRequestIds(getRequestScheduler.getId());
        for(Iterator i = activeRequestIds.iterator(); i.hasNext();) {
            Long requestId = (Long)i.next();
            java.util.regex.Matcher m = p.matcher(requestId.toString());
            if( m.matches()) {
                say("cancelAllGetRequest: request Id #"+requestId+" matches pattern!");
                jobsToKill.add(requestId);
            }
        }
        if(jobsToKill.isEmpty()) {
            sb.append("no get requests match the pattern=\""+pattern+"\n");
            return;
        }
        for(java.util.Iterator i = jobsToKill.iterator(); i.hasNext();){
            
            Long requestId = (Long)i.next();
            Job job = Job.getJob(requestId);
            if(job == null || !(job instanceof ContainerRequest)) {
                esay("cancelAllGetRequest: request with reqiest id "+requestId+" is not found\n");
                continue;
            }
            final ContainerRequest r = (ContainerRequest)job;
            sb.append("get request #"+requestId+" matches pattern=\""+pattern+"\"; canceling request \n");
            new Thread (new Runnable() {
                public void run() {
                    synchronized(r)
                    {
                        try {
                            State s = r.getState();
                            if(!State.isFinalState(s ))
                            {
                                r.setState(State.CANCELED,"Canceled by admin through cancelall command");
                            }
                        }
                        catch(IllegalStateTransition ist) {
                            esay(ist);
                        }
                    }
                }
            }).start();
        }
    }
    
    public void cancelAllPutRequest(StringBuffer sb,String pattern)  throws java.sql.SQLException {
        
        java.util.Set jobsToKill = new java.util.HashSet();
        java.util.regex.Pattern p = java.util.regex.Pattern.compile(pattern);
        Set activeRequestIds = 
        putStorage.getActiveRequestIds(putRequestScheduler.getId());
        for(Iterator i = activeRequestIds.iterator(); i.hasNext();) {
            Long requestId = (Long)i.next();
            java.util.regex.Matcher m = p.matcher(requestId.toString());
            if( m.matches()) {
                say("cancelAllPutRequest: request Id #"+requestId+" matches pattern!");
                jobsToKill.add(requestId);
            }
        }
        if(jobsToKill.isEmpty()) {
            sb.append("no put requests match the pattern=\""+pattern+"\n");
            return;
        }
        for(java.util.Iterator i = jobsToKill.iterator(); i.hasNext();){
            
            Long requestId = (Long)i.next();
            Job job = Job.getJob(requestId);
            if(job == null || !(job instanceof ContainerRequest)) {
                esay("cancelAllPutRequest: request with reqiest id "+requestId+" is not found\n");
                continue;
            }
            final ContainerRequest r = (ContainerRequest)job;
            sb.append("put request #"+requestId+" matches pattern=\""+pattern+"\"; canceling request ");
            new Thread (new Runnable() {
                public void run() {
                    synchronized(r)
                    {
                        try {
                            State s = r.getState();
                            if(!State.isFinalState(s ))
                            {
                                r.setState(State.CANCELED,"Canceled by admin through cancelall command");
                            }
                        }
                        catch(IllegalStateTransition ist) {
                            esay(ist);
                        }
                    }
                }
            }).start();
        }
    }
    
    public void cancelAllCopyRequest(StringBuffer sb,String pattern)  throws java.sql.SQLException {
        
        java.util.Set jobsToKill = new java.util.HashSet();
        java.util.regex.Pattern p = java.util.regex.Pattern.compile(pattern);
            say("cancelAllCopyRequest: calling copyStorage.getActiveRequestIds()");
        Set activeRequestIds = 
        copyStorage.getActiveRequestIds(copyRequestScheduler.getId());
        for(Iterator i = activeRequestIds.iterator(); i.hasNext();) {
            Long requestId = (Long)i.next();
            java.util.regex.Matcher m = p.matcher(requestId.toString());
            if( m.matches()) {
                say("cancelAllCopyRequest: request #"+requestId+" matches pattern!");
                jobsToKill.add(requestId);
            }
        }
        if(jobsToKill.isEmpty()) {
            sb.append("no copy requests match the pattern=\""+pattern+"\n");
            return;
        }
        for(java.util.Iterator i = jobsToKill.iterator(); i.hasNext();){
            
            Long requestId = (Long)i.next();
            Job job = Job.getJob(requestId);
            if(job == null || !(job instanceof ContainerRequest)) {
                esay("cancelAllCopyRequest: request with reqiest #"+requestId+" is not found\n");
                continue;
            }
            final ContainerRequest r = (ContainerRequest)job;
            sb.append("copy request #"+requestId+" matches pattern=\""+pattern+"\"; canceling request\n");
            new Thread (new Runnable() {
                public void run() {
                    synchronized(r)
                    {
                        try {
                            State s = r.getState();
                            if(!State.isFinalState(s ))
                            {
                                r.setState(State.CANCELED,
                                "Canceled by admin through cancelall command");
                            }
                        }
                        catch(IllegalStateTransition ist) {
                            esay(ist);
                        }
                    }
                }
            }).start();
        }
    }
    
   /**
    * Getter for property configuration.
    * @return Value of property configuration.
    */
   public org.dcache.srm.util.Configuration getConfiguration() {
       return configuration;
   }   
   
   /**
    * Getter for property requestCredentialStorage.
    * @return Value of property requestCredentialStorage.
    */
   public org.dcache.srm.request.RequestCredentialStorage getRequestCredentialStorage() {
       return requestCredentialStorage;
   }

    public Scheduler getGetRequestScheduler() {
        return getRequestScheduler;
    }
    
    public Scheduler getBringOnlineRequestScheduler() {
        return bringOnlineRequestScheduler;
    }

    public Scheduler getPutRequestScheduler() {
        return putRequestScheduler;
    }

    public Scheduler getCopyRequestScheduler() {
        return copyRequestScheduler;
    }
    
    public ReserveSpaceRequestStorage getReserveSpaceRequestStorage() {
        return reserveSpaceRequestStorage;
    }
    
    public BringOnlineRequestStorage getBringOnlineStorage() {
        return bringOnlineStorage;
    }
    
    public GetRequestStorage getGetStorage() {
        return getStorage;
    }

    public PutRequestStorage getPutStorage() {
        return putStorage;
    }

    public CopyRequestStorage getCopyStorage() {
        return copyStorage;
    }

    public BringOnlineFileRequestStorage getBringOnlineFileRequestStorage() {
        return bringOnlineFileRequestStorage;
    }

    public GetFileRequestStorage getGetFileRequestStorage() {
        return getFileRequestStorage;
    }

    public PutFileRequestStorage getPutFileRequestStorage() {
        return putFileRequestStorage;
    }

    public CopyFileRequestStorage getCopyFileRequestStorage() {
        return copyFileRequestStorage;
    }

    public Scheduler getReserveSpaceScheduler() {
        return reserveSpaceScheduler;
    }   
     
}
