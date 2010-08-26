// $Id$
// $Log: not supported by cvs2svn $
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

import org.globus.util.GlobusURL;
import org.dcache.srm.request.FileRequest;
import org.dcache.srm.util.Configuration;
import org.dcache.srm.request.Request;
import org.dcache.srm.request.ContainerRequest;
import org.dcache.srm.request.PutRequest;
import org.dcache.srm.request.PutFileRequest;
import org.dcache.srm.request.GetRequest;
import org.dcache.srm.request.GetFileRequest;
import org.dcache.srm.request.LsRequest;
import org.dcache.srm.request.LsFileRequest;
import org.dcache.srm.request.ReserveSpaceRequest;
import org.dcache.srm.request.CopyRequest;
import org.dcache.srm.request.BringOnlineRequest;
import org.dcache.srm.request.BringOnlineFileRequest;
import org.dcache.srm.request.sql.DatabaseRequestStorage;
import org.dcache.srm.request.sql.DatabaseContainerRequestStorage;
import org.dcache.srm.request.sql.GetFileRequestStorage;
import org.dcache.srm.request.sql.GetRequestStorage;
import org.dcache.srm.request.sql.BringOnlineFileRequestStorage;
import org.dcache.srm.request.sql.BringOnlineRequestStorage;
import org.dcache.srm.request.sql.PutFileRequestStorage;
import org.dcache.srm.request.sql.PutRequestStorage;
import org.dcache.srm.request.sql.ReserveSpaceRequestStorage;
import org.dcache.srm.request.sql.CopyFileRequestStorage;
import org.dcache.srm.request.sql.CopyRequestStorage;
import org.dcache.srm.request.sql.LsRequestStorage;
import org.dcache.srm.request.sql.LsFileRequestStorage;
import org.dcache.srm.request.sql.DatabaseRequestCredentialStorage;
import org.dcache.srm.util.Tools;
import java.util.Set;
import java.util.HashSet;
import java.util.Iterator;
import java.io.IOException;
import java.io.File;
import java.net.InetAddress;

import org.dcache.srm.SRMInvalidRequestException;
import org.dcache.srm.scheduler.Scheduler;
import org.dcache.srm.scheduler.SchedulerFactory;
import org.dcache.srm.scheduler.State;
import org.dcache.srm.scheduler.IllegalStateTransition;
import org.dcache.srm.scheduler.Job;
import org.dcache.srm.scheduler.JobStorage;
import org.dcache.srm.request.RequestCredential;
import org.dcache.srm.request.RequestCredentialStorage;
import org.dcache.srm.request.sql.RequestsPropertyStorage;
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
import org.dcache.commons.stats.RequestCounters;
import org.dcache.commons.stats.RequestExecutionTimeGauges;
import org.dcache.commons.stats.rrd.RrdRequestCounters;
import org.dcache.commons.stats.rrd.RrdRequestExecutionTimeGauges;
import org.dcache.commons.stats.MonitoringProxy;
import java.lang.reflect.Method;

import java.util.concurrent.TimeoutException;
import org.apache.log4j.Logger;

/**
 * SRM class creates an instance of SRM client class and publishes it on a
 * given port as a storage element.
 *
 * @author  timur
 */
public class SRM {
    private static final Logger logger = Logger.getLogger(SRM.class);
    private String name;
    private InetAddress host;
    private SRMAuthorization authorization;
    private SRMServerV1 server;
    private Configuration configuration;
    private RequestCredentialStorage requestCredentialStorage;
    //    private RequestScheduler getPutRequestScheduler();
    //    private RequestScheduler getCopyRequestScheduler();
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
    private LsRequestStorage lsRequestStorage;
    private LsFileRequestStorage lsFileRequestStorage;
    private RequestCounters<Class> srmServerV2Counters;
    private RequestCounters<String> srmServerV1Counters;
    private RequestCounters<Method> abstractStorageElementCounters;
    private RrdRequestCounters rrdSrmServerV2Counters;
    private RrdRequestCounters rrdSrmServerV1Counters;
    private RrdRequestCounters rrdAstractStorageElementCounters;
    private RequestExecutionTimeGauges<Class> srmServerV2Gauges;
    private RequestExecutionTimeGauges<String> srmServerV1Gauges;
    private RequestExecutionTimeGauges<Method> abstractStorageElementGauges;
    private RrdRequestExecutionTimeGauges rrdSrmServerV2Gauges;
    private RrdRequestExecutionTimeGauges rrdSrmServerV1Gauges;
    private RrdRequestExecutionTimeGauges rrdAstractStorageElementGauges;

    private RequestCounters srmRequestCounters;
    private RequestExecutionTimeGauges srmRequestGauges;
    private RrdRequestExecutionTimeGauges rrdSrmRequestGauges;
    private RrdRequestCounters rrdSrmRequestCounters;
    private static SRM srm;
    /**
     * Creates a new instance of SRM 
     * @param config
     * @param name
     * @throws java.io.IOException
     * @throws java.sql.SQLException
     * @throws java.lang.InterruptedException
     * @throws org.dcache.srm.scheduler.IllegalStateTransition
     * @throws electric.xml.ParseException
     */
    private SRM(Configuration config, String name)
            throws java.io.IOException,
            java.sql.SQLException,
            InterruptedException,
            org.dcache.srm.scheduler.IllegalStateTransition,
            electric.xml.ParseException {
        this.configuration = config;
        //First of all decorate the storage with counters and
        // gauges to measure the performace of storage operations
        this.storage = config.getStorage();
        abstractStorageElementCounters=
            new RequestCounters<Method> (
            this.storage.getClass().getName());
        abstractStorageElementGauges =
            new RequestExecutionTimeGauges<Method> (
                this.storage.getClass().getName());
        this.storage = MonitoringProxy.decorateWithMonitoringProxy(
                new Class[]{AbstractStorageElement.class},
                this.storage,
                abstractStorageElementCounters,
                abstractStorageElementGauges);
         config.setStorage(this.storage);

        this.name = name;
        srmServerV2Counters = new RequestCounters<Class>("SRMServerV2");
        srmServerV1Counters = new RequestCounters<String>("SRMServerV1");
        if (configuration.getCounterRrdDirectory() != null) {
            String rrddir = configuration.getCounterRrdDirectory() +
                    java.io.File.separatorChar + "srmv1";
            rrdSrmServerV1Counters =
                    new RrdRequestCounters(srmServerV1Counters, rrddir);
            rrdSrmServerV1Counters.startRrdUpdates();
            rrdSrmServerV1Counters.startRrdGraphPlots();
            rrddir = configuration.getCounterRrdDirectory() +
                    java.io.File.separatorChar + "srmv2";
            rrdSrmServerV2Counters =
                    new RrdRequestCounters(srmServerV2Counters, rrddir);
            rrdSrmServerV2Counters.startRrdUpdates();
            rrdSrmServerV2Counters.startRrdGraphPlots();
            rrddir =  configuration.getCounterRrdDirectory() +
                    java.io.File.separatorChar + "storage";

            rrdAstractStorageElementCounters =
                    new RrdRequestCounters(abstractStorageElementCounters, rrddir);
            rrdAstractStorageElementCounters.startRrdUpdates();
            rrdAstractStorageElementCounters.startRrdGraphPlots();


        }
        srmServerV2Gauges = new RequestExecutionTimeGauges<Class> ("SRMServerV2");
        srmServerV1Gauges = new RequestExecutionTimeGauges<String> ("SRMServerV1");
        if (configuration.getGaugeRrdDirectory() != null) {
            File rrddir = new File(configuration.getGaugeRrdDirectory() +
                    File.separatorChar + "srmv1");
            rrdSrmServerV1Gauges =
                    new RrdRequestExecutionTimeGauges(srmServerV1Gauges, rrddir);
            rrdSrmServerV1Gauges.startRrdUpdates();
            rrdSrmServerV1Gauges.startRrdGraphPlots();
            rrddir = new File(configuration.getGaugeRrdDirectory() +
                    File.separatorChar + "srmv2");
            rrdSrmServerV2Gauges =
                    new RrdRequestExecutionTimeGauges(srmServerV2Gauges, rrddir);
            rrdSrmServerV2Gauges.startRrdUpdates();
            rrdSrmServerV2Gauges.startRrdGraphPlots();
            rrddir = new File (configuration.getGaugeRrdDirectory() +
                    java.io.File.separatorChar + "storage");

            rrdAstractStorageElementGauges =
                    new RrdRequestExecutionTimeGauges(abstractStorageElementGauges, rrddir);
            rrdAstractStorageElementGauges.startRrdUpdates();
            rrdAstractStorageElementGauges.startRrdGraphPlots();


        }

         srmRequestCounters =
                 new RequestCounters("SRMRequestsCounters");
         srmRequestGauges =
                 new RequestExecutionTimeGauges("SRMRequestsGauges");

         RrdRequestExecutionTimeGauges rrdSrmRequestGauges;
         RrdRequestCounters rrdSrmRequestCounters;

        // these jdbc parameters need to be set before the
        // first jdbc instance is created
        // so we do it before everything else

        if (configuration.getMaxQueuedJdbcTasksNum() != null) {
            say("setMaxJdbcTasksNum to " +
                    configuration.getMaxQueuedJdbcTasksNum().intValue());
            org.dcache.srm.request.sql.JdbcConnectionPool.setMaxQueuedJdbcTasksNum(
                    configuration.getMaxQueuedJdbcTasksNum().intValue());
        }

        if (configuration.getJdbcExecutionThreadNum() != null) {
            say("set JDBC ExecutionThreadNum to " +
                    configuration.getJdbcExecutionThreadNum().intValue());
            org.dcache.srm.request.sql.JdbcConnectionPool.setExecutionThreadNum(
                    configuration.getJdbcExecutionThreadNum().intValue());
        }

        if (config.isGsissl()) {
            String protocol_property = System.getProperty("java.protocol.handler.pkgs");
            if (protocol_property == null) {
                protocol_property = "org.globus.net.protocol";
            } else {
                protocol_property = protocol_property + "|org.globus.net.protocol";
            }
            System.setProperty("java.protocol.handler.pkgs", protocol_property);
        }

        try {
            RequestsPropertyStorage.initPropertyStorage( config.getJdbcUrl(),
                config.getJdbcClass(),
                config.getJdbcUser(),
                config.getJdbcPass(),
                config.getNextRequestIdStorageTable()
                );
        } catch (IllegalStateException ise) {
            //already initialized
        }

        requestCredentialStorage = new DatabaseRequestCredentialStorage(config);
        RequestCredential.registerRequestCredentialStorage(requestCredentialStorage);
        SchedulerFactory.initSchedulerFactory(storage, config, name);

        //config.getMaxActiveGet(),config.getMaxDoneGet(),config.getGetLifetime(),config);
       /* getPutRequestScheduler() = new RequestScheduler("put ContainerRequest Scheduler",
        config.getMaxActivePut(),config.getMaxDonePut(),config.getPutLifetime(),config);
        getCopyRequestScheduler() = new RequestScheduler("copy ContainerRequest Scheduler",
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
        lsRequestStorage = new LsRequestStorage(configuration);
        lsFileRequestStorage = new LsFileRequestStorage(configuration);
        Job.registerJobStorage(bringOnlineFileRequestStorage);
        Job.registerJobStorage(getFileRequestStorage);
        Job.registerJobStorage(putFileRequestStorage);
        Job.registerJobStorage(copyFileRequestStorage);
        Job.registerJobStorage(bringOnlineStorage);
        Job.registerJobStorage(getStorage);
        Job.registerJobStorage(putStorage);
        Job.registerJobStorage(copyStorage);
        Job.registerJobStorage(reserveSpaceRequestStorage);
        Job.registerJobStorage(lsRequestStorage);
        Job.registerJobStorage(lsFileRequestStorage);

        bringOnlineStorage.updatePendingJobs();
        getStorage.updatePendingJobs();
        copyStorage.updatePendingJobs();
        putStorage.updatePendingJobs();
        bringOnlineFileRequestStorage.updatePendingJobs();
        getFileRequestStorage.updatePendingJobs();
        putFileRequestStorage.updatePendingJobs();
        copyFileRequestStorage.updatePendingJobs();
        reserveSpaceRequestStorage.updatePendingJobs();

        lsRequestStorage.updatePendingJobs();
        lsFileRequestStorage.updatePendingJobs();


        getLsRequestScheduler().jobStorageAdded(lsFileRequestStorage);
        lsFileRequestStorage.schedulePendingJobs(getLsRequestScheduler());


//        lsRequestStorage.schedulePendingJobs(getLsRequestScheduler());
//        getLsRequestScheduler().jobStorageAdded(lsRequestStorage);

        getBringOnlineRequestScheduler().jobStorageAdded(bringOnlineFileRequestStorage);
        bringOnlineFileRequestStorage.schedulePendingJobs(getBringOnlineRequestScheduler());

        getGetRequestScheduler().jobStorageAdded(getFileRequestStorage);
        getFileRequestStorage.schedulePendingJobs(getGetRequestScheduler());

        getPutRequestScheduler().jobStorageAdded(putFileRequestStorage);
        putFileRequestStorage.schedulePendingJobs(getPutRequestScheduler());

        getCopyRequestScheduler().jobStorageAdded(copyFileRequestStorage);
        getCopyRequestScheduler().jobStorageAdded(copyStorage);
        copyStorage.schedulePendingJobs(getCopyRequestScheduler());

        getReserveSpaceScheduler().jobStorageAdded(reserveSpaceRequestStorage);
        reserveSpaceRequestStorage.schedulePendingJobs(getReserveSpaceScheduler());

        if (configuration.isVacuum()) {
            say("starting vacuum thread");
            org.dcache.srm.request.sql.JdbcConnectionPool pool = org.dcache.srm.request.sql.JdbcConnectionPool.getPool(configuration.getJdbcUrl(),
                    configuration.getJdbcClass(), configuration.getJdbcUser(),
                    configuration.getJdbcPass());
            pool.startVacuumThread(configuration.getVacuum_period_sec() * 1000);
        }

        if (config.isStart_server()) {
            say("starting http server from the GLUE Soap toolkit");
            say("if you are ranning srm under Tomcat/Axis, this should be disabled");
            say("by specification of -start_server=false in srm batch");
            this.server = new SRMServerV1(this, config.getPort(),
                    config.getAuthorization(), config, requestCredentialStorage);
            host = server.getHost();
        } else {
            say("option -start_server=false is specified");
            say("it is assumed srm soap server is running under Tomcat/Axis");
            host = java.net.InetAddress.getLocalHost();
        }
        if (configuration.getSrmhost() == null) {
            configuration.setSrmhost(host.getHostName());
        }
        try {
            Thread.sleep(5);
        } catch (InterruptedException ie) {
        }

        this.say("srm started :\n\t" + configuration.toString());

    }

    /**
     * SRM is now a singleton, this will return an instance of
     * will create a new SRM if it does not exist
     * @param configuration
     * @param name
     * @return SRM
     * @throws java.io.IOException
     * @throws java.sql.SQLException
     * @throws java.lang.InterruptedException
     * @throws org.dcache.srm.scheduler.IllegalStateTransition
     * @throws electric.xml.ParseException
     */
    public static synchronized final SRM getSRM(Configuration configuration,
            String name) throws java.io.IOException,
            java.sql.SQLException,
            InterruptedException,
            org.dcache.srm.scheduler.IllegalStateTransition,
            electric.xml.ParseException {

        if(srm != null) {
            return srm;
        }

        srm = new SRM(configuration, name);
        SRM.class.notifyAll();
        return srm;
    }

    /**
     *  This method will return srm if it was already created
     *  or will wait for timeout millis for its creation
     * @param timeout
     * @return
     * @throws java.lang.InterruptedException
     * @throws java.util.concurrent.TimeoutException
     */
    public static synchronized final SRM getInstance(long timeout)
            throws InterruptedException, TimeoutException {
        long time_expired = 0;
        long wait_period = 1000;
        while(srm == null ) {
            System.out.println(new java.util.Date() +
                   " Waiting for srm initialization to complete.");
            SRM.class.wait(wait_period);
            time_expired += wait_period;
            if(time_expired > timeout) {
                throw new TimeoutException(
                        "startup takes longer then timeout");
            }
        }
        return srm;
    }

    /**
     *
     * @return instance of SRM if it was created or null if it was not
     */
    public static final SRM getSRM() {
        return srm;
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
    public int getPort() {
        return configuration.getPort();
    }

    /**
     * logs a message
     */
    public void say(String words) {
        storage.log("srm: " + words);
    }

    /**
     * logs an error message
     */
    public void esay(String words) {
        storage.elog("srm: " + words);
    }

    /**
     * logs an exception
     */
    public void esay(Throwable t) {
        storage.elog("srm exception:");
        storage.elog(t);
    }

    /**
     * @return the srmServerCounters
     */
    public RequestCounters<Class> getSrmServerV2Counters() {
        return srmServerV2Counters;
    }

    /**
     * @return the srmServerV1Counters
     */
    public RequestCounters<String> getSrmServerV1Counters() {
        return srmServerV1Counters;
    }

    /**
     * @return the srmServerV2Gauges
     */
    public RequestExecutionTimeGauges<Class> getSrmServerV2Gauges() {
        return srmServerV2Gauges;
    }

    /**
     * @return the srmServerV1Gauges
     */
    public RequestExecutionTimeGauges<String> getSrmServerV1Gauges() {
        return srmServerV1Gauges;
    }

    /**
     * @return the abstractStorageElementCounters
     */
    public RequestCounters<Method> getAbstractStorageElementCounters() {
        return abstractStorageElementCounters;
    }

    /**
     * @return the abstractStorageElementGauges
     */
    public RequestExecutionTimeGauges<Method> getAbstractStorageElementGauges() {
        return abstractStorageElementGauges;
    }

    private class TheAdvisoryDeleteCallbacks implements AdvisoryDeleteCallbacks {

        private boolean done = false;
        private boolean success = true;
        SRMUser user;
        String path;
        String error;

        public TheAdvisoryDeleteCallbacks(SRMUser user,
                String path) {
            this.user = user;
            this.path = path;
        }

        public void AdvisoryDeleteFailed(String reason) {
            error = " advisoryDelete(" + user + "," + path + ") AdvisoryDeleteFailed: " + reason;
            success = false;
            esay(error);
            done();
        }

        public void AdvisoryDeleteSuccesseded() {
            say(" advisoryDelete(" + user + "," + path + ") AdvisoryDeleteSuccesseded");
            done();
        }

        public void Exception(Exception e) {
            error = " advisoryDelete(" + user + "," + path + ") Exception :" + e;
            esay(error);
            success = false;
            done();
        }

        public void Timeout() {
            error = " advisoryDelete(" + user + "," + path + ") Timeout ";
            esay(error);
            success = false;
            done();
        }

        public void Error(String error) {
            this.error = " advisoryDelete(" + user + "," + path + ") Error " + error;
            esay(this.error);
            success = false;
            done();
        }

        public boolean waitCompleteion(long timeout) throws InterruptedException {
            long starttime = System.currentTimeMillis();
            while (true) {
                synchronized (this) {
                    wait(1000);
                    if (done) {
                        return success;
                    } else {
                        if ((System.currentTimeMillis() - starttime) > timeout) {
                            error = " advisoryDelete(" + user + "," + path + ") Timeout";
                            return false;
                        }
                    }
                }
            }
        }

        public synchronized void done() {
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
    public void advisoryDelete(final SRMUser user, RequestCredential credential, String[] SURLS) {
        say("SRM.advisoryDelete");
        if (user == null) {
            String error = "advisoryDelete: user is unknown," +
                    " user needs authorization to delete ";
            esay(error);
            throw new IllegalArgumentException(error);
        }

        for (int i = 0; i < SURLS.length; ++i) {
            try {
                GlobusURL gurl = new GlobusURL(SURLS[i]);
                if (!Tools.sameHost(configuration.getSrmhost(),
                        gurl.getHost())) {
                    String error = "advisoryDelete: surl is not local : " + gurl.getURL();
                    esay(error);
                    throw new IllegalArgumentException(error);
                }
            } catch (RuntimeException re) {
                esay(re);
                throw re;
            } catch (Exception e) {
                esay(e);
            }
        }

        final StringBuffer sb = new StringBuffer();
        TheAdvisoryDeleteCallbacks callabacks_array[] =
                new TheAdvisoryDeleteCallbacks[SURLS.length];
        for (int i = 0; i < SURLS.length; ++i) {
            try {
                GlobusURL gurl = new GlobusURL(SURLS[i]);
                String surlpath = gurl.getPath();
                int indx = surlpath.indexOf(SFN_STRING);
                if (indx != -1) {

                    surlpath = surlpath.substring(indx + SFN_STRING.length());
                }

                callabacks_array[i] = new TheAdvisoryDeleteCallbacks(user, surlpath);
                storage.advisoryDelete(user, surlpath, callabacks_array[i]);


            } catch (RuntimeException re) {
                esay(re);
                throw re;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        boolean failed = false;
        StringBuffer errorsb = new StringBuffer();
        try {
            for (int i = 0; i < SURLS.length; ++i) {
                if (!callabacks_array[i].waitCompleteion(3 * 60 * 1000)) {
                    failed = true;
                    errorsb.append(callabacks_array[i].getError()).append('\n');
                }

            }
        } catch (InterruptedException ie) {
            throw new RuntimeException(ie);

        }

        if (failed) {
            throw new RuntimeException(errorsb.toString());
        }
    }

    /**
     * The implementation of SRM Copy method.
     *
     * @param user
     *         an instance of the ReSRMUserr null if unknown
     * @param srcSURLS
     *         array of source SURL (Site specific URL) strings
     * @param destSURLS
     *         array of destination SURL (Site specific URL) strings
     * @param wantPerm
     *         array of boolean values indicating if permonent copies are
     *         desired
     * @return request status assosiated with this request
     */
    public RequestStatus copy(SRMUser user,
            RequestCredential credential,
            String[] srcSURLS,
            String[] destSURLS,
            boolean[] wantPerm,
            String client_host) {
        try {
            //require at least 10 minutes
            long cred_lifetime =
                    credential.getDelegatedCredentialRemainingLifetime() - 600000;
            if (cred_lifetime < 0) {
                return createFailedRequestStatus(
                        "delegated credentials lifetime is too short:" + credential.getDelegatedCredentialRemainingLifetime() + " ms");

            }
            if (srcSURLS == null || srcSURLS.length == 0 ||
                    destSURLS == null || destSURLS.length == 0) {
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
            for (int j = 0; j < src_num; j++) {
                sb.append("from_urls[").append(j).append("]=").append(from_urls[j]).append(",");
            }
            for (int j = 0; j < dst_num; j++) {
                sb.append("to_urls[").append(j).append("]=").append(to_urls[j]).append(",");
            }
            sb.append(")");
            say(sb.toString());

            if (src_num != dst_num) {
                return createFailedRequestStatus(
                        "number of from and to urls do not match");
            }

            for (int i = 0; i < dst_num; ++i) {
                for (int j = 0; j < dst_num; ++j) {
                    if (i != j) {
                        if (to_urls[i].equals(to_urls[j])) {
                            return createFailedRequestStatus(
                                    "list of sources contains the same url twice " +
                                    "url#" + i + " is " + to_urls[i] +
                                    " and url#" + j + " is " + to_urls[j]);
                        }
                    }
                }
            }
            long lifetime = configuration.getCopyLifetime();
            if (cred_lifetime < lifetime) {
                say("credential lifetime is less than default lifetime, using credential lifetime =" + cred_lifetime);
                lifetime = cred_lifetime;
            }
            // create a request object
            say("calling Request.createCopyRequest()");
            ContainerRequest r = new CopyRequest(
                    user,
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
                    null, null,
                    client_host,
                    null);
            say(" Copy Request = " + r);
            // RequesScheduler will take care of the rest
            r.schedule();

            // Return the request status
            RequestStatus rs = r.getRequestStatus();
            say(" copy returns RequestStatus = " + rs);
            return rs;
        } catch (Exception e) {
            esay(e);
            return createFailedRequestStatus("copy request generated error : " + e);
        }
    }

    /**
     * The implementation of SRM get method.
     * Checks the protocols, if it contains at least one supported then it
     * creates the request and places it in a request repository
     * and starts request handler
     *
     * @param user
     *         an instance of the ReSRMUserr null if unknown
     * @param surls
     *         array of SURL (Site specific URL) strings
     * @param protocols
     *         array of protocols understood by SRM client
     * @return request status assosiated with this request
     */
    public RequestStatus get(SRMUser user,
            RequestCredential credential,
            String[] surls,
            String[] protocols,
            String client_host) {
        int len = protocols.length;
        int i = 0;
        // create a request object
        try {
            say("get(): user = " + user);
            String[] supportedProtocols = storage.supportedGetProtocols();
            boolean foundMatchedProtocol = false;
            for (String supportedProtocol : supportedProtocols) {
                for (String protocol : protocols) {
                    if (supportedProtocol.equals(protocol)) {
                        foundMatchedProtocol = true;
                        break;
                    }
                }
            }
            if (!foundMatchedProtocol) {
                StringBuffer errorsb =
                        new StringBuffer("Protocol(s) specified not supported: [ ");
                for (String protocol : protocols) {
                    errorsb.append(protocol).append(' ');
                }
                errorsb.append(']');
                return createFailedRequestStatus(errorsb.toString());
            }
            ContainerRequest r =
                    new GetRequest(user, credential.getId(),
                    getStorage,
                    surls, protocols, configuration,
                    configuration.getGetLifetime(),
                    getFileRequestStorage,
                    configuration.getGetRetryTimeout(),
                    configuration.getGetMaxNumOfRetries(),
                    null,
                    client_host);
            r.schedule();
            // RequestScheduler will take care of the rest
            //getGetRequestScheduler().add(r);
            // Return the request status
            RequestStatus rs = r.getRequestStatus();
            say("get() initial RequestStatus = " + rs);
            return rs;
        } catch (Exception e) {
            esay(e);
            return createFailedRequestStatus("get error " + e);
        }

    }

    /**
     * this srm method is not implemented
     */
    public RequestStatus getEstGetTime(SRMUser user, RequestCredential credential, String[] SURLS, String[] protocols) {
        return createFailedRequestStatus("time is unknown");
    }

    /**
     * this srm method is not implemented
     */
    public RequestStatus getEstPutTime(SRMUser user, RequestCredential credential,
            String[] src_names,
            String[] dest_names,
            long[] sizes,
            boolean[] wantPermanent,
            String[] protocols) {
        return createFailedRequestStatus("time is unknown");
    }
    /**
     * The implementation of SRM getFileMetaData method.
     * Not really used by anyone.
     *
     * @param user
     *         an instance of the ReSRMUserr null if unknown
     * @param SURLS
     *         the array of SURLs of files of interest
     * @return FileMetaData array assosiated with these SURLs
     */
    private final static String SFN_STRING = "?SFN=";

    public FileMetaData[] getFileMetaData(SRMUser user, RequestCredential credential, String[] SURLS) {
        StringBuffer sb = new StringBuffer();
        sb.append("getFileMetaData(");
        if (SURLS == null) {
            sb.append("SURLS are null)");
            say(sb.toString());
            throw new IllegalArgumentException(sb.toString());
        }

        int len = SURLS.length;
        for (int i = 0; i < len; ++i) {
            sb.append(SURLS[i] + ",");
        }
        sb.append(")");
        say(sb.toString());

        FileMetaData[] fmds = new FileMetaData[len];
        // call getFileMetaData(String path) for each SURL in array
        for (int i = 0; i < len; ++i) {
            try {
                GlobusURL url = new GlobusURL(SURLS[i]);
                if (!Tools.sameHost(configuration.getSrmhost(),
                        url.getHost())) {
                    String error = "getFileMetaData: surl is not local : " + url.getURL();
                    esay(error);
                    throw new IllegalArgumentException(error);
                }

                String surlpath = url.getPath();
                int indx = surlpath.indexOf(SFN_STRING);
                if (indx != -1) {

                    surlpath = surlpath.substring(indx + SFN_STRING.length());
                }

                say("getFileMetaData(String[]) calling FileMetaData(" + surlpath + ")");

                FileMetaData fmd = storage.getFileMetaData(user, surlpath);
                fmd.SURL = SURLS[i];
                fmds[i] = new FileMetaData(fmd);
                say("FileMetaData[" + i + "]=" + fmds[i]);
            } catch (Exception e) {
                esay("getFileMetaData failed to parse SURL: " + e);
                throw new IllegalArgumentException("getFileMetaData failed to parse SURL: " + e);
            }
        }

        return fmds;
    }

    /**
     * not implemented
     */
    public String[] getProtocols(SRMUser user, RequestCredential credential) {
        try {
            return storage.supportedGetProtocols();
        } catch (SRMException srme) {
            return new String[0];
        }
    }

    /**
     * The implementation of SRM getRequestStatus method.
     *
     * @param user
     *         an instance of the ReSRMUserr null if unknown
     * @param requestId
     *         the id of the previously issued request
     * @return request status assosiated with this request
     */
    public RequestStatus getRequestStatus(SRMUser user, RequestCredential credential, int requestId) {
        say(" getRequestStatus(" + user + "," + requestId + ")");
        try {
            // Try to get the request with such id
            say("getRequestStatus() Request.getRequest(" + requestId + ");");
            ContainerRequest r = (ContainerRequest) ContainerRequest.getRequest(requestId);
            say("getRequestStatus() received Request  ");
            if (r != null) {
                // we found one make sure it is the same  user
                SRMUser req_user = r.getSRMUser();
                if (req_user == null || req_user.equals(user)) {
                    // say(" getRequestStatus() request found, returns request file status");
                    // and return the request status
                    RequestStatus rs = r.getRequestStatus();
                    say("obtained request status, returning rs for request id=" + requestId);
                    return rs;
                } else {
                    return createFailedRequestStatus("getRequestStatus(): request #" + requestId +
                            " owned by "+req_user+" does not belong to user " + user, requestId);
                }
            }
            return createFailedRequestStatus("getRequestStatus() request #" + requestId +
                    " was not found", requestId);
        } catch (Exception e) {
            esay(e);
            return createFailedRequestStatus("getting request #" + requestId +
                    " generated error : " + e, requestId);
        }
    }

    public RequestStatus mkPermanent(SRMUser user, RequestCredential credential, String[] SURLS) {
        return createFailedRequestStatus("not supported, all files are already permanent");
    }

    /**
     * The implementation of SRM pin method.
     * Currenly Not Implemented
     *
     * @param user
     *         an instance of the ReSRMUserr null if unknown
     * @param turls
     *         array of TURL (Transfer URL) strings
     * @return request status assosiated with this request
     */
    public RequestStatus pin(SRMUser user, RequestCredential credential, String[] TURLS) {
        return createFailedRequestStatus("pins by users are not supported, use get instead");
    }

    /**
     * used for testing only
     *
     * @param user
     *         an instance of the RSRMUseror null if unknown
     */
    public boolean ping(SRMUser user, RequestCredential credential) {
        return true;
    }

    /**
     * The implementation of SRM put method.
     *
     * @param user
     *         an instance of the ReSRMUserr null if unknown
     * @param requestId
     *         the id of the previously issued pin request
     * @param fileId
     *         the id of the file within pin request
     * @param state
     *         the new state of the request
     * @return request status assosiated with this request
     */
    public RequestStatus put(SRMUser user,
            RequestCredential credential,
            String[] sources,
            String[] dests,
            long[] sizes,
            boolean[] wantPerm,
            String[] protocols,
            String clientHost) {
        int len = dests.length;
        String[] dests_urls = new String[len];

        String srmprefix;
        // we do this to support implementations that
        // supply paths instead of the whole urls
        // this is not part of the spec

        for (int i = 0; i < len; ++i) {
            for (int j = 0; j < len; ++j) {
                if (i != j) {
                    if (dests[i].equals(dests[j])) {
                        return createFailedRequestStatus(
                                "put(): list of sources contains the same url twice " +
                                "url#" + i + " is " + dests[i] +
                                " and url#" + j + " is " + dests[j]);
                    }
                }
            }
        }

        srmprefix = "srm://" + configuration.getSrmhost() +
                ":" + configuration.getPort() + "/";

        for (int i = 0; i < len; ++i) {
            if (dests[i].startsWith("srm://")) {
                dests_urls[i] = dests[i];
            } else {
                dests_urls[i] = srmprefix + dests[i];
            }
        }
        try {

            String[] supportedProtocols = storage.supportedPutProtocols();
            boolean foundMatchedProtocol = false;
            for (String supportedProtocol : supportedProtocols) {
                for (String protocol : protocols) {
                    if (supportedProtocol.equals(protocol)) {
                        foundMatchedProtocol = true;
                        break;
                    }
                }
            }
            if (!foundMatchedProtocol) {
                StringBuffer errorsb =
                        new StringBuffer("Protocol(s) specified not supported: [ ");
                for (String protocol : protocols) {
                    errorsb.append(protocol).append(' ');
                }
                errorsb.append(']');
                return createFailedRequestStatus(errorsb.toString());
            }
            // create a new put request
            ContainerRequest r = new PutRequest(user,
                    credential.getId(),
                    putStorage,
                    sources, dests_urls, sizes,
                    wantPerm, protocols, configuration, configuration.getPutLifetime(),
                    putFileRequestStorage,
                    configuration.getPutRetryTimeout(),
                    configuration.getPutMaxNumOfRetries(),
                    clientHost,
                    null,
                    null,
                    null,
                    null);
            r.schedule();
            // return status
            return r.getRequestStatus();
        } catch (Exception e) {
            esay(e);
            return createFailedRequestStatus("put(): error " + e);
        }
    }

    /**
     * The implementation of SRM setFileStatus method.
     *  the only status that user can set file request into
     *  is "Done" status
     *
     * @param user
     *         an instance of the ReSRMUserr null if unknown
     * @param requestId
     *         the id of the previously issued pin request
     * @param fileId
     *         the id of the file within pin request
     * @param state
     *         the new state of the request
     * @return request status assosiated with this request
     */
    public RequestStatus setFileStatus(SRMUser user, RequestCredential credential,
            int requestId, int fileRequestId, String state) {
        try {
            say(" setFileStatus(" + requestId + "," + fileRequestId + "," + state + ");");
            if (!state.equalsIgnoreCase("done") && !state.equalsIgnoreCase("running")) {
                return createFailedRequestStatus("setFileStatus(): incorrect state " + state);
            }

            //try to get the request
            ContainerRequest r = (ContainerRequest) ContainerRequest.getRequest(requestId);
            if (r == null) {
                return createFailedRequestStatus("setFileStatus(): request #" + requestId + " was not found");
            }
            // check that user is the same
            SRMUser req_user = r.getUser();
            if (req_user != null && !req_user.equals(user)) {
                return createFailedRequestStatus(
                        "request #" + requestId + " owned by "+req_user +" does not belong to user " + user);
            }
            // get file request from request
            FileRequest fr = r.getFileRequest(fileRequestId);
            if (fr == null) {
                return createFailedRequestStatus("request #" + requestId +
                        " does not contain file request #" + fileRequestId);
            }
            synchronized (fr) {
                State s = fr.getState();
                if (s == State.CANCELED || s == State.DONE || s == State.FAILED) {
                    say("can not set status, the file status is already " + s);
                } else {
                    if (state.equalsIgnoreCase("done") && fr instanceof PutFileRequest &&
                            (fr.getState() == State.READY || fr.getState() == State.RUNNING)) {
                        PutFileRequest pfr = (PutFileRequest) fr;
                        if (pfr.getTurlString() != null) {
                            try {
                                if (storage.exists(user, pfr.getPath())) {
                                    fr.setStatus(state);
                                } else {
                                    pfr.setState(State.FAILED, "file transfer was not performed on SURL");
                                }
                            } catch (SRMException srme) {
                                pfr.setState(State.FAILED, "file transfer was not performed on SURL");
                            }
                        }

                    } else {

                        // process request
                        say(" calling fr.setStatus(\"" + state + "\")");
                        fr.setStatus(state);
                    }
                }
            }

            // return request status
            return r.getRequestStatus();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    /**
     * The implementation of SRM unPin method.
     * Currently unimplemented
     *
     * @param user
     *         an instance of the ReSRMUserr null if unknown
     * @param turls
     *         array of TURL (Transfer URL) strings
     * @param requestId
     *         the id of the previously issued pin request
     * @return request status assosiated with this request
     */
    public RequestStatus unPin(SRMUser user, RequestCredential credential,
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
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    private RequestStatus createFailedRequestStatus(String error) {
        esay("creating a failed request status with a message: " + error);
        RequestStatus rs = new RequestStatus();
        rs.requestId = -1;
        rs.errorMessage = error;
        rs.state = "Failed";
        return rs;
    }

    private RequestStatus createFailedRequestStatus(String error, int requestId) {
        esay("creating a failed request status with a message: " + error);
        RequestStatus rs = new RequestStatus();
        rs.requestId = requestId;
        rs.errorMessage = error;
        rs.state = "Failed";
        return rs;
    }

    public StorageElementInfo getStorageElementInfo(
            SRMUser user,
            RequestCredential credential) throws SRMException {
        return storage.getStorageElementInfo(user);
    }

    public void listGetRequests(StringBuffer sb) throws java.sql.SQLException {
        listRequests(sb,GetRequest.class);
    }

    public Set getGetRequestIds(SRMUser user, String description) throws java.sql.SQLException {
        return getActiveJobIds(GetRequest.class,description);
    }

    public Set getLsRequestIds(SRMUser user, String description) throws java.sql.SQLException {
        return getActiveJobIds(LsRequest.class,description);
    }

    public void listLatestCompletedGetRequests(StringBuffer sb, int maxCount) throws java.sql.SQLException {
        Set activeRequestIds = getStorage.getLatestCompletedRequestIds(maxCount);
        for (Iterator i = activeRequestIds.iterator(); i.hasNext();) {
            Long requestId = (Long) i.next();
            try {
                GetRequest gr = (GetRequest) ContainerRequest.getRequest(requestId);
                sb.append(gr).append('\n');
            } catch (SRMInvalidRequestException ire) {
                logger.error(ire);
            }
        }

    }

    public void listLatestFailedGetRequests(StringBuffer sb, int maxCount) throws java.sql.SQLException {
        Set activeRequestIds = getStorage.getLatestFailedRequestIds(maxCount);
        for (Iterator i = activeRequestIds.iterator(); i.hasNext();) {
            Long requestId = (Long) i.next();
            try {
                GetRequest gr = (GetRequest) ContainerRequest.getRequest(requestId);
                sb.append(gr).append('\n');
            } catch (SRMInvalidRequestException ire) {
                logger.error(ire);
            }
        }

    }

    public void listLatestDoneGetRequests(StringBuffer sb, int maxCount) throws java.sql.SQLException {
        Set activeRequestIds = getStorage.getLatestDoneRequestIds(maxCount);
        for (Iterator i = activeRequestIds.iterator(); i.hasNext();) {
            Long requestId = (Long) i.next();
            try {
                GetRequest gr = (GetRequest) ContainerRequest.getRequest(requestId);
                sb.append(gr).append('\n');
            } catch (SRMInvalidRequestException ire) {
                logger.error(ire);
            }
        }

    }

    public void listLatestCanceledGetRequests(StringBuffer sb, int maxCount) throws java.sql.SQLException {
        Set activeRequestIds = getStorage.getLatestCanceledRequestIds(maxCount);
        for (Iterator i = activeRequestIds.iterator(); i.hasNext();) {
            Long requestId = (Long) i.next();
            try {
                GetRequest gr = (GetRequest) ContainerRequest.getRequest(requestId);
                sb.append(gr).append('\n');
            } catch (SRMInvalidRequestException ire) {
                logger.error(ire);
            }
        }
    }

    public void printGetSchedulerInfo(StringBuffer sb) throws java.sql.SQLException {
        getGetRequestScheduler().getInfo(sb);
    }

    public void printLsSchedulerInfo(StringBuffer sb) throws java.sql.SQLException {
        getLsRequestScheduler().getInfo(sb);
    }

    public void printGetSchedulerThreadQueue(StringBuffer sb) throws java.sql.SQLException {
        getGetRequestScheduler().printThreadQueue(sb);

    }

    public void printGetSchedulerPriorityThreadQueue(StringBuffer sb) throws java.sql.SQLException {
        getGetRequestScheduler().printPriorityThreadQueue(sb);
    }

    public void printGetSchedulerReadyThreadQueue(StringBuffer sb) throws java.sql.SQLException {
        getGetRequestScheduler().printReadyQueue(sb);

    }

    public void printLsSchedulerThreadQueue(StringBuffer sb) throws java.sql.SQLException {
        getLsRequestScheduler().printThreadQueue(sb);

    }

    public void printLsSchedulerPriorityThreadQueue(StringBuffer sb) throws java.sql.SQLException {
        getLsRequestScheduler().printPriorityThreadQueue(sb);
    }

    public void printLsSchedulerReadyThreadQueue(StringBuffer sb) throws java.sql.SQLException {
        getLsRequestScheduler().printReadyQueue(sb);

    }

    public void listPutRequests(StringBuffer sb) throws java.sql.SQLException {
        listRequests(sb,PutRequest.class);
    }

    public Set getPutRequestIds(SRMUser user, String description) throws java.sql.SQLException {
        return getActiveJobIds(PutRequest.class,description);
    }

    public void listLatestCompletedPutRequests(StringBuffer sb, int maxCount) throws java.sql.SQLException {
        Set activeRequestIds = putStorage.getLatestCompletedRequestIds(maxCount);
        for (Iterator i = activeRequestIds.iterator(); i.hasNext();) {
            Long requestId = (Long) i.next();
            try {
                PutRequest pr = (PutRequest) ContainerRequest.getRequest(requestId);
                sb.append(pr).append('\n');
            } catch (SRMInvalidRequestException ire) {
                logger.error(ire);
            }
        }
    }

    public void listLatestFailedPutRequests(StringBuffer sb, int maxCount) throws java.sql.SQLException {
        Set activeRequestIds = putStorage.getLatestFailedRequestIds(maxCount);
        for (Iterator i = activeRequestIds.iterator(); i.hasNext();) {
            Long requestId = (Long) i.next();
            try {
                PutRequest pr = (PutRequest) ContainerRequest.getRequest(requestId);
                sb.append(pr).append('\n');
            } catch (SRMInvalidRequestException ire) {
                logger.error(ire);
            }
        }
    }

    public void listLatestCanceledPutRequests(StringBuffer sb, int maxCount) throws java.sql.SQLException {
        Set activeRequestIds = putStorage.getLatestCanceledRequestIds(maxCount);
        for (Iterator i = activeRequestIds.iterator(); i.hasNext();) {
            Long requestId = (Long) i.next();
            try {
                PutRequest pr = (PutRequest) ContainerRequest.getRequest(requestId);
                sb.append(pr).append('\n');
            } catch (SRMInvalidRequestException ire) {
                logger.error(ire);
            }
        }
    }

    public void listLatestDonePutRequests(StringBuffer sb, int maxCount) throws java.sql.SQLException {
        Set activeRequestIds = putStorage.getLatestDoneRequestIds(maxCount);
        for (Iterator i = activeRequestIds.iterator(); i.hasNext();) {
            Long requestId = (Long) i.next();
            try {
                PutRequest pr = (PutRequest) ContainerRequest.getRequest(requestId);
                sb.append(pr).append('\n');
            } catch (SRMInvalidRequestException ire) {
                logger.error(ire);
            }
        }
    }

    public void printPutSchedulerInfo(StringBuffer sb) throws java.sql.SQLException {
        getPutRequestScheduler().getInfo(sb);
    }

    public void printPutSchedulerThreadQueue(StringBuffer sb) throws java.sql.SQLException {
        getPutRequestScheduler().printThreadQueue(sb);

    }

    public void printPutSchedulerPriorityThreadQueue(StringBuffer sb) throws java.sql.SQLException {
        getPutRequestScheduler().printPriorityThreadQueue(sb);
    }

    public void printPutSchedulerReadyThreadQueue(StringBuffer sb) throws java.sql.SQLException {
        getPutRequestScheduler().printReadyQueue(sb);

    }

    public void listCopyRequests(StringBuffer sb) throws java.sql.SQLException {
        listRequests(sb,CopyRequest.class);
    }

    public Set getCopyRequestIds(SRMUser user, String description) throws java.sql.SQLException {
        return getActiveJobIds(CopyRequest.class,description);
    }

    public void listLatestCompletedCopyRequests(StringBuffer sb, int maxCount) throws java.sql.SQLException {
        Set activeRequestIds = copyStorage.getLatestCompletedRequestIds(maxCount);
        for (Iterator i = activeRequestIds.iterator(); i.hasNext();) {
            Long requestId = (Long) i.next();
            try {
                CopyRequest cr = (CopyRequest) ContainerRequest.getRequest(requestId);
                sb.append(cr).append('\n');
            } catch (SRMInvalidRequestException ire) {
                logger.error(ire);
            }
        }
    }

    public void listLatestFailedCopyRequests(StringBuffer sb, int maxCount) throws java.sql.SQLException {
        Set activeRequestIds = copyStorage.getLatestFailedRequestIds(maxCount);
        for (Iterator i = activeRequestIds.iterator(); i.hasNext();) {
            Long requestId = (Long) i.next();
            try {
                CopyRequest cr = (CopyRequest) ContainerRequest.getRequest(requestId);
                sb.append(cr).append('\n');
            } catch (SRMInvalidRequestException ire) {
                logger.error(ire);
            }
        }
    }

    public void listLatestCanceledCopyRequests(StringBuffer sb, int maxCount) throws java.sql.SQLException {
        Set activeRequestIds = copyStorage.getLatestCanceledRequestIds(maxCount);
        for (Iterator i = activeRequestIds.iterator(); i.hasNext();) {
            Long requestId = (Long) i.next();
            try {
                CopyRequest cr = (CopyRequest) ContainerRequest.getRequest(requestId);
                sb.append(cr).append('\n');
            } catch (SRMInvalidRequestException ire) {
                logger.error(ire);
            }
        }
    }

    public void listLatestDoneCopyRequests(StringBuffer sb, int maxCount) throws java.sql.SQLException {
        Set activeRequestIds = copyStorage.getLatestDoneRequestIds(maxCount);
        for (Iterator i = activeRequestIds.iterator(); i.hasNext();) {
            Long requestId = (Long) i.next();
            try {
                CopyRequest cr = (CopyRequest) ContainerRequest.getRequest(requestId);
                sb.append(cr).append('\n');
            } catch (SRMInvalidRequestException ire) {
                logger.error(ire);
            }
        }
    }

    public void printCopySchedulerInfo(StringBuffer sb) throws java.sql.SQLException {
        getCopyRequestScheduler().getInfo(sb);
    }

    public void printCopySchedulerThreadQueue(StringBuffer sb) throws java.sql.SQLException {
        getCopyRequestScheduler().printThreadQueue(sb);

    }

    public void printCopySchedulerPriorityThreadQueue(StringBuffer sb) throws java.sql.SQLException {
        getCopyRequestScheduler().printPriorityThreadQueue(sb);
    }

    public void printCopySchedulerReadyThreadQueue(StringBuffer sb) throws java.sql.SQLException {
        getCopyRequestScheduler().printReadyQueue(sb);

    }

    public void listBringOnlineRequests(StringBuffer sb) throws java.sql.SQLException {
        listRequests(sb,BringOnlineRequest.class);
    }

    public Set getBringOnlineRequestIds(SRMUser user, String description) throws java.sql.SQLException {
        return getActiveJobIds(BringOnlineRequest.class,description);
    }

    public void listLatestCompletedBringOnlineRequests(StringBuffer sb, int maxCount) throws java.sql.SQLException {
        Set activeRequestIds = bringOnlineStorage.getLatestCompletedRequestIds(maxCount);
        for (Iterator i = activeRequestIds.iterator(); i.hasNext();) {
            Long requestId = (Long) i.next();
            try {
                BringOnlineRequest pr = (BringOnlineRequest) ContainerRequest.getRequest(requestId);
                sb.append(pr).append('\n');
            } catch (SRMInvalidRequestException ire) {
                logger.error(ire);
            }
        }
    }

    public void listLatestFailedBringOnlineRequests(StringBuffer sb, int maxCount) throws java.sql.SQLException {
        Set activeRequestIds = bringOnlineStorage.getLatestFailedRequestIds(maxCount);
        for (Iterator i = activeRequestIds.iterator(); i.hasNext();) {
            Long requestId = (Long) i.next();
            try {
                BringOnlineRequest pr = (BringOnlineRequest) ContainerRequest.getRequest(requestId);
                sb.append(pr).append('\n');
            } catch (SRMInvalidRequestException ire) {
                logger.error(ire);
            }
        }
    }

    public void listLatestCanceledBringOnlineRequests(StringBuffer sb, int maxCount) throws java.sql.SQLException {
        Set activeRequestIds = bringOnlineStorage.getLatestCanceledRequestIds(maxCount);
        for (Iterator i = activeRequestIds.iterator(); i.hasNext();) {
            Long requestId = (Long) i.next();
            try {
                BringOnlineRequest pr = (BringOnlineRequest) ContainerRequest.getRequest(requestId);
                sb.append(pr).append('\n');
            } catch (SRMInvalidRequestException ire) {
                logger.error(ire);
            }
        }
    }

    public void listLatestDoneBringOnlineRequests(StringBuffer sb, int maxCount) throws java.sql.SQLException {
        Set activeRequestIds = bringOnlineStorage.getLatestDoneRequestIds(maxCount);
        for (Iterator i = activeRequestIds.iterator(); i.hasNext();) {
            Long requestId = (Long) i.next();
            try {
                BringOnlineRequest pr = (BringOnlineRequest) ContainerRequest.getRequest(requestId);
                sb.append(pr).append('\n');
            } catch (SRMInvalidRequestException ire) {
                logger.error(ire);
            }
        }
    }

    public void printBringOnlineSchedulerInfo(StringBuffer sb) throws java.sql.SQLException {
        getBringOnlineRequestScheduler().getInfo(sb);
    }

    public void printBringOnlineSchedulerThreadQueue(StringBuffer sb) throws java.sql.SQLException {
        getBringOnlineRequestScheduler().printThreadQueue(sb);

    }

    public void printBringOnlineSchedulerPriorityThreadQueue(StringBuffer sb) throws java.sql.SQLException {
        getBringOnlineRequestScheduler().printPriorityThreadQueue(sb);
    }

    public void printBringOnlineSchedulerReadyThreadQueue(StringBuffer sb) throws java.sql.SQLException {
        getBringOnlineRequestScheduler().printReadyQueue(sb);

    }

    private Scheduler getScheduler(Class requestType) {
        return SchedulerFactory.getSchedulerFactory().
                getScheduler(requestType);
    }
    public void listReserveSpaceRequests(StringBuffer sb) throws java.sql.SQLException {
        listRequests(sb,ReserveSpaceRequest.class);
    }

    public void listLsRequests(StringBuffer sb) throws java.sql.SQLException {
        listRequests(sb,LsRequest.class);
    }

    private void listRequests(StringBuffer sb,Class clazz) throws java.sql.SQLException {
        Set<Job> jobs = Job.getActiveJobs(clazz);
        for (Job job:jobs) {
            sb.append(job).append('\n');
        }
    }

    public double getLoad() {
        int copyRunning = getCopyRequestScheduler().getTotalRunningThreads();
        int maxCopyRunning = getCopyRequestScheduler().getThreadPoolSize();
        int getRunning = getGetRequestScheduler().getTotalRunningThreads();
        int maxGetRunning = getGetRequestScheduler().getThreadPoolSize();
        int putRunning = getPutRequestScheduler().getTotalRunningThreads();
        int maxPutRunning = getPutRequestScheduler().getThreadPoolSize();

        double load = (double) copyRunning / (double) maxCopyRunning / 3.0d +
                (double) getRunning / (double) maxGetRunning / 3.0d +
                (double) putRunning / (double) maxPutRunning / 3.0d;
        return load;
    }

    public void listRequest(StringBuffer sb, Long requestId, boolean longformat) throws java.sql.SQLException,
    SRMInvalidRequestException {
        Job job = Job.getJob(requestId);
        if (job == null) {
            sb.append("request with reqiest id " + requestId + " is not found\n");
            return;
        } else {
            sb.append("Job # " + requestId + " is in the state " + job.getState() + "\n");
            if (job instanceof ContainerRequest) {
                sb.append("Job is a Request:\n");
                ContainerRequest r = (ContainerRequest) job;
                sb.append(r.toString(longformat)).append('\n');
            } else if (job instanceof FileRequest) {
                FileRequest fr = (FileRequest) job;
                sb.append("Job is a FileRequest from Request #" + fr.getRequestId() + " \n");
                sb.append(fr.toString(longformat));
            }
        }
    }

    public void cancelRequest(StringBuffer sb, Long requestId) throws java.sql.SQLException,
    SRMInvalidRequestException {
        Job job = Job.getJob(requestId);
        if (job == null || !(job instanceof ContainerRequest)) {
            sb.append("request with reqiest id " + requestId + " is not found\n");
            return;
        }
        ContainerRequest r = (ContainerRequest) job;
        try {
            synchronized (job) {
                State s = job.getState();
                if (State.isFinalState(s)) {
                    sb.append("job state is already " + s + " can not cancel\n");
                } else {
                    r.setState(State.CANCELED, "Canceled by admin through cancel command");
                    sb.append("state changed, no warranty that the proccess will end immediately\n");
                    sb.append(r.toString(false)).append('\n');
                }
            }
        } catch (IllegalStateTransition ist) {
            sb.append(ist);
            esay(ist);
        }
    }

    public void cancelAllGetRequest(StringBuffer sb, String pattern)
            throws java.sql.SQLException, SRMInvalidRequestException {

        cancelAllRequest(sb, pattern, getGetRequestScheduler(), getStorage);
    }

    public void cancelAllBringOnlineRequest(StringBuffer sb, String pattern) 
            throws java.sql.SQLException, SRMInvalidRequestException {

        cancelAllRequest(sb, pattern, getBringOnlineRequestScheduler(), bringOnlineStorage);
    }

    public void cancelAllPutRequest(StringBuffer sb, String pattern)
            throws java.sql.SQLException, SRMInvalidRequestException {

        cancelAllRequest(sb, pattern, getPutRequestScheduler(), putStorage);
    }

    public void cancelAllCopyRequest(StringBuffer sb, String pattern) 
            throws java.sql.SQLException, SRMInvalidRequestException {

        cancelAllRequest(sb, pattern, getCopyRequestScheduler(), copyStorage);
    }

    public void cancelAllReserveSpaceRequest(StringBuffer sb, String pattern)
            throws java.sql.SQLException, SRMInvalidRequestException {

        cancelAllRequest(sb, pattern, getReserveSpaceScheduler(), reserveSpaceRequestStorage);
    }

    public void cancelAllLsRequests(StringBuffer sb, String pattern)
            throws java.sql.SQLException, SRMInvalidRequestException {

        cancelAllRequest(sb, pattern, getLsRequestScheduler(), lsRequestStorage);
    }

    private void cancelAllRequest(StringBuffer sb,
            String pattern,
            Scheduler scheduler,
            DatabaseRequestStorage storage) throws java.sql.SQLException,
            SRMInvalidRequestException {

        java.util.Set<Long> jobsToKill = new java.util.HashSet<Long>();
        java.util.regex.Pattern p = java.util.regex.Pattern.compile(pattern);
        Set activeRequestIds =
                storage.getActiveRequestIds(scheduler.getId());
        for (Iterator i = activeRequestIds.iterator(); i.hasNext();) {
            Long requestId = (Long) i.next();
            java.util.regex.Matcher m = p.matcher(requestId.toString());
            if (m.matches()) {
                say("cancelAllRequest: request Id #" + requestId + " in " + scheduler + " matches pattern!");
                jobsToKill.add(requestId);
            }
        }
        if (jobsToKill.isEmpty()) {
            sb.append("no requests match the pattern=\"" + pattern + " in scheduler " +
                    scheduler + "\n");
            return;
        }
        for (Long requestId : jobsToKill) {
            Job job = Job.getJob(requestId);
            if (job == null || !(job instanceof ContainerRequest)) {
                esay(" request with reqiest id " + requestId + " is not found\n");
                continue;
            }
            final ContainerRequest r = (ContainerRequest) job;
            sb.append("request #" + requestId + " matches pattern=\"" + pattern + "\"; canceling request \n");
            new Thread(new Runnable() {

                public void run() {
                    synchronized (r) {
                        try {
                            State s = r.getState();
                            if (!State.isFinalState(s)) {
                                r.setState(State.CANCELED, "Canceled by admin through cancelall command");
                            }
                        } catch (IllegalStateTransition ist) {
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
        return getScheduler(GetFileRequest.class);
    }

    public Scheduler getBringOnlineRequestScheduler() {
        return getScheduler(BringOnlineFileRequest.class);
    }

    public Scheduler getPutRequestScheduler() {
        return  getScheduler(PutFileRequest.class);
    }

    public Scheduler getCopyRequestScheduler() {
        return getScheduler(CopyRequest.class);
    }

    public ReserveSpaceRequestStorage getReserveSpaceRequestStorage() {
        return reserveSpaceRequestStorage;
    }

    public LsRequestStorage getLsRequestStorage() {
        return lsRequestStorage;
    }

    public LsFileRequestStorage getLsFileRequestStorage() {
        return lsFileRequestStorage;
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
        return getScheduler(ReserveSpaceRequest.class);
    }

    public Scheduler getLsRequestScheduler() {
        return getScheduler(LsFileRequest.class);
    }

    public static Set<Long> getActiveJobIds(Class type, String description) {
        Set<Job> jobs = Job.getActiveJobs(type);
        Set<Long> ids = new HashSet<Long>();
        for(Job job: jobs) {
            if(description != null ) {
                if( job instanceof Request ) {
                    Request r = (Request) job;
                    if(description.equals(r.getDescription())) {
                        ids.add( job.getId());
                    }
                }
            } else {
                ids.add( job.getId());
            }
        }
        return ids;
    }


}
