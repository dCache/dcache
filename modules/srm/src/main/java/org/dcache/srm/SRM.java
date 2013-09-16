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

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.URI;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import diskCacheV111.srm.FileMetaData;
import diskCacheV111.srm.RequestStatus;
import diskCacheV111.srm.StorageElementInfo;

import org.dcache.commons.stats.MonitoringProxy;
import org.dcache.commons.stats.RequestCounters;
import org.dcache.commons.stats.RequestExecutionTimeGauges;
import org.dcache.commons.stats.rrd.RrdRequestCounters;
import org.dcache.commons.stats.rrd.RrdRequestExecutionTimeGauges;
import org.dcache.srm.request.BringOnlineFileRequest;
import org.dcache.srm.request.BringOnlineRequest;
import org.dcache.srm.request.ContainerRequest;
import org.dcache.srm.request.CopyFileRequest;
import org.dcache.srm.request.CopyRequest;
import org.dcache.srm.request.FileRequest;
import org.dcache.srm.request.GetFileRequest;
import org.dcache.srm.request.GetRequest;
import org.dcache.srm.request.Job;
import org.dcache.srm.request.LsFileRequest;
import org.dcache.srm.request.LsRequest;
import org.dcache.srm.request.PutFileRequest;
import org.dcache.srm.request.PutRequest;
import org.dcache.srm.request.Request;
import org.dcache.srm.request.RequestCredential;
import org.dcache.srm.request.RequestCredentialStorage;
import org.dcache.srm.request.ReserveSpaceRequest;
import org.dcache.srm.request.sql.DatabaseJobStorageFactory;
import org.dcache.srm.request.sql.DatabaseRequestCredentialStorage;
import org.dcache.srm.request.sql.RequestsPropertyStorage;
import org.dcache.srm.scheduler.IllegalStateTransition;
import org.dcache.srm.scheduler.JobStorage;
import org.dcache.srm.scheduler.JobStorageFactory;
import org.dcache.srm.scheduler.Scheduler;
import org.dcache.srm.scheduler.SchedulerFactory;
import org.dcache.srm.scheduler.State;
import org.dcache.srm.util.Configuration;
import org.dcache.srm.v2_2.TFileStorageType;

/**
 * SRM class creates an instance of SRM client class and publishes it on a
 * given port as a storage element.
 *
 * @author  timur
 */
public class SRM {
    private static final Logger logger = LoggerFactory.getLogger(SRM.class);
    private final InetAddress host;
    private final Configuration configuration;
    private final RequestCredentialStorage requestCredentialStorage;
    private AbstractStorageElement storage;
    private final RequestCounters<Class<?>> srmServerV2Counters;
    private final RequestCounters<String> srmServerV1Counters;
    private final RequestCounters<Method> abstractStorageElementCounters;
    private RrdRequestCounters<?> rrdSrmServerV2Counters;
    private RrdRequestCounters<?> rrdSrmServerV1Counters;
    private RrdRequestCounters<?> rrdAstractStorageElementCounters;
    private final RequestExecutionTimeGauges<Class<?>> srmServerV2Gauges;
    private final RequestExecutionTimeGauges<String> srmServerV1Gauges;
    private final RequestExecutionTimeGauges<Method> abstractStorageElementGauges;
    private RrdRequestExecutionTimeGauges<?> rrdSrmServerV2Gauges;
    private RrdRequestExecutionTimeGauges<?> rrdSrmServerV1Gauges;
    private RrdRequestExecutionTimeGauges<?> rrdAstractStorageElementGauges;

    private static SRM srm;
    /**
     * Creates a new instance of SRM
     * @param config
     * @param name
     * @throws IOException
     * @throws SQLException
     * @throws InterruptedException
     * @throws IllegalStateTransition
     */
    private SRM(Configuration config, String name)
            throws IOException,
            SQLException,
            InterruptedException,
            IllegalStateTransition {
        this.configuration = config;
        //First of all decorate the storage with counters and
        // gauges to measure the performance of storage operations
        this.storage = config.getStorage();
        abstractStorageElementCounters=
                new RequestCounters<>(
                        this.storage.getClass().getName());
        abstractStorageElementGauges =
                new RequestExecutionTimeGauges<>(
                        this.storage.getClass().getName());
        this.storage = MonitoringProxy.decorateWithMonitoringProxy(
                new Class[]{AbstractStorageElement.class},
                this.storage,
                abstractStorageElementCounters,
                abstractStorageElementGauges);
        config.setStorage(this.storage);

        srmServerV2Counters = new RequestCounters<>("SRMServerV2");
        srmServerV1Counters = new RequestCounters<>("SRMServerV1");
        if (configuration.getCounterRrdDirectory() != null) {
            String rrddir = configuration.getCounterRrdDirectory() +
                    File.separatorChar + "srmv1";
            rrdSrmServerV1Counters =
                    new RrdRequestCounters<>(srmServerV1Counters, rrddir);
            rrdSrmServerV1Counters.startRrdUpdates();
            rrdSrmServerV1Counters.startRrdGraphPlots();
            rrddir = configuration.getCounterRrdDirectory() +
                    File.separatorChar + "srmv2";
            rrdSrmServerV2Counters =
                    new RrdRequestCounters<>(srmServerV2Counters, rrddir);
            rrdSrmServerV2Counters.startRrdUpdates();
            rrdSrmServerV2Counters.startRrdGraphPlots();
            rrddir =  configuration.getCounterRrdDirectory() +
                    File.separatorChar + "storage";

            rrdAstractStorageElementCounters =
                    new RrdRequestCounters<>(abstractStorageElementCounters, rrddir);
            rrdAstractStorageElementCounters.startRrdUpdates();
            rrdAstractStorageElementCounters.startRrdGraphPlots();


        }
        srmServerV2Gauges = new RequestExecutionTimeGauges<>("SRMServerV2");
        srmServerV1Gauges = new RequestExecutionTimeGauges<>("SRMServerV1");
        if (configuration.getGaugeRrdDirectory() != null) {
            File rrddir = new File(configuration.getGaugeRrdDirectory() +
                    File.separatorChar + "srmv1");
            rrdSrmServerV1Gauges =
                    new RrdRequestExecutionTimeGauges<>(srmServerV1Gauges, rrddir);
            rrdSrmServerV1Gauges.startRrdUpdates();
            rrdSrmServerV1Gauges.startRrdGraphPlots();
            rrddir = new File(configuration.getGaugeRrdDirectory() +
                    File.separatorChar + "srmv2");
            rrdSrmServerV2Gauges =
                    new RrdRequestExecutionTimeGauges<>(srmServerV2Gauges, rrddir);
            rrdSrmServerV2Gauges.startRrdUpdates();
            rrdSrmServerV2Gauges.startRrdGraphPlots();
            rrddir = new File (configuration.getGaugeRrdDirectory() +
                    File.separatorChar + "storage");

            rrdAstractStorageElementGauges =
                    new RrdRequestExecutionTimeGauges<>(abstractStorageElementGauges, rrddir);
            rrdAstractStorageElementGauges.startRrdUpdates();
            rrdAstractStorageElementGauges.startRrdGraphPlots();
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
        SchedulerFactory.initSchedulerFactory(config, name);
        DatabaseJobStorageFactory afactory = new DatabaseJobStorageFactory(configuration);
        JobStorageFactory.initJobStorageFactory(afactory);
        afactory.init();

        host = InetAddress.getLocalHost();

        configuration.addSrmHost(host.getCanonicalHostName());
        logger.debug("srm started :\n\t" + configuration.toString());
    }

    /**
     * SRM is now a singleton, this will return an instance of
     * will create a new SRM if it does not exist
     * @param configuration
     * @param name
     * @return SRM
     * @throws IOException
     * @throws SQLException
     * @throws InterruptedException
     * @throws IllegalStateTransition
     */
    public static synchronized final SRM getSRM(Configuration configuration,
            String name) throws IOException,
            SQLException,
            InterruptedException,
            IllegalStateTransition {

        if(srm != null) {
            return srm;
        }

        srm = new SRM(configuration, name);
        SRM.class.notifyAll();
        return srm;
    }

    /**
     *
     * @return instance of SRM if it was created or null if it was not
     */
    public static synchronized final SRM getSRM() {
        return srm;
    }

    public void stop() {
        SchedulerFactory.getSchedulerFactory().shutdown();
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
     * @return the srmServerCounters
     */
    public RequestCounters<Class<?>> getSrmServerV2Counters() {
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
    public RequestExecutionTimeGauges<Class<?>> getSrmServerV2Gauges() {
        return srmServerV2Gauges;
    }

    /**
     * @return the srmServerV1Gauges
     */
    public RequestExecutionTimeGauges<String> getSrmServerV1Gauges() {
        return srmServerV1Gauges;
    }

    /**
     * Get the storage that this srm is working with
     * @return the storage
     */
    public final AbstractStorageElement getStorage() {
        return storage;
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

        private boolean done;
        private boolean success = true;
        SRMUser user;
        URI surl;
        String error;

        public TheAdvisoryDeleteCallbacks(SRMUser user, URI surl) {
            this.user = user;
            this.surl = surl;
        }

        @Override
        public void AdvisoryDeleteFailed(String reason) {
            error = " advisoryDelete(" + user + "," + surl + ") AdvisoryDeleteFailed: " + reason;
            success = false;
            logger.error(error);
            done();
        }

        @Override
        public void AdvisoryDeleteSuccesseded() {
            logger.debug(" advisoryDelete(" + user + "," + surl + ") AdvisoryDeleteSuccesseded");
            done();
        }

        @Override
        public void Exception(Exception e) {
            error = " advisoryDelete(" + user + "," + surl + ") Exception :" + e;
            logger.error(error);
            success = false;
            done();
        }

        @Override
        public void Timeout() {
            error = " advisoryDelete(" + user + "," + surl + ") Timeout ";
            logger.error(error);
            success = false;
            done();
        }

        @Override
        public void Error(String error) {
            this.error = " advisoryDelete(" + user + "," + surl + ") Error " + error;
            logger.error(this.error);
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
                            error = " advisoryDelete(" + user + "," + surl + ") Timeout";
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

        public String getError() {
            return error;
        }
    }

    public void advisoryDelete(final SRMUser user, RequestCredential credential, String[] SURLS) {
        logger.debug("SRM.advisoryDelete");
        if (user == null) {
            String error = "advisoryDelete: user is unknown," +
                    " user needs authorization to delete ";
            logger.error(error);
            throw new IllegalArgumentException(error);
        }

        TheAdvisoryDeleteCallbacks callabacks_array[] =
                new TheAdvisoryDeleteCallbacks[SURLS.length];
        for (int i = 0; i < SURLS.length; ++i) {
            try {
                URI surl = new URI(SURLS[i]);
                callabacks_array[i] = new TheAdvisoryDeleteCallbacks(user, surl);
                storage.advisoryDelete(user, surl, callabacks_array[i]);

            } catch (RuntimeException re) {
                logger.error(re.toString());
                throw re;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        boolean failed = false;
        StringBuilder errorsb = new StringBuilder();
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
                logger.error(error);
                return createFailedRequestStatus(error);

            }
            String[] from_urls = srcSURLS;
            String[] to_urls = destSURLS;
            int src_num = from_urls.length;
            int dst_num = to_urls.length;
            // this is for loggin
            StringBuilder sb = new StringBuilder(" copy (");
            for (int j = 0; j < src_num; j++) {
                sb.append("from_urls[").append(j).append("]=").append(from_urls[j]).append(",");
            }
            for (int j = 0; j < dst_num; j++) {
                sb.append("to_urls[").append(j).append("]=").append(to_urls[j]).append(",");
            }
            sb.append(")");
            logger.debug(sb.toString());

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
                logger.debug("credential lifetime is less than default lifetime, using credential lifetime =" + cred_lifetime);
                lifetime = cred_lifetime;
            }
            // create a request object
            logger.debug("calling Request.createCopyRequest()");
            CopyRequest r = new CopyRequest(
                    user,
                    credential.getId(),
                    from_urls,
                    to_urls,
                    null, // no space reservation in v1
                    lifetime,
                    configuration.getCopyRetryTimeout(),
                    configuration.getCopyMaxNumOfRetries(),
                    SRMProtocol.V1_1,
                    TFileStorageType.PERMANENT,
                    null,
                    null, null,
                    client_host,
                    null);
            logger.debug(" Copy Request = " + r);
            // RequesScheduler will take care of the rest
            r.schedule();

            // Return the request status
            RequestStatus rs = r.getRequestStatus();
            logger.debug(" copy returns RequestStatus = " + rs);
            return rs;
        } catch (Exception e) {
            logger.error(e.toString());
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
        // create a request object
        try {
            logger.debug("get(): user = " + user);
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
                StringBuilder errorsb =
                        new StringBuilder("Protocol(s) specified not supported: [ ");
                for (String protocol : protocols) {
                    errorsb.append(protocol).append(' ');
                }
                errorsb.append(']');
                return createFailedRequestStatus(errorsb.toString());
            }
            GetRequest r =
                    new GetRequest(user, credential.getId(),
                            surls, protocols,
                            configuration.getGetLifetime(),
                            configuration.getGetRetryTimeout(),
                            configuration.getGetMaxNumOfRetries(),
                            null,
                            client_host);
            r.schedule();
            // RequestScheduler will take care of the rest
            //getGetRequestScheduler().add(r);
            // Return the request status
            RequestStatus rs = r.getRequestStatus();
            logger.debug("get() initial RequestStatus = " + rs);
            return rs;
        } catch (Exception e) {
            logger.error(e.toString());
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
    public FileMetaData[] getFileMetaData(SRMUser user, RequestCredential credential, String[] SURLS) {
        StringBuilder sb = new StringBuilder();
        sb.append("getFileMetaData(");
        if (SURLS == null) {
            sb.append("SURLS are null)");
            logger.debug(sb.toString());
            throw new IllegalArgumentException(sb.toString());
        }

        int len = SURLS.length;
        for (String surl : SURLS) {
            sb.append(surl).append(",");
        }
        sb.append(")");
        logger.debug(sb.toString());

        FileMetaData[] fmds = new FileMetaData[len];
        // call getFileMetaData(String path) for each SURL in array
        for (int i = 0; i < len; ++i) {
            try {
                URI surl = new URI(SURLS[i]);
                logger.debug("getFileMetaData(String[]) calling FileMetaData({})", surl);
                FileMetaData fmd = storage.getFileMetaData(user, surl, false);
                fmd.SURL = SURLS[i];
                fmds[i] = new FileMetaData(fmd);
                logger.debug("FileMetaData[" + i + "]=" + fmds[i]);
            } catch (Exception e) {
                logger.error("getFileMetaData failed to parse SURL: " + e);
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
        logger.debug(" getRequestStatus(" + user + "," + requestId + ")");
        try {
            // Try to get the request with such id
            logger.debug("getRequestStatus() Request.getRequest(" + requestId + ");");
            ContainerRequest<?> r = Job.getJob((long) requestId, ContainerRequest.class);
            logger.debug("getRequestStatus() received Request  ");
            if (r != null) {
                // we found one make sure it is the same  user
                SRMUser requestUser = r.getUser();
                if (requestUser == null || requestUser.equals(user)) {
                    // and return the request status
                    RequestStatus rs = r.getRequestStatus();
                    logger.debug("obtained request status, returning rs for request id=" + requestId);
                    return rs;
                } else {
                    return createFailedRequestStatus("getRequestStatus(): request #" + requestId +
                            " owned by "+requestUser+" does not belong to user " + user, requestId);
                }
            }
            return createFailedRequestStatus("getRequestStatus() request #" + requestId +
                    " was not found", requestId);
        } catch (Exception e) {
            logger.error(e.toString());
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
     * @param TURLS
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

        srmprefix = "srm://" + configuration.getSrmHost() +
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
                StringBuilder errorsb =
                        new StringBuilder("Protocol(s) specified not supported: [ ");
                for (String protocol : protocols) {
                    errorsb.append(protocol).append(' ');
                }
                errorsb.append(']');
                return createFailedRequestStatus(errorsb.toString());
            }
            // create a new put request
            PutRequest r = new PutRequest(user,
                    credential.getId(),
                    sources, dests_urls, sizes,
                    wantPerm, protocols, configuration.getPutLifetime(),
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
            logger.error(e.toString());
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
     * @param fileRequestId
     *         the id of the file within pin request
     * @param state
     *         the new state of the request
     * @return request status assosiated with this request
     */
    public RequestStatus setFileStatus(SRMUser user, RequestCredential credential,
            int requestId, int fileRequestId, String state) {
        try {
            logger.debug(" setFileStatus(" + requestId + "," + fileRequestId + "," + state + ");");
            if (!state.equalsIgnoreCase("done") && !state.equalsIgnoreCase("running")) {
                return createFailedRequestStatus("setFileStatus(): incorrect state " + state);
            }

            //try to get the request
            ContainerRequest<?> r = Job.getJob((long)requestId, ContainerRequest.class);

            // check that user is the same
            SRMUser req_user = r.getUser();
            if (req_user != null && !req_user.equals(user)) {
                return createFailedRequestStatus(
                        "request #" + requestId + " owned by "+req_user +" does not belong to user " + user);
            }
            // get file request from request
            FileRequest<?> fr = r.getFileRequest(fileRequestId);
            if (fr == null) {
                return createFailedRequestStatus("request #" + requestId +
                        " does not contain file request #" + fileRequestId);
            }
            synchronized (fr) {
                State s = fr.getState();
                if (s.isFinalState()) {
                    logger.debug("can not set status, the file status is already " + s);
                } else {
                    if (state.equalsIgnoreCase("done") && fr instanceof PutFileRequest &&
                            (s == State.READY || s == State.RUNNING)) {
                        PutFileRequest pfr = (PutFileRequest) fr;
                        if (pfr.getTurlString() != null) {
                            try {
                                if (storage.exists(user, pfr.getSurl())) {
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
                        logger.debug(" calling fr.setStatus(\"" + state + "\")");
                        fr.setStatus(state);
                    }
                }
            }

            // return request status
            return r.getRequestStatus();
        } catch(SRMInvalidRequestException e) {
            return createFailedRequestStatus(e.getMessage());
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
     * @param TURLS
     *         array of TURL (Transfer URL) strings
     * @param requestId
     *         the id of the previously issued pin request
     * @return request status assosiated with this request
     */
    public RequestStatus unPin(SRMUser user, RequestCredential credential,
            String[] TURLS, int requestId) {
        return createFailedRequestStatus("pins by users are not supported, use get instead");
    }

    private RequestStatus createFailedRequestStatus(String error) {
        logger.error("creating a failed request status with a message: " + error);
        RequestStatus rs = new RequestStatus();
        rs.requestId = -1;
        rs.errorMessage = error;
        rs.state = "Failed";
        return rs;
    }

    private RequestStatus createFailedRequestStatus(String error, int requestId) {
        logger.error("creating a failed request status with a message: " + error);
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

    public void listGetRequests(StringBuilder sb) throws SQLException {
        listRequests(sb, GetRequest.class);
    }

    public Set<Long> getGetRequestIds(SRMUser user, String description) throws SQLException {
        return getActiveJobIds(GetRequest.class,description);
    }

    public Set<Long> getLsRequestIds(SRMUser user, String description) throws SQLException {
        return getActiveJobIds(LsRequest.class,description);
    }

    public void listLatestCompletedGetRequests(StringBuilder sb, int maxCount) throws SQLException {
        listRequests(sb, getGetStorage().getLatestCompletedJobIds(maxCount), GetRequest.class);
    }

    public void listLatestFailedGetRequests(StringBuilder sb, int maxCount) throws SQLException {
        listRequests(sb, getGetStorage().getLatestFailedJobIds(maxCount), GetRequest.class);
    }

    public void listLatestDoneGetRequests(StringBuilder sb, int maxCount) throws SQLException {
        listRequests(sb, getGetStorage().getLatestDoneJobIds(maxCount), GetRequest.class);
    }

    public void listLatestCanceledGetRequests(StringBuilder sb, int maxCount) throws SQLException {
        listRequests(sb, getGetStorage().getLatestCanceledJobIds(maxCount), GetRequest.class);
    }


    public void printGetSchedulerInfo(StringBuilder sb) throws SQLException {
        getGetRequestScheduler().getInfo(sb);
    }

    public void printLsSchedulerInfo(StringBuilder sb) throws SQLException {
        getLsRequestScheduler().getInfo(sb);
    }

    public void printGetSchedulerThreadQueue(StringBuilder sb) throws SQLException {
        getGetRequestScheduler().printThreadQueue(sb);

    }

    public void printGetSchedulerPriorityThreadQueue(StringBuilder sb) throws SQLException {
        getGetRequestScheduler().printPriorityThreadQueue(sb);
    }

    public void printGetSchedulerReadyThreadQueue(StringBuilder sb) throws SQLException {
        getGetRequestScheduler().printReadyQueue(sb);

    }

    public void printLsSchedulerThreadQueue(StringBuilder sb) throws SQLException {
        getLsRequestScheduler().printThreadQueue(sb);

    }

    public void printLsSchedulerPriorityThreadQueue(StringBuilder sb) throws SQLException {
        getLsRequestScheduler().printPriorityThreadQueue(sb);
    }

    public void printLsSchedulerReadyThreadQueue(StringBuilder sb) throws SQLException {
        getLsRequestScheduler().printReadyQueue(sb);

    }

    public void listPutRequests(StringBuilder sb) throws SQLException {
        listRequests(sb, PutRequest.class);
    }

    public Set<Long> getPutRequestIds(SRMUser user, String description) throws SQLException {
        return getActiveJobIds(PutRequest.class,description);
    }

    public void listLatestCompletedPutRequests(StringBuilder sb, int maxCount) throws SQLException {
        listRequests(sb, getPutStorage().getLatestCompletedJobIds(maxCount), PutRequest.class);
    }

    public void listLatestFailedPutRequests(StringBuilder sb, int maxCount) throws SQLException {
        listRequests(sb, getPutStorage().getLatestFailedJobIds(maxCount), PutRequest.class);
    }

    public void listLatestCanceledPutRequests(StringBuilder sb, int maxCount) throws SQLException {
        listRequests(sb, getPutStorage().getLatestCanceledJobIds(maxCount), PutRequest.class);
    }

    public void listLatestDonePutRequests(StringBuilder sb, int maxCount) throws SQLException {
        listRequests(sb, getPutStorage().getLatestDoneJobIds(maxCount), PutRequest.class);
    }

    public void printPutSchedulerInfo(StringBuilder sb) throws SQLException {
        getPutRequestScheduler().getInfo(sb);
    }

    public void printPutSchedulerThreadQueue(StringBuilder sb) throws SQLException {
        getPutRequestScheduler().printThreadQueue(sb);

    }

    public void printPutSchedulerPriorityThreadQueue(StringBuilder sb) throws SQLException {
        getPutRequestScheduler().printPriorityThreadQueue(sb);
    }

    public void printPutSchedulerReadyThreadQueue(StringBuilder sb) throws SQLException {
        getPutRequestScheduler().printReadyQueue(sb);

    }

    public void listCopyRequests(StringBuilder sb) throws SQLException {
        listRequests(sb, CopyRequest.class);
    }

    public Set<Long> getCopyRequestIds(SRMUser user, String description) throws SQLException {
        return getActiveJobIds(CopyRequest.class,description);
    }

    public void listLatestCompletedCopyRequests(StringBuilder sb, int maxCount) throws SQLException {
        listRequests(sb, getCopyStorage().getLatestCompletedJobIds(maxCount), CopyRequest.class);
    }

    public void listLatestFailedCopyRequests(StringBuilder sb, int maxCount) throws SQLException {
        listRequests(sb, getCopyStorage().getLatestFailedJobIds(maxCount), CopyRequest.class);
    }

    public void listLatestCanceledCopyRequests(StringBuilder sb, int maxCount) throws SQLException {
        listRequests(sb, getCopyStorage().getLatestCanceledJobIds(maxCount), CopyRequest.class);
    }


    public void listLatestDoneCopyRequests(StringBuilder sb, int maxCount) throws SQLException {
        listRequests(sb, getCopyStorage().getLatestDoneJobIds(maxCount), CopyRequest.class);
    }

    public void printCopySchedulerInfo(StringBuilder sb) throws SQLException {
        getCopyRequestScheduler().getInfo(sb);
    }

    public void printCopySchedulerThreadQueue(StringBuilder sb) throws SQLException {
        getCopyRequestScheduler().printThreadQueue(sb);

    }

    public void printCopySchedulerPriorityThreadQueue(StringBuilder sb) throws SQLException {
        getCopyRequestScheduler().printPriorityThreadQueue(sb);
    }

    public void printCopySchedulerReadyThreadQueue(StringBuilder sb) throws SQLException {
        getCopyRequestScheduler().printReadyQueue(sb);

    }

    public void listBringOnlineRequests(StringBuilder sb) throws SQLException {
        listRequests(sb, BringOnlineRequest.class);
    }

    public Set<Long> getBringOnlineRequestIds(SRMUser user, String description) throws SQLException {
        return getActiveJobIds(BringOnlineRequest.class,description);
    }

    public void listLatestCompletedBringOnlineRequests(StringBuilder sb, int maxCount) throws SQLException {
        listRequests(sb, getBringOnlineStorage().getLatestCompletedJobIds(maxCount), BringOnlineRequest.class);
    }

    public void listLatestFailedBringOnlineRequests(StringBuilder sb, int maxCount) throws SQLException {
        listRequests(sb, getBringOnlineStorage().getLatestFailedJobIds(maxCount), BringOnlineRequest.class);
    }

    public void listLatestCanceledBringOnlineRequests(StringBuilder sb, int maxCount) throws SQLException {
        listRequests(sb, getBringOnlineStorage().getLatestCanceledJobIds(maxCount), BringOnlineRequest.class);
    }


    public void listLatestDoneBringOnlineRequests(StringBuilder sb, int maxCount) throws SQLException {
        listRequests(sb, getBringOnlineStorage().getLatestDoneJobIds(maxCount), BringOnlineRequest.class);
    }

    public void printBringOnlineSchedulerInfo(StringBuilder sb) throws SQLException {
        getBringOnlineRequestScheduler().getInfo(sb);
    }

    public void printBringOnlineSchedulerThreadQueue(StringBuilder sb) throws SQLException {
        getBringOnlineRequestScheduler().printThreadQueue(sb);

    }

    public void printBringOnlineSchedulerPriorityThreadQueue(StringBuilder sb) throws SQLException {
        getBringOnlineRequestScheduler().printPriorityThreadQueue(sb);
    }

    public void printBringOnlineSchedulerReadyThreadQueue(StringBuilder sb) throws SQLException {
        getBringOnlineRequestScheduler().printReadyQueue(sb);

    }

    private Scheduler getScheduler(Class<? extends Job> requestType) {
        return SchedulerFactory.getSchedulerFactory().
                getScheduler(requestType);
    }

    public void listReserveSpaceRequests(StringBuilder sb) throws SQLException {
        listRequests(sb, ReserveSpaceRequest.class);
    }

    public void listLsRequests(StringBuilder sb) throws SQLException {
        listRequests(sb, LsRequest.class);
    }

    private <T extends Job> void listRequests(StringBuilder sb,
            Set<Long> jobIds,
            Class<T> type) throws SQLException {
        for (long requestId : jobIds) {
            try {
                T request = Job.getJob(requestId, type);
                sb.append(request).append('\n');
            } catch (SRMInvalidRequestException ire) {
                logger.error(ire.toString());
            }
        }
    }

    private <T extends Request> void listRequests(StringBuilder sb, Class<T> clazz) throws SQLException {
        Set<T> requests = Job.getActiveJobs(clazz);
        for (T request: requests) {
            request.toString(sb,false);
            sb.append('\n');
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

    public void listRequest(StringBuilder sb, long requestId, boolean longformat) throws SQLException,
    SRMInvalidRequestException {
        Job job = Job.getJob(requestId, Job.class);
        if (job == null) {
            sb.append("request with id ").append(requestId)
                    .append(" is not found\n");
        } else {
            job.toString(sb,longformat);
        }
    }

    public void cancelRequest(StringBuilder sb, long requestId) throws SQLException,
    SRMInvalidRequestException {
        Job job = Job.getJob(requestId, Job.class);
        if (job == null || !(job instanceof ContainerRequest)) {
            sb.append("request with id ").append(requestId)
                    .append(" is not found\n");
            return;
        }
        ContainerRequest<?> r = (ContainerRequest<?>) job;
        try {
            r.setState(State.CANCELED, "Canceled by admin through cancel command");
            sb.append("state changed, no guarantee that the process will end immediately\n");
            sb.append(r.toString(false)).append('\n');
        } catch (IllegalStateTransition ist) {
            sb.append("Illegal State Transition : ").append(ist.getMessage());
            logger.error("Illegal State Transition : " +ist.getMessage());
        }
    }

    public void cancelAllGetRequest(StringBuilder sb, String pattern)
            throws SQLException, SRMInvalidRequestException {

        cancelAllRequest(sb, pattern, getGetRequestScheduler(), GetRequest.class);
    }

    public void cancelAllBringOnlineRequest(StringBuilder sb, String pattern)
            throws SQLException, SRMInvalidRequestException {

        cancelAllRequest(sb, pattern, getBringOnlineRequestScheduler(), BringOnlineRequest.class);
    }

    public void cancelAllPutRequest(StringBuilder sb, String pattern)
            throws SQLException, SRMInvalidRequestException {

        cancelAllRequest(sb, pattern, getPutRequestScheduler(), PutRequest.class);
    }

    public void cancelAllCopyRequest(StringBuilder sb, String pattern)
            throws SQLException, SRMInvalidRequestException {

        cancelAllRequest(sb, pattern, getCopyRequestScheduler(), CopyRequest.class);
    }

    public void cancelAllReserveSpaceRequest(StringBuilder sb, String pattern)
            throws SQLException, SRMInvalidRequestException {

        cancelAllRequest(sb, pattern, getReserveSpaceScheduler(), ReserveSpaceRequest.class);
    }

    public void cancelAllLsRequests(StringBuilder sb, String pattern)
            throws SQLException, SRMInvalidRequestException {

        cancelAllRequest(sb, pattern, getLsRequestScheduler(), LsRequest.class);
    }

    private void cancelAllRequest(StringBuilder sb,
            String pattern,
            Scheduler scheduler,
            Class<? extends Job> type) throws SQLException,
            SRMInvalidRequestException {

        Set<Long> jobsToKill = new HashSet<>();
        Pattern p = Pattern.compile(pattern);
        Set<Long> activeRequestIds = getActiveJobIds(type, null);
        for (long requestId : activeRequestIds) {
            Matcher m = p.matcher(String.valueOf(requestId));
            if (m.matches()) {
                logger.debug("cancelAllRequest: request Id #" + requestId + " in " + scheduler + " matches pattern!");
                jobsToKill.add(requestId);
            }
        }
        if (jobsToKill.isEmpty()) {
            sb.append("no requests match the pattern=\"").append(pattern)
                    .append(" in scheduler ").append(scheduler).append("\n");
            return;
        }
        for (long requestId : jobsToKill) {
            try {
                final ContainerRequest<?> job = Job.getJob(requestId, ContainerRequest.class);
                sb.append("request #").append(requestId)
                        .append(" matches pattern=\"").append(pattern)
                        .append("\"; canceling request \n");
                new Thread(new Runnable() {

                    @Override
                    public void run() {
                        try {
                            job.setState(State.CANCELED, "Canceled by admin through cancelall command");
                        } catch (IllegalStateTransition ist) {
                            logger.error("Illegal State Transition : " +ist.getMessage());
                        }
                    }
                }).start();
            } catch(SRMInvalidRequestException e) {
                logger.error("request with request id {} is not found", requestId);
            }
        }
    }

    /**
     * Getter for property configuration.
     * @return Value of property configuration.
     */
    public final Configuration getConfiguration() {
        return configuration;
    }

    /**
     * Getter for property requestCredentialStorage.
     * @return Value of property requestCredentialStorage.
     */
    public RequestCredentialStorage getRequestCredentialStorage() {
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

    public JobStorage<ReserveSpaceRequest> getReserveSpaceRequestStorage() {
        return JobStorageFactory.getJobStorageFactory().getJobStorage(ReserveSpaceRequest.class);
    }

    public JobStorage<LsRequest> getLsRequestStorage() {
        return JobStorageFactory.getJobStorageFactory().getJobStorage(LsRequest.class);
    }

    public JobStorage<LsFileRequest> getLsFileRequestStorage() {
        return JobStorageFactory.getJobStorageFactory().
                getJobStorage(LsFileRequest.class);
    }

    public JobStorage<BringOnlineRequest> getBringOnlineStorage() {
        return JobStorageFactory.getJobStorageFactory().getJobStorage(BringOnlineRequest.class);
    }

    public JobStorage<GetRequest> getGetStorage() {
        return JobStorageFactory.getJobStorageFactory().getJobStorage(GetRequest.class);
    }

    public JobStorage<PutRequest> getPutStorage() {
        return JobStorageFactory.getJobStorageFactory().getJobStorage(PutRequest.class);
    }

    public JobStorage<CopyRequest> getCopyStorage() {
        return JobStorageFactory.getJobStorageFactory().getJobStorage(CopyRequest.class);
    }

    public JobStorage<BringOnlineFileRequest> getBringOnlineFileRequestStorage() {
        return JobStorageFactory.getJobStorageFactory().getJobStorage(BringOnlineFileRequest.class);
    }

    public JobStorage<GetFileRequest> getGetFileRequestStorage() {
        return JobStorageFactory.getJobStorageFactory().getJobStorage(GetFileRequest.class);
    }

    public JobStorage<PutFileRequest> getPutFileRequestStorage() {
        return JobStorageFactory.getJobStorageFactory().getJobStorage(PutFileRequest.class);
    }

    public JobStorage<CopyFileRequest> getCopyFileRequestStorage() {
        return JobStorageFactory.getJobStorageFactory().getJobStorage(CopyFileRequest.class);
    }

    public Scheduler getReserveSpaceScheduler() {
        return getScheduler(ReserveSpaceRequest.class);
    }

    public Scheduler getLsRequestScheduler() {
        return getScheduler(LsFileRequest.class);
    }

    public static <T extends Job> Set<Long> getActiveJobIds(Class<T> type, String description)
            throws SQLException
    {
        Set<T> jobs = Job.getActiveJobs(type);
        Set<Long> ids = new HashSet<>();
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

    public boolean isFileBusy(URI surl) throws SQLException
    {
        return hasActivePutRequests(surl);
    }

    private boolean hasActivePutRequests(URI surl) throws SQLException
    {
        Set<PutFileRequest> requests = Job.getActiveJobs(PutFileRequest.class);
        for (PutFileRequest request: requests) {
            if (request.getSurl().equals(surl)) {
                return true;
            }
        }
        return false;
    }

    public <T extends FileRequest<?>> Iterable<T> getActiveFileRequests(Class<T> type, final URI surl)
            throws SQLException
    {
        return Iterables.filter(Job.getActiveJobs(type),
                new Predicate<T>()
                {
                    @Override
                    public boolean apply(T request)
                    {
                        return request.isTouchingSurl(surl);
                    }
                }
        );
    }
}
