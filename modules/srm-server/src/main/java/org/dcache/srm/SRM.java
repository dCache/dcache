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

package org.dcache.srm;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.io.Files;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;
import org.springframework.dao.DataAccessException;

import javax.annotation.Nonnull;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import dmg.cells.nucleus.CellLifeCycleAware;

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
import org.dcache.srm.request.sql.RequestsPropertyStorage;
import org.dcache.srm.scheduler.IllegalStateTransition;
import org.dcache.srm.scheduler.JobStorage;
import org.dcache.srm.scheduler.JobStorageFactory;
import org.dcache.srm.scheduler.SchedulerContainer;
import org.dcache.srm.scheduler.State;
import org.dcache.srm.util.Configuration;

import static com.google.common.base.Preconditions.*;
import static com.google.common.collect.Iterables.concat;
import static java.util.Arrays.asList;

/**
 * SRM class creates an instance of SRM client class and publishes it on a
 * given port as a storage element.
 *
 * @author  timur
 */
public class SRM implements CellLifeCycleAware
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SRM.class);
    private static final String SFN_STRING = "SFN=";
    private final Configuration configuration;
    private RequestCredentialStorage requestCredentialStorage;
    private final AbstractStorageElement storage;
    private final RequestCounters<Method> abstractStorageElementCounters;
    private RrdRequestCounters<?> rrdAstractStorageElementCounters;
    private final RequestExecutionTimeGauges<Method> abstractStorageElementGauges;
    private RrdRequestExecutionTimeGauges<?> rrdAstractStorageElementGauges;
    private SchedulerContainer schedulers;
    private DatabaseJobStorageFactory databaseFactory;
    private SRMUserPersistenceManager manager;
    private ScheduledExecutorService executor;
    private final List<Future<?>> tasks = new ArrayList<>();
    private long expiryPeriod;
    private String srmId;

    private static SRM srm;

    /**
     * Creates a new instance of SRM
     * @throws IOException
     * @throws InterruptedException
     * @throws DataAccessException
     */
    public SRM(Configuration config, AbstractStorageElement storage) throws IOException, InterruptedException,
            DataAccessException
    {
        this.configuration = config;
        //First of all decorate the storage with counters and
        // gauges to measure the performance of storage operations
        abstractStorageElementCounters=
                new RequestCounters<>(
                        storage.getClass().getName());
        abstractStorageElementGauges =
                new RequestExecutionTimeGauges<>(
                        storage.getClass().getName());
        this.storage = MonitoringProxy.decorateWithMonitoringProxy(
                new Class[]{AbstractStorageElement.class},
                storage,
                abstractStorageElementCounters,
                abstractStorageElementGauges);

        if (configuration.getCounterRrdDirectory() != null) {
            String rrddir =  configuration.getCounterRrdDirectory() +
                    File.separatorChar + "storage";

            rrdAstractStorageElementCounters =
                    new RrdRequestCounters<>(abstractStorageElementCounters, rrddir);
            rrdAstractStorageElementCounters.startRrdUpdates();
            rrdAstractStorageElementCounters.startRrdGraphPlots();


        }
        if (configuration.getGaugeRrdDirectory() != null) {
            File rrddir = new File (configuration.getGaugeRrdDirectory() +
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
            RequestsPropertyStorage.initPropertyStorage(
                    config.getTransactionManager(), config.getDataSource());
        } catch (IllegalStateException ise) {
            //already initialized
        }

        LOGGER.debug("srm started :\n\t{}", configuration.toString());
    }

    @Required
    public void setSrmId(@Nonnull String id)
    {
        srmId = checkNotNull(id);
    }

    @Nonnull
    public String getSrmId()
    {
        return srmId;
    }

    public void setSchedulers(SchedulerContainer schedulers)
    {
        this.schedulers = checkNotNull(schedulers);
    }

    @Required
    public void setRequestCredentialStorage(RequestCredentialStorage store)
    {
        RequestCredential.registerRequestCredentialStorage(store);
        requestCredentialStorage = store;
    }

    @Required
    public void setSrmUserPersistenceManager(SRMUserPersistenceManager manager)
    {
        this.manager = checkNotNull(manager);
    }

    public void setExecutor(java.util.concurrent.ScheduledExecutorService executor)
    {
        this.executor = checkNotNull(executor);
    }

    public static final synchronized void setSRM(SRM srm)
    {
        SRM.srm = srm;
        SRM.class.notifyAll();
    }

    public static final synchronized SRM getSRM()
    {
        while (srm == null) {
            try {
                SRM.class.wait();
            } catch (InterruptedException e) {
                throw new IllegalStateException("SRM has not been instantiated yet.");
            }
        }
        return srm;
    }

    public void start() throws Exception
    {
        checkState(schedulers != null, "Cannot start SRM with no schedulers");
        setSRM(this);
        databaseFactory = new DatabaseJobStorageFactory(srmId, configuration, manager);
        try {
            JobStorageFactory.initJobStorageFactory(databaseFactory);
            databaseFactory.init();
        } catch (RuntimeException e) {
            try {
                databaseFactory.shutdown();
            } catch (Exception suppressed) {
                e.addSuppressed(suppressed);
            }
            throw e;
        }
    }

    @Override
    public void afterStart()
    {
        databaseFactory.restoreJobsOnSrmStart(schedulers);

        /* Schedule expiration of active jobs individually for each job storage to
         * break expiration into smaller tasks.
         */
        for (JobStorage<?> jobStorage : databaseFactory.getJobStorages().values()) {
            tasks.add(executor.scheduleWithFixedDelay(() -> {
                jobStorage.getActiveJobs().stream()
                        .filter(Request.class::isInstance)
                        .map(Request.class::cast)
                        .forEach(Request::checkExpiration);
            }, expiryPeriod, expiryPeriod, TimeUnit.MILLISECONDS));
        }
    }

    @Override
    public void beforeStop()
    {
        tasks.forEach(f -> f.cancel(false));
    }

    public void stop() throws Exception
    {
        databaseFactory.shutdown();
    }

    @Required
    public void setExpiredJobCheckPeriod(long delay)
    {
        checkArgument(delay > 0, "period must be non-negative number: %s", delay);
        checkState(tasks.isEmpty(), "cannot adjust period after SRM is started");

        expiryPeriod = delay;
    }

    public long getExpiredJobCheckPeriod()
    {
        return expiryPeriod;
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

    public String[] getProtocols() throws SRMInternalErrorException
    {
        List<String> getProtocols = asList(storage.supportedGetProtocols());
        List<String> putProtocols = asList(storage.supportedPutProtocols());
        ImmutableList<String> protocols =
                ImmutableSet.copyOf(concat(getProtocols, putProtocols)).asList();
        return protocols.toArray(new String[protocols.size()]);
    }

    /**
     * Accept a newly created job.  This job will be added to the appropriate
     * scheduler's queue to be processed.
     * @param job The new job
     * @throws IllegalStateTransition Job state cannot be modified.
     */
    public void acceptNewJob(Job job) throws IllegalStateTransition
    {
        schedulers.schedule(job);
    }

    public Set<Long> getGetRequestIds(SRMUser user, String description) throws DataAccessException {
        return getActiveJobIds(GetRequest.class,description);
    }

    public Set<Long> getLsRequestIds(SRMUser user, String description) throws DataAccessException {
        return getActiveJobIds(LsRequest.class,description);
    }

    public CharSequence getSchedulerInfo()
    {
        return schedulers.getInfo();
    }

    public CharSequence getGetSchedulerInfo()
    {
        return schedulers.getDetailedInfo(GetFileRequest.class);
    }

    public CharSequence getLsSchedulerInfo()
    {
        return schedulers.getDetailedInfo(LsFileRequest.class);
    }

    public CharSequence getPutSchedulerInfo()
    {
        return schedulers.getDetailedInfo(PutFileRequest.class);
    }

    public CharSequence getCopySchedulerInfo()
    {
        return schedulers.getDetailedInfo(CopyRequest.class);
    }

    public CharSequence getBringOnlineSchedulerInfo()
    {
        return schedulers.getDetailedInfo(BringOnlineFileRequest.class);
    }

    public Set<Long> getPutRequestIds(SRMUser user, String description) throws DataAccessException {
        return getActiveJobIds(PutRequest.class, description);
    }

    public Set<Long> getCopyRequestIds(SRMUser user, String description) throws DataAccessException {
        return getActiveJobIds(CopyRequest.class,description);
    }

    public Set<Long> getBringOnlineRequestIds(SRMUser user, String description) throws DataAccessException {
        return getActiveJobIds(BringOnlineRequest.class,description);
    }

    public double getLoad() {
        return (schedulers.getLoad(CopyRequest.class) +
                schedulers.getLoad(GetFileRequest.class) +
                schedulers.getLoad(PutFileRequest.class)) / 3.0d;
    }

    public void listRequest(StringBuilder sb, long requestId, boolean longformat)
            throws DataAccessException, SRMInvalidRequestException
    {
        Job job = Job.getJob(requestId, Job.class);
        job.toString(sb,longformat);
    }

    public void cancelRequest(StringBuilder sb, long requestId)
            throws SRMInvalidRequestException
    {
        Job job = Job.getJob(requestId, Job.class);
        if (job == null || !(job instanceof ContainerRequest)) {
            sb.append("request with id ").append(requestId)
                    .append(" is not found\n");
            return;
        }
        ContainerRequest<?> r = (ContainerRequest<?>) job;
        try {
            r.setState(State.CANCELED, "Canceled by admin through cancel command.");
            sb.append("state changed, no guarantee that the process will end immediately\n");
            sb.append(r.toString(false)).append('\n');
        } catch (IllegalStateTransition ist) {
            sb.append("Illegal State Transition : ").append(ist.getMessage());
            LOGGER.error("Illegal State Transition : {}", ist.getMessage());
        }
    }

    public void cancelAllGetRequest(StringBuilder sb, String pattern)
            throws DataAccessException, SRMInvalidRequestException {

        cancelAllRequest(sb, pattern, GetRequest.class);
    }

    public void cancelAllBringOnlineRequest(StringBuilder sb, String pattern)
            throws DataAccessException, SRMInvalidRequestException {

        cancelAllRequest(sb, pattern, BringOnlineRequest.class);
    }

    public void cancelAllPutRequest(StringBuilder sb, String pattern)
            throws DataAccessException, SRMInvalidRequestException {

        cancelAllRequest(sb, pattern, PutRequest.class);
    }

    public void cancelAllCopyRequest(StringBuilder sb, String pattern)
            throws DataAccessException, SRMInvalidRequestException {

        cancelAllRequest(sb, pattern, CopyRequest.class);
    }

    public void cancelAllReserveSpaceRequest(StringBuilder sb, String pattern)
            throws DataAccessException, SRMInvalidRequestException {

        cancelAllRequest(sb, pattern, ReserveSpaceRequest.class);
    }

    public void cancelAllLsRequests(StringBuilder sb, String pattern)
            throws DataAccessException, SRMInvalidRequestException {

        cancelAllRequest(sb, pattern, LsRequest.class);
    }

    private void cancelAllRequest(StringBuilder sb,
            String pattern,
            Class<? extends Job> type)
            throws DataAccessException, SRMInvalidRequestException
    {
        Set<Long> jobsToKill = new HashSet<>();
        Pattern p = Pattern.compile(pattern);
        Set<Long> activeRequestIds = getActiveJobIds(type, null);
        for (long requestId : activeRequestIds) {
            Matcher m = p.matcher(String.valueOf(requestId));
            if (m.matches()) {
                LOGGER.debug("cancelAllRequest: request Id #{} of type {} " +
                        "matches pattern", requestId, type.getSimpleName());
                jobsToKill.add(requestId);
            }
        }
        if (jobsToKill.isEmpty()) {
            sb.append("no requests of type ")
                    .append(type.getSimpleName())
                    .append(" matched the pattern \"").append(pattern)
                    .append("\"\n");
            return;
        }
        for (long requestId : jobsToKill) {
            try {
                final ContainerRequest<?> job = Job.getJob(requestId, ContainerRequest.class);
                sb.append("request #").append(requestId)
                        .append(" matches pattern=\"").append(pattern)
                        .append("\"; canceling request \n");
                new Thread(() -> {
                    try {
                        job.setState(State.CANCELED, "Canceled by admin through cancelall command.");
                    } catch (IllegalStateTransition ist) {
                        LOGGER.error("Illegal State Transition : {}", ist.getMessage());
                    }
                }).start();
            } catch(SRMInvalidRequestException e) {
                LOGGER.error("request with request id {} is not found", requestId);
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

    public void setPutMaxReadyJobs(int value)
    {
        schedulers.setMaxReadyJobs(PutFileRequest.class, value);
    }

    public void setGetMaxReadyJobs(int value)
    {
        schedulers.setMaxReadyJobs(GetFileRequest.class, value);
    }

    public void setBringOnlineMaxReadyJobs(int value)
    {
        schedulers.setMaxReadyJobs(BringOnlineFileRequest.class, value);
    }

    public void setLsMaxReadyJobs(int value)
    {
        schedulers.setMaxReadyJobs(LsFileRequest.class, value);
    }

    public JobStorage<ReserveSpaceRequest> getReserveSpaceRequestStorage() {
        return databaseFactory.getJobStorage(ReserveSpaceRequest.class);
    }

    public JobStorage<LsRequest> getLsRequestStorage() {
        return databaseFactory.getJobStorage(LsRequest.class);
    }

    public JobStorage<LsFileRequest> getLsFileRequestStorage() {
        return databaseFactory.getJobStorage(LsFileRequest.class);
    }

    public JobStorage<BringOnlineRequest> getBringOnlineStorage() {
        return databaseFactory.getJobStorage(BringOnlineRequest.class);
    }

    public JobStorage<GetRequest> getGetStorage() {
        return databaseFactory.getJobStorage(GetRequest.class);
    }

    public JobStorage<PutRequest> getPutStorage() {
        return databaseFactory.getJobStorage(PutRequest.class);
    }

    public JobStorage<CopyRequest> getCopyStorage() {
        return databaseFactory.getJobStorage(CopyRequest.class);
    }

    public JobStorage<BringOnlineFileRequest> getBringOnlineFileRequestStorage() {
        return databaseFactory.getJobStorage(BringOnlineFileRequest.class);
    }

    public JobStorage<GetFileRequest> getGetFileRequestStorage() {
        return databaseFactory.getJobStorage(GetFileRequest.class);
    }

    public JobStorage<PutFileRequest> getPutFileRequestStorage() {
        return databaseFactory.getJobStorage(PutFileRequest.class);
    }

    public JobStorage<CopyFileRequest> getCopyFileRequestStorage() {
        return databaseFactory.getJobStorage(CopyFileRequest.class);
    }

    public <T extends Job> Set<T> getActiveJobs(Class<T> type) throws DataAccessException
    {
        JobStorage<T> jobStorage = databaseFactory.getJobStorage(type);
        return (jobStorage == null) ? Collections.<T>emptySet() : jobStorage.getActiveJobs();
    }

    public <T extends Job> Set<Long> getActiveJobIds(Class<T> type, String description)
            throws DataAccessException
    {
        Set<T> jobs = getActiveJobs(type);
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

    public <T extends FileRequest<?>> Iterable<T> getActiveFileRequests(Class<T> type, final URI surl)
            throws DataAccessException
    {
        return Iterables.filter(getActiveJobs(type), request -> request.isTouchingSurl(surl));
    }

    /**
     * Returns PutFileRequests on the given SURL or within the directory tree of that SURL.
     */
    public Stream<PutFileRequest> getActivePutFileRequests(URI surl)
            throws DataAccessException
    {
        String path = getPath(surl);
        return StreamSupport.stream(getActiveJobs(PutFileRequest.class).spliterator(), false)
                .filter(r -> getPath(r.getSurl()).startsWith(path));
    }

    /**
     * Returns true if an upload on the given SURL exists.
     */
    public boolean isFileBusy(URI surl) throws SRMException
    {
        return getActivePutFileRequests(surl).anyMatch(m -> m.getSurl().equals(surl));
    }

    /**
     * Returns true if multiple uploads on the given SURL exist.
     */
    public boolean hasMultipleUploads(URI surl) throws SRMException
    {
        return getActivePutFileRequests(surl).filter(m -> m.getSurl().equals(surl)).limit(2).count() > 1;
    }

    /**
     * Returns the file id of an active upload on the given SURL.
     */
    public String getUploadFileId(URI surl) throws SRMException
    {
        return getActivePutFileRequests(surl)
                .filter(m -> m.getSurl().equals(surl))
                .map(PutFileRequest::getFileId)
                .filter(Objects::nonNull)
                .findFirst()
                .orElse(null);
    }

    /**
     * Aborts uploads and downloads on the given SURL. Returns true if and only if an
     * upload was aborted.
     */
    public boolean abortTransfers(URI surl, String reason) throws SRMException
    {
        boolean didAbortUpload = false;

        for (PutFileRequest request : getActiveFileRequests(PutFileRequest.class, surl)) {
            try {
                request.abort(reason);
                didAbortUpload = true;
            } catch (IllegalStateTransition e) {
                // The request likely aborted or finished before we could abort it
                LOGGER.debug("Attempted to abort put request {}, but failed: {}",
                             request.getId(), e.getMessage());
            }
        }

        for (GetFileRequest request : getActiveFileRequests(GetFileRequest.class, surl)) {
            try {
                request.abort(reason);
            } catch (IllegalStateTransition e) {
                // The request likely aborted or finished before we could abort it
                LOGGER.debug("Attempted to abort get request {}, but failed: {}",
                             request.getId(), e.getMessage());
            }
        }

        return didAbortUpload;
    }

    /**
     * Checks if an active upload blocks the removal of a directory.
     */
    public void checkRemoveDirectory(URI surl) throws SRMException
    {
        Optional<URI> upload =
                getActivePutFileRequests(surl).map(PutFileRequest::getSurl).min(URI::compareTo);
        if (upload.isPresent()) {
            if (upload.get().equals(surl)) {
                throw new SRMInvalidPathException("Not a directory");
            } else {
                throw new SRMNonEmptyDirectoryException("Directory is not empty");
            }
        }
    }

    private static String getPath(URI surl)
    {
        String path = surl.getPath();
        String query = surl.getQuery();
        if (query != null) {
            int i = query.indexOf(SFN_STRING);
            if (i != -1) {
                path = query.substring(i + SFN_STRING.length());
            }
        }
        /* REVISIT
         *
         * This is not correct in the presence of symlinked directories. The
         * simplified path may refer to a different directory than the one
         * we will delete.
         *
         * For now we ignore this problem - fixing it requires resolving the
         * paths to an absolute path, which requires additional name space
         * lookups.
         */
        path = Files.simplifyPath(path);
        if (!path.endsWith("/")) {
            path = path + "/";
        }
        return path;
    }
}
