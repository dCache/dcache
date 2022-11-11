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
package org.dcache.services.bulk.store.jdbc.request;

import static org.dcache.services.bulk.BulkRequestInfo.NO_FURTHER_ENTRIES;
import static org.dcache.services.bulk.BulkRequestStatus.CANCELLED;
import static org.dcache.services.bulk.BulkRequestStatus.CANCELLING;
import static org.dcache.services.bulk.BulkRequestStatus.QUEUED;
import static org.dcache.services.bulk.BulkRequestStatus.STARTED;
import static org.dcache.services.bulk.util.BulkRequestTarget.NON_TERMINAL;
import static org.dcache.services.bulk.util.BulkRequestTarget.PLACEHOLDER_PNFSID;
import static org.dcache.services.bulk.util.BulkRequestTarget.ROOT_REQUEST_PARENT;
import static org.dcache.services.bulk.util.BulkRequestTarget.ROOT_REQUEST_PATH;

import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.security.auth.Subject;
import org.dcache.auth.Subjects;
import org.dcache.auth.attributes.Restriction;
import org.dcache.namespace.FileType;
import org.dcache.services.bulk.BulkPermissionDeniedException;
import org.dcache.services.bulk.BulkRequest;
import org.dcache.services.bulk.BulkRequestInfo;
import org.dcache.services.bulk.BulkRequestNotFoundException;
import org.dcache.services.bulk.BulkRequestStatus;
import org.dcache.services.bulk.BulkRequestStatusInfo;
import org.dcache.services.bulk.BulkRequestSummary;
import org.dcache.services.bulk.BulkRequestTargetInfo;
import org.dcache.services.bulk.BulkStorageException;
import org.dcache.services.bulk.store.BulkRequestStore;
import org.dcache.services.bulk.store.jdbc.JdbcBulkDaoUtils;
import org.dcache.services.bulk.store.jdbc.rtarget.JdbcBulkTargetStore;
import org.dcache.services.bulk.store.jdbc.rtarget.JdbcRequestTargetDao;
import org.dcache.services.bulk.util.BulkRequestFilter;
import org.dcache.services.bulk.util.BulkRequestTarget;
import org.dcache.services.bulk.util.BulkRequestTarget.State;
import org.dcache.services.bulk.util.BulkRequestTargetBuilder;
import org.dcache.vehicles.FileAttributes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;

/**
 * Uses underlying JDBC Dao implementations to satisfy the API.  Maintains a cache of request
 * optionals.
 */
public final class JdbcBulkRequestStore implements BulkRequestStore {

    protected static final Logger LOGGER
          = LoggerFactory.getLogger(JdbcBulkRequestStore.class);

    private static final Integer REQUEST_INFO_TARGET_LIMIT = 10000;

    class RequestLoader extends CacheLoader<String, Optional<BulkRequest>> {

        @Override
        public Optional<BulkRequest> load(String id) throws Exception {
            List<BulkRequest> list = requestDao.get(requestDao.where().requestIds(id), 1);
            if (list.isEmpty()) {
                return Optional.empty();
            }
            return Optional.ofNullable(list.get(0));
        }
    }

    private final ListMultimap<String, String> activeRequestsByUser = ArrayListMultimap.create();
    private final Map<String, String> userOfActiveRequest = new HashMap<>();

    private LoadingCache<String, Optional<BulkRequest>> requestCache;
    private JdbcBulkDaoUtils utils;
    private JdbcBulkRequestDao requestDao;
    private JdbcBulkRequestPermissionsDao requestPermissionsDao;
    private JdbcBulkTargetStore targetStore;
    private JdbcRequestTargetDao requestTargetDao;
    private long expiry;
    private TimeUnit expiryUnit;
    private long capacity;

    /**
     * For handling delayed clear requests.
     */
    private ScheduledExecutorService scheduler;

    /**
     * For updating counts to the counts table.
     */
    private ScheduledExecutorService countUpdater;
    private long updateInterval;
    private TimeUnit updateIntervalUnit;

    public void initialize() {
        requestCache = CacheBuilder.newBuilder()
              .expireAfterAccess(expiry, expiryUnit)
              .maximumSize(capacity)
              .build(new RequestLoader());
        countUpdater.schedule(this::updateCounts, updateInterval, updateIntervalUnit);
    }

    @Override
    public void abort(BulkRequest request, Throwable exception) {
        String requestId = request.getId();
        LOGGER.trace("abort {}, {}.", requestId, exception.toString());

        if (requestDao.count(requestDao.where().requestIds(requestId)) == 0) {
            LOGGER.error("Fatal error trying to abort {}: "
                  + "request not found; error which "
                  + "caused the abort: {}.", requestId, exception);
            return;
        }

        /*
         *  Do this to record the failure for viewing.
         */
        FileAttributes attributes = new FileAttributes();
        attributes.setFileType(FileType.SPECIAL);
        attributes.setPnfsId(PLACEHOLDER_PNFSID);

        BulkRequestTarget target = BulkRequestTargetBuilder.builder().rid(requestId)
              .pid(ROOT_REQUEST_PARENT).activity(request.getActivity())
              .path(ROOT_REQUEST_PATH).attributes(attributes).error(exception).build();

        try {
            targetStore.abort(target);
        } catch (BulkStorageException e) {
            LOGGER.error("failure to register abort message as target for {}: {}.", requestId,
                  e.getMessage());
        }

        requestDao.update(requestDao.where().requestIds(requestId),
              requestDao.set().status(BulkRequestStatus.COMPLETED));
        requestCache.invalidate(requestId);

        /*
         *  Abort is called only on failed activation,
         *  so there should be no actual targets to cancel.
         */
        if (request.isClearOnFailure()) {
            clear(requestId);
        }
    }

    @Override
    public void clear(Subject subject, String id)
          throws BulkStorageException, BulkPermissionDeniedException {
        String key = checkRequestPermissions(subject, id);
        LOGGER.trace("clear {}, {}.", key, id);

        Optional<BulkRequest> stored;
        try {
            stored = requestCache.get(id);
        } catch (ExecutionException e) {
            throw new BulkStorageException(e.getMessage(), e.getCause());
        }

        if (stored.isEmpty()) {
            throw new BulkRequestNotFoundException(id);
        }

        clear(stored.get());
    }

    @Override
    public void clear(String id) {
        LOGGER.trace("clear {}.", id);

        Optional<BulkRequest> stored = Optional.empty();

        try {
            stored = requestCache.get(id);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            LOGGER.error("Fatal error trying to clear {}: "
                  + "{}.", id, cause == null ? e.getMessage() : cause.getMessage());
        }

        if (stored.isEmpty()) {
            return;
        }

        clear(stored.get());
    }

    @Override
    public void clearWhenTerminated(Subject subject, String id)
          throws BulkStorageException, BulkPermissionDeniedException {
        String key = checkRequestPermissions(subject, id);
        LOGGER.trace("clearWhenTerminated {}, {}.", key, id);
        requestDao.update(requestDao.where().requestIds(id),
              requestDao.set().clearOnFailure(true).clearOnSuccess(true).delayClear(0));
        requestCache.invalidate(id);
    }

    @Override
    public long count(BulkRequestFilter rFilter) {
        int count = requestDao.count(requestDao.where().filter(rFilter));
        LOGGER.trace("count requests returning {}.", count);
        return count;
    }

    @Override
    public int countActive() throws BulkStorageException {
        int count = requestDao.count(requestDao.where().status(STARTED));
        LOGGER.trace("count active requests returning {}.", count);
        return count;
    }

    @Override
    public int countNonTerminated(String user) throws BulkStorageException {
        int count = requestDao.count(
              requestDao.where().status(STARTED, BulkRequestStatus.QUEUED).user(user));
        LOGGER.trace("count non terminated for {}: {}.", user, count);
        return count;
    }

    @Override
    public Collection<BulkRequest> find(Optional<BulkRequestFilter> requestFilter, Integer limit)
          throws BulkStorageException {
        limit = limit == null ? Integer.MAX_VALUE : limit;
        BulkRequestFilter rfilter = requestFilter.orElse(null);
        return requestDao.get(
                    requestDao.where().filter(rfilter).sorter("seq_no"), limit).stream()
              .collect(Collectors.toList());
    }

    @Override
    public synchronized ListMultimap<String, String> getActiveRequestsByUser()
          throws BulkStorageException {
        ListMultimap<String, String> result = ArrayListMultimap.create();
        result.putAll(activeRequestsByUser);
        return result;
    }

    @Override
    public Optional<BulkRequest> getRequest(String id) throws BulkStorageException {
        Optional<BulkRequest> stored = Optional.empty();
        try {
            stored = requestCache.get(id);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            LOGGER.error("Fatal error trying to get request {}: "
                  + "{}.", id, cause == null ? e.getMessage() : cause.getMessage());
        }

        if (stored.isEmpty()) {
            return Optional.empty();
        }

        BulkRequest request = stored.get();

        if (request == null) {
            throw new BulkStorageException(
                  "BulkRequest object missing for " + id + "!");
        }

        return Optional.of(request);
    }

    @Override
    public Set<BulkRequestSummary> getRequestSummaries(Set<BulkRequestStatus> status,
          Set<String> owners, String path) {
        LOGGER.trace("getRequestSummaries {}, {}, {}.", status, owners, path);

        /*
         *  Filter out the request ids by target paths first.
         */
        String[] rids = path == null ? null : targetStore.ridsOf(path).toArray(String[]::new);

        String[] users = owners == null ? null : owners.toArray(String[]::new);

        List<BulkRequest> requests =
              requestDao.get(requestDao.where().requestIds(rids).status(status).user(users),
                    Integer.MAX_VALUE);
        Set<BulkRequestSummary> summaries = new HashSet<>();

        for (BulkRequest r : requests) {
            try {
                summaries.add(new BulkRequestSummary(r.getUrlPrefix() + "/" + r.getId(),
                      r.getActivity(),
                      r.getStatusInfo(),
                      targetStore.countUnprocessed(r.getId())));
            } catch (BulkStorageException e) {
                LOGGER.error("Unable to retrieve unprocessed count for {}: {}.", r.getId(),
                      e.getMessage());
            }
        }

        return summaries;
    }

    @Override
    public Optional<Restriction> getRestriction(String id)
          throws BulkStorageException {
        Optional<JdbcBulkRequestPermissions> permissions = requestPermissionsDao.get(
              requestPermissionsDao.where().requestIds(id));

        if (permissions.isEmpty()) {
            return Optional.empty();
        }

        return Optional.of(permissions.get().getRestriction());
    }

    @Override
    public BulkRequestInfo getRequestInfo(Subject subject, String id, long offset)
          throws BulkStorageException, BulkPermissionDeniedException {
        LOGGER.trace("getRequestInfo {}, {}.", BulkRequestStore.uidGidKey(subject), id);

        BulkRequest stored = valid(id);

        if (!isRequestSubject(subject, id)) {
            throw new BulkPermissionDeniedException(id);
        }

        return processInfo(stored, offset);
    }

    @Override
    public Optional<BulkRequestStatus> getRequestStatus(String id)
          throws BulkStorageException {
        LOGGER.trace("getRequestInfo {}.", id);

        BulkRequest stored = get(id);

        if (stored == null) {
            return Optional.empty();
        }

        BulkRequestStatusInfo info = stored.getStatusInfo();
        if (info == null) {
            return Optional.empty();
        }

        return Optional.of(info.getStatus());
    }

    @Override
    public Optional<Subject> getSubject(String id) throws BulkStorageException {
        Optional<JdbcBulkRequestPermissions> permissions = requestPermissionsDao.get(
              requestPermissionsDao.where().requestIds(id));

        if (permissions.isEmpty()) {
            return Optional.empty();
        }

        return Optional.of(permissions.get().getSubject());
    }

    @Override
    public boolean isRequestSubject(Subject subject, String id)
          throws BulkStorageException {
        Optional<Subject> requestSubject = getSubject(id);
        if (requestSubject.isEmpty()) {
            return false;
        }
        return Subjects.isRoot(subject) ||
              BulkRequestStore.uidGidKey(subject)
                    .equals(BulkRequestStore.uidGidKey(requestSubject.get()));
    }

    /**
     * With the RDBMS implementation of the store, the following applies on restart:
     * <p>
     * (a) finished targets of a given request are left in the database; these should be skipped
     * when the request is reprocessed (on submit). (b) unfinished targets are deleted. (c) the
     * unfinished request is reset to QUEUED for reprocessing.
     *
     * @throws BulkStorageException
     */
    @Override
    public void load() throws BulkStorageException {
        LOGGER.trace("load called.");
        requestTargetDao.delete(requestTargetDao.where().state(NON_TERMINAL));
        /*
         *  deletion of the bulk request job (parent = -1) is necessary to avoid processing
         *  a placeholder target as if it represented a real namespace entry.
         */
        requestTargetDao.delete(requestTargetDao.where().pid(ROOT_REQUEST_PARENT));
        requestDao.update(requestDao.where().status(STARTED, CANCELLING),
              requestDao.set().status(QUEUED));
    }

    @Override
    public List<BulkRequest> next(Optional<String> sortedBy, Optional<Boolean> reverse, long limit)
          throws BulkStorageException {
        LOGGER.trace("next {}.", limit);
        return requestDao.get(
                    requestDao.where().status(QUEUED).sorter(sortedBy.orElse("arrived_at"))
                          .reverse(reverse.orElse(false)), (int) limit).stream()
              .collect(Collectors.toList());
    }

    @Override
    public void reset(String id) throws BulkStorageException {
        /**
         *  Eliminate <i>all</i> targets for request; start from scratch.
         */
        LOGGER.trace("reset {}.", id);
        targetStore.delete(id);
        requestDao.update(requestDao.where().requestIds(id),
              requestDao.set().status(QUEUED));
        try {
            requestCache.get(id).ifPresent(r -> {
                BulkRequestStatusInfo status = r.getStatusInfo();
                status.setStatus(QUEUED);
                status.setStartedAt(null);
                status.setLastModified(System.currentTimeMillis());
                status.setCompletedAt(null);
            });
        } catch (ExecutionException e) {
            throw new BulkStorageException(e.getMessage(), e.getCause());
        }
    }

    @Required
    public void setCapacity(long capacity) {
        this.capacity = capacity;
    }

    @Required
    public void setExpiry(long expiry) {
        this.expiry = expiry;
    }

    @Required
    public void setExpiryUnit(TimeUnit expiryUnit) {
        this.expiryUnit = expiryUnit;
    }

    @Required
    public void setTargetStore(JdbcBulkTargetStore targetStore) {
        this.targetStore = targetStore;
    }

    @Required
    public void setRequestDao(JdbcBulkRequestDao requestDao) {
        this.requestDao = requestDao;
    }

    @Required
    public void setRequestTargetDao(JdbcRequestTargetDao requestTargetDao) {
        this.requestTargetDao = requestTargetDao;
    }

    @Required
    public void setScheduler(ScheduledExecutorService scheduler) {
        this.scheduler = scheduler;
    }

    @Required
    public void setCountUpdater(ScheduledExecutorService countUpdater) {
        this.countUpdater = countUpdater;
    }

    @Required
    public void setUpdateInterval(long updateInterval) {
        this.updateInterval = updateInterval;
    }

    @Required
    public void setUpdateIntervalUnit(TimeUnit updateIntervalUnit) {
        this.updateIntervalUnit = updateIntervalUnit;
    }

    @Required
    public void setRequestPermissionsDao(JdbcBulkRequestPermissionsDao requestPermissionsDao) {
        this.requestPermissionsDao = requestPermissionsDao;
    }

    @Required
    public void setUtils(JdbcBulkDaoUtils utils) {
        this.utils = utils;
    }

    @Override
    public void store(Subject subject, Restriction restriction, BulkRequest request)
          throws BulkStorageException {
        try {
            /*
             *  Insertion order: request, permissions, must be maintained.
             */
            requestDao.insert(
                        requestDao.updateFrom(request, BulkRequestStore.uidGidKey(subject)))
                  .ifPresent(keyHolder ->
                        request.setSeqNo((long) keyHolder.getKeys().get("seq_no")));

            requestPermissionsDao.insert(
                  requestPermissionsDao.set().id(request.getId()).subject(subject)
                        .restriction(restriction));
        } catch (BulkStorageException e) {
            throw new BulkStorageException("store failed for " + request.getId(), e);
        }
    }

    @Override
    public boolean update(String requestId, BulkRequestStatus status)
          throws BulkStorageException {
        LOGGER.trace("update {}, {}.", requestId, status);
        BulkRequest stored = valid(requestId);
        BulkRequestStatus storedStatus = stored.getStatusInfo().getStatus();

        if (storedStatus == status) {
            return false;
        }

        boolean update = false;

        if (storedStatus == null) {
            update = requestDao.update(requestDao.where().requestIds(requestId),
                  requestDao.set().status(status)) == 1;
        } else {
            switch (storedStatus) {
                case COMPLETED:
                case CANCELLED:
                    break;
                case CANCELLING:
                    if (status == CANCELLED) {
                        update = requestDao.updateTo(status, requestId) == 1;
                    }
                    break;
                case STARTED:
                    switch (status) {
                        case COMPLETED:
                        case CANCELLED:
                            update = requestDao.updateTo(status, requestId) == 1;
                            break;
                        case CANCELLING:
                            update = requestDao.update(requestDao.where().requestIds(requestId),
                                  requestDao.set().status(status)) == 1;
                            break;
                    }
                    break;
                case QUEUED:
                    switch (status) {
                        case COMPLETED:
                        case CANCELLED:
                            update = requestDao.updateTo(status, requestId) == 1;
                            break;
                        default:
                            update = requestDao.update(requestDao.where().requestIds(requestId),
                                  requestDao.set().status(status)) == 1;
                    }
                    break;
                default:
                    break;
            }
        }

        if (!update) {
            return false;
        }

        /*
         * For the sake of cache coherence.  The queued object may have been evicted
         * and the object re-fetched from cache.
         */
        try {
            requestCache.get(requestId).ifPresent(r -> {
                BulkRequestStatusInfo cached = r.getStatusInfo();
                cached.setStatus(status);
                Long now = System.currentTimeMillis();
                cached.setLastModified(now);
                switch (status) {
                    case STARTED:
                        cached.setStartedAt(now);
                        cached.setCompletedAt(null);
                        break;
                    case QUEUED:
                        cached.setStartedAt(null);
                        cached.setCompletedAt(null);
                        break;
                    case CANCELLED:
                    case COMPLETED:
                        cached.setCompletedAt(now);
                        break;
                    default:
                }
            });
        } catch (ExecutionException e) {
            throw new BulkStorageException(e.getMessage(), e.getCause());
        }

        /*
         *  Maintain the in-memory active request id indices.  Check for clear on success or failure.
         */
        switch (status) {
            case CANCELLED:
            case COMPLETED:
                synchronized (this) {
                    String user = userOfActiveRequest.remove(requestId);
                    activeRequestsByUser.remove(user, requestId);
                }
                conditionallyClearTerminalRequest(stored);
                break;
            case STARTED:
                synchronized (this) {
                    String user = stored.getStatusInfo().getUser();
                    activeRequestsByUser.put(user, requestId);
                    userOfActiveRequest.put(requestId, user);
                }
                break;
        }

        return true;
    }

    private String checkRequestPermissions(Subject subject, String id)
          throws BulkPermissionDeniedException, BulkStorageException {
        String key = BulkRequestStore.uidGidKey(subject);

        Optional<JdbcBulkRequestPermissions> permissions = requestPermissionsDao.get(
              requestPermissionsDao.where().requestIds(id));

        if (permissions.isEmpty()) {
            throw new BulkPermissionDeniedException(id);
        }

        Subject requestSubject = permissions.get().getSubject();

        if (requestSubject == null) {
            LOGGER.error("Fatal error trying to clear {}: "
                  + "request has no subject.", id);
        }

        if (!Subjects.isRoot(subject) && !key.equals(BulkRequestStore.uidGidKey(requestSubject))) {
            throw new BulkPermissionDeniedException(id);
        }

        return key;
    }

    private void conditionallyClearTerminalRequest(BulkRequest stored) {
        String id = stored.getId();
        if (requestTargetDao.count(requestTargetDao.where().rid(id).state(State.FAILED)) > 0) {
            if (stored.isClearOnFailure()) {
                clear(id);
            }
        } else if (stored.isClearOnSuccess()) {
            clear(id);
        }
    }

    private void clear(BulkRequest request) {
        Integer delay = request.getDelayClear();
        String requestId = request.getId();
        if (delay == null || delay == 0) {
            requestDao.delete(requestDao.where().requestIds(requestId));
            requestCache.invalidate(requestId);
        } else {
            scheduler.schedule(() -> {
                requestDao.delete(requestDao.where().requestIds(requestId));
                requestCache.invalidate(requestId);
            }, delay, TimeUnit.SECONDS);
        }
    }

    private BulkRequest get(String id) throws BulkStorageException {
        try {
            return requestCache.get(id).orElse(null);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            throw new BulkStorageException(
                  cause == null ? e.getMessage() : cause.getMessage());
        }
    }

    private BulkRequestInfo processInfo(BulkRequest stored, long offset) {
        BulkRequestInfo info = new BulkRequestInfo();
        BulkRequestStatusInfo status = stored.getStatusInfo();
        String requestId = stored.getId();
        info.setId(requestId);
        info.setStatus(status.getStatus());
        info.setArrivedAt(status.getCreatedAt());
        info.setLastModified(status.getLastModified());
        info.setStartedAt(status.getStartedAt());
        info.setTargetPrefix(stored.getTargetPrefix());
        /*
         *  Order by id from offset.  Limit is 10000 per swatch.
         */
        List<BulkRequestTargetInfo> targets =
              requestTargetDao.get(requestTargetDao.where().rid(requestId).offset(offset)
                          .sorter("id"), REQUEST_INFO_TARGET_LIMIT)
                    .stream().filter(t -> !t.getPath().equals(ROOT_REQUEST_PATH))
                    .map(this::toRequestTargetInfo)
                    .collect(Collectors.toList());
        info.setTargets(targets);
        int size = targets.size();
        if (size == REQUEST_INFO_TARGET_LIMIT) {
            info.setNextSeqNo(targets.get(size - 1).getSeqNo() + 1);
        } else {
            info.setNextSeqNo(NO_FURTHER_ENTRIES);
        }
        return info;
    }

    private BulkRequestTargetInfo toRequestTargetInfo(BulkRequestTarget target) {
        BulkRequestTargetInfo info = new BulkRequestTargetInfo();
        info.setSeqNo(target.getId());
        info.setTarget(target.getPath().toString());
        info.setState(target.getState().name());
        info.setSubmittedAt(target.getCreatedAt());
        info.setStartedAt(target.getStartedAt());
        if (target.isTerminated()) {
            info.setFinishedAt(target.getLastUpdated());
        }
        Throwable errorObject = target.getThrowable();
        if (errorObject != null) {
            Throwable root = Throwables.getRootCause(errorObject);
            info.setErrorType(root.getClass().getCanonicalName());
            info.setErrorMessage(root.getMessage());
        }
        return info;
    }

    private void updateCounts() {
        try {
            utils.updateCounts(countActive(), targetStore.countsByState(), requestDao);
        } catch (BulkStorageException e) {
            LOGGER.error("Problem updating counts by state: {}.", e.toString());
        }

        countUpdater.schedule(this::updateCounts, updateInterval, updateIntervalUnit);
    }

    private BulkRequest valid(String id) throws BulkStorageException {
        BulkRequest stored = get(id);
        if (stored == null) {
            String error = "request id " + id + " is no longer valid!";
            throw new BulkRequestNotFoundException(error);
        }
        return stored;
    }
}
