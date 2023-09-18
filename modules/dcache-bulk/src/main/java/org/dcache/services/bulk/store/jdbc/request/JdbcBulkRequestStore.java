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
import static org.dcache.services.bulk.activity.BulkActivity.MINIMALLY_REQUIRED_ATTRIBUTES;
import static org.dcache.services.bulk.util.BulkRequestTarget.NON_TERMINAL;
import static org.dcache.services.bulk.util.BulkRequestTarget.PID.DISCOVERED;
import static org.dcache.services.bulk.util.BulkRequestTarget.PID.INITIAL;
import static org.dcache.services.bulk.util.BulkRequestTarget.PLACEHOLDER_PNFSID;
import static org.dcache.services.bulk.util.BulkRequestTarget.ROOT_REQUEST_PATH;
import static org.dcache.services.bulk.util.BulkRequestTarget.State.CREATED;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.FsPath;
import diskCacheV111.util.PnfsHandler;
import diskCacheV111.util.PnfsId;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import javax.security.auth.Subject;
import org.dcache.auth.Subjects;
import org.dcache.auth.attributes.Restriction;
import org.dcache.auth.attributes.Restrictions;
import org.dcache.cells.CellStub;
import org.dcache.namespace.FileType;
import org.dcache.services.bulk.BulkArchivedRequestInfo;
import org.dcache.services.bulk.BulkArchivedSummaryFilter;
import org.dcache.services.bulk.BulkArchivedSummaryInfo;
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
import org.dcache.services.bulk.util.BulkRequestTarget.PID;
import org.dcache.services.bulk.util.BulkRequestTarget.State;
import org.dcache.services.bulk.util.BulkRequestTargetBuilder;
import org.dcache.services.bulk.util.BulkServiceStatistics;
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

    private static final Integer FETCH_SIZE = 10000;

    class RequestLoader implements CacheLoader<String, Optional<BulkRequest>> {

        @Override
        public Optional<BulkRequest> load(String uid) throws Exception {
            List<BulkRequest> list = requestDao.get(requestDao.where().uids(uid), 1, true);
            if (list.isEmpty()) {
                return Optional.empty();
            }
            return Optional.ofNullable(list.get(0));
        }
    }

    private final ListMultimap<String, String> activeRequestsByUser = ArrayListMultimap.create();
    private final Map<String, String> userOfActiveRequest = new HashMap<>();

    private LoadingCache<String, Optional<BulkRequest>> requestCache;
    private JdbcBulkRequestDao requestDao;
    private JdbcBulkArchiveDao archiveDao;
    private JdbcBulkRequestPermissionsDao requestPermissionsDao;
    private JdbcBulkTargetStore targetStore;
    private JdbcRequestTargetDao requestTargetDao;
    private JdbcBulkDaoUtils utils;
    private long expiry;
    private TimeUnit expiryUnit;
    private long capacity;

    private BulkServiceStatistics statistics;

    private PnfsHandler pnfsHandler;

    public void initialize() {
        requestCache = Caffeine.newBuilder()
              .expireAfterAccess(expiry, expiryUnit)
              .maximumSize(capacity)
              .build(new RequestLoader());
    }

    @Override
    public void abort(BulkRequest request, Throwable exception) {
        String uid = request.getUid();
        LOGGER.trace("abort {}, {}.", uid, exception.toString());

        if (requestDao.count(requestDao.where().uids(uid)) == 0) {
            LOGGER.error("Fatal error trying to abort {}: "
                  + "request not found; error which "
                  + "caused the abort: {}.", uid, exception);
            return;
        }

        /*
         *  Do this to record the failure for viewing.
         */
        FileAttributes attributes = new FileAttributes();
        attributes.setFileType(FileType.SPECIAL);
        attributes.setPnfsId(PLACEHOLDER_PNFSID);

        Throwable root = Throwables.getRootCause(exception);

        BulkRequestTarget target = BulkRequestTargetBuilder.builder(statistics).rid(request.getId())
              .pid(PID.ROOT).activity(request.getActivity())
              .path(ROOT_REQUEST_PATH).attributes(attributes)
              .errorType(root.getClass().getCanonicalName())
              .errorMessage(root.getMessage()).build();

        try {
            targetStore.abort(target);
        } catch (BulkStorageException e) {
            LOGGER.error("failure to register abort message as target for {}: {}.", uid,
                  e.getMessage());
        }

        requestDao.update(requestDao.where().uids(uid),
              requestDao.set().status(BulkRequestStatus.COMPLETED));
        requestCache.invalidate(uid);

        /*
         *  Abort is called only on failed activation,
         *  so there should be no actual targets to cancel.
         */
        if (request.isClearOnFailure()) {
            clear(uid);
        }
    }

    @Override
    public void clear(Subject subject, String uid)
          throws BulkStorageException, BulkPermissionDeniedException {
        String key = checkRequestPermissions(subject, uid);
        LOGGER.trace("clear {}, {}.", key, uid);

        Optional<BulkRequest> stored;
        try {
            stored = requestCache.get(uid);
        } catch (CompletionException e) {
            throw new BulkStorageException(e.getMessage(), e.getCause());
        }

        if (stored.isEmpty()) {
            throw new BulkRequestNotFoundException(uid);
        }

        clear(stored.get());
    }

    @Override
    public void clear(String uid) {
        LOGGER.trace("clear {}.", uid);

        Optional<BulkRequest> stored = Optional.empty();

        try {
            stored = requestCache.get(uid);
        } catch (CompletionException e) {
            Throwable cause = e.getCause();
            LOGGER.error("Fatal error trying to clear {}: "
                  + "{}.", uid, cause == null ? e.getMessage() : cause.getMessage());
        }

        if (stored.isEmpty()) {
            return;
        }

        clear(stored.get());
    }

    @Override
    public void clearWhenTerminated(Subject subject, String uid)
          throws BulkStorageException, BulkPermissionDeniedException {
        String key = checkRequestPermissions(subject, uid);
        LOGGER.trace("clearWhenTerminated {}, {}.", key, uid);
        requestDao.update(requestDao.where().uids(uid),
              requestDao.set().clearOnFailure(true).clearOnSuccess(true).delayClear(0));
        requestCache.invalidate(uid);
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
        statistics.setActive(count);
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
                    requestDao.where().filter(rfilter).sorter("id"), limit, false).stream()
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
    public BulkArchivedRequestInfo getArchivedInfo(Subject subject, String rid)
          throws BulkPermissionDeniedException {
        List<BulkArchivedRequestInfo> list = archiveDao.get(archiveDao.where().uids(rid), 1);
        if (list.isEmpty()) {
            return null;
        }

        BulkArchivedRequestInfo info = list.get(0);

        if (!Subjects.isRoot(subject) && !Subjects.hasAdminRole(subject) &&
              !BulkRequestStore.uidGidKey(subject).equals(info.getOwner())) {
            throw new BulkPermissionDeniedException("Subject does not have "
                  + "permission to read archived request " + rid);
        }

        return info;
    }

    @Override
    public List<BulkArchivedSummaryInfo> getArchivedSummaryInfo(Subject subject,
          BulkArchivedSummaryFilter filter) throws BulkStorageException {
        try {
            return archiveDao.list(archiveDao.where().fromFilter(filter), filter.getLimit());
        } catch (ParseException e) {
           throw new BulkStorageException("problem parsing filter timestamp.", e);
        }
    }

    @Override
    public Long getKey(String uid) throws BulkStorageException {
        return valid(uid).getId();
    }

    @Override
    public Optional<BulkRequest> getRequest(String uid) throws BulkStorageException {
        Optional<BulkRequest> stored = Optional.empty();
        try {
            stored = requestCache.get(uid);
        } catch (CompletionException e) {
            Throwable cause = e.getCause();
            LOGGER.error("Fatal error trying to get request {}: "
                  + "{}.", uid, cause == null ? e.getMessage() : cause.getMessage());
        }

        if (stored.isEmpty()) {
            return Optional.empty();
        }

        BulkRequest request = stored.get();

        if (request == null) {
            throw new BulkStorageException(
                  "BulkRequest object missing for " + uid + "!");
        }

        return Optional.of(request);
    }

    @Override
    public List<BulkRequestSummary> getRequestSummaries(Set<BulkRequestStatus> status,
          Set<String> owners, String path, Long id) throws BulkStorageException {
        LOGGER.trace("getRequestSummaries {}, {}, {}.", status, owners, path);

        String pnfsId = path == null ? null : getPnfsidFor(path).toString();
        String[] users = owners == null ? null : owners.toArray(String[]::new);

        List<BulkRequest> requests = requestDao.get(
              requestDao.where().sorter("bulk_request.id").id(id).pnfsId(pnfsId).status(status)
                    .user(users), FETCH_SIZE, false);

        List<BulkRequestSummary> summaries = new ArrayList<>();

        for (BulkRequest r : requests) {
            try {
                summaries.add(new BulkRequestSummary(r.getId(),
                      r.getUrlPrefix() + "/" + r.getUid(),
                      r.getActivity(),
                      r.getStatusInfo(),
                      targetStore.countUnprocessed(r.getId())));
            } catch (BulkStorageException e) {
                LOGGER.error("Unable to retrieve unprocessed count for {}: {}.", r.getUid(),
                      e.getMessage());
            }
        }

        return summaries;
    }

    @Override
    public Optional<Restriction> getRestriction(String uid)
          throws BulkStorageException {
        Optional<JdbcBulkRequestPermissions> permissions = requestPermissionsDao.get(
              requestPermissionsDao.where().permId(uid));

        if (permissions.isEmpty()) {
            return Optional.empty();
        }

        return Optional.of(permissions.get().getRestriction());
    }

    @Override
    public BulkRequestInfo getRequestInfo(Subject subject, String uid, long offset)
          throws BulkStorageException, BulkPermissionDeniedException {
        LOGGER.trace("getRequestInfo {}, {}.", BulkRequestStore.uidGidKey(subject), uid);
        BulkRequest stored = valid(uid);

        if (!isRequestSubject(subject, uid)) {
            throw new BulkPermissionDeniedException(uid);
        }

        return processInfo(stored, offset);
    }

    @Override
    public Optional<BulkRequestStatus> getRequestStatus(String uid)
          throws BulkStorageException {
        LOGGER.trace("getRequestInfo {}.", uid);

        BulkRequest stored = get(uid);

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
    public Optional<Subject> getSubject(String uid) throws BulkStorageException {
        Optional<JdbcBulkRequestPermissions> permissions = requestPermissionsDao.get(
              requestPermissionsDao.where().permId(uid));

        if (permissions.isEmpty()) {
            return Optional.empty();
        }

        return Optional.of(permissions.get().getSubject());
    }

    @Override
    public boolean isRequestSubject(Subject subject, String uid)
          throws BulkStorageException {
        Optional<Subject> requestSubject = getSubject(uid);
        if (requestSubject.isEmpty()) {
            return false;
        }
        return Subjects.isRoot(subject) || Subjects.hasAdminRole(subject) ||
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
        /*
         *  Deletion of the bulk request job (parent = -1) is necessary to avoid processing
         *  a placeholder target as if it represented a real namespace entry.
         *
         *  We can safely delete all non-terminal discovered targets as they are produced
         *  from recursion and will be found again.
         *
         *  Update non-terminal initial nodes to created.
         */
        requestTargetDao.delete(requestTargetDao.where().pids(PID.ROOT.ordinal()));
        requestTargetDao.delete(
              requestTargetDao.where().pids(DISCOVERED.ordinal()).state(NON_TERMINAL));
        requestTargetDao.update(
              requestTargetDao.where().pids(INITIAL.ordinal()).state(NON_TERMINAL),
              requestTargetDao.set().state(CREATED).errorType(null).errorMessage(null));
        requestDao.update(requestDao.where().status(STARTED, CANCELLING),
              requestDao.set().status(QUEUED));
    }

    @Override
    public List<BulkRequest> next(Optional<String> sortedBy, Optional<Boolean> reverse, long limit)
          throws BulkStorageException {
        LOGGER.trace("next {}.", limit);
        return requestDao.get(
                    requestDao.where().status(QUEUED).sorter(sortedBy.orElse("arrived_at"))
                          .reverse(reverse.orElse(false)), (int) limit, true)
              .stream().collect(Collectors.toList());
    }

    @Override
    public void reset(String uid) throws BulkStorageException {
        /**
         *  Start from scratch:
         *  - delete ROOT
         *  - delete DISCOVERED
         *  - set INITIAL to CREATED
         *
         *  NOTE that without actually querying the database, we cannot know whether
         *  to decrement state counts here or not, since the reset may have been issued
         *  after a restart.  Hence all terminal target states shown is statistics are
         *  cumulative from start up.
         */
        LOGGER.trace("reset {}.", uid);
        requestTargetDao.delete(requestTargetDao.where().pids(PID.ROOT.ordinal()).ruids(uid));
        requestTargetDao.delete(requestTargetDao.where().pids(DISCOVERED.ordinal()).ruids(uid));
        requestTargetDao.update(requestTargetDao.where().pids(INITIAL.ordinal()).ruids(uid),
              requestTargetDao.set().state(CREATED).errorType(null).errorMessage(null));
        requestDao.update(requestDao.where().uids(uid),
              requestDao.set().status(QUEUED));
        try {
            requestCache.get(uid).ifPresent(r -> {
                BulkRequestStatusInfo status = r.getStatusInfo();
                status.setStatus(QUEUED);
                status.setStartedAt(null);
                status.setLastModified(System.currentTimeMillis());
                status.setCompletedAt(null);
            });
        } catch (CompletionException e) {
            throw new BulkStorageException(e.getMessage(), e.getCause());
        }
    }

    @Required
    public void setArchiveDao(JdbcBulkArchiveDao archiveDao) {
        this.archiveDao = archiveDao;
    }

    @Required
    public void setBulkUtils(JdbcBulkDaoUtils utils) {
        this.utils = utils;
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
    public void setPnfsManager(CellStub pnfsManager) {
        pnfsHandler = new PnfsHandler(pnfsManager);
        pnfsHandler.setSubject(Subjects.ROOT);
        pnfsHandler.setRestriction(Restrictions.none());
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
    public void setRequestPermissionsDao(JdbcBulkRequestPermissionsDao requestPermissionsDao) {
        this.requestPermissionsDao = requestPermissionsDao;
    }

    @Required
    public void setStatistics(BulkServiceStatistics statistics) {
        this.statistics = statistics;
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
                        request.setId((long) keyHolder.getKeys().get("id")));

            requestPermissionsDao.insert(
                  requestPermissionsDao.set().permId(request.getId()).subject(subject)
                        .restriction(restriction));

            requestDao.insertArguments(request);

            requestTargetDao.insertInitialTargets(request);
        } catch (BulkStorageException e) {
            throw new BulkStorageException("store failed for " + request.getUid(), e);
        }
    }

    @Override
    public boolean update(String uid, BulkRequestStatus status)
          throws BulkStorageException {
        LOGGER.trace("update {}, {}.", uid, status);
        BulkRequest stored = valid(uid);
        BulkRequestStatus storedStatus = stored.getStatusInfo().getStatus();

        if (storedStatus == status) {
            return false;
        }

        boolean update = false;

        if (storedStatus == null) {
            update = requestDao.update(requestDao.where().uids(uid),
                  requestDao.set().status(status)) == 1;
        } else {
            switch (storedStatus) {
                case COMPLETED:
                case CANCELLED:
                    break;
                case CANCELLING:
                    if (status == CANCELLED) {
                        update = requestDao.updateTo(status, uid) == 1;
                    }
                    break;
                case STARTED:
                    switch (status) {
                        case COMPLETED:
                        case CANCELLED:
                            update = requestDao.updateTo(status, uid) == 1;
                            break;
                        case CANCELLING:
                            update = requestDao.update(requestDao.where().uids(uid),
                                  requestDao.set().status(status)) == 1;
                            break;
                    }
                    break;
                case QUEUED:
                    switch (status) {
                        case COMPLETED:
                        case CANCELLED:
                            update = requestDao.updateTo(status, uid) == 1;
                            break;
                        default:
                            update = requestDao.update(requestDao.where().uids(uid),
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
            requestCache.get(uid).ifPresent(r -> {
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
        } catch (CompletionException e) {
            throw new BulkStorageException(e.getMessage(), e.getCause());
        }

        /*
         *  Maintain the in-memory active request id indices.  Check for clear on success or failure.
         */
        switch (status) {
            case CANCELLED:
            case COMPLETED:
                synchronized (this) {
                    String user = userOfActiveRequest.remove(uid);
                    activeRequestsByUser.remove(user, uid);
                }
                conditionallyClearTerminalRequest(stored);
                break;
            case STARTED:
                synchronized (this) {
                    String user = stored.getStatusInfo().getUser();
                    activeRequestsByUser.put(user, uid);
                    userOfActiveRequest.put(uid, user);
                }
                break;
        }

        return true;
    }

    /*
     *  Package so that archiver can call it.
     */
    void archiveRequest(BulkRequest request) {
        BulkArchivedRequestInfo info = new BulkArchivedRequestInfo(request);
        long nextId = 0;

        do {
            nextId = processTargets(request.getTargetPrefix(), request.getId(), nextId,
                  info::addTarget);
        } while (nextId != NO_FURTHER_ENTRIES);

        archiveDao.insert(info);
    }

    private String checkRequestPermissions(Subject subject, String uid)
          throws BulkPermissionDeniedException, BulkStorageException {
        String key = BulkRequestStore.uidGidKey(subject);

        Optional<JdbcBulkRequestPermissions> permissions = requestPermissionsDao.get(
              requestPermissionsDao.where().permId(uid));

        if (permissions.isEmpty()) {
            throw new BulkPermissionDeniedException(uid);
        }

        Subject requestSubject = permissions.get().getSubject();

        if (requestSubject == null) {
            LOGGER.error("Fatal error trying to clear {}: "
                  + "request has no subject.", uid);
        }

        if (!Subjects.isRoot(subject) && !Subjects.hasAdminRole(subject) &&
              !key.equals(BulkRequestStore.uidGidKey(requestSubject))) {
            throw new BulkPermissionDeniedException(uid);
        }

        return key;
    }

    private void conditionallyClearTerminalRequest(BulkRequest stored) {
        Long rid = stored.getId();
        if (requestTargetDao.count(requestTargetDao.where().rid(rid).state(State.FAILED)) > 0) {
            if (stored.isClearOnFailure()) {
                clear(stored.getUid());
            }
        } else if (stored.isClearOnSuccess()) {
            clear(stored.getUid());
        }
    }

    /*
     *  We now archive requests on clear.
     */
    private void clear(BulkRequest request) {
        archiveRequest(request);
        String uid = request.getUid();
        requestDao.delete(requestDao.where().uids(uid));
        requestCache.invalidate(uid);
    }

    private BulkRequest get(String uid) throws BulkStorageException {
        try {
            return requestCache.get(uid).orElse(null);
        } catch (CompletionException e) {
            Throwable cause = e.getCause();
            throw new BulkStorageException(
                  cause == null ? e.getMessage() : cause.getMessage());
        }
    }

    private PnfsId getPnfsidFor(String path) throws BulkStorageException {
        try {
            return pnfsHandler.getFileAttributes(path, MINIMALLY_REQUIRED_ATTRIBUTES).getPnfsId();
        } catch (CacheException e) {
            throw new BulkStorageException("could not retrieve pnfsid for " + path, e);
        }
    }

    private BulkRequestInfo processInfo(BulkRequest stored, long offset) {
        BulkRequestInfo info = new BulkRequestInfo();
        BulkRequestStatusInfo status = stored.getStatusInfo();
        String uid = stored.getUid();
        String prefixString = stored.getTargetPrefix();
        info.setUid(uid);
        info.setStatus(status.getStatus());
        info.setArrivedAt(status.getCreatedAt());
        info.setLastModified(status.getLastModified());
        info.setStartedAt(status.getStartedAt());
        info.setTargetPrefix(prefixString);

        List<BulkRequestTargetInfo> targetInfo = new ArrayList<>();
        info.setNextId(processTargets(prefixString, stored.getId(), offset, t-> targetInfo.add(t)));
        info.setTargets(targetInfo);
        return info;
    }

    private long processTargets(String prefixString, long id, long offset,
          Consumer<BulkRequestTargetInfo> consumer) {
        /*
         *  Order by id from offset.  Limit is FETCH_SIZE per swatch.
         */
        List<BulkRequestTarget> targets = new ArrayList<>(requestTargetDao.get(
              requestTargetDao.where().rid(id).offset(offset).notRootRequest()
                    .sorter("request_target.id"), FETCH_SIZE));

        FsPath prefix = Strings.emptyToNull(prefixString) == null ? null
              : FsPath.create(prefixString);

        BulkRequestTarget initial = targets.stream().filter(t -> t.getPid() == INITIAL).findAny()
              .orElse(null);

        boolean stripRecursive =
              prefix != null && initial != null && !initial.getPath().hasPrefix(prefix);

        targets.stream().map(t -> toRequestTargetInfo(t, stripRecursive ? prefix : null))
              .forEach(consumer);

        int size = targets.size();
        if (size == FETCH_SIZE) {
            return (targets.get(size - 1).getId() + 1);
        } else {
            return NO_FURTHER_ENTRIES;
        }
    }

    private BulkRequestTargetInfo toRequestTargetInfo(BulkRequestTarget target,  FsPath prefix) {
        BulkRequestTargetInfo info = new BulkRequestTargetInfo();
        info.setId(target.getId());
        info.setInitial(target.getPid() == INITIAL);
        if (prefix != null && !info.isInitial()) {
            info.setTarget(target.getPath().stripPrefix(prefix));
        } else {
            info.setTarget(target.getPath().toString());
        }
        info.setState(target.getState().name());
        info.setSubmittedAt(target.getCreatedAt());
        info.setStartedAt(target.getStartedAt());
        if (target.isTerminated()) {
            info.setFinishedAt(target.getLastUpdated());
        }
        info.setErrorType(target.getErrorType());
        info.setErrorMessage(target.getErrorMessage());
        return info;
    }

    private BulkRequest valid(String uid) throws BulkStorageException {
        BulkRequest stored = get(uid);
        if (stored == null) {
            throw new BulkRequestNotFoundException("request " + uid + " not found");
        }
        return stored;
    }
}
