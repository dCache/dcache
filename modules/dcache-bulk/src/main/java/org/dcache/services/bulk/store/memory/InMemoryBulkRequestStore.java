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
package org.dcache.services.bulk.store.memory;

import com.google.common.base.Strings;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import javax.annotation.concurrent.GuardedBy;
import javax.security.auth.Subject;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.dcache.auth.Subjects;
import org.dcache.auth.attributes.Restriction;
import org.dcache.services.bulk.BulkPermissionDeniedException;
import org.dcache.services.bulk.BulkRequest;
import org.dcache.services.bulk.BulkRequestNotFoundException;
import org.dcache.services.bulk.BulkRequestStatus;
import org.dcache.services.bulk.BulkRequestStatus.Status;
import org.dcache.services.bulk.BulkRequestStorageException;
import org.dcache.services.bulk.BulkStorageException;
import org.dcache.services.bulk.store.BulkRequestStore;

import static org.dcache.services.bulk.BulkRequestStatus.Status.CANCELLED;
import static org.dcache.services.bulk.BulkRequestStatus.Status.QUEUED;
import static org.dcache.services.bulk.store.BulkRequestStore.uidGidKey;

/**
 *  Simple set of maps and indices.
 */
public class InMemoryBulkRequestStore extends InMemoryStore
                implements BulkRequestStore
{
    /**
     *  Maintains ordering where earlier timestamps
     *  precede later ones.
     */
    public class StatusComparator implements Comparator<String>
    {
        @Override
        public int compare(String left, String right)
        {
            if (left == null) {
                return 1;
            }

            if (right == null) {
                return -1;
            }

            BulkRequestStatus statusLeft = status.get(left);
            BulkRequestStatus statusRight = status.get(right);

            if (statusLeft == null) {
                return 1;
            }

            if (statusRight == null) {
                return -1;
            }

            return Long.compare(statusLeft.getFirstArrived(),
                                statusRight.getFirstArrived());
        }
    }

    /**
     * We treat synchronization on these maps en bloc.
     */
    private final Map<String, BulkRequest>       requests;
    private final Map<String, BulkRequestStatus> status;
    private final Map<String, Subject>           requestSubject;
    private final Map<String, Restriction>       requestRestriction;
    private final Multimap<String, String>       uidGidRequests;
    private final StatusComparator               comparator;
    private final Predicate<String>              notTerminated;

    /**
     * Maintains order of arrival via comparator (above).
     */
    private final Set<String> queued;

    /**
     * Requests currently being processed.
     */
    private final Set<String> active;

    public InMemoryBulkRequestStore()
    {
        requests = new HashMap<>();
        status = new HashMap<>();
        requestSubject = new HashMap<>();
        requestRestriction = new HashMap<>();
        uidGidRequests = HashMultimap.create();
        comparator = new StatusComparator();
        queued = new TreeSet<>(comparator);
        active = new HashSet<>();
        notTerminated =
                        id -> {
                            BulkRequestStatus bstat = this.status.get(id);
                            if (bstat == null) {
                                return true;
                            }
                            Status status = bstat.getStatus();
                            if (status == null) {
                                return true;
                            }
                            return status != Status.CANCELLED
                                            && status != Status.COMPLETED;
                        };
    }

    @Override
    public void abort(String requestId, Throwable exception)
    {
        write.lock();
        try {
            queued.remove(requestId);
            active.remove(requestId);
            BulkRequestStatus status = this.status.get(requestId);
            BulkRequest request = requests.get(requestId);
            status.targetCompleted(request.getTarget(), exception);
            status.setStatus(Status.COMPLETED);
        } finally {
            write.unlock();
        }
    }

    @Override
    public void addTarget(String requestId)
    {
        write.lock();
        try {
            BulkRequestStatus bulkRequestStatus = status.get(requestId);
            if (bulkRequestStatus != null) {
                bulkRequestStatus.targetAdded();
            }
        } finally {
            write.unlock();
        }
    }

    public void clear(String requestId)
    {
        write.lock();
        try {
            queued.remove(requestId);
            active.remove(requestId);
            Subject subject = requestSubject.remove(requestId);
            requests.remove(requestId);
            status.remove(requestId);
            requestRestriction.remove(requestId);
            uidGidRequests.remove(uidGidKey(subject), requestId);
        } finally {
            write.unlock();
        }
    }

    @Override
    public void clear(Subject subject, String requestId)
                    throws BulkPermissionDeniedException
    {
        if (!isRequestSubject(subject, requestId)) {
            throw new BulkPermissionDeniedException(requestId);
        }

        clear(requestId);
    }

    /**
     *  Using the #find() method for this creates a bottleneck as the
     *  store grows in size, since this method is called very frequently
     *  and find is stream filtering, O(size of requests).
     */
    @Override
    public int countActive()
    {
        read.lock();
        try {
            return active.size();
        } finally {
            read.unlock();
        }
    }

    @Override
    public int countNonTerminated(String user)
    {
        read.lock();
        try {
             return (int) uidGidRequests.get(user)
                                        .stream()
                                        .filter(notTerminated)
                                        .count();
        } finally {
            read.unlock();
        }
    }

    @Override
    public Collection<BulkRequest> find(Optional<Predicate<BulkRequest>> requestFilter,
                                        Optional<Predicate<BulkRequestStatus>> statusFilter,
                                        Long limit)
    {
        read.lock();
        try {
            if (limit == null) {
                limit = Long.MAX_VALUE;
            }

            Stream<String> keys;

            if (statusFilter.isPresent()) {
                Predicate<BulkRequestStatus> sf = statusFilter.get();
                keys = status.entrySet().stream().filter(e -> sf.test(e.getValue()))
                             .map(Entry::getKey);
            } else {
                keys = requests.keySet().stream();
            }

            if (requestFilter.isPresent()) {
                Predicate<BulkRequest> rf = requestFilter.get();
                return keys.map(requests::get)
                           .filter(rf::test)
                           .limit(limit)
                           .collect(Collectors.toList());
            } else {
                return keys.map(requests::get)
                           .limit(limit)
                           .collect(Collectors.toList());
            }
        } finally {
            read.unlock();
        }
    }

    @Override
    public Comparator<String> getStatusComparator()
    {
        return comparator;
    }

    @Override
    public Set<String> getRequestUrls(Subject subject, Set<Status> status)
    {
        read.lock();
        try {
            /*
             *  do not allow non-root users to see other's requests
             */
            Predicate<Entry<String, BulkRequestStatus>> isUsersRequest
                = e -> Subjects.isRoot(subject) || uidGidKey(subject).equals
                                 (uidGidKey(requestSubject.get(e.getKey())));

            return this.status.entrySet().stream()
                                         .filter(isUsersRequest)
                                         .filter(e -> status.isEmpty()
                                                 ||   status.contains(e.getValue()
                                                                       .getStatus()))
                                         .map(e -> requests.get(e.getKey()))
                                         .map(r -> r.getUrlPrefix()
                                                         + "/" + r.getId())
                                         .collect(Collectors.toSet());
        } finally {
            read.unlock();
        }
    }

    @Override
    public Optional<BulkRequest> getRequest(String requestId)
    {
        read.lock();
        try {
            return Optional.ofNullable(requests.get(requestId));
        } finally {
            read.unlock();
        }
    }

    @Override
    public Optional<Restriction> getRestriction(String requestId)
    {
        read.lock();
        try {
            return Optional.ofNullable(requestRestriction.get(requestId));
        } finally {
            read.unlock();
        }
    }

    @Override
    public Optional<BulkRequestStatus> getStatus(String requestId)
    {
        read.lock();
        try {
            return Optional.ofNullable(status.get(requestId));
        } finally {
            read.unlock();
        }
    }

    @Override
    public BulkRequestStatus getStatus(Subject subject,
                                                 String requestId)
                    throws BulkPermissionDeniedException,
                    BulkRequestStorageException
    {
        read.lock();
        try {
            if (!isRequestSubject(subject, requestId)) {
                throw new BulkPermissionDeniedException(requestId);
            }
            return getNonNullStatus(requestId);
        } finally {
            read.unlock();
        }
    }

    @Override
    public Optional<Subject> getSubject(String requestId)
    {
        read.lock();
        try {
            return Optional.ofNullable(requestSubject.get(requestId));
        } finally {
            read.unlock();
        }
    }

    @Override
    public boolean isRequestSubject(Subject subject, String requestId)
    {
        read.lock();
        try {
            Subject requestSubject = this.requestSubject.get(requestId);
            return Subjects.isRoot(subject) ||
                            uidGidKey(subject).equals(
                                            uidGidKey(requestSubject));
        } finally {
            read.unlock();
        }
    }

    /*
     *  Used by wrapper class
     */
    public Set<String> ids()
    {
        read.lock();
        try {
            return requests.keySet().stream().collect(Collectors.toSet());
        } finally {
            read.unlock();
        }
    }

    @Override
    public void load() throws BulkStorageException
    {
        throw new BulkStorageException("Not supported for in-memory storage.");
    }

    /*
     * Since this is called frequently, it has been optimized
     * against the in-memory queue index, which is timestamp ordered.
     */
    @Override
    public List<BulkRequest> next(long limit)
    {
        read.lock();
        try {
            if (queued.isEmpty()) {
                return Collections.EMPTY_LIST;
            }
            return queued.stream()
                         .map(requests::get)
                         .limit(limit)
                         .collect(Collectors.toList());
        } finally {
            read.unlock();
        }
    }

    @Override
    public void reset(String requestId) throws BulkRequestStorageException
    {
        write.lock();
        try {
            BulkRequestStatus stored = getNonNullStatus(requestId);
            stored.setFailures(null);
            stored.setProcessed(0);
            stored.setTargets(0);
            stored.setStatus(QUEUED);
            active.remove(requestId);
            queued.add(requestId);
        } finally {
            write.unlock();
        }
    }

    @Override
    public void store(Subject subject,
                      Restriction restriction,
                      BulkRequest request,
                      BulkRequestStatus status)
    {
        write.lock();
        try {
            String requestId = request.getId();
            storeRequest(requestId, request, status);
            storePermissions(requestId, subject, restriction);
        } finally {
            write.unlock();
        }
    }

    @Override
    public void targetAborted(String requestId, String target, Throwable exception)
                    throws BulkRequestStorageException
    {
        write.lock();
        try {
            target = checkTarget(requestId, target);
            getNonNullStatus(requestId).targetAborted(target, exception);
        } finally {
            write.unlock();
        }
    }

    @Override
    public void targetCompleted(String requestId, String target, Throwable exception)
        throws BulkRequestStorageException
    {
        write.lock();
        try {
            target = checkTarget(requestId, target);
            getNonNullStatus(requestId).targetCompleted(target, exception);
        } finally {
            write.unlock();
        }
    }

    @Override
    public void update(String requestId, Status status)
                    throws BulkRequestStorageException
    {
        write.lock();
        try {
            switch (status)
            {
                case COMPLETED:
                case CANCELLING:
                case CANCELLED:
                    queued.remove(requestId);
                    active.remove(requestId);
                    break;
                case STARTED:
                    queued.remove(requestId);
                    active.add(requestId);
                    break;
            }

            LOGGER.trace("update {} to {}, queued {}, active {}.",
                         requestId, status, queued.size(), active.size());

            BulkRequestStatus current = getNonNullStatus(requestId);
            Status storedStatus = current.getStatus();

            if (storedStatus == null) {
                current.setStatus(status);
            } else if (storedStatus == status) {
                return;
            } else {
                switch (current.getStatus()) {
                    case COMPLETED:
                    case CANCELLED:
                        break;
                    case CANCELLING:
                        if (status == CANCELLED) {
                            current.setStatus(status);
                        }
                        break;
                    case STARTED:
                        if (status != QUEUED) {
                            current.setStatus(status);
                        }
                        break;
                    case QUEUED:
                        current.setStatus(status);
                        break;
                    default:
                        break;
                }
            }
        } finally {
            write.unlock();
        }
    }

    @GuardedBy("write")
    private BulkRequestStatus getNonNullStatus(String requestId)
                    throws BulkRequestNotFoundException
    {
        BulkRequestStatus stored = this.status.get(requestId);
        if (stored == null) {
            String error = "request id " + requestId
                            + " is no longer valid!";
            throw new BulkRequestNotFoundException(error);
        }
        return stored;
    }

    @GuardedBy("write")
    private String checkTarget(String requestId, String target)
                    throws BulkRequestStorageException
    {
        BulkRequest request = this.requests.get(requestId);
        if (request == null) {
            String error = "request id " + requestId
                            + " is no longer valid!";
            throw new BulkRequestNotFoundException(error);
        }

        String prefix = request.getTargetPrefix();
        if (Strings.emptyToNull(prefix) != null && target.startsWith(prefix)) {
            target = target.substring(prefix.length());
        }

        return target;
    }

    @GuardedBy("write")
    private void storeRequest(String requestId,
                              BulkRequest request,
                              BulkRequestStatus status)
    {
        requests.put(requestId, request);

        if (status == null) {
            status = new BulkRequestStatus();
            long now = System.currentTimeMillis();
            status.setFirstArrived(now);
            status.setStatus(QUEUED);
        }

        /*
         * Since queue comparator references status, add that first.
         */
        this.status.put(requestId, status);

        if (status.getStatus() == QUEUED) {
            queued.add(requestId);
        }
    }

    @GuardedBy("store,load")
    private void storePermissions(String requestId,
                                  Subject subject,
                                  Restriction restriction)
    {
        requestSubject.put(requestId, subject);
        uidGidRequests.put(uidGidKey(subject), requestId);
        requestRestriction.put(requestId, restriction);
    }
}
