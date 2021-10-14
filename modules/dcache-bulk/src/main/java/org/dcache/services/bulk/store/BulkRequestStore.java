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
package org.dcache.services.bulk.store;

import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import javax.security.auth.Subject;
import org.dcache.auth.Subjects;
import org.dcache.auth.attributes.Restriction;
import org.dcache.services.bulk.BulkPermissionDeniedException;
import org.dcache.services.bulk.BulkRequest;
import org.dcache.services.bulk.BulkRequestStatus;
import org.dcache.services.bulk.BulkRequestStatus.Status;
import org.dcache.services.bulk.BulkRequestStorageException;
import org.dcache.services.bulk.BulkStorageException;

/**
 * Implemented by the underlying stores; combines both internal and external APIs (the latter in
 * support of user-facing requests).
 */
public interface BulkRequestStore {

    static String uidGidKey(Subject subject) {
        try {
            if (subject == null) {
                return "<unknown>:<unknown>";
            }

            return Subjects.getUid(subject) + ":"
                  + Subjects.getPrimaryGid(subject);
        } catch (NoSuchElementException | IllegalArgumentException e) {
            return "<unknown>:<unknown>";
        }
    }

    /**
     * Does not throw exception, as this is a last resort cancellation.
     * <p>
     * Should not clear the request from store unless automatic clear is set.
     *
     * @param requestId unique id for request.
     * @param exception possibly associated with the abort.
     */
    void abort(String requestId, Throwable exception);

    /**
     * Updates the target count.
     *
     * @param requestId unique id for request.
     * @throws BulkRequestStorageException
     */
    void addTarget(String requestId) throws BulkRequestStorageException;

    /**
     * Releases all resources associated with this request id.
     *
     * @param subject   originator of the requests.
     * @param requestId unique id for request.
     * @throws BulkRequestStorageException
     */
    void clear(Subject subject, String requestId)
          throws BulkRequestStorageException,
          BulkPermissionDeniedException;

    /**
     * @param requestId unique id for request.
     */
    void clear(String requestId);

    /**
     * @return the number of currently active requests.
     */
    int countActive() throws BulkRequestStorageException;

    /**
     * @param user originator of the requests (=uidGidKey).
     * @return the number of (matching) requests owned by the owner which have yet to be completed.
     * @throws BulkRequestStorageException
     */
    int countNonTerminated(String user) throws BulkRequestStorageException;

    /**
     * @param requestFilter optional filter on the request.
     * @param statusFilter  optional filter on the request status
     * @param limit         max requests to return (can be <code>null</code>).
     * @return a collection of requests in the store which match the filters, if present; no filter
     * means return all.
     * @throws BulkRequestStorageException
     */
    Collection<BulkRequest> find(Optional<Predicate<BulkRequest>> requestFilter,
          Optional<Predicate<BulkRequestStatus>> statusFilter,
          Long limit)
          throws BulkRequestStorageException;

    /**
     * @param requestId unique id for request.
     * @return optional of the request.
     */
    Optional<BulkRequest> getRequest(String requestId)
          throws BulkRequestStorageException;

    /**
     * @param subject of request user
     * @param status  match only the requests with these statuses.
     * @return set of the corresponding request urls.
     * @throws BulkRequestStorageException
     */
    Set<String> getRequestUrls(Subject subject, Set<Status> status)
          throws BulkRequestStorageException;

    /**
     * @param requestId unique id for request.
     * @return optional of the Restriction of the request.
     * @throws BulkRequestStorageException
     */
    Optional<Restriction> getRestriction(String requestId)
          throws BulkRequestStorageException;

    /**
     * @param subject   of request user
     * @param requestId unique id for request.
     * @return optional of the corresponding request status.
     * @throws BulkRequestStorageException
     */
    BulkRequestStatus getStatus(Subject subject, String requestId)
          throws BulkRequestStorageException,
          BulkPermissionDeniedException;

    /**
     * @param requestId unique id for request.
     * @return optional of the status of the request.
     * @throws BulkRequestStorageException
     */
    Optional<BulkRequestStatus> getStatus(String requestId)
          throws BulkRequestStorageException;

    /**
     * @return comparator for request status by timestamp.  Takes requestId.
     */
    Comparator<String> getStatusComparator();

    /**
     * @param requestId unique id for request.
     * @return optional of the uid:gid subject of the request.
     * @throws BulkRequestStorageException
     */
    Optional<Subject> getSubject(String requestId)
          throws BulkRequestStorageException;

    /**
     * Checks to see if the subject owns or has access to the request.
     *
     * @param subject   of the user
     * @param requestId to check
     * @return true if request is accessible by user
     */
    boolean isRequestSubject(Subject subject, String requestId);

    /**
     * For internal use.
     *
     * @return set of ids.
     */
    Set<String> ids();

    /**
     * Load the store into memory. May be a NOP.
     */
    void load() throws BulkStorageException;

    /**
     * @param limit max requests to return
     * @return list of requests in the store which are queued to start; these should be ordered by
     * arrival time.
     * @throws BulkRequestStorageException
     */
    List<BulkRequest> next(long limit) throws BulkRequestStorageException;

    /**
     * Reset the request to QUEUED state.
     *
     * @param requestId unique id for request.
     * @throws BulkRequestStorageException
     */
    void reset(String requestId) throws BulkRequestStorageException;

    /**
     * Persist the store from memory. May be a NOP.
     */
    void save() throws BulkStorageException;

    /**
     * Store the request.  It is understood that an associated status object is also generated and
     * stored with it.
     *
     * @param subject     sending the request.
     * @param restriction on subject's permissions.
     * @param request     request specifics.
     * @param status      request status; if <code>null</code> a new instance will be created.
     * @throws BulkRequestStorageException
     */
    void store(Subject subject,
          Restriction restriction,
          BulkRequest request,
          BulkRequestStatus status)
          throws BulkRequestStorageException;

    /**
     * Update the request target record error.  Does not increment the completion count as this
     * target has not been added since it failed prematurely.
     *
     * @param requestId unique id for request.
     * @param target    belonging to the request.
     * @param exception if any
     * @throws BulkRequestStorageException
     */
    void targetAborted(String requestId, String target, Throwable exception)
          throws BulkRequestStorageException;

    /**
     * Update the request target completion count and record error.
     *
     * @param requestId unique id for request.
     * @param target    belonging to the request.
     * @param exception if any
     * @throws BulkRequestStorageException
     */
    void targetCompleted(String requestId, String target, Throwable exception)
          throws BulkRequestStorageException;

    /**
     * Update the status of the request.
     *
     * @param requestId unique id for request.
     * @param status    queued, started, cancelled, completed.
     * @throws BulkRequestStorageException
     */
    void update(String requestId, Status status)
          throws BulkRequestStorageException;
}
