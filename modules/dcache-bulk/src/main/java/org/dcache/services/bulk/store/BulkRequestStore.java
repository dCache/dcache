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

import com.google.common.collect.ListMultimap;
import java.util.Collection;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import javax.security.auth.Subject;
import org.dcache.auth.Subjects;
import org.dcache.auth.attributes.Restriction;
import org.dcache.services.bulk.BulkPermissionDeniedException;
import org.dcache.services.bulk.BulkRequest;
import org.dcache.services.bulk.BulkRequestInfo;
import org.dcache.services.bulk.BulkRequestStatus;
import org.dcache.services.bulk.BulkRequestSummary;
import org.dcache.services.bulk.BulkStorageException;
import org.dcache.services.bulk.util.BulkRequestFilter;

/**
 * Implemented by the underlying DAOs; combines both internal and external APIs (the latter in
 * support of user-facing requests).
 */
public interface BulkRequestStore {

    /**
     * Should identify the user/owner.
     *
     * @param subject passed in with the request.
     * @return string with form <uid>:<gid>
     */
    static String uidGidKey(Subject subject) {
        try {
            if (subject == null) {
                return "<unknown>:<unknown>";
            }

            return Subjects.getUid(subject) + ":" + Subjects.getPrimaryGid(subject);
        } catch (NoSuchElementException | IllegalArgumentException e) {
            return "<unknown>:<unknown>";
        }
    }

    /**
     * Does not throw exception, as this is an internal termination of the request.
     * <p>
     * Should not clear the request from store unless automatic clear is set.
     *
     * @param request which failed.
     * @param exception possibly associated with the abort.
     */
    void abort(BulkRequest request, Throwable exception);

    /**
     * Releases all resources associated with this request id.
     *
     * @param subject   originator of the requests.
     * @param uid unique id for request.
     * @throws BulkStorageException, BulkPermissionDeniedException
     */
    void clear(Subject subject, String uid)
          throws BulkStorageException, BulkPermissionDeniedException;

    /**
     * For internal use.
     *
     * @param uid unique id for request.
     */
    void clear(String uid);

    /**
     * For internal use.
     *
     * @param requestId unique id for request.
     */
    void clearWhenTerminated(Subject subject, String requestId)
          throws BulkStorageException, BulkPermissionDeniedException;

    /**
     * @param filter to match request.
     * @return number of matching entries.
     */
    long count(BulkRequestFilter filter);

    /**
     * @return the number of currently active requests.
     * @throws BulkStorageException
     */
    int countActive() throws BulkStorageException;

    /**
     * @param user originator of the requests (=uidGidKey).
     * @return the number of (matching) requests owned by the user which have yet to be completed.
     * @throws BulkStorageException
     */
    int countNonTerminated(String user) throws BulkStorageException;

    /**
     * @param filter optional filter on the request.
     * @param limit  max requests to return (can be <code>null</code>).
     * @return a collection of requests in the store which match the filter, if present; no filter
     * means return all.
     * @throws BulkStorageException
     */
    Collection<BulkRequest> find(Optional<BulkRequestFilter> filter, Integer limit)
          throws BulkStorageException;

    /**
     * @return a multimap of user : ordered request id list for currently active requests.
     * @throws BulkStorageException
     */
    ListMultimap<String, String> getActiveRequestsByUser() throws BulkStorageException;

    /**
     * @param uid unique id for request.
     * @return the key (sequence number) of the request.
     * @throws BulkStorageException
     */
    Long getKey(String uid) throws BulkStorageException;

    /**
     * @param uid unique id for request.
     * @return optional of the request.
     */
    Optional<BulkRequest> getRequest(String uid) throws BulkStorageException;

    /**
     * @param status  match only the requests with these statuses.
     * @param owners  match only the requests with these owners.
     * @param path match only the requests with targets whose paths equal or contain this path.
     * @param id match only the requests with id greater or equal to this one.
     * @return list of the corresponding request urls.
     * @throws BulkStorageException
     */
    List<BulkRequestSummary> getRequestSummaries(Set<BulkRequestStatus> status, Set<String> owners,
          String path, Long id) throws BulkStorageException;

    /**
     * @param uid unique id for request.
     * @return optional of the Restriction of the request.
     * @throws BulkStorageException
     */
    Optional<Restriction> getRestriction(String uid) throws BulkStorageException;

    /**
     * @param subject   of request user.
     * @param uid unique id for request.
     * @param offset    into the list of targets ordered by sequence number
     * @return optional of the corresponding request status.
     * @throws BulkStorageException, BulkPermissionDeniedException
     */
    BulkRequestInfo getRequestInfo(Subject subject, String uid, long offset)
          throws BulkStorageException, BulkPermissionDeniedException;

    /**
     * @param uid unique id for request.
     * @return optional of the status of the request.
     * @throws BulkStorageException
     */
    Optional<BulkRequestStatus> getRequestStatus(String uid)
          throws BulkStorageException;

    /**
     * @param uid unique id for request.
     * @return optional of the uid:gid subject of the request.
     * @throws BulkStorageException
     */
    Optional<Subject> getSubject(String uid) throws BulkStorageException;

    /**
     * Checks to see if the subject owns or has access to the request.
     *
     * @param subject   of the user.
     * @param uid to check.
     * @return true if request is accessible by user.
     */
    boolean isRequestSubject(Subject subject, String uid) throws BulkStorageException;

    /**
     * Load the store into memory. (May be a NOP).
     */
    void load() throws BulkStorageException;

    /**
     * @param sortedBy if provided, is the name of the field on which to sort.
     * @param reverse  if true, use reverse of natural ordering.
     * @param limit    the attribute that should be used to order the requests.
     * @return list of requests in the store which are queued to start; these should be ordered by
     * arrival time.
     * @throws BulkStorageException
     */
    List<BulkRequest> next(Optional<String> sortedBy, Optional<Boolean> reverse, long limit)
          throws BulkStorageException;

    /**
     * Reset the request to QUEUED state.
     *
     * @param uid unique id for request.
     * @throws BulkStorageException
     */
    void reset(String uid) throws BulkStorageException;

    /**
     * Store the request.
     *
     * @param subject     sending the request.
     * @param restriction on subject's permissions.
     * @param request     request specifics.
     * @throws BulkStorageException
     */
    void store(Subject subject, Restriction restriction, BulkRequest request)
          throws BulkStorageException;

    /**
     * Update the status of the request.
     *
     * @param uid unique id for request.
     * @param status    QUEUED, STARTED, CANCELLED, COMPLETED.
     * @return true if state changed.
     * @throws BulkStorageException
     */
    boolean update(String uid, BulkRequestStatus status)
          throws BulkStorageException;
}
