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

import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.dcache.namespace.FileType;
import org.dcache.services.bulk.BulkStorageException;
import org.dcache.services.bulk.util.BulkRequestTarget;
import org.dcache.services.bulk.util.BulkRequestTarget.State;
import org.dcache.services.bulk.util.BulkTargetFilter;

/**
 * Provides a higher-level programmatic wrapper around calls to the underlying target DAO.
 */
public interface BulkTargetStore {

    /**
     * Update the request target record.  Should not increment any completion count as this target
     * has not been added since it failed prematurely.
     *
     * @param target     belonging to the request.
     * @throws BulkStorageException
     */
    void abort(BulkRequestTarget target) throws BulkStorageException;

    /**
     * Cancel a single target.
     *
     * @param id of the target.
     */
    void cancel(long id);

    /**
     * Set all non-terminated targets to CANCELLED.
     *
     * @param rid of this request.
     */
    void cancelAll(Long rid);

    /**
     * @param filter to match.
     * @return number of matching entries.
     */
    long count(BulkTargetFilter filter);

    /**
     * @param rid of this request.
     * @return the number of unprocessed targets.
     */
    int countUnprocessed(Long rid) throws BulkStorageException;

    /**
     * @param rid of this request.
     * @return the number of unprocessed jobs.
     */
    int countFailed(Long rid) throws BulkStorageException;

    /**
     * @param filter     on targets to be included in the count.
     * @param excludeRoot whether to exclude the placeholder request targets from the count.
     * @param classifier to use for grouping.
     * @return a map of the combined results of target counts grouped by classifier.
     */
    Map<String, Long> counts(BulkTargetFilter filter, boolean excludeRoot, String classifier);

    /**
     * @return a map of the combined results of target counts grouped by state.
     */
    Map<String, Long> countsByState();

    /**
     * @param filter on the target.
     * @param limit  max targets to return (can be <code>null</code>).
     * @return a list of targets which match the filter.
     * @throws BulkStorageException
     */
    List<BulkRequestTarget> find(BulkTargetFilter filter, Integer limit)
          throws BulkStorageException;

    /**
     * @param rid of the request the targets belong to.
     * @param nonterminal only the initial targets which have not yet run.
     * @return paths of the targets
     */
    List<BulkRequestTarget> getInitialTargets(Long rid, boolean nonterminal);

    /**
     * @param type  REGULAR or DIR.
     * @param limit max targets to return (can be <code>null</code>).
     * @return a list of targets which are ready to run.
     * @throws BulkStorageException
     */
    List<BulkRequestTarget> nextReady(Long rid, FileType type, Integer limit)
          throws BulkStorageException;

    /**
     * @param id of the target.
     * @return optional of the corresponding target.
     * @throws BulkStorageException
     */
    Optional<BulkRequestTarget> getTarget(long id) throws BulkStorageException;

    /**
     * Store the target.
     *
     * @param target to store.
     * @return true if the target is stored by this call, false if not.
     * @throws BulkStorageException
     */
     boolean store(BulkRequestTarget target) throws BulkStorageException;

    /**
     * Store or update the target if it already exists.
     *
     * @param target to store.
     * @throws BulkStorageException
     */
    void storeOrUpdate(BulkRequestTarget target) throws BulkStorageException;

    /**
     * Update the status of the target.
     *
     * @param id
     * @param state
     * @param errorObject
     * @throws BulkStorageException
     */
    void update(Long id, State state, Throwable errorObject) throws BulkStorageException;
}
