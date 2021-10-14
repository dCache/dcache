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
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;
import org.dcache.services.bulk.BulkJobStorageException;
import org.dcache.services.bulk.BulkStorageException;
import org.dcache.services.bulk.job.BulkJob;
import org.dcache.services.bulk.job.BulkJob.State;
import org.dcache.services.bulk.job.BulkJobKey;

public interface BulkJobStore {

    /**
     * @param requestId of the jobs to cancel.
     * @throws BulkJobStorageException
     */
    void cancelAll(String requestId)
          throws BulkJobStorageException;

    /**
     * @param key unique key of job to remove.
     * @throws BulkStorageException
     */
    void delete(BulkJobKey key) throws BulkJobStorageException;

    /**
     * @param filter on the job.
     * @param limit  max jobs to return (can be <code>null</code>).
     * @return a collection of jobs in the store which match the filter.
     * @throws BulkJobStorageException
     */
    Collection<BulkJob> find(Predicate<BulkJob> filter, Long limit)
          throws BulkJobStorageException;

    /**
     * @param key unique key of the job.
     * @return optional of the corresponding job.
     * @throws BulkJobStorageException
     */
    Optional<BulkJob> getJob(BulkJobKey key) throws BulkJobStorageException;

    /**
     * @return sorted list of all keys in the store.
     */
    List<BulkJobKey> keys() throws BulkJobStorageException;

    /**
     * Store the job.
     *
     * @param job to store.
     * @throws BulkJobStorageException
     */
    void store(BulkJob job) throws BulkJobStorageException;

    /**
     * Update the status of the job.
     *
     * @param key       unique key of the job.
     * @param status    CREATED, INITIALIZED, STARTED, CANCELLED, COMPLETED, FAILED.
     * @param exception if failed.
     * @throws BulkJobStorageException
     */
    void update(BulkJobKey key, State status, Throwable exception)
          throws BulkJobStorageException;
}
