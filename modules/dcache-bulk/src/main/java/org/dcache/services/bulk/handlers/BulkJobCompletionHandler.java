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
package org.dcache.services.bulk.handlers;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import java.util.concurrent.TimeUnit;
import javax.annotation.concurrent.GuardedBy;
import org.dcache.services.bulk.job.BulkJob;
import org.dcache.services.bulk.queue.SignalAware;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages the set of jobs associated with a request.
 * <p>
 * Responsible for determining if all jobs have terminated.
 * <p>
 * Provides special #waitForChildren method for creating a directory barrier on asynchronous
 * descendants when required for depth-first expansion.
 */
public class BulkJobCompletionHandler {

    private static final Logger LOGGER
          = LoggerFactory.getLogger(BulkJobCompletionHandler.class);
    private static final Long MARKER = -1L;

    /**
     * Node dependency or edge table (parent, child) which tracks how many jobs of the request are
     * still outstanding, and is used to create a barrier on all child jobs of a given directory
     * during asynchronous depth-first operations.
     * <p>
     * Request termination is when the table is empty.
     */
    private final Multimap<Long, Long> descendants;

    /**
     * The queue implements this interface in order for it to be notified that a job or task has
     * completed.
     */
    private final SignalAware queue;

    public BulkJobCompletionHandler(SignalAware queue) {
        this.queue = queue;
        descendants = HashMultimap.create();
    }

    /**
     * When a job is submitted, it adds itself as the child of a parent job.  When it terminates, it
     * removes itself from the parent list.  The multimap implementation automatically removes the
     * parent key from the table when its child collection is empty.
     * <p>
     * Breadth-first directories do not remove themselves from the map until all their children are
     * registered.
     * <p>
     * Depth-first directories wait for their immediate children to terminate.  Subdirectories of
     * the root expansion node do not add themselves as children, since the expansion is done by
     * recursion rather than exec'ing a new job (as in breadth-first).
     */
    public void addChild(BulkJob job) {
        Preconditions.checkArgument(job.getParentKey().getRequestId()
                    .equals(job.getKey().getRequestId()),
              "Job completion listener is "
                    + "being shared between two "
                    + "different requests! "
                    + "This is a bug.");

        synchronized (descendants) {
            Long parentId = job.getParentKey().getJobId();
            Long childId = job.getKey().getJobId();
            descendants.put(parentId, childId);
            LOGGER.trace("addChild: parent {}, child {}; descendants {}.",
                  parentId, childId, descendants.size());
        }
    }

    /**
     * In the case of cancellation, all jobs are cleared.
     */
    public void clear() {
        LOGGER.trace("clear called with descendants {}.", descendants.size());
        synchronized (descendants) {
            descendants.clear();
            LOGGER.trace("clear, descendants now {}.", descendants.size());
        }
    }

    public boolean isRequestCompleted() {
        synchronized (descendants) {
            boolean completed = descendants.isEmpty();
            LOGGER.trace("request completed? descendants {}.",
                  descendants.size());
            return completed;
        }
    }

    public void jobCancelled(BulkJob job) {
        jobTerminated("Job cancelled", job);
    }

    public void jobCompleted(BulkJob job) {
        jobTerminated("Job completed", job);
    }

    public void jobFailed(BulkJob job) {
        jobTerminated("Job failed", job);
    }

    public void jobInterrupted(BulkJob job) {
        jobTerminated("Job interrupted", job);
    }

    /**
     * Called by the BulkRequestJob before exiting.
     */
    public void requestProcessingFinished(Long requestJobId) {
        synchronized (descendants) {
            descendants.remove(requestJobId, MARKER);
        }
    }

    /**
     * Called by the BulkRequestJob when it begins to run.
     * <p>
     * The top-level job registers a MARKER as child and removes it when it has finished submitting
     * all of its targets.  This is to prevent premature handling of request completion, should the
     * targets it has submitted complete before it has finished submitting all targets.
     */
    public void requestProcessingStarted(Long requestJobId) {
        synchronized (descendants) {
            descendants.put(requestJobId, MARKER);
        }
    }

    public void signalWaiting() {
        queue.signal();
    }

    /**
     * Used during depth-first expansions.
     */
    public void waitForChildren(Long parentId) throws InterruptedException {
        synchronized (descendants) {
            LOGGER.trace("waitForChildren of {} called; descendants {}.",
                  parentId, descendants.size());
            while (!areChildrenAllTerminated(parentId)) {
                LOGGER.trace("waitForChildren of {}; descendants {}, waiting ....",
                      parentId, descendants.size());
                descendants.wait(TimeUnit.SECONDS.toMillis(1));
            }

            LOGGER.trace("waitForChildren of {}, returning: descendants {}.",
                  parentId, descendants.size());
        }
    }

    @GuardedBy("descendants")
    @VisibleForTesting
    boolean areChildrenAllTerminated(Long parentId) {
        return descendants.isEmpty() || descendants.get(parentId).isEmpty();
    }

    private void jobTerminated(String how, BulkJob job) {
        LOGGER.trace("{} called for {}", how, job.getKey());
        if (job.getErrorObject() != null) {
            LOGGER.debug("{} terminated with exception: {}.",
                  job.getKey(),
                  job.getErrorObject().getMessage());
        }

        synchronized (descendants) {
            LOGGER.trace("removing job {} from parent {}",
                  job.getKey(),
                  job.getParentKey().getJobId());
            Long parentId = job.getParentKey().getJobId();
            Long childId = job.getKey().getJobId();

            descendants.remove(parentId, childId);

            LOGGER.trace("parent {} now has children {}",
                  parentId,
                  descendants.get(parentId));

            descendants.notifyAll();
        }

        queue.signal();
    }
}
