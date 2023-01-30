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
package org.dcache.services.bulk.job;

import static org.dcache.services.bulk.activity.BulkActivity.MINIMALLY_REQUIRED_ATTRIBUTES;
import static org.dcache.services.bulk.util.BulkRequestTarget.State.FAILED;
import static org.dcache.services.bulk.util.BulkRequestTarget.State.RUNNING;
import static org.dcache.services.bulk.util.BulkRequestTarget.computeFsPath;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.FsPath;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.dcache.namespace.FileType;
import org.dcache.services.bulk.BulkRequest;
import org.dcache.services.bulk.BulkServiceException;
import org.dcache.services.bulk.BulkStorageException;
import org.dcache.services.bulk.activity.BulkActivity;
import org.dcache.services.bulk.util.BatchedResult;
import org.dcache.services.bulk.util.BulkRequestTarget;
import org.dcache.services.bulk.util.BulkRequestTarget.PID;
import org.dcache.services.bulk.util.BulkRequestTarget.State;
import org.dcache.services.bulk.util.BulkServiceStatistics;
import org.dcache.vehicles.FileAttributes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This version of the container does no preprocessing, storing the targets as they go live. It thus
 * offers a much faster pathway toward target completion with potentially greater throughput.
 */
public final class RequestContainerJob extends AbstractRequestContainerJob {

    private static final Logger LOGGER = LoggerFactory.getLogger(RequestContainerJob.class);

    static class DirTarget {
        final FsPath path;
        final FileAttributes attributes;
        final PID pid;
        final Long id;

        DirTarget(Long id, PID pid, FsPath path, FileAttributes attributes) {
            this.id = id;
            this.pid = pid;
            this.attributes = attributes;
            this.path = path;
        }
    }

    private final Queue<DirTarget> dirs;

    public RequestContainerJob(BulkActivity activity, BulkRequestTarget target,
          BulkRequest request, BulkServiceStatistics statistics) {
        super(activity, target, request, statistics);
        dirs = new ConcurrentLinkedQueue<>();
    }

    @Override
    protected void processFileTargets() throws InterruptedException {
        List<BulkRequestTarget> requestTargets = getInitialTargets();

        if (requestTargets.isEmpty()) {
            containerState = ContainerState.STOP;
            update(FAILED);
            return;
        }

        for (BulkRequestTarget tgt : requestTargets) {
            checkForRequestCancellation();
            Long id = tgt.getId();
            FsPath path = tgt.getPath();
            if (targetPrefix != null && !path.contains(targetPrefix)) {
                path = computeFsPath(targetPrefix, tgt.getPath().toString());
            }

            switch (depth) {
                case NONE:
                    perform(id, PID.INITIAL, path, null);
                    break;
                default:
                    handleTarget(id, PID.INITIAL, path);
            }
        }
    }

    @Override
    protected void processDirTargets() throws InterruptedException {
        for (DirTarget dirTarget : dirs) {
            checkForRequestCancellation();
            perform(dirTarget.id, dirTarget.pid, dirTarget.path, dirTarget.attributes);
        }
    }

    @Override
    protected void handleDirTarget(Long id, PID pid, FsPath path, FileAttributes attributes) {
        dirs.add(new DirTarget(id, pid, path, attributes));
    }

    @Override
    protected void handleFileTarget(PID pid, FsPath path, FileAttributes attributes)
          throws InterruptedException {
        perform(null, pid, path, attributes);
    }

    @Override
    protected void retryFailed(BatchedResult result, FileAttributes attributes)
          throws BulkStorageException {
        BulkRequestTarget completedTarget = result.getTarget();
        Long id = completedTarget.getId();
        FsPath path = completedTarget.getPath();
        PID pid = completedTarget.getPid();
        completedTarget.resetToReady();
        statistics.decrement(completedTarget.getState().name());
        try {
            perform(id, pid, path, attributes);
        } catch (InterruptedException e) {
            LOGGER.debug("{}. retryFailed interrupted", ruid);
            targetStore.update(result.getTarget().getId(), FAILED, e);
        }
    }

    private void handleCompletion(BatchedResult result, FileAttributes attributes) {
        activity.handleCompletion(result);

        BulkRequestTarget completedTarget = result.getTarget();
        State state = completedTarget.getState();
        statistics.decrement(RUNNING.name());

        try {
            if (state == FAILED && activity.getRetryPolicy().shouldRetry(completedTarget)) {
                retryFailed(result, attributes);
                return;
            }

            targetStore.update(completedTarget.getId(), state, completedTarget.getThrowable());
        } catch (BulkStorageException e) {
            LOGGER.error("{} could not store target from result: {}, {}: {}.", ruid, result,
                  attributes, e.toString());
        }

        removeTarget(completedTarget); /* RELEASES SEMAPHORE */

        if (state == FAILED && request.isCancelOnFailure()) {
            cancel();
        }
    }

    private void handleTarget(Long id, PID pid, FsPath path) throws InterruptedException {
        checkForRequestCancellation();
        FileAttributes attributes = null;
        LOGGER.debug("handleTarget {}, path {}.", ruid, path);
        try {
            attributes = pnfsHandler.getFileAttributes(path, MINIMALLY_REQUIRED_ATTRIBUTES);
            if (attributes.getFileType() == FileType.DIR) {
                expandDepthFirst(id, pid, path, attributes);
            } else if (attributes.getFileType() != FileType.SPECIAL) {
                perform(id, pid, path, attributes);
            }
        } catch (BulkServiceException | CacheException e) {
            LOGGER.error("handleTarget {}, path {}, error {}.", ruid, path, e.getMessage());
            register(id, pid, path, Futures.immediateFailedFuture(e), attributes, e);
        }
    }

    private ListenableFuture perform(Long id, PID pid, FsPath path, FileAttributes attributes)
          throws InterruptedException {
        checkForRequestCancellation();

        if (hasBeenCancelled(id, pid, path, attributes)) {
            return Futures.immediateCancelledFuture();
        }

        semaphore.acquire();

        ListenableFuture future;
        try {
            future = activity.perform(ruid, id == null ? this.id.getAndIncrement() : id, path,
                  attributes);
        } catch (BulkServiceException | UnsupportedOperationException e) {
            LOGGER.error("{}, perform failed for {}: {}", ruid, path, e.getMessage());
            future = Futures.immediateFailedFuture(e);
            register(id, pid, path, future, attributes, e);
            return future;
        }

        register(id, pid, path, future, attributes, null);
        return future;
    }

    private void register(Long id, PID pid, FsPath path, ListenableFuture future, FileAttributes attributes,
          Throwable error) throws InterruptedException {
        checkForRequestCancellation();

        if (hasBeenCancelled(id, pid, path, attributes)) {
            return;
        }

        BulkRequestTarget target = toTarget(id, pid, path, Optional.ofNullable(attributes),
              error == null ? RUNNING : FAILED, error);

        BatchedResult result = new BatchedResult(target, future);

        if (error == null) {
            try {
                /*
                 *  If this is an insert (id == null), the target id will be updated to what is
                 *  returned from the database.
                 */
                targetStore.storeOrUpdate(target);
                statistics.increment(RUNNING.name());
            } catch (BulkStorageException e) {
                LOGGER.error("{}, could not store target from result {}, {}, {}: {}.", ruid, result,
                      attributes, e.toString());
            }
        }

        synchronized (waiting) {
            waiting.put(path, result);
            future.addListener(() -> handleCompletion(result, attributes),
                  activity.getCallbackExecutor());
        }
    }
}