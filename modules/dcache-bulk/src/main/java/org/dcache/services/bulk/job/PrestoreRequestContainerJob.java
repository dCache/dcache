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
import static org.dcache.services.bulk.util.BulkRequestTarget.State.CREATED;
import static org.dcache.services.bulk.util.BulkRequestTarget.State.FAILED;
import static org.dcache.services.bulk.util.BulkRequestTarget.State.READY;
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
import javax.ws.rs.HEAD;
import org.dcache.namespace.FileType;
import org.dcache.services.bulk.BulkRequest;
import org.dcache.services.bulk.BulkRequest.Depth;
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
 * This version of the container expands and stores all paths up front, then fetches them from the
 * database and updates them.   This is less efficient than the regular container job, but it allows
 * for full visibility of target states across their entire lifecycle.
 * <p>
 * The biggest bottleneck is the inability to batch requests to the pnfsHandler for attribute
 * information.  This is noticeable when the request is dominated by the size of the target list.
 * Using recursive expansion, for instance, is less costly because the ListHandler is quite
 * efficient at streaming back the child attributes.
 * <p>
 * For this reason, the strategy adopted is to grab all attributes first asynchronously using the
 * semaphore for the activity and put them into an in-memory temporary queue.  After that, they are
 * serially stored (to preserve depth-first ordering), and then asynchronously processed.
 */
public final class PrestoreRequestContainerJob extends AbstractRequestContainerJob {

    private static final Logger LOGGER = LoggerFactory.getLogger(PrestoreRequestContainerJob.class);

    class TargetInfo {

        final Long id;
        final FsPath path;
        final FileAttributes attributes;

        TargetInfo(Long id, FsPath path, FileAttributes attributes) {
            this.id = id;
            this.path = path;
            this.attributes = attributes;
        }
    }

    private final Queue<TargetInfo> targetInfo;
    private final Queue<BulkRequestTarget> readyTargets;

    public PrestoreRequestContainerJob(BulkActivity activity, BulkRequestTarget target,
          BulkRequest request, BulkServiceStatistics statistics) {
        super(activity, target, request, statistics);
        targetInfo = new ConcurrentLinkedQueue<>();
        readyTargets = new ConcurrentLinkedQueue<>();
    }

    @Override
    protected void handleDirTarget(Long id, PID pid, FsPath path, FileAttributes attributes)
          throws InterruptedException {
        checkForRequestCancellation();
        store(id, pid, path, attributes);
    }

    @Override
    protected void handleFileTarget(PID pid, FsPath path, FileAttributes attributes)
          throws InterruptedException {
        checkForRequestCancellation();
        store(null, pid, path, attributes);
    }

    @Override
    protected void preprocessTargets() throws InterruptedException {
        List<BulkRequestTarget> requestTargets = getInitialTargets();

        if (requestTargets.isEmpty()) {
            containerState = ContainerState.STOP;
            update(FAILED);
            return;
        }
        storeAll(requestTargets);
    }

    @Override
    protected void processFileTargets() throws InterruptedException {
        Optional<BulkRequestTarget> target;

        while (true) {
            try {
                checkForRequestCancellation();
                target = next(FileType.REGULAR);
                if (target.isEmpty()) {
                    break;
                }
                perform(target.get());
            } catch (BulkStorageException e) {
                LOGGER.error("run {}, error getting next file target: {}.", ruid, e.toString());
            }
        }

        /*
         *  Make sure LINK types are handled like files, since we allow them
         *  on expansion, but also from initial targets.
         */
        while (true) {
            try {
                checkForRequestCancellation();
                target = next(FileType.LINK);
                if (target.isEmpty()) {
                    break;
                }
                perform(target.get());
            } catch (BulkStorageException e) {
                LOGGER.error("run {}, error getting next file target: {}.", rid, e.toString());
            }
        }
    }

    @Override
    protected void processDirTargets() throws InterruptedException {
        Optional<BulkRequestTarget> target;
        while (true) {
            try {
                checkForRequestCancellation();
                target = next(FileType.DIR);
                if (target.isEmpty()) {
                    break;
                }
                perform(target.get());
            } catch (BulkStorageException e) {
                LOGGER.error("run {}, error getting next directory target: {}.", ruid, e.toString());
            }
        }
    }

    @Override
    protected void retryFailed(BatchedResult result, FileAttributes attributes)
          throws BulkStorageException {
        BulkRequestTarget completedTarget = result.getTarget();
        completedTarget.resetToReady();
        statistics.decrement(completedTarget.getState().name());
        try {
            perform(completedTarget);
        } catch (InterruptedException e) {
            LOGGER.debug("{}. retryFailed interrupted", ruid);
            targetStore.update(result.getTarget().getId(), FAILED, e);
        }
    }

    private void addInfo(BulkRequestTarget target) {
        Long id = target.getId();
        FsPath path = target.getPath();
        if (targetPrefix != null && !path.contains(targetPrefix)) {
            path = computeFsPath(targetPrefix, target.getPath().toString());
        }
        try {
            targetInfo.add(new TargetInfo(id, path, pnfsHandler.getFileAttributes(path,
                  MINIMALLY_REQUIRED_ATTRIBUTES)));
        } catch (CacheException e) {
            LOGGER.error("addInfo {}, path {}, error {}.", ruid, path, e.getMessage());
            target.setState(FAILED);
            target.setErrorObject(e);
            statistics.increment(FAILED.name());
            try {
                targetStore.storeOrUpdate(target);
            } catch (BulkStorageException ex) {
                LOGGER.error("addInfo {}, path {}, could not store, error {}.", ruid, path,
                      ex.getMessage());
            }
        } finally {
            semaphore.release();
        }
    }

    private void handleCompletion(BatchedResult result) {
        activity.handleCompletion(result); /* should release the semaphore */

        BulkRequestTarget completedTarget = result.getTarget();
        State state = completedTarget.getState();
        statistics.decrement(RUNNING.name());

        try {
            if (state == FAILED && activity.getRetryPolicy().shouldRetry(completedTarget)) {
                retryFailed(result, null);
                return;
            }

            targetStore.update(completedTarget.getId(), state, completedTarget.getThrowable());
        } catch (BulkStorageException e) {
            LOGGER.error("{} could not store target from result: {}: {}.", ruid, result.getTarget(),
                  e.toString());
        }

        removeTarget(completedTarget); /* RELEASES SEMAPHORE */

        if (state == FAILED && request.isCancelOnFailure()) {
            cancel();
        }
    }

    private Optional<BulkRequestTarget> next(FileType type) throws BulkStorageException {
        if (readyTargets.isEmpty()) {
            readyTargets.addAll(
                  targetStore.nextReady(rid, type, activity.getMaxPermits()));
        }

        BulkRequestTarget target = readyTargets.poll();
        if (target != null) {
            targetStore.update(target.getId(), READY, null);
        }

        return Optional.ofNullable(target);
    }

    private ListenableFuture perform(BulkRequestTarget target)
          throws InterruptedException {
        checkForRequestCancellation();
        Long id = target.getId();
        FsPath path = target.getPath();
        FileAttributes attributes = target.getAttributes();

        if (hasBeenCancelled(id, target.getPid(), path, attributes)) {
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
            register(target, future, e);
            return future;
        }

        register(target, future, null);
        return future;
    }

    private void register(BulkRequestTarget target, ListenableFuture future, Throwable error)
          throws InterruptedException {
        checkForRequestCancellation();

        if (hasBeenCancelled(target.getId(), target.getPid(), target.getPath(),
              target.getAttributes())) {
            return;
        }

        if (error == null) {
            target.setState(RUNNING);
            try {
                targetStore.update(target.getId(), RUNNING, error);
            } catch (BulkStorageException e) {
                LOGGER.error("{}, could not update target {},: {}.", ruid, target, e.toString());
            }
        } else {
            target.setErrorObject(error);
        }

        BatchedResult result = new BatchedResult(target, future);

        synchronized (waiting) {
            waiting.put(target.getPath(), result);
            future.addListener(() -> handleCompletion(result), activity.getCallbackExecutor());
        }
    }

    private void store(Long id, PID pid, FsPath path, FileAttributes attributes) throws InterruptedException {
        checkForRequestCancellation();
        LOGGER.debug("store {}, path {}.", rid, path);
        try {
            if (hasBeenCancelled(id, pid, path, attributes)) {
                return;
            }
            targetStore.storeOrUpdate(toTarget(id, pid, path, Optional.of(attributes),
                  CREATED, null));
        } catch (BulkStorageException e) {
            LOGGER.error("{}, could not store target {}, {}: {}.", ruid, path, attributes,
                  e.toString());
        }
    }

    private void storeAll(List<BulkRequestTarget> targets) throws InterruptedException {
        for (BulkRequestTarget target : targets) {
            checkForRequestCancellation();
            semaphore.acquire();
            activity.getActivityExecutor().submit(() -> addInfo(target)); /* RELEASES SEMAPHORE */
        }

        /*
         *  Create a barrier and wait for completion of requests for attributes
         *  before storing the targets.
         */
        semaphore.acquire(activity.getMaxPermits());

        /*
         *  For later processing.
         */
        semaphore.release(activity.getMaxPermits());

        if (targetInfo.isEmpty()) {
            containerState = containerState.STOP;
            update(FAILED);
            return;
        }

        /*
         *  This must be done serially to preserve id sequence of directories during expansion.
         */
        for (TargetInfo info : targetInfo) {
            LOGGER.debug("storeAll {}, path {}.", ruid, info.path);
            try {
                if (depth != Depth.NONE && info.attributes.getFileType() == FileType.DIR) {
                    expandDepthFirst(info.id, PID.INITIAL, info.path, info.attributes);
                } else if (info.attributes.getFileType() != FileType.SPECIAL) {
                    store(info.id, PID.INITIAL, info.path, info.attributes);
                }
            } catch (BulkServiceException | CacheException e) {
                LOGGER.error("storeAll {}, path {}, error {}.", ruid, info.path, e.getMessage());
            }
        }

        targetInfo.clear(); /* be nice to GC */
    }
}