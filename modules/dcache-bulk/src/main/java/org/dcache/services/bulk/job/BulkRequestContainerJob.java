/*
COPYRIGHT STATUS:
Dec 1st 2001, Fermi National Accelerator Laboratory (FNAL) documents and
software are sponsored by the U.S. Department of Energy under Contract No.
DE-AC02-76CH03000. Therefore, the U.S. Government retains a  world-wide
non-exclusive, royalty-free license to publish or reproduce these documents
and software for U.S. Government purposes.  All documents and software
available from this server are private under the U.S. and Foreign
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
import static org.dcache.services.bulk.util.BulkRequestTarget.State.CANCELLED;
import static org.dcache.services.bulk.util.BulkRequestTarget.State.COMPLETED;
import static org.dcache.services.bulk.util.BulkRequestTarget.State.CREATED;
import static org.dcache.services.bulk.util.BulkRequestTarget.State.FAILED;
import static org.dcache.services.bulk.util.BulkRequestTarget.State.READY;
import static org.dcache.services.bulk.util.BulkRequestTarget.State.RUNNING;
import static org.dcache.services.bulk.util.BulkRequestTarget.State.SKIPPED;
import static org.dcache.services.bulk.util.BulkRequestTarget.computeFsPath;

import com.google.common.base.Throwables;
import com.google.common.collect.Range;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.FsPath;
import diskCacheV111.util.NamespaceHandlerAware;
import diskCacheV111.util.PnfsHandler;
import java.lang.Thread.UncaughtExceptionHandler;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import javax.security.auth.Subject;
import javax.ws.rs.HEAD;
import org.dcache.auth.attributes.Restriction;
import org.dcache.cells.AbstractMessageCallback;
import org.dcache.cells.CellStub;
import org.dcache.namespace.FileType;
import org.dcache.services.bulk.BulkRequest;
import org.dcache.services.bulk.BulkRequest.Depth;
import org.dcache.services.bulk.BulkServiceException;
import org.dcache.services.bulk.BulkStorageException;
import org.dcache.services.bulk.activity.BulkActivity;
import org.dcache.services.bulk.activity.BulkActivity.TargetType;
import org.dcache.services.bulk.store.BulkTargetStore;
import org.dcache.services.bulk.util.BulkRequestTarget;
import org.dcache.services.bulk.util.BulkRequestTarget.PID;
import org.dcache.services.bulk.util.BulkRequestTarget.State;
import org.dcache.services.bulk.util.BulkRequestTargetBuilder;
import org.dcache.services.bulk.util.BulkServiceStatistics;
import org.dcache.util.CacheExceptionFactory;
import org.dcache.util.SignalAware;
import org.dcache.util.list.DirectoryEntry;
import org.dcache.util.list.DirectoryStream;
import org.dcache.util.list.ListDirectoryHandler;
import org.dcache.vehicles.FileAttributes;
import org.dcache.vehicles.PnfsGetFileAttributes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Container job for a list of targets which may or may not be associated with each other via a
 * common parent. It handles all file targets asynchronously, recurs if directory listing is
 * enabled, and processes directory targets serially last in depth-first reverse order.
 */
public final class BulkRequestContainerJob
      implements Runnable, NamespaceHandlerAware, Comparable<BulkRequestContainerJob>,
      UncaughtExceptionHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(BulkRequestContainerJob.class);

    private static final DirTargetSorter SORTER = new DirTargetSorter();

    static final AtomicLong taskCounter = new AtomicLong(0L);

    public static FsPath findAbsolutePath(String prefix, String path) {
        return computeFsPath(prefix, path);
    }

    /**
     * Directories that serve as targets. These are stored in memory, sorted and processed last.
     */
    static class DirTarget {

        final FsPath path;
        final FileAttributes attributes;
        final PID pid;
        final Long id;
        final int depth;

        DirTarget(Long id, PID pid, FsPath path, FileAttributes attributes) {
            this.id = id;
            this.pid = pid;
            this.attributes = attributes;
            this.path = path;
            depth = (int) path.toString().chars().filter(c -> c == '/').count();
        }
    }

    /**
     * Depth-first (descending order).
     */
    static class DirTargetSorter implements Comparator<DirTarget> {

        @Override
        public int compare(DirTarget o1, DirTarget o2) {
            return Integer.compare(o2.depth, o1.depth); /* DESCENDING ORDER */
        }
    }

    /**
     * Encapsulates manipulation of the semaphore.
     */
    class PermitHolder {

        private final AtomicBoolean holdingPermit = new AtomicBoolean(false);
        private Semaphore taskSemaphore;

        void acquireIfNotHoldingPermit() throws InterruptedException {
            if (taskSemaphore == null) {
                return;
            }

            if (holdingPermit.compareAndSet(false, true)) {
                taskSemaphore.acquire();
            }
        }

        void throttledRelease() {
            activity.throttle();
            releaseIfHoldingPermit();
        }

        void releaseIfHoldingPermit() {
            if (taskSemaphore == null) {
                return;
            }

            if (holdingPermit.compareAndSet(true, false)) {
                taskSemaphore.release();
            }
        }

        void setTaskSemaphore(Semaphore taskSemaphore) {
            this.taskSemaphore = taskSemaphore;
        }
    }

    /**
     * Container delays processing directory targets until the final step.
     */
    enum ContainerState {
        START, PROCESS_FILES, WAIT, PROCESS_DIRS, STOP
    }

    /**
     * Only INITIAL targets go through all three states. DISCOVERED targets already have their
     * proper paths and attributes from listing.
     */
    enum TaskState {
        FETCH_ATTRIBUTES, HANDLE_TARGET, HANDLE_DIR_TARGET
    }

    /**
     * Wrapper task for directory listing and target processing.
     */
    abstract class ContainerTask implements Runnable {

        final long seqNo = taskCounter.getAndIncrement();
        final PermitHolder permitHolder = new PermitHolder();

        Future taskFuture;
        ExecutorService taskExecutor;

        public void run() {
            try {
                doInner();
            } catch (RuntimeException e) {
                uncaughtException(Thread.currentThread(), e);
            }
        }

        void cancel() {
            if (taskFuture != null) {
                taskFuture.cancel(true);
                LOGGER.debug("{} - task future cancelled {}.", ruid, seqNo);
            }

            remove();
        }

        void expandDepthFirst(Long id, PID pid, FsPath path, FileAttributes dirAttributes)
              throws BulkServiceException, CacheException {
            LOGGER.debug("{} - expandDepthFirst, {}, {}, {}, {}", ruid, id, pid, path,
                  dirAttributes);
            try {
                new DirListTask(id, pid, path, dirAttributes).submitAsync();
            } catch (InterruptedException e) {
                LOGGER.trace("{} - expandDepthFirst {} interrupted.", ruid, id);
            }
        }

        void handleException(Throwable e) {
            remove();
            if (e instanceof InterruptedException) {
                containerState = ContainerState.STOP;
                jobTarget.setErrorObject(e);
                update(CANCELLED);
            } else {
                uncaughtException(Thread.currentThread(), e);
                Throwables.throwIfUnchecked(e);
            }
        }

        void submitAsync() throws InterruptedException {
            /*
             * Acquisition must be done outside the synchronized block (running),
             * else there could be a deadlock.
             */
            permitHolder.acquireIfNotHoldingPermit();

            synchronized (running) {
                if (jobTarget.isTerminated()) {
                    permitHolder.releaseIfHoldingPermit();
                    return;
                }

                running.put(seqNo, this);
                LOGGER.debug("{} - submitAsync {}, task count is now {}.", ruid, seqNo,
                      running.size());

                taskFuture = taskExecutor.submit(this);
            }
        }

        void remove() {
            permitHolder.releaseIfHoldingPermit();

            synchronized (running) {
                running.remove(seqNo);
                LOGGER.debug("{} - remove task {}, task count now {}.", ruid, seqNo,
                      running.size());

                if (running.isEmpty()) {
                    checkTransitionToDirs();
                }
            }
        }

        abstract void doInner();
    }

    class DirListTask extends ContainerTask {

        final Long id;
        final PID pid;
        final FsPath path;
        final FileAttributes dirAttributes;

        DirListTask(Long id, PID pid, FsPath path, FileAttributes dirAttributes) {
            this.id = id;
            this.pid = pid;
            this.path = path;
            this.dirAttributes = dirAttributes;
            taskExecutor = listExecutor;
            permitHolder.setTaskSemaphore(dirListSemaphore);
        }

        void doInner() {
            try {
                checkForRequestCancellation();
                DirectoryStream stream = getDirectoryListing(path);
                for (DirectoryEntry entry : stream) {
                    checkForRequestCancellation();
                    LOGGER.debug("{} - DirListTask, directory {}, entry {}", ruid, path,
                          entry.getName());
                    FsPath childPath = path.child(entry.getName());
                    FileAttributes childAttributes = entry.getFileAttributes();

                    switch (childAttributes.getFileType()) {
                        case DIR:
                            switch (depth) {
                                case ALL:
                                    expandDepthFirst(null, PID.DISCOVERED, childPath,
                                          childAttributes);
                                    break;
                                case TARGETS:
                                    switch (targetType) {
                                        case BOTH:
                                        case DIR:
                                            addDirTarget(null, PID.DISCOVERED, childPath,
                                                  childAttributes);
                                    }
                                    break;
                            }
                            break;
                        case LINK:
                        case REGULAR:
                            new TargetTask(
                                  toTarget(null, PID.DISCOVERED, childPath,
                                        Optional.of(childAttributes), CREATED, null),
                                  TaskState.HANDLE_TARGET).submitAsync();
                            break;
                        case SPECIAL:
                        default:
                            LOGGER.trace("{} - DirListTask, cannot handle special file {}.",
                                  ruid, childPath);
                            break;
                    }
                }

                checkForRequestCancellation();
                switch (targetType) {
                    case BOTH:
                    case DIR:
                        addDirTarget(id, pid, path, dirAttributes);
                        break;
                    case FILE:
                        /*
                         * Because we now store all initial targets immediately,
                         * we need to mark such a directory as SKIPPED; otherwise
                         * the request will not complete on the basis of querying for
                         * completed targets and finding this one unhandled.
                         */
                        if (pid == PID.INITIAL) {
                            targetStore.storeOrUpdate(
                                  toTarget(id, pid, path, Optional.of(dirAttributes),
                                        SKIPPED, null));
                        }
                }
            } catch (InterruptedException e) {
                /*
                 *  Cancelled.  Do nothing.
                 */
                permitHolder.releaseIfHoldingPermit();
            } catch (BulkServiceException | CacheException e) {
                /*
                 *  Fail fast
                 */
                containerState = ContainerState.STOP;
                jobTarget.setErrorObject(e);
                update();
            } finally {
                remove();
            }
        }

        private void addDirTarget(Long id, PID pid, FsPath path, FileAttributes attributes) {
            LOGGER.debug("{} - DirListTask, addDirTarget, adding directory {} ...", ruid, path);
            dirs.add(new DirTarget(id, pid, path, attributes));
        }

        private DirectoryStream getDirectoryListing(FsPath path)
              throws CacheException, InterruptedException {
            LOGGER.debug("{} - DirListTask, getDirectoryListing for path {}, calling list ...",
                  ruid, path);
            return listHandler.list(subject, restriction, path, null,
                  Range.closedOpen(0, Integer.MAX_VALUE), MINIMALLY_REQUIRED_ATTRIBUTES);
        }
    }

    class TargetTask extends ContainerTask {

        final BulkRequestTarget target;

        /*
         * From activity.perform()
         */
        ListenableFuture activityFuture;

        /*
         * Determines the doInner() switch
         */
        TaskState state;

        TargetTask(BulkRequestTarget target, TaskState initialState) {
            this.target = target;
            state = initialState;
            taskExecutor = BulkRequestContainerJob.this.executor;
            permitHolder.setTaskSemaphore(inFlightSemaphore);
        }

        void cancel() {
            if (activityFuture != null) {
                activityFuture.cancel(true);
                LOGGER.debug("{} - activity future cancelled for task {}.", ruid, seqNo);
            }

            if (target != null) {
                activity.cancel(targetPrefix, target);
                LOGGER.debug("{} - target cancelled for task {}.", ruid, seqNo);
            }

            super.cancel();
        }

        @Override
        void doInner() {
            try {
                checkForRequestCancellation();
                switch (state) {
                    case FETCH_ATTRIBUTES:
                        fetchAttributes();
                        break;
                    case HANDLE_DIR_TARGET:
                        performActivity();
                        break;
                    case HANDLE_TARGET:
                    default:
                        switch (depth) {
                            case NONE:
                                performActivity();
                                break;
                            default:
                                handleTarget();
                        }
                        break;
                }
            } catch (InterruptedException e) {
                /*
                 *  Cancellation.  Do nothing.
                 */
                permitHolder.releaseIfHoldingPermit();
            } catch (RuntimeException e) {
                target.setErrorObject(e);
                if (activityFuture == null) {
                    activityFuture = Futures.immediateFailedFuture(Throwables.getRootCause(e));
                }
                handleCompletion();
                uncaughtException(Thread.currentThread(), e);
            }
        }

        void handleException(Throwable e) {
            target.setErrorObject(e);
            if (activityFuture == null) {
                activityFuture = Futures.immediateFailedFuture(Throwables.getRootCause(e));
            }
            handleCompletion();
            super.handleException(e);
        }

        void performSync() throws InterruptedException {
            performActivity(false);

            try {
                activityFuture.get();
            } catch (ExecutionException e) {
                activityFuture = Futures.immediateFailedFuture(e.getCause());
            }

            handleCompletion();
        }

        /**
         * (1) retrieval of required file attributes.
         */
        private void fetchAttributes() {
            FsPath absolutePath = findAbsolutePath(targetPrefix,
                                                   target.getPath().toString());
            LOGGER.debug("{} - fetchAttributes for path {}, prefix {}, absolute path {} ", ruid, target.getPath(), targetPrefix, absolutePath);


            PnfsGetFileAttributes message = new PnfsGetFileAttributes(absolutePath.toString(),
                  MINIMALLY_REQUIRED_ATTRIBUTES);
            ListenableFuture<PnfsGetFileAttributes> requestFuture = pnfsHandler.requestAsync(
                  message);
            CellStub.addCallback(requestFuture, new AbstractMessageCallback<>() {
                @Override
                public void success(PnfsGetFileAttributes message) {
                    LOGGER.debug("{} - fetchAttributes for path {}, callback success.",
                          ruid, target.getPath());
                    FileAttributes attributes = message.getFileAttributes();
                    target.setAttributes(attributes);
                    state = TaskState.HANDLE_TARGET;
                    taskFuture = executor.submit(TargetTask.this);
                }

                @Override
                public void failure(int rc, Object error) {
                    LOGGER.error("{} - fetchAttributes, callback failure for {}.", ruid, target);
                    storeOrUpdate(CacheExceptionFactory.exceptionOf(
                          rc, Objects.toString(error, null)));
                }
            }, callbackExecutor);
        }

        /**
         * (2b) either recurs on directory or performs activity on file.
         */
        private void handleTarget() throws InterruptedException {
            LOGGER.debug("{} - handleTarget for {}, path {}.", ruid, target.getActivity(),
                  target.getPath());
            FileAttributes attributes = target.getAttributes();
            FileType type = attributes.getFileType();
            try {
                if (type == FileType.DIR) {
                    storeOrUpdate(null);
                    expandDepthFirst(target.getId(), target.getPid(), target.getPath(), attributes);
                    /*
                     * Swap out for the directory listing task.
                     * (We must do this AFTER the directory task has been added to running.)
                     */
                    remove();
                } else if (type != FileType.SPECIAL) {
                    performActivity();
                }
            } catch (BulkServiceException | CacheException e) {
                LOGGER.error("handleTarget {}, path {}, error {}.", ruid, target.getPath(),
                      e.getMessage());
                storeOrUpdate(e);
            }
        }

        /**
         * (3a) Performs activity on either file or directory target.
         */
        private void performActivity() throws InterruptedException {
            performActivity(true);
        }

        private void performActivity(boolean async) throws InterruptedException {
            Long id = target.getId();
            FsPath path = target.getPath();
            FileAttributes attributes = target.getAttributes();
            LOGGER.debug("{} - performActivity {} on {}.", ruid, activity, path);
            storeOrUpdate(null);

            if (hasBeenSpecificallyCancelled(this)) {
                LOGGER.debug("{} - performActivity hasBeenSpecificallyCancelled for {}.", ruid,
                             path);
                remove();
            }

            try {
                activityFuture = activity.perform(ruid,
                                                  id == null ? seqNo : id,
                                                  targetPrefix,
                                                  path,
                                                  attributes);
                if (async) {
                    activityFuture.addListener(() -> handleCompletion(), callbackExecutor);
                    permitHolder.throttledRelease();
                }
            } catch (BulkServiceException | UnsupportedOperationException e) {
                LOGGER.error("{}, perform failed for {}: {}", ruid, target, e.getMessage());
                activityFuture = Futures.immediateFailedFuture(e);
                if (async) {
                    handleCompletion();
                }
            }
        }

        private void handleCompletion() {
            LOGGER.debug("{} - handleCompletion {}", ruid, target.getPath());

            State state = RUNNING;
            try {
                checkForRequestCancellation();
                activity.handleCompletion(target, activityFuture);
                state = target.getState();

                if (state == FAILED && activity.getRetryPolicy().shouldRetry(target)) {
                    retryFailed();
                    return;
                }

                targetStore.update(target.getId(), state, target.getErrorType(),
                      target.getErrorMessage());
            } catch (BulkStorageException e) {
                LOGGER.error("{}, could not store target from result {}, {}, {}: {}.", ruid,
                      target.getId(), target.getPath(), target.getAttributes(), e.toString());
            } catch (InterruptedException e) {
                /*
                 *  Cancelled.  Do nothing.
                 */
                return;
            }

            if (state == FAILED && request.isCancelOnFailure() && !jobTarget.isTerminated()) {
                BulkRequestContainerJob.this.cancel();
            } else {
                remove();
            }
        }

        private void retryFailed() throws BulkStorageException {
            LOGGER.debug("{} - retryFailed {}.", ruid, target);
            target.resetToReady();
            try {
                performActivity();
            } catch (InterruptedException e) {
                LOGGER.debug("{}. retryFailed {}, interrupted.", ruid, target);
                activityFuture = Futures.immediateFailedFuture(e);
                handleCompletion();
            }
        }

        private void storeOrUpdate(Throwable error) {
            LOGGER.debug("{} - storeOrUpdate {}.", ruid, target);

            if (hasBeenSpecificallyCancelled(this)) {
                LOGGER.debug("{} - storeOrUpdate, hasBeenSpecificallyCancelled {}.", ruid,
                      target.getPath());
                return;
            }

            if (jobTarget.isTerminated()) {
                error = new InterruptedException();
            }

            if (error == null) {
                target.setState(RUNNING);
            } else {
                target.setErrorObject(error);
            }

            try {
                /*
                 * If this is an insert (id == null), the target id will be updated to what is
                 * returned from the database.
                 */
                targetStore.storeOrUpdate(target);
                LOGGER.debug("{} - storeOrUpdate, target id {}", ruid, target.getId());
            } catch (BulkStorageException e) {
                LOGGER.error("{}, could not store or update target from result {}, {}, {}: {}.",
                      ruid,
                      target.getId(), target.getPath(), target.getAttributes(), e.toString());
                error = e;
            }

            if (error != null) {
                remove();
            }
        }
    }

    private final BulkRequest request;
    private final BulkActivity activity;
    private final Long rid;
    private final String ruid;
    private final Depth depth;
    private final String targetPrefix;
    private final BulkServiceStatistics statistics;
    private final TargetType targetType;
    private final BulkRequestTarget jobTarget;
    private final Subject subject;
    private final Restriction restriction;

    private final Map<Long, ContainerTask> running;
    private final Set<FsPath> cancelledPaths;
    private final Queue<DirTarget> dirs;

    private BulkTargetStore targetStore;
    private PnfsHandler pnfsHandler;
    private ListDirectoryHandler listHandler;
    private SignalAware callback;
    private Thread runThread;
    private ExecutorService executor;
    private ExecutorService listExecutor;
    private ExecutorService callbackExecutor;
    private Semaphore dirListSemaphore;
    private Semaphore inFlightSemaphore;

    private volatile ContainerState containerState;

    public BulkRequestContainerJob(BulkActivity activity, BulkRequestTarget jobTarget,
          BulkRequest request, BulkServiceStatistics statistics) {
        this.request = request;
        this.activity = activity;
        this.jobTarget = jobTarget;
        this.subject = activity.getSubject();
        this.restriction = activity.getRestriction();
        this.statistics = statistics;

        rid = request.getId();
        ruid = request.getUid();
        depth = request.getExpandDirectories();
        targetPrefix = request.getTargetPrefix();
        targetType = activity.getTargetType();

        running = new HashMap<>();
        cancelledPaths = new HashSet<>();
        dirs = new ConcurrentLinkedQueue<>();

        containerState = ContainerState.START;
    }

    public void cancel() {
        interruptRunThread();

        /*
         * Thread may already have exited.
         *
         * Update terminates job target.
         */
        containerState = ContainerState.STOP;
        update(CANCELLED);
        targetStore.cancelAll(rid);

        LOGGER.debug("{} - cancel, running {}.", ruid, running.size());

        int count = 0;

        /*
         * Drain running tasks.
         */
        while (true) {
            ContainerTask task;
            synchronized (running) {
                if (running.isEmpty()) {
                    break;
                }

                task = running.values().iterator().next();
            }

            task.cancel(); // removes the task
            ++count;
        }

        LOGGER.trace("{} - cancel: {} tasks cancelled; running size: {}.", ruid, count,
              running.size());

        signalStateChange();
    }

    public void cancel(long targetId) {
        synchronized (running) {
            for (Iterator<ContainerTask> i = running.values().iterator(); i.hasNext(); ) {
                ContainerTask task = i.next();
                if (task instanceof TargetTask
                      && targetId == ((TargetTask) task).target.getId()) {
                    task.cancel(); // removes the task
                    break;
                }
            }
        }

        try {
            targetStore.update(targetId, CANCELLED, null, null);
        } catch (BulkStorageException e) {
            LOGGER.error("Failed to cancel {}::{}: {}.", ruid, targetId, e.toString());
        }
    }

    public void cancel(String targetPath) {
        LOGGER.debug("{} - cancel path {}.", ruid, targetPath);
        FsPath toCancel = findAbsolutePath(targetPrefix, targetPath);

        Optional<TargetTask> found;

        synchronized (running) {
            found = running.values().stream().filter(TargetTask.class::isInstance)
                  .map(TargetTask.class::cast).filter(t -> t.target.getPath().equals(toCancel))
                  .findAny();
        }

        if (found.isPresent()) {
            cancel(found.get().target.getId());
        } else {
            synchronized (cancelledPaths) {
                cancelledPaths.add(toCancel);
            }
        }
    }

    @Override
    public int compareTo(BulkRequestContainerJob other) {
        return jobTarget.getKey().compareTo(other.jobTarget.getKey());
    }

    public BulkActivity getActivity() {
        return activity;
    }

    public BulkRequestTarget getTarget() {
        return jobTarget;
    }

    public void initialize() {
        LOGGER.trace("BulkRequestContainerJob {}, initialize() called ...", jobTarget.getKey());
        jobTarget.setState(READY);
        containerState = ContainerState.PROCESS_FILES;
    }

    public boolean isReady() {
        switch (jobTarget.getState()) {
            case READY:
            case CREATED:
                return true;
            default:
                return false;
        }
    }

    @Override
    public void run() {
        setRunThread(Thread.currentThread());
        try {
            checkForRequestCancellation();
            switch (containerState) {
                case PROCESS_FILES:
                    LOGGER.debug("{} - run: PROCESS FILES", ruid);
                    processFileTargets();
                    containerState = ContainerState.WAIT;
                    break;
                case PROCESS_DIRS:
                    LOGGER.debug("{} - run: PROCESS DIRS", ruid);
                    processDirTargets();
                    containerState = ContainerState.STOP;
                    update(COMPLETED);
                    break;
                case STOP:
                    LOGGER.debug("{} - run: prematurely stopped; exiting", ruid);
                    update(CANCELLED);
                    setRunThread(null);
                    return;
                default:
                    throw new RuntimeException(
                          "run container called with container in wrong state " + containerState
                                + "; this is a bug.");
            }
        } catch (InterruptedException e) {
            LOGGER.debug("{} - run: interrupted", ruid);
            /*
             * If the state has not already been set to terminal, do so.
             */
            containerState = ContainerState.STOP;
            update(CANCELLED);
        }
        setRunThread(null);

        synchronized (running) {
            if (running.isEmpty()) {
                checkTransitionToDirs();
            }
        }
    }

    public void setDirListSemaphore(Semaphore dirListSemaphore) {
        this.dirListSemaphore = dirListSemaphore;
    }

    public void setInFlightSemaphore(Semaphore inFlightSemaphore) {
        this.inFlightSemaphore = inFlightSemaphore;
    }

    public void setListHandler(ListDirectoryHandler listHandler) {
        this.listHandler = listHandler;
    }

    public void setCallbackExecutor(ExecutorService callbackExecutor) {
        this.callbackExecutor = callbackExecutor;
    }

    public void setExecutor(ExecutorService executor) {
        this.executor = executor;
    }

    public void setListExecutor(ExecutorService listExecutor) {
        this.listExecutor = listExecutor;
    }

    public void setNamespaceHandler(PnfsHandler pnfsHandler) {
        this.pnfsHandler = pnfsHandler;
    }

    public void setTargetStore(BulkTargetStore targetStore) {
        this.targetStore = targetStore;
    }

    public void setCallback(SignalAware callback) {
        this.callback = callback;
    }

    @Override
    public void uncaughtException(Thread t, Throwable e) {
        /*
         * Won't leave the request in non-terminal state in case of uncaught exception.
         * We also try to handle uncaught exceptions here, so as not to kill the
         * manager thread.
         */
        containerState = ContainerState.STOP;
        jobTarget.setErrorObject(e);
        update();
        ThreadGroup group = t.getThreadGroup();
        if (group != null) {
            group.uncaughtException(t, e);
        } else {
            LOGGER.error("Uncaught exception: please report to team@dcache.org", e);
        }
    }

    public void update(State state) {
        if (jobTarget.setState(state)) {
            update();
        }
    }

    private void checkForRequestCancellation() throws InterruptedException {
        if (containerState == ContainerState.STOP || isRunThreadInterrupted()
              || jobTarget.isTerminated()) {
            throw new InterruptedException();
        }
    }

    private void checkTransitionToDirs() {
        LOGGER.debug("{} - checkTransitionToDirs: {}", ruid, containerState);
        if (containerState == ContainerState.WAIT) {
            containerState = ContainerState.PROCESS_DIRS;
            LOGGER.debug("{} - checkTransitionToDirs is now {}", ruid, containerState);
            executor.submit(this);
        }
    }

    private boolean hasBeenSpecificallyCancelled(TargetTask task) {
        synchronized (cancelledPaths) {
            BulkRequestTarget target = task.target;
            if (cancelledPaths.remove(target.getPath().toString())) {
                target = toTarget(target.getId(), target.getPid(), target.getPath(),
                      Optional.of(target.getAttributes()), CANCELLED, null);
                try {
                    if (target.getId() == null) {
                        targetStore.store(target);
                    } else {
                        targetStore.update(target.getId(), CANCELLED, null, null);
                    }
                } catch (BulkServiceException | UnsupportedOperationException e) {
                    LOGGER.error("hasBeenSpecificallyCancelled {}, failed for {}: {}", ruid,
                          target.getPath(),
                          e.toString());
                }
                return true;
            }
        }

        return false;
    }

    private synchronized void interruptRunThread() {
        if (runThread != null) {
            runThread.interrupt();
            LOGGER.debug("{} - container job interrupted.", ruid);
        }
    }

    private synchronized boolean isRunThreadInterrupted() {
        return runThread != null && runThread.isInterrupted();
    }

    private void processDirTargets() {
        if (dirs.isEmpty()) {
            LOGGER.debug("{} - processDirTargets, nothing to do.", ruid);
            return;
        }

        LOGGER.debug("{} - processDirTargets, size {}.", ruid, dirs.size());

        DirTarget[] sorted = dirs.toArray(new DirTarget[0]);
        Arrays.sort(sorted, SORTER);

        /*
         * Process serially in this thread
         */
        for (DirTarget dirTarget : sorted) {
            try {
                checkForRequestCancellation();
                new TargetTask(toTarget(dirTarget.id, dirTarget.pid, dirTarget.path,
                      Optional.of(dirTarget.attributes), CREATED, null),
                      TaskState.HANDLE_DIR_TARGET).performSync();
            } catch (InterruptedException e) {
                /*
                 * Cancel most likely called; stop processing.
                 */
                break;
            }
        }
    }

    private void processFileTargets() {
        List<BulkRequestTarget> requestTargets = targetStore.getInitialTargets(rid, true);

        LOGGER.debug("{} - processFileTargets, initial size {}.", ruid, requestTargets.size());

        if (requestTargets.isEmpty()) {
            LOGGER.error("{} - processFileTargets, no initial targets!.", ruid);
            containerState = ContainerState.STOP;
            update(FAILED);
            return;
        }

        for (BulkRequestTarget target : requestTargets) {
            try {
                checkForRequestCancellation();
                new TargetTask(target, TaskState.FETCH_ATTRIBUTES).submitAsync();
            } catch (InterruptedException e) {
                /*
                 * Cancel most likely called; stop processing.
                 */
                break;
            }
        }
    }

    private synchronized void setRunThread(Thread runThread) {
        this.runThread = runThread;
        if (runThread != null) {
            this.runThread.setUncaughtExceptionHandler(this);
        }
    }

    private void signalStateChange() {
        if (callback != null) {
            callback.signal();
        }
    }

    private BulkRequestTarget toTarget(Long id, PID pid, FsPath path,
          Optional<FileAttributes> attributes, State state, Throwable throwable) {
        String errorType = null;
        String errorMessage = null;
        Throwable root;
        if (throwable != null) {
            root = Throwables.getRootCause(throwable);
            errorType = root.getClass().getCanonicalName();
            errorMessage = root.getMessage();
        }

        return BulkRequestTargetBuilder.builder(statistics).attributes(attributes.orElse(null))
              .activity(activity.getName()).id(id).pid(pid).rid(rid).ruid(ruid).state(state)
              .createdAt(System.currentTimeMillis()).errorType(errorType)
              .errorMessage(errorMessage).path(path).build();
    }

    private void update() {
        try {
            targetStore.update(jobTarget.getId(), jobTarget.getState(),
                  jobTarget.getErrorType(),
                  jobTarget.getErrorMessage());
        } catch (BulkStorageException e) {
            LOGGER.error("{}, updateJobState: {}", ruid, e.toString());
        }

        signalStateChange();
    }
}
