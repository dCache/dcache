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
import com.google.common.util.concurrent.MoreExecutors;
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
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import javax.security.auth.Subject;
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
import org.dcache.vehicles.PnfsResolveSymlinksMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Container job for a list of targets which may or may not be associated with each other via a
 * common parent. It handles all file targets asynchronously, recurs if directory listing is enabled,
 * and processes directory targets serially last in depth-first reverse order.
 */
public final class BulkRequestContainerJob
      implements Runnable, NamespaceHandlerAware, Comparable<BulkRequestContainerJob>,
      UncaughtExceptionHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(BulkRequestContainerJob.class);

    private static final DirTargetSorter SORTER = new DirTargetSorter();

    static final AtomicLong taskCounter = new AtomicLong(0L);

    public static FsPath findAbsolutePath(String prefix, String path) {
        FsPath absPath = computeFsPath(null, path);
        if (prefix == null) {
            return absPath;
        }

        FsPath pref = FsPath.create(prefix);

        if (!absPath.hasPrefix(pref)) {
            absPath = computeFsPath(prefix, path);
        }

        return absPath;
    }

    /**
     *  Directories that serve as targets.  These are stored in memory, sorted and processed last.
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
            depth = (int)path.toString().chars().filter(c -> c == '/').count();
        }
    }

    /**
     *  Depth-first (descending order).
     */
    static class DirTargetSorter implements Comparator<DirTarget> {
        @Override
        public int compare(DirTarget o1, DirTarget o2) {
            return Integer.compare(o2.depth, o1.depth);  /* DESCENDING ORDER */
        }
    }

    /**
     *  Container delays processing directory targets until the final step.
     */
    enum ContainerState {
        START, PROCESS_FILES, WAIT, PROCESS_DIRS, STOP
    }

    /**
     *  Only INITIAL targets go through all three states.  DISCOVERED targets
     *  already have their proper paths and attributes from listing.
     */
    enum TaskState {
        RESOLVE_PATH, FETCH_ATTRIBUTES, HANDLE_TARGET, HANDLE_DIR_TARGET
    }

    /**
     *  Wrapper task for directory listing and target processing.
     */
    abstract class ContainerTask implements Runnable {
        final Consumer<Throwable> errorHandler = e -> uncaughtException(Thread.currentThread(), e);
        final long seqNo;

        Future taskFuture;

        ContainerTask() {
            seqNo = taskCounter.getAndIncrement();
        }

        public void run() {
            try {
                doInner();
            } catch (InterruptedException e) {
                remove();
                containerState = ContainerState.STOP;
                jobTarget.setErrorObject(e);
                update(CANCELLED);
            } catch (Throwable e) {
                remove();
                errorHandler.accept(e);
                Throwables.throwIfUnchecked(e);
            }
        }

        void cancel() {
            if (taskFuture != null) {
                taskFuture.cancel(true);
            }
            remove();
        }

        void expandDepthFirst(Long id, PID pid, FsPath path, FileAttributes dirAttributes)
              throws BulkServiceException, CacheException, InterruptedException {
            LOGGER.debug("{} - expandDepthFirst, {}, {}, {}, {}", ruid, id, pid, path, dirAttributes);
            new DirListTask(id, pid, path, dirAttributes).submitAsync();
        }

        void submitAsync() throws InterruptedException {
            checkForRequestCancellation();

            synchronized (running) {
                running.put(seqNo, this);
                LOGGER.debug("{} - submitAsync {}, task count is now {}.", ruid, seqNo, running.size());
            }
            taskFuture = executor.submit(this);
        }

        void remove() {
            synchronized (running) {
                running.remove(seqNo);
                LOGGER.debug("{} - remove task {}, task count now {}.", ruid, seqNo, running.size());
            }

            checkTransitionToDirs();
        }

        abstract void doInner() throws Throwable;
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
        }

        void doInner() throws Throwable {
            try {
                DirectoryStream stream = getDirectoryListing(path);
                for (DirectoryEntry entry : stream) {
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
            dirListSemaphore.acquire();
            try {
                LOGGER.debug("{} - DirListTask, getDirectoryListing for path {}, calling list ...",
                      ruid, path);
                return listHandler.list(subject, restriction, path, null,
                      Range.closedOpen(0, Integer.MAX_VALUE), MINIMALLY_REQUIRED_ATTRIBUTES);
            } finally {
                dirListSemaphore.release();
            }
        }
    }

    class TargetTask extends ContainerTask {

        final BulkRequestTarget target;

        /*
         *  From activity.perform()
         */
        ListenableFuture activityFuture;

        /*
         *  Determines the doInner() switch
         */
        TaskState state;

        boolean holdingPermit;

        TargetTask(BulkRequestTarget target, TaskState initialState) {
            this.target = target;
            state = initialState;
        }

        void cancel() {
            if (target != null) {
                activity.cancel(target);
            }

            super.cancel();
        }

        @Override
        void doInner() throws Throwable {
            switch (state) {
                case RESOLVE_PATH:
                    resolvePath();
                    break;
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
        }

        @Override
        void submitAsync() throws InterruptedException {
            if (!holdingPermit) {
                inFlightSemaphore.acquire();
                holdingPermit = true;
            }
            super.submitAsync();
        }

        void remove() {
            super.remove();
            if (holdingPermit) {
                inFlightSemaphore.release();
                holdingPermit = false;
            }
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
         *  (1) symlink resolution on initial targets; bypassed for discovered targets.
         */
        private void resolvePath() throws InterruptedException {
            LOGGER.debug("{} - resolvePath, resolving {}", ruid, target.getPath());
            PnfsResolveSymlinksMessage message = new PnfsResolveSymlinksMessage(
                  target.getPath().toString(), null);
            ListenableFuture<PnfsResolveSymlinksMessage> requestFuture = pnfsHandler.requestAsync(
                  message);
            CellStub.addCallback(requestFuture, new AbstractMessageCallback<>() {
                @Override
                public void success(PnfsResolveSymlinksMessage message) {
                    LOGGER.debug("{} - resolvePath {}, callback success.", ruid, target.getPath());
                    FsPath path = FsPath.create(message.getResolvedPath());
                    if (targetPrefix != null && !path.contains(targetPrefix)) {
                        path = computeFsPath(targetPrefix, path.toString());
                    }
                    LOGGER.debug("{} - resolvePath, resolved path {}", ruid, path);
                    target.setPath(path);
                    state = TaskState.FETCH_ATTRIBUTES;
                    taskFuture = executor.submit(TargetTask.this);
                }

                @Override
                public void failure(int rc, Object error) {
                    LOGGER.error("{} - resolvePath, callback failure for {}.", ruid, target);
                    try {
                        storeOrUpdate(CacheExceptionFactory.exceptionOf(
                              rc, Objects.toString(error, null)));
                    } catch (InterruptedException e) {
                        errorHandler.accept(e);
                    } finally {
                        remove();
                    }
                }
            }, MoreExecutors.directExecutor());
        }

        /**
         *  (2) retrieval of required file attributes.
         */
        private void fetchAttributes() throws InterruptedException {
            LOGGER.debug("{} - fetchAttributes for path {}", ruid, target.getPath());
            PnfsGetFileAttributes message = new PnfsGetFileAttributes(target.getPath().toString(),
                  MINIMALLY_REQUIRED_ATTRIBUTES);
            ListenableFuture<PnfsGetFileAttributes> requestFuture = pnfsHandler.requestAsync(message);
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
                    try {
                        storeOrUpdate(CacheExceptionFactory.exceptionOf(
                              rc, Objects.toString(error, null)));
                    } catch (InterruptedException e) {
                        errorHandler.accept(e);
                    } finally {
                        remove();
                    }
                }
            }, MoreExecutors.directExecutor());
        }

        /**
         *  (3b) either recurs on directory or performs activity on file.
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
                     *  Swap out for the directory listing task.
                     *  (We must do this AFTER the directory task has been added to running.)
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
         *  (3a) Performs activity on either file or directory target.
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

            if (hasBeenCancelled(this)) {
                LOGGER.debug("{} - performActivity hasBeenCancelled for {}.", ruid, path);
                remove();
            }

            try {
                activityFuture = activity.perform(ruid, id == null ? seqNo : id, path, attributes);
                if (async) {
                    activityFuture.addListener(() -> handleCompletion(), executor);
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
            }

            if (state == FAILED && request.isCancelOnFailure()) {
                cancel();
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

        private void storeOrUpdate(Throwable error) throws InterruptedException {
            LOGGER.debug("{} - storeOrUpdate {}.", ruid, target);

            if (hasBeenCancelled(this)) {
                LOGGER.debug("{} - storeOrUpdate, hasBeenCancelled {}.", ruid, target.getPath());
                return;
            }

            target.setState(error == null ? RUNNING : FAILED);
            target.setErrorObject(error);

            try {
                /*
                 *  If this is an insert (id == null), the target id will be updated to what is
                 *  returned from the database.
                 */
                targetStore.storeOrUpdate(target);
                LOGGER.debug("{} - storeOrUpdate, target id {}", ruid, target.getId());
            } catch (BulkStorageException e) {
                LOGGER.error("{}, could not store or update target from result {}, {}, {}: {}.", ruid,
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
        containerState = ContainerState.STOP;

        jobTarget.cancel();

        LOGGER.debug("{} - cancel:  target state is now {}.", ruid, jobTarget.getState());

        interruptRunThread();

        synchronized (running) {
            LOGGER.debug("{} - cancel:  running {}.", ruid, running.size());
            running.values().forEach(ContainerTask::cancel);
            LOGGER.debug("{} - cancel:  running targets cancelled.", ruid);
            running.clear();
        }

        LOGGER.debug("{} - cancel:  calling cancel all on target store.", ruid);
        targetStore.cancelAll(rid);

        signalStateChange();
    }

    public void cancel(long targetId) {
        synchronized (running) {
            for (Iterator<ContainerTask> i = running.values().iterator(); i.hasNext(); ) {
                ContainerTask task = i.next();
                if (task instanceof TargetTask
                      && targetId == ((TargetTask) task).target.getId()) {
                    task.cancel();
                    i.remove();
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

    public synchronized boolean isReady() {
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
             *  If the state has not already been set to terminal, do so.
             */
            containerState = ContainerState.STOP;
            update(CANCELLED);
        }
        setRunThread(null);
        checkTransitionToDirs();
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

    public void setExecutor(ExecutorService executor) {
        this.executor = executor;
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
        update(FAILED);
        ThreadGroup group = t.getThreadGroup();
        if (group != null) {
            group.uncaughtException(t, e);
        } else {
            LOGGER.error("Uncaught exception: please report to team@dcache.org", e);
        }
    }

    public void update(State state) {
        if (jobTarget.setState(state)) {
            try {
                targetStore.update(jobTarget.getId(), jobTarget.getState(), jobTarget.getErrorType(),
                      jobTarget.getErrorMessage());
            } catch (BulkStorageException e) {
                LOGGER.error("{}, updateJobState: {}", ruid, e.toString());
            }
            signalStateChange();
        }
    }

    private void checkForRequestCancellation() throws InterruptedException {
        if (isRunThreadInterrupted() || containerState == ContainerState.STOP
              || jobTarget.isTerminated()) {
            throw new InterruptedException();
        }
    }

    private void checkTransitionToDirs() {
        synchronized (running) {
            if (!running.isEmpty()) {
                LOGGER.debug("{} - checkTransitionToDirs, running {}", ruid, running.size());
                return;
            }
        }

        synchronized (this) {
            if (containerState == ContainerState.WAIT) {
                containerState = ContainerState.PROCESS_DIRS;
                executor.submit(this);
            }
        }
    }

    private boolean hasBeenCancelled(TargetTask task) {
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
                    LOGGER.error("hasBeenCancelled {}, failed for {}: {}", ruid, target.getPath(),
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

    private void processDirTargets() throws InterruptedException {
        if (dirs.isEmpty()) {
            LOGGER.debug("{} - processDirTargets, nothing to do.", ruid);
            return;
        }

        LOGGER.debug("{} - processDirTargets, size {}.", ruid, dirs.size());

        DirTarget[] sorted = dirs.toArray(new DirTarget[0]);
        Arrays.sort(sorted, SORTER);

        /*
         *  Process serially in this thread
         */
        for (DirTarget dirTarget : sorted) {
            new TargetTask(toTarget(dirTarget.id, dirTarget.pid, dirTarget.path,
                  Optional.of(dirTarget.attributes), CREATED, null),
                  TaskState.HANDLE_DIR_TARGET).performSync();
        }
    }

    private void processFileTargets() throws InterruptedException {
        List<BulkRequestTarget> requestTargets = targetStore.getInitialTargets(rid, true);

        LOGGER.debug("{} - processFileTargets, initial size {}.", ruid, requestTargets.size());

        if (requestTargets.isEmpty()) {
            LOGGER.error("{} - processFileTargets, no initial targets!.", ruid);
            containerState = ContainerState.STOP;
            update(FAILED);
            return;
        }

        for (BulkRequestTarget target : requestTargets) {
            new TargetTask(target, TaskState.RESOLVE_PATH).submitAsync();
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
}