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
import static org.dcache.services.bulk.util.BulkRequestTarget.State.CANCELLED;
import static org.dcache.services.bulk.util.BulkRequestTarget.State.COMPLETED;
import static org.dcache.services.bulk.util.BulkRequestTarget.State.FAILED;
import static org.dcache.services.bulk.util.BulkRequestTarget.State.READY;
import static org.dcache.services.bulk.util.BulkRequestTarget.State.RUNNING;
import static org.dcache.services.bulk.util.BulkRequestTarget.State.SKIPPED;
import static org.dcache.services.bulk.util.BulkRequestTarget.computeFsPath;

import com.google.common.collect.Range;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.FsPath;
import diskCacheV111.util.NamespaceHandlerAware;
import diskCacheV111.util.PnfsHandler;
import java.lang.Thread.UncaughtExceptionHandler;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;
import javax.security.auth.Subject;
import org.dcache.auth.attributes.Restriction;
import org.dcache.services.bulk.BulkRequest;
import org.dcache.services.bulk.BulkRequest.Depth;
import org.dcache.services.bulk.BulkServiceException;
import org.dcache.services.bulk.BulkStorageException;
import org.dcache.services.bulk.activity.BulkActivity;
import org.dcache.services.bulk.activity.BulkActivity.TargetType;
import org.dcache.services.bulk.store.BulkTargetStore;
import org.dcache.services.bulk.util.BatchedResult;
import org.dcache.services.bulk.util.BulkRequestTarget;
import org.dcache.services.bulk.util.BulkRequestTarget.PID;
import org.dcache.services.bulk.util.BulkRequestTarget.State;
import org.dcache.services.bulk.util.BulkRequestTargetBuilder;
import org.dcache.services.bulk.util.BulkServiceStatistics;
import org.dcache.util.SignalAware;
import org.dcache.util.list.DirectoryEntry;
import org.dcache.util.list.DirectoryStream;
import org.dcache.util.list.ListDirectoryHandler;
import org.dcache.vehicles.FileAttributes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for the implementations. Acts as a container for a list of targets which may or may
 * not be associated with each other via a common parent. It handles all targets by calling
 * activity.perform() on them serially using the activity's semaphore, and then holding them in a
 * map as waiting tasks with a callback listener.
 */
public abstract class AbstractRequestContainerJob
      implements Runnable, NamespaceHandlerAware, Comparable<AbstractRequestContainerJob>,
      UncaughtExceptionHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractRequestContainerJob.class);

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

    enum ContainerState {
        START, PROCESS_FILES, WAIT, PROCESS_DIRS, STOP
    }

    protected final BulkRequest request;
    protected final BulkActivity activity;
    protected final Long rid;
    protected final String ruid;
    protected final Depth depth;
    protected final String targetPrefix;
    protected final Map<FsPath, BatchedResult> waiting;

    /**
     * A temporary placeholder for tracking purposes; it will not be the same as the actual
     * autogenerated key in the database.
     */
    protected final AtomicLong id = new AtomicLong(0L);

    protected final BulkServiceStatistics statistics;
    protected BulkTargetStore targetStore;
    protected PnfsHandler pnfsHandler;
    protected Semaphore semaphore;

    protected volatile ContainerState containerState;

    private final TargetType targetType;
    private final BulkRequestTarget target;
    private final Subject subject;
    private final Restriction restriction;
    private final Set<FsPath> cancelledPaths;

    private ListDirectoryHandler listHandler;
    private SignalAware callback;

    private Thread runThread;

    AbstractRequestContainerJob(BulkActivity activity, BulkRequestTarget target,
          BulkRequest request, BulkServiceStatistics statistics) {
        this.request = request;
        this.activity = activity;
        this.target = target;
        this.subject = activity.getSubject();
        this.restriction = activity.getRestriction();
        this.statistics = statistics;
        waiting = new HashMap<>();
        cancelledPaths = new HashSet<>();
        rid = request.getId();
        ruid = request.getUid();
        depth = request.getExpandDirectories();
        targetPrefix = request.getTargetPrefix();
        targetType = activity.getTargetType();
        semaphore = new Semaphore(activity.getMaxPermits());
        containerState = ContainerState.START;
    }

    public void cancel() {
        containerState = ContainerState.STOP;

        target.cancel();
        statistics.decrement(RUNNING.name());

        LOGGER.debug("cancel {}:  target state is now {}.", ruid, target.getState());

        semaphore.drainPermits();

        interruptRunThread();

        synchronized (waiting) {
            LOGGER.debug("cancel {}:  waiting {}.", ruid, waiting.size());
            waiting.values().forEach(r -> r.cancel(activity));
            LOGGER.debug("cancel {}:  waiting targets cancelled.", ruid);
            statistics.decrement(RUNNING.name(), waiting.size());
            waiting.clear();
        }

        LOGGER.debug("cancel {}:  calling cancel all on target store.", ruid);
        targetStore.cancelAll(rid);

        signalStateChange();
    }

    public void cancel(long id) {
        synchronized (waiting) {
            for (Iterator<BatchedResult> i = waiting.values().iterator(); i.hasNext(); ) {
                BatchedResult result = i.next();
                if (result.getTarget().getId() == id) {
                    result.cancel(activity);
                    statistics.decrement(RUNNING.name());
                    i.remove();
                    break;
                }
            }
        }

        try {
            targetStore.update(id, CANCELLED, null);
        } catch (BulkStorageException e) {
            LOGGER.error("Failed to cancel {}::{}: {}.", ruid, id, e.toString());
        }
    }

    public void cancel(String path) {
        FsPath toCancel = findAbsolutePath(targetPrefix, path);

        Optional<BatchedResult> found;

        synchronized (waiting) {
            found = waiting.values().stream().filter(r -> r.getTarget().getPath().equals(toCancel))
                  .findAny();
        }

        if (found.isPresent()) {
            cancel(found.get().getTarget().getId());
        } else {
            synchronized (cancelledPaths) {
                cancelledPaths.add(toCancel);
            }
        }
    }

    @Override
    public int compareTo(AbstractRequestContainerJob other) {
        return target.getKey().compareTo(other.target.getKey());
    }

    public BulkActivity getActivity() {
        return activity;
    }

    public BulkRequestTarget getTarget() {
        return target;
    }

    public void initialize() {
        LOGGER.trace("BulkJob {}, initialize() called ...", target.getKey());
        target.setState(READY);
        containerState = ContainerState.PROCESS_FILES;
    }

    public synchronized boolean isReady() {
        switch (target.getState()) {
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
            switch (containerState) {
                case PROCESS_FILES:
                    preprocessTargets();
                    checkForRequestCancellation();
                    processFileTargets();
                    checkForRequestCancellation();
                    containerState = ContainerState.WAIT;
                    break;
                case PROCESS_DIRS:
                    checkForRequestCancellation();
                    semaphore = new Semaphore(1); /* synchronous */
                    processDirTargets();
                    containerState = ContainerState.STOP;
                    statistics.decrement(RUNNING.name());
                    update(COMPLETED);
                    break;
                default:
                    throw new RuntimeException(
                          "run container called with container in wrong state " + containerState
                                + "; this is a bug.");
            }
        } catch (InterruptedException e) {
            LOGGER.debug("run {} interrupted", ruid);
            /*
             *  If the state has not already been set to terminal, do so.
             */
            containerState = ContainerState.STOP;
            update(CANCELLED);
        }
        setRunThread(null);
        checkTransitionToDirs();
    }

    public void setListHandler(ListDirectoryHandler listHandler) {
        this.listHandler = listHandler;
    }

    @Override
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
         * Don't leave the request in non-terminal state in case of uncaught exception.
         * We also try to handle uncaught exceptions here, so as not to kill the
         * manager thread.
         */
        containerState = ContainerState.STOP;
        statistics.decrement(RUNNING.name());
        target.setErrorObject(e);
        update(FAILED);
        ThreadGroup group = t.getThreadGroup();
        if (group != null) {
            group.uncaughtException(t, e);
        } else {
            LOGGER.error("Uncaught exception: please report to team@dcache.org", e);
        }
    }

    public void update(State state) {
        if (target.setState(state)) {
            try {
                targetStore.update(target.getId(), target.getState(), target.getThrowable());
            } catch (BulkStorageException e) {
                LOGGER.error("{}, updateJobState: {}", ruid, e.toString());
            }
            signalStateChange();
        }
    }

    protected void checkForRequestCancellation() throws InterruptedException {
        if (isRunThreadInterrupted() || containerState == ContainerState.STOP
              || target.isTerminated()) {
            throw new InterruptedException();
        }
    }

    protected DirectoryStream getDirectoryListing(FsPath path)
          throws CacheException, InterruptedException {
        LOGGER.trace("getDirectoryListing {}, path {}, calling list ...", ruid, path);
        return listHandler.list(subject, restriction, path, null,
              Range.closedOpen(0, Integer.MAX_VALUE), MINIMALLY_REQUIRED_ATTRIBUTES);
    }

    protected void expandDepthFirst(Long id, PID pid, FsPath path, FileAttributes dirAttributes)
          throws BulkServiceException, CacheException, InterruptedException {
        checkForRequestCancellation();

        DirectoryStream stream = getDirectoryListing(path);
        for (DirectoryEntry entry : stream) {
            LOGGER.trace("expandDepthFirst {}, directory {}, entry {}", ruid, path,
                  entry.getName());
            FsPath childPath = path.child(entry.getName());
            FileAttributes childAttributes = entry.getFileAttributes();

            switch (childAttributes.getFileType()) {
                case DIR:
                    switch (depth) {
                        case ALL:
                            LOGGER.debug("expandDepthFirst {}, found directory {}, "
                                  + "expand ALL.", ruid, childPath);
                            expandDepthFirst(null, PID.DISCOVERED, childPath, childAttributes);
                            break;
                        case TARGETS:
                            switch (targetType) {
                                case BOTH:
                                case DIR:
                                    handleDirTarget(null, PID.DISCOVERED, childPath, childAttributes);
                            }
                            break;
                    }
                    break;
                case LINK:
                case REGULAR:
                    handleFileTarget(PID.DISCOVERED, childPath, childAttributes);
                    break;
                case SPECIAL:
                default:
                    LOGGER.trace("expandDepthFirst {}, cannot handle special "
                          + "file {}.", ruid, childPath);
                    break;
            }

            checkForRequestCancellation();
        }

        switch (targetType) {
            case BOTH:
            case DIR:
                handleDirTarget(id, pid, path, dirAttributes);
                break;
            case FILE:
                /*
                 * Because we now store all initial targets immediately,
                 * we need to mark such a directory as SKIPPED; otherwise
                 * the request will not complete on the basis of querying for
                 * completed targets and finding this one unhandled.
                 */
                if (pid == PID.INITIAL) {
                    targetStore.storeOrUpdate(toTarget(id, pid, path, Optional.of(dirAttributes),
                          SKIPPED, null));
                    statistics.increment(SKIPPED.name());
                }
        }
    }

    protected List<BulkRequestTarget> getInitialTargets() {
         return targetStore.getInitialTargets(rid, true);
    }

    protected boolean hasBeenCancelled(Long id, PID pid, FsPath path, FileAttributes attributes) {
        synchronized (cancelledPaths) {
            if (cancelledPaths.remove(path.toString())) {
                BulkRequestTarget target = toTarget(id, pid, path, Optional.of(attributes),
                      CANCELLED, null);
                try {
                    if (id == null) {
                        targetStore.store(target);
                    } else {
                        targetStore.update(target.getId(), CANCELLED, null);
                    }
                } catch (BulkServiceException | UnsupportedOperationException e) {
                    LOGGER.error("hasBeenCancelled {}, failed for {}: {}", ruid, path, e.toString());
                }
                return true;
            }
        }

        return false;
    }

    protected void preprocessTargets() throws InterruptedException {
        // NOP default
    }

    protected void removeTarget(BulkRequestTarget target) {
        synchronized (waiting) {
            waiting.remove(target.getPath());
        }

        semaphore.release();

        checkTransitionToDirs();
    }

    private void checkTransitionToDirs() {
        if (semaphore.availablePermits() == activity.getMaxPermits()) {
            synchronized (this) {
                if (containerState == ContainerState.WAIT) {
                    containerState = ContainerState.PROCESS_DIRS;
                    activity.getActivityExecutor().submit(this);
                }
            }
        }
    }

    protected BulkRequestTarget toTarget(Long id, PID pid, FsPath path, Optional<FileAttributes> attributes,
          State state, Object errorObject) {
        return BulkRequestTargetBuilder.builder().attributes(attributes.orElse(null))
              .activity(activity.getName()).id(id).pid(pid).rid(rid).ruid(ruid).state(state)
              .createdAt(System.currentTimeMillis()).error(errorObject).path(path)
              .build();
    }

    protected abstract void handleFileTarget(PID pid, FsPath path, FileAttributes attributes)
          throws InterruptedException;

    protected abstract void handleDirTarget(Long id, PID pid, FsPath path, FileAttributes attributes)
          throws InterruptedException;

    protected abstract void processFileTargets() throws InterruptedException;

    protected abstract void processDirTargets() throws InterruptedException;

    protected abstract void retryFailed(BatchedResult result, FileAttributes attributes)
          throws BulkStorageException;

    private void signalStateChange() {
        if (callback != null) {
            callback.signal();
        }
    }

    private synchronized void interruptRunThread() {
        if (runThread != null) {
            runThread.interrupt();
            LOGGER.debug("cancel {}: container job interrupted.", ruid);
        }
    }

    private synchronized boolean isRunThreadInterrupted() {
        return runThread != null && runThread.isInterrupted();
    }

    private synchronized void setRunThread(Thread runThread) {
        this.runThread = runThread;
        if (runThread != null) {
            this.runThread.setUncaughtExceptionHandler(this);
        }
    }
}