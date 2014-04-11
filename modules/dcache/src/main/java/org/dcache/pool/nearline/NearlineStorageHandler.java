/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2014 Deutsches Elektronen-Synchrotron
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.dcache.pool.nearline;

import com.google.common.base.Functions;
import com.google.common.base.Joiner;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;

import javax.annotation.PostConstruct;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.nio.channels.CompletionHandler;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.DiskErrorCacheException;
import diskCacheV111.util.FileInCacheException;
import diskCacheV111.util.FileNotFoundCacheException;
import diskCacheV111.util.HsmLocationExtractorFactory;
import diskCacheV111.util.PnfsHandler;
import diskCacheV111.util.PnfsId;
import diskCacheV111.util.TimeoutCacheException;
import diskCacheV111.vehicles.PoolFileFlushedMessage;
import diskCacheV111.vehicles.StorageInfo;
import diskCacheV111.vehicles.StorageInfoMessage;

import dmg.cells.nucleus.AbstractCellComponent;
import dmg.cells.nucleus.CellCommandListener;
import dmg.cells.nucleus.DelayedReply;
import dmg.cells.nucleus.NoRouteToCellException;
import dmg.util.command.Argument;
import dmg.util.command.Command;
import dmg.util.command.Option;

import org.dcache.cells.CellStub;
import org.dcache.namespace.FileAttribute;
import org.dcache.pool.classic.ChecksumModule;
import org.dcache.pool.classic.NopCompletionHandler;
import org.dcache.pool.nearline.spi.FlushRequest;
import org.dcache.pool.nearline.spi.NearlineRequest;
import org.dcache.pool.nearline.spi.NearlineStorage;
import org.dcache.pool.nearline.spi.RemoveRequest;
import org.dcache.pool.nearline.spi.StageRequest;
import org.dcache.pool.repository.EntryState;
import org.dcache.pool.repository.IllegalTransitionException;
import org.dcache.pool.repository.ReplicaDescriptor;
import org.dcache.pool.repository.Repository;
import org.dcache.pool.repository.StickyRecord;
import org.dcache.util.Checksum;
import org.dcache.vehicles.FileAttributes;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterables.transform;
import static org.dcache.namespace.FileAttribute.*;

/**
 * Entry point to and management interface for the nearline storage subsystem.
 */
public class NearlineStorageHandler extends AbstractCellComponent implements CellCommandListener
{
    private static final Logger LOGGER = LoggerFactory.getLogger(NearlineStorageHandler.class);

    private static final Set<Repository.OpenFlags> NO_FLAGS = Collections.emptySet();

    private final FlushRequestContainer flushRequests = new FlushRequestContainer();
    private final StageRequestContainer stageRequests = new StageRequestContainer();
    private final RemoveRequestContainer removeRequests = new RemoveRequestContainer();

    private ScheduledExecutorService scheduledExecutor;
    private ListeningExecutorService executor;
    private Repository repository;
    private ChecksumModule checksumModule;
    private PnfsHandler pnfs;
    private CellStub flushMessageTarget;
    private CellStub billingStub;
    private HsmSet hsmSet;
    private long stageTimeout;
    private long flushTimeout;
    private long removeTimeout;

    @Required
    public void setScheduledExecutor(ScheduledExecutorService executor)
    {
        this.scheduledExecutor = executor;
    }

    @Required
    public void setExecutor(ListeningExecutorService executor)
    {
        this.executor = executor;
    }

    @Required
    public void setRepository(Repository repository)
    {
        this.repository = repository;
    }

    @Required
    public void setChecksumModule(ChecksumModule checksumModule)
    {
        this.checksumModule = checksumModule;
    }

    @Required
    public void setPnfsHandler(PnfsHandler pnfs)
    {
        this.pnfs = pnfs;
    }

    @Required
    public void setFlushMessageTarget(CellStub flushMessageTarget)
    {
        this.flushMessageTarget = flushMessageTarget;
    }

    @Required
    public void setBillingStub(CellStub billingStub)
    {
        this.billingStub = billingStub;
    }

    @Required
    public void setHsmSet(HsmSet hsmSet)
    {
        this.hsmSet = hsmSet;
    }

    @PostConstruct
    public void init()
    {
        scheduledExecutor.scheduleWithFixedDelay(new TimeoutTask(), 30, 30, TimeUnit.SECONDS);
    }

    /**
     * Flushes a set of files to nearline storage.
     *
     * @param hsmType type of nearline storage
     * @param files files to flush
     * @param callback callback notified for every file flushed
     */
    public void flush(String hsmType,
                      Iterable<PnfsId> files,
                      CompletionHandler<Void, PnfsId> callback)
    {
        try {
            NearlineStorage nearlineStorage = hsmSet.getNearlineStorageByType(hsmType);
            checkArgument(nearlineStorage != null, "No such nearline storage: " + hsmType);
            flushRequests.addAll(nearlineStorage, files, callback);
        } catch (RuntimeException e) {
            for (PnfsId pnfsId : files) {
                callback.failed(e, pnfsId);
            }
        }
    }

    /**
     * Stages a file from nearline storage.
     *
     * TODO: Should eventually accept multiple files at once, but the rest of the pool
     *       doesn't support that yet.
     *
     * @param file attributes of file to stage
     * @param callback callback notified when file is staged
     */
    public void stage(String hsmInstance,
                      FileAttributes file,
                      CompletionHandler<Void, PnfsId> callback)
    {
        try {
            NearlineStorage nearlineStorage = hsmSet.getNearlineStorageByName(hsmInstance);
            checkArgument(nearlineStorage != null, "No such nearline storage: " + hsmInstance);
            stageRequests.addAll(nearlineStorage, Collections.singleton(file), callback);
        } catch (RuntimeException e) {
            callback.failed(e, file.getPnfsId());
        }
    }

    /**
     * Removes a set of files from nearline storage.
     *
     * @param hsmInstance instance name of nearline storage
     * @param files files to remove
     * @param callback callback notified for every file removed
     */
    public void remove(String hsmInstance,
                       Iterable<URI> files,
                       CompletionHandler<Void, URI> callback)
    {
        try {
            NearlineStorage nearlineStorage = hsmSet.getNearlineStorageByName(hsmInstance);
            checkArgument(nearlineStorage != null, "No such nearline storage: " + hsmInstance);
            removeRequests.addAll(nearlineStorage, files, callback);
        } catch (RuntimeException e) {
            for (URI location : files) {
                callback.failed(e, location);
            }
        }
    }

    public int getActiveFetchJobs()
    {
        return stageRequests.getCount(AbstractRequest.State.ACTIVE);
    }

    public int getFetchQueueSize()
    {
        return stageRequests.getCount(AbstractRequest.State.QUEUED);
    }

    public int getActiveStoreJobs()
    {
        return flushRequests.getCount(AbstractRequest.State.ACTIVE);
    }

    public int getStoreQueueSize()
    {
        return flushRequests.getCount(AbstractRequest.State.QUEUED);
    }


    /**
     * Abstract base class for request implementations.
     *
     * Provides support for registering callbacks and deregistering from a RequestContainer
     * when the request has completed.
     *
     * Implements part of NearlineRequest, although the interface isn't formally implemented.
     * Subclasses implement subinterfaces of NearlineRequest.
     *
     * @param <K> key identifying a request
     */
    private abstract static class AbstractRequest<K>
    {
        private enum State { QUEUED, ACTIVE, CANCELED }

        private final List<CompletionHandler<Void,K>> callbacks = new ArrayList<>();
        protected final long createdAt = System.currentTimeMillis();
        protected final UUID uuid = UUID.randomUUID();
        protected final NearlineStorage storage;
        protected final AtomicReference<State> state = new AtomicReference<>(State.QUEUED);
        protected volatile long activatedAt;

        private final List<Future<?>> asyncTasks = new ArrayList<>();

        AbstractRequest(NearlineStorage storage)
        {
            this.storage = storage;
        }

        // Implements NearlineRequest#activate
        public UUID getId()
        {
            return uuid;
        }

        public long getCreatedAt()
        {
            return createdAt;
        }

        protected synchronized <T> ListenableFuture<T> register(ListenableFuture<T> future)
        {
            // TODO: What if we are already cancelled?
            asyncTasks.add(future);
            return future;
        }

        public ListenableFuture<Void> activate()
        {
            if (!state.compareAndSet(State.QUEUED, State.ACTIVE)) {
                return Futures.immediateFailedFuture(new IllegalStateException("Request is no longer queued."));
            }
            activatedAt = System.currentTimeMillis();
            return Futures.immediateFuture(null);
        }

        // Guarded by the container containing this request
        public void addCallback(CompletionHandler<Void,K> callback)
        {
            callbacks.add(callback);
        }

        // Guarded by the container containing this request
        public Iterable<CompletionHandler<Void,K>> callbacks()
        {
            return this.callbacks;
        }

        public void cancel()
        {
            if (state.getAndSet(State.CANCELED) != State.CANCELED) {
                storage.cancel(uuid);
                synchronized(this) {
                    for (Future<?> task : asyncTasks) {
                        task.cancel(true);
                    }
                }
            }
        }

        @Override
        public String toString()
        {
            StringBuilder sb = new StringBuilder();
            sb.append(uuid).append(' ').append(state).append(' ').append(new Date(createdAt));
            long activatedAt = this.activatedAt;
            if (activatedAt > 0) {
                sb.append(' ').append(new Date(activatedAt));
            }
            return sb.toString();
        }
    }

    /**
     * An abstract container for requests of a particular type.
     *
     * Subclasses implement methods for extracting a request specific key, creating request
     * objects and submitting the request to a NearlineStorage.
     *
     * Supports thread safe addition and removal of requests from the container. If the same
     * request (as identified by the key) is added multiple times, the requests are collapsed
     * by adding the callback to the existing request.
     *
     * @param <K> key identifying a request
     * @param <F> information defining a replica
     * @param <R> type of request
     */
    private abstract static class AbstractRequestContainer<K, F, R extends AbstractRequest<K> & NearlineRequest<?>>
    {
        private final Map<K, R> requests = new HashMap<>();

        public synchronized void addAll(NearlineStorage storage,
                                        Iterable<F> files,
                                        CompletionHandler<Void,K> callback)
        {
            List<R> newRequests = new ArrayList<>();
            for (F file : files) {
                K key = extractKey(file);
                R request = requests.get(key);
                if (request == null) {
                    try {
                        request = checkNotNull(createRequest(storage, file));
                    } catch (Exception e) {
                        callback.failed(e, key);
                        continue;
                    }
                    requests.put(key, request);
                    newRequests.add(request);
                }
                request.addCallback(callback);
            }
            submit(storage, newRequests);
        }

        public synchronized void cancel(K key)
        {
            R request = requests.get(key);
            if (request != null) {
                request.cancel();
            }
        }

        public synchronized void cancelOldRequests(long timeout)
        {
            long now = System.currentTimeMillis();
            for (R request : requests.values()) {
                if (now - request.getCreatedAt() > timeout) {
                    request.cancel();
                }
            }
        }

        public synchronized int getCount(AbstractRequest.State state)
        {
            int cnt = 0;
            for (R request : requests.values()) {
                if (request.state.get() == state) {
                    cnt++;
                }
            }
            return cnt;
        }

        /**
         * Called by subclass to remove the request from the container and invoke the
         * callbacks of the request.
         *
         * @param key the key identifying the request to remove
         * @param cause cause of why the request failed, or null if the request was successful
         */
        protected void removeAndCallback(K key, Throwable cause)
        {
            for (CompletionHandler<Void,K> callback : remove(key)) {
                if (cause != null) {
                    callback.failed(cause, key);
                } else {
                    callback.completed(null, key);
                }
            }
        }

        public synchronized String printJobQueue()
        {
            return Joiner.on('\n').join(requests.values());
        }

        private synchronized Iterable<CompletionHandler<Void,K>> remove(K key)
        {
            R actualRequest = requests.remove(key);
            return (actualRequest != null) ? actualRequest.callbacks() : Collections.<CompletionHandler<Void,K>>emptyList();
        }

        /** Returns a key identifying the request for a particular replica. */
        protected abstract K extractKey(F file);

        /** Creates a new nearline storage request. */
        protected abstract R createRequest(NearlineStorage storage, F file) throws Exception;

        /** Submits requests to the nearline storage. */
        protected abstract void submit(NearlineStorage storage, Iterable<R> requests);
    }

    private class FlushRequestContainer extends AbstractRequestContainer<PnfsId, PnfsId, FlushRequestImpl>
    {
        @Override
        protected PnfsId extractKey(PnfsId id)
        {
            return id;
        }

        @Override
        protected FlushRequestImpl createRequest(NearlineStorage storage, PnfsId id)
                throws CacheException, InterruptedException
        {
            return new FlushRequestImpl(storage, id);
        }

        @Override
        protected void submit(NearlineStorage storage, Iterable<FlushRequestImpl> requests)
        {
            storage.flush(transform(requests, Functions.<FlushRequest>identity()));
        }
    }

    private class StageRequestContainer extends AbstractRequestContainer<PnfsId, FileAttributes, StageRequestImpl>
    {
        @Override
        protected PnfsId extractKey(FileAttributes file)
        {
            return file.getPnfsId();
        }

        @Override
        protected StageRequestImpl createRequest(NearlineStorage storage,
                                                 FileAttributes file)
                throws FileInCacheException
        {
            return new StageRequestImpl(storage, file);
        }

        @Override
        protected void submit(NearlineStorage storage, Iterable<StageRequestImpl> requests)
        {
            storage.stage(transform(requests, Functions.<StageRequest>identity()));
        }
    }

    private class RemoveRequestContainer extends AbstractRequestContainer<URI, URI, RemoveRequestImpl>
    {
        @Override
        protected URI extractKey(URI uri)
        {
            return uri;
        }

        @Override
        protected RemoveRequestImpl createRequest(NearlineStorage storage, URI uri)
        {
            return new RemoveRequestImpl(storage, uri);
        }

        @Override
        protected void submit(NearlineStorage storage, Iterable<RemoveRequestImpl> requests)
        {
            storage.remove(transform(requests, Functions.<RemoveRequest>identity()));
        }
    }

    private class FlushRequestImpl extends AbstractRequest<PnfsId> implements FlushRequest
    {
        private final ReplicaDescriptor descriptor;
        private final StorageInfoMessage infoMsg;

        public FlushRequestImpl(NearlineStorage nearlineStorage, PnfsId pnfsId) throws CacheException, InterruptedException
        {
            super(nearlineStorage);
            infoMsg = new StorageInfoMessage(getCellAddress().toString(), pnfsId, false);
            descriptor = repository.openEntry(pnfsId, NO_FLAGS);
            LOGGER.debug("Flush request created for {}.", pnfsId);
        }

        @Override
        public File getFile()
        {
            return descriptor.getFile();
        }

        @Override
        public FileAttributes getFileAttributes()
        {
            return descriptor.getFileAttributes();
        }

        @Override
        public long getDeadline()
        {
            return createdAt + flushTimeout;
        }

        @Override
        public ListenableFuture<Void> activate()
        {
            LOGGER.debug("Activating flush of {}.", getFileAttributes().getPnfsId());
            return register(Futures.transform(super.activate(), new PreFlushFunction(), executor));
        }

        @Override
        public String toString()
        {
            return super.toString() + ' ' + getFileAttributes().getPnfsId();
        }

        @Override
        public void failed(Exception cause)
        {
            descriptor.close();
            /* ListenableFuture#get throws ExecutionException */
            if (cause instanceof ExecutionException) {
                done(cause.getCause());
            } else {
                done(cause);
            }
        }

        @Override
        public void completed(Set<URI> uris)
        {
            try {
                descriptor.close();

                FileAttributes fileAttributesForNotification = getFileAttributesForNotification(uris);

                infoMsg.setStorageInfo(fileAttributesForNotification.getStorageInfo());

                PnfsId pnfsId = getFileAttributes().getPnfsId();
                notifyNamespace(pnfsId, fileAttributesForNotification);
                notifyFlushMessageTarget(pnfsId, fileAttributesForNotification);

                LOGGER.info("{} stored to {}.", pnfsId, uris);

                try {
                    repository.setState(pnfsId, EntryState.CACHED);
                } catch (IllegalTransitionException ignored) {
                    /* Apparently the file is no longer precious. Most
                     * likely it got deleted, which is fine, since the
                     * flush already succeeded.
                     */
                }
                done(null);
            } catch (Exception e) {
                done(e);
            }
        }

        private FileAttributes getFileAttributesForNotification(Set<URI> uris) throws CacheException
        {
            FileAttributes fileAttributes = descriptor.getFileAttributes();
            StorageInfo storageInfo = fileAttributes.getStorageInfo().clone();
            for (URI uri : uris) {
                try {
                    HsmLocationExtractorFactory.validate(uri);
                    storageInfo.addLocation(uri);
                    storageInfo.isSetAddLocation(true);
                } catch (IllegalArgumentException e) {
                    throw new CacheException(2, e.getMessage(), e);
                }
            }
            FileAttributes fileAttributesForNotification = new FileAttributes();
            fileAttributesForNotification.setAccessLatency(fileAttributes.getAccessLatency());
            fileAttributesForNotification.setRetentionPolicy(fileAttributes.getRetentionPolicy());
            fileAttributesForNotification.setStorageInfo(storageInfo);
            fileAttributesForNotification.setSize(fileAttributes.getSize());
            return fileAttributesForNotification;
        }

        private void notifyNamespace(PnfsId pnfsid, FileAttributes fileAttributes)
                throws InterruptedException
        {
            while (true) {
                try {
                    pnfs.fileFlushed(pnfsid, fileAttributes);
                    break;
                } catch (CacheException e) {
                    if (e.getRc() == CacheException.FILE_NOT_FOUND ||
                            e.getRc() == CacheException.NOT_IN_TRASH) {
                        /* In case the file was deleted, we are presented
                         * with the problem that the file is now on tape,
                         * however the location has not been registered
                         * centrally. Hence the copy on tape will not be
                         * removed by the HSM cleaner. The sensible thing
                         * seems to be to remove the file from tape here.
                         * For now we ignore this issue (REVISIT).
                         */
                        break;
                    }

                    /* The message to the PnfsManager failed. There are several
                     * possible reasons for this; we may have lost the
                     * connection to the PnfsManager; the PnfsManager may have
                     * lost its connection to the namespace or otherwise be in
                     * trouble; bugs; etc.
                     *
                     * We keep retrying until we succeed. This will effectively
                     * block this thread from flushing any other files, which
                     * seems sensible when we have trouble talking to the
                     * PnfsManager. If the pool crashes or gets restarted while
                     * waiting here, we will end up flushing the file again. We
                     * assume that the nearline storage is able to eliminate the
                     * duplicate; or at least tolerate the duplicate (given that
                     * this situation should be rare, we can live with a little
                     * bit of wasted tape).
                     */
                    LOGGER.error("Error notifying pnfsmanager about a flushed file: {} ({})",
                            e.getMessage(), e.getRc());
                }
                TimeUnit.MINUTES.sleep(2);
            }
        }

        private void notifyFlushMessageTarget(PnfsId pnfsId, FileAttributes fileAttributes)
        {
            try {
                PoolFileFlushedMessage poolFileFlushedMessage =
                        new PoolFileFlushedMessage(getCellName(), pnfsId, fileAttributes);
                poolFileFlushedMessage.setReplyRequired(false);
                flushMessageTarget.notify(poolFileFlushedMessage);
            } catch (NoRouteToCellException e) {
                LOGGER.info("Failed to send message to {}: {}",
                        flushMessageTarget.getDestinationPath(), e.getMessage());
            }
        }

        private void done(Throwable cause)
        {
            PnfsId pnfsId = getFileAttributes().getPnfsId();
            if (cause == null) {
                LOGGER.debug("Flush of {} completed successfully.", pnfsId);
            } else {
                if (cause instanceof InterruptedException || cause instanceof CancellationException) {
                    cause = new TimeoutCacheException("Flush was cancelled.", cause);
                }
                LOGGER.warn("Flush of {} failed with: {}.",
                            pnfsId, cause);
            }
            try {
                infoMsg.setTransferTime(System.currentTimeMillis() - activatedAt);
                infoMsg.setFileSize(getFileAttributes().getSize());
                infoMsg.setTimeQueued(activatedAt - createdAt);

                if (cause instanceof CacheException) {
                    infoMsg.setResult(((CacheException) cause).getRc(), cause.getMessage());
                } else if (cause != null) {
                    infoMsg.setResult(CacheException.DEFAULT_ERROR_CODE, cause.getMessage());
                }

                billingStub.notify(infoMsg);
            } catch (NoRouteToCellException e) {
                LOGGER.error("Failed to send message to billing: {}", e.getMessage());
            }
            flushRequests.removeAndCallback(pnfsId, cause);
        }

        private class PreFlushFunction implements AsyncFunction<Void, Void>
        {
            @Override
            public ListenableFuture<Void> apply(Void ignored)
                    throws CacheException, InterruptedException, NoSuchAlgorithmException, IOException
            {
                PnfsId pnfsId = descriptor.getFileAttributes().getPnfsId();
                LOGGER.debug("Checking if {} still exists.", pnfsId);
                try {
                    pnfs.getFileAttributes(pnfsId, EnumSet.noneOf(FileAttribute.class));
                } catch (FileNotFoundCacheException e) {
                    try {
                        repository.setState(pnfsId, EntryState.REMOVED);
                        LOGGER.info("File not found in name space; removed {}.", pnfsId);
                    } catch (CacheException f) {
                        LOGGER.error("File not found in name space, but failed to remove {}: {}", pnfsId,
                                     f.getMessage());
                    } catch (InterruptedException | IllegalTransitionException f) {
                        LOGGER.error("File not found in name space, but failed to remove {}: {}", pnfsId, f);
                    }
                    throw e;
                }
                checksumModule.enforcePreFlushPolicy(descriptor);
                return Futures.immediateFuture(null);
            }
        }
    }

    private class StageRequestImpl extends AbstractRequest<PnfsId> implements StageRequest
    {
        private final StorageInfoMessage infoMsg;
        private final ReplicaDescriptor descriptor;

        public StageRequestImpl(NearlineStorage storage, FileAttributes fileAttributes) throws FileInCacheException
        {
            super(storage);
            PnfsId pnfsId = fileAttributes.getPnfsId();
            infoMsg = new StorageInfoMessage(getCellAddress().toString(), pnfsId, true);
            infoMsg.setStorageInfo(fileAttributes.getStorageInfo());
            infoMsg.setFileSize(fileAttributes.getSize());
            descriptor =
                    repository.createEntry(
                            fileAttributes,
                            EntryState.FROM_STORE,
                            EntryState.CACHED,
                            Collections.<StickyRecord>emptyList(),
                            EnumSet.noneOf(Repository.OpenFlags.class));
            LOGGER.debug("Stage request created for {}.", pnfsId);
        }

        @Override
        public ListenableFuture<Void> allocate()
        {
            LOGGER.debug("Allocating space for stage of {}.", getFileAttributes().getPnfsId());
            return register(executor.submit(
                    new Callable<Void>()
                    {
                        @Override
                        public Void call()
                                throws InterruptedException
                        {
                            descriptor.allocate(descriptor.getFileAttributes().getSize());
                            return null;
                        }
                    }
            ));
        }

        @Override
        public synchronized ListenableFuture<Void> activate()
        {
            LOGGER.debug("Activating stage of {}.", getFileAttributes().getPnfsId());
            return super.activate();
        }

        @Override
        public File getFile()
        {
            return descriptor.getFile();
        }

        @Override
        public FileAttributes getFileAttributes()
        {
            return descriptor.getFileAttributes();
        }

        @Override
        public long getDeadline()
        {
            return createdAt + stageTimeout;
        }

        @Override
        public void failed(Exception cause)
        {
            /* ListenableFuture#get throws ExecutionException */
            if (cause instanceof ExecutionException) {
                done(cause.getCause());
            } else {
                done(cause);
            }
        }

        @Override
        public void completed(Set<Checksum> checksums)
        {
            Throwable error = null;
            try {
                if (checksumModule.hasPolicy(ChecksumModule.PolicyFlag.GET_CRC_FROM_HSM)) {
                    LOGGER.info("Obtained checksums {} for {} from HSM", checksums, getFileAttributes().getPnfsId());
                    descriptor.addChecksums(checksums);
                }
                checksumModule.enforcePostRestorePolicy(descriptor);
                descriptor.commit();
            } catch (InterruptedException | CacheException | RuntimeException | Error e) {
                error = e;
            } catch (NoSuchAlgorithmException e) {
                error = new CacheException(1010, "Checksum calculation failed: " + e.getMessage(), e);
            } catch (IOException e) {
                error = new DiskErrorCacheException("Checksum calculation failed due to I/O error: " + e.getMessage(), e);
            } finally {
                done(error);
            }
        }

        private void done(Throwable cause)
        {
            PnfsId pnfsId = getFileAttributes().getPnfsId();
            if (cause == null) {
                LOGGER.debug("Stage of {} completed successfully.",
                             pnfsId);
            } else {
                if (cause instanceof InterruptedException || cause instanceof CancellationException) {
                    cause = new TimeoutCacheException("Stage was cancelled.", cause);
                }
                LOGGER.warn("Stage of {} failed with {}.",
                            pnfsId, cause);
            }
            descriptor.close();
            if (cause instanceof CacheException) {
                infoMsg.setResult(((CacheException) cause).getRc(), cause.getMessage());
            } else if (cause != null) {
                infoMsg.setResult(CacheException.DEFAULT_ERROR_CODE, cause.toString());
            }
            infoMsg.setTransferTime(System.currentTimeMillis() - activatedAt);
            try {
                billingStub.notify(infoMsg);
            } catch (NoRouteToCellException e) {
                LOGGER.error("Failed to send message to billing: {}", e.getMessage());
            }
            stageRequests.removeAndCallback(pnfsId, cause);
        }

        @Override
        public String toString()
        {
            return super.toString() + ' ' + getFileAttributes().getPnfsId();
        }
    }

    private class RemoveRequestImpl extends AbstractRequest<URI> implements RemoveRequest
    {
        private final URI uri;

        RemoveRequestImpl(NearlineStorage storage, URI uri)
        {
            super(storage);
            this.uri = uri;
            LOGGER.debug("Stage request created for {}.", uri);
        }

        @Override
        public synchronized ListenableFuture<Void> activate()
        {
            LOGGER.debug("Activating remove of {}.", uri);
            return super.activate();
        }

        public URI getUri()
        {
            return uri;
        }

        public long getDeadline()
        {
            return createdAt + removeTimeout;
        }

        public void failed(Exception cause)
        {
            if (cause instanceof InterruptedException || cause instanceof CancellationException) {
                cause = new TimeoutCacheException("Stage was cancelled.", cause);
            }
            LOGGER.warn("Remove of {} failed with {}.", uri, cause);
            removeRequests.removeAndCallback(uri, cause);
        }

        public void completed(Void result)
        {
            LOGGER.debug("Remove of {} completed successfully.", uri);
            removeRequests.removeAndCallback(uri, null);
        }

        @Override
        public String toString()
        {
            return super.toString() + ' ' + uri;
        }
    }

    @Command(name = "rh set timeout",
            hint = "set restore timeout",
            description = "Set restore timeout for the HSM script. When the timeout expires " +
                    "the HSM script is killed.")
    class RestoreSetTimeoutCommand implements Callable<String>
    {
        @Argument(metaVar = "seconds")
        long timeout;

        @Override
        public String call()
        {
            synchronized (NearlineStorageHandler.this) {
                stageTimeout = TimeUnit.SECONDS.toMillis(timeout);
            }
            return "";
        }
    }

    @Command(name = "rh kill",
            hint = "kill restore request",
            description = "Remove an HSM restore request.")
    class RestoreKillCommand implements Callable<String>
    {
        @Argument
        PnfsId pnfsId;

        @Override
        public String call() throws NoSuchElementException, IllegalStateException
        {
            stageRequests.cancel(pnfsId);
            return "Kill initialized";
        }
    }

    @Command(name = "rh ls",
            hint = "list restore queue",
            description = "List the HSM requests on the restore queue.\n\n" +
                    "The columns in the output show: job id, job status, pnfs id, request counter, " +
                    "and request submission time.")
    class RestoreListCommand implements Callable<String>
    {
        @Override
        public String call()
        {
            return stageRequests.printJobQueue();
        }
    }

    @Command(name = "st set timeout",
            hint = "set store timeout",
            description = "Set store timeout for the HSM script. When the timeout expires " +
                    "the HSM script is killed.")
    class StoreSetTimeoutCommand implements Callable<String>
    {
        @Argument(metaVar = "seconds")
        long timeout;

        @Override
        public String call()
        {
            synchronized (NearlineStorageHandler.this) {
                flushTimeout = TimeUnit.SECONDS.toMillis(timeout);
            }
            return "";
        }
    }

    @Command(name = "st kill",
            hint = "kill store request",
            description = "Remove an HSM store request.")
    class StoreKillCommand implements Callable<String>
    {
        @Argument
        PnfsId pnfsId;

        @Override
        public String call() throws NoSuchElementException, IllegalStateException
        {
            flushRequests.cancel(pnfsId);
            return "Kill initialized";
        }
    }

    @Command(name = "st ls",
            hint = "list store queue",
            description = "List the HSM requests on the store queue.\n\n" +
                    "The columns in the output show: job id, job status, pnfs id, request counter, " +
                    "and request submission time.")
    class StoreListCommand implements Callable<String>
    {
        @Override
        public String call()
        {
            return flushRequests.printJobQueue();
        }
    }

    @Command(name = "rm set timeout",
            hint = "set tape remove timeout",
            description = "Set remove timeout for the HSM script. When the timeout expires " +
                    "the HSM script is killed.")
    class RemoveSetTimeoutCommand implements Callable<String>
    {
        @Argument(metaVar = "seconds")
        long timeout;

        @Override
        public String call()
        {
            synchronized (NearlineStorageHandler.this) {
                removeTimeout = TimeUnit.SECONDS.toMillis(timeout);
            }
            return "";
        }
    }

    @Command(name = "rm ls",
            hint = "list store queue",
            description = "List the HSM requests on the remove queue.\n\n" +
                    "The columns in the output show: job id, job status, pnfs id, request counter, " +
                    "and request submission time.")
    class RemoveListCommand implements Callable<String>
    {
        @Override
        public String call()
        {
            return removeRequests.printJobQueue();
        }
    }

    @Command(name = "rh restore",
            hint = "restore file from tape",
            description = "Restore a file from tape.")
    class RestoreCommand extends DelayedReply implements Callable<Serializable>, CompletionHandler<Void, PnfsId>
    {
        @Argument
        PnfsId pnfsId;

        @Option(name = "block",
                usage = "Block the shell until the restore has completed. This " +
                        "option is only relevant when debugging as the shell " +
                        "would usually time out before a real HSM is able to " +
                        "restore a file.")
        boolean block;

        @Override
        public void completed(Void result, PnfsId pnfsId)
        {
            reply("Fetched " + pnfsId);
        }

        @Override
        public void failed(Throwable exc, PnfsId pnfsId)
        {
            reply("Failed to fetch " + pnfsId + ": " + exc);
        }

        @Override
        public Serializable call()
        {
            /* We need to fetch the storage info and we don't want to
             * block the message thread while waiting for the reply.
             */
            executor.submit(new Runnable()
            {
                @Override
                public void run()
                {
                    try {
                        FileAttributes attributes = pnfs.getFileAttributes(pnfsId,
                                                                           EnumSet.of(PNFSID, SIZE, STORAGEINFO));
                        String hsm = hsmSet.getInstanceName(attributes);
                        stage(hsm, attributes, block ? RestoreCommand.this : new NopCompletionHandler<Void, PnfsId>());
                    } catch (CacheException e) {
                        failed(e, pnfsId);
                    }
                }
            });
            return block ? this : "Fetch request queued.";
        }
    }

    private class TimeoutTask implements Runnable
    {
        @Override
        public void run()
        {
            flushRequests.cancelOldRequests(flushTimeout);
            stageRequests.cancelOldRequests(stageTimeout);
            removeRequests.cancelOldRequests(removeTimeout);
        }
    }
}
