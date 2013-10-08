package org.dcache.pool.classic;

import com.google.common.base.Charsets;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.io.Files;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;

import javax.annotation.PreDestroy;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.net.URISyntaxException;
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
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.CacheFileAvailable;
import diskCacheV111.util.DiskErrorCacheException;
import diskCacheV111.util.FileInCacheException;
import diskCacheV111.util.FileNotInCacheException;
import diskCacheV111.util.HsmLocationExtractorFactory;
import diskCacheV111.util.HsmRunSystem;
import diskCacheV111.util.HsmSet;
import diskCacheV111.util.JobScheduler;
import diskCacheV111.util.PnfsHandler;
import diskCacheV111.util.PnfsId;
import diskCacheV111.util.Queable;
import diskCacheV111.util.SimpleJobScheduler;
import diskCacheV111.util.TimeoutCacheException;
import diskCacheV111.vehicles.PoolFileFlushedMessage;
import diskCacheV111.vehicles.PoolRemoveFilesFromHSMMessage;
import diskCacheV111.vehicles.StorageInfo;
import diskCacheV111.vehicles.StorageInfoMessage;
import diskCacheV111.vehicles.StorageInfos;

import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.DelayedReply;
import dmg.cells.nucleus.NoRouteToCellException;
import dmg.util.command.Argument;
import dmg.util.command.Command;
import dmg.util.command.Option;

import org.dcache.cells.AbstractCellComponent;
import org.dcache.cells.CellStub;
import org.dcache.cells.CellCommandListener;
import org.dcache.namespace.FileAttribute;
import org.dcache.pool.repository.EntryState;
import org.dcache.pool.repository.IllegalTransitionException;
import org.dcache.pool.repository.ReplicaDescriptor;
import org.dcache.pool.repository.Repository;
import org.dcache.pool.repository.Repository.OpenFlags;
import org.dcache.pool.repository.StickyRecord;
import org.dcache.util.Checksum;
import org.dcache.util.FireAndForgetTask;
import org.dcache.vehicles.FileAttributes;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.getFirst;

public class HsmStorageHandler2
    extends AbstractCellComponent implements CellCommandListener
{
    private static final Logger LOGGER =
        LoggerFactory.getLogger(HsmStorageHandler2.class);

    private static final int MAX_LINES = 200;

    private final Map<PnfsId, StoreThread> _storePnfsidList = new HashMap<>();
    private final Map<PnfsId, FetchThread> _restorePnfsidList = new HashMap<>();

    private final JobScheduler _fetchQueue;
    private final JobScheduler _storeQueue;
    private final Executor _hsmRemoveExecutor =
        Executors.newSingleThreadExecutor();
    private final ThreadPoolExecutor _hsmRemoveTaskExecutor =
        new ThreadPoolExecutor(1, 1, Long.MAX_VALUE, TimeUnit.NANOSECONDS,
                               new LinkedBlockingQueue<Runnable>());

    private long _maxStoreRun = TimeUnit.HOURS.toMillis(4);
    private long _maxRestoreRun = TimeUnit.HOURS.toMillis(4);
    private long _maxRemoveRun = TimeUnit.HOURS.toMillis(4);

    private Repository _repository;
    private HsmSet _hsmSet;
    private PnfsHandler _pnfs;
    private ChecksumModule _checksumModule;
    private String _flushMessageTarget;
    private CellStub _billingStub;

    private abstract class Info implements Queable
    {
        private final List<CacheFileAvailable> _callbacks = new ArrayList<>();
        private final PnfsId _pnfsId;
        private long _startTime = System.currentTimeMillis();
        protected Thread _thread;
        private boolean _killed;

        private Info(PnfsId pnfsId)
        {
            _pnfsId = pnfsId;
        }

        public PnfsId getPnfsId()
        {
            return _pnfsId;
        }

        public int getListenerCount()
        {
            return _callbacks.size();
        }

        public long getStartTime()
        {
            return _startTime;
        }

        synchronized void addCallback(CacheFileAvailable callback)
        {
            _callbacks.add(callback);
        }

        synchronized void executeCallbacks(Throwable exc)
        {
            LOGGER.trace("executing callbacks {} (callbacks={}) {}", _pnfsId, _callbacks, exc);
            for (CacheFileAvailable callback : _callbacks) {
                try {
                    callback.cacheFileAvailable(_pnfsId, exc);
                } catch (RuntimeException e) {
                    LOGGER.error("Exception in callback to " + callback.getClass().getName() +
                            ". Please report to support@dcache.org.", e);
                }
            }
        }

        protected synchronized void setThread(Thread thread)
                throws InterruptedException
        {
            if (_killed) {
                throw new InterruptedException("Killed before script could start");
            }
            _thread = thread;
        }

        @Override
        public synchronized void kill()
        {
            _killed = true;
            if (_thread != null) {
                _thread.interrupt();
            }
        }

        @Override
        public String toString()
        {
            return _pnfsId.toString() + "  " + getListenerCount() + " " + new Date(getStartTime());
        }
    }

    public HsmStorageHandler2()
    {
        _fetchQueue = new SimpleJobScheduler("fetch");
        _storeQueue = new SimpleJobScheduler("store");
    }

    @Required
    public void setRepository(Repository repository)
    {
        _repository = repository;
    }

    @Required
    public void setHsms(HsmSet hsmSet)
    {
        _hsmSet = hsmSet;
    }

    @Required
    public void setPnfsHandler(PnfsHandler pnfs)
    {
        _pnfs = pnfs;
    }

    @Required
    public void setChecksumModule(ChecksumModule checksumModule)
    {
        _checksumModule = checksumModule;
    }

    @Required
    public void setFlushMessageTarget(String flushMessageTarget)
    {
        _flushMessageTarget = flushMessageTarget;
    }

    @PreDestroy
    public void shutdown()
    {
        _fetchQueue.shutdown();
        _storeQueue.shutdown();
    }

    @Required
    public void setBillingStub(CellStub billingStub)
    {
        _billingStub = billingStub;
    }

    @Override
    public synchronized void printSetup(PrintWriter pw)
    {
        pw.println("#");
        pw.println("# HsmStorageHandler2(" + getClass().getName() + ")");
        pw.println("#");
        pw.println("rh set max active " + _fetchQueue.getMaxActiveJobs());
        pw.println("st set max active " + _storeQueue.getMaxActiveJobs());
        pw.println("rm set max active " + getMaxRemoveJobs());
        pw.println("rh set timeout " + (_maxRestoreRun / 1000L));
        pw.println("st set timeout " + (_maxStoreRun / 1000L));
        pw.println("rm set timeout " + (_maxRemoveRun / 1000L));
    }

    @Override
    public synchronized void getInfo(PrintWriter pw)
    {
        pw.println(" Restore Timeout  : " + (_maxRestoreRun / 1000L));
        pw.println("   Store Timeout  : " + (_maxStoreRun / 1000L));
        pw.println("  Remove Timeout  : " + (_maxRemoveRun / 1000L));
        pw.println("  Job Queues ");
        pw.println("    to store   " + _storeQueue.getActiveJobs() +
                    "(" + _storeQueue.getMaxActiveJobs() +
                    ")/" + _storeQueue.getQueueSize());
        pw.println("    from store " + _fetchQueue.getActiveJobs() +
                "(" + _fetchQueue.getMaxActiveJobs() +
                ")/" + _fetchQueue.getQueueSize());
        pw.println("    delete     " +
                "(" + getMaxRemoveJobs() +
                ")/" + "");
    }

    private synchronized String
        getSystemCommand(File file, FileAttributes fileAttributes,
                         HsmSet.HsmInfo hsm, String direction)
    {
        PnfsId pnfsId = fileAttributes.getPnfsId();
        StorageInfo storageInfo = StorageInfos.extractFrom(fileAttributes);

        String hsmCommand = hsm.getAttribute("command");
        if (hsmCommand == null) {
            throw new IllegalArgumentException("hsmCommand not specified in HsmSet");
        }

        String localPath = file.getPath();

        StringBuilder sb = new StringBuilder();

        sb.append(hsmCommand).append(" ").
            append(direction).append(" ").
            append(pnfsId).append("  ").
            append(localPath);

        sb.append(" -si=").append(storageInfo.toString());
        for (Map.Entry<String,String> attr : hsm.attributes()) {
            String key = attr.getKey();
            String val = attr.getValue();
            sb.append(" -").append(key);
            if (!Strings.isNullOrEmpty(val)) {
                sb.append("=").append(val);
            }
        }

        for (URI location: storageInfo.locations()) {
            if (location.getScheme().equals(hsm.getType()) && location.getAuthority().equals(hsm.getInstance())) {
                sb.append(" -uri=").append(location.toString());
            }
        }

        String completeCommand = sb.toString();
        LOGGER.debug("HSM_COMMAND: {}", completeCommand);
        return completeCommand;
    }

    public int getMaxActiveFetchJobs()
    {
        return _fetchQueue.getMaxActiveJobs();
    }

    public int getActiveFetchJobs()
    {
        return _fetchQueue.getActiveJobs();
    }

    public int getFetchQueueSize()
    {
        return _fetchQueue.getQueueSize();
    }

    public synchronized void fetch(FileAttributes fileAttributes,
                                   CacheFileAvailable callback)
        throws FileInCacheException, CacheException
    {
        FetchThread info = _restorePnfsidList.get(fileAttributes.getPnfsId());
        if (info == null) {
            info = new FetchThread(fileAttributes);
            try {
                _fetchQueue.add(info);
                _restorePnfsidList.put(fileAttributes.getPnfsId(), info);
            } catch (InvocationTargetException e) {
                /* This happens when the queued method of the FetchThread
                 * throws an exception. They have been designed not to
                 * throw any exceptions, so if this happens it must be a
                 * bug.
                 */
                throw new RuntimeException("Failed to queue fetch request",
                        e.getCause());
            }
        }
        if (callback != null) {
            info.addCallback(callback);
        }
    }

    private synchronized void removeFetchEntry(PnfsId id)
    {
        _restorePnfsidList.remove(id);
    }

    private class FetchThread extends Info
    {
        private final ReplicaDescriptor _handle;
        private final StorageInfoMessage _infoMsg;
        private long _timestamp;

        public FetchThread(FileAttributes fileAttributes)
            throws CacheException, FileInCacheException
        {
            super(fileAttributes.getPnfsId());
            _infoMsg = new StorageInfoMessage(getCellAddress().toString(), fileAttributes.getPnfsId(), true);
            _infoMsg.setStorageInfo(fileAttributes.getStorageInfo());
            _infoMsg.setFileSize(fileAttributes.getSize());
            _handle = _repository.createEntry(
                    fileAttributes,
                    EntryState.FROM_STORE,
                    EntryState.CACHED,
                    Collections.<StickyRecord>emptyList(),
                    EnumSet.noneOf(OpenFlags.class));
        }

        /**
         * Returns the name of an HSM accessible for this pool and which
         * contains the given file. Returns null if no such HSM exists.
         */
        private String findAccessibleLocation(FileAttributes fileAttributes)
        {
            StorageInfo file = fileAttributes.getStorageInfo();
            if (file.locations().isEmpty()
                    && _hsmSet.getHsmInstances().contains(file.getHsm())) {
                // This is for backwards compatibility until all info
                // extractors support URIs.
                return file.getHsm();
            } else {
                for (URI location : file.locations()) {
                    if (_hsmSet.getHsmInstances().contains(location.getAuthority())) {
                        return location.getAuthority();
                    }
                }
            }
            return null;
        }

        private String getFetchCommand(File file, FileAttributes fileAttributes)
        {
            String instance = findAccessibleLocation(fileAttributes);
            if (instance == null) {
                throw new IllegalArgumentException("HSM not defined on this pool: " +
                        fileAttributes.getStorageInfo().locations());
            }
            HsmSet.HsmInfo hsm = _hsmSet.getHsmInfoByName(instance);
            LOGGER.trace("getFetchCommand for {} on HSM {}", fileAttributes, instance);
            return getSystemCommand(file, fileAttributes, hsm, "get");
        }

        @Override
        public void queued(int id)
        {
            _timestamp = System.currentTimeMillis();
        }

        private void sendBillingInfo()
        {
            try {
                _billingStub.send(_infoMsg);
            } catch (NoRouteToCellException e) {
                LOGGER.error("Failed to send message to billing: {}", e.getMessage());
            }
        }

        @Override
        public void unqueued()
        {
            PnfsId pnfsId = getPnfsId();

            try {
                LOGGER.debug("Dequeuing {}", pnfsId);
                _handle.close();
            } finally {
                removeFetchEntry(pnfsId);

                CacheException e =
                    new CacheException(33, "Job dequeued (by operator)");

                executeCallbacks(e);

                _infoMsg.setTimeQueued(System.currentTimeMillis() - _timestamp);
                _infoMsg.setResult(e.getRc(), e.getMessage());
                sendBillingInfo();
            }
        }

        @Override
        public void run()
        {
            Exception excep = null;
            PnfsId pnfsId = getPnfsId();
            FileAttributes attributes = _handle.getFileAttributes();

            try {
                setThread(Thread.currentThread());
                try {
                    LOGGER.trace("FetchThread started");

                    long now = System.currentTimeMillis();
                    _infoMsg.setTimeQueued(now - _timestamp);
                    _timestamp = now;

                    String fetchCommand =
                        getFetchCommand(_handle.getFile(), attributes);
                    long fileSize = attributes.getSize();

                    LOGGER.debug("Waiting for space ({} bytes)", fileSize);
                    _handle.allocate(fileSize);
                    LOGGER.debug("Got Space ({} bytes)", fileSize);

                    new HsmRunSystem(fetchCommand, MAX_LINES, _maxRestoreRun).execute();

                    doChecksum(_handle);
                } finally {
                    /* Surpress thread interrupts after this point.
                     */
                    setThread(null);
                    Thread.interrupted();
                }
                _handle.commit();
                LOGGER.info("File successfully restored from tape");
            } catch (CacheException e) {
                LOGGER.error(e.toString());
                excep = e;
            } catch (InterruptedException e) {
                LOGGER.error("Process interrupted (timed out)");
                excep = new TimeoutCacheException("HSM script was killed (" + e.getMessage() + ")", e);
            } catch (IOException e) {
                LOGGER.error("Process got an I/O error: {}", e.toString());
                excep = e;
            } catch (IllegalThreadStateException  e) {
                LOGGER.error("Cannot stop process: {}", e.toString());
                excep = e;
            } catch (IllegalArgumentException e) {
                LOGGER.error("Cannot determine 'hsmInfo': {}", e.getMessage());
                excep = e;
            } catch (RuntimeException e) {
                LOGGER.error(e.toString(), e);
                excep = e;
            } finally {
                _handle.close();
                removeFetchEntry(pnfsId);
                executeCallbacks(excep);

                if (excep != null) {
                    if (excep instanceof CacheException) {
                        _infoMsg.setResult(((CacheException)excep).getRc(),
                                           excep.getMessage());
                    } else {
                        _infoMsg.setResult(44, excep.toString());
                    }
                }
                _infoMsg.setTransferTime(System.currentTimeMillis() - _timestamp);
                sendBillingInfo();
            }
        }

        private void doChecksum(ReplicaDescriptor handle)
            throws CacheException, InterruptedException
        {
            try {
                if (_checksumModule.hasPolicy(ChecksumModule.PolicyFlag.GET_CRC_FROM_HSM)) {
                    readChecksumFromHsm(handle);
                }
                _checksumModule.enforcePostRestorePolicy(handle);
            } catch (IOException e) {
                throw new DiskErrorCacheException("Checksum calculation failed due to I/O error: " + e.getMessage(), e);
            } catch (NoSuchAlgorithmException | CacheException e) {
                throw new CacheException(1010, "Checksum calculation failed: " + e.getMessage(), e);
            }
        }

        private void readChecksumFromHsm(ReplicaDescriptor handle)
                throws IOException, CacheException
        {
            File file = new File(handle.getFile().getCanonicalPath() + ".crcval");
            try {
                if (file.exists()) {
                    try {
                        String firstLine = Files.readFirstLine(file, Charsets.US_ASCII);
                        if (firstLine != null) {
                            Checksum checksum = Checksum.parseChecksum("1:" + firstLine);
                            LOGGER.info("Obtained checksum {} for {} from HSM", checksum, getPnfsId());
                            handle.addChecksums(Collections.singleton(checksum));
                        }
                    } finally {
                        file.delete();
                    }
                }
            } catch (FileNotFoundException e) {
                /* Should not happen unless somebody else is removing
                 * the file before we got a chance to read it.
                 */
                throw Throwables.propagate(e);
            }
        }
    }

    public synchronized int getMaxRemoveJobs()
    {
        return _hsmRemoveTaskExecutor.getMaximumPoolSize();
    }

    public synchronized void remove(CellMessage message)
    {
        assert message.getMessageObject() instanceof PoolRemoveFilesFromHSMMessage;

        HsmRemoveTask task =
            new HsmRemoveTask(getCellEndpoint(),
                              _hsmRemoveTaskExecutor,
                              _hsmSet, _maxRemoveRun, message);
        _hsmRemoveExecutor.execute(new FireAndForgetTask(task));
    }

    public int getMaxActiveStoreJobs()
    {
        return _storeQueue.getMaxActiveJobs();
    }

    public int getActiveStoreJobs()
    {
        return _storeQueue.getActiveJobs();
    }

    public int getStoreQueueSize()
    {
        return _storeQueue.getQueueSize();
    }

    public synchronized boolean store(PnfsId pnfsId, CacheFileAvailable callback)
        throws CacheException, InterruptedException
    {
        LOGGER.trace("store requested for {} {} callback",
                pnfsId, (callback == null) ? " w/o " : " with ");

        if (_repository.getState(pnfsId) == EntryState.CACHED) {
            LOGGER.debug("is already cached {}", pnfsId);
            return true;
        }

        StoreThread info = _storePnfsidList.get(pnfsId);
        if (info != null) {
            if (callback != null) {
                info.addCallback(callback);
            }
            LOGGER.debug("flush already in progress {} (callback={})", pnfsId, callback);
            return false;
        }

        info = new StoreThread(pnfsId);
        if (callback != null) {
            info.addCallback(callback);
        }

        try {
            _storeQueue.add(info);
        } catch (InvocationTargetException  e) {
            throw new RuntimeException("Failed to queue store request",
                                       e.getCause());
        }

        _storePnfsidList.put(pnfsId, info);
        LOGGER.debug("added to flush queue {} (callback={})", pnfsId, callback);
        return false;
    }

    protected synchronized void removeStoreEntry(PnfsId id)
    {
        _storePnfsidList.remove(id);
    }

    private class StoreThread extends Info
    {
        private final StorageInfoMessage _infoMsg;
        private long _timestamp;

	public StoreThread(PnfsId pnfsId)
        {
	    super(pnfsId);
            _infoMsg = new StorageInfoMessage(getCellAddress().toString(), pnfsId, false);
	}

        private String getStoreCommand(File file, FileAttributes fileAttributes)
        {
            StorageInfo storageInfo = fileAttributes.getStorageInfo();
            String hsmType = storageInfo.getHsm();
            LOGGER.trace("getStoreCommand for pnfsid={};hsm={};si={}",
                    fileAttributes.getPnfsId(), hsmType, storageInfo);

            // If multiple HSMs are defined for the given type, then we
            // currently pick the first. We may consider randomizing this
            // choice.
            HsmSet.HsmInfo hsm = getFirst(_hsmSet.getHsmInfoByType(hsmType), null);
            if (hsm == null) {
                throw new IllegalArgumentException("Info not found for : " + hsmType);
            }
            return getSystemCommand(file, fileAttributes, hsm, "put");
        }

        @Override
        public void queued(int id)
        {
            _timestamp = System.currentTimeMillis();
        }

        private void sendBillingInfo()
        {
            try {
                _billingStub.send(_infoMsg);
            } catch (NoRouteToCellException e) {
                LOGGER.error("Failed to send message to billing: {}", e.getMessage());
            }
        }

        @Override
        public void unqueued()
        {
            removeStoreEntry(getPnfsId());

            CacheException e =
                new CacheException(44, "Job dequeued (by operator)");

            executeCallbacks(e);

            _infoMsg.setTimeQueued(System.currentTimeMillis() - _timestamp);
            _infoMsg.setResult(e.getRc(), e.getMessage());
            sendBillingInfo();
        }

	@Override
        public void run()
        {
            PnfsId pnfsId = getPnfsId();
            Throwable excep = null;

            try {
                setThread(Thread.currentThread());

                LOGGER.trace("Store thread started {}", _thread);

                /* Check if name space entry still exists. If the name
                 * space entry was deleted, then we delete the file on
                 * the pool right away.
                 */
                try {
                    LOGGER.debug("Checking if file still exists");
                    _pnfs.getFileAttributes(pnfsId, EnumSet.noneOf(FileAttribute.class));
                } catch (CacheException e) {
                    switch (e.getRc()) {
                    case CacheException.FILE_NOT_FOUND:
                        try {
                            _repository.setState(pnfsId, EntryState.REMOVED);
                            LOGGER.info("File not found in name space; removed {}", pnfsId);
                        } catch (IllegalTransitionException f) {
                            LOGGER.error("File not found in name space, but failed to remove {}: {}", pnfsId, f);
                        }
                        break;

                    case CacheException.NOT_IN_TRASH:
                        LOGGER.warn("File no longer appears in the name space; the pool can however not confirm that it has been deleted and will thus not remove the file");
                        break;
                    }
                    throw e;
                }

                Set<OpenFlags> flags = Collections.emptySet();
                ReplicaDescriptor handle = _repository.openEntry(pnfsId, flags);
                FileAttributes fileAttributesForNotification = new FileAttributes();
                try {
                    _checksumModule.enforcePreFlushPolicy(handle);

                    FileAttributes fileAttributes = handle.getFileAttributes();
                    StorageInfo storageInfo = fileAttributes.getStorageInfo().clone();
                    _infoMsg.setStorageInfo(storageInfo);
                    _infoMsg.setFileSize(fileAttributes.getSize());
                    long now = System.currentTimeMillis();
                    _infoMsg.setTimeQueued(now - _timestamp);
                    _timestamp = now;

                    String storeCommand =
                        getStoreCommand(handle.getFile(), fileAttributes);

                    String output = new HsmRunSystem(storeCommand, MAX_LINES, _maxStoreRun).execute();
                    for (String uri : Splitter.on("\n").trimResults().omitEmptyStrings().split(output)) {
                        try {
                            URI location = new URI(uri);
                            HsmLocationExtractorFactory.validate(location);
                            storageInfo.addLocation(location);
                            storageInfo.isSetAddLocation(true);
                            LOGGER.debug("{}: added HSM location {}", pnfsId, location);
                        } catch (IllegalArgumentException | URISyntaxException e) {
                            LOGGER.error("HSM script produced BAD URI: {}", uri);
                            throw new CacheException(2, e.getMessage(), e);
                        }
                    }

                    fileAttributesForNotification.setAccessLatency(fileAttributes.getAccessLatency());
                    fileAttributesForNotification.setRetentionPolicy(fileAttributes
                            .getRetentionPolicy());
                    fileAttributesForNotification.setStorageInfo(storageInfo);
                    fileAttributesForNotification.setSize(fileAttributes.getSize());
                } finally {
                    /* Suppress thread interruptions after this point.
                     */
                    setThread(null);
                    Thread.interrupted();

                    handle.close();
                }

                while (true) {
                    try {
                        _pnfs.fileFlushed(pnfsId, fileAttributesForNotification);
                        break;
                    } catch (CacheException e) {
                        if (e.getRc() == CacheException.FILE_NOT_FOUND ||
                                e.getRc() == CacheException.NOT_IN_TRASH) {
                            /* In case the file was deleted, we are
                             * presented with the problem that the
                             * file is now on tape, however the
                             * location has not been registered
                             * centrally. Hence the copy on tape will
                             * not be removed by the HSM cleaner. The
                             * sensible thing seems to be to remove
                             * the file from tape here. For now we
                             * ignore this issue (REVISIT).
                             */
                            break;
                        }

                        /* The message to the PnfsManager
                         * failed. There are several possible
                         * reasons for this; we may have lost the
                         * connection to the PnfsManager; the
                         * PnfsManager may have lost its
                         * connection to PNFS or otherwise be in
                         * trouble; bugs; etc.
                         *
                         * We keep retrying until we succeed. This
                         * will effectively block this thread from
                         * flushing any other files, which seems
                         * sensible when we have trouble talking
                         * to the PnfsManager. If the pool crashes
                         * or gets restarted while waiting here,
                         * we will end up flushing the file
                         * again. We assume that the HSM script is
                         * able to eliminate the duplicate; or at
                         * least tolerate the duplicate (given
                         * that this situation should be rare, we
                         * can live with a little bit of wasted
                         * tape).
                         */
                        LOGGER.error("Error notifying PNFS about a flushed file: {} ({})",
                                e.getMessage(), e.getRc());
                    }
                    Thread.sleep(120000); // 2 minutes
                }

                notifyFlushMessageTarget(fileAttributesForNotification);

                LOGGER.info("File successfully stored to tape");

                _repository.setState(pnfsId, EntryState.CACHED);
            } catch (IllegalTransitionException e) {
                /* Apparently the file is no longer precious. Most
                 * likely it got deleted, which is fine, since the
                 * flush already succeeded.
                 */
            } catch (FileNotInCacheException e) {
                /* File was deleted before we could flush it. No harm
                 * done.
                 */
                _infoMsg.setResult(e.getRc(),
                                   "Flush aborted because file was deleted");
            } catch (CacheException e) {
                excep = e;
                LOGGER.error("Error while flushing to tape: {}", e.toString());
                _infoMsg.setResult(e.getRc(), e.getMessage());
            } catch (InterruptedException e) {
                excep = e;
                LOGGER.error("Process interrupted (timed out)");
                _infoMsg.setResult(1, "Flush timed out");
            } catch (IOException e) {
                excep = e;
                LOGGER.error("Process got an I/O error: {}", e.toString());
                _infoMsg.setResult(2, "IO Error: " + e.getMessage());
            } catch (IllegalThreadStateException e) {
                excep = e;
                LOGGER.error("Cannot stop process: {}", e.toString());
                _infoMsg.setResult(3, e.getMessage());
            } catch (IllegalArgumentException e) {
                excep = e;
                LOGGER.error("Cannot determine 'hsmInfo': {}", e.getMessage());
                _infoMsg.setResult(4, e.getMessage());
            } catch (Throwable t) {
                excep = t;
                LOGGER.error("Unexpected exception", t);
                _infoMsg.setResult(666, t.getMessage());
            } finally {
                removeStoreEntry(pnfsId);
                _infoMsg.setTransferTime(System.currentTimeMillis() - _timestamp);
                sendBillingInfo();
                executeCallbacks(excep);
            }
        }

        private void notifyFlushMessageTarget(FileAttributes fileAttributes)
        {
            try {
                PoolFileFlushedMessage poolFileFlushedMessage =
                    new PoolFileFlushedMessage(getCellName(), getPnfsId(), fileAttributes);
                poolFileFlushedMessage.setReplyRequired(false);
                CellMessage msg =
                    new CellMessage(new CellPath(_flushMessageTarget),
                                    poolFileFlushedMessage);
                sendMessage(msg);
            } catch (NoRouteToCellException e) {
                LOGGER.info("Failed to send message to {}: {}",
                        _flushMessageTarget, e.getMessage());
            }
        }
    }

    @Command(name = "rh set timeout",
            hint = "set restore timeout",
            usage = "Set restore timeout for the HSM script. When the timeout expires " +
                    "the HSM script is killed.")
    class RestoreSetTimeoutCommand implements Callable<String>
    {
        @Argument(metaVar = "seconds")
        long timeout;

        @Override
        public String call()
        {
            synchronized (HsmStorageHandler2.this) {
                _maxRestoreRun = TimeUnit.SECONDS.toMillis(timeout);
            }
            return "";
        }
    }

    @Command(name = "rh set max active",
            hint = "set restore concurrency",
            usage = "Set the maximum number of restore instances of the HSM script to " +
                    "start. Additional restore requests will be queued.")
    class RestoreSetMaxActiveCommand implements Callable<String>
    {
        @Argument
        int jobs;

        @Override
        public String call() throws IllegalArgumentException
        {
            checkArgument(jobs >= 0, "JOBS must be non-negative");
            _fetchQueue.setMaxActiveJobs(jobs);
            return "Max Active Hsm Restore Processes set to " + jobs;
        }
    }

    @Command(name = "rh kill",
            hint = "kill restore request",
            usage = "Remove an HSM restore request.")
    class RestoreKillCommand implements Callable<String>
    {
        @Argument
        int jobId;

        @Option(name = "force", usage = "Terminate the HSM script if it is running.")
        boolean force;

        @Override
        public String call() throws NoSuchElementException, IllegalStateException
        {
            _fetchQueue.kill(jobId, force);
            return "Kill initialized";
        }
    }

    @Command(name = "rh ls",
            hint = "list restore queue",
            usage = "List the HSM requests on the restore queue.\n\n" +
                    "The columns in the output show: job id, job status, pnfs id, request counter, " +
                    "and request submission time.")
    class RestoreListCommand implements Callable<String>
    {
        @Override
        public String call()
        {
            return _fetchQueue.printJobQueue();
        }
    }

    @Command(name = "st set timeout",
            hint = "set store timeout",
            usage = "Set store timeout for the HSM script. When the timeout expires " +
                    "the HSM script is killed.")
    class StoreSetTimeoutCommand implements Callable<String>
    {
        @Argument(metaVar = "seconds")
        long timeout;

        @Override
        public String call()
        {
            synchronized (HsmStorageHandler2.this) {
                _maxStoreRun = TimeUnit.SECONDS.toMillis(timeout);
            }
            return "";
        }
    }

    @Command(name = "st set max active",
            hint = "set store concurrency",
            usage = "Set the maximum number of store instances of the HSM script to " +
                    "start. Additional store requests will be queued.")
    class StoreSetMaxActiveCommand implements Callable<String>
    {
        @Argument
        int jobs;

        @Override
        public String call() throws IllegalArgumentException
        {
            checkArgument(jobs >= 0, "limit must be non-negative");
            _storeQueue.setMaxActiveJobs(jobs);
            return "Max active HSM store processes set to " + jobs;
        }
    }

    @Command(name = "st kill",
            hint = "kill store request",
            usage = "Remove an HSM store request.")
    class StoreKillCommand implements Callable<String>
    {
        @Argument
        int jobId;

        @Option(name = "force", usage = "Terminate the HSM script if it is running.")
        boolean force;

        @Override
        public String call() throws NoSuchElementException, IllegalStateException
        {
            _storeQueue.kill(jobId, force);
            return "Kill initialized";
        }
    }

    @Command(name = "st ls",
            hint = "list store queue",
            usage = "List the HSM requests on the store queue.\n\n" +
                    "The columns in the output show: job id, job status, pnfs id, request counter, " +
                    "and request submission time.")
    class StoreListCommand implements Callable<String>
    {
        @Override
        public String call()
        {
            return _storeQueue.printJobQueue();
        }
    }

    @Command(name = "rm set timeout",
            hint = "set tape remove timeout",
            usage = "Set remove timeout for the HSM script. When the timeout expires " +
                    "the HSM script is killed.")
    class RemoveSetTimeoutCommand implements Callable<String>
    {
        @Argument(metaVar = "seconds")
        long timeout;

        @Override
        public String call()
        {
            synchronized (HsmStorageHandler2.this) {
                _maxRemoveRun = TimeUnit.SECONDS.toMillis(timeout);
            }
            return "";
        }
    }

    @Command(name = "rm set max active",
            hint = "set remoe concurrency",
            usage = "Set the maximum number of remove instances of the HSM script to " +
                    "start. Additional remove requests will be queued.")
    class RemoveSetMaxActiveCommand implements Callable<String>
    {
        @Argument
        int jobs;

        @Override
        public String call() throws IllegalArgumentException
        {
            checkArgument(jobs >= 0, "Limit must be non-negative");
            _hsmRemoveTaskExecutor.setCorePoolSize(jobs);
            _hsmRemoveTaskExecutor.setMaximumPoolSize(jobs);
            return "Max active remover processes set to " + jobs;
        }
    }

    @Command(name = "rh restore",
            hint = "restore file from tape",
            usage = "Restore a file from tape.")
    class RestoreCommand extends DelayedReply implements Callable<Serializable>
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
        public Serializable call()
        {
            final CacheFileAvailable cfa = new CacheFileAvailable() {
                @Override
                public void cacheFileAvailable(PnfsId pnfsId, Throwable ee) {
                    if (ee == null) {
                        reply("Fetched " + pnfsId);
                    } else {
                        reply("Failed to fetch " + pnfsId + ": " + ee);
                    }
                }
            };

            /* We need to fetch the storage info and we don't want to
             * block the message thread while waiting for the reply.
             */
            Thread t = new Thread("rh restore") {
                @Override
                public void run() {
                    try {
                        FileAttributes attributes = _pnfs.getStorageInfoByPnfsId(pnfsId).getFileAttributes();
                        fetch(attributes, block ? cfa : null);
                    } catch (CacheException e) {
                        cfa.cacheFileAvailable(pnfsId, e);
                    }
                }
            };
            t.start();

            return block ? this : "Fetch request queued";
        }
    }
}
