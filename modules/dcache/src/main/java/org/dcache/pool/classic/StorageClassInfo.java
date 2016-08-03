package org.dcache.pool.classic;

import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Ordering;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.nio.channels.CompletionHandler;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import diskCacheV111.pools.StorageClassFlushInfo;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsId;

import org.dcache.pool.nearline.NearlineStorageHandler;
import org.dcache.pool.repository.CacheEntry;

import static com.google.common.collect.Iterables.transform;
import static java.util.Collections.min;
import static java.util.Collections.singleton;
import static java.util.Comparator.comparingLong;
import static java.util.Objects.requireNonNull;

/**
 * Holds the files to flush for a particular storage class.
 */
public class StorageClassInfo implements CompletionHandler<Void,PnfsId>
{
    private static final Logger LOGGER = LoggerFactory.getLogger(StorageClassInfo.class);

    private static class Entry implements Comparable<Entry>
    {
        final PnfsId pnfsId;
        final long timeStamp;
        final long size;

        Entry(CacheEntry entry)
        {
            pnfsId = requireNonNull(entry.getPnfsId());
            timeStamp = entry.getCreationTime();
            size = entry.getReplicaSize();
        }

        @Nonnull
        PnfsId pnfsId()
        {
            return pnfsId;
        }

        long getTimeStamp()
        {
            return timeStamp;
        }

        @Override
        public int compareTo(Entry entry)
        {
            return ComparisonChain.start()
                    .compare(timeStamp, entry.timeStamp)
                    .compare(pnfsId, entry.pnfsId)
                    .result();
        }
    }

    private final Map<PnfsId, Entry> _requests = new HashMap<>();
    private final Map<PnfsId, Entry> _failedRequests = new HashMap<>();
    private final String _storageClass;
    private final String _hsmName;
    private final NearlineStorageHandler _storageHandler;

    private boolean _isDefined;
    private long _expiration; // expiration time in millis since _time
    private long _maxTotalSize;
    private int _pending;

    /**
     * When flushing, new files are flushed right away.
     */
    private boolean _isOpen;

    /**
     * Suppresses open flushing when true.
     */
    private boolean _isDraining;

    private long _time; // creation time of oldest file or 0L.
    private long _totalSize;
    private int _activeCounter;
    private boolean _isSuspended;
    private int _errorCounter;
    private long _lastSubmittedAt;
    private long _recentFlushId;
    private int _requestsSubmitted;
    private int _maxRequests;
    private StorageClassInfoFlushable _callback;
    private Executor _callbackExecutor;

    public StorageClassInfo(NearlineStorageHandler storageHandler, String hsmName, String storageClass)
    {
        _storageHandler = requireNonNull(storageHandler);
        _storageClass = requireNonNull(storageClass);
        _hsmName = hsmName.toLowerCase();
    }

    public synchronized StorageClassFlushInfo getFlushInfo()
    {
        StorageClassFlushInfo info = new StorageClassFlushInfo(_hsmName, _storageClass);
        Collection<Entry> entries = _requests.values();
        if (entries.isEmpty()) {
            info.setOldestFileTimestamp(0);
        } else {
            Entry entry = min(entries, comparingLong(Entry::getTimeStamp));
            info.setOldestFileTimestamp(entry.timeStamp);
        }
        info.setRequestCount(_requests.size());
        info.setFailedRequestCount(_failedRequests.size());
        info.setExpirationTime(_expiration);
        info.setMaximumPendingFileCount(_pending);
        info.setTotalPendingFileSize(_totalSize);
        info.setMaximumAllowedPendingFileSize(_maxTotalSize);
        info.setActiveCount(_activeCounter);
        info.setSuspended(_isSuspended);
        info.setErrorCounter(_errorCounter);
        info.setLastSubmittedTime(_lastSubmittedAt);
        info.setFlushId(_recentFlushId);
        return info;
    }

    private synchronized Runnable internalCompleted()
    {
        _activeCounter--;
        if (_activeCounter <= 0) {
            _activeCounter = 0;
            if (_callback != null) {
                CallbackTask task = new CallbackTask(_hsmName, _storageClass, _errorCounter,
                                                     _recentFlushId, _requestsSubmitted, _callback);
                Executor executor = _callbackExecutor;
                _callbackExecutor = null;
                _callback = null;
                return () -> executor.execute(task);
            }
        }
        return () -> {};
    }

    private synchronized Runnable internalFailed(Throwable exc, PnfsId pnfsId)
    {
        _errorCounter++;
        if (exc instanceof CacheException) {
            CacheException ce = (CacheException) exc;
            if (ce.getRc() >= 30 && ce.getRc() < 40) {
                Entry entry = removeRequest(pnfsId);
                if (entry != null) {
                    _failedRequests.put(entry.pnfsId(), entry);
                }
                _errorCounter--;
            }
        }
        return internalCompleted();
    }

    @Override
    public void completed(Void nil, PnfsId attachment)
    {
        internalCompleted().run();
    }

    @Override
    public void failed(Throwable exc, PnfsId pnfsId)
    {
        internalFailed(exc, pnfsId).run();
    }

    private static class CallbackTask implements Runnable
    {
        private final int flushErrorCounter;
        private final long flushId;
        private final int requests;
        private final StorageClassInfoFlushable callback;
        private final String hsm;
        private final String storageClass;

        private CallbackTask(String hsm, String storageClass, int flushErrorCounter,
                             long flushId, int requests, StorageClassInfoFlushable callback)
        {
            this.flushErrorCounter = flushErrorCounter;
            this.flushId = flushId;
            this.requests = requests;
            this.callback = callback;
            this.hsm = hsm;
            this.storageClass = storageClass;
        }

        @Override
        public void run()
        {
            try {
                callback.storageClassInfoFlushed(hsm, storageClass, flushId, requests, flushErrorCounter);
            } catch (Throwable e) {
                Thread t = Thread.currentThread();
                t.getUncaughtExceptionHandler().uncaughtException(t, e);
            }
        }
    }

    public long flush(int maxCount, StorageClassInfoFlushable callback, Executor executor)
    {
        long id = System.currentTimeMillis();
        internalFlush(id, maxCount, callback, executor).run();
        return id;
    }

    private synchronized Runnable internalFlush(long id, int maxCount,
                                                StorageClassInfoFlushable callback, Executor executor)
    {
        LOGGER.info("Flushing {}", this);

        if (_activeCounter > 0) {
            throw new IllegalArgumentException("Is already active.");
        }

        _maxRequests = maxCount;

        List<Entry> entries = Ordering.natural().sortedCopy(_requests.values());
        maxCount = Math.min(entries.size(), maxCount);

        _isDraining = false;
        _errorCounter = 0;
        _requestsSubmitted = maxCount;
        _activeCounter = maxCount;
        _recentFlushId = _lastSubmittedAt = id;

        if (maxCount != 0) {
            _callback = callback;
            _callbackExecutor = executor;
            _storageHandler.flush(_hsmName, transform(entries.subList(0, maxCount), Entry::pnfsId), this);
        } else if (callback != null) {
            CallbackTask task = new CallbackTask(_hsmName, _storageClass, 0, id, 0, callback);
            return () -> executor.execute(task);
        }
        return () -> {};
    }

    public synchronized long getLastSubmitted()
    {
        return _lastSubmittedAt;
    }

    public synchronized int getActiveCount()
    {
        return _activeCounter;
    }

    public synchronized boolean isActive()
    {
        return _activeCounter > 0;
    }

    public synchronized int getErrorCount()
    {
        return _errorCounter;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(_storageClass, _hsmName);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (!(obj instanceof StorageClassInfo)) {
            return false;
        }
        StorageClassInfo info = (StorageClassInfo) obj;
        return info._storageClass.equals(_storageClass) && info._hsmName.equals(_hsmName);
    }

    private synchronized void addRequest(Entry entry)
    {
        _requests.put(entry.pnfsId(), entry);
        if (_time == 0L || entry.timeStamp < _time) {
            _time = entry.timeStamp;
        }
        _totalSize += entry.size;

        if (_isOpen && !_isDraining && _activeCounter > 0 && _requestsSubmitted < _maxRequests) {
            _requestsSubmitted++;
            _activeCounter++;
            _storageHandler.flush(_hsmName, singleton(entry.pnfsId()), this);
        }
    }

    private synchronized Entry removeRequest(PnfsId pnfsId)
    {
        Entry entry = _requests.remove(pnfsId);
        if (entry != null) {
            if (_requests.isEmpty()) {
                _time = 0L;
            }
            _totalSize -= entry.size;
        }
        return entry;
    }

    public synchronized void add(CacheEntry entry)
        throws CacheException
    {
        PnfsId pnfsId = entry.getPnfsId();
        if (_failedRequests.containsKey(pnfsId) || _requests.containsKey(pnfsId)) {
            throw new CacheException(44, "Request already added : " + pnfsId);
        }
        addRequest(new Entry(entry));
    }

    public synchronized void activate(PnfsId pnfsId) throws CacheException
    {
        Entry entry = _failedRequests.remove(pnfsId);
        if (entry == null) {
            throw new CacheException("Not a deactivated Request : " + pnfsId);
        }
        addRequest(entry);
    }

    public synchronized void activateAll()
    {
        for (Entry entry : _failedRequests.values()) {
            addRequest(entry);
        }
        _failedRequests.clear();
    }

    public synchronized void deactivate(PnfsId pnfsId) throws CacheException
    {
        Entry entry = removeRequest(pnfsId);
        if (entry == null) {
            throw new CacheException("Not an activated Request : " + pnfsId);
        }
        _failedRequests.put(pnfsId, entry);
    }

    public synchronized boolean remove(PnfsId pnfsId)
    {
        Entry entry = removeRequest(pnfsId);
        if (entry == null) {
            entry = _failedRequests.remove(pnfsId);
        }
        return entry != null;
    }

    public String getHsm()
    {
        return _hsmName;
    }

    public String getFullName()
    {
        return _storageClass + "@" + _hsmName;
    }

    public synchronized Collection<PnfsId> getRequests()
    {
        return new ArrayList<>(_requests.keySet());
    }

    public synchronized Collection<PnfsId> getFailedRequests()
    {
        return new ArrayList<>(_failedRequests.keySet());
    }

    public synchronized String toString()
    {
        return "SCI=" + _storageClass +
               "@" + _hsmName +
               ";def=" + _isDefined +
               ";exp=" + TimeUnit.MILLISECONDS.toSeconds(_expiration) +
               ";pend=" + _pending +
               ";maxTotal=" + _maxTotalSize +
               ";waiting=" + _requests.size();
    }

    public synchronized boolean hasExpired()
    {
        return (!_requests.isEmpty()) &&
               ((_time + _expiration) < System.currentTimeMillis());
    }

    public synchronized long expiresIn()
    {
        return _time == 0L ? 0L : ((_time + _expiration) - System.currentTimeMillis() ) / 1000;
    }

    public synchronized boolean isFull()
    {
        return (!_requests.isEmpty()) &&
               ((_requests.size() >= _pending) ||
             (_totalSize >= _maxTotalSize));
    }

    public synchronized void setSuspended(boolean suspended)
    {
        _isSuspended = suspended;
    }

    public synchronized boolean isSuspended()
    {
        return _isSuspended;
    }

    public synchronized boolean isTriggered()
    {
        return (hasExpired() || isFull()) && !isSuspended();
    }

    public String getStorageClass()
    {
        return _storageClass;
    }

    public synchronized void setTime(long time)
    {
        _time = time;
    }

    public synchronized long getTime()
    {
        return _time;
    }

    public synchronized void setDefined(boolean d)
    {
        _isDefined = d;
    }

    public synchronized boolean isDefined()
    {
        return _isDefined;
    }

    public synchronized void setOpen(boolean isOpen)
    {
        _isOpen = isOpen;
    }

    public synchronized boolean isOpen()
    {
        return _isOpen;
    }

    public synchronized void drain()
    {
        _isDraining = true;
    }

    public synchronized int size()
    {
        return _requests.size() + _failedRequests.size();
    }

    public synchronized long getTotalSize()
    {
        return _totalSize;
    }

    /**
     * Sets the maximum age of a request before a flush to tape will be triggered.
     *
     * @param expiration Maximum age in milliseconds
     */
    public synchronized void setExpiration(long expiration)
    {
        _expiration = expiration;
    }

    /**
     * Sets the maximum number of requests before a flush to tape will be triggered.
     *
     * @param pending Maximum number of requests
     */
    public synchronized void setPending(int pending)
    {
        _pending = pending;
    }

    /**
     * Sets the maximum accumulated size of requests before a flush to tape will be triggered.
     *
     * @param maxTotalSize Maximum size in bytes
     */
    public synchronized void setMaxSize(long maxTotalSize)
    {
        _maxTotalSize = maxTotalSize;
    }

    public synchronized long getMaxSize()
    {
        return _maxTotalSize;
    }

    public synchronized int getPending()
    {
        return _pending;
    }

    public synchronized long getExpiration()
    {
        return _expiration;
    }

    public synchronized int getRequestCount()
    {
        return _requests.size();
    }

    public synchronized int getFailedRequestCount()
    {
        return _failedRequests.size();
    }
}
