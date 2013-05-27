package org.dcache.pool.classic;

import com.google.common.collect.Ordering;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import diskCacheV111.pools.StorageClassFlushInfo;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.CacheFileAvailable;
import diskCacheV111.util.PnfsId;

import org.dcache.commons.util.NDC;
import org.dcache.pool.repository.CacheEntry;

/**
 * Holds the files to flush for a particular storage class.
 */
public class StorageClassInfo implements CacheFileAvailable
{
    private static final Logger LOGGER = LoggerFactory.getLogger(StorageClassInfo.class);

    private static class Entry implements Comparable<Entry>
    {
        final PnfsId pnfsId;
        final long timeStamp;
        final long size;

        Entry(CacheEntry entry)
        {
            pnfsId = entry.getPnfsId();
            timeStamp = entry.getCreationTime();
            size = entry.getReplicaSize();
        }

        @Override
        public int compareTo(Entry entry)
        {
            try {
                return (timeStamp > entry.timeStamp) ? 1 :
                        (timeStamp == entry.timeStamp) ? pnfsId.compareTo(entry.pnfsId) : -1;
            } catch (Exception e) {
                return -1;
            }
        }
    }

    private final Map<PnfsId, Entry> _requests = new HashMap<>();
    private final Map<PnfsId, Entry> _failedRequests = new HashMap<>();
    private final String _storageClass;
    private final String _hsmName;
    private final Executor _callbackExecutor = Executors.newSingleThreadExecutor();

    private boolean _defined;
    private long _expiration; // expiration time in millis since _time
    private long _maxTotalSize;
    private int _pending;

    private long _time; // creation time of oldest file or 0L.
    private long _totalSize;
    private int _activeCounter;
    private boolean _suspended;
    private int _errorCounter;
    private long _lastSubmittedAt;
    private long _recentFlushId;
    private int _requestsSubmitted;
    private StorageClassInfoFlushable _flushCallback;

    public StorageClassInfo(String hsmName, String storageClass)
    {
        _storageClass = storageClass;
        _hsmName = hsmName.toLowerCase();
    }

    public synchronized StorageClassFlushInfo getFlushInfo()
    {
        StorageClassFlushInfo info = new StorageClassFlushInfo(_hsmName, _storageClass);
        Collection<Entry> entries = _requests.values();
        if (entries.isEmpty()) {
            info.setOldestFileTimestamp(0);
        } else {
            Entry entry = Collections.min(entries);
            info.setOldestFileTimestamp(entry.timeStamp);
        }
        info.setRequestCount(_requests.size());
        info.setFailedRequestCount(_failedRequests.size());
        info.setExpirationTime(_expiration);
        info.setMaximumPendingFileCount(_pending);
        info.setTotalPendingFileSize(_totalSize);
        info.setMaximumAllowedPendingFileSize(_maxTotalSize);
        info.setActiveCount(_activeCounter);
        info.setSuspended(_suspended);
        info.setErrorCounter(_errorCounter);
        info.setLastSubmittedTime(_lastSubmittedAt);
        info.setFlushId(_recentFlushId);
        return info;
    }

    /**
     * Callback from HSM storage handler.
     */
    @Override
    public synchronized void cacheFileAvailable(PnfsId pnfsId, Throwable error)
    {
        if (error != null) {
            _errorCounter++;
            if (error instanceof CacheException) {
                CacheException cce = (CacheException) error;
                if (cce.getRc() >= 30 && cce.getRc() < 40) {
                    Entry entry = removeRequest(pnfsId);
                    if (entry != null) {
                        _failedRequests.put(entry.pnfsId, entry);
                    }
                    _errorCounter--;
                }
            }
        }

        _activeCounter--;
        if (_activeCounter <= 0) {
            _activeCounter = 0;
            if (_flushCallback != null) {
                _callbackExecutor.execute(new CallbackTask(this, _flushCallback));
            }
        }
    }

    private static class CallbackTask implements Runnable
    {
        private final int _flushErrorCounter;
        private final long _flushId;
        private final int _requests;
        private final StorageClassInfoFlushable _callback;
        private final String _hsm;
        private final String _storageClass;

        private CallbackTask(StorageClassInfo info, StorageClassInfoFlushable callback)
        {
            _hsm = info.getHsm();
            _storageClass = info.getStorageClass();
            _flushErrorCounter = info.getErrorCount();
            _flushId = info._recentFlushId;
            _requests = info._requestsSubmitted;
            _callback = callback;
        }

        @Override
        public void run()
        {
            try {
                _callback.storageClassInfoFlushed(_hsm, _storageClass, _flushId, _requests, _flushErrorCounter);
            } catch (Throwable t) {
                LOGGER.error("Problem in running storageClassInfoFlushed callback : {}", t.toString());
            }
        }
    }

    public synchronized long submit(HsmStorageHandler2 storageHandler, int maxCount,
                                    StorageClassInfoFlushable flushCallback)
    {
        if (_activeCounter > 0) {
            throw new IllegalArgumentException("Is already active");
        }

        _flushCallback = flushCallback;

        List<Entry> entries = Ordering.natural().sortedCopy(_requests.values());
        maxCount = (maxCount <= 0) ? entries.size() : Math.min(entries.size(), maxCount);
        _errorCounter = 0;
        _requestsSubmitted = 0;
        //
        // As long as we are in this loop, we will stuck
        // in the first 'cacheFileAvailable'.
        // (Should not be a problem because the store routine
        // is non blocking.)
        //
        _recentFlushId = _lastSubmittedAt = System.currentTimeMillis();

        try {
            for (Entry entry : entries.subList(0, maxCount)) {
                _requestsSubmitted++;
                NDC.push(entry.pnfsId.toString());
                try {
                    //
                    // if store returns true, it didn't register a
                    // callback and no store is initiated.
                    //
                    if (!storageHandler.store(entry.pnfsId, this)) {
                        _activeCounter++;
                    }
                } catch (CacheException e) {
                    _errorCounter++;
                    LOGGER.error("Problem flushing {}: {}", entry, e.toString());
                } catch (RuntimeException e) {
                    _errorCounter++;
                    LOGGER.error("Problem flushing " + entry + ". Please report to support@dcache.org.", e);
                } finally {
                    NDC.pop();
                }
            }
        } catch (InterruptedException e) {
            _errorCounter++;
            Thread.currentThread().interrupt();
        }

        if (_activeCounter <= 0) {
            _activeCounter = 0;
            if (_flushCallback != null) {
                _callbackExecutor.execute(new CallbackTask(this, _flushCallback));
            }
        }

        return _recentFlushId;
    }

    public synchronized long getLastSubmitted()
    {
        return _lastSubmittedAt;
    }

    public synchronized int getActiveCount()
    {
        return _activeCounter;
    }

    public synchronized int getErrorCount()
    {
        return _errorCounter;
    }

    @Override
    public int hashCode()
    {
        return _storageClass.hashCode() | _hsmName.hashCode();
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
        _requests.put(entry.pnfsId, entry);
        if (_time == 0L || entry.timeStamp < _time) {
            _time = entry.timeStamp;
        }
        _totalSize += entry.size;
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
        StringBuilder sb = new StringBuilder();
        sb.append("SCI=").append(_storageClass).
            append("@").append(_hsmName).
            append(";def=").append(_defined).
            append(";exp=").append(TimeUnit.MILLISECONDS.toSeconds(_expiration)).
            append(";pend=").append(_pending).
            append(";maxTotal=").append(_maxTotalSize).
            append(";waiting=").append(_requests.size());
        return sb.toString();
    }

    public synchronized boolean hasExpired()
    {
        return (_requests.size() > 0) &&
            ((_time + _expiration) < System.currentTimeMillis());
    }

    public synchronized long expiresIn()
    {
        return _time == 0L ? 0L : ((_time + _expiration) - System.currentTimeMillis() ) / 1000;
    }

    public synchronized boolean isFull()
    {
        return (_requests.size() > 0) &&
            ((_requests.size() >= _pending) ||
             (_totalSize >= _maxTotalSize));
    }

    public synchronized void setSuspended(boolean suspended)
    {
        _suspended = suspended;
    }

    public synchronized boolean isSuspended()
    {
        return _suspended;
    }

    public synchronized boolean isTriggered()
    {
        return (hasExpired() || isFull()) && !isSuspended();
    }

    public synchronized String getStorageClass()
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
        _defined = d;
    }

    public synchronized boolean isDefined()
    {
        return _defined;
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
