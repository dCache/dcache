package org.dcache.pool.classic;

import static java.util.Collections.min;
import static java.util.Collections.singleton;
import static java.util.Comparator.comparingLong;
import static java.util.Objects.requireNonNull;

import diskCacheV111.pools.StorageClassFlushInfo;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsId;
import java.nio.channels.CompletionHandler;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.dcache.pool.nearline.NearlineStorageHandler;
import org.dcache.pool.repository.CacheEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Holds the files to flush for a particular storage class.
 */
public class StorageClassInfo implements CompletionHandler<Void, PnfsId>, Comparable<StorageClassInfo> {

    private static final Logger LOGGER = LoggerFactory.getLogger(StorageClassInfo.class);

    private static class Entry implements Comparable<Entry> {

        final long timeStamp;
        final long size;

        Entry(CacheEntry entry) {
            timeStamp = entry.getCreationTime();
            size = entry.getReplicaSize();
        }

        long getTimeStamp() {
            return timeStamp;
        }

        @Override
        public int compareTo(Entry entry) {
            return Long.compare(timeStamp, entry.timeStamp);
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

    /**
     * The creation time of the oldest entry in the storage class.
     */
    private long _time;
    private long _totalSize;
    private int _activeCounter;
    private boolean _isSuspended;
    private int _errorCounter;
    private long _lastSubmittedAt;
    private long _recentFlushId;
    private int _requestsSubmitted;
    private int _maxRequests;

    public StorageClassInfo(NearlineStorageHandler storageHandler, String hsmName,
          String storageClass) {
        _storageHandler = requireNonNull(storageHandler);
        _storageClass = requireNonNull(storageClass);
        _hsmName = hsmName.toLowerCase();
    }

    public synchronized StorageClassFlushInfo getFlushInfo() {
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

    private synchronized void internalCompleted() {
        _activeCounter--;
        if (_activeCounter <= 0) {
            _activeCounter = 0;
        }
    }

    private synchronized void internalFailed(Throwable exc, PnfsId pnfsId) {
        _errorCounter++;
        if (exc instanceof CacheException) {
            CacheException ce = (CacheException) exc;
            if (ce.getRc() >= 30 && ce.getRc() < 40) {
                Entry entry = removeRequest(pnfsId);
                if (entry != null) {
                    _failedRequests.put(pnfsId, entry);
                }
                _errorCounter--;
            }
        }
        internalCompleted();
    }

    @Override
    public void completed(Void nil, PnfsId attachment) {
        internalCompleted();
    }

    @Override
    public void failed(Throwable exc, PnfsId pnfsId) {
        internalFailed(exc, pnfsId);
    }

    public long flush(int maxCount) {
        long id = System.currentTimeMillis();
        internalFlush(id, maxCount);
        return id;
    }

    private synchronized void internalFlush(long id, int maxCount) {
        LOGGER.info("Flushing {}", this);

        if (_activeCounter > 0) {
            throw new IllegalArgumentException("Is already active.");
        }

        _maxRequests = maxCount;

        maxCount = Math.min(_requests.size(), maxCount);

        _isDraining = false;
        _errorCounter = 0;
        _requestsSubmitted = maxCount;
        _activeCounter = maxCount;
        _recentFlushId = _lastSubmittedAt = id;

        if (maxCount != 0) {
            // REVISIT: why Map.Entry.comparingByValue().thenComparing(Map.Entry.comparingByKey()) doesn't work?!
            Comparator<Map.Entry<PnfsId, Entry>> byPnfsId = Map.Entry.comparingByKey();
            Comparator<Map.Entry<PnfsId, Entry>> byTimeStamp = Map.Entry.comparingByValue();
            Comparator<Map.Entry<PnfsId, Entry>> entryComparator = byTimeStamp.thenComparing(
                  byPnfsId);

            _storageHandler.flush(_hsmName,
                  _requests.entrySet().stream()
                        .sorted(entryComparator)
                        .map(Map.Entry::getKey)
                        .limit(maxCount)
                        .collect(Collectors.toList()),
                  this);
        }
    }

    public synchronized long getLastSubmitted() {
        return _lastSubmittedAt;
    }

    public synchronized int getActiveCount() {
        return _activeCounter;
    }

    public synchronized boolean isActive() {
        return _activeCounter > 0;
    }

    public synchronized int getErrorCount() {
        return _errorCounter;
    }

    @Override
    public int hashCode() {
        return Objects.hash(_storageClass, _hsmName);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof StorageClassInfo)) {
            return false;
        }
        StorageClassInfo info = (StorageClassInfo) obj;
        return info._storageClass.equals(_storageClass) && info._hsmName.equals(_hsmName);
    }

    @Override

    /**
     * Compares two storage classes by oldest entry.
     * @param anotherClass the {@code StorageClassInfo} to be compared.
     * @return the value 0 for the oldest entries in this and {@code anotherClass}
     *         storage classes have the same creation time; a value less than zero if
     *         oldest entry in this storage class created before the oldest entry in
     *         {@code anotherClass}; and value greater than zero in other cases.
     */
    public int compareTo(StorageClassInfo anotherClass) {

        return _time == 0L ?

              1 : Long.compare(_time, anotherClass._time);

    }

    private synchronized void addRequest(PnfsId pnfsId, Entry entry) {
        _requests.put(pnfsId, entry);
        if (_time == 0L || entry.timeStamp < _time) {
            _time = entry.timeStamp;
        }
        _totalSize += entry.size;

        if (_isOpen && !_isDraining && _activeCounter > 0 && _requestsSubmitted < _maxRequests) {
            _requestsSubmitted++;
            _activeCounter++;
            _storageHandler.flush(_hsmName, singleton(pnfsId), this);
        }
    }

    private synchronized Entry removeRequest(PnfsId pnfsId) {
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
          throws CacheException {
        PnfsId pnfsId = entry.getPnfsId();
        if (_failedRequests.containsKey(pnfsId) || _requests.containsKey(pnfsId)) {
            throw new CacheException(44, "Request already added : " + pnfsId);
        }
        addRequest(pnfsId, new Entry(entry));
    }

    public synchronized void activate(PnfsId pnfsId) throws CacheException {
        Entry entry = _failedRequests.remove(pnfsId);
        if (entry == null) {
            throw new CacheException("Not a deactivated Request : " + pnfsId);
        }
        addRequest(pnfsId, entry);
    }

    public synchronized void activateAll() {
        _failedRequests.forEach(this::addRequest);
        _failedRequests.clear();
    }

    public synchronized void deactivate(PnfsId pnfsId) throws CacheException {
        Entry entry = removeRequest(pnfsId);
        if (entry == null) {
            throw new CacheException("Not an activated Request : " + pnfsId);
        }
        _failedRequests.put(pnfsId, entry);
    }

    public synchronized boolean remove(PnfsId pnfsId) {
        Entry entry = removeRequest(pnfsId);
        if (entry == null) {
            entry = _failedRequests.remove(pnfsId);
        }
        return entry != null;
    }

    public String getHsm() {
        return _hsmName;
    }

    public String getFullName() {
        return _storageClass + "@" + _hsmName;
    }

    public synchronized Collection<PnfsId> getRequests() {
        return new ArrayList<>(_requests.keySet());
    }

    public synchronized Collection<PnfsId> getFailedRequests() {
        return new ArrayList<>(_failedRequests.keySet());
    }

    public synchronized String toString() {
        return "SCI=" + _storageClass +
              "@" + _hsmName +
              ";open=" + _isOpen +
              ";def=" + _isDefined +
              ";exp=" + TimeUnit.MILLISECONDS.toSeconds(_expiration) +
              ";pend=" + _pending +
              ";maxTotal=" + _maxTotalSize +
              ";waiting=" + _requests.size();
    }

    public synchronized boolean hasExpired() {
        return (!_requests.isEmpty()) &&
              ((_time + _expiration) < System.currentTimeMillis());
    }

    public synchronized long expiresIn() {
        return _time == 0L ? 0L : ((_time + _expiration) - System.currentTimeMillis()) / 1000;
    }

    public synchronized boolean isFull() {
        return (!_requests.isEmpty()) &&
              ((_requests.size() >= _pending) ||
                    (_totalSize >= _maxTotalSize));
    }

    public synchronized void setSuspended(boolean suspended) {
        _isSuspended = suspended;
    }

    public synchronized boolean isSuspended() {
        return _isSuspended;
    }

    public synchronized boolean isTriggered() {
        return (hasExpired() || isFull()) && !isSuspended();
    }

    public String getStorageClass() {
        return _storageClass;
    }

    public synchronized void setDefined(boolean d) {
        _isDefined = d;
    }

    public synchronized boolean isDefined() {
        return _isDefined;
    }

    public synchronized void setOpen(boolean isOpen) {
        _isOpen = isOpen;
    }

    public synchronized boolean isOpen() {
        return _isOpen;
    }

    public synchronized void drain() {
        _isDraining = true;
    }

    public synchronized int size() {
        return _requests.size() + _failedRequests.size();
    }

    public synchronized long getTotalSize() {
        return _totalSize;
    }

    /**
     * Sets the maximum age of a request before a flush to tape will be triggered.
     *
     * @param expiration Maximum age in milliseconds
     */
    public synchronized void setExpiration(long expiration) {
        _expiration = expiration;
    }

    /**
     * Sets the maximum number of requests before a flush to tape will be triggered.
     *
     * @param pending Maximum number of requests
     */
    public synchronized void setPending(int pending) {
        _pending = pending;
    }

    /**
     * Sets the maximum accumulated size of requests before a flush to tape will be triggered.
     *
     * @param maxTotalSize Maximum size in bytes
     */
    public synchronized void setMaxSize(long maxTotalSize) {
        _maxTotalSize = maxTotalSize;
    }

    public synchronized long getMaxSize() {
        return _maxTotalSize;
    }

    public synchronized int getPending() {
        return _pending;
    }

    public synchronized long getExpiration() {
        return _expiration;
    }

    public synchronized int getRequestCount() {
        return _requests.size();
    }

    public synchronized int getFailedRequestCount() {
        return _failedRequests.size();
    }
}
