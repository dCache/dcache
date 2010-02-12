// $Id: StorageClassInfo.java,v 1.20 2007-08-23 09:13:14 tigran Exp $

package org.dcache.pool.classic;

import diskCacheV111.pools.StorageClassFlushInfo;
import diskCacheV111.util.*;
import org.dcache.pool.repository.CacheEntry;

import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.log4j.NDC;

public class StorageClassInfo implements CacheFileAvailable
{
    private static Logger _log = LoggerFactory.getLogger(StorageClassInfo.class);

    static class Entry
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
    }

    static class RepositoryEntryComparator
        implements Comparator<Entry>
    {
        public int compare(Entry a, Entry b)
        {
            try {
                long al = a.timeStamp;
                long bl = b.timeStamp;
                return al > bl ? 1 :
                    al == bl ? a.pnfsId.compareTo(b.pnfsId) : -1;
            } catch (Exception e) {
                return -1;
            }
        }
    }

    private static RepositoryEntryComparator __repositoryComparator =
        new RepositoryEntryComparator();

    private long _time = 0; // creation time of oldest file or 0L.
    private Map<PnfsId, Entry> _requests = new HashMap();
    private Map<PnfsId, Entry> _failedRequests = new HashMap();
    private final String _name;
    private final String _hsmName;

    private long _expiration = 0;
    private int _pending = 0;
    private boolean _defined = false;
    private long _totalSize = 0L;
    private long _maxTotalSize = 0L;
    private int _activeCounter = 0;
    private boolean _suspended = false;
    private boolean _isActive = false;
    private int _errorCounter = 0;
    private long _lastSubmittedAt = 0L;
    private long _recentFlushId = 0L;
    private int _requestsSubmitted = 0;
    private StorageClassInfoFlushable _flushCallback;

    public StorageClassInfo(String hsmName, String storageClass)
    {
        _name = storageClass;
        _hsmName = hsmName.toLowerCase();
    }

    public synchronized StorageClassFlushInfo getFlushInfo()
    {
        StorageClassFlushInfo info = new StorageClassFlushInfo(_hsmName, _name);

        Collection<Entry> entries = _requests.values();
        if (entries.isEmpty()) {
            info.setOldestFileTimestamp(0);
        } else {
            Entry entry =
                Collections.min(entries, __repositoryComparator);
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
    public synchronized void cacheFileAvailable(PnfsId pnfsId, Throwable ce)
    {
        if (ce != null) {
            _errorCounter ++;

            if (ce instanceof CacheException) {
                CacheException cce = (CacheException)ce;

                if ((cce.getRc() >= 30) &&(cce.getRc() < 40)) {
                    Entry entry = removeRequest(pnfsId);
                    if (entry != null) {
                        _failedRequests.put(entry.pnfsId, entry);
                    }
                    _errorCounter --;
                }
            }
        }

        _activeCounter --;
        if (_activeCounter <= 0) {
            _activeCounter = 0;
            if (_flushCallback != null)
                new InformFlushCallback();
        }
    }

    private class InformFlushCallback implements Runnable
    {
        private int _flushErrorCounter = _errorCounter;
        private long _flushId = _recentFlushId;
        private int _requests = _requestsSubmitted;

        private StorageClassInfoFlushable _callback = _flushCallback;

        private InformFlushCallback()
        {
            if (_callback == null)
                return;
            new Thread(this).start();
        }

        public void run()
        {
            try {
                _callback.storageClassInfoFlushed(getHsm(), getStorageClass(), _flushId, _requests, _flushErrorCounter);
            } catch (Throwable t) {
                _log.error("Problem in running storageClassInfoFlushed callback : "+t);
            }
        }
    }

    public synchronized long submit(HsmStorageHandler2 storageHandler,
                                    int maxCount,
                                    StorageClassInfoFlushable flushCallback)
    {
        if (_activeCounter > 0)
            throw new IllegalArgumentException("Is already active");

        _flushCallback = flushCallback;

        List<Entry> entries = new ArrayList(_requests.values());
        Collections.sort(entries, __repositoryComparator);

        maxCount =
            maxCount <= 0
            ? entries.size()
            : Math.min(entries.size(), maxCount);
        _errorCounter = 0;
        _requestsSubmitted = 0;
        //
        // As long as we are in this loop, we will stuck
        // in the first 'cacheFileAvailable'.
        // (Should not be a problem because the store routine
        // is non blocking.)
        //
        _recentFlushId = _lastSubmittedAt = System.currentTimeMillis();

        for (Entry entry : entries.subList(0, maxCount)) {
            _requestsSubmitted ++;
            NDC.push(entry.pnfsId.toString());
            try {
                //
                // if store returns true, it didn't register a
                // callback and no store is initiated.
                //
                if (storageHandler.store(entry.pnfsId, this))
                    continue;

                _activeCounter++;

            } catch (Throwable e) {
                _errorCounter++;
                _log.error("Problem submitting : " + entry + " : " + e);
            } finally {
                NDC.pop();
            }
        }

        if (_activeCounter <= 0) {
            _activeCounter = 0;
            if (_flushCallback != null)
                new InformFlushCallback();
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
        return _name.hashCode() | _hsmName.hashCode();
    }

    @Override
    public boolean equals(Object obj)
    {
        if (!(obj instanceof StorageClassInfo))
            return false;
        StorageClassInfo info = (StorageClassInfo)obj;
        return info._name.equals(_name) && info._hsmName.equals(_hsmName);
    }

    private synchronized void addRequest(Entry entry)
    {
        _requests.put(entry.pnfsId, entry);
        if (_time == 0L || (entry.timeStamp < _time)) {
            _time = entry.timeStamp;
        }
        _totalSize += entry.size;
    }

    private synchronized Entry removeRequest(PnfsId pnfsId)
    {
        Entry entry = _requests.remove(pnfsId);
        if (entry != null) {
            if (_requests.isEmpty())
                _time = 0L;
            _totalSize -= entry.size;
        }
        return entry;
    }

    public synchronized void add(CacheEntry entry)
        throws CacheException
    {
        PnfsId pnfsId = entry.getPnfsId();
        if ((_failedRequests.get(pnfsId) != null) ||
            (_requests.get(pnfsId) != null))
            throw new
                CacheException(44, "Request already added : " + pnfsId);

        addRequest(new Entry(entry));
    }

    public synchronized void activate(PnfsId pnfsId) throws CacheException
    {
        Entry entry = _failedRequests.remove(pnfsId);
        if (entry == null) {
            throw new
                CacheException("Not a deactivated Request : "+pnfsId);
        }
        addRequest(entry);
    }

    public synchronized void activateAll() throws CacheException
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
            throw new
                CacheException("Not an activated Request : " + pnfsId);
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

    public String getName()
    {
        return _name;
    }

    public synchronized Collection<PnfsId> getRequests()
    {
        return new ArrayList<PnfsId>(_requests.keySet());
    }

    public synchronized Collection<PnfsId> getFailedRequests()
    {
        return new ArrayList<PnfsId>(_failedRequests.keySet());
    }

    public synchronized String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append("SCI=").append(_name).
            append("@").append(_hsmName).
            append(";def=").append(_defined).
            append(";exp=").append(_expiration/1000).
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
        return _time == 0L ? 0L :((_time + _expiration) - System.currentTimeMillis() ) / 1000;
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
        return _name;
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

    public synchronized void setExpiration(int expiration)
    {
        _expiration = expiration * 1000L;
    }

    public synchronized void setPending(int pending)
    {
        _pending = pending;
    }

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

    public synchronized int getExpiration()
    {
        return(int)(_expiration / 1000);
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
