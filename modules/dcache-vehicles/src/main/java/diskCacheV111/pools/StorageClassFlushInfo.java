package diskCacheV111.pools;

//Base class for messages to Pool


 import java.io.Serializable;

 public class StorageClassFlushInfo implements Serializable {

    private static final long serialVersionUID = 2092293652873859605L;
    private final String _hsmName;
    private final String _storageClass;
    //
    //  package private
    //
    long      _time;  // creation time of oldest file or 0L.
    int       _requestCount;
    int       _failedRequestCount;
    long      _expiration;
    int       _pending;
    boolean   _defined;
    long      _totalSize;
    long      _maxTotalSize;
    int       _activeCounter;
    boolean   _suspended;
    int       _errorCounter;
    long      _lastSubmittedAt;
    long      _recentFlushId;

    public StorageClassFlushInfo( String hsmName , String storageClass ){
	_hsmName      = hsmName ;
        _storageClass = storageClass ;
    }

    public String getHsm(){ return _hsmName ; }
    public String getStorageClass(){ return _storageClass ; }

    public void setOldestFileTimestamp(long time)
    {
        _time = time;
    }

    public long getOldestFileTimestamp()
    {
        return _time;
    }

    public void setRequestCount(int count)
    {
        _requestCount = count;
    }

    public int  getRequestCount()
    {
        return _requestCount;
    }

    public void setFailedRequestCount(int count)
    {
        _failedRequestCount = count;
    }

    public int  getFailedRequestCount()
    {
        return _failedRequestCount;
    }

    public void setExpirationTime(long time)
    {
        _expiration = time;
    }

    public long getExpirationTime()
    {
        return _expiration;
    }

    public void setMaximumPendingFileCount(int count)
    {
        _pending = count;
    }

    public int  getMaximumPendingFileCount()
    {
        return _pending;
    }

    public void setTotalPendingFileSize(long size)
    {
        _totalSize = size;
    }

    public long getTotalPendingFileSize()
    {
        return _totalSize;
    }

    public void setMaximumAllowedPendingFileSize(long size)
    {
        _maxTotalSize = size;
    }

    public long getMaximumAllowedPendingFileSize()
    {
        return _maxTotalSize;
    }

    public void setActiveCount(int count)
    {
        _activeCounter = count;
    }

    public int  getActiveCount()
    {
        return _activeCounter;
    }

    public void setSuspended(boolean suspended)
    {
        _suspended = suspended;
    }

    public void setErrorCounter(int count)
    {
        _errorCounter = count;
    }

    public void setFlushId(long id)
    {
        _recentFlushId = id;
    }

    public long getFlushId()
    {
        return _recentFlushId;
    }

    public void setLastSubmittedTime(long time)
    {
        _lastSubmittedAt = time;
    }

    public long getLastSubmittedTime()
    {
        return _lastSubmittedAt;
    }

    public String toString()
    {
        return _storageClass + '@' + _hsmName + '=' +
               "{r=" + _requestCount + '/' + _failedRequestCount + '}';
    }
}


