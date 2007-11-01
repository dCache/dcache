 // $Id: StorageClassFlushInfo.java,v 1.2.4.1 2006-08-23 17:39:07 patrick Exp $

package diskCacheV111.pools;

//Base class for messages to Pool


public class StorageClassFlushInfo implements java.io.Serializable {
    
    private static final long serialVersionUID = 2092293652873859605L;
    private String _hsmName = null ;
    private String _storageClass = null ;
    //
    //  package private 
    //
    long      _time            = 0 ;  // creation time of oldest file or 0L.
    int       _requestCount    = 0 ;
    int       _failedRequestCount = 0 ;
    long      _expiration      = 0 ;
    int       _pending         = 0 ;
    boolean   _defined         = false ;
    long      _totalSize       = 0L ;
    long      _maxTotalSize    = 0L ;
    int       _activeCounter   = 0 ;
    boolean   _suspended       = false ;
    int       _errorCounter    = 0 ;
    long      _lastSubmittedAt = 0L ;
    long      _recentFlushId   = 0L ;
    
    public StorageClassFlushInfo( String hsmName , String storageClass ){
	_hsmName      = hsmName ;
        _storageClass = storageClass ;
    }
    public String getHsm(){ return _hsmName ; }
    public String getStorageClass(){ return _storageClass ; }
    public long getOldestFileTimestamp(){ return _time ; }
    public int  getRequestCount(){ return _requestCount ; }
    public int  getFailedRequestCount(){ return _failedRequestCount ; }
    public long getExpirationTime(){ return _expiration ; }
    public int  getMaximumPendingFileCount(){ return _pending ; }
    public long getTotalPendingFileSize(){ return _totalSize ; }
    public long getMaximumAllowedPendingFileSize(){ return _maxTotalSize ; }
    public int  getActiveCount(){ return _activeCounter ; }
    public long getFlushId(){ return _recentFlushId ; }
    public long getLastSubmittedTime(){ return _lastSubmittedAt ; }
    public String toString(){
         StringBuffer sb = new StringBuffer() ;
         sb.append(_storageClass).append("@").append(_hsmName).append("=").
            append("{r=").append(_requestCount).append("/").append(_failedRequestCount).append("}");
         return sb.toString();
     }
}


