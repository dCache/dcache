// $Id: StorageClassInfo2.java,v 1.1 2007-10-08 14:11:02 behrmann Exp $

package diskCacheV111.pools;


import  diskCacheV111.util.*;
import  diskCacheV111.repository.* ;

import  java.util.*;

public class StorageClassInfo2 implements CacheFileAvailable
{
    class Request implements Comparable<Request>
    {
        PnfsId id;
        long time;
        long size;

        public Request(PnfsId id, long time, long size) {
            this.id = id;
            this.time = time;
            this.size = size;
        }

        public int compareTo(Request r)
        {
            return time < r.time
                ? -1
                : (time > r.time ? 1 : id.compareTo(r.id));

        }

        @Override
        public boolean equals(Object obj) {
            if( obj == this ) return true;
            if( !(obj instanceof Request) ) return false;

            Request other = (Request)obj;
            return  other.time == this.time && other.size == this.size && other.id.equals(id);
        }

        @Override
        public int hashCode() {
            return 17;
        }


    }

    private Map<PnfsId, Request> _pnfsIds = new HashMap<PnfsId, Request>();
    private SortedSet<Request> _requests = new TreeSet<Request>();
    private SortedSet<Request> _failedRequests = new TreeSet<Request>();

    private CacheRepository _repository;

    private final String    _name;
    private final String    _hsmName;

    private long      _expiration = 0 ;
    private int       _pending    = 0 ;
    private boolean   _defined    = false ;
    private long      _totalSize       = 0L ;
    private long      _maxTotalSize    = 0L ;
    private int       _activeCounter   = 0 ;
    private boolean   _suspended       = false ;
    private boolean   _isActive        = false ;
    private int       _errorCounter    = 0 ;
    private long      _lastSubmittedAt = 0L ;
    private long      _recentFlushId   = 0L ;
    private int       _requestsSubmitted = 0 ;
    private final Object    _activeCounterLock = new Object() ;
    private StorageClassInfoFlushable _flushCallback = null ;
    //
    // protection locks
    //     synchronized(this) :
    //                _requests
    //                _failedRequests
    //     synchronized(_activeCounterLock)
    //                _activeCounter
    //                _isActive
    //                _flushCallback
    //

    public StorageClassFlushInfo getFlushInfo()
    {
        StorageClassFlushInfo info =
            new StorageClassFlushInfo( _hsmName , _name ) ;

        synchronized( this ){
            info._time = getOldestCreationTime();
            info._requestCount    = _requests.size() ;
            info._failedRequestCount = _failedRequests.size() ;
            info._expiration      = _expiration ;
            info._pending         = _pending ;
            info._totalSize       = _totalSize ;
            info._maxTotalSize    = _maxTotalSize ;
        }
        synchronized( _activeCounterLock ){
            info._activeCounter   = _activeCounter ;
        }
        info._suspended       = _suspended ;
        info._errorCounter    = _errorCounter ;
        info._lastSubmittedAt = _lastSubmittedAt ;
        info._recentFlushId   = _recentFlushId ;
        return info ;
    }

    public void cacheFileAvailable( String pnfsId , Throwable ce )
    {
        if( ce != null ){

            _errorCounter ++ ;

            if( ce instanceof CacheException ){

                CacheException cce = (CacheException)ce ;

                if( ( cce.getRc() >= 30 ) && ( cce.getRc() < 40 ) ){

                    PnfsId id = new PnfsId(pnfsId) ;
                    synchronized (this) {
                        Request request = _pnfsIds.get(id);
                        if (request != null && _requests.remove(request)) {
                            _failedRequests.add(request);
                            _totalSize -= request.size;
                        }
                    }
                    _errorCounter -- ;
                }
            }
        }
        synchronized( _activeCounterLock ){
            _activeCounter -- ;
            //System.out.println("DEBUGFLUSH2 : adding : "+pnfsId ) ;
            if( _activeCounter <= 0 ){
                _isActive = false ;
                _activeCounter = 0 ;
                if( _flushCallback != null )new InformFlushCallback() ;
            }
        }
    }

    private class InformFlushCallback implements Runnable
    {
        private int  _flushErrorCounter = _errorCounter ;
        private long _flushId           = _recentFlushId ;
        private int  _requests          = _requestsSubmitted ;

        private StorageClassInfoFlushable _callback = _flushCallback ;

        private InformFlushCallback(){
            if( _callback == null )return ;
            new Thread(this).start() ;
        }
        public void run(){
            try{
                _callback.storageClassInfoFlushed( getHsm() , getStorageClass() , _flushId , _requests , _flushErrorCounter ) ;
            }catch(Throwable t ){
                System.err.println("Problem in running storageClassInfoFlushed callback : "+t);
            }
        }
    }

    public long submit(HsmStorageHandler2 storageHandler, int maxCount,
                       StorageClassInfoFlushable flushCallback)
    {
        synchronized( _activeCounterLock ){

            if( _isActive )
                throw new
                    IllegalArgumentException( "Is already active" ) ;

            _isActive      = true ;
            _flushCallback = flushCallback ;

            maxCount = maxCount <= 0 ? 0Xfffffff : maxCount ;
            _errorCounter = 0 ;
            _requestsSubmitted = 0 ;

            //
            //   As long as we are in this loop, we will stuck
            //   in the first 'cacheFileAvailable'.
            //   (Should not be a problem because the store routine
            //   is non blocking.)
            //

            _recentFlushId = _lastSubmittedAt = System.currentTimeMillis() ;

            synchronized (this) {
                int n = 0;
                for (Request request : _requests) {
                    if (n++ < maxCount)
                        break;

                    try {
                        CacheRepositoryEntry e = _repository.getEntry(request.id);
                        _requestsSubmitted ++ ;

                        //
                        // if store returns true, it didn't register a
                        // callback and not store is initiated.
                        //
                        if (storageHandler.store(e, this)) {
                            continue;
                        }

                        _activeCounter ++ ;

                        //System.out.println("DEBUGFLUSH2 : adding : "+e ) ;
                    } catch (Throwable ce) {
                        _errorCounter ++ ;
                        System.err.println("Problem submitting : "
                                           + request.id + " : " + ce);
                    }

                }

                _isActive = _activeCounter > 0 ;
                if( ! _isActive ){
                    _activeCounter = 0 ;
                    if( _flushCallback != null )new InformFlushCallback() ;
                }
            }
        }

        return _recentFlushId ;
    }

    public  long getLastSubmitted()
    {
        return _lastSubmittedAt ;
    }

    public  int getActiveCount()
    {
        synchronized( _activeCounterLock ) {
            return _activeCounter;
        }
    }

    public  int getErrorCount()
    {
        return _errorCounter;
    }

    public StorageClassInfo2(CacheRepository repository,
                             String hsmName, String storageClass)
    {
        _repository = repository;
        _name = storageClass;
        _hsmName = hsmName.toLowerCase();
    }

    @Override
    public int hashCode()
    {
        return _name.hashCode() | _hsmName.hashCode();
    }

    @Override
    public boolean equals( Object obj )
    {

        if( !(obj instanceof StorageClassInfo2) ) return false;
        StorageClassInfo2 info = (StorageClassInfo2)obj ;
        return info._name.equals(_name) && info._hsmName.equals(_hsmName) ;
    }

    public synchronized void addRequest(CacheRepositoryEntry entry)
        throws CacheException
    {
        PnfsId id = entry.getPnfsId();
        if (_pnfsIds.containsKey(id)) {
            throw new CacheException(44, "Request already added : " + id);
        }

        Request r = new Request(id, entry.getCreationTime(), entry.getSize());
        _pnfsIds.put(id, r);
        _requests.add(r);
        _totalSize += r.size;
    }

    public synchronized void activate(PnfsId pnfsId) throws CacheException
    {
        Request request = _pnfsIds.get(pnfsId);
        if (!_failedRequests.remove(request)) {
            throw new
                CacheException("Not a deactivated Request : " + pnfsId);
        }
        _requests.add(request);
        _totalSize += request.size;
    }

    public synchronized void activateAll() throws CacheException
    {
        for (Request request : _failedRequests) {
            _requests.add(request);
            _totalSize += request.size;
        }
        _failedRequests.clear();
    }

    public synchronized void deactivate(PnfsId pnfsId) throws CacheException
    {
        Request request = _pnfsIds.get(pnfsId);
        if (!_requests.remove(request)) {
            throw new
                CacheException( "Not an activated Request : " + pnfsId);
        }

        _failedRequests.add(request);
        _totalSize -= request.size;
    }

    public synchronized CacheRepositoryEntry removeRequest(PnfsId pnfsId)
        throws CacheException
    {
        Request request = _pnfsIds.remove(pnfsId);
        if (request == null) {
            return null;
        }

        if (_requests.remove(request)) {
            _totalSize -= request.size;
        } else {
            _failedRequests.remove(request);
        }
        return _repository.getEntry(pnfsId);
    }

    public String getHsm()
    {
        return _hsmName;
    }

    public String getName()
    {
        return _name;
    }

    public synchronized Iterator<CacheRepositoryEntry> getRequests()
    {
        List<CacheRepositoryEntry> result =
            new ArrayList<CacheRepositoryEntry>();
        for (Request request : _requests) {
            try {
                result.add(_repository.getEntry(request.id));
            } catch (CacheException ignored) {
                // Silently drop entries we cannot read from the collection
            }
        }
        return result.iterator();
    }

    public synchronized Iterator<CacheRepositoryEntry> getFailedRequests()
    {
        List<CacheRepositoryEntry> result =
            new ArrayList<CacheRepositoryEntry>();
        for (Request request : _failedRequests) {
            try {
                result.add(_repository.getEntry(request.id));
            } catch (CacheException ignored) {
                // Silently drop entries we cannot read from the collection
            }
        }
        return result.iterator();
    }

    public synchronized String toString()
    {
        StringBuffer sb = new StringBuffer() ;
        sb.append("SCI=").append(_name).
            append("@").append(_hsmName).
            append(";def=").append(_defined).
            append(";exp=").append(_expiration/1000).
            append(";pend=").append(_pending).
            append(";maxTotal=").append(_maxTotalSize).
            append(";waiting=").append(_requests.size()) ;
        return sb.toString() ;
    }

    public synchronized boolean hasExpired()
    {
        long time = getOldestCreationTime();
        return ( _requests.size() > 0 ) &&
            ( ( time + _expiration ) < System.currentTimeMillis() ) ;
    }

    public synchronized long expiresIn()
    {
        long time = getOldestCreationTime();
        return  time == 0L ? 0L : (( time + _expiration ) - System.currentTimeMillis()  ) / 1000 ;
    }

    public synchronized boolean isFull()
    {
        return ( _requests.size() > 0 ) &&
            ( ( _requests.size() >= _pending )  ||
              ( _totalSize >= _maxTotalSize  )     ) ;
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
        return ( hasExpired() || isFull() ) && ! isSuspended() ;
    }

    public synchronized String getStorageClass()
    {
        return _name;
    }

    public synchronized void setDefined(boolean d)
    {
        _defined = d;
    }

    public synchronized boolean isDefined()
    {
        return _defined;
    }

    public synchronized int  size()
    {
        return _requests.size() + _failedRequests.size() ;
    }

    public synchronized long getTotalSize()
    {
        return _totalSize;
    }

    public synchronized void setExpiration(int expiration)
    {
        _expiration = expiration * 1000;
    }

    public synchronized void setPending(int pending)
    {
        _pending = pending ;
    }

    public synchronized void setMaxSize(long maxTotalSize)
    {
        _maxTotalSize = maxTotalSize;
    }

    public synchronized long getMaxSize()
    {
        return _maxTotalSize;
    }

    public synchronized int  getPending()
    {
        return _pending;
    }

    public synchronized int  getExpiration()
    {
        return (int)(_expiration / 1000);
    }

    public synchronized int  getRequestCount()
    {
        return _requests.size() ;
    }

    public synchronized int  getFailedRequestCount()
    {
        return _failedRequests.size() ;
    }

    protected synchronized long getOldestCreationTime()
    {
        if (_requests.isEmpty()) {
            return 0L;
        } else {
            return _requests.first().time;
        }
    }
}
