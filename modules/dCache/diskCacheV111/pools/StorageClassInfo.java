// $Id: StorageClassInfo.java,v 1.20 2007-08-23 09:13:14 tigran Exp $

package diskCacheV111.pools;


import  diskCacheV111.util.*;
import  diskCacheV111.repository.* ;

import  java.util.*;

public class StorageClassInfo implements CacheFileAvailable {

   private long      _time     = 0 ;  // creation time of oldest file or 0L.
   private Map<PnfsId, CacheRepositoryEntry>   _requests       = new HashMap<PnfsId, CacheRepositoryEntry>() ;
   private Map<PnfsId, CacheRepositoryEntry>   _failedRequests = new HashMap<PnfsId, CacheRepositoryEntry>() ;
   private final String    _name;
   private final String    _hsmName;

   private long      _expiration = 0 ;
   private int       _pending    = 0 ;
   private boolean   _defined    = false ;
   private long      _totalSize       = 0L ;
   private long      _maxTotalSize    = 0L ;
   private int       _activeCounter   = 0 ;
   private boolean   _suspended       = false ;
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
   //                _flushCallback
   //
   public StorageClassFlushInfo getFlushInfo(){

       StorageClassFlushInfo info = new StorageClassFlushInfo( _hsmName , _name ) ;

       synchronized( this ){
          TreeSet<CacheRepositoryEntry> ts = new TreeSet<CacheRepositoryEntry> ( __repositoryComparator ) ;
          ts.addAll( _requests.values() ) ;
          try{
              info.setOldestFileTimestamp(ts.size() == 0 ? 0L : ts.first().getCreationTime());
          }catch(Exception ee){
              info.setOldestFileTimestamp(0L);
          }
          info.setRequestCount(_requests.size());
          info.setFailedRequestCount(_failedRequests.size());
          info.setExpirationTime(_expiration);
          info.setMaximumPendingFileCount(_pending);
          info.setTotalPendingFileSize(_totalSize);
          info.setMaximumAllowedPendingFileSize(_maxTotalSize);
       }
       synchronized( _activeCounterLock ){
           info.setActiveCount(_activeCounter);
       }
       info.setSuspended(_suspended);
       info.setErrorCounter(_errorCounter);
       info.setLastSubmittedTime(_lastSubmittedAt);
       info.setFlushId(_recentFlushId);
       return info;
   }
   private static RepositoryEntryComparator __repositoryComparator = null ;
   static {
       __repositoryComparator = new RepositoryEntryComparator() ;
   }
   public static class RepositoryEntryComparator implements Comparator<CacheRepositoryEntry> {
       public int compare( CacheRepositoryEntry a , CacheRepositoryEntry b ){
          try{
             long al = a.getCreationTime() ;
             long bl = b.getCreationTime() ;
             return al >  bl ? 1 :
                    al == bl ?
                    a.getPnfsId().toString().compareTo(b.getPnfsId().toString())  : -1 ;
          }catch(Exception ee ){
             return -1 ;
          }
          /*
          return
          new Long( ((CacheRepositoryEntry)a).getCreationTime() ).compare(
          new Long( ((CacheRepositoryEntry)b).getCreationTime() ) ;
          */
       }
   }
   public void cacheFileAvailable( String pnfsId , Throwable ce ){

      if( ce != null ){

         _errorCounter ++ ;

         if( ce instanceof CacheException ){

            CacheException cce = (CacheException)ce ;

            if( ( cce.getRc() >= 30 ) && ( cce.getRc() < 40 ) ){

               PnfsId id = new PnfsId(pnfsId) ;
               synchronized( this){
                   CacheRepositoryEntry info = _requests.remove( id ) ;
                   if( info != null ){
                      _time = 0L ;
                      _failedRequests.put( id , info ) ;
                      try{ _totalSize -= info.getSize() ; }catch(Exception ee ){}
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
             _activeCounter = 0 ;
             if( _flushCallback != null )new InformFlushCallback() ;
          }
      }
   }
   private class InformFlushCallback implements Runnable {

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
   public long submit( HsmStorageHandler2 storageHandler , int maxCount , StorageClassInfoFlushable flushCallback ){

      Iterator<CacheRepositoryEntry> i = null ;

      synchronized( this ){

         TreeSet<CacheRepositoryEntry> ts = new TreeSet<CacheRepositoryEntry>( __repositoryComparator ) ;
         //System.out.println("Storage class entries : "+_requests.values().size());
         ts.addAll( _requests.values() ) ;
         //System.out.println("Storage class entries (tc): "+ts.size());
         i = ts.iterator() ;

      }

      //
      //   As long as we are in this loop block, we will be stuck in
      //   the first 'cacheFileAvailable'.  (Should not be a problem
      //   because the store routine is non blocking.)
      //
      synchronized( _activeCounterLock ){

         if( _activeCounter > 0 )
            throw new
            IllegalArgumentException( "Is already active" ) ;

         _flushCallback = flushCallback ;

         maxCount = maxCount <= 0 ? 0Xfffffff : maxCount ;
         _errorCounter = 0 ;
         _requestsSubmitted = 0 ;

          _recentFlushId = _lastSubmittedAt = System.currentTimeMillis() ;

          for( int n = 0 ; i.hasNext() && ( n < maxCount ) ; n++ ){

             CacheRepositoryEntry e = i.next() ;
             _requestsSubmitted ++ ;
             try{

                   //
                   // if store returns true, it didn't register a
                   // callback and not store is initiated.
                   //
                   if( storageHandler.store( e , this ) )continue ;

                   _activeCounter ++ ;

                   //System.out.println("DEBUGFLUSH2 : adding : "+e ) ;

             }catch( Throwable ce ){
                _errorCounter ++ ;
                System.err.println("Problem submitting : "+e+" : "+ce ) ;
             }

          }

          if( _activeCounter <= 0 ){
             _activeCounter = 0 ;
             if( _flushCallback != null )new InformFlushCallback() ;
          }
      }

      return _recentFlushId ;

   }
   public  long getLastSubmitted(){ return _lastSubmittedAt ; }
   public  int getActiveCount(){
      synchronized( _activeCounterLock ){  return _activeCounter  ; }
   }
   public  int getErrorCount(){ return _errorCounter ; }
   public StorageClassInfo( String hsmName , String storageClass ){
      _name    = storageClass ;
      _hsmName = hsmName.toLowerCase() ;
   }

   @Override
   public int hashCode() {
	   return _name.hashCode() | _hsmName.hashCode();
   }

   @Override
   public boolean equals( Object obj ){

	  if( !(obj instanceof StorageClassInfo) ) return false;
      StorageClassInfo info = (StorageClassInfo)obj ;
      return info._name.equals(_name) && info._hsmName.equals(_hsmName) ;
   }
   public synchronized void addRequest( CacheRepositoryEntry request )throws CacheException {

       if( ( _failedRequests.get(request.getPnfsId()) != null ) ||
           ( _requests.get(request.getPnfsId()) != null )
                     )
         throw new
         CacheException(44,"Request already added : "+request.getPnfsId());

//       if( request.getSize() > 0x7fffffff ){
//          _failedRequests.put( request.getPnfsId() , request ) ;
//       }else{
          _requests.put( request.getPnfsId() , request ) ;
          if( _time == 0L || ( request.getCreationTime() < _time ) ){
             _time = request.getCreationTime() ;
          }
//       }
       _totalSize += request.getSize() ;
   }
   public synchronized void activate( PnfsId pnfsId ) throws CacheException {
      CacheRepositoryEntry info = _failedRequests.remove( pnfsId ) ;
      if( info == null ) {
        throw new
        CacheException( "Not a deactivated Request : "+pnfsId ) ;
      }
      addRequest( info ) ;

   }
   public synchronized void activateAll() throws CacheException {

      for( CacheRepositoryEntry entry : _failedRequests.values() ){
         addRequest( entry ) ;
      }
      _failedRequests.clear() ;
   }
   public synchronized void deactivate( PnfsId pnfsId ) throws CacheException {
      CacheRepositoryEntry info = _requests.remove( pnfsId ) ;
      if( info == null ) {
        throw new
        CacheException( "Not an activated Request : "+pnfsId ) ;
      }

      if( _requests.isEmpty() ) _time = 0L ;

      _failedRequests.put( pnfsId , info ) ;
      _totalSize -= info.getSize() ;

   }
   public synchronized CacheRepositoryEntry removeRequest( PnfsId pnfsId )throws CacheException {
      CacheRepositoryEntry info = _requests.remove( pnfsId ) ;
      if( info != null ){
    	  if( _requests.isEmpty() ) _time = 0L ;
      }else{
         info = _failedRequests.remove( pnfsId );
      }
      if( info != null ) _totalSize -= info.getSize() ;
      return info;
   }
   public String getHsm(){ return _hsmName ; }
   public String getName(){ return _name ; }
   public synchronized Iterator<CacheRepositoryEntry> getRequests(){ return new ArrayList<CacheRepositoryEntry>(_requests.values() ).iterator() ; }
   public synchronized Iterator<CacheRepositoryEntry> getFailedRequests(){ return new ArrayList<CacheRepositoryEntry>( _failedRequests.values() ).iterator() ; }
   public String toString(){
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
   public synchronized boolean hasExpired(){
       return ( _requests.size() > 0 ) &&
              ( ( _time + _expiration ) < System.currentTimeMillis() ) ;
   }
   public synchronized long expiresIn(){
       return  _time == 0L ? 0L : (( _time + _expiration ) - System.currentTimeMillis()  ) / 1000 ;
   }
   public synchronized boolean isFull(){
       return ( _requests.size() > 0 ) &&
              ( ( _requests.size() >= _pending )  ||
                ( _totalSize >= _maxTotalSize  )     ) ;
   }
   public synchronized void setSuspended( boolean suspended ){ _suspended = suspended ; }
   public synchronized boolean isSuspended(){ return _suspended ; }
   public synchronized boolean isTriggered(){
       return ( hasExpired() || isFull() ) && ! isSuspended() ;
   }
   public synchronized String getStorageClass(){ return _name ; }
   public synchronized void setTime( long time ){ _time = time ; }
   public synchronized long getTime(){ return _time ; }
   public synchronized void setDefined( boolean d ){ _defined = d ; }
   public synchronized boolean isDefined(){ return _defined ; }
   public synchronized int  size(){ return _requests.size() + _failedRequests.size() ; }
   public synchronized long getTotalSize(){ return _totalSize ; }
   public synchronized void setExpiration( int expiration){
      _expiration = expiration * 1000L  ;
   }
   public synchronized void setPending( int pending ){
      _pending = pending ;
   }
   public synchronized void setMaxSize( long maxTotalSize ){  _maxTotalSize = maxTotalSize ; }
   public synchronized long getMaxSize(){ return _maxTotalSize ; }
   public synchronized int  getPending(){ return _pending ; }
   public synchronized int  getExpiration(){ return (int)(_expiration / 1000); }
   public synchronized int  getRequestCount(){ return _requests.size() ; }
   public synchronized int  getFailedRequestCount(){ return _failedRequests.size() ; }
}
