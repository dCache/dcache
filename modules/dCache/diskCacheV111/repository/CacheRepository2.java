// $Id: CacheRepository2.java,v 1.39.2.6 2007-05-10 10:47:28 tigran Exp $

package diskCacheV111.repository ;

import diskCacheV111.util.event.* ;
import diskCacheV111.vehicles.* ;
import diskCacheV111.util.* ;

import java.util.* ;
import java.io.* ;
import dmg.cells.nucleus.CellMessage ;
import dmg.util.Logable ; 

public class CacheRepository2 implements CacheRepository {

    private final HashMap      _pnfsids        = new HashMap() ;
    private final HashMap      _removedPnfsids = new HashMap() ;
    private SpaceMonitor _spaceMonitor   = new FairQueueAllocation(0) ;    
    private long         _preciousSpace  = 0L ;  
    private long         _reservedSpace  = 0L ;
    private int          _inventoryCounter = 0 ;
    private File         _spaceReservation ;
    private File         _baseDir ;
    private File         _controlDir ;
    private File         _dataDir ;
    private Logable      _log = null ;
    private boolean      _isWindows = false ;
 
    private class NeedSpace implements SpaceRequestable {
       public void spaceNeeded( long space ){
           processNeedSpaceEvent(
               new CacheNeedSpaceEvent( CacheRepository2.this, space ) 
           ) ;
       }
    }
    //
    // DEBUG
    //
    private void resetAll(){ 
        _pnfsids.clear();
        _removedPnfsids.clear();
        _spaceMonitor   = new FairQueueAllocation(0);
        _preciousSpace  = 0 ;
    }
    public class CacheEntry  implements CacheRepositoryEntry  {
       public static final int CACHED      = 1 ;
       public static final int PRECIOUS    = 2 ;
       public static final int FROM_CLIENT = 4 ;
       public static final int FROM_STORE  = 8 ;
       public static final int TO_CLIENT   = 0x10 ;
       public static final int TO_STORE    = 0x20 ;
       public static final int REMOVED     = 0x40 ;
       public static final int DESTROYED   = 0x80 ;
       public static final int STICKY      = 0x100 ;
       public static final int BAD         = 0x200 ;
       public static final int PRIMARY = CACHED | PRECIOUS | FROM_CLIENT | FROM_STORE ;
       private final PnfsId  _pnfsId ;
       private long        _creationTime = System.currentTimeMillis() ;
       private long        _lastAccess   = _creationTime ;
       private long        _size         = 0 ;
       private StorageInfo _storageInfo = null ;
       private boolean     _isLocked    = false ;
       private long        _lockedUntil = 0 ;
       private int         _links       = 0 ;
       private int         _state       = 0 ;
       
       private CacheEntry( PnfsId pnfsId ){
          _pnfsId = pnfsId ;         
       }
       public String getState(){ return statusToString( _state ) ; }
       private  String statusToString( int status ){
          StringBuffer sb = new StringBuffer() ;
          sb.append( ( status & CACHED      ) != 0 ? "C" : "-" ) ; 
          sb.append( ( status & PRECIOUS    ) != 0 ? "P" : "-" ) ; 
          sb.append( ( status & FROM_CLIENT ) != 0 ? "C" : "-" ) ; 
          sb.append( ( status & FROM_STORE  ) != 0 ? "S" : "-" ) ; 
          sb.append( ( status & TO_CLIENT   ) != 0 ? "c" : "-" ) ; 
          sb.append( ( status & TO_STORE    ) != 0 ? "s" : "-" ) ; 
          sb.append( ( status & REMOVED     ) != 0 ? "R" : "-" ) ; 
          sb.append( ( status & DESTROYED   ) != 0 ? "D" : "-" ) ; 
          sb.append( ( status & STICKY      ) != 0 ? "X" : "-" ) ; 
          sb.append( ( status & BAD         ) != 0 ? "E" : "-" ) ; 
          return sb.toString() ;
       }
       public class TransitionException extends InconsistentCacheException {
           
           private static final long serialVersionUID = -398102360737221442L;
           
          public TransitionException( int fromState , int toState ){
             super( 33 , "Illegal State Transition "+
                    statusToString(fromState)+
                    " -> "+
                    statusToString(toState) ) ;
          }
       }
       private void checkDestroyed() throws CacheException {
          if( ( _state & DESTROYED ) != 0 )
            throw new InconsistentCacheException( 666,"Entry is destroyed "+_pnfsId ) ;
       }
       private void setPrimaryState( int newPrimaryState ) throws CacheException {
          setPrimaryState( newPrimaryState , false ) ;
       }
       private void setPrimaryState( int newPrimaryState, boolean force ) throws CacheException {
          switch( newPrimaryState ){
             case PRECIOUS : 
                 if( ( ! force ) && ( ( _state & FROM_CLIENT ) == 0 ) )
                    throw new 
                    TransitionException( _state , newPrimaryState ) ;
             break ;
             case CACHED : 
                 if( ( ! force ) && 
                     ( ( _state & PRECIOUS ) == 0 ) &&
                     ( ( _state & FROM_STORE ) == 0 ) )
                    throw new 
                    TransitionException( _state , newPrimaryState ) ;
             break ;
             case FROM_CLIENT : 
             case FROM_STORE  : 
                 if( ( ! force ) &&  ( ( _state & PRIMARY ) != 0 ) )
                    throw new TransitionException( _state , newPrimaryState ) ;
             break ;
          }
          _state &= ~ PRIMARY ;
          _state |= newPrimaryState ;
       }
       public void setBad( boolean bad ){
          if( bad )_state |= BAD ;
          else _state &= ~ BAD ;
       }
       public boolean isBad(){ return ( _state & BAD ) != 0 ; }
       public File getDataFile(){
          return new File( _dataDir , _pnfsId.toString() ) ;
       }
       public long getSize()  { 
         File file = getDataFile() ;
         return file.exists() ? file.length() : 0 ;
         /*
         return (( _state & PRECIOUS ) > 0 )|| 
                (( _state & CACHED   ) > 0 ) ? getDataFile().length() : 0 ;
         */
       }
       public long getCreationTime(){ return _creationTime ; }
       public String toString(){
          return _pnfsId.toString()+
                 " <"+statusToString(_state)+(_isLocked?"L":"-")+
                 "("+getRestLock()+")"+
                 "["+_links+"]> "+
                 getSize()+
                 " si={"+(_storageInfo==null?"<unknown>":_storageInfo.getStorageClass())+"}" ;
       }
       public  synchronized void setStorageInfo( StorageInfo info )
               throws CacheException{
           _setStorageInfo( info ) ;
       }
       private  synchronized void _setStorageInfo( StorageInfo info )
               throws CacheException{
       
           File f = new File( _controlDir , ".SI-"+_pnfsId ) ;
           ObjectOutputStream out = null ;
           try{
               
              out = new ObjectOutputStream(
                       new FileOutputStream(f) ) ;
              out.writeObject( info ) ;
           }catch(Exception e){
              f.delete() ;
              throw new
              CacheException(ERROR_IO_DISK,_pnfsId+" "+e.toString());
           }finally{
              try{ out.close() ; }catch(Exception we ){}
           }
           File rf = new File( _controlDir , "SI-"+_pnfsId ) ;
           if( _isWindows )rf.delete() ;
           if( ! f.renameTo( rf ) ){
              f.delete() ;
              throw new
              CacheException(ERROR_IO_DISK,"Rename failed : "+_pnfsId);
           }
           _storageInfo = info ;
       }
       public PnfsId       getPnfsId(){ return _pnfsId ; }
       public StorageInfo  getStorageInfo(){ 
          return _storageInfo ;
       }
       
       private StorageInfo _getStorageInfo()
               throws CacheException{
              
              
           File f = new File( _controlDir , "SI-"+_pnfsId ) ;
           ObjectInputStream in   = null ;
           StorageInfo       info = null ;
           try{
              in   = new ObjectInputStream(
                         new BufferedInputStream(new FileInputStream(f)));
              info = (StorageInfo)in.readObject() ;
           }catch(Exception e ){
              throw new
              CacheException(201,_pnfsId+" "+e.toString());
           }finally{
              try{ in.close() ; }catch(Exception we ){}
           }
           _creationTime = f.lastModified() ;
           return info ;
       }
       private synchronized void update() throws CacheException {
           _read() ;
           try{
              _storageInfo = _getStorageInfo() ;
           }catch(CacheException ee ){
              if( isPrecious() )throw ee ;
           }
       }
       public synchronized void setPrecious() throws CacheException {
          setPrecious(false);
       }
       public synchronized void setPrecious( boolean force ) throws CacheException {
           checkDestroyed() ;
           setPrimaryState( PRECIOUS , force ) ;
           _write2() ;

           CacheRepositoryEvent re = 
               new CacheRepositoryEvent(CacheRepository2.this,this) ;
           
           _preciousSpace += getSize() ;
           
           processAvailableEvent( re )  ;
           processPreciousEvent( re ) ;
           
       }
       public synchronized void setReceivingFromClient() throws CacheException {
          checkDestroyed() ;
          setPrimaryState( FROM_CLIENT ) ;
          _write2() ;
          processCreatedEvent( new CacheRepositoryEvent( CacheRepository2.this , this ) ) ;
       }
       public synchronized void setReceivingFromStore() throws CacheException {
          checkDestroyed() ;
          setPrimaryState( FROM_STORE ) ;
          _write2() ;
          processCreatedEvent( new CacheRepositoryEvent( CacheRepository2.this , this ) ) ;
       }
       public synchronized void setSendingToStore( boolean sending ) throws CacheException {
          checkDestroyed() ;
          if( ( _state & PRECIOUS ) == 0 )
             throw new TransitionException( _state , _state | TO_STORE ) ;
          _state = sending ? ( _state | TO_STORE ) : ( _state & ~ TO_STORE )  ;
       }
       public synchronized void incrementLinkCount() throws CacheException {
          checkDestroyed() ;
          _links ++ ;
          processTouchedEvent( new CacheRepositoryEvent( CacheRepository2.this , this ) ) ;
       }
       public synchronized void decrementLinkCount() throws CacheException {
          checkDestroyed() ;
          _links -- ;
          if( ( (_state & REMOVED) > 0 ) && ( _links <= 0 ) )destroy() ;
       }
       public synchronized int getLinkCount() throws CacheException {
          checkDestroyed() ;
          return _links ;
       }
       public  synchronized boolean isSticky(){ 
          return (_state & STICKY) > 0 ; 
       }
       public synchronized void setSticky( boolean sticky ) throws CacheException {
          checkDestroyed() ;
          //
          //
          if( ! ( sticky ^ ( ( _state & STICKY ) > 0 ) ) )return ;
          
          if( sticky )_state |= STICKY ;
          else _state &= ~ STICKY ;
          
          _write2() ;
               
          CacheRepositoryEvent re = 
               new CacheRepositoryEvent(CacheRepository2.this,this) ;
          processStickyEvent( re ) ;
          
       }
       public  synchronized void setCached() throws CacheException {
           checkDestroyed() ;
           int oldState = _state ;
           
           setPrimaryState( CACHED ) ;
           _write2() ;
           
           CacheRepositoryEvent re = 
               new CacheRepositoryEvent(CacheRepository2.this,this) ;
           
           
           //
           // clear the TO_STORE (substate) if we stored the file.
           //
           _state &= ~ TO_STORE ;
           //
           // send availableEvent if we came from the store,
           // else (if we have been precious before, substract the 
           // precious size).
           //  
           if( ( oldState & FROM_STORE ) != 0 ){
               lock( 60 * 1000 ) ; // make sure it's not removed im.
               processAvailableEvent( re ) ;
           }else if( ( oldState & PRECIOUS ) != 0 ){
               _preciousSpace -= getSize() ;
           }

           processCachedEvent( re ) ;
       }
       public synchronized boolean isReceivingFromClient() throws CacheException {
          return ( _state & FROM_CLIENT ) != 0  ; 
       }
       public synchronized boolean isReceivingFromStore() throws CacheException {
          return ( _state & FROM_STORE ) != 0  ; 
       }
       public synchronized boolean isSendingToStore() throws CacheException {
          return ( _state & TO_STORE ) != 0  ; 
       }
       public synchronized boolean isPrecious() throws CacheException {
          return ( _state & PRECIOUS ) != 0  ; 
       }
       public synchronized boolean isCached() throws CacheException {
          return ( _state & CACHED ) != 0  ; 
       }
       public synchronized boolean isDestroyed()  {
          return ( _state & DESTROYED ) != 0 ;
       }
       public synchronized boolean isRemoved()  {
          return ( _state & REMOVED ) != 0 ;
       }
       public synchronized void lock( boolean locked ){
          _isLocked = locked ;
       }
       public synchronized void lock( long milliSeconds ){
          long lockedUntil = System.currentTimeMillis() + milliSeconds ;
          if( _lockedUntil > lockedUntil )return ;
          _lockedUntil = milliSeconds == 0L ? 0 :  lockedUntil ;
       }
       public synchronized boolean isLocked(){          
          return _isLocked || ( getRestLock() > 0L ) ;
       }
       private synchronized long getRestLock(){
          if( _lockedUntil == 0L )return 0L ;
          long rest = _lockedUntil - System.currentTimeMillis()  ;
          if( rest < 0L ){
             _lockedUntil = 0L ;
             return 0L ;
          }
          return rest ;
       }
       public synchronized long getLastAccessTime() throws CacheException {
          return _lastAccess ;
       }
       public synchronized void touch() throws CacheException {
          File file = getDataFile() ;
          try{
             if( ! file.exists() )file.createNewFile() ;
          }catch(IOException ee){
             throw new
             CacheException("Io Error creating : "+file ) ;
          }
          getDataFile().setLastModified(_lastAccess= System.currentTimeMillis());
       }
       private synchronized void remove(){
          _state |= REMOVED ;
       }
       private synchronized void destroy(){
         new File( _controlDir ,"SI-"+_pnfsId ).delete() ;
         new File( _controlDir ,".SI-"+_pnfsId ).delete() ;
         new File( _controlDir ,"."+_pnfsId ).delete() ;
         new File( _controlDir ,_pnfsId.toString() ).delete() ;
         File dataFile = new File( _dataDir ,_pnfsId.toString() ) ;
         if( dataFile.exists() ){
            long size = dataFile.length() ;
            freeSpace(size) ;
            try{
              if( isPrecious() )_preciousSpace -= size ;
            }catch(Exception ii){}
            dataFile.delete() ;
         }
         _removedPnfsids.remove( _pnfsId ) ;
         _state |= DESTROYED ;
        
       }
       private synchronized void recover( PnfsHandler pnfs ) throws CacheException {

          StorageInfo storageinfo = null ; 
          _log.log( "Trying to recover "+_pnfsId ) ;
          while( ! Thread.interrupted() ){         
             try{
                _log.elog( "Recover "+_pnfsId.toString()+" : Trying to get storageinfo" ) ;
                storageinfo = pnfs.getStorageInfo(_pnfsId.toString()) ;
                _log.elog( "Recover "+_pnfsId.toString()+" : storageinfo of "+storageinfo ) ;
                break ;
             }catch(CacheException ce ){
                _log.elog( "Recover "+_pnfsId.toString()+" : get storageinfo got "+ce) ;
                if( ce.getRc() == CacheException.TIMEOUT ){
                   _log.elog( "Recover "+_pnfsId.toString()+" : Pnfs Request timed out (going to sleep)") ;
                   try{ Thread.sleep(30000) ; }catch(InterruptedException ee ){}
                   continue ;
                }
                throw ce ;
             }
          }
          if( storageinfo.isCreatedOnly() )
            throw new
            CacheException( CacheException.FILESIZE_UNKNOWN , 
                            "Unrecoverable : Filesize not yet set in pnfs" ) ;

          //
          if( storageinfo.getFileSize() != getSize() )
               throw new
               CacheException( CacheException.FILESIZE_MISMATCH , "File size mismatch" ) ;
          // 
          // if the filesize matches, we can keep the file
          //
          _state = storageinfo.isStored() ?  CACHED : PRECIOUS ;
          setStorageInfo( storageinfo ) ;
          _write2() ;
          return ;
       }
       public synchronized CacheRepositoryStatistics getCacheRepositoryStatistics() throws CacheException {
          throw new CacheException("no statistics" ) ;
       }
       private void _write( String str ) throws CacheException {
           PrintWriter pw = null ;
           File f  = new File( _controlDir , "."+_pnfsId.toString() ) ;
           File rf = new File( _controlDir , _pnfsId.toString() ) ;
           try{
              pw = new PrintWriter(
                     new FileWriter( f ) ) ;
              pw.println(str) ;
              if( ( _state & STICKY ) > 0 )pw.println("sticky") ;
           }catch( IOException ioe ){
              f.delete() ;
               throw new 
               CacheException(206,ioe.toString() ) ;
           }finally{
              try{ pw.close() ; }catch(Exception ee){}
           }
           if( _isWindows )rf.delete() ;
           if( ! f.renameTo( rf ) ){
              f.delete() ;
              throw new 
              CacheException( 207,"Rename failed : "+_pnfsId ) ;
           }
       }
       private void _write2() throws CacheException {
           PrintWriter pw = null ;
           File f  = new File( _controlDir , "."+_pnfsId.toString() ) ;
           File rf = new File( _controlDir , _pnfsId.toString() ) ;
           String
           str = (_state & PRECIOUS)    > 0  ? "precious" :
                 (_state & FROM_STORE)  > 0  ? "receiving.store" :
                 (_state & FROM_CLIENT) > 0  ? "receiving.cient" :
                 (_state & CACHED)      > 0  ? "cached" :
                                              "unknown"  ;
           try{
              pw = new PrintWriter(
                     new FileWriter( f ) ) ;
              pw.println(str) ;
              if( ( _state & STICKY ) > 0 )pw.println("sticky") ;
           }catch( IOException ioe ){
              f.delete() ;
               throw new 
               CacheException(206,ioe.toString() ) ;
           }finally{
              try{ pw.close() ; }catch(Exception ee){}
           }
           if( _isWindows )rf.delete() ;
           if( ! f.renameTo( rf ) ){
              f.delete() ;
              throw new 
              CacheException( 207,"Rename failed : "+_pnfsId ) ;
           }
       }
       private String _read() throws CacheException {
           File f = new File( _controlDir , _pnfsId.toString() ) ;
           BufferedReader br = null; 
           String line = null , line2 = null ;
           try{
              br = new BufferedReader(
                      new FileReader( f ) ) ;
              if( ( line  = br.readLine() ) != null )line2 = br.readLine() ;
           }catch(IOException e ){
               throw new 
               CacheException(205,e.toString() ) ;
           }finally{
               try{ br.close() ; }catch(Exception fe){}
           }
           if( ( line == null ) || ( line.length() == 0 ) )
              throw new
              CacheException( 206,"Illegal control file content (empty)" ) ;
              
           _state = 0 ;
           if( line.equals("precious") )_state = PRECIOUS ;
           else if( line.equals("receiving.store" )  )_state = FROM_STORE ;
           else if( line.equals("receiving.client" ) )_state = FROM_CLIENT ;
           else if( line.equals("cached" ) )_state = CACHED ;
           else  
              throw new 
              CacheException( 210 , "Illegal Control State : "+line ) ;

           if( ( line2 != null ) && ( line2.equals("sticky" ) ) )
              _state |= STICKY ;
           return line ;
       }
    }
    //
    // -------------------------------------------------------------------------
    //
    //    The CacheRepository
    //
    public CacheRepository2( File baseDir ) throws Exception {
       _baseDir    = baseDir ;
       _controlDir = new File( _baseDir , "control" ) ;
       _dataDir    = new File( _baseDir , "data" ) ;

       String tmp = (String)System.getProperties().get("os.name");
       _isWindows = ( tmp != null ) && ( tmp.toLowerCase().indexOf("windows") > -1 ) ;
       
       _spaceReservation = new File( _controlDir , "SPACE_RESERVATION" ) ;
       
       _spaceMonitor.addSpaceRequestListener( new NeedSpace() ) ;
       
       if( _log == null )_log = new Logable(){
           public void log( String s){} ;
           public void elog(String s){} ;
           public void plog(String s){} ;
       } ;

       initReservedSpace() ;

    }
    public void setLogable( Logable log ){
       _log = log ;
    }
    private void checkBaseDir() throws CacheException {
	if( ! _dataDir.isDirectory() )
	    throw new 
            CacheException(
            "Not a valid Pool Base (no data) : "+_dataDir );
		
	if( ! _controlDir.isDirectory() )
	    throw new 
            CacheException(
            "Not a valid Pool Base (no control) : "+_dataDir );
                           
    }
    public synchronized void runInventory() throws CacheException { 
       runInventory(null) ; 
    }
    
    public synchronized void runInventory( Logable log ) throws CacheException {
       runInventory( log , null , 0 ) ;
    }
    private class CacheEntryComparable implements Comparator {
        public int compare( Object o1 , Object o2 ){
          try{
           CacheEntry e1 = (CacheEntry)o1 ;
           CacheEntry e2 = (CacheEntry)o2 ;
           long l1 = e1.getLastAccessTime() ;
           long l2 = e2.getLastAccessTime() ;
           //
           // return revers sorting
           //
           return l1 == l2 ? 
                  e1.getPnfsId().toString().compareTo(e2.getPnfsId().toString()) :
                  l1 < l2 ? -1 : 1 ;
                  
          }catch(CacheException ce){
             throw new
             ClassCastException(ce.toString());
          }
        }
    }
    public synchronized void runInventory( 
              Logable log , 
              PnfsHandler pnfsHandler ,
              int flags ) 
           throws CacheException {

       if( _inventoryCounter ++ != 0 )resetAll() ;

       String [] list = _dataDir.list() ;
   
       if( log == null )log = new Logable(){
           public void log( String s){} ;
           public void elog(String s){} ;
           public void plog(String s){} ;
       } ;
       log.log( "runInventory : "+(list==null?"No":(""+list.length))+
                " datafile(s) found in "+_dataDir );
       
       runControlCrosscheck( log ) ;
       
       CacheEntry entry  = null ;
       PnfsId     pnfsId = null ;
       TreeMap sortedMap = new TreeMap(new CacheEntryComparable()) ;
       
       for( int i = 0 ; i < list.length ; i++ ){
          try{
          
             if( list[i].endsWith(".crcval") ){
                 log.elog("Removing "+list[i] ) ;
                 new File( _dataDir , list[i] ).delete() ;
                 continue ;
             }
             try{
                pnfsId = new PnfsId( list[i] ) ;
             }catch( Exception npnfs ){
                log.elog( "Not a pnfsId : "+list[i] ) ;
                continue ;
             }
             entry  = new CacheEntry( pnfsId ) ;
             entry._read() ;
             if( ( ( entry._state & CacheEntry.FROM_CLIENT) > 0 ) ||
                 ( ( entry._state & CacheEntry.FROM_STORE ) > 0 ) )
                 throw new
                 CacheException( entry.getPnfsId()+" : File in transient state detected" ) ;

             entry._storageInfo  = entry._getStorageInfo() ;

             if( entry._storageInfo.getFileSize() != entry.getDataFile().length() )
                 throw new
                 CacheException( CacheException.FILESIZE_MISMATCH, entry.getPnfsId()+" : FileSize mismatch" ) ;
               
             entry._lastAccess   = entry.getDataFile().lastModified() ;
             log.log( pnfsId.toString() + " : "+ entry ) ; 
             sortedMap.put( entry , entry ) ; 
             //  
          }catch(CacheException e ){
             log.elog( pnfsId.toString() + " : "+e.toString() ) ;
             if(  ( pnfsHandler == null ) || 
                  ( flags & ( ALLOW_CONTROL_RECOVERY | ALLOW_RECOVER_ANYWAY ) ) == 0 )
                throw e ;
                    
             log.elog( "Trying to recover : "+pnfsId ) ;
             try{

                entry.recover( pnfsHandler );
                log.elog(pnfsId.toString() + " : recovered sucessfully" ) ;
                entry._lastAccess   = entry.getDataFile().lastModified() ;
                sortedMap.put( entry  , entry ) ; 

             }catch(CacheException ce ){
                     //  
                switch( ce.getRc() ){
                   case CacheException.FILE_NOT_FOUND :
                       log.elog(pnfsId.toString() + " : recover : file not found -> removed" ) ;
                       new File( _controlDir ,"SI-"+pnfsId ).delete() ;
                       new File( _controlDir ,".SI-"+pnfsId ).delete() ;
                       new File( _controlDir ,"."+pnfsId ).delete() ;
                       new File( _controlDir ,pnfsId.toString() ).delete() ;
                       new File( _dataDir ,pnfsId.toString() ).delete() ;
                   break ;
                   case CacheException.FILESIZE_MISMATCH :
                       log.elog(pnfsId.toString() + " : recover : filesize mismatch" ) ;

                       if( ( entry._state & (CacheEntry.FROM_STORE|CacheEntry.CACHED) ) != 0 ){

                          log.elog(pnfsId.toString() + " : recover : file removed" ) ;
                          new File( _controlDir ,"SI-"+pnfsId ).delete() ;
                          new File( _controlDir ,".SI-"+pnfsId ).delete() ;
                          new File( _controlDir ,"."+pnfsId ).delete() ;
                          new File( _controlDir ,pnfsId.toString() ).delete() ;
                          new File( _dataDir ,pnfsId.toString() ).delete() ;

                       }else if( (flags & ALLOW_RECOVER_ANYWAY ) != 0 ){

                          log.elog(pnfsId.toString() + " : recover : file disabled" ) ;
                          entry.setBad(true);
                          sortedMap.put( entry  , entry ) ; 

                       }else{
                          throw ce ;
                       }

                   break ;
                   case CacheException.FILESIZE_UNKNOWN :
                       log.elog(pnfsId.toString() + " : recover : filesize unknown (very new file)" ) ;
                       if( (flags & ALLOW_RECOVER_ANYWAY ) != 0 ){

                          log.elog(pnfsId.toString() + " : recover : file disabled" ) ;
                          entry.setBad(true);
                          sortedMap.put( entry  , entry ) ; 

                       }else{
                          throw ce ;
                       }
                   break ;
                   default :
          
                       log.elog(pnfsId.toString() + " : recover : unexpected cache exception" ) ;
                       if( (flags & ALLOW_RECOVER_ANYWAY ) != 0 ){

                          log.elog(pnfsId.toString() + " : recover : file disabled" ) ;
                          entry.setBad(true);
                          sortedMap.put( entry  , entry ) ; 

                       }else{
                          throw ce ;
                       }
                   break ;
                }
             }
          }
       
       }
       Iterator entryLoop  = sortedMap.values().iterator() ;
       long     totalSpace = _reservedSpace ;
       while( entryLoop.hasNext() ){
          entry =(CacheEntry)entryLoop.next() ;
          log.log( "Sorted : "+entry.getPnfsId()+" : "+entry.getLastAccessTime() ) ;
          _pnfsids.put( entry.getPnfsId() , entry ) ;
          long fileLength = entry.getSize();
          totalSpace     += fileLength ;
          if( entry.isPrecious() )_preciousSpace += fileLength ;
          processScannedEvent( new CacheRepositoryEvent( this , entry ) ) ;
       }
       if( totalSpace > _spaceMonitor.getTotalSpace() ){
           String error = "Inventory : overbooked "+
                          totalSpace+" > "+
                          _spaceMonitor.getTotalSpace() ;
           if( ( flags & ALLOW_SPACE_RECOVERY ) == 0 )
             throw new
             CacheException( 206 , error ) ;
             
           log.elog(error);
           
           double diff = (double)totalSpace - (double)_spaceMonitor.getTotalSpace() ;
           if( diff / (double)totalSpace > 0.1)
             throw new
             CacheException("Inventory : total Space exceeded by > 10%, can't recover" ) ;
           entryLoop = sortedMap.values().iterator() ;
           while( entryLoop.hasNext() && ( totalSpace > _spaceMonitor.getTotalSpace() ) ){
              entry =(CacheEntry)entryLoop.next() ;
              if( entry.isPrecious() )continue ;
              processRemovedEvent( new CacheRepositoryEvent( this , entry ) ) ;
              long fileLength = entry.getSize();
              totalSpace     -= fileLength ;
              pnfsId          = entry.getPnfsId() ;
              _pnfsids.remove( pnfsId ) ;
              log.elog(pnfsId.toString() + " : overbooked : removed" ) ;
              new File( _controlDir ,"SI-"+pnfsId ).delete() ;
              new File( _controlDir ,".SI-"+pnfsId ).delete() ;
              new File( _controlDir ,"."+pnfsId ).delete() ;
              new File( _controlDir ,pnfsId.toString() ).delete() ;
              new File( _dataDir ,pnfsId.toString() ).delete() ;
              
           }
       
       }
       try{
          _spaceMonitor.allocateSpace( totalSpace , 1000 ) ;
       }catch(InterruptedException ee ){
          throw new
          CacheException(
             666 , "Not enough space in repository to store inventory ???");
       }
       log.elog( "runInventory : #="+_pnfsids.size()+
                ";space="+_spaceMonitor.getFreeSpace()+
                "/"+_spaceMonitor.getTotalSpace() );
       
    }
    private void runControlCrosscheck( Logable log ){
       String [] list = _controlDir.list() ;
       log.log( "runControlCrosscheck : checking "+list.length+" entries");
       for( int i = 0 , n = list.length ; i < n ; i++ ){
          String name = list[i] ;
          try{
          
             log.log("runControlCrosscheck : checking : "+name ) ;
             if( name.startsWith( "SI-" ) )name = name.substring(3) ;
             new PnfsId( name ) ;

             File f = new File( _dataDir , name ) ;
             if( ! f.exists() ){
                log.elog( "runControlCrosscheck : no datafile found for : "+name+" ; removing control");
                new File( _controlDir , name ).delete() ;
                new File( _controlDir , "SI-"+name ).delete() ;
             }
          }catch(Exception ee ){
             log.elog( "runControlCrosscheck : illegal file name found : "+name+" ; skipping");
          }
       }
       log.log( "runControlCrosscheck : done");
    }
    public synchronized Iterator pnfsids(){ 
       ArrayList result = new ArrayList() ;

       result.addAll( _pnfsids.keySet() );
       result.addAll( _removedPnfsids.keySet() );
       
       return result.iterator() ; 
    }
    public synchronized Iterator getActivePnfsids(){ 
       ArrayList result = new ArrayList() ;
       Iterator i = _pnfsids.keySet().iterator() ;
       while( i.hasNext() )result.add( i.next() ) ;
       return result.iterator() ; 
    }
    public synchronized List getValidPnfsidList(){ 
       ArrayList result = new ArrayList() ;
       Iterator i = _pnfsids.values().iterator() ;
       while( i.hasNext() ){
          CacheEntry e = (CacheEntry)i.next() ;
          try{
             if( e.isPrecious() || e.isCached() )
                 result.add( e.getPnfsId() ) ;
          }catch(Exception ee ){
          }
       }
       return result ; 
    }
    
    public synchronized CacheRepositoryEntry createEntry( PnfsId pnfsId )
           throws CacheException{
        CacheEntry entry = (CacheEntry)_pnfsids.get( pnfsId ) ;
        if( entry != null )
           throw new
           FileInCacheException( "Entry already exists (mem) : "+pnfsId ) ;
    
        try{
           File f= new File( _controlDir , pnfsId.toString() ) ;
           if( ! f.createNewFile() )
              throw new
              CacheException( 203,"PANIC : Entry already exists (fs) : "+pnfsId ) ;
        }catch(IOException ioe ){
          
           throw new 
           CacheException( ERROR_IO_DISK , "Low Level Exc : "+ioe) ;
        }
        entry = new CacheEntry( pnfsId ) ;
        _pnfsids.put( pnfsId , entry ) ;
        //
        //
        return entry ;
    }
    /**
      */
    public synchronized boolean removeEntry( CacheRepositoryEntry entry ) 
           throws CacheException {
        CacheEntry cacheEntry = (CacheEntry)entry ;
        if( cacheEntry.isDestroyed() || cacheEntry.isRemoved() )
           throw new
           FileNotInCacheException("Entry already removed") ;
        
        if( cacheEntry.isLocked() )return false ;
        
        PnfsId pnfsId = entry.getPnfsId() ;
        
        _pnfsids.remove( pnfsId ) ;
        _removedPnfsids.put( pnfsId , entry ) ;
        File f = entry.getDataFile() ;
        boolean fileExisted = f.exists() ;
        long    size        = fileExisted ? f.length() : 0 ;
        
        cacheEntry.remove() ;
        
        CacheRepositoryEvent removeEvent = new CacheRepositoryEvent( this , entry ) ;
        
        processRemovedEvent( removeEvent ) ;
        
        if( entry.getLinkCount() == 0 ){
            cacheEntry.destroy() ;
            processDestroyedEvent( new CacheRepositoryEvent( this , entry ) ) ;
        }
        
        return true ;
    }
    public CacheRepositoryEntry getEntry( PnfsId pnfsId )  throws CacheException {
       CacheEntry e = (CacheEntry)_pnfsids.get( pnfsId ) ;
       if( e ==  null )  {
         throw new 
         FileNotInCacheException( "Entry not in repository : "+pnfsId ) ;
       }
       return (CacheRepositoryEntry)e ;
    }
    public CacheRepositoryEntry getGenericEntry( PnfsId pnfsId )  throws CacheException {
       CacheEntry e = (CacheEntry)_pnfsids.get( pnfsId ) ;
       if( e ==  null ){
            e = (CacheEntry)_removedPnfsids.get( pnfsId ) ;
            if( e ==  null )
              throw new 
              FileNotInCacheException( "Entry not in (removed) repository : "+pnfsId ) ;
       }
       return (CacheRepositoryEntry)e ;
    }
    public boolean isRepositoryOk(){
       try{
       
           if( ! new File( _baseDir , "setup" ).exists() )return false ;
           
           File tmp = new File( _baseDir , "RepositoryOk" ) ;
	   
	   tmp.delete() ;

	   tmp.deleteOnExit() ;
	   
	   if( ! tmp.createNewFile() )return false ;
	   
	   if( ! tmp.exists() )return false ;
	   
	   return true ;
	   
	   
	}catch(Exception ee ){
	   return false ;
	}
    }
    private CacheRepositoryListener eventListener = null ;
    public synchronized void 
           addCacheRepositoryListener( CacheRepositoryListener listener ){
        eventListener = CacheEventMulticaster.add(eventListener,listener);
    }
    public synchronized void 
           removeCacheRepositoryListener( CacheRepositoryListener listener ){
        eventListener = CacheEventMulticaster.remove(eventListener,listener);
    }
    public void  processPreciousEvent( CacheRepositoryEvent event ){
       if( eventListener != null )
          eventListener.precious( event ) ;
    }
    public void  processCachedEvent( CacheRepositoryEvent event ){
       if( eventListener != null )
          eventListener.cached( event ) ;
    }
    public void  processCreatedEvent( CacheRepositoryEvent event ){
       if( eventListener != null )
          eventListener.created( event ) ;
    }
    public void  processTouchedEvent( CacheRepositoryEvent event ){
       if( eventListener != null )
          eventListener.touched( event ) ;
    }
    public void  processRemovedEvent( CacheRepositoryEvent event ){
       if( eventListener != null ){
          eventListener.removed( event ) ;
       }
    }
    public void  processDestroyedEvent( CacheRepositoryEvent event ){
       if( eventListener != null ){
          eventListener.destroyed( event ) ;
       }
    }
    public void  processScannedEvent( CacheRepositoryEvent event ){
       if( eventListener != null )
          eventListener.scanned( event ) ;
    }
    public void  processAvailableEvent( CacheRepositoryEvent event ){
       if( eventListener != null )
          eventListener.available( event ) ;
    }
    public void  processStickyEvent( CacheRepositoryEvent event ){
       if( eventListener != null )
          eventListener.sticky( event ) ;
    }
    public void  processNeedSpaceEvent( CacheNeedSpaceEvent event ){
       if( eventListener != null )
          eventListener.needSpace( event ) ;
    }
    //
    // Space reservation.
    //
    private synchronized void initReservedSpace() throws CacheException{
        _reservedSpace = 0L ;
       if( ! _spaceReservation.exists() )return ;
       try{
           BufferedReader br = new BufferedReader( 
                                  new InputStreamReader(
                                      new FileInputStream( _spaceReservation ) ) ) ;

           
           try{
           
              String valueString = br.readLine() ;
              if( valueString == null )return ;
              try{
                 _reservedSpace = Long.parseLong(valueString);
              }catch(Exception ee ){
                  _log.elog("SPACE_RESERVATION is invalid (not a number) : resetting to 0");
                  _reservedSpace = 0L ;
                   storeReservedSpace() ;
              }
              
           }finally{
              try{ br.close() ; }catch(Exception eee){}
           } 
       }catch(CacheException cioe ){
          throw cioe ;
       }catch(Exception ioe ){
           throw new
           CacheException(104,"Exception, reading "+_spaceReservation) ;
       }
    }
    private void storeReservedSpace() throws CacheException {
       try{
           PrintWriter pw = new PrintWriter( new FileOutputStream( _spaceReservation ) ) ;
           try{
              pw.println(""+_reservedSpace);
           }finally{
              try{ pw.close() ; }catch(Exception eee){}
           } 
       }catch(IOException ioe ){
           throw new
           CacheException(103,"Io Exception, writing "+_spaceReservation) ;
       }   
    }
    public void reserveSpace( long space , boolean blocking )
           throws CacheException , InterruptedException{
           
        if( space < 0L )
           throw new
           IllegalArgumentException("Space to reserve must be > 0");
           
        allocateSpace( space , blocking ? SpaceMonitor.BLOCKING : SpaceMonitor.NONBLOCKING ) ; 

        synchronized( this ){ 
           _reservedSpace += space ;
           try{
               storeReservedSpace() ;
           }catch(CacheException ee ){
              _reservedSpace -= space ;
              throw ee ;        
           }
        }
    }
    public void applyReservedSpace( long space ) 
           throws CacheException {
       
         modifyReservedSpace( space , false ) ;
    } 
    public void freeReservedSpace( long space ) 
           throws CacheException {
       
         modifyReservedSpace( space , true ) ;
    } 
    public void modifyReservedSpace( long space , boolean freeSpace ) 
           throws CacheException {
           
        if( space < 0L )
           throw new
          IllegalArgumentException("Space to free must be > 0");
          
        if( ( _reservedSpace - space ) < 0L )
           throw new
          IllegalArgumentException("Inconsistent space request (result<0)");
           
        if( freeSpace )freeSpace( space ) ; 
        synchronized( this ){
           _reservedSpace -= space ;
           try{
              storeReservedSpace() ;
           }catch(CacheException ee ){
              _reservedSpace += space ;
              throw ee ;        
           }
        }
    }

    public synchronized long getReservedSpace() {
       return _reservedSpace ;           
    }
    //
    // Space management
    //
    public long getTotalSpace(){ return _spaceMonitor.getTotalSpace() ; }
    public long getFreeSpace(){ return _spaceMonitor.getFreeSpace() ; }
    public long getPreciousSpace(){ return _preciousSpace ; }
    public void setTotalSpace( long size ){ 
        _spaceMonitor.setTotalSpace( size ) ; 
    }
    public void allocateSpace( long space )
           throws InterruptedException {
       _spaceMonitor.allocateSpace( space ) ;
    }
    public void allocateSpace( long space , long millis )
           throws InterruptedException {

       if( millis == SpaceMonitor.NONBLOCKING ){
          synchronized( _spaceMonitor ){
             if( ( _spaceMonitor.getTotalSpace() - 
                   _preciousSpace -
                   _reservedSpace ) < ( 3 * space ) )
               throw new
               MissingResourceException("Not enough Space Left",this.getClass().getName(),"Space") ;

              _spaceMonitor.allocateSpace( space ) ;

          }
       }else if(  millis == SpaceMonitor.BLOCKING ){
          _spaceMonitor.allocateSpace( space ) ;
       }else{
          _spaceMonitor.allocateSpace( space , millis ) ;
       }
    }
    public void freeSpace( long space ){
       _spaceMonitor.freeSpace( space ) ;
    }
    public synchronized void addSpaceRequestListener( SpaceRequestable listener ){
      throw new IllegalArgumentException("Not supported") ;
   
    }
    public boolean contains(PnfsId pnfsId) {
        return _pnfsids.containsKey( pnfsId ) ;
    }
}
