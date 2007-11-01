// $Id: HsmStorageHandler2.java,v 1.32.2.2 2007-03-23 16:58:00 tigran Exp $

package diskCacheV111.pools ;

import  diskCacheV111.util.* ;
import  diskCacheV111.vehicles .* ;
import  diskCacheV111.repository.* ;

import  dmg.util.* ;
import  dmg.cells.nucleus.* ;

import  java.util.* ;
import  java.io.IOException ;
import  java.io.PrintWriter ;
import  java.io.RandomAccessFile;
import  java.lang.reflect.InvocationTargetException ;

public class HsmStorageHandler2  {

    private final CacheRepository _repository  ;
    private Logable         _log         = null ;
    private final HsmSet          _hsmSet      ;
    private final PnfsHandler     _pnfs        ;
    private final CellAdapter     _cell        ;
    private final Hashtable       _storePnfsidList   = new Hashtable() ;
    private final Hashtable       _restorePnfsidList = new Hashtable() ;
    private final Object          _listLock          = new Object()  ;
    private long            _maxRuntime   = 4 * 3600 * 1000 ;
    private long            _maxStoreRun  = _maxRuntime ;
    private long            _maxRestoreRun= _maxRuntime ;
    private int             _maxLines     = 200 ;
    private final HsmStorageHandler2 _storageHandler ;
    private final JobScheduler       _fetchQueue     ;
    private final JobScheduler       _storeQueue     ;
    private boolean            _stickyAllowed  = false ;
    private final boolean            _checkPnfs      = true ;
    private final boolean     _removeUnexistingEntries;
    ////////////////////////////////////////////////////////////////////////////////
    //
    //    the generic part
    //
    public class Info {
        private long      _startTime = System.currentTimeMillis();
        private ArrayList _callbacks = new ArrayList() ;
        private final PnfsId    _pnfsId  ;
        private Thread    _thread    = null ;
        private boolean   _active    = false ;
        private Info( PnfsId pnfsId ){
           _pnfsId = pnfsId ;
        }
        public PnfsId getPnfsId(){ return _pnfsId ; }
        public int getListenerCount(){ return _callbacks.size() ; }
        public  long   getStartTime(){ return _startTime ; }
        public  Thread getThread(){ return _thread ; }
        public void startThread(){ _thread.start() ; }
        public void done(){ _active = false ; }
        synchronized void  addCallback( CacheFileAvailable callback ){
           _callbacks.add(callback) ;
        }
        synchronized List getCallbacks(){ return new ArrayList(_callbacks) ; }
        void   setThread( Thread thread ){ _thread = thread ; }
    }
    public HsmStorageHandler2( CellAdapter     cell ,
                               CacheRepository repository ,
                               HsmSet          hsmSet ,
                               PnfsHandler     pnfs ){
       _repository = repository ;
       _hsmSet     = hsmSet ;
       _pnfs       = pnfs ;
       _cell       = cell ;
       _storageHandler = this ;
       if( cell instanceof Logable )setLogable( (Logable)cell ) ;

       _fetchQueue = new SimpleJobScheduler( cell.getNucleus().getThreadGroup() , "F" ) ;
       _storeQueue = new SimpleJobScheduler( cell.getNucleus().getThreadGroup() , "S" ) ;

       _removeUnexistingEntries = Boolean.valueOf( _cell.getArgs().getOpt("remove-unexisting-entries-on-flush") ).booleanValue();
    }
    void setTimeout( long storeTimeout , long restoreTimeout ){

       if( storeTimeout > 0   )_maxStoreRun   = storeTimeout ;
       if( restoreTimeout > 0 )_maxRestoreRun = restoreTimeout ;

    }
    public void setStickyAllowed( boolean sticky ){
       _stickyAllowed = sticky ;
    }
    public void setMaxActiveRestores( int restores ){ _fetchQueue.setMaxActiveJobs( restores) ; }
    public void printSetup( PrintWriter pw ){
       pw.println("#\n# HsmStorageHandler2("+getClass().getName()+")\n#");
       pw.println("rh set max active "+_fetchQueue.getMaxActiveJobs() ) ;
       pw.println("st set max active "+_storeQueue.getMaxActiveJobs() ) ;
       pw.println("rh set timeout "+(_maxRestoreRun/1000L) ) ;
       pw.println("st set timeout "+(_maxStoreRun/1000L) ) ;
    }
    public void getInfo( PrintWriter pw ){
       pw.println( "StorageHandler ["+this.getClass().getName()+"]" ) ;
       pw.println( "  Version         : [$Id: HsmStorageHandler2.java,v 1.32.2.2 2007-03-23 16:58:00 tigran Exp $]");
       pw.println( " Sticky allowed   : "+_stickyAllowed ) ;
       pw.println( " Restore Timeout  : "+(_maxRestoreRun/1000L) ) ;
       pw.println( "   Store Timeout  : "+(_maxStoreRun/1000L) ) ;
       pw.println( "  Job Queues " ) ;
       pw.println( "    to store   "+_storeQueue.getActiveJobs()+
                   "("+_storeQueue.getMaxActiveJobs()+
                   ")/"+_storeQueue.getQueueSize() ) ;
       pw.println( "    from store "+_fetchQueue.getActiveJobs()+
                   "("+_fetchQueue.getMaxActiveJobs()+
                   ")/"+_fetchQueue.getQueueSize() ) ;
    }
    public Info getStorageInfoByPnfsId( PnfsId pnfsId ){
      synchronized( _listLock ){
         Info info = (Info)_restorePnfsidList.get( pnfsId ) ;
         if( info == null )return (Info)_storePnfsidList.get( pnfsId );
         return info ;
      }
    }
    private StorageInfo getStorageInfo( CacheRepositoryEntry entry )
            throws CacheException , InterruptedException {
        PnfsId pnfsId  = entry.getPnfsId() ;
        StorageInfo si = entry.getStorageInfo() ;
        if( si == null ){
            si = _pnfs.getStorageInfo( pnfsId.toString() ) ;
            if( si == null )
              throw new
              CacheException("Couldn't get storage info of "+pnfsId ) ;
            entry.setStorageInfo(si);

        }
        return si ;
    }
    private synchronized String
            getSystemCommand( PnfsId pnfsId , StorageInfo storageInfo , String direction )
            throws CacheException  {

        String hsmName      = storageInfo.getHsm() ;
        say( "getFetchCommand for pnfsid="+pnfsId+";hsm="+hsmName+";si="+storageInfo) ;
        HsmSet.HsmInfo info = _hsmSet.getHsmInfoByName( hsmName ) ;
        if( info == null )
           throw new
           IllegalArgumentException("Info not found for : "+hsmName ) ;

        String hsmCommand = info.getAttribute( "command" ) ;
        if( hsmCommand == null )
            throw new
            IllegalArgumentException("hsmCommand not specified in HsmSet" ) ;

        String localPath = _repository.getEntry(pnfsId).getDataFile().getPath() ;

        StringBuffer sb = new StringBuffer() ;
        sb.append(hsmCommand).append(" ").
           append(direction).append(" ").
           append(pnfsId).append("  ").
           append( localPath ).
           append(" -si=").append(storageInfo.toString());
        Enumeration attr = info.attributes() ;
        while( attr.hasMoreElements() ){
           String key = (String)attr.nextElement() ;
           String val = (String)info.getAttribute(key) ;
           sb.append(" -").append(key) ;
           if( ( val != null ) && ( val.length() > 0 ) )
              sb.append("=").append(val) ;
        }
        String completeCommand = sb.toString() ;
        say("HSM_COMMAND : "+completeCommand ) ;
        return completeCommand ;

    }
    //
    //   utils for the printout
    //
    public synchronized void setLogable( Logable log ){ _log = log ; }
    private void say( String msg ){
       if( _log != null )_log.log( "RSH : "+msg ) ;
    }
    private void esay( String msg ){
       if( _log != null )_log.log( "RSH ERROR : "+msg ) ;
    }
    //////////////////////////////////////////////////////////////////////
    //
    //   the fetch part
    //
    public JobScheduler getFetchScheduler(){ return _fetchQueue ; }
    public boolean fetch( PnfsId pnfsId ,
                          StorageInfo storageInfo ,
                          CacheFileAvailable callback )
            throws Exception {

        boolean wasCreated = false ;
        CacheRepositoryEntry entry = null ;
        synchronized( _repository ){
           say( "fetch : request for "+pnfsId ) ;
           try{
               entry = _repository.getEntry( pnfsId ) ;
               say( "fetch : entry found <"+entry+">") ;
           }catch( FileNotInCacheException fnice ){
               entry = _repository.createEntry( pnfsId ) ;
               wasCreated = true  ;
               say( "fetch : entry created <"+entry+">" ) ;
               try{
                  StorageInfo info  = storageInfo ;
                  String      value = info.getKey("flag-s") ;

                  if( ( value != null ) && ( ! value.equals("") ) ){
                     if( _stickyAllowed ){
                        say( "setting sticky bit of "+pnfsId ) ;
                        entry.setSticky(true) ;
                     }else{
                        say( pnfsId.toString()+" : setting sticky denied" ) ;
                     }
                  }
               }catch( Exception ee ){
                  esay("RepositoryLoader : ["+entry.getPnfsId()+
                       "] Can't set sticky/nonsticky due to : "+ee ) ;
               }
           }
           //
           // sync(repository) is not enough to
           // read/mod/write the entry.isXXX routines.
           //
           synchronized( entry ){
              if( entry.isCached() || entry.isPrecious() )return true ;

              if( wasCreated ){
                 try{
                    entry.lock(true) ;
                    entry.setReceivingFromStore() ;
                    if( storageInfo != null )entry.setStorageInfo(storageInfo);
                 }catch(Exception ce){
                    esay("fetchFile (1) : "+ce ) ;
                    try{_repository.removeEntry(entry) ;}catch(Exception ee){}
                    throw ce;
                 }
              }

              if( ! entry.isReceivingFromStore() )
                 throw new
                 InconsistentCacheException(14,
                   "entry not receiving from store but : "+entry.toString() ) ;

              _storageHandler.fetch( entry , callback ) ;

           }
        }
        return false ;

    }

    public synchronized void fetch( CacheRepositoryEntry entry ,
                                    CacheFileAvailable callback ){

       PnfsId       pnfsId = entry.getPnfsId() ;
       FetchThread  info   = (FetchThread)_restorePnfsidList.get(pnfsId) ;

       if( info != null ){
          if( callback != null )info.addCallback(callback);
          return ;
       }
       info = new FetchThread( entry ) ;
       if( callback != null )info.addCallback(callback);
       _restorePnfsidList.put( pnfsId , info ) ;

       try{
           _fetchQueue.add( info ) ;
       }catch(InvocationTargetException ite ){
           _restorePnfsidList.remove( pnfsId ) ;
           esay("Restore "+info+" not started due to : "+ite.getTargetException() ) ;
       }

    }
    public synchronized Iterator getPnfsIds(){

       ArrayList v = new ArrayList() ;
       for( Iterator e = _restorePnfsidList.keySet().iterator() ; e.hasNext() ; )
          v.add(e.next());
       for( Iterator e = _storePnfsidList.keySet().iterator() ; e.hasNext() ; )
          v.add(e.next());
       return v.iterator() ;
    }
    public synchronized Iterator getStorePnfsIds(){

       ArrayList v = new ArrayList() ;
       for( Iterator e = _storePnfsidList.keySet().iterator() ; e.hasNext() ; )
          v.add(e.next());
       return v.iterator() ;
    }
    public synchronized Iterator getRestorePnfsIds(){

       ArrayList v = new ArrayList() ;
       for( Iterator e = _restorePnfsidList.keySet().iterator() ; e.hasNext() ; )
          v.add(e.next());
       return v.iterator() ;
    }
    public synchronized Info getRestoreInfoByPnfsId( PnfsId pnfsId ){
        return (Info)_restorePnfsidList.get( pnfsId ) ;
    }
    private synchronized String
            getFetchCommand( PnfsId pnfsId , StorageInfo storageInfo )
            throws CacheException  {
        return getSystemCommand( pnfsId , storageInfo , "get" ) ;
    }
    private synchronized List getCallbackList( Map map  , PnfsId pnfsId ){

       Info info  = (Info)map.get( pnfsId )  ;

       return info == null ? new ArrayList() : info.getCallbacks() ;
    }
    private void executeCallbacks( List list , PnfsId pnfsId , Throwable exc ){

       say("DEBUGFLUSH : executeCallbacks : excecuting callbacks  "+pnfsId + " (callback="+list+") "+exc) ;
       for( Iterator e = list.iterator() ;  e.hasNext() ; ){

          CacheFileAvailable callback = (CacheFileAvailable)e.next() ;
          try{
             callback.cacheFileAvailable( pnfsId.toString(), exc ) ;
          }catch(Throwable t){
             esay("Throwable in callback to "+callback.getClass().getName()+" : "+t ) ;
          }
       }
    }
    private class FetchThread extends Info implements Batchable {

       private PnfsId               _pnfsId      = null ;
       private StorageInfo          _storageInfo = null ;
       private CacheRepositoryEntry _entry       = null ;
       private StorageInfoMessage   _infoMsg     = null ;
       private long                 _timestamp   = 0 ;
       private int id;
       public FetchThread( CacheRepositoryEntry entry ){
          super( entry.getPnfsId() ) ;
          _entry   = entry ;
          _pnfsId  = entry.getPnfsId() ;
          _infoMsg = new StorageInfoMessage(
                          _cell.getCellName()+"@"+_cell.getCellDomainName() ,
                          _pnfsId ,
                          true ) ;
       }
       public String toString(){ return _pnfsId.toString() ; }
       public String getClient(){ return "[Unknown]" ; }
       public long getClientId(){ return 0 ; }
       public void queued(){
          _timestamp = System.currentTimeMillis() ;
       }
       public double getTransferRate(){ return (double)10.0 ; }
       public void unqueued(){

          say("Unqueuing "+_pnfsId ) ;

          CacheException cex = new CacheException( 33 , "Job unqueued (by operator)") ;
          executeCallbacks( getCallbackList( _restorePnfsidList , _pnfsId )  , _pnfsId , cex ) ;

          synchronized( _repository ){

             try{

                _entry.lock(false) ;
                _repository.removeEntry( _entry ) ;

             }catch(Exception eee){
                esay(_pnfsId.toString()+
                     " : Failed to destroy repository entry after 'unqueue' : "+
                     eee) ;
             }

          }

          synchronized( HsmStorageHandler2.this ){
             _restorePnfsidList.remove( _pnfsId ) ;
          }

          _infoMsg.setTimeQueued( System.currentTimeMillis() - _timestamp ) ;
          _infoMsg.setResult( cex.getRc() , cex.getMessage() ) ;
          try{
             _cell.sendMessage(new CellMessage( new CellPath("billing"), _infoMsg ));
          }catch(Exception ie){
             esay("Could send 'billing info' : "+ie);
          }

       }
       public void run() {
           int    returnCode   = 1 ;
           RunSystem run       = null ;
           String    errmsg    = "ok" ;
           Exception excep     = null ;
           long      fileSize  = 0 ;
           say( "FetchThread started" ) ;
           try{
               _storageInfo = getStorageInfo( _entry ) ;
               _infoMsg.setStorageInfo( _storageInfo ) ;

               if( ( fileSize = _storageInfo.getFileSize() ) == 0 )
                 throw new
                 CacheException("Couldn't get file size of "+_pnfsId ) ;

               _infoMsg.setFileSize(fileSize);
               long now = System.currentTimeMillis() ;
               _infoMsg.setTimeQueued( now - _timestamp ) ;
               _timestamp = now ;

               String fetchCommand = getFetchCommand( _pnfsId , _storageInfo );

               say("Waiting for space ("+fileSize+" Bytes)" ) ;
               _repository.allocateSpace(fileSize) ;
               say("Got Space ("+fileSize+" Bytes)" ) ;


               run = new RunSystem( fetchCommand , _maxLines , _maxRestoreRun , _log ) ;
               run.go() ;
               returnCode = run.getExitValue() ;
               if( returnCode != 0 ){
                  errmsg = run.getErrorString() ;
                  excep  = new CacheException( returnCode , errmsg ) ;
               }
               say( "RunSystem. -> "+returnCode+" : "+run.getErrorString() ) ;

           }catch( CacheException cie ){
               esay( errmsg = "FetchThread : ("+_pnfsId+") CacheException : "+cie ) ;
               returnCode = 1 ;
               excep = cie ;
           }catch( InterruptedException ie ){
               esay( errmsg = "FetchThread : ("+_pnfsId+") Process interrupted (timed out)" ) ;
               returnCode = 1 ;
               excep = ie ;
           }catch( IOException  ioe ){
               esay( errmsg = "FetchThread : ("+_pnfsId+") Process got an IOException : "+ioe ) ;
               returnCode = 2 ;
               excep = ioe ;
           }catch( IllegalThreadStateException  itse ){
               esay( errmsg = "FetchThread : ("+_pnfsId+") Can't stop process : "+itse ) ;
               returnCode = 3 ;
               excep = itse ;
           }catch( IllegalArgumentException iae ){
               esay( errmsg = "FetchThread : can't determine 'hsmInfo' for "+
                     _storageInfo+" {"+iae+"}" ) ;
               returnCode = 4 ;
               excep = iae ;
           }
           //
           // we can't use the entry as lock because within this
           // lock we try to get the repository lock (implicitly
           // by _repository.destroyEntry. But this would be
           // the wrong sequence. we usually first get the
           // repository and then the entry. ===>>>> DEADLOCK
           //
           synchronized( _repository ){
              try{
                 if( returnCode == 0 ){
                    long realFilesize = _entry.getSize() ;
                    if( realFilesize != fileSize ) {

                    	/*
                    	 * due to this fact, we have to remove bad entry.
                    	 * to keep space calculation in sync with physical used file
                    	 * ajdust file size to expected one. Do this only if real file GT
                    	 * expected.
                    	 */


                    	if( realFilesize > fileSize ) {
	                    	RandomAccessFile raf = null;
	                    	try {

	                    		raf = new RandomAccessFile(_entry.getDataFile(), "rw");
	                    		raf.setLength(fileSize);

	                    	}catch(IOException ioe) {

	                    	}finally{
	                    		if( raf != null ) {
	                    			try {
	                    				raf.close();
	                    			}catch(IOException ignored) {}
	                    		}
	                    	}
                    	}

                    	// trigger cleanup
                       throw new
                       CacheException( 1 ,
                       "Filesize mismatch (expected="+fileSize+
                       ";found="+realFilesize+")" ) ;
                    }
                    say("Filesize check : ok") ;
                    _entry.setCached() ;
                 }
              }catch(CacheException iii ){
                 esay(iii.toString()) ;
                 excep = iii ;
              }
              if( excep != null ){
                 if( excep instanceof CacheException ){
                    _infoMsg.setResult( ((CacheException)excep).getRc() ,
                                        ((CacheException)excep).getMessage() ) ;
                 }else{
                    _infoMsg.setResult( 44 , excep.toString() ) ;
                 }
                 esay( "Removing entry ("+_pnfsId+") from repository" ) ;
                 try{
                    _entry.lock(false) ;
                    long size = _entry.getSize() ;
                    //
                    // removeentry will 'free' the actual number of
                    // bytes = size of the file on disk.
                    //
                   _repository.removeEntry( _entry ) ;
                   _repository.freeSpace( Math.max(0,fileSize-size) ) ;
                 }catch(Exception eee){
                   esay("PANIC : can't destroyEntry failed after 'failed Restore'") ;
                 }
              }

           }

           executeCallbacks( getCallbackList( _restorePnfsidList , _pnfsId )  , _pnfsId , excep ) ;

           synchronized( HsmStorageHandler2.this ){
              _restorePnfsidList.remove( _pnfsId ) ;
           }

           say( _pnfsId.toString()+" : FetchThread Done" ) ;
           _infoMsg.setTransferTime( System.currentTimeMillis() - _timestamp ) ;
           try{
              _cell.sendMessage(new CellMessage( new CellPath("billing"), _infoMsg ));
           }catch(Exception ie){
              esay("Could send 'billing info' : "+ie);
           }
       }
       public void ided(int id) {
           this.id = id;
       }
    }
    //////////////////////////////////////////////////////////////////////
    //
    //   the store part
    //
    private synchronized String
            getStoreCommand( PnfsId pnfsId , StorageInfo storageInfo )
            throws CacheException  {
        return getSystemCommand( pnfsId , storageInfo , "put" ) ;
    }
    public synchronized Info getStoreInfoByPnfsId( PnfsId pnfsId ){
        return (Info)_storePnfsidList.get( pnfsId ) ;
    }

    public JobScheduler getStoreScheduler(){ return _storeQueue ; }

    public boolean store( CacheRepositoryEntry entry , CacheFileAvailable callback )

           throws CacheException {

       say("DEBUGFLUSH : store : requested for "+entry+( callback == null ? " w/o " : " with " ) + "callback" ) ;
       if( entry.isCached() ){
          say("DEBUGFLUSH : store : isAlreadyCached "+entry ) ;
          return true ;
       }
       PnfsId       pnfsId = entry.getPnfsId() ;
       StoreThread  info   = null ;

       synchronized( this ){

          if( ( info = (StoreThread)_storePnfsidList.get(pnfsId) ) != null ){

             if( callback != null )info.addCallback(callback);
             say("DEBUGFLUSH : store : flush already in progress "+entry + " (callback="+callback+")" ) ;
             return false ;

          }

          _storePnfsidList.put( pnfsId , info = new StoreThread( entry )  ) ;

          try{
              if( _checkPnfs && _removeUnexistingEntries ){
                 //
                 // make sure the file still exists in pnfs.
                 //
                 say( pnfsId.toString()+" Getting storageinfo" ) ;
                 try{

                    //
                    // just to check if file still exists
                    //
                    StorageInfo storageInfo = _pnfs.getStorageInfo(pnfsId.toString());

                 }catch(CacheException exc ){

                    esay(pnfsId.toString()+" : Checking if exists in pnfs : "+exc ) ;
                    int rc = exc.getRc() ;
                    //
                    // in general we remove the entry if we get an
                    // exception. Except for the case were there is
                    // no answer (or the wrong one)  from the PnfsManager.
                    //
                    if( ( rc != CacheException.TIMEOUT ) && ( rc != CacheException.PANIC ) ){


                       esay( pnfsId.toString()+" REMOVEING ENTRY from repository (nonexistent)" ) ;

                       _repository.removeEntry( entry ) ;

                    }

                    throw exc /* new Exception("File already removed") */ ;

                 }
              }

               entry.setSendingToStore(true) ;
               entry.lock(true);

              if( callback != null )info.addCallback(callback);

              _storeQueue.add( info ) ;

              say("DEBUGFLUSH : store : added to flush queue "+entry + " (callback="+callback+")" ) ;
              return false ;

          }catch( Throwable excep ){

              Throwable t = excep instanceof InvocationTargetException ?
                            ((InvocationTargetException)excep).getTargetException() :
                            excep ;

              esay( pnfsId.toString()+" REMOVEING ENTRY from _storePnfsidList due to "+excep);

              _storePnfsidList.remove( pnfsId ) ;

              throw new CacheException( 44 , "Problem detected : "+excep ) ;
          }

       }
    }
    private class StoreThread extends Info implements Batchable {

	private PnfsId                _pnfsId      = null ;
	private StorageInfo           _storageInfo = null ;
        private CacheRepositoryEntry  _entry       = null ;
        private StorageInfoMessage    _infoMsg     = null ;
        private long                  _timestamp   = 0 ;
        private int id;

	public StoreThread( CacheRepositoryEntry entry ){
	    super( entry.getPnfsId() ) ;
            _entry       = entry ;
	    _pnfsId      = entry.getPnfsId() ;
            _infoMsg     = new StorageInfoMessage(
                          _cell.getCellName()+"@"+_cell.getCellDomainName() ,
                          _pnfsId ,
                          false ) ;
	}
       public String toString(){ return _pnfsId.toString() ; }
       public double getTransferRate(){ return (double)10.0 ; }
       public String getClient(){ return "[Unknown]" ; }
       public long   getClientId(){ return 0 ; }

       public void queued(){
          _timestamp = System.currentTimeMillis() ;
       }
       public void unqueued(){
          _infoMsg.setTimeQueued( System.currentTimeMillis() - _timestamp ) ;
          _infoMsg.setResult( 44 , "Unqueued ... " ) ;
          try{
             _cell.sendMessage(new CellMessage( new CellPath("billing"), _infoMsg ));
          }catch(Exception ie){
             esay("Could send 'billing info' : "+ie);
          }
          _storePnfsidList.remove( _pnfsId ) ;
       }
	public void run() {
           int       returnCode   = 1 ;
           RunSystem run       = null ;
           String    errmsg    = "ok" ;
           Throwable excep     = null ;
           long      fileSize  = 0 ;
           say( _pnfsId.toString()+" : StoreThread Started "+Thread.currentThread() ) ;

           try{
              _storageInfo = getStorageInfo( _entry ) ;
              _infoMsg.setStorageInfo( _storageInfo ) ;
              _infoMsg.setFileSize( _storageInfo.getFileSize() ) ;
              long now = System.currentTimeMillis() ;
              _infoMsg.setTimeQueued( now - _timestamp ) ;
              _timestamp = now ;

              String storeCommand = getStoreCommand(_pnfsId,_storageInfo);

              run = new RunSystem( storeCommand , _maxLines , _maxStoreRun , _log ) ;
              run.go() ;
              returnCode = run.getExitValue() ;
              if( returnCode != 0 ){
                  errmsg = run.getErrorString() ;
                  excep  = new CacheException( returnCode , errmsg ) ;
              }
              say( "RunSystem. -> "+returnCode+" : "+run.getErrorString() ) ;

           }catch( CacheException cie ){
               esay( errmsg = "StoreThread : ("+_pnfsId+") CacheException : "+cie ) ;
               returnCode = 1 ;
               excep = cie ;
           }catch( InterruptedException ie ){
               esay( errmsg = "StoreThread : ("+_pnfsId+") Process interrupted (timed out)" ) ;
               returnCode = 1 ;
               excep = ie ;
           }catch( IOException  ioe ){
               esay( errmsg = "StoreThread : ("+_pnfsId+") Process got an IOException : "+ioe ) ;
               returnCode = 2 ;
               excep = ioe ;
           }catch( IllegalThreadStateException  itse ){
               esay( errmsg = "StoreThread : ("+_pnfsId+") Can't stop process : "+itse ) ;
               returnCode = 3 ;
               excep = itse ;
           }catch( IllegalArgumentException iae ){
               esay( errmsg = "StoreThread : can't determine 'hsmInfo' for "+
                     _storageInfo+" {"+iae+"}" ) ;
               returnCode = 4 ;
               excep = iae ;
           }catch( Throwable t ){
               esay( errmsg = "StoreThread : unexpected throwable "+
                     _storageInfo+" {"+t+"}" ) ;
               returnCode = 666 ;
               excep = t ;
           }finally{
               try{
                  _entry.lock(false);
                  _entry.setSendingToStore(false) ;
               }catch(CacheException ieee){
                  esay( "Panic : can't set entry status to 'done' "+ieee);
               }
           }

           try{
              if( returnCode == 0 )_entry.setCached() ;
           }catch(CacheException iii ){
              esay(iii.toString()) ;
              excep = iii ;
           }
           if( excep != null ){
              if( excep instanceof CacheException ){
                 _infoMsg.setResult( ((CacheException)excep).getRc() ,
                                     ((CacheException)excep).getMessage() ) ;
              }else{
                 _infoMsg.setResult( 44 , excep.toString() ) ;
              }
           }


           executeCallbacks( getCallbackList( _storePnfsidList ,  _pnfsId )  , _pnfsId , excep ) ;
           /*
                             excep == null ? null :
                             new InvocationTargetException( excep , "TargetException : "+excep.toString() ) ) ;
           */
           synchronized( HsmStorageHandler2.this ){
              _storePnfsidList.remove( _pnfsId ) ;
           }
           say( _pnfsId.toString()+" : StoreThread Done "+Thread.currentThread() ) ;
           _infoMsg.setTransferTime( System.currentTimeMillis() - _timestamp ) ;
           try{
              _cell.sendMessage(new CellMessage( new CellPath("billing"), _infoMsg ));
           }catch(Exception ie){
              esay("Could send 'billing info' : "+ie);
           }
       }
        public void ided(int id) {
            this.id = id;
        }
    }
}
