// $Id: HsmStorageHandler2.java,v 1.47 2007-10-26 11:17:06 behrmann Exp $

package diskCacheV111.pools ;

import java.io.IOException;
import java.io.NotSerializableException;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.io.BufferedReader;
import java.io.StringReader;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import dmg.cells.nucleus.CellAdapter;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.NoRouteToCellException;
import dmg.util.Logable;

import diskCacheV111.movers.DCapProtocol_3_nio;
import diskCacheV111.repository.CacheRepository;
import diskCacheV111.repository.CacheRepositoryEntry;
import diskCacheV111.util.Batchable;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.CacheFileAvailable;
import diskCacheV111.util.FileNotInCacheException;
import diskCacheV111.util.HsmSet;
import diskCacheV111.util.InconsistentCacheException;
import diskCacheV111.util.JobScheduler;
import diskCacheV111.util.PnfsHandler;
import diskCacheV111.util.PnfsId;
import diskCacheV111.util.RunSystem;
import diskCacheV111.util.SimpleJobScheduler;
import diskCacheV111.vehicles.PoolFileFlushedMessage;
import diskCacheV111.vehicles.PoolRemoveFilesFromHSMMessage;
import diskCacheV111.vehicles.StorageInfo;
import diskCacheV111.vehicles.StorageInfoMessage;

public class HsmStorageHandler2  {


	private static Logger _logRepository = Logger.getLogger("logger.org.dcache.repository");
	private final static Logger _logSpaceAllocation = Logger.getLogger("logger.dev.org.dcache.poolspacemonitor." + HsmStorageHandler2.class.getName());

    private final CacheRepository _repository  ;
    private Logable         _log         = null ;
    private final HsmSet          _hsmSet     ;
    private final PnfsHandler     _pnfs ;
    private final CellAdapter     _cell ;
    private final Map<PnfsId, StoreThread>       _storePnfsidList   = new Hashtable<PnfsId, StoreThread>() ;
    private final Map<PnfsId, FetchThread>       _restorePnfsidList = new Hashtable<PnfsId, FetchThread>() ;
    private final Object          _listLock          = new Object()  ;
    private long            _maxRuntime   = 4 * 3600 * 1000 ;
    private long            _maxStoreRun  = _maxRuntime ;
    private long            _maxRestoreRun= _maxRuntime ;
    private long            _maxRemoveRun = _maxRuntime ;
    private int             _maxLines     = 200 ;
    private final HsmStorageHandler2 _storageHandler ;
    private final JobScheduler       _fetchQueue     ;
    private final JobScheduler       _storeQueue     ;
    private boolean            _stickyAllowed  = false ;
    private final boolean            _checkPnfs      = true ;
    private final boolean     _removeUnexistingEntries;
    private final Executor           _hsmRemoveExecutor =
        Executors.newSingleThreadExecutor();
    private final ThreadPoolExecutor _hsmRemoveTaskExecutor =
        new ThreadPoolExecutor(1, 1, Long.MAX_VALUE, TimeUnit.NANOSECONDS,
                               new LinkedBlockingQueue());

    ////////////////////////////////////////////////////////////////////////////////
    //
    //    the generic part
    //
    public class Info {
        private long      _startTime = System.currentTimeMillis() ;
        private final List<CacheFileAvailable> _callbacks = new ArrayList<CacheFileAvailable>() ;
        private final PnfsId    _pnfsId ;
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
        synchronized List<CacheFileAvailable> getCallbacks(){ return new ArrayList<CacheFileAvailable>(_callbacks) ; }
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

    void setTimeout(long storeTimeout, long restoreTimeout, long removeTimeout)
    {
       if( storeTimeout > 0   )_maxStoreRun   = storeTimeout ;
       if( restoreTimeout > 0 )_maxRestoreRun = restoreTimeout ;
       if( removeTimeout > 0 )_maxRemoveRun = removeTimeout ;
    }

    public void setStickyAllowed( boolean sticky ){
       _stickyAllowed = sticky ;
    }
    public void setMaxActiveRestores( int restores ){ _fetchQueue.setMaxActiveJobs( restores) ; }
    public void printSetup( PrintWriter pw ){
       pw.println("#\n# HsmStorageHandler2("+getClass().getName()+")\n#");
       pw.println("rh set max active "+_fetchQueue.getMaxActiveJobs() ) ;
       pw.println("st set max active "+_storeQueue.getMaxActiveJobs() ) ;
       pw.println("rm set max active " + getMaxRemoveJobs());
       pw.println("rh set timeout "+(_maxRestoreRun/1000L) ) ;
       pw.println("st set timeout "+(_maxStoreRun/1000L) ) ;
       pw.println("rm set timeout "+(_maxRemoveRun/1000L) ) ;
    }
    public void getInfo( PrintWriter pw ){
       pw.println( "StorageHandler ["+this.getClass().getName()+"]" ) ;
       pw.println( "  Version         : [$Id: HsmStorageHandler2.java,v 1.47 2007-10-26 11:17:06 behrmann Exp $]");
       pw.println( " Sticky allowed   : "+_stickyAllowed ) ;
       pw.println( " Restore Timeout  : "+(_maxRestoreRun/1000L) ) ;
       pw.println( "   Store Timeout  : "+(_maxStoreRun/1000L) ) ;
       pw.println( "  Remove Timeout  : "+(_maxRemoveRun/1000L) ) ;
       pw.println( "  Job Queues " ) ;
       pw.println( "    to store   "+_storeQueue.getActiveJobs()+
                   "("+_storeQueue.getMaxActiveJobs()+
                   ")/"+_storeQueue.getQueueSize() ) ;
       pw.println( "    from store "+_fetchQueue.getActiveJobs()+
                   "("+_fetchQueue.getMaxActiveJobs()+
                   ")/"+_fetchQueue.getQueueSize() ) ;
       pw.println( "    delete     "+ "" +
                   "(" + getMaxRemoveJobs() +
                   ")/"+"");
    }
    public Info getStorageInfoByPnfsId( PnfsId pnfsId ){
      synchronized( _listLock ){
         Info info = _restorePnfsidList.get( pnfsId ) ;
         if( info == null )return _storePnfsidList.get( pnfsId );
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
        getSystemCommand(PnfsId pnfsId, StorageInfo storageInfo,
                         HsmSet.HsmInfo hsm, String direction)
        throws CacheException
    {
        String hsmCommand = hsm.getAttribute( "command" ) ;
        if( hsmCommand == null )
            throw new
            IllegalArgumentException("hsmCommand not specified in HsmSet" ) ;

        String localPath = _repository.getEntry(pnfsId).getDataFile().getPath() ;

        StringBuilder sb = new StringBuilder() ;

        sb.append(hsmCommand).append(" ").
        append(direction).append(" ").
        append(pnfsId).append("  ").
        append( localPath );


        sb.append(" -si=").append(storageInfo.toString());
        for (Map.Entry<String,String> attr : hsm.attributes()) {
            String key = attr.getKey();
            String val = attr.getValue();
            sb.append(" -").append(key) ;
            if( ( val != null ) && ( val.length() > 0 ) )
                sb.append("=").append(val) ;
        }

        if( !storageInfo.locations().isEmpty() ) {
        	/*
        	 * new style
        	 */

        	for( URI location: storageInfo.locations() ) {
        		if( location.getScheme().equals(hsm.getType()) && location.getAuthority().equals(hsm.getInstance() ) ) {
        			sb.append(" -uri=").append(location.toString());
        			break;
        		}
        	}
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
    	_logRepository.debug( "RSH : "+msg ) ;
    }
    private void esay( String msg ){
    	_logRepository.error( "RSH ERROR : "+msg ) ;
    }
    //////////////////////////////////////////////////////////////////////
    //
    //   the fetch part
    //
    JobScheduler getFetchScheduler(){ return _fetchQueue ; }
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
       FetchThread  info   = _restorePnfsidList.get(pnfsId) ;

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
    public synchronized Iterator<PnfsId> getPnfsIds(){

    	// to avoid extra buffer copy tell array size in advance
       List<PnfsId> v = new ArrayList<PnfsId>(_restorePnfsidList.size() + _storePnfsidList.size() ) ;

       v.addAll(_restorePnfsidList.keySet());
       v.addAll(_storePnfsidList.keySet());

       return v.iterator() ;
    }
    public synchronized Iterator<PnfsId> getStorePnfsIds(){

       List<PnfsId> v = new ArrayList<PnfsId>() ;
       v.addAll(_storePnfsidList.keySet());
       return v.iterator() ;
    }
    public synchronized Iterator<PnfsId> getRestorePnfsIds(){

       List<PnfsId> v = new ArrayList<PnfsId>() ;
       v.addAll(_restorePnfsidList.keySet());
       return v.iterator() ;
    }
    public synchronized Info getRestoreInfoByPnfsId( PnfsId pnfsId ){
        return _restorePnfsidList.get( pnfsId ) ;
    }

    /**
     * Returns the name of an HSM accessible for this pool and which
     * contains the given file. Returns null if no such HSM exists.
     */
    private String findAccessibleLocation(StorageInfo file)
    {
        if (file.locations().isEmpty()
            && _hsmSet.getHsmInstances().contains(file.getHsm())) {
            // This is for backwards compatibility until all info
            // extractors support URIs.
            return file.getHsm();
        } else {
            for (URI location : file.locations()) {
                if (_hsmSet.getHsmInstances().contains(location.getAuthority())) {
                    return location.getAuthority();
                }
            }
        }
        return null;
    }

    private synchronized String
        getFetchCommand(PnfsId pnfsId, StorageInfo storageInfo)
            throws CacheException
    {
        String instance = findAccessibleLocation(storageInfo);
        if (instance == null) {
            throw new
                IllegalArgumentException("HSM not defined on this pool: " +
                                         storageInfo.locations());
        }
        HsmSet.HsmInfo hsm = _hsmSet.getHsmInfoByName(instance);

        say("getFetchCommand for pnfsid=" + pnfsId +
            ";hsm=" + instance + ";si=" + storageInfo);

        return getSystemCommand(pnfsId, storageInfo, hsm, "get");
    }

    private synchronized List<CacheFileAvailable> getCallbackList( Map map  , PnfsId pnfsId ){

       Info info  = (Info)map.get( pnfsId )  ;

       return info == null ? new ArrayList<CacheFileAvailable>() : info.getCallbacks() ;
    }
    private void executeCallbacks( List<CacheFileAvailable> list , PnfsId pnfsId , Throwable exc ){

       say("DEBUGFLUSH : executeCallbacks : excecuting callbacks  "+pnfsId + " (callback="+list+") "+exc) ;
       for(CacheFileAvailable callback: list ){

          try{
             callback.cacheFileAvailable( pnfsId.toString(), exc ) ;
          }catch(Throwable t){
             esay("Throwable in callback to "+callback.getClass().getName()+" : "+t ) ;
          }
       }
    }
    private class FetchThread extends Info implements Batchable {

       private final PnfsId               _pnfsId  ;
       private StorageInfo          _storageInfo = null ;
       private final CacheRepositoryEntry _entry ;
       private final StorageInfoMessage   _infoMsg      ;
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
       public double getTransferRate(){ return 10.0 ; }
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
               _logSpaceAllocation.debug("ALLOC: " + _pnfsId + " : " + fileSize );
               _repository.allocateSpace(fileSize) ;
               say("Got Space ("+fileSize+" Bytes)" ) ;


               run = new RunSystem( fetchCommand , _maxLines , _maxRestoreRun , _log ) ;
               run.go() ;
               returnCode = run.getExitValue() ;
               if( returnCode != 0 ){
                  errmsg = run.getErrorString() ;
                  /*
                   * while shell do not return error code bigger than 255,
                   * do a trick here
                   */
                  if(returnCode == 71  ) {
                      returnCode = CacheException.HSM_DELAY_ERROR;
                  }
                  excep  = new CacheException( returnCode , errmsg ) ;
                  esay("RunSystem. -> "+returnCode+" : "+run.getErrorString());
               } else {
                   say("RunSystem. -> "+returnCode+" : "+run.getErrorString());
               }

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
              } finally {

                  /*
                   * this part have to run in any case.
                   * callback execution is important to keep jobs counter in sync with
                   * real number of running jobs
                   */
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
                       _logSpaceAllocation.debug("FREE: " + _pnfsId + " : " + (fileSize-size) );
                       _repository.freeSpace( Math.max(0L,fileSize-size) ) ;
                     }catch(Exception eee){
                       esay("PANIC : can't destroyEntry failed after 'failed Restore'") ;
                     }
                  }

               }

               executeCallbacks( getCallbackList( _restorePnfsidList , _pnfsId )  , _pnfsId , excep ) ;

           }

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
    //   the remove part
    //

    public synchronized void setMaxRemoveJobs(int max)
    {
        _hsmRemoveTaskExecutor.setCorePoolSize(max);
        _hsmRemoveTaskExecutor.setMaximumPoolSize(max);
    }

    public synchronized int getMaxRemoveJobs()
    {
        return _hsmRemoveTaskExecutor.getMaximumPoolSize();
    }

    public synchronized void remove(CellMessage message)
    {
        assert message.getMessageObject() instanceof PoolRemoveFilesFromHSMMessage;

        HsmRemoveTask task =
            new HsmRemoveTask(_cell, _log,
                              _hsmRemoveTaskExecutor,
                              _hsmSet, _maxRemoveRun, message);
        _hsmRemoveExecutor.execute(task);
    }

    //////////////////////////////////////////////////////////////////////
    //
    //   the store part
    //
    private synchronized String
            getStoreCommand(PnfsId pnfsId, StorageInfo storageInfo)
            throws CacheException
    {
        String hsmType = storageInfo.getHsm() ;
        say("getStoreCommand for pnfsid=" + pnfsId +
            ";hsm=" + hsmType + ";si=" + storageInfo);
        List<HsmSet.HsmInfo> hsms = _hsmSet.getHsmInfoByType(hsmType);
        if (hsms.isEmpty()) {
            throw new
                IllegalArgumentException("Info not found for : " + hsmType);
        }

        // If multible HSMs are defined for the given type, then we
        // currently pick the first. We may consider randomising this
        // choice.
        HsmSet.HsmInfo hsm = hsms.get(0);

        //TODO:        storageInfo.addHsmStorageLocation(hsm.getInstance());

        return getSystemCommand(pnfsId, storageInfo, hsm, "put");
    }
    public synchronized Info getStoreInfoByPnfsId( PnfsId pnfsId ){
        return _storePnfsidList.get( pnfsId ) ;
    }

    JobScheduler getStoreScheduler(){ return _storeQueue ; }

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

          if( ( info = _storePnfsidList.get(pnfsId) ) != null ){

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

    private class StoreThread extends Info implements Batchable
    {
    	private final PnfsId _pnfsId;
        private final CacheRepositoryEntry _entry;
        private final StorageInfoMessage _infoMsg;
        private long _timestamp = 0;
        private int id;

	public StoreThread(CacheRepositoryEntry entry)
        {
	    super(entry.getPnfsId());
            String myName =
                _cell.getCellName() + "@" + _cell.getCellDomainName();
            _entry = entry;
            _pnfsId = entry.getPnfsId();
            _infoMsg = new StorageInfoMessage(myName, _pnfsId, false);
	}

       public String toString()
        {
            return _pnfsId.toString();
        }

       public double getTransferRate()
        {
            return 10.0;
        }

       public String getClient()
        {
            return "[Unknown]";
        }

        public long getClientId()
        {
            return 0;
        }

        public void queued()
        {
            _timestamp = System.currentTimeMillis();
        }

        public void unqueued()
        {
            _infoMsg.setTimeQueued(System.currentTimeMillis() - _timestamp);
            _infoMsg.setResult(44, "Unqueued ... ");
            try {
                _cell.sendMessage(new CellMessage(new CellPath("billing"), _infoMsg));
            } catch (Exception e) {
                esay("Could send 'billing info' : " + e);
            }
          _storePnfsidList.remove(_pnfsId);
       }

	public void run()
        {
            int returnCode = 1;
            RunSystem run = null;
            String errmsg = "ok";
            Throwable excep = null;
            StorageInfo storageInfo = null;

            say(_pnfsId.toString() + " : StoreThread Started "
                + Thread.currentThread());

            try {
                storageInfo = getStorageInfo(_entry);
                _infoMsg.setStorageInfo(storageInfo);
                _infoMsg.setFileSize(storageInfo.getFileSize());
                long now = System.currentTimeMillis();
                _infoMsg.setTimeQueued(now - _timestamp);
                _timestamp = now;

                String storeCommand = getStoreCommand(_pnfsId, storageInfo);

                run = new RunSystem(storeCommand, _maxLines, _maxStoreRun, _log);
                run.go();
                returnCode = run.getExitValue();
                if (returnCode != 0) {
                    errmsg = run.getErrorString();
                    excep  = new CacheException(returnCode, errmsg);
                    esay("RunSystem. -> " + returnCode + " : "
                         + run.getErrorString());
                } else {
                    say("RunSystem. -> " + returnCode + " : "
                        + run.getErrorString());
                }

            } catch (CacheException e) {
                esay(errmsg = "StoreThread : (" + _pnfsId
                     + ") CacheException : " + e);
                returnCode = 1;
                excep = e;
            } catch (InterruptedException e) {
                esay(errmsg = "StoreThread : (" + _pnfsId
                     + ") Process interrupted (timed out)");
                returnCode = 1;
                excep = e;
            } catch (IOException e) {
                esay(errmsg = "StoreThread : (" + _pnfsId
                     + ") Process got an IOException : " + e);
                returnCode = 2;
                excep = e;
            } catch (IllegalThreadStateException e) {
                esay(errmsg = "StoreThread : (" + _pnfsId
                     + ") Can't stop process : " + e);
                returnCode = 3;
                excep = e;
            } catch (IllegalArgumentException e) {
                esay(errmsg = "StoreThread : can't determine 'hsmInfo' for "
                     + storageInfo+" {" + e + "}");
                returnCode = 4;
                excep = e;
            } catch (Throwable t) {
                esay(errmsg = "StoreThread : unexpected throwable " +
                     storageInfo + " {" + t + "}");
                returnCode = 666;
                excep = t;
            } finally {
                try {
                    _entry.lock(false);
                    _entry.setSendingToStore(false);
                } catch (CacheException e) {
                    esay( "Panic : can't set entry status to 'done' " + e);
                }
            }

            try {
                if (returnCode == 0) {
                    String outputData = run.getOutputString();
                    if (outputData != null && outputData.length() != 0) {
                        BufferedReader in =
                            new BufferedReader(new StringReader(outputData));
                        String line = null;
                        try {
                            while ((line = in.readLine()) != null) {
                                URI location = new URI(line);
                                storageInfo.addLocation(location);
                                storageInfo.isSetAddLocation(true);
                                _logRepository.debug(_pnfsId.toString()
                                                     + ": added HSM location "
                                                     + location);
                            }
                        } catch (URISyntaxException use) {
                            esay(_entry.getPnfsId().toString() +
                                 " :  flush script produces BAD URI : " + line);
                            throw new CacheException(2, use.getMessage());
                        } catch (IOException ie) {
                            // never happens on strings
                            throw new RuntimeException("Bug detected");
                        }
                    }

                    for (;;) {
                        try {
                            /* It is very important that we use
                             * storageInfo rather than
                             * _entry.getStorageInfo(), as we added
                             * new URIs to the former.
                             */
                            _pnfs.fileFlushed(_pnfsId, storageInfo);
                            break;
                        } catch(CacheException e) {
                            if (e.getRc() == CacheException.FILE_NOT_FOUND) {
                                /* In case the file was deleted, we are
                                 * presented with the problem that the
                                 * file is now on tape, however the
                                 * location has not been registered
                                 * centrally. Hence the copy on the tape
                                 * will not be removed by the HSM
                                 * cleaner. The sensible thing seems to be
                                 * to remove the file from tape here. For
                                 * now we ignore this issue (REVISIT).
                                 */
                                break;
                            }

                            /* The message to the PnfsManager
                             * failed. There are several possible
                             * reasons for this; we may have lost the
                             * connection to the PnfsManager; the
                             * PnfsManager may have lost its
                             * connection to PNFS or otherwise be in
                             * trouble; bugs; etc.
                             *
                             * We keep retrying until we succeed. This
                             * will effectively block this thread from
                             * flushing any other files, which seems
                             * sensible when we have trouble talking
                             * to the PnfsManager. If the pool crashes
                             * or gets restarted while waiting here,
                             * we will end up flushing the file
                             * again. We assume that the HSM script is
                             * able to eliminate the duplicate; or at
                             * least tolerate the duplicate (given
                             * that this situation should be rare, we
                             * can live with a little bit of wasted
                             * tape).
                             */
                            esay("Error notifying PNFS about a flushed file: "
                                 + e.getMessage() + "(" + e.getRc() + ")");
                        }
                        Thread.sleep(120000); // 2 minutes
                    }

                    _entry.setCached();

                    notifyFlushMessageTarget(storageInfo);
                }
            } catch (InterruptedException e) {
                esay(e.toString());
                excep = e;
            } catch (CacheException e) {
                esay(e.toString());
                excep = e;
            } finally {
                /*
                 * this part have to run in any case.
                 * callback execution is important to keep jobs counter in sync with
                 * real number of running jobs
                 */
                if (excep != null) {
                    if (excep instanceof CacheException) {
                        _infoMsg.setResult(((CacheException)excep).getRc(),
                                           ((CacheException)excep).getMessage());
                    } else {
                        _infoMsg.setResult(44, excep.toString());
                    }
                }

                executeCallbacks(getCallbackList(_storePnfsidList, _pnfsId), _pnfsId, excep);
            }

            synchronized (HsmStorageHandler2.this) {
                _storePnfsidList.remove(_pnfsId);
            }
            say(_pnfsId.toString() + " : StoreThread Done " +
                Thread.currentThread());
            _infoMsg.setTransferTime(System.currentTimeMillis() - _timestamp);
            try {
                _cell.sendMessage(new CellMessage(new CellPath("billing"), _infoMsg ));
            } catch (Exception e) {
                esay("Could send 'billing info' : " + e);
            }
        }

        private void notifyFlushMessageTarget(StorageInfo info)
        {
            String flushMessageTarget = _cell.getArgs().getOpt("flushMessageTarget");
            if (flushMessageTarget == null || flushMessageTarget.length() == 0) {
                flushMessageTarget = "broadcast";
            }

            try {
                PoolFileFlushedMessage poolFileFlushedMessage =
                    new PoolFileFlushedMessage(_cell.getCellName(),
                                               _pnfsId, info);
                /*
                 * no replays from secondary message targets
                 */
                poolFileFlushedMessage.setReplyRequired(false);
                CellMessage msg =
                    new CellMessage(new CellPath(flushMessageTarget),
                                    poolFileFlushedMessage);
                _cell.sendMessage(msg);
            } catch (NotSerializableException e) {
                // never happens
            } catch (NoRouteToCellException e) {
                _logRepository.info("failed to send message to flushMessageTarget (" + flushMessageTarget + ") : " + e.getMessage());
            }
        }

        public void ided(int id) {
            this.id = id;
        }
    }
}
