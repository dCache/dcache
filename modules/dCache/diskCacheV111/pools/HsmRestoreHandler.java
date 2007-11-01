// $Id: HsmRestoreHandler.java,v 1.2 2001-03-24 21:19:38 cvs Exp $

package diskCacheV111.pools ;

import  diskCacheV111.util.* ;
import  diskCacheV111.vehicles .* ;
import  dmg.util.Logable ;

import  java.util.* ;
import  java.io.IOException ;

public class HsmRestoreHandler  {

    private CacheRepository0 _repository  = null ;
    private Logable         _log         = null ;
    private HsmSet          _hsmSet      = null ;
    private Hashtable       _restorePnfsidList = new Hashtable() ;
    
    public class RestoreInfo {
        private RestoreInfo(){}
        private long   _startTime = new Date().getTime() ;
        private Vector _callbacks = new Vector() ;
        private void addCallback( CacheFileAvailable callback ){
           _callbacks.addElement(callback) ;
        }
        private Enumeration getCallbacks(){ return _callbacks.elements() ; }
        public int getListenerCount(){ return _callbacks.size() ; }
        public long getStartTime(){ return _startTime ; }
    }
    private long  _maxRuntime   = 4 * 3600 * 1000 ;
    private int   _maxLines     = 200 ;
    public HsmRestoreHandler( CacheRepository0 repository , HsmSet hsmSet ){
       _repository = repository ;
       _hsmSet     = hsmSet ;
    }
    public synchronized void setLogable( Logable log ){ _log = log ; }
    public synchronized boolean fetch( String pnfsId , StorageInfo storageInfo )
           throws CacheException {
       return fetch( pnfsId , storageInfo , null ) ;
    }
    public synchronized boolean fetch( String pnfsId , 
                                       StorageInfo  storageInfo ,
                                       CacheFileAvailable callback )
           throws CacheException {

       CacheRepository0.CacheEntry entry = null ;
       try{
          entry = _repository.getEntry( pnfsId ) ;           
       }catch(FileNotInCacheException nf ){
          entry = null ;
       }
       RestoreInfo info = (RestoreInfo)_restorePnfsidList.get( pnfsId ) ;
       //
       // check for inconsistencies
       //
       //
       // the repository has no idea about the file,
       // do we have ?
       //
       if( ( entry == null ) && ( info != null ) )
            throw new
            InconsistentCacheException( 666 , pnfsId+" : in not in rep. but in our restoreList" ) ;
       //
       // the repository nows about the file, we don't
       //
       if( ( entry != null ) && ( info == null ) ){
          //
          // comes from client. somebody should protect us from these request.
          //  
          if( entry.isReceiving() == CacheRepository0.CLIENT  )
            throw new
            InProgressCacheException( 1 , pnfsId+" : receiving by client" ) ;
          //
          //  worst : copying from HSM ( not we )
          //
          if( entry.isReceiving() == CacheRepository0.STORE  )
            throw new
            InconsistentCacheException( 666 , pnfsId+" : receiving by HSM" ) ;
       
          throw new
            FileInCacheException( "File is ready to use" ) ;
       }
       //
       // so , the repository and we seem to have the same view of the
       // world, at least according to this pnfsid.
       //
       boolean firstStore = info == null ;
       if( info == null ){
           entry = _repository.createEntry( pnfsId ) ;
           entry.setReceiving( CacheRepository0.STORE ) ;
           entry.setStorageInfo( storageInfo ) ;
           _restorePnfsidList.put( pnfsId , info = new RestoreInfo() ) ;
           
           new Thread( new FetchThread(pnfsId,storageInfo) , "restore-"+pnfsId ).start() ;
       }
       if( callback != null )info.addCallback( callback ) ;
       return firstStore ;
    }
    public synchronized Enumeration getPnfsIds(){ 
    
       Vector v = new Vector() ;
       for( Enumeration e = _restorePnfsidList.keys() ; e.hasMoreElements() ; )
          v.addElement(e.nextElement());
       return v.elements() ;
    }
    public synchronized RestoreInfo getRestoreInfoByPnfsId( String pnfsId ){
        return (RestoreInfo)_restorePnfsidList.get( pnfsId ) ;
    }
    private synchronized String getFetchCommand( String pnfsId , StorageInfo storageInfo ){
        //
        // which HSM 
        //
        
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
            
        StringBuffer sb = new StringBuffer() ;
        sb.append(hsmCommand).append(" get ").append(pnfsId).append("  ") ;
        sb.append(_repository.getDataFile(pnfsId).getPath()) ;
        sb.append(" -si=").append(storageInfo.toString());
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
    private void say( String msg ){ 
       if( _log != null )_log.log( "RSH : "+msg ) ;
    }
    private void esay( String msg ){
       if( _log != null )_log.log( "RSH ERROR : "+msg ) ;
    }
    private class FetchThread implements Runnable {
       private String      _pnfsId      = null ;
       private StorageInfo _storageInfo = null ;
       public FetchThread( String pnfsId , StorageInfo storageInfo ){
          _pnfsId      = pnfsId ;
          _storageInfo = storageInfo ;
       }
       public void run() {
           int    returnCode   = 1 ;
           RunSystem run       = null ;
           String    errmsg    = "ok" ;
           Exception excep     = null ;
           say( "FetchThread started" ) ;
           try{
               String fetchCommand = getFetchCommand( _pnfsId , _storageInfo );
               say( "Creating RunSystem for : "+fetchCommand ) ;
               run = new RunSystem( fetchCommand , _maxLines , _maxRuntime , _log ) ;
               say( "RunSystem.go()" ) ;
               run.go() ;
               returnCode = run.getExitValue() ;
               say( "RunSystem. -> "+returnCode+" : "+run.getErrorString() ) ;
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
               esay( errmsg = "FetchThread : can't determine 'hsmInfo' for "+_storageInfo+" {"+iae+"}" ) ;
               returnCode = 4 ;
               excep = iae ;
           }
           synchronized( HsmRestoreHandler.this ){
              
	      if (returnCode != 0){
                  esay( "FetchThread : ("+_pnfsId+") problem : "+returnCode ) ;
                  esay( errmsg = run.getErrorString()) ;  
                  excep = new CacheException( returnCode , errmsg ) ;    
                  _repository.destroyEntry( _pnfsId );
	      }else{
	          say("Fetch command succeeded");
                  try{
                      CacheRepository0.CacheEntry entry = _repository.getEntry(_pnfsId) ;
                      entry.setCached() ;
                  }catch(CacheException ee ){
                      esay(ee.toString()) ;
                      _repository.destroyEntry( _pnfsId ) ;
                      excep = ee ;
                  }
	      }
              Enumeration e  = ((RestoreInfo) _restorePnfsidList.get( _pnfsId )).getCallbacks() ;
              while( e.hasMoreElements() ){
                 try{
                     ((CacheFileAvailable)e.nextElement()).cacheFileAvailable(_pnfsId,excep) ;
                 }catch(Throwable t){
                    esay("Throwable in callback : "+t ) ;
                 }
              }
              _restorePnfsidList.remove( _pnfsId ) ;
           }
           say( _pnfsId+" : FetchThread Done" ) ;
       }
    }
   
}
