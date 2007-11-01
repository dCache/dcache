package dmg.apps.psdl.cells ;

import  dmg.apps.psdl.vehicles.* ;
import  dmg.apps.psdl.misc.* ;
import  dmg.cells.nucleus.* ;
import  dmg.util.*;
import  java.util.* ;
import  java.io.* ;

/**
  *  
  *
  * @author Patrick Fuhrmann
  * @version 0.1, 15 Feb 1998
  */
public class         PoolObserver  
          extends    CellAdapter 
          implements Runnable           {

   private CellNucleus _nucleus     = null ;
   private Args        _args        = null ;
   private File        _base        = null ;
   private File        _control , _data ;
   private StateInfoUpdater  _poolInfoSpray = null ;
   //
   // pool specific attributes
   //
   private String      _poolName ;
   private long        _total       = 0 ;
   private long        _maxSize     = 0 ;
   private String  []  _keys        = new String[0] ;
   private long        _maxFileSize = 0 ;
   private int         _priority    = 0 ;
   private int         _operations  = 0 ;
   private PoolInfo    _poolInfo    = null ;
   //
   // to fake an internal problem
   //
   private boolean     _denyRequests = false ;
   //
   private int         _fileCount = 0 ;
   
   private Thread    []_queueThread  = { null , null } ;
   private Hashtable []_moverHash    = { new Hashtable() , new Hashtable() }  ;
   private Object    []_moverLock    = { new Object()    , new Object()    } ;
   private Hashtable []_requestHash  = { new Hashtable() , new Hashtable() } ;
   private Vector    []_fifo         = { new Vector()    , new Vector()    } ;
   private boolean   []_queueState   = { true , true } ;
   
   private class Mover {
       private StateInfo _moverInfo ;
       private CellPath  _path ;
       private boolean   _isBusy = false ;
       
       public Mover( StateInfo info , CellPath path ){
          _moverInfo = info ;
          _path      = path ;
       }
       public String   getName(){ return _moverInfo.getName() ; }
       public void     setBusy( boolean busy ){ _isBusy = busy ; }
       public boolean  isBusy(){ return _isBusy ; }
       public CellPath getPath(){ return _path ; }
       public void     setInfo( StateInfo info ){ _moverInfo = info ; }
       public String   toString(){
          return _moverInfo.toString()+" : "
                 +(_isBusy?"Busy : ":"Idle : ")+_path.toString() ;
       }
   
   }
   private class Request {
      private Mover       _mover = null ;
      private CellMessage _msg   = null ;
      
      public Request( CellMessage msg ){ _msg = msg ; }
      
      public void        setMover( Mover mover ){ _mover = mover ; }
      public Mover       getMover(){   return _mover ; }      
      public CellMessage getMessage(){ return _msg ; }
      public String      toString(){
          return "Mover="+(_mover==null?"None":_mover.getName())+
                 ";Msg="+_msg ;
      }
   }

   public PoolObserver( String name , String argString ){
      super( name , argString , false ) ;
      
      useInterpreter( true ) ;
      
      _nucleus     = getNucleus() ;
      _args        = getArgs() ;
      
      try{
         if( _args.argc() < 2 )
           throw new 
           IllegalArgumentException( 
           "USAGE : ... <poolName> <poolBase> [<PoolMgrName> [...]]" ) ;
           
          _poolName = _args.argv(0) ;
          _base     = new File( _args.argv(1) ) ;          
          _data     = new File( _base , "data" ) ;
          
          if( ! _data.isDirectory() )
               throw new 
               IllegalArgumentException(
               "Not a valid Pool Base (no data) : "+_base ) ;

          _control  = new File( _base , "control" ) ;
          
          if( ! _control.isDirectory() )
               throw new 
               IllegalArgumentException(
               "Not a valid Pool Base (no control) : "+_base ) ;
               
          
          _poolInfoSpray = new StateInfoUpdater( _nucleus ,  10 ) ;
          
          for( int i = 2 ; i < _args.argc() ; i++ )
            _poolInfoSpray.addTarget( _args.argv(i) ) ;
          //
          // scanPool updates all pool specific variables and starts
          // the _poolInfoSpray
          //
          _scanPool() ;
          
      }catch(IllegalArgumentException iae ){
         start() ;
         kill() ;
         throw iae ;
      }catch(Exception e ){
         start() ;
         kill() ;
         throw new IllegalArgumentException( e.toString() ) ;
      }

      _queueThread[0] = new Thread( this ) ;
      _queueThread[1] = new Thread( this ) ;
      _queueThread[0].start() ;
      _queueThread[1].start() ;
      start() ;
   
   }
   private void putRequestAnswer( CellMessage msg , PsdlPutRequest req ){
   
      Request ir  = (Request)_requestHash[0].remove( msg.getLastUOID() ) ;
      if( ir == null ){
         esay( "PANIC : Won't forward unknown request : "+req ) ;
         return ; 
      }
      //
      //  releasse the mover
      //
      Mover mover = ir.getMover() ;
      say( "Releasing mover "+mover ) ;
      synchronized( _moverLock[0] ){
         mover.setBusy(false) ;
         _moverLock[0].notifyAll() ;
      }
      //
      //
      msg.nextDestination();
      if( req.getReturnCode() == 0 ){
         //
         //
         try{
            sendMessage( msg ) ;
         }catch( Exception ioe ){
            new File( _data , req.getPnfsId().toString() ).delete() ;
            esay( "PANIC : couldn't return msg to  client : "+req ) ;
         } 

      }else{
         //
         //  request failed ( get the original message )
         //
         new File( _data , req.getPnfsId().toString() ).delete() ;
         PsdlPutRequest r = (PsdlPutRequest)ir.getMessage().getMessageObject() ;
         _total -= r.getSize() ;                    
         say( "Readjusting the total poolsize to  : "+_total ) ;
         try{
            sendMessage( msg ) ;
         }catch( Exception ioe ){
            esay( "PANIC : couldn't return msg to  client : "+req ) ;
         } 
      }

   }
   public void messageToForward( CellMessage msg ){
   
      Object obj = msg.getMessageObject() ;
      
      if( obj instanceof PsdlPutRequest ){
          putRequestAnswer( msg , (PsdlPutRequest) obj ) ;
          return ;
      }
      
      super.messageToForward( msg ) ;
      
   }
   public void messageArrived( CellMessage msg ){
       Object obj = msg.getMessageObject() ;
       if( obj instanceof StateInfo ){
          //
          // the registy of the hsm and date movers
          //
          moverInfoArrived( msg , (StateInfo) obj ) ;
          //
       }else if( obj instanceof PsdlPutRequest ){
          psdlPutRequestArrived( msg , (PsdlPutRequest) obj ) ;
       }else if( obj instanceof HsmStoreRequest ){
          hsmStoreRequestArrived( msg , (HsmStoreRequest) obj ) ;
       }else if( obj instanceof RemoveRequest ){
          psdlRemoveRequestArrived( msg , (RemoveRequest) obj ) ;
       }else if( obj instanceof NoRouteToCellException ){
          exceptionArrived( msg , (NoRouteToCellException) obj ) ;
       }else{
          esay( "Unidentified Object arrived : "+obj.getClass() ) ;
       }
     
   }
   private void exceptionArrived( CellMessage omsg , 
                                  NoRouteToCellException exc ){
       for( int queue = 0 ; queue < 2 ; queue ++ )                         
       synchronized( _moverLock[queue] ){
          CellMessage msg = 
               (CellMessage)_requestHash[queue].remove( exc.getUOID() ) ;
          if( msg != null ){
              //
              // exception from 'mover not found'
              //
              try{
                 msg.revertDirection() ;
                 sendMessage( msg ) ;
              }catch( Exception e ){
                 esay( "PANIC : Can't inform client of NoRoute exception : "+
                       msg.getDestinationPath() ) ;
              }
              return ;
          } 
       }
       esay( "PANIC : exception arrived for unknown message : "+exc.getUOID());
       return ;
   }
   private void psdlRemoveRequestArrived( CellMessage msg , RemoveRequest req ){
      File file = new File( _data , req.getPnfsId().toString() ) ; 
      if( ! file.exists() ){
         esay( "File not found : "+file ) ;          
      }else{
         long size = file.length() ;
         if( ! file.delete() ){
             esay( "File not deleted : "+file ) ;
         }else{            
             _total -= size ;
             say( "Total adjusted to "+_total ) ;
         }
      }
      return ;
   }
   private void hsmStoreRequestArrived( CellMessage msg , HsmStoreRequest req ){
      say( "Queuing request : "+req ) ;
      addRequest( 1 , msg ) ;
   }
   private void psdlPutRequestArrived( CellMessage msg , PsdlPutRequest req ){

      if( _denyRequests || ( req.getSize() >= ( _maxSize - _total ) ) ){
         esay( "PoolObserver : No more space in pool " ) ;
         req.setReturnValue( 22 , 
             "PoolObserver : No more space in pool "+req.getSize()+
             " "+_maxSize+" "+_total ) ;
         msg.revertDirection() ;
         try{
            sendMessage( msg ) ;          
         }catch( Exception e ){
            esay( "Can't send negative response to client : "+e ) ;
         }
         return ;
      }else{
         say( "Queuing request : "+req ) ;
         _total += req.getSize() ;
         addRequest( 0 , msg ) ;
      }
   }
   private void addRequest( int queue , CellMessage msg ){
      Request ir = new Request( msg ) ;
      synchronized( _moverLock[queue] ){
         _requestHash[queue].put( msg.getUOID()  , ir ) ; 
         _fifo[queue].addElement( ir ) ;
         _moverLock[queue].notifyAll() ;
      }
   
   }
   private void stopRequestQueue( int queue ){
      synchronized( _moverLock[queue] ){
         _queueState[queue] = false ;
         _moverLock.notifyAll() ;
      }
   }
   private void runQueueManager( int queue ){
      //
      // each runQueueManager is running in a private thread.
      //
      synchronized( _moverLock[queue] ){
         Mover mover = null ;
         while( true ){
             //
             // to run the mover-request assingment, we need
             // at least one request and one idle mover.
             // otherwise we 'wait'
             //
             while(
                    //
                    // is there a request pending ?
                    //
                    ( _fifo[queue].size() > 0 ) &&
                    //
                    // and a mover available ?
                    //
                    ( ( mover = getIdleMover( queue ) ) != null )   ){
                 
                 //
                 // allocate the mover
                 //  
                 mover.setBusy(true) ;
                 // 
                 // assign the request to the mover
                 // and remove the request from the queue.
                 //
                 Request         ir  = (Request)_fifo[queue].elementAt(0) ;
                 ir.setMover( mover ) ;
                 CellMessage     msg = ir.getMessage() ;
                 PsdlCoreRequest req = (PsdlCoreRequest)msg.getMessageObject() ;
                 _fifo[queue].removeElementAt(0) ;
                 say( "Mover assigned : "+mover+" to "+req ) ;
                 //
                 // send the request to the assigned mover
                 //
                 CellPath dest = msg.getDestinationPath() ;
                 dest.add( mover.getPath() ) ;
                 dest.next() ;
                 try{
                    sendMessage( msg  ) ;
                 }catch( Exception e ){
                    //
                    // if it fails, (NoRouteToCell) we remove the
                    // mover from our list. (unexpected death of file)
                    //
                    esay( "WARNING : Couldn't forward msg to : "+mover ) ;
                    _requestHash[queue].remove( msg.getUOID() ) ;
                    _moverHash[queue].remove( mover.getName() ) ;
                    // mover.setBusy( false ) ;
                    req.setReturnValue( 21 , "no route to selected mover" ) ;
                    req.setRequestType("answer");
                    msg.revertDirection() ;
                    try{
                       sendMessage( msg ) ;
                    }catch( Exception eee ){
                       esay( "PANIC : Couldn't inform client : "+eee) ;
                    }
                 }
             }
             //
             // at this point we are waiting for a mover or a 
             // request. Whatever comes in, will notify us.
             //
             try{ _moverLock[queue].wait() ; }
             catch( InterruptedException ie ){
                say( "QueueManager Thread intrrupeted" ) ;
                break ;
             }
             //
             // to stop us, the _moverState must be switched
             // off and we have to be notified.
             // we only stop after all requests have been
             // processed.
             //
             if( ! _queueState[queue] )return ;
         
         }
      
      }
   }
   private Mover getIdleMover( int queue ){
      //
      // checks the mover queue 'queue' for an idle mover
      //
      say( "Searching idle mover" ) ;
      synchronized( _moverLock[queue] ){
         Mover mover = null ;
         Enumeration e = _moverHash[queue].elements() ;
         for( ; e.hasMoreElements() ; ){
            if( ! ( mover = (Mover)e.nextElement() ).isBusy() ){
               say( "Mover idle : "+mover ) ;
               return mover ;
            }
         }
         say( "No idle mover found" ) ;
         return null ;         
      }
   }
   private void moverInfoArrived( CellMessage msg , StateInfo info ){
      int queue = 0 ;
      if( info instanceof MoverInfo )queue = 0 ;
      else if( info instanceof HsmMoverInfo )queue = 1 ;
      else{
         esay( "Unknown StateInfo arrived : "+info ) ;
         return ;
      }
      
      String moverName = info.getName() ;
      
      if( info.isUp() ){
         //   say( "Got up info for "+info ) ;
         //
         // make a copy of the source path,
         // revert the direction and store the
         // result in the hastable of all drives
         //
         CellPath path      = msg.getSourcePath() ;
         CellPath moverPath = (CellPath)path.clone() ;
         moverPath.revert() ;
         //
         // 
         // if the required mover already exists we 
         // simply set the new info inside the Mover
         // class, otherwise we create a new entry
         // and inform all related listeners.
         //
         synchronized( _moverLock[queue] ){
           Mover mover = (Mover)_moverHash[queue].get( moverName ) ;
           if( mover == null ){
               _moverHash[queue].put( moverName , 
                                      new Mover( info , 
                                                 moverPath ) ) ;
              _moverLock[queue].notifyAll() ;
           }else{
               mover.setInfo( info ) ;
           }
         }
      }else{
         say( "Got DOWN info for "+info ) ;
         if( _moverHash[queue].remove( moverName ) == null ){
             esay( "PANIC : won't remove unknown mover "+moverName ) ;
         }
      }
   }
   public void run(){
   
     if( Thread.currentThread() == _queueThread[0] ){
         runQueueManager(0) ;
     }else if( Thread.currentThread() == _queueThread[1] ){
         runQueueManager(1) ;
     } 
   }
   public void cleanUp(){
      say( "Clean up requested" ) ;
      say( "Stopping Threads" ) ;
      //
      // sends the down message to the pool manager
      //
      _poolInfoSpray.down() ;
      stopRequestQueue(0) ;
      stopRequestQueue(1) ;
      //
      // now we should wait for the two queues to die.
      // They will process all queued jobs first.
   }
   private void _scanPool() throws IOException {
      _control   = new File( _base , "control" ) ;
      _data      = new File( _base , "data" ) ;
      File setup = new File( _base , "setup" ) ;
      //
      // first check the most obvious things to make 
      // sure we are really observing a psdl pool.
      //
      if( ( ! _base.exists()         ) ||
          ( ! _control.isDirectory() ) || 
          ( !  setup.isFile()        ) || 
          ( ! _data.isDirectory()    )    )
            throw new 
            IllegalArgumentException( 
               "Not a valid Pool Base (Setup invalid) : "+_base ) ;
      //
      // read the setup file, and fill out the corresponding
      // internal variable _maxFileSize ... 
      //
      _updateSetupFile( setup ) ;
      //
      // scan the data files and update 'total' and _fileCount ;
      //
      String [] files = _data.list() ;
      _fileCount = files.length ;
      for( int i= 0 ; i < _fileCount ; i++ ){
         _total += new File( _data , files[i] ).length() ;
      }
      //
      // set the correct poolInfo for the distributer
      //
      updatePoolInfo() ;   
   
   }
   private void updatePoolInfo(){
      _poolInfo = new PoolInfo( _poolName , _maxSize , _maxFileSize ,
                                _priority , _operations , _keys         ) ;
                                
      _poolInfoSpray.setInfo( _poolInfo ) ;   
   }
   public String hh_set_deny = "on|off" ;
   public String ac_set_deny_$_1( Args args ){
      _denyRequests = args.argv(0).equals( "on" ) ;
      return "Deny Requests : "+_denyRequests ;
   }
   public String hh_set_poolsize = "<poolSize/bytes>" ;
   public String ac_set_poolsize_$_1( Args args ) {
       _maxSize = new Long( args.argv(0) ).longValue() ;
       updatePoolInfo() ;   
       return "" ;
   }
   public String hh_set_maxfilesize = "<maximumFileSize/bytes>" ;
   public String ac_set_maxfilesize_$_1( Args args ) {
       _maxFileSize = new Long( args.argv(0) ).longValue() ;
       updatePoolInfo() ;   
       return "" ;
   }
   public String hh_set_priority = "<poolPriority>" ;
   public String ac_set_priority_$_1( Args args ) {
       _priority = new Integer( args.argv(0) ).intValue() ;
       updatePoolInfo() ;   
       return "" ;
   }
   public String hh_add_hsmkey = "<hsmKey> # e.g. <store>.<group> " ;
   public synchronized String ac_add_hsmkey_$_1( Args args ) 
          throws CommandException {
   
       String newKey = args.argv(0) ;
       int pos = findHsmKey( newKey ) ;
       if( pos > -1 )
          throw new 
          CommandException( "Key already exists : "+newKey ) ;
          
       String [] keys = new String[_keys.length+1] ;
       int i ;
       for( i = 0 ; i < _keys.length ; i++ )keys[i] = _keys[i] ;
       keys[i] = newKey ;
       _keys = keys ;
       updatePoolInfo() ;   
       
       return "" ;
   }
   public String hh_remove_hsmkey = "<hsmKey> # e.g. <store>.<group> " ;
   public synchronized String ac_remove_hsmkey_$_1( Args args )
          throws CommandException {
   
       String rmKey = args.argv(0) ;
       int pos = findHsmKey( rmKey ) ;
       if( ( _keys.length == 0 ) || ( pos == -1 ) )
          throw new 
          CommandException( "Key not found : "+rmKey ) ;
          
       String [] keys = new String[_keys.length-1] ;
       int i , j ;
       for( i = 0 , j = 0  ; i < _keys.length ; i++ )
          if( i != pos )keys[j++] = _keys[i] ;
          
       _keys = keys ;
       
       updatePoolInfo() ;   
       return "" ;
   }       
   private int findHsmKey( String key ){
      int i ;
      for( i = 0 ;  ( i < _keys.length ) &&
                    ( ! _keys[i].equals( key ) ) ; i++ ) ;
      return i == _keys.length ? -1 : i ;
   } 
   public String hh_add_operation = "put|get" ;
   public String ac_add_operation_$_1( Args args ) throws CommandException {
     String operation = args.argv(0) ;
     if( operation.equals( "put" ) ){
         _operations |= 1 ;
     }else if( operation.equals( "get" ) ){
         _operations |= 2 ;
     }else 
        throw new 
        CommandException( "Not a valid operation : "+operation ) ;
     updatePoolInfo() ;   
     return "" ;
   }
   public String hh_remove_operation = "put|get" ;
   public String ac_remove_operation_$_1( Args args ) throws CommandException {
     String operation = args.argv(0) ;
     if( operation.equals( "put" ) ){
         _operations &= ~ 1 ;
     }else if( operation.equals( "get" ) ){
         _operations &= ~ 2 ;
     }else 
        throw new 
        CommandException( "Not a valid operation : "+operation ) ;
     updatePoolInfo() ;   
     return "" ;
   }
   public String hh_add_poolmgr = "<poolManagerName>" ;
   public String ac_add_poolmgr_$_1( Args args ){
      _poolInfoSpray.addTarget( args.argv(0) ) ;
      return "" ;
   }
   public String hh_remove_poolmgr = "<poolManagerName>" ;
   public String ac_remove_poolmgr_$_1( Args args ){
      _poolInfoSpray.removeTarget( args.argv(0) ) ;
      return "" ;
   }
   private void _updateSetupFile( File setup ){
      BufferedReader r = null ;
      try{
         r = new BufferedReader( new FileReader( setup ) ) ;
         String line  = null ;
         while( ( line = r.readLine() ) != null ){
            if( ( line.length() < 1 ) || ( line.charAt(0) == '#' ) )continue;
               
            try{
               command( new Args( line ) ) ;
            }catch( Exception ee ){ 
               esay( "Problem executing line : "+line+" : "+ee ) ;
               continue; 
            }
         }
         r.close() ;
      }catch( Exception e ){
         try{ r.close() ; }catch(Exception ie){}
         throw new 
         IllegalArgumentException( "Not a Pool : "+_base.getName()+" "+e ) ;
      }
      if( _maxSize == 0 )
         throw new IllegalArgumentException( "Pool Size not specified in setup" ) ;
         
   }
   public void getInfo( PrintWriter pw ){
      pw.println( " PoolName    : "+_poolName ) ;
      pw.println( " Base        : "+_base.getAbsolutePath() ) ;
      pw.println( " Used        : "+_total ) ;
      pw.println( " Files       : "+_fileCount ) ;
      pw.print(   " PoolMgr     : ") ;
        String [] poolMgr = _poolInfoSpray.getTargets() ;
        for( int i = 0 ; i < poolMgr.length ; i++ )
           pw.print( poolMgr[i]+" " ) ;
      pw.println("") ;
      if( _poolInfo != null )_poolInfo.toWriter( pw ) ;
      pw.println( " Data Mover Infos : " ) ;
      moverInfos( 0 , pw ) ;
      pw.println( " Hsm Mover Infos : " ) ;
      moverInfos( 1 , pw ) ;
      pw.println( " Pending Data Mover Requests : " ) ;
      requestInfos( 0 , pw ) ;
      pw.println( " Pending Hsm Mover Requests : " ) ;
      requestInfos( 1 , pw ) ;
   }
   public void requestInfos( int queue , PrintWriter pw ){

     Enumeration e = _requestHash[queue].elements() ;
     for( ; e.hasMoreElements() ; ){
         Request         ir    = (Request)e.nextElement() ;
         Mover           mover = ir.getMover() ;
         CellMessage     msg   = ir.getMessage() ;
         PsdlCoreRequest req   = (PsdlCoreRequest)msg.getMessageObject() ;
         if( mover == null ){
            pw.println( "   Waiting : "+req ) ;
         }else{
            pw.println( "   "+mover.getName() +" : "+req ) ;
         }
     }
   }
   private void moverInfos( int queue , PrintWriter pw ){
      Enumeration e = _moverHash[queue].elements() ;
      for( ; e.hasMoreElements() ; ){     
          pw.println( "   "+ e.nextElement() ) ;
      }
   }
  
   
}
