package dmg.cells.nucleus ;

import  dmg.util.Args ;
import  java.io.*;
import  java.util.* ;
import  java.lang.reflect.* ;
import  java.net.Socket ;
import  java.text.* ;

/**
  *
  *
  * @author Patrick Fuhrmann
  * @version 0.1, 15 Feb 1998
  */
public class CellNucleus implements Runnable {

   private final  int    INITIAL  =  0 ;
   private final  int    ACTIVE   =  1 ;
   private final  int    REMOVING =  2 ;
   private final  int    DEAD     =  3 ;

   private static CellGlue __cellGlue  = null ;
   private        String    _cellName  = null ;
   private        String    _cellType  = null ;
   private        ThreadGroup _threads      = null ;
   private        Thread    _eventThread    = null ;
   private        SyncFifo2 _eventQueue     = null ;
   private        Thread    _killThread     = null ;
   private        KillEvent _killEvent      = null ;
   private        Cell      _cell           = null ;
   private        Date      _creationTime   = new Date() ;
   private        int       _state          = INITIAL ;
   private        int       _printoutLevel  = 0 ;
   private        Hashtable _waitHash         = new Hashtable() ;
   private        boolean   _runAsyncCallback = false ;

   public CellNucleus( Cell cell , String name ){

        this( cell , name , "Generic" ) ;
   }
   public CellNucleus( Cell cell , String name , String type ){

      if(  __cellGlue == null ){
         //
         // the cell gluon hasn't yet been created
         // ( we insist in creating a SystemCell first. )
         //
         if( cell instanceof dmg.cells.nucleus.SystemCell ){
             __cellGlue = new CellGlue( name  ) ;
              _cellName    = "System" ;
              _cellType    = "System" ;
             __cellGlue.setSystemNucleus( this ) ;
         }else{
             throw new
             IllegalArgumentException( "System must be first Cell" ) ;
         }

      }else{
         //
         // we don't accept more then one System.cells
         //
         if( cell instanceof dmg.cells.nucleus.SystemCell ){
             throw new
             IllegalArgumentException( "System already exists" ) ;
         }else{
              _cellName    = name.replace('@' , '+' ) ;
              _cellType    = type ;
             if( ( _cellName == null ) ||
                 ( _cellName.equals("") ) )_cellName = "*" ;
             if( _cellName.charAt( _cellName.length() - 1 ) == '*' ){
                if( _cellName.length() == 1 ){
                   _cellName = "$-"+getUnique() ;
                }else{
                   _cellName = _cellName.substring(0,_cellName.length()-1)+
                               "-"+getUnique() ;
                }

             }

         }
      }
      _cell = cell ;
      //
      // for the use in restricted sandboxes
      //
      try{

//        _threads = new ThreadGroup( __cellGlue.getMasterThreadGroup(),
//                                    _cellName+"-threads" ) ;
        _threads = new CellThreadGroup(
                                    this ,
                                    __cellGlue.getMasterThreadGroup(),
                                    _cellName+"-threads" ) ;

      }catch( SecurityException se ){
        _threads = null ;
      }
      _eventQueue  = new SyncFifo2();
      _eventThread = new Thread( _threads , this , "Messages" ) ;
      _eventThread.start() ;

      nsay( "Created : "+name ) ;
      _state = ACTIVE ;

      //
      // make ourself known to the world
      //
      _printoutLevel = __cellGlue.getDefaultPrintoutLevel() ;
      __cellGlue.addCell( _cellName , this ) ;

   }
   void setSystemNucleus( CellNucleus nucleus ){
      __cellGlue.setSystemNucleus( nucleus ) ;
   }
   public void setAsyncCallback( boolean asyncCallback ){
        _runAsyncCallback = asyncCallback ;
   }
   public String getCellName(){ return _cellName ; }
   public String getCellType(){ return _cellType ; }
   public CellAddressCore getThisAddress(){
      return new CellAddressCore( _cellName , __cellGlue.getCellDomainName() ) ;
   }
   public CellDomainInfo getCellDomainInfo(){
      return __cellGlue.getCellDomainInfo() ;
   }
   public String getCellDomainName(){
      return __cellGlue.getCellDomainName() ;
   }
   public String [] getCellNames(){ return __cellGlue.getCellNames() ; }
   public CellInfo getCellInfo( String name ){
       return __cellGlue.getCellInfo( name ) ;
   }
   public CellInfo getCellInfo(){
       return __cellGlue.getCellInfo( getCellName() ) ;
   }
   public Dictionary getDomainContext(){ return __cellGlue.getCellContext() ; }
   public Reader getDomainContextReader( String contextName )
          throws FileNotFoundException  {
       Object o = __cellGlue.getCellContext( contextName ) ;
       if( o == null )
           throw new
           FileNotFoundException( "Context not found : "+contextName ) ;
       return new StringReader( o.toString() ) ;
   }
   public void   setDomainContext( String contextName , Object context ){
       __cellGlue.getCellContext().put( contextName , context ) ;
   }
   public Object getDomainContext( String str ){
       return __cellGlue.getCellContext( str ) ;
   }
   public String [] [] getClassProviders(){
       return __cellGlue.getClassProviders() ;
   }
   public synchronized void setClassProvider( String selection , String provider ){
      __cellGlue.setClassProvider( selection , provider ) ;
   }
   Cell getThisCell(){ return _cell ; }

   CellInfo _getCellInfo(){
      CellInfo info = new CellInfo() ;
      info.setCellName(   getCellName() ) ;
      info.setDomainName( getCellDomainName() ) ;
      info.setCellType(   getCellType() ) ;
      info.setCreationTime( _creationTime ) ;
      try{
         info.setCellVersion( getCellVersionByObject( _cell ) ) ;
      }catch(Exception e ){}
      try{
         info.setPrivateInfo(  _cell.getInfo()  ) ;
      }catch( Exception e ){
         info.setPrivateInfo(  "Not yet/No more available\n"  ) ;
      }
      try{
         info.setShortInfo(    _cell.toString()  ) ;
      }catch( Exception e ){
         info.setShortInfo(  "Not yet/No more available"  ) ;
      }
      try{
         info.setCellClass(    _cell.getClass().getName()  ) ;
         info.setEventQueueSize( getEventQueueSize()  ) ;
         info.setState( _state  ) ;
         info.setThreadCount( _threads.activeCount() ) ;
      }catch(Exception e){

         info.setCellClass( "Unknown" ) ;
         info.setEventQueueSize( 0 ) ;
         info.setState( 0 ) ;
         info.setThreadCount( 0 ) ;
      }
      return info ;
   }
//   protected void finalize()  throws Throwable {
//      say( "finalize called " ) ;
//   }
   public void   setPrintoutLevel( int level ){ _printoutLevel = level ; }
   public int    getPrintoutLevel(){ return _printoutLevel ; }
   public void   setPrintoutLevel( String cellName , int level ){
      __cellGlue.setPrintoutLevel( cellName , level ) ;
   }
   public int    getPrintoutLevel( String cellName ){
      return __cellGlue.getPrintoutLevel( cellName ) ;
   }
   public void   sendMessage( CellMessage msg )
          throws NotSerializableException,
                 NoRouteToCellException    {

      __cellGlue.sendMessage( this , msg , true , true ) ;

   }
   public void   resendMessage( CellMessage msg )
          throws NotSerializableException,
                 NoRouteToCellException    {

      __cellGlue.sendMessage( this , msg , false , true ) ;

   }
   public void   sendMessage( CellMessage msg ,
                              boolean locally ,
                              boolean remotely    )
          throws NotSerializableException,
                 NoRouteToCellException    {

      __cellGlue.sendMessage( this , msg , locally , remotely ) ;

   }
   public CellMessage   sendAndWait( CellMessage msg , long timeout )
          throws NotSerializableException,
                 NoRouteToCellException,
                 InterruptedException      {
       return sendAndWait( msg  , true , true , timeout ) ;
   }
   public CellMessage   sendAndWait( CellMessage msg ,
                                     boolean local ,
                                     boolean remote ,
                                     long    timeout )
          throws NotSerializableException,
                 NoRouteToCellException   ,
                 InterruptedException      {

      CellLock    lock   = new CellLock() ;
      CellMessage answer = null ;
      UOID        uoid   = null ;

      synchronized( lock ){

         synchronized( _waitHash ){
            __cellGlue.sendMessage( this , msg , local , remote ) ;
            _waitHash.put( uoid = msg.getUOID() , lock ) ;
            nsay( "sendAndWait : adding to hash : "+msg.getUOID() ) ;
         }
         //
         // because of a linux native thread problem with
         // wait( n > 0 ) , we have to use a interruptedFlag
         // and the time messurement.
         //
         long start = System.currentTimeMillis() ;
         try{
            while( timeout > 0 ){
               lock.wait( timeout ) ;
               if( lock.getObject() != null )break ;
               timeout -= ( System.currentTimeMillis() - start ) ;
            }
         }catch(InterruptedException we ){
            _waitHash.remove( uoid ) ;
            throw we ;
         }

         answer = (CellMessage)lock.getObject() ;

         if( answer == null ){
            _waitHash.remove( uoid ) ;
            return null ;
         }

      }
      return new CellMessage( answer ) ;

   }
   public HashMap getWaitQueue(){
      Enumeration e    = _waitHash.keys() ;
      HashMap     hash = new HashMap() ;
      while( e.hasMoreElements() ){
         Object   key  = e.nextElement() ;
         CellLock lock = (CellLock)_waitHash.get( key ) ;
         if( lock == null )continue ;
         hash.put( key , lock ) ;
      }
      return hash ;
   }
   public int updateWaitQueue(){
      if( _waitHash.size() == 0 )return 0 ;
      Enumeration e    = _waitHash.keys() ;
      Hashtable   hash = new Hashtable() ;
      long        now  = new Date().getTime();
      while( e.hasMoreElements() ){
         Object   key  = e.nextElement() ;
         CellLock lock = (CellLock)_waitHash.get( key ) ;
         if( ( lock == null             ) ||
             ( lock.isSync()            ) ||
             ( lock.getTimeout() >= now )     )continue ;
         hash.put( key , lock ) ;
      }
      synchronized( _waitHash ){
         for( e = hash.keys() ; e.hasMoreElements() ; ){
            _waitHash.remove( e.nextElement() ) ;
         }
      }
      //
      // _waitHash can't be used here. Otherwise
      // we will end up in a deadlock ( NO LOCKS WHILE CALLING CALLBACKS)
      //
      for( e = hash.elements() ; e.hasMoreElements() ; ){
         CellLock lock = (CellLock)e.nextElement();
         lock.getCallback().answerTimedOut( lock.getMessage() ) ;
      }
      return _waitHash.size();

   }
   public void sendMessage( CellMessage msg ,
                            boolean local ,
                            boolean remote ,
                            CellMessageAnswerable callback ,
                            long    timeout )
          throws NotSerializableException  {

      CellLock    lock   = new CellLock( msg , callback , timeout ) ;
      CellMessage answer = null ;
      UOID        uoid   = null ;

      synchronized( lock ){

         synchronized( _waitHash ){
            try{
                __cellGlue.sendMessage( this , msg , local , remote ) ;
            }catch( NoRouteToCellException nrtce ){
                if( callback != null )
                    callback.exceptionArrived( msg , nrtce ) ;
                return ;
            }
            _waitHash.put( uoid = msg.getUOID() , lock ) ;
         }

      }
      return ;

   }

   public void addCellEventListener( CellEventListener listener ){
      __cellGlue.addCellEventListener( this , listener ) ;

   }
   public void export(){ __cellGlue.export( this ) ;  }
   /**
    *
    * The kill method schedules the specified cell for deletion.
    * The actual remove operation will run in a different
    * thread. So on return of this method the cell may
    * or may not be alive.
    */
   public void kill(){   __cellGlue.kill( this ) ;  }
   /**
    *
    * The kill method schedules this Cell for deletion.
    * The actual remove operation will run in a different
    * thread. So on return of this method the cell may
    * or may not be alive.
    */
   public void kill( String cellName ) throws IllegalArgumentException {
      __cellGlue.kill( this , cellName ) ;
   }
   public void run(){
     if( Thread.currentThread() == _eventThread ){
        CellEvent event ;
        nsay( "messageThread : started" ) ;
        //
        // the standard SyncFifo return 'null' if interrupted.
        //
        while( ( event = (CellEvent)_eventQueue.pop() ) != null ){
          if( event instanceof LastMessageEvent ){
             nsay( "messageThread : LastMessageEvent arrived" ) ;
             try{
               _cell.messageArrived( (MessageEvent)event ) ;
             }catch( Throwable nse ){
               nesay( "messageThread : "+
                      "Exception in cell.messageArrived(LastMessageEvent) " ) ;
               nesay( nse ) ;
             }
             break ;
          }else if( event instanceof RoutedMessageEvent ){
             nsay( "messageThread : RoutedMessageEvent arrived" ) ;
             try{
               _cell.messageArrived( (RoutedMessageEvent)event ) ;
             }catch( Throwable nse ){
               nesay( "messageThread : "+
                      "Exception in cell.messageArrived(RoutedMessageEvent)") ;
               nse.printStackTrace();
             }
          }else if( event instanceof MessageEvent ){
             MessageEvent msgEvent = (MessageEvent) event ;
             nsay( "messageThread : MessageEvent arrived" ) ;
             try{
                CellMessage  msg = msgEvent.getMessage() ;
                //
                // deserialize the message
                //
                msg = new CellMessage( msgEvent.getMessage() ) ;
                //
                // and deliver it
                //
                nsay( "messageThread : delivering message : "+msg) ;
                _cell.messageArrived( new MessageEvent( msg ) ) ;
                nsay( "messageThread : delivering message done : "+msg) ;
                //
             }catch( Throwable nse ){
                nesay( "messageThread : "+
                       "Exception in cell.messageArrived(MessageEvent)") ;
                nse.printStackTrace();
             }
          }
        }
        nsay( "messageThread : stopped" ) ;
     }else if( Thread.currentThread() == _killThread ){
        nsay( "killerThread : started" ) ;
        KillEvent  event = _killEvent  ;
        _state =  REMOVING ;
        addToEventQueue( new LastMessageEvent() ) ;
        try{
           _cell.prepareRemoval( event ) ;
        }catch( Throwable nse ){
           nesay( "killerThread : "+
                  "Exception in cell.prepareRemoval(LastMessageEvent)") ;
           nesay( nse ) ;
        }
        int activeCount = 0 ;
        nsay( "killerThread : waiting for all threads in "+_threads+" to finish" ) ;
        while( ( activeCount =  _threads.activeCount() ) > 0 ){
           nsay( "killerThread : still "+activeCount+" threads active" ) ;
           Thread [] elements = new Thread[activeCount] ;
           int n = _threads.enumerate( elements ) ;
           for( int i = 0 ; i < n; i++ ){
               nsay( "killerThread : interrupting "+elements[i].getName() ) ;
               elements[i].interrupt() ;
           }
           try{
              Thread.sleep(2000) ;
           }catch( InterruptedException ie ){
              nesay( "killerThread : Interrupted while waiting for "+n+" threads" );
              break ;
           }

        }
        try{
           _threads.destroy() ;
        }catch( Throwable t ){
           nesay( "killerThread : _threads.destroy : "+t ) ;
        }
        __cellGlue.destroy( this ) ;
        _cell  = null ;
        _state =  DEAD ;
        nsay( "killerThread : stopped" ) ;
     }
   }
   public Thread newThread( Runnable target ){
      return new Thread( _threads , target ) ;
   }
   public Thread newThread( Runnable target , String name ){
      return new Thread( _threads , target , name ) ;
   }
   //
   //  package
   //
   Thread [] getThreads( String cellName ){
      return __cellGlue.getThreads( cellName ) ;
   }
   public ThreadGroup getThreadGroup(){ return _threads ; }
   Thread [] getThreads(){
      if( _threads == null )return new Thread[0] ;

      int threadCount = _threads.activeCount() ;
      Thread [] list  = new Thread[threadCount] ;
      int rc = _threads.enumerate( list ) ;
      if( rc == list.length )return list ;
      Thread [] ret = new Thread[rc] ;
      System.arraycopy( list , 0 , ret , 0 , rc ) ;
      return ret ;
   }
   int  getUnique(){ return __cellGlue.getUnique() ; }

   int  getEventQueueSize(){ return _eventQueue.size() ; }

   void addToEventQueue( CellEvent ce ){
      //
      //
      if( ce instanceof RoutedMessageEvent ){
         if( _cell instanceof CellTunnel ){
            //
            // nothing to do ( no transformation needed )
            //
         }else{
            //
            // originally this case has not been forseen,
            // but it appeared rather useful. It allows alias
            // cells which serves several different cells names.
            // mainly useful for debuggin purposes ( see alias
            // package.
            //
            ce = new MessageEvent( ((RoutedMessageEvent)ce).getMessage() ) ;
         }
      }
      if( ce instanceof MessageEvent ){
         //
         // we have to cover 3 cases :
         //   - absolutely asynchronous request
         //   - asynchronous , but we have a callback to call
         //   - synchronous
         //
         CellMessage msg = ((MessageEvent)ce).getMessage() ;
         if( msg != null ){
            nsay( "addToEventQueue : message arrived : "+msg ) ;
            CellLock lock ;
            synchronized( _waitHash ){
                Enumeration e = _waitHash.keys() ;
                for( ; e.hasMoreElements() ; )
                   nsay( "addToEventQueue : Queue : "+(UOID)e.nextElement() ) ;
                lock = (CellLock)_waitHash.remove( msg.getLastUOID() ) ;
            }
            if( lock != null ){
               //
               // we were waiting for you ( sync or async )
               //
               nsay( "addToEventQueue : lock found for : "+msg) ;
               if( lock.isSync() ){
                  nsay( "addToEventQueue : is synchronized : "+msg ) ;
                  synchronized( lock ){
                     lock.setObject( msg ) ;
                     lock.notifyAll() ;
                  }
                  nsay( "addToEventQueue : dest. was triggered : "+msg ) ;
               }else{
                  CellMessageAnswerable callback = lock.getCallback() ;
                  nsay( "addToEventQueue : is asynchronized : "+msg ) ;
                  CellMessage answer = null ;
                  Object      obj    = null ;
                  try{
                     answer = new CellMessage( msg ) ;
                     obj    = answer.getMessageObject() ;
                  }catch( NotSerializableException nse ){
                     obj = nse ;
                  }
                  if( _runAsyncCallback ){
                     final Object                asyncObj      = obj ;
                     final CellLock              asyncLock     = lock ;
                     final CellMessageAnswerable asyncCallback = callback ;
                     final CellMessage           asyncAnswer   = answer ;
                     new Thread(
                        new Runnable(){
                           public void run(){
                              nsay("Starting async callback");
                              try{
                                 if( asyncObj instanceof Exception ){
                                     asyncCallback.
                                           exceptionArrived( asyncLock.getMessage() ,
                                                             (Exception)asyncObj) ;
                                 }else{
                                     asyncCallback.
                                            answerArrived( asyncLock.getMessage(),
                                                           asyncAnswer) ;
                                 }
                              }catch( Throwable t ){
                                 nesay( "addToEventQueue : throwable in callback : "+t ) ;
                                 nesay( t ) ;
                              }
                              nsay("Async Callback done");
                           }
                        }
                     ).start() ;

                  }else{
                     try{
                        if( obj instanceof Exception ){
                            callback.exceptionArrived( lock.getMessage() ,
                                                       (Exception)obj       ) ;
                        }else{
                            callback.answerArrived( lock.getMessage() ,
                                                    answer               ) ;
                        }
                     }catch( Throwable t ){
                        nesay( "addToEventQueue : throwable in callback : "+t ) ;
                        nesay( t ) ;
                     }
                  }
                  nsay( "addToEventQueue : callback done for : "+msg ) ;
               }
               return ;
            }
         }     // end of : msg != null
      }        // end of : ce instanceof MessageEvent
      _eventQueue.push( ce ) ;

   }
   void sendKillEvent(  KillEvent ce ){
      nsay( "sendKillEvent : received "+ce ) ;
      _killEvent  = ce ;
      _killThread = new Thread( __cellGlue.getKillerThreadGroup() ,
                                this ,
                                "killer-"+_cellName ) ;
      _killThread.start() ;
      nsay( "sendKillEvent : "+_killThread.getName()+" started on group "+
            _killThread.getThreadGroup().getName() ) ;

   }
   //
   // helper to get version string from arbitrary object
   //
   public static CellVersion getCellVersionByObject( Object obj ) throws Exception {
       Class c =  obj.getClass()  ;

       Method m = c.getMethod( "getCellVersion" , null ) ;

       return (CellVersion)m.invoke( obj , null ) ;
   }
   public static CellVersion getCellVersionByClass( Class c ) throws Exception {

       Method m = c.getMethod( "getCellVersion" , null ) ;

       return (CellVersion)m.invoke( null , null ) ;
   }
   ////////////////////////////////////////////////////////////
   //
   //   create new cell by different arguments
   //   String , String [] , Socket
   //   can choose between systemLoader only or
   //   Domain loader.
   //
   public Cell createNewCell( String cellClass ,
                              String cellName  ,
                              String cellArgs ,
                              boolean systemOnly )
       throws ClassNotFoundException ,
              NoSuchMethodException ,
              SecurityException ,
              InstantiationException ,
              InvocationTargetException ,
              IllegalAccessException ,
              ClassCastException          {

       Object [] args = new Object[1] ;
       args[0] = cellArgs ;

       return (Cell)__cellGlue._newInstance( cellClass ,
                                             cellName ,
                                             args ,
                                             systemOnly       ) ;
   }
   public Class loadClass( String className ) throws ClassNotFoundException {
       return __cellGlue.loadClass( className ) ;
   }
   /*
   public Cell createNewCell( String cellClass ,
                              String cellName  ,
                              String [] cellArgs )
       throws ClassNotFoundException ,
              NoSuchMethodException ,
              SecurityException ,
              InstantiationException ,
              InvocationTargetException ,
              IllegalAccessException ,
              ClassCastException          {

       Object [] args = new Object[1] ;
       args[0] = cellArgs ;

       return (Cell)__cellGlue._newInstance( cellClass ,
                                             cellName ,
                                             args ,
                                             true      ) ;
   }
   */
   public Object  createNewCell( String className ,
                                 String cellName ,
                                 String [] argsClassNames  ,
                                 Object [] args             )
       throws ClassNotFoundException ,
              NoSuchMethodException ,
              InstantiationException ,
              IllegalAccessException ,
              InvocationTargetException ,
              ClassCastException                       {

        if( argsClassNames == null )
          return __cellGlue._newInstance(
                     className , cellName , args , false ) ;
        else
          return __cellGlue._newInstance(
                     className , cellName, argsClassNames , args  , false ) ;
   }
   public Cell createNewCell( String cellClass ,
                              String cellName  ,
                              Socket socket ,
                              boolean systemOnly   )
       throws ClassNotFoundException ,
              NoSuchMethodException ,
              SecurityException ,
              InstantiationException ,
              InvocationTargetException ,
              IllegalAccessException ,
              ClassCastException          {

       Object [] args = new Object[1] ;
       args[0] = socket ;

       return (Cell)__cellGlue._newInstance( cellClass ,
                                             cellName ,
                                             args ,
                                             systemOnly       ) ;
   }
   ////////////////////////////////////////////////////////////
   //
   //
   // the routing stuff
   //
   public void routeAdd( CellRoute  route ) throws IllegalArgumentException {
     __cellGlue.routeAdd( route ) ;
   }
   public void routeDelete( CellRoute  route ) throws IllegalArgumentException {
     __cellGlue.routeDelete( route ) ;
   }
   CellRoute routeFind( CellAddressCore addr ){
      return __cellGlue.getRoutingTable().find( addr ) ;
   }
   CellRoutingTable getRoutingTable(){ return __cellGlue.getRoutingTable() ; }
   CellRoute [] getRoutingList(){ return __cellGlue.getRoutingList() ; }
   //
   CellTunnelInfo [] getCellTunnelInfos(){ return __cellGlue.getCellTunnelInfos() ; }
   //

   public static final int  PRINT_CELL          =    1 ;
   public static final int  PRINT_ERROR_CELL    =    2 ;
   public static final int  PRINT_NUCLEUS       =    4 ;
   public static final int  PRINT_ERROR_NUCLEUS =    8 ;
   public static final int  PRINT_FATAL         = 0x10 ;
   public static final int  PRINT_ERRORS        =
        PRINT_ERROR_CELL|PRINT_ERROR_NUCLEUS;
   public static final int  PRINT_EVERYTHING    =
        PRINT_CELL|PRINT_ERROR_CELL|PRINT_NUCLEUS|PRINT_ERROR_NUCLEUS|PRINT_FATAL;


   void loadCellPrinter( String cellPrinterName , Args args ) throws Exception {

       __cellGlue.loadCellPrinter( cellPrinterName ,args  ) ;
   }
   public void say( int level , String str ){
     if( ( (_printoutLevel & level ) > 0 ) || ( ( level & PRINT_FATAL ) != 0 ) )
      __cellGlue.say( _cellName , _cellType , level , str )  ;
   }
   public void say( String str ){
     if((_printoutLevel & PRINT_CELL ) > 0 )
     __cellGlue.say( _cellName , _cellType , PRINT_CELL , str ) ;
     return ;
   }
   public void esay( String str ){
     if((_printoutLevel & PRINT_ERROR_CELL ) > 0)
     __cellGlue.say( _cellName , _cellType , PRINT_ERROR_CELL , str ) ;
     return ;
   }
   public void fsay( String str ){
     __cellGlue.say( _cellName , _cellType , PRINT_FATAL , str ) ;
     return ;
   }
   private void nsay( String str ){
     if((_printoutLevel & PRINT_NUCLEUS ) > 0)
     __cellGlue.say( _cellName , _cellType , PRINT_NUCLEUS , str ) ;
     return ;
   }
   private void nesay( String str ){
     if((_printoutLevel & PRINT_ERROR_NUCLEUS ) > 0)
     __cellGlue.say( _cellName , _cellType , PRINT_ERROR_NUCLEUS , str ) ;
     return ;
   }
   public void esay(Throwable t ){
      StringWriter sw = new StringWriter() ;
      t.printStackTrace( new PrintWriter( sw ) ) ;
      StringTokenizer st = new StringTokenizer( sw.toString() , "\n" ) ;
      while( st.hasMoreTokens() ){
         esay(st.nextToken()) ;
      }
   }
   private void nesay(Throwable t ){
      StringWriter sw = new StringWriter() ;
      t.printStackTrace( new PrintWriter( sw ) ) ;
      StringTokenizer st = new StringTokenizer( sw.toString() , "\n" ) ;
      while( st.hasMoreTokens() ){
         nesay(st.nextToken()) ;
      }
   }

}
