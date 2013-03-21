package dmg.cells.nucleus ;

import dmg.util.CollectionFactory;

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
//
// package
//
/**
  *
  *
  * @author Patrick Fuhrmann
  * @version 0.1, 15 Feb 1998
  */
class CellGlue {

   private final String    _cellDomainName      ;
   private final Map<String, CellNucleus> _cellList =
       CollectionFactory.newConcurrentHashMap();
   private final Map<String,List<CellEventListener>> _cellEventListener =
       CollectionFactory.newConcurrentHashMap();
   private final Map<String, CellNucleus> _killedCellList =
       CollectionFactory.newConcurrentHashMap();
   private final Map<String, Object> _cellContext =
       CollectionFactory.newConcurrentHashMap();
   private final AtomicInteger       _uniqueCounter       = new AtomicInteger(100) ;
   public  int       _printoutLevel       = 0 ;
   public  int       _defPrintoutLevel    = CellNucleus.PRINT_ERRORS ;
   private CellNucleus          _systemNucleus     = null ;
   private ClassLoaderProvider  _classLoader       = null ;
   private CellRoutingTable     _routingTable      = new CellRoutingTable() ;
   private ThreadGroup          _masterThreadGroup = null ;
   private ThreadGroup          _killerThreadGroup = null ;
   private final Executor _killerExecutor;
   private final ThreadPoolExecutor _emergencyKillerExecutor;

   private final static Logger _logMessages =
       LoggerFactory.getLogger("logger.org.dcache.cells.messages");
   private final static Logger _logGlue =
       LoggerFactory.getLogger(CellGlue.class);

   CellGlue( String cellDomainName ){

      String cellDomainNameLocal  = cellDomainName ;

      if( ( cellDomainName == null ) || ( cellDomainName.equals("") ) )
    	  cellDomainNameLocal  = "*" ;

      if( cellDomainNameLocal.charAt( cellDomainNameLocal.length() - 1 ) == '*' ){
    	  cellDomainNameLocal =
    		  cellDomainNameLocal.substring(0,cellDomainNameLocal.length())+
             System.currentTimeMillis() ;
      }
      _cellDomainName = cellDomainNameLocal;
      _classLoader       = new ClassLoaderProvider() ;
      _masterThreadGroup = new ThreadGroup( "Master-Thread-Group" ) ;
      _killerThreadGroup = new ThreadGroup( "Killer-Thread-Group" ) ;
      ThreadFactory killerThreadFactory = new ThreadFactory()
      {
          @Override
          public Thread newThread(Runnable r)
          {
              return new Thread(_killerThreadGroup, r);
          }
      };
      _killerExecutor = Executors.newCachedThreadPool(killerThreadFactory);
      _emergencyKillerExecutor = new ThreadPoolExecutor(1, 1,
                      0L, TimeUnit.MILLISECONDS,
                      new LinkedBlockingQueue<Runnable>(),
                      killerThreadFactory);
      _emergencyKillerExecutor.prestartCoreThread();
      new CellUrl( this ) ;
   }
   ThreadGroup getMasterThreadGroup(){return _masterThreadGroup ; }

   synchronized void addCell( String name , CellNucleus cell )
        throws IllegalArgumentException {

      if(  _killedCellList.get( name ) != null )
         throw new IllegalArgumentException( "Name Mismatch ( cell " + name + " exist  )" ) ;
      if(  _cellList.get( name ) != null )
         throw new IllegalArgumentException( "Name Mismatch ( cell " + name + " exist )" ) ;

      _cellList.put( name , cell ) ;

      sendToAll( new CellEvent( name , CellEvent.CELL_CREATED_EVENT ) ) ;
   }

    void setSystemNucleus(CellNucleus nucleus)
    {
        _systemNucleus = nucleus;
    }

    CellNucleus getSystemNucleus()
    {
        return _systemNucleus;
    }

   String [] [] getClassProviders(){ return _classLoader.getProviders() ; }

   void setClassProvider( String selection , String provider ){
       String type  = null ;
       String value = null ;
       int    pos   = provider.indexOf(':') ;
       if( pos < 0 ){
           if( provider.indexOf('/') >= 0 ){
              type  = "dir" ;
              value = provider ;
           }else if( provider.indexOf( '@' ) >= 0 ){
              type  = "cells" ;
              value = provider ;
           }else if( provider.equals( "system" ) ){
              type  = "system" ;
           }else if( provider.equals( "none" ) ){
              type  = "none" ;
           }else
              throw new
              IllegalArgumentException( "Can't determine provider type" ) ;
       }else{
           type  = provider.substring( 0 , pos ) ;
           value = provider.substring( pos+1 ) ;
       }
       if( type.equals( "dir" ) ){
          File file = new File( value ) ;
          if( ! file.isDirectory() )
             throw new
             IllegalArgumentException( "Not a directory : "+value ) ;
          _classLoader.addFileProvider( selection , new File( value ) ) ;
       }else if( type.equals( "cell" ) ){
          _classLoader.addCellProvider( selection ,
                                        _systemNucleus ,
                                        new CellPath( value ) ) ;
       }else if( type.equals( "system" ) ){
          _classLoader.addSystemProvider( selection );
       }else if( type.equals( "none" ) ){
          _classLoader.removeSystemProvider( selection );
       }else
         throw new
        IllegalArgumentException( "Provider type not supported : "+type ) ;

   }
   synchronized void export( CellNucleus cell ){

      sendToAll( new CellEvent( cell.getCellName() ,
                                CellEvent.CELL_EXPORTED_EVENT ) ) ;
   }
   private Class  _loadClass( String className ) throws ClassNotFoundException {
       return _classLoader.loadClass( className ) ;
   }
   public Class loadClass( String className ) throws ClassNotFoundException {
       return _classLoader.loadClass( className ) ;
   }
   Object  _newInstance( String className ,
                         String cellName ,
                         Object [] args  ,
                         boolean   systemOnly    )
       throws ClassNotFoundException ,
              NoSuchMethodException ,
              SecurityException ,
              InstantiationException ,
              InvocationTargetException ,
              IllegalAccessException ,
              ClassCastException                       {

      Class      newClass = null ;
      if( systemOnly )
          newClass =  Class.forName( className ) ;
      else
          newClass = _loadClass( className ) ;

      Object [] arguments = new Object[args.length+1] ;
      arguments[0] = cellName ;
      for( int i = 0 ; i < args.length ; i++ )
         arguments[i+1] = args[i] ;
      Class [] argClass  = new Class[arguments.length] ;
      for( int i = 0 ; i < arguments.length ; i++ )
          argClass[i] = arguments[i].getClass() ;

      return  newClass.getConstructor( argClass ).
                       newInstance( arguments ) ;

   }
   Object  _newInstance( String className ,
                         String cellName ,
                         String [] argsClassNames  ,
                         Object [] args  ,
                         boolean   systemOnly    )
       throws ClassNotFoundException ,
              NoSuchMethodException ,
              SecurityException ,
              InstantiationException ,
              InvocationTargetException ,
              IllegalAccessException ,
              ClassCastException                       {

      Class      newClass = null ;
      if( systemOnly )
          newClass =  Class.forName( className ) ;
      else
          newClass = _loadClass( className ) ;

      Object [] arguments = new Object[args.length+1] ;
      arguments[0] = cellName ;

      for( int i = 0 ; i < args.length ; i++ )
         arguments[i+1] = args[i] ;

      Class [] argClasses  = new Class[arguments.length] ;

      ClassLoader loader = newClass.getClassLoader() ;
      argClasses[0] = java.lang.String.class ;
      if( loader == null ){
          for( int i = 1 ; i < argClasses.length ; i++ )
             argClasses[i] = Class.forName( argsClassNames[i-1] ) ;
      }else{
          for( int i = 1 ; i < argClasses.length ; i++ )
             argClasses[i] = loader.loadClass( argsClassNames[i-1] ) ;
      }

      return  newClass.getConstructor( argClasses ).
                       newInstance( arguments ) ;

   }

    Map<String, Object> getCellContext()
    {
        return _cellContext;
    }

   Object           getCellContext( String str ){
       return _cellContext.get( str ) ;
   }
   CellDomainInfo   getCellDomainInfo(){
     CellDomainInfo info = new CellDomainInfo(_cellDomainName) ;
//     info.setCellDomainName( _cellDomainName ) ;
     return info ;
   }
   public void routeAdd( CellRoute route ){
      _routingTable.add( route ) ;
      sendToAll( new CellEvent( route , CellEvent.CELL_ROUTE_ADDED_EVENT ) ) ;
   }
   public void routeDelete( CellRoute route ){
      _routingTable.delete( route ) ;
      sendToAll( new CellEvent( route , CellEvent.CELL_ROUTE_DELETED_EVENT ) ) ;
   }
   CellRoutingTable getRoutingTable(){ return _routingTable ; }
   CellRoute [] getRoutingList(){ return _routingTable.getRoutingList() ; }
   synchronized CellTunnelInfo [] getCellTunnelInfos(){

      List<CellTunnelInfo> v = new ArrayList<CellTunnelInfo>() ;

      for( CellNucleus cellNucleus : _cellList.values() ){

         Cell c = cellNucleus.getThisCell() ;

         if( c instanceof CellTunnel ){
            v.add( ((CellTunnel)c).getCellTunnelInfo() ) ;
         }
      }

      return v.toArray( new CellTunnelInfo[v.size()] ) ;

   }

    synchronized List<String> getCellNames()
    {
        int size = _cellList.size() + _killedCellList.size();
        List<String> allCells = new ArrayList<String>(size);
        allCells.addAll(_cellList.keySet());
        allCells.addAll(_killedCellList.keySet());
        return allCells;
    }

   int getUnique(){ return _uniqueCounter.incrementAndGet() ; }

   CellInfo getCellInfo( String name ){
      CellNucleus nucleus = _cellList.get( name ) ;
      if( nucleus == null ){
         nucleus = _killedCellList.get( name ) ;
         if( nucleus == null )return null ;
      }
      return nucleus._getCellInfo() ;
   }
   Thread [] getThreads( String name ){
      CellNucleus nucleus = _cellList.get( name ) ;
      if( nucleus == null ){
         nucleus = _killedCellList.get( name ) ;
         if( nucleus == null )return null ;
      }
      return nucleus.getThreads() ;
   }
   private void sendToAll( CellEvent event ){
      //
      // inform our event listener
      //

      for( List<CellEventListener>  listners: _cellEventListener.values() ){

         for( CellEventListener hallo : listners ){

            if( hallo == null ){
              say( "event distributor found NULL" ) ;
              continue ;
            }
            try{
               switch( event.getEventType() ){
                 case CellEvent.CELL_CREATED_EVENT :
                      hallo.cellCreated( event ) ;
                 break ;
                 case CellEvent.CELL_EXPORTED_EVENT :
                      hallo.cellExported( event ) ;
                 break ;
                 case CellEvent.CELL_DIED_EVENT :
                      hallo.cellDied( event ) ;
                 break ;
                 case CellEvent.CELL_ROUTE_ADDED_EVENT :
                      hallo.routeAdded( event ) ;
                 break ;
                 case CellEvent.CELL_ROUTE_DELETED_EVENT :
                      hallo.routeDeleted( event ) ;
                 break ;
               }
            }catch( Exception anye ){
              say( "Exception while sending "+event + " ex : "+anye ) ;
            }
         }

      }

   }

   void setPrintoutLevel( int level ){ _printoutLevel = level ; }
   int  getPrintoutLevel(){ return _printoutLevel ; }
   int  getDefaultPrintoutLevel(){ return _defPrintoutLevel ; }
   void setPrintoutLevel( String cellName , int level ){

      if( cellName.equals("CellGlue") ){
         setPrintoutLevel(level) ;
         return ;
      }else if( cellName.equals("default") ){
         _defPrintoutLevel = level ;
         return ;
      }

      CellNucleus nucleus = _cellList.get( cellName ) ;
      if( nucleus != null )nucleus.setPrintoutLevel( level ) ;
   }
   int getPrintoutLevel( String cellName ){

      if( cellName.equals("CellGlue") )return getPrintoutLevel() ;
      if( cellName.equals("default") )return getDefaultPrintoutLevel() ;
      CellNucleus nucleus =  _cellList.get( cellName ) ;

      if( nucleus != null )return nucleus.getPrintoutLevel() ;

      return -1 ;
   }

   void say(String str){
       if( ( _printoutLevel & CellNucleus.PRINT_NUCLEUS ) != 0 ) {
           _logGlue.warn(str);
       } else {
           _logGlue.info(str);
       }
   }

   void esay( String str ){
       if( ( _printoutLevel & CellNucleus.PRINT_NUCLEUS ) != 0 ) {
           _logGlue.error(str);
       } else {
           _logGlue.info(str);
       }
   }
   String getCellDomainName(){  return _cellDomainName ; }
   void   kill( CellNucleus nucleus ){
      _kill( nucleus , nucleus , 0 ) ;
   }
   void   kill( CellNucleus sender , String cellName )
          throws IllegalArgumentException {
      CellNucleus nucleus =  _cellList.get( cellName ) ;
      if(  nucleus == null )
         throw new IllegalArgumentException( "Cell Not Found : "+cellName  ) ;
      _kill( sender , nucleus , 0 ) ;

   }

   /**
    * Print diagnostic information about a cell's ThreadGroup to
    * stdout.
    */
   void threadGroupList(String cellName)
   {
       CellNucleus nucleus =  _cellList.get(cellName);

       if(nucleus == null ) {
           nucleus = _killedCellList.get(cellName);
       }

       if(nucleus != null) {
           nucleus.threadGroupList();
       } else {
           _logGlue.warn("cell " + cellName + " is not running");
       }
   }

    /**
     * Returns a named cell. This method also returns cells that have
     * been killed, but which are not dead yet.
     *
     * @param cellName the name of the cell
     * @return The cell with the given name or null if there is no such
     * cell.
     */
    CellNucleus getCell(String cellName)
    {
        CellNucleus nucleus = _cellList.get(cellName);
        if (nucleus == null) {
            nucleus = _killedCellList.get(cellName);
        }
        return nucleus;
    }

    /**
     * Blocks until the given cell is dead.
     *
     * @param cellName the name of the cell
     * @param timeout the time to wait in milliseconds. A timeout
     *                of 0 means to wait forever.
     * @throws InterruptedException if another thread interrupted the
     *         current thread before or while the current thread was
     *         waiting for a notification. The interrupted status of
     *         the current thread is cleared when this exception is
     *         thrown.
     * @return True if the cell died, false in case of a timeout.
     */
    synchronized boolean join(String cellName, long timeout) throws InterruptedException
    {
        if (timeout == 0) {
            while (getCell(cellName) != null) {
                wait();
            }
            return true;
        } else {
            while (getCell(cellName) != null && timeout > 0) {
                long time = System.currentTimeMillis();
                wait(timeout);
                timeout = timeout - (System.currentTimeMillis() - time);
            }
            return (timeout > 0);
        }
    }

   synchronized void destroy( CellNucleus nucleus ){
       String name = nucleus.getCellName() ;
       _killedCellList.remove( name ) ;
       say( "destroy : sendToAll : killed"+name ) ;
       notifyAll();
//
//        CELL_DIED_EVENT moved to _kill. Otherwise
//        we have bouncing message because the WELL_KNOWN_ROUTE
//        is still there but the entry in the ps list is not.
//
//       sendToAll( new CellEvent( name , CellEvent.CELL_DIED_EVENT ) ) ;
       return ;
   }

    private synchronized void _kill(CellNucleus source,
                                    CellNucleus destination,
                                    long to)
    {
        CellPath sourceAddr = new CellPath(source.getCellName(),
                                           getCellDomainName());
        final KillEvent killEvent = new KillEvent(sourceAddr, to);
        String cellToKill = destination.getCellName();
        final CellNucleus destNucleus = _cellList.remove(cellToKill);

        if (destNucleus == null) {
            esay("Warning : (name not found in _kill) " + cellToKill);
            return;
        }

        _cellEventListener.remove(cellToKill);
        sendToAll(new CellEvent(cellToKill, CellEvent.CELL_DIED_EVENT));
        _killedCellList.put(cellToKill, destNucleus);

        Runnable command = new Runnable()
        {
            @Override
            public void run()
            {
                destNucleus.shutdown(killEvent);
            }
        };
        try {
            _killerExecutor.execute(command);
        } catch (OutOfMemoryError e) {
            /* This can signal that we cannot create any more threads. The emergency
             * pool has one thread preallocated for this situation.
             */
            _emergencyKillerExecutor.execute(command);
        }
    }

   private static final int MAX_ROUTE_LEVELS  =  16 ;

   void   sendMessage( CellNucleus nucleus , CellMessage msg )
          throws SerializationException,
                 NoRouteToCellException    {

          sendMessage( nucleus , msg , true , true ) ;

   }
   void   sendMessage( CellNucleus nucleus ,
                       CellMessage msg ,
                       boolean     resolveLocally ,
                       boolean     resolveRemotely )
       throws SerializationException,
              NoRouteToCellException    {

      boolean firstSend = ! msg.isStreamMode() ;

      CellMessage transponder = msg ;
      if( firstSend ){
          //
          // this is the original send command
          // - so we have to set the UOID ( sender needs it )
          // - we have to convert the message to stream.
          // - and we have to set our address to find the way back
          //
          transponder = new CellMessage( msg ) ;
          transponder.addSourceAddress( nucleus.getThisAddress() ) ;
      }

      if( transponder.getSourcePath().hops() > 30 ){
         esay( "Hop count exceeds 30, dumping : "+transponder ) ;
         return ;
      }
      CellPath    destination  = transponder.getDestinationPath() ;
      CellAddressCore destCore = destination.getCurrent() ;
      String      cellName     = destCore.getCellName() ;
      String      domainName   = destCore.getCellDomainName();

      say( "sendMessage : "+transponder.getUOID()+" send to "+destination);
      if( _logMessages.isDebugEnabled() ) {

    	  CellMessage messageToSend;

    	  if( transponder.isStreamMode() ) {
    		  messageToSend = new CellMessage(transponder);
    	  }else{
    		  messageToSend = transponder;
    	  }

    	  String messageObject = messageToSend.getMessageObject() == null? "NULL" : messageToSend.getMessageObject().getClass().getName();
    	  _logMessages.debug("glueSendMessage src=" + messageToSend.getSourceAddress() +
  			   " dest=" + messageToSend.getDestinationAddress() + " [" + messageObject + "] UOID=" + messageToSend.getUOID().toString() );
      }
      //
      //  if the cellname is an *, ( stream mode only ) we can skip
      //  this address, because it was needed to reach our domain,
      //  which hopefully happened.
      //
      if( ( ! firstSend ) && cellName.equals("*") ){
            say( "sendMessage : * detected ; skipping destination" );
            destination.next() ;
            destCore = destination.getCurrent() ;
      }


      transponder.isRouted( false ) ;
      //
      // this is the big iteration loop
      //
      for( int iter = 0 ; iter < MAX_ROUTE_LEVELS ; iter ++ ){
         cellName    = destCore.getCellName() ;
         domainName  = destCore.getCellDomainName() ;
         say( "sendMessage : next hop at "+iter+" : "+cellName+"@"+domainName ) ;

         //
         //  now we try to find the destination cell in our domain
         //
         CellNucleus destNucleus = _cellList.get( cellName ) ;
         if( domainName.equals( _cellDomainName ) ){
            if( cellName.equals("*") ){
                  say( "sendMessagex : * detected ; skipping destination" );
                  destination.next() ;
                  destCore = destination.getCurrent() ;
                  continue ;
            }
            //
            // the domain name was specified ( other then 'local' )
            // and points to our domain.
            //
            if( destNucleus == null ){
//               say( "sendMessage : Not found : "+destination ) ;
               if( firstSend ){
                  throw new
                      NoRouteToCellException(
                           transponder.getUOID(),
                           destination,
                           "Initial Send");
               }else{
                  sendException( nucleus , transponder , destination , cellName ) ;
                  return ;
               }
            }
            if( iter == 0 ){
               //
               // here we really found the destination cell ( no router )
               //
//               say( "sendMessage : message "+transponder.getUOID()+
//                    " addToEventQueue of "+cellName ) ;
               destNucleus.addToEventQueue(  new MessageEvent( transponder ) ) ;
            }else{
               //
               // this is a router, so we have to prepare the message for
               // routing
               //
  //             destNucleus.addToEventQueue(  new RoutedMessageEvent( transponder ) ) ;
               transponder.isRouted( true ) ;
               transponder.addSourceAddress(
                    new CellAddressCore( "*" , _cellDomainName ) ) ;
//               say( "sendMessage : message "+transponder.getUOID()+
//                    " forwarded addToEventQueue of "+cellName ) ;
               destNucleus.addToEventQueue(  new RoutedMessageEvent( transponder ) ) ;
            }
            return ;
         }else if( domainName.equals( "local" ) &&
                   ( resolveLocally || ( iter != 0 )  ) ){
            //
            // the domain name was 'local'  AND
            // (  we are assumed to deliver locally ||
            //    we are already in the routing part   )
            //
            if( destNucleus != null ){
//               say( "sendMessage : locally delivered : "+destination ) ;
               if( iter == 0 ){
//                  say( "sendMessage : message "+transponder.getUOID()+
//                       " addToEventQueue of "+cellName ) ;
                  destNucleus.addToEventQueue(  new MessageEvent( transponder ) ) ;
               }else{
                  transponder.isRouted( true ) ;
                  transponder.addSourceAddress(
                       new CellAddressCore( "*" , _cellDomainName ) ) ;
//                  say( "sendMessage : message "+transponder.getUOID()+
//                       " forwarded addToEventQueue of "+cellName ) ;
                  destNucleus.addToEventQueue(  new RoutedMessageEvent( transponder ) ) ;
               }
               return ;
            }else if( iter == MAX_ROUTE_LEVELS ){
               say( "sendMessage : max route iteration reached : "+destination ) ;
               if( firstSend ){
                  throw new
                       NoRouteToCellException(
                          transponder.getUOID(),
                          destination ,
                          "Initial Send");
               }else{
                  sendException( nucleus , transponder , destination ,  cellName ) ;
                  return ;
               }
            }
            //
            // destNuclues == null , is no problem in our case because
            // 'wellknowncells' also use local as keyword.
            //
         }else if( domainName.equals( "local" ) &&
                   ( ! resolveRemotely        ) &&
                   ( iter == 0                )     ){
            //
            // the domain is specified AND
            // we are assumed not to deliver remotely AND
            // we are not yet in the routing part
            //
            throw new
                NoRouteToCellException(
                     transponder.getUOID(),
                     destination,
                     " ! resolve remotely : "+destCore);

         }
         //
         // so, the destination cell wasn't found locally.
         // let's consult the routes
         //
         CellRoute route = _routingTable.find( destCore ) ;
         if( ( route == null ) || ( iter == MAX_ROUTE_LEVELS )){
            say( "sendMessage : no route destination for : "+destCore ) ;
            if( firstSend ){
               throw new
                  NoRouteToCellException(
                     transponder.getUOID(),
                     destination,
                     "Missing routing entry for "+destCore);
            }else{
               sendException( nucleus , transponder , destination , destCore.toString() ) ;
               return ;
            }
         }
         say( "sendMessage : using route : "+route ) ;
         destCore    = route.getTarget() ;
         if( route.getRouteType() == CellRoute.ALIAS )
             destination.replaceCurrent( destCore ) ;
      }
      // end of big iteration loop

   }
   private void sendException( CellNucleus nucleus ,
                               CellMessage msg ,
                               CellPath    destination ,
                               String      routeTarget )
          throws SerializationException,
                 NoRouteToCellException    {
            //
            // here we try to inform the last sender that we are
            // not able to deliver the packet.
            //
            say( "sendMessage : Route target Not found : "+routeTarget ) ;
            NoRouteToCellException exception =
                 new   NoRouteToCellException(
                              msg.getUOID() ,
                              destination ,
                              "Tunnel cell >"+routeTarget+
                              "< not found at >"+_cellDomainName+"<" ) ;
            CellPath retAddr = (CellPath)msg.getSourcePath().clone() ;
            retAddr.revert() ;
            CellExceptionMessage ret =
                 new CellExceptionMessage( retAddr , exception )  ;
            esay( "Sending CellException to "+retAddr ) ;
            ret.setLastUOID( msg.getUOID() ) ;
            sendMessage( nucleus , ret ) ;

   }

    void addCellEventListener(CellNucleus nucleus, CellEventListener listener)
    {
        List<CellEventListener> v;
        if ((v = _cellEventListener.get(nucleus.getCellName())) == null) {
            v = CollectionFactory.newCopyOnWriteArrayList();
            _cellEventListener.put(nucleus.getCellName(), v);
        }
        v.add(listener);
    }

   @Override
   public String toString(){ return _cellDomainName ; }

}
