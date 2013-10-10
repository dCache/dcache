package dmg.cells.nucleus ;

import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import dmg.util.CollectionFactory;

/**
  *
  *
  * @author Patrick Fuhrmann
  * @version 0.1, 15 Feb 1998
  */
class CellGlue {
    private final static Logger LOGGER =
            LoggerFactory.getLogger(CellGlue.class);

    private final String    _cellDomainName      ;
    private final ConcurrentMap<String, CellNucleus> _cellList = Maps.newConcurrentMap();
    private final Set<CellNucleus> _killedCells = Collections.newSetFromMap(Maps.<CellNucleus, Boolean>newConcurrentMap());
    private final Map<String,List<CellEventListener>> _cellEventListener =
       CollectionFactory.newConcurrentHashMap();
    private final Map<String, Object> _cellContext =
       CollectionFactory.newConcurrentHashMap();
    private final AtomicInteger       _uniqueCounter       = new AtomicInteger(100) ;
    private CellNucleus          _systemNucleus;
    private ClassLoaderProvider  _classLoader;
    private CellRoutingTable     _routingTable      = new CellRoutingTable() ;
    private ThreadGroup          _masterThreadGroup;

   private ThreadGroup          _killerThreadGroup;
   private final Executor _killerExecutor;
   private final ThreadPoolExecutor _emergencyKillerExecutor;

   CellGlue( String cellDomainName ){

      String cellDomainNameLocal  = cellDomainName ;

      if( ( cellDomainName == null ) || ( cellDomainName.equals("") ) ) {
          cellDomainNameLocal = "*";
      }

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

   void addCell(String name, CellNucleus cell)
        throws IllegalArgumentException
   {
      if (_cellList.putIfAbsent(name, cell) != null) {
          throw new IllegalArgumentException("Name Mismatch ( cell " + name + " exist )");
      }
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
       String type;
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
           }else {
               throw new
                       IllegalArgumentException("Can't determine provider type");
           }
       }else{
           type  = provider.substring( 0 , pos ) ;
           value = provider.substring( pos+1 ) ;
       }
       switch (type) {
       case "dir":
           File file = new File(value);
           if (!file.isDirectory()) {
               throw new
                       IllegalArgumentException("Not a directory : " + value);
           }
           _classLoader.addFileProvider(selection, new File(value));
           break;
       case "cell":
           _classLoader.addCellProvider(selection,
                   _systemNucleus,
                   new CellPath(value));
           break;
       case "system":
           _classLoader.addSystemProvider(selection);
           break;
       case "none":
           _classLoader.removeSystemProvider(selection);
           break;
       default:
           throw new
                   IllegalArgumentException("Provider type not supported : " + type);
       }

   }
   synchronized void export( CellNucleus cell ){

      sendToAll( new CellEvent( cell.getCellName() ,
                                CellEvent.CELL_EXPORTED_EVENT ) ) ;
   }
   private Class<?>  _loadClass( String className ) throws ClassNotFoundException {
       return _classLoader.loadClass( className ) ;
   }
   public Class<?> loadClass( String className ) throws ClassNotFoundException {
       return _classLoader.loadClass( className ) ;
   }

    Cell  _newInstance( String className ,
                         String cellName ,
                         Object [] args  ,
                         boolean   systemOnly    )
       throws ClassNotFoundException ,
              NoSuchMethodException ,
              SecurityException ,
              InstantiationException ,
              InvocationTargetException ,
              IllegalAccessException ,
              ClassCastException
    {
      Class<? extends Cell> newClass;
      if( systemOnly ) {
          newClass = Class.forName(className).asSubclass(Cell.class);
      } else {
          newClass = _loadClass(className).asSubclass(Cell.class);
      }

      Object [] arguments = new Object[args.length+1] ;
      arguments[0] = cellName ;
      System.arraycopy(args, 0, arguments, 1, args.length);
      Class<?>[] argClass  = new Class<?>[arguments.length] ;
      for( int i = 0 ; i < arguments.length ; i++ ) {
          argClass[i] = arguments[i].getClass();
      }

      return  newClass.getConstructor( argClass ).
                       newInstance( arguments ) ;
   }

    Cell  _newInstance( String className ,
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
              ClassCastException
   {
      Class<? extends Cell> newClass;
      if( systemOnly ) {
          newClass = Class.forName(className).asSubclass(Cell.class);
      } else {
          newClass = _loadClass(className).asSubclass(Cell.class);
      }

      Object [] arguments = new Object[args.length+1] ;
      arguments[0] = cellName ;

      System.arraycopy(args, 0, arguments, 1, args.length);

      Class<?>[] argClasses  = new Class<?>[arguments.length] ;

      ClassLoader loader = newClass.getClassLoader() ;
      argClasses[0] = String.class ;
      if( loader == null ){
          for( int i = 1 ; i < argClasses.length ; i++ ) {
              argClasses[i] = Class.forName(argsClassNames[i - 1]);
          }
      }else{
          for( int i = 1 ; i < argClasses.length ; i++ ) {
              argClasses[i] = loader.loadClass(argsClassNames[i - 1]);
          }
      }

       Constructor<? extends Cell> constructor = newClass.getConstructor(argClasses);
       try {
          return constructor.newInstance(arguments) ;
      } catch (InvocationTargetException e) {
           for (Class<?> clazz: constructor.getExceptionTypes()) {
               if (clazz.isAssignableFrom(e.getTargetException().getClass())) {
                   throw e;
               }
           }
           throw Throwables.propagate(e.getTargetException());
       }
   }

    Map<String, Object> getCellContext()
    {
        return _cellContext;
    }

   Object           getCellContext( String str ){
       return _cellContext.get( str ) ;
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

   List<CellTunnelInfo> getCellTunnelInfos()
   {
      List<CellTunnelInfo> v = new ArrayList<>();
      for (CellNucleus cellNucleus: _cellList.values()) {
         Cell c = cellNucleus.getThisCell();
         if (c instanceof CellTunnel) {
            v.add(((CellTunnel) c).getCellTunnelInfo());
         }
      }
      return v;
   }

    List<String> getCellNames()
    {
        return new ArrayList<>(_cellList.keySet());
    }

   int getUnique(){ return _uniqueCounter.incrementAndGet() ; }

   CellInfo getCellInfo(String name) {
       CellNucleus nucleus = getCell(name);
       return (nucleus == null) ? null : nucleus._getCellInfo();
   }

   Thread [] getThreads(String name) {
       CellNucleus nucleus = getCell(name);
       return (nucleus == null) ? null : nucleus.getThreads();
   }

   private void sendToAll( CellEvent event ){
      //
      // inform our event listener
      //

      for( List<CellEventListener>  listners: _cellEventListener.values() ){

         for( CellEventListener hallo : listners ){

            if( hallo == null ){
              LOGGER.trace("event distributor found NULL");
              continue;
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
            }catch( Exception e ){
              LOGGER.info("Error while sending {}: {}", event, e);
            }
         }

      }

   }

   String getCellDomainName(){  return _cellDomainName ; }
   void   kill( CellNucleus nucleus ){
      _kill( nucleus , nucleus , 0 ) ;
   }
   void   kill( CellNucleus sender , String cellName )
          throws IllegalArgumentException {
      CellNucleus nucleus =  _cellList.get( cellName ) ;
      if(  nucleus == null || _killedCells.contains(nucleus)) {
          throw new IllegalArgumentException("Cell Not Found : " + cellName);
      }
      _kill( sender , nucleus , 0 ) ;

   }

   /**
    * Print diagnostic information about a cell's ThreadGroup to
    * stdout.
    */
   void threadGroupList(String cellName)
   {
       CellNucleus nucleus =  _cellList.get(cellName);
       if (nucleus != null) {
           nucleus.threadGroupList();
       } else {
           LOGGER.warn("cell {} is not running", cellName);
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
        return _cellList.get(cellName);
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

   synchronized void destroy(CellNucleus nucleus)
   {
       _cellList.remove(nucleus.getCellName());
       _killedCells.remove(nucleus);
       LOGGER.trace("destroy : sendToAll : killed {}", nucleus.getCellName());
       notifyAll();
//
//        CELL_DIED_EVENT moved to _kill. Otherwise
//        we have bouncing message because the WELL_KNOWN_ROUTE
//        is still there but the entry in the ps list is not.
//
//       sendToAll( new CellEvent( name , CellEvent.CELL_DIED_EVENT ) ) ;
   }

    private void _kill(CellNucleus source, final CellNucleus destination, long to)
    {
        String cellToKill = destination.getCellName();
        if (!_killedCells.add(destination)) {
            LOGGER.trace("Cell is being killed: {}", cellToKill);
            return;
        }

        CellPath sourceAddr = new CellPath(source.getCellName(), getCellDomainName());
        final KillEvent killEvent = new KillEvent(sourceAddr, to);
        sendToAll(new CellEvent(cellToKill, CellEvent.CELL_DIED_EVENT));

        Runnable command = new Runnable()
        {
            @Override
            public void run()
            {
                destination.shutdown(killEvent);
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
          transponder = msg.encode();
          transponder.addSourceAddress( nucleus.getThisAddress() ) ;
      }

      if( transponder.getSourcePath().hops() > 30 ){
         LOGGER.error("Hop count exceeds 30, dumping: {}", transponder);
         return ;
      }
      CellPath    destination  = transponder.getDestinationPath() ;
      CellAddressCore destCore = destination.getCurrent() ;
      String      cellName     = destCore.getCellName() ;
      String      domainName;

      LOGGER.trace("sendMessage : {} send to {}", transponder.getUOID(), destination);

      //
      //  if the cellname is an *, ( stream mode only ) we can skip
      //  this address, because it was needed to reach our domain,
      //  which hopefully happened.
      //
      if( ( ! firstSend ) && cellName.equals("*") ){
            LOGGER.trace("sendMessage : * detected ; skipping destination");
            destination.next() ;
            destCore = destination.getCurrent() ;
      }


      //
      // this is the big iteration loop
      //
      for( int iter = 0 ; iter < MAX_ROUTE_LEVELS ; iter ++ ){
         cellName    = destCore.getCellName() ;
         domainName  = destCore.getCellDomainName() ;
         LOGGER.trace("sendMessage : next hop at {}: {}@{}", iter, cellName, domainName);

         //
         //  now we try to find the destination cell in our domain
         //
         CellNucleus destNucleus = _cellList.get( cellName ) ;
         if (destNucleus != null && _killedCells.contains(destNucleus)) {
             destNucleus = null;
         }
         if( domainName.equals( _cellDomainName ) ){
            if( cellName.equals("*") ){
                  LOGGER.trace("sendMessagex : * detected ; skipping destination");
                  destination.next() ;
                  destCore = destination.getCurrent() ;
                  continue ;
            }
            //
            // the domain name was specified ( other then 'local' )
            // and points to our domain.
            //
            if( destNucleus == null ){
               if( firstSend ){
                  throw new
                      NoRouteToCellException(
                           transponder.getUOID(),
                           destination,
                           cellName + "@" + _cellDomainName + " not found");
               }else{
                  sendException( nucleus , transponder , destination , cellName ) ;
                  return ;
               }
            }
            if( iter == 0 ){
               //
               // here we really found the destination cell ( no router )
               //
               destNucleus.addToEventQueue(  new MessageEvent( transponder ) ) ;
            }else{
               //
               // this is a router, so we have to prepare the message for
               // routing
               //
  //             destNucleus.addToEventQueue(  new RoutedMessageEvent( transponder ) ) ;
               transponder.addSourceAddress(
                    new CellAddressCore( "*" , _cellDomainName ) ) ;
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
               if( iter == 0 ){
                  destNucleus.addToEventQueue(  new MessageEvent( transponder ) ) ;
               }else{
                  transponder.addSourceAddress(
                       new CellAddressCore( "*" , _cellDomainName ) ) ;
                  destNucleus.addToEventQueue(  new RoutedMessageEvent( transponder ) ) ;
               }
               return ;
            }else if( iter == MAX_ROUTE_LEVELS ){
               LOGGER.trace("sendMessage : max route iteration reached: {}", destination);
               if( firstSend ){
                  throw new
                       NoRouteToCellException(
                          transponder.getUOID(),
                          destination ,
                          cellName + " not found and routing limit reached");
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
            LOGGER.trace("sendMessage : no route destination for : {}", destCore);
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
         LOGGER.trace("sendMessage : using route : {}", route);
         destCore    = route.getTarget() ;
         if( route.getRouteType() == CellRoute.ALIAS ) {
             destination.replaceCurrent(destCore);
         }
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
            LOGGER.debug("Message from {} could not be delivered because no route to {} is known; the sender will be notified.",
                    msg.getSourcePath(), routeTarget);
            NoRouteToCellException exception =
                 new   NoRouteToCellException(
                              msg.getUOID() ,
                              destination ,
                              "Tunnel cell >"+routeTarget+
                              "< not found at >"+_cellDomainName+"<" ) ;
            CellPath retAddr = msg.getSourcePath().revert();
            CellExceptionMessage ret =
                 new CellExceptionMessage( retAddr , exception )  ;
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
