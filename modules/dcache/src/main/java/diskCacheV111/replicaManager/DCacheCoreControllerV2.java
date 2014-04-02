//   $Id$

package diskCacheV111.replicaManager ;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.MissingResourceException;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.Vector;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import diskCacheV111.pools.PoolCellInfo;
import diskCacheV111.pools.PoolCostInfo;
import diskCacheV111.repository.CacheRepositoryEntryInfo;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.IteratorCookie;
import diskCacheV111.util.PnfsId;
import diskCacheV111.util.SpreadAndWait;
import diskCacheV111.util.UptimeParser;
import diskCacheV111.vehicles.CostModulePoolInfoTable;
import diskCacheV111.vehicles.Message;
import diskCacheV111.vehicles.PnfsAddCacheLocationMessage;
import diskCacheV111.vehicles.PnfsClearCacheLocationMessage;
import diskCacheV111.vehicles.PnfsGetCacheLocationsMessage;
import diskCacheV111.vehicles.PnfsModifyCacheLocationMessage;
import diskCacheV111.vehicles.Pool2PoolTransferMsg;
import diskCacheV111.vehicles.PoolCheckFileMessage;
import diskCacheV111.vehicles.PoolManagerGetPoolListMessage;
import diskCacheV111.vehicles.PoolQueryRepositoryMsg;
import diskCacheV111.vehicles.PoolRemoveFilesMessage;
import diskCacheV111.vehicles.PoolStatusChangedMessage;

import dmg.cells.nucleus.CellAdapter;
import dmg.cells.nucleus.CellEvent;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellNucleus;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.NoRouteToCellException;
import dmg.cells.nucleus.UOID;
import dmg.util.CommandSyntaxException;

import org.dcache.cells.CellStub;
import org.dcache.util.Args;
import org.dcache.vehicles.FileAttributes;
import org.dcache.vehicles.PnfsGetFileAttributes;

import static java.util.concurrent.TimeUnit.MINUTES;

/**
  *  Basic cell for performing central monitoring and
  *  file replica manipulation operations.
  *  Cells, intending to make use of these features must extend this Cell.
  *  In oder to be informed about cache location modifications,
  *  the PnfsManager startup must get an addiational option :
  *  <pre>
  *     create diskCacheV111.cells.PnfsManager2 \
  *               ...                           \
  *             -cmRelay=&lt;RelayCellName&gt;
  *  </pre>
  *  <strong>RelayCellName</strong> is the name of the cell inheriting from this cell.
  *  <p>
  *  The following synchronized methods are supported :
  * <ul>
  * <li><strong>List getPoolList</strong> returns all active pools.
  * <li><strong>List getCacheLocationList</strong> returns all cache locations for a particular file.
  * <li><strong>List getPoolRepository</strong> returns all pnfsId's for a particular pool.
  * </ul>
  * To be informed about cache notification events, the following method must be
  * overwritten.
  * <pre>
  *    public void cacheLocationModified(
  *           PnfsModifyCacheLocationMessage msg , boolean wasAdded ){
  *
  *      PnfsId pnfsId   = msg.getPnfsId() ;
  *      String poolName = msg.getPoolName() ;
  *                 ...
  *    }
  * </pre>
  * Some operation may need some time. We allow to run these operations asynchronously.
  * <pre>
  *     TaskObserver observer = <em>method( arguments )</em> ;
  *
  *       // do something and wait for the method to be done.
  *
  *     synchronized( observer ){
  *        while( observer.isDone() )observer.wait() ;
  *     }
  * </pre>
  * The following methods support this pattern :
  * <ul>
  * <li>replicatePnfsId( PnfsId pnfsId ) ;
  * <li>removeCopy( PnfsId pnfsId ) ;
  * </ul>
  */

abstract public class DCacheCoreControllerV2 extends CellAdapter {
   private final static Logger _log =
       LoggerFactory.getLogger(DCacheCoreControllerV2.class);

   private String      _cellName;
   private Args        _args;
   private CellNucleus _nucleus;
   private static final long _timeout                 = 2 * 60 * 1000L ;
   private static final long _TO_GetPoolRepository    = 2 * 60 * 1000L;
   private static final long _TO_GetPoolTags          = 2 * 60 * 1000L;
   private static final long _TO_MessageQueueUpdate   =     15 * 1000L; // 15 seconds
   private static final long _TO_GetStorageInfo       = _timeout;
   private static final long _TO_GetCacheLocationList = _timeout;

   private boolean     _dcccDebug;

   private final BlockingQueue<CellMessage> _msgFifo ;
   private LinkedList<PnfsAddCacheLocationMessage> _cachedPnfsAddCacheLocationMessage = new LinkedList<>();
   private final CellStub _poolManager;
   private final CellStub _pnfsManager;
   private final CellStub _poolStub;

   private static CostModulePoolInfoTable _costTable;
   private static final Object _costTableLock = new Object();

   protected final Map<String, String> _hostMap = new TreeMap<>();     // Map pool to the host Name
   protected boolean _enableSameHostReplica;

   public void    setEnableSameHostReplica ( boolean d ) { _enableSameHostReplica = d; }
   public boolean getEnableSameHostReplica ( )           { return _enableSameHostReplica; }

   public void    setDebug ( boolean d ) { _dcccDebug = d; }
   public boolean getDebug ( )           { return _dcccDebug; }

   public DCacheCoreControllerV2( String cellName , String args )
   {

      super( cellName , args , false ) ;

      _cellName = cellName ;
      _args     = getArgs() ;
      _nucleus  = getNucleus() ;
      _msgFifo = new LinkedBlockingQueue<>() ;
      _poolManager = new CellStub(this, new CellPath("PoolManager"), 2, MINUTES);
      _pnfsManager = new CellStub(this, new CellPath("PnfsManager"), 2, MINUTES);
      _poolStub = new CellStub(this, null, 2, MINUTES);

      useInterpreter( true ) ;

      new MessageProcessThread();

      _nucleus.export() ;
      _log.info("Starting");

   }

   abstract protected
           List<String> getPoolListResilient ()
           throws Exception;

   // Thread to re-queue messages queue

   private class MessageProcessThread implements Runnable {
     private final String _threadName = "DCacheCoreController-MessageProcessing";

     public MessageProcessThread(){
       _nucleus.newThread( this , _threadName ).start() ;
     }

     @Override
     public void run() {
       _log.info("Thread <" + Thread.currentThread().getName() + "> started");

       boolean done = false;
       while (!done) {
         CellMessage message;
         try {
           message = _msgFifo.take();
         }
         catch (InterruptedException e) {
           done = true;
           continue;
         }

         try {
           processCellMessage(message);
         }
         catch (Throwable ex) {
           _log.warn(Thread.currentThread().getName() + " : " +ex);
         }

       } // - while()
       _log.info("Thread <" + Thread.currentThread().getName() + "> finished");
     }  // - run()
   }


   /**
    *  OVERRIDE CellAdapter.commandArrived()
    *
    *  Comment from original method:
    *  If overwritten this method delivers commands which
    *  produced a syntax error which intereted by the
    *  CommandInterpreter. The original message string
    *  is provides together with a help text offered
    *  by the interpreter.
    *  If not overwritten this helptext is send back to the
    *  caller.
    *
    * @param str is the orginal command string.
    * @param cse is the syntax error exception thrown by the
    *            command interpreter. cse.getHelpText offers
    *            the possible help text.
    * @return the object which is send back to the caller.
    *             If <code>null</code> nothing is send back.
    */
   /*
    * @todo : report "Failed to remove" to reduction task
    */
   @Override
   public Serializable commandArrived(String str, CommandSyntaxException cse) {
       if (str.startsWith("Removed ")) {
           _log.debug("commandArrived (ignored):  cse=[" + cse + "], str = ["+str+"]");
           return null;
       } else if ( str.startsWith("Failed to remove ")
               ||  str.startsWith("Syntax Error")
               ||  str.startsWith("(3) CacheException")
               ||  str.startsWith("diskCacheV111.")
           ) {
           _log.warn("commandArrived (ignored): cse=[" + cse + "], str = ["+str+"]");
           return null;
       }

       _log.debug("commandArrived - call super cse=[" + cse + "], str = ["+str+"]");
       return super.commandArrived(str, cse);
   }

   //
   // task feature
   //
   /**
     * COMMAND HELP for 'task ls'
     */
   public static final String hh_task_ls = " # list pending tasks";
   /**
     *  COMMAND : task ls
     *  displays details of all pending asynchronous tasks.
     */
   public String ac_task_ls( Args args ){
      StringBuilder sb = new StringBuilder() ;
      synchronized( _taskHash ){
          for (Object o : _taskHash.values()) {
              sb.append(o.toString()).append("\n");
          }
      }
      return sb.toString() ;
   }


   public static final String hh_task_remove = "<task_id> # remove task";
   /**
     *  COMMAND : task remove <task_id>
     *  removes asynchronous task
     */
   public String ac_task_remove_$_1( Args args ){
      StringBuilder sb = new StringBuilder() ;
      Iterable<TaskObserver> allTasks;

      String s = args.argv(0);
      if( s.equals("*") ) {
          sb.append("Removed:\n");
          synchronized( _taskHash ){
	      allTasks = new HashSet<>(_taskHash.values());
	  }
          for (TaskObserver task : allTasks) {
              if (task != null) {
                  task.setErrorCode(-2, "Removed by command");
                  sb.append(task.toString()).append("\n");
              }
          }
      } else {
          boolean poolFound = false;
          synchronized( _taskHash ){
              allTasks = new HashSet<>(_taskHash.values());
          }

          for (TaskObserver task : allTasks) {
              if (task != null && !task.isDone()) {
                  if ((task.getType()
                          .equals("Reduction") && ((ReductionObserver) task)
                          .getPool().equals(s))
                          || (task.getType().equals("Replication") &&
                          ((((MoverTask) task).getSrcPool().equals(s))
                                  || ((MoverTask) task).getDstPool().equals(s)))
                          ) {
                      poolFound = true;
                      task.setErrorCode(-2, "Removed by command");
                      sb.append(task.toString()).append("\n");
                  }
              }
          }

          if (!poolFound) {
              Long id = Long.parseLong(args.argv(0));
              TaskObserver task;
              synchronized (_taskHash) {
                  task = _taskHash.get(id);
              }

              if (task == null) {
                  sb.append("task ").append(id).append(" not found");
              } else {
                  task.setErrorCode( -2, "Removed by command");
                  sb.append(task.toString());
              }
          }
      }
      return sb.toString() ;
   }

   private long __taskId = 10000L ;
   private synchronized long __nextTaskId(){ return __taskId++ ; }

   private final HashMap<Long, TaskObserver> _taskHash         = new LinkedHashMap<>() ;
   private final HashMap<UOID, MoverTask> _messageHash      = new HashMap<>() ;
   private final HashMap<String, ReductionObserver> _modificationHash = new HashMap<>() ;
   private P2pObserver _p2p          = new P2pObserver();

   /** Keep track of p2p transfers scheduled by replicaManager
    *  This class in NOT synchronized
    */

   private class P2pObserver {
     // Hashtables to keep p2p transfer counters for each pool
     //   addressed by pool name
     private Hashtable<String,AtomicInteger> _p2pClientCount;
     private Hashtable<String,AtomicInteger> _p2pServerCount;

     synchronized public void reset() {
       _p2pClientCount     = new Hashtable<>();
       _p2pServerCount     = new Hashtable<>();
     }

     public P2pObserver () {
       reset();
     }

     public int getClientCount( String dst ) {
       AtomicInteger clientCount;

       clientCount = _p2pClientCount.get(dst);
       return (clientCount != null) ? clientCount.get() : 0;
     }

     public int getServerCount( String src ) {
       AtomicInteger serverCount;

       serverCount = _p2pServerCount.get(src);
       return (serverCount != null) ? serverCount.get() : 0;
     }

     synchronized public void add(String src, String dst) {
       AtomicInteger clientCount, serverCount;

       serverCount =  _p2pServerCount.get( src );
       if( serverCount == null ) {
         serverCount = new AtomicInteger(1);
         _p2pServerCount.put(src,serverCount);
       } else {
         serverCount.incrementAndGet();
       }

       clientCount =  _p2pClientCount.get( dst );
       if( clientCount == null ) {
         clientCount = new AtomicInteger(1);
         _p2pClientCount.put(dst,clientCount);
       } else {
         clientCount.incrementAndGet();
       }
     }

     synchronized public void remove(String src, String dst) {
       AtomicInteger clients;
       AtomicInteger servers;

       servers  =  _p2pServerCount.get( src );
       if( servers != null ) {
         if( servers.decrementAndGet() <= 0 ) {
             _p2pServerCount.remove(src);
         }
       }

       clients  =  _p2pClientCount.get( dst );
       if( clients != null ) {
         if( clients.decrementAndGet() <= 0 ) {
             _p2pClientCount.remove(dst);
         }
       }
     }
   }

   //
   //  basic task observer. It is base class for
   //  all asynchronous commands.
   //
   public class TaskObserver {

      private long   _id        = __nextTaskId() ;
      protected String _type;
      protected int    _errorCode;
      protected String _errorMsg;
      protected boolean _done;
      protected String  _status   = "Active" ;
      private long   _creationTime;

      public TaskObserver( String type ){
         _type = type ;
         _creationTime = System.currentTimeMillis();
         synchronized( _taskHash ){
            _taskHash.put(_id, this ) ;
         }
      }
      public String toString(){
          StringBuilder sb = new StringBuilder() ;
          sb.append("Id=").append(_id).append(";type=").
             append(_type).append(";status=").append(_status).append(";") ;
          if( _done ){
             sb.append("Rc=") ;
             if( _errorCode == 0 ) {
                 sb.append(0).append(";");
             } else {
                 sb.append("{").append(_errorCode).append(",").
                         append(_errorMsg).append("};");
             }
          }
          return sb.toString();
      }
      /**
        *
	*/
      public void setOk(){
         setErrorCode(0,null);
      }
      /**
        *  Returns the error code of the asynchronous method call.
	*  Is only valid after isDone == true.
	*
	*  @return the error code of the asynchronous call.
	*/
      public int getErrorCode(){  return _errorCode ; }
      /**
        *  Returns the error message of the asynchronous method call.
	*  Is only valid after isDone == true and getErrorCode != 0.
	*
	*  @return the error code of the asynchronous call.
	*/
      public String getErrorMessage(){ return _errorMsg ; }

      public void setErrorCode( int errorCode , String errorString ){
         _errorCode = errorCode ;
         _errorMsg  = errorString ;
         _status    = "done";
         finished();
      }

      public void  finished(){
          _done = true ;
          taskFinished(this); // synchronious callBack

          if (this.getType().equals("Replication")) {
            MoverTask mt = (MoverTask) this;
            mt.moverTaskFinishedHook();
          }

          synchronized( _taskHash ){
              _taskHash.remove( new Long(_id) ) ;
          }

          // Asynchronious notification
          _log.debug("Task finished - notifyAll waiting for _taskHash");
          synchronized( this ){
              notifyAll() ;
          }
      }
      /**
        * Checks if the asynchronous method call has been finished.
	*
	*/
      public boolean isDone(){ return _done ; }
      public String getType(){ return _type ; }
      public long   getId(){   return _id ; }

      public void messageArrived( CellMessage msg ){
        _log.debug( "DCacheCoreController::TaskObserver - CellMessage arrived, " + msg ) ;
      }

       public void messageArrived( Message msg ){
        _log.debug( "DCacheCoreController::TaskObserver - Message arrived, " + msg ) ;
      }

       public void setStatus( String status ){ _status = status ; }

      public long getCreationTime() { return _creationTime; }

   }

   /**
    *  Tear down task by pool name (for Reducer) or
    *  source or destination pool name (replicator)
    */
   protected void  taskTearDownByPoolName( String poolName ){
      Iterable<TaskObserver> allTasks;

      synchronized (_taskHash) {
        allTasks = new HashSet<>(_taskHash.values());
      }

       for (TaskObserver task : allTasks) {
           if (task != null && !task.isDone()) {
               if ((task.getType().equals("Reduction") &&
                       ((ReductionObserver) task).getPool().equals(poolName))
                       || (task.getType().equals("Replication") &&
                       ((((MoverTask) task).getSrcPool().equals(poolName))
                               || ((MoverTask) task).getDstPool()
                               .equals(poolName)))
                       ) {
                   task.setErrorCode(-3, "Task tear down");
               }
           }
       }
   }

   //
   //  remove a copy of this file
   //
   public class ReductionObserver extends TaskObserver  {
       protected PnfsId _pnfsId;
       protected String _poolName;
       protected TaskObserver _oldTask;
       private String _key;
       private boolean _pnfsIdDeleted;

       public ReductionObserver( PnfsId pnfsId , String poolName ) throws Exception {
           super("Reduction");
          _pnfsId   = pnfsId ;
          _poolName = poolName ;
          _key = _pnfsId.getId() + "@" + poolName;
          synchronized( _modificationHash ){
            removeCopy( _pnfsId , _poolName , true ) ;
            _oldTask = _modificationHash.put( _key , this );
            if( _oldTask != null ) // Diagnose illegal situation
            {
                _log.warn("ReductionObserver() internal error: task overriden in the _modificationHash"
                        + ", old task=" + _oldTask);
            }
          }
       }
/* OBSOLETE / UNUSED
       public void messageArrived( CellMessage msg ){
         _log.debug( "DCacheCoreController::ReductionObserver - "
              +"CellMessage arrived (ignored), " + msg ) ;
       };
*/
       @Override
       public void messageArrived( Message reply ){
          if( reply.getReturnCode() == 0 ){
             setOk() ;
          }else{
             setErrorCode( reply.getReturnCode() , reply.getErrorObject().toString());
          }
       }

       public String toString() {
           StringBuilder sb = new StringBuilder();
           sb.append("Id=").append(super.getId()).append(";type=");
           sb.append(super._type).append("( ").append(_pnfsId).append(" ").
                   append(_poolName).append(" )");
           sb.append(";status=").append(super._status).append(";");

           if (super._done) {
               sb.append("Rc=");
               if (super._errorCode == 0) {
                   sb.append(0).append(";");
               } else {
                   sb.append("{").append(super._errorCode).append(",").
                           append(super._errorMsg).append("};");
               }
           } else {
               sb.append("runtime= ").append(UptimeParser.valueOf((System.
                       currentTimeMillis() - super.getCreationTime()) / 1000));
           }
           return sb.toString();
       }


       @Override
       public void  finished(){
         synchronized( _modificationHash ) {
           _modificationHash.remove( _key );
         }
         super.finished();
       }
       public PnfsId getPnfsId() { return _pnfsId; }
       public String getPool()   { return _poolName; }
       public  boolean isPnfsIdDeleted()   { return _pnfsIdDeleted; }
       public  void setPnfsIdDeleted( boolean b)   { _pnfsIdDeleted = b; }
   }
   //
   // creates replica
   //
   public class MoverTask extends TaskObserver {
      private PnfsId _pnfsId;
      private String _srcPool;
      private String _dstPool;
      private boolean _pnfsIdDeleted;

      public MoverTask( PnfsId pnfsId , String source , String destination ){
        super("Replication");
        _pnfsId  = pnfsId;
        _srcPool = source;
        _dstPool = destination;
        _p2p.add(_srcPool,_dstPool);
      }
      protected void moverTaskFinishedHook() {
        _p2p.remove(_srcPool,_dstPool);
      }
      public PnfsId getPnfsId()    { return _pnfsId; }
      public String getSrcPool()   { return _srcPool; }
      public String getDstPool()   { return _dstPool; }
      public  boolean isPnfsIdDeleted()   { return _pnfsIdDeleted; }
      public  void setPnfsIdDeleted( boolean b)   { _pnfsIdDeleted = b; }

      @Override
      public void messageArrived( CellMessage msg ){
          Message reply;

          // @todo : process destination pool error
          if( msg.getMessageObject() instanceof NoRouteToCellException ) {
              setErrorCode(-103,"MoverTask: dmg.cells.nucleus.NoRouteToCellException");
              _log.debug("MoverTask got error NoRouteToCellException");
              return;
          }

          try {
              reply = (Message) msg.getMessageObject();
          } catch (Exception ex) {
              setErrorCode(-101,"MoverTask: exception converting reply message="+ ex.getMessage());
              return;
          }

         if( reply.getReturnCode() == 0 ) {
             setOk();
         } else{
            setErrorCode( reply.getReturnCode() , reply.getErrorObject().toString() ) ;
            _log.debug("MoverTask got error ReturnCode=" + reply.getReturnCode()
                +", ErrorObject=["+ reply.getReturnCode() +"]"
                +"reply=\n["
                + reply +"]");
         }
      }

      public String toString(){
          StringBuilder sb = new StringBuilder() ;
          sb.append("Id=").append(super.getId()).append(";type=");
          sb.append(super._type).append("( ").append(_pnfsId).append(" ").append(_srcPool).append(" -> ").append(_dstPool).append(" )");
          sb.append(";status=").append(super._status).append(";") ;

          if( super._done ){
             sb.append("Rc=") ;
             if( super._errorCode == 0 ) {
                 sb.append(0).append(";");
             } else {
                 sb.append("{").append(super._errorCode).append(",").
                         append(super._errorMsg).append("};");
             }
          }else{
              sb.append("runtime= ").append( UptimeParser.valueOf( (System.currentTimeMillis() - super.getCreationTime())/1000 ) );
          }
          return sb.toString();
      }
   }

   protected TaskObserver movePnfsId( PnfsId pnfsId , String source , String destination )
      throws Exception {

      FileAttributes fileAttributes = getFileAttributes(pnfsId);

      Collection<String> hash = new HashSet<>(getCacheLocationList( pnfsId , false )) ;

      /* @todo
       * Cross check info from pnfs companion
      */
      if( ! hash.contains(source) ) {
          throw new
                  IllegalStateException("PnfsId " + pnfsId + " not found in " + source);
      }

      if( hash.contains(destination) ) {
          throw new
                  IllegalStateException("PnfsId " + pnfsId + " already found in " + destination);
      }


      Pool2PoolTransferMsg req =
           new Pool2PoolTransferMsg(source, destination, fileAttributes);
      req.setDestinationFileStatus( Pool2PoolTransferMsg.PRECIOUS ) ;

      CellMessage msg = new CellMessage( new CellPath(destination) , req ) ;

      MoverTask task = new MoverTask( pnfsId , source , destination ) ;

      // Don't even think of it ...
      synchronized( _messageHash ) {
        sendMessage(msg);
        _messageHash.put( msg.getUOID() , task ) ;
      }

      /*
      UOID idAfter = msg.getUOID();
      _log.debug("movePnfsId: AFTER sendMessage p2p, msg src=" +source + " dest=" +destination
           +" msg=<"+msg+">");

      _log.debug("CellMessage UOID AFTER =" + idAfter + " pnfsId=" +pnfsId
           +" is firstDest= " + msg.isFirstDestination() );
      */
      return task ;

   }
   private Random _random = new Random(System.currentTimeMillis());
   /**
     * removes a copy from the cache of the specified pnfsId. An exception is thrown if
     * there is only one copy left.
     */

/*
 * OBSOLETE
 * comment out - whether it breaks

   protected TaskObserver removeCopy( PnfsId pnfsId ) throws Exception {

      List sourcePoolList = getCacheLocationList( pnfsId , true ) ;

      if ( sourcePoolList.size() == 0)
        throw new
            IllegalStateException("no pools found for pnfsId=" + pnfsId );

      if( sourcePoolList.size() == 1 )
        throw new
        IllegalArgumentException("Can't reduce to 0 copies, pnfsId=" +pnfsId);

      String source = (String)sourcePoolList.get( _random.nextInt( sourcePoolList.size()));

      return new ReductionObserver( pnfsId , source ) ;
   }
 * end OBSOLETE
*/

   /**
    * removes a copy from the cache of the specified pnfsId.
    * Limits list of available pools reported by PoolManager
    * by the Set of 'writable' pools 'poolList' in argument.
    * An exception is thrown if there is only one copy left.
    */
   protected TaskObserver removeCopy(PnfsId pnfsId, Set<String> writablePools )
       throws Exception {

     List<String> sourcePoolList = getCacheLocationList(pnfsId, false);

     /** @todo
      *  synchronize on writable pools; currently the copy is used.
      */

     sourcePoolList.retainAll( writablePools );

     if ( sourcePoolList.size() == 0 ) {
         throw new
                 IllegalStateException("no deletable replica found for pnfsId=" + pnfsId);
     }

     List<String> confirmedSourcePoolList = confirmCacheLocationList(pnfsId, sourcePoolList);

     //
     if (confirmedSourcePoolList.size() <= 0) {
         _log.debug("pnfsid = " +pnfsId+", writable pools=" + writablePools);
         _log.debug("pnfsid = " +pnfsId+", confirmed pools=" + confirmedSourcePoolList );
         throw new
                 IllegalArgumentException("no deletable 'online' replica found for pnfsId=" + pnfsId );
     }
     if ( confirmedSourcePoolList.size() == 1 ) {
         throw new
                 IllegalArgumentException("Can't reduce to 0 writable ('online') copies, pnfsId=" +
                 pnfsId + " confirmed pool=" + confirmedSourcePoolList);
     }

     String source = confirmedSourcePoolList.get(
        _random.nextInt(confirmedSourcePoolList.size()) );

     return new ReductionObserver(pnfsId, source);
   }

    private String bestDestPool(List<Object> pools, long fileSize, Set<String> srcHosts ) throws Exception {

        double bestCost = 1.0;
        String bestPool;
        PoolCostInfo bestCostInfo = null;
        boolean spaceFound = false;
        boolean qFound = false;

        long total;
        long precious;
        long free;
        long removable;
        long available;
        long used;
        int  qmax, qlength;
        String host;

        synchronized (_costTableLock) {
            getCostTable();

            if ( _costTable == null ) {
                throw new IllegalArgumentException( "CostTable is not defined (null pointer)");
            }

            /* Find pool with minimum used space
             * 'cached' space is considered as removable and available
             * used space counted as total-available, and includes space used for current transfers
             * in addition to the precious space.
             */
            for (Object pool : pools) {
                try {
                    String poolName = pool.toString();
                    // Do not do same host replication
                    if (!_enableSameHostReplica && srcHosts != null) {
                        synchronized (_hostMap) {
                            host = _hostMap.get(poolName);
                            if (host != null && !host.equals("")
                                    && (srcHosts.contains(host))) {
                                _log.debug("best pool: skip destination pool " + poolName + ", destination host " + host + " is on the source host list " + srcHosts);
                                continue;
                            }
                        }
                    }

                    PoolCostInfo costInfo = _costTable
                            .getPoolCostInfoByName(poolName);
                    if (costInfo == null) {
                        _log.info("bestPool : can not find costInfo for pool " + poolName + " in _costTable");
                        continue;
                    }
                    total = costInfo.getSpaceInfo().getTotalSpace();
                    precious = costInfo.getSpaceInfo().getPreciousSpace();
                    free = costInfo.getSpaceInfo().getFreeSpace();
                    removable = costInfo.getSpaceInfo().getRemovableSpace();
                    available = free + removable;
                    used = total - available;

                    PoolCostInfo.PoolQueueInfo cq = costInfo
                            .getP2pClientQueue();
                    qmax = cq.getMaxActive();
                    // use q max =1 when max was not set :
                    qmax = (qmax == 0) ? 1 : qmax;
                    qmax = (qmax < 0) ? 0 : qmax;
                    // Get client queue info from cost table - ...
                    // not valid and not updated, can not be used
                    //  qlength    = cq.getActive() + cq.getQueued();

                    // get internal replica manager's p2p client count :
                    qlength = _p2p.getClientCount(poolName);

                    double itCost = (double) used / (double) total;
                    if (free >= fileSize) {
                        spaceFound = true;
                        if (qlength < qmax) {
                            qFound = true;
                            if (itCost < bestCost) {
                                bestCost = itCost;
                                bestCostInfo = costInfo;
                            }
                        }
                    }
                } catch (Exception e) {
                    /** @todo
                     *  WHAT exception ? track it
                     */
                    _log.warn("bestPool : ignore exception " + e);
                    if (_dcccDebug) {
                        _log.debug("Stack dump for ignored exception :");
                        e.printStackTrace();
                    }
                }
            }

            if (bestCostInfo == null) {
              throw new IllegalArgumentException(
                "Try again : Can not find good destination pool - no space is available or p2p client queue is full. "
                +" File size="+fileSize + ", spaceFound=" +spaceFound +", queueAvailable=" + qFound);
            }

            total      = bestCostInfo.getSpaceInfo().getTotalSpace();
            precious   = bestCostInfo.getSpaceInfo().getPreciousSpace() + fileSize;
            free       = bestCostInfo.getSpaceInfo().getFreeSpace() - fileSize;
            removable  = bestCostInfo.getSpaceInfo().getRemovableSpace();

            bestCostInfo.setSpaceUsage(total, free, precious, removable);
//          bestCostInfo.getP2pClientQueue().modifyQueue( +1 );
        }

        bestPool = bestCostInfo.getPoolName();

        _log.debug("best pool: " + bestPool + "; cost = " + bestCost);

        return bestPool;
    }

      /**
       * Creates a new replica for the specified pnfsId.
       * Gets sets of source and destination pools and
       * limits them to the sets of readable and writable pools respectively.
       *
       * Returns an Exception if there is no pool left, not holding a copy of
       * this file.
       */
      protected static final String selectSourcePoolError      = "Select source pool error : ";
      protected static final String selectDestinationPoolError = "Select destination pool error : ";

      protected MoverTask replicatePnfsId( PnfsId pnfsId, Set<String> readablePools, Set<String> writablePools )
          throws Exception {

        if (readablePools.size() == 0) {
            throw new                    // do not change - initial substring is used as signature
                    IllegalArgumentException("replicatePnfsId, argument"
                    + " readablePools.size() == 0 "
                    + " for pnfsId=" + pnfsId);
        }

        if (writablePools.size() == 0) {
            throw new                    // do not change - initial substring is used as signature
                    IllegalArgumentException("replicatePnfsId, argument"
                    + " writablePools.size() == 0 "
                    + " for pnfsId=" + pnfsId);
        }


        // Talk to the pnfs and pool managers:

        // get list of pools where pnfsId is.
        // Second arg, boolean - ask pools to confirm pnfsid - we do not confirm now
        List<String> pnfsidPoolList = getCacheLocationList(pnfsId, false);

        // Get list of all pools
        Set<Object> allPools       = new HashSet<Object>(getPoolListResilient());

        // Get Source pool
        // ---------------
        List<String> sourcePoolList = new Vector<>( pnfsidPoolList );

        if (sourcePoolList.size() == 0) {
            throw new                    // do not change - initial substring is used as signature
                    IllegalArgumentException(selectSourcePoolError
                    + "PnfsManager reported no pools (cacheinfoof) for pnfsId=" + pnfsId);
        }

        sourcePoolList.retainAll( allPools );       // pnfs manager knows about them

        if (sourcePoolList.size() == 0) {
            throw new                    // do not change - initial substring is used as signature
                    IllegalArgumentException(selectSourcePoolError
                    + "there are no resilient pools in the pool list provided by PnfsManager for pnfsId=" + pnfsId);
        }

        /** @todo
         *  synchronize on readable pools; currently the copy is used.
         */

        sourcePoolList.retainAll( readablePools );  // they are readable

        if (sourcePoolList.size() == 0) {
            throw new                    // do not change - initial substring is used as signature
                    IllegalArgumentException(selectSourcePoolError
                    + " replica found in resilient pool(s) but the pool is not in online,drainoff or offline-prepare state. pnfsId=" + pnfsId);
        }

        List<String> confirmedSourcePoolList = confirmCacheLocationList(pnfsId, sourcePoolList);

        //
        if (confirmedSourcePoolList.size() == 0) {
            throw new                    // do not change - initial substring is used as signature
                    IllegalArgumentException(selectSourcePoolError
                    + "pools selectable for read did not confirm they have pnfsId=" + pnfsId);
        }

        String source = confirmedSourcePoolList.get(
                        _random.nextInt(confirmedSourcePoolList.size()) );

        // Get destination pool
        // --------------------

        List<Object> destPools = new Vector<>( allPools );
        destPools.removeAll( pnfsidPoolList ); // get pools without this pnfsid

        synchronized ( writablePools ) {
          destPools.retainAll( writablePools );  // check if it writable
        }

        if (destPools.size() == 0) {
            throw new // do not change - initial substring is used as signature
                    IllegalArgumentException(selectDestinationPoolError
                    + " no pools found in online state and not having listed pnfsId=" + pnfsId);
        }

        FileAttributes fileAttributes = getFileAttributes(pnfsId) ;
        long fileSize  = fileAttributes.getSize();

        // do not use pools on the same host
        Set<String> sourceHosts = new HashSet<>();

          for (Object pool : sourcePoolList) {
              String poolName = pool.toString();
              String host = _hostMap.get(poolName);
              sourceHosts.add(host);
          }

        String destination = bestDestPool(destPools, fileSize, sourceHosts );

        return replicatePnfsId( fileAttributes, source, destination);
      }

   /**
    *  Creates a new cache copy of the specified pnfsId.
    *  Identical to
    *    private TaskObserver replicatePnfsId( PnfsId pnfsId, String source, String destination )
    *  except gets StorageInfo as argument instead of getting it internally
    *    to facilitate implementation of external loop over destination pools.
    *
    */
   private MoverTask replicatePnfsId(FileAttributes attributes, String source,
                                        String destination) throws Exception {

     PnfsId pnfsId = attributes.getPnfsId();
     _log.info("Sending p2p for " + pnfsId + " " + source + " -> " + destination);

     Pool2PoolTransferMsg req =
         new Pool2PoolTransferMsg(source, destination, attributes);
     req.setDestinationFileStatus( Pool2PoolTransferMsg.PRECIOUS ) ;

     CellMessage msg = new CellMessage(new CellPath(destination), req);

     MoverTask task = new MoverTask(pnfsId, source, destination);

     // Don't even think of it ...
     synchronized (_messageHash) {
       sendMessage(msg);
       _messageHash.put(msg.getUOID(), task);
     }
     /*
     _log.debug("movePnfsId: replicatePnfsId AFTER send message, src=" +source + " dest=" +destination
          +" msg=<"+msg+">");

     UOID idAfter = msg.getUOID();
     _log.debug("UOID AFTER =" + idAfter + " pnfsId=" +pnfsId
          +" is firstDest= " + msg.isFirstDestination() );
     */
     return task;
   }

   private void getCostTable() throws CacheException, InterruptedException
   {
       synchronized (_costTableLock) {
           if (_costTable == null ||
               System.currentTimeMillis() > _costTable.getTimestamp() + 240 * 1000) {
               _costTable = _poolManager.sendAndWait("xcm ls", CostModulePoolInfoTable.class);
           }
       }
   }


   public void reportGetFreeSpaceProblem(CellMessage msg) {
     if (msg == null) {
       _log.warn("Request Timed out");
       return;
     }

     Object o = msg.getMessageObject();
     if (o instanceof Exception) {
       _log.warn("GetFreeSpace: got exception" + ( (Exception) o).getMessage());
     }
     else if (o instanceof String) {
       _log.warn("GetFreeSpace: got error '" + o.toString() + "'");
     }
     else {
       _log.warn("GetFreeSpace: Unexpected class arrived : " + o.getClass().getName());
     }
   }


   // methods from the cellEventListener Interface
   //   public void cleanUp() {}
   /*
      public void cellCreated(CellEvent ce) {}

      public void cellDied(CellEvent ce) {}

      public void cellExported(CellEvent ce) {}

      public void routeAdded(CellEvent ce) {}

      public void routeDeleted(CellEvent ce) {}
    */
   @Override
   public void cellCreated(CellEvent ce) {
       super.cellCreated(ce);
       _log.debug("DCCC cellCreated called, ce=" + ce);
   }

   @Override
   public void cellDied(CellEvent ce) {
       super.cellDied(ce);
       _log.debug("DCCC cellDied called, ce=" + ce);
   }

   @Override
   public void cellExported(CellEvent ce) {
       super.cellExported(ce);
       _log.debug("DCCC cellExported called, ce=" + ce);
   }

   @Override
   public void routeAdded(CellEvent ce) {
       super.routeAdded(ce);
       _log.debug("DCCC routeAdded called, ce=" + ce);
   }

   @Override
   public void routeDeleted(CellEvent ce) {
       super.routeDeleted(ce);
       _log.debug("DCCC routeDeleted called, ce=" + ce);
   }
   /** do not overload - it will disable messageArrived( CellMessage m);
   public void messageArrived( MessageEvent msg ){
     _log.debug( "DCacheCoreController: Got Message (ignored): " +msg );
   }
   */
   // end cellEventListener Interface

   @Override
   public void messageArrived(CellMessage msg) {

     _log.debug( "DCacheCoreController: message arrived. Original msg=" +msg );

     boolean expected = preprocessCellMessage( msg ) ;

     if ( expected ) {
       /** @todo process exception
        */
       try {
         _msgFifo.put(msg);
       }
       catch (InterruptedException ex) {
         _log.debug("DCacheCoreController: messageArrived() - ignore InterruptedException");
       }
     }
   }

   protected CellMessage messageQueuePeek() {
     return _msgFifo.peek();
   }

   /**
    * @param msg CellMessage
    * @return boolean - message recognized and needs further processing
    */

   public boolean preprocessCellMessage( CellMessage msg ) {
     Object obj = msg.getMessageObject() ;

     if( obj == null ) {
       _log.debug( "DCacheCoreController: preprocess Cell message null <" +msg +">" ) ;
       return false;
     }

     if ( _dcccDebug && obj instanceof Object[] ) {
       _log.debug( "DCacheCoreController: preprocess Cell message Object[] <" +msg +">" ) ;
       Object[] arr = (Object[]) obj;
       for( int j=0; j<arr.length; j++ ) {
         _log.debug("msg[" +j+ "]='" +  arr[j].toString() +"'" );
       }
       return false;
     }

     boolean taskFound = false;
     boolean msgFound  = true;

     if( obj instanceof PnfsAddCacheLocationMessage ){
       PnfsAddCacheLocationMessage paclm = (PnfsAddCacheLocationMessage)obj;
       _log.debug( "DCacheCoreController: preprocess Cell message PnfsAddCacheLocationMessage <" + paclm +">" ) ;
     }
     else if( obj instanceof PnfsClearCacheLocationMessage ){
       PnfsClearCacheLocationMessage pcclm = (PnfsClearCacheLocationMessage)obj;
       _log.debug( "DCacheCoreController: preprocess Cell message PnfsClearCacheLocationMessage <" +pcclm +">" ) ;
     }
     else if( obj instanceof PoolStatusChangedMessage ){
       PoolStatusChangedMessage pscm = (PoolStatusChangedMessage)obj;
       _log.debug( "DCacheCoreController: preprocess Cell message PoolStatusChangedMessage <" +pscm +">" ) ;
     }
     else if ( obj instanceof PoolRemoveFilesMessage ) {
       PoolRemoveFilesMessage prmf = (PoolRemoveFilesMessage)obj;
       _log.debug( "DCacheCoreController: preprocess Cell message PoolRemoveFilesMessage <" +prmf +">" ) ;
     } else {
       msgFound  = false;

       // Check message has associated task waiting
       /**
       UOID idAfter = msg.getLastUOID();
       _log.debug("UOID CHECK =" + idAfter );
       */
       taskFound = _messageHash.containsKey( msg.getLastUOID() );
     }

     // DEBUG
     //
     if ( obj instanceof Pool2PoolTransferMsg ) {
       Pool2PoolTransferMsg m = (Pool2PoolTransferMsg) obj;
       _log.debug( "DCacheCoreController: preprocess DUMP Cell message Pool2PoolTransferMsg "
             +m + " isReply=" +m.isReply()
             + " id=" +m.getId() ) ;
     }

     /**
      * @todo
      * validate early pool name is on resilient pools list
      */
     _log.debug( "DCacheCoreController: preprocess Cell message. msgFound=" +msgFound
     +" taskFound="+taskFound +" msg uiod O=" + msg.getLastUOID() );

     if ( ! (msgFound || taskFound) )  {
       _log.warn( "DCacheCoreController: preprocess Cell message - ignore unexpected message "
           + msg ) ;
     }

     return (msgFound || taskFound );
   }

   /**
    *
    * @param msg CellMessage
    */

   private void processCellMessage( CellMessage msg ) {

     final int maxAddListSize = 1023; // Max # of accumulated "add" messages
     LinkedList<PnfsAddCacheLocationMessage> l = _cachedPnfsAddCacheLocationMessage;

     Object obj = msg.getMessageObject();

     CellMessage nextMsg = messageQueuePeek();
     Object      nextObj = (nextMsg == null) ? null
         : nextMsg.getMessageObject();

     boolean isPnfsAddCacheLocationMessage = (obj instanceof
                                              PnfsAddCacheLocationMessage);
     boolean nextPnfsAddCacheLocationMessage = (nextObj != null) &&
         (nextObj instanceof PnfsAddCacheLocationMessage);

     _log.debug("DCacheCoreController: process queued CellMessage. Before adding msg="
         + msg + " qsize="+l.size() +" next=" +  nextPnfsAddCacheLocationMessage );

     // Process PnfsAddCacheLocationMessage

     if ( isPnfsAddCacheLocationMessage ) {
         l.add((PnfsAddCacheLocationMessage) obj);
     }

     // Process accumulated "add" messages when
     //   - current message is not "entry added ti the pool"
     //   - there is no next message in the queue
     //   -   or next message is different then "add"
     //   - too many messages accumulated

     if ( ! isPnfsAddCacheLocationMessage
      ||  ! nextPnfsAddCacheLocationMessage
      || ( l.size() >= maxAddListSize ) ) {
       if( l.size() != 0 ) {
         _log.debug("DCacheCoreController: process queued CellMessage. Flush queue qsize="+l.size() );
         processPnfsAddCacheLocationMessage( l );
         l.clear();
       }
     }

     if ( isPnfsAddCacheLocationMessage ) {
         return;
     }

     // end PnfsAddCacheLocationMessage processing


     if( obj instanceof PnfsClearCacheLocationMessage ){
       processPnfsClearCacheLocationMessage( (PnfsClearCacheLocationMessage) obj ) ;
       return ;
     }

     if( obj instanceof PoolStatusChangedMessage ){
       processPoolStatusChangedMessage( (PoolStatusChangedMessage) obj );
       return ;
     }

     if ( obj instanceof PoolRemoveFilesMessage ) {
       processPoolRemoveFiles( (PoolRemoveFilesMessage) obj );
       return ;
     }

     TaskObserver task;

     /** DEBUG
     UOID idAfter = msg.getLastUOID();
     _log.debug("UOID REMOVE =" + idAfter );
     */

     synchronized( _messageHash ){
       task = _messageHash.remove( msg.getLastUOID() );
     }

     if( task != null ) {
       _log.debug( "DCacheCoreController: process CellMessage, task found for UOID=" + msg.getLastUOID() );
       task.messageArrived(msg);
     }
     else{
       _log.warn( "DCacheCoreController: processCellMessage() - ignore message, task not found " +
             "message=["+msg+"]");
     }
   }

   // Placeholder - This method can be overriden
   protected void processPoolStatusChangedMessage( PoolStatusChangedMessage msg ) {
     _log.debug( "DCacheCoreController: default processPoolStatusChangedMessage() called for" + msg ) ;
   }

   // Placeholder - This method can be overriden
   protected void processPoolRemoveFiles( PoolRemoveFilesMessage msg )
   {
     _log.debug( "DCacheCoreController: default processPoolRemoveFilesMessage() called" ) ;
     String poolName     = msg.getPoolName();
     String filesList[]  = msg.getFiles();
     String stringPnfsId;

     if( poolName == null ) {
       _log.debug( "PoolRemoveFilesMessage - no pool defined");
       return;
     }
     if( filesList == null ) {
       _log.debug("PoolRemoveFilesMessage - no file list defined");
       return;
     }
     for( int j=0; j<filesList.length; j++ ){
       if(filesList[j] == null ) {
         _log.debug("DCCC: default PoolRemoveFiles(): file["+j+"]='null' removed from pool "+poolName);
       }else{
         stringPnfsId = filesList[j];
         _log.debug("DCCC: default PoolRemoveFiles(): file["+j+"]=" + stringPnfsId +" removed from pool "+poolName);
       }
     }
   }

   private void processPnfsAddCacheLocationMessage(PnfsAddCacheLocationMessage msg)
   {
     cacheLocationModified(msg, true);
   }

   private void processPnfsAddCacheLocationMessage(List<PnfsAddCacheLocationMessage> ml)
   {
     cacheLocationAdded(ml);
   }

   private void processPnfsClearCacheLocationMessage( PnfsModifyCacheLocationMessage msg )
   {
     String poolName = msg.getPoolName();
     PnfsId pnfsId   = msg.getPnfsId();
     String key      = pnfsId.getId() + "@" + poolName;

     cacheLocationModified(msg, false); // wasAdded=false, replica removed

     synchronized (_modificationHash) {
       ReductionObserver o = _modificationHash.get(key);
       _log.debug("processPnfsClearCacheLocationMessage() : TaskObserver=<" + o +
            ">;msg=[" + msg + "]");
       // Filter out async msgs triggered replica removals by Cleaner
       // for the same pool and different pnfsId
       if (o != null && o.getPnfsId().equals(pnfsId) ) {
           o.messageArrived(msg);
       }
     }
   }

   /**
     *  Obsolete : Called whenever a file cache location changes.
     * <pre>
     *    public void cacheLocationModified(
     *           PnfsModifyCacheLocationMessage msg , boolean wasAdded ){
     *
     *      PnfsId pnfsId   = msg.getPnfsId() ;
     *      String poolName = msg.getPoolName() ;
     *                 ...
     *    }
     * </pre>
     */
    abstract public
        void cacheLocationModified(
          PnfsModifyCacheLocationMessage msg , boolean wasAdded ) ;

    /**
     *  Called whenever a file cache location changes - add message
     * <pre>
     *    public void cacheLocationAdded(
     *           List<PnfsAddCacheLocationMessage> ml ){
     *      Iterate throught list :
     *      msg = ml.next();
     *      PnfsId pnfsId   = msg.getPnfsId() ;
     *      String poolName = msg.getPoolName() ;
     *                 ...
     *    }
     * </pre>
     */
    abstract public
        void cacheLocationAdded( List<PnfsAddCacheLocationMessage> ml );

   /**
     *  Called whenever a Task finished.
     * <pre>
     *    public void taskFinished(
     *           TaskObserver task ){
     *
     *      if( task.isDone() )
     *          String rc   = task.getErrorCode() ;
     *                 ...
     *    }
     * </pre>
     */
    abstract public
        void taskFinished( TaskObserver task );

   /**
     *  Returns the file attributes of the specified pnfsId. Mainly used by
     *  other DCacheCoreController methods.
     *
     *  @param pnfsId pnfsId for which the method should return the cache locations.
     *
     *  @throws MissingResourceException if the PoolManager is not available, times out or
     *          returns an illegal Object.
     *  @throws NoRouteToCellException if the cell environment couldn't find the PnfsManager.
     *  @throws InterruptedException if the method was interrupted.
     *
     */
   protected FileAttributes getFileAttributes(PnfsId pnfsId)
           throws MissingResourceException,
                  NoRouteToCellException,
                  InterruptedException
     {
         try {
             PnfsGetFileAttributes msg = new PnfsGetFileAttributes(pnfsId, Pool2PoolTransferMsg.NEEDED_ATTRIBUTES);
             return _pnfsManager.sendAndWait(msg).getFileAttributes();
         } catch (CacheException e) {
             throw new
                     MissingResourceException(
                     e.getMessage(),
                     "PnfsManager",
                     "PnfsGetFileAttrobutes");
         }
     }

   protected void removeCopy( PnfsId pnfsId , String poolName , boolean force )
           throws Exception {

       CellMessage msg = new CellMessage(
            new CellPath(poolName) ,
            "rep rm "+( force ? " -force " : "" ) + pnfsId ) ;
       //       _log.debug("removeCopy: sendMessage, pool=" + poolName +"pnfsId=" +pnfsId );
       sendMessage( msg ) ;
   }

   /**
     *  Returns a list of pool names where we expect the file to be.
     *  The information is taken from the pnfs database. If the <em>checked</em>
     *  arguments is set 'true' each pool returned is checked first. If the
     *  file couldn't be found in the pool itself, the corresponding pool is removed
     *  from the list. The same is true if the pool doesn't reply or the reply is
     *  somehow illegal.
     *
     *  @param pnfsId pnfsId for which the method should return the cache locations.
     *  @param checked checks if the information return from the pnfs database is still
     *         valid. May take significatly more time.
     *  @throws MissingResourceException if the PoolManager is not available, times out or
     *          returns an illegal Object.
     *  @throws NoRouteToCellException if the cell environment couldn't find the PoolManager.
     *  @throws InterruptedException if the method was interrupted.
     *
     */
   protected List<String> getCacheLocationList(PnfsId pnfsId, boolean checked)
           throws MissingResourceException,
                  NoRouteToCellException,
                  InterruptedException
   {
       PnfsGetCacheLocationsMessage msg = new PnfsGetCacheLocationsMessage(pnfsId);

       try {
           msg = _pnfsManager.sendAndWait(msg);
       } catch (CacheException e) {
           throw new MissingResourceException(
                   e.getMessage(),
                   "PnfsManager",
                   "PnfsGetCacheLocation");
       }

       if( ! checked ) {
           return new ArrayList<>(msg.getCacheLocations());
       }

       Collection<String> assumed   = new HashSet<>( msg.getCacheLocations() ) ;
       HashSet<String> confirmed = new HashSet<>( );

       if ( assumed.size() <= 0 )            // nothing to do
       {
           return new ArrayList<>(confirmed); // return empty List
       }

       SpreadAndWait<PoolCheckFileMessage> controller = new SpreadAndWait<>(new CellStub(this, null, _TO_GetCacheLocationList));

//       _log.debug("getCacheLocationList: SpreadAndWait to " + assumed.size() +" pools");

       for (Object pool : assumed) {
           String poolName = pool.toString();
           PoolCheckFileMessage query = new PoolCheckFileMessage(poolName, pnfsId);
           try {
               controller.send(new CellPath(poolName), PoolCheckFileMessage.class, query);
           } catch (Exception eeee) {
               _log.warn("Problem sending query to " + query
                       .getPoolName() + " " + eeee);
           }
       }
       controller.waitForReplies() ;

       // We may had have problem sending messge to some pools
       // and getting reply from the other,
       // in addition have/'do not have' reply
       // Copy certanly 'confirmed' pools to another map
       // instead of dropping 'not have' pools from the original map

       for (PoolCheckFileMessage reply: controller.getReplies().values()) {
	  _log.trace("getCacheLocationList : PoolCheckFileMessage={}", reply);
          if (reply.getHave()) {
              confirmed.add(reply.getPoolName());
          }
       }

       return new ArrayList<>( confirmed ) ;
   }

   /**
    *  Returns confirmed list of pool names where replica of the file present.
    *
    *  @param pnfsId pnfsId for which the method should return the cache locations.
    *  @throws InterruptedException if the method was interrupted.
    *
    */
   protected List<String> confirmCacheLocationList(PnfsId pnfsId,
                                                   List<String> poolList)
           throws InterruptedException
   {
       Collection<String> assumed   = new HashSet<>(poolList);
       HashSet<String> confirmed = new HashSet<>();

       if (assumed.size() <= 0)             // nothing to do
       {
           return new ArrayList<>(confirmed); // return empty List
       }

       SpreadAndWait<PoolCheckFileMessage> controller = new SpreadAndWait<>(new CellStub(this, null, _TO_GetCacheLocationList));

       for (Object pool : assumed) {
           String poolName = pool.toString();
           PoolCheckFileMessage query = new PoolCheckFileMessage(poolName, pnfsId);

           try {
               controller.send(new CellPath(poolName), PoolCheckFileMessage.class, query);
           } catch (Exception ex) {
               _log.warn("Problem sending query to " + query
                       .getPoolName() + " " + ex);
           }
       }
       controller.waitForReplies();

       // We may had have problem sending messge to some pools
       // and getting reply from the other,
       // in addition have/'do not have' reply
       // Copy certanly 'confirmed' pools to another map
       // instead of dropping 'not have' pools from the original map

       for (PoolCheckFileMessage reply: controller.getReplies().values()) {
	   _log.trace("confirmCacheLocationList : PoolCheckFileMessage={}", reply);
           if (reply.getHave()) {
               confirmed.add(reply.getPoolName());
           }
       }
       return new ArrayList<>(confirmed);
   }

   /**
     *  Returns a list of active pool names (Strings).
     *
     *  @return list of pool names (Strings)
     *  @throws MissingResourceException if the PoolManager is not available, times out or
     *          returns an illegal Object.
     *  @throws InterruptedException if the method was interrupted.
     */
   protected List<String> getPoolList()
           throws MissingResourceException,
                  InterruptedException
   {
       try {
           return _poolManager.sendAndWait(new PoolManagerGetPoolListMessage()).getPoolList();
       } catch (CacheException e) {
           throw new
                   MissingResourceException(
                   e.getMessage(),
                   "PoolManager",
                   "PoolManagerGetPoolListMessage");
       }
   }

   protected List<String> getPoolGroup(String pGroup)
       throws InterruptedException,
       NoRouteToCellException {

     Object[] r;
     try {
       r = _poolManager.sendAndWait("psux ls pgroup " + pGroup, Object[].class);
     } catch (CacheException e) {
       _log.info( "GetPoolGroup: {}", e.getMessage());
       return null;
     }

     if ( r.length != 3 ) {
       _log.info("getPoolGroup: The length of reply=" + r.length +" != 3");
       return null;
     }else{
       String groupName = (String) r[0];
       Object [] poolsArray = (Object []) r[1];
       List<String> poolList = new ArrayList<>();

       _log.debug("Length of the group=" + poolsArray.length );

       for( int j=0; j<poolsArray.length; j++ ) {
         _log.debug("Pool " +j+ " : " + poolsArray[j]);
         poolList.add((String) poolsArray[j]);
       }

       _log.debug("getPoolGroup: Info: '{}' pool group name='{}'", pGroup, groupName );
       if(_log.isDebugEnabled()){
           _log.debug("Pools: {}", Arrays.toString(poolsArray));
       }
       return poolList;
     }
   }

   protected String getPoolHost(String poolName)
           throws InterruptedException, NoRouteToCellException
   {
       PoolCellInfo msg;
       try {
           msg = _poolStub.sendAndWait(new CellPath(poolName), "xgetcellinfo", PoolCellInfo.class);
       } catch (CacheException e) {
           throw new MissingResourceException(
                   e.getMessage(), poolName, "xgetcellinfo");
       }

       Map<String, String> map = msg.getTagMap();

       _log.debug("getHostPool: msgAnswer={}", msg);
       _log.debug("getHostPool: tag map={}", map);

       return (map == null) ? null : map.get("hostname");
   }

   /**
     *  Returns a list of PnfsId's from the specified pool.
     *
     *  @param poolName of the pool from which to obtain the file repository list.
     *  @return list of PnfsId's (Strings)
     *  @throws MissingResourceException if the PoolManager is not available, times out or
     *          returns an illegal Object.
     *  @throws ConcurrentModificationException if the repository changes while
                this list is produced.
     *  @throws NoRouteToCellException if the cell environment couldn't find the PoolManager.
     *  @throws InterruptedException if the method was interrupted.
     */
   protected List<CacheRepositoryEntryInfo> getPoolRepository( String poolName )
          throws MissingResourceException ,
                 ConcurrentModificationException ,
                 NoRouteToCellException ,
                 InterruptedException
   {
       List<CacheRepositoryEntryInfo> list = new ArrayList<>();

       IteratorCookie cookie = new IteratorCookie();
       while (!cookie.done()) {
           PoolQueryRepositoryMsg msg;
           try {
               msg = _poolStub.sendAndWait(new CellPath(poolName),
                                           new PoolQueryRepositoryMsg(poolName, cookie));
           } catch (CacheException e) {
               throw new MissingResourceException( e.getMessage(), poolName, "PoolQueryRepositoryMsg");
           }

           cookie = msg.getCookie();
           if (cookie.invalidated()) {
               throw new ConcurrentModificationException("Pool file list of " + poolName + " was invalidated");
           }

           list.addAll(msg.getInfos());
       }
       return list;
   }

   protected Object sendObject(String poolName, Serializable object)
       throws Exception
   {
       return _poolStub.sendAndWait(new CellPath(poolName), object, Object.class);
   }
}
