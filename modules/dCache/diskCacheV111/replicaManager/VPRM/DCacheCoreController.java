//   $Id: DCacheCoreController.java,v 1.19.24.1 2007-10-12 23:35:36 aik Exp $

package diskCacheV111.replicaManager ;

import  diskCacheV111.pools.PoolCostInfo ;
import  diskCacheV111.vehicles.* ;
import  diskCacheV111.util.* ;

import  dmg.cells.nucleus.* ;
import  dmg.util.* ;

import  java.io.* ;
import  java.util.*;

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
abstract public class DCacheCoreController extends CellAdapter {

   private String      _cellName = null ;
   private Args        _args     = null ;
   private CellNucleus _nucleus  = null ;
   private long        _timeout  = 10L * 1000L ;
   private File        _config   = null ;
   private boolean     _dcccDebug = false;

   public void setDebug ( boolean d ) { _dcccDebug = d; }
   public boolean getDebug ( )           { return _dcccDebug; }

   protected void dsay( String s ){
      if(_dcccDebug)
        say("DEBUG: " +s) ;
   }

   private StringBuffer itToSB(Iterator it) {

     StringBuffer sb = new StringBuffer();

     for ( ; it.hasNext(); ) {
       sb.append(it.next().toString()).append(" ");
     }
     return sb;
   }

   public DCacheCoreController( String cellName , String args ) throws Exception {

      super( cellName , args , false ) ;

      _cellName = cellName ;
      _args     = getArgs() ;
      _nucleus  = getNucleus() ;

      String tmp = _args.getOpt("configDirectory") ;
      if( tmp == null )
         throw new
         IllegalArgumentException("'configDirectory' not specified");

      _config = new File(tmp) ;

      if( ! _config.isDirectory() )
         throw new
         IllegalArgumentException("'configDirectory' not a directory");


      useInterpreter( true ) ;

      export() ;
      say("Starting");

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
   public Object commandArrived(String str, CommandSyntaxException cse) {
     if ( str.startsWith("Removed ") ) {
       dsay("commandArrived - 'Removed <pnfsid>' - ignore");
       return null;
     } else if ( str.startsWith( "Failed to remove " ) ) {
       dsay("commandArrived - 'Failed to remove ' - ignore");
       return null;
     }
     dsay("commandArrived - call super");
     return super.commandArrived(str, cse );
   }

   //
   // task feature
   //
   /**
     * COMMAND HELP for 'task ls'
     */
   public String hh_task_ls = " # list pending tasks";
   /**
     *  COMMAND : task ls
     *  displays details of all pending asynchronous tasks.
     */
   public String ac_task_ls( Args args ){
      StringBuffer sb = new StringBuffer() ;
      for( Iterator i = _taskHash.values().iterator() ; i.hasNext() ; ){
         sb.append(i.next().toString()).append("\n");
      }
      return sb.toString() ;
   }
   private long __taskId = 10000L ;
   private synchronized long __nextTaskId(){ return __taskId++ ; }

   private HashMap _taskHash         = new HashMap() ;
   private HashMap _messageHash      = new HashMap() ;
   private HashMap _modificationHash = new HashMap() ;
   //
   //  basic task observer. is the base class for
   //  all asynchronous commands.
   //
   public class TaskObserver {

      private long   _id        = __nextTaskId() ;
      private String _type      = null ;
      private int    _errorCode = 0 ;
      private String _errorMsg  = null;
      private boolean _done     = false ;
      private String  _status   = "Active" ;

      public TaskObserver( String type ){
         _type = type ;
         synchronized( _taskHash ){
            _taskHash.put( new Long(_id) , this ) ;
         }
      }
      public String toString(){
          StringBuffer sb = new StringBuffer() ;
          sb.append("Id=").append(_id).append(";type=").
             append(_type).append(";status=").append(_status).append(";") ;
          if( _done ){
             sb.append("Rc=") ;
             if( _errorCode == 0 )sb.append(0).append(";") ;
             else sb.append("{").append(_errorCode).append(",").
                     append(_errorMsg).append("};");
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
         synchronized( _taskHash ){
            _taskHash.remove( new Long(_id) ) ;
            _done = true ;
         }
         dsay("Task finished - notifyAll waiting for _taskHash");
         taskFinished(this); // callBack

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
        dsay( "DCacheCoreController::TaskObserver - CellMessage arrived, " + msg ) ;
      };
      public void messageArrived( Message msg ){
        dsay( "DCacheCoreController::TaskObserver - Message arrived, " + msg ) ;
      };
      public void setStatus( String status ){ _status = status ; }

   }
   //
   //  remove a copy of this file
   //
   public class ReductionObserver extends TaskObserver  {
       private PnfsId _pnfsId = null ;
       private String _poolName = null ;
       private TaskObserver _oldTask = null;
       private String _key = null;
       public ReductionObserver( PnfsId pnfsId , String poolName ) throws Exception {
           super("Reduction");
          _pnfsId   = pnfsId ;
          _poolName = poolName ;
          _key = _pnfsId.getId() + "@" + poolName;
          synchronized( _modificationHash ){
            removeCopy( _pnfsId , _poolName , true ) ;
            _oldTask = (TaskObserver) _modificationHash.put( _key , this ) ;
            if( _oldTask != null ) // Diagnose illegal situation
              esay("ReductionObserver() internal error: task overriden in the _modificationHash"
                  + ", old task=" +_oldTask );
          }
       }
       public void messageArrived( CellMessage msg ){
         say( "DCacheCoreController::ReductionObserver - "
              +"CellMessage arrived (ignored), " + msg ) ;
       };
       public void messageArrived( Message reply ){
          if( reply.getReturnCode() == 0 ){
             setOk() ;
          }else{
             setErrorCode( reply.getReturnCode() , reply.getErrorObject().toString());
          }
       }
       public void  finished(){
         synchronized( _modificationHash ) {
           _modificationHash.remove( _key );
         }
         super.finished();
       }
       public PnfsId getPnfsId() { return _pnfsId; } ;
       public String getPool()   { return _poolName; } ;
   }
   //
   // creates a replicum.
   //
   public class MoverTask extends TaskObserver {
      private PnfsId _pnfsId  = null ;
      private String _srcPool = null ;
      private String _dstPool = null ;

      public MoverTask( PnfsId pnfsId , String source , String destination ){
        super("Replication");
        _pnfsId  = pnfsId;
        _srcPool = source;
        _dstPool = destination;
      }
      public PnfsId getPnfsId()    { return _pnfsId; } ;
      public String getSrcPool()   { return _srcPool; } ;
      public String getDstPool()   { return _dstPool; } ;

      public void messageArrived( CellMessage msg ){
         Message reply = (Message)msg.getMessageObject() ;
         if( reply.getReturnCode() == 0 )
            setOk() ;
         else{
            setErrorCode( reply.getReturnCode() , reply.getErrorObject().toString() ) ;
            dsay("MoverTask got error ReturnCode=" + reply.getReturnCode()
                +", ErrorObject=["+ reply.getReturnCode() +"]"
                +"reply=\n["
                + reply +"]");
         }
      }
   }
   protected TaskObserver movePnfsId( PnfsId pnfsId , String source , String destination )
      throws Exception {

      StorageInfo storageInfo = getStorageInfo( pnfsId ) ;

      HashSet hash = new HashSet(getCacheLocationList( pnfsId , false )) ;

      if( ! hash.contains(source) )
         throw new
         IllegalStateException("PnfsId "+pnfsId+" not found in "+source ) ;

      if( hash.contains(destination) )
         throw new
         IllegalStateException("PnfsId "+pnfsId+" already found in "+destination ) ;


      Pool2PoolTransferMsg req =
           new Pool2PoolTransferMsg( source , destination ,
                                     pnfsId , storageInfo   ) ;
      req.setDestinationFileStatus( Pool2PoolTransferMsg.PRECIOUS ) ;

      CellMessage msg = new CellMessage( new CellPath(destination) , req ) ;

      MoverTask task = new MoverTask( pnfsId , source , destination ) ;

      synchronized( _messageHash ){
//        dsay("movePnfsId: sendMessage p2p, src=" +source + " dest=" +destination);
        _messageHash.put( msg.getUOID() , task ) ;
         sendMessage( msg ) ;
      }
      return task ;

   }
   private Random _random = new Random(System.currentTimeMillis());
   /**
     * removes a copy from the cache of the specified pnfsId. An exception is thrown if
     * there is only one copy left.
     */
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

   /**
    * removes a copy from the cache of the specified pnfsId.
    * Limits list of available pools reported by PoolManager
    * by the Set of 'writable' pools 'poolList' in argument.
    * An exception is thrown if there is only one copy left.
    */
   protected TaskObserver removeCopy(PnfsId pnfsId, Set writablePools )
       throws Exception {

     List sourcePoolList = getCacheLocationList( pnfsId, true );

     synchronized ( writablePools ) {
       sourcePoolList.retainAll( writablePools );
     }

     if ( sourcePoolList.size() == 0 )
       throw new
           IllegalStateException("no writable copy found for pnfsId=" + pnfsId );

     if ( sourcePoolList.size() == 1 )
       throw new
           IllegalArgumentException("Can't reduce to 0 writable copies, pnfsId=" +
                                    pnfsId);

     String source = (String) sourcePoolList.get(
        _random.nextInt(sourcePoolList.size())
        );

     return new ReductionObserver(pnfsId, source);
   }

   /**
     *  Creates a new cache copy of the specified pnfsId.
     *  Returns an Exception if there is no pool left, not
     *  holding a copy of this file.
     */
   protected TaskObserver replicatePnfsId( PnfsId pnfsId )
      throws Exception {


      List sourcePoolList = getCacheLocationList( pnfsId , true ) ;
      if( sourcePoolList.size() == 0 )
        throw new                   // do not change - initial substring is used as signature
          IllegalArgumentException("No pools found, no source pools found");

      HashSet allPools = new HashSet( getPoolList() ) ;

      for( Iterator i = sourcePoolList.iterator() ; i.hasNext() ; )
          allPools.remove( i.next() ) ;

      if( allPools.size() == 0 )
        throw new                   // do not change - initial substring is used as signature
         IllegalArgumentException( "No pools found, no destination pools left");

      List allPoolList = new ArrayList( allPools ) ;

      String source = (String)sourcePoolList.get( _random.nextInt( sourcePoolList.size() ) ) ;
      String destination = (String)allPoolList.get( _random.nextInt( allPoolList.size() ) ) ;

      return replicatePnfsId( pnfsId, source, destination );
      }

      /**
       * Creates a new replica for the specified pnfsId.
       * Gets sets of source and destination pools and
       * limits them to the sets of readable and writable pools respectively.
       *
       *  Returns an Exception if there is no pool left,
       *  not holding a copy of this file.
       */
      protected TaskObserver replicatePnfsId( PnfsId pnfsId, Set readablePools, Set writablePools )
          throws Exception {

        if (readablePools.size() == 0)
          throw new                    // do not change - initial substring is used as signature
              IllegalArgumentException("replicatePnfsId, argument"
                                       + " readablePools.size() == 0 "
                                       + " for pnfsId=" + pnfsId);

        if (writablePools.size() == 0)
          throw new                    // do not change - initial substring is used as signature
              IllegalArgumentException("replicatePnfsId, argument"
                                       + " writablePools.size() == 0 "
                                       + " for pnfsId=" + pnfsId);


        // Talk to the pnfs and pool managers:

        // get list of pools where pnfsId is.
        // Second arg, boolean - ask pools to confirm pnfsid
        List pnfsidPoolList = getCacheLocationList(pnfsId, true);

        // Get list of all pools
        Set  allPools       = new HashSet(getPoolList());

        // Get Source pool
        // ---------------
        List sourcePoolList = new Vector( pnfsidPoolList );

        if (sourcePoolList.size() == 0)
          throw new                    // do not change - initial substring is used as signature
              IllegalArgumentException("No pools found,"
                +" no source pools found for pnfsId=" + pnfsId );

        sourcePoolList.retainAll( allPools );       // pnfs manager knows about them

        if (sourcePoolList.size() == 0)
          throw new                    // do not change - initial substring is used as signature
              IllegalArgumentException("No pools found,"
                +" no known source pools found for pnfsId=" + pnfsId );

        synchronized( readablePools ) {
          sourcePoolList.retainAll( readablePools );  // they are readable
        }

        if (sourcePoolList.size() == 0)
          throw new                    // do not change - initial substring is used as signature
              IllegalArgumentException("No pools found,"
                +" no readable source pools found for pnfsId=" + pnfsId );

        String source = (String) sourcePoolList.get(
                        _random.nextInt(sourcePoolList.size()) );

        // Get destination pool
        // --------------------

        Vector destPools = new Vector( allPools );
        destPools.removeAll( pnfsidPoolList ); // get pools without this pnfsid

        synchronized ( writablePools ) {
          destPools.retainAll( writablePools );  // check if it writable
        }

        if (destPools.size() == 0)
          throw new // do not change - initial substring is used as signature
              IllegalArgumentException("No pools found,"
                                       +" no writable destination pools found for pnfsId=" + pnfsId );

        StorageInfo storageInfo = getStorageInfo( pnfsId ) ;
        long fileSize  = storageInfo.getFileSize();
        long freeSpace = 0L;

        String destination = null;
        int index;
        do {
          // Choose destination pool
          index       = _random.nextInt( destPools.size());
          destination = (String) destPools.get(index);
          freeSpace   = getFreeSpace(destination);
          if ( freeSpace > 0L && freeSpace >= fileSize )
            break; // pool found - take first place where file will fit
          else
            destPools.remove(index); // get pool out of  the list and try again
        } while ( destPools.size() > 0 );

        if (destPools.size() == 0)
          throw new         // do not change - initial substring is used as signature
              IllegalArgumentException("replicatePnfsId - check Free Space,"
                                       +" no destination pools found with free space available to put pnfsId="
                                       + pnfsId + " size=" + fileSize );

        // start async replication and return observer
        //
        return replicatePnfsId( storageInfo, pnfsId, source, destination);
      }


      /**
        *  Creates a new cache copy of the specified pnfsId.
        */
      private TaskObserver replicatePnfsId( PnfsId pnfsId, String source, String destination )
         throws Exception {

      StorageInfo storageInfo = getStorageInfo( pnfsId ) ;

      say("Sending p2p for "+pnfsId+" "+source+" -> "+destination);

      Pool2PoolTransferMsg req =
           new Pool2PoolTransferMsg( source , destination ,
                                     pnfsId , storageInfo   ) ;
      req.setDestinationFileStatus( Pool2PoolTransferMsg.PRECIOUS ) ;

      CellMessage msg = new CellMessage( new CellPath(destination) , req ) ;

      MoverTask task = new MoverTask( pnfsId , source , destination ) ;

      synchronized( _messageHash ){
//        dsay("replicatePnfsId: sendMessage p2p, src=" +source + " dest=" +destination);
        sendMessage( msg ) ;
        _messageHash.put( msg.getUOID() , task ) ;
      }
      return task ;

   }

   /**
    *  Creates a new cache copy of the specified pnfsId.
    *  Identical to
    *    private TaskObserver replicatePnfsId( PnfsId pnfsId, String source, String destination )
    *  except gets StorageInfo as argument instead of getting it internally
    *    to facilitate implementation of external loop over destination pools.
    *
    */
   private TaskObserver replicatePnfsId(StorageInfo storageInfo,
                                        PnfsId pnfsId, String source,
                                        String destination) throws Exception {

     say("Sending p2p for " + pnfsId + " " + source + " -> " + destination);

     Pool2PoolTransferMsg req =
         new Pool2PoolTransferMsg(source, destination,
                                  pnfsId, storageInfo);
     req.setDestinationFileStatus( Pool2PoolTransferMsg.PRECIOUS ) ;

     CellMessage msg = new CellMessage(new CellPath(destination), req);

     MoverTask task = new MoverTask(pnfsId, source, destination);

     synchronized (_messageHash) {
 //        dsay("replicatePnfsId: sendMessage p2p, src=" +source + " dest=" +destination);
       sendMessage(msg);
       _messageHash.put(msg.getUOID(), task);
     }
     return task;

   }

   private long getFreeSpace(String poolName) throws InterruptedException,
       NoRouteToCellException,
       NotSerializableException {
     long timeout = 10000L;

     CellMessage cellMessage = new CellMessage(
         new CellPath("PoolManager"),
         "xcm ls " + poolName + " -l");
     CellMessage reply = null;

     dsay("getFreeSpace: sendMessage, poolName=" + poolName + "\n"
         + "message=" + cellMessage);

     reply = sendAndWait(cellMessage, timeout);

     dsay("Reply Arrived");

     if (reply == null || ! (reply.getMessageObject()instanceof Object[])) {
       reportGetFreeSpaceProblem(reply);
       return -1L;
     }

     Object[] r = (Object[]) reply.getMessageObject();

     if (r.length != 3) {
       say("getFreeSpace: The length of PoolManager reply=" + r.length + " != 3");
       return -1L;
     }
     else {
       PoolCostInfo info = (PoolCostInfo) r[1];
       long freeSpace = info.getSpaceInfo().getFreeSpace();

       dsay("getFreeSpace info: pool " + poolName + "\tfreeSpace=" + freeSpace + " (" +
            freeSpace / 1024 / 1024 + " MB)");
       return freeSpace;
     }
   }

   public void reportGetFreeSpaceProblem(CellMessage msg) {
     if (msg == null) {
       say("Request Timed out");
       return;
     }

     Object o = msg.getMessageObject();
     if (o instanceof Exception) {
       say("GetFreeSpace: got exception" + ( (Exception) o).getMessage());
     }
     else if (o instanceof String) {
       say("GetFreeSpace: got error '" + o.toString() + "'");
     }
     else {
       say("GetFreeSpace: Unexpected class arrived : " + o.getClass().getName());
     }
   }


   public void say( String str ){
      pin( str ) ;
      super.say( str ) ;
   }

   // methods from the cellEventListener Interface
   public void cleanUp() {}
/*
   public void cellCreated(CellEvent ce) {}

   public void cellDied(CellEvent ce) {}

   public void cellExported(CellEvent ce) {}

   public void routeAdded(CellEvent ce) {}

   public void routeDeleted(CellEvent ce) {}
*/
public void cellCreated( CellEvent ce ) {
  super.cellCreated(ce);
  dsay("DCCC cellCreated called, ce=" +ce);
}
public void cellDied( CellEvent ce ) {
  super.cellDied(ce);
  dsay("DCCC cellDied called, ce=" +ce);
}
public void cellExported( CellEvent ce ) {
  super.cellExported(ce);
  dsay("DCCC cellExported called, ce=" +ce);
}
public void routeAdded( CellEvent ce ) {
  super.routeAdded(ce);
  dsay("DCCC routeAdded called, ce=" +ce);
}
public void routeDeleted( CellEvent ce ) {
  super.routeDeleted(ce);
  dsay("DCCC routeDeleted called, ce=" +ce);
}

 // end cellEventListener Interface

   public void messageArrived( Message msg ){
     say( "DCacheCoreController: Got Message (ignored): " +msg );
   }

   public void messageArrived( CellMessage msg ) {

     Object obj = msg.getMessageObject() ;

     say( "DCacheCoreController: Got CELL message,"  +msg ) ;

     if( obj instanceof PnfsAddCacheLocationMessage ){
       say( "DCacheCoreController: Got PnfsAddCacheLocationMessage" ) ;
       ourCacheLocationModified( (PnfsModifyCacheLocationMessage) obj, true ) ;
       return ;
     }else if( obj instanceof PnfsClearCacheLocationMessage ){
       say( "DCacheCoreController: Got PnfsClearCacheLocationMessage" ) ;
       ourCacheLocationModified( (PnfsModifyCacheLocationMessage) obj, false ) ;
       return ;

     }
/*
     if (obj instanceof PnfsModifyCacheLocationMessage) {

       say("DCacheCoreController: Got PnfsModifyCacheLocationMessage");
       ourCacheLocationModified(
           (PnfsModifyCacheLocationMessage) obj,
           obj instanceof PnfsAddCacheLocationMessage
           );

       return;
     }
*/
     //else
     if( obj instanceof PoolStatusChangedMessage ){
       poolStatusChanged( (PoolStatusChangedMessage) obj );
       return ;
     }else{
       say( "DCacheCoreController: Got CellMessage, remove task from messageHash" + msg );
     }

     synchronized( _messageHash ){
       TaskObserver task = (TaskObserver)_messageHash.remove( msg.getLastUOID() ) ;
       if( task != null )
         task.messageArrived( msg ) ;
       else{
         say( "DCacheCoreController: " +
              "task was not found in the messageHash for the CellMessage (msg ignored) " + msg );
         dsay("ignored message=["+msg+"]");
       }
     }
   }

   protected void poolStatusChanged( PoolStatusChangedMessage msg ) {
     say( "DCacheCoreController: Got PoolStatusChangedMessage, " + msg ) ;
   }

   private void ourCacheLocationModified(
         PnfsModifyCacheLocationMessage msg ,
         boolean wasAdded ){
        PnfsId _pnfsId = null ;
        String _poolName = null ;
        String _key = null;

        cacheLocationModified(msg, wasAdded);

       // Only for PnfsClearCacheLocationMessage

       if (wasAdded == false) {
         _poolName = msg.getPoolName();
         _pnfsId   = msg.getPnfsId();
         _key = _pnfsId.getId() + "@" + _poolName;

         synchronized (_modificationHash) {
           ReductionObserver o = (ReductionObserver) _modificationHash.get(_key);
           dsay("ourCacheLocationModified:: \n"
               + " pool=" + _poolName + " wasAdded=" + wasAdded + "\n"
               + " TaskObserver=[" + o + "]\n"
               + " msg=[" + msg + ";pool=" + _poolName + "]");

           if (o != null){
             // Filter out async msgs which may come from Cleaner,
             // for the same pool and different pnfsId
             PnfsId pnfsId = msg.getPnfsId();
             if (o.getPnfsId().equals( pnfsId ) )
               o.messageArrived(msg);
           }
         }
       }
   }

   /**
     *  Called whenever a file cache location changes.
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
     *  Returns the storage info of the specified pnfsId. Mainly used by
     *  other DCacheCoreController methods.
     *
     *  @param pnfsId pnfsId for which the method should return the cache locations.
     *
     *  @throws MissingResourceException if the PoolManager is not available, times out or
     *          returns an illegal Object.
     *  @throws java.io.NotSerializableException in case of an assertion.
     *  @throws NoRouteToCellException if the cell environment couldn't find the PoolManager.
     *  @throws InterruptedException if the method was interrupted.
     *
     */
   protected StorageInfo getStorageInfo( PnfsId pnfsId )
           throws MissingResourceException,
                  java.io.NotSerializableException ,
                  NoRouteToCellException,
                  InterruptedException                {

       PnfsGetStorageInfoMessage msg = new PnfsGetStorageInfoMessage(pnfsId) ;

       CellMessage cellMessage = new CellMessage( new CellPath( "PnfsManager" ) , msg ) ;
       CellMessage answer = null;

//       dsay("getStorageInfo: sendAndWait, pnfsId=" +pnfsId );
       answer = sendAndWait( cellMessage , _timeout ) ;

       if( answer == null )
          throw new
          MissingResourceException(
            "Timeout "+_timeout ,
             "PnfsManager",
             "PnfsGetStorageInfoMessage" ) ;

       msg = (PnfsGetStorageInfoMessage) answer.getMessageObject() ;
       if( msg.getReturnCode() != 0 )
          throw new
          MissingResourceException(
             msg.getErrorObject().toString() ,
             "PnfsManager",
             "PnfsGetStorageInfoMessage" ) ;

       return msg.getStorageInfo() ;
   }
   protected void removeCopy( PnfsId pnfsId , String poolName , boolean force )
             throws Exception {

       CellMessage msg = new CellMessage(
             new CellPath(poolName) ,
             "rep rm "+( force ? " -force " : "" ) + pnfsId ) ;
       //

//       dsay("removeCopy: sendMessage, pool=" + poolName +"pnfsId=" +pnfsId );
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
     *  @throws java.io.NotSerializableException in case of an assertion.
     *  @throws NoRouteToCellException if the cell environment couldn't find the PoolManager.
     *  @throws InterruptedException if the method was interrupted.
     *
     */
   protected List getCacheLocationList( PnfsId pnfsId , boolean checked )
           throws MissingResourceException,
                  java.io.NotSerializableException ,
                  NoRouteToCellException,
                  InterruptedException                {

       PnfsGetCacheLocationsMessage msg = new PnfsGetCacheLocationsMessage(pnfsId) ;

       CellMessage cellMessage = new CellMessage( new CellPath( "PnfsManager" ) , msg ) ;
       CellMessage answer = null;

//       dsay("getCacheLocationList: sendAndWait, pnfsId=" +pnfsId );
       answer = sendAndWait( cellMessage , _timeout ) ;
       if( answer == null )
          throw new
          MissingResourceException(
            "Timeout "+_timeout ,
             "PnfsManager",
             "PnfsGetCacheLocation" ) ;

       msg = (PnfsGetCacheLocationsMessage) answer.getMessageObject() ;
       if( msg.getReturnCode() != 0 )
          throw new
          MissingResourceException(
             msg.getErrorObject().toString() ,
             "PnfsManager",
             "PnfsGetCacheLocationsMessage" ) ;


       if( ! checked )return new ArrayList( msg.getCacheLocations() );

       HashSet assumed = new HashSet( msg.getCacheLocations() ) ;

       SpreadAndWait controller = new SpreadAndWait( getNucleus() , _timeout ) ;

//       dsay("getCacheLocationList: SpreadAndWait to " + assumed.size() +" pools");

       PoolCheckFileMessage query = null ;
       for( Iterator i = assumed.iterator() ; i.hasNext() ; ){

           String poolName = i.next().toString() ;

           query = new PoolCheckFileMessage( poolName , pnfsId ) ;

           cellMessage = new CellMessage( new CellPath( poolName ) , query ) ;

           try{
              controller.send( cellMessage ) ;
           }catch(Exception eeee ){
              esay("Problem sending query to "+query.getPoolName()+" "+eeee);
           }

       }
       controller.waitForReplies() ;

       for( Iterator i = controller.getReplies() ; i.hasNext() ; ){
          query = (PoolCheckFileMessage) ((CellMessage)i.next()).getMessageObject() ;
          if( ! query.getHave() )assumed.remove( query.getPoolName() ) ;
       }

       return new ArrayList( assumed ) ;
   }
   /**
     *  Returns a list of active pool names (Strings).
     *
     *  @return list of pool names (Strings)
     *  @throws MissingResourceException if the PoolManager is not available, times out or
     *          returns an illegal Object.
     *  @throws java.io.NotSerializableException in case of an assertion.
     *  @throws NoRouteToCellException if the cell environment couldn't find the PoolManager.
     *  @throws InterruptedException if the method was interrupted.
     */
   protected List getPoolList()
           throws MissingResourceException,
                  java.io.NotSerializableException ,
                  NoRouteToCellException,
                  InterruptedException                {

       PoolManagerGetPoolListMessage msg = new PoolManagerGetPoolListMessage() ;

       CellMessage cellMessage = new CellMessage( new CellPath( "PoolManager" ) , msg ) ;
       CellMessage answer = null;

//       dsay("getPoolList: sendAndWait" );
       answer = sendAndWait( cellMessage , _timeout ) ;
       if( answer == null )
          throw new
          MissingResourceException(
            "Timeout : "+_timeout ,
             "PoolManager",
             "PoolManagerGetPoolListMessage" ) ;


       msg = (PoolManagerGetPoolListMessage) answer.getMessageObject() ;
       if( msg.getReturnCode() != 0 )
          throw new
          MissingResourceException(
             msg.getErrorObject().toString() ,
             "PoolManager",
             "PoolManagerGetPoolListMessage" ) ;

       return  msg.getPoolList() ;
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
     *  @throws java.io.NotSerializableException in case of an assertion.
     *  @throws NoRouteToCellException if the cell environment couldn't find the PoolManager.
     *  @throws InterruptedException if the method was interrupted.
     */
   protected List getPoolRepository( String poolName )
          throws MissingResourceException ,
                 ConcurrentModificationException ,
                 java.io.NotSerializableException ,
                 NoRouteToCellException ,
                 InterruptedException                {


       List list = new ArrayList() ;

       for(
            IteratorCookie cookie = new IteratorCookie() ;
            ! cookie.done() ;
          ){

           PoolQueryRepositoryMsg msg = new PoolQueryRepositoryMsg(poolName,cookie) ;

           CellMessage cellMessage = new CellMessage( new CellPath( poolName ) , msg ) ;
           CellMessage answer = null;

//           dsay("getPoolRepository: sendAndWait" );
           answer = sendAndWait( cellMessage , _timeout ) ;

           if( answer == null )
              throw new
              MissingResourceException(
                     "PoolQueryRepositoryMsg timed out" ,
                     poolName ,
                     ""+_timeout ) ;

           msg = (PoolQueryRepositoryMsg) answer.getMessageObject() ;

           cookie = msg.getCookie() ;
           if( cookie.invalidated() )
              throw new
              ConcurrentModificationException("Pool file list of "+poolName+" was invalidated" ) ;

           list.addAll( msg.getPnfsIds() ) ;

       }

       return list ;
   }

   //---------------
   protected Object sendObject(String cellPath, Object object)
       throws Exception {
     return sendObject(new CellPath(cellPath), object);
   }

   protected Object sendObject(CellPath cellPath, Object object)
       throws Exception {
     long timeout = 10000;

     CellMessage res = sendAndWait( new CellMessage(cellPath, object), _timeout);

     if (res == null)
       throw new Exception("Request timed out");

     return res.getMessageObject();
   }

}
