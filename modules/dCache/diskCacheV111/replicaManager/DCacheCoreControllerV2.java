//   $Id: DCacheCoreControllerV2.java,v 1.21 2007-10-04 01:15:55 aik Exp $

package diskCacheV111.replicaManager ;

import  diskCacheV111.pools.PoolCostInfo ;
import  diskCacheV111.pools.PoolCellInfo ;

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

abstract public class DCacheCoreControllerV2 extends CellAdapter {
   private final static String _cvsId = "$Id: DCacheCoreControllerV2.java,v 1.21 2007-10-04 01:15:55 aik Exp $";

   private String      _cellName = null ;
   private Args        _args     = null ;
   private CellNucleus _nucleus  = null ;
   private static final long _timeout                 = 2 * 60 * 1000L ;
   private static final long _TO_GetPoolRepository    = 2 * 60 * 1000L;
   private static final long _TO_GetPoolTags          = 2 * 60 * 1000L;
   private static final long _TO_MessageQueueUpdate   =     15 * 1000L; // 15 seconds
   private static final long _TO_GetStorageInfo       = _timeout;
   private static final long _TO_GetCacheLocationList = _timeout;
   private static final long _TO_GetPoolGroup         = 2*60 * 1000L;
   private static final long _TO_GetPoolList          = 2*60 * 1000L;
   private static final long _TO_GetFreeSpace         = 2*60 * 1000L;
   private static final long _TO_SendObject           = 2*60 * 1000L;

   private File        _config   = null ;
   private boolean     _dcccDebug = false;

   private  static CostModulePoolInfoTable _costTable = null;
   private  static Object _costTableLock = new Object();

   protected Map _hostMap = new TreeMap();     // Map pool to the host Name
   protected boolean _enableSameHostReplica = false;

   public void    setEnableSameHostReplica ( boolean d ) { _enableSameHostReplica = d; }
   public boolean getEnableSameHostReplica ( )           { return _enableSameHostReplica; }

   public void    setDebug ( boolean d ) { _dcccDebug = d; }
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

   public DCacheCoreControllerV2( String cellName , String args ) throws Exception {

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

      new MessageTimeoutThread();

      _nucleus.export() ;
      say("Starting");

   }

   abstract protected
           List getPoolListResilient ()
           throws Exception;

   // Helper thread to expire timeouts in message queue
   // Required for SpreadAndWait( nucleus, timeout ) to operate

   private class MessageTimeoutThread implements Runnable {
     private String _threadName = "DCacheCoreController-MessageTimeout";
       public MessageTimeoutThread(){
           _nucleus.newThread( this , _threadName ).start() ;
       }
       public void run() {
           while (true){
               _nucleus.updateWaitQueue();
               try {
                   Thread.currentThread().sleep( _TO_MessageQueueUpdate );
               } catch (InterruptedException e){
                   say( _threadName + " thread interrupted" ) ;
                   break ;
               }
           }
           say( _threadName + " thread finished" ) ;
       }
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
       if (str.startsWith("Removed ")) {
           dsay("commandArrived (ignored):  cse=[" + cse + "], str = ["+str+"]");
           return null;
       } else if ( str.startsWith("Failed to remove ")
               ||  str.startsWith("Syntax Error")
               ||  str.startsWith("(3) CacheException")
               ||  str.startsWith("diskCacheV111.")
           ) {
           esay("commandArrived (ignored): cse=[" + cse + "], str = ["+str+"]");
           return null;
       }

       dsay("commandArrived - call super cse=[" + cse + "], str = ["+str+"]");
       return super.commandArrived(str, cse);
   }

   public void getInfo(PrintWriter pw) {
    pw.println("       Version : " + _cvsId);
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
      synchronized( _taskHash ){
          for( Iterator i = _taskHash.values().iterator() ; i.hasNext() ; ){
             sb.append(i.next().toString()).append("\n");
          }
      }
      return sb.toString() ;
   }


   public String hh_task_remove = "<task_id> # remove task";
   /**
     *  COMMAND : task remove <task_id>
     *  removes asynchronous task
     */
   public String ac_task_remove_$_1( Args args ){
      StringBuffer sb = new StringBuffer() ;
      HashSet allTasks;

      String s = args.argv(0);
      if( s.equals("*") ) {
          sb.append("Removed:\n");
          synchronized( _taskHash ){
	      allTasks = new HashSet(_taskHash.values());
	  }
	  for( Iterator i = allTasks.iterator() ; i.hasNext() ; ){
	      TaskObserver task = (TaskObserver) i.next();
	      if (task != null) {
		  task.setErrorCode( -2, "Removed by command");
		  sb.append(task.toString()).append("\n");
	      }
	  }
      } else {
          boolean poolFound = false;
          synchronized( _taskHash ){
              allTasks = new HashSet(_taskHash.values());
          }

	  for (Iterator i = allTasks.iterator(); i.hasNext(); ) {
	      TaskObserver task = (TaskObserver) i.next();
	      if (task != null && !task.isDone()) {
		  if ( (task.getType().equals("Reduction") && ((ReductionObserver) task).getPool().equals(s) )
		       || (task.getType().equals("Replication") &&
			   ( (  ((MoverTask) task).getSrcPool().equals(s) )
			     || ((MoverTask) task).getDstPool().equals(s) ) )
		       ) {
		      poolFound = true;
		      task.setErrorCode( -2, "Removed by command");
		      sb.append(task.toString()).append("\n");
		  }
	      }
          }

          if (!poolFound) {
              Long id = new Long(Long.parseLong(args.argv(0)));
              TaskObserver task = null;
              synchronized (_taskHash) {
                  task = (TaskObserver) _taskHash.get(id);
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

   private HashMap _taskHash         = new LinkedHashMap() ;
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
      private long   _creationTime;

      public TaskObserver( String type ){
         _type = type ;
         _creationTime = System.currentTimeMillis();
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
          _done = true ;
          taskFinished(this); // synchronious callBack

          synchronized( _taskHash ){
              _taskHash.remove( new Long(_id) ) ;
          }

          // Asynchronious notification
          dsay("Task finished - notifyAll waiting for _taskHash");
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

      public long getCreationTime() { return _creationTime; }

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
         dsay( "DCacheCoreController::ReductionObserver - "
              +"CellMessage arrived (ignored), " + msg ) ;
       };
       public void messageArrived( Message reply ){
          if( reply.getReturnCode() == 0 ){
             setOk() ;
          }else{
             setErrorCode( reply.getReturnCode() , reply.getErrorObject().toString());
          }
       }

       public String toString() {
           StringBuffer sb = new StringBuffer();
           sb.append("Id=").append(super.getId()).append(";type=");
           sb.append(super._type).append("( ").append(_pnfsId).append(" ").
                   append(_poolName).append(" )");
           sb.append(";status=").append(super._status).append(";");

           if (super._done) {
               sb.append("Rc=");
               if (super._errorCode == 0) sb.append(0).append(";");
               else sb.append("{").append(super._errorCode).append(",").
                       append(super._errorMsg).append("};");
           } else {
               sb.append("runtime= ").append(UptimeParser.valueOf((System.
                       currentTimeMillis() - super.getCreationTime()) / 1000));
           }
           return sb.toString();
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

      public String toString(){
          StringBuffer sb = new StringBuffer() ;
          sb.append("Id=").append(super.getId()).append(";type=");
          sb.append(super._type).append("( ").append(_pnfsId).append(" ").append(_srcPool).append(" -> ").append(_dstPool).append(" )");
          sb.append(";status=").append(super._status).append(";") ;

          if( super._done ){
             sb.append("Rc=") ;
             if( super._errorCode == 0 )sb.append(0).append(";") ;
             else sb.append("{").append(super._errorCode).append(",").
                     append(super._errorMsg).append("};");
          }else{
              sb.append("runtime= ").append( UptimeParser.valueOf( (System.currentTimeMillis() - super.getCreationTime())/1000 ) );
          }
          return sb.toString();
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

     List sourcePoolList = getCacheLocationList( pnfsId, false );

     /** @todo
      *  synchronize on writable pools; currently the copy is used.
      */

     sourcePoolList.retainAll( writablePools );

     if ( sourcePoolList.size() == 0 )
       throw new
           IllegalStateException("no writable copy found for pnfsId=" + pnfsId );

     List confirmedSourcePoolList = confirmCacheLocationList(pnfsId, sourcePoolList);

     //
     if (confirmedSourcePoolList.size() <= 0) {
         dsay("pnfsid = " +pnfsId+", writable pools=" + writablePools);
         dsay("pnfsid = " +pnfsId+", confirmed pools=" + confirmedSourcePoolList );
         throw new
                 IllegalArgumentException("no writable 'online' copy found for pnfsId=" + pnfsId );
     }
     if ( confirmedSourcePoolList.size() == 1 )
       throw new
           IllegalArgumentException("Can't reduce to 0 writable ('online') copies, pnfsId=" +
                                    pnfsId+" confirmed pool=" +confirmedSourcePoolList );

     String source = (String) confirmedSourcePoolList.get(
        _random.nextInt(confirmedSourcePoolList.size()) );

     return new ReductionObserver(pnfsId, source);
   }

   /**
     *  Creates a new cache copy of the specified pnfsId.
     *  Returns an Exception if there is no pool left, not
     *  holding a copy of this file.
     */
    protected TaskObserver replicatePnfsId(PnfsId pnfsId) throws Exception {

        List sourcePoolList = getCacheLocationList(pnfsId, true);
        if (sourcePoolList.size() == 0)
            throw new // do not change - initial substring is used as signature
                    IllegalArgumentException(
                    "No pools found, no source pools found");

        HashSet allPools = new HashSet(getPoolListResilient());

        for (Iterator i = sourcePoolList.iterator(); i.hasNext(); )
            allPools.remove(i.next().toString());

        if (allPools.size() == 0)
            throw new // do not change - initial substring is used as signature
                    IllegalArgumentException(
                    "No pools found, no destination pools left");

        List allPoolList = new ArrayList(allPools);

        StorageInfo storageInfo = getStorageInfo(pnfsId);
        long fileSize = storageInfo.getFileSize();

        String source = (String) sourcePoolList.get(_random.nextInt(
                sourcePoolList.size()));

        //
        // do not use pools on the same host
        Set sourceHosts = new HashSet();

        for (Iterator s = sourcePoolList.iterator(); s.hasNext(); ) {
            String poolName = s.next().toString() ;
            String host = (String)_hostMap.get(poolName);
            sourceHosts.add(host);
        }

//        String destination = bestPool(allPoolList, fileSize);
        String destination = bestPool(allPoolList, fileSize, sourceHosts );

        return replicatePnfsId(pnfsId, source, destination);
    }

    private String bestPool(List pools, long fileSize ) throws Exception {
        return bestPool( pools, fileSize, null );
    }

    private String bestPool(List pools, long fileSize, Set srcHosts ) throws Exception {

        double bestCost = 1.0;
        String bestPool = null;
        PoolCostInfo bestCostInfo = null;

        long total;
        long precious;
        long free;
        long removable;
        long available;
        long used;
        String host;

        synchronized (_costTableLock) {
            getCostTable(this);

            if ( _costTable == null ) {
                throw new IllegalArgumentException( "CostTable is not defined (null pointer)");
            }

            /* Find pool with minimum used space
             * 'cached' space is considered as removable and available
             * used space counted as total-available, and includes space used for current transfers
             * in addition to the precious space.
             */
            Iterator it = pools.iterator();
            while (it.hasNext()) {
                try {
                    String poolName = it.next().toString() ;
                    // Do not do same host replication
                    if ( ! _enableSameHostReplica && srcHosts != null ) {
                        synchronized ( _hostMap ) {
                            host = (String) _hostMap.get( poolName );
                            if( host != null && ! host.equals("")
                                && (srcHosts.contains(host)) ) {
                                 dsay("best pool: skip destination pool " + poolName + ", destination host " + host +" is on the source host list " +srcHosts);
                                continue;
                            }
                        }
                    }

                    PoolCostInfo costInfo = _costTable.getPoolCostInfoByName(poolName);
		    if ( costInfo == null ) {
			say( "bestPool : can not find costInfo for pool " + poolName +" in _costTable");
			continue;
		    }
                    total      = costInfo.getSpaceInfo().getTotalSpace();
                    precious   = costInfo.getSpaceInfo().getPreciousSpace();
                    free       = costInfo.getSpaceInfo().getFreeSpace();
                    removable  = costInfo.getSpaceInfo().getRemovableSpace();
                    available  = free + removable;
                    used       = total - available;

                    double itCost = (double) used / (double) total;
                    if ( (itCost < bestCost)
                         && (free >= fileSize)) {
                        bestCost     = itCost;
                        bestCostInfo = costInfo;
                    }
                }  catch(Exception e)  {
                    /** @todo
                     *  WHAT exception ? track it
                     */
                    say("bestPool : ignore exception " +e);
		    if ( _dcccDebug ) {
			dsay("Stack dump for ignored exception :");
			e.printStackTrace();
		    }
                    continue;
                }
            }

            if (bestCostInfo == null) {
                throw new IllegalArgumentException(
                        "No pools found, can not get destination pool with available space="
                                    +fileSize);
            }

            total      = bestCostInfo.getSpaceInfo().getTotalSpace();
            precious   = bestCostInfo.getSpaceInfo().getPreciousSpace() + fileSize;
            free       = bestCostInfo.getSpaceInfo().getFreeSpace() - fileSize;
            removable  = bestCostInfo.getSpaceInfo().getRemovableSpace();

            bestCostInfo.setSpaceUsage(total, free, precious, removable);
        }

        bestPool = bestCostInfo.getPoolName();

        dsay("best pool: " + bestPool + "; cost = " + bestCost);

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
        // Second arg, boolean - ask pools to confirm pnfsid - we do not confirm now
        List pnfsidPoolList = getCacheLocationList(pnfsId, false );

        // Get list of all pools
        Set  allPools       = new HashSet(getPoolListResilient());

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

        /** @todo
         *  synchronize on readable pools; currently the copy is used.
         */

        sourcePoolList.retainAll( readablePools );  // they are readable

        if (sourcePoolList.size() == 0)
          throw new                    // do not change - initial substring is used as signature
              IllegalArgumentException("No pools found,"
                +" no readable source pools found for pnfsId=" + pnfsId );

        List confirmedSourcePoolList = confirmCacheLocationList(pnfsId, sourcePoolList);

        //
        if (confirmedSourcePoolList.size() == 0)
          throw new                    // do not change - initial substring is used as signature
              IllegalArgumentException("No pools found,"
                +" no readable source pools found confirming pnfsId=" + pnfsId );

        String source = (String) confirmedSourcePoolList.get(
                        _random.nextInt(confirmedSourcePoolList.size()) );

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

        // do not use pools on the same host
        Set sourceHosts = new HashSet();

        for (Iterator s = sourcePoolList.iterator(); s.hasNext(); ) {
            String poolName = s.next().toString() ;
            String host = (String)_hostMap.get(poolName);
            sourceHosts.add( host );
        }
//        String destination = bestPool(destPools, fileSize);
        String destination = bestPool(destPools, fileSize, sourceHosts );

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

     String command = new String( "xcm ls " + poolName + " -l" );

     CellMessage cellMessage = new CellMessage(
         new CellPath("PoolManager"),
         command );
     CellMessage reply = null;

     dsay("getFreeSpace: sendMessage, poolName=" + poolName
          + " command=[" + command+"]\n"
          + "message=" + cellMessage);

     reply = sendAndWait(cellMessage, _TO_GetFreeSpace );

     dsay("get free space reply arrived");

     if (reply == null || ! (reply.getMessageObject()instanceof Object[])) {
       reportGetFreeSpaceProblem(reply);
       return -1L;
     }

     Object[] r = (Object[]) reply.getMessageObject();

     if (r.length != 3) {
       esay("getFreeSpace: The length of PoolManager reply=" + r.length + " != 3");
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

   private void getCostTable(CellAdapter cell)
           throws InterruptedException,
           NoRouteToCellException,
           NotSerializableException {

       synchronized (_costTableLock) {

           if (_costTable == null ||
               System.currentTimeMillis() > _costTable.getTimestamp() + 240 * 1000) {

               String command = new String("xcm ls");

               CellMessage cellMessage = new CellMessage(
                       new CellPath("PoolManager"), command);
               CellMessage reply = null;

               dsay("getCostTable(): sendMessage, " + " command=[" + command +
                    "]\n" + "message=" + cellMessage);

               reply = cell.sendAndWait(cellMessage, _TO_GetFreeSpace);

               dsay("DEBUG: Cost table reply arrived");

               if (reply == null ||
                   !(reply.getMessageObject() instanceof CostModulePoolInfoTable)) {

                   throw new IllegalArgumentException(
                           "received null pointer or wrong object type from PoolManager in getCostTable");
               }

               Object obj = reply.getMessageObject();
               if ( obj == null ) {
                   throw new IllegalArgumentException(
                           "received null pointer from getCostTable from PoolManager");
               } else {
                   _costTable = (CostModulePoolInfoTable) obj;
               }
           }
       }
   }


   public void reportGetFreeSpaceProblem(CellMessage msg) {
     if (msg == null) {
       esay("Request Timed out");
       return;
     }

     Object o = msg.getMessageObject();
     if (o instanceof Exception) {
       esay("GetFreeSpace: got exception" + ( (Exception) o).getMessage());
     }
     else if (o instanceof String) {
       esay("GetFreeSpace: got error '" + o.toString() + "'");
     }
     else {
       esay("GetFreeSpace: Unexpected class arrived : " + o.getClass().getName());
     }
   }


   public void say( String str ){
      pin( str ) ;
      super.say( str ) ;
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
   public void cellCreated(CellEvent ce) {
       super.cellCreated(ce);
       dsay("DCCC cellCreated called, ce=" + ce);
   }

   public void cellDied(CellEvent ce) {
       super.cellDied(ce);
       dsay("DCCC cellDied called, ce=" + ce);
   }

   public void cellExported(CellEvent ce) {
       super.cellExported(ce);
       dsay("DCCC cellExported called, ce=" + ce);
   }

   public void routeAdded(CellEvent ce) {
       super.routeAdded(ce);
       dsay("DCCC routeAdded called, ce=" + ce);
   }

   public void routeDeleted(CellEvent ce) {
       super.routeDeleted(ce);
       dsay("DCCC routeDeleted called, ce=" + ce);
   }

   // end cellEventListener Interface

   public void messageArrived( Message msg ){
     dsay( "DCacheCoreController: Got Message (ignored): " +msg );
   }

   public void messageArrived( CellMessage msg ) {

     Object obj = msg.getMessageObject() ;

     dsay( "DCacheCoreController: Got CELL message,"  +msg ) ;

     if ( _dcccDebug && obj instanceof Object[] ) {
       Object[] arr = (Object[]) obj;
       for( int j=0; j<arr.length; j++ ) {
         dsay("msg[" +j+ "]='" +  arr[j].toString() +"'" );
       }
     }

     if( obj instanceof PnfsAddCacheLocationMessage ){
       dsay( "DCacheCoreController: Got PnfsAddCacheLocationMessage" ) ;
       ourCacheLocationModified( (PnfsModifyCacheLocationMessage) obj, true ) ;
       return ;
     }else if( obj instanceof PnfsClearCacheLocationMessage ){
       dsay( "DCacheCoreController: Got PnfsClearCacheLocationMessage" ) ;
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
       dsay( "DCacheCoreController: Got PoolStatusChangedMessage, " + msg ) ;
       poolStatusChanged( (PoolStatusChangedMessage) obj );
       return ;
     }else if ( obj instanceof PoolRemoveFilesMessage ) {
       dsay( "DCacheCoreController: Got PoolRemoveFilesMessage, " + msg ) ;
       PoolRemoveFiles( (PoolRemoveFilesMessage) obj );
       return ;
     } else{
       say( "DCacheCoreController: Got CellMessage, remove task from messageHash" + msg );
     }

     synchronized( _messageHash ){
       TaskObserver task = (TaskObserver)_messageHash.remove( msg.getLastUOID() ) ;
       if( task != null )
         task.messageArrived( msg ) ;
       else{
         esay( "DCacheCoreController: " +
              "task was not found in the messageHash for the CellMessage (msg ignored) " + msg );
         dsay("ignored message=["+msg+"]");
       }
     }
   }

   // Placeholder - This method can be overriden
   protected void poolStatusChanged( PoolStatusChangedMessage msg ) {
     dsay( "DCacheCoreController: default poolStatusChanged(...) called" ) ;
   }

   // Placeholder - This method can be overriden
   protected void PoolRemoveFiles( PoolRemoveFilesMessage msg )
   {
     dsay( "DCacheCoreController: default PoolRemoveFilesMessage(...) called" ) ;
     String poolName     = msg.getPoolName();
     String filesList[]  = msg.getFiles();
     String stringPnfsId = null;

     if( poolName == null ) {
       dsay( "PoolRemoveFilesMessage - no pool defined");
       return;
     }
     if( filesList == null ) {
       dsay("PoolRemoveFilesMessage - no file list defined");
       return;
     }
     for( int j=0; j<filesList.length; j++ ){
       if(filesList[j] == null ) {
         dsay("DCCC: default PoolRemoveFiles(): file["+j+"]='null' removed from pool "+poolName);
       }else{
         stringPnfsId = filesList[j];
         dsay("DCCC: default PoolRemoveFiles(): file["+j+"]=" + stringPnfsId +" removed from pool "+poolName);
       }
     }
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
       answer = sendAndWait( cellMessage , _TO_GetStorageInfo ) ;

       if( answer == null )
          throw new
          MissingResourceException(
            "Timeout "+ _TO_GetStorageInfo,
             "PnfsManager",
             "PnfsGetStorageInfoMessage" ) ;

       msg = (PnfsGetStorageInfoMessage) answer.getMessageObject() ;
       if( msg.getReturnCode() != 0 ) {
         dsay("getStorageInfo() PnfsGetStorageInfoMessage answer error: err="
              +msg.getReturnCode()
              + ", message='" + msg + "'" );
          throw new
          MissingResourceException(
             msg.getErrorObject().toString() ,
             "PnfsManager",
             "PnfsGetStorageInfoMessage" ) ;
       }

       return msg.getStorageInfo() ;
   }

   protected void removeCopy( PnfsId pnfsId , String poolName , boolean force )
           throws Exception {

       CellMessage msg = new CellMessage(
            new CellPath(poolName) ,
            "rep rm "+( force ? " -force " : "" ) + pnfsId ) ;
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
       answer = sendAndWait( cellMessage , _TO_GetCacheLocationList ) ;
       if( answer == null )
          throw new
          MissingResourceException(
            "Timeout "+ _TO_GetCacheLocationList,
             "PnfsManager",
             "PnfsGetCacheLocation" ) ;

       msg = (PnfsGetCacheLocationsMessage) answer.getMessageObject() ;
       if( msg.getReturnCode() != 0 )
          throw new
          MissingResourceException(
             msg.getErrorObject().toString() ,
             "PnfsManager",
             "PnfsGetCacheLocationsMessage" ) ;

       if( ! checked )
         return new ArrayList( msg.getCacheLocations() );

       HashSet assumed   = new HashSet( msg.getCacheLocations() ) ;
       HashSet confirmed = new HashSet( );

       if ( assumed.size() <= 0 )            // nothing to do
         return new ArrayList( confirmed ) ; // return empty List

       SpreadAndWait controller = new SpreadAndWait( getNucleus() , _TO_GetCacheLocationList ) ;

//       dsay("getCacheLocationList: SpreadAndWait to " + assumed.size() +" pools");

       PoolCheckFileMessage query = null ;
       for( Iterator i = assumed.iterator() ; i.hasNext() ; ){
           String poolName = i.next().toString() ;
           query = new PoolCheckFileMessage( poolName , pnfsId ) ;
           CellMessage cellMessage2Pool = new CellMessage( new CellPath( poolName ) , query ) ;

           try{
              controller.send( cellMessage2Pool ) ;
           }catch(Exception eeee ){
              esay("Problem sending query to "+query.getPoolName()+" "+eeee);
           }
       }
       controller.waitForReplies() ;

       // We may had have problem sending messge to some pools
       // and getting reply from the other,
       // in addition have/'do not have' reply
       // Copy certanly 'confirmed' pools to another map
       // instead of dropping 'not have' pools from the original map

       for( Iterator i = controller.getReplies() ; i.hasNext() ; ){
          query = (PoolCheckFileMessage) ((CellMessage)i.next()).getMessageObject() ;
	  dsay("getCacheLocationList : PoolCheckFileMessage=" +query); // DEBUG pool tags
          if( query.getHave() )
            confirmed.add( query.getPoolName() );
       }

       return new ArrayList( confirmed ) ;
   }

   /**
    *  Returns confirmed list of pool names where replica of the file present.
    *
    *  @param pnfsId pnfsId for which the method should return the cache locations.
    *  @throws InterruptedException if the method was interrupted.
    *
    */
   protected List confirmCacheLocationList( PnfsId pnfsId, List poolList )
           throws InterruptedException
   {
       HashSet assumed   = new HashSet(poolList);
       HashSet confirmed = new HashSet();

       if (assumed.size() <= 0)             // nothing to do
           return new ArrayList(confirmed); // return empty List

       SpreadAndWait controller = new SpreadAndWait(getNucleus(), _TO_GetCacheLocationList);

       PoolCheckFileMessage query = null;
       for (Iterator i = assumed.iterator(); i.hasNext(); ) {
           String poolName = i.next().toString();
           query = new PoolCheckFileMessage(poolName, pnfsId);
           CellMessage cellMessage2Pool = new CellMessage(new CellPath(poolName), query);

           try {
               controller.send(cellMessage2Pool);
           } catch (Exception ex) {
               esay("Problem sending query to " + query.getPoolName() + " " +ex);
           }
       }
       controller.waitForReplies();

       // We may had have problem sending messge to some pools
       // and getting reply from the other,
       // in addition have/'do not have' reply
       // Copy certanly 'confirmed' pools to another map
       // instead of dropping 'not have' pools from the original map

       for (Iterator i = controller.getReplies(); i.hasNext(); ) {
           query = (PoolCheckFileMessage) ((CellMessage) i.next()).
                   getMessageObject();
	   dsay("confirmCacheLocationList : PoolCheckFileMessage=" +query); // DEBUG pool tags
           if (query.getHave())
               confirmed.add(query.getPoolName());
       }
       return new ArrayList(confirmed);
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
       answer = sendAndWait( cellMessage , _TO_GetPoolList ) ;
       if( answer == null )
          throw new
          MissingResourceException(
            "Timeout : " + _TO_GetPoolList,
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

   protected List getPoolGroup ( String pGroup )
       throws InterruptedException,
       NoRouteToCellException,
       NotSerializableException {

     String command = new String( "psux ls pgroup " + pGroup );
     CellMessage cellMessage = new CellMessage(
        new CellPath("PoolManager" ),
        command ) ;
     CellMessage reply = null;

     dsay("getPoolGroup: sendMessage, command=["+command+"]\n"
          + "message=" +cellMessage );

     reply = sendAndWait( cellMessage , _TO_GetPoolGroup ) ;

     if ( reply == null || ! (reply.getMessageObject() instanceof Object [] ) ) {
       reportProblemGPG( reply );
       return null;
     }

     Object [] r = (Object []) reply.getMessageObject();

     if ( r.length != 3 ) {
       say("getPoolGroup: The length of reply=" + r.length +" != 3");
       return null;
     }else{
       String groupName = (String) r[0];
       Object [] poolsArray = (Object []) r[1];
       List poolList = new ArrayList ();

       dsay("Length of the group=" + poolsArray.length );

       for( int j=0; j<poolsArray.length; j++ ) {
         dsay("Pool " +j+ " : " +  (String) poolsArray[j] );
         poolList.add( (String) poolsArray[j] );
       }

       dsay("getPoolGroup: Info: '"+ pGroup +"' pool group name='" + groupName + "'\n"
           + "Pools: " + poolsArray );
       return poolList;
     }
   }

   private void reportProblemGPG( CellMessage msg ) {
     if( msg == null ) {
       say("Request Timed out");
       return;
     }

     Object o = msg.getMessageObject() ;
     if( o instanceof Exception ){
       say( "GetPoolGroup: got exception" +((Exception)o).getMessage() ) ;
     }else if( o instanceof String ){
       say( "GetPoolGroup: got error '" +o.toString() + "'" ) ;
     }else{
       say( "GetPoolGroup: Unexpected class arrived : "+o.getClass().getName() ) ;
     }
   }

//   protected String getPoolTags( String poolName )
   protected String getPoolHost( String poolName )
           throws InterruptedException, NoRouteToCellException,
           NotSerializableException {

       PoolCheckMessage msg = new PoolCheckMessage(poolName);

       msg.setReplyRequired(true);
       CellMessage      cellMessage = new CellMessage( new CellPath(poolName) , "xgetcellinfo" ) ;

       CellMessage answer = null;
       String poolHost = null;

       dsay("getHostPool: send xgetcellinfo message to pool " + poolName );

       answer = sendAndWait( cellMessage , _TO_GetPoolTags ) ;

       if (answer == null)
           throw new MissingResourceException(
                "Timeout : " + _TO_GetPoolTags,  poolName,
                 "xgetcellinfo");

       if ( ! (answer.getMessageObject() instanceof PoolCellInfo) ) {
           throw new IllegalArgumentException ( "getPoolHost() received wrong object type from Pool "
                 + poolName + ", obj=" + answer.getMessageObject() );
       }

       PoolCellInfo msgAnswer = (PoolCellInfo) answer.getMessageObject() ;

       if( msgAnswer.getErrorCode() != 0 )
           throw new
                   MissingResourceException( "getPoolHost(): received error from pool=" +poolName
                   + ", error="  + msgAnswer.getErrorCode() + ", error message='"
                   + msgAnswer.getErrorMessage() +"'",
               " pool ", poolName ) ;

       Map    map = null ;

       poolHost = (String) (
            ( map = msgAnswer.getTagMap() ) == null
              ? null
              : map.get("hostname") ) ;

       dsay("getHostPool: msgAnswer=" + msgAnswer );
       dsay("getHostPool: tag map=" + map );

       return poolHost;
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
           answer = sendAndWait( cellMessage , _TO_GetPoolRepository ) ;

           if( answer == null )
              throw new
              MissingResourceException(
                     "PoolQueryRepositoryMsg timed out" ,
                     poolName,
                     " " +_TO_GetPoolRepository ) ;

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

     CellMessage res = sendAndWait( new CellMessage(cellPath, object), _TO_SendObject );

     if (res == null)
       throw new Exception("Request timed out");

     return res.getMessageObject();
   }

}

/**
 * $Log: not supported by cvs2svn $
 * Revision 1.16.2.8  2007/04/14 00:42:20  aik
 * use correct confirmed list size as argument for random()
 *
 * Revision 1.16.2.7  2007/03/30 00:40:33  aik
 * - got hostname from pool by xgetcellinfo, nonreplication to poold on the same finished
 * - do not verify cachelocation consistency; I do check with pools file is present anyway
 * - when pool arrived and "enable" message to the pool has failed, do not try to get pool list from the dead pool
 *
 * Revision 1.16.2.6  2007/03/10 21:12:37  aik
 * add debug printout to track lost host name; implement task removal by pool name or all tasks
 *
 * Revision 1.16.2.5  2007/03/02 22:37:27  aik
 *  - fix iterators in source pool map and best pools check
 *  - cleanup - remove large commented out block of code which was never used "ignore all zero-size files" in getPoolRepository()
 *
 * Revision 1.16.2.4  2007/02/26 23:50:35  aik
 * 1) set 'reply required' when checking host name; 2) do not try to check replica in not writable pools
 *
 * Revision 1.16.2.3  2007/02/20 15:29:43  tigran
 * backport of java 1.5 code to 1.4
 *
 * Revision 1.16.2.2  2007/02/14 22:53:38  aik
 * merge in changes from trunk [disallow same host replication, more]. Backport generics to java 1.4
 *
 * Revision 1.20  2007/02/09 23:13:42  aik
 * - disallow same host replication on besteffort bassis when pool has host name tag.
 * Enabled by default and can be turned off to be used on test installations.
 * - do not replicate _to_ pools in drainoff and offline-prepare state.
 *
 *
 */
