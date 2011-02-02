// $Id$
//
package diskCacheV111.replicaManager ;

import  dmg.cells.nucleus.*;
import  dmg.util.*;
import  dmg.cells.services.*;

import  diskCacheV111.util.PnfsId ;
import  diskCacheV111.vehicles.* ;
import  diskCacheV111.pools.* ;

import  java.io.*;
import  java.util.*;
import  java.util.regex.* ;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *  The EasyCopyCellModules needs to be launched within the
 *  dmg.cells.services.CommandTaskCell. It can drain a pool
 *  to a pool group, to a set of pools or using the PoolManager
 *  replicate command.
 *
 * @author  Patrick Fuhrmann
 * @version 0.0, Aug 22, 2006
 */

public class EasyCopyCellModule implements  CommandTaskCell.CellCommandTaskable, java.io.Serializable {

   private final static Logger _log =
       LoggerFactory.getLogger(EasyCopyCellModule.class);

   private final CellAdapter  _cell;
   private File         _base     = null ;
   private int          _state    = ST_IDLE ;
   private Pattern      _pattern  = null ;
   private Object       _lock     = this ;
   private boolean      _halted   = false ;
   private int          _processInParallel = 1 ;
   private int          _processErrorCount = 0 ;
   private ProtocolInfo _protocolInfo  = null ;
   private String       _siProtocol    = "DCap/3" ;
   private String       _siDestination = "localhost" ;
   private int          _copyMode      = Pool2PoolTransferMsg.UNDETERMINED;
   private String       _message        = "IDLE" ;
   private Map<PnfsId, RepositoryEntry>          _inProgressMap  = null ;
   private String       _sourcePoolName = null ;
   private long         _totalNumberOfFiles = 0L ;
   private long         _totalNumberOfBytes = 0L ;
   private long         _currentNumberOfFiles = 0L ;
   private long         _currentNumberOfBytes = 0L ;
   private long         _errorNumberOfFiles = 0L ;
   private long         _errorNumberOfBytes = 0L ;
   private ObjectOutputStream _errorOutput  = null ;
   private String       _poolGroupName      = null ;
   private List         _poolGroupList      = null ;
   private PoolInfoHandler _poolInfoHandler = null ;

   private final static int RE_IDLE  = 0 ;
   private final static int RE_WAITING_FOR_STORAGEINFO = 1;
   private final static int RE_WAITING_FOR_REPLICATION = 2 ;

   private static final int ST_IDLE                   = 0 ;
   private static final int ST_WAITING_FOR_REPOSITORY = 1 ;
   private static final int ST_ERROR                  = 2 ;
   private static final int ST_PROCESSING             = 3 ;
   private static final int ST_BUSY                   = 4 ;
   private static final int ST_WAITING_FOR_PGROUP     = 5 ;
   private static final int ST_CHECKING_POOLS         = 6 ;
   private static final int ST_PROCESSING_COPYTO      = 7 ;

   private static final String[] _stateStrings = {
      "idle" , "waiting_for_rep" , "error" , "processing" , "busy" ,
      "waiting_for_pgroup" , "checking_pools" , "processing_copyto"
   };

   private final CommandTaskCell.CellCommandTaskCore _core;

   public EasyCopyCellModule( CommandTaskCell.CellCommandTaskCore core ) throws Exception {

       _cell = core.getParentCell() ;
       _core = core ;
       _log.info("Started : "+core.getName());

       String baseName = getParameter("base") ;
       String tmp      = getParameter("autogenerate");
       boolean autogen = ( tmp != null ) && ( tmp.equals("") || tmp.equals("true")|| tmp.equals("yes") ) ;

       if( baseName == null )
          throw new
          IllegalArgumentException("-base not specified anywhere");

       _base = new File( baseName ) ;
       if( ( ! _base.exists() ) || ( ! _base.isDirectory() ) ){
          if( ! autogen )
             throw new
             IllegalArgumentException("-base =("+baseName+") doesn't exist or is not a directory");

          if( ! _base.mkdirs() )
              throw new
             IllegalArgumentException("Can't create -base =("+baseName+")");
       }
       _base = new File( _base , core.getModuleName() ) ;
       _base = new File( _base , core.getName() ) ;
       if( ( ! _base.exists() ) && ! _base.mkdirs() )
          throw new
          IllegalArgumentException("Can't create "+_base);

   }
   private String getParameter( String parameterName ){

       String parameter = _core.getTaskArgs().getOpt(parameterName) ;
       if( parameter == null )parameter = _core.getModuleArgs().getOpt(parameterName) ;
       if( parameter == null )parameter = _cell.getArgs().getOpt(parameterName);
       return parameter ;

   }
   /////////////////////////////////////////////////////////////////////////////////////////
   //
   //    COMMAND INTERFACE
   //
   public String hh_clear = " # clears state" ;
   public String ac_clear_$_0( Args args ) throws Exception {
      _state   = ST_IDLE ;
      _message = "CLEARED" ;

      return "" ;
   }
   public String hh_keeponly = "pinned|cached|precious|bad|locked|<storageClass>" ;
   public String ac_keeponly_$_1( Args args ) throws Exception {
       return do_exclude( args , false ) ;
   }
   public String hh_exclude = "pinned|cached|precious|bad|locked|<storageClass>" ;
   public String ac_exclude_$_1( Args args ) throws Exception {
       return do_exclude( args , true ) ;
   }
   private String do_exclude( Args args , boolean exclude ) throws Exception {

      String selection = args.argv(0) ;
      boolean     keep = ! exclude ;

      synchronized( _lock ){

          checkStateOk();
          _state = ST_BUSY ;
          _message = "Excluding : "+selection ;

      }
      File sourceFile = new File( _base , "repository.raw" ) ;
      if( ! sourceFile.exists() )
        throw new IOException("Repository file not found") ;

      File destFile = new File( _base  , "selection.raw" ) ;

       _totalNumberOfFiles = 0 ;
       _totalNumberOfBytes = 0L ;

      ObjectInputStream ois = new ObjectInputStream( new FileInputStream( sourceFile ) ) ;
      try{
         try{
             ObjectOutputStream oos = new ObjectOutputStream( new FileOutputStream( destFile ) ) ;
             try{

                 while( ! Thread.interrupted() ){

                     RepositoryEntry entry = (RepositoryEntry)ois.readObject() ;

                     boolean dontCopy = true ;

                     if( selection.equals("pinned") ){
		         dontCopy = ( keep ^ entry._pinned ) ;
	             }else if( selection.equals("cached") ){
		         dontCopy = ( keep ^ entry._cached ) ;
	             }else if( selection.equals("bad") ){
		         dontCopy = ( keep ^ entry._bad ) ;
	             }else if( selection.equals("precious") ){
		         dontCopy = ( keep ^ entry._precious ) ;
	             }else if( selection.equals("locked") ){
		         dontCopy = ( keep ^ entry._locked ) ;
		     }else{
		         dontCopy = keep ^ entry._info.equals(selection) ;
		     }
		     if( dontCopy )continue ;

                     _totalNumberOfFiles ++ ;
                     _totalNumberOfBytes += entry._size ;

                     oos.writeObject( entry ) ;

                 }
             }finally{
                try{ oos.close() ; }catch(Exception eee ){}
             }
         }catch(EOFException eof ){
         }catch(Exception cnf ){
             destFile.delete();
             throw cnf ;
         }finally{
            try{ ois.close() ; }catch(Exception ee ){}
         }
         sourceFile.delete() ;  // we need this for windows.
         destFile.renameTo( sourceFile ) ;
      }catch(Exception ee ){
         _message = "Done : excluding ..." + selection ;
         throw ee ;
      }finally{
          synchronized( _lock ){
            _state = ST_IDLE ;
            _message = "Done : excluding ... "+selection ;
         }
      }
      return "" ;
   }
   public String hh_ls_active = "" ;
   public String ac_ls_active_$_0( Args args ) throws Exception {
     List<RepositoryEntry> list;
     synchronized( _lock ){

           if( _inProgressMap == null )return "" ;
//         if( ( _state == ST_IDLE ) || ( _inProgressMap == null ) )
//            throw new
//            IllegalStateException("Nothing in progress");

         list = new ArrayList<RepositoryEntry>(_inProgressMap.values());
     }

     StringBuffer sb = new StringBuffer();
     for (RepositoryEntry entry : list) {
         sb.append(entry._pnfsId).append(" [").
            append(entry._state).append("] ").append(entry._message==null? "":entry._message).
            append("\n");
     }
     return sb.toString() ;
   }
   private class RepositoryStatistics {

      private final Map<String,long[]> storageClassMap =
          new HashMap<String,long[]>();
      private final Map<String,long[]> errorMap =
          new HashMap<String,long[]>();
      private long badCount      = 0L ;
      private long badSize       = 0L ;
      private long preciousCount = 0L ;
      private long preciousSize  = 0L ;
      private long pinnedCount   = 0L ;
      private long pinnedSize    = 0L ;
      private long totalCount    = 0L ;
      private long totalSize     = 0L ;
      private long cachedCount   = 0L ;
      private long cachedSize    = 0L ;
      private long lockedCount   = 0L ;
      private long lockedSize    = 0L ;
      private long errorCount    = 0L ;

      private void scan( RepositoryEntry entry ){
         scan( entry , false ) ;
      }
      private void scan( RepositoryEntry entry , boolean withError ){

          long [] counter = storageClassMap.get(entry._info);
          if( counter == null )storageClassMap.put( entry._info , counter = new long[2] ) ;

          long size   = entry._size ;
          counter[0] += 1 ;
          counter[1] += size ;

          if( entry._pinned   ){ pinnedCount ++ ; pinnedSize += size ; }
          if( entry._bad      ){ badCount ++ ; badSize += size ; }
          if( entry._precious ){ preciousCount ++ ; preciousSize += size ; }
          if( entry._cached   ){ cachedCount ++ ; cachedSize += size ; }
          if( entry._locked   ){ lockedCount ++ ; lockedSize += size ; }
          totalCount ++ ;
          totalSize += size ;

          if( ( ! withError ) || ( entry._message == null ) || ( errorMap.size() > 20 ) )return ;

          errorCount ++ ;
          counter = errorMap.get( entry._message ) ;
          if( counter == null )errorMap.put( entry._message , counter = new long[1] ) ;
          counter[0] ++ ;

      }
      private String toTable(){
         int dataWidth = 15 ;
         int classWith = 30 ;
         StringBuffer sb = new StringBuffer() ;
         sb.append(Formats.field("Class",classWith,Formats.CENTER)).append(" ").
            append(Formats.field("File Count",dataWidth,Formats.RIGHT)).
            append(Formats.field("Bytes",dataWidth,Formats.RIGHT)).
            append("\n") ;
         sb.append(Formats.field("pinned",classWith,Formats.CENTER)).
            append(Formats.field(""+pinnedCount,dataWidth,Formats.RIGHT)).
            append(Formats.field(""+pinnedSize,dataWidth,Formats.RIGHT)).
            append("\n") ;
         sb.append(Formats.field("bad",classWith,Formats.CENTER)).
            append(Formats.field(""+badCount,dataWidth,Formats.RIGHT)).
            append(Formats.field(""+badSize,dataWidth,Formats.RIGHT)).
            append("\n") ;
         sb.append(Formats.field("precious",classWith,Formats.CENTER)).
            append(Formats.field(""+preciousCount,dataWidth,Formats.RIGHT)).
            append(Formats.field(""+preciousSize,dataWidth,Formats.RIGHT)).
            append("\n") ;
         sb.append(Formats.field("cached",classWith,Formats.CENTER)).
            append(Formats.field(""+cachedCount,dataWidth,Formats.RIGHT)).
            append(Formats.field(""+cachedSize,dataWidth,Formats.RIGHT)).
            append("\n") ;
         sb.append(Formats.field("locked",classWith,Formats.CENTER)).
            append(Formats.field(""+lockedCount,dataWidth,Formats.RIGHT)).
            append(Formats.field(""+lockedSize,dataWidth,Formats.RIGHT)).
            append("\n") ;

         for (Map.Entry<String,long[]> e : storageClassMap.entrySet()) {
            String   name = e.getKey() ;
            long [] value = e.getValue() ;
            sb.append(Formats.field(name,classWith,Formats.CENTER)).
               append(Formats.field(""+value[0],dataWidth,Formats.RIGHT)).
               append(Formats.field(""+value[1],dataWidth,Formats.RIGHT)).
               append("\n") ;
         }
         sb.append(Formats.field("TOTAL",classWith,Formats.CENTER)).
            append(Formats.field(""+totalCount,dataWidth,Formats.RIGHT)).
            append(Formats.field(""+totalSize,dataWidth,Formats.RIGHT)).
            append("\n") ;

         if( errorMap.size() > 0 ){
            sb.append("\nNumber of different error types : ").append(errorMap.size()).append("\n\n");
            sb.append(Formats.field("Error-Type",14,Formats.RIGHT)).append("   Error Message").append("\n");
            for (Map.Entry<String,long[]> e : errorMap.entrySet()) {
               String   name = e.getKey() ;
               long [] value = e.getValue() ;
               sb.append(Formats.field(""+value[0],12,Formats.RIGHT)).append("   ").append(name).append("\n");
            }
         }
         return sb.toString();
      }
   }
   public String hh_ls_stat = "[-error]" ;
   public String ac_ls_stat_$_0( Args args ) throws Exception {

      boolean useError = args.getOpt("error") != null ;

      RepositoryStatistics stats = new RepositoryStatistics() ;

      File rawFile = new File( _base , useError ? "error.raw" : "repository.raw" ) ;
      if( ! rawFile.exists() )
        throw new IOException("Repository file not found") ;

      ObjectInputStream ois = new ObjectInputStream( new FileInputStream( rawFile ) ) ;

      try{
          Object obj = null ;
          while( ( obj = ois.readObject() ) != null ){

              if( ! ( obj instanceof RepositoryEntry ) )
                 throw new
                 Exception("Illegal Format found in repository");

              stats.scan( (RepositoryEntry)obj , true ) ;

          }

      }catch(EOFException eof ){
      }finally{
         try{ ois.close() ; }catch(IOException eee ){}
      }

      return stats.toTable();
   }
   public String hh_ls_files = "[-l]" ;
   public String ac_ls_files_$_0( Args args ) throws Exception {

      StringBuffer  sb = new StringBuffer() ;
      boolean extended = args.getOpt("l") != null ;

      File rawFile = new File( _base , "repository.raw" ) ;
      if( ! rawFile.exists() )
        throw new IOException("Repository file not found") ;

      ObjectInputStream ois = new ObjectInputStream( new FileInputStream( rawFile ) ) ;

      try{
          Object obj = null ;
          while( ( obj = ois.readObject() ) != null ){

              if( ! ( obj instanceof RepositoryEntry ) ){
                  sb.append("!!! Illegal Entry found : ").
                     append(obj.getClass().getName() ).
                     append("\n") ;
                  break ;
              }
              RepositoryEntry entry = (RepositoryEntry)obj ;
              sb.append(entry.toString()) ;
              if(extended&&(entry._message!=null))sb.append(" ").append(entry._message);
              sb.append("\n");
          }

      }catch(EOFException eof ){
      }catch(IOException ioe ){
          sb.append("!!! Io Loop ended du to : "+ioe ) ;
      }finally{
         try{ ois.close() ; }catch(IOException eee ){}
      }
      return sb.toString();
   }
   public String hh_send = "<destination> <message>" ;
   public String ac_send_$_2( Args args ) throws Exception {
       CellMessage msg = new CellMessage( new CellPath( args.argv(0) ) , args.argv(1) ) ;
       _core.sendMessage(msg);
       return "" ;
   }
   public String hh_info = "" ;
   public String ac_info( Args args ){
      StringWriter sw = new StringWriter() ;
      PrintWriter  pw = new PrintWriter( sw ) ;
      getInfo(pw) ;
      pw.flush() ;
      return sw.getBuffer().toString() ;
   }
   private class PoolInfoHandler {
      private class PoolInfo implements Comparable<PoolInfo> {

          private final String _poolName;
          private long   _freeSpace      = 0L ;
          private long   _removableSpace = 0L ;

          private PoolInfo( String name ){
              _poolName = name ;
          }
          public String getName(){ return this._poolName ; }
          public int compareTo(PoolInfo info){
             long all = _freeSpace+_removableSpace ;
             long xall = info._freeSpace + info._removableSpace ;
             return all < xall ? -1 : all > xall ? 1 : _poolName.compareTo(info._poolName) ;
          }
          @Override
		public String toString(){
             return _poolName+";free="+_freeSpace+";rem="+_removableSpace;
          }
          public long getAvailableSpace(){ return _freeSpace+_removableSpace ; }
      }
      private Map<String,PoolInfo> _pools = new HashMap<String,PoolInfo>();
      private void update( PoolCellInfo cellInfo ){

          PoolCostInfo               cost  = cellInfo.getPoolCostInfo() ;
          PoolCostInfo.PoolSpaceInfo space = cost.getSpaceInfo() ;
          this.update(
                cellInfo.getCellName() ,
                space.getFreeSpace() ,
                space.getRemovableSpace() );

      }
      private void update( String poolName , long free , long removable ){
         PoolInfo info = _pools.get(poolName) ;
         if( info == null )_pools.put( poolName , info = new PoolInfo( poolName ) ) ;
         info._freeSpace = free ;
         info._removableSpace = removable ;
      }
      private int size(){ return _pools.size() ; }
      private PoolInfo getBestPool(){
          return (_pools.size() == 0 ? null : Collections.max(_pools.values()));
      }
      @Override
	public String toString(){
         StringBuffer sb = new StringBuffer() ;
         TreeSet<PoolInfo> set = new TreeSet<PoolInfo>(_pools.values());
         for (PoolInfo info : set) {
            sb.append(info.toString()).append("\n");
         }
         sb.append("First : ").append(set.first().toString()).append("\n");
         sb.append("Last  : ").append(set.last().toString()).append("\n");
         return sb.toString();
      }
   }
   private void processCopyToPoolGroup() throws Exception {
      synchronized( _lock ){

          try{
              //
              // do we have to resolve the pgroup first ?
              //
              if( _state == ST_WAITING_FOR_PGROUP ){

                 _message = "Waiting for pgroup "+_poolGroupName ;
                 try{
                     _poolGroupList = null ;
                     _core.sendMessage(
                           new CellMessage(
                                   new CellPath("PoolManager") ,
                                   "psux ls pgroup "+_poolGroupName
                                          )
                                      ) ;

                 }catch(Exception ee ){
                    _message = "Sending message to PoolManager failed" ;
                    throw ee ;
                 }
                 _log.info("Waiting for pool group reply");
                 while( _state == ST_WAITING_FOR_PGROUP )_lock.wait() ;
                 if( _state == ST_IDLE ){
                    _log.warn("PGroup request to PoolManager failed : "+_message);
                    return ;
                 }

              }
              //
              // here either have a resolved pgroup or a list of pools
              // from the command line.
              //
              if( _state != ST_CHECKING_POOLS ){
                  _message = "PANIC : Illegal state" ;
                  return ;
              }
              if( _poolGroupList.size() == 0 ){
                  _message = "No pools specified or pool group empty";
                  return ;
              }
              _log.info("PoolGroup list : "+_poolGroupList);
              int waitingFor = 0 ;
              for (Object o : _poolGroupList) {
                  /*
                   * FIXME: in error conditions the counter of waiting messages is not
                   * updated and we start to wait for ever.
                   * probably we have to use java.util.concurrent.Condition for this.
                   *
                   */
                 String poolName = (String)o;
                 try{
                    _core.sendMessage( new CellMessage( new CellPath(poolName) , "xgetcellinfo") ) ;
                    waitingFor ++ ;
                 }catch(Exception ee ){
                    _log.warn("Couldn't send 'xgetcellinfo' to "+poolName);
                 }
              }
              _log.info("Waiting for pool infos of pgroup "+_poolGroupName);
              _message = "Waiting for PoolCellInfo(s)" ;

              _poolGroupList.clear();
              while( _poolGroupList.size() < waitingFor )_lock.wait() ;

              _log.info("PoolInfo list : "+_poolGroupList);

              _poolInfoHandler = new PoolInfoHandler() ;

              for (Object o : _poolGroupList) {
                  if (o instanceof PoolCellInfo) {
                      PoolCellInfo cellInfo = (PoolCellInfo)o ;
                      if(!cellInfo.getCellName().equals(_sourcePoolName) )
                          _poolInfoHandler.update(  cellInfo ) ;
                  }
              }

              _state   = ST_PROCESSING_COPYTO ;
              _message = "Processing copyto of group "+_poolGroupName+
                         " with "+_poolInfoHandler.size()+" active pools";

              processCopyto( _poolInfoHandler ) ;

              _message = "Done : "+_message ;

          }finally{
             _state = ST_IDLE ;
          }

      }

   }
   private void replyForCopytoArrived( Object sendObj , Object obj ){

      if( sendObj instanceof PnfsGetStorageInfoMessage ){

          PnfsGetStorageInfoMessage query = (PnfsGetStorageInfoMessage)sendObj ;
          PnfsId pnfsId = query.getPnfsId() ;
          synchronized( _lock ){

              RepositoryEntry entry = _inProgressMap.remove(pnfsId);
              if( entry == null ){
                  _log.warn("answerArrived : Unexpected PnfsGetStorageInfoMessage for "+pnfsId+" arrived") ;
                  return ;
              }

              if( obj == null ){
                 storeError(entry,9,"Request for PnfsGetStorageInfoMessage timed out");
              }else if( obj instanceof PnfsGetStorageInfoMessage ){

                 PnfsGetStorageInfoMessage reply = (PnfsGetStorageInfoMessage)obj ;
                 if( reply.getReturnCode() == 0 ){
                     _log.info("PnfsGetStorageInfoMessage : ok for "+pnfsId);
                     try{
                         StorageInfo si = reply.getStorageInfo() ;

                         PoolInfoHandler.PoolInfo info = _poolInfoHandler.getBestPool() ;
                         if( info == null )
                            throw new
                            Exception("No pool available");

                         if( info.getAvailableSpace() < ( 2 * si.getFileSize() ) )
                            throw new
                            Exception("Available pool too small");

                         Pool2PoolTransferMsg msg =
                                new Pool2PoolTransferMsg(
                                      _sourcePoolName ,
                                      info.getName() ,
                                      pnfsId ,
                                      si ) ;
                         if( _copyMode < 0 ){
                             msg.setDestinationFileStatus(
                                    entry._precious ? Pool2PoolTransferMsg.PRECIOUS :
                                                      Pool2PoolTransferMsg.CACHED     );
                         }else{
                             msg.setDestinationFileStatus(_copyMode);
                         }

                         _core.sendMessage(
                               new CellMessage( new CellPath( info.getName() ) , msg )
                                          );

                         _inProgressMap.put( pnfsId , entry ) ;

                     }catch(Exception eee ){
                         storeError(entry,9,"PnfsGetStorageInfoMessage : Problem in creating pool 2 pool request : "+eee);
                     }
                 }else{
                     storeError(entry,9,"PnfsGetStorageInfoMessage : "+reply.getErrorObject());
                 }
              }else{
                 storeError(entry,10,"Expected PnfsGetStorageInfo, got "+obj.getClass().getName());
              }
              _lock.notifyAll();
          }

      }else if( sendObj instanceof Pool2PoolTransferMsg ){

          Pool2PoolTransferMsg query = (Pool2PoolTransferMsg)sendObj ;
          PnfsId pnfsId = query.getPnfsId() ;
          synchronized( _lock ){

              RepositoryEntry entry = _inProgressMap.remove(pnfsId);
              if( entry == null ){
                  _log.warn("answerArrived : Unexpected PnfsGetStorageInfoMessage for "+pnfsId+" arrived") ;
                  return ;
              }

              if( obj == null ){
                 storeError(entry,9,"Request for PnfsGetStorageInfoMessage timed out");
              }else if( obj instanceof Pool2PoolTransferMsg ){

                 Pool2PoolTransferMsg reply = (Pool2PoolTransferMsg)obj ;
                 if( reply.getReturnCode() == 0 ){
                     _log.info("Pool2PoolTransferMsg : ok for "+pnfsId);
                     _currentNumberOfBytes += entry._size ;
                     _currentNumberOfFiles ++ ;
                     try{
                        _core.sendMessage( new CellMessage( new CellPath(reply.getDestinationPoolName()),"xgetcellinfo"));
                     }catch(Exception ee ){}
                 }else{
                      storeError(entry,9,"Pool2PoolTransferMsg : "+reply.getErrorObject());
                 }
              }else{
                 storeError(entry,10,"Expected Pool2PoolTransferMsg, got "+obj.getClass().getName());
              }
              _lock.notifyAll();
          }

      }else if( obj instanceof PoolCellInfo ){
          synchronized( _lock ){
             _poolInfoHandler.update( (PoolCellInfo)obj ) ;
          }
      }

   }
   private void processCopyto( PoolInfoHandler handler ) throws Exception {

      File rawFile = new File( _base , "repository.raw" ) ;
      if( ! rawFile.exists() )
        throw new IOException("Repository file not found") ;

      File errorFileName = new File( _base , "error.raw") ;

      _errorNumberOfBytes   = 0L ;
      _errorNumberOfFiles   = 0L ;
      _currentNumberOfFiles = 0L ;
      _currentNumberOfBytes = 0L ;

      ObjectInputStream ois = new ObjectInputStream( new FileInputStream( rawFile ) ) ;

      try{

          Object obj = null ;

         _errorOutput = new ObjectOutputStream( new FileOutputStream( errorFileName )  ) ;

         _inProgressMap = new HashMap<PnfsId,RepositoryEntry>();

         while( ! Thread.interrupted() ){

             while( ( _inProgressMap.size() >= _processInParallel ) )_lock.wait() ;

             try{
                obj = ois.readObject() ;
             }catch(EOFException eof ){
                 _log.info("processReplication finished (interrupted="+Thread.interrupted()+")");
                 break ;
             }catch(ClassNotFoundException cnfe ){
                 _log.warn("!!! Io Loop ended du to : "+cnfe ) ;
                 _message = "processReplication failed due to : "+cnfe ;
                 break ;
             }catch(IOException ioe ){
                 _log.warn("!!! Io Loop ended du to : "+ioe ) ;
                 _message = "processReplication failed due to : "+ioe ;
                 break ;
             }

             if( ! ( obj instanceof RepositoryEntry ) ){
                 _log.warn("!!! Illegal Entry found : "+obj.getClass().getName() ) ;
                 break ;
             }

             RepositoryEntry entry = (RepositoryEntry)obj ;

             if( _halted ){
                 storeError( entry , 44 , "Halted" ) ;
                 continue ;
             }
             if( entry._locked ){
                  storeError( entry , 45 , "File is Locked" ) ;
                  continue ;
             }
             PnfsGetStorageInfoMessage query =
                 new PnfsGetStorageInfoMessage(PoolMgrReplicateFileMsg.getRequiredAttributes());
             query.setPnfsId(entry._pnfsId);
             CellMessage msg = new CellMessage( new CellPath("PnfsManager") , query ) ;
             try{
                _core.sendMessage( msg );
             }catch(Exception e){
                storeError( entry , 5 , "Can't send StorageInfoQuery" ) ;
                continue ;
             }
             _log.info("processReplication : adding : "+entry._pnfsId ) ;
             entry._state   = RE_WAITING_FOR_STORAGEINFO ;
             entry._message = "Waiting for storage info";
             _inProgressMap.put( entry._pnfsId , entry ) ;
          }
          _log.info("Waiting for outstanding requests : "+_inProgressMap.size()  );

          while( ( ! Thread.interrupted() ) && ( _inProgressMap.size() > 0 ) )_lock.wait() ;

      }finally{
         try{ ois.close() ; }catch(IOException eee ){}
         try{ _errorOutput.close() ; }catch(Exception ee ){}
         errorFileName.renameTo( rawFile ) ;
         _totalNumberOfFiles = _errorNumberOfFiles ;
         _totalNumberOfBytes = _errorNumberOfBytes ;
         _halted = false ;

      }

   }
   private void poolInfosArrived( Object orginal , Object reply ){
      synchronized( _lock ){
         _poolGroupList.add(reply);
         _lock.notifyAll() ;
      }
   }
   private void poolGroupListArrived( Object orginal , Object reply ){
      synchronized( _lock ){
         try{
            if( reply == null )
                throw new Exception("Request to PoolManager timed out");

            if( reply instanceof Exception )
                throw (Exception)reply ;

            if( ( ! ( reply instanceof Object [] ) ) ||
                    ( ((Object [])reply).length < 3 )    )
                 throw new Exception("Illegally formated reply on 'psux ls pgroup'");


            Object [] a = (Object [])  ((Object [])reply)[1] ;
            List list = new ArrayList() ;
            for( int i = 0 ; i < a.length ; i++ )list.add( a[i] ) ;
            _poolGroupList = list ;
            _state   = ST_CHECKING_POOLS ;
            _message = "PGroup pool arrived";

         }catch(Exception ee ){
            _state   = ST_IDLE ;
            _message = ee.getMessage() ;
         }finally{
            _lock.notifyAll() ;
         }

      }
   }
   private void processReplication() throws IOException {

      File rawFile = new File( _base , "repository.raw" ) ;
      if( ! rawFile.exists() )
        throw new IOException("Repository file not found") ;

      File errorFileName = new File( _base , "error.raw") ;

      ObjectInputStream ois = new ObjectInputStream( new FileInputStream( rawFile ) ) ;

      try{
          Object obj = null ;

          synchronized( this ){

             _errorOutput = new ObjectOutputStream( new FileOutputStream( errorFileName )  ) ;

             _inProgressMap     = new HashMap<PnfsId,RepositoryEntry>();
             _state             = ST_PROCESSING ;
             _message           = "Processing ... ";
             _processErrorCount = 0 ;
             int recordCounter  = 0 ;
             while( ! Thread.interrupted() ){

                 while( ( _inProgressMap.size() >= _processInParallel ) )_lock.wait() ;

                 try{
                    obj = ois.readObject() ;
                 }catch(EOFException eof ){
                     _log.info("processReplication finished (interrupted="+Thread.interrupted()+")");
                     break ;
                 }catch(ClassNotFoundException cnfe ){
                     _log.warn("!!! Io Loop ended du to : "+cnfe ) ;
                     _message = "processReplication failed due to : "+cnfe ;
                     break ;
                 }catch(IOException ioe ){
                     _log.warn("!!! Io Loop ended du to : "+ioe ) ;
                     _message = "processReplication failed due to : "+ioe ;
                     break ;
                 }
                 recordCounter ++ ;

                 if( ! ( obj instanceof RepositoryEntry ) ){
                     _log.warn("!!! Illegal Entry found : "+obj.getClass().getName() ) ;
                     break ;
                 }

                 RepositoryEntry           entry = (RepositoryEntry)obj ;
                 PnfsGetStorageInfoMessage query =
                     new PnfsGetStorageInfoMessage(PoolMgrReplicateFileMsg.getRequiredAttributes());
                 query.setPnfsId(entry._pnfsId);
                 CellMessage msg = new CellMessage( new CellPath("PnfsManager") , query ) ;
                 try{
                    _core.sendMessage( msg );
                 }catch(Exception e){
                     entry._state   = 0 ;
                     entry._message = "Can't send StorageInfoQuery" ;
                     try{ _errorOutput.writeObject(entry) ; }catch(Exception ee ){} ;
                     _log.warn("Can't send StorageInfoQuery : "+e);
                    _processErrorCount ++ ;
                    continue ;
                 }
                 _log.info("processReplication : adding : "+entry._pnfsId ) ;
                 entry._state   = RE_WAITING_FOR_STORAGEINFO ;
                 entry._message = "Waiting for storage info";
                 _inProgressMap.put( entry._pnfsId , entry ) ;

             }
             _log.info("Waiting for outstanding requests : "+_inProgressMap.size()  );

             while( ( ! Thread.interrupted() ) && ( _inProgressMap.size() > 0 ) )_lock.wait() ;

             try{ _errorOutput.close() ; }catch(Exception ee ){}

             _state    = ST_IDLE ;
             _message  = "Processing ... Done with "+recordCounter+" record ";

          }
      }catch(InterruptedException ie ){
          _log.warn("!!! Io Loop was interrupted" ) ;
          _message = "processReplication has been interrupted" ;
      }finally{
         try{ ois.close() ; }catch(IOException eee ){}
         try{ _errorOutput.close() ; }catch(Exception ee ){}
         synchronized(this){
             _state = ST_IDLE ;
             errorFileName.renameTo( rawFile ) ;
         }
      }
   }
   private void checkStateOk() throws IllegalStateException{
      if( _state != ST_IDLE )
         throw new
         IllegalStateException("Task still in mode : "+_stateStrings[_state]+" "+_message);
   }
   public String hh_load_pool = "<PoolName> [<StorageInfoPattern>]" ;
   public String ac_load_pool_$_1_2( Args args ) throws Exception {

       String  poolName = args.argv(0) ;
       Pattern pattern  = args.argc() > 1 ? Pattern.compile(args.argv(1)) : null ;

       synchronized( _lock ){

          checkStateOk();

          _message = "Loading "+poolName+ " Filter = "+( pattern != null ? args.argv(1) : "none" ) ;
          _pattern = pattern ;
          _state   = ST_WAITING_FOR_REPOSITORY ;

          CellMessage msg = new CellMessage( new CellPath( poolName ) , "rep ls -l" ) ;
          _core.sendMessage(msg);

          _sourcePoolName = poolName ;
       }
       return _message ;
   }
   public String hh_replicate = "OPTIONS # see help go for further infos" ;
   public String fh_replicate =
       " OPTIONS :\n"+
       "   -parallel=<parallelStreams>\n"+
       "   -destination=<destinationIpAddress>\n"+
       "   -protocol=<protocol> # NOT USED YET\n"+
       "   -copy-mode=precious|cached|same|nn\n" ;
   public String ac_replicate_$_0( Args args ) throws Exception {

       String p    = args.getOpt("parallel") ;
       String dest = args.getOpt("destination") ;
       String prot = args.getOpt("protocol") ;
       String mode = args.getOpt("copy-mode") ;

       synchronized( _lock ){

          checkStateOk() ;

          if( p != null    )_processInParallel = Integer.parseInt(p);
          if( dest != null )_siDestination     = dest ;
          if( prot != null )_siProtocol        = prot ;
          if( mode != null )_copyMode          = convertCopyMode(mode) ;

          _cell.getNucleus().newThread(
             new Runnable(){
                public void run(){
                   try{
                       processReplication() ;
                   }catch(Exception ee ){
                       _log.warn("processReplication terminated : "+ee );
                   }
                }
             }, "GO"
          ).start() ;

       }

       return "" ;
   }
   public String hh_halt = " # halt processing" ;
   public String ac_halt_$_0( Args args ){
       synchronized( _lock ){
          if( _state == ST_IDLE )
             throw new
             IllegalArgumentException("No processing anything");

          _halted = true ;
       }
       return "Halt initiated" ;
   }
   public String hh_copyto_group = "<poolGroup> [-copy-mode=precious|cached|nn|same] [-parallel=<streams>]";
   public String ac_copyto_group_$_1( Args args ) throws Exception {

       _poolGroupName = args.argv(0) ;

       return copy_to( args , ST_WAITING_FOR_PGROUP ) ;
   }
   public String hh_copyto_pools = "<pool1> [<pool2>[...]] [-copy-mode=precious|cached|nn|same] [-parallel=<streams>]";
   public String ac_copyto_pools_$_1_999( Args args ) throws Exception {

       _poolGroupList = new ArrayList() ;
       for( int i = 0 ; i < args.argc() ; i++ )_poolGroupList.add(args.argv(i)) ;

       return copy_to( args , ST_CHECKING_POOLS ) ;
   }

   private String copy_to( Args args , int state ) throws Exception {


       String p    = args.getOpt("parallel") ;
       String mode = args.getOpt("copy-mode") ;

       if( _sourcePoolName == null )
           throw new
           IllegalArgumentException("No Pool Repository Loaded yet");

       synchronized( _lock ){

          checkStateOk() ;

          _halted = false ;

          if( p != null    )_processInParallel = Integer.parseInt(p);
          if( mode != null )_copyMode          = convertCopyMode(mode) ;

          _state =  state ;

          _cell.getNucleus().newThread(
             new Runnable(){
                public void run(){
                   try{
                       processCopyToPoolGroup() ;
                   }catch(Exception ee ){
                       _log.warn("processCopyToPoolGroup terminated : "+ee );
                   }
                }
             }, "GO"
          ).start() ;

       }

       return "" ;
   }
   private int convertCopyMode( String copyModeString ){
      if( copyModeString.equals("cached") ){
         return Pool2PoolTransferMsg.CACHED ;
      }else if( copyModeString.equals("precious") ){
         return Pool2PoolTransferMsg.PRECIOUS ;
      }else if( copyModeString.equals("nn") ){
         return Pool2PoolTransferMsg.UNDETERMINED ;
      }else if( copyModeString.equals("same") ){
         return -1 ;
      }else
         throw new
         IllegalArgumentException("Copy Modes : cached,precious,same,nn");
   }
   private String convertCopyMode( int mode ){
      switch( mode ){
         case Pool2PoolTransferMsg.CACHED       : return "cached" ;
         case Pool2PoolTransferMsg.PRECIOUS     : return "precious" ;
         case Pool2PoolTransferMsg.UNDETERMINED : return "nn" ;
         default : return "same" ;
      }
   }
   public String hh_set = "<key> <value> # see help set";
   public String fh_set =
       "set <key> <Value>\n"+
       "   destination <clientHostName>\n"+
       "   copy-mode   nn|same|precious|cached\n"+
       "   parallel    <numberOfParallelTransfers>\n"+
       "   protocol    <clientProtocol> # !!!  IS STILL IGNORED\n"+
       "                  DCap/3\n"+
       "                  Ftp/1\n";
   public String ac_set_$_2( Args args )throws Exception {

      String para  = args.argv(0) ;
      String value = args.argv(1) ;

      synchronized( _lock ){
         if( para.equals("destination") ){

            checkStateOk() ;
            _siDestination = value ;

         }else if( para.equals("protocol") ){

             checkStateOk() ;
            _siProtocol    = value ;

         }else if( para.equals("copy-mode") ){

             checkStateOk() ;
            _copyMode = convertCopyMode(value) ;

         }else if( para.equals("parallel") ){

            _processInParallel = Integer.parseInt(value);
            _lock.notifyAll();

         }else
            throw new
            IllegalArgumentException("No a valid parameter : "+para);

      }
      return "" ;
   }
   //
   //    get storage info
   //    get cache info
   //
   private void replicateMessageArrived( Object sendObj , Object obj ){

       PoolMgrReplicateFileMsg query = (PoolMgrReplicateFileMsg)sendObj ;
       PnfsId pnfsId = query.getPnfsId() ;
       synchronized( this ){

           RepositoryEntry entry = _inProgressMap.remove(pnfsId);
           if( entry == null ){
               _log.warn("answerArrived : Unexpected PoolMgrReplicateFileMsg for "+pnfsId+" arrived") ;
               return ;
           }

           if( obj instanceof PoolMgrReplicateFileMsg ){

              PoolMgrReplicateFileMsg reply = (PoolMgrReplicateFileMsg)obj ;
              if( reply.getReturnCode() == 0 ){
                  _log.info("PoolMgrReplicateFileMsg : ok for "+pnfsId);
              }else{
                   storeError(entry,9,"PoolMgrReplicateFileMsg : "+reply.getErrorObject());
              }
           }else{
              storeError(entry,10,"PoolMgrReplicateFileMsg didn't arrive, instead : "+obj.getClass().getName());
           }
           _lock.notifyAll();
       }
   }
   private void storageInfoArrived( Object sendObj , Object obj ){

      PnfsGetStorageInfoMessage query = (PnfsGetStorageInfoMessage)sendObj ;
       PnfsId pnfsId = query.getPnfsId() ;
       synchronized( this ){

           RepositoryEntry entry = _inProgressMap.remove(pnfsId);
           if( entry == null ){
               _log.warn("answerArrived : Unexpected StorageInfo for "+pnfsId+" arrived") ;
               return ;
           }

           if( obj instanceof PnfsGetStorageInfoMessage ){

              PnfsGetStorageInfoMessage reply = (PnfsGetStorageInfoMessage)obj ;
              if( reply.getReturnCode() == 0 ){
                  //
                  // everything ok
                  //
                  _log.warn("StorageInfo for "+pnfsId+" "+reply);
                  //
                  _protocolInfo = new DCapProtocolInfo( "DCap", 3, 0,_siDestination ,2222) ;
                  StorageInfo storageInfo = reply.getStorageInfo() ;

                  PoolMgrReplicateFileMsg req =
                     new PoolMgrReplicateFileMsg(reply.getFileAttributes(),
                          _protocolInfo ,
                          storageInfo.getFileSize()
                     );
                  req.setReplyRequired(true);
                  req.setDestinationFileStatus(_copyMode);

                  try{

                      CellMessage msg = new CellMessage( new CellPath("PoolManager") , req ) ;
                      _core.sendMessage(msg);
                      entry._state   = RE_WAITING_FOR_REPLICATION ;
                      entry._message = "Waiting for replication" ;
                      _inProgressMap.put( pnfsId , entry ) ;
                      _log.info("Sending PoolMgrReplicateFileMsg request");


                  }catch(Exception eeee ){
                      storeError( entry , 6 , "Sending PoolMgrReplicateFileMsg request FAILED" ) ;
                  }

              }else{
                   Object o = reply.getErrorObject() ;
                   storeError(entry,7,"StorageInfo arrived with ["+o.getClass().getName()+"] : "+reply.getErrorObject());
              }
           }else{
              storeError(entry,8,"StorageInfo for didn't arrive, instead : "+obj.getClass().getName());
           }
           _lock.notifyAll();
       }

   }
   private void storeError( RepositoryEntry entry , int rc , String message ){
      _processErrorCount++ ;
      entry._state = rc ;
      entry._message = message ;
      try{ _errorOutput.writeObject(entry) ; }catch(Exception eee ){}

      _errorNumberOfFiles ++ ;
      _errorNumberOfBytes += entry._size ;

      _log.warn("Problem "+entry._pnfsId+" ["+rc+"] "+message ) ;
   }
   public void answerArrived( CellMessage request , CellMessage answer ){

      _log.info("Answer arrived for task : "+_core.getName()+" : "+answer.getMessageObject().getClass().getName());

      Object sendObj = request.getMessageObject() ;
      Object obj     = answer.getMessageObject() ;

      if( _state == ST_WAITING_FOR_REPOSITORY ){

         try{

             repositoryArrived( answer , (String)obj );
             _message = "Done : "+_message ;

         }catch(Exception ee ){
             _message = "Failed "+_message+" due to "+ee.getMessage() ;
         }finally{
             _state   = ST_IDLE ;
         }
         return ;

      }else if( _state == ST_WAITING_FOR_PGROUP ){

         poolGroupListArrived( sendObj , obj ) ;

      }else if( _state == ST_CHECKING_POOLS ){

         poolInfosArrived( sendObj , obj ) ;

      }else if( _state == ST_PROCESSING_COPYTO ){

         replyForCopytoArrived( sendObj , obj ) ;

      }else if( _state == ST_PROCESSING ){

         if( sendObj instanceof PoolMgrReplicateFileMsg ){

             replicateMessageArrived( sendObj , obj ) ;

         }else if( sendObj instanceof PnfsGetStorageInfoMessage ){

             storageInfoArrived( sendObj , obj ) ;

         }else{
         }
      }

   }
   public void exceptionArrived( CellMessage request , Exception   exception ){
      _log.info("Exception arrived for task : "+_core.getName()+" : "+exception);

      Object sendObj = request.getMessageObject() ;

      if( _state == ST_WAITING_FOR_PGROUP ){

         poolGroupListArrived( sendObj , exception ) ;

      }else if( _state == ST_CHECKING_POOLS ){

         poolInfosArrived( sendObj , exception ) ;

      }else if( _state == ST_PROCESSING_COPYTO ){

         replyForCopytoArrived( sendObj , exception ) ;

      }

   }
   public void answerTimedOut( CellMessage request ){

      _log.info("Timeout arrived for task : "+_core.getName() );
      Object sendObj = request.getMessageObject() ;

      if( _state == ST_WAITING_FOR_PGROUP ){

         poolGroupListArrived( sendObj , null ) ;

      }else if( _state == ST_CHECKING_POOLS ){

         poolInfosArrived( sendObj , null ) ;

      }else if( _state == ST_PROCESSING_COPYTO ){

         replyForCopytoArrived( sendObj , null ) ;

      }

   }
   private void repositoryArrived( CellMessage reply , String repositoryString )
           throws Exception {

      File rawFile = new File( _base , "repository.raw" ) ;

      try{
         convertRepositoryListing( repositoryString ,rawFile , _pattern ) ;
      }catch(Exception ee ){
         rawFile.delete() ;
         throw ee ;
      }
      return ;
   }
   public void getInfo( PrintWriter pw ){
       pw.println(" -----------------------------------------------------" ) ;
       pw.println("Task" ) ;
       pw.println("----" ) ;
       pw.println("        Name : "+ _core.getName() ) ;
       pw.println(" Module Args : "+ _core.getModuleArgs().toString().trim() ) ;
       pw.println("   Task Args : "+ _core.getTaskArgs().toString().trim() ) ;
       pw.println("Parameter" ) ;
       pw.println("---------" ) ;
       pw.println(" Destination : "+ _siDestination ) ;
       pw.println("    Protocol : "+ _siProtocol ) ;
       pw.println("   Copy Mode : "+convertCopyMode(_copyMode));
       pw.println("    Parallel : "+_processInParallel);
       pw.println("Progress" ) ;
       pw.println("--------" ) ;
       pw.println("   Source : "+(_sourcePoolName==null?"NONE":_sourcePoolName));
       pw.println("   Status : "+ _stateStrings[_state] ) ;
       pw.println("  Message : "+ _message ) ;
       pw.println("");
       printCopyProgress(pw);
   }
   private void printCopyProgress( PrintWriter pw ){

      if( ( _currentNumberOfFiles == 0 ) &&
          ( _errorNumberOfFiles   == 0 ) &&
          ( _totalNumberOfFiles   == 0 )    )return ;

      StringBuffer sb = new StringBuffer() ;

      int width = 16 ;

      sb.append(" ").
         append(Formats.field("Type",width,Formats.CENTER)).
         append(Formats.field("Files",width,Formats.CENTER)).
         append(Formats.field("Bytes",width,Formats.CENTER)).
         append("\n");
      sb.append(" ").
         append(Formats.field("Total",width,Formats.CENTER)).
         append(Formats.field(""+_totalNumberOfFiles,width,Formats.CENTER)).
         append(Formats.field(""+_totalNumberOfBytes,width,Formats.CENTER)).
         append("\n");
      sb.append(" ").
         append(Formats.field("Current",width,Formats.CENTER)).
         append(Formats.field(""+_currentNumberOfFiles,width,Formats.CENTER)).
         append(Formats.field(""+_currentNumberOfBytes,width,Formats.CENTER)).
         append("\n");
       sb.append(" ").
         append(Formats.field("Error",width,Formats.CENTER)).
         append(Formats.field(""+_errorNumberOfFiles,width,Formats.CENTER)).
         append(Formats.field(""+_errorNumberOfBytes,width,Formats.CENTER)).
         append("\n");

      if( _state == ST_PROCESSING_COPYTO )sb.append( drawProgressBar( 40 , _currentNumberOfBytes , _totalNumberOfBytes ) ) ;

      pw.print(sb.toString());
   }
   private String drawProgressBar( int width , long cursor , long maxCursor ){
      StringBuffer sb = new StringBuffer() ;
      String skip = "  " ;
      double x    = (double)cursor / (double) maxCursor * width ;
      int    n    = Math.min( (int) x , width ) ;
      int    i    = 0 ;

      sb.append(skip).append("+") ;
      for( i = 0 ; i < width ; i++ )sb.append("-") ;
      sb.append("+\n").append(skip).append("|");

      for(  i = 0 ; i < n ; i++ )sb.append("*") ;
      for( ; i < width ; i++ )sb.append(" ") ;
      sb.append("|\n").append(skip).append("+") ;
      for( i = 0 ; i < width ; i++ )sb.append("-") ;sb.append("+");
      return sb.toString() ;
   }
   public void timer(){
       //_log.info("Timer of "+_core.getName()+" triggered");
   }
   @Override
public String toString(){
      return  _core.getName()+";Status="+_stateStrings[_state]+";m="+_message ;
   }
   /**
     *        PRIVATE PART
     */
   public static class RepositoryEntry implements java.io.Serializable {

       public PnfsId  _pnfsId   = null ;
       public long    _size     = 0 ;
       public String  _info     = null ;
       public boolean _bad      = false ;
       public boolean _precious = false ;
       public boolean _pinned   = false ;
       public boolean _cached   = false ;
       public boolean _locked   = false ;
       public int     _state    = RE_IDLE ;
       public String  _message  = null ;
       public String getModeString(){
          StringBuffer sb = new StringBuffer() ;
          sb.append("<").
             append(_cached?"C":"-").
             append(_precious?"P":"-").
             append(_pinned?"X":"-").
             append(_bad?"E":"-").
             append(_locked?"L":"-").
             append(">");
          return sb.toString();
       }
       @Override
	public String toString(){
          StringBuffer sb = new StringBuffer() ;
          sb.append(_pnfsId.toString()).append(" ").
             append(getModeString()).append(" ").
             append(_size).append(" {").append(_info).append("} ") ;

          return sb.toString();
       }
   }
   private void convertRepositoryListing( String message , File cache , Pattern pattern ) throws IOException {

       StringTokenizer    st  = new StringTokenizer( message , "\n" ) ;
       ObjectOutputStream oos = new ObjectOutputStream( new FileOutputStream( cache ) ) ;

       _totalNumberOfFiles = 0 ;
       _totalNumberOfBytes = 0L ;
       try{
          while( st.hasMoreTokens() ){

             String line = st.nextToken() ;
             StringTokenizer ss = new StringTokenizer( line ) ;
             int count = ss.countTokens() ;

             if( count != 4 )
                throw new
                IOException("Repository File Syntax Error (wrong token count)");

             RepositoryEntry entry = new RepositoryEntry() ;

             entry._pnfsId = new PnfsId( ss.nextToken() ) ;
             String mode   = ss.nextToken();
             entry._size   = Long.parseLong( ss.nextToken() ) ;
             String tmp    = ss.nextToken() ;

             if( ! tmp.startsWith("si={" ) )
               throw new
               IOException("Repository File Syntax Error (wrong storage info)");

             entry._info = tmp.substring(4,tmp.length()-1);

             if( ( pattern != null ) && ( ! pattern.matcher(entry._info).matches() ) )continue ;

             if( mode.indexOf("E") > -1   )entry._bad      = true ;
             if( mode.indexOf("X") > -1   )entry._pinned   = true ;
             if( mode.startsWith("<-P--") )entry._precious = true ;
             if( mode.startsWith("<C---") )entry._cached   = true ;
             if( mode.startsWith("<--C-")  ||  mode.startsWith("<---S")   )entry._locked   = true ;

             _totalNumberOfFiles ++ ;
             _totalNumberOfBytes += entry._size ;
             oos.writeObject(entry);
          }
       }finally{
          try{ oos.close() ; }catch(IOException eee ){}
       }
   }

}
