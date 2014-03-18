/*
 * CopyManager.java
 *
 * Created on February 18, 2005, 12:56 PM
 */

package diskCacheV111.replicaManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.StringTokenizer;

import diskCacheV111.pools.PoolCellInfo;
import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.Pool2PoolTransferMsg;
import diskCacheV111.vehicles.PoolCheckFileMessage;
import diskCacheV111.vehicles.PoolModifyPersistencyMessage;

import dmg.cells.nucleus.CellAdapter;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellPath;

import org.dcache.cells.CellStub;
import org.dcache.util.Args;
import org.dcache.vehicles.FileAttributes;

import static java.util.concurrent.TimeUnit.MINUTES;

/**
 *
 * @author  patrick
 */

public class CopyManager extends CellAdapter {

   private final static Logger _log =
       LoggerFactory.getLogger(CopyManager.class);

   private final Object     _processLock = new Object() ;
   private final CellStub _poolStub;
   private boolean    _isActive;
   private Thread     _worker;
   private CopyWorker _copy;
   private String    _status      = "IDLE" ;
   private final Parameter      _parameter      = new Parameter();
   private PoolRepository _poolRepository;
    private String _source;
   private String [] _destination;
   private boolean _precious;
   private static class Parameter {
       private PoolCellInfo   _sourceInfo;
       private PoolCellInfo  []  _destinationInfo;
       private long   _started;
       private long   _finished;
       private int    _filesFinished;
       private long   _bytesFinished;
       private int    _filesFailed;
       private long   _bytesFailed;
       private int    _currentlyActive;
       private int    _maxActive       = 4 ;
       private boolean _stopped;
       public void reset(){
           _sourceInfo      = null ;
           _destinationInfo = null ;
           _filesFinished = 0 ;
           _bytesFinished = 0L ;
           _filesFailed   = 0 ;
           _bytesFailed   = 0L ;
           _maxActive     = 4 ;
           _currentlyActive = 0 ;
           _stopped         = false ;
           _started         = System.currentTimeMillis();
           _finished        = 0L ;
       }
       public String toString(){
           return "Progress(f,b)="+_filesFinished+","+_bytesFinished+
                  ";Failed(f,b)="+_filesFailed+","+_bytesFailed+
                  ";Active(c,m)="+_currentlyActive+","+_maxActive;
       }
   }

   public CopyManager( String cellName , String args )
   {
      super( cellName , args ) ;
      _poolStub = new CellStub(this, null, 10, MINUTES);
      start() ;
   }

   private void resetParameter( String from , String to , boolean precious ){
       _source         = from ;
       _destination    = new String[1] ;
       _destination[0] = to ;
       _precious       = precious ;
       _parameter.reset() ;
   }
   private void resetParameter( String from , String [] toArray , boolean precious ){
       _source         = from ;
       _destination    = toArray ;
       _precious       = precious ;
       _parameter.reset() ;
   }
   public static final String hh_stop = "[-interrupt]  # DEBUG only" ;
   public String ac_stop( Args args ){
       boolean inter = args.hasOption("interrupt") ;
       synchronized( _processLock ){
           if( ! _isActive ) {
               throw new
                       IllegalStateException("No copy process active");
           }
           if( _parameter._stopped ) {
               throw new
                       IllegalStateException("Process already stopped");
           }
           _parameter._stopped = true ;

           if( inter ) {
               _worker.interrupt();
           }
       }
       return "Stop initiated "+(inter?"(interrupted)":"") ;
   }
   public static final String hh_drain = " # drains current transfers but doesn't start new transfers" ;
   public String ac_drain( Args args ){
       synchronized( _processLock ){
           if( ! _isActive ) {
               throw new
                       IllegalStateException("No copy process active");
           }

           _parameter._stopped = true ;

           _processLock.notifyAll() ;
       }
       return "Interrupt initiated" ;
   }
   public static final String fh_ls =
     "  ls [options] [pnfsId]\n"+
     "       OPTIONS\n"+
     "          -e :  lists transfers with error state (overwrites all other options)\n"+
     "          -d :  lists 'done' in addition to 'active' transfers\n"+
     "          -w :  lists 'wait' in addition to 'active' transfers\n"+
     "          -a :  lists all transfers (wait,active,done)\n";
   public static final String hh_ls = "[-d] [-w] [-a] [pnfsId]" ;
   public String ac_ls_$_0_1( Args args ){

       boolean w = args.hasOption("w") ;
       boolean d = args.hasOption("d") ;
       boolean a = args.hasOption("a") ;
       boolean e = args.hasOption("e") ;

       PoolRepository rep;
       StringBuilder sb  = new StringBuilder() ;

       synchronized( _processLock ){
           rep = _poolRepository ;
       }
       if( rep == null ) {
           throw new
                   IllegalArgumentException("No pool repository yet [" + _status + "]");
       }

       if( args.argc() > 0 ){
           String pnfsId = args.argv(0) ;
           PoolFileEntry entry = rep.getRepositoryMap().get(pnfsId ) ;
           if( entry == null ) {
               throw new
                       IllegalArgumentException("Transfer not found for " + pnfsId);
           }

           sb.append( entry.toString() ).append("\n");
           return sb.toString() ;
       }
       for (PoolFileEntry entry : rep.getRepositoryMap().values()) {

           String entryString = entry.toString() + "\n";

           if (a) {
               sb.append(entryString);
           } else if (e) {
               if (entry.returnCode != 0) {
                   sb.append(entryString);
               }
           } else {
               switch (entry.state) {
               case PoolFileEntry.TRANSFER:
               case PoolFileEntry.STATE:
                   sb.append(entryString);
                   break;
               case PoolFileEntry.DONE:
                   if (d) {
                       sb.append(entryString);
                   }
                   break;
               case PoolFileEntry.IDLE:
                   if (w) {
                       sb.append(entryString);
                   }
                   break;
               }
           }
       }
       return sb.toString() ;
   }
   @Override
   public void getInfo( PrintWriter pw ){
       pw.println("     Name : "+getCellName() ) ;
       pw.println("    Class : "+this.getClass().getName() ) ;
       pw.println("  Version : $Id$");
       pw.println("     Mode : "+(_isActive?"ACTIVE":"IDLE")+" "+(_parameter._stopped?"STOPPED":"") ) ;
       pw.println("   Status : "+_status) ;

       if( _parameter._started == 0L ) {
           return;
       }
       pw.print(" Transfer : "+_source+" -> ") ;
       for (String s : _destination) {
           pw.print(s + " ");
       }
       pw.println("");
       pw.println("  Started : "+new Date(_parameter._started));
       if( _parameter._finished != 0L ) {
           pw.println(" Finished : " + new Date(_parameter._finished));
       }
       pw.println("    Param : "+_parameter) ;
       Parameter p;
       PoolRepository rep;
       synchronized( _processLock ){
           p = _parameter ;
           rep = _poolRepository ;
       }
       if( ( p == null ) || ( rep == null ) || ( rep.getTotalSize() == 0L ) ) {
           return;
       }
       if( p._started == 0L ) {
           return;
       }

       float percent       = ((float)p._bytesFinished)/(float)rep.getTotalSize() ;
       float percentFailed = ((float)p._bytesFailed)/(float)rep.getTotalSize() ;

       pw.println(" Progress" )  ;

       int maxSize = 40 ;
       int done   = (int)(percent       * (float)maxSize ) ;
       int failed = (int)(percentFailed * (float)maxSize ) ;
       done   = Math.min(done,40) ;
       pw.println(" +----------------------------------------+");
       pw.print(" |");
       int i = 0 ;
       for(  ; i < failed ; i++ ) {
           pw.print("?");
       }
       for(  ; i < done ; i++ ) {
           pw.print("*");
       }
       for( ; i < maxSize ; i++ ) {
           pw.print(" ");
       }
       pw.println("| "+( (int)( percent * 100.0 ) )+" %" );
       pw.println(" +----------------------------------------+");

       long now  = p._finished != 0L ? p._finished : System.currentTimeMillis() ;
       long diff = now - p._started ;
       if( diff == 0L ) {
           return;
       }
       float bytesPerSecond = (float)p._bytesFinished / (float)diff * (float)1000.0 ;
       String [] units = { "Bytes" , "KBytes" , "MBytes" , "GBytes" , "TBytes" } ;
       i = 0 ;
       float value = bytesPerSecond ;
       for(  ; i < units.length ; i++ ) {
           if( value > (float)1024.0 ) {
               value /= (float) 1024.0;
           } else {
               break;
           }
       }
       pw.println( " Average Speed : "+value+" "+units[i]+"/second");


   }
   public static final String hh_copy = "<fromPool> <toPool> [toPool2 [...]] [-max=<maxParallel>] [-precious]" ;
   public String ac_copy_$_2_999( Args args )
   {
       synchronized( _processLock ){

           if( _isActive ) {
               throw new
                       IllegalStateException("Copy process is active");
           }

           int    dests = args.argc() - 1 ;
           String from  = args.argv(0);
           String [] to = new String[dests] ;
           for( int i = 0 ; i < dests ; i++ ) {
               to[i] = args.argv(i + 1);
           }

           resetParameter( from , to , args.hasOption("precious") ) ;

           String max = args.getOpt("max") ;
           if( max != null ) {
               _parameter._maxActive = Integer.parseInt(max);
           }

           _worker = getNucleus().newThread( _copy = new CopyWorker() , "Worker" ) ;
           _worker.start() ;

           _isActive = true ;

       }
       return "" ;
   }
   private  void setStatus( String newStatus ){
       _log.info("STATUS : "+newStatus);
       synchronized( _processLock ){
           _status = newStatus ;
       }
   }
   private class CopyWorker implements Runnable {
       @Override
       public void run() {
           try{
//               setStatus("Waiting for source pool infos");
//               getSourcePoolInfos();
//               setStatus("Waiting for destination pool infos");
//               getDestinationPoolInfos();
               setStatus("Waiting for pool repository of "+_source);
               PoolRepository rep = getExtendedPoolRepository(_source);
               synchronized( _processLock ){
                   _poolRepository = rep ;
               }

               setStatus("Processing "+rep.getFileCount()+" files");

               processFiles(  _poolRepository.getRepositoryMap() ) ;

               setStatus("Done with "+rep.getFileCount()+" files");

           }catch(Exception ee ){
               setStatus("Run  stopped : "+ee);
           }finally{
               synchronized( _processLock ){
                   _isActive = false ;
                   _parameter._finished = System.currentTimeMillis() ;
               }
           }
       }
        private void processFiles( Map<String, PoolFileEntry> map ) throws InterruptedException {

            Thread us        = Thread.currentThread() ;
            int    poolIndex = 0 ;

            for( Iterator<PoolFileEntry> i = map.values().iterator() ; i.hasNext() ;
                 poolIndex = ( poolIndex + 1 ) % _destination.length     ){

               PoolFileEntry entry = i.next() ;
               if( _precious && ! entry.isPrecious() ) {
                   continue;
               }

               //
               // next pool
               //
               entry._destination = _destination[poolIndex] ;

               _log.info("Next entry to process : "+entry);

               synchronized( _processLock ){

                   /////////////////////////////////////////////////////////////
                   //
                   // wait for next file to finish or interrupt
                   //
                   while( ( ! us.isInterrupted() ) &&
                          ( !  _parameter._stopped )           &&
                          ( _parameter._currentlyActive >= _parameter._maxActive ) )
                   //
                   {
                       _processLock.wait();
                   }
                   //
                   ////////////////////////////////////////////////////////////
                   if(  _parameter._stopped ) {
                       break;
                   }

//                   if( us.isInterrupted() )
//                        throw new
//                        InterruptedException("Interrupted in 'startTransfer' loop" ) ;

                   _log.info("Starting query for : "+entry) ;
                   sendQuery( entry )  ;
                   _log.info("Starting transfer Done for : "+entry) ;

               }
           }

           _log.info( _parameter._stopped?"processing stopped":"All files submitted" ) ;

           synchronized( _processLock ){

               _log.info("Waiting for residual transfers to be finished : "+_parameter._currentlyActive );

               while( ( ! us.isInterrupted() ) &&
                      ( _parameter._currentlyActive > 0 ) )

               {
                   _processLock.wait();
               }

               if( us.isInterrupted() ) {
                   throw new
                           InterruptedException("Interrupted in 'adjust status' loop");
               }


           }
           _log.info("Finished");
        }

   }

   private void sendQuery( PoolFileEntry entry ){

       synchronized( _processLock ){

           entry.timestamp = System.currentTimeMillis() ;

           PoolCheckFileMessage query = new PoolCheckFileMessage( _source , entry.getPnfsId() ) ;

           CellMessage msg = new CellMessage( new CellPath(_source) , query ) ;

           _parameter._currentlyActive ++ ;
           try{
               _log.info("sendQuery : sending query for " + entry ) ;
               sendMessage( msg ) ;
           }catch(Exception ee ){
               setEntryFinished( entry , 1 , ee ) ;
               return ;
           }

           entry.state = PoolFileEntry.QUERY_1 ;
       }

   }
   private void sendState( PoolFileEntry entry ){

       synchronized( _processLock ){

           entry.timestamp = System.currentTimeMillis() ;

           PoolModifyPersistencyMessage pool =
                new PoolModifyPersistencyMessage( entry._destination , entry.getPnfsId() , true ) ;

           CellMessage out = new CellMessage( new CellPath(entry._destination) , pool ) ;

           try{
               _log.info("sendQuery : sending query for " + entry ) ;
               sendMessage( out ) ;
               entry.state = PoolFileEntry.STATE ;
           }catch(Exception ee ){
               setEntryFinished( entry , 5 , ee ) ;
           }

       }

   }
   private void startTransfer( PoolFileEntry entry ){
       synchronized( _processLock ){

           entry.timestamp = System.currentTimeMillis() ;

           FileAttributes fileAttributes = new FileAttributes();
           fileAttributes.setPnfsId(entry.getPnfsId());
           Pool2PoolTransferMsg pool2pool =
             new Pool2PoolTransferMsg(_source, entry._destination, fileAttributes);

           CellMessage msg = new CellMessage( new CellPath(entry._destination) , pool2pool ) ;

           try{
               _log.info("startTransfer : sending 'start transfer' for "+entry ) ;
               sendMessage( msg ) ;
           }catch(Exception ee ){
               setEntryFinished( entry , 1 , ee ) ;
               return ;
           }

           entry.state = PoolFileEntry.TRANSFER ;
       }
   }
   private void pool2poolAnswerArrived( Pool2PoolTransferMsg msg ){

       PnfsId pnfsId   = msg.getPnfsId() ;
       String poolName = msg.getDestinationPoolName() ;

       synchronized( _processLock ){

           PoolFileEntry entry = _poolRepository.getRepositoryMap().get( pnfsId.toString() ) ;

           if( entry == null ){
               _log.warn("p2pAnswerArrived : entry not found in rep : "+pnfsId);
               return ;
           }
           _log.info("pool2poolAnswerArrived: "+entry+" -> "+msg.getReturnCode() ) ;
           if( msg.getReturnCode() == 0 ){

               entry.transferOk = true ;

               if( entry.isPrecious() ){

                   _log.info("pool2poolAnswerArrived for "+pnfsId+" Sending precious to "+poolName);
                   PoolModifyPersistencyMessage pool =
                        new PoolModifyPersistencyMessage( poolName , pnfsId , true ) ;

                   CellMessage out = new CellMessage( new CellPath(poolName) , pool ) ;

                   try{
                       sendMessage( out ) ;
                       entry.state = PoolFileEntry.STATE ;
                   }catch(Exception ee ){
                       setEntryFinished( entry , 3 , ee ) ;
                   }
               }else{
                   entry.stateOk = true ;
                   setEntryFinished( entry ) ;

               }
           }else{
               setEntryFinished( entry , 2 , msg.getErrorObject() ) ;
           }
       }
   }
   private void poolModifyPersistencyAnswerArrived( PoolModifyPersistencyMessage msg ){
       PnfsId pnfsId = msg.getPnfsId() ;
       synchronized( _processLock ){
           PoolFileEntry entry = _poolRepository.getRepositoryMap().get( pnfsId.toString() ) ;
           _log.info("poolModifyPersistencyAnswerArrived: "+entry+" -> "+msg.getReturnCode() ) ;
           if( msg.getReturnCode() == 0 ){
               entry.stateOk = true ;
               setEntryFinished( entry ) ;
           }else{
               setEntryFinished( entry , 5 , msg.getErrorObject() ) ;
           }
       }

   }
   private void poolCheckFileAnswerArrived( PoolCheckFileMessage msg ){
       PnfsId pnfsId = msg.getPnfsId() ;
       synchronized( _processLock ){
           PoolFileEntry entry = _poolRepository.getRepositoryMap().get( pnfsId.toString() ) ;
           _log.info("poolCheckFileAnswerArrived: "+entry+" -> "+msg ) ;
           if( msg.getReturnCode() == 0 ){

               if( entry.state == PoolFileEntry.QUERY_1 ){
                   if( msg.getHave() ){
                       _log.info("poolCheckFile answer for initial query ok for "+entry ) ;
                       startTransfer( entry ) ;
                   }else{
                       _log.info("poolCheckFile answer for initial query 'file not found' for "+entry);
                       setEntryFinished(entry);
                   }
               }else if( entry.state == PoolFileEntry.QUERY_2 ){
                   //
                   // not used yet.
                   //
                   if( msg.getHave() ){
                       _log.info("poolCheckFile answer for final query ok for "+entry ) ;
                       sendState( entry ) ;
                   }else{
                       _log.info("poolCheckFile answer for final query 'file not found' for "+entry);
                       setEntryFinished(entry);
                   }
               }else{
                   setEntryFinished( entry , 10 , "checkFileAnswer arrived in illegal state "+entry.state ) ;
               }
           }else{
               setEntryFinished( entry , 5 , msg.getErrorObject() ) ;
           }
       }

   }
   private void setEntryFinished( PoolFileEntry entry ){
       setEntryFinished( entry , 0 , null ) ;
   }
   private void setEntryFinished( PoolFileEntry entry , int rc , Object ro ){
       entry.state        = PoolFileEntry.DONE ;
       entry.returnCode   = rc ;
       entry.returnObject = ro ;
       _parameter._filesFinished ++ ;
       _parameter._bytesFinished += entry.getSize() ;
       if( rc != 0 ){
           _parameter._filesFailed ++ ;
           _parameter._bytesFailed += entry.getSize() ;
       }
       _parameter._currentlyActive -- ;
       _processLock.notifyAll() ;
   }
@Override
public void messageArrived( CellMessage message ){
       Object obj = message.getMessageObject() ;
       if( obj instanceof Pool2PoolTransferMsg  ){
           pool2poolAnswerArrived( (Pool2PoolTransferMsg ) obj );
       }else if( obj instanceof PoolModifyPersistencyMessage ){
           poolModifyPersistencyAnswerArrived( (PoolModifyPersistencyMessage) obj ) ;
       }else if( obj instanceof PoolCheckFileMessage ){
           poolCheckFileAnswerArrived( (PoolCheckFileMessage) obj ) ;
       }else{
           _log.warn("Unexpected message arrived : "+message);
       }
   }
   public PoolRepository getExtendedPoolRepository(String poolName) throws Exception {
       String obj = _poolStub.sendAndWait(new CellPath(poolName), "rep ls -l", String.class);
       Map<String, PoolFileEntry> map = new HashMap<>() ;
       StringTokenizer st = new StringTokenizer(obj , "\n" ) ;
       long total = 0L ;
       int  counter = 0;
       while( st.hasMoreTokens() ){
           String entryString = st.nextToken() ;
           try{
               PoolFileEntry entry = new PoolFileEntry( entryString ) ;
               total += entry.getSize() ;
               counter ++ ;
               map.put( entry.getPnfsId().toString() , entry ) ;
           }catch(Exception ee){
               _log.warn("Invalid format "+entryString);
           }

       }
       return new PoolRepository( map , counter , total )  ;
   }
   private class PoolRepository {
       private final long _totalSize;
       private final int  _fileCount;
       private final Map<String, PoolFileEntry>  _map;
       private PoolRepository( Map<String, PoolFileEntry> map , int counter , long total ){
           _map = map ;
           _fileCount = counter ;
           _totalSize = total ;
       }
       public long getTotalSize(){ return _totalSize ; }
       public int  getFileCount(){ return _fileCount ; }
       public Map<String, PoolFileEntry>  getRepositoryMap(){ return _map ; }
   }

   private class PoolFileEntry {

       private final static int IDLE     = 0 ;
       private final static int TRANSFER = 1 ;
       private final static int STATE    = 2 ;
       private final static int DONE     = 3 ;
       private final static int QUERY_1  = 4 ;
       private final static int QUERY_2  = 5 ;

       private long    _size;
       private boolean _isPrecious;
       private PnfsId  _pnfsId;
       private boolean _exists      = true ;
       private String  _destination;
       //
       // internal only
       //
       private boolean transferOk;
       private boolean stateOk;
       private Object  returnObject;
       private int     returnCode;
       private int     state        = IDLE ;
       private long    timestamp;

       private PoolFileEntry( String fileEntryString ){
           StringTokenizer st = new StringTokenizer(fileEntryString) ;
           _pnfsId     = new PnfsId( st.nextToken() ) ;
           _isPrecious = st.nextToken().startsWith("<-P") ;
           _size       = Long.parseLong( st.nextToken() ) ;
       }
       public long getSize(){ return _size ; }
       public boolean isPrecious(){ return _isPrecious ; }
       public PnfsId  getPnfsId(){ return _pnfsId ; }
       public String toString(){
           StringBuilder sb = new StringBuilder() ;
           sb.append(_pnfsId.toString()).
              append(";size=").append(_size).
              append(";p=").append(_isPrecious).
              append(";d=").append( _destination == null ? "?" : _destination ).
              append(";").
              append( state == IDLE ? "IDLE" :
                      state == TRANSFER ? "TRANSFER" :
                      state == STATE    ? "STATE" :
                      state == DONE     ? "DONE" : "<unknown>" ).
              append(";T=").append(transferOk).
              append(";S=").append(stateOk).
              append(";") ;
              if( returnCode == 0 ){
                  sb.append("OK;") ;
              }else{
                  sb.append("RC={").append(returnCode).append(",").
                     append(returnObject==null?"":returnObject.toString()).
                     append("}");
              }
            return sb.toString();
       }
   }
}
