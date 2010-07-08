// $Id: DummyStager.java,v 1.2 2002-06-24 06:02:17 cvs Exp $

package diskCacheV111.hsmControl ;

import java.util.* ;
import java.io.* ;
import java.text.* ;

import dmg.util.* ;
import dmg.cells.nucleus.* ;

import diskCacheV111.vehicles.* ;
import diskCacheV111.util.* ;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DummyStager extends CellAdapter {

    private final static Logger _log =
        LoggerFactory.getLogger(DummyStager.class);

    private CellNucleus _nucleus ;
    private Args        _args ;
    private int         _requests  = 0 ;
    private int         _failed    = 0 ;
    private int         _outstandingRequests = 0 ;
    private File        _database  = null ;
    private SimpleDateFormat formatter
         = new SimpleDateFormat ("MM.dd hh:mm:ss");

    public DummyStager( String name , String  args ) throws Exception {
       super( name , args , false ) ;
       _nucleus = getNucleus() ;
       _args    = getArgs() ;
       try{
          if( _args.argc() < 1 )
            throw new
            IllegalArgumentException("Usage : ... <database>") ;

          _database = new File( _args.argv(0) ) ;
          if( ! _database.isDirectory() )
             throw new
             IllegalArgumentException( "Not a directory : "+_database);
       }catch(Exception e){
          start() ;
          kill() ;
          throw e ;
       }
       useInterpreter( true );
       _nucleus.newThread( new QueueWatch() , "queueWatch").start() ;
       start();
       export();
    }
    private class QueueWatch implements Runnable {
       public void run(){
          _log.info("QueueWatch started" ) ;
          while( ! Thread.currentThread().interrupted() ){
             try{
                Thread.currentThread().sleep(60000);
             }catch(InterruptedException ie ){
                break ;
             }
             _nucleus.updateWaitQueue() ;
          }
          _log.info( "QueueWatch stopped" ) ;
       }
    }
    public String toString(){
       return "Req="+_requests+";Err="+_failed+";" ;
    }
    public void getInfo( PrintWriter pw ){
       pw.println("DummyStager : [$Id: DummyStager.java,v 1.2 2002-06-24 06:02:17 cvs Exp $]" ) ;
       pw.println("Requests    : "+_requests ) ;
       pw.println("Failed      : "+_failed ) ;
       pw.println("Outstanding : "+_outstandingRequests ) ;
    }
    public void messageArrived( CellMessage msg ){
       Object obj = msg.getMessageObject() ;
       _requests ++ ;
       if( obj instanceof StagerMessage ){
          StagerMessage stager = (StagerMessage)obj ;
          _log.info( stager.toString() ) ;
          try{
             sendStageRequest( stager ) ;
             stager.setSucceeded();
          }catch(Exception iiee ){
             stager.setFailed( 33 , iiee ) ;
             _log.warn("Problem in sendStageRequest: "+iiee);
          }
          msg.revertDirection() ;
          try{
             sendMessage( msg ) ;
          }catch(Exception ee ){
             _log.warn("Problem replying : "+ee ) ;
          }
       }else{
          _log.warn("Unknown message arrived ("+msg.getSourcePath()+") : "+
               msg.getMessageObject() ) ;
         _failed ++ ;
       }
    }
    private class StageCompanion implements CellMessageAnswerable {
       private StagerMessage _stager = null ;
       private StageCompanion( StagerMessage stager ){
         _stager = stager ;
       }
       public void answerArrived( CellMessage request , CellMessage answer ){
          _log.info( "Answer for : "+answer.getMessageObject() ) ;
          _outstandingRequests -- ;
       }
       public void exceptionArrived( CellMessage request , Exception exception ){
          _log.warn( "Exception for : "+_stager+" : "+exception  ) ;
          _outstandingRequests -- ;
       }
       public void answerTimedOut( CellMessage request ){
          _log.warn( "Timeout for : "+_stager  ) ;
          _outstandingRequests -- ;
       }
    }
    private void sendStageRequest( StagerMessage stager ){
        PoolMgrSelectReadPoolMsg request =
          new PoolMgrSelectReadPoolMsg(
               stager.getPnfsId(),
               stager.getStorageInfo(),
               stager.getProtocolInfo(), 0);
        try{
            sendMessage(
               new CellMessage(
                        new CellPath("PoolManager") ,
                        request ) ,
               true , true ,
               new StageCompanion( stager ) ,
               1*24*60*60*1000
                       ) ;
             _outstandingRequests ++ ;
        }catch(Exception ee ){
           _log.warn("Failed to send request to PM : "+ee) ;
        }
    }
    //
    //  stage and pin example for Timur
    //
    private HashMap _companionMap = new HashMap() ;
    private class ExampleCompanion implements CellMessageAnswerable {
       private PnfsId _pnfsId = null ;
       private String _host   = null ;
       private boolean _pin   = false ;
       private StorageInfo _storageInfo = null ;
       private String      _status = "<WaitingForStorageInfo>" ;
       private String      _poolName = null ;
       private ExampleCompanion( PnfsId pnfsId , String host , boolean pin ){
         _pnfsId = pnfsId ;
         _host   = host ;
         _pin    = pin ;
         synchronized( _companionMap ){
            if( _companionMap.get(pnfsId) != null )
               throw new
               IllegalArgumentException( "Staging "+_pnfsId+" in progess");

             _companionMap.put( pnfsId , this ) ;
         }
       }
       public void setStatus(String message ){_status = message ;}
       public void answerArrived( CellMessage req , CellMessage answer ){
          _log.info( "Answer for : "+answer.getMessageObject() ) ;
          Message message = (Message)answer.getMessageObject() ;
          if( message.getReturnCode() != 0 ){

             _log.warn( _status = "Manual stage : "+_pnfsId+" "+message.getErrorObject() ) ;
             return ;
          }
          if( message instanceof PnfsGetStorageInfoMessage ){
             _storageInfo = ((PnfsGetStorageInfoMessage)message).getStorageInfo() ;
             _log.info( "Manual Stager : storageInfoArrived : "+_storageInfo ) ;

             DCapProtocolInfo pinfo = new DCapProtocolInfo( "DCap",3,0,_host,0) ;
             PoolMgrSelectReadPoolMsg request =
               new PoolMgrSelectReadPoolMsg(
                    _pnfsId,
                    _storageInfo ,
                    pinfo , 0);
             try{
                sendMessage(
                   new CellMessage(
                            new CellPath("PoolManager") ,
                            request ) ,
                   true , true ,
                   this ,
                   1*24*60*60*1000
                           ) ;
                _status = "<WaitingForStage>" ;
             }catch(Exception ee ){
                _log.warn(_status = "Manual Stage : exception in sending stage req. : "+ee ) ;
                return ;
             }
          }else if( message instanceof PoolMgrSelectReadPoolMsg ){
             PoolMgrSelectReadPoolMsg select = (PoolMgrSelectReadPoolMsg)message;
             _log.info( "Manual Stager : PoolMgrSelectReadPoolMsg : "+select ) ;
             _poolName = select.getPoolName() ;
             if( _pin ){
                PoolSetStickyMessage sticky =
                   new PoolSetStickyMessage( _poolName , _pnfsId , true ) ;
                try{
                   sendMessage(
                      new CellMessage(
                               new CellPath(_poolName) ,
                               sticky ) ,
                      true , true ,
                      this ,
                      60*1000
                              ) ;
                   _status = " (sticky) assumed O.K." ;
                }catch(Exception ee ){
                   _log.warn(_status = "Manual Stage : exception in sending sticky req. : "+ee ) ;
                   return ;
                }
             }else{
                _status = "O.K." ;
             }
          }else if( message instanceof PoolSetStickyMessage ){
             //
             // will no come
             //
             _status = " (sticky) O.K." ;
          }
       }
       public void exceptionArrived( CellMessage request , Exception exception ){
          _log.warn( _status = "Exception for : "+_pnfsId+" : "+exception  ) ;
       }
       public void answerTimedOut( CellMessage request ){
          _log.warn( _status = "Timeout for : "+_pnfsId  ) ;
       }
       public String toString(){
          if( _poolName != null ){
             return _pnfsId.toString()+" Staged at : "+_poolName+" ; "+_status;
          }else{
             return _pnfsId.toString()+" "+_status ;
          }
       }
    }
    public String hh_stage_remove = "<pnfsId>" ;
    public String ac_stage_remove_$_1(Args args )throws Exception {
       Object x = _companionMap.remove(new PnfsId(args.argv(0)));
       if( x == null )
          throw new
          IllegalArgumentException("Not found : "+_args.argv(0));
       return "Removed : "+args.argv(0) ;
    }
    public String hh_stage_ls = "" ;
    public String ac_stage_ls( Args args )throws Exception {
       Iterator i = _companionMap.values().iterator() ;
       StringBuffer sb = new StringBuffer() ;
       while( i.hasNext() ){
          sb.append( i.next().toString() ).append("\n");
       }
       return sb.toString() ;
    }
    public String hh_stage_file = "<pnfsId> <destinationHost>" ;
    public String ac_stage_file_$_2( Args args ) throws Exception {
        PnfsId pnfsId = new PnfsId( args.argv(0) ) ;
        String host   = args.argv(1) ;
        boolean pin   = args.getOpt("pin") != null ;

        ExampleCompanion companion = new ExampleCompanion(pnfsId,host,pin) ;

       PnfsGetStorageInfoMessage storageInfoMsg =
              new PnfsGetStorageInfoMessage( pnfsId ) ;

       try{
          sendMessage( new CellMessage(
                             new CellPath("PnfsManager") ,
                             storageInfoMsg ) ,
                       true , true ,
                       companion ,
                       3600 * 1000 ) ;
       }catch(Exception ee ){
           companion.setStatus("Problem sending 'getStorageInfo' : "+ee );
           _log.warn( "Problem sending 'getStorageInfo' : "+ee ) ;
          throw ee ;
       }
       return "" ;
    }
}
