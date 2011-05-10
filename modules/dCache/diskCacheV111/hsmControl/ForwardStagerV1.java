// $Id: ForwardStagerV1.java,v 1.2 2007-10-26 15:14:14 tigran Exp $

package diskCacheV111.hsmControl ;

import java.util.* ;
import java.io.* ;
import java.text.* ;

import dmg.util.* ;
import dmg.cells.nucleus.* ;

import diskCacheV111.vehicles.* ;
import diskCacheV111.util.* ;

/**
  *  Simple Version of a prestager. It receives stage requests from the
  *  Prestaging doors (dCap, SRM), converts those requests to a
  *  PoolMgrSelectReadPoolMsg, which it sends to the PoolManager.
  *  Though the cell does some bookkeeping it doesn't store the
  *  original request.
  */
public class ForwardStagerV1 extends CellAdapter {

    private final CellNucleus      _nucleus ;
    private final Args             _args    ;
    private final File             _database;

    private SimpleDateFormat _formatter       = new SimpleDateFormat ("MM.dd hh:mm:ss");
    private final Statistics       _statistics      = new Statistics() ;
    private CellPath         _poolManagerPath = new CellPath("PoolManager") ;
    private final Date             _started         = new Date() ;

    private static class Statistics {

        private Date _started                 = new Date() ;
        private long _totalRequests           = 0L ;
        private long _poolManagerReplyOk      = 0L ;
        private long _poolManagerReplyFailed  = 0L ;
        private long _replyToSenderOk         = 0L ;
        private long _replyToSenderFailed     = 0L ;
        private long _unknownMessagesReceived = 0L ;
        private long _sendingToPoolManagerFailed = 0L ;

        private long _currentMinuteTotalRequests  = 0L ;
        private long _previousMinuteTotalRequests = 0L ;

        private long _intervalStarted = System.currentTimeMillis()  ;
        private long _interval        = 60000L ;
        private float _average        = (float)0.0;

        private synchronized void reset(){
           _started                 = new Date() ;
           _totalRequests           = 0L ;
           _poolManagerReplyOk      = 0L ;
           _poolManagerReplyFailed  = 0L ;
           _replyToSenderOk         = 0L ;
           _replyToSenderFailed     = 0L ;
           _unknownMessagesReceived = 0L ;
           _sendingToPoolManagerFailed  = 0L ;
           _currentMinuteTotalRequests  = 0L ;
           _previousMinuteTotalRequests = 0L ;
           _intervalStarted         = System.currentTimeMillis();
           _average                 = (float)0.0;
        }
        private synchronized void sendingToPoolManager( Exception ee ){
           _totalRequests ++ ;
           if( ee != null )_sendingToPoolManagerFailed ++ ;

           updateTimer() ;

           _currentMinuteTotalRequests++ ;
        }
        private synchronized void updateTimer(){
           if( ( System.currentTimeMillis() - _intervalStarted ) > _interval ){
               _previousMinuteTotalRequests = _currentMinuteTotalRequests ;
               _currentMinuteTotalRequests  = 0L ;
               _intervalStarted             = System.currentTimeMillis() ;
               _average = Math.max( _average , getRateInPreviousInterval() ) ;
           }
        }
        private synchronized float getMaxRate(){ return _average ; }
        private synchronized void setInterval( long i ){ _interval = i ; }
        private synchronized float getRateInPreviousInterval(){
           return (float)_previousMinuteTotalRequests / ( (float)_interval/(float)1000.0 ) ;
        }
        private synchronized float getAverageRate(){
             return (float)_totalRequests /   (float)( (System.currentTimeMillis() - _started.getTime() )/1000L)  ;
        }
        private synchronized void poolManagerReply( int rc , Object error ){
            if( rc == 0 ){
               _poolManagerReplyOk ++ ;
            }else{
               _poolManagerReplyFailed ++ ;
            }
        }
        private synchronized void replyToSender( Exception ee ){
           if( ee == null ){
              _replyToSenderOk ++ ;
           }else{
              _replyToSenderFailed ++ ;
           }
        }
        private synchronized void unknownMessage( CellMessage msg ){
           _unknownMessagesReceived ++ ;
        }
        private synchronized long getOutstanding(){
           return ( _totalRequests - _sendingToPoolManagerFailed ) - ( _poolManagerReplyOk +  _poolManagerReplyFailed ) ;
        }
        private synchronized long getTotalFailures(){
           return _sendingToPoolManagerFailed+_poolManagerReplyFailed+_replyToSenderFailed+_replyToSenderFailed;
        }
        public String toString(){
           return "Req="+_totalRequests+
                  ";Outstanding="+getOutstanding()+
                  ";Failures="+getTotalFailures();
        }
    } ;

    public ForwardStagerV1( String name , String  args ) throws Exception {

       super( name , args , false ) ;
       _nucleus = getNucleus() ;
       _args    = getArgs() ;

       try{
          if( _args.argc() < 1 )
            throw new
            IllegalArgumentException("Usage : ... <database> -poolManagerName=<poolManagerName>") ;

          _database = new File( _args.argv(0) ) ;
          if( ! _database.isDirectory() )
             throw new
             IllegalArgumentException( "Not a directory : "+_database);

          String poolManagerName = _args.getOpt("poolManagerName") ;
          if( ( poolManagerName != null ) && ( ! poolManagerName.equals("") ) ){
               _poolManagerPath = new CellPath( poolManagerName ) ;
          }
          say("Using PoolManger : "+_poolManagerPath ) ;

       }catch(Exception e){
          start() ;
          kill() ;
          throw e ;
       }
       useInterpreter( true );

       start();
       export();

    }
    public void getInfo( PrintWriter pw ){

       pw.println( "Generic Information" ) ;
       pw.println( "--------------------" ) ;
       pw.println( "   Stager Class   : "+ this.getClass().getName() ) ;
       pw.println( "   Stager Release : [$Id: ForwardStagerV1.java,v 1.2 2007-10-26 15:14:14 tigran Exp $]" ) ;
       pw.println( "   Stager Started : "+_started ) ;
       pw.println( "   PoolManager    : "+_poolManagerPath ) ;
       pw.println( ""  ) ;
       pw.println("Statistics since "+_statistics._started ) ;
       pw.println("------------------------------------------------") ;
       pw.println("                                 Statistics Started : "+_statistics._started ) ;
       pw.println("  Total Number of Requests Forwarded to PoolManager : "+_statistics._totalRequests ) ;
       pw.println("     Requests, failed to be forwarded toPoolManager : "+_statistics._sendingToPoolManagerFailed ) ;
       pw.println("                     Valid replies from PoolManager : "+_statistics._poolManagerReplyOk ) ;
       pw.println("                   Invalid replies from PoolManager : "+_statistics._poolManagerReplyFailed ) ;
       pw.println("         Messages successfully replied to Requestor : "+_statistics._replyToSenderOk ) ;
       pw.println("         Messages failed to be replied to Requestor : "+_statistics._replyToSenderFailed ) ;
       pw.println("                          Unknown Messages received : "+_statistics._unknownMessagesReceived ) ;
       pw.println("                               Outstanding Messages : "+_statistics.getOutstanding() ) ;
       pw.println("                                       Total Errors : "+_statistics.getTotalFailures() ) ;
       pw.println( ""  ) ;

       _statistics.updateTimer() ;

       pw.println("Rates") ;
       pw.println("-----") ;
       pw.println("  Request Rate ["+(_statistics._interval/1000L)+" sec av.] : "+_statistics.getRateInPreviousInterval()+" requests/second") ;
       pw.println("      Max Rate ["+(_statistics._interval/1000L)+" sec av.] : "+_statistics.getMaxRate()+" requests/second") ;
       pw.println("  Average Rate : "+_statistics.getAverageRate()+" requests/second") ;
    }
    public String toString(){
       return _statistics.toString() ;
    }
    public String hh_stat_reset = "Reset statistics counter" ;
    public String ac_stat_reset( Args args ){

        _statistics.reset() ;

        return "Counter resetted" ;
    }
    public String hh_stat_set_interval = "<seconds> # set the avarage time for rate calculation" ;
    public String ac_stat_set_interval_$_1( Args args ){

        long i = Long.parseLong( args.argv(0) ) ;
        if( i <= 0L )
          throw new
          IllegalArgumentException("Interval must be greater 1");

        _statistics.setInterval( i * 1000L ) ;
        return "New Interval set to "+i+" seconds" ;
    }
    public void messageArrived( CellMessage msg ){

       Object obj = msg.getMessageObject() ;
       //
       // The original Stager Message
       //
       if( obj instanceof StagerMessage ){

          StagerMessage stager = (StagerMessage)obj ;

          say( stager.toString() ) ;
          try{

             sendStageRequest( stager ) ;
             stager.setSucceeded();
             _statistics.sendingToPoolManager(null) ;

          }catch(Exception iiee ){

             stager.setFailed( 33 , iiee ) ;
             esay("Problem in sendStageRequest: "+iiee);
             _statistics.sendingToPoolManager(iiee) ;

          }
          msg.revertDirection() ;
          try{

             sendMessage( msg ) ;
             _statistics.replyToSender(null) ;

          }catch(Exception ee ){

             esay("Problem replying : "+ee ) ;
             _statistics.replyToSender(ee) ;

          }


       }else if( obj instanceof PoolMgrSelectReadPoolMsg ){
          //
          // for bookkeeping only
          //
          PoolMgrSelectReadPoolMsg reply = (PoolMgrSelectReadPoolMsg)obj ;
          _statistics.poolManagerReply( reply.getReturnCode() , reply.getErrorObject() ) ;
          say("PoolManagerReply : "+reply);

       }else{

          esay("Unknown message arrived ("+msg.getSourcePath()+") : "+msg.getMessageObject() ) ;
          _statistics.unknownMessage( msg ) ;

       }
    }
    /**
      * Converts a stager request into a PoolMgrSelectReadPoolMsg message
      * and sends it to the PoolManager.
      *
      */
    private void sendStageRequest( StagerMessage stager ) throws Exception {

        PoolMgrSelectReadPoolMsg request =
          new PoolMgrSelectReadPoolMsg(
               stager.getPnfsId(),
               stager.getStorageInfo(),
               stager.getProtocolInfo(), 0);

        request.setSkipCostUpdate(true);
        request.setReplyRequired(true);

        sendMessage(   new CellMessage(  _poolManagerPath ,  request )  ) ;

    }
    //---------------------------------------------------------------------------------------
    //
    // END OF OFFICIAL STAGER CODE
    //
    //
    //  stage and pin example for Timur
    //
    private Map<PnfsId, ExampleCompanion> _companionMap = new HashMap<PnfsId, ExampleCompanion>() ;
    private class ExampleCompanion implements CellMessageAnswerable {
       private final PnfsId _pnfsId ;
       private final String _host ;
       private final boolean _pin ;
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
          say( "Answer for : "+answer.getMessageObject() ) ;
          Message message = (Message)answer.getMessageObject() ;
          if( message.getReturnCode() != 0 ){

             esay( _status = "Manual stage : "+_pnfsId+" "+message.getErrorObject() ) ;
             return ;
          }
          if( message instanceof PnfsGetStorageInfoMessage ){
             _storageInfo = ((PnfsGetStorageInfoMessage)message).getStorageInfo() ;
             say( "Manual Stager : storageInfoArrived : "+_storageInfo ) ;

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
                esay(_status = "Manual Stage : exception in sending stage req. : "+ee ) ;
                return ;
             }
          }else if( message instanceof PoolMgrSelectReadPoolMsg ){
             PoolMgrSelectReadPoolMsg select = (PoolMgrSelectReadPoolMsg)message;
             say( "Manual Stager : PoolMgrSelectReadPoolMsg : "+select ) ;
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
                   esay(_status = "Manual Stage : exception in sending sticky req. : "+ee ) ;
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
          esay( _status = "Exception for : "+_pnfsId+" : "+exception  ) ;
       }
       public void answerTimedOut( CellMessage request ){
          esay( _status = "Timeout for : "+_pnfsId  ) ;
       }
       public String toString(){
          if( _poolName != null ){
             return _pnfsId.toString()+" Staged at : "+_poolName+" ; "+_status;
          }else{
             return _pnfsId.toString()+" "+_status ;
          }
       }
    }
    public String hh_stage_remove = "<pnfsId> ; DEBUGGING ONLY" ;
    public String ac_stage_remove_$_1(Args args )throws Exception {
       Object x = _companionMap.remove(new PnfsId(args.argv(0)));
       if( x == null )
          throw new
          IllegalArgumentException("Not found : "+_args.argv(0));
       return "Removed : "+args.argv(0) ;
    }
    public String hh_stage_ls = "" ;
    public String ac_stage_ls( Args args )throws Exception {

       StringBuffer sb = new StringBuffer() ;
       for(ExampleCompanion companion : _companionMap.values() ){
          sb.append( companion.toString() ).append("\n");
       }
       return sb.toString() ;
    }
    public String hh_stage_file = "<pnfsId> <destinationHost> ; DEBUGGING ONLY" ;
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
           esay( "Problem sending 'getStorageInfo' : "+ee ) ;
          throw ee ;
       }
       return "" ;
    }
}
