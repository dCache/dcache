// $Id: ScheduledStager.java,v 1.1 2002-05-06 09:00:52 cvs Exp $

package diskCacheV111.hsmControl ;

import java.util.* ;
import java.io.* ;
import java.text.* ;

import dmg.util.* ;
import dmg.cells.nucleus.* ;

import diskCacheV111.vehicles.* ;
import diskCacheV111.util.* ;

public class ScheduledStager extends CellAdapter {
    private CellNucleus _nucleus ;
    private Args        _args ;
    private int         _requests  = 0 ;
    private int         _failed    = 0 ;
    private int         _outstandingRequests = 0 ;
    private File        _database  = null ;
	private StagerDB    _db = null;
    private SimpleDateFormat formatter
         = new SimpleDateFormat ("MM.dd hh:mm:ss");

    public ScheduledStager( String name , String  args ) throws Exception {
       super( name , args , false ) ;
       _nucleus = getNucleus() ;
       _args    = getArgs() ;
       try{
		  _db = new StagerDB();
       }catch(Exception e){
          start() ;
          kill() ;
          throw e ;
       }
       useInterpreter( true );
       _nucleus.newThread( new QueueWatch() , "queueWatch").start() ;
	   _nucleus.newThread( new StageEngine(_db) , "StageEngine").start() ;
       start();
       export();
    }
    private class QueueWatch implements Runnable {
       public void run(){
          say("QueueWatch started" ) ;
          while( ! Thread.currentThread().interrupted() ){
             try{
                Thread.currentThread().sleep(60000);
             }catch(InterruptedException ie ){
                break ;
             }
             _nucleus.updateWaitQueue() ;
          }
          say( "QueueWatch stopped" ) ;
       }
    }
    public String toString(){
       return "Req="+_requests+";Err="+_failed+";" ;
    }
    public void getInfo( PrintWriter pw ){
       pw.println("ScheduledStager : [$Id: ScheduledStager.java,v 1.1 2002-05-06 09:00:52 cvs Exp $]" ) ;
       pw.println("Requests    : "+_requests ) ;
       pw.println("Failed      : "+_failed ) ;
       pw.println("Outstanding : "+_outstandingRequests ) ;
    }
    public void messageArrived( CellMessage msg ){
       Object obj = msg.getMessageObject() ;
       _requests ++ ;
       if( obj instanceof StagerMessage ){
          StagerMessage stager = (StagerMessage)obj ;
          say( stager.toString() ) ;
          _db.insert(stager);
          msg.revertDirection() ;
          try{
             sendMessage( msg ) ;
          }catch(Exception ee ){
             esay("Problem replying : "+ee ) ;
          }
       }else{
          esay("Unknown message arrived ("+msg.getSourcePath()+") : "+
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
          say( "Answer for : "+answer.getMessageObject() ) ;
          _outstandingRequests -- ;
       }
       public void exceptionArrived( CellMessage request , Exception exception ){
          esay( "Exception for : "+_stager+" : "+exception  ) ;
          _outstandingRequests -- ;
       }
       public void answerTimedOut( CellMessage request ){
          esay( "Timeout for : "+_stager  ) ;
          _outstandingRequests -- ;
       }
    }
    private void sendStageRequest( StagerMessage stager ){
        PoolMgrSelectReadPoolMsg request =
          new PoolMgrSelectReadPoolMsg(
               stager.getPnfsId(),
               stager.getStorageInfo(),
               stager.getProtocolInfo(), 0);
        request.setSkipCostUpdate(true);
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
           esay("Failed to send request to PM : "+ee) ;
        }
    }

    private class StageEngine implements Runnable {

	   private StagerDB _db = null;
	   StagerMessage[] msg = null;

	   public StageEngine( StagerDB db) {
	      _db = db;
	   }

       public void run(){

		  long idle;
          say("StageEngine started" ) ;
          while( ! Thread.currentThread().interrupted() ){
             try{
				msg = _db.nextSet((long)15);
				for(int i = 0; i < msg.length; i++) {
					sendStageRequest(msg[i]);
				}
             }catch(Exception e ){
                break ;
             }

          }

          say( "StageEngine stopped" ) ;
       }
    }

}
