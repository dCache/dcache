//
// $Id: SessionHandler.java,v 1.5 2007-05-24 13:51:15 tigran Exp $
//
package diskCacheV111.doors.dCapV5 ;

import diskCacheV111.vehicles.*;
import diskCacheV111.util.*;


import dmg.cells.nucleus.*;
import dmg.util.*;

import java.util.*;

/**
  * @author Patrick Fuhrmann
  * @version 0.1, Jan 18 2002
  *
  *
  *
  *  
  */
public abstract class SessionHandler extends CommandInterpreter {

   protected VspArgs _vargs     = null ;
   protected int     _sessionId = 0 ;
   protected int     _commandId = 0 ;
   protected boolean _verbose   = false ;
   protected int     _state     = 0 ;
   protected DoorRequestInfoMessage _info   = null ;

   private SessionRoot _sessionRoot = null ;
   private CellAdapter _cell  = null ;
   private String  _status    = "<init>" ;
   private long    _statusSince  = System.currentTimeMillis() ;
   private long    _timestamp    = System.currentTimeMillis() ;
   private int     _uid       = -1 ;
   protected  MessageEventTimer _timer = null ;
   
   protected static final int ANSWER_ARRIVED    = 1 ;
   protected static final int EXCEPTION_ARRIVED = 2 ;
   protected static final int TIMER_EXPIRED     = 3 ;
   protected static final int PERIODIC          = 4 ;
   protected static final int RETRY_TIMER       = 5 ;
   protected static final int SHUTDOWN          = 6 ;

   protected SessionHandler( SessionRoot sessionRoot ,
                             int sessionId , 
                             int commandId , 
                             VspArgs args )  throws Exception {

       _sessionRoot = sessionRoot ;
       _cell        = _sessionRoot.getCellAdapter() ;
       _timer       = _sessionRoot.getTimer() ;
       _sessionId = sessionId ;
       _commandId = commandId ;
       _vargs     = args ;

       _info      = new DoorRequestInfoMessage( 
                          _cell.getNucleus().getCellName()+"@"+
                          _cell.getNucleus().getCellDomainName() ) ;

       try{ _uid = Integer.parseInt( args.getOpt("uid") ) ; }
       catch(Exception ee){}
   }
   protected void sendComment( String comment ){
      String reply = ""+_sessionId+" 1 "+
                     _vargs.getName()+" "+comment ;
      _sessionRoot.println( reply ) ;
      _sessionRoot.say( reply ) ;
   }
   protected String getStringOption( String optName , String defaultValue ){
       String value       = defaultValue ;
       String valueString = (String)_cell.getDomainContext().get("dcap-"+optName) ;
       if( valueString != null )value = valueString ;
       if( ( valueString = _vargs.getOpt(optName) ) != null )value = valueString ;          
       return value ;
   }
   protected long getLongOption( String optName , long defaultValue ){
       long   value       = defaultValue ;
       String valueString = (String)_cell.getDomainContext().get("dcap-"+optName) ;
       if( valueString != null ){
          try{ value = Long.parseLong(valueString) ;
          }catch(Exception ee){}
       }
       valueString = _vargs.getOpt(optName) ;
       if( valueString != null ){
          try{ value = Long.parseLong(valueString) ;
          }catch(Exception ee){}
       }
       return value ;
   }
   protected boolean isLocked(){
       return _cell.getDomainContext().get("dcapLock2") != null ;
   }
   public String hh_send = "<subId> <command> [<returnCode> [<returnMessage>]]" ;
   public String ac_send_$_2_4( Args args )throws Exception {
     StringBuffer sb = new StringBuffer() ;
     sb.append(_sessionId).append(" ").
        append(args.argv(0)).append(" ").
        append(args.argv(1)) ;
        
        if( args.argc() > 2 )sb.append(" ").append(args.argv(2)) ;
        if( args.argc() > 3 )sb.append(" ").append(args.argv(3)) ;
   
     _sessionRoot.println( sb.toString() ) ;
     return "" ;
   }
   public String hh_shutdown = "<returnCode> <returnMessage>" ;
   public String ac_shutdown_$_2( Args args )throws Exception {
      
      sendReply("UserIntervention", 
                Integer.parseInt(args.argv(0)) ,
                args.argv(1) ) ;
      removeUs() ;
      return "Shutdown Done" ;
   }
   protected void sendReply( String tag , int rc , String msg ){

      String problem = null ;
      _info.setTransactionTime( System.currentTimeMillis()-_timestamp);
      if( rc == 0 ){
         problem = ""+_sessionId+
                  " "+_commandId+
                  " "+_vargs.getName()+
                  " ok"   ;

         _sessionRoot.say( tag+" : "+problem ) ;
      }else{
         problem = ""+_sessionId+
                  " "+_commandId+
                  " "+_vargs.getName()+
                  " failed "+rc + 
                  " \""+msg+"\""  ;

         _sessionRoot.esay( tag+" : "+problem ) ;
         _info.setResult( rc , msg ) ;
      }        
      _sessionRoot.println( problem ) ;
      try{
         _cell.sendMessage( 
             new CellMessage( new CellPath("billing") ,
                              _info ) ) ;
      }catch(Exception ee){
         _sessionRoot.esay("Couldn't send billing info : "+ee );
      }
      return ;
   }
   protected void sendReply( String tag , Message msg ){

      sendReply( tag , 
                 msg.getReturnCode() , 
                 msg.getErrorObject().toString() ) ;

   }
   protected synchronized void removeUs(){
      _sessionRoot.removeSession( Integer.valueOf(_sessionId ) ) ;
   }
   protected void setStatus( String status ){
     _status = status ;
     _statusSince = System.currentTimeMillis() ;
   }
   protected void setState( int state , String status ){
      _status = status ;
      _state  = state ;
   }
   public abstract void go();
   protected int getState(){ return _state ; }
   protected String getStatus(){ return _status ; }
   public String toString(){
      return "["+_sessionId+"]["+_uid+"] "+
             _status+"("+
             ( (System.currentTimeMillis()-_statusSince)/1000 )+")" ;
   }
   public void send( CellPath destination ,
                     Object messageObject ,
                     long   timeout  ,
                     MessageTimerEvent receiverObject        ){

      CellMessage msg = new CellMessage( destination , messageObject ) ; 
      _timer.send( msg , timeout , receiverObject ) ;
   }
   public void say( String message ){ 
      _sessionRoot.say( "ID-"+_sessionId+" : "+message ) ; 
   }
   public void esay( String message ){ 
      _sessionRoot.esay( "ID-"+_sessionId+" : "+message ) ; 
   }
}
