package diskCacheV111.util  ;

import  dmg.cells.nucleus.* ;
import  dmg.util.* ;
import  java.io.* ;

public class MessageEventTestCell extends CellAdapter {
   private String  _cellName = null ;
   private Args    _args     = null ;
   private CellNucleus _nucleus = null ;
   private MessageEventTimer _timer = null ;
   private boolean   _echoOnly = false ;
   private boolean   _silent   = false ;
   private boolean   _expectmore   = false ;
   private int       _replies  = 1 ;
   public MessageEventTestCell( String cellName , String args ){
      super( cellName , args , false ) ;
      _cellName = cellName ;
      _args     = getArgs() ;
      _nucleus  = getNucleus() ;
      _timer    = new MessageEventTimer( _nucleus ) ;
      
      _echoOnly = _args.getOpt("echo") != null ;
      
      useInterpreter( true ) ;
      
         _nucleus.newThread( 
             new Runnable(){
                 public void run(){
                    try{
                       _timer.loop() ;
                    }catch(InterruptedException ie ){
                       esay("Loop interrupted .. ") ;
                    }
                 }   
             } ,
             "loop" 
         ).start() ;

      start() ;
   } 
   private class InternalEvent implements MessageTimerEvent {
      public void event( MessageEventTimer timer ,
                         Object eventObject , 
		         int eventType ){
                         
          say( "Event : type : "+eventType ) ;
          if( eventObject instanceof CellMessage [] ){
             CellMessage [] cellMessage = (CellMessage [])eventObject ;
             for( int i = 0 ; i < cellMessage.length ; i++ ){
                say( "   "+i+" -> "+cellMessage[i].getMessageObject() ) ;
             }  
             if( _expectmore )
             try{
                timer.reschedule( 20000 ) ;
             }catch(IllegalMonitorStateException ime ){
                esay("Reschedule failed : "+ime ) ;
             } 
          }else if( eventObject instanceof Long ){
             say("Timer expired "+eventObject ) ;
             timer.addTimer(null,eventObject,this,((Long)eventObject).longValue());
          }else{
             say( "Timer expired   Object : "+eventObject ) ;
            
          }         
      }
   }
   public String hh_send = "<string> <timeout> [<destination>]" ;
   public String ac_send_$_2_3( Args args )throws Exception {
      CellPath path = new CellPath( args.argc() > 2 ? args.argv(2) : "x" ) ;
      String countString = args.getOpt("count") ;
      int count = 1 ;
      if( countString != null ){
         try{
            count = Integer.parseInt(countString) ;
         }catch(Exception ee){}
      }
      for( int i = 0 ; i < count ; i++ ){
         CellMessage msg = new CellMessage( 
                 path ,
                 new IllegalArgumentException(args.argv(0)+"-"+i) ) ;
         _timer.send( msg , Long.parseLong(args.argv(1)) , new InternalEvent() ) ;
      }
      return "Sent : "+count ;
   }
   public void getInfo( PrintWriter pw ){
      pw.println( " timer : "+_timer ) ;
   }
   public String hh_reset = "  # resets the counters" ;
   
   public String ac_reset( Args args ){
      return "Done" ;
   }
   public String hh_add_timer = "<timeout> [-key=<key>] [-periodic]" ;
   public String ac_add_timer_$_1( Args args )throws Exception {
      long timerTimeout = Long.parseLong( args.argv(0) ) ;
      
      _timer.addTimer( args.getOpt("key") , 
                       args.getOpt("periodic")!= null ? new Long(timerTimeout) : null ,
                       new InternalEvent() , 
                       timerTimeout ) ;
      
      return "Scheduled now + " + timerTimeout+ " msec's" ; 
   }
   public String hh_remove_timer = "<timerPrivateKey>" ;
   public String ac_remove_timer_$_1( Args args )throws Exception {
      return _timer.removeTimer(args.argv(0)) ? "Ok" : "Failed" ;
   }
   public String ac_silent_$_1( Args args )throws Exception {
      _silent = args.argv(0).equals("on") ;
      return "Silent set to : "+_silent ;
   }
   public String ac_expectmore_$_1( Args args )throws Exception {
      _expectmore = args.argv(0).equals("on") ;
      return "Expectmore set to : "+_expectmore ;
   }
   public String ac_replies_$_1( Args args )throws Exception {
   
     _replies = Integer.parseInt( args.argv(0) ) ;
     return "Set number of replies to : "+_replies  ;
   }
   public void messageToForward( CellMessage msg ){
      say("Got message to forward : "+msg ) ;
   }
   public void messageArrived( CellMessage msg ){
     if( _echoOnly ){
        if( ! _silent ){
           msg.revertDirection() ;
           try{
           
              Object messageObject = 
                  new IllegalArgumentException("Reply "+msg.getMessageObject());
              for( int i = 0 ; i < _replies ; i++ ){
                 msg.setMessageObject(messageObject );
                 sendMessage(msg) ;
                 say("Message replied "+i ) ;
              }
           }catch(Exception ee ){
              esay("PANIC : can't return message to sender" ) ;
           }
        }else{
           say("Reply suppressed");
        }
     }else{
        say( "Message send to timer : "+msg.getMessageObject() ) ;
        _timer.messageArrived(msg) ;
     }
   }

} 
