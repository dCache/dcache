package diskCacheV111.util ;
import dmg.cells.nucleus.* ;
import dmg.cells.network.* ;
import dmg.util.*;
import java.io.*;
import java.util.*;
import java.net.* ;


public class PingIt extends CellAdapter implements Runnable {
   private CellNucleus _nucleus = null ;
   private Thread      _worker  = null ;
   private Args        _args    = null ;
   private long        _replyTimeout = 5000 ;
   private long        _sleepTime    =10000 ;
   private String      _mailAddress  = null ;
   private String      _destination  = null ;
   private boolean     _reachable    = true ;
   private Date        _timestamp    = new Date() ;
   private String      _lastMessage  = "<none>" ;
   /**
     *  PingIt -d=<destinationCell> -m=<mailAddress>
     *         -rt=<replyTimeout> -st=<sleepTime>
     */
   public PingIt(  String cellName , String args ){
      super( cellName , args , false ) ;
      _nucleus  = getNucleus() ;
      _args     = getArgs() ;

      scanArgs( _args ) ;

      useInterpreter( true ) ;
      _worker =  _nucleus.newThread(this,"ping");
      _worker.start() ;
       start() ;
   }
   private void scanArgs( Args args ){
      String value = null ;
      if( (( value = args.getOpt("d") ) != null ) && ( value.length() > 0 )){
         _destination = value ;
      }
      if( (( value = args.getOpt("m") ) != null ) && ( value.length() > 0 )){
         _mailAddress = value ;
      }
      if( (( value = args.getOpt("rt") ) != null ) && ( value.length() > 0 )){
         try{  _replyTimeout = Long.parseLong( value ) ; }
         catch(Exception ee){}
      }
      if( (( value = args.getOpt("st") ) != null ) && ( value.length() > 0 )){
         try{  _sleepTime = Long.parseLong( value ) ; }
         catch(Exception ee){}
      }

   }
   public String ac_set_mailaddress_$_1( Args args )throws Exception {
      _mailAddress = args.argv(0) ;
      return "Mailaddress set to "+_mailAddress  ;

   }
   public String ac_set_destination_$_1(Args args )throws Exception {
      _destination = args.argv(0) ;
      return "Destination Cell set to "+_destination ;
   }
   public String ac_set_replytimeout_$_1( Args args )throws Exception{
     long to = Long.parseLong( args.argv(0) ) ;
     if( to < 1 )
        throw new
        IllegalArgumentException("Timeout value must be > 0");
     _replyTimeout = to * 1000 ;
     return "Reply Timeout set to "+to+" seconds" ;
   }
   public String ac_set_sleeptime_$_1( Args args )throws Exception {
     long to = Long.parseLong( args.argv(0) ) ;
     if( to < 1 )
        throw new
        IllegalArgumentException("Time value must be > 0");
     _sleepTime = to * 1000 ;
     return "Sleep Time set to "+to+" seconds" ;
   }
   public void getInfo( PrintWriter pw ){
     pw.println( "Destination Cell : "+(_destination==null?"<none>":_destination) ) ;
     pw.println( "Mail To          : "+(_mailAddress==null?"<none>":_mailAddress) ) ;
     pw.println( "Reply Timeout    : "+(_replyTimeout/1000)+" seconds" ) ;
     pw.println( "Sleep Time       : "+(_sleepTime/1000)+" seconds" ) ;
     pw.println( "Status           : "+( _reachable ? "Ok" : "Down" ) ) ;
     pw.println( "Last Message     : "+_lastMessage ) ;
   }
   public void run(){
     say("Ping started");

     while(! Thread.currentThread().interrupted() ){
        try{

           Thread.currentThread().sleep(_sleepTime) ;

           String destination = _destination ;
           if( destination == null )continue ;

           CellPath path = new CellPath( destination ) ;

           CellMessage reply = _nucleus.sendAndWait(
                                  new CellMessage( path , new PingMessage() ) ,
                                  _replyTimeout ) ;


           if( reply == null ){
              failed( "no reply : "+destination ) ;
              continue ;
           }else if( ! ( reply.getMessageObject() instanceof PingMessage ) ){
              failed( "weird obj : "+reply.getMessageObject().getClass().getName() ) ;
              continue;
           }
           isOk();
        }catch(InterruptedException ie ){
           failed("interrupted");
           break ;
        }catch(Exception ie ){
           failed("excp : "+ie ) ;
        }
     }
     say("Ping Done");
     kill() ;
   }
   private void failed( String message ){
      _lastMessage = message ;
      if( _reachable ){
         _timestamp = new Date() ;
         sendMail(message) ;
         esay(message) ;
         _reachable = false ;
      }

   }
   private void isOk(){
      if( ! _reachable ){
         _timestamp = new Date() ;
         sendMail("ok") ;
         esay("ok") ;
         _reachable = true ;
      }

   }
   private void sendMail(String message){
      String mailAddress = _mailAddress ;
      if( mailAddress == null )return ;
      URL         url = null ;
      PrintWriter pw  = null ;
      try{
         url = new URL( "mailto:"+mailAddress ) ;
         URLConnection con = url.openConnection() ;
         con.connect() ;
         pw = new PrintWriter( new OutputStreamWriter( con.getOutputStream() ) ) ;
         pw.println("Subject: "+_nucleus.getCellName()+"@"+_nucleus.getCellDomainName());
         pw.println(message);
      }catch(Exception ee){
         esay("Failed to send mail to <"+mailAddress+"> : "+ee);
      }finally{
         try{ pw.close() ; }catch(Exception ie){}
      }
   }
}

