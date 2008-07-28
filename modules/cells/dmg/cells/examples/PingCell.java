package dmg.cells.examples ;

import  dmg.cells.nucleus.* ;
import  java.util.Date ;

import  dmg.util.* ;
import  dmg.cells.network.* ;

/**
  *  
  *
  * @author Patrick Fuhrmann
  * @version 0.1, 15 Feb 1998
  */
public class PingCell implements Cell , Runnable {

   private CellNucleus _nucleus         = null ;
   private int         _messageSent     = 0 ;
   private int         _messageReceived = 0 ;
   private long        _delay           = 1000 ;
   private int         _size            = 100 ;
   private Gate        _finalGate       = new Gate( false ) ;
   private String      _destination     = null ;
   private Thread      _sendThread      = null ;
   private Exception   _exception       = null ;
   
   public PingCell( String cellName , String argString ){
   
      Args args = new Args( argString ) ;
      if( args.argc() < 3 )
         throw new IllegalArgumentException( 
                   "Usage : ... <destCell> <msecDelay> <size>" ) ;
      
      _destination = args.argv(0) ;
      _delay       = new Integer( args.argv(1)).intValue() ;
      _size        = new Integer( args.argv(2)).intValue() ;
      
      _nucleus = new CellNucleus( this , cellName ) ;
      _nucleus.export() ;
      
      _sendThread = new Thread( this ) ;
      _sendThread.start() ;
            
   }
   public PingCell( String cellName ){
   
      _nucleus = new CellNucleus( this , cellName ) ;
      _nucleus.export() ;
            
   }
   public void run(){
     if( Thread.currentThread() == _sendThread ){
        while( ! Thread.interrupted() ){
           try{
              _nucleus.sendMessage(
                   new CellMessage( 
                     new CellPath( _destination ) , 
                     new PingMessage(_size) 
                                   ) ) ;
                   
              _messageSent++ ;
              
              Thread.sleep( _delay ) ;
              
           
           }catch( Exception e ){
             _exception = e ;
             break ;
           }
        
        }
     
     
     }
   
   }
   public String getInfo(){
     StringBuffer sb = new StringBuffer() ;
     sb.append( " Delay Echo Cell     : "+_nucleus.getCellName() + "\n" ) ;
     sb.append( " Ping interval       : "+_delay + " msec\n" ) ;
     sb.append( " Ping size           : "+_size + " bytes\n" ) ;
     sb.append( " Destination         : "+_destination + "\n" ) ;
     sb.append( " Messages Sent       : "+_messageSent + "\n" ) ;
     sb.append( " Messages Received   : "+_messageReceived + "\n" ) ;
     if( _exception != null ){
       sb.append( " Stopped because   : "+_exception + "\n" ) ;
     }
     return sb.toString() ;
   }
   public String toString(){ 
        return "Dest="+_destination+
               ";Delay="+_delay ;
   }
   public void   messageArrived( MessageEvent me ){
   
     _messageReceived ++ ;
     
//     _nucleus.say( " messageArrived : "+me.getClass() ) ;
     
     if( me instanceof LastMessageEvent ){
     
        _nucleus.say( "last message received; opening final gate" ) ;
        _finalGate.open() ;
        
     }else{
        CellMessage msg    = me.getMessage() ;
        CellPath    source = msg.getSourcePath() ;
        Object      obj    = msg.getMessageObject() ;
        
        _nucleus.say( " ---------------------------------------------------" ) ;
        _nucleus.say( " CellMessage From         : "+ source ) ; 
        _nucleus.say( " CellMessage Object Class : "+ obj.getClass().toString() ) ;
        _nucleus.say( " CellMessage Object Info  : "+ obj.toString() ) ;
        _nucleus.say( " ---------------------------------------------------" ) ;
        
        
     }
     
   }
   public void   prepareRemoval( KillEvent ce ){
     _nucleus.say( "prepareRemoval : waiting for final gate to open" ) ;
     _finalGate.check() ;
     _nucleus.say( "prepareRemoval : final gate passed" ) ;
     _sendThread.interrupt() ;
     // this will remove whatever was stored for us
   }
   public void   exceptionArrived( ExceptionEvent ce ){
     _nucleus.say( " exceptionArrived "+ce ) ;
   }

}
 
