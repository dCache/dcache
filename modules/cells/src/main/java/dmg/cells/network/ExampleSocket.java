package dmg.cells.network ;

import  dmg.cells.nucleus.* ;
import  java.util.Date ;
import  java.io.* ;
import  java.net.* ;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
  *
  *
  * @author Patrick Fuhrmann
  * @version 0.1, 15 Feb 1998
  */
public class ExampleSocket implements Cell, Runnable {

   private final static Logger _log =
       LoggerFactory.getLogger(ExampleSocket.class);

   private CellNucleus  _nucleus = null ;
   private Thread       _worker  = null ;
   private InputStream  _input   = null ;
   private OutputStream _output  = null ;
   private Socket       _socket ;

   public ExampleSocket( String cellName , Socket socket ){

      _nucleus = new CellNucleus( this , cellName ) ;
      _nucleus.export() ;

      _worker = _nucleus.newThread( this , "I/O Engine" ) ;
      _worker.start() ;

      _socket  = socket ;
      try{
         _input   = _socket.getInputStream() ;
         _output  = _socket.getOutputStream() ;
      }catch( Exception nse ){
         _log.info( " Problem in creating streams : "+nse ) ;
         _nucleus.kill() ;
      }
      if( _input == null ){
         _log.info( " Problem _input is null ") ;
         throw new IllegalArgumentException( " input is null" ) ;
      }
      if( _output == null ){
         _log.info( " Problem _input is null " ) ;
         throw new IllegalArgumentException( " output is null" ) ;
      }

   }
   @Override
   public void run(){
      if( Thread.currentThread() == _worker ){
         byte [] b = new byte[1024] ;
         int i = 0 ;

         try{
            while( ( ( i = _input.read( b ) ) >= 0 ) && ! Thread.interrupted() ) {
                _output.write(b, 0, i);
            }

         }catch( Exception nse ){
               _log.info( " Problem in i/o : "+nse ) ;
         }
         try{
           _input.close();
           _output.close() ;
           _socket.close() ;
         }catch( Exception nsea ){
               _log.info( " Problem in i/o : "+nsea ) ;
         }
         _nucleus.kill();

      }

   }
   @Override
   public String getInfo(){
     return "Example Cell"+_nucleus.getCellName() ;
   }
   @Override
   public void   messageArrived( MessageEvent me ){
     if( me instanceof LastMessageEvent ) {
         return;
     }

     CellMessage msg = me.getMessage() ;
     _log.info( " CellMessage From   : "+msg.getSourceAddress() ) ;
     _log.info( " CellMessage To     : "+msg.getDestinationAddress() ) ;
     _log.info( " CellMessage Object : "+msg.getMessageObject() ) ;
     _log.info( "" ) ;

   }
   @Override
   public void   prepareRemoval( KillEvent ce ){
     _log.info( " prepareRemoval "+ce ) ;
         try{
           _input.close();
           _output.close() ;
           _socket.close() ;
         }catch( Exception nsea ){
               _log.info( " Problem in i/o : "+nsea ) ;
         }
     _worker.interrupt() ;
   }
   @Override
   public void   exceptionArrived( ExceptionEvent ce ){
     _log.info( " exceptionArrived "+ce ) ;
   }

}
