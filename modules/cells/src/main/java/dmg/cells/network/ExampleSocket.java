package dmg.cells.network ;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

import dmg.cells.nucleus.Cell;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellNucleus;
import dmg.cells.nucleus.CellVersion;
import dmg.cells.nucleus.ExceptionEvent;
import dmg.cells.nucleus.KillEvent;
import dmg.cells.nucleus.LastMessageEvent;
import dmg.cells.nucleus.MessageEvent;

import org.dcache.util.Version;

/**
  *
  *
  * @author Patrick Fuhrmann
  * @version 0.1, 15 Feb 1998
  */
public class ExampleSocket implements Cell, Runnable {

   private final static Logger _log =
       LoggerFactory.getLogger(ExampleSocket.class);

   private CellNucleus  _nucleus;
   private Thread       _worker;
   private InputStream  _input;
   private OutputStream _output;
   private Socket       _socket ;
   private final Version version = Version.of(this);

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
         _log.debug( " Problem in creating streams : "+nse ) ;
         _nucleus.kill() ;
      }
      if( _input == null ){
         _log.debug( " Problem _input is null ") ;
         throw new IllegalArgumentException( " input is null" ) ;
      }
      if( _output == null ){
         _log.debug( " Problem _input is null " ) ;
         throw new IllegalArgumentException( " output is null" ) ;
      }

   }
   @Override
   public void run(){
      if( Thread.currentThread() == _worker ){
         byte [] b = new byte[1024] ;
         int i;

         try{
            while( ( ( i = _input.read( b ) ) >= 0 ) && ! Thread.interrupted() ) {
                _output.write(b, 0, i);
            }

         }catch( Exception nse ){
               _log.debug( " Problem in i/o : "+nse ) ;
         }
         try{
           _input.close();
           _output.close() ;
           _socket.close() ;
         }catch( Exception nsea ){
               _log.debug( " Problem in i/o : "+nsea ) ;
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
     _log.debug( " CellMessage From   : "+msg.getSourcePath() ) ;
     _log.debug( " CellMessage To     : "+msg.getDestinationPath() ) ;
     _log.debug( " CellMessage Object : "+msg.getMessageObject() ) ;
     _log.debug( "" ) ;

   }
   @Override
   public void   prepareRemoval( KillEvent ce ){
     _log.debug( " prepareRemoval "+ce ) ;
         try{
           _input.close();
           _output.close() ;
           _socket.close() ;
         }catch( Exception nsea ){
               _log.debug( " Problem in i/o : "+nsea ) ;
         }
     _worker.interrupt() ;
   }
   @Override
   public void   exceptionArrived( ExceptionEvent ce ){
     _log.debug( " exceptionArrived "+ce ) ;
   }

   @Override
   public CellVersion getCellVersion()
   {
       return new CellVersion(version);
   }
}
