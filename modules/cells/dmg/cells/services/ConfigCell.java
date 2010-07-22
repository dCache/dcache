package dmg.cells.services ;

import  dmg.cells.nucleus.* ;
import  java.util.* ;
import  java.io.* ;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
  *
  *
  * @author Patrick Fuhrmann
  * @version 0.1, 15 Feb 1998
  */
public class ConfigCell implements Cell, Runnable  {

   private final static Logger _log =
       LoggerFactory.getLogger(ConfigCell.class);

   private CellNucleus _nucleus   = null ;
   private Thread      _worker    = null ;
   private Object      _readyLock = new Object() ;
   private boolean     _ready     = false ;
   private String      _configBase ;
   private String      _message   = "Init" ;
   private CellShell   _shell ;
   private Vector      _status       = new Vector() ;
   private Object      _statusLock   = new Object() ;

   public ConfigCell( String cellName , String cellArgs ){

      _configBase = cellArgs ;
      _nucleus = new CellNucleus( this , cellName ) ;

      _shell   = new CellShell( _nucleus ) ;

      _worker  = new Thread( this ) ;
      _worker.start() ;
   }
   public void run(){
     if( Thread.currentThread() == _worker ){
         String filename = _configBase + "/" +
                           _nucleus.getCellDomainName() + ".conf" ;
         String line , answer ;
         setStatus( "Starting on "+filename ) ;
         try{
            BufferedReader in = new BufferedReader(
                                new FileReader( filename ) ) ;
            while( ( line = in.readLine() ) != null ){

               setStatus( line ) ;
               answer = _shell.command( line ) ;
               setStatus( answer ) ;
            }
            in.close() ;
         }catch( Exception e ){
            setStatus( " Exc :  " + e.toString() ) ;
         }
         setStatus( "Ready" ) ;

     }
   }
   public String toString(){ return _message ; }
   private String getStatus(){
      StringBuffer sb = new StringBuffer() ;
      synchronized( _statusLock ){
         for( int i = 0 ; i < _status.size() ; i++ ){
            sb.append( (String)_status.elementAt(i) + "\n" ) ;
         }
      }
      return sb.toString() ;
   }
   private void setStatus( String st ){
     if( st.equals("\n") )return  ;
     _message = st ;
     _log.info( st ) ;
     synchronized( _statusLock ){
       if( _status.size() > 40 ){
          _status.removeElementAt(0) ;
       }
       _status.addElement( st ) ;
     }
   }

   public String getInfo(){
     return getStatus() ;
   }
   public void   messageArrived( MessageEvent me ){

     if( me instanceof LastMessageEvent ){
        _log.info( "Last message received; releasing lock" ) ;
        synchronized( _readyLock ){
            _ready = true ;
            _readyLock.notifyAll();
        }
     }else{
        CellMessage msg = me.getMessage() ;
        _log.info( " CellMessage From   : "+msg.getSourceAddress() ) ;
        _log.info( " CellMessage To     : "+msg.getDestinationAddress() ) ;
        _log.info( " CellMessage Object : "+msg.getMessageObject() ) ;
        _log.info( "" ) ;
     }

   }
   public void   prepareRemoval( KillEvent ce ){
     _log.info( "prepareRemoval received" ) ;
     synchronized( _readyLock ){
        if( ! _ready ){
           _log.info( "waiting for last message to be processed" ) ;
           try{ _readyLock.wait()  ; }catch(InterruptedException ie){}
        }
     }
     _log.info( "finished" ) ;
     // this will remove whatever was stored for us
   }
   public void   exceptionArrived( ExceptionEvent ce ){
     _log.info( " exceptionArrived "+ce ) ;
   }

}
