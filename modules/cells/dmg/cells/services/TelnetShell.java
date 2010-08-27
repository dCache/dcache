package dmg.cells.services ;

import  dmg.cells.nucleus.* ;
import  dmg.protocols.telnet.* ;
import  dmg.cells.network.* ;
import  dmg.util.* ;

import  java.util.Date ;
import  java.net.Socket ;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *   The TelnetShell is a simple implementation of the
 *   telnet protocol. The Class seperates the control from
 *   the dataflow and uses different callback methods to
 *   inform the user. If not overloaded the class simply
 *   accepts a connection and sends the input lines to
 *   a shell and returns the output. If CellMessages arrive
 *   they are send to the telnet client.
 *   If the Class is overloaded the client class may
 *   overload the methods checkAuthentication, telnetLine
 *   and telnetControl.
 **
  *
  *
  * @author Patrick Fuhrmann
  * @version 0.1, 15 Feb 1998
  *
 */
public class TelnetShell implements Cell, Runnable   {

   private final static Logger _log =
       LoggerFactory.getLogger(TelnetShell.class);

   private CellNucleus _nucleus   = null ;
   private Thread      _worker    = null ;
   private Object      _readyLock = new Object() ;
   private boolean     _ready     = false ;
   private boolean     _authenticationRequired  = false ;
   private String      _destination = null ;
   private boolean     _syncMode  = true ;

   private TelnetInputStream   _telnetInput ;
   private TelnetOutputStream  _telnetOutput ;
   private Thread              _telnetReceiver ;
   private Socket              _socket ;
   private int                 _telnetState = 0 ;
   private String              _user = null , _password = null ;
   private CellShell           _cellShell ;

   /**
    *   Contructs a new TelnetShell Cell. The constructor is usually
    *   called by a variant of the GNLCell. Overloading this class
    *   means as well providing a constructor with exactly this
    *   signature.
    *   <pre>
    *     public class OtherTelnetShell extends TelnetShell {
    *        OtherTelnetShell( String name , Socket socket ){
    *              super( name , socket ) ;
    *                        .....
    *        }
    *     }
    *   </pre>
    */
   public TelnetShell( String cellName , Socket socket ) throws Exception {
        _TelnetShell( cellName , socket , false ) ;
   }
   public void _TelnetShell( String cellName , Socket socket , boolean auth )
          throws Exception {

      _socket  = socket ;
      _authenticationRequired = auth ;
      _nucleus = new CellNucleus( this , cellName ) ;
      _nucleus.setPrintoutLevel(0) ;

      _telnetInput  = new TelnetInputStream( _socket.getInputStream() ) ;
      _telnetOutput = new TelnetOutputStream( _socket.getOutputStream() ) ;

      _telnetReceiver = new Thread( this ) ;
      _telnetReceiver.start();

      _cellShell    = new CellShell( _nucleus ) ;

      _destination  = _nucleus.getCellName() ;
      if( _authenticationRequired ){
         _telnetOutput.setEcho( true ) ;
         print( "\n   User     : " ) ;
      }else{
         print( prompt() ) ;
         _telnetState = 2 ;
      }

   }
   public String prompt(){
      return _destination == null ? " .. > " : (_destination+" > ")  ;
   }
   /**
   *    Overloading this methods allows to check user and password string
   *    A return of false cancels the connection. Otherwise
   *    the connection proceeds.
   */
   public boolean checkAuthentication( String user , String password ){
      return true ;
   }
   public void println( String str ){
       try {
           _telnetOutput.write(str + "\n");
       } catch (Exception ee) {
           _log.warn("ignoring exception on telnetoutputstream.write {}",
                   ee.toString());
       }
   }
   public void print( String str ){
       try {
           _telnetOutput.write(str);
       } catch (Exception ee) {
           _log.warn("ignoring exception on telnetoutputstream.write {}",
                   ee.toString());
       }
   }
   public void sendControl( byte [] d ){
       try {
           _telnetOutput.write(d);
       } catch (Exception ee) {
           _log.warn("ignoring exception on telnetoutputstream.write {}",
                   ee.toString());
       }
   }
   public String shell( String str ) throws CommandExitException {
     return _cellShell.command( str ) ;
   }
   //
   //
   private void _lineReady( String str ){

      _log.info( "Line received : " + str ) ;

      switch( _telnetState ){
        case 0 :
          _user = str ;
          _telnetOutput.setEcho( false ) ;
          _telnetState = 1 ;
          print( "   Password  : " ) ;
        break ;
        case  1 :
           _password = str ;
           _telnetOutput.setEcho( true ) ;
           _telnetState = 2 ;
           _log.info( " User : "+_user+" ; password : "+_password ) ;
          print( "\n\n" ) ;
          if( ! checkAuthentication( _user , _password ) )finish() ;
          print( prompt() ) ;
        break ;
        case  2 :
           telnetLine( str ) ;
        break ;
      }
   }
   public void telnetControl( byte [] d ){

   }
   public void setEcho( boolean echo ){
      _telnetOutput.setEcho( echo ) ;
   }
   public void telnetLine( String command ) {
     try{
        if( execute( command ) > 0 ){ finish() ; return ; }
        print( prompt() ) ;
     }catch( Exception ee ){
        println( " Exception : "+ee ) ;
     }
   }
   public int execute( String command ) throws Exception {
      if( command.equals("") )return 0 ;
      if( _destination == null ){
         return localExcecute( command ) ;
      }else if( _destination.equals( _nucleus.getCellName() ) ){
         try{
            print( shell( command ) ) ;
         }catch( CommandExitException cee ){
            print( "Shell Exit (code="+cee.getExitCode()+
                                ";msg="+cee.getMessage()+")\n" ) ;
            print( "Back to .. mode\n" ) ;
            _destination = null ;
         }
      }else{
         if( command.equals( "exit" ) ){
            _destination = null ;
            print( "Back to .. mode\n" ) ;
            return 0 ;
         }
         try{
             CellMessage msg = new CellMessage(
                     new CellPath( _destination ) ,
                     command ) ;
           if( _syncMode ){
             msg = _nucleus.sendAndWait( msg , 10000 )  ;
             if( msg == null ){
                print( "Timeout ... \n" ) ;
                return 0;
             }
             Object obj = msg.getMessageObject() ;
             if( obj == null ){
                 print( msg.toString()+"\n" ) ;
                 return 0;
             }
             _log.info( "execute : message object : "+obj.getClass() ) ;
             if( obj instanceof Object [] ){
                Object [] ar = (Object []) obj ;
                for( int i = 0 ; i < ar.length ; i++ ){
                   if( ar[i] == null )continue ;
                   String output = ar[i].toString() ;
                   print( output ) ;
                   if( output.charAt(output.length()-1) != '\n' )print("\n") ;
                }
             }else{
                String output = obj.toString() ;
                print( output ) ;
                if( output.charAt(output.length()-1) != '\n' )print("\n") ;
             }
           }else{

             _nucleus.sendMessage(msg )  ;
             print( "Msg UOID ="+msg.getUOID()+"\n" )  ;
           }
         }catch( Exception ex ){
             print( "Problem : "+ex+"\n" )  ;
             ex.printStackTrace() ;
         }

      }
      return 0 ;

   }
   private int localExcecute( String str ){
      Args args = new Args( str ) ;
      if( args.argc() < 1 ){
         _help() ;
         return 0 ;
      }

      if( args.argv(0).equals( "set" ) ){
          args.shift() ;
          if( args.argc() != 2 ){ _help() ; return 0 ; }
          if( args.argv(0).equals( "sync" ) ){
             if( args.argv(1).equals("on") ){
                _syncMode = true ;
             }else if( args.argv(1).equals("off") ){
                _syncMode = false ;
             }else{ _help() ; return 0 ; }
          }else if( args.argv(0).equals( "destination" ) ||
                    args.argv(0).equals( "dest" )    ){
             if( args.argv(1).equals("local") ){
                _destination = _nucleus.getCellName() ;
             }else{
                _destination = args.argv(1) ;
             }
          }else{ _help() ; return 0 ; }
          print( "OK\n" ) ;
      }else if( args.argv(0).equals( "exit" ) ){
         return 1 ;
      }else{
         _help() ;
      }

      return 0 ;
   }
   private void _help(){
         print( " set dest local\n" ) ;
         print( " set dest <cellAddress>\n" ) ;
         print( " set sync  on|off\n" ) ;
         print( " exit\n" ) ;
   }
   private void _runTelnetReceiver(){
      try{
         Object obj ;
         StringBuffer sb = new StringBuffer() ;

         while( ( obj = _telnetInput.readNext() ) != null ){
           if( obj instanceof Character ){
              char c = ((Character)obj).charValue() ;
              _log.info( "Character arrived : "+c ) ;
              if( c == '\n' ){
                 _lineReady( sb.toString() ) ;
                 sb.setLength(0) ;
                 continue ;
              }
              sb.append( ((Character)obj).charValue() ) ;
           }else if( obj instanceof byte [] ){
              _log.info( "byte array arrived " ) ;
              telnetControl( (byte [])obj ) ;
           }

         }
         _log.info( "EOF encountered" ) ;
      }catch( Exception ioe ){
        _log.info("Exception in _runTelnetReceiver : "+ioe ) ;
      }
      finish();
   }
   public void finish(){
       try {
           _telnetOutput.close();
       } catch (Exception e) {
           _log.warn("ignoring exception on telnetoutputstream.close {}",
                   e.toString());
       }
       _nucleus.kill();
   }
   public void run(){
      if( Thread.currentThread() == _telnetReceiver ){

          _runTelnetReceiver() ;

      }

   }
   public String toString(){ return getInfo() ; }

   public String getInfo(){
     return "Example Cell"+_nucleus.getCellName() ;
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
        println("");
        println( " CellMessage From   : "+msg.getSourceAddress() ) ;
        println( " CellMessage To     : "+msg.getDestinationAddress() ) ;
        Object obj = msg.getMessageObject() ;
        println( " CellMessage Object : "+obj.getClass().getName() ) ;
        if( obj instanceof Object [] ){
           Object [] array = (Object [] ) obj ;
           for( int i = 0 ; i < array.length ; i++ ) {
               println( array[i].toString()) ;
           }
        }else{
           println( obj.toString()) ;
        }
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
     finish() ;
     _log.info( "finished" ) ;
     // this will remove whatever was stored for us
   }
   public void   exceptionArrived( ExceptionEvent ce ){
     _log.info( " exceptionArrived "+ce ) ;
   }
   public static void main( String [] args ){
      new SystemCell( "telnetDomain" ) ;
      new GNLCell( "telnet" , "dmg.cells.services.TelnetShell" , 22112 ) ;

   }

}
