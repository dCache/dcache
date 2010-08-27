package dmg.cells.services ;

import   dmg.cells.nucleus.* ;
import   dmg.util.* ;
import   dmg.protocols.ssh.* ;
import   dmg.cells.services.login.* ;
import java.io.* ;
import java.net.* ;
import javax.security.auth.Subject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
  *
  *
  * @author Patrick Fuhrmann
  * @version 0.1, 15 Feb 1998
  */
public class      StreamLoginCell
       extends    CellAdapter
       implements Runnable  {

  private final static Logger _log =
      LoggerFactory.getLogger(StreamLoginCell.class);

  private StreamEngine   _engine ;
  private ControlBufferedReader _in ;
  private PrintWriter    _out ;
  private InetAddress    _host ;
  private Subject         _subject ;
  private Thread         _workerThread ;
  private CellShell      _shell ;
  private String         _destination = null ;
  private boolean        _syncMode    = true ;
  private Gate           _readyGate   = new Gate(false) ;
  private int            _syncTimeout = 10 ;
  private int            _commandCounter = 0 ;
  private String         _lastCommand    = "<init>" ;
  private Reader         _reader  = null ;
  private CellNucleus    _nucleus = null ;
  public StreamLoginCell( String name , StreamEngine engine ){
     super( name ) ;

     _engine  = engine ;
     _nucleus = getNucleus() ;
     _reader  = engine.getReader() ;
     _in      = new ControlBufferedReader( _reader ) ;
     _out     = new PrintWriter( engine.getWriter() ) ;
     _subject    = engine.getSubject();
     _host    = engine.getInetAddress() ;

     _shell        = new CellShell( _nucleus ) ;
     _destination  = getCellName() ;
     _workerThread = _nucleus.newThread( this , "worker" ) ;
     _workerThread.start() ;

     useInterpreter(false) ;
  }
  public void run(){
    if( Thread.currentThread() == _workerThread ){
        print( prompt() ) ;
        _in.onControlC("interrupted") ;
        while( true ){
           try{
               if( ( _lastCommand = _in.readLine() ) == null )break ;
               _commandCounter++ ;
               if( execute( _lastCommand ) > 0 ){
                  //
                  // we need to close the socket AND
                  // have to go back to readLine to
                  // finish the ssh protocol gracefully.
                  //
                  try{ _out.close() ; }catch(Exception ee){}
               }else{
                  print( prompt() ) ;
               }
           }catch( IOException e ){
              _log.info("EOF Exception in read line : "+e ) ;
              break ;
           }catch( Exception e ){
              _log.info("I/O Error in read line : "+e ) ;
              break ;
           }

        }
        _log.info( "EOS encountered" ) ;
        _readyGate.open() ;
        kill() ;

    }
  }
  public void cleanUp() {

    _log.info("Clean up called");
    println("");
    try {
        _out.close();
     } catch (Exception ee) {
        _log.warn("ignoring exception on PrintWriter.close {}", ee.toString());
     }
     _workerThread.interrupt() ;
     try {
         if (!_engine.getSocket().isClosed()) {
             _log.info("Close socket");
             _engine.getSocket().close();
         }
     } catch (Exception ee) { ; }
//     _readyGate.check() ;
     _log.info( "finished" ) ;

   }
  public void println( String str ){
     _out.println( str ) ;
     _out.flush() ;
  }
  public void print( String str ){
     _out.print( str ) ;
     _out.flush() ;
  }
   public String prompt(){
      return _destination == null ? " .. > " : (_destination+" > ")  ;
   }
   public int execute( String command ) throws Exception {
      if( command.equals("") )return 0 ;
      if( _destination == null ){
         try{
             print( command( command ) ) ;
             return 0 ;
         }catch( CommandExitException cee ){
             return 1 ;
         }
      }else if( _destination.equals( getCellName() ) ){
         try{
             printObject( _shell.objectCommand( command ) ) ;
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
                 if( ( msg = sendAndWait( msg , 1000*_syncTimeout ) ) == null ){
                    print( "Timeout ... \n" ) ;
                    return 0;
                 }
                 printObject( msg.getMessageObject() ) ;
             }else{
                sendMessage(msg )  ;
                print( "Msg UOID ="+msg.getUOID()+"\n" )  ;
             }
         }catch( Exception ex ){
             print( "Problem : "+ex+"\n" )  ;
             ex.printStackTrace() ;
         }

      }
      return 0 ;

   }
   private void printObject( Object obj ){
      if( obj == null ){
         println( "Received 'null' Object" ) ;
         return ;
      }
      String output = null ;
      if( obj instanceof Object [] ){
         Object [] ar = (Object []) obj ;
         for( int i = 0 ; i < ar.length ; i++ ){
            if( ar[i] == null )continue ;

            print( output = ar[i].toString() ) ;
            if(  ( output.length() > 0 ) &&
                 ( output.charAt(output.length()-1) != '\n' )

               )print("\n") ;
         }
      }else{
         print( output =  obj.toString() ) ;
         if( ( output.length() > 0 ) &&
             ( output.charAt(output.length()-1) != '\n' ) )print("\n") ;
      }

   }
  //
  // the cell implemetation
  //
   public String toString(){ return Subjects.getDisplayName(_subject)+"@"+_host ; }
   public void getInfo( PrintWriter pw ){
     pw.println( "            Stream LoginCell" ) ;
     pw.println( "         User  : "+Subjects.getDisplayName(_subject) ) ;
     pw.println( "         Host  : "+_host ) ;
     pw.println( " Last Command  : "+_lastCommand ) ;
     pw.println( " Command Count : "+_commandCounter ) ;
   }
   public void   messageArrived( CellMessage msg ){

        Object obj = msg.getMessageObject() ;
        println("");
        println( " CellMessage From   : "+msg.getSourceAddress() ) ;
        println( " CellMessage To     : "+msg.getDestinationAddress() ) ;
        println( " CellMessage Object : "+obj.getClass().getName() ) ;
        printObject( obj ) ;

   }
   ///////////////////////////////////////////////////////////////////////////
   //                                                                       //
   // the interpreter stuff                                                 //
   //                                                                       //
   public String fh_set_sync =
                 " Syntax : set sync on|off \n" +
                 "          sets the message send mode to synchronized or\n"+
                 "          asynchronized mode. The default timeout for the\n"+
                 "          sync mode is 10 seconds. Use the 'set timeout'\n"+
                 "          commmand to change this value\n" ;

   public String hh_set_dest    = "local|<destinationCell>" ;
   public String hh_set_sync    = "on|off" ;
   public String hh_set_timeout = "<seconds>" ;
   public String hh_set_echochar = "on|off|<echoChar>" ;
   public String ac_set_echochar_$_1( Args args ){
      String s = args.argv(0) ;
      if( ! ( _reader instanceof SshInputStreamReader ) )
         return "Change of echo not supported by this terminal\n" ;

      SshInputStreamReader r = (SshInputStreamReader)_reader ;
      if( s.equals("off") ){
         r.setEcho(false) ;
         r.setEchoChar( (char)0 ) ;
      }else if( s.equals("on") ){
         r.setEcho(true) ;
      }else {
         r.setEcho(false) ;
         r.setEchoChar( s.charAt(0) ) ;
      }
      return "Done\n" ;
   }

   public String ac_show_timeout( Args args ){
      return "Sync timeout = "+_syncTimeout+" seconds \n" ;
   }
   public String ac_set_timeout_$_1( Args args ) throws Exception {
      _syncTimeout = new Integer( args.argv(0) ).intValue() ;
      return "" ;
   }
   public String ac_set_sync_on( Args args ){
      _syncMode = true ;
      return "" ;
   }
   public String ac_set_sync_off( Args args ){
      _syncMode = false ;
      return "" ;
   }

   public String ac_set_dest_$_1( Args args ){
      if( args.argv(0).equals("local") ){
         _destination = getCellName() ;
      }else{
         _destination = args.argv(0) ;
      }
      return "" ;
   }
   public String ac_exit( Args args ) throws CommandExitException {
      throw new CommandExitException( "" , 0 ) ;
   }



}
