package dmg.cells.services ;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.Reader;
import java.net.InetAddress;

import dmg.cells.nucleus.CellAdapter;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellNucleus;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.CellShell;
import dmg.cells.services.login.ControlBufferedReader;
import dmg.protocols.ssh.SshInputStreamReader;
import dmg.util.CommandExitException;
import dmg.util.Gate;
import dmg.util.StreamEngine;

import org.dcache.auth.Subjects;
import org.dcache.util.Args;

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
  private String         _destination;
  private boolean        _syncMode    = true ;
  private Gate           _readyGate   = new Gate(false) ;
  private int            _syncTimeout = 10 ;
  private int            _commandCounter;
  private String         _lastCommand    = "<init>" ;
  private Reader         _reader;
  private CellNucleus    _nucleus;
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
  @Override
  public void run(){
    if( Thread.currentThread() == _workerThread ){
        print( prompt() ) ;
        _in.onControlC("interrupted") ;
        while( true ){
           try{
               if( ( _lastCommand = _in.readLine() ) == null ) {
                   break;
               }
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
  @Override
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
     } catch (Exception ee) {
     }
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
   public int execute( String command )
   {
      if( command.equals("") ) {
          return 0;
      }
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
                 if( ( msg = getNucleus().sendAndWait(msg, (long) (1000 * _syncTimeout))) == null ){
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
      String output;
      if( obj instanceof Object [] ){
         Object [] array = (Object []) obj ;
          for (Object o : array) {
              if (o == null) {
                  continue;
              }

              print(output = o.toString());
              if ((output.length() > 0) &&
                      (output.charAt(output.length() - 1) != '\n')

                      ) {
                  print("\n");
              }
          }
      }else{
         print( output =  obj.toString() ) ;
         if( ( output.length() > 0 ) &&
             ( output.charAt(output.length()-1) != '\n' ) ) {
             print("\n");
         }
      }

   }
  //
  // the cell implemetation
  //
   public String toString(){ return Subjects.getDisplayName(_subject)+"@"+_host ; }
   @Override
   public void getInfo( PrintWriter pw ){
     pw.println( "            Stream LoginCell" ) ;
     pw.println( "         User  : "+Subjects.getDisplayName(_subject) ) ;
     pw.println( "         Host  : "+_host ) ;
     pw.println( " Last Command  : "+_lastCommand ) ;
     pw.println( " Command Count : "+_commandCounter ) ;
   }
   @Override
   public void   messageArrived( CellMessage msg ){

        Object obj = msg.getMessageObject() ;
        println("");
        println( " CellMessage From   : "+msg.getSourcePath() ) ;
        println( " CellMessage To     : "+msg.getDestinationPath() ) ;
        println( " CellMessage Object : "+obj.getClass().getName() ) ;
        printObject( obj ) ;

   }
   ///////////////////////////////////////////////////////////////////////////
   //                                                                       //
   // the interpreter stuff                                                 //
   //                                                                       //
   public static final String fh_set_sync =
                 " Syntax : set sync on|off \n" +
                 "          sets the message send mode to synchronized or\n"+
                 "          asynchronized mode. The default timeout for the\n"+
                 "          sync mode is 10 seconds. Use the 'set timeout'\n"+
                 "          commmand to change this value\n" ;

   public static final String hh_set_dest    = "local|<destinationCell>" ;
   public static final String hh_set_sync    = "on|off" ;
   public static final String hh_set_timeout = "<seconds>" ;
   public static final String hh_set_echochar = "on|off|<echoChar>" ;
   public String ac_set_echochar_$_1( Args args ){
      String s = args.argv(0) ;
      if( ! ( _reader instanceof SshInputStreamReader ) ) {
          return "Change of echo not supported by this terminal\n";
      }

      SshInputStreamReader r = (SshInputStreamReader)_reader ;
       switch (s) {
       case "off":
           r.setEcho(false);
           r.setEchoChar((char) 0);
           break;
       case "on":
           r.setEcho(true);
           break;
       default:
           r.setEcho(false);
           r.setEchoChar(s.charAt(0));
           break;
       }
      return "Done\n" ;
   }

   public String ac_show_timeout( Args args ){
      return "Sync timeout = "+_syncTimeout+" seconds \n" ;
   }
   public String ac_set_timeout_$_1( Args args )
   {
      _syncTimeout = new Integer(args.argv(0));
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
