// $Id: DCapDoor.java,v 1.17 2007-04-05 19:44:07 podstvkv Exp $

package diskCacheV111.doors ;

import diskCacheV111.util.VspArgs;
import java.io.BufferedReader;
import java.io.PrintWriter;
import java.io.Reader;

import dmg.cells.nucleus.CellAdapter;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellNucleus;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.CellVersion;
import dmg.security.CellUser;
import dmg.util.Args;
import dmg.util.CommandExitException;
import dmg.util.KeepAliveListener;
import dmg.util.StreamEngine;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
  * @author Patrick Fuhrmann
  * @version 1.0, Aug 04 2001
  *
  *   Simple Template for a Door. It runs the DiskCacheV111
  *   Protocol V3 version.
  *
  *
  *
  */
public class      DCapDoor
       extends    CellAdapter
       implements Runnable, KeepAliveListener           {

    private StreamEngine   _engine;
    private BufferedReader _in;
    private PrintWriter    _out;
    private String         _host;
    private CellUser         _username;
    private Thread         _workerThread , _anyThread ;
    private int            _commandCounter = 0;
    private String         _lastCommand    = "<init>";
    private Reader         _reader         = null;
    private CellNucleus    _nucleus        = null ;
    private boolean        _dcapLock       = true ;
    private String         _authenticator  = null ;

    /**
     * DCAP command interpreter.
     */
    private final DcapProtocolInterpreter _interpreter;

    /////////////////////////////////////////////////////////////////////
    //
    //         the constructor
    //
    public DCapDoor( String name , StreamEngine engine , Args args )
           throws Exception {
	//
        // the cell stuff
        //
        super( name , DCapDoor.class.getName(), args , false );
        _nucleus = getNucleus() ;

        try{
           //
           // all we need, to talk to the client.
           //
	   _engine   = engine;
	   _reader   = engine.getReader();
	   _in       = new BufferedReader( _reader );
	   _out      = new PrintWriter( engine.getWriter() );
	   _username = engine.getUserName();
	   _host     = engine.getInetAddress().toString();

           _authenticator = args.getOpt("authenticator") ;
           _authenticator = _authenticator == null ?
                            "pam" : _authenticator ;

           String user = _username.getName();
           if( args.getOpt("keepPrincipal") == null ){
              int p = user.indexOf('@');
              user = p < 0 ? user : user.substring(0,p);
           }
           int p = user.indexOf(':');
           if( p < 0 ){
              _username.setName(user) ;
           }else{
              String password = "*" ;
              _username.setName( user.substring(0,p) );
              password   = user.length() > (p+1) ?
                           user.substring(p+1) :
                           "*" ;

              if( ! checkUser( _username.getName() , password ) ){
            	  _username.setName(user) ;
              }
              password = "" ;
           }

           _interpreter = new DCapDoorInterpreterV3(this, _out, _username);
           addCommandListener(_interpreter);
        }catch(Exception ee ){
           start() ;
           kill() ;
           throw ee ;
        }
        //
        // we have to use CellAdapapter.newThread instead of
        // new Thread because we want to have the worker
        // thread to be a member of the cell ThreadGroup.
        //
	_workerThread = _nucleus.newThread( this , "worker" );
	_workerThread.start();

//        _anyThread = _nucleus.newThread( this , "anyThread" ) ;
//        _anyThread.start() ;
        start() ;
    }
    public static CellVersion getStaticCellVersion(){
        return new CellVersion(diskCacheV111.util.Version.getVersion(),"$Revision: 1.17 $" );
    }
    @Override
    public  CellVersion getCellVersion(){
        return getStaticCellVersion() ;
    }
    public void keepAlive(){
       if( _interpreter instanceof KeepAliveListener )
          ((KeepAliveListener)_interpreter).keepAlive();
    }
    private boolean checkUser( String userName , String password ){
       String [] request = new String[5] ;

       request[0] = "request" ;
       request[1] = userName ;
       request[2] = "check-password" ;
       request[3] = userName ;
       request[4] = password ;

       try{
          CellMessage msg = new CellMessage( new CellPath(_authenticator) ,
                                             request ) ;

          msg = sendAndWait( msg , 10000 ) ;
          if( msg == null )
             throw new
             Exception("Pam request timed out");

          Object [] r = (Object [])msg.getMessageObject() ;

          return ((Boolean)r[5]).booleanValue() ;

       }catch(Exception ee){
          esay(ee);
          return false ;
       }

    }
    //
    // possible ways to bail out
    //
    //   i)  The connection is broken or closed by the
    //       client ( which is a protocolException because
    //       we expect a 'byebye' first.
    //  ii)  The client sends a 'byebye' and closes the
    //       connection.
    // iii)  We are killed, which means, that all threads
    //       ( including the client communicationThread )
    //       will be interrupted.
    //
    public void run(){
	if( Thread.currentThread() == _workerThread ){
            //
            // check for lock
            //
            say( "Checking DCap lock" ) ;
            try{
               while( true ){
                   String lock = (String)_nucleus.getDomainContext().get("dcapLock") ;
                   if( lock == null )break ;
                   TimeUnit.SECONDS.sleep(5);
               }

            }catch(InterruptedException iee){
               say("Interrupted the 'dcap' lock" ) ;
               say( "ComThread : Client communication Thread finished" );
               _stateChanged( __connectionLostEvent ) ;
               return ;
            }
            say("DCapLock released");
            _dcapLock = false ;

            try {
                while (true) {
                    if ((_lastCommand = _in.readLine()) == null) {
                        break;
                    }

                    if(_lastCommand.length() == 0) continue;

                    _commandCounter++;
                    say("Executing command: " + _lastCommand);
                    VspArgs args;
                    try {
                        args = new VspArgs(_lastCommand);
                    }catch(IllegalArgumentException e) {
                        println("protocol violation: " + e.getMessage());
                        esay("protocol violation ["+e.getMessage()+"]from " + _engine.getInetAddress());
                        break;
                    }

                    if (execute(args) > 0) {
                        println("0 0 server byebye");
                        say("ComThread : protocol ended");
                        break;
                    }
                }
            } catch (IOException e) {
                esay("Got IO exception " +e.toString() + " from: " + _engine.getInetAddress());
            } catch (Exception e) {
                 esay("ComThread : got " + e);
                 esay(e);
            }finally{
                _out.close();
            }

	    say( "ComThread : Client communication Thread finished" );
            _stateChanged( __connectionLostEvent ) ;
	}else if( Thread.currentThread() == _anyThread  ){
            try{
                 say( "AnyThread : started" ) ;
                 Thread.sleep( 60 * 60 * 1000 )  ;
                 say( "AnyThread : woke up" ) ;
            }catch(InterruptedException ie ){
                say( "AnyThread : was interrupted" ) ;
            }
            say( "AnyThread : finished" ) ;
        }
    }
    private static final int __connectionLostEvent = 1 ;
    private static final int __weWereKilledEvent   = 2 ;
    private static final int __abortCacheFinishedEvent = 3 ;
    private static final int __NormalOperation      = 1 ;
    private static final int __AbortCacheProtOnBye  = 2 ;
    private static final int __WeAreFinished        = 3 ;
    private static final int __AbortCacheProtOnKill = 4 ;

    private boolean _connectionLost =false ;
    private boolean _abortCacheFinished = false ;
    private int _state = __NormalOperation  ;
    private void abortCacheProtocol(){

       say( "abortCacheProtocol : starting" ) ;
       try{
            TimeUnit.SECONDS.sleep(10) ;
       }catch(InterruptedException ie ){
          say( "abortCacheProtocol : interrupted " ) ;
       }
       say( "abortCacheProtocol : finished" ) ;

    }
    private synchronized void _stateChanged( int event ){
       say( "_stateChanged : state = "+_state+" ; event = "+event ) ;
       switch( _state ){

          case __NormalOperation :
             switch( event ){
                case __connectionLostEvent :
                   //
                   // this is the usual case
                   //
                   _state = __AbortCacheProtOnBye ;
                   _nucleus.newThread(
                      new Runnable(){
                         //
                         //Warning : this code is no longer synchronized
                         //
                         public void run(){
                            say( "Starting abortCacheProtocol" ) ;
                            abortCacheProtocol() ;
                            _stateChanged( __abortCacheFinishedEvent ) ;
                            say( "Finished abortCacheProtocol" ) ;
                         }
                      } , "finishCacheProtocol" ).start() ;
                break ;
                case __weWereKilledEvent :
                   println( "0 0 server shutdown" ) ;
                   _out.close();
                   _state = __AbortCacheProtOnKill ;
                   _nucleus.newThread(
                      new Runnable(){
                         //
                         //Warning : this code is no longer synchronized
                         //
                         public void run(){
                            say( "Starting abortCacheProtocol" ) ;
                            abortCacheProtocol() ;
                            say( "Finished abortCacheProtocol" ) ;
                            _stateChanged( __abortCacheFinishedEvent ) ;
                         }
                      } , "finishCacheProtocol" ).start() ;

                break ;
             }
          break ;
          case __AbortCacheProtOnBye :
             switch( event ){
                case __abortCacheFinishedEvent :
                   _state = __WeAreFinished ;
                   kill() ;
                break ;
             }
          break ;
          case __AbortCacheProtOnKill :
             //
             // this state can only become WeAreFinished if
             // two events have been accured.
             //  __abortCacheFinished and __connectionLost
             //
             switch( event ){
                case __abortCacheFinishedEvent :
                   _abortCacheFinished = true ;
                   if( _connectionLost )
                      _state = __WeAreFinished ;
                break ;
                case __connectionLostEvent :
                   _connectionLost = true ;
                   if( _abortCacheFinished )
                     _state = __WeAreFinished ;
                break ;
             }
          break ;
          case __WeAreFinished :
             switch( event ){
                case __weWereKilledEvent :
                   say( "Done" ) ;
                break ;
             }
          break ;
       }
       say( "_stateChanged :  new state = "+_state ) ;
       notifyAll() ;
    }
    private synchronized void waitForFinish( long timeout )
            throws InterruptedException {
       long end = System.currentTimeMillis() + timeout ;
       while( _state != __WeAreFinished ){
           long rest = end - System.currentTimeMillis() ;
           say( "waitForFinish : waiting for "+rest+" seconds" ) ;
           if( rest <=0  )break ;
           wait( rest ) ;
       }
    }
    @Override
    public void   cleanUp(){

	say( "CleanUp : starting" );
        _stateChanged( __weWereKilledEvent ) ;
        try{
           say("CleanUp : waiting for final gate" ) ;
           waitForFinish( 2000 ) ;
        }catch(InterruptedException ie ){
           say("CleanUp : PANIC : interrupted (system left in an undefined state)" ) ;
        }
        if( _state != __WeAreFinished )
            say("CleanUp : PANIC : timeout (system left in an undefined state)" ) ;
	say( "CleanUp : finished" );

        _interpreter.close();
        _out.close();
	try {
	    if (!_engine.getSocket().isClosed()) {
		say("Close socket");
		_engine.getSocket().close();
	    }
        } catch (IOException e) {
            esay("DcapDoor: got I/O exception closing socket:" + e.getMessage());
        }

    }

    private synchronized void println( String str ){
        say( "toclient(println) : "+str ) ;
	_out.println( str );
	_out.flush();
    }

    private int execute( VspArgs args ) throws Exception {

        try{

           String answer = _interpreter.execute(args);
           if( answer != null ){
              say( "Our answer : "+answer ) ;
              println( answer ) ;
           }

        }catch(CommandExitException e) {
            return 1;
        }

        return 0 ;
    }

    ///////////////////////////////////////////////////////////////
    //
    // the stuff which makes us a cell
    //
    @Override
    public String toString(){
        return _username+"@"+_host+( _dcapLock ? " (LOCKED)" : "" ) ;
    }

    @Override
    public void getInfo( PrintWriter pw ){
	pw.println( "            DCapDoor" +( _dcapLock ? " (LOCKED)" : "" ));
	pw.println( "         User  : "+_username );
	pw.println( "         Host  : "+_host );
	pw.println( " Last Command  : "+_lastCommand );
	pw.println( " Command Count : "+_commandCounter );
        _interpreter.getInfo(pw);
    }

    @Override
    public void   messageArrived( CellMessage msg ){
        _interpreter.messageArrived(msg);
    }
}
