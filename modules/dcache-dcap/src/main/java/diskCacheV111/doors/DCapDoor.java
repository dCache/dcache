// $Id: DCapDoor.java,v 1.17 2007-04-05 19:44:07 podstvkv Exp $

package diskCacheV111.doors ;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Reader;
import java.util.concurrent.TimeUnit;

import diskCacheV111.util.VspArgs;

import dmg.cells.nucleus.CellAdapter;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellNucleus;
import dmg.util.CommandExitException;
import dmg.util.KeepAliveListener;
import dmg.util.StreamEngine;

import org.dcache.auth.Subjects;
import org.dcache.util.Args;

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

    private final static Logger _log =
        LoggerFactory.getLogger(DCapDoor.class);

    private StreamEngine   _engine;
    private BufferedReader _in;
    private PrintWriter    _out;
    private String         _host;
    private Subject         _subject;
    private Thread         _workerThread , _anyThread ;
    private int            _commandCounter;
    private String         _lastCommand    = "<init>";
    private Reader         _reader;
    private CellNucleus    _nucleus;
    private boolean        _dcapLock       = true ;

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
	   _out      = new PrintWriter(engine.getWriter(), true);
            _subject = engine.getSubject();
	   _host     = engine.getInetAddress().toString();

           _interpreter = new DCapDoorInterpreterV3(this, _out, _subject, engine.getInetAddress());
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

    @Override
    public void keepAlive(){
       if( _interpreter instanceof KeepAliveListener ) {
           ((KeepAliveListener) _interpreter).keepAlive();
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
    @Override
    public void run(){
	if( Thread.currentThread() == _workerThread ){
            //
            // check for lock
            //
            _log.info( "Checking DCap lock" ) ;
            try{
               while( true ){
                   String lock = (String)_nucleus.getDomainContext().get("dcapLock") ;
                   if( lock == null ) {
                       break;
                   }
                   TimeUnit.SECONDS.sleep(5);
               }

            }catch(InterruptedException iee){
               _log.info("Interrupted the 'dcap' lock" ) ;
               _log.info( "ComThread : Client communication Thread finished" );
               _stateChanged( __connectionLostEvent ) ;
               return ;
            }
            _log.info("DCapLock released");
            _dcapLock = false ;

            try {
                while (true) {
                    if ((_lastCommand = _in.readLine()) == null) {
                        break;
                    }

                    if(_lastCommand.length() == 0) {
                        continue;
                    }

                    _commandCounter++;
                    _log.info("Executing command: " + _lastCommand);
                    VspArgs args;
                    try {
                        args = new VspArgs(_lastCommand);
                    }catch(IllegalArgumentException e) {
                        println("protocol violation: " + e.getMessage());
                        _log.debug("protocol violation [{}] from {}", e.getMessage(), _engine.getInetAddress());
                        break;
                    }

                    if (execute(args) > 0) {
                        println("0 0 server byebye");
                        _log.info("ComThread : protocol ended");
                        break;
                    }
                }
            } catch (IOException e) {
                _log.warn("Got IO exception " +e.toString() + " from: " + _engine.getInetAddress());
            } catch (Exception e) {
                _log.warn("ComThread : got " + e, e);
            }finally{
                _out.close();
            }

	    _log.info( "ComThread : Client communication Thread finished" );
            _stateChanged( __connectionLostEvent ) ;
	}else if( Thread.currentThread() == _anyThread  ){
            try{
                 _log.info( "AnyThread : started" ) ;
                 Thread.sleep( 60 * 60 * 1000 )  ;
                 _log.info( "AnyThread : woke up" ) ;
            }catch(InterruptedException ie ){
                _log.info( "AnyThread : was interrupted" ) ;
            }
            _log.info( "AnyThread : finished" ) ;
        }
    }
    private static final int __connectionLostEvent = 1 ;
    private static final int __weWereKilledEvent   = 2 ;
    private static final int __abortCacheFinishedEvent = 3 ;
    private static final int __NormalOperation      = 1 ;
    private static final int __AbortCacheProtOnBye  = 2 ;
    private static final int __WeAreFinished        = 3 ;
    private static final int __AbortCacheProtOnKill = 4 ;

    private boolean _connectionLost;
    private boolean _abortCacheFinished;
    private int _state = __NormalOperation  ;

    private synchronized void _stateChanged( int event ){
       _log.info( "_stateChanged : state = "+_state+" ; event = "+event ) ;
       switch( _state ){

          case __NormalOperation :
             switch( event ){
                case __connectionLostEvent :
                   //
                   // this is the usual case
                   //
                   _state = __AbortCacheProtOnBye ;
                   _stateChanged(__abortCacheFinishedEvent);
                break ;
                case __weWereKilledEvent :
                   println( "0 0 server shutdown" ) ;
                   _out.close();
                   _state = __AbortCacheProtOnKill ;
                   _stateChanged(__abortCacheFinishedEvent);
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
                   if( _connectionLost ) {
                       _state = __WeAreFinished;
                   }
                break ;
                case __connectionLostEvent :
                   _connectionLost = true ;
                   if( _abortCacheFinished ) {
                       _state = __WeAreFinished;
                   }
                break ;
             }
          break ;
          case __WeAreFinished :
             switch( event ){
                case __weWereKilledEvent :
                   _log.info( "Done" ) ;
                break ;
             }
          break ;
       }
       _log.info( "_stateChanged :  new state = "+_state ) ;
       notifyAll() ;
    }
    private synchronized void waitForFinish( long timeout )
            throws InterruptedException {
       long end = System.currentTimeMillis() + timeout ;
       while( _state != __WeAreFinished ){
           long rest = end - System.currentTimeMillis() ;
           _log.info( "waitForFinish : waiting for "+rest+" seconds" ) ;
           if( rest <=0  ) {
               break;
           }
           wait( rest ) ;
       }
    }
    @Override
    public void   cleanUp(){

	_log.info( "CleanUp : starting" );
        _stateChanged( __weWereKilledEvent ) ;
        try{
           _log.info("CleanUp : waiting for final gate" ) ;
           waitForFinish( 2000 ) ;
        }catch(InterruptedException ie ){
           _log.info("CleanUp : PANIC : interrupted (system left in an undefined state)" ) ;
        }
        if( _state != __WeAreFinished ) {
            _log.info("CleanUp : PANIC : timeout (system left in an undefined state)");
        }
	_log.info( "CleanUp : finished" );

        _interpreter.close();
        _out.close();
	try {
	    if (!_engine.getSocket().isClosed()) {
		_log.info("Close socket");
		_engine.getSocket().close();
	    }
        } catch (IOException e) {
            _log.warn("DcapDoor: got I/O exception closing socket:" + e.getMessage());
        }

    }

    private synchronized void println( String str ){
        _log.info( "toclient(println) : "+str ) ;
	_out.println(str);
    }

    private int execute( VspArgs args ) throws Exception {

        try{

           String answer = _interpreter.execute(args);
           if( answer != null ){
              _log.info( "Our answer : "+answer ) ;
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
        return Subjects.getDisplayName(_subject)+"@"+_host+( _dcapLock ? " (LOCKED)" : "" ) ;
    }

    @Override
    public void getInfo( PrintWriter pw ){
	pw.println( "            DCapDoor" +( _dcapLock ? " (LOCKED)" : "" ));
        pw.println( "         User  : "+Subjects.getDisplayName(_subject) );
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
