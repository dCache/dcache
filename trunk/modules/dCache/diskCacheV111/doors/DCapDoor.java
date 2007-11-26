// $Id: DCapDoor.java,v 1.17 2007-04-05 19:44:07 podstvkv Exp $

package diskCacheV111.doors ;

import java.io.BufferedReader;
import java.io.PrintWriter;
import java.io.Reader;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Hashtable;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.VspArgs;
import dmg.cells.nucleus.CellAdapter;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellNucleus;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.CellVersion;
import dmg.security.CellUser;
import dmg.util.Args;
import dmg.util.CommandException;
import dmg.util.CommandExitException;
import dmg.util.Gate;
import dmg.util.KeepAliveListener;
import dmg.util.StreamEngine;

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
    private Gate           _readyGate      = new Gate(false);
    private int            _commandCounter = 0;
    private String         _lastCommand    = "<init>";
    private Reader         _reader         = null;
    private CellNucleus    _nucleus        = null ;
    private Object         _interpreter    = null ;
    private boolean        _dcapLock       = true ;
    private String         _authenticator  = null ;
    ////////////////////////////////////////////////////////////
    //
    //            The static part
    //         ---------------------
    //
    //       -) unique ID
    //       -) create the command method hashtable 
    //
    // ..........................................................
    ///
    // we need a unique serial number for all DCapDoors.
    // So it has to be a class variable (static).
    // As a matter of fact it has to be unique
    // concerning all possible doors. We will use the
    // the cell context for it later.
    //
    private static long   __counter = 10000 ;
    private synchronized static long nextUniqueId(){
       return __counter++ ;
    }
    //
    // create a hashtable for the client commands
    //
    private static Hashtable     __commandHash       = null ;
    private static Constructor   __interConstructor  = null ;
    private static Method        __messageArrived    = null ;
    private static Method        __getInfo           = null ;
    private final static String  __defaultInterpreterClass = 
             "diskCacheV111.doors.DCapDoorInterpreterV3" ; 
    private synchronized static void 
            loadInterpreter( String className )throws Exception {
    
        //
        // are we already done ?
        //
        if( __commandHash != null )return ;
        Class interClass = Class.forName( className ) ;
        //
        // get the constructor
        //
        Class [] argClass  = { dmg.cells.nucleus.CellAdapter.class ,
                               java.io.PrintWriter.class ,
                               dmg.security.CellUser.class } ;
        __interConstructor = interClass.getConstructor( argClass ) ;
        //
        //  some useful functions
        //
        try{
           Class [] ac = { dmg.cells.nucleus.CellMessage.class } ;
           __messageArrived = interClass.getMethod( "messageArrived" , ac ) ;
        }catch(NoSuchMethodException nsme ){}
        try{
           Class [] ac = { java.io.PrintWriter.class } ;
           __getInfo = interClass.getMethod( "getInfo" , ac ) ;
        }catch(NoSuchMethodException nsme ){}
        //
        // create the command hash
        //
        __commandHash = new Hashtable() ;
        
        Method [] all = interClass.getMethods() ;
        for( int i = 0 ; i < all.length ; i++ ){
           String name = all[i].getName() ;
           
           if( name.length() < 5 )continue ;
           
           if( ! name.startsWith( "com_" ) )continue ;
              
           Class returnType = all[i].getReturnType() ;
           if( ! returnType.equals(java.lang.String.class) )
             continue ;
             
           Class [] param = all[i].getParameterTypes() ;
           boolean paramsFit =
                ( param.length == 3   ) &&
                param[0].equals(int.class) &&
                param[1].equals(int.class) &&
                param[2].equals(diskCacheV111.util.VspArgs.class)     ;
                
           if( ! paramsFit )continue ;
           __commandHash.put( name.substring(4) , all[i] ) ;
        }
    }
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
           
           // 
           // prepare the interpreter ( default or defined )
           //
           String className = args.getOpt("interpreter" ) ;
           className = className == null ? 
                       __defaultInterpreterClass : className ;
               
           loadInterpreter( className ) ;

           _authenticator = args.getOpt("authenticator") ;
           _authenticator = _authenticator == null ?
                            "pam" : _authenticator ;
           
           Object [] theArgs = new Object[3] ;
           theArgs[0] = this ;
           theArgs[1] = _out ;
           
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
           theArgs[2] = _username;
           
           _interpreter = __interConstructor.newInstance( theArgs ) ;
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
    public  CellVersion getCellVersion(){ 
        return getStaticCellVersion() ; 
    }
    public void keepAlive(){
       if( _interpreter instanceof KeepAliveListener )
          ((KeepAliveListener)_interpreter).keepAlive();
    }
    public void say( String str ){
       super.say(str) ;
       pin(str) ;
    }
    public void esay( String str ){
       super.esay(str) ;
       pin("ERROR : "+str) ;
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
                   Thread.sleep(5000) ;
               }
            
            }catch(InterruptedException iee){
               say("Interrupted the 'dcap' lock" ) ;
               say( "ComThread : Client communication Thread finished" );
               _stateChanged( __connectionLostEvent ) ;
               return ;
            }
            say("DCapLock released");
            _dcapLock = false ;
            boolean shutdown = false  ;
	    while( true ){
		try{
		    if( ( _lastCommand = _in.readLine() ) == null )break;
		    _commandCounter++;
		    if( execute( _lastCommand ) > 0 ){
			//
			// we need to close the socket AND
			// have to go back to readLine to
			// finish the ssh protocol gracefully.
                        // The other protocols don't care.
			//
                        println( "0 0 server byebye" ) ;
			try{ _out.close(); }catch(Exception ee){} 
                        shutdown = true ;
                        say( "ComThread : protocol ended" ) ;
		    }
		}catch( Exception e ){
                    if( shutdown ){
                       say( "ComThread : shutdown finished with : "+e ) ;
                       break ;
                    }else{
                       if( Thread.currentThread().isInterrupted() ){
                          say( "ComThread : was interrupted" ) ;
                       }
                       try{ _out.close(); }catch(Exception ee){} 
                       shutdown = true ;
                       esay( "ComThread : got "+e ) ;
                       esay(e);
                    }
		}
		
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
            Thread.sleep(10000) ;
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
                   try{ _out.close() ; }catch(Exception e ){}
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
        try{ _out.close() ; }catch(Exception e ){}
	try {
	    if (!_engine.getSocket().isClosed()) {
		say("Close socket");
		_engine.getSocket().close();
	    }
	} catch (Exception ee) { ; }
	
    }

    private synchronized void println( String str ){ 
        say( "toclient(println) : "+str ) ;
	_out.println( str );
	_out.flush();
    }

    private synchronized void print( String str ){
        say( "toclient(print) : "+str ) ;
	_out.print( str );
	_out.flush();
    }
    private int execute( String line ) throws Exception {
	if( line.equals("") )return 0 ;
        
        say( "Client command : "+line ) ;
        
        VspArgs args = null ;
        //
        // command syntax preparation 
        //
        try{
           args = new VspArgs( line ) ;        
        }catch( IllegalArgumentException iae ){
           //
           // we don't accept a syntax error at this point.
           // simply to dangerous.
           //
           esay( "Protocol syntax violation : "+line ) ;
           throw iae ;           
        }
        int sessionId  = args.getSessionId() ;
        int commandId  = args.getSubSessionId() ;
        String name    = args.getName() ;
        String command = args.getCommand() ;
        say( "Execute : lookup "+command ) ;
        Method m = (Method)__commandHash.get( command ) ;
        if( m == null ){
            protocolViolation(sessionId,commandId,name,669,
                              "Invalid command '"+line+"'" ) ;
            return 0 ;
        }
        try{
           Object [] p = new Object[3] ;
           p[0] = Integer.valueOf(sessionId) ;
           p[1] = Integer.valueOf(commandId) ;
           p[2] = args;

           String role = args.getOpt("role");
           _username.setRole(role);

           String answer = (String)m.invoke( _interpreter , p ) ;
           if( answer != null ){
              say( "Our answer : "+answer ) ;
              println( answer ) ;
           }
           
        }catch(InvocationTargetException ite){
           Throwable t = ite.getTargetException() ;
           if( t instanceof CommandExitException ){
              return 1 ;
           }else if( t instanceof CacheException ){
              CacheException ce =
                 (CacheException)t ;
              internalError(sessionId,commandId,name,
                            ce.getRc() ,
                            ce.getMessage() ) ;
              return 0 ;
           }else if( t instanceof CommandException ){
              CommandException cse =
                 (CommandException)t ;
              protocolViolation(sessionId,commandId,name,
                                cse.getErrorCode() ,
                                cse.getErrorMessage() ) ;
              return 0 ;
           }else{
              internalError(sessionId,commandId,name,701,
                            m.getName()+" : "+t      ) ;
              return 0 ;
           }
        }catch(Throwable t){
           internalError(sessionId,commandId,name,702,
                         "Could't invoke "+m.getName()+" : "+t ) ;
           return 0 ;
        }
        return 0 ;
    }
    private void protocolViolation( int sessionId , int commandId , String name ,
                                    int errorCode , String errorMessage ){
        String problem = ""+sessionId+" "+commandId+" "+name+" " +
                         "failed "+errorCode+
                         " \"protocolViolation : "+errorMessage+"\"" ;
        println(problem) ;
        esay(problem) ;
    }
    private void internalError( int sessionId , int commandId , String name ,
                                int errorCode , String errorMessage ){
        String problem = ""+sessionId+" "+commandId+" "+name+" "+
                         "failed "+errorCode+
                         " \"internalError : "+errorMessage+"\"" ;
        println(problem) ;
        esay(problem) ;
    }
    ///////////////////////////////////////////////////////////////
    //
    // the stuff which makes us a cell
    //
    public String toString(){ 
        return _username+"@"+_host+( _dcapLock ? " (LOCKED)" : "" ) ; 
    }
    
    public void getInfo( PrintWriter pw ){
	pw.println( "            DCapDoor" +( _dcapLock ? " (LOCKED)" : "" ));
	pw.println( "         User  : "+_username );
	pw.println( "         Host  : "+_host );
	pw.println( " Last Command  : "+_lastCommand );
	pw.println( " Command Count : "+_commandCounter );
        if( __getInfo != null ){
           Object [] args = new Object[1] ;
           args[0] = pw ;
           try{
              __getInfo.invoke( _interpreter , args ) ;
           }catch( InvocationTargetException ite ){
              esay( "invoke.getInfo : "+ite.getTargetException() ) ;
           }catch( Exception e ){
              esay( "invoke.getInfo : "+e ) ;
           }
        }
    }

    public void   messageArrived( CellMessage msg ){
       if( __messageArrived != null ){
           Object [] args = new Object[1] ;
           args[0] = msg ;
           try{
              __messageArrived.invoke( _interpreter , args ) ;
           }catch( InvocationTargetException ite ){
           
           }catch( Exception e ){
           
           }
       }
    }
}



