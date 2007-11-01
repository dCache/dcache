// $Id: DCapDoorInterpreterV5.java,v 1.2 2002-02-03 23:31:58 cvs Exp $
//
package diskCacheV111.doors.dCapV5 ;

import diskCacheV111.vehicles.*;
import diskCacheV111.util.*;


import dmg.cells.nucleus.*;
import dmg.cells.network.*;
import dmg.util.*;

import java.util.*;
import java.io.*;
import java.net.*;
import java.lang.reflect.*;

/**
  * @author Patrick Fuhrmann
  * @version 0.1, Jan 18 2002
  *
  *
  *
  *  
  */
public class DCapDoorInterpreterV5 implements SessionRoot {
    private PrintWriter _out         = null ;
    private CellAdapter _cell        = null ;
    private Args        _args        = null ;
    private String      _ourName     = "server" ;
    private PnfsHandler _pnfs        = null ;
    private Hashtable   _sessions    = new Hashtable() ;
    private String  _poolManagerName = null ;
    private String  _pnfsManagerName = null ;
    private CellPath _poolMgrPath    = null ;
    private int      _majorVersion   = 0 ;
    private int      _minorVersion   = 0 ;  
    private String   _revision       = "$Revision: 1.2 $" ; 
    private int      _uid            = -1 ;
    private int      _pid            = -1 ;
    private MessageEventTimer _timer          = null ;
    private CellPath          _prestagerPath  = new CellPath( "Prestager" ) ;
    
    public DCapDoorInterpreterV5( CellAdapter cell , PrintWriter pw ){

        _out  = pw ;
        _cell = cell ;
        _args = _cell.getArgs() ;
        _poolManagerName = _args.getOpt("PoolMgr" ) ;
        _poolManagerName = _poolManagerName == null ? 
                           "PoolManager" : _poolManagerName ;
        _pnfsManagerName = _args.getOpt("PnfsMgr" ) ;
        _pnfsManagerName = _pnfsManagerName == null ? 
                           "PnfsManager" : _pnfsManagerName ;
         
        _poolMgrPath     = new CellPath( _poolManagerName ) ;               
        _pnfs = new PnfsHandler( _cell , new CellPath( _pnfsManagerName ) ) ;
        
        _timer = new MessageEventTimer( _cell.getNucleus() ) ;
        
        _cell.getNucleus().newThread( 
               new Runnable(){
                  public void run(){ 
                    try{
                      _timer.loop() ; 
                    }catch(Exception ee ){
                       esay("TimerLoop interrupted : "+ee ) ;
                       ee.printStackTrace() ;
                    }
                  }
               } ,
               "timerLoop" 
        ).start() ;

        say("Constructor Done" ) ;
    }
    public synchronized void println( String str ){ 
        say( "(DCapDoorInterpreterV5) toclient(println) : "+str ) ;
	_out.println( str );
	_out.flush();
    }

    public synchronized void print( String str ){
        say( "(DCapDoorInterpreterV5) toclient(print) : "+str ) ;
	_out.print( str );
	_out.flush();
    }
    public void say( String message ){
       _cell.say("DCAP-V5 "+message ) ;
    }
    public void esay( String message ){
       _cell.esay( "DCAP-V5 ERROR "+message ) ;
    }
    //////////////////////////////////////////////////////////////////
    //
    //   the command functions  String com_<commandName>(int,int,Args)
    //
    public String com_hello( int sessionId , int commandId , VspArgs args )
           throws Exception {
           
       if( args.argc() < 2 )
          throw new
          CommandExitException( "Command Syntax Exception" , 2 ) ;
          
       try{
             _majorVersion = Integer.parseInt( args.argv(3) ) ;
             _minorVersion = Integer.parseInt( args.argv(2) ) ;
       }catch(Exception e ){
          throw new
          CommandExitException( "Command Argument Exception" , 2) ;
       }
       String tmp = args.getOpt( "uid" ) ;
       if( tmp != null )
       try{ _uid = Integer.parseInt(tmp) ;
       }catch(Exception ee){}
       if( ( tmp = args.getOpt( "pid" ) ) != null )
       try{ _pid = Integer.parseInt(tmp) ;
       }catch(Exception ee){}
       
       String yourName = args.getName() ;
       if( yourName.equals("server") )_ourName = "client" ;
       return "0 0 "+_ourName+" welcome "+_majorVersion+" "+_minorVersion ;
    }
    public String hh_session = "<sessionId> [<args...>]" ;
    public Object ac_session_$_1_99( Args args ) throws Exception {
      int sessionId = Integer.parseInt( args.argv(0) ) ;
      SessionHandler sh = 
              (SessionHandler)_sessions.get( new Integer(sessionId) ) ;
      
      if( sh == null )
        throw new
        NoSuchElementException( "Session "+sessionId+" not found");
        
      args.shift() ;
      return sh.command( args ) ;
    
    }
    public String com_byebye( int sessionId , int commandId , VspArgs args )
           throws Exception {
       throw new CommandExitException("byeBye",commandId)  ;
    }
    public synchronized String com_open( int sessionId , int commandId , VspArgs args )
           throws Exception {
           
        
       if( args.argc() < 4 )
          throw new
          CommandException( 3  , "Not enough arguments for put" ) ;
        
       if( _sessions.get( new Integer(sessionId) ) != null )
          throw new
          CommandException( 5 , "Duplicated session id" ) ;
      
       try{   
            SessionHandler handler = new IoHandler(this,sessionId,commandId,args) ;
            _sessions.put( new Integer(sessionId) , handler ) ;
            handler.go() ;
       }catch(CacheException ce ){
          throw new CommandException(ce.getRc() ,
                                     ce.getMessage() ) ;
       }catch(Exception e){
          throw new CommandException(44 , e.getMessage() ) ;
       
       }
       
       return null ;     
    }
    public void   getInfo( PrintWriter pw ){
       pw.println( " ----- DCapDoorInterpreterV5 ----------" ) ;
       pw.println( "  Version  : "+_minorVersion+"/"+_majorVersion ) ;
       pw.println( "  Revision : "+_revision ) ;
       pw.println( "  Pid      : "+_pid ) ;
       pw.println( "  Uid      : "+_uid ) ;
       Enumeration e = _sessions.keys() ;
       for( ; e.hasMoreElements() ; ){
           Integer i = (Integer)e.nextElement() ;
           Object o = _sessions.get(i) ;
           pw.println( i.toString()+ " -> "+o.toString() );
       }
    }
    public void   messageArrived( CellMessage msg ){
       _timer.messageArrived( msg ) ;
    }
    public MessageEventTimer getTimer(){ return _timer ; }
    public void removeSession( Integer sessionId ){
       _sessions.remove( sessionId ) ;
    }
    public CellAdapter getCellAdapter(){ return _cell ; }
}
