// $Id: DCapDoorInterpreterV3.java,v 1.86 2007-07-08 17:50:47 tigran Exp $
//
package diskCacheV111.doors;

import diskCacheV111.vehicles.*;
import diskCacheV111.util.*;


import dmg.cells.nucleus.*;
import dmg.util.*;
import dmg.security.CellUser;
import diskCacheV111.services.PermissionHandler;

import java.util.*;
import java.io.*;
import java.net.*;


import diskCacheV111.util.AccessLatency;
import diskCacheV111.util.FileMetaData;
import diskCacheV111.util.RetentionPolicy;


/**
 * @author Patrick Fuhrmann
 * @version 0.1, Jan 27 2000
 *
 *   Simple Template for a Door. It runs the DiskCacheV111
 *   Protocol V1 version.
 *
 *
 *
 */
public class DCapDoorInterpreterV3 implements KeepAliveListener {
    private final PrintWriter _out          ;
    private final CellAdapter _cell         ;
    private final Args        _args         ;
    private String      _ourName     = "server" ;
    private CellUser    _user        = null ;
    private final PnfsHandler _pnfs         ;
    private final Map<Integer, SessionHandler>     _sessions    = new HashMap<Integer, SessionHandler>() ;
    private String  _poolManagerName = null ;
    private String  _pnfsManagerName = null ;
    private String  _userMetaProvider        = null ;
    private int _userMetaTimeout     = 60; // user metadata proveder timeout in seconds
    private int _userMetaRetries     = 60; // user metadata proveder timeout in seconds

    private CellPath _poolMgrPath    = null ;
    private final Object  _messageLock     = new Object() ;
    private String  _pid             = null ;
    private String  _uid             = null ;
    private int     _userUid         = -1 ;
    private int     _userGid         = -1 ;
    private String  _userHome        = "?" ;
    private int     _majorVersion    = 0 ;
    private int     _minorVersion    = 0 ;
    private Date    _startedTS       = null ;
    private Date    _lastCommandTS   = null ;
    private boolean _authorizationRequired = false ;
    private boolean _authorizationStrong   = false ;

    /**
     * a flag which is true if strong authentication succeeded
     */
    private boolean _isAuthorized = false;
    private boolean _strictSize            = false ;
    private String  _poolProxy        = null ;
    private Version _minClientVersion = null ;
    private Version _maxClientVersion = null ;
    private boolean _checkStrict      = true ;
    private long    _poolRetry        = 0L ;
    private String  _hsmManager       = null ;
    private boolean _doPreallocation  = false ;
    private final PermissionHandler _permissionHandler;
    private boolean _truncateAllowed  = false ;
    private String  _ioQueueName      = null ;
    private boolean _ioQueueAllowOverwrite = false ;
    private boolean _readOnly = false;

    /**
     * DN+Role based information
     */
    private VOInfo _voInfo = null;

    // flag defined in batch file to allow/disallow AccessLatency and RetentionPolicy re-definition

    private boolean _isAccessLatencyOverwriteAllowed = false;
    private boolean _isRetentionPolicyOverwriteAllowed = false;

    public DCapDoorInterpreterV3( CellAdapter cell , PrintWriter pw , CellUser user ){

        _out  = pw ;
        _cell = cell ;
        _args = _cell.getArgs() ;
        _user = user;

        if( _user.getName() != null && _user.getRole() != null ) {
            _voInfo = new VOInfo(_user.getName(), _user.getRole() );

            if( _userMetaProvider != null ){
                _isAuthorized = getUserMetadata( _user.getName(), _user.getRole() , _userMetaProvider ) ;
            }

        }

        _poolManagerName = _args.getOpt("PoolMgr" ) ;
        _poolManagerName = _poolManagerName == null ?
        "PoolManager" : _poolManagerName ;
        _pnfsManagerName = _args.getOpt("PnfsMgr" ) ;
        _pnfsManagerName = _pnfsManagerName == null ?
        "PnfsManager" : _pnfsManagerName ;

        _poolProxy       = _args.getOpt("poolProxy");
        _cell.say("Pool Proxy "+( _poolProxy == null ? "not set" : ( "set to "+_poolProxy ) ) );

        _userMetaProvider        = _args.getOpt("usermap") ;


        // allow file truncating
        String truncate = _args.getOpt("truncate");
        _truncateAllowed = (truncate != null) && truncate.equals("true") ;

        _isAccessLatencyOverwriteAllowed = _args.getOpt("allow-access-policy-overwrite") != null ;
        if(_isAccessLatencyOverwriteAllowed) {
        	_cell.say("Allowes to overwrite AccessLatency");
        }
        _isRetentionPolicyOverwriteAllowed = _args.getOpt("allow-retention-policy-overwrite") != null;
        if(_isRetentionPolicyOverwriteAllowed){
        	_cell.say("Allowed to overwrite RetentionPolicy");
        }

        if(_args.getOpt("usermap-timeout") != null) {
            _userMetaTimeout = Integer.parseInt(_args.getOpt("usermap-timeout"));
        }
        if(_args.getOpt("usermap-retries") != null) {
            _userMetaRetries = Integer.parseInt(_args.getOpt("usermap-retries"));
        }

        _poolMgrPath     = new CellPath( _poolManagerName ) ;
        _pnfs = new PnfsHandler( _cell , new CellPath( _pnfsManagerName ) ) ;
        _permissionHandler = new PermissionHandler( _cell, _pnfs );

        _checkStrict     = ( _args.getOpt("check") != null ) &&
        ( _args.getOpt("check").equals("strict") ) ;

        _strictSize      = _args.getOpt("strict-size") != null ;

        _hsmManager      = _args.getOpt("hsm") ;

        _doPreallocation = _args.getOpt("preallocate-space") != null ;

        _startedTS = new Date() ;
        //
        //   client version restrictions
        //
        String restriction = (String)_cell.getDomainContext().get("dCapDoor-clientVersion");
        installVersionRestrictions(
        restriction==null?_args.getOpt("clientVersion"):restriction ) ;

        String poolRetryValue = (String)_cell.getDomainContext().get("dCapDoor-poolRetry") ;
        poolRetryValue = poolRetryValue == null ?
        (String)_cell.getDomainContext().get("poolRetry") :poolRetryValue;
        poolRetryValue = poolRetryValue == null ?
        _args.getOpt("poolRetry") : poolRetryValue ;

        if( poolRetryValue != null )
            try{
                _poolRetry = Long.parseLong(poolRetryValue) * 1000L ;
            }catch(Exception ee ){
                _cell.esay("Problem in setting PoolRetry Value : "+ee ) ;
            }
        _cell.say("PoolRetry timer set to "+(_poolRetry/1000L)+" seconds");

        String auth = _args.getOpt("authorization") ;
        _authorizationStrong   = ( auth != null ) && auth.equals("strong") ;
        _authorizationRequired = ( auth != null ) &&
        ( auth.equals("strong") || auth.equals("required") ) ;

        if( _authorizationRequired )_cell.say("Authorization required");
        if( _authorizationStrong   )_cell.say("Authorization strong");

        _ioQueueName = _args.getOpt("io-queue") ;
        _ioQueueName = ( _ioQueueName == null ) || ( _ioQueueName.length() == 0 ) ? null : _ioQueueName ;
        _cell.say( "IoQueueName = "+(_ioQueueName==null?"<undefined>":_ioQueueName));

        String tmp = _args.getOpt("io-queue-overwrite") ;
        _ioQueueAllowOverwrite = ( tmp != null ) && tmp.equals("allowed" ) ;
        _cell.say("IoQueueName : overwrite : "+(_ioQueueAllowOverwrite?"allowed":"denied"));

        String check = (String)_cell.getDomainContext().get("dCapDoor-check");
        if( check != null )_checkStrict = check.equals("strict") ;
        /*VP
        if( _userMeta != null )getUserMetadata( _user , _userMeta ) ;
        _cell.say("Door authenticated for "+
                  _user+"("+_userUid+","+
                  _userGid+","+_userHome+")");
         */
        _readOnly = _args.getOpt("readOnly") != null ;
        if (_readOnly)
	    _cell.say("Door is configured as read-only");
        else
	    _cell.say("Door is configured as read/write");

        _cell.say("Check : "+(_checkStrict?"Strict":"Fuzzy"));
        _cell.say("Constructor Done" ) ;
    }
    private static class Version implements Comparable<Version> {
        private final int _major;
        private final int _minor;
        private Version( String versionString ){
            StringTokenizer st = new StringTokenizer(versionString,".");
            _major = Integer.parseInt(st.nextToken());
            _minor = Integer.parseInt(st.nextToken());
        }
        public int compareTo( Version other ){
            return _major < other._major ? -1 :
                _major > other._major ?  1 :
                    _minor < other._minor ? -1 :
                        _minor > other._minor ?  1 : 0;
        }
        @Override
        public String toString(){ return ""+_major+"."+_minor ; }
        private Version( int major , int minor ){
            _major = major ;
            _minor = minor ;
        }
        @Override
        public boolean equals(Object obj ) {
           if( obj == this ) return true;
           if( !(obj instanceof Version) ) return false;

            return ((Version)obj)._major == this._major && ((Version)obj)._minor == this._minor;
        }
        @Override
        public int hashCode() {
            return _minor ^ _major;
        }



    }
    private void installVersionRestrictions( String versionString ){
        //
        //   majorMin.minorMin[:majorMax.minorMax]
        //
        if( versionString == null ){
            _cell.say("Client Version not restricted");
            return ;
        }
        _cell.say("Client Version Restricted to : "+versionString);
        try{
            StringTokenizer st = new StringTokenizer(versionString,":");
            _minClientVersion  = new Version( st.nextToken() ) ;
            _maxClientVersion  = st.countTokens() > 0 ? new Version(st.nextToken()) : null ;
        }catch(Exception ee ){
            _cell.esay("Client Version : syntax error (limits ignored) : "+versionString);
            _cell.esay(ee);
            _minClientVersion = _maxClientVersion = null ;
        }
    }
    public synchronized void println( String str ){
        _cell.say( "(DCapDoorInterpreterV3) toclient(println) : "+str ) ;
        _out.println( str );
        _out.flush();
    }

    public synchronized void print( String str ){
        _cell.say( "(DCapDoorInterpreterV3) toclient(print) : "+str ) ;
        _out.print( str );
        _out.flush();
    }
    //VPprivate boolean getUserMetadata( String userName , String provider ){
    private boolean getUserMetadata( String userName , String userRole , String provider ){
        String [] request = new String[5] ;

        request[0] = "request" ;
        request[1] = userName ;
        request[2] = "get-metainfo" ;
        //VP  request[3] = userName ;
        request[3] = userRole==null ? "UNSPECIFIED" : userRole;
        request[4] = "uid,gid,home" ;
        _userUid = -1;
        _userGid = -1;

        try{
            CellMessage msg = null;

            for(int retry=0; retry<_userMetaRetries;++retry) {
                msg  = new CellMessage( new CellPath(provider) , request ) ;
                msg = _cell.sendAndWait( msg , _userMetaTimeout*1000 ) ;
                if(msg != null) {
                    break;
                }
                _cell.esay("get-metainfo to "+provider+" timed out for "+userName+
                " on retry #"+retry);
                try {
                    Thread.sleep(_userMetaTimeout*1000);
                }
                catch(InterruptedException ie) {
                    break;
                }
            }
            if( msg == null ){
                return false ;
            }
            Object result = msg.getMessageObject() ;
            if( result instanceof Exception ){
                _cell.esay( "Exception from "+provider+" : "+result );
                return false ;
            }
            Object [] r = (Object [])result ;

            String tmp = (String)r[5] ;
            try{ _userUid = Integer.parseInt(tmp) ; }catch(NumberFormatException e){/* 'bad' strings silently ignored */}
            tmp = (String)r[6] ;
            try{ _userGid = Integer.parseInt(tmp) ; }catch(NumberFormatException e){/* 'bad' strings silently ignored */}
            _userHome = (String)r[7] ;
            return true ;
        }catch(Exception ee){
            _cell.esay(ee);
            return false ;
        }

    }
    public void keepAlive(){
        List<SessionHandler> list = null ;
        synchronized( this ){
            list = new ArrayList<SessionHandler>(_sessions.values()) ;
        }

        for ( SessionHandler sh: list ){
            try{
                sh.keepAlive() ;
            }catch(Throwable t ){
                _cell.esay("Keep Alive problem in "+sh+" :"+t);
            }
        }
    }
    //////////////////////////////////////////////////////////////////
    //
    //   the command functions  String com_<commandName>(int,int,Args)
    //
    public String com_hello( int sessionId , int commandId , VspArgs args )
    throws Exception {

        _lastCommandTS = new Date() ;
        if( args.argc() < 2 )
            throw new
            CommandExitException( "Command Syntax Exception" , 2 ) ;

        Version version = null ;
        try{
            _majorVersion = Integer.parseInt( args.argv(2) ) ;
            _minorVersion = Integer.parseInt( args.argv(3) ) ;
        }catch(NumberFormatException e ){
            _majorVersion = _minorVersion = 0 ;
            _cell.esay("Syntax error in client version number : "+e);
        }
        version = new Version( _majorVersion , _minorVersion ) ;
        _cell.say("Client Version : "+version );
        if( ( ( _minClientVersion != null ) && ( version.compareTo( _minClientVersion ) < 0 ) ) ||
        ( ( _maxClientVersion != null ) && ( version.compareTo( _maxClientVersion ) > 0 ) )  ){

            String error = "Client version rejected : "+version ;
            _cell.esay(error);
            throw new
            CommandExitException(error , 1 );
        }
        String yourName = args.getName() ;
        if( yourName.equals("server") )_ourName = "client" ;
        _pid = args.getOpt("pid") ;
        _pid = _pid == null ?  "<unknown>" : _pid ;
        _uid = args.getOpt("uid") ;
        _uid = _uid == null ?  "<unknown>" : _uid ;
        return "0 0 "+_ourName+" welcome "+_majorVersion+" "+_minorVersion ;
    }
    public String com_byebye( int sessionId , int commandId , VspArgs args )
    throws Exception {
        _lastCommandTS = new Date() ;
        throw new CommandExitException("byeBye",commandId)  ;
    }
    public synchronized String com_open( int sessionId , int commandId , VspArgs args )
    throws Exception {


        _lastCommandTS = new Date() ;
        if( args.argc() < 4 )
            throw new
            CommandException( 3  , "Not enough arguments for put" ) ;

        if( _sessions.get( Integer.valueOf(sessionId) ) != null )
            throw new
            CommandException( 5 , "Duplicated session id" ) ;

        if( _userMetaProvider != null ){
        	getUserMetadata( _user.getName(), _user.getRole() , _userMetaProvider ) ;
        }
        _cell.say("Door authenticated for "+
        		_user.getName()+"("+_user.getRole()+","+_userUid+","+
        _userGid+","+_userHome+")");
        if( _authorizationStrong && ( _userUid < 0 ) ) {
            throw new
            CacheException( 2 , "User "+_user.getName()+" is not authorized") ;
        }
        //VP
        try{


            //
            //  we have to make sure not to receive messages
            //  before we have stored our sessionId in the hash.
            //  (==synchronized)
            //
            synchronized( _messageLock ){
               SessionHandler se =  new IoHandler(sessionId,commandId,args);

               // if user authentificated tell it to Session Handler
               if( _userMetaProvider != null ) {

                   se.setOwner( _user.getName() ) ;
                   if( _userUid >= 0 ) {
                       se.setUid(_userUid );
                   }
                   if( _userGid >= 0 ) {
                       se.setGid(_userGid );
                   }
               }
               _sessions.put( Integer.valueOf(sessionId) , se ) ;
            }
        }catch(CacheException ce ){
            throw new CommandException(ce.getRc() ,
            ce.getMessage() ) ;
        }catch(Exception e){
            e.printStackTrace();
            throw new CommandException(44 , e.getMessage() ) ;

        }

        return null ;
    }
    public synchronized String com_stage( int sessionId , int commandId , VspArgs args )
    throws Exception {


        _lastCommandTS = new Date() ;
        if( args.argc() < 1 )
            throw new
            CommandException( 3  , "Not enough arguments for stage" ) ;

        if( _sessions.get( Integer.valueOf(sessionId) ) != null )
            throw new
            CommandException( 5 , "Duplicated session id" ) ;

        if( _userMetaProvider != null ){
        	getUserMetadata( _user.getName(), _user.getRole() , _userMetaProvider ) ;
        }
        _cell.say("Door authenticated for "+
        		_user.getName()+"("+_user.getRole()+","+_userUid+","+
        _userGid+","+_userHome+")");
        if( _authorizationStrong && ( _userUid < 0 ) ) {
            throw new
            CacheException( 2 , "User "+_user.getName()+" is not authorized") ;
        }
        //VP
        try{
            //
            //  we have to make sure not to receive messages
            //  before we have stored our sessionId in the hash.
            //  (==synchronized)
            synchronized( _messageLock ){
                _sessions.put( Integer.valueOf(sessionId) ,
                new PrestageHandler(sessionId,commandId,args) ) ;
            }
        }catch(CacheException ce ){
            throw new CommandException(ce.getRc() ,
            ce.getMessage() ) ;
        }catch(Exception e){
            throw new CommandException(44 , e.getMessage() ) ;

        }
        return null ;
    }
    public synchronized String com_lstat( int sessionId , int commandId , VspArgs args )
    throws Exception {

        return get_stat( sessionId , commandId , args , false ) ;

    }
    public synchronized String com_stat( int sessionId , int commandId , VspArgs args )
    throws Exception {

        return get_stat( sessionId , commandId , args , true ) ;

    }

    public synchronized String com_unlink( int sessionId , int commandId , VspArgs args )
    throws Exception {

    	if (_readOnly) {
	    throw new CacheException( 2 , "Cannot execute 'unlink': Permission denied") ;
    	}
        return do_unlink( sessionId , commandId , args , true ) ;

    }

    public synchronized String com_rename( int sessionId , int commandId , VspArgs args )
    throws Exception {

    	if (_readOnly) {
	    throw new CacheException( 2 , "Cannot execute 'rename': Permission denied") ;
    	}
        return do_rename( sessionId , commandId , args ) ;

    }

    public synchronized String com_rmdir( int sessionId , int commandId , VspArgs args )
    throws Exception {

    	if (_readOnly) {
	    throw new CacheException( 2 , "Cannot execute 'rmdir': Permission denied") ;
    	}
        return do_rmdir( sessionId , commandId , args , true ) ;

    }

    public synchronized String com_mkdir( int sessionId , int commandId , VspArgs args )
    throws Exception {

    	if (_readOnly) {
	    throw new CacheException( 2 , "Cannot execute 'mkdir': Permission denied") ;
    	}
        return do_mkdir( sessionId , commandId , args , true ) ;

    }

    public synchronized String com_chmod( int sessionId , int commandId , VspArgs args )
    throws Exception {

    	if (_readOnly) {
	    throw new CacheException( 2 , "Cannot execute 'chmod': Permission denied") ;
    	}
        return do_chmod( sessionId , commandId , args , true ) ;

    }

    public synchronized String com_chown( int sessionId , int commandId , VspArgs args )
    throws Exception {

    	if (_readOnly) {
	    throw new CacheException( 2 , "Cannot execute 'chown': Permission denied") ;
    	}
        return do_chown( sessionId , commandId , args , true ) ;

    }


    public synchronized String com_chgrp( int sessionId , int commandId , VspArgs args )
    throws Exception {

    	if (_readOnly) {
	    throw new CacheException( 2 , "Cannot execute 'chgrp': Permission denied") ;
    	}
        return do_chgrp( sessionId , commandId , args , true ) ;

    }

    public synchronized String com_opendir( int sessionId , int commandId , VspArgs args )
    throws Exception {

        return do_opendir( sessionId , commandId , args ) ;

    }

    private synchronized String do_unlink(
    int sessionId , int commandId , VspArgs args , boolean resolvePath )
    throws Exception {


        _lastCommandTS = new Date() ;
        if( args.argc() < 1 )
            throw new
            CommandException( 3  , "Not enough arguments for unlink" ) ;

        if( _sessions.get( Integer.valueOf(sessionId) ) != null )
            throw new
            CommandException( 5 , "Duplicated session id" ) ;

        if( _userMetaProvider != null ){
        	getUserMetadata( _user.getName(), _user.getRole() , _userMetaProvider ) ;
        }
        _cell.say("Door authenticated for "+
        		_user.getName()+"("+_user.getRole()+","+_userUid+","+
        _userGid+","+_userHome+")");
        if( _authorizationStrong && ( _userUid < 0 ) ) {
            throw new
            CacheException( 2 , "User "+_user.getName()+" is not authorized") ;
        }

        try{
            //
            synchronized( _messageLock ){
                _sessions.put( Integer.valueOf(sessionId) ,
                new UnlinkHandler(sessionId,commandId,args,resolvePath) ) ;
            }
        }catch(CacheException ce ){
            throw new CommandException(ce.getRc() ,
            ce.getMessage() ) ;
        }catch(Exception e){
            throw new CommandException(44 , e.getMessage() ) ;

        }
        return null ;
    }



    private synchronized String do_rename(
    int sessionId , int commandId , VspArgs args )
    throws Exception {


        _lastCommandTS = new Date() ;
        if( args.argc() < 1 )
            throw new
            CommandException( 3  , "Not enough arguments for unlink" ) ;

        if( _sessions.get( Integer.valueOf(sessionId) ) != null )
            throw new
            CommandException( 5 , "Duplicated session id" ) ;

        if( _userMetaProvider != null ){
        	getUserMetadata( _user.getName(), _user.getRole() , _userMetaProvider ) ;
        }
        _cell.say("Door authenticated for "+
        		_user.getName()+"("+_user.getRole()+","+_userUid+","+
        _userGid+","+_userHome+")");
        if( _authorizationStrong && ( _userUid < 0 ) ) {
            throw new
            CacheException( 2 , "User "+_user.getName()+" is not authorized") ;
        }

        try{
            //
            synchronized( _messageLock ){
                _sessions.put( Integer.valueOf(sessionId) ,
                new RenameHandler(sessionId,commandId,args) ) ;
            }
        }catch(CacheException ce ){
            throw new CommandException(ce.getRc() ,
            ce.getMessage() ) ;
        }catch(Exception e){
            throw new CommandException(44 , e.getMessage() ) ;

        }
        return null ;
    }


    private synchronized String do_rmdir(
    int sessionId , int commandId , VspArgs args , boolean resolvePath )
    throws Exception {


        _lastCommandTS = new Date() ;
        if( args.argc() < 1 )
            throw new
            CommandException( 3  , "Not enough arguments for rmdir" ) ;

        if( _sessions.get( Integer.valueOf(sessionId) ) != null )
            throw new
            CommandException( 5 , "Duplicated session id" ) ;

        if( _userMetaProvider != null ){
        	getUserMetadata( _user.getName(), _user.getRole() , _userMetaProvider ) ;
        }
        _cell.say("Door authenticated for "+
        		_user.getName()+"("+_user.getRole()+","+_userUid+","+
        _userGid+","+_userHome+")");
        if( _authorizationStrong && ( _userUid < 0 ) ) {
            throw new
            CacheException( 2 , "User "+_user.getName()+" is not authorized") ;
        }

        try{
            //
            synchronized( _messageLock ){
                _sessions.put( Integer.valueOf(sessionId) ,
                new RmDirHandler(sessionId,commandId,args,resolvePath) ) ;
            }
        }catch(CacheException ce ){
            throw new CommandException(ce.getRc() ,
            ce.getMessage() ) ;
        }catch(Exception e){
            throw new CommandException(44 , e.getMessage() ) ;

        }
        return null ;
    }

    private synchronized String do_mkdir(
    int sessionId , int commandId , VspArgs args , boolean resolvePath )
    throws Exception {


        _lastCommandTS = new Date() ;
        if( args.argc() < 1 )
            throw new
            CommandException( 3  , "Not enough arguments for unlink" ) ;

        if( _sessions.get( Integer.valueOf(sessionId) ) != null )
            throw new
            CommandException( 5 , "Duplicated session id" ) ;

        if( _userMetaProvider != null ){
        	getUserMetadata( _user.getName(), _user.getRole() , _userMetaProvider ) ;
        }
        _cell.say("Door authenticated for "+
        		_user.getName()+"("+_user.getRole()+","+_userUid+","+
        _userGid+","+_userHome+")");
        if( _authorizationStrong && ( _userUid < 0 ) ) {
            throw new
            CacheException( 2 , "User "+_user.getName()+" is not authorized") ;
        }

        try{
            //
            synchronized( _messageLock ){
                _sessions.put( Integer.valueOf(sessionId) ,
                new MkDirHandler(sessionId,commandId,args,resolvePath) ) ;
            }
        }catch(CacheException ce ){
            throw new CommandException(ce.getRc() ,
            ce.getMessage() ) ;
        }catch(Exception e){
            throw new CommandException(44 , e.getMessage() ) ;

        }
        return null ;
    }

    private synchronized String do_chown(
    int sessionId , int commandId , VspArgs args , boolean resolvePath )
    throws Exception {


        _lastCommandTS = new Date() ;
        if( args.argc() < 1 )
            throw new
            CommandException( 3  , "Not enough arguments for chown" ) ;

        if( _sessions.get( Integer.valueOf(sessionId) ) != null )
            throw new
            CommandException( 5 , "Duplicated session id" ) ;

        if( _userMetaProvider != null ){
        	getUserMetadata( _user.getName(), _user.getRole() , _userMetaProvider ) ;
        }
        _cell.say("Door authenticated for "+
        		_user.getName()+"("+_user.getRole()+","+_userUid+","+
        _userGid+","+_userHome+")");
        if( _authorizationStrong && ( _userUid < 0 ) ) {
            throw new
            CacheException( 2 , "User "+_user.getName()+" is not authorized") ;
        }

        try{
            //
            synchronized( _messageLock ){
                _sessions.put( Integer.valueOf(sessionId) ,
                new ChownHandler(sessionId,commandId,args,resolvePath) ) ;
            }
        }catch(CacheException ce ){
            throw new CommandException(ce.getRc() ,
            ce.getMessage() ) ;
        }catch(Exception e){
            throw new CommandException(44 , e.getMessage() ) ;

        }
        return null ;
    }


    private synchronized String do_chgrp(
            int sessionId , int commandId , VspArgs args , boolean resolvePath )
            throws Exception {

                _lastCommandTS = new Date() ;
                if( args.argc() < 1 )
                    throw new
                    CommandException( 3  , "Not enough arguments for chgrp" ) ;

                if( _sessions.get( Integer.valueOf(sessionId) ) != null )
                    throw new
                    CommandException( 5 , "Duplicated session id" ) ;

                if( _userMetaProvider != null ){
                	getUserMetadata( _user.getName(), _user.getRole() , _userMetaProvider ) ;
                }
                _cell.say("Door authenticated for "+
                		_user.getName()+"("+_user.getRole()+","+_userUid+","+
                _userGid+","+_userHome+")");
                if( _authorizationStrong && ( _userUid < 0 ) ) {
                    throw new
                    CacheException( 2 , "User "+_user.getName()+" is not authorized") ;
                }

                try{
                    //
                    synchronized( _messageLock ){
                        _sessions.put( Integer.valueOf(sessionId) ,
                        new ChgrpHandler(sessionId,commandId,args,resolvePath) ) ;
                    }
                }catch(CacheException ce ){
                    throw new CommandException(ce.getRc() ,
                    ce.getMessage() ) ;
                }catch(Exception e){
                    throw new CommandException(44 , e.getMessage() ) ;

                }
                return null ;
     }

    private synchronized String do_chmod(
            int sessionId , int commandId , VspArgs args , boolean resolvePath )
            throws Exception {


                _lastCommandTS = new Date() ;
                if( args.argc() < 1 )
                    throw new
                    CommandException( 3  , "Not enough arguments for chmod" ) ;

                if( _sessions.get( Integer.valueOf(sessionId) ) != null )
                    throw new
                    CommandException( 5 , "Duplicated session id" ) ;

                if( _userMetaProvider != null ){
                	getUserMetadata( _user.getName(), _user.getRole() , _userMetaProvider ) ;
                }
                _cell.say("Door authenticated for "+
                		_user.getName()+"("+_user.getRole()+","+_userUid+","+
                _userGid+","+_userHome+")");
                if( _authorizationStrong && ( _userUid < 0 ) ) {
                    throw new
                    CacheException( 2 , "User "+_user.getName()+" is not authorized") ;
                }

                try{
                    //
                    synchronized( _messageLock ){
                        _sessions.put( Integer.valueOf(sessionId) ,
                        new ChmodHandler(sessionId,commandId,args,resolvePath) ) ;
                    }
                }catch(CacheException ce ){
                    throw new CommandException(ce.getRc() ,
                    ce.getMessage() ) ;
                }catch(Exception e){
                    throw new CommandException(44 , e.getMessage() ) ;

                }
                return null ;
            }

    private synchronized String do_opendir(  int sessionId , int commandId , VspArgs args )
    throws Exception {


        _lastCommandTS = new Date() ;
        if( args.argc() < 1 )
            throw new
            CommandException( 3  , "Not enough arguments for opendir" ) ;

        if( _sessions.get( Integer.valueOf(sessionId) ) != null )
            throw new
            CommandException( 5 , "Duplicated session id" ) ;

        if( _userMetaProvider != null ){
        	getUserMetadata( _user.getName(), _user.getRole() , _userMetaProvider ) ;
        }
        _cell.say("Door authenticated for "+
        		_user.getName()+"("+_user.getRole()+","+_userUid+","+
        _userGid+","+_userHome+")");
        if( _authorizationStrong && ( _userUid < 0 ) ) {
            throw new
            CacheException( 2 , "User "+_user.getName()+" is not authorized") ;
        }

        try{
            //
            SessionHandler se =  new OpenDirHandler(sessionId,commandId,args);

            // if user authentificated tell it to Session Handler
            if( _userMetaProvider != null ) {

                se.setOwner( _user.getName() ) ;
                if( _userUid >= 0 ) {
                    se.setUid(_userUid );
                }
                if( _userGid >= 0 ) {
                    se.setGid(_userGid );
                }
            }

            synchronized( _messageLock ){
                _sessions.put( Integer.valueOf(sessionId) , se ) ;
            }
        }catch(CacheException ce ){
            throw new CommandException(ce.getRc() ,
            ce.getMessage() ) ;
        }catch(Exception e){
            _cell.esay(e);
            throw new CommandException(44 , e.getMessage() ) ;

        }
        return null ;
    }

    private synchronized String get_stat(
    int sessionId , int commandId , VspArgs args , boolean resolvePath )
    throws Exception {


        _lastCommandTS = new Date() ;
        if( args.argc() < 1 )
            throw new
            CommandException( 3  , "Not enough arguments for stat" ) ;

        if( _sessions.get( Integer.valueOf(sessionId) ) != null )
            throw new
            CommandException( 5 , "Duplicated session id" ) ;

        try{
            //
            synchronized( _messageLock ){
                _sessions.put( Integer.valueOf(sessionId) ,
                new StatHandler(sessionId,commandId,args,resolvePath) ) ;
            }
        }catch(CacheException ce ){
            throw new CommandException(ce.getRc() ,
            ce.getMessage() ) ;
        }catch(Exception e){
            throw new CommandException(44 , e.getMessage() ) ;

        }
        return null ;
    }
    public synchronized String com_ping( int sessionId , int commandId , VspArgs args )
    throws Exception {


        String problem = ""+sessionId+" "+commandId+" server pong" ;

        println( problem ) ;


        return null ;
    }
    public synchronized String com_check( int sessionId , int commandId , VspArgs args )
    throws Exception {


        _lastCommandTS = new Date() ;
        if( args.argc() < 1 )
            throw new
            CommandException( 3  , "Not enough arguments for check" ) ;

        if( _sessions.get( Integer.valueOf(sessionId) ) != null )
            throw new
            CommandException( 5 , "Duplicated session id" ) ;

        if( _userMetaProvider != null ){
        	getUserMetadata( _user.getName(), _user.getRole() , _userMetaProvider ) ;
        }
        _cell.say("Door authenticated for "+
        		_user.getName()+"("+_user.getRole()+","+_userUid+","+
        _userGid+","+_userHome+")");
        if( _authorizationStrong && ( _userUid < 0 ) ) {
            throw new
            CacheException( 2 , "User "+_user.getName()+" is not authorized") ;
        }


        List<String> assumedLocations;

        try {
            PnfsId pnfsId = new PnfsId( args.argv(0));
            assumedLocations = _pnfs.getCacheLocations( pnfsId ) ;
        }catch(IllegalArgumentException iae ) {
            DCapUrl url = new DCapUrl( args.argv(0) );
            String  fileName = url.getFilePart() ;
            if( fileName == null )
                throw new
                IllegalArgumentException("Not a valid filepart in : "+args.argv(0));
            assumedLocations = _pnfs.getCacheLocationsByPath( fileName ) ;
        }

        if( assumedLocations.isEmpty() ) {
            throw new
            CacheException( 4 , "File is not cached" ) ;
        }

        //VP
        try{
            //
            //  we have to make sure not to receive messages
            //  before we have stored our sessionId in the hash.
            //  (==synchronized)
            synchronized( _messageLock ){
                _sessions.put( Integer.valueOf(sessionId) ,
                new CheckFileHandler(sessionId,commandId,args, assumedLocations) ) ;
            }
        }catch(CacheException ce ){
            throw new CommandException(ce.getRc() ,
            ce.getMessage() ) ;
        }catch(Exception e){
            throw new CommandException(44 , e.getMessage() ) ;

        }
        return null ;
    }
    public synchronized String
    com_status( int sessionId , int commandId , VspArgs args )
    throws Exception {


        _lastCommandTS = new Date() ;
        IoHandler io = (IoHandler)_sessions.get( Integer.valueOf(sessionId) ) ;
        if( io == null )
            throw new
            CommandException( 5 , "Session ID "+sessionId+" not found." ) ;


        return ""+sessionId+" "+commandId+" "+args.getName()+
        " ok "+" 0 " + "\""+io.toString()+ "\"" ;
    }
    public String hh_get_door_info = "[-binary]" ;
    public Object ac_get_door_info( Args args ){
        IoDoorInfo info = new IoDoorInfo( _cell.getCellName() ,
        _cell.getCellDomainName() ) ;
        info.setProtocol("dcap","3");
        info.setOwner( _uid == null ? "0" : _uid ) ;
        info.setProcess( _pid == null ? "0" : _pid ) ;
        List<IoDoorEntry> list = new ArrayList<IoDoorEntry>(_sessions.size()) ;
        synchronized( this ){
            for( SessionHandler session: _sessions.values() ){
                if( ! ( session instanceof IoHandler ) )continue ;
                list.add( ((IoHandler)session).getIoDoorEntry() ) ;
            }
        }
        info.setIoDoorEntries( list.toArray(new IoDoorEntry[list.size()]) );
        if( args.getOpt("binary") != null )
            return info ;
        else
            return info.toString() ;
    }
    public String hh_toclient = " <id> <subId> server <command ...>" ;
    public String ac_toclient_$_3_99( Args args )throws Exception {
        StringBuffer sb = new StringBuffer() ;
        for( int i = 0 ; i < args.argc() ; i++ )sb.append(args.argv(i)).append(" ");
        String str = sb.toString() ;
        _cell.say("toclient (commander) : "+str ) ;
        println(str);
        return "" ;
    }
    public String hh_retry = "<sessionId> [-weak]" ;
    public String ac_retry_$_1( Args args ) throws Exception {
        int sessionId = Integer.parseInt(args.argv(0));
        SessionHandler session = null ;
        if( ( session = _sessions.get( Integer.valueOf(sessionId) ) ) == null )
            throw new
            CommandException( 5 , "No such session ID "+sessionId ) ;

        if( ! ( session instanceof PnfsSessionHandler ) )
            throw new
            CommandException( 6 , "Not a PnfsSessionHandler "+sessionId+
            " but "+session.getClass().getName() ) ;

        ((PnfsSessionHandler)session).again( args.getOpt("weak") == null ) ;

        return "" ;
    }
    ////////////////////////////////////////////////////////////////////
    //
    //      the client handler
    //
    protected  class SessionHandler                {
        protected VspArgs _vargs     = null ;
        protected int     _sessionId = 0 ;
        protected int     _commandId = 0 ;
        protected boolean _verbose   = false ;
        protected long    _started   = System.currentTimeMillis() ;
        protected DoorRequestInfoMessage _info   = null ;

        protected String  _status          = "<init>" ;
        protected long    _statusSince     = System.currentTimeMillis() ;
        protected String  _ioHandlerQueue  = null ;

        private long      _timestamp       = System.currentTimeMillis() ;
        private int       _uid             = -1 ;
        private int       _gid             = -1 ;
        private String    _owner           =  "unknown";

        protected SessionHandler( int sessionId , int commandId , VspArgs args )
        throws Exception {

            _sessionId = sessionId ;
            _commandId = commandId ;
            _vargs     = args ;

            _info      = new DoorRequestInfoMessage(
            _cell.getNucleus().getCellName()+"@"+
            _cell.getNucleus().getCellDomainName() ) ;

            try{ _uid = Integer.parseInt( args.getOpt("uid") ) ; } catch(NumberFormatException e){/* 'bad' strings silently ignored */}

            _ioHandlerQueue = args.getOpt("io-queue") ;
            _ioHandlerQueue = (_ioHandlerQueue == null ) || ( _ioHandlerQueue.length() == 0 ) ?
                               null : _ioHandlerQueue ;
        }
        protected void sendComment( String comment ){
            String reply = ""+_sessionId+" 1 "+
            _vargs.getName()+" "+comment ;
            println( reply ) ;
            _cell.say( reply ) ;
        }
        public void keepAlive(){
            _cell.say("Keep alived called for : "+this);
        }
        protected void sendReply( String tag , int rc , String msg ){

            String problem = null ;
            _info.setTransactionTime( System.currentTimeMillis()-_timestamp);
            if( rc == 0 ){
                problem = ""+_sessionId+
                " "+_commandId+
                " "+_vargs.getName()+
                " ok"   ;

                _cell.say( tag+" : "+problem ) ;
            }else{
                problem = ""+_sessionId+
                " "+_commandId+
                " "+_vargs.getName()+
                " failed "+rc +
                " \""+msg+"\""  ;

                _cell.esay( tag+" : "+problem ) ;
                _info.setResult( rc , msg ) ;
            }
            println( problem ) ;
            try{
                _cell.sendMessage(
                new CellMessage( new CellPath("billing") ,
                _info ) ) ;
            }catch(Exception ee){
                _cell.esay("Couldn't send billing info : "+ee );
            }
            return ;
        }
        protected void sendReply( String tag , Message msg ){

            sendReply( tag ,
            msg.getReturnCode() ,
            msg.getErrorObject().toString() ) ;

        }
        protected synchronized void removeUs(){
            _sessions.remove( Integer.valueOf(_sessionId ) ) ;
        }
        protected void setStatus( String status ){
            _status = status ;
            _statusSince = System.currentTimeMillis() ;
        }
        @Override
        public String toString(){
            return "["+_sessionId+"]["+_uid+"]["+_pid+"] "+
            _status+"("+
            ( (System.currentTimeMillis()-_statusSince)/1000 )+")" ;
        }
        protected int getGid() {
            return _gid;
        }
        protected void setGid(int gid) {
            _gid = gid;
            _info.setGid( gid);
        }
        protected String getOwner() {
            return _owner;
        }
        protected void setOwner(String owner) {
            _owner = owner;
            _info.setOwner(owner);
        }
        protected int getUid() {
            return _uid;
        }
        protected void setUid(int uid) {
            _uid = uid;
            _info.setUid( uid );
        }
    }
    abstract protected  class PnfsSessionHandler  extends SessionHandler  {

        protected PnfsId       _pnfsId       = null ;
        protected String       _path         = null;
        protected boolean      _isUrl        = false ;
        protected StorageInfo  _storageInfo  = null ;
        protected FileMetaData _fileMetaData = null ;
        private   long         _timer        = 0L ;
        private   final Object       _timerLock    = new Object() ;
        private long           _timeout      = 0L ;

        protected PnfsGetStorageInfoMessage  _getStorageInfo  = null ;
        protected PnfsGetFileMetaDataMessage _getFileMetaData = null ;

        protected PnfsSessionHandler(
        int sessionId , int commandId , VspArgs args )
        throws Exception {

            this( sessionId , commandId , args , false , true ) ;
            _path           = args.getOpt("path");
            if(_path != null ) {
                _info.setPath(_path);
            }
            String tmp = args.getOpt("timeout") ;
            if( tmp != null ){
                try{
                    long timeout = Long.parseLong(tmp) ;
                    if( timeout > 0L ){
                        _timeout = timeout ;
                        say("PnfsSessionHandler : user timeout set to : "+_timeout ) ;
                        _timeout = System.currentTimeMillis() + ( _timeout * 1000L ) ;
                    }
                }catch(NumberFormatException e){/* 'bad' strings silently ignored */}
            }

        }
        protected PnfsSessionHandler(
        int sessionId , int commandId , VspArgs args ,
        boolean metaDataOnly , boolean resolvePath       )
        throws Exception {

            super( sessionId , commandId , args ) ;

            if( metaDataOnly )askForFileMetaData(resolvePath) ;
            else              askForStorageInfo() ;

        }
        @Override
        public void keepAlive(){
            synchronized( _timerLock ){
                if( ( _timeout > 0L ) && ( _timeout < System.currentTimeMillis() ) ){
                    esay("User timeout triggered") ;
                    sendReply("keepAlive" , 112 , "User timeout canceled session" ) ;
                    removeUs();
                    return ;
                }
                if( ( _timer > 0L ) && ( _timer < System.currentTimeMillis() ) ){
                    _timer = 0L ;
                    esay("Restarting session "+_sessionId);
                    try{
                        askForStorageInfo() ;
                    }catch(Exception ee ){
                        sendReply( "keepAlive" , 111 , ee.getMessage() )  ;
                        removeUs() ;
                        return ;
                    }
                }
            }
        }
        protected void setTimer( long timeout ){
            synchronized( _timerLock ){

                _timer = timeout == 0L ? 0L : System.currentTimeMillis() + timeout ;
            }
        }
        public void say( String msg ){
            _cell.say( (_pnfsId==null?"NoPnfsIdYet":_pnfsId.toString())+" : "+msg ) ;
        }
        public void esay( String msg ){
            _cell.esay( (_pnfsId==null?"NoPnfsIdYet":_pnfsId.toString())+" : "+msg ) ;
        }
        public void again( boolean strong ) throws Exception {
            askForStorageInfo() ;
        }

        private void askForStorageInfo() throws Exception {
            setTimer( 60 * 1000 ) ;

            try{
                _pnfsId         = new PnfsId(_vargs.argv(0)) ;
                _getStorageInfo = new PnfsGetStorageInfoMessage( _pnfsId ) ;
            }catch(Exception ee ){
                //
                // seems not to be a pnfsId, might be a url.
                // (if not, we let the exception go)
                //
                DCapUrl url      = new DCapUrl( _vargs.argv(0)) ;
                String  fileName = url.getFilePart() ;
                if( fileName == null )
                    throw new
                    IllegalArgumentException("Not a valid filepart in : "+_vargs.argv(0));

                _getStorageInfo = new PnfsGetStorageInfoMessage() ;
                _getStorageInfo.setPnfsPath( fileName ) ;
                _info.setPath(fileName);
                _isUrl     = true ;
            }

            say( "Requesting storageInfo for "+_getStorageInfo ) ;

            _getStorageInfo.setId(_sessionId) ;

            synchronized( _messageLock ){
                _cell.sendMessage(
                new CellMessage(
                new CellPath( _pnfsManagerName )  ,
                _getStorageInfo
                )
                ) ;
            }
            setStatus( "WaitingForPnfs" ) ;
        }
        private void askForFileMetaData( boolean resolvePath ) throws Exception {
            setTimer( 60 * 1000 ) ;

            try{
                _pnfsId          = new PnfsId(_vargs.argv(0)) ;
                _getFileMetaData = new PnfsGetFileMetaDataMessage( _pnfsId ) ;
            }catch(Exception ee ){
                //
                // seems not to be a pnfsId, might be a url.
                // (if not, we let the exception go)
                //
                DCapUrl url      = new DCapUrl( _vargs.argv(0)) ;
                String  fileName = url.getFilePart() ;
                if( fileName == null )
                    throw new
                    IllegalArgumentException("Not a valid filepart in : "+_vargs.argv(0));

                _getFileMetaData = new PnfsGetFileMetaDataMessage() ;
                _getFileMetaData.setPnfsPath( fileName ) ;
                _getFileMetaData.setResolve(resolvePath);
                _isUrl     = true ;
            }

            say( "Requesting FileMetaData for "+_getFileMetaData ) ;

            _getFileMetaData.setId(_sessionId) ;

            synchronized( _messageLock ){
                _cell.sendMessage(
                new CellMessage(
                new CellPath( _pnfsManagerName )  ,
                _getFileMetaData
                )
                ) ;
            }
            setStatus( "WaitingForPnfs (FileMetaData)" ) ;
        }
        public void
        pnfsGetStorageInfoArrived( PnfsGetStorageInfoMessage reply ){

            setTimer(0);

            _getStorageInfo = reply ;

            _cell.say( "pnfsGetStorageInfoArrived : "+_getStorageInfo ) ;

            if( _getStorageInfo.getReturnCode() != 0 ){
                try{
                    if( ! storageInfoNotAvailable() ){
                        sendReply( "pnfsGetStorageInfoArrived" , reply )  ;
                        removeUs() ;
                        return ;
                    }
                }catch(CacheException ce ){
                    sendReply( "pnfsGetStorageInfoArrived" , ce.getRc() , ce.getMessage() )  ;
                    removeUs() ;
                    return ;
                }
            }
            _storageInfo = _getStorageInfo.getStorageInfo() ;
            if( _pnfsId == null )_pnfsId = _getStorageInfo.getPnfsId() ;

            _info.setPnfsId( _pnfsId ) ;

            for( int i = 0 ; i < _vargs.optc() ; i++ ){
                String key   = _vargs.optv(i) ;
                String value = _vargs.getOpt(key) ;
                _storageInfo.setKey( key , value == null ? "" : value ) ;

            }

            storageInfoAvailable() ;
        }
        public void
        pnfsGetFileMetaDataArrived( PnfsGetFileMetaDataMessage reply ){

            setTimer(0);

            _getFileMetaData = reply ;

            _cell.say( "pnfsGetFileMetaDataArrived : "+_getFileMetaData ) ;

            if( _getFileMetaData.getReturnCode() != 0 ){
                try{
                    if( ! fileMetaDataNotAvailable() ){
                        sendReply( "pnfsGetFileMetaDataArrived" , reply )  ;
                        removeUs() ;
                        return ;
                    }
                }catch(CacheException ce ){
                    sendReply( "pnfsGetFileMetaDataArrived" , ce.getRc() , ce.getMessage() )  ;
                    removeUs() ;
                    return ;
                }
            }
            _fileMetaData = _getFileMetaData.getMetaData() ;
            if( _pnfsId == null )_pnfsId = _getFileMetaData.getPnfsId() ;

            _info.setPnfsId( _pnfsId ) ;
            if( _path == null ) _path = _getFileMetaData.getPnfsPath();
            if(_path != null ) _info.setPath(_path);

            fileMetaDataAvailable() ;
        }
        public void storageInfoAvailable(){}
        public void fileMetaDataAvailable(){}

        public boolean storageInfoNotAvailable() throws CacheException { return false ; }
        public boolean fileMetaDataNotAvailable() throws CacheException { return false ; }

        @Override
        public String toString(){
            return "["+_pnfsId+"]"+" {timer="+
            (_timer==0L?"off":""+(_timer-System.currentTimeMillis()))+"} "+
            super.toString() ;
        }
    }
    ////////////////////////////////////////////////////////////////////
    //
    //      the basic prestage handler
    //
    private CellPath _prestagerPath = new CellPath( "Prestager" ) ;
    protected  class PrestageHandler  extends PnfsSessionHandler  {
        private long   _time = 0L ;
        private String _destination = null ;
        private PrestageHandler( int sessionId , int commandId , VspArgs args )
        throws Exception {

            super( sessionId , commandId , args ) ;

            try{ _time = Long.parseLong( args.getOpt("stagetime" ) ) ; }catch(NumberFormatException e){/* 'bad' strings silently ignored */}
            _destination = args.getOpt( "location" ) ;
        }
        @Override
        public void storageInfoAvailable(){
            //
            // we are not called if the pnfs request failed.
            //
            StagerMessageV0 sm = new StagerMessageV0( _pnfsId ) ;
            sm.setStorageInfo( _storageInfo ) ;
            sm.setProtocol( "DCap",3,0,_destination);
            sm.setStageTime( _time ) ;

            CellMessage stagerMessage = new CellMessage( _prestagerPath , sm ) ;
            setStatus("Waiting for reply from Prestager");
            Message msg = null ;
            try{
                stagerMessage = _cell.sendAndWait(  stagerMessage , 20000 ) ;
                msg = (Message) stagerMessage.getMessageObject() ;
            }catch(Exception ee ){
                sendReply( "storageInfoAvailable", 2 , ee.toString() ) ;
                removeUs() ;
                return ;
            }
            if( msg.getReturnCode() != 0 ){
                sendReply( "storageInfoAvailable" ,
                msg.getReturnCode() ,
                (String)msg.getErrorObject()  ) ;
                removeUs() ;
                return ;
            }
            sendReply( "storageInfoAvailable" , 0 , "" ) ;
            removeUs() ;
            return ;
        }
        @Override
        public String toString(){ return "st "+super.toString() ; }
    }
    ////////////////////////////////////////////////////////////////////
    //
    //      the file stat handler
    //
    protected  class StatHandler  extends PnfsSessionHandler  {

        private StatHandler( int sessionId ,
        int commandId ,
        VspArgs args ,
        boolean followLinks )
        throws Exception {

            super( sessionId , commandId , args , true , followLinks ) ;
        }
        @Override
        public void fileMetaDataAvailable(){
            //
            // we are not called if the pnfs request failed.
            //
            FileMetaData meta = _getFileMetaData.getMetaData() ;

            StringBuilder sb = new StringBuilder() ;
            sb.append(_sessionId).append(" ").
            append(_commandId).append(" ").
            //FIXME: do we support links?
            // append(_vargs.getName()).append(_followLinks?" stat ":" stat ");
            append(_vargs.getName()).append(" stat ");

            sb.append("-st_size=").append(meta.getFileSize()).append(" ");
            sb.append("-st_uid=").append(meta.getUid()).append(" ");
            sb.append("-st_gid=").append(meta.getGid()).append(" ");
            sb.append("-st_atime=").append(meta.getLastAccessedTime()/1000).append(" ");
            sb.append("-st_mtime=").append(meta.getLastModifiedTime()/1000).append(" ");
            sb.append("-st_ctime=").append(meta.getCreationTime()/1000).append(" ");

            FileMetaData.Permissions user  = meta.getUserPermissions() ;
            FileMetaData.Permissions group = meta.getGroupPermissions();
            FileMetaData.Permissions world = meta.getWorldPermissions() ;

            sb.append("-st_mode=").
            append(meta.isRegularFile()?"-":
                meta.isDirectory()?"d":
                    meta.isSymbolicLink()?"l":"x").
                    append(user.canRead()?"r":"-").
                    append(user.canWrite()?"w":"-").
                    append(user.canExecute()?"x":"-").
                    append(group.canRead()?"r":"-").
                    append(group.canWrite()?"w":"-").
                    append(group.canExecute()?"x":"-").
                    append(world.canRead()?"r":"-").
                    append(world.canWrite()?"w":"-").
                    append(world.canExecute()?"x":"-").
                    append(" ") ;

                    sb.append("-st_ino=").append(_pnfsId.intValue()&0xfffffff) ;

                    println( sb.toString() ) ;
                    removeUs() ;
                    return ;
        }
        @Override
        public String toString(){ return "st "+super.toString() ; }
    }

    ////////////////////////////////////////////////////////////////////
    //
    //      the file unlink handler
    //
    protected  class UnlinkHandler  extends PnfsSessionHandler  {
        private UnlinkHandler( int sessionId ,
        int commandId ,
        VspArgs args ,
        boolean followLinks )
        throws Exception {

            super( sessionId , commandId , args , true , followLinks ) ;
        }
        @Override
        public void fileMetaDataAvailable(){
            //
            // we are not called if the pnfs request failed.
            //

            String path = _getFileMetaData.getPnfsPath();

            StringBuffer sb = new StringBuffer( );
            sb.append(_sessionId).append(" ").
            append(_commandId).append(" ").
            append(_vargs.getName()).append(" ");

            FileMetaData meta = _getFileMetaData.getMetaData() ;
            if( meta.isDirectory() ) {
                sb.append("failed 17 \"Path is a Directory\" EISDIR");
            }else{

                boolean fileWriteAllowed;
                boolean dirWriteAllowed;

                // FIXME: make use of permission handle
                try {
                    String parent = new File( path ).getParent() ;
                    PnfsGetStorageInfoMessage parentInfo = _pnfs.getStorageInfoByPath(parent);
                    FileMetaData dirMeta = parentInfo.getMetaData() ;

                    FileMetaData.Permissions dirUser  = dirMeta.getUserPermissions() ;
                    FileMetaData.Permissions dirGroup = dirMeta.getGroupPermissions();
                    FileMetaData.Permissions dirWorld = dirMeta.getWorldPermissions() ;

                    FileMetaData.Permissions user  = meta.getUserPermissions() ;
                    FileMetaData.Permissions group = meta.getGroupPermissions();
                    FileMetaData.Permissions world = meta.getWorldPermissions() ;

                    dirWriteAllowed =
                    ( ( dirMeta.getUid() == _userUid ) && dirUser.canWrite()  ) ||
                    ( ( dirMeta.getGid() == _userGid ) && dirGroup.canWrite() ) ||
                    dirWorld.canWrite() ;

                    fileWriteAllowed =
                    ( ( meta.getUid() == _userUid ) && user.canWrite()  ) ||
                    ( ( meta.getGid() == _userGid ) && group.canWrite() ) ||
                    world.canWrite() ;

                }catch(CacheException e) {
                    dirWriteAllowed = false;
                    fileWriteAllowed = false;
                }

                if( ! (dirWriteAllowed  && fileWriteAllowed) || _readOnly) {
                    sb.append("failed 19 \"Permission denied\" EACCES");
                    esay("Permission denied for user: " + _userUid +" grop: " + _userGid + "to unlink a file: "+path);
                }else{

                    try {
                        _pnfs.deletePnfsEntry( path );
                        sb.append("ok");
                    }catch( CacheException e) {
                        sb.append("failed 22 \"" + e.getMessage() + "\"");
                    }
                }
            }

            println( sb.toString() ) ;
            removeUs() ;

            return ;
        }
        @Override
        public String toString(){ return "uk "+super.toString() ; }
    }

    protected  class ChmodHandler  extends PnfsSessionHandler  {

        private int _permission = 0;
        private ChmodHandler( int sessionId ,
        int commandId ,
        VspArgs args ,
        boolean followLinks )
        throws Exception {

            super( sessionId , commandId , args , true , followLinks ) ;

            _permission = Integer.parseInt( args.getOpt("mode") );
        }
        @Override
        public void fileMetaDataAvailable(){
            //
            // we are not called if the pnfs request failed.
            //


            StringBuffer sb = new StringBuffer( );
            sb.append(_sessionId).append(" ").
            append(_commandId).append(" ").
            append(_vargs.getName()).append(" ");

            FileMetaData meta = _getFileMetaData.getMetaData() ;

            if( _userUid == meta.getUid() ) {


                meta.setUserPermissions( new FileMetaData.Permissions ( (_permission >> 6 ) & 0x7 ) ) ;
                meta.setGroupPermissions(new FileMetaData.Permissions ( (_permission >> 3 ) & 0x7 )) ;
                meta.setWorldPermissions( new FileMetaData.Permissions( _permission  & 0x7 ) ) ;

                _pnfs.pnfsSetFileMetaData(_pnfsId, meta);

                sb.append("ok");
            }else{
                sb.append("failed 23 \"permission deny\" EACCES");
            }

            println( sb.toString() ) ;
            removeUs() ;

            return ;
        }
        @Override
        public String toString(){ return "uk "+super.toString() ; }
    }


    protected  class ChownHandler  extends PnfsSessionHandler  {

        private int _owner = -1;
        private int _group = -1;

        private ChownHandler( int sessionId ,  int commandId ,  VspArgs args ,  boolean followLinks )
        throws Exception {

            super( sessionId , commandId , args , true , followLinks ) ;

            String[] owner_group = args.getOpt("owner").split("[:]");
            _owner  = Integer.parseInt(owner_group[0]);
            if( owner_group.length == 2 ) {
                _group = Integer.parseInt(owner_group[1]);
            }
        }

        @Override
        public void fileMetaDataAvailable(){
            //
            // we are not called if the pnfs request failed.
            //

            StringBuffer sb = new StringBuffer( );
            sb.append(_sessionId).append(" ").
            append(_commandId).append(" ").
            append(_vargs.getName()).append(" ");

            FileMetaData meta = _getFileMetaData.getMetaData() ;

            if( _userUid == meta.getUid() ) {

                if( _owner >= 0 ) {
                    meta.setUid( _owner );
                }

                if( _group >= 0 ) {
                    meta.setGid( _group );
                }

                _pnfs.pnfsSetFileMetaData(_pnfsId, meta);

                sb.append("ok");
            }else{
                sb.append("failed 23 \"permission deny\" EACCES");
            }


            println( sb.toString() ) ;
            removeUs() ;

            return ;
        }
        @Override
        public String toString(){ return "uk "+super.toString() ; }
    }

    protected  class ChgrpHandler  extends PnfsSessionHandler  {

        private int _group = -1;

        private ChgrpHandler( int sessionId ,  int commandId ,  VspArgs args ,  boolean followLinks )
        throws Exception {

            super( sessionId , commandId , args , true , followLinks ) ;

            _group = Integer.parseInt(args.getOpt("group"));
        }

        @Override
        public void fileMetaDataAvailable(){
            //
            // we are not called if the pnfs request failed.
            //

            StringBuffer sb = new StringBuffer( );
            sb.append(_sessionId).append(" ").
            append(_commandId).append(" ").
            append(_vargs.getName()).append(" ");

            FileMetaData meta = _getFileMetaData.getMetaData() ;

            if( _userUid == meta.getUid() ) {

                if( _group >= 0 ) {
                    meta.setGid( _group );
                }

                _pnfs.pnfsSetFileMetaData(_pnfsId, meta);

                sb.append("ok");
            }else{
                sb.append("failed 23 \"permission deny\" EACCES");
            }


            println( sb.toString() ) ;
            removeUs() ;

            return ;
        }
        @Override
        public String toString(){ return "uk "+super.toString() ; }
    }


    protected  class RenameHandler  extends PnfsSessionHandler  {


        private String _newName = null;

        private RenameHandler( int sessionId ,
        int commandId ,
        VspArgs args  )
        throws Exception {

            super( sessionId , commandId , args , true , false ) ;
            _newName = args.argv(1);

        }

        @Override
        public void fileMetaDataAvailable(){
            //
            // we are not called if the pnfs request failed.
            //


            StringBuffer sb = new StringBuffer( );
            sb.append(_sessionId).append(" ").
            append(_commandId).append(" ").
            append(_vargs.getName()).append(" ");

            FileMetaData meta = _getFileMetaData.getMetaData() ;

            boolean fileWriteAllowed = false;

            FileMetaData.Permissions user  = meta.getUserPermissions() ;
            FileMetaData.Permissions group = meta.getGroupPermissions();
            FileMetaData.Permissions world = meta.getWorldPermissions() ;


            fileWriteAllowed =
            ( ( meta.getUid() == _userUid ) && user.canWrite()  ) ||
            ( ( meta.getGid() == _userGid ) && group.canWrite() ) ||
            world.canWrite() ;


            if( ! fileWriteAllowed || _readOnly) {
                sb.append("failed 19 \"Permission denied\" EACCES");
                esay("Permission denied for user: " + _userUid +" grop: " + _userGid + "to rename a file: "+ _pnfsId);
            }else{


                try{
                    _pnfs.renameEntry(_pnfsId, _newName);
                    sb.append("ok");
                }catch(Exception e){
                    sb.append("failed 23 \"" + e.getMessage() + "\"");
                }

            }
            println( sb.toString() ) ;
            removeUs() ;

            return ;
        }
        @Override
        public String toString(){ return "rn "+super.toString() ; }
    }


    ////////////////////////////////////////////////////////////////////
    //
    //      the file rmdir handler
    //
    protected  class RmDirHandler  extends PnfsSessionHandler  {

        private RmDirHandler( int sessionId ,
        int commandId ,
        VspArgs args ,
        boolean followLinks )
        throws Exception {

            super( sessionId , commandId , args , true , followLinks ) ;

        }
        @Override
        public void fileMetaDataAvailable(){
            //
            // we are not called if the pnfs request failed.
            //

            String path = _getFileMetaData.getPnfsPath();

            StringBuffer sb = new StringBuffer( );
            sb.append(_sessionId).append(" ").
            append(_commandId).append(" ").
            append(_vargs.getName()).append(" ");

            try {

                if( ! _permissionHandler.canDeleteDir(_userUid, _userGid , path) ) {
                    sb.append("failed 19 \"Permission denied\" EACCES");
                    esay("Permission denied for user: " + _userUid +" grop: " + _userGid + " to unlink a dir: "+path);
                }else{
                    _pnfs.deletePnfsEntry( path );
                    sb.append("ok");
                }

            }catch( CacheException e) {
                sb.append("failed 21 \"" + e.getMessage() + "\"");
            }

            println( sb.toString() ) ;
            removeUs() ;

            return ;
        }
        @Override
        public String toString(){ return "uk "+super.toString() ; }
    }

    protected  class MkDirHandler  extends PnfsSessionHandler  {

        int _perm = 0755;

        private MkDirHandler( int sessionId ,
        int commandId ,
        VspArgs args ,
        boolean followLinks )
        throws Exception {

            super( sessionId , commandId , args , true , followLinks ) ;

            String pMode = args.getOpt("mode");
            if( pMode != null) {
                try {
                    _perm = Integer.parseInt(pMode);
                }catch(NumberFormatException e) {
                    // do nothing
                }
            }

        }


        @Override
        public boolean fileMetaDataNotAvailable() {

            boolean rc = false;

            StringBuffer sb = new StringBuffer( );
            sb.append(_sessionId).append(" ").
            append(_commandId).append(" ").
            append(_vargs.getName()).append(" ");

            try {
                String path = _getFileMetaData.getPnfsPath();

                say("creating directory " + path + ", with mode=" + _perm);
                if( _permissionHandler.canCreateDir(_userUid, _userGid , path) ) {
                    _pnfs.createPnfsDirectory( path , _userUid, _userGid, _perm );
                    sb.append("ok") ;
                }else{
                    sb.append("failed 19 \"Permission denied\" EACCES");
                    rc = true;
                }

            }catch( CacheException ce ){
                sb.append("failed 19 \"Can't make a directory\"");
                rc = true;
            }

            println( sb.toString() ) ;
            return rc;
        }


        @Override
        public void fileMetaDataAvailable() {

            StringBuffer sb = new StringBuffer( );
            sb.append(_sessionId).append(" ").
            append(_commandId).append(" ").
            append(_vargs.getName()).append(" ");


            sb.append("failed 20 \"Directory exist\" EEXIST");
            println( sb.toString() ) ;
            removeUs() ;
            return ;

        }

        @Override
        public String toString(){ return "mk "+super.toString() ; }
    }


    ////////////////////////////////////////////////////////////////////
    //
    //      the check file handler
    //
    protected  class CheckFileHandler  extends PnfsSessionHandler  {

        private final String _destination  ;
        private final List<String>  _assumedLocations;
        private final String _protocolName  ;

        private CheckFileHandler( int sessionId , int commandId , VspArgs args, List<String> locations )
        throws Exception {

            super( sessionId , commandId , args ) ;

            _info.setMessageType("check") ;
            _destination      = args.getOpt( "location" ) ;
            String protocolName = args.getOpt( "protocol" ) ;
            if( protocolName == null) {
            	_protocolName     = "DCap/3" ;
            }else{
            	_protocolName     = protocolName ;
            }
            _assumedLocations = locations;
        }
        @Override
        public void storageInfoAvailable(){
            //
            //    i) check pnfs for possible file locations.
            //       if not found : send 'File not cached'
            //   ii) Get PoolManager database entries for this
            //       file : which pools would be allowed to
            //       deliver this file to this destination.
            //  iii) If there is no match between the
            //       assumed pools and the pools specified
            //       by the PoolManager database, we
            //       send 'File not cached'.
            //   vi) if option 'check' != strict, we
            //       send 'File found'.
            //  vii) if not, we check each matched pool
            //       for the fileentry.
            //
            try{


                //
                // we are not called if the pnfs request failed.
                //
                PoolMgrQueryPoolsMsg query =
                new PoolMgrQueryPoolsMsg( "read" ,
                _storageInfo.getStorageClass()+"@"+
                _storageInfo.getHsm() ,
                _storageInfo.getCacheClass() ,
                _protocolName ,
                _destination ,
                null ) ;

                CellMessage checkMessage = new CellMessage( _poolMgrPath , query ) ;
                setStatus("Waiting for reply from PoolManager");

                checkMessage = _cell.sendAndWait(  checkMessage , 20000 ) ;
                query = (PoolMgrQueryPoolsMsg) checkMessage.getMessageObject() ;

                if( query.getReturnCode() != 0 )
                    throw new
                    CacheException( query.getReturnCode() ,
                    query.getErrorObject() == null ? "?" :
                        query.getErrorObject().toString()      ) ;
                        //
                        //
                        Set<String>   assumedHash = new HashSet<String>( _assumedLocations ) ;
                        List<String> []   lists       = query.getPools() ;
                        List<String> result      = new ArrayList<String>() ;

                        for( int i = 0 ; i < lists.length ; i++ ){
                            for( String pool: lists[i] ){
                                if( assumedHash.contains(pool) )
                                    result.add( pool ) ;
                            }
                        }

                        if( result.size() == 0 )
                            throw new
                            CacheException(4,"File not cached") ;


                        if( _checkStrict ){

                            SpreadAndWait controller = new SpreadAndWait( _cell.getNucleus(), 10000 ) ;

                            for( String pool: result ){

                                say("Sending query to pool : "+pool ) ;
                                PoolCheckFileCostMessage request =
                                new PoolCheckFileCostMessage( pool , _pnfsId , 0L ) ;
                                controller.send( new CellMessage( new CellPath(pool) , request ) );
                            }
                            controller.waitForReplies() ;
                            int numberOfReplies = controller.getReplyCount() ;
                            say("Number of valied replies : "+numberOfReplies);
                            if( numberOfReplies == 0 )
                                throw new
                                CacheException(4,"File not cached") ;

                            Iterator<CellMessage> iterate = controller.getReplies() ;
                            int found = 0 ;
                            while( iterate.hasNext() ){
                                CellMessage msg = iterate.next() ;
                                Object obj = msg.getMessageObject() ;
                                if( ! ( obj instanceof PoolCheckFileCostMessage ) ){
                                    esay("Unexpected reply from PoolCheckFileCostMessage : "+
                                    obj.getClass().getName() ) ;
                                    continue ;
                                }
                                PoolCheckFileCostMessage reply = (PoolCheckFileCostMessage)obj ;
                                if( reply.getHave() ){
                                    say(" pool " +reply.getPoolName()+" ok" ) ;
                                    found ++ ;
                                }else{
                                    say(" pool " +reply.getPoolName()+" File not found" ) ;
                                }
                            }
                            if( found == 0 )
                                throw new
                                CacheException(4,"File not cached") ;

                        }
            }catch(CacheException cee ){
                // timur: since this situation can occur during the normal
                //        operation (dc_check of non-cached file)
                //        I am changing the following log to be
                //        logged only if the debugging is on
                //        anyway sendReply will log the response
                if((_cell.getNucleus().getPrintoutLevel() & CellNucleus.PRINT_CELL ) > 0 ) {
                    _cell.esay(cee);
                }
                sendReply( "storageInfoAvailable" , cee.getRc() , cee.getMessage()) ;
                removeUs() ;
                return ;
            }catch(Exception ee ){
                _cell.esay(ee);
                sendReply( "storageInfoAvailable" , 104 , ee.getMessage()) ;
                removeUs() ;
                return ;
            }


            sendReply( "storageInfoAvailable" , 0 , "" ) ;
            removeUs() ;
            return ;
        }
        @Override
        public String toString(){ return "ck "+super.toString() ; }
    }
    ////////////////////////////////////////////////////////////////////
    //
    //      the basic IO handler
    //
    protected  class IoHandler  extends PnfsSessionHandler  {
        private String           _ioMode       = null ;
        private DCapProtocolInfo _protocolInfo = null ;
        private String           _pool         = "<unknown>" ;
        private String []        _hosts        = null ;
        private boolean          _isHsmRequest = false ;
        private boolean          _overwrite    = false ;
        private String           _checksumString  = null ;
        private boolean          _truncate        = false;
        private boolean          _isNew           = false;
        private String            _truncFile       = null;
        private boolean          _poolRequestDone = false ;
        private String            _permission      = null;
        private boolean          _fileCheck = true;
        private boolean          _passive = false;
        private String            _accessLatency = null;
        private String            _retentionPolicy = null;

        private IoHandler( int sessionId , int commandId , VspArgs args )
        throws Exception {

            super( sessionId , commandId , args ) ;

            _ioMode = _vargs.argv(1) ;
            int   port    = Integer.parseInt( _vargs.argv(3) ) ;

            StringTokenizer st = new StringTokenizer( _vargs.argv(2) , "," ) ;
            _hosts    = new String[st.countTokens()]  ;
            for( int i = 0 ; i < _hosts.length ; i++ )_hosts[i] = st.nextToken() ;
            //
            //
            _protocolInfo = new DCapProtocolInfo( "DCap",3,0, _hosts , port  ) ;
            _protocolInfo.setSessionId( _sessionId ) ;
            if( _isAuthorized ) {
                _protocolInfo.setVOInfo(_voInfo);
            }

            _isHsmRequest = ( args.getOpt("hsm") != null  );
            if( _isHsmRequest ){
                say("Hsm Feature Requested" ) ;
                if( _hsmManager == null )
                    throw new
                    CacheException( 105 , "Hsm Support Not enabled" ) ;
            }

            _overwrite      = args.getOpt("overwrite")   != null ;
            _strictSize     = args.getOpt("strict-size") != null ;
            _checksumString = args.getOpt("checksum") ;
            _truncFile      = args.getOpt("truncate");
            _truncate       = ( _truncFile != null ) && _truncateAllowed  ;
            _permission     = args.getOpt("mode");
            _fileCheck      = args.getOpt("skip-file-check") == null;
            _protocolInfo.fileCheckRequaried(_fileCheck);

            _passive        = args.getOpt("passive") != null;
            _protocolInfo.isPassive(_passive);
            _accessLatency = args.getOpt("access-latency");
            _retentionPolicy = args.getOpt("retention-policy");

            _protocolInfo.door( new CellPath(_cell.getCellName(), _cell.getCellDomainName()) ) ;

        }

        public IoDoorEntry getIoDoorEntry(){
            return
            new
            IoDoorEntry( _sessionId ,
            _pnfsId ,
            _pool ,
            _status ,
            _statusSince ,
            _hosts[0] );
        }
        @Override
        public void again( boolean strong ) throws Exception {
            if( strong )_poolRequestDone = false ;
            super.again(strong);
        }

        @Override
        public boolean storageInfoNotAvailable() throws CacheException {
            //
            // hsm only support files in the cache.
            //
            if( _isHsmRequest )
                throw new
                CacheException( 107 , "Hsm only supports existing files" ) ;
            //
            // if this is not a url it's of course and error.
            //
            if( ! _isUrl )return false ;
            _cell.say("storageInfoNotAvailable : is url (mode="+_ioMode+")");

            if( _ioMode.equals("r") )
                throw new
                CacheException( 2 , "No such file or directory" );
            //
            // for now we regard each error as 'file not found'
            // (so we can create it);
            //
            // first we have to findout if we are allowed to create a file.
            //
            String parent = new File(_getStorageInfo.getPnfsPath() ).getParent() ;
            PnfsGetStorageInfoMessage parentInfo = _pnfs.getStorageInfoByPath(parent);
            FileMetaData meta = parentInfo.getMetaData() ;
            _cell.say("Parent meta of "+parent+" : "+meta ) ;
            FileMetaData.Permissions user  = meta.getUserPermissions() ;
            FileMetaData.Permissions group = meta.getGroupPermissions();
            FileMetaData.Permissions world = meta.getWorldPermissions() ;
            boolean writeAllowed =
            ( ( meta.getUid() == _userUid ) && user.canWrite()  ) ||
            ( ( meta.getGid() == _userGid ) && group.canWrite() ) ||
            world.canWrite() ;
            //
            if( _authorizationStrong && ( _userUid < 0 ) )
                throw new
                CacheException( 2 , "No Meta data found for user : "+_user.getName() ) ;
            if( ! writeAllowed || _readOnly)
                throw new
                CacheException( 2 , "Permission denied (Parent)" );

            int uid = 0 ;
            try{ uid = Integer.parseInt(_uid) ; }catch(NumberFormatException e){/* 'bad' strings silently ignored */}

            int mode = 0644;
            if( _permission != null ) {
                try { mode = Integer.parseInt(_permission, 8); } catch(NumberFormatException e){/* 'bad' strings silently ignored */}
            }
            PnfsCreateEntryMessage pnfsEntry =
            _pnfs.createPnfsEntry( _getStorageInfo.getPnfsPath() ,
            _userUid < 0 ? uid : _userUid ,
            _userGid < 0 ? 0 : _userGid ,
            mode ) ;

            _cell.say("storageInfoNotAvailable : created pnfsid : "
            +pnfsEntry.getPnfsId() + " path : "+pnfsEntry.getPnfsPath());
            _getStorageInfo = pnfsEntry ;
            _isNew = true;

            return true ;
        }

        @Override
        public void storageInfoAvailable(){

            _cell.say(_pnfsId.toString()+" storageInfoAvailable after "+
            (System.currentTimeMillis()-_started) );

            PoolMgrSelectPoolMsg getPoolMessage = null ;

            FileMetaData meta = _getStorageInfo.getMetaData() ;

            if( ! meta.isRegularFile() ){
                sendReply( "pnfsGetStorageInfoArrived", 1 ,
                "Not a File" ) ;
                removeUs() ;
                return ;
            }


            FileMetaData.Permissions user  = meta.getUserPermissions() ;
            FileMetaData.Permissions group = meta.getGroupPermissions();
            FileMetaData.Permissions world = meta.getWorldPermissions() ;
            boolean writeAllowed = _isUrl  ||
            ( ( meta.getUid() == _userUid ) && user.canWrite()  ) ||
            ( ( meta.getGid() == _userGid ) && group.canWrite() ) ||
            world.canWrite() ;
            boolean readAllowed =
            ( ( meta.getUid() == _userUid ) && user.canRead()  ) ||
            ( ( meta.getGid() == _userGid ) && group.canRead() ) ||
            world.canRead() ;

            if( _storageInfo.isCreatedOnly() || _overwrite || _truncate ||
            ( _isHsmRequest && ( _ioMode.indexOf( 'w' ) >= 0 ) ) ){
                //
                //
                if( _isHsmRequest && _storageInfo.isStored() ){
                    sendReply( "pnfsGetStorageInfoArrived", 1 ,
                    "HsmRequest : file already stored" ) ;
                    removeUs() ;
                    return ;
                }
                //
                // the file is an  pnfsEntry only.
                // 'read only would be nonsense'
                //
                if( _ioMode.indexOf( 'w' ) < 0 ){

                    sendReply( "pnfsGetStorageInfoArrived", 1 ,
                    "File doesn't exist (can't be readOnly)" ) ;
                    removeUs() ;
                    return ;
                }

                if( ( _isUrl || _authorizationRequired ) && ! writeAllowed || _readOnly){
                    sendReply( "pnfsGetStorageInfoArrived", 1 ,
                    "Permission denied" ) ;
                    removeUs() ;
                    return ;
                }
                // we need a write pool
                //
                _protocolInfo.setAllowWrite(true) ;
                if( _overwrite ){
                    _storageInfo.setKey("overwrite","true");
                    _cell.say("Overwriting requested");
                }

                if( _truncate && ! _isNew ) {
                    try {
                        if( _isUrl ) {
                            String path = _getStorageInfo.getPnfsPath() ;
                            _cell.say("truncating path " + path );
                            _pnfs.deletePnfsEntry( path );
                            _getStorageInfo =   _pnfs.createPnfsEntry( path , _userUid, _userGid, 0644 ) ;
                            _pnfsId = _getStorageInfo.getPnfsId() ;
                            _storageInfo = _getStorageInfo.getStorageInfo() ;
                        }else{
                            _pnfsId = new PnfsId( _truncFile );
                            _getStorageInfo = _pnfs.getStorageInfoByPnfsId( _pnfsId ) ;
                            _storageInfo = _getStorageInfo.getStorageInfo() ;
                        }

                    }catch(CacheException ce ) {
                        _cell.esay(ce);
                        sendReply( "pnfsGetStorageInfoArrived", 1,
                        "Failed to truncate file");
                        removeUs();
                        return;
                    }
                }


                if( !_isUrl && (_path != null) ) {
                    _storageInfo.setKey("path", _path);
                }

                if( _checksumString != null ){
                    _storageInfo.setKey("checksum",_checksumString);
                    _cell.say("Checksum from client "+_checksumString);
                    storeChecksumInPnfs( _pnfsId , _checksumString ) ;
                }


                // adjust accessLatency and retention policy if it' allowed and defined

                if( _isAccessLatencyOverwriteAllowed && _accessLatency != null ) {
                	try {
                		AccessLatency accessLatency = AccessLatency.getAccessLatency(_accessLatency);
                		_storageInfo.setAccessLatency(accessLatency);
                		_storageInfo.isSetAccessLatency(true);

                	}catch(IllegalArgumentException e) { /* bad AccessLatency ignored*/}
                }

                if( _isRetentionPolicyOverwriteAllowed && _retentionPolicy != null ) {
                	try {
                		RetentionPolicy retentionPolicy = RetentionPolicy.getRetentionPolicy(_retentionPolicy);
                		_storageInfo.setRetentionPolicy(retentionPolicy);
                		_storageInfo.isSetRetentionPolicy(true);

                	}catch(IllegalArgumentException e) { /* bad RetentionPolicy ignored*/}
                }


                //
                // try to get some space to store the file.
                //
                getPoolMessage = new PoolMgrSelectWritePoolMsg(_pnfsId,_storageInfo,_protocolInfo,0) ;
                getPoolMessage.setIoQueueName(_ioQueueName );
            }else{
                //
                // sorry, we don't allow write (not yet)
                // (except if 'overwrite' is given [FERMI ingest]
                //
                if( _ioMode.indexOf( 'w' ) > -1){

                    sendReply( "pnfsGetStorageInfoArrived", 1 ,
                    "File is readOnly" ) ;
                    removeUs() ;
                    return ;
                }
                //
                // we need to tell the mover as well.
                // The client may try to write without
                // specifying so.
                //
                _protocolInfo.setAllowWrite(false) ;

                if( ( _isUrl || _authorizationRequired ) && ! readAllowed ){
                    sendReply( "pnfsGetStorageInfoArrived", 2 ,
                    "Permission denied" ) ;
                    removeUs() ;
                    return ;
                }
                //
                // try to get some space to store the file.
                //
                getPoolMessage = new PoolMgrSelectReadPoolMsg(_pnfsId,_storageInfo,_protocolInfo,0) ;
                getPoolMessage.setIoQueueName(_ioQueueName );
            }

            if( _verbose )sendComment("opened");

            getPoolMessage.setId(_sessionId);
            synchronized( _messageLock ){
                try{
                    _cell.sendMessage(
                    new CellMessage(
                    new CellPath( _isHsmRequest ?
                    _hsmManager : _poolManagerName )  ,
                    getPoolMessage
                    )
                    ) ;
                }catch(Exception ie){
                    sendReply( "pnfsGetStorageInfoArrived" , 2 ,
                    ie.toString() ) ;
                    removeUs()  ;
                    return ;
                }
            }
            setStatus( "WaitingForGetPool" ) ;
            setTimer(_poolRetry) ;
            return ;

        }
        private void storeChecksumInPnfs( PnfsId pnfsId , String checksumString){
            try{
                PnfsFlagMessage flag =
                new PnfsFlagMessage(pnfsId,"c","put") ;
                flag.setReplyRequired(false) ;
                flag.setValue(checksumString);

                _cell.sendMessage(
                new CellMessage(
                new CellPath("PnfsManager") ,
                flag
                )
                );
            }catch(Exception eee ){
                _cell.esay("Failed to send crc to PnfsManager : "+eee ) ;
            }
        }

        public void
        poolMgrSelectPoolArrived( PoolMgrSelectPoolMsg reply ){

            setTimer(0L);
            _cell.say( "poolMgrGetPoolArrived : "+reply ) ;
            _cell.say(_pnfsId.toString()+" poolMgrSelectPoolArrived after "+
            (System.currentTimeMillis()-_started) );

            if( reply.getReturnCode() != 0 ){
                sendReply( "poolMgrGetPoolArrived" , reply )  ;
                removeUs() ;
                return ;
            }
            String pool = null ;
            if( ( pool = reply.getPoolName() ) == null ){
                sendReply( "poolMgrGetPoolArrived" , 33 , "No pools available" ) ;
                removeUs() ;
                return ;
            }
            _pool = pool ;
            PoolIoFileMessage poolMessage  = null ;

            if( reply instanceof PoolMgrSelectReadPoolMsg ){
                poolMessage =
                new PoolDeliverFileMessage(
                pool,
                _pnfsId ,
                _protocolInfo ,
                _storageInfo           ) ;
            }else if( reply instanceof PoolMgrSelectWritePoolMsg ){

                if( _doPreallocation )handleSpacePreallocation( pool , _storageInfo ) ;
                poolMessage =
                new PoolAcceptFileMessage(
                pool,
                _pnfsId ,
                _protocolInfo ,
                _storageInfo           ) ;
            }else{
                sendReply( "poolMgrGetPoolArrived" , 7 ,
                "Illegal Message arrived : "+reply.getClass().getName() ) ;
                removeUs()  ;
                return ;
            }

            poolMessage.setId( _sessionId ) ;

            // current request is a initiator for the pool request
            // we need this to trace back pool billing information
            poolMessage.setInitiator( _info.getTransaction() );
            if( _ioQueueName != null )poolMessage.setIoQueueName( _ioQueueName ) ;
            if( _ioQueueAllowOverwrite &&
                ( _ioHandlerQueue != null     ) &&
                ( _ioHandlerQueue.length() > 0 )    )poolMessage.setIoQueueName( _ioHandlerQueue ) ;


            synchronized( _messageLock ){
                if( _poolRequestDone ){
                    esay("Ignoring double message");
                    return ;
                }
                try{
                    CellPath toPool = null ;
                    if( _poolProxy == null ){
                        toPool = new CellPath(pool);
                    }else{
                        toPool = new CellPath(_poolProxy);
                        toPool.add(pool);
                    }
                    _cell.sendMessage(
                    new CellMessage( toPool  , poolMessage )
                    ) ;
                    _poolRequestDone = true ;
                }catch(Exception ie){
                    sendReply( "poolMgrGetPoolArrived" , 2 ,
                    ie.toString() ) ;
                    removeUs()  ;
                    return ;
                }
            }
            setStatus( "WaitingForOpenFile" ) ;

        }
        private void handleSpacePreallocation( String pool , StorageInfo storageInfo ){
            String tmp = storageInfo.getKey("alloc-size");
            long   allocSize = 0L ;
            if (tmp != null) try {
                allocSize = Long.parseLong(tmp);
            } catch (NumberFormatException ee) {
                /* bad alloc-size ignored*/
            }

            if( allocSize <= 0L ){
                _cell.say("Preallocating not defined" ) ;
                return ;
            }
            _cell.say("Preallocating : "+allocSize ) ;
            try{
                PoolReserveSpaceMessage space =
                new PoolReserveSpaceMessage( pool , allocSize ) ;

                CellMessage request = new CellMessage( new CellPath(pool) , space ) ;

                request = _cell.sendAndWait( request , 30000L ) ;
                if( ( request == null ) ||
                ! ( request.getMessageObject() instanceof PoolReserveSpaceMessage ) )

                    throw new
                    Exception("Zero or invalid reply from pool space reservation "+pool ) ;


                space = (PoolReserveSpaceMessage)request.getMessageObject() ;

                storageInfo.setKey( "use-preallocated-space" , ""+allocSize ) ;

                _cell.say("Pool "+pool+" now reserving "+space.getReservedSpace() ) ;
                try{
                    Thread.sleep(30000L);
                }catch(InterruptedException eeee){}
                _cell.say("Preallocation done");

            }catch(Exception ee ){
                _cell.say("Space preallocation canceled : "+ee ) ;
            }

        }
        public void
        poolIoFileArrived( PoolIoFileMessage reply ){

            _cell.say( "poolIoFileArrived : "+reply ) ;
            if( reply.getReturnCode() != 0 ){


                // bad entry in cacheInfo and pool Manager did not check it ( for performance reason )
                // try again
                if( reply.getReturnCode() == CacheException.FILE_NOT_IN_REPOSITORY ) {
                    _poolRequestDone = false;
                    this.storageInfoAvailable();
                    return;
                }


                sendReply( "poolIoFileArrived" , reply )  ;
                removeUs() ;
                return ;
            }
            //
            // nothing to do here ( we are still waiting for
            //   doorTransferFinished )
            //
            setStatus( "WaitingForDoorTransferOk" ) ;
        }

        public void poolPassiveIoFileMessage( PoolPassiveIoFileMessage reply) {

            InetSocketAddress poolSocketAddress = reply.socketAddress();

            StringBuffer sb = new StringBuffer() ;
            sb.append(_sessionId).append(" ").
            append(_commandId).append(" ").
            append(_vargs.getName()).
            append(" connect ").append(poolSocketAddress.getHostName() ).
            append(" ").append(poolSocketAddress.getPort() ).append(" ").
            append(diskCacheV111.util.Base64.byteArrayToBase64(reply.challange()) );
            ;
            println( sb.toString() ) ;
            setStatus( "WaitingForDoorTransferOk" ) ;

        }

        // FIXME: sleep with lock
        public synchronized void
        doorTransferArrived( DoorTransferFinishedMessage reply ){

            if( reply.getReturnCode() == 0 ){

                long filesize = reply.getStorageInfo().getFileSize() ;
                say( "doorTransferArrived : fs="+filesize+";strict="+_strictSize+";m="+_ioMode);
                if( _strictSize && ( filesize > 0L ) && ( _ioMode.indexOf("w") > -1 ) ){

                    for( int count = 0 ; count < 10 ; count++  ){
                        try{
                            long fs = _pnfs.getStorageInfoByPnfsId( _pnfsId).
                            getStorageInfo().
                            getFileSize() ;
                            say("doorTransferArrived : Size of "+_pnfsId+" : "+fs ) ;
                            if( fs > 0L )break ;
                        }catch(Exception ee ){
                            esay("Problem getting storage info (check) for "+_pnfsId+" : "+ee ) ;
                        }
                        try{
                            Thread.sleep(10000L);
                        }catch(InterruptedException ie ){
                            break ;
                        }
                    }
                }
                sendReply( "doorTransferArrived" , 0 , "" ) ;
            }else{
                sendReply( "doorTransferArrived" , reply ) ;
            }
            removeUs() ;
            setStatus( "<done>" ) ;
        }
        @Override
        public String toString(){ return "io ["+_pool+"] "+super.toString() ; }
    }

    ////////////////////////////////////////////////////////////////////
    //
    //      the basic opendir handler
    //
    protected  class OpenDirHandler  extends PnfsSessionHandler  {

        private DCapProtocolInfo _protocolInfo = null ;
        private String           _pool         = "dirLookupPool" ;
        private String []        _hosts        = null ;

        private OpenDirHandler( int sessionId , int commandId , VspArgs args )
        throws Exception {

            super( sessionId , commandId , args , true, true ) ;

            int   port    = Integer.parseInt( _vargs.argv(2) ) ;

            StringTokenizer st = new StringTokenizer( _vargs.argv(1) , "," ) ;
            _hosts    = new String[st.countTokens()]  ;
            for( int i = 0 ; i < _hosts.length ; i++ )_hosts[i] = st.nextToken() ;
            //
            //
            _protocolInfo = new DCapProtocolInfo( "DCap",3,0, _hosts , port  ) ;
            _protocolInfo.setSessionId( _sessionId ) ;
            String pool = args.getOpt("lookupPool");
            if( pool != null ) {
                _pool = pool;
            }

        }



        @Override
        public void fileMetaDataAvailable(){
            //
            // we are not called if the pnfs request failed.
            //

            String path = _getFileMetaData.getPnfsPath();


            if( ! _fileMetaData.isDirectory() ) {
                sendReply( "fileMetaDataAvailable" , 22 ,"\"" +path +"is not a directory\" ENOTDIR" ) ;
                removeUs()  ;
                return ;
            }

            PoolIoFileMessage poolIoFileMessage = new PoolIoFileMessage(_pool,_pnfsId, _protocolInfo);

			poolIoFileMessage.setId(_sessionId);
			if (_ioQueueName != null) {
				poolIoFileMessage.setIoQueueName(_ioQueueName);
			}
			if (_ioQueueAllowOverwrite && (_ioHandlerQueue != null)
					&& (_ioHandlerQueue.length() > 0)) {
				poolIoFileMessage.setIoQueueName(_ioHandlerQueue);
			}

			synchronized (_messageLock) {
				try {
					_cell.sendMessage(new CellMessage(new CellPath(_pool),
							poolIoFileMessage));
				} catch (Exception ie) {
					sendReply("poolMgrGetPoolArrived", 2, ie.toString());
					removeUs();
					return;
				}
			}



            return ;
        }

        public void
        poolIoFileArrived( PoolIoFileMessage reply ){

            _cell.say( "poolIoFileArrived : "+reply ) ;
            if( reply.getReturnCode() != 0 ){
                sendReply( "poolIoFileArrived" , reply )  ;
                removeUs() ;
                return ;
            }
            //
            // nothing to do here ( we are still waiting for
            //   doorTransferFinished )
            //
            setStatus( "WaitingForDoorTransferOk" ) ;
        }

        public synchronized void
        doorTransferArrived( DoorTransferFinishedMessage reply ){

            if( reply.getReturnCode() == 0 ){
                sendReply( "doorTransferArrived" , 0 , "" ) ;
            }else{
                sendReply( "doorTransferArrived" , reply ) ;
            }

            removeUs();
            setStatus( "<done>" ) ;
        }


        @Override
        public String toString() { return "od ["+_pool+"] "+super.toString() ; }
    }


    public void   getInfo( PrintWriter pw ){
        pw.println( " ----- DCapDoorInterpreterV3 ----------" ) ;
        pw.println( "      pid  = "+_pid ) ;
        pw.println( "      uid  = "+_uid ) ;
        pw.println( "(auth)uid  = "+_userUid ) ;
        pw.println( "(auth)gid  = "+_userGid ) ;
        pw.println( "     home  = "+(_userHome==null?"?":_userHome) ) ;
        pw.println( "  Version  = "+_majorVersion+"/"+_minorVersion ) ;
        pw.println( "  VLimits  = "+
        (_minClientVersion==null?"*":_minClientVersion.toString() ) +":" +
        (_maxClientVersion==null?"*":_maxClientVersion.toString() ) ) ;
        pw.println( "   Started = "+_startedTS ) ;
        pw.println( "   Last at = "+_lastCommandTS ) ;

        for( Map.Entry<Integer, SessionHandler> session: _sessions.entrySet() ){
            pw.println( session.getKey().toString()+ " -> "+session.getValue().toString() );
        }
    }
    public void   messageArrived( CellMessage msg ){
        Object         object  = msg.getMessageObject();
        SessionHandler handler = null ;
        Message        reply   = null ;

        if (object instanceof Message ){
            reply   = (Message)object ;
            synchronized( _messageLock ){
                handler = _sessions.get( Integer.valueOf((int)reply.getId())) ;
                if( handler == null ){
                    _cell.esay( "Unexpected message for session : "+
                    reply.getId() ) ;
                    return ;
                }
            }
        }else{
            _cell.say("Unexpected message class "+object.getClass());
            _cell.say("source = "+msg.getSourceAddress());
            return ;
        }
        if( reply instanceof DoorTransferFinishedMessage ){

            ((IoHandler)handler).doorTransferArrived( (DoorTransferFinishedMessage)reply )  ;

        }else if( reply instanceof PoolMgrGetPoolMsg ){

            ((IoHandler)handler).poolMgrSelectPoolArrived( (PoolMgrSelectPoolMsg)reply )  ;

        }else if( reply instanceof PnfsGetStorageInfoMessage ){

            ((PnfsSessionHandler)handler).pnfsGetStorageInfoArrived( (PnfsGetStorageInfoMessage)reply )  ;

        }else if( reply instanceof PnfsGetFileMetaDataMessage ){

            ((PnfsSessionHandler)handler).pnfsGetFileMetaDataArrived( (PnfsGetFileMetaDataMessage)reply )  ;

        }else if( reply instanceof PoolIoFileMessage ){

            ((IoHandler)handler).poolIoFileArrived( (PoolIoFileMessage)reply )  ;

        }else if( reply instanceof PoolPassiveIoFileMessage ){

            ((IoHandler)handler).poolPassiveIoFileMessage( (PoolPassiveIoFileMessage)reply )  ;

        } else {

            _cell.say("Unexpected message class "+object.getClass());
            _cell.say("source = "+msg.getSourceAddress());

        }
    }


}
