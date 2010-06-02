package diskCacheV111.doors;

import diskCacheV111.vehicles.*;
import diskCacheV111.util.*;


import dmg.cells.nucleus.*;
import dmg.util.*;
import diskCacheV111.services.acl.PermissionHandler;
import diskCacheV111.services.acl.DelegatingPermissionHandler;
import diskCacheV111.services.acl.GrantAllPermissionHandler;
import gplazma.authz.AuthorizationException;
import diskCacheV111.poolManager.RequestContainerV5;
import diskCacheV111.poolManager.PoolSelectionUnit.DirectionType;

import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentHashMap;
import java.io.*;
import java.net.*;

import diskCacheV111.util.AccessLatency;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.CheckStagePermission;
import diskCacheV111.util.FileMetaData;
import diskCacheV111.util.RetentionPolicy;
import org.dcache.auth.Subjects;
import org.dcache.auth.UnionLoginStrategy;
import org.dcache.auth.GplazmaLoginStrategy;
import org.dcache.auth.CachingLoginStrategy;
import org.dcache.auth.LoginStrategy;
import org.dcache.auth.LoginReply;
import org.dcache.auth.attributes.LoginAttribute;
import org.dcache.auth.attributes.ReadOnly;
import org.dcache.auth.UserNamePrincipal;
import org.dcache.auth.AuthzQueryHelper;

import diskCacheV111.util.PnfsHandler;
import java.security.Principal;
import org.dcache.acl.ACLException;
import org.dcache.acl.enums.AccessType;
import org.dcache.acl.enums.AccessMask;

import javax.security.auth.Subject;
import org.dcache.auth.FQANPrincipal;
import org.dcache.auth.Origin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.dcache.namespace.FileType;
import org.dcache.namespace.FileAttribute;
import org.dcache.vehicles.FileAttributes;
import org.dcache.vehicles.PnfsGetFileAttributes;
import org.globus.gsi.jaas.GlobusPrincipal;
import static org.dcache.namespace.FileAttribute.*;

public class DCapDoorInterpreterV3 implements KeepAliveListener,
        DcapProtocolInterpreter {

    public static final Logger _log =
        LoggerFactory.getLogger(DCapDoorInterpreterV3.class);

    /**
     * Ascii commands supported by this interpreter.
     */
    private enum DcapCommand {

        HELLO   ("hello"),
        BYEBYE  ("byebye"),
        OPEN    ("open"),
        CHECK   ("check"),
        CHGRP   ("chgrp"),
        CHOWN   ("chown"),
        CHMOD   ("chmod"),
        LSTAT   ("lstat"),
        MKDIR   ("mkdir"),
        OPENDIR ("opendir"),
        PING    ("ping"),
        RENAME  ("rename"),
        RMDIR   ("rmdir"),
        STAGE   ("stage"),
        STAT    ("stat"),
        STATUS  ("status"),
        UNLINK  ("unlink");

        private static final long serialVersionUID = 8393273905860276227L;

        private final String _value;
        private static final Map<String, DcapCommand> _commands =
                new HashMap<String, DcapCommand>();
        static {
            for (DcapCommand command : DcapCommand.values()) {
                _commands.put(command.getCommand(), command);
            }
        }

        DcapCommand(String value) {
            _value = value;
        }

        String getCommand() {
            return _value;
        }

        static DcapCommand get(String s) {
            DcapCommand command = _commands.get(s);
            if(command == null) {
                throw new IllegalArgumentException("Unsupported command: " + s);
            }
            return command;
        }
    }

    private final PrintWriter _out          ;
    private final CellEndpoint _cell        ;
    private final Args        _args         ;
    private String      _ourName     = "server" ;
    private final ConcurrentMap<Integer,SessionHandler> _sessions =
        new ConcurrentHashMap<Integer,SessionHandler>();
    private String  _poolManagerName = null ;
    private String  _pnfsManagerName = null ;


    private CellPath _poolMgrPath    = null ;

    /**
     * The client PID set through the hello command. Only used for
     * billing purposes.
     */
    private String _pid = "<unknown>";

    /**
     * The client UID set through the hello command. Used for setting
     * the owner of new name space entries. Never used for
     * authorization.
     */
    private int _uid = -1;

    /**
     * The client GID set through the hello command. Used for setting
     * the group of new name space entries. Never used for
     * authorization.
     */
    private int _gid = -1;

    private int     _majorVersion    = 0 ;
    private int     _minorVersion    = 0 ;
    private Date    _startedTS       = null ;
    private Date    _lastCommandTS   = null ;

    /**
     * If false, then authorization checks on read and write
     * operations are bypassed for non URL operations. If true, then
     * such operations are subject to authorization checks.
     */
    private boolean _authorizationRequired = false;

    /**
     * If true, then the Subject of the request must have a UID and
     * GID. If false, then a Subject without a UID and GID (i.e. a
     * Nobody) will be allowed to proceed, but only allowed to perform
     * operations authorized to world.
     */
    private boolean _authorizationStrong = false;

    protected final CellPath _billingCellPath = new CellPath("billing");
    private final InetAddress _clientAddress;

    /**
     * Subjected provided to us as a result of authentication in the
     * tunnel.
     */
    private final Subject _authenticatedSubject;

    private final LoginStrategy _loginStrategy;

    private final PermissionHandler _permissionHandler;

    /**
     * Tape Protection
     */
    private final String _stageConfigurationFilePath;
    private final CheckStagePermission _checkStagePermission;

    private boolean _strictSize            = false ;
    private String  _poolProxy        = null ;
    private Version _minClientVersion = null ;
    private Version _maxClientVersion = null ;
    private boolean _checkStrict      = true ;
    private long    _poolRetry        = 0L ;
    private String  _hsmManager       = null ;

    private boolean _truncateAllowed  = false ;
    private String  _ioQueueName      = null ;
    private boolean _ioQueueAllowOverwrite = false ;

    // flag defined in batch file to allow/disallow AccessLatency and RetentionPolicy re-definition

    private boolean _isAccessLatencyOverwriteAllowed = false;
    private boolean _isRetentionPolicyOverwriteAllowed = false;

    public DCapDoorInterpreterV3(CellEndpoint cell, PrintWriter pw, Subject subject)
        throws ACLException, IOException, AuthorizationException
    {
        _out  = pw ;
        _cell = cell ;
        _args = cell.getArgs();
        _authenticatedSubject = new Subject(true,
                                            subject.getPrincipals(),
                                            subject.getPublicCredentials(),
                                            subject.getPrivateCredentials());

        String auth = _args.getOpt("authorization") ;
        _authorizationStrong   = ( auth != null ) && auth.equals("strong") ;
        _authorizationRequired = ( auth != null ) &&
        ( auth.equals("strong") || auth.equals("required") ) ;

        if( _authorizationRequired )_log.debug("Authorization required");
        if( _authorizationStrong   )_log.debug("Authorization strong");

        _loginStrategy = createLoginStrategy();

        _pnfsManagerName = _args.getOpt("pnfsManager" ) ;
        _pnfsManagerName = _pnfsManagerName == null ?
        "PnfsManager" : _pnfsManagerName ;

        _poolManagerName = _args.getOpt("poolManager" ) ;
        _poolManagerName = _poolManagerName == null ?
        "PoolManager" : _poolManagerName ;


        _poolProxy       = _args.getOpt("poolProxy");

        if (_poolProxy != null) {
            _log.debug("Pool Proxy set to {}", _poolProxy);
        }

        // allow file truncating
        String truncate = _args.getOpt("truncate");
        _truncateAllowed = (truncate != null) && truncate.equals("true") ;

        _isAccessLatencyOverwriteAllowed = _args.getOpt("allow-access-policy-overwrite") != null ;
        if(_isAccessLatencyOverwriteAllowed) {
            _log.debug("Allowes to overwrite AccessLatency");
        }
        _isRetentionPolicyOverwriteAllowed = _args.getOpt("allow-retention-policy-overwrite") != null;
        if(_isRetentionPolicyOverwriteAllowed){
            _log.debug("Allowed to overwrite RetentionPolicy");
        }

        _poolMgrPath     = new CellPath( _poolManagerName ) ;

        _checkStrict     = ( _args.getOpt("check") != null ) &&
        ( _args.getOpt("check").equals("strict") ) ;

        _strictSize      = _args.getOpt("strict-size") != null ;

        _hsmManager      = _args.getOpt("hsm") ;

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
            } catch (NumberFormatException e) {
                _log.error("Problem in setting PoolRetry Value: {}", e);
            }
        _log.debug("PoolRetry timer set to {} seconds", _poolRetry/1000L);

        _ioQueueName = _args.getOpt("io-queue") ;
        _ioQueueName = ( _ioQueueName == null ) || ( _ioQueueName.length() == 0 ) ? null : _ioQueueName ;
        _log.debug("IoQueueName = {}",
                   (_ioQueueName==null) ? "<undefined>" : _ioQueueName);

        String tmp = _args.getOpt("io-queue-overwrite") ;
        _ioQueueAllowOverwrite = ( tmp != null ) && tmp.equals("allowed" ) ;
        _log.debug("IoQueueName : overwrite : {}",
                   _ioQueueAllowOverwrite ? "allowed" : "denied");

        String check = (String)_cell.getDomainContext().get("dCapDoor-check");
        if( check != null )_checkStrict = check.equals("strict") ;

        // TODO: This should be the source IP of the request, however
        // this information is currently unavailable in
        // DCapDoorInterpreterV3.
        _clientAddress = InetAddress.getLocalHost();

        String permissionHandlerClasses = _args.getOpt("permission-handler");
        if (permissionHandlerClasses == null ||
            permissionHandlerClasses.isEmpty()) {
            _permissionHandler = new GrantAllPermissionHandler();
        } else {
            _permissionHandler = new DelegatingPermissionHandler(_cell);
        }
        _log.debug("Permission Handler: {}", _permissionHandler);

        if (_args.getOpt("readOnly") != null)
            _log.debug("Door is configured as read-only");
        else
            _log.debug("Door is configured as read/write");

        _stageConfigurationFilePath = _args.getOpt("stageConfigurationFilePath");
        _checkStagePermission = new CheckStagePermission(_stageConfigurationFilePath);
        _log.debug("Check : {}", _checkStrict ? "Strict" : "Fuzzy");
        _log.debug("Constructor Done");
    }

    private LoginStrategy createLoginStrategy()
        throws AuthorizationException
    {
        LoginStrategy gplazma =
            new GplazmaLoginStrategy(new AuthzQueryHelper(_cell));
        UnionLoginStrategy union = new UnionLoginStrategy();
        union.setLoginStrategies(Collections.singletonList(gplazma));
        if (!_authorizationStrong) {
            union.setAnonymousAccess(UnionLoginStrategy.AccessLevel.FULL);
        }
        return new CachingLoginStrategy(union);
    }

    private LoginReply login(String user)
        throws CacheException
    {
        /* The client can specify a custom user name to be used.
         */
        Subject subject;
        if (user != null) {
            subject = new Subject();
            subject.getPublicCredentials().addAll(_authenticatedSubject.getPublicCredentials());
            subject.getPrivateCredentials().addAll(_authenticatedSubject.getPrivateCredentials());
            subject.getPrincipals().add(new UserNamePrincipal(user));

            for (Principal principal: _authenticatedSubject.getPrincipals()) {
                if (!(principal instanceof UserNamePrincipal)) {
                    subject.getPrincipals().add(principal);
                }
            }
        } else {
            subject = _authenticatedSubject;
        }

        LoginReply login = _loginStrategy.login(subject);

        Origin origin;
        if (Subjects.isNobody(login.getSubject())) {
            origin = new Origin(Origin.AuthType.ORIGIN_AUTHTYPE_WEAK,
                                _clientAddress);
        } else {
            origin = new Origin(Origin.AuthType.ORIGIN_AUTHTYPE_STRONG,
                                _clientAddress);
        }
        login.getSubject().getPrincipals().add(origin);

        _log.info("Login completed for {}", login);

        return login;
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
            _log.debug("Client Version not restricted");
            return ;
        }
        _log.debug("Client Version Restricted to : {}", versionString);
        try{
            StringTokenizer st = new StringTokenizer(versionString,":");
            _minClientVersion  = new Version( st.nextToken() ) ;
            _maxClientVersion  = st.countTokens() > 0 ? new Version(st.nextToken()) : null ;
        } catch (Exception e) {
            _log.error("Client Version : syntax error (limits ignored) : {} : {}", versionString, e.toString());
            _minClientVersion = _maxClientVersion = null ;
        }
    }
    public synchronized void println( String str ){
        _log.debug("(DCapDoorInterpreterV3) toclient(println) : {}", str);
        _out.println( str );
        _out.flush();
    }

    public synchronized void print( String str ){
        _log.debug("(DCapDoorInterpreterV3) toclient(print) : {}", str);
        _out.print( str );
        _out.flush();
    }

    public void keepAlive(){
        for (SessionHandler sh: _sessions.values()){
            try{
                sh.keepAlive() ;
            } catch (Throwable t) {
                _log.error("Keep Alive problem in {}: {}", sh, t);
            }
        }
    }

    /**
     * Convenience method to start a SessionHandler.
     */
    private void start(SessionHandler session)
        throws CommandException
    {
        session.start();
    }

    //////////////////////////////////////////////////////////////////
    //
    //   the command functions  String com_<commandName>(int,int,Args)
    //
    public String com_hello( int sessionId , int commandId , VspArgs args )
        throws CommandException
    {
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
            _log.error("Syntax error in client version number : {}",
                       e.toString());
        }
        version = new Version( _majorVersion , _minorVersion ) ;
        _log.debug("Client Version : {}", version);
        if( ( ( _minClientVersion != null ) && ( version.compareTo( _minClientVersion ) < 0 ) ) ||
        ( ( _maxClientVersion != null ) && ( version.compareTo( _maxClientVersion ) > 0 ) )  ){

            String error = "Client version rejected : "+version ;
            _log.error(error.toString());
            throw new
            CommandExitException(error , 1 );
        }
        String yourName = args.getName() ;
        if( yourName.equals("server") )_ourName = "client" ;
        String pid = args.getOpt("pid");
        if (pid != null) {
            _pid = pid;
        }
        String uid = args.getOpt("uid") ;
        if (uid != null) {
            try {
                _uid = Integer.parseInt(uid);
            } catch (NumberFormatException e) {
                _log.warn("Client specified invalid UID: {}", uid);
            }
        }
        String gid = args.getOpt("gid") ;
        if (gid != null) {
            try {
                _gid = Integer.parseInt(gid);
            } catch (NumberFormatException e) {
                _log.warn("Client specified invalid GID: {}", gid);
            }
        }
        return "0 0 "+_ourName+" welcome "+_majorVersion+" "+_minorVersion ;
    }
    public String com_byebye( int sessionId , int commandId , VspArgs args )
        throws CommandException
    {
        _lastCommandTS = new Date() ;
        throw new CommandExitException("byeBye",commandId)  ;
    }
    public synchronized String com_open( int sessionId , int commandId , VspArgs args )
        throws CacheException, CommandException
    {
        _lastCommandTS = new Date() ;
        if( args.argc() < 4 )
            throw new
            CommandException( 3  , "Not enough arguments for put" ) ;

        start(new IoHandler(sessionId,commandId,args));
        return null ;
    }

    public synchronized String com_stage( int sessionId , int commandId , VspArgs args )
        throws CacheException, CommandException
    {
        _lastCommandTS = new Date() ;
        if( args.argc() < 1 )
            throw new
            CommandException( 3  , "Not enough arguments for stage" ) ;

        start(new PrestageHandler(sessionId, commandId, args));
        return null ;
    }
    public synchronized String com_lstat( int sessionId , int commandId , VspArgs args )
        throws CacheException, CommandException
    {
        return get_stat( sessionId , commandId , args , false ) ;

    }
    public synchronized String com_stat( int sessionId , int commandId , VspArgs args )
        throws CacheException, CommandException
    {
        return get_stat( sessionId , commandId , args , true ) ;

    }

    public synchronized String com_unlink( int sessionId , int commandId , VspArgs args )
        throws CacheException, CommandException
    {
        return do_unlink( sessionId , commandId , args , true ) ;
    }

    public synchronized String com_rename( int sessionId , int commandId , VspArgs args )
        throws CacheException, CommandException
    {
        return do_rename( sessionId , commandId , args ) ;
    }

    public synchronized String com_rmdir( int sessionId , int commandId , VspArgs args )
        throws CacheException, CommandException
    {
        return do_rmdir( sessionId , commandId , args , true ) ;
    }

    public synchronized String com_mkdir( int sessionId , int commandId , VspArgs args )
        throws CacheException, CommandException
    {
        return do_mkdir( sessionId , commandId , args , true ) ;
    }

    public synchronized String com_chmod( int sessionId , int commandId , VspArgs args )
        throws CacheException, CommandException
    {
        return do_chmod( sessionId , commandId , args , true ) ;
    }

    public synchronized String com_chown( int sessionId , int commandId , VspArgs args )
        throws CacheException, CommandException
    {
        return do_chown( sessionId , commandId , args , true ) ;
    }


    public synchronized String com_chgrp( int sessionId , int commandId , VspArgs args )
        throws CacheException, CommandException
    {
        return do_chgrp( sessionId , commandId , args , true ) ;
    }

    public synchronized String com_opendir( int sessionId , int commandId , VspArgs args )
        throws CacheException, CommandException
    {
        return do_opendir( sessionId , commandId , args ) ;

    }

    private synchronized String do_unlink(
    int sessionId , int commandId , VspArgs args , boolean resolvePath )
        throws CommandException, CacheException
    {
        _lastCommandTS = new Date() ;
        if( args.argc() < 1 )
            throw new
            CommandException( 3  , "Not enough arguments for unlink" ) ;

        start(new UnlinkHandler(sessionId, commandId, args, resolvePath));
        return null ;
    }



    private synchronized String do_rename(int sessionId, int commandId, VspArgs args)
        throws CommandException, CacheException
    {
        _lastCommandTS = new Date() ;
        if( args.argc() < 1 )
            throw new
            CommandException( 3  , "Not enough arguments for unlink" ) ;

        start(new RenameHandler(sessionId, commandId, args));
        return null ;
    }


    private synchronized String do_rmdir(
    int sessionId , int commandId , VspArgs args , boolean resolvePath )
        throws CommandException, CacheException
    {
        _lastCommandTS = new Date() ;
        if( args.argc() < 1 )
            throw new
            CommandException( 3  , "Not enough arguments for rmdir" ) ;

        start(new RmDirHandler(sessionId, commandId, args, resolvePath));
        return null ;
    }

    private synchronized String do_mkdir(
    int sessionId , int commandId , VspArgs args , boolean resolvePath )
        throws CommandException, CacheException
    {
        _lastCommandTS = new Date() ;
        if( args.argc() < 1 )
            throw new
            CommandException( 3  , "Not enough arguments for unlink" ) ;

        start(new MkDirHandler(sessionId, commandId, args, resolvePath));
        return null ;
    }

    private synchronized String do_chown(
    int sessionId , int commandId , VspArgs args , boolean resolvePath )
        throws CommandException, CacheException
    {
        _lastCommandTS = new Date() ;
        if( args.argc() < 1 )
            throw new
            CommandException( 3  , "Not enough arguments for chown" ) ;

        start(new ChownHandler(sessionId, commandId, args, resolvePath));
        return null ;
    }


    private synchronized String do_chgrp(
            int sessionId , int commandId , VspArgs args , boolean resolvePath )
        throws CommandException, CacheException
    {
                _lastCommandTS = new Date() ;
                if( args.argc() < 1 )
                    throw new
                    CommandException( 3  , "Not enough arguments for chgrp" ) ;

                start(new ChgrpHandler(sessionId, commandId, args, resolvePath));
                return null ;
     }

    private synchronized String do_chmod(
            int sessionId , int commandId , VspArgs args , boolean resolvePath )
        throws CommandException, CacheException
    {
                _lastCommandTS = new Date() ;
                if( args.argc() < 1 )
                    throw new
                    CommandException( 3  , "Not enough arguments for chmod" ) ;

                start(new ChmodHandler(sessionId, commandId, args, resolvePath));
                return null ;
            }

    private synchronized String do_opendir(  int sessionId , int commandId , VspArgs args )
        throws CommandException, CacheException
    {
        _lastCommandTS = new Date() ;
        if( args.argc() < 1 )
            throw new
            CommandException( 3  , "Not enough arguments for opendir" ) ;

        start(new OpenDirHandler(sessionId,commandId,args));
        return null ;
    }

    private synchronized String get_stat(
    int sessionId , int commandId , VspArgs args , boolean resolvePath )
        throws CommandException, CacheException
    {
        _lastCommandTS = new Date() ;
        if( args.argc() < 1 )
            throw new
            CommandException( 3  , "Not enough arguments for stat" ) ;

        start(new StatHandler(sessionId, commandId, args, resolvePath));
        return null ;
    }

    public synchronized String com_ping( int sessionId , int commandId , VspArgs args )
    {
        println(String.valueOf(sessionId)+" "+commandId+" server pong");
        return null ;
    }

    public synchronized String com_check( int sessionId , int commandId , VspArgs args )
        throws CommandException, CacheException
    {
        _lastCommandTS = new Date() ;
        if( args.argc() < 1 )
            throw new
            CommandException( 3  , "Not enough arguments for check" ) ;

        start(new CheckFileHandler(sessionId, commandId, args));
        return null ;
    }

    public String com_status(int sessionId, int commandId, VspArgs args)
        throws CommandException
    {
        _lastCommandTS = new Date() ;
        SessionHandler handler = (SessionHandler) _sessions.get(sessionId);
        if (handler == null) {
            throw new
                CommandException(5, "Session ID " + sessionId + " not found.");
        }

        return ""+sessionId+" "+commandId+" "+args.getName()+
            " ok "+" 0 " + "\""+handler+ "\"" ;
    }

    public String hh_get_door_info = "[-binary]" ;
    public Object ac_get_door_info( Args args ){
        IoDoorInfo info = new IoDoorInfo( _cell.getCellInfo().getCellName() ,
        _cell.getCellInfo().getDomainName() ) ;
        info.setProtocol("dcap","3");
        info.setOwner(String.valueOf(_uid));
        info.setProcess(_pid);
        List<IoDoorEntry> list = new ArrayList<IoDoorEntry>(_sessions.size());
        for (SessionHandler session: _sessions.values()) {
            if (session instanceof IoHandler) {
                list.add(((IoHandler) session).getIoDoorEntry());
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
        _log.debug("toclient (commander) : {}", str);
        println(str);
        return "" ;
    }
    public String hh_retry = "<sessionId> [-weak]" ;
    public String ac_retry_$_1( Args args ) throws Exception {
        int sessionId = Integer.parseInt(args.argv(0));
        SessionHandler session;
        if ((session = _sessions.get(sessionId)) == null) {
            throw new CommandException(5, "No such session ID " + sessionId);
        }

        if (!(session instanceof PnfsSessionHandler)) {
            throw new
                CommandException(6, "Not a PnfsSessionHandler "+sessionId+
                                 " but "+session.getClass().getName());
        }

        ((PnfsSessionHandler) session).again( args.getOpt("weak") == null ) ;

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

        private final long      _timestamp       = System.currentTimeMillis() ;

        protected Subject _subject;
        protected Origin _origin;
        protected boolean _readOnly;

        protected SessionHandler(int sessionId, int commandId, VspArgs args)
            throws CommandException
        {
            _sessionId = sessionId ;
            _commandId = commandId ;
            _vargs     = args ;

            _info      = new DoorRequestInfoMessage(
            _cell.getCellInfo().getCellName()+"@"+
            _cell.getCellInfo().getDomainName() ) ;

            _ioHandlerQueue = args.getOpt("io-queue") ;
            _ioHandlerQueue = (_ioHandlerQueue == null ) || ( _ioHandlerQueue.length() == 0 ) ?
                               null : _ioHandlerQueue ;
        }

        protected void doLogin()
            throws CacheException
        {
            LoginReply login = login(_vargs.getOpt("role"));
            _subject = login.getSubject();
            _origin = Subjects.getOrigin(_subject);

            _readOnly =
                (DCapDoorInterpreterV3.this._args.getOpt("readOnly") != null);
            for (LoginAttribute attribute: login.getLoginAttributes()) {
                if (attribute instanceof ReadOnly) {
                    _readOnly |= ((ReadOnly) attribute).isReadOnly();
                }
            }

            if (!Subjects.isNobody(_subject)) {
                _info.setUid((int) Subjects.getUid(_subject));
                _info.setGid((int) Subjects.getPrimaryGid(_subject));
                _info.setOwner(Subjects.getUserName(_subject));
            }
        }

        /**
         * Called from the start method. Must perform whatever action
         * is needed to kick of processing the request.
         */
        protected void doStart()
            throws CacheException, CommandException
        {
        }

        /**
         * Starts the SessionHandler. This will cause the session
         * handler to be added the session map and initiate any action
         * required to process the request.
         */
        public final void start()
            throws CommandException
        {
            boolean isStarted = false;
            addUs();
            try {
                doLogin();
                doStart();
                isStarted = true;
            } catch (CacheException e) {
                throw new CommandException(e.getRc(), e.getMessage());
            } finally {
                if (!isStarted) {
                    removeUs();
                }
            }
        }

        protected void sendComment( String comment ){
            String reply = ""+_sessionId+" 1 "+
            _vargs.getName()+" "+comment ;
            println( reply ) ;
            _log.debug(reply.toString()) ;
        }
        public void keepAlive(){
            _log.debug("Keep alived called for : {}", this);
        }

        protected void sendReply( String tag , int rc , String msg){
            sendReply(tag, rc, msg, "");
        }

        protected void sendReply( String tag , int rc , String msg , String posixErr){

            String problem;
            _info.setTransactionTime( System.currentTimeMillis()-_timestamp);
            if( rc == 0 ){
                problem = String.format("%d %d %s ok", _sessionId, _commandId, _vargs.getName());
                _log.debug("{}: {}", tag, problem);
            }else{
                problem = String.format("%d %d %s failed %d \"%s\" %s",
                        _sessionId, _commandId, _vargs.getName(),
                        rc, msg, posixErr);
                _log.error("{}: {}", tag, problem);
                _info.setResult( rc , msg ) ;
            }
            println( problem ) ;
            postToBilling(_info);
        }

        protected void sendReply( String tag , Message msg ){

            sendReply( tag ,
            msg.getReturnCode() ,
            msg.getErrorObject().toString() ) ;

        }

        protected void addUs()
            throws CommandException
        {
            if (_sessions.putIfAbsent(_sessionId, this) != null) {
                throw new CommandException(5, "Duplicated session id");
            }
        }

        protected void removeUs()
        {
            _sessions.remove(_sessionId);
        }

        protected void setStatus( String status ){
            _status = status ;
            _statusSince = System.currentTimeMillis() ;
        }
        @Override
        public String toString(){
            return "["+_sessionId+"]["+getUid()+"]["+_pid+"] "+
            _status+"("+
            ( (System.currentTimeMillis()-_statusSince)/1000 )+")" ;
        }

        /**
         * Returns the UID that is used as an owner for new name space
         * entries.
         */
        protected int getUid()
        {
            if (!Subjects.isNobody(_subject)) {
                return (int) Subjects.getUid(_subject);
            }

            String s = _vargs.getOpt("uid");
            if (s != null) {
                try {
                    return Integer.parseInt(s);
                } catch (NumberFormatException e) {
                    _log.warn("Failed to parse UID: {}", s);
                }
            }

            return _uid;
        }

        /**
         * Returns the GID that is used as an owner for new name space
         * entries.
         */
        protected int getGid()
        {
            if (!Subjects.isNobody(_subject)) {
                return (int) Subjects.getPrimaryGid(_subject);
            }

            String s = _vargs.getOpt("gid");
            if (s != null) {
                try {
                    return Integer.parseInt(s);
                } catch (NumberFormatException e) {
                    _log.warn("Failed to parse GID: {}", s);
                }
            }

            return _gid;
        }

        /**
         * Returns the value that is used as mode for new name space
         * entries.
         *
         * @param mode Default value if mode was not specified
         */
        protected int getMode(int mode)
        {
            String s = _vargs.getOpt("mode");
            if (s != null) {
                try {
                    mode = Integer.decode(s);
                } catch (NumberFormatException e) {
                    _log.warn("Failed to parse mode: {}", s);
                }
            }
            return mode;
        }
    }
    abstract protected  class PnfsSessionHandler  extends SessionHandler  {

        protected PnfsId       _pnfsId       = null ;
        protected String       _path         = null;
        protected StorageInfo  _storageInfo  = null ;
        protected FileMetaData _fileMetaData = null ;
        private   long         _timer        = 0L ;
        private   final Object       _timerLock    = new Object() ;
        private long           _timeout      = 0L ;

        protected Set<FileAttribute> _attributes;
        protected PnfsGetFileAttributes _message;
        protected PnfsHandler _pnfs;

        protected PnfsSessionHandler(int sessionId, int commandId, VspArgs args,
                                     boolean metaDataOnly, boolean resolvePath)
            throws CacheException, CommandException
        {
            super(sessionId, commandId, args);

            _attributes = FileMetaData.getKnownFileAttributes();
            if (!metaDataOnly) {
                _attributes.add(STORAGEINFO);
            }

            String tmp = args.getOpt("timeout");
            if (tmp != null) {
                try {
                    long timeout = Long.parseLong(tmp);
                    if (timeout > 0L) {
                        _log.info("PnfsSessionHandler: user timeout set to {}", timeout);
                        _timeout = System.currentTimeMillis() + (timeout * 1000L);
                    }
                } catch (NumberFormatException e) {
                    /* 'bad' strings silently ignored */
                }
            }
        }

        @Override
        protected void doLogin()
            throws CacheException
        {
            super.doLogin();
            _pnfs = new PnfsHandler(_cell, new CellPath(_pnfsManagerName));
            if (_permissionHandler instanceof GrantAllPermissionHandler) {
                _pnfs.setSubject(_subject);
            }
        }

        @Override
        protected void doStart()
            throws CacheException
        {
            try {
                askForFileAttributes();
            } catch (NoRouteToCellException e) {
                throw new CacheException(e.getMessage());
            }
        }

        @Override
        public void keepAlive(){
            synchronized( _timerLock ){
                if( ( _timeout > 0L ) && ( _timeout < System.currentTimeMillis() ) ){
                    _log.warn("User timeout triggered") ;
                    sendReply("keepAlive" , 112 , "User timeout canceled session" ) ;
                    removeUs();
                    return ;
                }
                if( ( _timer > 0L ) && ( _timer < System.currentTimeMillis() ) ){
                    _timer = 0L ;
                    _log.warn("Restarting session {}", _sessionId);
                    try{
                        askForFileAttributes();
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
        public void again( boolean strong ) throws Exception {
            askForFileAttributes() ;
        }

        protected void askForFileAttributes()
            throws IllegalArgumentException, NoRouteToCellException
        {
            setTimer(60 * 1000);

            try {
                _pnfsId = new PnfsId(_vargs.argv(0));
                _message = new PnfsGetFileAttributes(_pnfsId, _attributes);
            } catch (IllegalArgumentException e) {
                /* Seems not to be a pnfsId, might be a url.
                 */
                DCapUrl url = new DCapUrl(_vargs.argv(0));
                String fileName = url.getFilePart();
                _message = new PnfsGetFileAttributes(fileName, _attributes);
                _path = fileName;
            }

            _log.debug("Requesting file attributes for {}", _message);

            _message.setId(_sessionId) ;
            _message.setReplyRequired(true);

            /* if is url, _path is already pointing to to correct path */
            if (_path == null) {
                _path = _vargs.getOpt("path");
            }
            if (_path != null) {
                _info.setPath(_path);
            }

            _pnfs.send(_message);
            setStatus("WaitingForPnfs");
        }

        public void pnfsGetFileAttributesArrived(PnfsGetFileAttributes reply)
        {
            setTimer(0);

            _message = reply;

            _log.debug("pnfsGetFileAttributesArrived: {}", _message);

            if (_message.getReturnCode() != 0) {
                try {
                    if (!fileAttributesNotAvailable()){
                        removeUs();
                        return;
                    }
                } catch (CacheException e) {
                    sendReply("pnfsGetFileAttributesArrived", e.getRc(), e.getMessage());
                    removeUs();
                    return;
                }
            }

            FileAttributes fileAttributes = _message.getFileAttributes();

            _pnfsId = _message.getPnfsId();
            _info.setPnfsId(_pnfsId);

            if (fileAttributes.isDefined(STORAGEINFO)) {
                _storageInfo = fileAttributes.getStorageInfo();
                for (int i = 0; i < _vargs.optc(); i++) {
                    String key = _vargs.optv(i);
                    String value = _vargs.getOpt(key);
                    _storageInfo.setKey(key, value == null ? "" : value);
                }
            }

            _fileMetaData = new FileMetaData(fileAttributes);

            fileAttributesAvailable();
        }

        abstract protected void fileAttributesAvailable();

        protected boolean fileAttributesNotAvailable() throws CacheException
        {
            sendReply("fileAttributesNotAvailable", _message);
            return false;
        }

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
    private final CellPath _prestagerPath = new CellPath( "Prestager" ) ;
    protected  class PrestageHandler  extends PnfsSessionHandler  {
        private long   _time = 0L ;
        private String _destination = null ;
        private PrestageHandler( int sessionId , int commandId , VspArgs args )
            throws CacheException, CommandException
        {
            super( sessionId , commandId , args, false, true ) ;

            try{ _time = Long.parseLong( args.getOpt("stagetime" ) ) ; }catch(NumberFormatException e){/* 'bad' strings silently ignored */}
            _destination = args.getOpt( "location" ) ;
        }

        @Override
        public void fileAttributesAvailable(){
            try {
                if( _message.getFileAttributes().getFileType() != FileType.REGULAR ) {
                    sendReply( "storageInfoAvailable" , CacheException.NOT_FILE,
                               "path is not a regular file", "EINVAL" ) ;
                    return;
                }
                try {
                    if( !_checkStagePermission.canPerformStaging(_subject, _storageInfo) ) {
                        sendReply( "storageInfoAvailable" , CacheException.PERMISSION_DENIED,
                                   "Staging file not allowed", "EACCES" ) ;
                        return;
                    }
                } catch (IOException e) {
                    _log.error("Error while reading data from StageConfiguration.conf file : {}", e.getMessage());
                }
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
                    return ;
                }
                if( msg.getReturnCode() != 0 ){
                    sendReply( "storageInfoAvailable" ,
                               msg.getReturnCode() ,
                               (String)msg.getErrorObject()  ) ;
                    return ;
                }
                sendReply( "storageInfoAvailable" , 0 , "" ) ;
            } finally {
                removeUs() ;
            }
        }
        @Override
        public String toString(){ return "st "+super.toString() ; }
    }
    ////////////////////////////////////////////////////////////////////
    //
    //      the file stat handler
    //
    protected  class StatHandler  extends PnfsSessionHandler  {

        private StatHandler(int sessionId,
                            int commandId,
                            VspArgs args,
                            boolean followLinks)
            throws CacheException, CommandException
        {
            super( sessionId , commandId , args , true , followLinks ) ;
        }

        @Override
        public void fileAttributesAvailable()
        {
            try {
                FileMetaData meta = _fileMetaData;
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

                sb.append("-st_ino=").append(_pnfsId.toString().hashCode()&0xfffffff) ;

                println( sb.toString() ) ;
            } finally {
                removeUs() ;
            }
        }
        @Override
        public String toString(){ return "st "+super.toString() ; }
    }

    ////////////////////////////////////////////////////////////////////
    //
    //      the file unlink handler
    //
    protected  class UnlinkHandler  extends PnfsSessionHandler  {
        private UnlinkHandler(int sessionId,
                              int commandId,
                              VspArgs args,
                              boolean followLinks)
            throws CacheException, CommandException
        {
            super( sessionId , commandId , args , true , followLinks ) ;
        }

        @Override
        protected void doLogin()
            throws CacheException
        {
            super.doLogin();
            if (_readOnly) {
                throw new CacheException(2, "Cannot execute 'unlink': Permission denied") ;
            }
        }

        @Override
        public void fileAttributesAvailable()
        {
            try {
                //
                // we are not called if the pnfs request failed.
                //
                String path = _message.getPnfsPath();
                FileMetaData meta = _fileMetaData;
                if (!meta.isDirectory()) {
                    try {
                        if (_permissionHandler.canDeleteFile(_pnfsId, _subject, _origin) != AccessType.ACCESS_ALLOWED) {
                            sendReply("fileAttributesAvailable", 19, "Permission denied to remove file", "EACCES");
                            return;
                        }
                        _pnfs.deletePnfsEntry(path);
                        sendReply("fileAttributesAvailable", 0, "");
                        sendRemoveInfoToBilling(path);
                    } catch (ACLException e) {
                        sendReply("fileAttributesAvailable", 19, e.getMessage(), "EACCES");
                    } catch (CacheException e) {
                        sendReply("fileAttributesAvailable", 19, e.getMessage(), "EACCES");
                    }
                } else {
                    sendReply("fileAttributesAvailable", 17, "Path is a Directory", "EISDIR");
                }
            } finally {
                removeUs();
            }
        }

        private void sendRemoveInfoToBilling(String path)
        {
            CellInfo cellInfo = _cell.getCellInfo();
            DoorRequestInfoMessage infoRemove =
                new DoorRequestInfoMessage(cellInfo.getCellName() + "@"
                                           + cellInfo.getDomainName(), "remove");
            if (!Subjects.isNobody(_subject)) {
                infoRemove.setOwner(Subjects.getUserName(_subject));
                infoRemove.setUid((int) Subjects.getUid(_subject));
                infoRemove.setGid((int) Subjects.getPrimaryGid(_subject));
            }
            infoRemove.setPath(path);

            postToBilling(infoRemove);
        }

        @Override
        public String toString(){ return "uk "+super.toString() ; }
    }

    protected  class ChmodHandler  extends PnfsSessionHandler  {

        private int _permission = 0;

        private ChmodHandler(int sessionId,
                             int commandId,
                             VspArgs args,
                             boolean followLinks)
            throws CacheException, CommandException
        {
            super( sessionId , commandId , args , true , followLinks ) ;

            _permission = Integer.decode(args.getOpt("mode"));
        }

        @Override
        protected void doLogin()
            throws CacheException
        {
            super.doLogin();
            if (_readOnly) {
                throw new CacheException(2, "Cannot execute 'chmod': Permission denied");
            }
        }

        @Override
        public void fileAttributesAvailable()
        {
            try {
                FileMetaData meta = _fileMetaData;
                if (_permissionHandler.canSetAttributes(_path, _subject, _origin, org.dcache.acl.enums.FileAttribute.FATTR4_MODE) == AccessType.ACCESS_ALLOWED) {
                    meta.setUserPermissions(new FileMetaData.Permissions((_permission >> 6) & 0x7));
                    meta.setGroupPermissions(new FileMetaData.Permissions((_permission >> 3) & 0x7));
                    meta.setWorldPermissions(new FileMetaData.Permissions(_permission & 0x7));

                    _pnfs.pnfsSetFileMetaData(_pnfsId, meta);
                    sendReply("fileAttributesAvailable", 0, "");
                } else {
                    sendReply("fileAttributesAvailable", 23, "Permission denied", "EACCES");
                }
            } catch (ACLException e) {
                sendReply("fileAttributesAvailable", 23, e.getMessage(), "EACCES");
            } catch (CacheException e) {
                sendReply("fileAttributesAvailable", 23, e.getMessage(), "EACCES");
            } finally {
                removeUs();
            }
        }
        @Override
        public String toString(){ return "uk "+super.toString() ; }
    }


    protected  class ChownHandler  extends PnfsSessionHandler  {

        private int _owner = -1;
        private int _group = -1;

        private ChownHandler(int sessionId, int commandId,
                             VspArgs args, boolean followLinks)
            throws CacheException, CommandException
        {
            super( sessionId , commandId , args , true , followLinks ) ;

            String[] owner_group = args.getOpt("owner").split("[:]");
            _owner  = Integer.parseInt(owner_group[0]);
            if( owner_group.length == 2 ) {
                _group = Integer.parseInt(owner_group[1]);
            }
        }

        @Override
        protected void doLogin()
            throws CacheException
        {
            super.doLogin();
            if (_readOnly) {
                throw new CacheException(2, "Cannot execute 'chown': Permission denied");
            }
        }

        @Override
        public void fileAttributesAvailable(){

            FileMetaData meta = _fileMetaData;

            try {
                if (_permissionHandler.canSetAttributes(_path, _subject, _origin, org.dcache.acl.enums.FileAttribute.FATTR4_OWNER) == AccessType.ACCESS_ALLOWED) {
                    if (_owner >= 0) {
                        meta.setUid(_owner);
                    }

                    if (_group >= 0) {
                        meta.setGid(_group);
                    }

                    _pnfs.pnfsSetFileMetaData(_pnfsId, meta);

                    sendReply("fileAttributesAvailable", 0, "");
                } else {
                    sendReply("fileAttributesAvailable", 23, "Permission denied", "EACCES");
                }
            } catch (ACLException e) {
                sendReply("fileAttributesAvailable", 23, e.getMessage(), "EACCES");
            } catch (CacheException e) {
                sendReply("fileAttributesAvailable", 23, e.getMessage(), "EACCES");
            } finally {
                removeUs();
            }

        }
        @Override
        public String toString(){ return "uk "+super.toString() ; }
    }

    protected  class ChgrpHandler  extends PnfsSessionHandler  {

        private int _group = -1;

        private ChgrpHandler(int sessionId,  int commandId,
                             VspArgs args,  boolean followLinks)
            throws CacheException, CommandException
        {
            super( sessionId , commandId , args , true , followLinks ) ;

            _group = Integer.parseInt(args.getOpt("group"));
        }

        @Override
        protected void doLogin()
            throws CacheException
        {
            super.doLogin();
            if (_readOnly) {
                throw new CacheException(2, "Cannot execute 'chgrp': Permission denied");
            }
        }

        @Override
        public void fileAttributesAvailable(){

            FileMetaData meta = _fileMetaData;

            try {
                if (_permissionHandler.canSetAttributes(_path, _subject, _origin, org.dcache.acl.enums.FileAttribute.FATTR4_OWNER_GROUP) == AccessType.ACCESS_ALLOWED) {
                    if (_group >= 0) {
                        meta.setGid(_group);
                    }

                    _pnfs.pnfsSetFileMetaData(_pnfsId, meta);

                    sendReply("fileAttributesAvailable", 0, "");
                } else {
                    sendReply("fileAttributesAvailable", 23, "Permission denied", "EACCES");
                }
            } catch (ACLException e) {
                sendReply("fileAttributesAvailable", 23, e.getMessage(), "EACCES");
            } catch (CacheException e) {
                sendReply("fileAttributesAvailable", 23, e.getMessage(), "EACCES");
            } finally {
                removeUs();
            }
        }
        @Override
        public String toString(){ return "uk "+super.toString() ; }
    }


    protected  class RenameHandler  extends PnfsSessionHandler  {


        private String _newName = null;

        private RenameHandler(int sessionId, int commandId, VspArgs args)
            throws CacheException, CommandException
        {
            super( sessionId , commandId , args , true , false ) ;
            _newName = args.argv(1);
        }

        @Override
        protected void doLogin()
            throws CacheException
        {
            super.doLogin();
            if (_readOnly) {
                throw new CacheException(2, "Cannot execute 'rename': Permission denied");
            }
        }

        @Override
        public void fileAttributesAvailable()
        {
            try {
                FileMetaData meta = _fileMetaData;

                boolean fileWriteAllowed = false;

                FileMetaData.Permissions user  = meta.getUserPermissions() ;
                FileMetaData.Permissions group = meta.getGroupPermissions();
                FileMetaData.Permissions world = meta.getWorldPermissions() ;


                fileWriteAllowed =
                    (Subjects.hasUid(_subject, meta.getUid()) && user.canWrite()) ||
                    (Subjects.hasGid(_subject, meta.getGid()) && group.canWrite()) ||
                    world.canWrite();

                if (!fileWriteAllowed) {
                    sendReply("fileAttributesAvailable", 23, "Permission denied", "EACCES");
                    _log.warn("Permission denied for {} to rename a file: {}",
                              Subjects.getDisplayName(_subject), _pnfsId);
                } else {
                    _pnfs.renameEntry(_pnfsId, _newName);
                    sendReply("fileAttributesAvailable", 23, "Permission denied", "EACCES");
                }
            } catch (Exception e) {
                sendReply("fileAttributesAvailable", 23, e.getMessage(), "EACCES");
            } finally {
                removeUs();
            }
        }
        @Override
        public String toString(){ return "rn "+super.toString() ; }
    }


    ////////////////////////////////////////////////////////////////////
    //
    //      the file rmdir handler
    //
    protected  class RmDirHandler  extends PnfsSessionHandler  {

        private RmDirHandler(int sessionId,
                             int commandId,
                             VspArgs args,
                             boolean followLinks)
            throws CacheException, CommandException
        {
            super( sessionId , commandId , args , true , followLinks ) ;
        }

        @Override
        protected void doLogin()
            throws CacheException
        {
            super.doLogin();
            if (_readOnly) {
                throw new CacheException(2, "Cannot execute 'rmdir': Permission denied");
            }
        }

        @Override
        public void fileAttributesAvailable()
        {
            try {
                String path = _message.getPnfsPath();
                if (new File(path).list().length == 0) {//if directory is empty, then check permission
                    if (_permissionHandler.canDeleteDir(_pnfsId, _subject, _origin) == AccessType.ACCESS_ALLOWED) {
                        _pnfs.deletePnfsEntry(path);
                        sendReply("fileAttributesAvailable", 0, "");
                    } else {
                        sendReply("fileAttributesAvailable", 23, "Permission denied", "EACCES");
                    }
                } else {//directory not empty
                    sendReply("fileAttributesAvailable", 23, "Directory not empty", "ENOTEMPTY");
                }
            } catch (ACLException e) {
                sendReply("fileAttributesAvailable", 23, e.getMessage(), "EACCES");
            } catch (CacheException e) {
                sendReply("fileAttributesAvailable", 23, e.getMessage(), "EACCES");
            } finally {
                removeUs();
            }
        }
        @Override
        public String toString(){ return "uk "+super.toString() ; }
    }

    protected  class MkDirHandler  extends PnfsSessionHandler  {

        private MkDirHandler(int sessionId,
                             int commandId,
                             VspArgs args,
                             boolean followLinks)
            throws CacheException, CommandException
        {
            super( sessionId , commandId , args , true , followLinks ) ;
        }

        @Override
        protected void doLogin()
            throws CacheException
        {
            super.doLogin();
            if (_readOnly) {
                throw new CacheException(2, "Cannot execute 'mkdir': Permission denied");
            }
        }

        @Override
        public boolean fileAttributesNotAvailable() throws CacheException
        {
            try {
                String path = _message.getPnfsPath();
                String parent = new File(path).getParent();

                if (_permissionHandler.canCreateDir(_pnfs.getPnfsIdByPath(parent), _subject, _origin) == AccessType.ACCESS_ALLOWED) {
                    _pnfs.createPnfsDirectory( path , getUid(), getGid(), getMode(0755));
                    sendReply("fileAttributesNotAvailable", 0, "");
                }else{
                    sendReply("fileAttributesNotAvailable", 23, "Permission denied", "EACCES");
                }
            } catch (ACLException e) {
                sendReply("fileAttributesNotAvailable", 23, e.getMessage(), "EACCES");
            }
            return false;
        }

        @Override
        public void fileAttributesAvailable() {

            sendReply("fileAttributesAvailable", 20, "Directory exists", "EEXIST");
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
    protected class CheckFileHandler  extends PnfsSessionHandler  {

        private final String _destination  ;
        private final String _protocolName  ;
        private List<String>  _assumedLocations;

        private CheckFileHandler(int sessionId, int commandId, VspArgs args)
            throws CacheException, CommandException
        {
            super( sessionId , commandId , args, false, true ) ;

            _info.setMessageType("check") ;
            _destination      = args.getOpt( "location" ) ;
            String protocolName = args.getOpt( "protocol" ) ;
            if( protocolName == null) {
                _protocolName     = "DCap/3" ;
            }else{
                _protocolName     = protocolName ;
            }
        }

        @Override
        protected void doStart()
            throws CacheException
        {
            try {
                PnfsId pnfsId = new PnfsId(_vargs.argv(0));
                _assumedLocations = _pnfs.getCacheLocations(pnfsId);
            } catch (IllegalArgumentException e) {
                DCapUrl url = new DCapUrl(_vargs.argv(0));
                String fileName = url.getFilePart();
                _assumedLocations = _pnfs.getCacheLocationsByPath(fileName);
            }

            if (_assumedLocations.isEmpty()) {
                throw new CacheException(4, "File is not cached");
            }

            super.doStart();
        }

        @Override
        public void fileAttributesAvailable(){
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
                  new PoolMgrQueryPoolsMsg( DirectionType.READ,
                _protocolName ,
                _destination ,
                _storageInfo ) ;

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

                            SpreadAndWait controller = new SpreadAndWait( _cell, 10000 ) ;

                            for( String pool: result ){

                                _log.debug("Sending query to pool {}", pool);
                                PoolCheckFileCostMessage request =
                                new PoolCheckFileCostMessage( pool , _pnfsId , 0L ) ;
                                controller.send( new CellMessage( new CellPath(pool) , request ) );
                            }
                            controller.waitForReplies() ;
                            int numberOfReplies = controller.getReplyCount() ;
                            _log.debug("Number of valied replies: {}", numberOfReplies);
                            if( numberOfReplies == 0 )
                                throw new
                                CacheException(4,"File not cached") ;

                            Iterator<CellMessage> iterate = controller.getReplies() ;
                            int found = 0 ;
                            while( iterate.hasNext() ){
                                CellMessage msg = iterate.next() ;
                                Object obj = msg.getMessageObject() ;
                                if( ! ( obj instanceof PoolCheckFileCostMessage ) ){
                                    _log.error("Unexpected reply from PoolCheckFileCostMessage: {}",
                                               obj.getClass().getName());
                                    continue ;
                                }
                                PoolCheckFileCostMessage reply = (PoolCheckFileCostMessage)obj ;
                                if( reply.getHave() ){
                                    _log.debug("pool {}: ok",
                                               reply.getPoolName());
                                    found ++ ;
                                }else{
                                    _log.debug("pool {}: File not found",
                                               reply.getPoolName());
                                }
                            }
                            if( found == 0 )
                                throw new
                                CacheException(4,"File not cached") ;

                        }
                         sendReply( "storageInfoAvailable" , 0 , "" ) ;
            }catch(CacheException cee ){
                // timur: since this situation can occur during the normal
                //        operation (dc_check of non-cached file)
                //        I am changing the following log to be
                //        logged only if the debugging is on
                //        anyway sendReply will log the response
                _log.debug(cee.toString());
                sendReply( "storageInfoAvailable" , cee.getRc() , cee.getMessage()) ;
            }catch(Exception ee ){
                _log.error(ee.toString());
                sendReply( "storageInfoAvailable" , 104 , ee.getMessage()) ;
            }finally{
                removeUs();
            }

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
        private Integer          _moverId      = null;
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
        private boolean _isUrl;

        private IoHandler(int sessionId, int commandId, VspArgs args)
            throws CacheException, CommandException
        {
            super(sessionId, commandId, args, false, true);

            _ioMode = _vargs.argv(1) ;
            int   port    = Integer.parseInt( _vargs.argv(3) ) ;

            StringTokenizer st = new StringTokenizer( _vargs.argv(2) , "," ) ;
            _hosts    = new String[st.countTokens()]  ;
            for( int i = 0 ; i < _hosts.length ; i++ )_hosts[i] = st.nextToken() ;
            //
            //
            _protocolInfo = new DCapProtocolInfo( "DCap",3,0, _hosts , port  ) ;
            _protocolInfo.setSessionId( _sessionId ) ;

            _isHsmRequest = ( args.getOpt("hsm") != null  );
            if( _isHsmRequest ){
                _log.debug("Hsm Feature Requested");
                if( _hsmManager == null )
                    throw new
                    CacheException( 105 , "Hsm Support Not enabled" ) ;
            }

            _overwrite      = args.getOpt("overwrite")   != null ;
            _strictSize     = args.getOpt("strict-size") != null ;
            _checksumString = args.getOpt("checksum") ;
            _truncFile      = args.getOpt("truncate");
            _truncate       = ( _truncFile != null ) && _truncateAllowed  ;
            _fileCheck      = args.getOpt("skip-file-check") == null;
            _protocolInfo.fileCheckRequaried(_fileCheck);

            _passive        = args.getOpt("passive") != null;
            _protocolInfo.isPassive(_passive);
            _accessLatency = args.getOpt("access-latency");
            _retentionPolicy = args.getOpt("retention-policy");

            _protocolInfo.door( new CellPath(_cell.getCellInfo().getCellName(),
                    _cell.getCellInfo().getDomainName()) ) ;

        }

        @Override
        protected void doLogin()
            throws CacheException
        {
            super.doLogin();
        }

        @Override
        protected void askForFileAttributes()
            throws IllegalArgumentException, NoRouteToCellException
        {
            setTimer(60 * 1000);

            try {
                _pnfsId = new PnfsId(_vargs.argv(0));
                _message = new PnfsGetFileAttributes(_pnfsId, _attributes);
            } catch (IllegalArgumentException e) {
                /* Seems not to be a pnfsId, might be a url.
                 */
                DCapUrl url = new DCapUrl(_vargs.argv(0));
                String fileName = url.getFilePart();
                _message = new PnfsGetFileAttributes(fileName, _attributes);
                _isUrl = true;
                _path = fileName;
            }

            _log.debug("Requesting file attributes for {}", _message);

            if (_vargs.argv(1).equals("r")) {
                _message.setAccessMask(EnumSet.of(AccessMask.READ_DATA));
            }
            _message.setId(_sessionId);
            _message.setReplyRequired(true);

            /* If _authorizationRequired is false then non-url based
             * access bypasses authorization.
             */
            if (!_isUrl && !_authorizationRequired) {
                _pnfs = new PnfsHandler(_pnfs, null);
            }

            /* if is url, _path is already pointing to to correct path */
            if (_path == null) {
                _path = _vargs.getOpt("path");
            }
            if (_path != null) {
                _info.setPath(_path);
            }

            _pnfs.send(_message);
            setStatus("WaitingForPnfs");
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
        public boolean fileAttributesNotAvailable() throws CacheException {
            //
            // hsm only support files in the cache.
            //
            if( _isHsmRequest )
                throw new
                CacheException( 107 , "Hsm only supports existing files" ) ;
            //
            // if this is not a url it's of course an error.
            //
            if( ! _isUrl ) {
                sendReply("fileAttributesNotAvailable", _message);
                return false ;
            }

            _log.debug("storageInfoNotAvailable : is url (mode={})", _ioMode);

            if( _ioMode.equals("r") )
                throw new
                CacheException( 2 , "No such file or directory" );

            //
            // for now we regard each error as 'file not found'
            // (so we can create it);
            //
            // first we have to findout if we are allowed to create a file.
            //
            String path = _message.getPnfsPath();
            String parent = new File(path).getParent();
            _log.debug("Creating file. path=_getStorageInfo.getPnfsPath()  -> path = {}", path);
            _log.debug("Creating file. parent = new File(path).getParent()  -> parent = {}", parent);
            _log.info("Creating file {}", path);
            try {

                if (_permissionHandler.canCreateFile(_pnfs.getPnfsIdByPath(parent), _subject, _origin) != AccessType.ACCESS_ALLOWED)
                throw new
                    CacheException(19, "Permission denied (Parent)");

            } catch (ACLException e) {
                throw new CacheException(e.getMessage());
            }

            PnfsCreateEntryMessage pnfsEntry =
                _pnfs.createPnfsEntry(_message.getPnfsPath(),
                                      getUid(), getGid(), getMode(0644));

            _log.debug("storageInfoNotAvailable : created pnfsid: {} path: {}",
                       pnfsEntry.getPnfsId(), pnfsEntry.getPnfsPath());
            _message = pnfsEntry;

            _isNew = true;

            return true ;
        }

        @Override
        public void fileAttributesAvailable()
        {
            _log.debug("{} storageInfoAvailable after {} ",
                       _pnfsId, (System.currentTimeMillis()-_started));

            PoolMgrSelectPoolMsg getPoolMessage = null ;

            FileMetaData meta = _fileMetaData;

            if( ! meta.isRegularFile() ){
                sendReply( "fileAttributesAvailable", 1 ,
                "Not a File" ) ;
                removeUs() ;
                return ;
            }


            if( _storageInfo.isCreatedOnly() || _overwrite || _truncate ||
            ( _isHsmRequest && ( _ioMode.indexOf( 'w' ) >= 0 ) ) ){
                //
                //
                if( _isHsmRequest && _storageInfo.isStored() ){
                    sendReply( "fileAttributesAvailable", 1 ,
                    "HsmRequest : file already stored" ) ;
                    removeUs() ;
                    return ;
                }
                //
                // the file is an  pnfsEntry only.
                // 'read only would be nonsense'
                //
                if( _ioMode.indexOf( 'w' ) < 0 ){

                    sendReply( "fileAttributesAvailable", 1 ,
                    "File doesn't exist (can't be readOnly)" ) ;
                    removeUs() ;
                    return ;
                }


                // we need a write pool
                //
                _protocolInfo.setAllowWrite(true) ;
                if( _overwrite ){
                    _storageInfo.setKey("overwrite","true");
                    _log.debug("Overwriting requested");
                }

                if( _truncate && ! _isNew ) {
                    try {
                        if( _isUrl ) {
                            String path = _message.getPnfsPath();
                            _log.debug("truncating path {}", path);
                            _pnfs.deletePnfsEntry( path );
                            _message = _pnfs.createPnfsEntry(path , getUid(), getGid(), getMode(0644));
                            _pnfsId = _message.getPnfsId() ;
                            _storageInfo = _message.getFileAttributes().getStorageInfo();
                        }else{
                            _pnfsId = new PnfsId( _truncFile );
                            _message = _pnfs.getStorageInfoByPnfsId( _pnfsId ) ;
                            _storageInfo = _message.getFileAttributes().getStorageInfo() ;
                        }

                    }catch(CacheException ce ) {
                        _log.error(ce.toString());
                        sendReply( "fileAttributesAvailable", 1,
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
                    _log.debug("Checksum from client {}", _checksumString);
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
                if( _path != null ) {
                    getPoolMessage.setPnfsPath(_path);
                }
            }else{
                //
                // sorry, we don't allow write (not yet)
                // (except if 'overwrite' is given [FERMI ingest]
                //
                if( _ioMode.indexOf( 'w' ) > -1){

                    sendReply( "fileAttributesAvailable", 1 ,
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

               if ( _isUrl || _authorizationRequired ) {

                    try {
                        if (!_ioMode.equals("r") || _permissionHandler.canReadFile(_path, _subject, _origin) != AccessType.ACCESS_ALLOWED) {
                    sendReply( "fileAttributesAvailable", 2 , "Permission denied", "EACCES" ) ;
                    removeUs() ;
                    return ;
                }

                    } catch (ACLException e) {
                        sendReply("fileAttributesAvailable", 1, e.getMessage());
                        removeUs();
                        return;
                    } catch (CacheException e) {
                        sendReply("fileAttributesAvailable", 1, e.getMessage());
                        removeUs();
                        return;
                    }
                }
                //
                // try to get some space to store the file.
                //
               int allowedStates;
               try {
                   allowedStates =
                       _checkStagePermission.canPerformStaging(_subject, _storageInfo)
                       ? RequestContainerV5.allStates
                       : RequestContainerV5.allStatesExceptStage;
               } catch (IOException e) {
                   allowedStates = RequestContainerV5.allStatesExceptStage;
                   _log.error("Error while reading data from StageConfiguration.conf file : {}", e.getMessage());
               }
               getPoolMessage =
                   new PoolMgrSelectReadPoolMsg(_pnfsId,
                                                _storageInfo,
                                                _protocolInfo,
                                                0,
                                                allowedStates);
               getPoolMessage.setIoQueueName(_ioQueueName );
            }

            if( _verbose )sendComment("opened");

            getPoolMessage.setSubject(_subject);
            getPoolMessage.setId(_sessionId);
            try {
                _cell.sendMessage(new CellMessage(new CellPath(_isHsmRequest
                                                               ? _hsmManager
                                                               : _poolManagerName) ,
                                                  getPoolMessage));
            } catch (Exception ie) {
                sendReply( "fileAttributesAvailable" , 2 ,
                           ie.toString() ) ;
                removeUs()  ;
                return ;
            }
            setStatus( "WaitingForGetPool" ) ;
            setTimer(_poolRetry) ;
            return ;

        }
        private void storeChecksumInPnfs( PnfsId pnfsId , String checksumString){
            try{
                PnfsFlagMessage flag =
                new PnfsFlagMessage(pnfsId,"c", PnfsFlagMessage.FlagOperation.SET) ;
                flag.setReplyRequired(false) ;
                flag.setValue(checksumString);

                _pnfs.send(flag);
            }catch(Exception eee ){
                _log.error("Failed to send crc to PnfsManager : {}", eee.toString());
            }
        }

        public void
        poolMgrSelectPoolArrived( PoolMgrSelectPoolMsg reply ){

            setTimer(0L);
            _log.debug("poolMgrGetPoolArrived : {}", reply);
            _log.debug("{} poolMgrSelectPoolArrived after {}",
                       _pnfsId, (System.currentTimeMillis() - _started));

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

            // use the updated StorageInfo from PoolManager/SpaceManager
            _storageInfo = reply.getStorageInfo();

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


            if( _poolRequestDone ){
                _log.debug("Ignoring double message");
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
                _cell.sendMessage(new CellMessage(toPool, poolMessage));
                _poolRequestDone = true ;
            }catch(Exception ie){
                sendReply( "poolMgrGetPoolArrived" , 2 ,
                           ie.toString() ) ;
                removeUs()  ;
                return ;
            }
            setStatus( "WaitingForOpenFile" ) ;

        }
        public void
        poolIoFileArrived( PoolIoFileMessage reply ){

            _log.debug("poolIoFileArrived : {}", reply);
            if( reply.getReturnCode() != 0 ){


                // bad entry in cacheInfo and pool Manager did not check it ( for performance reason )
                // try again
                if( reply.getReturnCode() == CacheException.FILE_NOT_IN_REPOSITORY ) {
                    _poolRequestDone = false;
                    this.fileAttributesAvailable();
                    return;
                }


                sendReply( "poolIoFileArrived" , reply )  ;
                removeUs() ;
                return ;
            }
            _moverId = reply.getMoverId();
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

            println( sb.toString() ) ;
            setStatus( "WaitingForDoorTransferOk" ) ;

        }

        // FIXME: sleep with lock
        public synchronized void
        doorTransferArrived( DoorTransferFinishedMessage reply ){

            if( reply.getReturnCode() == 0 ){

                long filesize = reply.getStorageInfo().getFileSize() ;
                _log.info("doorTransferArrived : fs={};strict={};m={}",
                          new Object[] { filesize, _strictSize, _ioMode });
                if( _strictSize && ( filesize > 0L ) && ( _ioMode.indexOf("w") > -1 ) ){

                    for( int count = 0 ; count < 10 ; count++  ){
                        try{
                            long fs = _pnfs.getStorageInfoByPnfsId( _pnfsId).
                            getStorageInfo().
                            getFileSize() ;
                            _log.info("doorTransferArrived : Size of {}: {}",
                                      _pnfsId, fs);
                            if( fs > 0L )break ;
                        }catch(Exception ee ){
                            _log.error("Problem getting storage info (check) for {}: {}", _pnfsId, ee);
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

            /*
             * mover is already gone
             */
            _moverId = null;
            removeUs() ;
            setStatus( "<done>" ) ;
        }
        @Override
        public String toString(){ return "io ["+_pool+"] "+super.toString() ; }

        @Override
        public void removeUs() {
            if( _moverId != null ) {
                PoolMoverKillMessage message = new PoolMoverKillMessage(_pool, _moverId);
                message.setReplyRequired(false);

                try {
                    _cell.sendMessage(new CellMessage(new CellPath(_pool), message));
                } catch (NoRouteToCellException e) {
                    _log.error("pool {} is unreachable", _pool);
                }
            }
            super.removeUs();
        }
    }

    ////////////////////////////////////////////////////////////////////
    //
    //      the basic opendir handler
    //
    protected  class OpenDirHandler  extends PnfsSessionHandler  {

        private DCapProtocolInfo _protocolInfo = null ;
        private String           _pool         = "dirLookupPool" ;
        private String []        _hosts        = null ;

        private OpenDirHandler(int sessionId, int commandId, VspArgs args)
            throws CacheException, CommandException
        {
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
        public void fileAttributesAvailable(){
            //
            // we are not called if the pnfs request failed.
            //

            String path = _message.getPnfsPath();

            if( ! _fileMetaData.isDirectory() ) {
                sendReply( "fileAttributesAvailable" , 22, path +" is not a directory", "ENOTDIR" ) ;
                removeUs()  ;
                return ;
            }

            try {
                if (_permissionHandler.canListDir(_pnfsId, _subject, _origin) != AccessType.ACCESS_ALLOWED) {
                    sendReply("fileAttributesAvailable", 19, "Permission denied to list directory", "EACCES");
                    return;
                }

            } catch ( ACLException e ) {
                sendReply("fileAttributesAvailable", 19, e.getMessage() + ". Can't list a directory",  "EACCES");
            } catch ( CacheException e ) {
                sendReply("fileAttributesAvailable", 19, e.getMessage() + ". Can't list a directory", "EACCES");
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

            try {
                _cell.sendMessage(new CellMessage(new CellPath(_pool),
                                                  poolIoFileMessage));
            } catch (Exception ie) {
                sendReply("poolMgrGetPoolArrived", 2, ie.toString());
                removeUs();
                return;
            }
        }

        public void
        poolIoFileArrived( PoolIoFileMessage reply ){

            _log.debug("poolIoFileArrived : {}", reply);
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

    @Override
    public String execute(VspArgs args)
        throws CommandExitException
    {
        int sessionId = args.getSessionId();
        int commandId = args.getSubSessionId();

        DcapCommand dcapCommand;
        try {
            dcapCommand = DcapCommand.get(args.getCommand());
        } catch(IllegalArgumentException e) {
            return protocolViolation(sessionId, commandId, args.getName(), 669,
                    "Invalid command '"+ args.getCommand() +"'");
        }

        try {
            switch(dcapCommand) {
                case HELLO:
                    return com_hello(sessionId, commandId, args);
                case BYEBYE:
                    return com_byebye(sessionId, commandId, args);
                case OPEN:
                    return com_open(sessionId, commandId, args);
                case CHECK:
                    return com_check(sessionId, commandId, args);
                case CHGRP:
                    return com_chgrp(sessionId, commandId, args);
                case CHOWN:
                    return com_chown(sessionId, commandId, args);
                case CHMOD:
                    return com_chmod(sessionId, commandId, args);
                case LSTAT:
                    return com_lstat(sessionId, commandId, args);
                case MKDIR:
                    return com_mkdir(sessionId, commandId, args);
                case OPENDIR:
                    return com_opendir(sessionId, commandId, args);
                case PING:
                    return com_ping(sessionId, commandId, args);
                case RENAME:
                    return com_rename(sessionId, commandId, args);
                case RMDIR:
                    return com_rmdir(sessionId, commandId, args);
                case STAGE:
                    return com_stage(sessionId, commandId, args);
                case STAT:
                    return com_stat(sessionId, commandId, args);
                case STATUS:
                    return com_status(sessionId, commandId, args);
                case UNLINK:
                    return com_unlink(sessionId, commandId, args);
                default:
                    /*
                     * just in case we added a new command
                     */
                    throw new UnsupportedOperationException("command not supported: " + dcapCommand);
            }
        } catch (CommandExitException  e) {
            throw e;
        } catch (CommandException  e) {
            return commandFailed(sessionId, commandId, args.getName(), e.getErrorCode(),
                    e.getErrorMessage());
        } catch(CacheException e) {
            return commandFailed(sessionId, commandId, args.getName(), e.getRc(),
                    e.getMessage());
        } catch(RuntimeException e) {
            _log.error(e.toString(), e);
            return commandFailed(sessionId, commandId, args.getName(), 44, e.getMessage());
        }
    }

    private String commandFailed(int sessionId, int commandId, String name,
            int errorCode, String errorMessage) {
        String problem = String.format("%d %d %s failed %d \"internalError : %s\"",
                sessionId, commandId, name, errorCode, errorMessage);
        _log.debug(problem.toString());
        return problem;
    }

    private String protocolViolation(int sessionId, int commandId, String name,
            int errorCode, String errorMessage) {
        String problem= String.format("%d %d %s failed %d \"protocolViolation : %s\"",
                sessionId, commandId, name, errorCode, errorMessage);
        _log.debug(problem.toString());
        return problem;
    }

    @Override
    public void close() {
        for(SessionHandler sh: _sessions.values()) {
            try {
                sh.removeUs();
            } catch (RuntimeException e) {
                /*
                 * we catch all RunTimeExceptions to be able to remove all sessions
                 */
                _log.error("failed to removed session: " + sh, e);
            }
        }
    }

    public void   getInfo( PrintWriter pw ){
        pw.println( " ----- DCapDoorInterpreterV3 ----------" ) ;
        pw.println( "      User = " + Subjects.getDisplayName(_authenticatedSubject));
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

    public void  messageArrived(CellMessage msg)
    {
        Object object = msg.getMessageObject();

        if (!(object instanceof Message)) {
            _log.warn("Unexpected message class {} source = {}",
                      object.getClass(), msg.getSourceAddress());
            return;
        }

        Message reply = (Message) object;
        SessionHandler handler = _sessions.get((int) reply.getId());
        if (handler == null) {
            _log.warn("Unexpected message ({}) for session : {}",
                      reply.getClass(), reply.getId());
            return;
        }

        if( reply instanceof DoorTransferFinishedMessage ){

            ((IoHandler)handler).doorTransferArrived( (DoorTransferFinishedMessage)reply )  ;

        }else if( reply instanceof PoolMgrGetPoolMsg ){

            ((IoHandler)handler).poolMgrSelectPoolArrived( (PoolMgrSelectPoolMsg)reply )  ;

        }else if( reply instanceof PnfsGetFileAttributes ){

            ((PnfsSessionHandler)handler).pnfsGetFileAttributesArrived( (PnfsGetFileAttributes)reply )  ;

        }else if( reply instanceof PoolIoFileMessage ){

            ((IoHandler)handler).poolIoFileArrived( (PoolIoFileMessage)reply )  ;

        }else if( reply instanceof PoolPassiveIoFileMessage ){

            ((IoHandler)handler).poolPassiveIoFileMessage( (PoolPassiveIoFileMessage)reply )  ;

        } else {
            _log.warn("Unexpected message class {} source = {}",
                      object.getClass(), msg.getSourceAddress());
        }
    }

    private void postToBilling(DoorRequestInfoMessage info) {
        try {
            _cell.sendMessage(new CellMessage(_billingCellPath, info));
        } catch (NoRouteToCellException ee) {
            _log.info("Billing is not available.");
        }
    }
}
