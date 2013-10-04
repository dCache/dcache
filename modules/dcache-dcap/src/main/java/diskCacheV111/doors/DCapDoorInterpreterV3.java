package diskCacheV111.doors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import diskCacheV111.namespace.NameSpaceProvider;
import diskCacheV111.poolManager.PoolSelectionUnit.DirectionType;
import diskCacheV111.poolManager.RequestContainerV5;
import diskCacheV111.util.AccessLatency;
import diskCacheV111.util.Base64;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.CheckStagePermission;
import diskCacheV111.util.DCapUrl;
import diskCacheV111.util.FileMetaData;
import diskCacheV111.util.PnfsHandler;
import diskCacheV111.util.PnfsId;
import diskCacheV111.util.RetentionPolicy;
import diskCacheV111.util.SpreadAndWait;
import diskCacheV111.util.VspArgs;
import diskCacheV111.vehicles.DCapProtocolInfo;
import diskCacheV111.vehicles.DoorRequestInfoMessage;
import diskCacheV111.vehicles.DoorTransferFinishedMessage;
import diskCacheV111.vehicles.IoDoorEntry;
import diskCacheV111.vehicles.IoDoorInfo;
import diskCacheV111.vehicles.Message;
import diskCacheV111.vehicles.PnfsCreateEntryMessage;
import diskCacheV111.vehicles.PnfsFlagMessage;
import diskCacheV111.vehicles.PoolAcceptFileMessage;
import diskCacheV111.vehicles.PoolCheckFileMessage;
import diskCacheV111.vehicles.PoolDeliverFileMessage;
import diskCacheV111.vehicles.PoolIoFileMessage;
import diskCacheV111.vehicles.PoolMgrQueryPoolsMsg;
import diskCacheV111.vehicles.PoolMgrSelectPoolMsg;
import diskCacheV111.vehicles.PoolMgrSelectReadPoolMsg;
import diskCacheV111.vehicles.PoolMgrSelectWritePoolMsg;
import diskCacheV111.vehicles.PoolMoverKillMessage;
import diskCacheV111.vehicles.PoolPassiveIoFileMessage;
import diskCacheV111.vehicles.StorageInfo;

import dmg.cells.nucleus.CellAddressCore;
import dmg.cells.nucleus.CellEndpoint;
import dmg.cells.nucleus.CellInfo;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.NoRouteToCellException;
import dmg.util.Args;
import dmg.util.CommandException;
import dmg.util.CommandExitException;
import dmg.util.KeepAliveListener;

import org.dcache.acl.enums.AccessMask;
import org.dcache.auth.CachingLoginStrategy;
import org.dcache.auth.LoginNamePrincipal;
import org.dcache.auth.LoginReply;
import org.dcache.auth.LoginStrategy;
import org.dcache.auth.Origin;
import org.dcache.auth.Subjects;
import org.dcache.auth.UnionLoginStrategy;
import org.dcache.auth.attributes.LoginAttribute;
import org.dcache.auth.attributes.ReadOnly;
import org.dcache.cells.CellStub;
import org.dcache.namespace.FileAttribute;
import org.dcache.pinmanager.PinManagerPinMessage;
import org.dcache.services.login.RemoteLoginStrategy;
import org.dcache.vehicles.FileAttributes;
import org.dcache.vehicles.PnfsGetFileAttributes;

import static org.dcache.namespace.FileAttribute.*;
import static org.dcache.namespace.FileType.DIR;
import static org.dcache.namespace.FileType.REGULAR;

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
                new HashMap<>();
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
        new ConcurrentHashMap<>();
    private String  _poolManagerName;
    private String  _pnfsManagerName;

    private final CellStub _gPlazmaStub;
    private CellStub _pinManagerStub;
    private CellPath _poolMgrPath;

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
    private int _uid = NameSpaceProvider.DEFAULT;

    /**
     * The client GID set through the hello command. Used for setting
     * the group of new name space entries. Never used for
     * authorization.
     */
    private int _gid = NameSpaceProvider.DEFAULT;

    private int     _majorVersion;
    private int     _minorVersion;
    private Date    _startedTS;
    private Date    _lastCommandTS;

    /**
     * If false, then authorization checks on read and write
     * operations are bypassed for non URL operations. If true, then
     * such operations are subject to authorization checks.
     */
    private boolean _authorizationRequired;

    /**
     * If true, then the Subject of the request must have a UID and
     * GID. If false, then a Subject without a UID and GID (i.e. a
     * Nobody) will be allowed to proceed, but only allowed to perform
     * operations authorized to world.
     */
    private boolean _authorizationStrong;

    private final UnionLoginStrategy.AccessLevel _anonymousAccessLevel;

    private final CellPath _billingCellPath;
    private final InetAddress _clientAddress;

    /**
     * Subjected provided to us as a result of authentication in the
     * tunnel.
     */
    private final Subject _authenticatedSubject;

    private final LoginStrategy _loginStrategy;

    /**
     * Tape Protection
     */
    private final String _stageConfigurationFilePath;
    private final CheckStagePermission _checkStagePermission;

    private boolean _strictSize;
    private String  _poolProxy;
    private Version _minClientVersion;
    private Version _maxClientVersion;
    private boolean _checkStrict      = true ;
    private long    _poolRetry;
    private String  _hsmManager;

    private boolean _truncateAllowed;
    private String  _ioQueueName;
    private boolean _ioQueueAllowOverwrite;

    // flag defined in batch file to allow/disallow AccessLatency and RetentionPolicy re-definition

    private boolean _isAccessLatencyOverwriteAllowed;
    private boolean _isRetentionPolicyOverwriteAllowed;
    private final boolean _readOnlyDoor;

    public DCapDoorInterpreterV3(CellEndpoint cell, PrintWriter pw,
            Subject subject, InetAddress clientAddress)
    {
        _out  = pw ;
        _cell = cell ;
        _args = cell.getArgs();
        _authenticatedSubject = new Subject(true,
                                            subject.getPrincipals(),
                                            subject.getPublicCredentials(),
                                            subject.getPrivateCredentials());

        _clientAddress = clientAddress;
        String auth = _args.getOpt("authorization") ;
        _authorizationStrong   = ( auth != null ) && auth.equals("strong") ;
        _authorizationRequired = ( auth != null ) &&
        ( auth.equals("strong") || auth.equals("required") ) ;

        _log.debug("Authorization required:  {}", _authorizationRequired);
        _log.debug("Authorization strong: {}", _authorizationStrong);

        String anon = _args.getOpt("anonymous-access");
        _anonymousAccessLevel = ( anon != null ) ? UnionLoginStrategy.AccessLevel.valueOf(anon.toUpperCase()) :
            UnionLoginStrategy.AccessLevel.READONLY;
        _log.debug("Anonymous access level : {}", _anonymousAccessLevel);

        _pnfsManagerName = _args.getOpt("pnfsManager");
        _poolManagerName = _args.getOpt("poolManager");
        _poolProxy = _args.getOpt("poolProxy");

        if (_poolProxy != null) {
            _log.debug("Pool Proxy set to {}", _poolProxy);
        }

        // allow file truncating
        String truncate = _args.getOpt("truncate");
        _truncateAllowed = (truncate != null) && truncate.equals("true") ;

        _isAccessLatencyOverwriteAllowed = _args.hasOption("allow-access-policy-overwrite") ;
        _log.debug("Allowed to overwrite AccessLatency: {}", _isAccessLatencyOverwriteAllowed);

        _isRetentionPolicyOverwriteAllowed = _args.hasOption("allow-retention-policy-overwrite");
        _log.debug("Allowed to overwrite RetentionPolicy: {}", _isRetentionPolicyOverwriteAllowed);

        _poolMgrPath     = new CellPath( _poolManagerName ) ;
        _pinManagerStub = new CellStub(cell, new CellPath(_args.getOpt("pinManager")));
        _gPlazmaStub = new CellStub(_cell, new CellPath(_args.getOpt("gplazma")), 30000);
        _billingCellPath = new CellPath(_args.getOpt("billing"));

        _checkStrict     = _args.hasOption("check") &&
        ( _args.getOpt("check").equals("strict") ) ;

        _strictSize      = _args.hasOption("strict-size") ;

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

        if( poolRetryValue != null ) {
            try {
                _poolRetry = Long.parseLong(poolRetryValue) * 1000L;
            } catch (NumberFormatException e) {
                _log.error("Problem in setting PoolRetry Value: {}", e);
            }
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
        if( check != null ) {
            _checkStrict = check.equals("strict");
        }

        String ro = _args.getOpt("read-only");
        _readOnlyDoor = ( ro != null ) && ro.equalsIgnoreCase("true");

        if (_readOnlyDoor) {
            _log.debug("Door is configured as read-only");
        }
        else {
            _log.debug("Door is configured as read/write");
        }

        _stageConfigurationFilePath = _args.getOpt("stageConfigurationFilePath");
        _checkStagePermission = new CheckStagePermission(_stageConfigurationFilePath);
        _log.debug("Check : {}", _checkStrict ? "Strict" : "Fuzzy");

        _loginStrategy = createLoginStrategy();
    }

    private LoginStrategy createLoginStrategy()
    {
        UnionLoginStrategy union = new UnionLoginStrategy();

        if (_authorizationStrong || _authorizationRequired) {
            union.setLoginStrategies(Collections.<LoginStrategy>singletonList(new RemoteLoginStrategy(_gPlazmaStub)));
        }

        if (!_authorizationStrong ) {
            union.setAnonymousAccess(_anonymousAccessLevel);
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
            subject.getPrincipals().addAll(_authenticatedSubject.getPrincipals());
            subject.getPrincipals().add(new LoginNamePrincipal(user));
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
        @Override
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
           if( obj == this ) {
               return true;
           }
           if( !(obj instanceof Version) ) {
               return false;
           }

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
        _out.println(str);
    }

    @Override
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
        if( args.argc() < 2 ) {
            throw new
                    CommandExitException("Command Syntax Exception", 2);
        }

        Version version;
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
            _log.error(error);
            throw new
            CommandExitException(error , 1 );
        }
        String yourName = args.getName() ;
        if( yourName.equals("server") ) {
            _ourName = "client";
        }
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
        if( args.argc() < 4 ) {
            throw new
                    CommandException(3, "Not enough arguments for put");
        }

        start(new IoHandler(sessionId,commandId,args));
        return null ;
    }

    public synchronized String com_stage( int sessionId , int commandId , VspArgs args )
        throws CommandException
    {
        _lastCommandTS = new Date() ;
        if( args.argc() < 1 ) {
            throw new
                    CommandException(3, "Not enough arguments for stage");
        }

        start(new PrestageHandler(sessionId, commandId, args));
        return null ;
    }
    public synchronized String com_lstat( int sessionId , int commandId , VspArgs args )
        throws CommandException
    {
        return get_stat( sessionId , commandId , args , false ) ;

    }
    public synchronized String com_stat( int sessionId , int commandId , VspArgs args )
        throws CommandException
    {
        return get_stat( sessionId , commandId , args , true ) ;

    }

    public synchronized String com_unlink( int sessionId , int commandId , VspArgs args )
        throws CommandException
    {
        return do_unlink( sessionId , commandId , args , true ) ;
    }

    public synchronized String com_rename( int sessionId , int commandId , VspArgs args )
        throws CommandException
    {
        return do_rename( sessionId , commandId , args ) ;
    }

    public synchronized String com_rmdir( int sessionId , int commandId , VspArgs args )
        throws CommandException
    {
        return do_rmdir( sessionId , commandId , args , true ) ;
    }

    public synchronized String com_mkdir( int sessionId , int commandId , VspArgs args )
        throws CommandException
    {
        return do_mkdir( sessionId , commandId , args , true ) ;
    }

    public synchronized String com_chmod( int sessionId , int commandId , VspArgs args )
        throws CommandException
    {
        return do_chmod( sessionId , commandId , args , true ) ;
    }

    public synchronized String com_chown( int sessionId , int commandId , VspArgs args )
        throws CommandException
    {
        return do_chown( sessionId , commandId , args , true ) ;
    }


    public synchronized String com_chgrp( int sessionId , int commandId , VspArgs args )
        throws CommandException
    {
        return do_chgrp( sessionId , commandId , args , true ) ;
    }

    public synchronized String com_opendir( int sessionId , int commandId , VspArgs args )
        throws CommandException
    {
        return do_opendir( sessionId , commandId , args ) ;

    }

    private synchronized String do_unlink(
    int sessionId , int commandId , VspArgs args , boolean resolvePath )
        throws CommandException
    {
        _lastCommandTS = new Date() ;
        if( args.argc() < 1 ) {
            throw new
                    CommandException(3, "Not enough arguments for unlink");
        }

        start(new UnlinkHandler(sessionId, commandId, args, resolvePath));
        return null ;
    }



    private synchronized String do_rename(int sessionId, int commandId, VspArgs args)
        throws CommandException
    {
        _lastCommandTS = new Date() ;
        if( args.argc() < 1 ) {
            throw new
                    CommandException(3, "Not enough arguments for unlink");
        }

        start(new RenameHandler(sessionId, commandId, args));
        return null ;
    }


    private synchronized String do_rmdir(
    int sessionId , int commandId , VspArgs args , boolean resolvePath )
        throws CommandException
    {
        _lastCommandTS = new Date() ;
        if( args.argc() < 1 ) {
            throw new
                    CommandException(3, "Not enough arguments for rmdir");
        }

        start(new RmDirHandler(sessionId, commandId, args, resolvePath));
        return null ;
    }

    private synchronized String do_mkdir(
    int sessionId , int commandId , VspArgs args , boolean resolvePath )
        throws CommandException
    {
        _lastCommandTS = new Date() ;
        if( args.argc() < 1 ) {
            throw new
                    CommandException(3, "Not enough arguments for unlink");
        }

        start(new MkDirHandler(sessionId, commandId, args, resolvePath));
        return null ;
    }

    private synchronized String do_chown(
    int sessionId , int commandId , VspArgs args , boolean resolvePath )
        throws CommandException
    {
        _lastCommandTS = new Date() ;
        if( args.argc() < 1 ) {
            throw new
                    CommandException(3, "Not enough arguments for chown");
        }

        start(new ChownHandler(sessionId, commandId, args, resolvePath));
        return null ;
    }


    private synchronized String do_chgrp(
            int sessionId , int commandId , VspArgs args , boolean resolvePath )
        throws CommandException
    {
                _lastCommandTS = new Date() ;
                if( args.argc() < 1 ) {
                    throw new
                            CommandException(3, "Not enough arguments for chgrp");
                }

                start(new ChgrpHandler(sessionId, commandId, args, resolvePath));
                return null ;
     }

    private synchronized String do_chmod(
            int sessionId , int commandId , VspArgs args , boolean resolvePath )
        throws CommandException
    {
                _lastCommandTS = new Date() ;
                if( args.argc() < 1 ) {
                    throw new
                            CommandException(3, "Not enough arguments for chmod");
                }

                start(new ChmodHandler(sessionId, commandId, args, resolvePath));
                return null ;
            }

    private synchronized String do_opendir(  int sessionId , int commandId , VspArgs args )
        throws CommandException
    {
        _lastCommandTS = new Date() ;
        if( args.argc() < 1 ) {
            throw new
                    CommandException(3, "Not enough arguments for opendir");
        }

        start(new OpenDirHandler(sessionId,commandId,args));
        return null ;
    }

    private synchronized String get_stat(
    int sessionId , int commandId , VspArgs args , boolean resolvePath )
        throws CommandException
    {
        _lastCommandTS = new Date() ;
        if( args.argc() < 1 ) {
            throw new
                    CommandException(3, "Not enough arguments for stat");
        }

        start(new StatHandler(sessionId, commandId, args, resolvePath));
        return null ;
    }

    public synchronized String com_ping( int sessionId , int commandId , VspArgs args )
    {
        println(String.valueOf(sessionId)+" "+commandId+" server pong");
        return null ;
    }

    public synchronized String com_check( int sessionId , int commandId , VspArgs args )
        throws CommandException
    {
        _lastCommandTS = new Date() ;
        if( args.argc() < 1 ) {
            throw new
                    CommandException(3, "Not enough arguments for check");
        }

        start(new CheckFileHandler(sessionId, commandId, args));
        return null ;
    }

    public String com_status(int sessionId, int commandId, VspArgs args)
        throws CommandException
    {
        _lastCommandTS = new Date() ;
        SessionHandler handler = _sessions.get(sessionId);
        if (handler == null) {
            throw new
                CommandException(5, "Session ID " + sessionId + " not found.");
        }

        return ""+sessionId+" "+commandId+" "+args.getName()+
            " ok "+" 0 " + "\""+handler+ "\"" ;
    }

    public static final String hh_get_door_info = "[-binary]" ;
    public Object ac_get_door_info( Args args ){
        IoDoorInfo info = new IoDoorInfo( _cell.getCellInfo().getCellName() ,
        _cell.getCellInfo().getDomainName() ) ;
        info.setProtocol("dcap","3");
        info.setOwner(String.valueOf(_uid));
        info.setProcess(_pid);
        List<IoDoorEntry> list = new ArrayList<>(_sessions.size());
        for (SessionHandler session: _sessions.values()) {
            if (session instanceof IoHandler) {
                list.add(((IoHandler) session).getIoDoorEntry());
            }
        }

        info.setIoDoorEntries( list.toArray(new IoDoorEntry[list.size()]) );
        if( args.hasOption("binary") ) {
            return info;
        } else {
            return info.toString();
        }
    }
    public static final String hh_toclient = " <id> <subId> server <command ...>" ;
    public String ac_toclient_$_3_99( Args args )
    {
        StringBuilder sb = new StringBuilder() ;
        for( int i = 0 ; i < args.argc() ; i++ ) {
            sb.append(args.argv(i)).append(" ");
        }
        String str = sb.toString() ;
        _log.debug("toclient (commander) : {}", str);
        println(str);
        return "" ;
    }
    public static final String hh_retry = "<sessionId> [-weak]" ;
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

        ((PnfsSessionHandler) session).again( !args.hasOption("weak") ) ;

        return "" ;
    }
    ////////////////////////////////////////////////////////////////////
    //
    //      the client handler
    //
    protected  class SessionHandler                {
        protected VspArgs _vargs;
        protected int     _sessionId;
        protected int     _commandId;
        protected boolean _verbose;
        protected long    _started   = System.currentTimeMillis() ;
        protected DoorRequestInfoMessage _info;

        protected String  _status          = "<init>" ;
        protected long    _statusSince     = System.currentTimeMillis() ;
        protected String  _ioHandlerQueue;

        private final long      _timestamp       = System.currentTimeMillis() ;

        protected Subject _subject;
        protected Origin _origin;
        protected boolean _readOnly;

        protected SessionHandler(int sessionId, int commandId, VspArgs args)
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

            _readOnly = _readOnlyDoor;
            for (LoginAttribute attribute: login.getLoginAttributes()) {
                if (attribute instanceof ReadOnly) {
                    _readOnly |= ((ReadOnly) attribute).isReadOnly();
                }
            }

            _info.setSubject(_subject);
        }

        /**
         * Called from the start method. Must perform whatever action
         * is needed to kick of processing the request.
         */
        protected void doStart()
            throws CacheException
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
            _log.debug(reply) ;
        }
        public void keepAlive(){
            _log.debug("Keep alived called for : {}", this);
        }

        protected void sendReply( String tag , int rc , String msg){
            sendReply(tag, rc, msg, "");
        }

        protected void sendReply( String tag , int rc , String msg , String posixErr){

            String problem;
            _info.setTransactionDuration(System.currentTimeMillis()-_timestamp);
            if( rc == 0 ){
                problem = String.format("%d %d %s ok", _sessionId, _commandId, _vargs.getName());
            }else{
                problem = String.format("%d %d %s failed %d \"%s\" %s",
                        _sessionId, _commandId, _vargs.getName(),
                        rc, msg, posixErr);
                _info.setResult( rc , msg ) ;
            }

            _log.debug("{}: {}", tag, problem);
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

        protected String       _path;
        protected FileAttributes _fileAttributes;
        private   long         _timer;
        private   final Object       _timerLock    = new Object() ;
        private long           _timeout;

        protected Set<FileAttribute> _attributes;
        protected PnfsGetFileAttributes _message;
        protected PnfsHandler _pnfs;

        protected PnfsSessionHandler(int sessionId, int commandId, VspArgs args,
                                     boolean metaDataOnly, boolean resolvePath)
        {
            super(sessionId, commandId, args);

            _attributes = FileMetaData.getKnownFileAttributes();
            _attributes.add(PNFSID);
            _attributes.add(TYPE);
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
            _pnfs.setSubject(_subject);
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
                    try {
                        again(true);
                    } catch (Exception e) {
                        sendReply("keepAlive", 111, e.getMessage());
                        removeUs() ;
                    }
                }
            }
        }
        protected void setTimer( long timeout ){
            synchronized( _timerLock ){

                _timer = timeout == 0L ? 0L : System.currentTimeMillis() + timeout ;
            }
        }

        public void again(boolean strong)
            throws IllegalArgumentException, NoRouteToCellException
        {
            askForFileAttributes();
        }

        protected void askForFileAttributes()
            throws IllegalArgumentException, NoRouteToCellException
        {
            setTimer(60 * 1000);

            String fileIdentifier = _vargs.argv(0);

            if( PnfsId.isValid(fileIdentifier)) {
                PnfsId pnfsId = new PnfsId(fileIdentifier);
                _message = new PnfsGetFileAttributes(pnfsId, _attributes);
            } else {
                DCapUrl url = new DCapUrl(fileIdentifier);
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

            _fileAttributes = _message.getFileAttributes();

            _info.setPnfsId(_fileAttributes.getPnfsId());

            if (_fileAttributes.isDefined(STORAGEINFO)) {
                StorageInfo storageInfo = _fileAttributes.getStorageInfo();
                for (int i = 0; i < _vargs.optc(); i++) {
                    String key = _vargs.optv(i);
                    String value = _vargs.getOpt(key);
                    storageInfo.setKey(key, value == null ? "" : value);
                }
            }

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
            return "["+((_fileAttributes == null) ? "null" : _fileAttributes.getPnfsId())+"]"+" {timer="+
            (_timer==0L?"off":""+(_timer-System.currentTimeMillis()))+"} "+
            super.toString() ;
        }
    }

    ////////////////////////////////////////////////////////////////////
    //
    //      the basic prestage handler
    //
    protected class PrestageHandler extends PnfsSessionHandler
    {
        private final String _destination;

        private PrestageHandler(int sessionId, int commandId, VspArgs args)
        {
            super(sessionId, commandId, args, false, true);
            _destination = args.getOpt("location");
        }

        @Override
        public void fileAttributesAvailable()
        {
            try {
                if (_fileAttributes.getFileType() != REGULAR) {
                    sendReply("storageInfoAvailable", CacheException.NOT_FILE,
                              "path is not a regular file", "EINVAL");
                    return;
                }

                DCapProtocolInfo protocolInfo =
                    new DCapProtocolInfo("DCap", 3, 0,
                        new InetSocketAddress(_destination, 0));
                PinManagerPinMessage message =
                    new PinManagerPinMessage(_fileAttributes, protocolInfo,
                                             null, 0);
                _pinManagerStub.send(message);
                sendReply("storageInfoAvailable", 0, "");
            } catch (NoRouteToCellException e) {
                sendReply("storageInfoAvailable", 2,
                          "Staging service is offline");
            } finally {
                removeUs();
            }
        }

        @Override
        public String toString()
        {
            return "st " + super.toString();
        }
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
        {
            super( sessionId , commandId , args , true , followLinks ) ;
        }

        @Override
        public void fileAttributesAvailable()
        {
            try {
                FileMetaData meta = new FileMetaData(_fileAttributes);
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

                sb.append("-st_ino=").append(_fileAttributes.getPnfsId().toString().hashCode()&0xfffffff) ;

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
                if (_fileAttributes.getFileType() != DIR) {
                    _pnfs.deletePnfsEntry(path);
                    sendReply("fileAttributesAvailable", 0, "");
                    sendRemoveInfoToBilling(path);
                } else {
                    sendReply("fileAttributesAvailable", 17, "Path is a Directory", "EISDIR");
                }
            } catch (CacheException e) {
                sendReply("fileAttributesAvailable", 19, e.getMessage(), "EACCES");
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
            infoRemove.setSubject(_subject);
            infoRemove.setPath(path);

            postToBilling(infoRemove);
        }

        @Override
        public String toString(){ return "uk "+super.toString() ; }
    }

    protected  class ChmodHandler  extends PnfsSessionHandler  {

        private int _permission;

        private ChmodHandler(int sessionId,
                             int commandId,
                             VspArgs args,
                             boolean followLinks)
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
                FileMetaData meta = new FileMetaData(_fileAttributes);
                meta.setMode(_permission);
                _pnfs.pnfsSetFileMetaData(_fileAttributes.getPnfsId(), meta);
                sendReply("fileAttributesAvailable", 0, "");
            } catch (CacheException e) {
                sendReply("fileAttributesAvailable", 19, e.getMessage(), "EACCES");
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

            FileMetaData meta = new FileMetaData(_fileAttributes);

            try {
                if (_owner >= 0) {
                    meta.setUid(_owner);
                }

                if (_group >= 0) {
                    meta.setGid(_group);
                }

                _pnfs.pnfsSetFileMetaData(_fileAttributes.getPnfsId(), meta);

                sendReply("fileAttributesAvailable", 0, "");
            } catch (CacheException e) {
                sendReply("fileAttributesAvailable", 19, e.getMessage(), "EACCES");
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

            FileMetaData meta = new FileMetaData(_fileAttributes);

            try {
                if (_group >= 0) {
                    meta.setGid(_group);
                }

                _pnfs.pnfsSetFileMetaData(_fileAttributes.getPnfsId(), meta);

                sendReply("fileAttributesAvailable", 0, "");
            } catch (CacheException e) {
                sendReply("fileAttributesAvailable", 19, e.getMessage(), "EACCES");
            } finally {
                removeUs();
            }
        }
        @Override
        public String toString(){ return "uk "+super.toString() ; }
    }


    protected  class RenameHandler  extends PnfsSessionHandler  {


        private String _newName;

        private RenameHandler(int sessionId, int commandId, VspArgs args)
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
                _pnfs.renameEntry(_fileAttributes.getPnfsId(), _newName);
                sendReply("fileAttributesAvailable", 0, "");
            } catch (CacheException e) {
                sendReply("fileAttributesAvailable", 19, e.getMessage(), "EACCES");
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
                _pnfs.deletePnfsEntry(_message.getPnfsPath(), EnumSet.of(DIR));
                sendReply("fileAttributesAvailable", 0, "");
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
            String path = _message.getPnfsPath();
            _pnfs.createPnfsDirectory(path, getUid(), getGid(),
                                      getMode(NameSpaceProvider.DEFAULT));
            sendReply("fileAttributesNotAvailable", 0, "");
            return false;
        }

        @Override
        public void fileAttributesAvailable() {

            sendReply("fileAttributesAvailable", 20, "Directory exists", "EEXIST");
            removeUs() ;
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
        {
            super( sessionId , commandId , args, false, true ) ;

            _info = new DoorRequestInfoMessage(
                    _cell.getCellInfo().getCellName()+"@"+_cell.getCellInfo().getDomainName(), "check");
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
                  new PoolMgrQueryPoolsMsg(DirectionType.READ,
                                           _protocolName ,
                                           _destination ,
                                           _fileAttributes.getStorageInfo());

                CellMessage checkMessage = new CellMessage( _poolMgrPath , query ) ;
                setStatus("Waiting for reply from PoolManager");

                checkMessage = _cell.sendAndWait(  checkMessage , 20000 ) ;
                query = (PoolMgrQueryPoolsMsg) checkMessage.getMessageObject() ;

                if( query.getReturnCode() != 0 ) {
                    throw new
                            CacheException(query.getReturnCode(),
                            query.getErrorObject() == null ? "?" :
                                    query.getErrorObject().toString());
                }
                        //
                        //
                        Set<String>   assumedHash = new HashSet<>( _assumedLocations ) ;
                        List<String> []   lists       = query.getPools() ;
                        List<String> result      = new ArrayList<>() ;

                for (List<String> pools : lists) {
                    for (String pool : pools) {
                        if (assumedHash.contains(pool)) {
                            result.add(pool);
                        }
                    }
                }

                        if( result.size() == 0 ) {
                            throw new
                                    CacheException(4, "File not cached");
                        }


                        if( _checkStrict ){

                            SpreadAndWait<PoolCheckFileMessage> controller = new SpreadAndWait<>(new CellStub(_cell, null, 10000));
                            for( String pool: result ){

                                _log.debug("Sending query to pool {}", pool);
                                PoolCheckFileMessage request =
                                    new PoolCheckFileMessage(pool, _fileAttributes.getPnfsId());
                                controller.send(new CellPath(pool), PoolCheckFileMessage.class, request);
                            }
                            controller.waitForReplies() ;
                            int numberOfReplies = controller.getReplyCount() ;
                            _log.debug("Number of valied replies: {}", numberOfReplies);
                            if( numberOfReplies == 0 ) {
                                throw new
                                        CacheException(4, "File not cached");
                            }

                            int found = 0 ;
                            for (PoolCheckFileMessage reply: controller.getReplies().values()) {
                                if( reply.getHave() ){
                                    _log.debug("pool {}: ok",
                                               reply.getPoolName());
                                    found ++ ;
                                }else{
                                    _log.debug("pool {}: File not found",
                                               reply.getPoolName());
                                }
                            }
                            if( found == 0 ) {
                                throw new
                                        CacheException(4, "File not cached");
                            }

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
        private String           _ioMode;
        private DCapProtocolInfo _protocolInfo;
        private String           _pool         = "<unknown>" ;
        private Integer          _moverId;
        private boolean          _isHsmRequest;
        private boolean          _overwrite;
        private String           _checksumString;
        private boolean          _truncate;
        private boolean          _isNew;
        private String            _truncFile;
        private boolean          _poolRequestDone;
        private String            _permission;
        private boolean          _passive;
        private String            _accessLatency;
        private String            _retentionPolicy;
        private boolean _isUrl;
        private PoolMgrSelectReadPoolMsg.Context _readPoolSelectionContext;
        private InetSocketAddress _clientSocketAddress;

        private IoHandler(int sessionId, int commandId, VspArgs args)
            throws CacheException
        {
            super(sessionId, commandId, args, false, true);

            _ioMode = _vargs.argv(1) ;
            int   port    = Integer.parseInt( _vargs.argv(3) ) ;

            StringTokenizer st = new StringTokenizer( _vargs.argv(2) , "," ) ;

            _passive = args.hasOption("passive");
            if (_passive) {
                _clientSocketAddress = new InetSocketAddress(_clientAddress, port);
            } else {
                String hostname = st.nextToken();

                _clientSocketAddress = new InetSocketAddress(hostname, port);

                if (_clientSocketAddress.isUnresolved()) {
                    _log.debug("Client sent unresolvable hostname {}", hostname);
                    throw new CacheException("Unknown host: " + hostname);
                }
            }

            _protocolInfo = new DCapProtocolInfo( "DCap",3,0, _clientSocketAddress  ) ;
            _protocolInfo.setSessionId( _sessionId ) ;

            _isHsmRequest = args.hasOption("hsm");
            if( _isHsmRequest ){
                _log.debug("Hsm Feature Requested");
                if( _hsmManager == null ) {
                    throw new
                            CacheException(105, "Hsm Support Not enabled");
                }
            }

            _overwrite      = args.hasOption("overwrite") ;
            _strictSize     = args.hasOption("strict-size") ;
            _checksumString = args.getOpt("checksum") ;
            _truncFile      = args.getOpt("truncate");
            _truncate       = ( _truncFile != null ) && _truncateAllowed  ;

            _protocolInfo.isPassive(_passive);
            _accessLatency = args.getOpt("access-latency");
            _retentionPolicy = args.getOpt("retention-policy");

            _protocolInfo.door( new CellPath(_cell.getCellInfo().getCellName(),
                    _cell.getCellInfo().getDomainName()) ) ;

            _attributes.addAll(PoolMgrSelectReadPoolMsg.getRequiredAttributes());
        }

        @Override
        protected void askForFileAttributes()
            throws IllegalArgumentException, NoRouteToCellException
        {
            setTimer(60 * 1000);

            try {
                PnfsId pnfsId = new PnfsId(_vargs.argv(0));
                _message = new PnfsGetFileAttributes(pnfsId, _attributes);
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
            /*
             * as long as door waits for reply to GetFileAttributes
             * the  _fileAttributes is null.
             */
            PnfsId pnfsid =
                    _fileAttributes != null ? _fileAttributes.getPnfsId() : null;

            return new IoDoorEntry(_sessionId,
                                   pnfsid,
                                   _pool,
                                   _status,
                                   _statusSince,
                                   _clientSocketAddress.getAddress().getHostAddress());
        }

        @Override
        public void again(boolean strong)
            throws IllegalArgumentException, NoRouteToCellException
        {
            if( strong ) {
                _poolRequestDone = false;
            }
            super.again(strong);
        }

        @Override
        public boolean fileAttributesNotAvailable() throws CacheException {
            if(_readOnly && _ioMode.indexOf('w') >= 0) {
                throw new CacheException( 2 , "Only read-only access allowed" );
            }            //
            // hsm only support files in the cache.
            //
            if( _isHsmRequest ) {
                throw new
                        CacheException(107, "Hsm only supports existing files");
            }
            //
            // if this is not a url it's of course an error.
            //
            if( ! _isUrl ) {
                sendReply("fileAttributesNotAvailable", _message);
                return false ;
            }

            _log.debug("storageInfoNotAvailable : is url (mode={})", _ioMode);

            if( _ioMode.equals("r") ) {
                throw new
                        CacheException(2, "No such file or directory");
            }

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

            PnfsCreateEntryMessage pnfsEntry =
                _pnfs.createPnfsEntry(_message.getPnfsPath(),
                                      getUid(), getGid(),
                                      getMode(NameSpaceProvider.DEFAULT));

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
                       _fileAttributes.getPnfsId(),
                       (System.currentTimeMillis()-_started));

            PoolMgrSelectPoolMsg getPoolMessage;

            if (_fileAttributes.getFileType() != REGULAR){
                sendReply( "fileAttributesAvailable", 1 ,
                "Not a File" ) ;
                removeUs() ;
                return ;
            }

            if (_fileAttributes.getStorageInfo().isCreatedOnly() || _overwrite || _truncate ||
            ( _isHsmRequest && ( _ioMode.indexOf( 'w' ) >= 0 ) ) ){
                if(_readOnly) {
                    sendReply("fileAttributesAvailable", 1,"Only read-only access allowed" );
                    removeUs() ;
                    return ;
                }                //
                //
                if (_isHsmRequest && _fileAttributes.getStorageInfo().isStored()){
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
                    _fileAttributes.getStorageInfo().setKey("overwrite","true");
                    _log.debug("Overwriting requested");
                }

                if( _truncate && ! _isNew ) {
                    try {
                        if( _isUrl ) {
                            String path = _message.getPnfsPath();
                            _log.debug("truncating path {}", path);
                            _pnfs.deletePnfsEntry( path );
                            _message = _pnfs.createPnfsEntry(path , getUid(), getGid(), getMode(NameSpaceProvider.DEFAULT));
                            _fileAttributes = _message.getFileAttributes();
                        }else{
                            _message = _pnfs.getStorageInfoByPnfsId(_fileAttributes.getPnfsId()) ;
                            _fileAttributes = _message.getFileAttributes();
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
                    _fileAttributes.getStorageInfo().setKey("path", _path);
                }

                if( _checksumString != null ){
                    _fileAttributes.getStorageInfo().setKey("checksum",_checksumString);
                    _log.debug("Checksum from client {}", _checksumString);
                    storeChecksumInPnfs( _fileAttributes.getPnfsId() , _checksumString ) ;
                }


                // adjust accessLatency and retention policy if it' allowed and defined

                if( _isAccessLatencyOverwriteAllowed && _accessLatency != null ) {
                    try {
                        AccessLatency accessLatency = AccessLatency.getAccessLatency(_accessLatency);
                        _fileAttributes.setAccessLatency(accessLatency);

                    }catch(IllegalArgumentException e) { /* bad AccessLatency ignored*/}
                }

                if( _isRetentionPolicyOverwriteAllowed && _retentionPolicy != null ) {
                    try {
                        RetentionPolicy retentionPolicy = RetentionPolicy.getRetentionPolicy(_retentionPolicy);
                        _fileAttributes.setRetentionPolicy(retentionPolicy);

                    }catch(IllegalArgumentException e) { /* bad RetentionPolicy ignored*/}
                }


                //
                // try to get some space to store the file.
                //
                long preallocated = 0L;
                String value = _fileAttributes.getStorageInfo().getKey("alloc-size");
                if (value != null) {
                    try {
                        preallocated = Long.parseLong(value);
                    } catch (NumberFormatException e) {
                        // bad values are ignored
                    }
                }
                getPoolMessage = new PoolMgrSelectWritePoolMsg(_fileAttributes, _protocolInfo, preallocated);
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

                //
                // try to get some space to store the file.
                //
               EnumSet<RequestContainerV5.RequestState> allowedStates;
               try {
                   allowedStates =
                       _checkStagePermission.canPerformStaging(_subject, _fileAttributes.getStorageInfo())
                       ? RequestContainerV5.allStates
                       : RequestContainerV5.allStatesExceptStage;
               } catch (IOException e) {
                   allowedStates = RequestContainerV5.allStatesExceptStage;
                   _log.error("Error while reading data from StageConfiguration.conf file : {}", e.getMessage());
               }
               getPoolMessage =
                   new PoolMgrSelectReadPoolMsg(_fileAttributes,
                                                _protocolInfo,
                                                _readPoolSelectionContext,
                                                allowedStates);
               getPoolMessage.setIoQueueName(_ioQueueName );
            }

            if( _verbose ) {
                sendComment("opened");
            }

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
            poolMgrSelectPoolArrived( PoolMgrSelectPoolMsg reply )
        {

            setTimer(0L);
            _log.debug("poolMgrGetPoolArrived : {}", reply);
            _log.debug("{} poolMgrSelectPoolArrived after {}",
                       _fileAttributes.getPnfsId(),
                       (System.currentTimeMillis() - _started));

            if (reply.getReturnCode() != 0) {
                if (reply.getReturnCode() == CacheException.OUT_OF_DATE ||
                    _poolRetry == 0) {
                    try {
                        again(true);
                    } catch (NoRouteToCellException e) {
                        _log.error("No route to {}", e.getDestinationPath());
                        sendReply( "poolMgrGetPoolArrived" , reply )  ;
                        removeUs();
                    }
                } else {
                    setTimer(_poolRetry);
                }
                return;
            }
            String pool = reply.getPoolName();
            if (pool == null) {
                sendReply( "poolMgrGetPoolArrived" , 33 , "No pools available" ) ;
                removeUs() ;
                return ;
            }

            // use the updated file attributes from PoolManager/SpaceManager
            _fileAttributes = reply.getFileAttributes();

            _pool = pool ;
            PoolIoFileMessage poolMessage;

            if( reply instanceof PoolMgrSelectReadPoolMsg ){
                _readPoolSelectionContext =
                    ((PoolMgrSelectReadPoolMsg) reply).getContext();
                poolMessage =
                        new PoolDeliverFileMessage(
                                pool,
                                _protocolInfo ,
                                _fileAttributes);
            }else if( reply instanceof PoolMgrSelectWritePoolMsg ){
                poolMessage =
                        new PoolAcceptFileMessage(
                                pool,
                                _protocolInfo ,
                                _fileAttributes);
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
            if( _ioQueueName != null ) {
                poolMessage.setIoQueueName(_ioQueueName);
            }
            if( _ioQueueAllowOverwrite &&
                ( _ioHandlerQueue != null     ) &&
                ( _ioHandlerQueue.length() > 0 )    ) {
                poolMessage.setIoQueueName(_ioHandlerQueue);
            }


            if( _poolRequestDone ){
                _log.debug("Ignoring double message");
                return ;
            }
            try{
                CellPath toPool;
                CellAddressCore poolAddress = reply.getPoolAddress();
                if( _poolProxy == null ){
                    toPool = new CellPath(poolAddress);
                }else{
                    toPool = new CellPath(_poolProxy);
                    toPool.add(poolAddress);
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
                if (reply.getReturnCode() == CacheException.FILE_NOT_IN_REPOSITORY) {
                    try {
                        again(true);
                        return;
                    } catch (NoRouteToCellException e) {
                        _log.error("No route to {}", e.getDestinationPath());
                    }
                }

                sendReply("poolIoFileArrived", reply);
                removeUs();
                return;
            }
            _moverId = reply.getMoverId();
            //
            // nothing to do here ( we are still waiting for
            //   doorTransferFinished )
            //
            setStatus( "WaitingForDoorTransferOk" ) ;
        }

        public void poolPassiveIoFileMessage( PoolPassiveIoFileMessage<byte[]> reply) {

            InetSocketAddress poolSocketAddress = reply.socketAddress();

            StringBuilder sb = new StringBuilder() ;
            sb.append(_sessionId).append(" ").
            append(_commandId).append(" ").
            append(_vargs.getName()).
            append(" connect ").append(poolSocketAddress.getHostName() ).
            append(" ").append(poolSocketAddress.getPort() ).append(" ").
            append(Base64.byteArrayToBase64(reply.challange()) );

            println( sb.toString() ) ;
            setStatus( "WaitingForDoorTransferOk" ) ;

        }

        // FIXME: sleep with lock
        public synchronized void
        doorTransferArrived( DoorTransferFinishedMessage reply ){

            if( reply.getReturnCode() == 0 ){

                long filesize = reply.getFileAttributes().getSize() ;
                _log.info("doorTransferArrived : fs={};strict={};m={}", filesize, _strictSize, _ioMode);
                if( _strictSize && ( filesize > 0L ) && (_ioMode.contains("w")) ){

                    for( int count = 0 ; count < 10 ; count++  ){
                        try{
                            long fs = _pnfs.getFileAttributes(_fileAttributes.getPnfsId(), EnumSet.of(SIZE)).getSize();
                            _log.info("doorTransferArrived : Size of {}: {}",
                                      _fileAttributes.getPnfsId(), fs);
                            if( fs > 0L ) {
                                break;
                            }
                        }catch(Exception ee ){
                            _log.error("Problem getting storage info (check) for {}: {}", _fileAttributes.getPnfsId(), ee);
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

        private DCapProtocolInfo _protocolInfo;
        private String           _pool         = "dirLookupPool" ;

        private OpenDirHandler(int sessionId, int commandId, VspArgs args)
        {
            super( sessionId , commandId , args , true, true ) ;

            int   port    = Integer.parseInt( _vargs.argv(2) ) ;

            StringTokenizer st = new StringTokenizer( _vargs.argv(1) , "," ) ;
            InetSocketAddress clientSocketAddress = new InetSocketAddress(st.nextToken(), port);            //
            //
            _protocolInfo = new DCapProtocolInfo( "DCap",3,0, clientSocketAddress  ) ;
            _protocolInfo.setSessionId( _sessionId ) ;
            String pool = args.getOpt("lookupPool");
            if( pool != null ) {
                _pool = pool;
            }

        }

        @Override
        protected void askForFileAttributes()
            throws IllegalArgumentException, NoRouteToCellException
        {
            setTimer(60 * 1000);

            try {
                PnfsId pnfsId = new PnfsId(_vargs.argv(0));
                _message = new PnfsGetFileAttributes(pnfsId, _attributes);
            } catch (IllegalArgumentException e) {
                /* Seems not to be a pnfsId, might be a url.
                 */
                DCapUrl url = new DCapUrl(_vargs.argv(0));
                String fileName = url.getFilePart();
                _message = new PnfsGetFileAttributes(fileName, _attributes);
                _path = fileName;
            }

            _log.debug("Requesting file attributes for {}", _message);

            _message.setAccessMask(EnumSet.of(AccessMask.LIST_DIRECTORY));
            _message.setId(_sessionId);
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

        @Override
        public void fileAttributesAvailable(){
            //
            // we are not called if the pnfs request failed.
            //

            String path = _message.getPnfsPath();

            if( _fileAttributes.getFileType() != DIR) {
                sendReply( "fileAttributesAvailable" , 22, path +" is not a directory", "ENOTDIR" ) ;
                removeUs()  ;
                return ;
            }

            PoolIoFileMessage poolIoFileMessage = new PoolIoFileMessage(_pool,_fileAttributes.getPnfsId(), _protocolInfo);

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
        _log.debug(problem);
        return problem;
    }

    private String protocolViolation(int sessionId, int commandId, String name,
            int errorCode, String errorMessage) {
        String problem= String.format("%d %d %s failed %d \"protocolViolation : %s\"",
                sessionId, commandId, name, errorCode, errorMessage);
        _log.debug(problem);
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

    @Override
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

    @Override
    public void  messageArrived(CellMessage msg)
    {
        Object object = msg.getMessageObject();

        if (!(object instanceof Message)) {
            _log.warn("Unexpected message class {} source = {}",
                      object.getClass(), msg.getSourcePath());
            return;
        }

        Message reply = (Message) object;
        SessionHandler handler = _sessions.get((int) reply.getId());
        if (handler == null) {
            _log.info("Reply ({}) for obsolete session: {}", reply.getClass(), reply.getId());
            return;
        }

        if( reply instanceof DoorTransferFinishedMessage ){

            ((IoHandler)handler).doorTransferArrived( (DoorTransferFinishedMessage)reply )  ;

        }else if( reply instanceof PoolMgrSelectPoolMsg ){

            ((IoHandler)handler).poolMgrSelectPoolArrived( (PoolMgrSelectPoolMsg)reply )  ;

        }else if( reply instanceof PnfsGetFileAttributes ){

            ((PnfsSessionHandler)handler).pnfsGetFileAttributesArrived( (PnfsGetFileAttributes)reply )  ;

        }else if( reply instanceof PoolIoFileMessage ){

            ((IoHandler)handler).poolIoFileArrived( (PoolIoFileMessage)reply )  ;

        }else if( reply instanceof PoolPassiveIoFileMessage ){

            ((IoHandler)handler).poolPassiveIoFileMessage( (PoolPassiveIoFileMessage<byte[]>) reply )  ;

        } else {
            _log.warn("Unexpected message class {} source = {}",
                      object.getClass(), msg.getSourcePath());
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
