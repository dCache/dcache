/*
 * GssFtpDoorV1.java
 *
 * Created on Sep 24, 2003, 9:53 AM
 */

package diskCacheV111.doors;

//cells
import dmg.cells.nucleus.CellVersion;
import dmg.util.StreamEngine;
import dmg.util.Args;

//dcache
import diskCacheV111.util.Base64;
import diskCacheV111.util.KAuthFile;
import org.dcache.auth.*;
import gplazma.authz.AuthorizationException;
import diskCacheV111.services.acl.GrantAllPermissionHandler;

//java
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Iterator;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import javax.security.auth.Subject;

//jgss
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.ChannelBinding;
import org.ietf.jgss.GSSName;
import org.ietf.jgss.MessageProp;
/**
 *
 * @author  timur
 */
public abstract class GssFtpDoorV1 extends AbstractFtpDoorV1
{
    public static final String GLOBUS_URL_COPY_DEFAULT_USER =
        ":globus-mapping:";

    protected GSSName _gssIdentity;
    // GSS general
    protected String _gssFlavor;

    // GSS GSI context and others
    protected GSSContext _serviceContext;

    // For multiple attribute support
    //protected AuthenticationMessage authmessage;

    protected Iterator<GroupList> _userAuthGroupLists;

    /** Creates a new instance of GsiFtpDoorV1 */
    public GssFtpDoorV1(String name, StreamEngine engine, Args args)
        throws InterruptedException, ExecutionException
    {
        super(name,engine,args);
    }

    @Override
    protected void init()
        throws Exception
    {
        super.init();
        _gssFlavor = "unknown";
    }

    protected void secure_reply(String answer, String code) {
        answer = answer+"\r\n";
        byte[] data = answer.getBytes();
        MessageProp prop = new MessageProp(0, false);
        try{
            data = _serviceContext.wrap(data, 0, data.length, prop);
        } catch ( GSSException e ) {
            println("500 Reply encryption error: " + e);
            return;
        }
        println(code + " " + Base64.byteArrayToBase64(data));
    }

    public void ac_auth(String arg) {
        info("GssFtpDoorV1::secure_reply: going to authorize using " + _gssFlavor);
        if ( !arg.equals("GSSAPI") ) {
            reply("504 Authenticating method not supported");
            return;
        }
        if (_serviceContext != null && _serviceContext.isEstablished()) {
            reply("234 Already authenticated");
            return;
        }

        try {
            _serviceContext = getServiceContext();
        } catch( Exception e ) {
            error(e.toString());
            reply("500 Error: " + e.toString());
            return;
        }
        reply("334 ADAT must follow");
    }

    public void ac_adat(String arg) {
        if ( arg == null || arg.length() <= 0 ) {
            reply("501 ADAT must have data");
            return;
        }

        if ( _serviceContext == null ) {
            reply("503 Send AUTH first");
            return;
        }
        byte[] token = Base64.base64ToByteArray(arg);
        ChannelBinding cb;
        try {
            cb = new ChannelBinding(_engine.getInetAddress(),
            InetAddress.getLocalHost(), null);
        } catch( UnknownHostException e ) {
            reply("500 Can not determine address of local host: " + e);
            return;
        }

        try {
            //_serviceContext.setChannelBinding(cb);
            //debug("GssFtpDoorV1::ac_adat: CB set");
            token = _serviceContext.acceptSecContext(token, 0, token.length);
            //debug("GssFtpDoorV1::ac_adat: Token created");
            _gssIdentity = _serviceContext.getSrcName();
            //debug("GssFtpDoorV1::ac_adat: User principal: " + UserPrincipal);
        } catch( Exception e ) {
            _logger.error("GssFtpDoorV1::ac_adat: got service context exception", e);
            reply("535 Authentication failed: " + e);
            return;
        }
        if (token != null) {
            if (!_serviceContext.isEstablished()) {
                reply("335 ADAT="+Base64.byteArrayToBase64(token));
            }
            else {
                reply("235 ADAT="+Base64.byteArrayToBase64(token));
            }
        }
        else {
            if (!_serviceContext.isEstablished()) {
                reply("335 ADAT=");
            }
            else {
                info("GssFtpDoorV1::ac_adat: security context established " +
                     "with " + _gssIdentity);
                reply("235 OK");
            }
        }
    }

    public void secure_command(String answer, String sectype)
    throws dmg.util.CommandExitException {
        if ( answer == null || answer.length() <= 0 ) {
            reply("500 Wrong syntax of "+sectype+" command");
            return;
        }

        if ( _serviceContext == null || !_serviceContext.isEstablished()) {
            reply("503 Security context is not established");
            return;
        }


        byte[] data = Base64.base64ToByteArray(answer);
        MessageProp prop = new MessageProp(0, false);
        try {
            data = _serviceContext.unwrap(data, 0, data.length, prop);
        } catch( GSSException e ) {
            reply("500 Can not decrypt command: " + e);
            error("GssFtpDoorV1::secure_command: got GSSException: " +
                   e.getMessage());
            return;
        }

        // At least one C-based client sends a zero byte at the end
        // of a secured command. Truncate trailing zeros.
        // Search from the right end of the string for a non-null character.
        int i;
        for(i = data.length;i > 0 && data[i-1] == 0 ;i--) {
            //do nothing, just decrement i
        }
        String msg = new String(data, 0, i);
        msg = msg.trim();

        if ( msg.equalsIgnoreCase("CCC") ) {
            _gReplyType = "clear";
            reply("200 OK");
        }
        else {
            _gReplyType = sectype;
            ftpcommand(msg);
        }

    }

    public static CellVersion getStaticCellVersion() {
        return new CellVersion(diskCacheV111.util.Version.getVersion(),
                               "$Revision: 1.18 $" );
    }

    public void ac_user(String arg) {

        KAuthFile authf;

        _pwdRecord = null;
        authRecord = null;
        _user = null;
        info("GssFtpDoorV1::ac_user <" + arg + ">");
        if (arg.equals("")) {
            reply(err("USER",arg));
            return;
        }

        if (_serviceContext == null || !_serviceContext.isEstablished()) {
            reply("530 Authentication required");
            return;
        }

        _user = arg;
        _dnUser = _gssIdentity.toString();
        if( _use_gplazmaAuthzCell || _use_gplazmaAuthzModule ) {
            try {
                authRecord = authorize();
            } catch(AuthorizationException ae) {
                error(ae);
                reply("530 User Authorization failed: " + ae.getMessage());
                return;

            }
        } else {
            try {
                authf = new KAuthFile(_kpwdFilePath);
            } catch( Exception e ) {
                reply("530 User authentication file not found: " + e);
                return;
            }

            if (_user.equals(GLOBUS_URL_COPY_DEFAULT_USER)) {
                info("GssFtpDoorV1::ac_user: non-gplazma special case, user is " + _user);
                _user = authf.getIdMapping(_gssIdentity.toString() );
                if (_user == null) {
                    reply("530 User Name for GSI Identity" +
                    _gssIdentity.toString() + " not found.");
                    return;
                }
            }
            _pwdRecord = authf.getUserRecord(_user);

            if ( _pwdRecord == null ) {
                reply("530 User " + _user + " not found.");
                return;
            }
            debug(_user+" has record "+_pwdRecord);

            info("GssFtpDoorV1::ac_user: looking up: " +
                 _gssIdentity.toString());
            if ( !((UserAuthRecord)_pwdRecord).hasSecureIdentity(_gssIdentity.toString()) ) {
                _pwdRecord = null;
                reply("530 Permission denied");
                return;
            }
            _pathRoot = _pwdRecord.Root;
            _curDirV = _pwdRecord.Home;
        }

        if (_pwdRecord == null && authRecord == null) {
            reply("530 Permission denied");
            return;
        }

        resetPwdRecord();

        reply("200 User "+_user+" logged in");
    }

    public void resetPwdRecord()
    {
        if (authRecord != null) {
            Set<GroupList> uniqueGroupListSet = new LinkedHashSet<GroupList>(
                    authRecord.getGroupLists());
            _userAuthGroupLists = uniqueGroupListSet.iterator();
            setNextPwdRecord();
        } else {
            _userAuthGroupLists = null;
            setSubjectForPnfsHandler(Subjects.NOBODY);
        }
    }

    protected void setSubjectForPnfsHandler(Subject subject)
    {
        if (_permissionHandler instanceof GrantAllPermissionHandler) {
            _pnfs.setSubject(subject);
        }
    }

    protected boolean setNextPwdRecord()
    {
        if (_userAuthGroupLists == null || !_userAuthGroupLists.hasNext()) {
            setSubjectForPnfsHandler(Subjects.NOBODY);
            _pwdRecord = null;
            return false;
        }

        GroupList grplist  = _userAuthGroupLists.next();
        _pwdRecord = grplist.getUserAuthRecord();
        _user = _pwdRecord.Username;

        if(_pathRoot == null) {
            _curDirV = _pwdRecord.Home;
            _pathRoot = _pwdRecord.Root;

            if (_curDirV == null || _curDirV.length() == 0 ) {
                _curDirV ="/";
            }

            if (_pathRoot == null || _pathRoot.length() == 0) {
                _pathRoot = "/";
            }
        }

        setSubjectForPnfsHandler(getSubject());

        return true;
    }

    // Some clients, even though the user is already logged in via GSS and ADAT,
    // will send a dummy PASS anyway. "Already logged in" is distracting
    // and the "Going to evaluate strong password" message is misleading
    // since nothing is actually done for this command.
    // Example = ubftp client
    public void ac_pass(String arg) {
        debug("GssFtpDoorV1::ac_pass: PASS is a no-op with " +
                "GSSAPI authentication.");
        if ( _pwdRecord != null || _gssIdentity != null ) {
            reply(ok("PASS"));
            return;
        }
        else {
            reply("500 Send USER first");
            return;
        }
    }


    /**
     * The concrete implementation of this method returns the GSSContext
     * specific to the particular security mechanism.
     */
    protected abstract GSSContext getServiceContext() throws GSSException;

    /**
     * Communicates with gPlazma in protocol and configuration  appropriate
     * manner in order to receiceve authorization
     * @return AuthorizationRecord obtained from gPlazma service
     */
    protected abstract AuthorizationRecord authorize()
            throws AuthorizationException ;
}
