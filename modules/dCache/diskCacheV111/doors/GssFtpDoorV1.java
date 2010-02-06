// $Id: GssFtpDoorV1.java,v 1.18 2007-10-29 13:29:24 behrmann Exp $
// $Log: not supported by cvs2svn $
// Revision 1.17  2007/10/25 20:02:42  behrmann
// Made all fields conform to normal dCache naming policy.
//
// Revision 1.16  2007/10/25 18:53:06  behrmann
// Made the code more DRY by unifying argument parsing.
//
// Added more documentation to fields.
//
// Revision 1.15  2007/03/27 19:20:27  tdh
// Merge of support for multiple attributes from 1.7.1.
//
// Revision 1.14  2006/12/15 16:08:44  tdh
// Added code to make delegation from cell to gPlazma optional, through the batch file parameter "delegate-to-gplazma". Default is to not delegate.
//
// Revision 1.13  2006/11/12 15:53:18  tigran
// pass cell to gPlazma
// if cell is not given, then System.out.println is used
//
// Revision 1.12  2006/08/07 20:16:42  tdh
// Do not reply 530 if gPlazma cell fails but direct call to gplazma modules are still to be made.
//
// Revision 1.11  2006/07/25 16:05:40  tdh
// Make message to gPlazma cell independent of gPlazma domain.
//
// Revision 1.10  2006/07/03 19:56:50  tdh
// Added code to throw and/or catch AuthenticationServiceExceptions from GPLAZMA cell.
//
// Revision 1.9  2006/06/29 20:25:30  tdh
// Changed hard-coded path of gplazma cell to gPlazma@gPlazmaDomain.
//
// Revision 1.8  2006/06/13 17:15:09  tdh
// Changed logic of ac_user to use gplazma cell for authentification if specified.
//
// Revision 1.7  2005/11/22 10:59:30  patrick
// Versioning enabled.
//
// Revision 1.6  2005/09/14 17:18:20  kennedy
// Do not say <user already logged in> when dummy PASS sent by client
//
// Revision 1.5  2005/09/14 14:12:15  tigran
// fixed copy/paste error
// added _dnUser for GSS/GSI
//
// Revision 1.4  2005/05/20 16:51:32  timur
// adding optional usage of vo authorization module
//
// Revision 1.3  2004/09/08 21:25:43  timur
// remote gsiftp transfer manager will now use ftp logger too, fixed ftp door logging problem
//
// Revision 1.2  2004/08/19 18:22:28  timur
// gridftp door gives pool a host name instead of address, reformated code
//
// Revision 1.1  2003/09/25 16:52:42  cvs
// use globus java cog kit gsi gss library instead of gsint
//
//
// Revision 1.1  2003/05/06 22:10:48  cvs
// new ftp door classes structure
//
/*
 * GssFtpDoorV1.java
 *
 * Created on Sep 24, 2003, 9:53 AM
 */

package diskCacheV111.doors;

//cells
import dmg.cells.nucleus.CellVersion;
import dmg.cells.nucleus.CellPath;
import dmg.util.StreamEngine;
import dmg.util.Args;

//dcache
import diskCacheV111.util.Base64;
import diskCacheV111.util.KAuthFile;
import diskCacheV111.util.FQAN;
import org.dcache.auth.*;
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

import gplazma.authz.AuthorizationController;


/**
 *
 * @author  timur
 */
public abstract class GssFtpDoorV1 extends AbstractFtpDoorV1
{
    public static final String GLOBUS_URL_COPY_DEFAULT_USER =
        ":globus-mapping:";

    protected GSSName GSSIdentity;
    // GSS general
    protected String _GSSFlavor;

    // GSS GSI context and others
    protected GSSContext serviceContext;

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
        _GSSFlavor = "unknown";
    }

    protected void secure_reply(String answer, String code) {
        answer = answer+"\r\n";
        byte[] data = answer.getBytes();
        MessageProp prop = new MessageProp(0, false);
        try{
            data = serviceContext.wrap(data, 0, data.length, prop);
        } catch ( GSSException e ) {
            println("500 Reply encryption error: " + e);
            return;
        }
        println(code + " " + Base64.byteArrayToBase64(data));
    }

    public void ac_auth(String arg) {
        info("GssFtpDoorV1::secure_reply: going to authorize " + _GSSFlavor);
        if ( !arg.equals("GSSAPI") ) {
            reply("504 Authenticating method not supported");
            return;
        }
        if (serviceContext != null && serviceContext.isEstablished()) {
            reply("234 Already authenticated");
            return;
        }

        try {
            serviceContext = getServiceContext();
        } catch( Exception e ) {
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

        if ( serviceContext == null ) {
            reply("503 Send AUTH first");
            return;
        }
        byte[] token = Base64.base64ToByteArray(arg);
        ChannelBinding cb;
        try {
            cb = new ChannelBinding(_engine.getInetAddress(),
            InetAddress.getLocalHost(), null);
            //debug("GssFtpDoorV1::ac_adat: Local address: " + InetAddress.getLocalHost());
            //debug("GssFtpDoorV1::ac_adat: Client address: " + _engine.getInetAddress());
        } catch( UnknownHostException e ) {
            reply("500 Can not determine address of local host: " + e);
            return;
        }

        try {
            //serviceContext.setChannelBinding(cb);
            //debug("GssFtpDoorV1::ac_adat: CB set");
            token = serviceContext.acceptSecContext(token, 0, token.length);
            //debug("GssFtpDoorV1::ac_adat: Token created");
            GSSIdentity = serviceContext.getSrcName();
            //debug("GssFtpDoorV1::ac_adat: User principal: " + UserPrincipal);
        } catch( Exception e ) {
            _logger.error("GssFtpDoorV1::ac_adat: got service context exception", e);
            reply("535 Authentication failed: " + e);
            return;
        }
        if (token != null) {
            if (!serviceContext.isEstablished()) {
                reply("335 ADAT="+Base64.byteArrayToBase64(token));
            }
            else {
                reply("235 ADAT="+Base64.byteArrayToBase64(token));
            }
        }
        else {
            if (!serviceContext.isEstablished()) {
                reply("335 ADAT=");
            }
            else {
                info("GssFtpDoorV1::ac_adat: security context established " +
                     "with " + GSSIdentity);
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

        if ( serviceContext == null || !serviceContext.isEstablished()) {
            reply("503 Security context is not established");
            return;
        }


        byte[] data = Base64.base64ToByteArray(answer);
        MessageProp prop = new MessageProp(0, false);
        try {
            data = serviceContext.unwrap(data, 0, data.length, prop);
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

        if (serviceContext == null || !serviceContext.isEstablished()) {
            reply("530 Authentication required");
            return;
        }

        _user = arg;
        _dnUser = GSSIdentity.toString();
        if (!_use_gplazmaAuthzCell && !_use_gplazmaAuthzModule) {
            try {
                authf = new KAuthFile(_kpwdFilePath);
            } catch( Exception e ) {
                reply("530 User authentication file not found: " + e);
                return;
            }

            if (_user.equals(GLOBUS_URL_COPY_DEFAULT_USER)) {
                info("GssFtpDoorV1::ac_user: non-gplazma special case, user is " + _user);
                _user = authf.getIdMapping(GSSIdentity.toString() );
                if (_user == null) {
                    reply("530 User Name for GSI Identity" +
                    GSSIdentity.toString() + " not found.");
                    return;
                }
            }

            _pwdRecord = authf.getUserRecord(_user);

            if ( _pwdRecord == null ) {
                reply("530 User " + _user + " not found.");
                return;
            }

            info("GssFtpDoorV1::ac_user: looking up: " +
                 GSSIdentity.toString());
            if ( !((UserAuthRecord)_pwdRecord).hasSecureIdentity(GSSIdentity.toString()) ) {
                _pwdRecord = null;
                reply("530 Permission denied");
                return;
            }
            _pathRoot = _pwdRecord.Root;
            _curDirV = _pwdRecord.Home;
        }

        if (_use_gplazmaAuthzCell) {
            AuthzQueryHelper authHelper;
            try {
                authHelper = new AuthzQueryHelper(this);
                authHelper.setDelegateToGplazma(_delegate_to_gplazma);
                authRecord =  authHelper.getAuthorization(serviceContext, new CellPath("gPlazma"), this).getAuthorizationRecord();
            } catch( Exception e ) {
                error(e);
                if (!_use_gplazmaAuthzModule) {
                    reply("530 Authorization Service failed: " + e);
                }
                error("GssFtpDoorV1::ac_user: authorization through gPlazma " +
                      "cell failed: " + e.getMessage());
                authRecord = null;
            }
        }

        if (authRecord==null && _use_gplazmaAuthzModule) {
            AuthorizationController authCtrl;
            try {
                authCtrl = new AuthorizationController(_gplazmaPolicyFilePath);
                //authCrtl.setLoglevel();
            } catch (Exception e) {
                reply("530 Authorization Service failed to initialize: " + e);
                return;
            }
            if (_user.equals(GLOBUS_URL_COPY_DEFAULT_USER)) {
                info("GssFtpDoorV1::ac_user: gplazma special case, user is " + _user);
                try {
                    authRecord = RecordConvert.gPlazmaToAuthorizationRecord(authCtrl.authorize(serviceContext, null, null, null));
                } catch ( Exception e ) {
                    reply("530 User Authorization record failed to be retrieved: " + e);
                    return;
                }
            } else {
                try {
                    authRecord = RecordConvert.gPlazmaToAuthorizationRecord(authCtrl.authorize(serviceContext, _user, null, null));
                } catch ( Exception e ) {
                    reply("530 User Authorization record failed to be retrieved: " + e);
                    return;
                }
            }
        }

        if (_pwdRecord == null && authRecord == null) {
            reply("530 Permission denied");
            return;
        }

        if (_permissionHandler instanceof GrantAllPermissionHandler) {
            Subject subject;
            if(authRecord != null) {
                subject = Subjects.getSubject(authRecord);
            } else {
                subject = Subjects.getSubject(_pwdRecord, true);
            }
            subject.getPrincipals().add(_origin);
            subject.setReadOnly();
            _pnfs.setSubject(subject);
        }

        resetPwdRecord();

        reply("200 User "+_user+" logged in");
    }

    public void resetPwdRecord()
    {
        if (authRecord != null) {
            Set<GroupList> uniqueGroupListSet = new LinkedHashSet<GroupList>(authRecord.getGroupLists());
            _userAuthGroupLists = uniqueGroupListSet.iterator();
            setNextPwdRecord();
        } else {
            _userAuthGroupLists = null;
        }
    }

    protected boolean setNextPwdRecord()
    {
        if (_userAuthGroupLists == null || !_userAuthGroupLists.hasNext()) {
            _pwdRecord = null;
            return false;
        }

        GroupList grplist  = _userAuthGroupLists.next();
        String fqan = grplist.getAttribute();
        int i=0, glsize = grplist.getGroups().size();
        int GIDS[] = (glsize > 0) ? new int[glsize] : null;
        for(Group group : grplist.getGroups()) {
             GIDS[i++] = group.getGid();
        }
        _pwdRecord = new UserAuthRecord(
                authRecord.getIdentity(),
                authRecord.getName(),
                fqan,
                authRecord.isReadOnly(),
                authRecord.getPriority(),
                authRecord.getUid(),
                GIDS,
                authRecord.getHome(),
                authRecord.getRoot(),
                "/",
                new HashSet<String>());

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
        return true;
    }

    // Some clients, even though the user is already logged in via GSS and ADAT,
    // will send a dummy PASS anyway. "Already logged in" is distracting
    // and the "Going to evaluate strong password" message is misleading
    // since nothing is actually done for this command.
    // Example = ubftp client
    public void ac_pass(String arg) {
        debug("GssFtpDoorV1::ac_pass: PASS is a no-op with GSSAPI authentication.");
        if ( _pwdRecord != null || GSSIdentity != null ) {
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
}
