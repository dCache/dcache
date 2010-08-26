// $Id: KerberosFtpDoorV1.java,v 1.12 2007-10-26 08:19:34 behrmann Exp $
// $Log: not supported by cvs2svn $
// Revision 1.11  2007/10/25 22:51:10  timur
// make doors/KerberosFtpDoorV1.java compile after recent Gerd's changes
//
// Revision 1.10  2005/05/19 05:55:43  timur
// added support for monitoring door state via dcache pages
//
// Revision 1.9  2004/09/09 20:27:37  timur
// made ftp transaction logging optional
//
// Revision 1.8  2004/09/09 18:39:40  timur
// added the uid,gid to the user names in FTP logs
//
// Revision 1.7  2004/09/08 21:25:43  timur
// remote gsiftp transfer manager will now use ftp logger too, fixed ftp door logging problem
//
// Revision 1.6  2004/08/19 18:22:28  timur
// gridftp door gives pool a host name instead of address, reformated code
//
// Revision 1.5  2003/09/25 16:52:05  cvs
// use globus java cog kit gsi gss library instead of gsint
//
// Revision 1.4  2003/09/02 21:06:20  cvs
// removed logging of security info by kftp and gsiftp doors, changest in srm
//
// Revision 1.3  2003/05/12 19:26:19  cvs
// create worker threads from sublclasses
//
// Revision 1.2  2003/05/07 17:44:24  cvs
// new ftp doors are ready
//
// Revision 1.1  2003/05/06 22:10:48  cvs
// new ftp door classes structure
//
/*
 * KerberosFtpDoorV1.java
 *
 * Created on May 6, 2003, 3:05 PM
 */

package diskCacheV111.doors;

//java util
import java.util.StringTokenizer;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import javax.security.auth.Subject;

//java net
import java.net.InetAddress;
import java.net.UnknownHostException;

//cells
import dmg.util.StreamEngine;
import dmg.util.Args;
import dmg.cells.nucleus.CellPath;

//jgss
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.Oid;
import org.ietf.jgss.ChannelBinding;
import org.ietf.jgss.GSSName;

import diskCacheV111.services.acl.GrantAllPermissionHandler;
import diskCacheV111.util.KAuthFile;
import org.dcache.auth.UserAuthRecord;
import org.dcache.auth.AuthzQueryHelper;
import org.dcache.auth.RecordConvert;
import org.dcache.auth.Subjects;
import gplazma.authz.AuthorizationController;
import java.util.List;
import java.util.Collections;

/**
 *
 * @author  timur
 */
public class KerberosFtpDoorV1 extends GssFtpDoorV1 {

    private String MyPrincipalStr;
// TODO: the str1 variable looks like it was once some sort of debug aid
//       but it seems like it should be removed...
    private String str1;
    private String[] KDCList;

    /** Creates a new instance of KerberosFtpDoorV1 */
    public KerberosFtpDoorV1(String name, StreamEngine engine, Args args)
        throws InterruptedException, ExecutionException
    {
        super(name,engine,args);
    }

    @Override
    protected void init()
        throws Exception
    {
        super.init();

        Args args = getArgs();
        if( ( MyPrincipalStr = args.getOpt("svc-principal") ) == null ){
            String problem = "KerberosFTPDoorV1: -svc-principal not specified";
            throw new IllegalArgumentException(problem);
        }
        info("KerberosFTPDoorV1: initializing kerberos ftp door service. " +
             "Principal is '" + MyPrincipalStr + "'");
        _GSSFlavor = "k5";
        str1 = MyPrincipalStr;
        debug("KerberosFTPDoorV1: str1 is " + str1);

        String kdclist;
        if( ( kdclist = args.getOpt("kdc-list") ) != null ) {
            StringTokenizer tokens = new StringTokenizer(kdclist, ",");
            int n = tokens.countTokens();
            KDCList = new String[n];
            for( int i = 0; i < n; i++ ) {
                KDCList[i] = tokens.nextToken();
                info("KerberosFTPDoorV1: kdc[" + i + "] = " + KDCList[i]);
            }
        }
        ftpDoorName="Kerberos FTP";
    }

    public void startTlog(String path, String action) {
        if (_tLog == null) {
            return;
        }
        try {
            String user_string = _user;
            if (_pwdRecord != null) {
                user_string += "("+_pwdRecord.UID+"."+_pwdRecord.GID+")";
            }
            _tLog.begin(user_string, "krbftp", action, path,
                        _engine.getInetAddress());
        }
        catch (Exception e) {
            error("KerberosFTPDoorV1::startTlog: couldn't start tLog. " +
                  "Ignoring exception: " + e.getMessage());
        }
    }

    protected GSSContext getServiceContext() throws GSSException {
        Oid krb5Mechanism = new Oid("1.2.840.113554.1.2.2");
        Oid krb5PrincipalNameType = new Oid("1.2.840.113554.1.2.2.1");
        int nretry = 10;
        GSSException error = null;
        Properties sysp = System.getProperties();
        GSSCredential MyCredential = null;

        GSSManager _GManager = GSSManager.getInstance();
        debug("KerberosFTPDoorV1::getServiceContext: calling " +
             "_GManager.createName(\"" + MyPrincipalStr + "\", null)");
        debug("KerberosFTPDoorV1::getServiceContext str1 is " + str1);
        GSSName MyPrincipal = _GManager.createName(MyPrincipalStr, null);
        info("KerberosFTPDoorV1::getServiceContext: principal=\"" +
             MyPrincipal + "\"");

        while( MyCredential == null && nretry-- > 0 ) {
            if( KDCList != null && KDCList.length > 0 ) {
                String kdc = KDCList[nretry % KDCList.length];
                sysp.put("java.security.krb5.kdc", kdc);
            }

            try {
                MyCredential = _GManager.createCredential(MyPrincipal,
                GSSCredential.DEFAULT_LIFETIME,
                krb5Mechanism,
                GSSCredential.ACCEPT_ONLY);
            }
            catch( GSSException e ) {
                debug("KerberosFTPDoorV1::getServiceContext: got exception " +
                      " while looking up credential: " + e.getMessage());
                error = e;
            }
        }
        if( MyCredential == null ) {
            throw error;
        }
        info("KerberosFTPDoorV1::getServiceContext: credential=\"" +
             MyCredential + "\"");
        GSSContext context = _GManager.createContext(MyCredential);

        try {
            ChannelBinding cb = new ChannelBinding(_engine.getInetAddress(),
                                                   InetAddress.getLocalHost(),
                                                   null);
            context.setChannelBinding(cb);
        }
        catch( UnknownHostException e ) {
            String errmsg = "KerberosFTPDoorV1::getServiceContext: can't " +
                            "bind channel to localhost:" + e.getMessage();
            error(errmsg);
            throw new GSSException(GSSException.NO_CRED, 0, errmsg);
        }

        return context;
    }

    @Override
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
                 List<String> emptyRolesList= Collections.emptyList();
                authRecord =  authHelper.getAuthorization(_dnUser,emptyRolesList,_user, new CellPath("gPlazma"), this).getAuthorizationRecord();
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
            List<String> emptyRolesList= Collections.emptyList();
            try {
                authRecord = RecordConvert.gPlazmaToAuthorizationRecord(authCtrl.authorize( _dnUser, emptyRolesList, null,
                    _user, null, null));
            } catch ( Exception e ) {
                reply("530 User Authorization record failed to be retrieved: " + e);
                return;
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

}
