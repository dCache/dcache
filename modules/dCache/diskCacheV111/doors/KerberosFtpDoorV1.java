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

//java net
import java.net.InetAddress;
import java.net.UnknownHostException;

//cells
import dmg.util.StreamEngine;
import dmg.util.Args;

//jgss
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.Oid;
import org.ietf.jgss.ChannelBinding;
import org.ietf.jgss.GSSName;
import gplazma.authz.AuthorizationController;
import gplazma.authz.AuthorizationException;
import org.dcache.vehicles.AuthorizationMessage;
import org.dcache.auth.AuthorizationRecord;
import org.dcache.auth.AuthzQueryHelper;
import org.dcache.auth.RecordConvert;
import java.util.List;
import java.util.Collections;


/**
 *
 * @author  timur
 */
public class KerberosFtpDoorV1 extends GssFtpDoorV1 {

    private String _myPrincipalStr;
    private String[] _kdcList;

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
        if( ( _myPrincipalStr = args.getOpt("svc-principal") ) == null ){
            String problem = "KerberosFTPDoorV1: -svc-principal not specified";
            throw new IllegalArgumentException(problem);
        }
        info("KerberosFTPDoorV1: initializing kerberos ftp door service. " +
             "Principal is '" + _myPrincipalStr + "'");
        _gssFlavor = "k5";
        String kdclist;
        if( ( kdclist = args.getOpt("kdc-list") ) != null ) {
            StringTokenizer tokens = new StringTokenizer(kdclist, ",");
            int n = tokens.countTokens();
            _kdcList = new String[n];
            for( int i = 0; i < n; i++ ) {
                _kdcList[i] = tokens.nextToken();
                info("KerberosFTPDoorV1: kdc[" + i + "] = " + _kdcList[i]);
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
             "_GManager.createName(\"" + _myPrincipalStr + "\", null)");
        GSSName MyPrincipal = _GManager.createName(_myPrincipalStr, null);
        info("KerberosFTPDoorV1::getServiceContext: principal=\"" +
             MyPrincipal + "\"");

        while( MyCredential == null && nretry-- > 0 ) {
            if( _kdcList != null && _kdcList.length > 0 ) {
                String kdc = _kdcList[nretry % _kdcList.length];
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

    /**
     * Communicates with gPlazma in protocol and configuration  appropriate
     * manner in order to receiceve authorization
     * @return AuthorizationRecord obtained from gPlazma service
     */
    protected AuthorizationRecord authorize()
            throws AuthorizationException {
        if (_use_gplazmaAuthzCell) {
            AuthzQueryHelper authHelper  = new AuthzQueryHelper(this);
            List<String> emptyRolesList= Collections.emptyList();
            AuthorizationMessage authorizationMessage =
                    authHelper.getAuthorization(_dnUser,emptyRolesList, _user);
            return authorizationMessage.getAuthorizationRecord();
        } else  if ( _use_gplazmaAuthzModule) {
            AuthorizationController authCtrl =
                new AuthorizationController(_gplazmaPolicyFilePath);
            List<String> emptyRolesList= Collections.emptyList();
            return RecordConvert.gPlazmaToAuthorizationRecord(
                authCtrl.authorize(_dnUser, emptyRolesList,
                    null,_user, null,null));
        }  else {
            throw new AuthorizationException(
                    "_use_gplazmaAuthzCell is false and " +
                    "_use_gplazmaAuthzModule is false");
        }

    }
}
