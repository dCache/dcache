/*
 * KerberosFtpDoorV1.java
 *
 * Created on May 6, 2003, 3:05 PM
 */

package diskCacheV111.doors;

//java util

import org.ietf.jgss.ChannelBinding;
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSName;
import org.ietf.jgss.Oid;

import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Properties;
import java.util.StringTokenizer;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.PermissionDeniedCacheException;

import dmg.util.Args;
import dmg.util.StreamEngine;

import org.dcache.auth.LoginNamePrincipal;
import org.dcache.auth.Subjects;

//java net
//cells
//jgss

/**
 *
 * @author  timur
 */
public class KerberosFtpDoorV1 extends GssFtpDoorV1 {

    private String _myPrincipalStr;
    private String[] _kdcList;

    /** Creates a new instance of KerberosFtpDoorV1 */
    public KerberosFtpDoorV1(String name, StreamEngine engine, Args args)
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

    @Override
    public void startTlog(FTPTransactionLog tlog, String path, String action) {
        if (_subject != null) {
            try {
                String user =
                    Subjects.getUserName(_subject) + "("+Subjects.getUid(_subject) + "." + Subjects.getPrimaryGid(_subject) + ")";
                tlog.begin(user, "krbftp", action, path,
                           _engine.getInetAddress());
            }
            catch (Exception e) {
                error("KerberosFTPDoorV1::startTlog: couldn't start tLog. " +
                      "Ignoring exception: " + e.getMessage());
            }
        }
    }

    @Override
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

    @Override
    public void ac_user(String arg)
    {
        if (arg.equals("")) {
            reply(err("USER",arg));
            return;
        }

        Subject subject = new Subject();
        subject.getPrincipals().add(new LoginNamePrincipal(arg));
        subject.getPrincipals().add(new KerberosPrincipal(_gssIdentity.toString()));
        subject.getPrincipals().add(_origin);

        try {
            login(subject);
            reply("200 User " + arg + " logged in");
        } catch (PermissionDeniedCacheException e) {
            warn("Login denied for " + subject);
            println("530 Login incorrect");
        } catch (CacheException e) {
            error("Login failed for " + subject + ": " + e);
            println("530 Login failed: " + e.getMessage());
        }
    }
}
