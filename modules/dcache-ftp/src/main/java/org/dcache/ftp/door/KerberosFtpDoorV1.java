package org.dcache.ftp.door;

import org.ietf.jgss.ChannelBinding;
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSName;
import org.ietf.jgss.Oid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Properties;

import diskCacheV111.doors.FTPTransactionLog;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.PermissionDeniedCacheException;

import org.dcache.auth.LoginNamePrincipal;
import org.dcache.auth.Subjects;
import org.dcache.cells.Option;

/**
 *
 * @author  timur
 */
public class KerberosFtpDoorV1 extends GssFtpDoorV1
{

    private static final Logger LOGGER = LoggerFactory.getLogger(KerberosFtpDoorV1.class);


    @Option(name = "svc-principal",
            required = true)
    private String _myPrincipalStr;

    @Option(name = "kdc-list")
    private String _kdcListOption;

    private String[] _kdcList;


    @Override
    public void init()
    {
        _gssFlavor = "k5";
        ftpDoorName = "Kerberos FTP";
        if (_kdcListOption != null) {
            _kdcList = _kdcListOption.split(",");
        }
        super.init();
    }

    @Override
    public void startTlog(FTPTransactionLog tlog, String path, String action) {
        if (_subject != null) {
            try {
                String user =
                    Subjects.getUserName(_subject) + "("+Subjects.getUid(_subject) + "." + Subjects.getPrimaryGid(_subject) + ")";
                tlog.begin(user, "krbftp", action, path, _remoteAddress.getAddress());
            }
            catch (Exception e) {
                LOGGER.error("KerberosFTPDoorV1::startTlog: couldn't start tLog. " +
                        "Ignoring exception: {}", e.getMessage());
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
        GSSCredential myCredential = null;

        GSSManager _GManager = GSSManager.getInstance();
        LOGGER.debug("KerberosFTPDoorV1::getServiceContext: calling " +
             "_GManager.createName(\"{}\", null)", _myPrincipalStr);
        GSSName myPrincipal = _GManager.createName(_myPrincipalStr, null);
        LOGGER.info("KerberosFTPDoorV1::getServiceContext: principal=\"{}\"", myPrincipal);

        while( myCredential == null && nretry-- > 0 ) {
            if( _kdcList != null && _kdcList.length > 0 ) {
                String kdc = _kdcList[nretry % _kdcList.length];
                sysp.put("java.security.krb5.kdc", kdc);
            }

            try {
                myCredential = _GManager.createCredential(myPrincipal,
                GSSCredential.DEFAULT_LIFETIME,
                krb5Mechanism,
                GSSCredential.ACCEPT_ONLY);
            }
            catch( GSSException e ) {
                LOGGER.debug("KerberosFTPDoorV1::getServiceContext: got exception " +
                      " while looking up credential: {}", e.getMessage());
                error = e;
            }
        }
        if( myCredential == null ) {
            throw error;
        }
        LOGGER.info("KerberosFTPDoorV1::getServiceContext: credential=\"{}\"", myCredential);
        GSSContext context = _GManager.createContext(myCredential);

        try {
            ChannelBinding cb = new ChannelBinding(_remoteAddress.getAddress(),
                                                   InetAddress.getLocalHost(),
                                                   null);
            context.setChannelBinding(cb);
        }
        catch( UnknownHostException e ) {
            String errmsg = "KerberosFTPDoorV1::getServiceContext: can't " +
                            "bind channel to localhost:" + e.getMessage();
            LOGGER.error(errmsg);
            throw new GSSException(GSSException.NO_CRED, 0, errmsg);
        }

        return context;
    }

    @Override
    public void ftp_user(String arg)
    {
        if (arg.equals("")) {
            reply(err("USER",arg));
            return;
        }

        if (_serviceContext == null || !_serviceContext.isEstablished()) {
            reply("530 Authentication required");
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
            LOGGER.warn("Login denied for {}", subject);
            reply("530 Login denied");
        } catch (CacheException e) {
            LOGGER.error("Login failed for {}: {}", subject, e.getMessage());
            reply("530 Login failed: " + e.getMessage());
        }
    }
}
