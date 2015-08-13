/*
 * GsiFtpDoorV1.java
 *
 * Created on May 6, 2003, 3:06 PM
 */

package diskCacheV111.doors;

import org.globus.gsi.CredentialException;
import org.globus.gsi.GSIConstants;
import org.globus.gsi.X509Credential;
import org.globus.gsi.gssapi.GSSConstants;
import org.globus.gsi.gssapi.GlobusGSSCredentialImpl;
import org.gridforum.jgss.ExtendedGSSContext;
import org.gridforum.jgss.ExtendedGSSManager;
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSManager;

import javax.security.auth.Subject;

import java.io.IOException;
import java.security.cert.X509Certificate;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.PermissionDeniedCacheException;

import dmg.util.Args;
import dmg.util.StreamEngine;

import org.dcache.auth.LoginNamePrincipal;
import org.dcache.auth.Subjects;
import org.dcache.cells.Option;
import org.dcache.util.Crypto;

/**
 *
 * @author  timur
 */
public class GsiFtpDoorV1 extends GssFtpDoorV1
{
    @Option(
        name="service-key",
        required=true
    )
    protected String service_key;

    @Option(
        name="service-cert",
        required=true
    )
    protected String service_cert;

    @Option(
        name="service-trusted-certs",
        required=true
    )
    protected String service_trusted_certs;

    @Option(
            name="gridftp.security.ciphers",
            required=true
    )
    protected String cipherFlags;

    private String _user;

    /** Creates a new instance of GsiFtpDoorV1 */
    public GsiFtpDoorV1(String name, StreamEngine engine, Args args)
    {
        super(name,engine,args);
    }

    @Override
    protected void init()
        throws Exception
    {
        super.init();

        _gssFlavor = "gsi";

        ftpDoorName="GSI FTP";
    }

    @Override
    public void startTlog(FTPTransactionLog tlog, String path, String action) {
        if (_subject != null) {
            try {
                String user =
                    _user + "("+Subjects.getUid(_subject) + "." + Subjects.getPrimaryGid(_subject) + ")";
                tlog.begin(user, "gsiftp", action, path,
                           _engine.getInetAddress());
            }
            catch (Exception e) {
                error("GsiFtpDoor: couldn't start tLog. " +
                      "Ignoring exception: " + e.getMessage());
            }
        }
    }

    @Override
    protected GSSContext getServiceContext() throws GSSException {

        X509Credential serviceCredential;
        try {
            serviceCredential = new X509Credential(service_cert, service_key);
        }
        catch (CredentialException gce) {
            String errmsg = "GsiFtpDoor: couldn't load " +
                            "host globus credentials: " + gce.toString();
            error(errmsg);
            throw new GSSException(GSSException.NO_CRED, 0, errmsg);
        }
        catch(IOException ioe) {
            throw new GSSException(GSSException.NO_CRED, 0,
                                   "could not load host globus credentials "+ioe.toString());
        }

        GSSCredential cred = new GlobusGSSCredentialImpl(serviceCredential,
                                                    GSSCredential.ACCEPT_ONLY);
        GSSManager manager = ExtendedGSSManager.getInstance();
        ExtendedGSSContext context =
                               (ExtendedGSSContext)manager.createContext(cred);

        context.setOption(GSSConstants.GSS_MODE, GSIConstants.MODE_GSI);
        context.setBannedCiphers(Crypto.getBannedCipherSuitesFromConfigurationValue(cipherFlags));

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
        try {
            subject.getPrincipals().add(_origin);

            if (!arg.equals(GLOBUS_URL_COPY_DEFAULT_USER)) {
                subject.getPrincipals().add(new LoginNamePrincipal(arg));
            }

            if (!(_serviceContext instanceof ExtendedGSSContext)) {
                throw new RuntimeException("GSSContext not instance of ExtendedGSSContext");
            }

            ExtendedGSSContext extendedcontext =
                (ExtendedGSSContext) _serviceContext;
            X509Certificate[] chain =
                (X509Certificate[]) extendedcontext.inquireByOid(GSSConstants.X509_CERT_CHAIN);
            subject.getPublicCredentials().add(chain);

            login(subject);

            _user = arg;

            reply("200 User " + arg + " logged in");
        } catch (GSSException e) {
            error("Failed to extract X509 chain: " + e);
            println("530 Login failed: " + e.getMessage());
        } catch (PermissionDeniedCacheException e) {
            warn("Login denied for " + subject + ": " + e);
            println("530 Login incorrect");
        } catch (CacheException e) {
            error("Login failed for " + subject + ": " + e);
            println("530 Login failed: " + e.getMessage());
        }
    }
}
