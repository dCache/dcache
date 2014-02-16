package org.dcache.ftp.door;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;

import java.io.IOException;
import java.security.cert.CertPath;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;

import diskCacheV111.doors.FTPTransactionLog;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.PermissionDeniedCacheException;

import org.dcache.auth.LoginNamePrincipal;
import org.dcache.auth.Subjects;
import org.dcache.auth.util.X509Utils;
import org.dcache.cells.Option;
import org.dcache.gplazma.AuthenticationException;
import org.dcache.util.CertPaths;
import org.dcache.util.CertificateFactories;
import org.dcache.util.Crypto;

import static java.util.Arrays.asList;

/**
 *
 * @author  timur
 */
public class GsiFtpDoorV1 extends GssFtpDoorV1
{
    private static final Logger LOGGER = LoggerFactory.getLogger(GsiFtpDoorV1.class);

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

    private CertificateFactory cf;

    private String _user;

    @Override
    public void init()
    {
        _gssFlavor = "gsi";
        ftpDoorName="GSI FTP";
        cf = CertificateFactories.newX509CertificateFactory();
        super.init();
    }

    @Override
    public void startTlog(FTPTransactionLog tlog, String path, String action) {
        if (_subject != null) {
            try {
                String user =
                    _user + "("+Subjects.getUid(_subject) + "." + Subjects.getPrimaryGid(_subject) + ")";
                tlog.begin(user, "gsiftp", action, path, _remoteAddress.getAddress());
            }
            catch (Exception e) {
                LOGGER.error("GsiFtpDoor: couldn't start tLog. " +
                        "Ignoring exception: {}", e.getMessage());
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
            LOGGER.error(errmsg);
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
            subject.getPublicCredentials().add(cf.generateCertPath(asList(chain)));

            try {
                login(subject);
                _user = arg;
                reply("200 User " + arg + " logged in");
            } catch (PermissionDeniedCacheException e) {
                LOGGER.warn("Login denied for {}: {}",
                            X509Utils.getSubjectFromX509Chain(chain, false), e.getMessage());
                println("530 Login denied");
            } catch (CacheException e) {
                LOGGER.error("Login failed for {}: {}",
                             X509Utils.getSubjectFromX509Chain(chain, false), e.getMessage());
                println("530 Login failed: " + e.getMessage());
            }
        } catch (GSSException | CertificateException | AuthenticationException e) {
            LOGGER.error("Failed to extract X509 chain: {}", e.toString());
            reply("530 Login failed: " + e.getMessage());
        }
    }
}
