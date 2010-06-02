/*
 * GsiFtpDoorV1.java
 *
 * Created on May 6, 2003, 3:06 PM
 */

package diskCacheV111.doors;

import java.util.concurrent.ExecutionException;

//cells
import dmg.util.StreamEngine;
import dmg.util.Args;
import dmg.cells.nucleus.CellVersion;

//jgss
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSManager;

// globus gsi
import org.globus.gsi.GlobusCredential;
import org.globus.gsi.GlobusCredentialException;
import org.globus.gsi.gssapi.GlobusGSSCredentialImpl;
import org.globus.gsi.TrustedCertificates;
import org.gridforum.jgss.ExtendedGSSManager;
import org.gridforum.jgss.ExtendedGSSContext;
import org.globus.gsi.GSIConstants;
import org.globus.gsi.gssapi.GSSConstants;

import org.dcache.cells.Option;
import gplazma.authz.AuthorizationController;
import gplazma.authz.AuthorizationException;
import org.dcache.vehicles.AuthorizationMessage;
import org.dcache.auth.AuthorizationRecord;
import org.dcache.auth.AuthzQueryHelper;
import org.dcache.auth.RecordConvert;
import org.dcache.auth.Subjects;
import org.dcache.auth.UserNamePrincipal;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.PermissionDeniedCacheException;

import java.security.cert.X509Certificate;
import javax.security.auth.Subject;

/**
 *
 * @author  timur
 */
public class GsiFtpDoorV1 extends GssFtpDoorV1
{
    @Option(
        name="service-key",
        defaultValue="/etc/grid-security/hostkey.pem"
    )
    protected String service_key;

    @Option(
        name="service-cert",
        defaultValue="/etc/grid-security/hostcert.pem"
    )
    protected String service_cert;

    @Option(
        name="service-trusted-certs",
        defaultValue="/etc/grid-security/certificates"
    )
    protected String service_trusted_certs;

    private String _user;

    /** Creates a new instance of GsiFtpDoorV1 */
    public GsiFtpDoorV1(String name, StreamEngine engine, Args args)
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
        _gssFlavor = "gsi";

        // Grid FTP Performance Markers options:
        String arg_string = args.getOpt("usePerfMarkers");
        if (arg_string != null) {
            if ( arg_string.equalsIgnoreCase("true") )
                _perfMarkerConf.use = true;
            else if ( arg_string.equalsIgnoreCase("false") )
                _perfMarkerConf.use = false;
            else {
                String msg = "GsiFtpDoor: illegal command option value in " +
                             "usePerfMarkers='" + arg_string +
                             "'. It must be 'true' or 'false'.";
                fatal(msg);
                throw new RuntimeException(msg);
            }
        }

        arg_string = args.getOpt("perfMarkerPeriod");
        if ( ( arg_string != null ) && ( ! arg_string.equals("") ) ){
            try {
                int period = Integer.parseInt(arg_string) ;
                if (period <= 0) {
                    _perfMarkerConf.period = 0;
                    _perfMarkerConf.use    = false;
                } else {
                   _perfMarkerConf.period = period * 1000;
                   _perfMarkerConf.use    = true;
                }
            } catch (NumberFormatException ex) {
                String msg = "GsiFtpDoor: error in -perfMarkerPeriod " +
                             "argument: '" + arg_string +
                             "' is not an integer.";
                fatal(msg);
                throw new RuntimeException(msg);
            }
        }
        info("GsiFtpDoor: Performance Markers : " + _perfMarkerConf.use +
             " Period : " + _perfMarkerConf.period ) ;

        ftpDoorName="GSI FTP";
    }

    public static CellVersion getStaticCellVersion() {
        return new CellVersion(diskCacheV111.util.Version.getVersion(),
                               "$Revision: 1.17 $");
    }

    public void startTlog(String path,String action) {
        if (_tLog == null || _subject == null) {
            return;
        }
        try {
            String user =
                _user + "("+Subjects.getUid(_subject) + "." + Subjects.getPrimaryGid(_subject) + ")";
            _tLog.begin(user, "gsiftp", action, path,
                        _engine.getInetAddress());
        }
        catch (Exception e) {
            error("GsiFtpDoor: couldn't start tLog. " +
                  "Ignoring exception: " + e.getMessage());
        }
    }

    protected GSSContext getServiceContext() throws GSSException {

        GlobusCredential serviceCredential;
        try {
            serviceCredential = new GlobusCredential(service_cert, service_key);
        }
        catch (GlobusCredentialException gce) {
            String errmsg = "GsiFtpDoor: couldn't load " +
                            "host globus credentials: " + gce.toString();
            error(errmsg);
            throw new GSSException(GSSException.NO_CRED, 0, errmsg);
        }

        GSSCredential cred = new GlobusGSSCredentialImpl(serviceCredential,
                                                    GSSCredential.ACCEPT_ONLY);
        TrustedCertificates trusted_certs =
                               TrustedCertificates.load(service_trusted_certs);
        GSSManager manager = ExtendedGSSManager.getInstance();
        ExtendedGSSContext context =
                               (ExtendedGSSContext)manager.createContext(cred);

        context.setOption(GSSConstants.GSS_MODE, GSIConstants.MODE_GSI);
        context.setOption(GSSConstants.TRUSTED_CERTIFICATES, trusted_certs);

        return context;
    }

    @Override
    public void ac_user(String arg)
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
                subject.getPrincipals().add(new UserNamePrincipal(arg));
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
