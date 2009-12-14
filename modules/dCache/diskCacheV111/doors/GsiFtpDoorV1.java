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
        if (_tLog == null) {
            return;
        }
        try {
            String user_string = _user;
            if (_pwdRecord != null) {
                user_string += "("+_pwdRecord.UID+"."+_pwdRecord.GID+")";
            }
            _tLog.begin(user_string, "gsiftp", action, path,
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
    /**
     * Communicates with gPlazma in protocol and configuration  appropriate
     * manner in order to receiceve authorization
     * @return AuthorizationRecord obtained from gPlazma service
     */
    protected AuthorizationRecord authorize()
            throws AuthorizationException {
        if (_use_gplazmaAuthzCell) {
            AuthzQueryHelper authHelper = new AuthzQueryHelper(this);
            authHelper.setDelegateToGplazma(_delegate_to_gplazma);
            AuthorizationMessage authorizationMessage;
            if (_user.equals(GLOBUS_URL_COPY_DEFAULT_USER)) {
                info("GssFtpDoorV1::ac_user: gplazma special case, user is " +
                        _user);
                authorizationMessage =
                        authHelper.getAuthorization(_serviceContext);
            } else {
                authorizationMessage =
                        authHelper.getAuthorization(_serviceContext, _user);

            }
            return authorizationMessage.getAuthorizationRecord();
        } else  if ( _use_gplazmaAuthzModule) {
            AuthorizationController authCtrl =
                    new AuthorizationController(_gplazmaPolicyFilePath);
            if (_user.equals(GLOBUS_URL_COPY_DEFAULT_USER)) {
                info("GssFtpDoorV1::ac_user: gplazma special case, user is " +
                        _user);
                return RecordConvert.gPlazmaToAuthorizationRecord(
                        authCtrl.authorize(_serviceContext, null, null, null));
            } else {
                return RecordConvert.gPlazmaToAuthorizationRecord(
                        authCtrl.authorize(_serviceContext, _user, null, null));
            }
        }  else {
            throw new AuthorizationException(
                    "_use_gplazmaAuthzCell is false and " +
                    "_use_gplazmaAuthzModule is false");
        }

    }
}
