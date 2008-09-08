// $Id: GsiFtpDoorV1.java,v 1.17 2007-10-25 20:02:42 behrmann Exp $
// $Log: not supported by cvs2svn $
// Revision 1.16  2005/11/22 10:59:30  patrick
// Versioning enabled.
//
// Revision 1.15  2005/11/08 14:16:35  patrick
// perf switch off with perfMarkerPeriod=0 (from 1.6.6)
//
// Revision 1.11.6.2  2005/10/31 10:21:16  patrick
// perf switch off with perfMarkerPeriod=0
//
// Revision 1.11.6.1  2005/10/31 05:42:41  patrick
// Performance Markers from head to 1.6.6
//
// Revision 1.14  2005/10/26 17:56:41  aik
// GFtp performance markers implementation.
//
// Revision 1.11.8.1  2005/10/12 23:08:13  aik
// GridFtp performance markers implementation. First (or ~0.75) cut.
//
// Revision 1.11  2005/05/19 05:55:43  timur
// added support for monitoring door state via dcache pages
//
// Revision 1.10  2004/09/09 20:27:37  timur
// made ftp transaction logging optional
//
// Revision 1.9  2004/09/09 18:39:40  timur
// added the uid,gid to the user names in FTP logs
//
// Revision 1.8  2004/09/08 21:25:43  timur
// remote gsiftp transfer manager will now use ftp logger too, fixed ftp door logging problem
//
// Revision 1.7  2004/08/19 18:22:28  timur
// gridftp door gives pool a host name instead of address, reformated code
//
// Revision 1.6  2003/09/25 16:52:05  cvs
// use globus java cog kit gsi gss library instead of gsint
//
// Revision 1.5  2003/09/02 21:06:20  cvs
// removed logging of security info by kftp and gsiftp doors, changest in srm
//
// Revision 1.4  2003/06/12 22:39:30  cvs
// added code to allow an individual user to be read-only
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
        _GSSFlavor = "gsi";

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
}
