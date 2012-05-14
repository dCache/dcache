/*
 * $Id: GsiTunnel.java,v 1.6 2006-10-11 09:49:58 tigran Exp $
 */

package javatunnel;

import java.io.*;
import java.util.Iterator;

//jgss
import javax.security.auth.Subject;
import org.dcache.auth.FQANPrincipal;
import org.dcache.gplazma.util.CertificateUtils;
import org.ietf.jgss.*;

// globus gsi
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.glite.voms.FQAN;
import org.globus.gsi.GSIConstants;
import org.globus.gsi.GlobusCredential;
import org.globus.gsi.GlobusCredentialException;
import org.globus.gsi.gssapi.GSSConstants;
import org.globus.gsi.gssapi.GlobusGSSCredentialImpl;
import org.globus.gsi.gssapi.auth.AuthorizationException;
import org.globus.gsi.TrustedCertificates;
import org.globus.gsi.jaas.GlobusPrincipal;
import org.gridforum.jgss.ExtendedGSSContext;
import org.gridforum.jgss.ExtendedGSSManager;

import static org.dcache.util.Files.checkFile;
import static org.dcache.util.Files.checkDirectory;

import dmg.util.Args;

class GsiTunnel extends GssTunnel  {

    private final static Logger _log = LoggerFactory.getLogger(GsiTunnel.class);

    private ExtendedGSSContext _e_context = null;

    private final static String SERVICE_KEY = "service_key";
    private final static String SERVICE_CERT = "service_cert";
    private final static String SERVICE_TRUSTED_CERTS = "service_trusted_certs";

    private final Args _arguments;
    private Subject _subject = new Subject();

    // Creates a new instance of GssTunnel
    public GsiTunnel(String args) throws GSSException, IOException {
        this(args, true);
    }

    public GsiTunnel(String args, boolean init)
            throws GSSException, IOException {
        _arguments = new Args(args);

        if( init ) {
            GlobusCredential serviceCredential;

            String service_key = _arguments.getOption(SERVICE_KEY);
            String service_cert = _arguments.getOption(SERVICE_CERT);
            String service_trusted_certs = _arguments.getOption(SERVICE_TRUSTED_CERTS);
            /* Unfortunately, we can't rely on GlobusCredential to provide
             * meaningful error messages so we catch some obvious problems
             * early.
             */
            checkFile(service_key);
            checkFile(service_cert);
            checkDirectory(service_trusted_certs);

            try {
                serviceCredential = new GlobusCredential(service_cert, service_key);
            } catch (GlobusCredentialException e) {
                throw new GSSException(GSSException.NO_CRED, 0, e.getMessage());
            }

            GSSCredential cred = new GlobusGSSCredentialImpl(serviceCredential, GSSCredential.ACCEPT_ONLY);
            TrustedCertificates trusted_certs = TrustedCertificates.load(service_trusted_certs);
            GSSManager manager = ExtendedGSSManager.getInstance();
            _e_context = (ExtendedGSSContext) manager.createContext(cred);
            _e_context.setOption(GSSConstants.GSS_MODE, GSIConstants.MODE_GSI);
            _e_context.setOption(GSSConstants.TRUSTED_CERTIFICATES, trusted_certs);

            _context = _e_context;
            // do not use channel binding with GSIGSS
            super.useChannelBinding(false);
        }
    }

    @Override
    public boolean verify( InputStream in, OutputStream out, Object addon) {

        try {
        	if( super.verify(in, out, addon) ) {
                    _subject.getPrincipals().add( new GlobusPrincipal(_e_context.getSrcName().toString()) );
        		scanExtendedAttributes(_e_context);
        	}
        } catch( GSSException e) {
            _log.error("Failed to verify: {}", e.toString());
        }

        return _context.isEstablished();
    }


    @Override
    public Convertable makeCopy() throws IOException {
        try {
            return new GsiTunnel(_arguments.toString(), true);
        } catch (GSSException e) {
            throw new IOException(e);
        }
    }

    private void scanExtendedAttributes(ExtendedGSSContext gssContext) {

        try {

            Iterator<String> fqans = CertificateUtils.getFQANsFromGSSContext(gssContext).iterator();
            boolean primary = true;
            while (fqans.hasNext()) {
                String fqanValue = fqans.next();
                FQAN fqan = new FQAN(fqanValue);
                String group = fqan.getGroup();
                String role = fqan.getRole();
                String s;
                if(role == null  || role.equals("") ) {
                    s = group;
                }else{
                    s = group + "/Role=" + role;
                }
                _subject.getPrincipals().add( new FQANPrincipal(s, primary));
                primary = false;
            }

        } catch (AuthorizationException e) {
            _log.error("Failed to get users group and role context: {}", e.toString());
        }

    }

    @Override
    public Subject getSubject() {
        return _subject;
    }
}
