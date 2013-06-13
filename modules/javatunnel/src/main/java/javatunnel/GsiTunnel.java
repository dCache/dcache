/*
 * $Id: GsiTunnel.java,v 1.6 2006-10-11 09:49:58 tigran Exp $
 */

package javatunnel;

import org.glite.voms.FQAN;
import org.globus.gsi.CredentialException;
import org.globus.gsi.GSIConstants;
import org.globus.gsi.TrustedCertificates;
import org.globus.gsi.X509Credential;
import org.globus.gsi.gssapi.GSSConstants;
import org.globus.gsi.gssapi.GlobusGSSCredentialImpl;
import org.globus.gsi.gssapi.auth.AuthorizationException;
import org.globus.gsi.jaas.GlobusPrincipal;
import org.gridforum.jgss.ExtendedGSSContext;
import org.gridforum.jgss.ExtendedGSSManager;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Iterator;

import org.dcache.auth.FQANPrincipal;
import org.dcache.gplazma.util.CertificateUtils;
import org.dcache.util.Crypto;

import static org.dcache.util.Files.checkDirectory;
import static org.dcache.util.Files.checkFile;

//jgss
// globus gsi


class GsiTunnel extends GssTunnel  {

    private static final Logger _log = LoggerFactory.getLogger(GsiTunnel.class);

    private ExtendedGSSContext _e_context = null;

    private static final String CIPHER_FLAGS = "ciphers";

    private static final String service_key           = "/etc/grid-security/hostkey.pem";
    private static final String service_cert          = "/etc/grid-security/hostcert.pem";
    private static final String service_trusted_certs = "/etc/grid-security/certificates";
    private Subject _subject = new Subject();

    // Creates a new instance of GssTunnel
    public GsiTunnel(String args) throws GSSException, IOException {
        this(args, true);
    }


    public GsiTunnel(String args, boolean init)
            throws GSSException, IOException {

        Args arguments = new Args(args);

        if( init ) {

            X509Credential serviceCredential;

            checkFile(service_key);
            checkFile(service_cert);
            checkDirectory(service_trusted_certs);

            try {
                serviceCredential = new X509Credential(service_cert, service_key);
            } catch (CredentialException e) {
                throw new GSSException(GSSException.NO_CRED, 0, e.getMessage());
            } catch(IOException ioe) {
                throw new GSSException(GSSException.NO_CRED, 0,
                                       "could not load host globus credentials "+ioe.toString());
            }

            GSSCredential cred = new GlobusGSSCredentialImpl(serviceCredential, GSSCredential.ACCEPT_ONLY);
            TrustedCertificates trusted_certs = TrustedCertificates.load(service_trusted_certs);
            GSSManager manager = ExtendedGSSManager.getInstance();
            _e_context = (ExtendedGSSContext) manager.createContext(cred);
            _e_context.setOption(GSSConstants.GSS_MODE, GSIConstants.MODE_GSI);
            _e_context.setOption(GSSConstants.TRUSTED_CERTIFICATES, trusted_certs);
            _e_context.setBannedCiphers(
                    Crypto.getBannedCipherSuitesFromConfigurationValue(arguments.getOpt(CIPHER_FLAGS)));
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
            return new GsiTunnel(null, true);
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
