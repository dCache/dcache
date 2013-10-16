/*
 * $Id: GsiTunnel.java,v 1.6 2006-10-11 09:49:58 tigran Exp $
 */

package javatunnel;

import org.glite.voms.FQAN;
import org.glite.voms.PKIVerifier;
import org.globus.gsi.CredentialException;
import org.globus.gsi.GSIConstants;
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
import org.slf4j.MDC;

import javax.security.auth.Subject;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.cert.X509Certificate;
import java.util.Iterator;

import dmg.util.Args;

import org.dcache.auth.FQANPrincipal;
import org.dcache.auth.util.GSSUtils;
import org.dcache.util.Crypto;

import static org.dcache.util.Files.checkDirectory;
import static org.dcache.util.Files.checkFile;
//jgss
// globus gsi

class GsiTunnel extends GssTunnel  {

    private static final Logger _log = LoggerFactory.getLogger(GsiTunnel.class);

    private ExtendedGSSContext _e_context;

    private static final String SERVICE_KEY = "service_key";
    private static final String SERVICE_CERT = "service_cert";
    private static final String SERVICE_TRUSTED_CERTS = "service_trusted_certs";
    private static final String SERVICE_VOMS_DIR = "service_voms_dir";
    private static final String CIPHER_FLAGS = "ciphers";

    private final Args _arguments;
    private PKIVerifier _pkiVerifier;
    private Subject _subject = new Subject();

    // Creates a new instance of GssTunnel
    public GsiTunnel(String args) throws GSSException, IOException {
        this(args, true);
    }

    public GsiTunnel(String args, boolean init)
            throws GSSException, IOException {
        _arguments = new Args(args);

        if( init ) {
            X509Credential serviceCredential;
            String service_key = _arguments.getOption(SERVICE_KEY);
            String service_cert = _arguments.getOption(SERVICE_CERT);
            String service_trusted_certs = _arguments.getOption(SERVICE_TRUSTED_CERTS);
            String service_voms_dir = _arguments.getOption(SERVICE_VOMS_DIR);

            /* Unfortunately, we can't rely on GlobusCredential to provide
             * meaningful error messages so we catch some obvious problems
             * early.
             */
            checkFile(service_key);
            checkFile(service_cert);
            checkDirectory(service_trusted_certs);

            try {
                _pkiVerifier = GSSUtils.getPkiVerifier(service_voms_dir,
                                                      service_trusted_certs,
                                                      MDC.getCopyOfContextMap());
            } catch ( Exception e) {
                throw new GSSException(GSSException.FAILURE, 0, e.getMessage());
            }

            try {
                serviceCredential = new X509Credential(service_cert, service_key);
            } catch (CredentialException e) {
                throw new GSSException(GSSException.NO_CRED, 0, e.getMessage());
            } catch(IOException ioe) {
                throw new GSSException(GSSException.NO_CRED, 0,
                                       "could not load host globus credentials "+ioe.toString());
            }

            GSSCredential cred = new GlobusGSSCredentialImpl(serviceCredential, GSSCredential.ACCEPT_ONLY);
            GSSManager manager = ExtendedGSSManager.getInstance();
            _e_context = (ExtendedGSSContext) manager.createContext(cred);
            _e_context.setOption(GSSConstants.GSS_MODE, GSIConstants.MODE_GSI);
            _e_context.setBannedCiphers(
                    Crypto.getBannedCipherSuitesFromConfigurationValue(_arguments.getOption(CIPHER_FLAGS)));
            _context = _e_context;
            // do not use channel binding with GSIGSS
            super.useChannelBinding(false);
        }
    }

    @Override
    public boolean verify(InputStream in, OutputStream out, Object addon) {
        try {
            if (super.verify(in, out, addon)) {
                X509Certificate[] chain = (X509Certificate[]) _e_context.inquireByOid(GSSConstants.X509_CERT_CHAIN);
                _subject.getPublicCredentials().add(chain);
                _subject.getPrincipals().add(new GlobusPrincipal(
                                       _e_context.getSrcName().toString()));
                scanExtendedAttributes(_e_context);
            }
        } catch (GSSException e) {
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
            Iterator<String> fqans
                = GSSUtils.getFQANsFromGSSContext(gssContext, _pkiVerifier)
                .iterator();
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
