package org.dcache.gplazma.plugins;

import java.io.IOException;
import java.security.Principal;
import java.security.cert.CRLException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Properties;
import java.util.Set;

import org.dcache.auth.FQANPrincipal;
import org.dcache.gplazma.AuthenticationException;
import org.dcache.gplazma.SessionID;
import org.glite.voms.PKIStore;
import org.glite.voms.PKIVerifier;
import org.glite.voms.VOMSValidator;
import org.glite.voms.ac.ACValidator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Validates and extracts FQANs from any X509Certificate certificate
 * chain in the public credentials.
 */
public class VomsPlugin implements GPlazmaAuthenticationPlugin
{
    private static final Logger _log =
        LoggerFactory.getLogger(VomsPlugin.class);

    private static final String DEFAULT_CADIR =
        "/etc/grid-security/certificates";
    private static final String DEFAULT_VOMSDIR =
        "/etc/grid-security/vomsdir";

    private static final String CADIR = "cadir";
    private static final String VOMSDIR = "vomsdir";

    private final String _caDir;
    private final String _vomsDir;

    private PKIVerifier _pkiVerifier;

    public VomsPlugin(Properties properties)
    {
        _caDir = properties.getProperty(CADIR, DEFAULT_CADIR);
        _vomsDir = properties.getProperty(VOMSDIR, DEFAULT_VOMSDIR);
    }

    protected synchronized PKIVerifier getPkiVerifier()
        throws IOException, CertificateException, CRLException
    {
        if (_pkiVerifier == null) {
            _pkiVerifier =
                new PKIVerifier(new PKIStore(_vomsDir, PKIStore.TYPE_VOMSDIR),
                                new PKIStore(_caDir, PKIStore.TYPE_CADIR));
        }
        return _pkiVerifier;
    }

    @Override
    public void authenticate(SessionID sID,
                             Set<Object> publicCredentials,
                             Set<Object> privateCredentials,
                             Set<Principal> identifiedPrincipals)
        throws AuthenticationException
    {
        try {
            VOMSValidator validator =
                new VOMSValidator(null, new ACValidator(getPkiVerifier()));
            boolean primary = true;

            for (Object credential: publicCredentials) {
                if (credential instanceof X509Certificate[]) {
                    X509Certificate[] chain = (X509Certificate[]) credential;
                    validator.setClientChain(chain).validate();
                    for (String fqan: validator.getAllFullyQualifiedAttributes()) {
                        identifiedPrincipals.add(new FQANPrincipal(fqan, primary));
                        primary = false;
                    }
                }
            }

            if (primary) {
                throw new AuthenticationException("X509 certificate chain or VOMS extensions missing");
            }
        } catch (IOException e) {
            _log.error("Failed to load PKI stores: {}", e.getMessage());
            throw new AuthenticationException(e.getMessage(), e);
        } catch (CertificateException e) {
            throw new AuthenticationException(e.getMessage(), e);
        } catch (CRLException e) {
            throw new AuthenticationException(e.getMessage(), e);
        }
    }
}
