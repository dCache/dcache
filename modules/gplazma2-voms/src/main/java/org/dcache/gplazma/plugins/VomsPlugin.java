package org.dcache.gplazma.plugins;

import static com.google.common.base.Preconditions.checkArgument;
import static org.dcache.gplazma.util.Preconditions.checkAuthentication;

import java.io.IOException;
import java.security.Principal;
import java.security.cert.CRLException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.dcache.auth.FQANPrincipal;
import org.dcache.gplazma.AuthenticationException;
import org.dcache.gplazma.util.CertificateUtils;
import org.glite.voms.VOMSValidator;
import org.glite.voms.ac.ACValidator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

/**
 * Validates and extracts FQANs from any X509Certificate certificate
 * chain in the public credentials.
 */
public class VomsPlugin implements GPlazmaAuthenticationPlugin
{
    private static final Logger _log =
        LoggerFactory.getLogger(VomsPlugin.class);

    private static final String CADIR =
        "gplazma.vomsdir.ca";
    private static final String VOMSDIR =
        "gplazma.vomsdir.dir";

    private final String _caDir;
    private final String _vomsDir;
    private final Map<?, ?> _mdcContext;

    public VomsPlugin(Properties properties)
    {
        _caDir = properties.getProperty(CADIR);
        _vomsDir = properties.getProperty(VOMSDIR);

        checkArgument(_vomsDir != null, "Undefined property: " + VOMSDIR);
        checkArgument(_caDir != null, "Undefined property: " + CADIR);

        _mdcContext = MDC.getCopyOfContextMap();
    }

    @Override
    public void authenticate(Set<Object> publicCredentials,
                             Set<Object> privateCredentials,
                             Set<Principal> identifiedPrincipals)
        throws AuthenticationException
    {
        try {
            VOMSValidator validator =
                new VOMSValidator(null,
                                new ACValidator(CertificateUtils.getPkiVerifier
                                                (_vomsDir, _caDir, _mdcContext)));
            boolean primary = true;

            boolean hasX509 = false;
            boolean hasFQANs = false;

            for (Object credential: publicCredentials) {
                if (credential instanceof X509Certificate[]) {
                    hasX509 = true;
                    X509Certificate[] chain = (X509Certificate[]) credential;
                    validator.setClientChain(chain).validate();
                    for (String fqan: validator.getAllFullyQualifiedAttributes()) {
                        hasFQANs = true;
                        identifiedPrincipals.add(new FQANPrincipal(fqan, primary));
                        primary = false;
                    }
                }
            }

            checkAuthentication(hasX509, "no X509 certificate chain");
            checkAuthentication(hasFQANs, "no FQANs");

        } catch (IOException e) {
            _log.error("failed to load PKI stores: {}", e.getMessage());
            throw new AuthenticationException(e.getMessage(), e);
        } catch (CertificateException e) {
            throw new AuthenticationException(e.getMessage(), e);
        } catch (CRLException e) {
            throw new AuthenticationException(e.getMessage(), e);
        }
    }
}
