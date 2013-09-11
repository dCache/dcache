package org.dcache.gplazma.plugins;

import org.glite.voms.PKIVerifier;
import org.glite.voms.VOMSValidator;
import org.glite.voms.ac.ACValidator;
import org.slf4j.MDC;

import java.io.IOException;
import java.security.Principal;
import java.security.cert.CRLException;
import java.security.cert.CertPath;
import java.security.cert.CertificateException;
import java.util.Properties;
import java.util.Set;

import org.dcache.auth.FQANPrincipal;
import org.dcache.auth.util.GSSUtils;
import org.dcache.gplazma.AuthenticationException;
import org.dcache.util.CertPaths;

import static com.google.common.base.Preconditions.checkArgument;
import static org.dcache.gplazma.util.Preconditions.checkAuthentication;

/**
 * Validates and extracts FQANs from any X509Certificate certificate chain in
 * the public credentials.
 */
public class VomsPlugin implements GPlazmaAuthenticationPlugin {

    private static final String CADIR = "gplazma.vomsdir.ca";
    private static final String VOMSDIR = "gplazma.vomsdir.dir";

    private final PKIVerifier _pkiVerifier;

    public VomsPlugin(Properties properties) throws CertificateException,
                    CRLException, IOException {
        String caDir = properties.getProperty(CADIR);
        String vomsDir = properties.getProperty(VOMSDIR);

        checkArgument(caDir != null, "Undefined property: " + VOMSDIR);
        checkArgument(vomsDir != null, "Undefined property: " + CADIR);

        _pkiVerifier = GSSUtils.getPkiVerifier(vomsDir, caDir,
                        MDC.getCopyOfContextMap());
    }

    @Override
    public void authenticate(Set<Object> publicCredentials,
                    Set<Object> privateCredentials,
                    Set<Principal> identifiedPrincipals)
                    throws AuthenticationException {
        VOMSValidator validator
            = new VOMSValidator(null, new ACValidator(_pkiVerifier));

        boolean primary = true;
        boolean hasX509 = false;
        boolean hasFQANs = false;

        for (Object credential : publicCredentials) {
            if (CertPaths.isX509CertPath(credential)) {
                hasX509 = true;
                validator.setClientChain(CertPaths.getX509Certificates((CertPath) credential)).validate();
                for (String fqan : validator.getAllFullyQualifiedAttributes()) {
                    hasFQANs = true;
                    identifiedPrincipals.add(new FQANPrincipal(fqan, primary));
                    primary = false;
                }
            }
        }

        checkAuthentication(hasX509, "no X509 certificate chain");
        checkAuthentication(hasFQANs, "no FQANs");
    }
}
