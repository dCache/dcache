package org.dcache.gplazma.plugins;

import java.io.IOException;
import java.security.Principal;
import java.security.cert.CRLException;
import java.security.cert.CertPath;
import java.security.cert.CertificateException;
import java.util.Properties;
import java.util.Set;

import org.dcache.auth.FQANPrincipal;
import org.dcache.gplazma.AuthenticationException;
import org.dcache.util.CertPaths;

import static com.google.common.base.Preconditions.checkArgument;
import java.util.List;

import static java.util.Arrays.asList;
import static org.dcache.gplazma.util.Preconditions.checkAuthentication;

import eu.emi.security.authn.x509.X509CertChainValidatorExt;
import org.italiangrid.voms.VOMSAttribute;
import org.italiangrid.voms.VOMSValidators;
import org.italiangrid.voms.ac.VOMSACValidator;
import org.italiangrid.voms.store.VOMSTrustStore;
import org.italiangrid.voms.store.VOMSTrustStores;
import org.italiangrid.voms.util.CertificateValidatorBuilder;

/**
 * Validates and extracts FQANs from any X509Certificate certificate chain in
 * the public credentials.
 */
public class VomsPlugin implements GPlazmaAuthenticationPlugin {

    private static final String CADIR = "gplazma.vomsdir.ca";
    private static final String VOMSDIR = "gplazma.vomsdir.dir";
    private final VOMSACValidator validator;

    public VomsPlugin(Properties properties) throws CertificateException,
                    CRLException, IOException {
        String caDir = properties.getProperty(CADIR);
        String vomsDir = properties.getProperty(VOMSDIR);

        checkArgument(caDir != null, "Undefined property: " + VOMSDIR);
        checkArgument(vomsDir != null, "Undefined property: " + CADIR);

        VOMSTrustStore vomsTrustStore = VOMSTrustStores.newTrustStore(asList(vomsDir));
        X509CertChainValidatorExt certChainValidator = new CertificateValidatorBuilder().trustAnchorsDir(caDir).build();
        validator = VOMSValidators.newValidator(vomsTrustStore, certChainValidator);
    }

    @Override
    public void authenticate(Set<Object> publicCredentials,
                    Set<Object> privateCredentials,
                    Set<Principal> identifiedPrincipals)
                    throws AuthenticationException {
        boolean primary = true;
        boolean hasX509 = false;
        boolean hasFQANs = false;

        for (Object credential : publicCredentials) {
            if (CertPaths.isX509CertPath(credential)) {
                hasX509 = true;
                List<VOMSAttribute> attrs = validator.validate(CertPaths.getX509Certificates((CertPath) credential));
                for (VOMSAttribute attr : attrs) {
                    for (String fqan : attr.getFQANs()) {
                        hasFQANs = true;
                        identifiedPrincipals.add(new FQANPrincipal(fqan, primary));
                        primary = false;
                    }
                }
            }
        }

        checkAuthentication(hasX509, "no X509 certificate chain");
        checkAuthentication(hasFQANs, "no FQANs");
    }
}
