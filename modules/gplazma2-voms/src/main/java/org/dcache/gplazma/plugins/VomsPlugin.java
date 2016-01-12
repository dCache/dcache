package org.dcache.gplazma.plugins;

import eu.emi.security.authn.x509.X509CertChainValidatorExt;
import org.italiangrid.voms.VOMSAttribute;
import org.italiangrid.voms.VOMSValidators;
import org.italiangrid.voms.ac.VOMSACValidator;
import org.italiangrid.voms.ac.VOMSValidationResult;
import org.italiangrid.voms.store.VOMSTrustStore;
import org.italiangrid.voms.store.VOMSTrustStores;
import org.italiangrid.voms.util.CertificateValidatorBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.Principal;
import java.security.cert.CRLException;
import java.security.cert.CertPath;
import java.security.cert.CertificateException;
import java.util.Base64;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.Set;

import org.dcache.auth.FQANPrincipal;
import org.dcache.gplazma.AuthenticationException;
import org.dcache.gplazma.util.CertPaths;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Arrays.asList;
import static org.dcache.gplazma.util.Preconditions.checkAuthentication;

/**
 * Validates and extracts FQANs from any X509Certificate certificate chain in
 * the public credentials.
 */
public class VomsPlugin implements GPlazmaAuthenticationPlugin
{
    private static final Logger LOG = LoggerFactory.getLogger(VomsPlugin.class);

    private static final String CADIR = "gplazma.vomsdir.ca";
    private static final String VOMSDIR = "gplazma.vomsdir.dir";
    private final String caDir;
    private final String vomsDir;
    private VOMSACValidator validator;
    private final Random random = new Random();

    public VomsPlugin(Properties properties)
            throws CertificateException, CRLException, IOException
    {
        caDir = properties.getProperty(CADIR);
        vomsDir = properties.getProperty(VOMSDIR);
        checkArgument(caDir != null, "Undefined property: " + CADIR);
        checkArgument(vomsDir != null, "Undefined property: " + VOMSDIR);
    }

    @Override
    public void start()
    {
        VOMSTrustStore vomsTrustStore = VOMSTrustStores.newTrustStore(asList(vomsDir));
        X509CertChainValidatorExt certChainValidator = new CertificateValidatorBuilder().trustAnchorsDir(caDir).build();
        validator = VOMSValidators.newValidator(vomsTrustStore, certChainValidator);
    }

    @Override
    public void stop()
    {
        validator.shutdown();
    }

    @Override
    public void authenticate(Set<Object> publicCredentials,
                    Set<Object> privateCredentials,
                    Set<Principal> identifiedPrincipals)
                    throws AuthenticationException
    {
        boolean primary = true;
        boolean hasX509 = false;
        boolean hasFQANs = false;
        String ids = null;
        boolean multipleIds = false;

        for (Object credential : publicCredentials) {
            if (CertPaths.isX509CertPath(credential)) {
                hasX509 = true;
                List<VOMSValidationResult> results = validator.validateWithResult(CertPaths.getX509Certificates((CertPath) credential));
                for (VOMSValidationResult result : results) {
                    if (result.isValid()) {
                        VOMSAttribute attr = result.getAttributes();

                        for (String fqan : attr.getFQANs()) {
                            hasFQANs = true;
                            identifiedPrincipals.add(new FQANPrincipal(fqan, primary));
                            primary = false;
                        }
                    } else {
                        byte[] rawId = new byte[3]; // a Base64 char represents 6 bits; 4 chars represent 3 bytes.
                        random.nextBytes(rawId);
                        String id = Base64.getEncoder().withoutPadding().encodeToString(rawId);
                        LOG.warn("Validation failure {}: {}", id, result.getValidationErrors());
                        if (ids == null) {
                            ids = id;
                        } else {
                            ids = ids + ", " + id;
                            multipleIds = true;
                        }
                    }
                }
            }
        }

        if (ids != null && !hasFQANs) {
            String failure = multipleIds ? "failures" : "failure";
            throw new AuthenticationException("validation " + failure + ": " + ids);
        }
        checkAuthentication(hasX509, "no X509 certificate chain");
        checkAuthentication(hasFQANs, "no FQANs");
    }
}
