package org.dcache.gplazma.plugins;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Arrays.asList;
import static org.dcache.gplazma.util.Preconditions.checkAuthentication;
import static org.dcache.util.TimeUtils.getMillis;

import eu.emi.security.authn.x509.X509CertChainValidatorExt;
import eu.emi.security.authn.x509.proxy.ProxyUtils;
import java.io.IOException;
import java.security.Principal;
import java.security.cert.CRLException;
import java.security.cert.CertPath;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Base64;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import org.dcache.auth.FQANPrincipal;
import org.dcache.gplazma.AuthenticationException;
import org.dcache.gplazma.util.CertPaths;
import org.italiangrid.voms.VOMSAttribute;
import org.italiangrid.voms.VOMSValidators;
import org.italiangrid.voms.ac.VOMSACValidator;
import org.italiangrid.voms.ac.VOMSValidationResult;
import org.italiangrid.voms.error.VOMSValidationErrorMessage;
import org.italiangrid.voms.store.VOMSTrustStore;
import org.italiangrid.voms.store.VOMSTrustStores;
import org.italiangrid.voms.util.CertificateValidatorBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Validates and extracts FQANs from any X509Certificate certificate chain in the public
 * credentials.
 */
public class VomsPlugin implements GPlazmaAuthenticationPlugin {

    private static final Logger LOGGER = LoggerFactory.getLogger(VomsPlugin.class);

    private static final String CADIR = "gplazma.vomsdir.ca";
    private static final String VOMSDIR = "gplazma.vomsdir.dir";
    private static final String TRUST_ANCHORS_REFRESH_INTERVAL = "gplazma.vomsdir.refresh-interval";
    private final String caDir;
    private final String vomsDir;
    private final long trustAnchorsUpdateInterval;
    private VOMSACValidator validator;
    private final Random random = new Random();

    public VomsPlugin(Properties properties)
          throws CertificateException, CRLException, IOException {
        caDir = properties.getProperty(CADIR);
        vomsDir = properties.getProperty(VOMSDIR);
        trustAnchorsUpdateInterval = getMillis(properties, TRUST_ANCHORS_REFRESH_INTERVAL);

        checkArgument(caDir != null, "Undefined property: " + CADIR);
        checkArgument(vomsDir != null, "Undefined property: " + VOMSDIR);
        checkArgument(trustAnchorsUpdateInterval > 0,
              TRUST_ANCHORS_REFRESH_INTERVAL + " has to be positive non-zero integer, specified: "
                    + trustAnchorsUpdateInterval);
    }

    @Override
    public void start() {
        VOMSTrustStore vomsTrustStore = VOMSTrustStores.newTrustStore(asList(vomsDir));
        X509CertChainValidatorExt certChainValidator = new CertificateValidatorBuilder()
              .lazyAnchorsLoading(false)
              .trustAnchorsUpdateInterval(trustAnchorsUpdateInterval)
              .trustAnchorsDir(caDir)
              .build();
        validator = VOMSValidators.newValidator(vomsTrustStore, certChainValidator);
    }

    @Override
    public void stop() {
        validator.shutdown();
    }

    @Override
    public void authenticate(Set<Object> publicCredentials,
          Set<Object> privateCredentials,
          Set<Principal> identifiedPrincipals)
          throws AuthenticationException {
        boolean primary = true;
        boolean hasX509 = false;
        boolean hasFQANs = false;
        String ids = null;
        boolean multipleIds = false;

        for (Object credential : publicCredentials) {
            if (CertPaths.isX509CertPath(credential)) {
                hasX509 = true;
                List<VOMSValidationResult> results = validator.validateWithResult(
                      CertPaths.getX509Certificates((CertPath) credential));
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
                        String message = buildErrorMessage(result.getValidationErrors());
                        X509Certificate[] chain = CertPaths.getX509Certificates(
                              (CertPath) credential);
                        X509Certificate eec = ProxyUtils.getEndUserCertificate(chain);
                        if (eec == null) {
                            LOGGER.warn("Validation failure {}: {}", id, message);
                        } else {
                            LOGGER.warn("Validation failure {} for DN \"{}\": {}", id,
                                  eec.getSubjectX500Principal().getName(), message);
                        }
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


    private String buildErrorMessage(List<VOMSValidationErrorMessage> errors) {
        return errors.isEmpty() ? "(unknown)" : errors.stream().
              map(VOMSValidationErrorMessage::toString).
              collect(Collectors.joining(", ", "[", "]"));
    }
}
