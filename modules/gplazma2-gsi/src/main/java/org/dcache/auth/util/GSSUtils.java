package org.dcache.auth.util;

import com.google.common.base.Objects;
import org.glite.voms.PKIStore;
import org.glite.voms.PKIVerifier;
import org.glite.voms.VOMSAttribute;
import org.glite.voms.VOMSValidator;
import org.glite.voms.ac.ACValidator;
import org.globus.gsi.gssapi.GSSConstants;
import org.globus.gsi.gssapi.auth.AuthorizationException;
import org.gridforum.jgss.ExtendedGSSContext;
import org.gridforum.jgss.ExtendedGSSCredential;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSException;
import org.slf4j.MDC;

import java.io.File;
import java.io.IOException;
import java.security.cert.CRLException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Extraction and conversion methods useful when dealing with GSS/VOMS
 * certificates and proxies.<br>
 * <br>
 *
 * Methods borrowed and/or adapted from gplazma1,
 * gplazma.authz.util.X509CertUtil.
 *
 * @author arossi
 */
public class GSSUtils {

    static final String SYS_VOMSDIR = "VOMSDIR";
    static final String SYS_CADIR = "CADIR";
    static final String CAPNULL = "/Capability=NULL";
    static final String ROLENULL = "/Role=NULL";

    /**
     * Re-use the verifier throughout the domain/JVM when possible, as it
     * is costly in both time and memory to instantiate (avoid out-of-memory
     * errors).  The map allows for multiple instances mapped to different
     * certificate directories.
     */
    private static final Map<PKIKey, PKIVerifier> verifiers = new HashMap<>();

    /**
     * For storing the verifier.  Key is concatenation of two directory paths.
     *
     * @author arossi
     */
    private static class PKIKey {
        private final String vomsDir;
        private final String caDir;

        private PKIKey(String vomsDir, String caDir)
        {
            if (vomsDir == null) {
                vomsDir = System.getProperty(SYS_VOMSDIR);
            }
            if (vomsDir == null) {
                vomsDir = PKIStore.DEFAULT_VOMSDIR;
            }
            this.vomsDir = checkNotNull(vomsDir);

            if (caDir == null) {
                caDir = System.getProperty(SYS_CADIR);
            }
            if (caDir == null) {
                caDir = PKIStore.DEFAULT_CADIR;
            }
            this.caDir = checkNotNull(caDir);
        }

        public String toString() {
            return Objects.toStringHelper(PKIKey.class)
                    .add("vomsDir", vomsDir)
                    .add("caDir", caDir)
                    .toString();
        }

        public boolean equals(Object object) {
            if (!(object instanceof PKIKey)) {
                return false;
            }
            PKIKey other = (PKIKey) object;
            return vomsDir.equals(other.vomsDir) && caDir.equals(other.caDir);
        }

        public int hashCode() {
            return (33 * vomsDir.hashCode()) ^ caDir.hashCode();
        }
    }

    /**
     * Extracts the X509Certificate chain from the GSS context, then extracts
     * the FQAN from the certificate (chain). Attributes are not validated.
     *
     * TODO Update this method not to use the deprecated .parse() on the
     * VOMSValidator.
     */
    @SuppressWarnings("deprecation")
    public static Iterable<String> getFQANsFromGSSContext(String vomsDir, String caDir, ExtendedGSSContext gssContext)
                    throws AuthorizationException {
        X509Certificate[] chain;

        try {
            chain = (X509Certificate[]) gssContext.inquireByOid(GSSConstants.X509_CERT_CHAIN);
        } catch (GSSException gsse) {
            throw new AuthorizationException(
                            "Could not extract certificate chain from context "
                                            + gsse.getMessage() + "\n"
                                            + gsse.getCause());
        }

        return extractFQANs(vomsDir, caDir, chain);
    }

    /**
     * Return some iterable of FQANs for this credential.  If the credential
     * type is unknown or has not FQANs then an empty iterable is returned.
     */
    public static Iterable<String> getFQANsFromGSSCredential(String vomsDir, String caDir, GSSCredential credential)
            throws GSSException, AuthorizationException
    {
        X509Certificate[] chain = null;

        if (credential instanceof ExtendedGSSCredential) {
            chain = (X509Certificate[]) ((ExtendedGSSCredential)credential).inquireByOid(GSSConstants.X509_CERT_CHAIN);
        }

        return (chain == null) ? Collections.<String>emptyList() : extractFQANs(vomsDir, caDir, chain);
    }

    public static Iterable<String> extractFQANs(String vomsDir, String caDir, X509Certificate[] chain)
            throws AuthorizationException
    {
        try {
            VOMSValidator validator = new VOMSValidator(null,
                    new ACValidator(getPkiVerifier(vomsDir, caDir)));
            validator.setClientChain(chain).parse();
            List<VOMSAttribute> listOfAttributes = validator.getVOMSAttributes();
            return getFQANSfromVOMSAttributes(listOfAttributes);
        } catch (AuthorizationException ae) {
            throw new AuthorizationException(ae.toString());
        } catch (Exception e) {
            throw new AuthorizationException("Could not validate role.");
        }
    }

    /**
     * Normalizes the FQANS into simple names in the case of <code>NULL</code>
     * ROLE and/or CAPABILITY.
     */
    private static Set<String>
            getFQANSfromVOMSAttributes(List<VOMSAttribute> listOfAttributes) {
        Set<String> fqans = new LinkedHashSet<>();

        for (VOMSAttribute vomsAttribute : listOfAttributes) {
            List<?> listOfFqans = vomsAttribute.getFullyQualifiedAttributes();
            for (Object fqan : listOfFqans) {
                String attr = (String) fqan;
                if (attr.endsWith(CAPNULL)) {
                    attr = attr.substring(0, attr.length() - CAPNULL.length());
                }
                if (attr.endsWith(ROLENULL)) {
                    attr = attr.substring(0, attr.length() - ROLENULL.length());
                }
                fqans.add(attr);
            }
        }

        return fqans;
    }

    /**
     * The verifier is used in validating and extracting the VOMS attributes and
     * extensions.
     *
     * @param vomsDir
     *            a specialized non-default location
     * @param caDir
     *            a specialized non-default location
     */
    public static synchronized PKIVerifier getPkiVerifier(String vomsDir, String caDir)
            throws IOException, CertificateException, CRLException
    {
        PKIKey key = new PKIKey(vomsDir, caDir);
        PKIVerifier verifier = verifiers.get(key);
        if (verifier == null) {
            /*
             * Since PKIStore instantiates internal threads to periodically
             * reload the store, we reset the MDC to avoid that messages logged
             * by the refresh thread have the wrong context.
             */
            Map<String,String> map = MDC.getCopyOfContextMap();
            try {
                MDC.clear();

                PKIStore vomsStore = null;
                File actualDir = new File(key.vomsDir);
                if (actualDir.exists() && actualDir.isDirectory()
                                && actualDir.list().length > 0) {
                    vomsStore = new PKIStore(key.vomsDir, PKIStore.TYPE_VOMSDIR);
                }

                PKIStore caStore = null;
                actualDir = new File(key.caDir);
                if (actualDir.exists() && actualDir.isDirectory()
                                && actualDir.list().length > 0) {
                    caStore = new PKIStore(key.caDir, PKIStore.TYPE_CADIR);
                }

                verifier = new PKIVerifier(vomsStore, caStore);
                verifiers.put(key, verifier);
            } finally {
                if (map != null) {
                    MDC.setContextMap(map);
                }
            }
        }
        return verifier;
    }
}
