package org.dcache.auth.util;

import java.io.File;
import java.io.IOException;
import java.security.cert.CRLException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.glite.voms.PKIStore;
import org.glite.voms.PKIVerifier;
import org.glite.voms.VOMSAttribute;
import org.glite.voms.VOMSValidator;
import org.glite.voms.ac.ACValidator;
import org.globus.gsi.gssapi.GSSConstants;
import org.globus.gsi.gssapi.auth.AuthorizationException;
import org.gridforum.jgss.ExtendedGSSContext;
import org.ietf.jgss.GSSException;
import org.slf4j.MDC;

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
     * Extracts the X509Certificate chain from the GSS context,
     * then extracts the FQAN from the certificate (chain).  Attributes
     * are not validated.
     *
     * TODO Update this method not to use the deprecated .parse() on the
     * VOMSValidator.
     */
    @SuppressWarnings("deprecation")
    public static Collection<String> getFQANsFromGSSContext(
                    ExtendedGSSContext gssContext, PKIVerifier pkiVerifier)
                    throws AuthorizationException {
        X509Certificate[] chain;

        try {
            chain = (X509Certificate[])
                            gssContext.inquireByOid(GSSConstants.X509_CERT_CHAIN);
        } catch (GSSException gsse) {
            throw new AuthorizationException(
                            "Could not extract certificate chain from context "
                                            + gsse.getMessage() + "\n"
                                            + gsse.getCause());
        }

        try {
            VOMSValidator validator
                = new VOMSValidator(null,
                                new ACValidator
                                (getPkiVerifier(null, null, MDC.getCopyOfContextMap())));
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
     * The verifier is used in validating and extracting the VOMS attributes and
     * extensions.
     *
     * @param vomsDir
     *            a specialized non-default location
     * @param caDir
     *            a specialized non-default location
     */
    public static synchronized PKIVerifier getPkiVerifier(String vomsDir,
                    String caDir, Map<?, ?> mdcContext) throws IOException,
                    CertificateException, CRLException {
        /*
         * Since PKIStore instantiates internal threads to periodically reload
         * the store, we reset the MDC to avoid that messages logged by the
         * refresh thread have the wrong context.
         */
        Map<?, ?> map = MDC.getCopyOfContextMap();
        try {
            if (mdcContext != null) {
                MDC.setContextMap(mdcContext);
            }

            if (vomsDir == null) {
                vomsDir = System.getProperty(SYS_VOMSDIR);
            }

            if (vomsDir == null) {
                vomsDir = PKIStore.DEFAULT_VOMSDIR;
            }

            PKIStore vomsStore = null;
            File actualDir = new File(vomsDir);
            if (actualDir.exists() && actualDir.isDirectory()
                            && actualDir.list().length > 0) {
                vomsStore = new PKIStore(vomsDir, PKIStore.TYPE_VOMSDIR);
            }

            if (caDir == null) {
                caDir = System.getProperty(SYS_CADIR);
            }

            if (caDir == null) {
                caDir = PKIStore.DEFAULT_CADIR;
            }

            PKIStore caStore = null;
            actualDir = new File(caDir);
            if (actualDir.exists() && actualDir.isDirectory()
                            && actualDir.list().length > 0) {
                caStore = new PKIStore(caDir, PKIStore.TYPE_CADIR);
            }

            return new PKIVerifier(vomsStore, caStore);
        } finally {
            if (map != null) {
                MDC.setContextMap(map);
            }
        }
    }

    /**
     * Normalizes the FQANS into simple names in the case of <code>NULL</code>
     * ROLE and/or CAPABILITY.
     */
    private static Set<String> getFQANSfromVOMSAttributes(List<VOMSAttribute> listOfAttributes) {
        Set<String> fqans = new LinkedHashSet<>();

        for (VOMSAttribute vomsAttribute : listOfAttributes) {
            List listOfFqans = vomsAttribute.getFullyQualifiedAttributes();
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
}
