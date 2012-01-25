package org.dcache.gplazma.util;

import java.io.File;
import java.io.IOException;
import java.security.cert.CRLException;
import java.security.cert.CertificateEncodingException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Enumeration;
import java.util.Map;
import java.util.StringTokenizer;

import org.bouncycastle.asn1.ASN1Sequence;
import org.bouncycastle.asn1.ASN1Set;
import org.bouncycastle.asn1.DERObjectIdentifier;
import org.bouncycastle.asn1.DERString;
import org.bouncycastle.asn1.x509.TBSCertificateStructure;
import org.bouncycastle.asn1.x509.X509Name;
import org.dcache.gplazma.AuthenticationException;
import org.glite.voms.PKIStore;
import org.glite.voms.PKIVerifier;
import org.glite.voms.VOMSValidator;
import org.glite.voms.ac.ACValidator;
import org.globus.gsi.bc.BouncyCastleUtil;
import org.globus.gsi.gssapi.GSSConstants;
import org.globus.gsi.gssapi.auth.AuthorizationException;
import org.gridforum.jgss.ExtendedGSSContext;
import org.ietf.jgss.GSSException;
import org.slf4j.MDC;

/**
 * Extraction and conversion methods useful when dealing with X509/Globus/VOMS
 * certificates and proxies.<br>
 * <br>
 *
 * Methods borrowed and/or adapted from gplazma1,
 * gplazma.authz.util.X509CertUtil.
 *
 * @author arossi
 */
public class CertificateUtils {

    static final String SYS_VOMSDIR = "VOMSDIR";
    static final String SYS_CADIR = "CADIR";
    static final String CAPNULL = "/Capability=NULL";
    static final String ROLENULL = "/Role=NULL";

    private static PKIVerifier pkiVerifier;

    /**
     * Extracts the X509Certificate chain from the GSS context,
     * then extracts the FQAN from the certificate (chain).  Attributes
     * are not validated.
     *
     * TODO Update this method not to use the deprecated .parse() on the
     * VOMSValidator.
     *
     * @param gssContext
     * @return collection of FQAN strings.
     * @throws AuthorizationException
     */
    @SuppressWarnings("deprecation")
    public static Collection<String> getFQANsFromGSSContext(
                    ExtendedGSSContext gssContext)
                    throws AuthorizationException {
        X509Certificate[] chain;

        try {
            chain = (X509Certificate[])
                            gssContext.inquireByOid(GSSConstants.X509_CERT_CHAIN);
        } catch (final GSSException gsse) {
            throw new AuthorizationException(
                            "Could not extract certificate chain from context "
                                            + gsse.getMessage() + "\n"
                                            + gsse.getCause());
        }

        try {
            final VOMSValidator validator
                = new VOMSValidator(null,
                                new ACValidator
                                (getPkiVerifier(null, null, MDC.getCopyOfContextMap())));
                validator.setClientChain(chain).parse();
            return new ArrayList<String>(
                            Arrays.asList(validator.getAllFullyQualifiedAttributes()));
        } catch (final AuthorizationException ae) {
            throw new AuthorizationException(ae.toString());
        } catch (final Exception e) {
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
     * @param mdcContext
     * @return singleton instance of verifier
     * @throws IOException
     * @throws CertificateException
     * @throws CRLException
     */
    public static synchronized PKIVerifier getPkiVerifier(String vomsDir,
                    String caDir, Map<?, ?> mdcContext) throws IOException,
                    CertificateException, CRLException {
        if (pkiVerifier == null) {
            /*
             * Since PKIStore instantiates internal threads to periodically
             * reload the store, we reset the MDC to avoid that messages logged
             * by the refresh thread have the wrong context.
             */
            final Map<?, ?> map = MDC.getCopyOfContextMap();
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
                if (actualDir.exists()
                                && actualDir.isDirectory()
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
                if (actualDir.exists()
                                && actualDir.isDirectory()
                                && actualDir.list().length > 0) {
                    caStore = new PKIStore(caDir, PKIStore.TYPE_CADIR);
                }

                pkiVerifier = new PKIVerifier(vomsStore, caStore);
            } finally {
                if (map != null) {
                    MDC.setContextMap(map);
                }
            }
        }
        return pkiVerifier;
    }

    /**
     * Extracts the user subject from the certificate chain. <br>
     * <br>
     * Code adapted from gplazma1 (gplazma.authz.util.X509CertUtil).
     *
     * @param chain
     * @param omitEmail
     * @return Subject as Globus-style DN.
     * @throws AuthenticationException
     */
    public static String getSubjectFromX509Chain(X509Certificate[] chain,
                    boolean omitEmail) throws AuthenticationException {
        String subjectDN = null;
        TBSCertificateStructure tbsCert = null;
        X509Certificate clientcert = null;
        try {
            for (final X509Certificate testcert : chain) {
                tbsCert = BouncyCastleUtil.getTBSCertificateStructure(testcert);
                final int certType = BouncyCastleUtil.getCertificateType(tbsCert);
                if (!org.globus.gsi.CertUtil.isImpersonationProxy(certType)) {
                    clientcert = testcert;
                    break;
                }
            }
        } catch (final IOException e) {
            throw new AuthenticationException(
                            "getSubjectFromX509Chain, can't extract client certificate: "
                                            + e.getMessage(), e);
        } catch (final CertificateException e) {
            throw new AuthenticationException(
                            "getSubjectFromX509Chain, can't extract client certificate: "
                                            + e.getMessage(), e);
        }

        if (clientcert == null) {
            throw new AuthenticationException(
                            "getSubjectFromX509Chain, could not find clientcert");
        }

        if (tbsCert != null) {
            subjectDN = toGlobusString(
                            (ASN1Sequence) tbsCert.getSubject().getDERObject(),
                            omitEmail);
        }

        return subjectDN;
    }

    /**
     * Finds the certificate authority which issued the user proxy. <br>
     * <br>
     * Code adapted from gplazma1 (gplazma.authz.util.X509CertUtil).
     *
     * @param chain
     * @param skipImpersonation
     *            tells the method to look for the original cert in the chain
     * @return certificate authority id as Globus-style DN.
     * @throws AuthenticationException
     */
    public static String getSubjectX509Issuer(X509Certificate[] chain,
                    boolean skipImpersonation) throws AuthenticationException {
        if (chain == null) {
            return null;
        }

        X509Certificate clientcert = null;
        for (final X509Certificate testcert : chain) {
            try {
                if (!skipImpersonation) {
                    clientcert = testcert;
                    break;
                }
                final TBSCertificateStructure tbsCert
                    = BouncyCastleUtil.getTBSCertificateStructure(testcert);
                final int certType = BouncyCastleUtil.getCertificateType(tbsCert);
                if (!org.globus.gsi.CertUtil.isImpersonationProxy(certType)) {
                    clientcert = testcert;
                    break;
                }
            } catch (final CertificateEncodingException t) {
                throw new AuthenticationException("getSubjectX509Issuer", t);
            } catch (final IOException t) {
                throw new AuthenticationException("getSubjectX509Issuer", t);
            } catch (final CertificateException t) {
                throw new AuthenticationException("getSubjectX509Issuer", t);
            }
        }
        if (clientcert == null) {
            throw new AuthenticationException(
                            "getSubjectX509Issuer, could not find clientcert");
        }
        return toGlobusDN(clientcert.getIssuerDN().toString(),
                        skipImpersonation);
    }

    /**
     * Inverts order of parts and substitutes '/' for ','. <br>
     * <br>
     * Code adapted from gplazma1 (gplazma.authz.util.X509CertUtil).
     *
     * @param certDN
     * @param invert
     *            the order of the parts
     * @return Globus-style (path) DN.
     */
    public static String toGlobusDN(String certDN, boolean invert) {
        final StringTokenizer tokens = new StringTokenizer(certDN, ",");
        final StringBuffer buf = new StringBuffer();
        String token;

        while (tokens.hasMoreTokens()) {
            token = tokens.nextToken().trim();
            if (invert) {
                buf.insert(0, token);
                buf.insert(0, "/");
            } else {
                buf.append("/");
                buf.append(token);
            }
        }

        return buf.toString();
    }

    /**
     * Processing similar to {@link #toGlobusDN(String)}; optionally excludes
     * email-element. <br>
     * <br>
     * Code adapted from gplazma1 (gplazma.authz.util.X509CertUtil).
     *
     * @param seq
     * @param omitEmail
     * @return Globus-style (path) DN.
     */
    public static String toGlobusString(ASN1Sequence seq, boolean omitEmail) {
        if (seq == null) {
            return null;
        }

        final Enumeration e = seq.getObjects();
        final StringBuffer buf = new StringBuffer();
        while (e.hasMoreElements()) {
            final ASN1Set set = (ASN1Set) e.nextElement();
            final Enumeration ee = set.getObjects();
            boolean didappend = false;
            while (ee.hasMoreElements()) {
                final ASN1Sequence s = (ASN1Sequence) ee.nextElement();
                final DERObjectIdentifier oid = (DERObjectIdentifier) s.getObjectAt(0);
                final String sym = (String) X509Name.DefaultSymbols.get(oid);
                if (oid.equals(X509Name.EmailAddress) && omitEmail) {
                    continue;
                }
                if (!didappend) {
                    buf.append('/');
                    didappend = true;
                }
                if (sym == null) {
                    buf.append(oid.getId());
                } else {
                    buf.append(sym);
                }
                buf.append('=');
                buf.append(((DERString) s.getObjectAt(1)).getString());
                if (ee.hasMoreElements()) {
                    buf.append('+');
                }
            }
        }
        return buf.toString();
    }
}
