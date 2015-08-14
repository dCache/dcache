package org.dcache.gplazma.util;

import org.bouncycastle.asn1.ASN1Sequence;
import org.bouncycastle.asn1.ASN1Set;
import org.bouncycastle.asn1.DERObjectIdentifier;
import org.bouncycastle.asn1.DERString;
import org.bouncycastle.asn1.x509.TBSCertificateStructure;
import org.bouncycastle.asn1.x509.X509Name;
import org.globus.gsi.bc.BouncyCastleUtil;

import java.io.IOException;
import java.security.cert.CertificateEncodingException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Enumeration;
import java.util.StringTokenizer;

import org.dcache.gplazma.AuthenticationException;

import static org.dcache.gplazma.util.Preconditions.checkAuthentication;

/**
 * Extraction and conversion methods useful when dealing with X509/Globus
 * certificates and proxies.<br>
 * <br>
 *
 * Methods borrowed and/or adapted from gplazma1,
 * gplazma.authz.util.X509CertUtil.
 *
 * @author arossi
 */
public class X509Utils {

    /**
     * Extracts the user subject from the certificate chain.
     *
     * @return Subject as Globus-style DN.
     */
    public static String getSubjectFromX509Chain(X509Certificate[] chain,
                    boolean omitEmail) throws AuthenticationException {
        try {
            X509Certificate clientcert = BouncyCastleUtil.getIdentityCertificate(chain);
            checkAuthentication(clientcert != null, "no client certificate");
            TBSCertificateStructure tbsCert = BouncyCastleUtil.getTBSCertificateStructure(clientcert);
            return toGlobusString(
                    (ASN1Sequence) tbsCert.getSubject().getDERObject(),
                    omitEmail);
        } catch (IOException | CertificateException e) {
            throw new AuthenticationException(e.getMessage(), e);
        }
    }

    /**
     * Finds the certificate authority which issued the user proxy.
     *
     * @param skipImpersonation
     *            tells the method to look for the original cert in the chain
     * @return certificate authority id as Globus-style DN.
     */
    @SuppressWarnings("null")
    public static String getSubjectX509Issuer(X509Certificate[] chain,
                    boolean skipImpersonation) throws AuthenticationException {
        if (chain == null || chain.length == 0) {
            return null;
        }

        try {
            X509Certificate clientcert;
            if (skipImpersonation) {
                clientcert = chain[0];
            } else {
                clientcert = BouncyCastleUtil.getIdentityCertificate(chain);
                checkAuthentication(clientcert != null, "no client certificate");
            }
            return toGlobusDN(clientcert.getIssuerDN().toString(),
                    skipImpersonation); // Is using skipImpression correct?
        } catch (CertificateEncodingException t) {
            throw new AuthenticationException("badly formatted certificate: "
                    + t.getMessage(), t);
        } catch (CertificateException t) {
            throw new AuthenticationException("problem with certificate: "
                    + t.getMessage(), t);
        }
    }

    /**
     * Inverts order of parts and substitutes '/' for ','. <br>
     * <br>
     * Code adapted from gplazma1 (gplazma.authz.util.X509CertUtil).
     *
     * @param invert
     *            the order of the parts
     * @return Globus-style (path) DN.
     */
    public static String toGlobusDN(String certDN, boolean invert) {
        StringTokenizer tokens = new StringTokenizer(certDN, ",");
        StringBuilder buf = new StringBuilder();
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
     * @return Globus-style (path) DN.
     */
    public static String toGlobusString(ASN1Sequence seq, boolean omitEmail) {
        if (seq == null) {
            return null;
        }

        Enumeration<?> e = seq.getObjects();
        StringBuilder buf = new StringBuilder();
        while (e.hasMoreElements()) {
            ASN1Set set = ASN1Set.getInstance(e.nextElement());
            Enumeration<?> ee = set.getObjects();
            boolean didappend = false;
            while (ee.hasMoreElements()) {
                ASN1Sequence s = (ASN1Sequence) ee.nextElement();
                String derString = ((DERString)s.getObjectAt(1)).getString();
                /*
                 * A temporary hack against Globus 2.  For some reason,
                 * DN=proxy is no longer accepted
                 */
                if ("proxy".equalsIgnoreCase(derString.trim())) {
                    continue;
                }
                DERObjectIdentifier oid = (DERObjectIdentifier) s.getObjectAt(0);
                String sym = (String) X509Name.DefaultSymbols.get(oid);
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
                buf.append(derString);
                if (ee.hasMoreElements()) {
                    buf.append('+');
                }
            }
        }
        return buf.toString();
    }
}
