package org.dcache.gplazma.util;


import eu.emi.security.authn.x509.X509CertChainValidatorExt;
import org.globus.gsi.gssapi.GSSConstants;
import org.globus.gsi.gssapi.auth.AuthorizationException;
import org.gridforum.jgss.ExtendedGSSContext;
import org.gridforum.jgss.ExtendedGSSCredential;
import org.italiangrid.voms.ac.VOMSACValidator;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSException;
import java.security.cert.X509Certificate;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.italiangrid.voms.VOMSAttribute;
import org.italiangrid.voms.VOMSValidators;
import org.italiangrid.voms.store.VOMSTrustStore;
import org.italiangrid.voms.store.VOMSTrustStores;
import org.italiangrid.voms.util.CertificateValidatorBuilder;

import static java.util.Arrays.asList;

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

    static final String CAPNULL = "/Capability=NULL";
    static final String ROLENULL = "/Role=NULL";


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
    {
        VOMSTrustStore vomsTrustStore = VOMSTrustStores.newTrustStore(asList(vomsDir));
        X509CertChainValidatorExt certChainValidator = new CertificateValidatorBuilder().trustAnchorsDir(caDir).build();
        VOMSACValidator validator = VOMSValidators.newValidator(vomsTrustStore, certChainValidator);
        List<VOMSAttribute> listOfAttributes = validator.validate(chain);
        return getFQANSfromVOMSAttributes(listOfAttributes);
    }

    /**
     * Normalizes the FQANS into simple names in the case of <code>NULL</code>
     * ROLE and/or CAPABILITY.
     */
    private static Set<String>
            getFQANSfromVOMSAttributes(List<VOMSAttribute> listOfAttributes) {
        Set<String> fqans = new LinkedHashSet<>();

        for (VOMSAttribute vomsAttribute : listOfAttributes) {
            List<?> listOfFqans = vomsAttribute.getFQANs();
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
