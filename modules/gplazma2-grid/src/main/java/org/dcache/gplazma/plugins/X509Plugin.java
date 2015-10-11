package org.dcache.gplazma.plugins;

import org.bouncycastle.asn1.ASN1ObjectIdentifier;
import org.bouncycastle.asn1.ASN1Sequence;
import org.bouncycastle.asn1.DEREncodable;
import org.bouncycastle.asn1.DERObjectIdentifier;
import org.bouncycastle.asn1.DERSequence;
import org.bouncycastle.asn1.x509.PolicyInformation;
import org.globus.gsi.gssapi.jaas.GlobusPrincipal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.Principal;
import java.security.cert.CertPath;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;

import org.dcache.auth.EntityDefinitionPrincipal;
import org.dcache.auth.LoAPrincipal;
import org.dcache.gplazma.AuthenticationException;
import org.dcache.gplazma.util.CertPaths;

import static eu.emi.security.authn.x509.helpers.CertificateHelpers.getExtensionBytes;
import static org.dcache.auth.EntityDefinition.*;
import static org.dcache.auth.LoA.*;
import static org.dcache.gplazma.util.CertPaths.*;
import static org.dcache.gplazma.util.Preconditions.checkAuthentication;

/**
 * Extracts GlobusPrincipals from any X509Certificate certificate
 * chain in the public credentials.
 *
 * The certificate is not validated (ie no CRL checks and no CA
 * signature check). It is assumed that the door did this check
 * already.
 */
public class X509Plugin implements GPlazmaAuthenticationPlugin
{
    private static final Logger LOG = LoggerFactory.getLogger(X509Plugin.class);
    private static final String OID_CERTIFICATE_POLICIES = "2.5.29.32";
    private static final String OID_ANY_POLICY = "2.5.29.32";
    private static final DERSequence ANY_POLICY = new DERSequence(new ASN1ObjectIdentifier(OID_ANY_POLICY));

    public X509Plugin(Properties properties) {}

    @Override
    public void authenticate(Set<Object> publicCredentials,
                             Set<Object> privateCredentials,
                             Set<Principal> identifiedPrincipals)
        throws AuthenticationException
    {
        String message = "no X.509 certificate chain";

        boolean found = false;
        for (Object credential : publicCredentials) {
            if (isX509CertPath(credential)) {
                X509Certificate eec = CertPaths.getEndEntityCertificate((CertPath) credential);

                if (eec == null) {
                    message = "X.509 chain contains no End-Entity Certificate";
                } else {
                    identifiedPrincipals.add(new GlobusPrincipal(eec.getSubjectX500Principal()));

                    listPolicies(eec).stream().
                            map(PolicyInformation::getInstance).
                            map(PolicyInformation::getPolicyIdentifier).
                            map(DERObjectIdentifier::getId).
                            map(X509Plugin::asPrincipal).
                            filter(Objects::nonNull).
                            forEach(identifiedPrincipals::add);

                    found = true;
                }
            }
        }
        checkAuthentication(found, message);
    }


    private List<DEREncodable> listPolicies(X509Certificate eec)
            throws AuthenticationException
    {
        byte[] encoded;
        try {
            encoded = getExtensionBytes(eec, OID_CERTIFICATE_POLICIES);
        } catch (IOException e) {
            LOG.warn("Malformed policy extension {}: {}",
                    eec.getIssuerX500Principal().getName(), e.getMessage());
            return Collections.emptyList();
        }

        if (encoded == null) {  // has no Certificate Policies extension.
            return Collections.emptyList();
        }

        Enumeration<DEREncodable> policySource = ASN1Sequence.getInstance(encoded).getObjects();
        List<DEREncodable> policies = new ArrayList();
        while (policySource.hasMoreElements()) {
            DEREncodable policy = policySource.nextElement();
            if (!policy.equals(ANY_POLICY)) {
                policies.add(policy);
            }
        }
        return policies;
    }


    private static Principal asPrincipal(String oid)
    {
        switch (oid) {

        /*
         * IGTF LoA.  This encapsulates the LoA corresponding to existing
         * Authentication Policies (APs) described below.
         */

        case "1.2.840.113612.5.2.5.1":
            return new LoAPrincipal(IGTF_LOA_ASPEN);
        case "1.2.840.113612.5.2.5.2":
            return new LoAPrincipal(IGTF_LOA_BIRCH);
        case "1.2.840.113612.5.2.5.3":
            return new LoAPrincipal(IGTF_LOA_CEDER);
        case "1.2.840.113612.5.2.5.4":
            return new LoAPrincipal(IGTF_LOA_DOGWOOD);

        /*
         * IGTF Authentication-Policy.  Amongst other things, this defines
         * how the user was identified to the Certificate Authority, which
         * includes an element of LoA.
         */

        case "1.2.840.113612.5.2.2.1":
            return new LoAPrincipal(IGTF_AP_CLASSIC);
        case "1.2.840.113612.5.2.2.2":
            return new LoAPrincipal(IGTF_AP_SGCS);
        case "1.2.840.113612.5.2.2.3":
            return new LoAPrincipal(IGTF_AP_SLCS);
        case "1.2.840.113612.5.2.2.4":
            return new LoAPrincipal(IGTF_AP_EXPERIMENTAL);
        case "1.2.840.113612.5.2.2.5":
            return new LoAPrincipal(IGTF_AP_MICS);
        case "1.2.840.113612.5.2.2.6":
            return new LoAPrincipal(IGTF_AP_IOTA);

        /*
         * IGTF Entity-Definition.  Currently (2015-10-11), this mostly
         * identifies robot certificates.
         */

        case "1.2.840.113612.5.2.3.3.1":
            return new EntityDefinitionPrincipal(ROBOT);
        case "1.2.840.113612.5.2.3.3.2":
            return new EntityDefinitionPrincipal(HOST);
        case "1.2.840.113612.5.2.3.3.3":
            return new EntityDefinitionPrincipal(PERSON);
        }

        return null;
    }
}
