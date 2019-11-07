package org.dcache.gplazma.plugins;

import com.google.common.collect.ImmutableMap;
import com.google.common.net.InetAddresses;
import com.google.common.net.InternetDomainName;
import eu.emi.security.authn.x509.proxy.ProxyChainInfo;
import eu.emi.security.authn.x509.proxy.ProxyUtils;
import org.bouncycastle.asn1.ASN1Encodable;
import org.bouncycastle.asn1.ASN1ObjectIdentifier;
import org.bouncycastle.asn1.ASN1Sequence;
import org.bouncycastle.asn1.DERSequence;
import org.bouncycastle.asn1.x509.GeneralName;
import org.bouncycastle.asn1.x509.PolicyInformation;
import org.globus.gsi.gssapi.jaas.GlobusPrincipal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.security.Principal;
import java.security.cert.CertPath;
import java.security.cert.CertificateException;
import java.security.cert.CertificateParsingException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.dcache.auth.EmailAddressPrincipal;
import org.dcache.auth.EntityDefinition;
import org.dcache.auth.EntityDefinitionPrincipal;
import org.dcache.auth.LoA;
import org.dcache.auth.LoAPrincipal;
import org.dcache.auth.LoAs;
import org.dcache.auth.Origin;
import org.dcache.gplazma.AuthenticationException;
import org.dcache.gplazma.util.CertPaths;
import org.dcache.gplazma.util.IGTFInfoDirectory;

import static eu.emi.security.authn.x509.helpers.CertificateHelpers.getExtensionBytes;
import static org.dcache.auth.EntityDefinition.*;
import static org.dcache.auth.LoA.*;
import static org.dcache.gplazma.util.CertPaths.isX509CertPath;
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
    private static final String OID_EKU_TLS_SERVER = "1.3.6.1.5.5.7.3.1";
    private static final DERSequence ANY_POLICY = new DERSequence(new ASN1ObjectIdentifier(OID_ANY_POLICY));
    private static final Map<String,LoA> LOA_POLICIES = ImmutableMap.<String,LoA>builder()
            /*
             * IGTF LoA.  This encapsulates the LoA corresponding to existing
             * Authentication Policies (APs) described below.
             */
            .put("1.2.840.113612.5.2.5.1", IGTF_LOA_ASPEN)
            .put("1.2.840.113612.5.2.5.2", IGTF_LOA_BIRCH)
            .put("1.2.840.113612.5.2.5.3", IGTF_LOA_CEDAR)
            .put("1.2.840.113612.5.2.5.4", IGTF_LOA_DOGWOOD)
            /*
             * IGTF Authentication-Policy.  Amongst other things, this defines
             * how the user was identified to the Certificate Authority, which
             * includes an element of LoA.
             */
            .put("1.2.840.113612.5.2.2.1", IGTF_AP_CLASSIC)
            .put("1.2.840.113612.5.2.2.2", IGTF_AP_SGCS)
            .put("1.2.840.113612.5.2.2.3", IGTF_AP_SLCS)
            .put("1.2.840.113612.5.2.2.4", IGTF_AP_EXPERIMENTAL)
            .put("1.2.840.113612.5.2.2.5", IGTF_AP_MICS)
            .put("1.2.840.113612.5.2.2.6", IGTF_AP_IOTA)
            .build();

    private static final Map<String,EntityDefinition> ENTITY_DEFINITION_POLICIES
            = ImmutableMap.<String,EntityDefinition>builder()
            .put("1.2.840.113612.5.2.3.3.1", ROBOT)
            .put("1.2.840.113612.5.2.3.3.2", HOST)
            .put("1.2.840.113612.5.2.3.3.3", PERSON)
            .build();

    private static final Pattern ROBOT_CN_PATTERN = Pattern.compile("/CN=[rR]obot[^/\\p{Alnum}]");
    private static final Pattern CN_PATTERN = Pattern.compile("/CN=([^/]*)");

    private final IGTFInfoDirectory infoDirectory;

    public X509Plugin(Properties properties)
    {
        String path = properties.getProperty("gplazma.x509.igtf-info.path");
        infoDirectory = path == null ? null : new IGTFInfoDirectory(path);
    }

    @Override
    public void authenticate(Set<Object> publicCredentials,
                             Set<Object> privateCredentials,
                             Set<Principal> identifiedPrincipals)
        throws AuthenticationException
    {
        String message = "no X.509 certificate chain";

        Optional<Origin> origin = identifiedPrincipals.stream()
                .filter(Origin.class::isInstance)
                .map(Origin.class::cast)
                .findFirst();

        boolean found = false;
        for (Object credential : publicCredentials) {
            try {
                if (isX509CertPath(credential)) {
                    X509Certificate[] chain = CertPaths.getX509Certificates((CertPath) credential);

                    if (origin.isPresent() && ProxyUtils.isProxy(chain)) {
                        ProxyChainInfo info = new ProxyChainInfo(chain);
                        InetAddress address = origin.get().getAddress();
                        if (!info.isHostAllowedAsSource(address.getAddress())) {
                            message = "forbidden client address: " + InetAddresses.toAddrString(address);
                            continue;
                        }
                    }

                    X509Certificate eec = ProxyUtils.getEndUserCertificate(chain);
                    if (eec == null) {
                        message = "X.509 chain contains no End-Entity Certificate";
                        continue;
                    }

                    identifiedPrincipals.addAll(identifyPrincipalsFromEEC(eec));

                    found = true;
                }
            } catch (IOException | CertificateException e) {
                message = "broken certificate: " + e.getMessage();
            }
        }
        checkAuthentication(found, message);
    }

    private List<Principal> identifyPrincipalsFromEEC(X509Certificate eec)
            throws AuthenticationException, CertificateParsingException
    {
        Set<Principal> caPrincipals = identifyPrincipalsFromIssuer(eec);

        List<Principal> policyPrincipals = listPolicies(eec).stream()
                .map(PolicyInformation::getInstance)
                .map(PolicyInformation::getPolicyIdentifier)
                .map(ASN1ObjectIdentifier::getId)
                .flatMap(X509Plugin::oidPrincipals)
                .collect(Collectors.toList());

        Collection<List<?>> san = listSubjectAlternativeNames(eec);

        List<Principal> emailPrincipals = subjectAlternativeNameWithTag(san, GeneralName.rfc822Name)
                .filter(EmailAddressPrincipal::isValid)
                .map(EmailAddressPrincipal::new)
                .collect(Collectors.toList());

        boolean haveEntityDefn = policyPrincipals.stream()
                .anyMatch(EntityDefinitionPrincipal.class::isInstance);

        Principal subject = new GlobusPrincipal(eec.getSubjectX500Principal());

        Optional<Principal> heuristicPrincipal = haveEntityDefn
                ? Optional.empty()
                : guessEntityDefinition(eec, subject.getName(), san);

        List<Principal> principals = new ArrayList<>();
        principals.addAll(caPrincipals);
        principals.add(subject);
        principals.addAll(policyPrincipals);
        principals.addAll(emailPrincipals);
        heuristicPrincipal.ifPresent(principals::add);
        return principals;
    }

    /** Use heuristics to guess what kind of entity this certificate represents. */
    private Optional<Principal> guessEntityDefinition(X509Certificate eec,
            String subject, Collection<List<?>> san) throws CertificateParsingException
    {
        if (ROBOT_CN_PATTERN.matcher(subject).find()) {
            return Optional.of(new EntityDefinitionPrincipal(ROBOT));
        }

        if (eec.getExtendedKeyUsage().stream().anyMatch(OID_EKU_TLS_SERVER::equals)
                && (subjectAlternativeNameWithTag(san, GeneralName.dNSName).findAny().isPresent()
                || hasCnWithFqdn(subject))) {
            return Optional.of(new EntityDefinitionPrincipal(HOST));
        }

        /*
         *  Unfortunately there is no heuristic we can use to determine if the
         *  certificate is a "natural person" (PERSON).  Examples of CNs issued
         *  by CAs:
         *
         *      /CN=Alexander Paul Millar
         *      /CN=victor.mendez
         *      /CN=Jesus-Cruz-Guzman
         *      /CN=Jose.Luis.Garza.Rivera
         *
         *  In addition, some CAs add numbers to the CN in an effort to ensure
         *  the CN is unique.
         */

        return Optional.empty();
    }

    private static Stream<String> subjectAlternativeNameWithTag(Collection<List<?>> san, int tag)
            throws CertificateParsingException
    {
        return san.stream()
                .filter(n -> ((Integer) n.get(0)) == tag)
                .map(n -> n.get(1))
                .filter(Objects::nonNull)
                .map(String::valueOf);
    }

    private static boolean hasCnWithFqdn(String subject)
    {
        Matcher cnMatcher = CN_PATTERN.matcher(subject);
        while (cnMatcher.find()) {
            String cnValue = cnMatcher.group(1);
            if (InternetDomainName.isValid(cnValue)
                    && InternetDomainName.from(cnValue).hasPublicSuffix()) {
                return true;
            }
        }
        return false;
    }


    private Set<Principal> identifyPrincipalsFromIssuer(X509Certificate eec)
    {
        if (infoDirectory == null) {
            return Collections.emptySet();
        }

        // REVISIT: this assumes that issuer of EEC is the CA.  This
        //          is currently true for all IGTF CAs.
        GlobusPrincipal issuer = new GlobusPrincipal(eec.getIssuerX500Principal());

        return infoDirectory.getPrincipals(issuer);
    }

    private List<ASN1Encodable> listPolicies(X509Certificate eec)
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

        Enumeration<ASN1Encodable> policySource = ASN1Sequence.getInstance(encoded).getObjects();
        List<ASN1Encodable> policies = new ArrayList<>();
        while (policySource.hasMoreElements()) {
            ASN1Encodable policy = policySource.nextElement();
            if (!policy.equals(ANY_POLICY)) {
                policies.add(policy);
            }
        }
        return policies;
    }

    private static Collection<List<?>> listSubjectAlternativeNames(X509Certificate certificate)
            throws CertificateParsingException
    {
        Collection<List<?>> names = certificate.getSubjectAlternativeNames();
        return names == null ? Collections.emptyList() : names;
    }

    /** All principals obtained by a policy OID. */
    private static Stream<? extends Principal> oidPrincipals(String oid)
    {
        return loaOidPrincipals(oid)
                .orElseGet(() -> entityDefnPrincipal(oid)
                        .orElse(Stream.empty()));
    }

    private static Optional<Stream<? extends Principal>> entityDefnPrincipal(String oid)
    {
        return Optional.ofNullable(ENTITY_DEFINITION_POLICIES.get(oid))
            .map(EntityDefinitionPrincipal::new)
            .map(Stream::of);
    }

    private static Optional<Stream<? extends Principal>> loaOidPrincipals(String oid)
    {
        return Optional.ofNullable(LOA_POLICIES.get(oid))
                .map(EnumSet::of)
                .map(LoAs::withImpliedLoA)
                .map(c -> c.stream().map(LoAPrincipal::new));
    }
}
