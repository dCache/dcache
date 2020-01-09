package org.dcache.gplazma.plugins;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Streams;
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
import java.util.Arrays;
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

    private static final int SHORTEST_LOA_POLICY_OID = LOA_POLICIES.keySet().stream()
            .mapToInt(String::length)
            .min().orElse(0);

    private static final Map<String,EntityDefinition> ENTITY_DEFINITION_POLICIES
            = ImmutableMap.<String,EntityDefinition>builder()
            .put("1.2.840.113612.5.2.3.3.1", ROBOT)
            .put("1.2.840.113612.5.2.3.3.2", HOST)
            .put("1.2.840.113612.5.2.3.3.3", PERSON)
            .build();

    private static final Set<LoA> IGTF_AP = EnumSet.of(IGTF_AP_CLASSIC,
            IGTF_AP_SGCS, IGTF_AP_SLCS, IGTF_AP_EXPERIMENTAL, IGTF_AP_MICS,
            IGTF_AP_IOTA);

    private static final Set<LoA> IGTF_LOA = EnumSet.of(IGTF_LOA_ASPEN,
            IGTF_LOA_BIRCH, IGTF_LOA_CEDAR, IGTF_LOA_DOGWOOD);

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

        List<String> policies = listPolicies(eec);

        List<Principal> loaPrincipals = policies.stream()
                .flatMap(X509Plugin::loaPrincipals)
                .collect(Collectors.toList());

        Collection<List<?>> san = listSubjectAlternativeNames(eec);

        List<Principal> emailPrincipals = subjectAlternativeNamesWithTag(san, GeneralName.rfc822Name)
                .filter(EmailAddressPrincipal::isValid)
                .map(EmailAddressPrincipal::new)
                .collect(Collectors.toList());

        Principal subject = new GlobusPrincipal(eec.getSubjectX500Principal());

        Optional<EntityDefinition> entity = identifyEntityDefinition(eec, policies,
                subject, san);

        List<Principal> principals = new ArrayList<>();
        principals.addAll(caPrincipals);
        principals.addAll(loaPrincipals);

        principals = filterOutErroneousLoAs(subject, principals);
        addImpliedLoA(entity, principals);

        principals.add(subject);
        principals.addAll(emailPrincipals);
        entity.map(EntityDefinitionPrincipal::new).ifPresent(principals::add);

        return principals;
    }

    private List<Principal> filterOutErroneousLoAs(Principal subject, List<Principal> principals)
    {
        EnumSet<LoA> assertedLoAs = assertedLoAs(principals);

        EnumSet<LoA> assertedIgtfAp = EnumSet.copyOf(assertedLoAs);
        assertedIgtfAp.retainAll(IGTF_AP);

        Optional<Stream<Principal>> filteredPrincipals = Optional.empty();
        if (assertedIgtfAp.size() > 1) {
            LOG.warn("Suppressing IGTF AP principals for \"{}\" as an incompatible"
                    + " set is asserted: {}", subject.getName(), assertedIgtfAp);
            filteredPrincipals = Optional.of(principals.stream().filter(p -> !(p instanceof LoAPrincipal && IGTF_AP.contains(((LoAPrincipal)p).getLoA()))));
        }

        EnumSet<LoA> assertedIgtfLoAs = EnumSet.copyOf(assertedLoAs);
        assertedIgtfLoAs.retainAll(IGTF_LOA);

        if (assertedIgtfLoAs.size() > 1) {
            LOG.warn("Suppressing IGTF LoA principals for \"{}\" as an incompatible"
                    + " set is asserted: {}", subject.getName(), assertedIgtfLoAs);
            filteredPrincipals = Optional.of(filteredPrincipals.orElse(principals.stream())
                    .filter(p -> !(p instanceof LoAPrincipal && IGTF_LOA.contains(((LoAPrincipal)p).getLoA()))));
        }

        return filteredPrincipals.map(s -> s.collect(Collectors.toList())).orElse(principals);
    }

    private static EnumSet<LoA> assertedLoAs(Collection<Principal> principals)
    {
        return principals.stream().filter(LoAPrincipal.class::isInstance)
                .map(LoAPrincipal.class::cast)
                .map(LoAPrincipal::getLoA)
                .collect(Collectors.toCollection(() -> EnumSet.noneOf(LoA.class)));
    }

    private Optional<EntityDefinition> identifyEntityDefinition(X509Certificate eec,
            List<String> policies, Principal subject, Collection<List<?>> san)
            throws CertificateParsingException
    {
        Set<EntityDefinition> assertedEntityDefn = policies.stream()
                .map(ENTITY_DEFINITION_POLICIES::get)
                .filter(Objects::nonNull)
                .collect(Collectors.toCollection(() -> EnumSet.noneOf(EntityDefinition.class)));

        if (assertedEntityDefn.size() == 1) {
            EntityDefinition entity = assertedEntityDefn.iterator().next();
            return Optional.of(entity);
        }

        if (assertedEntityDefn.size() > 1) {
            LOG.warn("Multiple EntityTypes asserted: {}", assertedEntityDefn);
            return Optional.empty();
        }

        // No entity asserted as a policy, let's try to guess...

        return guessEntityDefinition(eec, subject.getName(), san);

    }

    /** Use heuristics to guess what kind of entity this certificate represents. */
    private Optional<EntityDefinition> guessEntityDefinition(X509Certificate eec,
            String subject, Collection<List<?>> san) throws CertificateParsingException
    {
        if (ROBOT_CN_PATTERN.matcher(subject).find()) {
            return Optional.of(ROBOT);
        }

        List<String> eku = eec.getExtendedKeyUsage();
        if (eku != null && eku.stream().anyMatch(OID_EKU_TLS_SERVER::equals)
                && (hasSubjectAlternativeNameOfType(san, GeneralName.dNSName)
                || hasCnWithFqdn(subject))) {
            return Optional.of(HOST);
        }

        /*
         *  Unfortunately there is no good heuristic we can use to determine if
         *  the certificate is a "natural person" (PERSON).  Examples of CNs
         *  issued by CAs:
         *
         *      /CN=Alexander Paul Millar
         *      /CN=victor.mendez
         *      /CN=Jesus-Cruz-Guzman
         *      /CN=Jose.Luis.Garza.Rivera
         *
         *  In addition, some CAs append numbers or email addresses to the
         *  person's name in an effort to ensure the CN is unique.
         *
         *  Further, although some (but not all) CAs include an email address
         *  as a SubjectAlternativeName, some CAs include an email address as
         *  a SubjectAlternativeName in HOST certificates.
         *
         *  Perhaps the best we can do is require that a PERSON certificate has
         *  no DNS, IP-address or URI SujectAlternativeName entry, and that we
         *  haven't already identified the entity as a ROBOT or a HOST.
         */

        if (!hasSubjectAlternativeNameOfType(san, GeneralName.dNSName,
                GeneralName.iPAddress, GeneralName.uniformResourceIdentifier)) {
            return Optional.of(PERSON);
        }

        return Optional.empty();
    }

    private static boolean hasSubjectAlternativeNameOfType(Collection<List<?>> san,
            int... tags)
    {
        return san.stream()
                .mapToInt(n -> ((Integer) n.get(0)))
                .anyMatch(t -> Arrays.stream(tags).anyMatch(t2 -> t2 == t));
    }

    private static Stream<String> subjectAlternativeNamesWithTag(Collection<List<?>> san,
            int tag)
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

    private List<String> listPolicies(X509Certificate eec)
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

        Enumeration<ASN1Encodable> asn1EncodedPolicies = ASN1Sequence.getInstance(encoded).getObjects();
        List<String> policies = new ArrayList<>();
        while (asn1EncodedPolicies.hasMoreElements()) {
            ASN1Encodable asn1EncodedPolicy = asn1EncodedPolicies.nextElement();
            if (asn1EncodedPolicy.equals(ANY_POLICY)) {
                continue;
            }
            PolicyInformation policy = PolicyInformation.getInstance(asn1EncodedPolicy);
            policies.add(policy.getPolicyIdentifier().getId());
        }
        return policies;

    }

    private static Collection<List<?>> listSubjectAlternativeNames(X509Certificate certificate)
            throws CertificateParsingException
    {
        Collection<List<?>> names = certificate.getSubjectAlternativeNames();
        return names == null ? Collections.emptyList() : names;
    }

    /** A stream of LoA principals obtained directly from a policy OID. */
    private static Stream<LoAPrincipal> loaPrincipals(String oid)
    {
        Optional<LoAPrincipal> principal = loaFromOid(oid).map(LoAPrincipal::new);
        return Streams.stream(principal);
    }

    private static Optional<LoA> loaFromOid(String oid)
    {
        LoA loa = LOA_POLICIES.get(oid);
        if (loa == null) {
            /*
             * Some CAs assert the version-specific OID; e.g.,
             * when asserting IGTF_AP_IOTA, the CA adds
             * 1.2.840.113612.5.2.2.6.1 for version 1 of the document, rather
             * than the more generic 1.2.840.113612.5.2.2.6.
             */
            int lastDot = oid.lastIndexOf('.');
            if (lastDot >= SHORTEST_LOA_POLICY_OID) {
                String oidWithoutLastDot = oid.substring(0, lastDot);
                loa = LOA_POLICIES.get(oidWithoutLastDot);
            }
        }
        return Optional.ofNullable(loa);
    }

    /** Add all implied LoAs, given the LoA assertions so far. */
    private static void addImpliedLoA(Optional<EntityDefinition> entity,
            Collection<Principal> principals)
    {
        EnumSet<LoA> assertedLoAs = assertedLoAs(principals);

        LoAs.withImpliedLoA(entity, assertedLoAs).stream()
                .filter(l -> !assertedLoAs.contains(l))
                .map(LoAPrincipal::new)
                .forEach(principals::add);
    }
}
