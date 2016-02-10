package org.dcache.gplazma.monitor;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.bouncycastle.asn1.ASN1InputStream;
import org.bouncycastle.asn1.ASN1ObjectIdentifier;
import org.bouncycastle.asn1.ASN1OctetString;
import org.bouncycastle.asn1.ASN1Sequence;
import org.bouncycastle.asn1.x509.AttributeCertificate;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.asn1.x509.Extensions;
import org.italiangrid.voms.VOMSAttribute;
import org.italiangrid.voms.asn1.VOMSACUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.security.rsa.RSAPublicKeyImpl;

import javax.security.auth.x500.X500Principal;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.Principal;
import java.security.PublicKey;
import java.security.cert.CertPath;
import java.security.cert.CertificateParsingException;
import java.security.cert.X509Certificate;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.EnumSet;
import java.util.Enumeration;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.Stack;

import org.dcache.auth.FQAN;
import org.dcache.gplazma.configuration.ConfigurationItemControl;
import org.dcache.gplazma.monitor.LoginMonitor.Result;
import org.dcache.gplazma.monitor.LoginResult.AccountPhaseResult;
import org.dcache.gplazma.monitor.LoginResult.AccountPluginResult;
import org.dcache.gplazma.monitor.LoginResult.AuthPhaseResult;
import org.dcache.gplazma.monitor.LoginResult.AuthPluginResult;
import org.dcache.gplazma.monitor.LoginResult.MapPhaseResult;
import org.dcache.gplazma.monitor.LoginResult.MapPluginResult;
import org.dcache.gplazma.monitor.LoginResult.PAMPluginResult;
import org.dcache.gplazma.monitor.LoginResult.PhaseResult;
import org.dcache.gplazma.monitor.LoginResult.SessionPhaseResult;
import org.dcache.gplazma.monitor.LoginResult.SessionPluginResult;
import org.dcache.gplazma.monitor.LoginResult.SetDiff;
import org.dcache.gplazma.util.CertPaths;

import static java.util.concurrent.TimeUnit.*;
import static org.dcache.gplazma.configuration.ConfigurationItemControl.*;
import static org.dcache.gplazma.monitor.LoginMonitor.Result.FAIL;
import static org.dcache.gplazma.monitor.LoginMonitor.Result.SUCCESS;
import static org.dcache.util.Bytes.toHexString;



/**
 * This class takes a LoginResult and provides an ASCII-art description
 * of the process.
 */
public class LoginResultPrinter
{
    private static final Logger _log =
            LoggerFactory.getLogger(LoginResultPrinter.class);

    private static final String ATTRIBUTE_CERTIFICATE_OID =
            "1.3.6.1.4.1.8005.100.100.5";

    private static final String VOMS_CERTIFICATES_OID =
            "1.3.6.1.4.1.8005.100.100.10";

    private static final ImmutableMap<String, String> OID_TO_NAME =
            new ImmutableMap.Builder<String, String>()
                    .put("1.2.840.113549.1.1.4", "MD5 with RSA")
                    .put("1.2.840.113549.1.1.5", "SHA-1 with RSA")
                    .put("1.2.840.113549.1.1.11", "SHA-256 with RSA")
                    .put("1.2.840.113549.1.1.13", "SHA-512 with RSA")
                    .put("2.16.840.1.101.3.4.2.1", "SHA-256")
                    .put("2.16.840.1.101.3.4.2.2", "SHA-384")
                    .put("2.16.840.1.101.3.4.2.3", "SHA-512")

                    // Various extended key usage OIDs
                    .put("1.3.6.1.5.5.7.3.1", "SSL server")
                    .put("1.3.6.1.5.5.7.3.2", "SSL client")
                    .put("1.3.6.1.5.5.7.3.3", "code signing")
                    .put("1.3.6.1.5.5.7.3.4", "email protection")
                    .put("1.3.6.1.5.5.7.3.5", "IPSec end system")
                    .put("1.3.6.1.5.5.7.3.6", "IPSec tunnel")
                    .put("1.3.6.1.5.5.7.3.7", "IPSec user")
                    .put("1.3.6.1.5.5.7.3.8", "time stamp")
                    .put("1.3.6.1.5.5.7.3.9", "OCSP signing")
                    .put("1.3.6.1.4.1.311.10.3.4", "Microsoft EPS")

                    // Certificate extensions
                    .put("1.3.6.1.4.1.8005.100.100.4", "VOMS FQANs")
                    .put(ATTRIBUTE_CERTIFICATE_OID, "VOMS AC")
                    .put(VOMS_CERTIFICATES_OID, "VOMS certificates")
                    .put("1.3.6.1.4.1.8005.100.100.11", "VOMS generic attributes")
                    .put("2.5.29.35", "Authority key identifier")
                    .put("2.5.29.56", "No revocation info")
                    .build();

    private static final ImmutableList<String> BASIC_KEY_USAGE_LABELS =
            new ImmutableList.Builder<String>()
            .add("digital signature")
            .add("non-repudiation")
            .add("key encipherment")
            .add("data encipherment")
            .add("key agreement")
            .add("key certificate signing")
            .add("CRL signing")
            .add("encipher only")
            .add("decipher only")
            .build();

    private static final Collection<ConfigurationItemControl> ALWAYS_OK =
            EnumSet.of(OPTIONAL, SUFFICIENT);

    private final LoginResult _result;
    private StringBuilder _sb;

    public LoginResultPrinter(LoginResult result)
    {
        _result = result;
    }

    public String print()
    {
        _sb = new StringBuilder();
        printInitialPart();
        printPhase(_result.getAuthPhase());
        printPhase(_result.getMapPhase());
        printPhase(_result.getAccountPhase());
        printPhase(_result.getSessionPhase());
        printValidation();
        return _sb.toString();
    }


    private void printInitialPart()
    {
        Result result = getOverallResult();
        _sb.append("LOGIN ").append(stringFor(result)).append("\n");

        printLines(" in", buildInLines());
        printLines("out", buildOutItems());

        _sb.append(" |\n");
    }


    private void printLines(String label, List<String> lines)
    {
        boolean isFirst = true;

        for(String line : lines) {
            _sb.append(" |   ");

            if(isFirst) {
                _sb.append(label).append(": ");
            } else {
                _sb.append("     ");
            }
            _sb.append(line).append("\n");
            isFirst = false;
        }
    }



    private List<String> buildInLines()
    {
        List<String> lines = new LinkedList<>();

        for(Principal principal : initialPrincipals()) {
            lines.add(principal.toString());
        }

        for(Object credential : publicCredentials()) {
            String display = print(credential);
            for(String line : Splitter.on('\n').split(display)) {
                lines.add(line);
            }
        }

        for(Object credential : privateCredentials()) {
            String display = print(credential);
            for(String line : Splitter.on('\n').split(display)) {
                lines.add(line);
            }
        }

        return lines;
    }

    private String print(Object credential)
    {
        if (CertPaths.isX509CertPath(credential)) {
            return print(CertPaths.getX509Certificates((CertPath) credential));
        } else {
            return credential.toString();
        }
    }

    private String print(X509Certificate[] certificates)
    {
        StringBuilder sb = new StringBuilder();

        sb.append("X509 Certificate chain:\n");

        int i = 1;
        for(X509Certificate certificate : certificates) {
            boolean isLastCertificate = i == certificates.length;
            sb.append("  |\n");
            String certDetails = print(certificate);
            boolean isFirstLine = true;
            for(String line : Splitter.on('\n').omitEmptyStrings().split(certDetails)) {
                if(isFirstLine) {
                    sb.append("  +--");
                } else if(!isLastCertificate) {
                    sb.append("  |  ");
                } else {
                    sb.append("     ");
                }
                sb.append(line).append('\n');
                isFirstLine = false;
            }
            i++;
        }

        return sb.toString();
    }

    private String print(X509Certificate certificate)
    {
        StringBuilder sb = new StringBuilder();

        String subject = certificate.getSubjectX500Principal().getName(X500Principal.RFC2253);
        String issuer = certificate.getIssuerX500Principal().getName(X500Principal.RFC2253);

        boolean isSelfSigned = subject.equals(issuer);

        sb.append(String.format("%s [%d]", subject, certificate.getSerialNumber()));
        if(isSelfSigned) {
            sb.append(" (self-signed)");
        }
        sb.append('\n');

        sb.append("  |\n");
        if(!isSelfSigned) {
            sb.append("  +--Issuer: ").append(issuer).append('\n');
        }
        sb.append("  +--Validity: ").append(validityStatementFor(certificate)).append('\n');
        sb.append("  +--Algorithm: ").append(nameForOid(certificate.getSigAlgOID())).append('\n');
        sb.append("  +--Public key: ").append(describePublicKey(certificate.getPublicKey())).append('\n');

        String sanInfo = subjectAlternateNameInfoFor(certificate);
        if(!sanInfo.isEmpty()) {
            sb.append("  +--Subject alternative names:");

            if(isSingleLine(sanInfo)) {
                sb.append(" ").append(sanInfo).append('\n');
            } else {
                sb.append('\n');
                for(String line : Splitter.on('\n').omitEmptyStrings().split(sanInfo)) {
                    sb.append("  |      ").append(line).append('\n');
                }
            }
        }

        String vomsInfo = vomsInfoFor(certificate);

        String extendedKeyUsage = extendedKeyUsageFor(certificate);

        if(!vomsInfo.isEmpty()) {
            sb.append("  +--Attribute certificates:");

            if(isSingleLine(vomsInfo)) {
                sb.append(" ").append(vomsInfo).append('\n');
            } else {
                sb.append('\n');
                for(String line : Splitter.on('\n').omitEmptyStrings().split(vomsInfo)) {
                    if(extendedKeyUsage.isEmpty()) {
                        sb.append("     ");
                    } else {
                        sb.append("  |  ");
                    }

                    sb.append(line).append('\n');
                }
            }
        }

        if(!extendedKeyUsage.isEmpty()) {
            sb.append("  +--Key usage: ").append(extendedKeyUsage).append('\n');
        }

        return sb.toString();
    }

    private static String describePublicKey(PublicKey key)
    {
        StringBuilder sb = new StringBuilder();

        sb.append(key.getAlgorithm());

        if (key instanceof RSAPublicKeyImpl) {
            int bits = (((RSAPublicKeyImpl)key).getModulus().bitLength() + 7) & ~7;
            sb.append(' ').append(bits).append(" bits");
        } else {
            sb.append(" (unknown ").append(key.getClass().getCanonicalName()).
                    append(")");
        }

        return sb.toString();
    }

    private static String subjectAlternateNameInfoFor(X509Certificate certificate)
    {
        StringBuilder sb = new StringBuilder();

        Collection<List<?>> nameLists;
        try {
            nameLists = certificate.getSubjectAlternativeNames();
        } catch (CertificateParsingException e) {
            return "problem (" + e.getMessage() + ")";
        }

        if(nameLists == null) {
            return "";
        }

        boolean isFirst = true;
        for(List<?> nameList : nameLists) {
            int tag = (Integer) nameList.get(0);
            Object object = nameList.get(1);
            byte[] bytes;

            if(!isFirst) {
                sb.append('\n');
            }

            // Tags 0--8 are defined in RFC 5280
            switch(tag) {
                case 0: // otherName, OtherName
                    bytes = (byte[]) object;
                    sb.append("otherName: ").append(toHexString(bytes));
                    break;

                case 1: // rfc822Name, IA5String
                    sb.append("email: ").append(object);
                    break;

                case 2: // dNSName, IA5String
                    sb.append("DNS: ").append(object);
                    break;

                case 3: // x400Address, ORAddress
                    bytes = (byte[]) object;
                    sb.append("x400: ").append(toHexString(bytes));
                    break;

                case 4: // directoryName, Name
                    sb.append("dirName: ").append(object);
                    break;

                case 5: // ediPartyName, EDIPartyName
                    bytes = (byte[]) object;
                    sb.append("EDI party name: ").append(toHexString(bytes));
                    break;

                case 6: // uniformResourceIdentifier, IA5String
                    sb.append("URI: ").append(String.valueOf(object));
                    break;

                case 7: // iPAddress, OCTET STRING
                    sb.append("IP: ").append(String.valueOf(object));
                    break;

                case 8: // registeredID, OBJECT IDENTIFIER
                    bytes = (byte[]) object;
                    sb.append("oid: ").append(toHexString(bytes));
                    break;

                default:
                    bytes = (byte[]) object;
                    // Include sufficient information to build a better
                    // representation for currently unknown tags
                    sb.append(object.getClass().getSimpleName());
                    sb.append(" [").append(tag).append("]");
                    sb.append(" ").append(toHexString(bytes));
                    break;
            }

            isFirst = false;
        }

        return sb.toString();
    }

    private String vomsInfoFor(X509Certificate x509Certificate)
    {
        List<AttributeCertificate> certificates;

        try {
             certificates =
                    extractAttributeCertificates(x509Certificate);
        } catch (IOException e) {
            return "problem (" + e.getMessage() + ")";
        }

        StringBuilder sb = new StringBuilder();

        int i = 1;
        for(AttributeCertificate certificate : certificates) {
            boolean isLastCertificate = i == certificates.size();
            String info = attributeCertificateInfoFor(certificate);
            boolean isFirstLine = true;
            for(String line : Splitter.on('\n').omitEmptyStrings().split(info)) {
                if( isFirstLine) {
                    sb.append("  |\n");
                    sb.append("  +--");
                    isFirstLine = false;
                } else if(isLastCertificate) {
                    sb.append("     ");
                } else {
                    sb.append("  |  ");
                }
                sb.append(line).append('\n');
            }

            i++;
        }

        return sb.toString();
    }

    private static List<AttributeCertificate> extractAttributeCertificates(X509Certificate certificate) throws IOException
    {
        List<AttributeCertificate> certificates =
                new ArrayList<>();

        byte[] payload = certificate.getExtensionValue(ATTRIBUTE_CERTIFICATE_OID);

        if(payload == null) {
            return Collections.emptyList();
        }

        payload = decodeEncapsulation(payload);

        InputStream in = new ByteArrayInputStream(payload);
        ASN1Sequence acSequence =
                (ASN1Sequence) new ASN1InputStream(in).readObject();

        for(Enumeration<ASN1Sequence> e1=acSequence.getObjects();
                e1.hasMoreElements(); ) {
            ASN1Sequence acSequence2 = e1.nextElement();

            for(Enumeration<ASN1Sequence> e2=acSequence2.getObjects();
                    e2.hasMoreElements(); ) {
                ASN1Sequence acSequence3 = e2.nextElement();
                certificates.add(AttributeCertificate.getInstance(acSequence3));
            }
        }

        return certificates;
    }

    /**
     * Octet String encapsulation - see RFC 3280 section 4.1
     */
    private static byte[] decodeEncapsulation(byte[] payload) throws IOException
    {
        ASN1InputStream payloadStream =
                new ASN1InputStream(new ByteArrayInputStream(payload));
        return ((ASN1OctetString) payloadStream.readObject()).getOctets();
    }


    private String attributeCertificateInfoFor(AttributeCertificate certificate)
    {
        VOMSAttribute attribute = VOMSACUtils.deserializeVOMSAttributes(certificate);

        StringBuilder sb = new StringBuilder();
        sb.append(attribute.getIssuer().getName(X500Principal.RFC2253)).append('\n');
        sb.append("  +--Validity: ").append(validityStatementFor(certificate)).append('\n');

        Extensions extensions = certificate.getAcinfo().getExtensions();
        if (extensions != null) {
            ASN1ObjectIdentifier[] ids = extensions.getExtensionOIDs();
            if (ids != null && ids.length != 0) {
                sb.append("  +--Extensions:\n");
                sb.append("  |    |\n");
                int index = 1;
                for (ASN1ObjectIdentifier id : ids) {
                    boolean isLast = index == ids.length;
                    Extension e = extensions.getExtension(id);
                    String padding = isLast ? "  |       " : "  |    |  ";
                    sb.append(extensionInfoFor(id, e, attribute, padding));
                    index++;
                }
            }
        }

        String oid = certificate.getSignatureAlgorithm().getAlgorithm().getId();
        sb.append("  +--Algorithm: ").append(nameForOid(oid)).append('\n');

        String fqanInfo = fqanInfoFor(attribute);
        if(!fqanInfo.isEmpty()) {
            sb.append("  +--FQANs: ").append(fqanInfo).append('\n');
        }

        return sb.toString();
    }

    private static StringBuilder extensionInfoFor(ASN1ObjectIdentifier id,
            Extension extension, VOMSAttribute attribute, String padding)
    {
        StringBuilder sb = new StringBuilder();
        if (VOMS_CERTIFICATES_OID.equals(id.toString())) {
            X509Certificate[] chain = attribute.getAACertificates();
            if (chain == null) {
                sb.append("  |    +--Issuer chain: missing\n");
            } else {
                switch (chain.length) {
                case 0:
                    sb.append("  |    +--Issuer chain: empty\n");
                    break;
                case 1:
                    String singleDn = chain[0].getIssuerX500Principal().getName(X500Principal.RFC2253);
                    sb.append("  |    +--Issuer: ").append(singleDn).append('\n');
                    break;
                default:
                    sb.append("  |    +--Issuer chain:\n");
                    sb.append(padding).append("   |\n");

                    for (X509Certificate certificate : chain) {
                        String thisDn = certificate.getIssuerX500Principal().getName(X500Principal.RFC2253);
                        sb.append(padding).append("   +--").append(thisDn).append('\n');
                    }
                }
            }
        } else {
            sb.append("  |    +--").append(nameForOid(id.toString()));
            if (extension.isCritical()) {
                sb.append(" [CRITICAL]");
            }
            sb.append('\n');
        }

        return sb;
    }

    private static String fqanInfoFor(VOMSAttribute attribute)
    {
        List<String> fqans = attribute.getFQANs();

        if(fqans.size() > 0) {
            StringBuilder sb = new StringBuilder();

            FQAN fqan = new FQAN(String.valueOf(fqans.get(0)));
            sb.append(fqan);

            if(fqans.size() > 1) {
                FQAN fqan2 = new FQAN(String.valueOf(fqans.get(1)));
                sb.append(", ").append(fqan2);

                if(fqans.size() > 2) {
                    sb.append(", ...");
                }
            }
            return sb.toString();
        } else {
            return "";
        }
    }

    private static String nameForOid(String oid)
    {
        String name = OID_TO_NAME.get(oid);
        if(name == null) {
            name = oid;
        }
        return name;
    }





    private String extendedKeyUsageFor(X509Certificate certificate)
    {
        List<String> labels = new LinkedList<>();

        try {
            boolean usageAllowed[] = certificate.getKeyUsage();
            if(usageAllowed != null) {
                int i = 0;
                for(String usageLabel : BASIC_KEY_USAGE_LABELS) {
                    if(usageAllowed[i]) {
                        labels.add(usageLabel);
                    }
                    i++;
                }
            }

            List<String> extendedUses = certificate.getExtendedKeyUsage();
            if(extendedUses != null) {
                for(String use : extendedUses) {
                    labels.add(nameForOid(use));
                }
            }

            return Joiner.on(", ").join(labels);
        } catch (CertificateParsingException e) {
            return "parsing problem (" + e.getMessage() + ")";
        }
    }

    private String validityStatementFor(X509Certificate certificate)
    {
        Date notBefore = certificate.getNotBefore();
        Date notAfter = certificate.getNotAfter();
        return validityStatementFor(notBefore, notAfter);
    }

    private String validityStatementFor(AttributeCertificate certificate)
    {
        try {
            Date notBefore = certificate.getAcinfo().getAttrCertValidityPeriod().getNotBeforeTime().getDate();
            Date notAfter = certificate.getAcinfo().getAttrCertValidityPeriod().getNotAfterTime().getDate();
            return validityStatementFor(notBefore, notAfter);
        } catch(ParseException e) {
            return "problem parsing validity info (" + e.getMessage() + ")";
        }
    }

    private String validityStatementFor(Date notBefore, Date notAfter)
    {
        Date now = new Date();

        if(now.after(notAfter)) {
            return validityStatementFor("expired ",
                    now.getTime() - notAfter.getTime(), " ago");
        } else if(now.before(notBefore)) {
            return validityStatementFor("not yet, in ",
                    notBefore.getTime() - now.getTime());
        } else {
            return validityStatementFor("OK for ",
                    notAfter.getTime() - now.getTime());
        }
    }

    private String validityStatementFor(String prefix, long milliseconds)
    {
        return validityStatementFor(prefix, milliseconds, "");
    }

    private String validityStatementFor(String prefix, long milliseconds, String suffix)
    {
        long days = MILLISECONDS.toDays(milliseconds);
        milliseconds -= DAYS.toMillis(days);
        long hours = MILLISECONDS.toHours(milliseconds);
        milliseconds -= HOURS.toMillis(hours);
        long minutes = MILLISECONDS.toMinutes(milliseconds);
        milliseconds -= MINUTES.toMillis(minutes);
        long seconds = MILLISECONDS.toSeconds(milliseconds);
        milliseconds -= SECONDS.toMillis(seconds);

        StringBuilder sb = new StringBuilder();

        sb.append(prefix);

        Stack<String> phrases = new Stack<>();

        if(seconds > 0 || milliseconds > 0) {
            double secondsAndMillis = seconds + milliseconds / 1000.0;
            phrases.push(String.format("%.1f seconds", secondsAndMillis));
        }

        if(minutes > 0) {
            phrases.push(buildPhrase(minutes, "minute"));
        }

        if(hours > 0) {
            phrases.push(buildPhrase(hours, "hour"));
        }

        if(days > 0) {
            phrases.push(buildPhrase(days, "day"));
        }

        while(!phrases.isEmpty()) {
            sb.append(phrases.pop());
            if(phrases.size() > 1) {
                sb.append(", ");
            } else if(phrases.size() == 1) {
                sb.append(" and ");
            }
        }

        sb.append(suffix);

        return sb.toString();
    }

    private String buildPhrase(long count, String scalar)
    {
        if(count == 1) {
            return "1 " + scalar;
        } else {
            return count + " " + scalar + "s";
        }
    }


    private Set<Principal> initialPrincipals()
    {
        Set<Principal> principal;

        AuthPhaseResult auth = _result.getAuthPhase();
        if(auth.hasHappened()) {
            principal = auth.getPrincipals().getBefore();
        } else {
            principal = Collections.emptySet();
        }

        return principal;
    }

    private Set<Object> publicCredentials()
    {
        AuthPhaseResult auth = _result.getAuthPhase();
        return auth.getPublicCredentials();
    }

    private Set<Object> privateCredentials()
    {
        AuthPhaseResult auth = _result.getAuthPhase();
        return auth.getPrivateCredentials();
    }

    private List<String> buildOutItems()
    {
        List<String> labels = new LinkedList<>();

        for(Principal principal : finalPrincipals()) {
            labels.add(principal.toString());
        }

        return labels;
    }



    private Set<Principal> finalPrincipals()
    {
        SessionPhaseResult session = _result.getSessionPhase();
        Set<Principal> principals;
        if(session.hasHappened()) {
            principals = session.getPrincipals().getAfter();
        } else {
            principals = Collections.emptySet();
        }

        return principals;
    }


    private <T extends PAMPluginResult> void printPhase(PhaseResult<T> result)
    {
        if(result.hasHappened()) {
            _sb.append(String.format(" +--%s %s\n", result.getName(),
                    stringFor(result.getResult())));
            printPrincipalsDiff(" |   |  ", result.getPrincipals());

            int count = result.getPluginResults().size();

            if(count > 0) {
                _sb.append(" |   |\n");

                int index = 1;
                for(T plugin : result.getPluginResults()) {
                    boolean isLast = index == count;
                    printPlugin(plugin, isLast);
                    index++;
                }
            }

            _sb.append(" |\n");

        } else {
            _sb.append(" +--(").append(result.getName()).append(") skipped\n");
            _sb.append(" |\n");
        }
    }

    private void printPlugin(PAMPluginResult result, boolean isLast)
    {
        printPluginHeader(result);

        if(result instanceof AuthPluginResult) {
            printPluginBehaviour((AuthPluginResult) result, isLast);
        } else if(result instanceof MapPluginResult) {
            printPluginBehaviour((MapPluginResult) result, isLast);
        } else if(result instanceof AccountPluginResult) {
            printPluginBehaviour((AccountPluginResult) result, isLast);
        } else if(result instanceof SessionPluginResult) {
            printPluginBehaviour((SessionPluginResult) result, isLast);
        } else {
            throw new IllegalArgumentException("unknown type of plugin " +
                    "result: " + result.getClass().getCanonicalName());
        }

        if(!isLast) {
            _sb.append(" |   |\n");
        }
    }


    private void printPluginBehaviour(AuthPluginResult plugin, boolean isLast)
    {
        String prefix = isLast ? " |        " : " |   |    ";
        printPrincipalsDiff(prefix, plugin.getIdentified());
    }

    private void printPluginBehaviour(MapPluginResult plugin, boolean isLast)
    {
        String prefix = isLast ? " |        " : " |   |    ";
        printPrincipalsDiff(prefix, plugin.getPrincipals());
    }

    private void printPluginBehaviour(AccountPluginResult plugin, boolean isLast)
    {
        String prefix = isLast ? " |        " : " |   |    ";
        printPrincipalsDiff(prefix, plugin.getAuthorized());
    }

    private void printPluginBehaviour(SessionPluginResult plugin, boolean isLast)
    {
        String prefix = isLast ? " |        " : " |   |    ";
        printPrincipalsDiff(prefix, plugin.getAuthorized());
    }

    private void printValidation()
    {
        if(_result.hasValidationHappened()) {
            Result result = _result.getValidationResult();
            String label = stringFor(_result.getValidationResult());
            _sb.append(" +--VALIDATION ").append(label);
            if(result == Result.FAIL) {
                _sb.append(" (").append(_result.getValidationError()).append(")");
            }
            _sb.append('\n');
        } else {
            _sb.append(" +--(VALIDATION) skipped\n");
        }
    }


    private void printPluginHeader(PAMPluginResult plugin)
    {
        ConfigurationItemControl control = plugin.getControl();
        Result result = plugin.getResult();
        String resultLabel = stringFor(result);
        String name = plugin.getName();
        String error;
        if(result == SUCCESS) {
            error = "";
        } else {
            error = " (" + plugin.getError() + ")";
        }
        _sb.append(String.format(" |   +--%s %s:%s%s => %s", name,
                plugin.getControl().name(), resultLabel, error,
                ALWAYS_OK.contains(control) ? "OK" : resultLabel));

        if((result == SUCCESS && control == SUFFICIENT) ||
                (result == FAIL && control == REQUISITE)) {
            _sb.append(" (ends the phase)");
        }

        _sb.append('\n');
    }


    private Result getOverallResult()
    {
        AuthPhaseResult auth = _result.getAuthPhase();
        MapPhaseResult map = _result.getMapPhase();
        AccountPhaseResult account = _result.getAccountPhase();
        SessionPhaseResult session = _result.getSessionPhase();

        boolean success = auth.hasHappened() && auth.getResult() == SUCCESS &&
                map.hasHappened() && map.getResult() == SUCCESS &&
                account.hasHappened() && account.getResult() == SUCCESS &&
                session.hasHappened() && session.getResult() == SUCCESS &&
                _result.getValidationResult() == SUCCESS;

        return success ? SUCCESS : FAIL;
    }


    private String stringFor(Result result)
    {
        return result == SUCCESS ? "OK" : "FAIL";
    }

    private void printPrincipalsDiff(String prefix, SetDiff<Principal> diff)
    {
        if(diff == null) {
            return;
        }

        Set<Principal> added = diff.getAdded();

        boolean isFirst = true;
        for(Principal principal : added) {
            if(isFirst) {
                _sb.append(prefix).append("  added: ");
                isFirst = false;
            } else {
                _sb.append(prefix).append("         ");
            }
            _sb.append(principal).append('\n');
        }

        Set<Principal> removed = diff.getRemoved();

        isFirst = true;
        for(Principal principal : removed) {
            if(isFirst) {
                _sb.append(prefix).append("removed: ");
                isFirst = false;
            } else {
                _sb.append(prefix).append("         ");
            }
            _sb.append(principal).append('\n');
        }
    }

    private static boolean isSingleLine(String data)
    {
        return !data.contains("\n");
    }
}
