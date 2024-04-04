/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2014 - 2024 Deutsches Elektronen-Synchrotron
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.dcache.gsi;

import eu.emi.security.authn.x509.X509Credential;
import eu.emi.security.authn.x509.helpers.proxy.DraftRFCProxyCertInfoExtension;
import eu.emi.security.authn.x509.helpers.proxy.ProxyAddressRestrictionData;
import eu.emi.security.authn.x509.helpers.proxy.ProxyCSRImpl;
import eu.emi.security.authn.x509.helpers.proxy.ProxyCertInfoExtension;
import eu.emi.security.authn.x509.helpers.proxy.ProxyGeneratorHelper;
import eu.emi.security.authn.x509.helpers.proxy.ProxySAMLExtension;
import eu.emi.security.authn.x509.helpers.proxy.ProxyTracingExtension;
import eu.emi.security.authn.x509.helpers.proxy.RFCProxyCertInfoExtension;
import eu.emi.security.authn.x509.impl.CertificateUtils;
import eu.emi.security.authn.x509.impl.KeyAndCertCredential;
import eu.emi.security.authn.x509.proxy.CertificateExtension;
import eu.emi.security.authn.x509.proxy.ProxyCSR;
import eu.emi.security.authn.x509.proxy.ProxyCertificateOptions;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.StringWriter;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.InvalidKeyException;
import java.security.KeyPair;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.SignatureException;
import java.security.cert.CertPath;
import java.security.cert.CertificateEncodingException;
import java.security.cert.X509Certificate;
import java.security.interfaces.RSAPublicKey;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import eu.emi.security.authn.x509.proxy.ProxyPolicy;
import eu.emi.security.authn.x509.proxy.ProxyType;
import org.bouncycastle.asn1.ASN1Encodable;
import org.bouncycastle.asn1.ASN1InputStream;
import org.bouncycastle.asn1.DERNull;
import org.bouncycastle.asn1.DERSet;
import org.bouncycastle.asn1.pkcs.Attribute;
import org.bouncycastle.asn1.pkcs.PKCSObjectIdentifiers;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x509.AlgorithmIdentifier;
import org.bouncycastle.asn1.x509.SubjectPublicKeyInfo;
import org.bouncycastle.crypto.params.AsymmetricKeyParameter;
import org.bouncycastle.crypto.util.PrivateKeyFactory;
import org.bouncycastle.openssl.jcajce.JcaPEMWriter;
import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.operator.DefaultDigestAlgorithmIdentifierFinder;
import org.bouncycastle.operator.OperatorCreationException;
import org.bouncycastle.operator.bc.BcRSAContentSignerBuilder;
import org.bouncycastle.pkcs.PKCS10CertificationRequest;
import org.bouncycastle.pkcs.PKCS10CertificationRequestBuilder;

/**
 * Common code for creating a delegation request and finalizing the delegated proxy credential.
 */
public final class X509DelegationHelper {

    public static X509Delegation newDelegation(CertPath path,
          KeyPairCache keyPairs)
          throws NoSuchAlgorithmException, NoSuchProviderException {
        X509Certificate[] certificates = path.getCertificates().toArray(X509Certificate[]::new);
        if (certificates.length == 0) {
            throw new IllegalArgumentException("Certificate path is empty.");
        }

        X509Certificate first = certificates[0];
        int bits = ((RSAPublicKey) first.getPublicKey()).getModulus().bitLength();
        return new X509Delegation(keyPairs.getKeyPair(bits), certificates);
    }

    public static String createRequest(X509Certificate[] chain, KeyPair keyPair)
          throws GeneralSecurityException, IOException {
        ProxyCertificateOptions options = new ProxyCertificateOptions(chain);
        options.setPublicKey(keyPair.getPublic());
        options.setLimited(true);
        return pemEncode(generateProxyReq(options,
                    keyPair.getPrivate())
              .getCSR());
    }

    public static X509Credential acceptCertificate(String encodedCertificate,
          X509Delegation delegation)
          throws GeneralSecurityException {
        X509Certificate[] certificates = finalizeChain(encodedCertificate,
              delegation.getCertificates());
        return new KeyAndCertCredential(delegation.getKeyPair().getPrivate(),
              certificates);
    }

    public static X509Certificate[] finalizeChain(String encodedCertificate,
          X509Certificate[] certificates)
          throws GeneralSecurityException {
        X509Certificate certificate;
        try {
            certificate = CertificateUtils.loadCertificate(
                  new ByteArrayInputStream(
                        encodedCertificate.getBytes(
                              StandardCharsets.UTF_8)),
                  CertificateUtils.Encoding.PEM);
        } catch (IOException e) {
            throw new GeneralSecurityException("Supplied certificate is unacceptable: "
                  + e.getMessage());
        }

        return Stream.concat(Stream.of(certificate),
              Stream.of(certificates)).toArray(
              X509Certificate[]::new);
    }

    private static String pemEncode(Object item) throws IOException {
        StringWriter writer = new StringWriter();
        try (JcaPEMWriter pem = new JcaPEMWriter(writer)) {
            pem.writeObject(item);
        }
        return writer.toString();
    }

    private X509DelegationHelper() {
    } // static utility


    // REVISIT: patched version of ProxyCSRGenerator#generate due to
    //   use of hardcodded banned hash SHA1
    //   https://github.com/eu-emi/canl-java/issues/122
    /**
     * Generate the proxy certificate object. Use this method if you want to manually
     * specify the CSR signing key. This is normally the case when
     * the {@link ProxyCertificateOptions} parameter contains a manually set public key.
     *
     * @param param request creation parameters
     * @param signingKey private key
     * @return Proxy certificate signing request
     * @throws InvalidKeyException invalid key exception
     * @throws SignatureException signature exception
     * @throws IllegalArgumentException when signingKey is null and public key was manually set
     */
    public static ProxyCSR generateProxyReq(ProxyCertificateOptions param, PrivateKey signingKey)
            throws InvalidKeyException, SignatureException
    {
        PublicKey pubKey = param.getPublicKey();
        KeyPair keyPair;
        if (pubKey == null)
            keyPair = ProxyGeneratorHelper.generateKeyPair(param.getKeyLength());
        else
            keyPair = new KeyPair(pubKey, null);
        if (signingKey == null)
            signingKey = keyPair.getPrivate();
        if (signingKey == null)
            throw new IllegalArgumentException("Signing (private) key can not be null " +
                    "when using a manually set public key");

        X509Certificate []chain = param.getParentCertChain();
        ProxyType type = param.getType();
        BigInteger serial = ProxyGeneratorHelper.establishSerial(param);
        X500Name proxySubjectName = ProxyGeneratorHelper.generateDN(chain[0].getSubjectX500Principal(), type,
                param.isLimited(), serial);
        List<Attribute> attributes = generateAttributes(param);

        PKCS10CertificationRequest req;
        try
        {
            ASN1InputStream is = new ASN1InputStream(keyPair.getPublic().getEncoded());
            SubjectPublicKeyInfo subjectPublicKeyInfo = SubjectPublicKeyInfo.getInstance(is.readObject());
            is.close();
            PKCS10CertificationRequestBuilder builder = new PKCS10CertificationRequestBuilder(
                    proxySubjectName, subjectPublicKeyInfo);
            for (Attribute attribute: attributes)
                builder.addAttribute(attribute.getAttrType(), attribute.getAttributeValues());

            AlgorithmIdentifier algorithmIdentifier = new AlgorithmIdentifier(PKCSObjectIdentifiers.sha256WithRSAEncryption);
            AlgorithmIdentifier signatureAi = new AlgorithmIdentifier(algorithmIdentifier.getAlgorithm(), DERNull.INSTANCE);
            AlgorithmIdentifier hashAi = new DefaultDigestAlgorithmIdentifierFinder().find(signatureAi);
            BcRSAContentSignerBuilder csBuilder = new BcRSAContentSignerBuilder(signatureAi, hashAi);
            AsymmetricKeyParameter pkParam = PrivateKeyFactory.createKey(signingKey.getEncoded());
            ContentSigner signer = csBuilder.build(pkParam);
            req = builder.build(signer);
        } catch (IOException e)
        {
            throw new InvalidKeyException("Problem with the proxy CSR private key", e);
        } catch (OperatorCreationException e)
        {
            throw new SignatureException("Problem signing the proxy CSR", e);
        }
        return new ProxyCSRImpl(req, keyPair.getPrivate());
    }

    private static List<Attribute> generateAttributes(ProxyCertificateOptions param)
    {
        List<Attribute> attributes = new ArrayList<Attribute>();

        List<CertificateExtension> additionalExts = param.getExtensions();
        for (CertificateExtension ext: additionalExts)
            addAttribute(attributes, ext);

        ProxyPolicy policy = param.getPolicy();
        int pathLimit = param.getProxyPathLimit();
        if (param.getType() != ProxyType.LEGACY && (policy != null || pathLimit != -1))
        {
            if (policy == null)
                policy = new ProxyPolicy(ProxyPolicy.INHERITALL_POLICY_OID);

            String oid = param.getType() == ProxyType.DRAFT_RFC ?
                    DraftRFCProxyCertInfoExtension.DRAFT_EXTENSION_OID
                    : RFCProxyCertInfoExtension.RFC_EXTENSION_OID;
            ProxyCertInfoExtension extValue = param.getType() == ProxyType.DRAFT_RFC ?
                    new DraftRFCProxyCertInfoExtension(pathLimit, policy) :
                    new RFCProxyCertInfoExtension(pathLimit, policy);
            CertificateExtension ext = new CertificateExtension(oid, extValue, true);
            addAttribute(attributes, ext);
        }

        if (param.getProxyTracingIssuer() != null)
        {
            ProxyTracingExtension extValue = new ProxyTracingExtension(param.getProxyTracingIssuer());
            CertificateExtension ext = new CertificateExtension(
                    ProxyTracingExtension.PROXY_TRACING_ISSUER_EXTENSION_OID,
                    extValue, false);
            addAttribute(attributes, ext);
        }
        if (param.getProxyTracingSubject() != null)
        {
            ProxyTracingExtension extValue = new ProxyTracingExtension(param.getProxyTracingSubject());
            CertificateExtension ext = new CertificateExtension(
                    ProxyTracingExtension.PROXY_TRACING_SUBJECT_EXTENSION_OID,
                    extValue, false);
            addAttribute(attributes, ext);
        }

        if (param.getSAMLAssertion() != null)
        {
            ProxySAMLExtension extValue = new ProxySAMLExtension(param.getSAMLAssertion());
            CertificateExtension ext = new CertificateExtension(
                    ProxySAMLExtension.SAML_OID, extValue, false);
            addAttribute(attributes, ext);
        }

        String[] srcExcl = param.getSourceRestrictionExcludedAddresses();
        String[] srcPerm = param.getSourceRestrictionPermittedAddresses();
        if (srcExcl != null || srcPerm != null)
        {
            ProxyAddressRestrictionData extValue = new ProxyAddressRestrictionData();
            if (srcExcl != null)
            {
                for (String addr: srcExcl)
                    extValue.addExcludedIPAddressWithNetmask(addr);
            }
            if (srcPerm != null)
            {
                for (String addr: srcPerm)
                    extValue.addPermittedIPAddressWithNetmask(addr);
            }
            CertificateExtension ext = new CertificateExtension(
                    ProxyAddressRestrictionData.SOURCE_RESTRICTION_OID, extValue, false);
            addAttribute(attributes, ext);
        }

        String[] tgtExcl = param.getTargetRestrictionExcludedAddresses();
        String[] tgtPerm = param.getTargetRestrictionPermittedAddresses();
        if (tgtExcl != null || tgtPerm != null)
        {
            ProxyAddressRestrictionData extValue = new ProxyAddressRestrictionData();
            if (tgtExcl != null)
            {
                for (String addr: tgtExcl)
                    extValue.addExcludedIPAddressWithNetmask(addr);
            }
            if (tgtPerm != null)
            {
                for (String addr: tgtPerm)
                    extValue.addPermittedIPAddressWithNetmask(addr);
            }
            CertificateExtension ext = new CertificateExtension(
                    ProxyAddressRestrictionData.TARGET_RESTRICTION_OID, extValue, false);
            addAttribute(attributes, ext);
        }

        return attributes;
    }
    private static void addAttribute(List<Attribute> attributes, ASN1Encodable ext)
    {
        Attribute a = new Attribute(PKCSObjectIdentifiers.pkcs_9_at_extensionRequest,
                new DERSet(ext));
        attributes.add(a);
    }
}
