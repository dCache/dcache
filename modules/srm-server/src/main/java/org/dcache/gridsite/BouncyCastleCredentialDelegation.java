/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2014 Deutsches Elektronen-Synchrotron
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
package org.dcache.gridsite;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import org.bouncycastle.asn1.ASN1Encodable;
import org.bouncycastle.asn1.ASN1InputStream;
import org.bouncycastle.asn1.ASN1Sequence;
import org.bouncycastle.asn1.ASN1StreamParser;
import org.bouncycastle.asn1.DEREncodable;
import org.bouncycastle.asn1.DERObject;
import org.bouncycastle.asn1.DERPrintableString;
import org.bouncycastle.asn1.DERSequence;
import org.bouncycastle.asn1.DERSet;
import org.bouncycastle.asn1.x509.X509CertificateStructure;
import org.bouncycastle.asn1.x509.X509Name;
import org.bouncycastle.jce.PKCS10CertificationRequest;
import org.bouncycastle.jce.provider.X509CertificateObject;
import org.bouncycastle.openssl.PEMWriter;
import org.globus.gsi.X509Credential;
import org.globus.gsi.gssapi.GlobusGSSCredentialImpl;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.x500.X500Principal;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.security.interfaces.RSAPrivateKey;
import java.security.interfaces.RSAPublicKey;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;

import org.dcache.delegation.gridsite2.DelegationException;

import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Iterables.transform;
import static java.util.Collections.singleton;
import static org.ietf.jgss.GSSCredential.INITIATE_AND_ACCEPT;

/**
 * An in-progress credential delegation that uses BouncyCastle.
 */
public class BouncyCastleCredentialDelegation implements CredentialDelegation
{
    private static final Logger LOG = LoggerFactory.getLogger(BouncyCastleCredentialDelegation.class);

    private static final Function<X509Certificate,X509Certificate> AS_BC_CERTIFICATE =
            new Function<X509Certificate,X509Certificate>() {
                @Override
                public X509Certificate apply(X509Certificate certificate)
                {
                    if(certificate instanceof X509CertificateObject) {
                        return certificate;
                    } else {
                        try {
                            ByteArrayInputStream in =
                                    new ByteArrayInputStream(certificate.getEncoded());
                            return loadCertificate(in);
                        } catch (GeneralSecurityException | IOException e) {
                            throw new RuntimeException("failed to convert" +
                                    " certificate: " + e.getMessage());
                        }
                    }
                }
            };

    private final DelegationIdentity _id;
    private final Iterable<X509Certificate> _certificates;
    private final X509Certificate _first;
    private final String _pemRequest;

    protected KeyPair _keyPair;


    BouncyCastleCredentialDelegation(KeyPair keypair, DelegationIdentity id,
            Iterable<X509Certificate> certificates) throws DelegationException
    {
        _id = id;
        _certificates = certificates;
        _first = certificates.iterator().next();
        _keyPair = keypair;

        try {
            _pemRequest = pemEncode(createRequest(_first, keypair));
        } catch (GeneralSecurityException e) {
            LOG.error("Failed to create CSR: {}", e.toString());
            throw new DelegationException("cannot create certificate-signing" +
                    " request: " + e.getMessage());
        } catch (IOException e) {
            LOG.error("Failed to convert CSR to PEM: {}", e.toString());
            throw new DelegationException("cannot PEM-encode certificate-" +
                    "signing request: " + e.getMessage());
        }
    }

    private static PKCS10CertificationRequest createRequest(X509Certificate cert,
            KeyPair keyPair) throws GeneralSecurityException
    {
        return new PKCS10CertificationRequest(cert.getSigAlgName(),
                buildProxyDN(cert.getSubjectX500Principal()),
                keyPair.getPublic(), null, keyPair.getPrivate());
    }

    private static X509Name buildProxyDN(X500Principal principal)
            throws GeneralSecurityException
    {
        ASN1StreamParser parser = new ASN1StreamParser(principal.getEncoded());

        DERSequence seq;
        try {
            DERObject object = parser.readObject().getDERObject();
            if (!(object instanceof DERSequence)) {
                throw new IOException("not a DER-encoded ASN.1 sequence");
            }
            seq = (DERSequence) object;
        } catch (IOException e) {
            throw new GeneralSecurityException("failed to parse DN: " +
                    e.getMessage());
        }

        List<DEREncodable> rdn = new ArrayList(seq.size()+1);
        for(Enumeration e = seq.getObjects(); e.hasMoreElements();) {
            rdn.add((DEREncodable) e.nextElement());
        }

        DERSequence atv = new DERSequence(new ASN1Encodable[]{X509Name.CN,
            new DERPrintableString("proxy")});
        rdn.add(new DERSet(atv));

        ASN1Encodable[] rdnArray = rdn.toArray(new ASN1Encodable[rdn.size()]);
        return new X509Name(new DERSequence(rdnArray));
    }

    private static X509Certificate loadCertificate(InputStream in) throws IOException,
            GeneralSecurityException
    {
        DERObject certInfo = new ASN1InputStream(in).readObject();
        ASN1Sequence seq = ASN1Sequence.getInstance(certInfo);
        return new X509CertificateObject(new X509CertificateStructure(seq));
    }

    private static String pemEncode(Object item) throws IOException
    {
        StringWriter writer = new StringWriter();
        try (PEMWriter pem = new PEMWriter(writer)) {
            pem.writeObject(item);
        }
        return writer.toString();
    }

    @Override
    public String getCertificateSigningRequest()
    {
        return _pemRequest;
    }

    @Override
    public DelegationIdentity getId()
    {
        return _id;
    }

    @Override
    public GSSCredential acceptCertificate(String encodedCertificate) throws DelegationException
    {
        X509Certificate certificate;

        try {
            certificate = pemDecodeCertificate(encodedCertificate);
            verifyCertificate(certificate);
        } catch (CertificateException e) {
            LOG.debug("Bad certificate: {}", e.getMessage());
            throw new DelegationException("Supplied certificate is " +
                    "unacceptable: " + e.getMessage());
        }

        X509Certificate[] newCertificates =
                Iterables.toArray(transform(concat(singleton(certificate),
                _certificates), AS_BC_CERTIFICATE), X509Certificate.class);

        X509Credential proxy = new X509Credential(_keyPair.getPrivate(),
                newCertificates);

        try {
            return new GlobusGSSCredentialImpl(proxy, INITIATE_AND_ACCEPT);
        } catch (GSSException e) {
            LOG.error("Failed to create delegated credential: {}",
                    e.getMessage());
            throw new DelegationException("Unable to create delegated" +
                    " credential: " + e.getMessage());
        }
    }

    private static X509Certificate pemDecodeCertificate(String encoded)
            throws CertificateException
    {
        byte[] data;

        try {
            data = encoded.getBytes("UTF-8");
        } catch (UnsupportedEncodingException ignored) {
            throw new RuntimeException("UTF-8 not supported");
        }
        InputStream in = new ByteArrayInputStream(data, 0, data.length);

        CertificateFactory cf;
        try {
            cf = CertificateFactory.getInstance("X.509");
        } catch (CertificateException e) {
            throw new RuntimeException("Failed to find CertificateFactory" +
                    " for X.509: " + e.getMessage());
        }

        return (X509Certificate) cf.generateCertificate(in);
    }

    private void verifyCertificate(X509Certificate certificate)
            throws CertificateException
    {
        RSAPublicKey pubKey = (RSAPublicKey)certificate.getPublicKey();
        RSAPrivateKey privKey = (RSAPrivateKey)_keyPair.getPrivate();

        if (!pubKey.getModulus().equals(privKey.getModulus())) {
            throw new CertificateException("does not match signing request");
        }
    }
}
