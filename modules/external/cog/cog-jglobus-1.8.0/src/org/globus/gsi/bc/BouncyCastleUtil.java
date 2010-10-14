/*
 * Copyright 1999-2006 University of Chicago
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.globus.gsi.bc;

import org.globus.util.I18n;

import java.io.ByteArrayOutputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.security.cert.X509Certificate;
import java.security.cert.CertificateEncodingException;
import java.security.cert.CertificateException;
import java.security.Security;

import org.globus.gsi.GSIConstants;
import org.globus.gsi.CertUtil;
import org.globus.gsi.TrustedCertificates;
import org.globus.gsi.proxy.ext.ProxyPolicy;
import org.globus.gsi.proxy.ext.ProxyCertInfo;
import org.globus.common.ChainedCertificateException;

import org.bouncycastle.asn1.ASN1Sequence;
import org.bouncycastle.asn1.ASN1OctetString;
import org.bouncycastle.asn1.ASN1Set;
import org.bouncycastle.asn1.DEROutputStream;
import org.bouncycastle.asn1.DERInputStream;
import org.bouncycastle.asn1.DERObjectIdentifier;
import org.bouncycastle.asn1.DERString;
import org.bouncycastle.asn1.DERBoolean;
import org.bouncycastle.asn1.DERInteger;
import org.bouncycastle.asn1.DEREncodable;
import org.bouncycastle.asn1.DERObject;
import org.bouncycastle.asn1.DERBitString;
import org.bouncycastle.asn1.x509.TBSCertificateStructure;
import org.bouncycastle.asn1.x509.X509Extension;
import org.bouncycastle.asn1.x509.X509Extensions;
import org.bouncycastle.asn1.x509.X509Name;
import org.bouncycastle.asn1.x509.BasicConstraints;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.jce.provider.X509CertificateObject;

/**
 * A collection of various utility functions.
 */
public class BouncyCastleUtil {

    static {
	Security.addProvider(new BouncyCastleProvider());
    } 

    private static I18n i18n =
        I18n.getI18n("org.globus.gsi.errors",
                     BouncyCastleUtil.class.getClassLoader());

    /**
     * Converts given <code>DERObject</code> into
     * a DER-encoded byte array.
     *
     * @param obj DERObject to convert.
     * @return the DER-encoded byte array
     * @exception IOException if conversion fails
     */
    public static byte[] toByteArray(DERObject obj) 
	throws IOException {
	ByteArrayOutputStream bout = new ByteArrayOutputStream();
	DEROutputStream der = new DEROutputStream(bout);
	der.writeObject(obj);
	return bout.toByteArray();
    }
    
    /**
     * Converts the DER-encoded byte array into a 
     * <code>DERObject</code>.
     *
     * @param data the DER-encoded byte array to convert.
     * @return the DERObject.
     * @exception IOException if conversion fails
     */
    public static DERObject toDERObject(byte[] data) 
	throws IOException {
	ByteArrayInputStream inStream = new ByteArrayInputStream(data);
	DERInputStream derInputStream = new DERInputStream(inStream);
	return derInputStream.readObject();
    }

    /**
     * Replicates a given <code>DERObject</code>.
     *
     * @param obj the DERObject to replicate.
     * @return a copy of the DERObject.
     * @exception IOException if replication fails
     */
    public static DERObject duplicate(DERObject obj) 
	throws IOException {
	return toDERObject(toByteArray(obj));
    }

    /**
     * Extracts the TBS certificate from the given certificate.
     *
     * @param cert the X.509 certificate to extract the TBS certificate from.
     * @return the TBS certificate
     * @exception IOException if extraction fails.
     * @exception CertificateEncodingException if extraction fails.
     */
    public static TBSCertificateStructure getTBSCertificateStructure(X509Certificate cert)
	throws CertificateEncodingException, IOException {
	DERObject obj = BouncyCastleUtil.toDERObject(cert.getTBSCertificate());
	return TBSCertificateStructure.getInstance(obj);
    }

    /**
     * Extracts the value of a certificate extension.
     * 
     * @param ext the certificate extension to extract the value from.
     * @exception IOException if extraction fails.
     */
    public static DERObject getExtensionObject(X509Extension ext) 
	throws IOException {
	return toDERObject(ext.getValue().getOctets());
    }

    /**
     * Returns certificate type of the given certificate. 
     * Please see {@link #getCertificateType(TBSCertificateStructure,
     * TrustedCertificates) getCertificateType} for details for 
     * determining the certificate type.
     *
     * @param cert the certificate to get the type of.
     * @param trustedCerts the trusted certificates to double check the 
     *                     {@link GSIConstants#EEC GSIConstants.EEC} 
     *                     certificate against.
     * @return the certificate type as determined by 
     *             {@link #getCertificateType(TBSCertificateStructure, 
     *              TrustedCertificates) getCertificateType}.
     * @exception CertificateException if something goes wrong.
     */
    public static int getCertificateType(X509Certificate cert,
					 TrustedCertificates trustedCerts)
	throws CertificateException {
	try {
	    return getCertificateType(getTBSCertificateStructure(cert), 
				      trustedCerts);
	} catch (IOException e) {
	    // but this should not happen
	    throw new ChainedCertificateException("", e);
	}
    }
    
    /**
     * Returns certificate type of the given certificate. 
     * Please see {@link #getCertificateType(TBSCertificateStructure) 
     * getCertificateType} for details for determining the certificate type.
     *
     * @param cert the certificate to get the type of.
     * @return the certificate type as determined by 
     *             {@link #getCertificateType(TBSCertificateStructure) 
     *              getCertificateType}.
     * @exception CertificateException if something goes wrong.
     */
    public static int getCertificateType(X509Certificate cert) 
	throws CertificateException {
	try {
	    return getCertificateType(getTBSCertificateStructure(cert));
	} catch (IOException e) {
	    // but this should not happen
	    throw new ChainedCertificateException("", e);
	}
    }

    /**
     * Returns certificate type of the given certificate. 
     * This function calls {@link #getCertificateType(TBSCertificateStructure) 
     * getCertificateType} to get the certificate type. In case
     * the certificate type was initially determined as 
     * {@link GSIConstants#EEC GSIConstants.EEC} it is checked
     * against the trusted certificate list to see if it really
     * is a CA certificate. If the certificate is present in the
     * trusted certificate list the certificate type is changed
     * to {@link GSIConstants#CA GSIConstants.CA}. Otherwise, it is
     * left as it is (This is useful in cases where a valid CA
     * certificate does not have a BasicConstraints extension)
     *
     * @param crt the certificate to get the type of.
     * @param trustedCerts the trusted certificates to double check the 
     *                     {@link GSIConstants#EEC GSIConstants.EEC} 
     *                     certificate against. If null, a default
     *                     set of trusted certificate will be loaded
     *                     from a standard location.
     * @return the certificate type. The certificate type is determined
     *         by rules described above.
     * @exception IOException if something goes wrong.
     * @exception CertificateException for proxy certificates, if 
     *            the issuer DN of the certificate does not match
     *            the subject DN of the certificate without the
     *            last <I>CN</I> component. Also, for GSI-3 proxies
     *            when the <code>ProxyCertInfo</code> extension is 
     *            not marked as critical.
     */
    public static int getCertificateType(TBSCertificateStructure crt,
					 TrustedCertificates trustedCerts) 
	throws CertificateException, IOException {
	int type = getCertificateType(crt);

	// check subject of the cert in trusted cert list
	// to make sure the cert is not a ca cert
	if (type == GSIConstants.EEC) {
	    if (trustedCerts == null) {
		trustedCerts = 
		    TrustedCertificates.getDefaultTrustedCertificates();
	    } 
	    if (trustedCerts != null && 
		trustedCerts.getCertificate(crt.getSubject().toString()) != null) {
		type = GSIConstants.CA;
	    }
	}

	return type;
    }

    /**
     * Returns certificate type of the given TBS certificate. <BR>
     * The certificate type is {@link GSIConstants#CA GSIConstants.CA}
     * <B>only</B> if the certificate contains a 
     * BasicConstraints extension and it is marked as CA.<BR>
     * A certificate is a GSI-2 proxy when the subject DN of the certificate
     * ends with <I>"CN=proxy"</I> (certificate type {@link 
     * GSIConstants#GSI_2_PROXY GSIConstants.GSI_2_PROXY}) or 
     * <I>"CN=limited proxy"</I> (certificate type {@link
     * GSIConstants#GSI_2_LIMITED_PROXY GSIConstants.LIMITED_PROXY}) component
     * and the issuer DN of the certificate matches the subject DN without
     * the last proxy <I>CN</I> component.<BR>
     * A certificate is a GSI-3 proxy when the subject DN of the certificate
     * ends with a <I>CN</I> component, the issuer DN of the certificate
     * matches the subject DN without the last <I>CN</I> component and
     * the certificate contains {@link ProxyCertInfo ProxyCertInfo} critical
     * extension. 
     * The certificate type is {@link GSIConstants#GSI_3_IMPERSONATION_PROXY
     * GSIConstants.GSI_3_IMPERSONATION_PROXY} if the policy language of
     * the {@link ProxyCertInfo ProxyCertInfo} extension is set to
     * {@link ProxyPolicy#IMPERSONATION ProxyPolicy.IMPERSONATION} OID.
     * The certificate type is {@link GSIConstants#GSI_3_LIMITED_PROXY
     * GSIConstants.GSI_3_LIMITED_PROXY} if the policy language of
     * the {@link ProxyCertInfo ProxyCertInfo} extension is set to
     * {@link ProxyPolicy#LIMITED ProxyPolicy.LIMITED} OID.
     * The certificate type is {@link GSIConstants#GSI_3_INDEPENDENT_PROXY
     * GSIConstants.GSI_3_INDEPENDENT_PROXY} if the policy language of
     * the {@link ProxyCertInfo ProxyCertInfo} extension is set to
     * {@link ProxyPolicy#INDEPENDENT ProxyPolicy.INDEPENDENT} OID.
     * The certificate type is {@link GSIConstants#GSI_3_RESTRICTED_PROXY
     * GSIConstants.GSI_3_RESTRICTED_PROXY} if the policy language of
     * the {@link ProxyCertInfo ProxyCertInfo} extension is set to
     * any other OID then the above.<BR>
     * The certificate type is {@link GSIConstants#EEC GSIConstants.EEC}
     * if the certificate is not a CA certificate or a GSI-2 or GSI-3 proxy.
     *
     * @param crt the TBS certificate to get the type of.
     * @return the certificate type. The certificate type is determined
     *         by rules described above.
     * @exception IOException if something goes wrong.
     * @exception CertificateException for proxy certificates, if 
     *            the issuer DN of the certificate does not match
     *            the subject DN of the certificate without the
     *            last <I>CN</I> component. Also, for GSI-3 proxies
     *            when the <code>ProxyCertInfo</code> extension is 
     *            not marked as critical.
     */
    public static int getCertificateType(TBSCertificateStructure crt)
	throws CertificateException, IOException {
	X509Extensions extensions = crt.getExtensions();
	X509Extension ext = null;

	if (extensions != null) {
	    ext = extensions.getExtension(X509Extensions.BasicConstraints);
	    if (ext != null) {
		BasicConstraints basicExt = getBasicConstraints(ext);
		if (basicExt.isCA()) {
		    return GSIConstants.CA;
		}
	    }
	}
	
	int type = GSIConstants.EEC;
	
	// does not handle multiple AVAs
	X509Name subject = crt.getSubject();

	ASN1Set entry = X509NameHelper.getLastNameEntry(subject);
	ASN1Sequence ava = (ASN1Sequence)entry.getObjectAt(0);
	if (X509Name.CN.equals(ava.getObjectAt(0))) {
	    String value = ((DERString)ava.getObjectAt(1)).getString();
	    if (value.equalsIgnoreCase("proxy")) {
		type = GSIConstants.GSI_2_PROXY;
	    } else if (value.equalsIgnoreCase("limited proxy")) {
		type = GSIConstants.GSI_2_LIMITED_PROXY;
	    } else if (extensions != null) {
                boolean gsi4 = true;
                // GSI_4
		ext = extensions.getExtension(ProxyCertInfo.OID);
                if (ext == null) {
                    // GSI_3
                    ext = extensions.getExtension(ProxyCertInfo.OLD_OID);
                    gsi4 = false;
                }
		if (ext != null) {
		    if (ext.isCritical()) {
			ProxyCertInfo proxyCertExt = getProxyCertInfo(ext);
                        ProxyPolicy proxyPolicy = 
                            proxyCertExt.getProxyPolicy();
                        DERObjectIdentifier oid = 
                            proxyPolicy.getPolicyLanguage();
			if (ProxyPolicy.IMPERSONATION.equals(oid)) {
                            if (gsi4) {
                                type = GSIConstants.GSI_4_IMPERSONATION_PROXY;
                            } else {
                                type = GSIConstants.GSI_3_IMPERSONATION_PROXY;
                            }
			} else if (ProxyPolicy.INDEPENDENT.equals(oid)) {
                            if (gsi4) {
                                type = GSIConstants.GSI_4_INDEPENDENT_PROXY;
                            } else {
                                type = GSIConstants.GSI_3_INDEPENDENT_PROXY;
                            }
			} else if (ProxyPolicy.LIMITED.equals(oid)) {
                            if (gsi4) {
                                type = GSIConstants.GSI_4_LIMITED_PROXY;
                            } else {
                                type = GSIConstants.GSI_3_LIMITED_PROXY;
                            }
			} else {
                            if (gsi4) {
                                type = GSIConstants.GSI_4_RESTRICTED_PROXY;
                            } else {
                                type = GSIConstants.GSI_3_RESTRICTED_PROXY;
                            }
			}
                        
		    } else {
                        String err = i18n.getMessage("proxyCertCritical");
			throw new CertificateException(err);
		    }
		}
	    }
	    
	    if (CertUtil.isProxy(type)) {
		X509NameHelper iss = new X509NameHelper(crt.getIssuer());
		iss.add((ASN1Set)BouncyCastleUtil.duplicate(entry));
		X509Name issuer = iss.getAsName();
		if (!issuer.equals(subject)) {
                    String err = i18n.getMessage("proxyDNErr");
		    throw new CertificateException(err);
		}
	    }
	}

	return type;
    }

    /**
     * Gets a boolean array representing bits of the KeyUsage extension.
     * 
     * @see java.security.cert.X509Certificate#getKeyUsage
     * @exception IOException if failed to extract the KeyUsage extension value.
     */
    public static boolean[] getKeyUsage(X509Extension ext) 
	throws IOException {
	DERBitString bits = (DERBitString)getExtensionObject(ext);

	// copied from X509CertificateObject
	byte [] bytes = bits.getBytes();
	int length = (bytes.length * 8) - bits.getPadBits();
	
	boolean[]  keyUsage = new boolean[(length < 9) ? 9 : length];
	
	for (int i = 0; i != length; i++) {
	    keyUsage[i] = (bytes[i / 8] & (0x80 >>> (i % 8))) != 0;
	}
	
	return keyUsage;
    }

    /**
     * Creates a <code>BasicConstraints</code> object from given
     * extension.
     *
     * @param ext the extension.
     * @return the <code>BasicConstraints</code> object.
     * @exception IOException if something fails.
     */
    public static BasicConstraints getBasicConstraints(X509Extension ext) 
	throws IOException {
	DERObject obj = BouncyCastleUtil.getExtensionObject(ext);
	if (obj instanceof ASN1Sequence) {
	    ASN1Sequence seq = (ASN1Sequence)obj;
	    int size = seq.size();
	    if (size == 0) {
		return new BasicConstraints(false);
	    } else if (size == 1) {
		DEREncodable value = seq.getObjectAt(0);
		if (value instanceof DERInteger) {
		    int length = ((DERInteger)value).getValue().intValue();
		    return new BasicConstraints(false, length);
		} else if (value instanceof DERBoolean) {
		    boolean ca = ((DERBoolean)value).isTrue();
		    return new BasicConstraints(ca);
		}
	    } 
	}
	return BasicConstraints.getInstance(obj);
    }
    
    /**
     * Creates a <code>ProxyCertInfo</code> object from given
     * extension.
     *
     * @param ext the extension.
     * @return the <code>ProxyCertInfo</code> object.
     * @exception IOException if something fails.
     */
    public static ProxyCertInfo getProxyCertInfo(X509Extension ext) 
	throws IOException {
	return ProxyCertInfo.getInstance(BouncyCastleUtil.getExtensionObject(ext));
    }

    /**
     * Returns the subject DN of the given certificate in the Globus format.
     *
     * @param cert the certificate to get the subject of. The certificate
     *             must be of <code>X509CertificateObject</code> type.
     * @return the subject DN of the certificate in the Globus format.
     */
    public static String getIdentity(X509Certificate cert) {
	if (cert == null) {
	    return null;
	}
	if (cert instanceof X509CertificateObject) {
	    return X509NameHelper.toString((X509Name)cert.getSubjectDN());
	} else {
            String err = i18n.getMessage("certTypeErr", cert.getClass());
	    throw new IllegalArgumentException(err);
	}
    }

    /**
     * Finds the identity certificate in the given chain and
     * returns the subject DN of that certificate in the Globus format.
     *
     * @param chain the certificate chain to find the identity 
     *              certificate in. The certificates must be 
     *              of <code>X509CertificateObject</code> type.
     * @return the subject DN of the identity certificate in
     *         the Globus format.
     * @exception CertificateException if something goes wrong.
     */
    public static String getIdentity(X509Certificate [] chain)
	throws CertificateException {
	return getIdentity(getIdentityCertificate(chain));
    }

    /**
     * Finds the identity certificate in the given chain.
     * The identity certificate is the first certificate in the 
     * chain that is not an impersonation proxy (full or limited)
     *
     * @param chain the certificate chain to find the identity 
     *              certificate in.
     * @return the identity certificate.
     * @exception CertificateException if something goes wrong.
     */
    public static X509Certificate getIdentityCertificate(X509Certificate [] chain) 
	throws CertificateException {
	if (chain == null) {
	    throw new IllegalArgumentException(i18n
                                               .getMessage("certChainNull"));
	}
	int certType;
	for (int i=0;i<chain.length;i++) {
	    certType = getCertificateType(chain[i]);
	    if (!CertUtil.isImpersonationProxy(certType)) {
		return chain[i];
	    }
	}
	return null;
    }

    /**
     * Retrieves the actual value of the X.509 extension.
     * 
     * @param certExtValue the DER-encoded OCTET string value of the extension.
     * @return the decoded/actual value of the extension (the octets).
     */
    public static byte[] getExtensionValue(byte [] certExtValue) 
	throws IOException {
	ByteArrayInputStream inStream = new ByteArrayInputStream(certExtValue);
	DERInputStream derInputStream = new DERInputStream(inStream);
	DERObject object = derInputStream.readObject();
	if (object instanceof ASN1OctetString) {
	    return ((ASN1OctetString)object).getOctets();
	} else {
	    throw new IOException(i18n.getMessage("octectExp"));
	}
    }

    public static int getProxyPathConstraint(X509Certificate cert)
            throws IOException, CertificateEncodingException {
        
        TBSCertificateStructure crt = getTBSCertificateStructure(cert);
        return getProxyPathConstraint(crt);
    }


    public static int getProxyPathConstraint(TBSCertificateStructure crt) 
        throws IOException {

        ProxyCertInfo proxyCertExt = getProxyCertInfo(crt);
        return (proxyCertExt != null) ? proxyCertExt.getPathLenConstraint() :
            -1;
    }

    public static ProxyCertInfo getProxyCertInfo(TBSCertificateStructure crt) 
	throws IOException {

	X509Extensions extensions = crt.getExtensions();
	if (extensions == null) {
	    return null;
	}
	X509Extension ext = 
	    extensions.getExtension(ProxyCertInfo.OID);
        if (ext == null) {
            ext = extensions.getExtension(ProxyCertInfo.OLD_OID);
        }
	return (ext != null) ? BouncyCastleUtil.getProxyCertInfo(ext) : null;
    }

}
