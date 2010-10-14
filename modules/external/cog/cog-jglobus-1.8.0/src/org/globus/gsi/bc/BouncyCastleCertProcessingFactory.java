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

import java.math.BigInteger;
import java.util.Random;
import java.util.GregorianCalendar;
import java.util.TimeZone;
import java.util.Iterator;
import java.util.Calendar;
import java.io.InputStream;
import java.io.IOException;
import java.io.ByteArrayInputStream;
import java.security.cert.X509Certificate;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.GeneralSecurityException;
import java.security.KeyPairGenerator;
import java.security.KeyPair;

import org.globus.util.I18n;

import org.globus.gsi.GlobusCredential;
import org.globus.gsi.GSIConstants;
import org.globus.gsi.CertUtil;
import org.globus.gsi.X509ExtensionSet;
import org.globus.gsi.proxy.ext.ProxyCertInfo;
import org.globus.gsi.proxy.ext.ProxyPolicy;
import org.globus.gsi.proxy.ext.ProxyCertInfoExtension;
import org.globus.gsi.proxy.ext.GlobusProxyCertInfoExtension;

import org.bouncycastle.x509.X509V3CertificateGenerator;
import org.bouncycastle.jce.PKCS10CertificationRequest;
import org.bouncycastle.jce.provider.X509CertificateObject;
import org.bouncycastle.asn1.DERConstructedSet;
import org.bouncycastle.asn1.DERObject;
import org.bouncycastle.asn1.DERBitString;
import org.bouncycastle.asn1.x509.X509Name;
import org.bouncycastle.asn1.DERInputStream;
import org.bouncycastle.asn1.ASN1Sequence;
import org.bouncycastle.asn1.x509.TBSCertificateStructure;
import org.bouncycastle.asn1.x509.X509CertificateStructure;
import org.bouncycastle.asn1.x509.X509Extensions;
import org.bouncycastle.asn1.x509.X509Extension;
import org.bouncycastle.asn1.x509.KeyUsage;

/**
 * Provides certificate processing API such as creating new 
 * certificates, certificate requests, etc.
 */
public class BouncyCastleCertProcessingFactory {

    private static I18n i18n =
        I18n.getI18n("org.globus.gsi.errors",
                     BouncyCastleCertProcessingFactory.class.getClassLoader());

    private static BouncyCastleCertProcessingFactory factory;

    protected BouncyCastleCertProcessingFactory() {
    }
    
    /**
     * Returns an instance of this class..
     *
     * @return <code>BouncyCastleCertProcessingFactory</code> instance.
     */
    public static synchronized BouncyCastleCertProcessingFactory getDefault() {
	if (factory == null) {
	    factory = new  BouncyCastleCertProcessingFactory();
	}
	return factory;
    }

    /**
     * Creates a proxy certificate from the certificate request. 
     *
     * @see #createCertificate(InputStream, X509Certificate, PrivateKey, 
     *      int, int, X509ExtensionSet, String) createCertificate
     */
    public X509Certificate createCertificate(InputStream certRequestInputStream,
					     X509Certificate cert,
					     PrivateKey privateKey,
					     int lifetime,
					     int delegationMode) 
	throws IOException, GeneralSecurityException {
	return createCertificate(certRequestInputStream,
				 cert,
				 privateKey,
				 lifetime,
				 delegationMode,
				 (X509ExtensionSet)null,
				 null);
    }

    /**
     * Creates a proxy certificate from the certificate request. 
     *
     * @see #createCertificate(InputStream, X509Certificate, PrivateKey, 
     *      int, int, X509ExtensionSet, String) createCertificate
     */
    public X509Certificate createCertificate(InputStream certRequestInputStream,
					     X509Certificate cert,
					     PrivateKey privateKey,
					     int lifetime,
					     int delegationMode,
					     X509ExtensionSet extSet)
	throws IOException, GeneralSecurityException {
	return createCertificate(certRequestInputStream,
				 cert,
				 privateKey,
				 lifetime,
				 delegationMode,
				 extSet,
				 null);
    }
    
    /**
     * Creates a proxy certificate from the certificate request. 
     * (Signs a certificate request creating a new certificate)
     *
     * @see #createProxyCertificate(X509Certificate, PrivateKey, PublicKey, 
     *        int, int, X509ExtensionSet, String) createProxyCertificate
     * @param certRequestInputStream the input stream to read the
     *        certificate request from.
     * @param cert the issuer certificate
     * @param privateKey the private key to sign the new 
     *        certificate with.
     * @param lifetime lifetime of the new certificate in seconds.
     *        If 0 (or less then) the new certificate will have the
     *        same lifetime as the issuing certificate. 
     * @param delegationMode the type of proxy credential to create
     * @param extSet a set of X.509 extensions to be included in the new
     *        proxy certificate. Can be null. If delegation mode is 
     *        {@link GSIConstants#GSI_3_RESTRICTED_PROXY
     *        GSIConstants.GSI_3_RESTRICTED_PROXY} or 
     *        {@link GSIConstants#GSI_4_RESTRICTED_PROXY
     *        GSIConstants.GSI_4_RESTRICTED_PROXY} then  
     *        {@link org.globus.gsi.proxy.ext.ProxyCertInfoExtension 
     *        ProxyCertInfoExtension} must be present in the extension
     *        set. 
     * @param cnValue the value of the CN component of the subject of
     *        the new certificate. If null, the defaults will be used
     *        depending on the proxy certificate type created.
     * @return <code>X509Certificate</code> the new proxy certificate
     * @exception IOException if error reading the certificate
     *            request
     * @exception GeneralSecurityException if a security error
     *            occurs.
     */
    public X509Certificate createCertificate(InputStream certRequestInputStream,
					     X509Certificate cert,
					     PrivateKey privateKey,
					     int lifetime,
					     int delegationMode,
					     X509ExtensionSet extSet,
					     String cnValue) 
	throws IOException, GeneralSecurityException {

	DERInputStream derin = new DERInputStream(certRequestInputStream);
	DERObject reqInfo = derin.readObject();
	PKCS10CertificationRequest certReq = 
	    new PKCS10CertificationRequest((ASN1Sequence)reqInfo);
	
	boolean rs = certReq.verify();
	    
	if (!rs) {
            String err = i18n.getMessage("certReqVerification");
	    throw new GeneralSecurityException(err);
	}

	return createProxyCertificate(cert,
				      privateKey,
				      certReq.getPublicKey(),
				      lifetime,
				      delegationMode,
				      extSet,
				      cnValue);
    }
    
    /**
     * Loads a X509 certificate from the specified input stream.
     * Input stream must contain DER-encoded certificate.
     *
     * @param in the input stream to read the certificate from.
     * @return <code>X509Certificate</code> the loaded certificate.
     * @exception GeneralSecurityException if certificate failed to load.
     */
    public X509Certificate loadCertificate(InputStream in)
	throws IOException, GeneralSecurityException {
	DERInputStream derin = new DERInputStream(in);
	DERObject certInfo = derin.readObject();
	ASN1Sequence seq = ASN1Sequence.getInstance(certInfo);
	return new X509CertificateObject(new X509CertificateStructure(seq));
    }

    /**
     * Creates a new proxy credential from the specified certificate
     * chain and a private key.
     *
     * @see #createCredential(X509Certificate[], PrivateKey, int, 
     *      int, int, X509ExtensionSet, String) createCredential
     */
    public GlobusCredential createCredential(X509Certificate [] certs,
					     PrivateKey privateKey,
					     int bits,
					     int lifetime,
					     int delegationMode)
	throws GeneralSecurityException {
	return createCredential(certs, privateKey, bits,
				lifetime, delegationMode, 
				(X509ExtensionSet)null, null);
    }
    
    /**
     * Creates a new proxy credential from the specified certificate
     * chain and a private key.
     *
     * @see #createCredential(X509Certificate[], PrivateKey, int, 
     *      int, int, X509ExtensionSet, String) createCredential
     */
    public GlobusCredential createCredential(X509Certificate [] certs,
					     PrivateKey privateKey,
					     int bits,
					     int lifetime,
					     int delegationMode,
					     X509ExtensionSet extSet)
	throws GeneralSecurityException {
	return createCredential(certs, privateKey, bits, lifetime, 
				delegationMode, extSet, null);
    }

    /**
     * Creates a new proxy credential from the specified certificate
     * chain and a private key. A set of X.509 extensions
     * can be optionally included in the new proxy certificate.
     * This function automatically creates a "RSA"-based key pair.
     *
     * @see #createProxyCertificate(X509Certificate, PrivateKey, PublicKey,
     *        int, int, X509ExtensionSet, String) createProxyCertificate
     * @param certs the certificate chain for the new proxy credential.
     *        The top-most certificate <code>cert[0]</code> will be
     *        designated as the issuing certificate.
     * @param privateKey the private key of the issuing certificate.
     *        The new proxy certificate will be signed with that
     *        private key.
     * @param bits the strength of the key pair for the new
     *        proxy certificate.
     * @param lifetime lifetime of the new certificate in seconds.
     *        If 0 (or less then) the new certificate will have the
     *        same lifetime as the issuing certificate. 
     * @param delegationMode the type of proxy credential to create
     * @param extSet a set of X.509 extensions to be included in the new
     *        proxy certificate. Can be null. If delegation mode is 
     *        {@link GSIConstants#GSI_3_RESTRICTED_PROXY
     *        GSIConstants.GSI_3_RESTRICTED_PROXY} or
     *        {@link GSIConstants#GSI_4_RESTRICTED_PROXY
     *        GSIConstants.GSI_4_RESTRICTED_PROXY} then 
     *        {@link org.globus.gsi.proxy.ext.ProxyCertInfoExtension 
     *        ProxyCertInfoExtension} must be present in the extension
     *        set. 
     * @param cnValue the value of the CN component of the subject of
     *        the new proxy credential. If null, the defaults will be used
     *        depending on the proxy certificate type created.
     * @return <code>GlobusCredential</code> the new proxy credential.
     * @exception GeneralSecurityException if a security error
     *            occurs.
     */
    public GlobusCredential createCredential(X509Certificate [] certs,
					     PrivateKey privateKey,
					     int bits,
					     int lifetime,
					     int delegationMode,
					     X509ExtensionSet extSet,
					     String cnValue)
	throws GeneralSecurityException {
	
        X509Certificate[] bcCerts = new X509Certificate[certs.length];
        for (int i = 0; i < certs.length; i++) {
            if (! (certs[i] instanceof X509CertificateObject)) {
                bcCerts[i] = CertUtil
                    .loadCertificate(new ByteArrayInputStream(certs[i]
                                                              .getEncoded()));
            } else {
                bcCerts[i] = certs[i];
            }
        }
        
	KeyPairGenerator keyGen = null;
	keyGen = KeyPairGenerator.getInstance("RSA", "BC");
	keyGen.initialize(bits);
        KeyPair keyPair = keyGen.genKeyPair();
	
	X509Certificate newCert = createProxyCertificate(bcCerts[0],
							 privateKey,
							 keyPair.getPublic(),
							 lifetime,
							 delegationMode,
							 extSet,
							 cnValue);
	
	X509Certificate[] newCerts = new X509Certificate[bcCerts.length+1];
	newCerts[0] = newCert;
	System.arraycopy(certs, 0, newCerts, 1, certs.length);
	
	return new GlobusCredential(keyPair.getPrivate(),
				    newCerts);
    }

    /**
     * Creates a certificate request from the specified
     * subject DN and a key pair.
     * The <I>"MD5WithRSAEncryption"</I> is used as the
     * signing algorithm of the certificate request.
     *
     * @param subject the subject of the certificate request
     * @param keyPair the key pair of the certificate request
     * @return the certificate request.
     * @exception GeneralSecurityException if security error 
     *            occurs.
     */
    public byte[] createCertificateRequest(String subject,
					   KeyPair keyPair)
	throws GeneralSecurityException {
	X509Name name = new X509Name(subject);
	return createCertificateRequest(name, 
					"MD5WithRSAEncryption",
					keyPair);
    }

    /**
     * Creates a certificate request from the specified
     * certificate and a key pair. The certificate's
     * subject DN with <I>"CN=proxy"</I> name component 
     * appended to the subject is used as the subject 
     * of the certificate request. 
     * Also the certificate's signing algorithm is
     * used as the certificate request signing algorithm.
     *
     * @param cert the certificate to create the certificate 
     *        request from.
     * @param keyPair the key pair of the certificate request
     * @return the certificate request.
     * @exception GeneralSecurityException if security error 
     *            occurs.
     */
    public byte[] createCertificateRequest(X509Certificate cert,
					   KeyPair keyPair)
	throws GeneralSecurityException {

	String issuer = cert.getSubjectDN().getName();
	X509Name subjectDN = new X509Name(issuer + ",CN=proxy");
	String sigAlgName = cert.getSigAlgName();
	return createCertificateRequest(subjectDN,
					sigAlgName,
					keyPair);
    }

    /**
     * Creates a certificate request from the specified
     * subject name, signing algorithm, and a key pair.
     *
     * @param subjectDN the subject name of the certificate 
     *        request.
     * @param sigAlgName the signing algorithm name.
     * @param keyPair the key pair of the certificate request
     * @return the certificate request.
     * @exception GeneralSecurityException if security error 
     *            occurs.
     */
    public byte[] createCertificateRequest(X509Name subjectDN,
					   String sigAlgName,
					   KeyPair keyPair)
	throws GeneralSecurityException {
	DERConstructedSet attrs = null;
	PKCS10CertificationRequest certReq = null;
	certReq = new PKCS10CertificationRequest(sigAlgName,
						 subjectDN,
						 keyPair.getPublic(),
						 attrs,
						 keyPair.getPrivate());
	
	return certReq.getEncoded();
    }

    /**
     * Creates a proxy certificate. A set of X.509 extensions
     * can be optionally included in the new proxy certificate. <BR>
     * If a GSI-2 proxy is created, the serial number of the proxy 
     * certificate will be the same as of the issuing certificate.
     * Also, none of the extensions in the issuing certificate will
     * be copied into the proxy certificate.<BR>
     * If a GSI-3 or GSI 4 proxy is created, the serial number of the proxy
     * certificate will be picked randomly. If the issuing certificate
     * contains a <i>KeyUsage</i> extension, the extension
     * will be copied into the proxy certificate with <i>keyCertSign</i>
     * and <i>nonRepudiation</i> bits turned off. No other extensions
     * are currently copied.
     * 
     * The methods defaults to creating GSI 4 proxy
     * 
     * @param issuerCert the issuing certificate
     * @param issuerKey private key matching the public key of issuer
     *        certificate. The new proxy certificate will be
     *        signed by that key.
     * @param publicKey the public key of the new certificate
     * @param lifetime lifetime of the new certificate in seconds.
     *        If 0 (or less then) the new certificate will have the
     *        same lifetime as the issuing certificate. 
     * @param proxyType can be one of 
     *        {@link GSIConstants#DELEGATION_LIMITED
     *        GSIConstants.DELEGATION_LIMITED}, 
     *        {@link GSIConstants#DELEGATION_FULL
     *        GSIConstants.DELEGATION_FULL}, 
     *
     *        {@link GSIConstants#GSI_2_LIMITED_PROXY
     *        GSIConstants.GSI_2_LIMITED_PROXY}, 
     *        {@link GSIConstants#GSI_2_PROXY
     *        GSIConstants.GSI_2_PROXY}, 
     *        {@link GSIConstants#GSI_3_IMPERSONATION_PROXY
     *        GSIConstants.GSI_3_IMPERSONATION_PROXY}, 
     *        {@link GSIConstants#GSI_3_LIMITED_PROXY
     *        GSIConstants.GSI_3_LIMITED_PROXY},
     *        {@link GSIConstants#GSI_3_INDEPENDENT_PROXY
     *        GSIConstants.GSI_3_INDEPENDENT_PROXY},
     *        {@link GSIConstants#GSI_3_RESTRICTED_PROXY
     *        GSIConstants.GSI_3_RESTRICTED_PROXY}.
     *        {@link GSIConstants#GSI_4_IMPERSONATION_PROXY
     *        GSIConstants.GSI_4_IMPERSONATION_PROXY}, 
     *        {@link GSIConstants#GSI_4_LIMITED_PROXY
     *        GSIConstants.GSI_3_LIMITED_PROXY},
     *        {@link GSIConstants#GSI_4_INDEPENDENT_PROXY
     *        GSIConstants.GSI_4_INDEPENDENT_PROXY},
     *        {@link GSIConstants#GSI_4_RESTRICTED_PROXY
     *        GSIConstants.GSI_4_RESTRICTED_PROXY}.
     *
     *        If {@link GSIConstants#DELEGATION_LIMITED
     *        GSIConstants.DELEGATION_LIMITED} and if
     *        {@link CertUtil#isGsi2Enabled() CertUtil.isGsi2Enabled}
     *        returns true then a GSI-2 limited proxy will be
     *        created. Else if {@link CertUtil#isGsi3Enabled() 
     *        CertUtil.isGsi3Enabled} returns true then a GSI-3
     *        limited proxy will be created. If not,
     *        a GSI-4 limited proxy will be created. 
     *
     *        If {@link GSIConstants#DELEGATION_FULL
     *        GSIConstants.DELEGATION_FULL} and if
     *        {@link CertUtil#isGsi2Enabled() CertUtil.isGsi2Enabled}
     *        returns true then a GSI-2 full proxy will be
     *        created. Else if {@link CertUtil#isGsi3Enabled() 
     *        CertUtil.isGsi3Enabled} returns true then a GSI-3
     *        full proxy will be created. If not,
     *        a GSI-4 full proxy will be created. 
     *
     * @param extSet a set of X.509 extensions to be included in the new
     *        proxy certificate. Can be null. If delegation mode is 
     *        {@link GSIConstants#GSI_3_RESTRICTED_PROXY
     *        GSIConstants.GSI_3_RESTRICTED_PROXY} or      
     *        {@link GSIConstants#GSI_4_RESTRICTED_PROXY
     *        GSIConstants.GSI_4_RESTRICTED_PROXY} then 
     *        {@link org.globus.gsi.proxy.ext.ProxyCertInfoExtension 
     *        ProxyCertInfoExtension} must be present in the extension
     *        set. 
     *
     * @param cnValue the value of the CN component of the subject of
     *        the new certificate. If null, the defaults will be used
     *        depending on the proxy certificate type created.
     * @return <code>X509Certificate</code> the new proxy certificate.
     * @exception GeneralSecurityException if a security error
     *            occurs.
     */
    public X509Certificate createProxyCertificate(X509Certificate issuerCert_, 
                                                  PrivateKey issuerKey,
                                                  PublicKey publicKey,
                                                  int lifetime,
                                                  int proxyType,
                                                  X509ExtensionSet extSet,
                                                  String cnValue) 
	throws GeneralSecurityException {

        X509Certificate issuerCert = issuerCert_;
        if (! (issuerCert_ instanceof X509CertificateObject)) {
            issuerCert = CertUtil
                .loadCertificate(new ByteArrayInputStream(issuerCert
                                                          .getEncoded()));
        }
	
	if (proxyType == GSIConstants.DELEGATION_LIMITED) {
	    int type = BouncyCastleUtil.getCertificateType(issuerCert);
            if (CertUtil.isGsi4Proxy(type)) {
		proxyType = GSIConstants.GSI_4_LIMITED_PROXY;
	    } else if (CertUtil.isGsi3Proxy(type)) {
		proxyType = GSIConstants.GSI_3_LIMITED_PROXY;
	    } else if (CertUtil.isGsi2Proxy(type)) {
		proxyType = GSIConstants.GSI_2_LIMITED_PROXY;
	    } else {
                // default to RFC compliant proxy
                if (CertUtil.isGsi2Enabled()) {
                    proxyType = GSIConstants.GSI_2_LIMITED_PROXY;
                } else {
                    proxyType = CertUtil.isGsi3Enabled() ?
                        GSIConstants.GSI_3_LIMITED_PROXY :
                        GSIConstants.GSI_4_LIMITED_PROXY;
                }
	    }
	} else if (proxyType == GSIConstants.DELEGATION_FULL) {
	    int type = BouncyCastleUtil.getCertificateType(issuerCert);
            if (CertUtil.isGsi4Proxy(type)) {
		proxyType = GSIConstants.GSI_4_IMPERSONATION_PROXY;
            } else if (CertUtil.isGsi3Proxy(type)) {
		proxyType = GSIConstants.GSI_3_IMPERSONATION_PROXY;
	    } else if (CertUtil.isGsi2Proxy(type)) {
		proxyType = GSIConstants.GSI_2_PROXY;
	    } else {
                // Default to RFC complaint proxy
                if (CertUtil.isGsi2Enabled()) {
                    proxyType = GSIConstants.GSI_2_PROXY;
                } else {
		proxyType = (CertUtil.isGsi3Enabled()) ? 
		    GSIConstants.GSI_3_IMPERSONATION_PROXY :
		    GSIConstants.GSI_4_IMPERSONATION_PROXY;		    
                }
            }
        }

	X509V3CertificateGenerator certGen = new X509V3CertificateGenerator();

	org.globus.gsi.X509Extension x509Ext = null;
	BigInteger serialNum = null;
	String delegDN = null;
	
	if (CertUtil.isGsi3Proxy(proxyType) || 
            CertUtil.isGsi4Proxy(proxyType) ) {
	    Random rand = new Random();
	    delegDN = String.valueOf(Math.abs(rand.nextInt()));
	    serialNum = new BigInteger(20, rand);
	
	    if (extSet != null) {
		x509Ext = extSet.get(ProxyCertInfo.OID.getId());
                if (x509Ext == null) {
                    x509Ext = extSet.get(ProxyCertInfo.OLD_OID.getId());
                }
	    }
	    
	    if (x509Ext == null) {
		// create ProxyCertInfo extension
		ProxyPolicy policy = null;
                if (CertUtil.isLimitedProxy(proxyType)) {
                    policy = new ProxyPolicy(ProxyPolicy.LIMITED);
                } else if (CertUtil.isIndependentProxy(proxyType)) {
                    policy = new ProxyPolicy(ProxyPolicy.INDEPENDENT);
                } else if (CertUtil.isImpersonationProxy(proxyType)) {
                    //since limited has already been checked, this should work.
                    policy = new ProxyPolicy(ProxyPolicy.IMPERSONATION);
		} else if 
                    ((proxyType == GSIConstants.GSI_3_RESTRICTED_PROXY) 
                     || (proxyType == GSIConstants.GSI_4_RESTRICTED_PROXY)) {
                    String err = i18n.getMessage("restrictProxy");
		    throw new IllegalArgumentException(err);
		} else {
                    String err = i18n.getMessage("invalidProxyType");
		    throw new IllegalArgumentException(err);
		}
		
		ProxyCertInfo proxyCertInfo = new ProxyCertInfo(policy);
		x509Ext = new ProxyCertInfoExtension(proxyCertInfo);
                if (CertUtil.isGsi4Proxy(proxyType)) {
                    // RFC compliant OID
                    x509Ext = new ProxyCertInfoExtension(proxyCertInfo);
                } else {
                    // old OID
                    x509Ext = new GlobusProxyCertInfoExtension(proxyCertInfo);
                }
	    }

	    try {
		// add ProxyCertInfo extension to the new cert
		certGen.addExtension(x509Ext.getOid(),
				     x509Ext.isCritical(),
				     x509Ext.getValue());
		
		// handle KeyUsage in issuer cert
		TBSCertificateStructure crt = 
		    BouncyCastleUtil.getTBSCertificateStructure(issuerCert);

		X509Extensions extensions = crt.getExtensions();
		if (extensions != null) {
		    X509Extension ext;
		    
		    // handle key usage ext
		    ext = extensions.getExtension(X509Extensions.KeyUsage);
		    if (ext != null) {

			// TBD: handle this better
			if (extSet != null && 
			    (extSet.get(X509Extensions.KeyUsage.getId()) 
                             != null)) {
                            String err = i18n.getMessage("keyUsageExt");
			    throw new GeneralSecurityException(err);
			}
			    
			
			DERBitString bits = (DERBitString)BouncyCastleUtil
                            .getExtensionObject(ext);
		    
			byte [] bytes = bits.getBytes();

			// make sure they are disabled
			if ((bytes[0] & KeyUsage.nonRepudiation) != 0) {
			    bytes[0] ^= KeyUsage.nonRepudiation;
			}

			if ((bytes[0] & KeyUsage.keyCertSign) != 0) {
			    bytes[0] ^= KeyUsage.keyCertSign;
			}

			bits = new DERBitString(bytes, bits.getPadBits());

			certGen.addExtension(X509Extensions.KeyUsage,
					     ext.isCritical(),
					     bits);
		    }
		}

	    } catch (IOException e) {
		// but this should not happen
		throw new GeneralSecurityException(e.getMessage());
	    }
	    
	} else if (proxyType == GSIConstants.GSI_2_LIMITED_PROXY) {
	    delegDN = "limited proxy";
	    serialNum = issuerCert.getSerialNumber();
	} else if (proxyType == GSIConstants.GSI_2_PROXY) {
	    delegDN = "proxy";
	    serialNum = issuerCert.getSerialNumber();
	} else {
            String err = i18n.getMessage("unsupportedProxy", 
                                         Integer.toString(proxyType));
	    throw new IllegalArgumentException(err);
	}

	// add specified extensions
	if (extSet != null) {
	    Iterator iter = extSet.oidSet().iterator();
	    while(iter.hasNext()) {
		String oid = (String)iter.next();
		// skip ProxyCertInfo extension
		if (oid.equals(ProxyCertInfo.OID.getId()) ||
                    oid.equals(ProxyCertInfo.OLD_OID.getId())) {
		    continue;
		}
		x509Ext = (org.globus.gsi.X509Extension)extSet.get(oid);
		certGen.addExtension(x509Ext.getOid(),
				     x509Ext.isCritical(),
				     x509Ext.getValue());
	    }
	}

	X509Name issuerDN = (X509Name)issuerCert.getSubjectDN();

	X509NameHelper issuer  = new X509NameHelper(issuerDN);
	
	X509NameHelper subject = new X509NameHelper(issuerDN); 
	subject.add(X509Name.CN, (cnValue == null) ? delegDN : cnValue);
	
	certGen.setSubjectDN(subject.getAsName());
        certGen.setIssuerDN(issuer.getAsName());

        certGen.setSerialNumber(serialNum);
        certGen.setPublicKey(publicKey);
        certGen.setSignatureAlgorithm(issuerCert.getSigAlgName());

	GregorianCalendar date =
	    new GregorianCalendar(TimeZone.getTimeZone("GMT"));
	/* Allow for a five minute clock skew here. */
	date.add(Calendar.MINUTE, -5);
	certGen.setNotBefore(date.getTime());

        /* If hours = 0, then cert lifetime is set to user cert */
        if (lifetime <= 0) {
            certGen.setNotAfter(issuerCert.getNotAfter());
        } else {
	    date.add(Calendar.MINUTE, 5);
	    date.add(Calendar.SECOND, lifetime);
            certGen.setNotAfter(date.getTime());
        }
	
	/**
	 * FIXME: Copy appropriate cert extensions - this should NOT be done
	 * the last time we talked to Doug E. This should investigated more.
	 */

	return certGen.generateX509Certificate(issuerKey);
    }

    // ----------------- OLDER API ------------------------------

    private X509ExtensionSet createExtensionSet(ProxyCertInfo proxyCertInfo) {
	X509ExtensionSet set = null;
	if (proxyCertInfo != null) {
	    set = new X509ExtensionSet();
	    set.add(new ProxyCertInfoExtension(proxyCertInfo));
	}	
	return set;
    }

    /**
     * @deprecated Please use 
     * {@link #createProxyCertificate(X509Certificate, PrivateKey, PublicKey,
     * int, int, X509ExtensionSet, String) createProxyCertificate()} instead. 
     * The <code>ProxyCertInfo</code> parameter can be passed in the 
     * <code>X509ExtensionSet</code> using 
     * {@link org.globus.gsi.proxy.ext.ProxyCertInfoExtension 
     * ProxyCertInfoExtension} class.
     */ 
    public X509Certificate createProxyCertificate(X509Certificate issuerCert, 
                                                  PrivateKey issuerKey,
                                                  PublicKey publicKey,
                                                  int lifetime,
                                                  int proxyType,
                                                  ProxyCertInfo proxyCertInfo,
                                                  String cnValue) 
	throws GeneralSecurityException {
	return createProxyCertificate(issuerCert, issuerKey,
				      publicKey, lifetime,
				      proxyType,
				      createExtensionSet(proxyCertInfo),
				      cnValue);
    }    

    /**
     * @deprecated Please use 
     * {@link #createCredential(X509Certificate[], PrivateKey, int,
     * int, int, X509ExtensionSet, String) createCredential()}
     * instead. The <code>ProxyCertInfo</code> parameter can be passed in the 
     * <code>X509ExtensionSet</code> using 
     * {@link org.globus.gsi.proxy.ext.ProxyCertInfoExtension 
     * ProxyCertInfoExtension} class.
     */ 
    public GlobusCredential createCredential(X509Certificate [] certs,
					     PrivateKey privateKey,
					     int bits,
					     int lifetime,
					     int delegationMode,
					     ProxyCertInfo proxyCertInfoExt,
					     String cnValue)
	throws GeneralSecurityException {
	return createCredential(certs, privateKey, bits, lifetime,
				delegationMode, 
				createExtensionSet(proxyCertInfoExt),
				cnValue);
    }

    /**
     * @deprecated 
     *
     * @see #createCredential(X509Certificate[], PrivateKey, int, 
     *      int, int, ProxyCertInfo, String) createCredential
     */
    public GlobusCredential createCredential(X509Certificate [] certs,
					     PrivateKey privateKey,
					     int bits,
					     int lifetime,
					     int delegationMode,
					     ProxyCertInfo proxyCertInfoExt) 
	throws GeneralSecurityException {
	return createCredential(certs, privateKey, bits, lifetime, 
				delegationMode, proxyCertInfoExt, null);
    }

    /**
     * @deprecated Please use 
     * {@link #createCertificate(InputStream, X509Certificate, PrivateKey, int,
     * int, X509ExtensionSet, String) createCertificate()} instead. The
     * <code>ProxyCertInfo</code> parameter can be passed in the 
     * <code>X509ExtensionSet</code> using 
     * {@link org.globus.gsi.proxy.ext.ProxyCertInfoExtension 
     * ProxyCertInfoExtension} class.
     */ 
    public X509Certificate createCertificate(InputStream certRequestInputStream,
					     X509Certificate cert,
					     PrivateKey privateKey,
					     int lifetime,
					     int delegationMode,
					     ProxyCertInfo proxyCertInfoExt,
					     String cnValue) 
	throws IOException, GeneralSecurityException {
	return createCertificate(certRequestInputStream,
				 cert, privateKey, lifetime, delegationMode, 
				 createExtensionSet(proxyCertInfoExt),
				 cnValue);
    }

    /**
     * @deprecated 
     *
     * @see #createCertificate(InputStream, X509Certificate, PrivateKey, 
     *      int, int, ProxyCertInfo, String) createCertificate
     */
    public X509Certificate createCertificate(InputStream certRequestInputStream,
					     X509Certificate cert,
					     PrivateKey privateKey,
					     int lifetime,
					     int delegationMode,
					     ProxyCertInfo proxyCertInfoExt) 
	throws IOException, GeneralSecurityException {
	return createCertificate(certRequestInputStream,
				 cert,
				 privateKey,
				 lifetime,
				 delegationMode,
				 proxyCertInfoExt,
				 null);
    }

}
