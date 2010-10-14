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
package org.globus.gsi.ptls;

import java.util.Vector;
import java.util.Arrays;
import java.io.ByteArrayInputStream;
import java.security.GeneralSecurityException;
import java.security.cert.X509Certificate;
import java.security.cert.CertificateFactory;

import org.globus.common.ChainedGeneralSecurityException;
import org.globus.gsi.GSIConstants;
import org.globus.util.I18n;

import COM.claymoresystems.cert.X509Cert;
import COM.claymoresystems.cert.X509Name;
import COM.claymoresystems.sslg.DistinguishedName;
import COM.claymoresystems.sslg.CertVerifyPolicyInt;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * A collection of utility functions for PureTLS library.
 */
public class PureTLSUtil {

    private static I18n i18n =
            I18n.getI18n("org.globus.gsi.ptls.errors",
                         PureTLSUtil.class.getClassLoader());

    private static Log logger = 
	LogFactory.getLog(PureTLSUtil.class.getName());

    static {
	PureTLSContext.init();
    }

    /**
     * Converts PureTLS specific X509 certificate object 
     * into standard Java X509 certificate object
     * (right now it is using BouncyCastle provider to 
     * convert).
     *
     * @param cert PureTLS X509 certificate object 
     * @return standard Java X509 certificate object
     * @exception GeneralSecurityException if conversion fails.
     */
    public static X509Certificate convertCert(X509Cert cert) 
	throws GeneralSecurityException {
	CertificateFactory f = CertificateFactory.getInstance("X.509", "BC");
	ByteArrayInputStream in = new ByteArrayInputStream(cert.getDER());
	return (X509Certificate)f.generateCertificate(in);
    }

    /**
     * Converts Globus formatted string into a X509Name
     * object.
     *
     * @param globusID Globus-formatted subject to convert.
     * @return the X509Name object.
     * @exception Exception if conversion fails.
     */
    public static X509Name getX509Name(String globusID) 
	throws Exception {
	Vector dn = new Vector();
	int off = 0;
	int start = 0;
	boolean done = false;
	while(!done) {

	    int pos1 = globusID.indexOf('=', off);
	    if (pos1 == -1) {
            throw new Exception(i18n.getMessage("malformedName", new String[] {
                "=", globusID }));
        }

	    Vector rdn = null;

	    start = pos1 + 1;
	    for (;;) {
		int pos2 = globusID.indexOf('=', pos1+1);
		if (pos2 == -1) {
		    rdn = parseRDN(globusID.substring(off));
		    done = true;
		    break;
		} else {
		    int pos3 = globusID.lastIndexOf('/', pos2);
		    if (pos3 != -1) {
			if (pos3 <= pos1) {
			    pos1 = pos2;
			} else {
			    rdn = parseRDN(globusID.substring(off, pos3));
			    off = pos3;
			    break;
			}
		    } else {
			throw new Exception(i18n.getMessage("malformedName", new String[] {
                    "/", globusID}));
		    }
		}
	    }

	    if (rdn != null) {
		dn.addElement(rdn);
	    }
	}
	return new X509Name(dn);
    }

    private static Vector parseRDN(String token) 
	throws Exception {
        if (token.charAt(0) != '/') {
            throw new Exception(i18n.getMessage("invalidToken00", token));
        }
	Vector rdn = null;
	int pos = token.indexOf('+');
	if (pos == -1) {
	    rdn = new Vector(1);
	    rdn.addElement(getAVA(token.substring(1)));
	} else {
	    rdn = new Vector(2);
	    rdn.addElement(getAVA(token.substring(1, pos)));
	    rdn.addElement(getAVA(token.substring(pos+1)));
	}
	return rdn;
    }

    private static String[] getAVA(String rdn) 
	throws Exception {
	int pos = rdn.indexOf('=');
	if (pos == -1) {
	    throw new Exception(i18n.getMessage("rdnMissing", rdn));
	}
	String [] ava = new String[2];
	ava[0] = rdn.substring(0, pos).trim().toUpperCase();
	ava[1] = rdn.substring(pos+1).trim();
	return ava;
    }

    /**
     * Returns the base name of a proxy. Strips all
     * "cn=proxy" or "cn=limited proxy" components.
     *
     * @deprecated Only works with Globus legacy proxies.
     */
    public static X509Name getBase(DistinguishedName name) {
	X509Name nm = dupName(name);
	Vector dn = nm.getName();
	int len = dn.size();
	for (int i=len-1;i>=0;i--) {
	    Vector rdn = (Vector)dn.elementAt(i);
	    // checks only first ava entry
	    String [] ava = (String[])rdn.elementAt(0);
	    if (ava[0].equalsIgnoreCase("CN") &&
		(ava[1].equalsIgnoreCase("proxy") || 
		 ava[1].equalsIgnoreCase("limited proxy"))) {
		dn.removeElementAt(i);
	    } else {
		break;
	    }
	}
	return new X509Name(dn);
    }

    /**
     * Returns proxy name. 
     *
     * @deprecated Only works for Globus legacy proxies.
     */
    public static int checkProxyName(X509Cert cert) {
	int rs = -1;
	DistinguishedName subject = dupName(cert.getSubjectName());
	Vector subjectDN = subject.getName();
	Vector lastAva = (Vector)subjectDN.elementAt(subjectDN.size()-1);
	String [] ava = (String[])lastAva.elementAt(0);
	
	if (ava[0].equalsIgnoreCase("CN")) {
	    if (ava[1].equalsIgnoreCase("proxy")) {
		rs = GSIConstants.GSI_2_PROXY;
	    } else if (ava[1].equalsIgnoreCase("limited proxy")) {
		rs = GSIConstants.GSI_2_LIMITED_PROXY;
	    }
		
	    if (rs != -1) {
		Vector nameDN = dupName(cert.getIssuerName()).getName();
		nameDN.addElement(lastAva);
		X509Name newName = new X509Name(nameDN);

		return (Arrays.equals(subject.getNameDER(), newName.getNameDER())) ? rs : -1;
	    }
	}

	return rs;
    }

    /**
     * Replicates a X509Name object.
     * 
     * @param name X509Name object to replicate.
     * @return the replicated object.
     */
    public static X509Name dupName(DistinguishedName name) {
	return new X509Name( name.getName() );
    }

    /**
     * Converts standard Java X509 certificate array into a Vector
     * of X509Cert objects (in the reverse order)
     *
     * @param certs certificate array to convert.
     * @return the converted Vector of X509Cert objects. Null if
     *         <code>certs</code> array was null.
     * @exception GeneralSecurityException if conversion fails.
     */
    public static Vector certificateChainToVector(X509Certificate[] certs)
	throws GeneralSecurityException {
        if (certs == null) {
            return null;
        }
	Vector v = new Vector(certs.length);
	try {
	    for (int i=certs.length-1;i>=0;i--) {
		v.addElement(new X509Cert(certs[i].getEncoded()));
	    }
	} catch (Exception e) {
	    throw new ChainedGeneralSecurityException(i18n.
                getMessage("conversionFail"), e);
	}
	return v;
    }
    
    /**
     * Converts a Vector of X509Cert objects into a standard
     * Java X509 certificate array (in the reverse order).
     *
     * @param chain the Vector of X509Cert objects to convert.
     * @return the converted X509 certificate array
     * @exception GeneralSecurityException if conversion fails.
     */
    public static X509Certificate[] certificateChainToArray(Vector chain)
	throws GeneralSecurityException {
	int size = chain.size();
	X509Certificate [] certs = new X509Certificate[size];
	for (int i=0;i<size;i++) {
	    certs[i] = convertCert((X509Cert)chain.elementAt(size - 1 - i));
	}
	return certs;
    }

    /**
     * Returns a default certificate checking policy.
     * This is not used as much as the certificate checking
     * was mostly abstracted out from PureTLS code and
     * moved into {@link org.globus.gsi.proxy.ProxyPathValidator
     * ProxyPathValidator}.
     *
     * @return the default certificate checking policy.
     */
    public static CertVerifyPolicyInt getDefaultCertVerifyPolicy() {
	CertVerifyPolicyInt certPolicy = 
	    new CertVerifyPolicyInt();

	// we do validation checking now
	certPolicy.checkDates(false);
	certPolicy.requireBasicConstraints(false);
	certPolicy.requireBasicConstraintsCritical(false);
	certPolicy.requireKeyUsage(false);

	return certPolicy;
    }

    /**
     * Returns the Globus formatted representation of the
     * subject DN of the specified certificate.
     *
     * @param cert the encoded certificate
     * @return the Globus formatted representation of the 
     *         subject DN.
     * @exception Exception if something goes wrong.
     * @deprecated Only works with Globus legacy proxies.
     */
    public static String getGlobusId(byte[] cert) 
	throws Exception {
	X509Cert crt = new X509Cert(cert);
	DistinguishedName subject = PureTLSUtil.getBase(crt.getSubjectName());
	return toGlobusID(subject);
    }

    /**
     * Returns the Globus formatted representation of the
     * subject DN of the specified DN.
     *
     * @param subject the DN
     * @return the Globus formatted representation of the 
     *         subject DN.
     */
    public static String toGlobusID(DistinguishedName subject) {
	Vector dn = subject.getName();
	int len = dn.size();
	StringBuffer buf = new StringBuffer();
	for (int i=0;i<len;i++) {
	    Vector rdn = (Vector)dn.elementAt(i);
	    // checks only first ava entry
	    String [] ava = (String[])rdn.elementAt(0);
	    buf.append('/').append(ava[0]).append('=').append(ava[1]);
	}
	return buf.toString();
    }
    




/*
import cryptix.provider.rsa.BaseRSAPrivateKey;
import cryptix.provider.rsa.BaseRSAPublicKey;
import COM.claymoresystems.cert.X509RSAPublicKey;

// Convert Cryptix BaseRSAPrivateKey into RSAPrivateKey
new WrappedRSAPrivateKey((BaseRSAPrivateKey)privateKey)
// Convert Cryptix BaseRSAPublicKey into X509RSAPublicKey
new X509RSAPublicKey(((BaseRSAPublicKey)keyPair.getPublic())),
*/

}
