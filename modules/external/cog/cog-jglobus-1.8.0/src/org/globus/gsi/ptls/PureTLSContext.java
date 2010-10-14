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

import java.security.cert.X509Certificate;
import java.security.PrivateKey;
import java.security.Security;
import java.security.interfaces.RSAPrivateCrtKey;
import java.security.GeneralSecurityException;
import java.util.Vector;
import java.io.IOException;

import org.globus.gsi.GlobusCredential;
import org.globus.common.ChainedGeneralSecurityException;

import COM.claymoresystems.ptls.SSLContext;
import COM.claymoresystems.ptls.SSLSessionData;
import COM.claymoresystems.cert.X509RSAPrivateKey;
import cryptix.provider.rsa.RawRSAPrivateKey;

import org.bouncycastle.jce.provider.BouncyCastleProvider;

/**
 * Extensions to the PureTLS's <code>SSLContext</code> class 
 * to allow setting the context credentials and trusted certificates
 * as objects instead of reading them from files.
 * Also allows the use of standard Java security objects for
 * credential and trusted certificates instead of proprietary
 * PureTLS/Cryptix format.
 */
public class PureTLSContext extends SSLContext {
    
    static {
	// The BC provider must be after Cryptix and 
	// ClaymoreSystems provider
	Security.removeProvider("BC");
	Security.addProvider(new BouncyCastleProvider());
    }

    static void init() {
	// noop - just to install the providers
    }

    /**
     * Sets trusted certificates.
     *
     * @param certs the trusted certificates.
     * @exception GeneralSecurityException in case of an error
     */
    public void setTrustedCertificates(X509Certificate [] certs) 
	throws GeneralSecurityException {
	Vector list = getRootList();
	if (list == null) {
	    list = new Vector();
	    setRootList(list);
	}
        list.clear();
        if (certs != null) {
            for (int i=0;i<certs.length;i++) {
                list.addElement(certs[i].getEncoded());
            }
        }
    }
    
    /**
     * Sets the credential.
     *
     * @param cred the credential
     * @exception GeneralSecurityException in case of an error
     */ 
    public void setCredential(GlobusCredential cred) 
	throws GeneralSecurityException {
	X509Certificate [] certs = cred.getCertificateChain();
	Vector v = new Vector(certs.length);
	for (int i=certs.length-1;i>=0;i--) {
	    v.addElement(certs[i].getEncoded());
	}
	setCertificateChain(v);
	setPrivateKey(convertPrivateKey(cred.getPrivateKey()));
	try {
	    setPublicKeyFromCert(certs[0].getEncoded());
	} catch (IOException e) {
	    throw new ChainedGeneralSecurityException("", e);
	}
    }
    
    private static PrivateKey convertPrivateKey(PrivateKey key) {
	if (key instanceof RSAPrivateCrtKey) {
	    RSAPrivateCrtKey rsaKey = (RSAPrivateCrtKey)key;
	    RawRSAPrivateKey ck = new RawRSAPrivateKey(rsaKey.getPrivateExponent(),
						       rsaKey.getPrimeP(),
						       rsaKey.getPrimeQ(),
						       rsaKey.getCrtCoefficient());
	    return new X509RSAPrivateKey(ck);
	} else {
	    throw new IllegalArgumentException();
	}
    }

    // ensure that sessions are not reused
    protected synchronized SSLSessionData findSession(String key) {
	return null;
    }
    
    protected synchronized void storeSession(String key, SSLSessionData sd) {
    }

}
