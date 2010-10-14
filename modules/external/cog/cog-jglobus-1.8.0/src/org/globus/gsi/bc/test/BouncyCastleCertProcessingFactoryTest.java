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
package org.globus.gsi.bc.test;

import java.util.Set;
import java.security.cert.X509Certificate;
import org.globus.gsi.GlobusCredential;
import org.globus.gsi.GSIConstants;
import org.globus.gsi.X509ExtensionSet;
import org.globus.gsi.X509Extension;
import org.globus.gsi.bc.BouncyCastleCertProcessingFactory;
import org.globus.gsi.bc.BouncyCastleX509Extension;
import org.globus.gsi.proxy.ext.ProxyPolicy;
import org.globus.gsi.proxy.ext.ProxyCertInfo;
import org.globus.gsi.proxy.ext.ProxyCertInfoExtension;

import org.bouncycastle.asn1.x509.BasicConstraints;
import org.bouncycastle.asn1.x509.X509Extensions;

import junit.framework.TestCase;
import junit.framework.TestSuite;
import junit.framework.Test;

public class BouncyCastleCertProcessingFactoryTest extends TestCase {

    public static BouncyCastleCertProcessingFactory factory = 
	BouncyCastleCertProcessingFactory.getDefault();

    public BouncyCastleCertProcessingFactoryTest(String name) {
	super(name);
    }
    
    public static void main (String[] args) {
	junit.textui.TestRunner.run (suite());
    }

    public static Test suite() {
	return new TestSuite(BouncyCastleCertProcessingFactoryTest.class);
    }
    
    public void testResctrictedNoProxyCertInfoExt() throws Exception {
	
	GlobusCredential cred = GlobusCredential.getDefaultCredential();
	
	try {
	    factory.createCredential(cred.getCertificateChain(),
				     cred.getPrivateKey(),
				     512,
				     60 * 60,
				     GSIConstants.GSI_3_RESTRICTED_PROXY,
				     (X509ExtensionSet)null,
				     null);
	    fail("Expected to fail");
	} catch (IllegalArgumentException e) {
	    // that's what we expected
	}
    }

    public void testResctrictedWithOtherExt() throws Exception {

	GlobusCredential cred = GlobusCredential.getDefaultCredential();

	X509Extension ext = null;
	
	String oid = "1.2.3.4";
	String expectedValue = "foo";
	boolean critical = false;

	String policyOid = "1.2.3.4.5.6.7.8.9";
	String policyValue = "bar";
	
	X509ExtensionSet extSet = new X509ExtensionSet();
	ext = new X509Extension(oid, critical, expectedValue.getBytes());
	extSet.add(ext);
	
	BasicConstraints constraints = new BasicConstraints(false, 15);
	ext = new BouncyCastleX509Extension(X509Extensions.BasicConstraints.getId(),
					    false, constraints);
	extSet.add(ext);
	
	ProxyPolicy policy = new ProxyPolicy(policyOid, policyValue.getBytes());
	ext = new ProxyCertInfoExtension(new ProxyCertInfo(policy));
	extSet.add(ext);
	
	GlobusCredential newCred = 
	    factory.createCredential(cred.getCertificateChain(),
				     cred.getPrivateKey(),
				     512,
				     60 * 60,
				     GSIConstants.GSI_3_RESTRICTED_PROXY,
				     extSet,
				     null);
	
	X509Certificate newCert = newCred.getCertificateChain()[0];

	System.out.println(newCert);

	verifyExtension(newCert, oid, expectedValue, critical);
	
	byte [] realValue = 
	    X509Extension.getExtensionValue(newCert, 
					    ProxyCertInfo.OID.getId());
	assertTrue(realValue != null && realValue.length > 0);

	ProxyCertInfo proxyCertInfo = 
	    ProxyCertInfo.getInstance(realValue);
	
	assertTrue(proxyCertInfo != null);
	assertTrue(proxyCertInfo.getProxyPolicy() != null);
	assertEquals(policyOid, 
		     proxyCertInfo.getProxyPolicy().getPolicyLanguage().getId());
	assertEquals(policyValue,
		     proxyCertInfo.getProxyPolicy().getPolicyAsString());
    }

    public void testExtensions() throws Exception {
	
	GlobusCredential cred = GlobusCredential.getDefaultCredential();
	X509Extension ext = null;
	
	String oid1 = "1.2.3.4";
	String expectedValue1 = "foo";
	boolean critical1 = false;
	
	String oid2 = "5.6.7.8";
	String expectedValue2 = "bar";
	boolean critical2 = true;
	
	X509ExtensionSet extSet = new X509ExtensionSet();
	ext = new X509Extension(oid1, critical1, expectedValue1.getBytes());
	extSet.add(ext);
	ext = new X509Extension(oid2, critical2, expectedValue2.getBytes());
	extSet.add(ext);

	GlobusCredential newCred = 
	    factory.createCredential(cred.getCertificateChain(),
				     cred.getPrivateKey(),
				     512,
				     60 * 60,
				     GSIConstants.GSI_3_IMPERSONATION_PROXY,
				     extSet,
				     null);

	X509Certificate newCert = newCred.getCertificateChain()[0];

	System.out.println(newCert);

	verifyExtension(newCert, oid1, expectedValue1, critical1);
	verifyExtension(newCert, oid2, expectedValue2, critical2);
    }

    private void verifyExtension(X509Certificate cert, 
				 String oid,
				 String expectedValue,
				 boolean critical) throws Exception {
	byte [] realValue = X509Extension.getExtensionValue(cert, oid);

	assertTrue(realValue != null && realValue.length > 0);
	assertEquals(expectedValue, new String(realValue));

	Set exts = null;
	if (critical) {
	    exts = cert.getCriticalExtensionOIDs();
	} else {
	    exts = cert.getNonCriticalExtensionOIDs();
	}
	
	assertTrue(exts.contains(oid));
    }

}

