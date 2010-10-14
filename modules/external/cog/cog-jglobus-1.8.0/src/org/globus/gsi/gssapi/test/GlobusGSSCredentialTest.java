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
package org.globus.gsi.gssapi.test;

import java.io.File;
import java.security.cert.X509Certificate;

import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSCredential;

import org.gridforum.jgss.ExtendedGSSManager;
import org.gridforum.jgss.ExtendedGSSCredential;

import org.globus.gsi.gssapi.GlobusGSSCredentialImpl;
import org.globus.gsi.gssapi.GlobusGSSManagerImpl;
import org.globus.gsi.gssapi.GlobusGSSException;
import org.globus.gsi.gssapi.GSSConstants;

import junit.framework.TestCase;
import junit.framework.TestSuite;
import junit.framework.Test;

public class GlobusGSSCredentialTest extends TestCase {

    ExtendedGSSManager manager;

    public GlobusGSSCredentialTest(String name) {
	super(name);
    }
    
    public static void main (String[] args) {
	junit.textui.TestRunner.run (suite());
    }

    public static Test suite() {
	return new TestSuite(GlobusGSSCredentialTest.class);
    }
    
    protected void setUp() throws Exception {
	manager = new GlobusGSSManagerImpl();
    }

    public void testImportBadFile() throws Exception {
	String handle = "PROXY = /a/b/c";
	
	try {
	    manager.createCredential(handle.getBytes(),
				     ExtendedGSSCredential.IMPEXP_MECH_SPECIFIC,
				     GSSCredential.DEFAULT_LIFETIME,
				     null,
				     GSSCredential.ACCEPT_ONLY);
	    fail("Exception not thrown as expected.");
	} catch (GSSException e) {
	    // TODO: check for specific major/minor code
	}
	
    }

    public void testImportBadOption() throws Exception {
	String handle = "PROXY = /a/b/c";
	
	try {
	    manager.createCredential(handle.getBytes(),
				     3,
				     GSSCredential.DEFAULT_LIFETIME,
				     null,
				     GSSCredential.ACCEPT_ONLY);
	    fail("Exception not thrown as expected.");
	} catch (GSSException e) {
	    if (e.getMajor() != GSSException.FAILURE &&
		e.getMinor() != GlobusGSSException.BAD_ARGUMENT) {
		e.printStackTrace();
		fail("Unexpected exception");
	    }
	}
	
    }

    public void testImportExportOpaque() throws Exception {
	
	GlobusGSSCredentialImpl cred = 
	    (GlobusGSSCredentialImpl)manager.createCredential(GSSCredential.ACCEPT_ONLY);
	assertTrue(cred != null);
	
	byte [] data = cred.export(ExtendedGSSCredential.IMPEXP_OPAQUE);
	assertTrue(data != null);

	System.out.println(new String(data));
	
	GlobusGSSCredentialImpl cred2 = 
	    (GlobusGSSCredentialImpl)manager.createCredential(data,
							      ExtendedGSSCredential.IMPEXP_OPAQUE,
							      GSSCredential.DEFAULT_LIFETIME,
							      null,
							      GSSCredential.ACCEPT_ONLY);
	assertTrue(cred2 != null);
	assertEquals(cred.getPrivateKey(), cred2.getPrivateKey());
    }
    
    public void testImportExportMechSpecific() throws Exception {
	
	GlobusGSSCredentialImpl cred = 
	    (GlobusGSSCredentialImpl)manager.createCredential(GSSCredential.ACCEPT_ONLY);
	assertTrue(cred != null);
	
	byte [] data = cred.export(ExtendedGSSCredential.IMPEXP_MECH_SPECIFIC);
	assertTrue(data != null);

	String handle = new String(data);
	System.out.println(handle);
	
	GlobusGSSCredentialImpl cred2 = 
	    (GlobusGSSCredentialImpl)manager.createCredential(data,
							      ExtendedGSSCredential.IMPEXP_MECH_SPECIFIC,
							      GSSCredential.DEFAULT_LIFETIME,
							      null,
							      GSSCredential.ACCEPT_ONLY);
	assertTrue(cred2 != null);

	assertEquals(cred.getPrivateKey(), cred2.getPrivateKey());

	handle = handle.substring(handle.indexOf('=')+1);
	assertTrue((new File(handle)).delete());
    }

    public void testInquireByOid() throws Exception {

	ExtendedGSSCredential cred =
	    (ExtendedGSSCredential)manager.createCredential(GSSCredential.ACCEPT_ONLY);

	Object tmp = null;
	X509Certificate[] chain = null;
	
	tmp = cred.inquireByOid(GSSConstants.X509_CERT_CHAIN);
	assertTrue(tmp != null);
	assertTrue(tmp instanceof X509Certificate[]);
	chain = (X509Certificate[])tmp;
	assertTrue(chain.length > 0);
    }
}
