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

import org.globus.gsi.gssapi.GlobusGSSCredentialImpl;

import org.globus.gsi.gssapi.GSSConstants;
import org.globus.gsi.GlobusCredential;
import org.globus.gsi.GSIConstants;

import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSName;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.MessageProp;
import org.ietf.jgss.Oid;

import org.gridforum.jgss.ExtendedGSSManager;
import org.gridforum.jgss.ExtendedGSSCredential;
import org.gridforum.jgss.ExtendedGSSContext;

import org.globus.gsi.gssapi.GlobusGSSManagerImpl;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.cert.X509Certificate;
import java.util.Date;
import java.net.Socket;
import java.net.ServerSocket;
import java.net.InetAddress;

import junit.framework.TestCase;
import junit.framework.TestSuite;
import junit.framework.Test;

public class GlobusGSSContextTest extends TestCase {

    private static final byte [] MSG = "this is a test 1 2 3".getBytes();

    GSSContext clientContext;
    GSSContext serverContext;

    public GlobusGSSContextTest(String name) {
	super(name);
    }
    
    public static void main (String[] args) {
	junit.textui.TestRunner.run (suite());
    }

    public static Test suite() {
	return new TestSuite(GlobusGSSContextTest.class);
    }
    
    protected void setUp() throws Exception {

	System.setProperty("org.globus.gsi.gssapi.provider",
			   "org.globus.gsi.gssapi.GlobusGSSManagerImpl");

	GSSManager manager = getGSSManager();

        GSSCredential gssCred = 
	    manager.createCredential(GSSCredential.INITIATE_ONLY);

        GSSName gssName = gssCred.getName();

	serverContext = manager.createContext((GSSCredential)null);
	
	clientContext = manager.createContext(gssName,
					      GSSConstants.MECH_OID,
					      null, 
					      GSSContext.DEFAULT_LIFETIME);

    }

    protected void tearDown() throws Exception {
	if (clientContext != null) {
	    clientContext.dispose();
	}

	if (serverContext != null) {
	    serverContext.dispose();
	}
    }

    protected GSSManager getGSSManager() throws Exception {
	return new GlobusGSSManagerImpl();
    }

    private void establishContext() throws Exception {

	assertTrue("client ctx already established.", !clientContext.isEstablished());
	assertTrue("server ctx already established.", !serverContext.isEstablished());

	byte [] empty = new byte[0];

	byte [] inToken = empty;
	byte [] outToken = null;

	while (!clientContext.isEstablished()) {
	    outToken = 
		clientContext.initSecContext(inToken, 0, inToken.length);
	    
	    if (outToken == null || outToken.length == 0) {
		fail("bad token");
	    }

	    inToken = 
		serverContext.acceptSecContext(outToken, 0, outToken.length);
	    
	    if (inToken == null && !clientContext.isEstablished()) {
		fail("bad token");
	    }
	}
	
	assertTrue("client ctx not established.", clientContext.isEstablished());
	assertTrue("server ctx not established.", serverContext.isEstablished());
    }

    public void testInquireByOid() throws Exception {

	ExtendedGSSContext cc = (ExtendedGSSContext)clientContext;

	ExtendedGSSContext sc = (ExtendedGSSContext)serverContext;
	sc.setOption(GSSConstants.REQUIRE_CLIENT_AUTH,
		     Boolean.FALSE);

	clientContext.requestCredDeleg(false);
	clientContext.requestConf(false);
	
	establishContext();

	Object tmp = null;
	X509Certificate[] chain = null;
	
	// should get server's chain
	tmp = cc.inquireByOid(GSSConstants.X509_CERT_CHAIN);
	assertTrue(tmp != null);
	assertTrue(tmp instanceof X509Certificate[]);
	chain = (X509Certificate[])tmp;
	assertTrue(chain.length > 0);
	
	// should be null since client auth disabled
	tmp = sc.inquireByOid(GSSConstants.X509_CERT_CHAIN);
	assertTrue(tmp == null);
    }

    // basic delegation tests
    public void testDelegation() throws Exception {

	// enable delegation
	clientContext.requestCredDeleg(true);
	clientContext.requestConf(true);

	ExtendedGSSContext ctx = (ExtendedGSSContext)clientContext;
	ctx.setOption(GSSConstants.DELEGATION_TYPE,
		      GSIConstants.DELEGATION_TYPE_FULL);

	establishContext();
	
	ExtendedGSSCredential cred = null;

	cred = (ExtendedGSSCredential)serverContext.getDelegCred();
	assertTrue(cred != null);

	GlobusCredential proxy = null;
	proxy = ((GlobusGSSCredentialImpl)cred).getGlobusCredential();
	assertTrue(proxy != null);
	assertTrue( (proxy.getProxyType() == GSIConstants.GSI_2_PROXY) ||
		    (proxy.getProxyType() == 
                     GSIConstants.GSI_3_IMPERSONATION_PROXY) ||
                    (proxy.getProxyType() == 
                     GSIConstants.GSI_4_IMPERSONATION_PROXY));

	System.out.println(proxy);
	
	GSSManager manager = getGSSManager();

        GSSCredential gssCred = 
	    manager.createCredential(GSSCredential.INITIATE_ONLY);

	// create server ctx using delegated cred
	serverContext = manager.createContext((GSSCredential)null);

	// create client ctx using default creds
	clientContext = manager.createContext(gssCred.getName(),
					      GSSConstants.MECH_OID,
					      cred,
					      GSSContext.DEFAULT_LIFETIME);
	clientContext.requestCredDeleg(true);

	establishContext();

	cred = (ExtendedGSSCredential)serverContext.getDelegCred();
	assertTrue(cred != null);
	
	proxy = ((GlobusGSSCredentialImpl)cred).getGlobusCredential();
	assertTrue(proxy != null);
	assertTrue( (proxy.getProxyType() == GSIConstants.GSI_2_LIMITED_PROXY)
                    || (proxy.getProxyType() == 
                        GSIConstants.GSI_3_LIMITED_PROXY) || 
                    (proxy.getProxyType() == 
                     GSIConstants.GSI_4_LIMITED_PROXY));
	System.out.println(proxy);
    }

    public void testNewDelegation() throws Exception {
	// disable delegation
	clientContext.requestCredDeleg(false);
	clientContext.requestConf(true);
	
	establishContext();
	int i=0;

	byte [] input = new byte[0];
	byte [] output = null;

	ExtendedGSSContext cl = (ExtendedGSSContext)clientContext;
	ExtendedGSSContext sr = (ExtendedGSSContext)serverContext;

	do {
	    output = cl.initDelegation(null, null, 0, input, 0, input.length);
	    if (i == 0) { // first token length should be greater then 1
		assertTrue(output.length != 1);
	    }
	    input = sr.acceptDelegation(0, output, 0, output.length);
	    i++;
	} while (!cl.isDelegationFinished());

	assertTrue("client ctx not established.", cl.isDelegationFinished());
	assertTrue("server ctx not established.", sr.isDelegationFinished());

	ExtendedGSSCredential cred = null;

	cred = (ExtendedGSSCredential)sr.getDelegatedCredential();
	assertTrue(cred != null);

	// disables wrap/unwrap of delegation tokens
	cl.setOption(GSSConstants.GSS_MODE,
		     GSIConstants.MODE_SSL);
	sr.setOption(GSSConstants.GSS_MODE,
		     GSIConstants.MODE_SSL);

	int reqLifetime = 240; // 4 minutes
	i = 0;
	input = new byte[0];
	do {
	    output = cl.initDelegation(cred, null, reqLifetime, input, 0, input.length);
	    if (i == 0) { // first token should be of length 1
		assertEquals(1, output.length);
	    }
	    input = sr.acceptDelegation(0, output, 0, output.length);
	    i++;
	} while (!cl.isDelegationFinished());
	
	assertTrue("client ctx not established.", cl.isDelegationFinished());
	assertTrue("server ctx not established.", sr.isDelegationFinished());

	cred = (ExtendedGSSCredential)sr.getDelegatedCredential();
	assertTrue(cred != null);

	GlobusCredential globusCred = 
	    ((GlobusGSSCredentialImpl)cred).getGlobusCredential();
	Date notAfter = globusCred.getCertificateChain()[0].getNotAfter();
	Date notBefore = globusCred.getCertificateChain()[0].getNotBefore();
	System.out.println(globusCred);

	int seconds = (int)((notAfter.getTime() - notBefore.getTime() - 5*60000) / 1000);

	assertEquals("lifetime", reqLifetime, seconds);
    }

    
    public void testContextExpiration() throws Exception {
	// disable delegation
	int time = 15; // 15 seconds;

	clientContext.requestCredDeleg(false);
	clientContext.requestConf(true);

	// request short context life time
	clientContext.requestLifetime(time); 

	// enable context expiration checking
	ExtendedGSSContext ctx = (ExtendedGSSContext)clientContext;
	ctx.setOption(GSSConstants.CHECK_CONTEXT_EXPIRATION,
		      Boolean.TRUE);

	establishContext();

	assertTrue(clientContext.getLifetime() > 0);

	Thread.sleep((int)(time * 1.3 * 1000));
	
	assertTrue(clientContext.getLifetime() < 0);
	
	try {
	    clientContext.wrap(MSG, 0, MSG.length, null);
	    fail("Wrap() did not throw exeption as expected");
	} catch (GSSException e) {
	    if (e.getMajor() != GSSException.CONTEXT_EXPIRED) {
		e.printStackTrace();
		fail("Unexpected GSSException");
	    }
	}

	try {
	    clientContext.unwrap(MSG, 0, MSG.length, null);
	    fail("Unwrap() did not throw exeption as expected");
	} catch (GSSException e) {
	    if (e.getMajor() != GSSException.CONTEXT_EXPIRED) {
		e.printStackTrace();
		fail("Unexpected GSSException");
	    }
	}

	try {
	    clientContext.getMIC(MSG, 0, MSG.length, null);
	    fail("getMIC() did not throw exeption as expected");
	} catch (GSSException e) {
	    if (e.getMajor() != GSSException.CONTEXT_EXPIRED) {
		e.printStackTrace();
		fail("Unexpected GSSException");
	    }
	}

	try {
	    clientContext.verifyMIC(MSG, 0, MSG.length,
				    MSG, 0, MSG.length, null);
	    fail("verifyMIC() did not throw exeption as expected");
	} catch (GSSException e) {
	    if (e.getMajor() != GSSException.CONTEXT_EXPIRED) {
		e.printStackTrace();
		fail("Unexpected GSSException");
	    }
	}
	
    }

    public void testLimitedProxyChecking() throws Exception {
	clientContext.requestCredDeleg(true);
	clientContext.requestConf(true);
	
	establishContext();
	
	ExtendedGSSCredential cred = null;

	cred = (ExtendedGSSCredential)serverContext.getDelegCred();
	assertTrue(cred != null);

	GlobusCredential proxy = null;
	proxy = ((GlobusGSSCredentialImpl)cred).getGlobusCredential();
	assertTrue(proxy != null);
	assertTrue( (proxy.getProxyType() == GSIConstants.GSI_2_LIMITED_PROXY)
                    || (proxy.getProxyType() == 
                        GSIConstants.GSI_3_LIMITED_PROXY) || 
                    (proxy.getProxyType() == 
                     GSIConstants.GSI_4_LIMITED_PROXY));

	GSSManager manager = getGSSManager();

	// create server ctx using delegated cred
	serverContext = manager.createContext((GSSCredential)null);

	// create client ctx using default creds
	clientContext = manager.createContext(null, 
					      GSSConstants.MECH_OID,
					      cred,
					      GSSContext.DEFAULT_LIFETIME);
	
	ExtendedGSSContext sr = (ExtendedGSSContext)serverContext;
	sr.setOption(GSSConstants.REJECT_LIMITED_PROXY,
		     Boolean.TRUE);
	
	try {
	    establishContext();
	    fail("establishContext() did not throw exception as expected");
	} catch (GSSException e) {
	    if (e.getMajor() != GSSException.UNAUTHORIZED) {
		e.printStackTrace();
		fail("Unexpected Exception");
	    }
	}

	// create server ctx using delegated cred
	serverContext = manager.createContext(cred);
	
	// create client ctx using default creds
	clientContext = manager.createContext(null, 
					      GSSConstants.MECH_OID,
					      null,
					      GSSContext.DEFAULT_LIFETIME);
	
	ExtendedGSSContext cl = (ExtendedGSSContext)clientContext;
	cl.setOption(GSSConstants.REJECT_LIMITED_PROXY,
		     Boolean.TRUE);
	
	try {
	    establishContext();
	    fail("establishContext() did not throw exception as expected");
	} catch (GSSException e) {
	    if (e.getMajor() != GSSException.UNAUTHORIZED) {
		e.printStackTrace();
		fail("Unexpected Exception");
	    }
	}
      }

    /* client has credentials but it requests to be anonymous */
    public void testAnonymousClient1() throws Exception {
	clientContext.requestCredDeleg(false);
	
	// request anonymity
	clientContext.requestAnonymity(true);
	
	// without this handshake will fail
	ExtendedGSSContext sr = (ExtendedGSSContext)serverContext;
	sr.setOption(GSSConstants.REQUIRE_CLIENT_AUTH,
		     Boolean.FALSE);
	
	establishContext();
	
	assertTrue(clientContext.getSrcName().isAnonymous());
	assertTrue(clientContext.getAnonymityState());
	assertTrue(serverContext.getSrcName().isAnonymous());
    }

    /* client is initialized with anonymous credentials */
    public void testAnonymousClient2() throws Exception {

	GSSManager manager = getGSSManager();

	GSSName anonName = manager.createName((String)null, null);
	assertTrue(anonName.isAnonymous());

	GSSCredential anonCred = manager.createCredential(anonName,
							  GSSCredential.INDEFINITE_LIFETIME,
							  (Oid)null,
							  GSSCredential.INITIATE_AND_ACCEPT);
	assertTrue(anonCred.getName().isAnonymous());

	// client ctx initalized with anon cred
	clientContext = manager.createContext(null, 
					      GSSConstants.MECH_OID,
					      anonCred, 
					      GSSContext.DEFAULT_LIFETIME);

	// without this handshake will fail
	ExtendedGSSContext sr = (ExtendedGSSContext)serverContext;
	sr.setOption(GSSConstants.REQUIRE_CLIENT_AUTH,
		     Boolean.FALSE);
	
	establishContext();

	assertTrue(clientContext.getSrcName().isAnonymous());
	assertTrue(clientContext.getAnonymityState());
	assertTrue(serverContext.getSrcName().isAnonymous());
    }

    /* checks if anonymity state is set correctly */
    public void testAnonymousServer1() throws Exception {
	clientContext.requestCredDeleg(false);
	clientContext.requestAnonymity(false);

	// request anonymity - this should have no baring on server context
	serverContext.requestAnonymity(true);
	
	establishContext();

	// should be false - client is not anonymous
	assertTrue(!serverContext.getAnonymityState());
    }

    /* checks if anonymity state is set correctly */
    public void testAnonymousServer2() throws Exception {
	clientContext.requestCredDeleg(false);
	clientContext.requestAnonymity(true);
	
	// request anonymity - this should have no baring on server context
	serverContext.requestAnonymity(true);
	
	// without this handshake will fail
	ExtendedGSSContext sr = (ExtendedGSSContext)serverContext;
	sr.setOption(GSSConstants.REQUIRE_CLIENT_AUTH,
		     Boolean.FALSE);
	
	establishContext();
	
	// should be true - client is anonymous
	assertTrue(serverContext.getAnonymityState());
    }
    
    /* checks if server will catch an error where the cred is anonymous */
    public void testAnonymousServer3() throws Exception {
	
	GSSManager manager = getGSSManager();

	GSSName anonName = manager.createName((String)null, null);
	assertTrue(anonName.isAnonymous());

	GSSCredential anonCred = manager.createCredential(anonName,
							  GSSCredential.INDEFINITE_LIFETIME,
							  (Oid)null,
							  GSSCredential.INITIATE_AND_ACCEPT);
	assertTrue(anonCred.getName().isAnonymous());

	// server ctx initalized with anon cred
	serverContext = manager.createContext(anonCred);

	try {
	    establishContext();
	    fail("establishContext() did not throw exception as expected.");
	} catch (GSSException e) {
	    if (e.getMajor() != GSSException.DEFECTIVE_CREDENTIAL) {
		e.printStackTrace();
		fail("Unexpected GSSException");
	    }
	}
    }

    public void testBadUsage1() throws Exception {
	GSSManager manager = getGSSManager();
	
	GSSCredential cred = manager.createCredential(null,
						      GSSCredential.DEFAULT_LIFETIME,
						      (Oid)null,
						      GSSCredential.INITIATE_ONLY);
	
	// creates an accepter context with credential that is 
	// supposed to be used for initiators
	serverContext = manager.createContext(cred);
	
	try {
	    establishContext();
	    fail("establishContext() did not throw exception as expected.");
	} catch (GSSException e) {
	    if (e.getMajor() != GSSException.DEFECTIVE_CREDENTIAL) {
		e.printStackTrace();
		fail("Unexpected GSSException");
	    }
	}
    }

    public void testBadUsage2() throws Exception {
	GSSManager manager = getGSSManager();
	
	GSSCredential cred = manager.createCredential(null,
						      GSSCredential.DEFAULT_LIFETIME,
						      (Oid)null,
						      GSSCredential.ACCEPT_ONLY);
	
	// creates an initiator context with credential that is 
	// supposed to be used for acceptor
	clientContext = manager.createContext(null, 
					      GSSConstants.MECH_OID,
					      cred,
					      GSSContext.DEFAULT_LIFETIME);
	
	try {
	    establishContext();
	    fail("establishContext() did not throw exception as expected.");
	} catch (GSSException e) {
	    if (e.getMajor() != GSSException.DEFECTIVE_CREDENTIAL) {
		e.printStackTrace();
		fail("Unexpected GSSException");
	    }
	}
    }

    // basic request confidentiality tests
    public void testRequestConf1() throws Exception {
	// client requests confidentiality but server doesn't support it
	clientContext.requestCredDeleg(false);
	clientContext.requestConf(true);
	serverContext.requestConf(false);
	establishContext();
	assertTrue(!clientContext.getConfState());
    }

    public void testRequestConf2() throws Exception {
	// client & server request confidentiality
	clientContext.requestCredDeleg(false);
	clientContext.requestConf(true);
	serverContext.requestConf(true);
	establishContext();
	assertTrue(clientContext.getConfState());
    }

    // getMIC()/verifyMIC tests

    public void testMic1() throws Exception {
	clientContext.requestCredDeleg(false);
	clientContext.requestConf(false);
	establishContext();
	
	runMicTests();
    }

    public void testMic2() throws Exception {
	clientContext.requestCredDeleg(false);
	clientContext.requestConf(true);
	establishContext();
	
	runMicTests();
    }

    private void runMicTests() throws Exception {

	assertTrue("client ctx not established.", clientContext.isEstablished());
	assertTrue("server ctx not established.", serverContext.isEstablished());
	
	int [] msgSize = {10, 100, 1000, 10000, 16384, 100000};

	for (int i=0;i<msgSize.length;i++) {

	    ByteArrayOutputStream out = new ByteArrayOutputStream();
	    DataOutputStream dout = new DataOutputStream(out);

	    while ( dout.size() < msgSize[i] ) { 
		dout.writeLong(System.currentTimeMillis());
	    }
	    
	    byte [] msg = out.toByteArray();
	    
	    byte [] wToken = clientContext.getMIC(msg, 0, msg.length, null);
	    
	    serverContext.verifyMIC(wToken, 0, wToken.length,
				    msg, 0, msg.length, null);
	}
    }

    public void testBadMicChangeSeq() throws Exception {
	runBadMicTest(4, GSSException.BAD_MIC);
    }

    public void testBadMicChangeLen() throws Exception {
	runBadMicTest(10, GSSException.DEFECTIVE_TOKEN);
    }

    public void testBadMicChangeDigest() throws Exception {
	runBadMicTest(15, GSSException.BAD_MIC);
    }

    private void runBadMicTest(int off, int expectedError) throws Exception {
	
	clientContext.requestCredDeleg(false);
	clientContext.requestConf(true);
	
	establishContext();
	
	byte [] mic = clientContext.getMIC(MSG, 0, MSG.length, null);
	
	mic[off] = 5;
	
	try {
	    serverContext.verifyMIC(mic, 0, mic.length,
				    MSG, 0, MSG.length, null);
	    fail("verify mic did not fail!");
	} catch (GSSException e) {
	    if (e.getMajor() != expectedError) {
		e.printStackTrace();
		fail("Unexpected GSSException");
	    }
	}
    }
    
    public void testOldToken() throws Exception {
	clientContext.requestCredDeleg(false);
	clientContext.requestConf(true);
	
	establishContext();
	
	byte [] mic1 = clientContext.getMIC(MSG, 0, MSG.length, null);
	
	byte [] mic2 = clientContext.getMIC(MSG, 0, MSG.length, null);
	
	serverContext.verifyMIC(mic1, 0, mic1.length,
				MSG, 0, MSG.length, null);
	
	serverContext.verifyMIC(mic2, 0, mic2.length,
				MSG, 0, MSG.length, null);

	try {
	    serverContext.verifyMIC(mic1, 0, mic1.length,
				    MSG, 0, MSG.length, null);
	    fail("verify mic did not fail!");
	} catch (GSSException e) {
	    if (e.getMajor() != GSSException.OLD_TOKEN) {
		e.printStackTrace();
		fail("Unexpected GSSException");
	    }
	}
    }

    public void testGapToken() throws Exception {
	clientContext.requestCredDeleg(false);
	clientContext.requestConf(true);
	
	establishContext();
	
	byte [] mic = null;
	
	mic = clientContext.getMIC(MSG, 0, MSG.length, null);
	
	mic = clientContext.getMIC(MSG, 0, MSG.length, null);
	
	try {
	    serverContext.verifyMIC(mic, 0, mic.length,
				    MSG, 0, MSG.length, null);
	    fail("verify mic did not fail!");
	} catch (GSSException e) {
	    if (e.getMajor() != GSSException.GAP_TOKEN) {
		e.printStackTrace();
		fail("Unexpected GSSException");
	    }
	}
    }
    
    // basic wrap/unwrap tests

     public void testWrap1() throws Exception {
	clientContext.requestCredDeleg(false);
	clientContext.requestConf(false);
	establishContext();
	
	runWrapTests(false, false, 0);
	runWrapTests(false, true, 0);
	
	// tests GSI_BIG mode
	runWrapTests(false, false, GSSConstants.GSI_BIG);
     }

    public void testWrap2() throws Exception {
	clientContext.requestCredDeleg(false);
	clientContext.requestConf(true);
	establishContext();
	
	runWrapTests(true, false, 0);
	runWrapTests(true, true, 0);
    }

    private void runWrapTests(boolean privacy, boolean reqConf, int qop) throws Exception {

	assertTrue("client ctx not established.", clientContext.isEstablished());
	assertTrue("server ctx not established.", serverContext.isEstablished());
	
	int [] msgSize = {10, 100, 1000, 10000, 16384, 100000};

	for (int i=0;i<msgSize.length;i++) {

	    ByteArrayOutputStream out = new ByteArrayOutputStream();
	    DataOutputStream dout = new DataOutputStream(out);

	    while ( dout.size() < msgSize[i] ) { 
		dout.writeLong(System.currentTimeMillis());
	    }
	    
	    byte [] msg = out.toByteArray();

	    MessageProp wProp = new MessageProp(qop, reqConf);

	    byte [] wToken = clientContext.wrap(msg, 0, msg.length, wProp);

	    assertEquals(privacy, wProp.getPrivacy());
	    assertEquals(qop, wProp.getQOP());

	    MessageProp uwProp = new MessageProp(reqConf);

	    byte [] uwToken = serverContext.unwrap(wToken, 0, wToken.length, uwProp);

	    assertEquals(privacy, uwProp.getPrivacy());
	    assertEquals(qop, uwProp.getQOP());

	    assertEquals(msg.length, uwToken.length);
	    
	    for (int j=0;j<msg.length;j++) {
		assertEquals(msg[j], uwToken[j]);
	    }
	    
	}
    }    

    public void testBadUnwrap1() throws Exception {

	clientContext.requestCredDeleg(false);
	clientContext.requestConf(true);
	
	establishContext();

	byte [] wToken = clientContext.wrap(MSG, 0, MSG.length, null);
	
	wToken[7]++;

	try {
	    byte [] uwToken = serverContext.unwrap(wToken, 0, wToken.length, null);
	    fail("unwrap did not fail");
	} catch (GSSException e) {
	    if (e.getMajor() != GSSException.BAD_MIC) {
		e.printStackTrace();
		fail("Unexpected GSSException");
	    }
	}
    }

    public void testBadUnwrap2() throws Exception {

	clientContext.requestCredDeleg(false);
	clientContext.requestConf(true);
	
	establishContext();

	byte [] wToken = clientContext.wrap(MSG, 0, MSG.length, null);
	
	wToken[4] = 5;

	try {
	    byte [] uwToken = serverContext.unwrap(wToken, 0, wToken.length, null);
	    fail("unwrap did not fail");
	} catch (GSSException e) {
	    if (e.getMajor() != GSSException.DEFECTIVE_TOKEN) {
		e.printStackTrace();
		fail("Unexpected GSSException");
	    }
	}
    }

    public void testBadUnwrap3() throws Exception {
	
	clientContext.requestCredDeleg(false);
	clientContext.requestConf(false);

	establishContext();
	
	byte [] wToken = clientContext.wrap(MSG, 0, MSG.length, null);
	
	byte pByte = wToken[4];
	wToken[7] = 5;

	// because this fails the context is invalidated

	try {
	    byte [] uwToken = 
		serverContext.unwrap(wToken, 0, wToken.length, null);
	    fail("unwrap did not fail as excepted");
	} catch (GSSException e) {
	    if (e.getMajor() != GSSException.BAD_MIC) {
		e.printStackTrace();
		fail("Unexpected GSSException");
	    }
	}

	// even if this is unwrap() is ok the context is invalidated 
	// and this will throw an exception

	wToken[4] = pByte;

	try {
	    byte [] uwToken = 
		serverContext.unwrap(wToken, 0, wToken.length, null);
	    fail("unwrap did not fail as excepted");
	} catch (GSSException e) {
	    // I'm little bit unsure about this condition
	    if (e.getMajor() != GSSException.BAD_MIC) {
		e.printStackTrace();
		fail("Unexpected GSSException");
	    }
	}
	
    }

    public void testStreamInitAcceptContext() throws Exception {

	assertTrue("client ctx already established.", 
		   !clientContext.isEstablished());
	assertTrue("server ctx already established.", 
		   !serverContext.isEstablished());
	
	clientContext.requestCredDeleg(true);
	clientContext.requestConf(false);

	ServerSocket serverSocket = new ServerSocket(0);

	Server serverThread = new Server(serverSocket, serverContext);
	serverThread.start();

	Socket client = new Socket(InetAddress.getLocalHost(),
				   serverSocket.getLocalPort());

	OutputStream out = client.getOutputStream();
	InputStream in = client.getInputStream();

	while (!clientContext.isEstablished()) {
	    clientContext.initSecContext(in, out);
	    out.flush();
	}
	
	// make sure the thread is complete
	serverThread.join();
	
	client.close();
	serverSocket.close();
	
	if (serverThread.getException() != null) {
	    throw serverThread.getException();
	}
	
	assertTrue("client ctx not established.", clientContext.isEstablished());
	assertTrue("server ctx not established.", serverContext.isEstablished());

	// just run some wrap/unwrap tests
	runWrapTests(false, false, 0);
    }

    class Server extends Thread {

	ServerSocket _serverSocket;
	GSSContext _serverContext;
	Exception _exception;

	public Server(ServerSocket s, GSSContext context) {
	    _serverSocket = s;
	    _serverContext = context;
	}
	
	public void run() {
	    Socket server = null;
	    try {
		server = _serverSocket.accept();
		OutputStream out = server.getOutputStream();
		InputStream in = server.getInputStream();

		while (!_serverContext.isEstablished()) {
		    _serverContext.acceptSecContext(in, out);
		    out.flush();
		}
		
	    } catch (Exception e) {
		_exception = e;
	    } finally {
		if (server != null) {
		    try { server.close(); } catch (Exception e) {}
		}
	    }
	}

	public Exception getException() {
	    return _exception;
	}
    }

}
