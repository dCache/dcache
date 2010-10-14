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
package org.globus.myproxy.test;

import org.globus.myproxy.MyProxy;
import org.globus.myproxy.MyProxyException;
import org.globus.myproxy.CredentialInfo;
import org.globus.myproxy.GetParams;
import org.globus.myproxy.ChangePasswordParams;
import org.globus.myproxy.InfoParams;
import org.globus.myproxy.InitParams;
import org.globus.myproxy.StoreParams;
import org.globus.gsi.gssapi.auth.IdentityAuthorization;
import org.globus.gsi.gssapi.GlobusGSSCredentialImpl;
import org.globus.gsi.bc.BouncyCastleOpenSSLKey;
import org.globus.gsi.GlobusCredential;
import org.globus.util.TestUtil;

import junit.framework.TestCase;
import junit.framework.TestSuite;
import junit.framework.Test;

import org.gridforum.jgss.ExtendedGSSManager;

import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSManager;

public class MyProxyTest extends TestCase { 

    private static final String CONFIG = 
	"org/globus/myproxy/test/test.properties";
    
    private static final String username = "testusername";
    private static final String password = "123456";
    private static final int lifetime    = 2 * 3600;

    private MyProxy myProxy;
    private GSSCredential cred;

    private static TestUtil util;

    static {
	try {
	    util = new TestUtil(CONFIG);
	} catch (Exception e) {
	    e.printStackTrace();
	    System.exit(-1);
	}
    }
    
    public MyProxyTest(String name) {
	super(name);
    }
    
    public static void main (String[] args) {
	junit.textui.TestRunner.run (suite());
    }

    public static Test suite() {
	return new TestSuite(MyProxyTest.class);
    }

    public void setUp() throws Exception {
	myProxy = new MyProxy(util.get("myproxy.host"),
			      util.getAsInt("myproxy.port"));
	GSSManager manager = ExtendedGSSManager.getInstance();
	cred = manager.createCredential(GSSCredential.INITIATE_ONLY);

	String subjectDN = util.get("myproxy.subject");
	if (subjectDN != null) {
	    myProxy.setAuthorization(new IdentityAuthorization(subjectDN));
	}

    }

    public void testPutGet1() throws Exception {
	myProxy.put(cred,
		    username,
		    password,
		    lifetime);

	try {
	    myProxy.get(cred,
			username,
			"ascbdef",
			lifetime);
	    fail("Did not throw exception as expected");
	} catch (MyProxyException e) {
	}

    }

    public void testPutGet2() throws Exception {
	myProxy.put(cred,
		    username,
		    password,
		    lifetime);

	GSSCredential mCred = myProxy.get(cred,
					  username,
					  password,
					  lifetime);
	assertTrue(mCred != null);
	GlobusCredential gCred = 
	    ((GlobusGSSCredentialImpl)mCred).getGlobusCredential();
	assertTrue(gCred != null);
	gCred.verify();
    }

    public void testDestroy() throws Exception {
	myProxy.put(cred,
		    username,
		    password,
		    lifetime);

	myProxy.destroy(cred,
			username,
			password);
	
	try {
	    myProxy.get(cred,
			username,
			password,
			lifetime);
	    fail("Did not fail as expected");
	} catch (MyProxyException e) {
	}
    }

    public void testInfo() throws Exception {
	myProxy.put(cred,
		    username,
		    password,
		    lifetime);
	
	CredentialInfo info = myProxy.info(cred,
					   username,
					   password);
	
	assertTrue(info != null);
	assertEquals(cred.getName().toString(),
		     info.getOwner());
	
	long diff  = ((info.getEndTime() - info.getStartTime())/1000) -
	    cred.getRemainingLifetime();
	
	// 360 - 5 min diff in delegation plus 1 min for padding
	assertTrue(diff > 0 && diff < 360);
    }


    public void testInfo2() throws Exception {
	String credName1     = "foo";
	String credDesc1     = "foo credential";
	String credRetriever = "foo retriever";

	String credName2   = "bar";
	String credDesc2   = "bar credential";
	String credRenewer = "bar renewer";
	
	InitParams params1 
	    = new InitParams();

	params1.setUserName(username);
	params1.setPassphrase(password);
	params1.setLifetime(lifetime);
	params1.setCredentialName(credName1);
	params1.setCredentialDescription(credDesc1);
	params1.setRetriever(credRetriever);

	InitParams params2
	    = new InitParams();

	params2.setUserName(username);
	params2.setPassphrase(password);
	params2.setLifetime(lifetime);
	params2.setCredentialName(credName2);
	params2.setCredentialDescription(credDesc2);
	params2.setRenewer(credRenewer);
	
	myProxy.put(cred, params1);

	myProxy.put(cred, params2);
	
	InfoParams infoParams
	    = new InfoParams();
	
	infoParams.setUserName(username);
	infoParams.setPassphrase(password);
	
	CredentialInfo info[] = myProxy.info(cred, infoParams);

	assertTrue(info != null);
	assertTrue(info.length > 0);

	boolean f1 = false;
	boolean f2 = false;

	for (int i=0;i<info.length;i++) {
	    if (credName1.equals(info[i].getName())) {
		f1 = true;
		assertEquals(credDesc1, 
			     info[i].getDescription());
		assertEquals(cred.getName().toString(),
			     info[i].getOwner());
		assertEquals(credRetriever,
			     info[i].getRetrievers());
	    } else if (credName2.equals(info[i].getName())) {
		f2 = true;
		assertEquals(credDesc2, 
			     info[i].getDescription());
		assertEquals(cred.getName().toString(),
			     info[i].getOwner());
		assertEquals(credRenewer,
			     info[i].getRenewers());
	    }
	}
	
	if (!f1) {
	    fail("did not find " + credName1 + " credential");
	}
	if (!f2) {
	    fail("did not find " + credName2 + " credential");
	}
    }

    // get credential anonymously
    public void testPutGet3() throws Exception {
	myProxy.put(cred,
		    username,
		    password,
		    lifetime);

	GSSCredential mCred = myProxy.get(username,
					  password,
					  lifetime);
	assertTrue(mCred != null);
	GlobusCredential gCred = 
	    ((GlobusGSSCredentialImpl)mCred).getGlobusCredential();
	assertTrue(gCred != null);
	gCred.verify();
    }

    public void testChangePassword() throws Exception {
	myProxy.put(cred,
		    username,
		    password,
		    lifetime);

	String newPwd = "newPassword123";

	ChangePasswordParams params 
	    = new ChangePasswordParams();
	params.setUserName(username);
	params.setPassphrase(password);
	params.setNewPassphrase(newPwd);

	myProxy.changePassword(cred,
			       params);
	
	GetParams getParams
	    = new GetParams();
	getParams.setUserName(username);
	getParams.setPassphrase(password);

	try {
	    myProxy.get(cred, getParams);
	    fail("Did not fails as expected");
	} catch (MyProxyException e) {
	    // that should be bad pwd error
	    e.printStackTrace();
	}
	
	// after that it should be successful
	getParams.setPassphrase(newPwd);
	myProxy.get(cred, getParams);
    }

    public void testStore() throws Exception {
        String credName = "foobar";
        String credDesc = "foobar description";

        StoreParams storeRequest = new StoreParams();
        storeRequest.setUserName(username);
        storeRequest.setCredentialName(credName);
        storeRequest.setCredentialDescription(credDesc);

        GlobusCredential globusCred = 
            ((GlobusGSSCredentialImpl)cred).getGlobusCredential();

	myProxy.store(cred,
                      globusCred.getCertificateChain(),
                      new BouncyCastleOpenSSLKey(globusCred.getPrivateKey()),
                      storeRequest);
    }

    public void testCertAuthentications() throws Exception {
        InitParams params = new InitParams();
        
	params.setUserName(username);
	params.setLifetime(lifetime);
	params.setRenewer("*");
        
	myProxy.put(cred, params);

        GetParams getRequest = new GetParams();
        getRequest.setUserName(username);
        getRequest.setLifetime(lifetime);

        try {
            myProxy.get(cred, getRequest);
            fail("did not throw exception");
        } catch (MyProxyException e) {
            // that's ok
        }

        getRequest.setAuthzCreds(cred);

        GSSCredential newCred = myProxy.get(cred, getRequest);
        assertTrue(newCred != null);
    }

}
