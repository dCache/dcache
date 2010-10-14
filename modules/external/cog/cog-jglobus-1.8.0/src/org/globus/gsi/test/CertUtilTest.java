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
package org.globus.gsi.test;

import org.globus.gsi.CertUtil;
import org.globus.gsi.OldCertUtil;
import org.globus.gsi.GSIConstants;

import junit.framework.TestCase;
import junit.framework.TestSuite;
import junit.framework.Test;

public class CertUtilTest extends TestCase {
    
    static final String DN_1 = "CN=gawor, OU=MCS,O=ANL, O=Globus,  C=US";
    static final String DN_2 = "CN=proxy, CN=gawor, OU=MCS,O=ANL, O=Globus,  C=US";
    static final String DN_3 = "CN=limited proxy, CN=proxy, CN=gawor, OU=MCS,O=ANL, O=Globus,  C=US";
    static final String DN_4 = "CN=limited proxyy, CN=proxy, CN=gawor, OU=MCS,O=ANL, O=Globus,  C=US";
    
    static final String RDN_1 = "C=US,O=Globus,O=ANL,OU=MCS,CN=gawor";
    static final String RDN_2 = "C=US,O=Globus,O=ANL,OU=MCS,CN=gawor,CN=proxy,CN=proxy";
    static final String RDN_3 = "C=US,O=Globus,O=ANL,OU=MCS,CN=gawor,CN=proxy,CN=limited proxy";
    static final String RDN_4 = "C=US,O=Globus,O=ANL,OU=MCS,CN=gawor,CN=proxy,CN=limited proxyy";

    public CertUtilTest(String name) {
	super(name);
    }
    
    public static void main (String[] args) {
	junit.textui.TestRunner.run (suite());
    }

    public static Test suite() {
	return new TestSuite(CertUtilTest.class);
    }
    
    public void testGetBase1() throws Exception {
	
	assertEquals("CN=gawor", 
		     OldCertUtil.getBase("CN=gawor", true));
	
	assertEquals(RDN_1,
		     OldCertUtil.getBase(RDN_1, true));
	
	assertEquals(RDN_1,
		     OldCertUtil.getBase(RDN_2, true));
	
	assertEquals(RDN_1,
		     OldCertUtil.getBase(RDN_3, true));
	
    }

    public void testGetBase2() throws Exception {
	
	assertEquals("CN=gawor", 
		     OldCertUtil.getBase("CN=gawor", false));

	assertEquals(DN_1,
		     OldCertUtil.getBase(DN_1, false));
	
	assertEquals(DN_1,
		     OldCertUtil.getBase(DN_2, false));
	
	assertEquals(DN_1,
		     OldCertUtil.getBase(DN_3, false));
	
    }

    public void testGetProxyType1() throws Exception {

	assertEquals(-1, 
		     OldCertUtil.getProxyType(RDN_1, true));

	assertEquals(GSIConstants.GSI_2_PROXY,
		     OldCertUtil.getProxyType(RDN_2, true));

	assertEquals(GSIConstants.GSI_2_LIMITED_PROXY,
		     OldCertUtil.getProxyType(RDN_3, true));

	assertEquals(-1, 
		     OldCertUtil.getProxyType(RDN_4, true));
	
    }

    public void testGetProxyType2() throws Exception {
	
	assertEquals(-1, 
		     OldCertUtil.getProxyType(DN_1, false));
	
	assertEquals(GSIConstants.GSI_2_PROXY,
		     OldCertUtil.getProxyType(DN_2, false));
	
	assertEquals(GSIConstants.GSI_2_LIMITED_PROXY,
		     OldCertUtil.getProxyType(DN_3, false));

	assertEquals(-1, 
		     OldCertUtil.getProxyType(DN_4, false));
	
    }
    
}
