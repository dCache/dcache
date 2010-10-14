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

import org.globus.gsi.gssapi.GlobusGSSName;

import org.ietf.jgss.GSSName;
import org.ietf.jgss.GSSException;

import junit.framework.TestCase;
import junit.framework.TestSuite;
import junit.framework.Test;

public class GlobusGSSNameTest extends TestCase {

    public GlobusGSSNameTest(String name) {
	super(name);
    }
    
    public static void main (String[] args) {
	junit.textui.TestRunner.run (suite());
    }

    public static Test suite() {
	return new TestSuite(GlobusGSSNameTest.class);
    }
    
    public void testParse() throws Exception {
	GSSName name = null;

	name = new GlobusGSSName("/C=US");

	name = new GlobusGSSName("/C=US/O=ANL");

	name = new GlobusGSSName("/C=US/O=Globus/O=ANL/OU=MCS/CN=gawor/CN=proxy");

	name = new GlobusGSSName("/C=US/O=Globus/O=ANL/OU=MCS/CN=gawor/CN=host/wiggum.mcs.anl.gov");

	name = new GlobusGSSName("/C=US/O=Globus/O=ANL/OU=MCS/CN=host/wiggum.mcs.anl.gov/CN=gawor");

	name = new GlobusGSSName("/C=US/CN=host/pitcairn.mcs.anl.gov/CN=gawor+OU=ANL");

	name = new GlobusGSSName("/C=US/CN=gawor+OU=ANL/CN=host/wiggum.mcs.anl.gov");
    }

    public void testAnonymous() throws Exception {
	GSSName name;
	
	name = new GlobusGSSName((String)null);
	assertTrue(name.isAnonymous());
	
	name = new GlobusGSSName("/C=US/O=Globus/O=ANL/CN=gawor", null);
	assertTrue(!name.isAnonymous());
    }

    public void testEquals() throws Exception {

	GSSName n1 = new GlobusGSSName();
	GSSName n2 = new GlobusGSSName((String)null);

	GSSName n3 = new GlobusGSSName("/C=US/O=Globus/O=ANL/OU=MCS/CN=gawor/CN=proxy");
	GSSName n4 = new GlobusGSSName("/C=US/O=Globus/O=ANL/OU=MCS/CN=gawor");
	GSSName n5 = new GlobusGSSName("/C=US/O=Globus/O=ANL/OU=MCS/CN=gawor/CN=proxy");

	assertTrue(n1.equals(n1));
	assertTrue(n2.equals(n2));
	assertTrue(n3.equals(n3));
	assertTrue(n4.equals(n4));
	
	assertTrue(n1.equals(n2));
	assertTrue(!n2.equals(n3));
	assertTrue(!n3.equals(n4));
	assertTrue(n5.equals(n3));
	assertTrue(!n5.equals(n4));
    }

    public void testConversion() throws Exception {
	
	GSSName n1 = new GlobusGSSName("ftp@140.221.11.99", GSSName.NT_HOSTBASED_SERVICE);
	
        assertEquals("/CN=ftp/wiggum.mcs.anl.gov", n1.toString());

	GSSName n2 = new GlobusGSSName("/C=US/O=Globus/O=ANL/CN=gawor", null);
	
	assertEquals("/C=US/O=Globus/O=ANL/CN=gawor", n2.toString());
    }

    public void testBadHostbasedService() throws Exception {
	try {
	    GSSName n1 = new GlobusGSSName("host@", GSSName.NT_HOSTBASED_SERVICE);
	    fail("Did not thrown exception.");
	} catch (GSSException e) {
	}

	try {
	    GSSName n2 = new GlobusGSSName("host/wiggum.mcs.anl.gov", GSSName.NT_HOSTBASED_SERVICE);
	    fail("Did not thrown exception.");
	} catch (GSSException e) {
	}

	GSSName n3 = new GlobusGSSName("host@wigGUm.mcs.anl.gov", GSSName.NT_HOSTBASED_SERVICE);

	GSSName n4 = new GlobusGSSName("/C=US/O=Globus/CN=wiggum.mcs-7.anl.gov", null);
	GSSName n5 = new GlobusGSSName("/C=US/O=Globus/CN=wiggum-8", null);

	// test with interface name, host cn entry at the end
	GSSName n6 = new GlobusGSSName("/C=US/O=Globus/CN=wiggum-9.mcs.anl.gov", null);

	// test with interface name, host cn entry in the middle
	GSSName n7 = new GlobusGSSName("/C=US/CN=wiggum-9.mcs.anl.gov/O=Globus", null);

	// test with regular, host cn entry in the middle
	GSSName n8 = new GlobusGSSName("/C=US/CN=wiggum.mcs.anl.gov/O=Globus", null);

	// test with regular name, host cn entry at the end
	GSSName n9 = new GlobusGSSName("/C=US/O=Globus/CN=wiggum.mcs.anl.gov", null);
	

	assertTrue(!n3.equals(n4));
	assertTrue(!n3.equals(n5));

	assertTrue(n3.equals(n6));
	assertTrue(n3.equals(n7));
	assertTrue(n3.equals(n8));
	assertTrue(n3.equals(n9));

    }
    
    public void testHostbasedService2() throws Exception {
	GSSName n1 = new GlobusGSSName("host@cvs.globus.org", GSSName.NT_HOSTBASED_SERVICE);
	GSSName n2 = new GlobusGSSName("/C=US/O=Globus/CN=globuscvs.mcs.anl-external.org", null);

	assertEquals("/CN=host/globuscvs.mcs.anl-external.org", n1.toString());
	assertTrue(n1.equals(n2));

	GSSName m1 = new GlobusGSSName("host@dc.isi.edu", GSSName.NT_HOSTBASED_SERVICE);
	GSSName m2 = new GlobusGSSName("/C=US/O=Globus/CN=dc-user2.isi.edu", null);
    
	assertEquals("/CN=host/dc-user2.isi.edu", m1.toString());
	assertTrue(m1.equals(m2));
    }

    public void testHostbasedService3() throws Exception {
    GSSName n1 = new GlobusGSSName("host@wiggum.mcs.anl.gov", GSSName.NT_HOSTBASED_SERVICE);
    GSSName n2 = new GlobusGSSName("/C=US/O=Globus/CN=host/wiggum.mcs.anl.gov/CN=12345678", null);
    
    assertTrue(n1.equals(n2));
    }

    public void testEquals2() throws Exception {
	
	GSSName n1 = new GlobusGSSName("host@wigGUm.mcs.anl.gov", GSSName.NT_HOSTBASED_SERVICE);
	GSSName n2 = new GlobusGSSName("/C=US/O=Globus/CN=wiggum.mcs.anl.gov", null);
	GSSName n3 = new GlobusGSSName("/C=US/O=Globus/CN=host/wiggum.MCS.anl.gov", null);
	GSSName n4 = new GlobusGSSName("/C=US/O=Globus/CN=ftp/wiggum.mcs.anl.gOv", null);
	GSSName n5 = new GlobusGSSName("ftp@wiggum.mcs.anl.gov", GSSName.NT_HOSTBASED_SERVICE);
	GSSName n6 = new GlobusGSSName("host@140.221.11.99", GSSName.NT_HOSTBASED_SERVICE);
	GSSName n7 = new GlobusGSSName("/C=US/O=Globus/CN=wiggum-9.mcs.anl.gov", null);
	
	assertTrue(n1.equals(n1));
	assertTrue(n2.equals(n2));
	
	assertTrue(n1.equals(n2));
	assertTrue(n2.equals(n1));
	
	assertTrue(n1.equals(n3));
	assertTrue(n3.equals(n1));

	assertTrue(!n4.equals(n1));
	assertTrue(n5.equals(n4));
	assertTrue(n4.equals(n5));

	assertTrue(!n1.equals(n5));

	assertTrue(n1.equals(n6));

	assertTrue(n7.equals(n6));

	assertTrue(!n2.equals(n3));
	assertTrue(!n3.equals(n4));
	assertTrue(!n4.equals(n7));
    }

}
