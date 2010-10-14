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
package org.globus.common.tests;

import org.globus.common.ResourceManagerContact;

import junit.framework.TestCase;
import junit.framework.Test;
import junit.framework.TestSuite;

public class ResourceManagerContactTest extends TestCase {

    public  ResourceManagerContactTest(String name) {
	super(name);
    }

    public static void main (String[] args) {
	junit.textui.TestRunner.run (suite());
    }

    public static Test suite() {
	return new TestSuite(ResourceManagerContactTest.class);
    }

    public void testBasic() throws Exception {
	ResourceManagerContact c = null;
	
	c = new ResourceManagerContact("pitcairn.mcs.anl.gov");
	verify(c, "pitcairn.mcs.anl.gov", 2119);

	c = new ResourceManagerContact("pitcairn.mcs.anl.gov:123");
	verify(c, "pitcairn.mcs.anl.gov", 123);

	c = new ResourceManagerContact("pitcairn.mcs.anl.gov:123/job");
	verify(c, "pitcairn.mcs.anl.gov", 123, "/job");

	c = new ResourceManagerContact("pitcairn.mcs.anl.gov/job");
	verify(c, "pitcairn.mcs.anl.gov", 2119, "/job");

	c = new ResourceManagerContact("pitcairn.mcs.anl.gov:/job");
	verify(c, "pitcairn.mcs.anl.gov", 2119, "/job");

	c = new ResourceManagerContact("pitcairn.mcs.anl.gov::cn=jarek");
	verify(c, "pitcairn.mcs.anl.gov", 2119, "/jobmanager", "cn=jarek");

	c = new ResourceManagerContact("pitcairn.mcs.anl.gov:123:cn=jarek");
	verify(c, "pitcairn.mcs.anl.gov", 123, "/jobmanager", "cn=jarek");
	
	c = new ResourceManagerContact("pitcairn.mcs.anl.gov:/job:cn=jarek");
	verify(c, "pitcairn.mcs.anl.gov", 2119, "/job", "cn=jarek");
	
	c = new ResourceManagerContact("pitcairn.mcs.anl.gov/job:cn=jarek");
	verify(c, "pitcairn.mcs.anl.gov", 2119, "/job", "cn=jarek");
	
	c = new ResourceManagerContact("pitcairn.mcs.anl.gov:123/job:cn=jarek");
	verify(c, "pitcairn.mcs.anl.gov", 123, "/job", "cn=jarek");
    }


    public void testBasicIPv6() throws Exception {
	ResourceManagerContact c = null;
	c = new ResourceManagerContact("[3ffe:2a00:100:7031::1]");
	verify(c, "[3ffe:2a00:100:7031::1]", 2119);

	c = new ResourceManagerContact("[3ffe:2a00:100:7031::1]:123");
	verify(c, "[3ffe:2a00:100:7031::1]", 123);
	
	c = new ResourceManagerContact("[3ffe:2a00:100:7031::1]/job");
	verify(c, "[3ffe:2a00:100:7031::1]", 2119, "/job");
     }

    private void verify(ResourceManagerContact contact,
			String hostname,
			int port) {
	verify(contact, hostname, port, "/jobmanager", null);
    }
    
    private void verify(ResourceManagerContact contact,
			String hostname,
			int port,
			String serviceName) {
	verify(contact, hostname, port, serviceName, null);
    }

    private void verify(ResourceManagerContact contact,
			String hostname,
			int port,
			String serviceName,
			String dn) {
	assertEquals("hostname", hostname, contact.getHostName());
	assertEquals("port", port, contact.getPortNumber());
	assertEquals("service", serviceName, contact.getServiceName());
	assertEquals("dn", dn, contact.getGlobusDN());
    }

}
