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
package org.globus.io.urlcopy.test;

import java.io.File;

import org.globus.io.gass.server.GassServer;
import org.globus.io.urlcopy.UrlCopy;
import org.globus.util.GlobusURL;
import org.globus.util.TestUtil;

import junit.framework.TestCase;
import junit.framework.TestSuite;
import junit.framework.Test;

public class UrlCopyTest extends TestCase { 

    private static final String CONFIG = 
	"org/globus/io/urlcopy/test/test.properties";

    private static TestUtil util;
    
    private GassServer server;

    static {
	try {
	    util = new TestUtil(CONFIG);
	} catch (Exception e) {
	    e.printStackTrace();
	    System.exit(-1);
	}
    }

    public UrlCopyTest(String name) {
	super(name);
    }
    
    public static void main (String[] args) {
	junit.textui.TestRunner.run (suite());
    }

    public static Test suite() {
	return new TestSuite(UrlCopyTest.class);
    }

    public void setUp() throws Exception {
	server = new GassServer();
	server.setOptions( GassServer.STDOUT_ENABLE |
			   GassServer.STDERR_ENABLE |
			   GassServer.READ_ENABLE |
			   GassServer.WRITE_ENABLE );
    }

    public void testGassGet() throws Exception {

	File src = new File(util.get("src.file"));
	assertTrue(src.exists());

	String from = server.getURL() + "/" +
	    util.get("src.file");

	File tmp = File.createTempFile("gassget", null);
	tmp.deleteOnExit();
	
	UrlCopy uc = new UrlCopy();
	uc.setSourceUrl(new GlobusURL(from));
	uc.setDestinationUrl(new GlobusURL(tmp.toURL()));
	uc.copy();
	
	assertEquals(src.length(), tmp.length());
    }

    public void testGassPut() throws Exception {

	File src = new File(util.get("src.file"));
	assertTrue(src.exists());

	File tmp = File.createTempFile("gassput", null);
	tmp.deleteOnExit();

	String to = server.getURL() + "/" + tmp.getAbsolutePath();

	System.out.println(to);

	UrlCopy uc = new UrlCopy();
	uc.setSourceUrl(new GlobusURL(src.toURL()));
	uc.setDestinationUrl(new GlobusURL(to));
	uc.copy();
	
	assertEquals(src.length(), tmp.length());
    }

    public void tearDown() throws Exception {
	if (server != null) {
	    server.shutdown();
	}
    }
}
