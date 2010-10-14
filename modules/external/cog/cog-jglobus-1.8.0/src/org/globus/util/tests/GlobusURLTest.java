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
package org.globus.util.tests;

import org.globus.util.GlobusURL;

import junit.framework.TestCase;
import junit.framework.Test;
import junit.framework.TestSuite;

public class GlobusURLTest extends TestCase {

    public GlobusURLTest(String name) {
	super(name);
    }
    
    public static void main (String[] args) {
	junit.textui.TestRunner.run (suite());
    }

    public static Test suite() {
	return new TestSuite(GlobusURLTest.class);
    }
    
    public void testParse() {
	GlobusURL url = null;

        try {
            url = new GlobusURL("file://host1");
        } catch(Exception e) {
            fail("Parse failed: " + e.getMessage());
        }
        checkUrl(url, "file", "host1", -1, null, null, null);

	try {
	    url = new GlobusURL("http:///file1");
	} catch(Exception e) {
	    fail("Parse failed: " + e.getMessage());
	}
	checkUrl(url, "http", "", 80, "file1", null, null);

	try {
            url = new GlobusURL("http://host1:124");
        } catch(Exception e) {
	    fail("Parse failed: " + e.getMessage());
        }
	checkUrl(url, "http", "host1", 124, null, null, null);

	try {
            url = new GlobusURL("http://host1:124/");
        } catch(Exception e) {
	    fail("Parse failed: " + e.getMessage());
        }
	checkUrl(url, "http", "host1", 124, null, null, null);

	try {
            url = new GlobusURL("http://host1/mis/ptys");
        } catch(Exception e) {
	    fail("Parse failed: " + e.getMessage());
        }
	checkUrl(url, "http", "host1", 80, "mis/ptys", null, null);
	
	try {
            url = new GlobusURL("http://usr@host1");
        } catch(Exception e) {
	    fail("Parse failed: " + e.getMessage());
        }
	checkUrl(url, "http", "host1", 80, null, "usr", null);

	try {
            url = new GlobusURL("http://usr:@host1:124");
        } catch(Exception e) {
	    fail("Parse failed: " + e.getMessage());
        }
	checkUrl(url, "http", "host1", 124, null, "usr", "");

	try {
            url = new GlobusURL("http://usr:pwd@host1:124//mis");
        } catch(Exception e) {
	    fail("Parse failed: " + e.getMessage());
        }
	checkUrl(url, "http", "host1", 124, "/mis", "usr", "pwd");

	try {
            url = new GlobusURL(" gsiftp://localhost/foo");
        } catch(Exception e) {
	    fail("Parse failed: " + e.getMessage());
        }
	checkUrl(url, "gsiftp", "localhost", 2811, "foo", null, null);
    }
    
    private void checkUrl(GlobusURL url, 
			  String protocol, String host, 
			  int port, String urlPath,
			  String user, String pwd) {
	assertEquals("protocol", protocol, url.getProtocol());
	assertEquals("host", host, url.getHost());
	assertEquals("port", port, url.getPort());
	assertEquals("urlpath", urlPath, url.getPath());
	assertEquals("user", user, url.getUser());
	assertEquals("pwd", pwd, url.getPwd());
    }

    public void testParseBad() {
	try {
            new GlobusURL("http:/host1");
	    fail("The url was parsed ok!");
        } catch(Exception e) {
        }
    }

    public void testEquals1() {
	GlobusURL url, url2, url3;

	url = url2 = url3 = null;
	try {
            url = new GlobusURL("http://host1:123/jarek");
	    url2 = new GlobusURL("http://host1:123/jarek");
	    url3 = new GlobusURL("ftp://host1:123/jarek");
        } catch(Exception e) {
        }
	
	assertTrue("c1", url.equals("HTTP://host1:123/jarek"));
	assertTrue("c2", !url.equals("HTTP://host1:123/Jarek"));
	assertTrue("c3", url.equals(url));
	assertTrue("c4", url.equals(url2));
	assertTrue("c5", !url.equals(url3));
    }
    
    public void testIPv6Address() {
	GlobusURL url = null;

	try {
            url = new GlobusURL(
                 "http://[1080:0:0:0:8:800:200C:417A]/index.html"
            );
        } catch(Exception e) {
	    fail("Parse failed: " + e.getMessage());
        }
	checkUrl(url, "http", "[1080:0:0:0:8:800:200C:417A]", 80, 
		 "index.html", null, null);

	try {
            url = new GlobusURL(
		"hdl://[3ffe:2a00:100:7031::1]:123"
            );
        } catch(Exception e) {
	    fail("Parse failed: " + e.getMessage());
        }
	checkUrl(url, "hdl", "[3ffe:2a00:100:7031::1]", 123, 
		 null, null, null);

	try {
            url = new GlobusURL(
		"p1://gawor:123@[3ffe:2a00:100:7031::1]:123/testFile"
            );
        } catch(Exception e) {
	    fail("Parse failed: " + e.getMessage());
        }
	checkUrl(url, "p1", "[3ffe:2a00:100:7031::1]", 123, 
		 "testFile", "gawor", "123");
	
    }
    
}
