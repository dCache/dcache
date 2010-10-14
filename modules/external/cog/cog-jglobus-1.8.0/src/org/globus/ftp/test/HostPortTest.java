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
package org.globus.ftp.test;

import org.globus.ftp.HostPort6;

import junit.framework.TestCase;
import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
   Test HostPort
 **/
public class HostPortTest extends TestCase {

    private static Log logger = 
	LogFactory.getLog(HostPortTest.class.getName());

    public static void main(String[] argv) {
	junit.textui.TestRunner.run (suite());
    }
    
    public static Test suite() {
	return new TestSuite(HostPortTest.class);
    }

    public HostPortTest(String name) {
	super(name);
    }

    public void testCreateIPv6() {
        HostPort6 hp = null;

        hp = new HostPort6(HostPort6.IPv6, 
                           "1080::8:800:200C:417A",
                           123);
        assertEquals(HostPort6.IPv6, hp.getVersion());
        assertEquals("1080::8:800:200C:417A", hp.getHost());
        assertEquals(123, hp.getPort());
        assertEquals("|2|1080::8:800:200C:417A|123|", hp.toFtpCmdArgument());

        hp = new HostPort6(HostPort6.IPv4, 
                           "192.168.1.1",
                           456);
        assertEquals(HostPort6.IPv4, hp.getVersion());
        assertEquals("192.168.1.1", hp.getHost());
        assertEquals(456, hp.getPort());
        assertEquals("|1|192.168.1.1|456|", hp.toFtpCmdArgument());
    }

    public void testParseIPv6() {
        parseIPv6("|||6446|",
                  null, null, 6446);
        parseIPv6("|1|132.235.1.2|6275|",
                  "1", "132.235.1.2", 6275);
        parseIPv6("|2|1080::8:800:200C:417A|5282|",
                  "2", "1080::8:800:200C:417A", 5282);
    }
    
    private void parseIPv6(String reply,
                           String version,
                           String host,
                           int port) {
        HostPort6 p = new HostPort6(reply);
        
        assertEquals(version, p.getVersion());
        assertEquals(host, p.getHost());
        assertEquals(port, p.getPort());

        String text = p.toFtpCmdArgument();
        assertEquals(reply, text);
    }

}

