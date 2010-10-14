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

import org.globus.ftp.HostPort;
import org.globus.ftp.HostPort6;
import org.globus.ftp.HostPortList;

import junit.framework.TestCase;
import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
   Test HostPortList
 **/
public class HostPortListTest extends TestCase {

    private static Log logger = 
	LogFactory.getLog(HostPortListTest.class.getName());

    private static String nl = System.getProperty("line.separator");
    private static String space = " ";

    static final String param1 = "140,221,65,198,173,202";
    static final String msg1 ="Entering Striped Passive Mode" + nl +
	space + param1 + nl +
	"229 End";

    static final String hp1str = "140,221,65,198,173,202";
    static final String hp2str = "140,221,65,198,1,50";
    static final String param2 = hp1str + space + hp2str;
    static final String msg2 ="Entering Striped Passive Mode" + nl +
	space + hp1str + nl +
	space + hp2str + nl +
	"229 End";
    static final String msg2_text = hp1str + space + hp2str;

    static final String hp1str_6 = "|1|140.221.65.198|6789|";
    static final String hp2str_6 = "|2|1080::8:800:200C:417A|50|";
    static final String param2_6 = hp1str + space + hp2str;
    static final String msg2_6 ="Entering Striped Passive Mode" + nl +
	space + hp1str_6 + nl +
	space + hp2str_6 + nl +
	"229 End";
    static final String msg2_text_6 = hp1str_6 + space + hp2str_6;

    static final String BAD_REPLY_1 = "MODE E ok.";

    static final String BAD_REPLY_2 = "Extensions supported:" + nl +
	space + "REST STREAM" + nl + 
	space + "ESTO" + nl + 
	space + "ERET" + nl + 
	space + "MDTM" + nl + 
	space + "SIZE" + nl + 
	space + "PARALLEL" + nl + 
	space + "DCAU" + nl + 
	"211 END";
    
    static final String hp5str = "127,0,0,1,100,0";
    static final String param5 = hp1str + space + hp2str + space + hp5str;
    static final String msg5 ="Entering Striped Passive Mode" + nl +
	space + hp1str + nl +
	space + hp2str + nl +
	space + hp5str + nl +
	"229 End";


    public static void main(String[] argv) {
	junit.textui.TestRunner.run (suite());
    }
    
    public static Test suite() {
	return new TestSuite(HostPortListTest.class);
    }

    public HostPortListTest(String name) {
	super(name);
    }

    public void testString() {
	logger.info("testing construction from String");
	testString(msg1, param1);
	testString(msg2, param2);
	testConstructorError(BAD_REPLY_1);
	testConstructorError(BAD_REPLY_2);
    }

    public void testHP() {
	logger.info("testing construction from HostPort object");
	HostPort hp1 = new HostPort(param1);
	HostPortList hpl1 = new HostPortList();
	hpl1.add(hp1);
	//double checking - make sure this does not change internal state
	testObject(hpl1, param1);
	testObject(hpl1, param1);

	HostPortList hpl2 = new HostPortList();
        hpl2.add(new HostPort(hp1str));
	hpl2.add(new HostPort(hp2str));
	testObject(hpl2, param2);
	testObject(hpl2, param2);
    }

    public void testMixed() {
	logger.info("testing construction from String, and later modification by add(HostPort)");

	HostPortList hpl2 = new HostPortList(msg1);
	hpl2.add(new HostPort(hp2str));
	testObject(hpl2, param2);
	testObject(hpl2, param2);
	hpl2.add(new HostPort(hp5str));
	testObject(hpl2, param5);
	testObject(hpl2, param5);
    }

    /**
       make sure that message "msg" is properly converted 
       to the SPOR command argument "arg"
    **/
    private void testString(String msg, String ftpCmdArg) {
	String result = new HostPortList(msg).toFtpCmdArgument();
	assertEquals(ftpCmdArg, result);
    }

    private void testObject(HostPortList hpl, String ftpCmdArg) {
	String msg = hpl.toFtpCmdArgument(); 
	assertEquals(ftpCmdArg, msg);
    }

    /** 
	assume this is a bad argument to HostPortList constructor.
	make sure the constructor throws an exception.
    **/
    private void testConstructorError(String msg) {
	logger.info("checking bad message: " + msg);
	boolean threwOk = false;
	try {
	    new HostPortList(msg);
	} catch (IllegalArgumentException e) {
	    threwOk = true;
	} 
	
	if (! threwOk ) {
	    fail("HostPortList constructor did not throw an exception when it should have");
	}
	logger.debug("okay, throws exception as expected.");
    }


    public void testParseIPv4() {
        HostPortList list = HostPortList.parseIPv4Format(msg2);

        assertEquals(2, list.size());
        
        HostPort p1 = new HostPort(hp1str);
        HostPort p2 = new HostPort(hp2str);
        
        assertEquals(p1.getHost(), list.get(0).getHost());
        assertEquals(p1.getPort(), list.get(0).getPort());

        assertEquals(p2.getHost(), list.get(1).getHost());
        assertEquals(p2.getPort(), list.get(1).getPort());

        assertEquals(msg2_text, list.toFtpCmdArgument());
    }

    public void testParseIPv6() {
        HostPortList list = HostPortList.parseIPv6Format(msg2_6);

        assertEquals(2, list.size());
        
        HostPort6 p1 = new HostPort6(hp1str_6);
        HostPort6 p2 = new HostPort6(hp2str_6);
        
        assertEquals(p1.getHost(), list.get(0).getHost());
        assertEquals(p1.getPort(), list.get(0).getPort());
        assertEquals(p1.getVersion(), ((HostPort6)list.get(0)).getVersion());

        assertEquals(p2.getHost(), list.get(1).getHost());
        assertEquals(p2.getPort(), list.get(1).getPort());
        assertEquals(p2.getVersion(), ((HostPort6)list.get(1)).getVersion());

        assertEquals(msg2_text_6, list.toFtpCmdArgument());
    }
}
