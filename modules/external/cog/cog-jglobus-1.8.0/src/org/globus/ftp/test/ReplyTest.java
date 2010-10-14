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

import org.globus.ftp.vanilla.Reply;

import java.io.BufferedReader;
import java.io.StringReader;

import junit.framework.TestCase;
import junit.framework.Test;
import junit.framework.TestSuite;
import junit.framework.AssertionFailedError;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
   test Reply
 **/
public class ReplyTest extends TestCase {

    Log logger = LogFactory.getLog(ReplyTest.class.getName());
    public static void main(String[] argv) {
	junit.textui.TestRunner.run (suite());
    }
    
    public static Test suite() {
	return new TestSuite(ReplyTest.class);
    }

    public ReplyTest(String name) {
	super(name);
    }

    public void testReply() {
	String lineSep = System.getProperty("line.separator");

	testReply("230 User pafcio logged in.\r\n", 
		  230,
		  "User pafcio logged in.",
		  false);
	testReply("200 Command okay.\r\n",
		   200, 
		   "Command okay.",
		  false);

	testReply("123-First line\r\n" +
		  " Second line\r\n" +
		  " 234 A line beginning with numbers\r\n" +
		  "123 The last line\r\n",
		  123, 
		  "First line" + lineSep +
		  " Second line" + lineSep +
		  " 234 A line beginning with numbers" + lineSep +
		  "123 The last line",
		  true);

	//superfluous characters after EOL 
	//this is okay; Reply would normally read from stream
	// so it should not read more than it has to
	testReply("200 Command okay.\r\naaaa", 200, "Command okay.", false);

	parseBadReply("");
	parseBadReply("\r\n");
	parseBadReply("0");
	parseBadReply("1   fds\r\n");
	parseBadReply("  1   fds\r\n");
	parseBadReply("200p   fds\r\n");
	parseBadReply("2000   fds\r\n");
	parseBadReply("345454");
	parseBadReply("345454\r\n");

	//no EOL before last line
	parseBadReply("123-First line\r\n" 
		  + " Second line\r\n"
		  + " 234 A line beginning with numbers" + "123 The last line");

    }
    
    //check if bad reply gets detected
    private void parseBadReply(String s) {
	logger.info("bad construction:" + s);
	boolean thrown = false;
	try {
	    parseReply(s);
	} catch (AssertionFailedError e) {
	    thrown = true;
	}
	if ( ! thrown) 
	    fail("A faulty reply was not detected.");
    }

    // fully test reply; check if input values match parsed values
    private void testReply(String uReplyString, int uCode,  String uMessage, boolean uMultiline) {
	logger.info("testing object: " + uReplyString);
	int uClass = uCode / 100;
	try {
	    Reply r = new Reply(new BufferedReader(new StringReader(uReplyString)));
	    int rCode =  r.getCode();
	    arrowQuote("code", rCode);
	    assertTrue(rCode == uCode);

	    int rClass = r.getCategory();
	    arrowQuote("class", rClass);
	    assertTrue(rClass == uClass);

	    String rMessage = r.getMessage();
	    arrowQuote("message", rMessage);
	    assertTrue(rMessage.equals(uMessage));

	    assertTrue(r.isMultiline() == uMultiline);

	} catch (Exception e) {
	    fail("Exception thrown: " +  e.toString());
	}
       
    }

    //only parse reply and see if an exception gets thrown
    private void parseReply(String uReplyString) {
	logger.debug("parsing: " + uReplyString);
	try {
	    Reply r = new Reply(new BufferedReader(new StringReader(uReplyString)));
	} catch (Exception e) {
	    fail("Exception thrown: " +  e.toString());
	}
       
    }    

    private static void arrowQuote(String desc, String content) {
	//System.out.println(desc + " ->" + content + "<-\n");
    }

    private static void arrowQuote(String desc, int content) {
	//System.out.println(desc + " ->" + content + "<-\n");
    }
}
