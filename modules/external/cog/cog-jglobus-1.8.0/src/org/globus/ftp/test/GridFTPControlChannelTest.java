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

import org.globus.ftp.HostPortList;
import org.globus.ftp.vanilla.Reply; 
import org.globus.ftp.vanilla.Command; 
import org.globus.ftp.vanilla.FTPControlChannel; 
import org.globus.ftp.extended.GridFTPControlChannel;

import junit.framework.TestCase;
import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.ietf.jgss.GSSCredential;

/**
   Test GridFTPControlChannel
 **/
public class GridFTPControlChannelTest extends TestCase{

    private static Log logger = 
	LogFactory.getLog(GridFTPControlChannelTest.class.getName());
    
    public GridFTPControlChannelTest(String name) {
	super(name);
    }

    public static void main (String[] args) throws Exception{
	junit.textui.TestRunner.run (suite());
    }

    public static Test suite ( ) {
	return new TestSuite(GridFTPControlChannelTest.class);
    }
    
    public void test3PartyParallel() throws Exception{
	logger.info("3rd party parallel (using OPTS RETR Parallelism)");
	try {
	    test3PartyParallel(
		       TestEnv.serverAHost,
		       TestEnv.serverAPort,
		       TestEnv.serverASubject,
		       TestEnv.serverADir + "/" + TestEnv.serverAFile,
		       
		       TestEnv.serverBHost,
		       TestEnv.serverBPort,
		       TestEnv.serverBSubject,
		       TestEnv.serverBDir + "/" + TestEnv.serverBFile,

		       null, /* use default cred */
		       5					  
		       );
	} catch (Exception e) {
	    logger.error("", e);
	    fail(e.toString());
	} 
    }
    
    /**
       Test authenticate()
     **/
    public void testAuth() throws Exception{
	logger.info("authenticate()");
	try {
 	    testAuth(TestEnv.serverAHost,
		     TestEnv.serverAPort,
		     TestEnv.serverASubject,
		     null); /* use default cred */
	} catch (Exception e) {
	    logger.error("", e);
	    fail(e.toString());
	}
    }

    /**
       Test third party transfer
     **/
    public void test3Party() throws Exception{
	logger.info("3rd party");
	try {
	    test3Party(
		       TestEnv.serverAHost,
		       TestEnv.serverAPort,
		       TestEnv.serverASubject,
		       TestEnv.serverADir + "/" + TestEnv.serverAFile,
		       
		       TestEnv.serverBHost,
		       TestEnv.serverBPort,
		       TestEnv.serverBSubject,
		       TestEnv.serverBDir + "/" + TestEnv.serverBFile,

		       null /* use default cred */
		       );

	} catch (Exception e) {
	    logger.error("", e);
	    fail(e.toString());
	}
    }

    /** 
	Test striped third party transfer
     **/
    public void test3PartyStriping() throws Exception{
	logger.info("3rd party striping (using SPAS/SPOR)");
	try {
	    test3PartyStriping(
		       TestEnv.serverAHost,
		       TestEnv.serverAPort,
		       TestEnv.serverASubject,
		       TestEnv.serverADir + "/" + TestEnv.serverAFile,
		       
		       TestEnv.serverBHost,
		       TestEnv.serverBPort,
		       TestEnv.serverBSubject,
		       TestEnv.serverBDir + "/" + TestEnv.serverBFile,

		       null, /* use default cred */
		       5					  
		       );
	} catch (Exception e) {
	    logger.error("", e);
	    fail(e.toString());
	}
    }
 
    private void testAuth(String host, 
			  int port,
			  String subject,
			  GSSCredential cred)
	throws Exception {

	GridFTPControlChannel pi = new GridFTPControlChannel(host, port);
	pi.open();
	pi.setAuthorization(TestEnv.getAuthorization(subject));
	pi.authenticate(cred);
	
    } //testAuth

    private void checkPositive(Reply r) {
	logger.debug("tester: received: " + r.toString());
	if (400<= r.getCode()) {
	    fail("received non positive reply: " + r.toString());
	}
    }

    private void test3Party(String host1, 
			    int port1,
			    String subject1,
			    String sourceFile,
			    String host2,
			    int port2,
			    String subject2,
			    String destFile,
			    GSSCredential cred)
	throws Exception {
	
	//
	// pi2 = control channel to destination server
	//
	
	GridFTPControlChannel pi2 = new GridFTPControlChannel(host2, port2);
	pi2.open();
	pi2.setAuthorization(TestEnv.getAuthorization(subject2));
	logger.debug("Connected to server 2.");
	pi2.authenticate(cred);
	    
	pi2.write(new Command("TYPE", "I"));
	checkPositive(pi2.read());
	    
	pi2.write(new Command("PBSZ", "16384"));
	checkPositive(pi2.read());
	    
	pi2.write(new Command("PASV"));
	Reply pasvReply = pi2.read();
	checkPositive(pasvReply);
	
	//parse PASV reply of the form:
	//227 Entering Passive Mode (140,221,65,198,172,18)
	if (pasvReply.getCode() != 227)
	    fail("received unexpected server reply to Pasv: " + pasvReply.getCode());
	String pasvReplyMsg=pasvReply.getMessage();
	logger.debug("tester: The message is: " + pasvReplyMsg);
	int openBracket = pasvReplyMsg.indexOf("(");
	int closeBracket = pasvReplyMsg.indexOf(")", openBracket);
	String portCommandParam = pasvReplyMsg.substring(openBracket+1, 
							 closeBracket);
	
		      
	    
	pi2.write(new Command("STOR", destFile));
	//do not wait for reply
	
	//
	// pi1 = control channel to source server
	//
	
	GridFTPControlChannel pi1 = new GridFTPControlChannel(host1, port1);
	pi1.open();
	pi1.setAuthorization(TestEnv.getAuthorization(subject1));
	logger.debug("Connected to server 1.");
	pi1.authenticate(cred);
      
	pi1.write(new Command("TYPE", "I"));
	checkPositive(pi1.read());
	
	pi1.write(new Command("SIZE", sourceFile));
	checkPositive(pi1.read());
	
	pi1.write(new Command("PBSZ", "16384"));
	checkPositive(pi1.read());
	
	//PORT
	Command port = new Command("PORT", portCommandParam);
	pi1.write(port);
	checkPositive(pi1.read());
	
	pi1.write(new Command("RETR", sourceFile));
	
	// 150 Opening BINARY mode data connection.
	checkPositive(pi1.read());
	checkPositive(pi2.read());
	
	//226 Transfer complete
	checkPositive(pi1.read());
	checkPositive(pi2.read());
	
	pi1.write(new Command("QUIT"));
	pi2.write(new Command("QUIT"));
	
	//221 Service closing control connection.
	checkPositive(pi1.read());
	checkPositive(pi2.read());
	
	pi1.close();
	pi2.close();
    }

    private void test3PartyParallel(String host1, 
				    int port1,
				    String subject1,
				    String sourceFile,
				    String host2,
				    int port2,
				    String subject2,
				    String destFile,
				    GSSCredential cred,
				    int parallelism)
	throws Exception {
	

	    //
	    // pi2 = control channel to destination server
	    //

	    GridFTPControlChannel pi2 = new GridFTPControlChannel(host2, port2);
	    pi2.open();
	    pi2.setAuthorization(TestEnv.getAuthorization(subject2));
	    logger.debug("Connected to server 2.");
	    pi2.authenticate(cred);

	    //FEAT
	    doesServerSupportParallel(pi2);	    

	    pi2.write(new Command("TYPE", "I"));
	    checkPositive(pi2.read());
	    
	    pi2.write(new Command("MODE", "E"));
	    checkPositive(pi2.read());
    
	    pi2.write(new Command("PBSZ", "16384"));
	    checkPositive(pi2.read());
	    
	    pi2.write(new Command("PASV"));
	    Reply pasvReply = pi2.read();
	    checkPositive(pasvReply);

	    //parse PASV reply of the form:
	    //227 Entering Passive Mode (140,221,65,198,172,18)
	    if (pasvReply.getCode() != 227)
		fail("received unexpected server reply to Pasv: " + pasvReply.getCode());
	    String pasvReplyMsg=pasvReply.getMessage();
	    logger.debug("tester: The message is: " + pasvReplyMsg);
	    int openBracket = pasvReplyMsg.indexOf("(");
	    int closeBracket = pasvReplyMsg.indexOf(")", openBracket);
	    String portCommandParam = pasvReplyMsg.substring(openBracket+1, 
							 closeBracket);
    
		      
	    
	    pi2.write(new Command("STOR", destFile));
	    //do not wait for reply

	    //
	    // pi1 = control channel to source server
	    //
	    
	    GridFTPControlChannel pi1 = new GridFTPControlChannel(host1, port1);
	    pi1.open();
	    pi1.setAuthorization(TestEnv.getAuthorization(subject1));
	    logger.debug("Connected to server 1.");
	    pi1.authenticate(cred);

	    //FEAT
	    doesServerSupportParallel(pi1);	    
      
	    pi1.write(new Command("TYPE", "I"));
	    checkPositive(pi1.read());

	    pi1.write(new Command("MODE", "E"));
	    checkPositive(pi1.read());
    
	    pi1.write(new Command("SIZE", sourceFile));
	    checkPositive(pi1.read());

	    pi1.write(new Command("OPTS", "RETR Parallelism=" +
				  parallelism + "," + 
				  parallelism + "," + 
				  parallelism +";")); 


	    pi1.write(new Command("PBSZ", "16384"));
	    checkPositive(pi1.read());
	    
	    //PORT
	    Command port = new Command("PORT", portCommandParam);
	    pi1.write(port);
	    checkPositive(pi1.read());
	        
	    pi1.write(new Command("RETR", sourceFile));

	    
	    for(;;) {
		Reply reply1 = pi1.read();
		//200 PORT command successful.
		if (reply1.getCode() == 200) {
		    continue;
		}
		// 150 Opening BINARY mode data connection.
		if (reply1.getCode() == 150) {
		    continue;
		}
		//perf marker
		if (reply1.getCode() == 112) {
		    continue;
		}
                //restart marker
                if (reply1.getCode() == 111) {
                    continue;
                }
		//226 Transfer complete
		if (reply1.getCode() == 226) {
		    break;
		}
		fail("received unexpected reply from server 1: " + 
		     reply1.toString());
	    }

	    for(;;) {
		Reply reply1 = pi2.read();
		//200 PORT command successful.
		if (reply1.getCode() == 200) {
		    continue;
		}
		// 150 Opening BINARY mode data connection.
		if (reply1.getCode() == 150) {
		    continue;
		}
		//perf marker
		if (reply1.getCode() == 112) {
		    continue;
		}
                //restart marker
                if (reply1.getCode() == 111) {
                    continue;
                }
		//226 Transfer complete
		if (reply1.getCode() == 226) {
		    break;
		}
		fail("received unexpected reply from server 2: " + 
		     reply1.toString());
	    }
	    
	    pi1.write(new Command("QUIT"));
	    pi2.write(new Command("QUIT"));

	    //221 Service closing control connection.
	    checkPositive(pi1.read());
	    checkPositive(pi2.read());

	    pi1.close();
	    pi2.close();
    }//test3PartyParallel


    //using ESTO and ERET
    private void test3PartyStriping(String host1, 
				    int port1,
				    String subject1,
				    String sourceFile,
				    String host2,
				    int port2,
				    String subject2,
				    String destFile,
				    GSSCredential cred,
				    int parallelism)
	throws Exception {
	
	//
	// pi2 = control channel to destination server
	//
	
	GridFTPControlChannel pi2 = new GridFTPControlChannel(host2, port2);
	pi2.open();
	pi2.setAuthorization(TestEnv.getAuthorization(subject2));
	logger.debug("Connected to server 2.");
	pi2.authenticate(cred);

	//FEAT
	doesServerSupportParallel(pi2);	    
	
	pi2.write(new Command("TYPE", "I"));
	checkPositive(pi2.read());
	
	pi2.write(new Command("MODE", "E"));
	checkPositive(pi2.read());
	
	pi2.write(new Command("PBSZ", "16384"));
	checkPositive(pi2.read());
	
	pi2.write(new Command("SPAS"));
	Reply spasReply = pi2.read();
	checkPositive(spasReply);
	logger.debug("tester: Received reply to SPAS.");
	
	if (spasReply.getCode() != 229)
	    fail("received unexpected server reply to Spas: " + spasReply.getCode());
	String spasReplyMsg=spasReply.getMessage();
	logger.debug("tester: The message is: " + spasReplyMsg);
	
	String sporCommandParam = 
	    new HostPortList(spasReply.getMessage()).toFtpCmdArgument();
	
	pi2.write(new Command("ESTO", "A 0 " + destFile));
	//do not wait for reply
	
	//
	// pi1 = control channel to source server
	//
	
	GridFTPControlChannel pi1 = new GridFTPControlChannel(host1, port1);
	pi1.open();
	pi1.setAuthorization(TestEnv.getAuthorization(subject1));
	logger.debug("Connected to server 1.");
	pi1.authenticate(cred);

	//FEAT
	doesServerSupportParallel(pi1);	    
	
	pi1.write(new Command("TYPE", "I"));
	checkPositive(pi1.read());
	
	pi1.write(new Command("MODE", "E"));
	checkPositive(pi1.read());
	
	pi1.write(new Command("SIZE", sourceFile));
	Reply sizeReply = pi1.read();
	checkPositive(sizeReply);
	long sourceFileSize = Long.parseLong(sizeReply.getMessage());
	    
	pi1.write(new Command("OPTS", "RETR Parallelism=" +
			      parallelism + "," + 
			      parallelism + "," + 
			      parallelism +";")); 
	
	
	pi1.write(new Command("PBSZ", "16384"));
	checkPositive(pi1.read());
	
	//PORT
	Command port = new Command("SPOR", sporCommandParam);
	pi1.write(port);
	checkPositive(pi1.read());
	
	pi1.write(new Command("ERET", "P 0 " + sourceFileSize 
			      + " " + sourceFile));
	
	for(;;) {
	    Reply reply1 = pi1.read();
	    //200 PORT command successful.
	    if (reply1.getCode() == 200) {
		continue;
	    }
	    // 150 Opening BINARY mode data connection.
	    if (reply1.getCode() == 150) {
		continue;
	    }
	    //perf marker
	    if (reply1.getCode() == 112) {
		continue;
	    }
	    //restart marker
	    if (reply1.getCode() == 111) {
		continue;
	    }
	    //226 Transfer complete
	    if (reply1.getCode() == 226) {
		break;
	    }
	    fail("received unexpected reply from server 1: " + 
		 reply1.toString());
	}
	
	for(;;) {
	    Reply reply2 = pi2.read();
	    //200 PORT command successful.
	    if (reply2.getCode() == 200) {
		continue;
	    }
	    // 150 Opening BINARY mode data connection.
	    if (reply2.getCode() == 150) {
		continue;
	    }
	    //perf marker
	    if (reply2.getCode() == 112) {
		continue;
	    }
	    //restart marker
	    if (reply2.getCode() == 111) {
		continue;
	    }
	    //226 Transfer complete
	    if (reply2.getCode() == 226) {
		break;
	    }
	    fail("received unexpected reply from server 2: " + 
		 reply2.toString());
	}
	
	pi1.write(new Command("QUIT"));
	pi2.write(new Command("QUIT"));
	
	//221 Service closing control connection.
	checkPositive(pi1.read());
	checkPositive(pi2.read());
	
	pi1.close();
	pi2.close();
    }//test3rdPartyStriping

    //ensure that the server supports PARALLEL, or fail
    private void doesServerSupportParallel(FTPControlChannel pi2) 
	throws Exception{
	    pi2.write(new Command("FEAT"));
	    Reply featReply = pi2.read();
	    checkPositive(featReply);
	    //logger.debug("tester: feat response received");
	    String featMsg = featReply.getMessage();
	    //logger.debug("tester: " + featMsg);
	    int line = 0;
	    int thisLineStarts = 0;
	    int thisLineEnds = 0;
	    for(;;line++) {
		thisLineEnds =featMsg.indexOf('\n', thisLineStarts);
		if (thisLineEnds == -1)
		    //PARALLEL extension not found
		    fail("Server does not support PARALLEL");
		String thisLine=featMsg.substring(thisLineStarts, thisLineEnds);
		//logger.debug("feat line -> " + thisLine + "<-");
		if (thisLine.indexOf("PARALLEL") != -1) {
		    //PARALLEL found 
		    logger.debug("Server does support parallel (feat reply line " 
				+ line + " )");
		    break;
		}
		thisLineStarts = thisLineEnds+1;
	    }
	    
	    
    }


} 
