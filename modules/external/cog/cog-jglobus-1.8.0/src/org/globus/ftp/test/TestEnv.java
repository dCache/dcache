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

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Properties;
import java.io.IOException;
import java.io.InputStream;

import org.globus.gsi.gssapi.auth.Authorization;
import org.globus.gsi.gssapi.auth.HostAuthorization;
import org.globus.gsi.gssapi.auth.IdentityAuthorization;

/**
   Utility for tests. This class has two functions. First, it 
   holds in public variables the test properties. Second, it
   supplies simplified syntax for setting the values for 
   class loggers.
 **/
public class TestEnv {
    /**
       This logger can be used anywhere in the application.
     **/
    public static Log logger = 
	LogFactory.getLog(TestEnv.class.getName());

    public static final int UNDEFINED = -1;
    
    // local destination directory
    public static String localDestDir;
    // local source file (full name)
    public static String localSrcDir;
    public static String localSrcFile;
    public static int localServerPort;

    //GridFTP server (source)
    public static String serverAHost;
    public static int serverAPort;
    public static String serverASubject;
    public static String serverADir;
    public static String serverAFile;
    public static int serverAFileSize;
    public static String serverAFileChecksum;
    public static String serverALargeFile;
    public static String serverASmallFile;
    public static String serverANoSuchFile;
    public static int serverANoSuchPort;

    //GridFTP server (dest)
    public static String serverBHost;
    public static int serverBPort;
    public static String serverBSubject;
    public static String serverBDir;
    public static String serverBFile;
    public static String serverBNoSuchDir;

    //non existent server
    public static String noSuchServer;

    //FTP server (src)
    public static String serverFHost;
    public static int serverFPort;
    public static String serverFDir;
    public static String serverFFile;
    public static int serverFFileSize;
    public static String serverFUser;
    public static String serverFPassword;
    public static String serverFNoSuchFile;
    public static int serverFNoSuchPort;

    //FTP server (dest) (optional)
    // if not supplied in test properties, these variables will be null
    public static String serverGHost = null;
    public static int serverGPort = UNDEFINED;
    public static String serverGDir = null;
    public static String serverGFile = null;
    public static String serverGUser = null;
    public static String serverGPassword = null;
    public static String serverGNoSuchDir;

    public static int parallelism;

    //local file separator
    public static String fileSeparator;
    //new line
    public static String nl;

    public static boolean failUnset = false;

    public static final String CONFIG = "test.properties";

    static {

	InputStream in = null;

	try {
	    
	    Thread t = Thread.currentThread();
	    in = t.getContextClassLoader().getResourceAsStream(CONFIG);

	    if (in == null) {
		throw new IOException("Test configuration file not found: " +
				      CONFIG);
	    }

	    Properties props = new Properties();
	    props.load(in);

	    String prefix = "org.globus.ftp.test.";


	    // general



	    fileSeparator = System.getProperty("file.separator");
	    nl = System.getProperty("line.separator");
	    noSuchServer = props.getProperty(prefix + "noSuchServer.host");
	    
	    parallelism = toInt(
				props.getProperty(prefix + 
						  "gridftp.parallelism")
				);


	    
	    // local


	    localDestDir = props.getProperty(prefix + "local.destDir");
	    localSrcFile = props.getProperty(prefix + "local.srcFile");
	    localSrcDir  = props.getProperty(prefix + "local.srcDir");
	    
	    // local server port
	    String lssStr =  props.getProperty(prefix + 
					       "local.serverPort");
	    localServerPort = toIntOptional(lssStr);
	    
	    //server A


	    
	    serverAHost = props.getProperty(prefix + "gridftp.serverA.host");
	    serverAPort = toInt(
				props.getProperty(prefix + "gridftp.serverA.port")
				);
	    serverASubject = props.getProperty(prefix + "gridftp.serverA.subject");

	    serverADir = props.getProperty(prefix + "gridftp.serverA.dir");
	    serverAFile = props.getProperty(prefix + "gridftp.serverA.file");
	    serverAFileSize = toInt(
				    props.getProperty(prefix + "gridftp.serverA.file.size")
				    );
            serverAFileChecksum = props.getProperty(prefix + "gridftp.serverA.file.checksum");
	    serverASmallFile = props.getProperty(prefix + "gridftp.serverA.smallFile");
	    serverALargeFile = props.getProperty(prefix + "gridftp.serverA.largeFile");
	    serverANoSuchFile = props.getProperty(prefix + "gridftp.serverA.nosuchfile");


	    //defining non existent port:
	    //user is allowed not to define it
	    String noSuchPort_str =    props.getProperty(prefix + "gridftp.serverA.noSuchPort");
	    serverANoSuchPort = toIntOptional(noSuchPort_str);




	    // server B


	    
	    serverBHost = props.getProperty(prefix + "gridftp.serverB.host");
	    serverBPort = toInt(
				props.getProperty(prefix + "gridftp.serverB.port")
				);
	    serverBSubject = props.getProperty(prefix + "gridftp.serverB.subject");

	    serverBDir = props.getProperty(prefix + "gridftp.serverB.dir");
	    serverBFile = props.getProperty(prefix + "gridftp.serverB.file");
	    serverBNoSuchDir = props.getProperty(prefix + "gridftp.serverB.nosuchdir");



	    // FTP server F (src)



	    serverFHost = props.getProperty(prefix + "serverF.host");
	    serverFPort = toInt(
				props.getProperty(prefix + "serverF.port")
				);
	    serverFDir = props.getProperty(prefix + "serverF.dir");
	    serverFFile = props.getProperty(prefix + "serverF.file");
	    serverFFileSize = toInt(
				    props.getProperty(prefix + "serverF.file.size"));
	    serverFNoSuchFile = props.getProperty(prefix + "serverF.nosuchfile");
	    serverFUser = props.getProperty(prefix + "serverF.user");
	    serverFPassword = props.getProperty(prefix + "serverF.password");
	    //defining non existent port:
	    //user is allowed not to define it
	    String fNoSuchPort_str =    props.getProperty(prefix + "gridftp.serverF.noSuchPort");
	    serverFNoSuchPort = toIntOptional(fNoSuchPort_str);
	    


	    // FTP server G (dest) (optional)




	    serverGHost = props.getProperty(prefix + "serverG.host");
	    if (serverGHost.equals("")) {
		serverGHost = null;
	    } else {
		String portStr = props.getProperty(prefix + "serverG.port");
		serverGPort = toInt(portStr);
		serverGDir = props.getProperty(prefix + "serverG.dir");
		serverGFile = props.getProperty(prefix + "serverG.file");
		serverGNoSuchDir = props.getProperty(prefix + "serverG.nosuchdir");
		serverGUser = props.getProperty(prefix + "serverG.user");
		serverGPassword = props.getProperty(prefix + "serverG.password");
	    }	    
	    //logger.debug(show());

	} catch (Exception e) {
	    if (e instanceof NumberFormatException) {
		logger.info("Error: Badly formatted numbers in properties file.");
	    }
	    logger.info("stack trace:\n");
	    e.printStackTrace();
	    TestCase.fail(e.toString());
	} finally {
	    if (in != null) {
		try { in.close(); } catch(Exception ee) {}
	    }
	}
    }

    /**
       @return human readable description of current test environment.
     **/
    static String show() {
	String desc = 
	    "Test Environment" + nl +
	    "================" + nl +
	    "parallelism = " + parallelism + nl +	
	    "noSuchServer: " + noSuchServer + nl +
	    nl +

	    "local dest dir = " + localDestDir + nl + 
	    "local src dir = " + localSrcDir + nl + 
	    "local src file = " + localSrcFile + nl + 
	    "local server port = " + localServerPort + nl + 
	    "================" + nl +
	    "GridFTP source server: " + nl +
	    "serverAHost = " + serverAHost + nl +
	    "serverAPort = " + serverAPort + nl +
	    "serverADir = " + serverADir + nl +
	    "serverAFile = " + serverAFile + nl +
	    "serverALargeFile = " + serverALargeFile + nl +
	    "serverANoSuchFile = " + serverANoSuchFile + nl +
	    "serverANoSuchPort = " + 
	    ((serverANoSuchPort == UNDEFINED) ?
	     "UNDEFINED" :
	     Integer.toString(serverANoSuchPort)
	     )  + nl +
	    nl +
	    "GridFTP dest server: " + nl +
	    "serverBHost = " + serverBHost + nl +
	    "serverBPort = " + serverBPort + nl +
	    "serverBDir = " + serverBDir + nl +
	    "serverBFile = " + serverBFile + nl +
	    "ServerBNoSuchDir = " + serverBNoSuchDir + nl +
	    nl +
	    "FTP source server: " + nl +
	    "serverFHost = " + serverFHost + nl +
	    "serverFPort = " + serverFPort + nl +
	    "serverFDir = " + serverFDir + nl +
	    "serverFFile = " + serverFFile + nl +
	    "serverFFileSize = " + serverFFileSize + nl +
	    "serverFUser = " + serverFUser + nl +
	    "serverFPassword = " + serverFPassword + nl + 
	    "serverFNoSuchFile = " + serverFNoSuchFile + nl + 
	    "serverFNoSuchPort = " + serverFNoSuchPort + nl 
	    ;
	if (serverGHost != null) {
	    desc += nl +	    
		"FTP dest server: " + nl +
		"serverGHost = " + serverGHost + nl +
		"serverGPort = " + serverGPort + nl +
		"serverGDir = " + serverGDir + nl +
		"serverGFile = " + serverGFile + nl +
		"serverGUser = " + serverGUser + nl +
		"serverGPassword = " + serverGPassword + nl;
		} else {
		    desc += nl + "FTP dest server: UNDEFINED";
		}
	return desc;
    }

    // convert to integer
    // an optional argument
    private static int toIntOptional(String str) 
	throws NumberFormatException {
	return (str == null || str.equals("")) ?
	    UNDEFINED : toInt(str);
    }
	
    private static int toInt(String str) 
	throws NumberFormatException{
	try {
	    return Integer.parseInt(str);
	} catch (NumberFormatException e) {
	    logger.error("This is not an integer: " + str);
	    throw e;
	}
    }

    public static Authorization getAuthorization(String subject) {
	if (subject == null) {
	    return HostAuthorization.getInstance();
	} else {
	    return new IdentityAuthorization(subject);
	}
    }
}
