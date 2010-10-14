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
package org.globus.gatekeeper;

import org.globus.gsi.GlobusCredential;
import org.globus.security.gridmap.GridMap;

// GSS implementation specific class
import org.globus.gsi.gssapi.GlobusGSSCredentialImpl;

import java.util.Properties;
import java.io.FileInputStream;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.spi.LoggingEvent;

import org.ietf.jgss.GSSCredential;

public class Gatekeeper {

    private static final String message =
	"\n" +
	"Syntax: java Gatekeeper [options]\n" +
	"        java Gatekeeper -version\n" +
	"        java Gatekeeper -help\n\n" +
	"\tOptions\n" +
	"\t-help | -usage\n" +
	"\t\tDisplays usage\n" +
	"\t-p | -port\n" +
	"\t\tPort of the Gatekeeper\n" +
	"\t-d | -debug\n" +
	"\t\tEnable debug mode\n" +
	"\t-s | -services\n" +
	"\t\tSpecifies services configuration file.\n " +
	"\t-l | -log\n" +
	"\t\tSpecifies log file.\n" +
	"\t-gridmap\n" +
	"\t\tSpecifies gridmap file.\n" +
	"\t-proxy\n" +
	"\t\tProxy credentials to use.\n" +
	"\t-serverKey\n" +
	"\t\tSpecifies private key (to be used with -serverCert.\n" +
	"\t-serverCert\n" +
	"\t\tSpecifies certificate (to be used with -serverKey.\n" +
	"\t-caCertDir\n" +
	"\t\tSpecifies locations (directory or files) of trusted \n" +
	"\t\tCA certificates.\n";
    
    public static void main(String[] args) {
	
	int port = GateKeeperServer.PORT;
	boolean debug = false;
	String configFile = null;
	String logFile = null;
	String gridMapFile = null;
	String proxyFile = null;
	String serverKey = null;
	String serverCert = null;
	String caCertDir = null;
	boolean error = false;

	for (int i = 0; i < args.length; i++) {
	    
	    if (args[i].equalsIgnoreCase("-p") ||
		args[i].equalsIgnoreCase("-port")) {
		port = Integer.parseInt(args[++i]);
	    } else if (args[i].equalsIgnoreCase("-d") ||
		       args[i].equalsIgnoreCase("-debug")) {
		debug = true;
	    } else if (args[i].equalsIgnoreCase("-s") ||
		       args[i].equalsIgnoreCase("-services")) {
		configFile = args[++i];
	    } else if (args[i].equalsIgnoreCase("-l") ||
		       args[i].equalsIgnoreCase("-log")) {
		logFile = args[++i];
	    } else if (args[i].equalsIgnoreCase("-gridmap")) {
		gridMapFile = args[++i];
	    } else if (args[i].equalsIgnoreCase("-proxy")) {
		proxyFile = args[++i];
	    } else if (args[i].equalsIgnoreCase("-serverKey")) {
		serverKey = args[++i];
	    } else if (args[i].equalsIgnoreCase("-serverCert")) {
		serverCert = args[++i];
	    } else if (args[i].equalsIgnoreCase("-cacertdir")) {
		caCertDir = args[++i];
	    } else if (args[i].equalsIgnoreCase("-help") ||
		       args[i].equalsIgnoreCase("-usage")) {
		System.out.println(message);
		System.exit(0);
	    } else {
		System.err.println();
		System.err.println("Error: unreconginzed argument " + i + " : " + args[i]);
		error = true;
		break;
	    }
	    
	}

	if (error) {
	    System.err.println("\nSyntax: java GateKeeperServer [-help][-p port][-c configFile]");
	    System.err.println("\nUse -help to display full usage.");
	    System.exit(1);
	}
	
	if (proxyFile != null && (serverKey != null || serverCert != null)) {
	    System.err.println();
	    System.err.println("Error: You cannot specify -proxy with -serverKey or -serverCert.");
	    System.exit(1);
	}

	if ( (serverKey != null && serverCert == null) ||
	     (serverKey == null && serverCert != null)) {
	    System.err.println();
	    System.err.println("Error: -serverKey and -serverCert must be specified togeter");
	}

	GSSCredential gssCred = null;
	GlobusCredential credentials = null;

	try {
	    if (proxyFile == null && serverKey == null && serverCert == null) {
		credentials = GlobusCredential.getDefaultCredential();
	    } else if (proxyFile != null) {
		credentials = new GlobusCredential(proxyFile);
	    } else if (serverKey != null && serverCert != null) {
		credentials = new GlobusCredential(serverCert, serverKey);
	    } else {
		System.err.println();
		System.err.println("Error: No credentials loaded.");
		System.exit(1);
	    }

	    gssCred = new GlobusGSSCredentialImpl(credentials, 
						  GSSCredential.ACCEPT_ONLY);
	} catch(Exception e) {
	    System.err.println("Failed to load credentials: " + e.getMessage());
	    System.exit(1);
	}
	
	GridMap gridMap = new GridMap();

	if (gridMapFile != null) {
	    try {
		gridMap.load(gridMapFile);
	    } catch(Exception e) {
		System.err.println("Failed to load grid map file: " + e.getMessage());
		System.exit(1);
	    }
	} else {
	    try {
		// only allow myself in case the grid map was not specified
		gridMap.map( gssCred.getName().toString(), 
			     System.getProperty("user.name") );
	    } catch (Exception e) {
		System.err.println("Failed to initialize gridmap file: " + e.getMessage());
		System.exit(1);
	    }
	}
	
	Properties props = null;
	
	if (configFile != null) {
	    props = new Properties();
	    FileInputStream in = null;
	    try {
		in = new FileInputStream(configFile);
		props.load(in);
	    } catch(Exception e) {
		System.err.println("Failed to load services configuration file: " + e.getMessage());
		System.exit(1);
	    } finally {
		if (in != null) {
		    try { in.close(); } catch (Exception e) {}
		}
	    }
	}

	if (debug) {
	    // registers a console root appeneder
	    BasicConfigurator.configure();
	} else {
	    // regiseter a null root appender - just a hack
	    Logger root = Logger.getRootLogger();
	    root.addAppender(new AppenderSkeleton() {
		    public void close() {}
		    public boolean requiresLayout() {
			return false;
		    }
		    public void append(LoggingEvent event) {}
		});
	}
	
	GateKeeperServer gk = null;

        try {
            gk = new GateKeeperServer(gssCred, port);
	 
	    gk.setGridMap(gridMap);

	    if (logFile != null) {
		gk.setLogFile(logFile);
	    }

	    if (props != null) {
		gk.registerServices(props);
	    } else {
		gk.registerService("jobmanager", 
				   "org.globus.gatekeeper.jobmanager.ForkJobManagerService", 
				   null);
	    }
	    
            System.out.println("GRAM contact: " + gk.getContact());
        } catch(Exception e) {
	    System.err.println("Gatekeeper failed to start: " + e.getMessage());
	    if (debug) {
		e.printStackTrace();
	    }
	}
    }

}

