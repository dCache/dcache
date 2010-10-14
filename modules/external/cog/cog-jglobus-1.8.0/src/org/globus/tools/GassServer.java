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
package org.globus.tools;

import org.globus.common.Version;

/** Controls a Gass Server.
 * <pre>
 * Syntax: java GassServer [options]
 *        java GassServer -version
 *        java GassServer -help
 * Options
 * -help | -usage
 *   Displays usage
 * -s | -silent
 *   Enable silent mode (Don't output server URL)
 * -r | -read
 *   Enable read access to the local file system 
 * -w | -write
 *   Enable write access to the local file system 
 * -o
 *   Enable stdout redirection
 * -e
 *   Enable stderr redirection
 * -c | -client-shutdown
 *   Allow client to trigger shutdown the GASS server
 *   See globus-gass-server-shutdown 
 * -p <port> | -port <port>
 *   Start the GASS server using the specified port
 * -i | -insecure
 *   Start the GASS server without security
 * -n <options>
 *   Disable <options>, which is a string consisting 
 *   of one or many of the letters "crwoe"
 * </pre>
 */
public class GassServer {

    private static final String message =
	"\n" +
	"Syntax: java GassServer [options]\n" +
	"        java GassServer -version\n" +
	"        java GassServer -help\n\n" +
	"\tOptions\n" +
	"\t-help | -usage\n" +
	"\t\tDisplays usage\n" +
	"\t-s | -silent\n" +
	"\t\tEnable silent mode (Don't output server URL)\n" +
	"\t-r | -read\n" +
	"\t\tEnable read access to the local file system\n " +
	"\t-w | -write\n" +
	"\t\tEnable write access to the local file system\n " +
	"\t-o\n" +
	"\t\tEnable stdout redirection\n" +
	"\t-e\n" +
	"\t\tEnable stderr redirection\n" +
	"\t-c | -client-shutdown\n" +
	"\t\tAllow client to trigger shutdown the GASS server\n" +
	"\t\tSee globus-gass-server-shutdown\n" + 
	"\t-p <port> | -port <port>\n" +
	"\t\tStart the GASS server using the specified port\n" +
	"\t-i | -insecure\n" +
	"\t\tStart the GASS server without security\n" +
	"\t-n <options>\n" +
	"\t\tDisable <options>, which is a string consisting \n" + 
	"\t\tof one or many of the letters \"crwoe\"\n\n";
    
    /**
     * Create a server that listens on the given port.  If no port is
     * given choose an unused one at random. 
     */
    public static void main(String[] args) {
	
	int port = 0;
	boolean secure = true;
	boolean debug  = false;
	boolean error  = false;
	boolean silent = false;
	int options = 
	    org.globus.io.gass.server.GassServer.READ_ENABLE |
	    org.globus.io.gass.server.GassServer.WRITE_ENABLE |
	    org.globus.io.gass.server.GassServer.STDOUT_ENABLE |
	    org.globus.io.gass.server.GassServer.STDERR_ENABLE;
	
	for (int i = 0; i < args.length; i++) {
      
	    if (args[i].equalsIgnoreCase("-p") ||
		args[i].equalsIgnoreCase("-port")) {
		port = Integer.parseInt(args[++i]);
	    } else if (args[i].equalsIgnoreCase("-i") ||
		       args[i].equalsIgnoreCase("-insecure")) {
		secure = false;
	    } else if (args[i].equalsIgnoreCase("-s") ||
		       args[i].equalsIgnoreCase("-silent")) {
		silent = true;
	    } else if (args[i].equalsIgnoreCase("-d") ||
		       args[i].equalsIgnoreCase("-debug")) {
		debug = true;
	    } else if (args[i].equalsIgnoreCase("-c") ||
		       args[i].equalsIgnoreCase("-client-shutdown")) {
		options |= org.globus.io.gass.server.GassServer.CLIENT_SHUTDOWN_ENABLE;
	    } else if (args[i].equalsIgnoreCase("-r") ||
		       args[i].equalsIgnoreCase("-read")) {
		options |= org.globus.io.gass.server.GassServer.READ_ENABLE;
	    } else if (args[i].equalsIgnoreCase("-w") ||
		       args[i].equalsIgnoreCase("-write")) {
		options |= org.globus.io.gass.server.GassServer.WRITE_ENABLE;
	    } else if (args[i].equalsIgnoreCase("-o")) {
		options |= org.globus.io.gass.server.GassServer.STDOUT_ENABLE;
	    } else if (args[i].equalsIgnoreCase("-e")) {
		options |= org.globus.io.gass.server.GassServer.STDERR_ENABLE;
	    } else if (args[i].equalsIgnoreCase("-version")) {
		System.err.println(Version.getVersion());
		System.exit(1);
	    } else if (args[i].equalsIgnoreCase("-n")) {
		int op = setOptions(options, args[++i]);
		if (op == -1) {
		    error = true;
		    break;
		} else {
		    options = op;
		}
	    } else if (args[i].equalsIgnoreCase("-help") ||
		       args[i].equalsIgnoreCase("-usage")) {
		System.out.println(message);
		System.exit(0);
	    } else {
		System.err.println("\nError: unreconginzed argument " + i + " : " + args[i]);
		error = true;
		break;
	    }
      
	}
	
	if (error) {
	    System.err.println("\nSyntax: java GassServer [-help][-{s,r,w,c,o,e}][-p port]");
	    System.err.println("\nUse -help to display full usage.");
	    System.exit(1);
	    System.exit(1);
	}

	try {
      
	    org.globus.io.gass.server.GassServer s = 
		new org.globus.io.gass.server.GassServer(secure, port);

	    s.setOptions(options);
      
	    if (debug) {
		displayOptions(s.getOptions());
	    }
      
	    if (!silent) {
		System.out.println(s.getURL());
	    }
      
	} catch(Exception e) {
	    System.err.println( "Unable to start GASS Server: " +
				e.getMessage() );
	    if (debug) {
		e.printStackTrace();
	    }
	}
    }

    private static int setOptions(int options, String arg) {
	int size = arg.length();
	for (int i=0;i<size;i++) {
	    switch(arg.charAt(i)) {
	    case 'r' : options &= ~org.globus.io.gass.server.GassServer.READ_ENABLE; break;
	    case 'w' : options &= ~org.globus.io.gass.server.GassServer.WRITE_ENABLE; break;
	    case 'o' : options &= ~org.globus.io.gass.server.GassServer.STDOUT_ENABLE; break;
	    case 'e' : options &= ~org.globus.io.gass.server.GassServer.STDERR_ENABLE; break;
	    case 'c' : options &= ~org.globus.io.gass.server.GassServer.CLIENT_SHUTDOWN_ENABLE; break;
	    default:
		System.err.println("Option unrecognized: " + arg.charAt(i));
		return -1;
	    }
	}
	return options;
    }

    private static void displayOptions(int options) {
	System.out.println("GASS Options:");
	System.out.println("read     : " + 
			   isEnabled(options, 
				     org.globus.io.gass.server.GassServer.READ_ENABLE));
	System.out.println("write    : " + 
			   isEnabled(options,
				     org.globus.io.gass.server.GassServer.WRITE_ENABLE));
	System.out.println("stdout   : " + 
			   isEnabled(options,
				     org.globus.io.gass.server.GassServer.STDOUT_ENABLE));
	System.out.println("stderr   : " + 
			   isEnabled(options,
				     org.globus.io.gass.server.GassServer.STDERR_ENABLE));
	System.out.println("shutdown : " + 
			   isEnabled(options,
				     org.globus.io.gass.server.GassServer.CLIENT_SHUTDOWN_ENABLE));
    }

    private static String isEnabled(int options, int option) {
	return ( (options & option) != 0) ? "enabled" : "disabled";
    }
}
