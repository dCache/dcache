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

import org.globus.util.GlobusURL;
import org.globus.common.Version;

/** Shuts down a Gass server.
 *<pre>
 * Syntax: java GassServerShutdown [-usage] [-version] <GASS-URL>
 *        java GassServerShutdown -help
  * Allows the user to shut down a (remotely) running GASS server, started
  * with client-shutdown permissions (option -c).
  * Options:
  * -help, -usage        Displays usage
  * -version             Displays version
 *</pre>
 */
public class GassServerShutdown {

  private static final String message =
      "\n" +
      "Syntax: java GassServerShutdown [-usage] [-version] <GASS-URL>\n" +
      "        java GassServerShutdown -help\n\n" +
      "\tAllows the user to shut down a (remotely) running\n" + 
      "\tGASS server, started with client-shutdown permissions \n" + 
      "\t(option -c).\n\n" +
      "\tOptions:\n" +
      "\t-help | -usage\n" +
      "\t\tDisplays usage\n" +
      "\t-version\n" +
      "\t\tDisplays version\n\n";
  
    public static void main(String args[]) {

	boolean error       = false;
	boolean debug       = false;
	String url          = null;
	
	for (int i = 0; i < args.length; i++) {
	    if (args[i].charAt(0) != '-' && i+1 == args.length) {
		// rsl spec
		if (url != null) {
		    error = true;
		    System.err.println("Error: Gass URL already specifed");
		    break;
		}

		url = args[i];
	    } else if (args[i].equalsIgnoreCase("-help") ||
		       args[i].equalsIgnoreCase("-usage")) {
		System.err.println(message);
		System.exit(1);
	    } else if (args[i].equalsIgnoreCase("-version")) {
		System.err.println(Version.getVersion());
		System.exit(1);
	    } else {
		System.err.println("Error: Argument not recognized : " + args[i]);
		error = true;
	    }
	}

	if (!error && url == null) {
	    System.err.println("Error: Gass URL is not specified!");
	    error = true;
	}

	if (error) {
	    System.err.println("\nUsage : java GassServerShutdown [-help] <GASS-URL>");
	    System.exit(1);
	}
    
	GlobusURL gassURL = null;

	try {
	    gassURL = new GlobusURL(url);
	} catch(Exception e) {
	    System.err.println("Invalid Gass URL: " + e.getMessage());
	    System.exit(1);
	}

	try {
	    org.globus.io.gass.server.GassServer.shutdown(null, gassURL);
	} catch (Exception e) {
	    System.err.println("Gass shutdown failed: " + e.getMessage());
	    System.exit(1);
	}
    
  }
    
}
