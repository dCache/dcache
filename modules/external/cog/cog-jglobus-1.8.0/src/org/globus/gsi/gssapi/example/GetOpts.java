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
package org.globus.gsi.gssapi.example;

import org.ietf.jgss.GSSContext;

public class GetOpts {

    public boolean conf = true;
    public int lifetime = GSSContext.DEFAULT_LIFETIME;
    public boolean gsiMode = true;
    public boolean deleg = false;
    public boolean limitedDeleg = true;
    public boolean rejectLimitedProxy = false;
    public boolean anonymity = false;
    public String auth = null;

    protected String usage;
    protected String helpMsg;
    
    public GetOpts(String usage, String helpMsg) {
	this.usage = usage;
	this.helpMsg = helpMsg;
    }

    public int parse(String[] args) {
	int i = 0;
	for (i=0;i<args.length;i++) {
	    if (args[i].charAt(0) != '-') {
		// treat as user arg
		break;
	    }
	    i += parseArg(args, i);
	}
	return i;
    }

    protected int parseArg(String[] args, int i) {
	int j = 0;

	if (args[i].equalsIgnoreCase("-enable-conf")) {
	    conf = true;
	} else if (args[i].equalsIgnoreCase("-disable-conf")) {
	    conf = false;
	} else if (args[i].equalsIgnoreCase("-gss-mode")) {
	    String arg = args[++i];
	    if (arg.equalsIgnoreCase("ssl")) {
		gsiMode = false;
	    } else if (arg.equalsIgnoreCase("gsi")) {
		gsiMode = true;
	    } else {
		error("Invalid -gss-mode argument: " + arg);
	    }
	    j = 1;
	} else if (args[i].equalsIgnoreCase("-deleg-type")) {
	    String arg = args[++i];
	    if (arg.equalsIgnoreCase("full")) {
		limitedDeleg = false;
		deleg = true;
	    } else if (arg.equalsIgnoreCase("limited")) {
		limitedDeleg = true;
		deleg = true;
	    } else if (arg.equalsIgnoreCase("none")) {
		deleg = false;
	    } else {
		error("Invalid -deleg-type argument: " + arg);
	    }
	    j = 1;
	} else if (args[i].equalsIgnoreCase("-auth")) {
	    String arg = args[++i];
	    if (arg.equalsIgnoreCase("host")) {
		auth = "host";
	    } else if (arg.equalsIgnoreCase("self")) {
		auth = "self";
	    } else {
		auth = arg;
	    }
	    j = 1;
	} else if (args[i].equalsIgnoreCase("-lifetime")) {
	    lifetime = Integer.parseInt(args[++i]);
	    j = 1;
	} else if (args[i].equalsIgnoreCase("-rejectLimitedProxy")) {
	    rejectLimitedProxy = true;
	} else if (args[i].equalsIgnoreCase("-anonymous")) {
	    anonymity = true;
	} else if (args[i].equalsIgnoreCase("-help")) {
	    System.err.println(this.usage);
	    System.err.println();
	    System.err.println(this.helpMsg);
	    System.exit(0);
	} else {
	    System.err.println("Argument not recognized: " + args[i]);
	    System.exit(1);
	}

	return j;
    }

    protected static void error(String msg) {
	System.err.println(msg);
	System.exit(1);
    }

}
