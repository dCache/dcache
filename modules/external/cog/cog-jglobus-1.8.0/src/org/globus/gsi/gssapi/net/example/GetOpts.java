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
package org.globus.gsi.gssapi.net.example;

import org.globus.gsi.gssapi.net.GssSocket;

public class GetOpts extends org.globus.gsi.gssapi.example.GetOpts {

    public int wrapMode = GssSocket.SSL_MODE;

    public GetOpts(String usage, String helpMsg) {
	super(usage, helpMsg);
    }

    protected int parseArg(String[] args, int i) {
	if (args[i].equalsIgnoreCase("-wrap-mode")) {
	    String arg = args[++i];
	    if (arg.equalsIgnoreCase("ssl")) {
		wrapMode = GssSocket.SSL_MODE;
	    } else if (arg.equalsIgnoreCase("gsi")) {
		wrapMode = GssSocket.GSI_MODE;
	    } else {
		error("Invalid -wrap-mode argument: " + arg);
	    }
	    return 1;
	} else {
	    return super.parseArg(args, i);
	}
    }

}
