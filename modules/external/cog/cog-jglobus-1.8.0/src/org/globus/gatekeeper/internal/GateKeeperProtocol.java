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
package org.globus.gatekeeper.internal;

import java.io.InputStream;
import java.io.IOException;

import org.globus.util.http.HTTPProtocol;

import org.globus.gatekeeper.GateKeeperException;
import org.globus.gatekeeper.BadRequestException;
import org.globus.gatekeeper.ServiceNotFoundException;
import org.globus.gatekeeper.AuthorizationException;

/**
 * This is the class responsible for preparing messages before they are sent to
 * the client using the HTTP protocol known as the GateKeeper Protocol.
 * GateKeeperProtocol implements ProtocolSend, which is an interface used by
 * the GateKeeper to send messages to client independant of the implementation.
 *
 */
public class GateKeeperProtocol extends HTTPProtocol {

    private static GateKeeperProtocol protocol = null;

    private static final String GATEKEEPER_APP = "application/x-globus-gram";

    public static GateKeeperProtocol getInstance(String prot) {
	if (protocol == null) {
	    protocol = new GateKeeperProtocol();
	}
	return protocol;
    }
    
    public String getErrorMessage(Exception e) {
	if (e instanceof BadRequestException) {
	    return getBadRequestErrorReply();
	} else if (e instanceof ServiceNotFoundException) {
	    return getFileNotFoundErrorReply();
	} else if (e instanceof AuthorizationException) {
	    return getForbiddenErrorReply();
	} else {
	    // default
	    return getServerErrorReply();
	}
    }
    
    public String getPingSuccessMessage() {
	return getOKReply(GATEKEEPER_APP);
    }

    public GateKeeperRequest parseRequest(InputStream in) 
	throws IOException, GateKeeperException {
	return new GateKeeperRequest(in);
    }
    
}
