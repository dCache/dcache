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
package org.globus.gsi.gssapi.auth;

import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSName;

/**
 * Interface for authorization mechanisms.
 * The authorization is performed once the connection was authenticated.
 */
public abstract class Authorization {
    
    /**
     * Performes authorization checks. Throws 
     * <code>AuthorizationException</code> if the authorization fails.
     * Otherwise, the function completes normally.
     *
     * @param context the securit context
     * @param host host address of the peer.
     * @exception AuthorizationException if the peer is
     *            not authorized to access/use the resource.
     */
    public abstract void authorize(GSSContext context, String host) 
	throws AuthorizationException;

    protected void generateAuthorizationException(GSSName expected,
						  GSSName target)
	throws AuthorizationException {

	String lineSep = System.getProperty("line.separator");
	StringBuffer msg = new StringBuffer();
	msg.append("Mutual authentication failed").append(lineSep)
	    .append("  Expected target subject name=\"")
	    .append(expected.toString()).append("\"")
	    .append(lineSep)
	    .append("  Target returned subject name=\"")
	    .append(target.toString())
	    .append("\"");

	throw new AuthorizationException(msg.toString());         
    }
}
