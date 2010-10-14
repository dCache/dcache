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
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSName;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Implements the simplest authorization mechanism that does
 * not do any authorization checks.
 */
public class NoAuthorization extends GSSAuthorization {

    private static Log logger =
	LogFactory.getLog(NoAuthorization.class.getName());

    private static NoAuthorization authorization;
    
    /**
     * Returns a single instance of this class.
     *
     * @return the instance of this class.
     */
    public synchronized static NoAuthorization getInstance() {
	if (authorization == null) {
	    authorization = new NoAuthorization();
	}
	return authorization;
    }
    
    /**
     * Always returns null.
     */
    public GSSName getExpectedName(GSSCredential cred, String host) 
	throws GSSException {
        return null;
    }

    /**
     * Performs no authorization checks. The function is always
     * successful. It does not throw any exceptions.
     *
     */
    public void authorize(GSSContext context, String host)
	throws AuthorizationException {
	logger.debug("Authorization: NONE");
    }
    
}
