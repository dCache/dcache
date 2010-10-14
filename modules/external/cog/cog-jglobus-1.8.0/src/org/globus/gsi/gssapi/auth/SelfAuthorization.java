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

import org.gridforum.jgss.ExtendedGSSManager;

import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSName;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSCredential;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Performs the identity authorization check. The identity
 * is obtained from specified Globus credentials.
 */
public class SelfAuthorization 
    extends GSSAuthorization {

    private static Log logger =
	LogFactory.getLog(SelfAuthorization.class.getName());

    private static SelfAuthorization authorization;
    
    /**
     * Returns a single instance of this class.
     *
     * @return the instance of this class.
     */
    public synchronized static SelfAuthorization getInstance() {
	if (authorization == null) {
	    authorization = new SelfAuthorization();
	}
	return authorization;
    }

    public GSSName getExpectedName(GSSCredential cred, String host) 
	throws GSSException {
        if (cred == null) {
            GSSManager manager = ExtendedGSSManager.getInstance();
            cred = manager.createCredential(GSSCredential.INITIATE_AND_ACCEPT);
        }
        return cred.getName();
    }

    /**
     * Performs self authorization.
     */
    public void authorize(GSSContext context, String host)
	throws AuthorizationException {
	logger.debug("Authorization: SELF");
	
	try {
	    if (!context.getSrcName().equals(context.getTargName())) {
		GSSName expected = null;
		GSSName target = null;
		if (context.isInitiator()) {
		    expected = context.getSrcName();
		    target = context.getTargName();
		} else {
		    expected = context.getTargName();
		    target = context.getSrcName();
		}
		generateAuthorizationException(expected, target);
	    }
	} catch (GSSException e) {
	    throw new AuthorizationException("Authorization failure", e);
	}
    }
    
}
