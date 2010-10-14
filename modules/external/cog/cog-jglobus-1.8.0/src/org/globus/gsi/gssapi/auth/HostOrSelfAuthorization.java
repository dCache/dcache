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
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSCredential;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class HostOrSelfAuthorization extends GSSAuthorization {

    private static Log logger =
	LogFactory.getLog(HostOrSelfAuthorization.class.getName());

    private static HostOrSelfAuthorization authorization;
    
    private HostAuthorization hostAuthz;
    /**
     * Returns a single instance of this class.
     *
     * @return the instance of this class.
     */
    public synchronized static HostOrSelfAuthorization getInstance() {
	if (authorization == null) {
	    authorization = new HostOrSelfAuthorization("host");
	}
	return authorization;
    }

    public HostOrSelfAuthorization(String service) {
        if (service == null) {
            service = "host";
        }
        this.hostAuthz = new HostAuthorization(service);
    }

    // returning null for now.
    public GSSName getExpectedName(GSSCredential cred, String host) 
	throws GSSException {
        return null;
    }

    /**
     * Performs host authorization. If that fails, performs self authorization
     */
    public void authorize(GSSContext context, String host)
	throws AuthorizationException {
	logger.debug("Authorization: HOST/SELF");
	
	try {
            
            GSSName expected = this.hostAuthz.getExpectedName(null, host);
            
            GSSName target = null;
            if (context.isInitiator()) {
                target = context.getTargName();
            } else {
                target = context.getSrcName();
            }

            if (!expected.equals(target)) { 
                logger.debug("Host authorization failed. Expected " 
                             + expected + " target is " + target);
                
                if (!context.getSrcName().equals(context.getTargName())) {
                    if (context.isInitiator()) {
                        expected = context.getSrcName();
                    } else {
                        expected = context.getTargName();
                    }
                    generateAuthorizationException(expected, target);
                }
            }
	} catch (GSSException e) {
	    throw new AuthorizationException("Authorization failure", e);
	}
    }
    
}
