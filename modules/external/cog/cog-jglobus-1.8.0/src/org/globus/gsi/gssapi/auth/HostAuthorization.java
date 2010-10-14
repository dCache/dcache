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
 * Implements a simple host authorization mechanism.
 * The peer's host name (in FQDN form) is compared with the
 * host name specified in the peer's certificate chain.
 */
public class HostAuthorization 
    extends GSSAuthorization {

    private static Log logger =
        LogFactory.getLog(HostAuthorization.class.getName());

    public static final HostAuthorization ldapAuthorization = 
        new HostAuthorization("ldap");

    private String _service = null;

    private static HostAuthorization hostAuthorization;
    
    public HostAuthorization(String service) {
        _service = (service == null) ? "host" : service;
    }
    
    /**
     * Returns an instance of host authentication.
     *
     * @return an instance of this class initialized with
     *         <i>host</i> as a service.
     */
    public synchronized static HostAuthorization getInstance() {
        if (hostAuthorization == null) {
            hostAuthorization = new HostAuthorization("host");
        }
        return hostAuthorization;
    }
    
    public GSSName getExpectedName(GSSCredential cred, String host) 
        throws GSSException {
        GSSManager manager = ExtendedGSSManager.getInstance();
        return manager.createName(_service + "@" + host, 
                                  GSSName.NT_HOSTBASED_SERVICE);
    }
    
    /**
     * Performs host authentication. The hostname of the peer is
     * compared with the hostname specified in the peer's (topmost) 
     * certificate in the certificate chain. The hostnames must
     * match exactly (in case-insensitive way)
     *
     * @param context the security context
     * @param host host address of the peer.
     * @exception AuthorizationException if the hostnames
     *            do not match.
     */
    public void authorize(GSSContext context, String host)
        throws AuthorizationException {
        logger.debug("Authorization: HOST");

        try {
            GSSName expected = getExpectedName(null, host);
        
            GSSName target = null;
            if (context.isInitiator()) {
                target = context.getTargName();
            } else {
                target = context.getSrcName();
            }
        
            if (!expected.equals(target)) {
                generateAuthorizationException(expected, target);
            }
        } catch (GSSException e) {
            throw new AuthorizationException("Authorization failure", e);
        }
    }

    public boolean equals(Object o) {
        if (o == null) {
            return false;
        }
        if (this == o) {
            return true;
        }
        if (o instanceof HostAuthorization) {
            HostAuthorization other = (HostAuthorization)o;
            if (this._service == null) {
                return (other._service == null);
            } else {
                return this._service.equals(other._service);
            }
        }
        return false;
    }
    
    public int hashCode() {
        return (this._service == null) ? 0 : this._service.hashCode();
    }
    
}
