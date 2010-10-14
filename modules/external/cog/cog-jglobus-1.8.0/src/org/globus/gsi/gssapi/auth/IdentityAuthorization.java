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
 * Implements a simple identity authorization mechanism.
 * The given identify is compared with the peer's identity.
 */
public class IdentityAuthorization 
    extends GSSAuthorization {

    private static Log logger =
        LogFactory.getLog(IdentityAuthorization.class.getName());

    protected String _identity;
    
    /**
     * Constructor used by superclasses.
     */
    protected IdentityAuthorization() {
    }

    /**
     * Creates a new instance of this class with given
     * expected identity.
     *
     * @param identity the expected identity. Must not be null.
     */
    public IdentityAuthorization(String identity) {
        setIdentity(identity);
    }

    /**
     * Sets the expected identity for the authorization 
     * check.
     *
     * @param identity the expected identity. Must not be null.
     */
    public void setIdentity(String identity) {
        if (identity == null) {
            throw new IllegalArgumentException("Identity cannot be null");
        }
        _identity = identity;
    }
    
    /**
     * Returns the expected identity.
     *
     * @return the expected identity.
     */
    public String getIdentity() {
        return _identity;
    }

    public GSSName getExpectedName(GSSCredential cred, String host) 
        throws GSSException {
        GSSManager manager = ExtendedGSSManager.getInstance();
        return manager.createName(_identity, null);
    }

    /**
     * Performs identity authorization. The given identity is compared
     * with the peer's identity.
     *
     * @param context the security context
     * @param host host address of the peer.
     * @exception AuthorizationException if the peer's
     *            identity does not match the expected identity.
     */
    public void authorize(GSSContext context, String host)
        throws AuthorizationException {
        logger.debug("Authorization: IDENTITY");

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
        if (o instanceof IdentityAuthorization) {
            IdentityAuthorization other = (IdentityAuthorization)o;
            if (this._identity == null) {
                return (other._identity == null);
            } else {
                return this._identity.equals(other._identity);
            }
        }
        return false;
    }
    
    public int hashCode() {
        return (this._identity == null) ? 0 : this._identity.hashCode();
    }
    
}
