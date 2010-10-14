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
package org.globus.mds.gsi.jndi;


import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslException;

import javax.security.auth.callback.CallbackHandler;  // from JAAS

import java.util.Map;

import org.globus.mds.gsi.common.GSIMechanism;

import org.ietf.jgss.GSSException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


public class SaslClientWrapper implements SaslClient {
   
    private com.sun.security.sasl.preview.SaslClient client;

    public SaslClientWrapper(com.sun.security.sasl.preview.SaslClient client) {
        this.client = client;
    }

    public boolean hasInitialResponse() {
        return this.client.hasInitialResponse();
    }
    
    public byte[] evaluateChallenge(byte[] challengeData) throws SaslException {
        try {
            return this.client.evaluateChallenge(challengeData);
        } catch (com.sun.security.sasl.preview.SaslException e) {
            throw new SaslException("", e);
        }
    }

    public byte[] wrap(byte[] outgoing,
                       int offset,
                       int len)
        throws SaslException {
        try {
            return this.client.wrap(outgoing, offset, len);
        } catch (com.sun.security.sasl.preview.SaslException e) {
            throw new SaslException("", e);
        }
    }

    public byte[] unwrap(byte[] incoming,
                         int offset,
                         int len) 
        throws SaslException {
        try {
            return this.client.unwrap(incoming, offset, len);
        } catch (com.sun.security.sasl.preview.SaslException e) {
            throw new SaslException("", e);
        }
    }

    public void dispose() 
        throws SaslException {
        try {
            this.client.dispose();
        } catch (com.sun.security.sasl.preview.SaslException e) {
            throw new SaslException("", e);
        }
    }
    
    public Object getNegotiatedProperty(String propName) {
        try {
            return client.getNegotiatedProperty(propName);
        } catch (com.sun.security.sasl.preview.SaslException e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    public boolean isComplete() {
        return this.client.isComplete();
    }

    public String getMechanismName() {
        return this.client.getMechanismName();
    }
    
}
