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
import javax.security.sasl.SaslClientFactory;

import java.util.Map;
import java.util.Hashtable;

import javax.security.auth.callback.CallbackHandler; // JAAS

public class SaslClientFactoryWrapper implements SaslClientFactory {

    private com.sun.security.sasl.preview.SaslClientFactory factory;

    public SaslClientFactoryWrapper() {
        this(new ClientFactory());
    }

    public SaslClientFactoryWrapper(com.sun.security.sasl.preview.SaslClientFactory factory) {
        this.factory = factory;
    }


    public SaslClient createSaslClient(String[] mechs,
                                       String authorizationId,
                                       String protocol,
                                       String serverName,
                                       Map props,
                                       CallbackHandler cbh) 
        throws SaslException {
        try {
            com.sun.security.sasl.preview.SaslClient client =
                this.factory.createSaslClient(mechs, 
                                              authorizationId,
                                              protocol, 
                                              serverName,
                                              toHashtable(props),
                                              cbh);
            return (client == null) ? null : new SaslClientWrapper(client);
        } catch (com.sun.security.sasl.preview.SaslException e) {
            throw new SaslException("", e);
        }
    }
    
    public String[] getMechanismNames(Map props) {
        return this.factory.getMechanismNames(toHashtable(props));
    }

    private static Hashtable toHashtable(Map props) {
        if (props == null) {
            return null;
        }
        if (props instanceof Hashtable) {
            return (Hashtable)props;
        } else {
            return new Hashtable(props);
        }
    }
    
}
