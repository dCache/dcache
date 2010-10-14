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

import com.sun.security.sasl.preview.SaslClientFactory;
import com.sun.security.sasl.preview.SaslClient;
import com.sun.security.sasl.preview.SaslException;

import java.util.Map;
import java.util.Hashtable;

import org.globus.mds.gsi.common.GSIMechanism;

import javax.security.auth.callback.CallbackHandler; // JAAS

/**
 * Client factory for Globus GSI.
 * The name of the mechanims should be changed, as GSSAPI specifically
 * appies to Kerberos V5. 
 */
public class ClientFactory implements SaslClientFactory {
    
    private static final String myMechs[] = { GSIMechanism.NAME };
    
    private static final int GSI = 0;
    
    public ClientFactory() {
    }
    
    public SaslClient createSaslClient(String[] mechs,
				       String authorizationId,
				       String protocol,
				       String serverName,
				       Hashtable props,
				       CallbackHandler cbh)
	throws SaslException {
	return createSaslClient(mechs, 
				authorizationId,
				protocol,
				serverName,
				(Map)props,
				cbh);
    }
    
    public SaslClient createSaslClient(String[] mechs,
				       String authorizationId,
				       String protocol,
				       String serverName,
				       Map props,
				       CallbackHandler cbh) 
	throws SaslException {
	
	for (int i = 0; i < mechs.length; i++) {
	    if (mechs[i].equals(myMechs[GSI])) {
		return new GSIMech(authorizationId, 
				   protocol, 
				   serverName,
				   props,
				   cbh);
	    }
	}
	return null;
    }
    
    /**
     * Returns an array of names of mechanisms that match the
     * specified mechanism selection policies.
     */
    public String[] getMechanismNames(Map props) {
        return (String[])myMechs.clone();
    }

    /**
     * Returns an array of names of mechanisms that match the 
     * specified mechanism selection policies.
     */
    public String[] getMechanismNames(Hashtable props) {
	return (String[])myMechs.clone();
    }
    
    /**
     * This function is replaced with the above one
     * in the latest RFC.
     */
    public String[] getMechanismNames() {
	return getMechanismNames(null);
    }
    
}
