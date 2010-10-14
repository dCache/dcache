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

import com.sun.security.sasl.preview.Sasl;
import com.sun.security.sasl.preview.SaslClient;
import com.sun.security.sasl.preview.SaslException;

import javax.security.auth.callback.CallbackHandler;  // from JAAS

import java.util.Map;

import org.globus.mds.gsi.common.GSIMechanism;

import org.ietf.jgss.GSSException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Implements the SASL client mechanism for GSI.
 * <p>
 *
 */
public class GSIMech extends GSIMechanism implements SaslClient {
   
    private static Log logger = 
	LogFactory.getLog(GSIMech.class.getName());
 
    public GSIMech(String authzID, String protocol, String serverName,
		   Map props, CallbackHandler cbh) 
	throws SaslException {

	if (props != null) {
	    // Max receive buffer size
	    String prop = (String)props.get(MAX_BUFFER);
	    if (prop != null) {
		try {
		    this.recvMaxBufSize = Integer.parseInt(prop);
		} catch (NumberFormatException e) {
		    throw new SaslException("Property must be string representation of integer: " + MAX_BUFFER);
		}
	    }
	    
	    // Max send buffer size
	    prop = (String)props.get(MAX_SEND_BUF);
	    if (prop != null) {
		try {
		    this.sendMaxBufSize = Integer.parseInt(prop);
		} catch (NumberFormatException e) {
		    throw new SaslException("Property must be string representation of integer: " + MAX_SEND_BUF);
		}
	    }
	}

	try {
	    init(serverName, props);
	} catch(Exception e) {
	    throw new SaslException("Failed to initialize", e);
	}
    }

    /**
     * Determines whether this mechanism has an optional initial response.
     * If true, caller should call <tt>evaluateChallenge()</tt> with an
     * empty array to get the initial response.
     *
     * @return true if this mechanism has an initial response.
     *         Always returns true for SSL.
     */    
    public boolean hasInitialResponse() {
	return true;
    }
    
    /**
     * Evaluates the challenge data and generates a response.
     *
     * @param challengeData The non-null challenge sent from the server.
     *
     * @return The possibly null reponse to send to the server.
     * It is null if the challenge accompanied a "SUCCESS" status and the challenge
     * only contains data for the client to update its state and no response
     * needs to be sent to the server.
     * @exception SaslException If an error occurred while processing
     * the challenge or generating a response.
     */
    public byte[] evaluateChallenge(byte[] challengeData) throws SaslException {
	
	if (challengeData == null) {
	    throw new SaslException("Received null challenge data");
	}

	byte [] token = null;
	try {
	    token = exchangeData(challengeData);
	} catch (GSSException e) {
	    throw new SaslException("evaluateChanllenge failed", e);
	} catch (Exception e) {
	    throw new SaslException("evaluateChanllenge failed", e);
	}
	return token;
    }

    /**
     * Wraps a byte array to be sent to the server.
     * This method can be called only after the authentication exchange has
     * completed (i.e., when <tt>isComplete()</tt> returns true) and only if
     * the authentication exchange has negotiated integrity and/or privacy 
     * as the quality of protection; otherwise, a <tt>SaslException</tt> is thrown.
     * <p>
     * Returns SSL wrapped byte array.
     *
     * @param outgoing A non-null byte array containing the bytes to encode.
     * @param offset The starting position at <tt>outgoing</tt> of the bytes to use.
     * @param len The number of bytes from <tt>outgoing</tt> to use.
     * @return A non-null byte array containing the encoded bytes.
     * @exception SaslException if the authentication exchange has not completed or
     * if the negotiated quality of protection has neither integrity nor privacy.
     */
    public byte[] wrap(byte[] outgoing,
		       int offset,
		       int len)
	throws SaslException {
	try {
	    return context.wrap(outgoing, offset, len, null);
	} catch(GSSException e) {
	    throw new SaslException("wrap failed", e);
	}
    }

    /**
     * Unwraps a byte array received from the server.
     * This method can be called only after the authentication exchange has
     * completed (i.e., when <tt>isComplete()</tt> returns true) and only if
     * the authentication exchange has negotiated integrity and/or privacy 
     * as the quality of protection; otherwise, a <tt>SaslException</tt> is thrown.
     * <p>
     * Returns SSL unwraped byte array.
     *
     * @param incoming A non-null byte array containing the encoded bytes
     *       from the server.
     * @param offset The starting position at <tt>incoming</tt> of the bytes to use.
     * @param len The number of bytes from <tt>incoming</tt> to use.
     * @return A non-null byte array containing the decoded bytes.
     * @exception SaslException if the authentication exchange has not completed or
     * if the negotiated quality of protection has neither integrity nor privacy.
     */
    public byte[] unwrap(byte[] incoming,
			 int offset,
			 int len) 
	throws SaslException {
	try {
	    return context.unwrap(incoming, offset, len, null);
	} catch(GSSException e) {
	    throw new SaslException("unwrap failed", e);
	}
    }

    /**
     * Disposes of the internal I/O streams.
     * Invoking this method invalidates the SaslClient instance. 
     *
     * @throws SaslException If a problem was encountered while disposing
     * the resources.
     */
    public void dispose() 
	throws SaslException {
	logger.debug("dispose");
	try {
	    context.dispose();
	} catch (GSSException e) {
	    throw new SaslException("dispose failed", e);
	}
    }
    

    /**
     * Retrieves the negotiated property.
     * This method can be called only after the authentication exchange has
     * completed (i.e., when <tt>isComplete()</tt> returns true); otherwise, a
     * <tt>SaslException</tt> is thrown.
     * 
     * @return The value of the negotiated property. If null, the property was
     * not negotiated or is not applicable to this mechanism.
     * @exception SaslException if this authentication exchange has not completed
     * 
     */
    public String getNegotiatedProperty(String propName) 
	throws SaslException {
	logger.debug("getNegotiatedProperty: " + propName);
	if (propName.equals(Sasl.QOP)) {
	    if (isPrivacyQop()) {
		return "auth-conf";
	    } else if (isIntegrityQop()) {
		return "auth-int";
	    } else {
		return "auth";
	    }
	} else if (propName.equals(Sasl.MAX_BUFFER)) {
	    return Integer.toString(getRecvMaxBufSize());
	} else if (propName.equals(MAX_SEND_BUF)) {
	    return Integer.toString(getSendMaxBufSize());
	} else {
	    return null;
	}
    }
    
    protected void finalize() throws Throwable {
	dispose();
    }
    
}
