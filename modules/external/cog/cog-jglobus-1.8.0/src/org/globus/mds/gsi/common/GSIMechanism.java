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
package org.globus.mds.gsi.common;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.StringTokenizer;

import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSName;

import org.globus.gsi.gssapi.GSSConstants;

import org.gridforum.jgss.ExtendedGSSManager;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class GSIMechanism {

    private static Log logger = 
	LogFactory.getLog(GSIMechanism.class.getName());

    public static final String SECURITY_CREDENTIALS = 
	"org.globus.gsi.credentials";

    // the user-friendly GSI mechanism name - may change
    public static final String NAME2   = "GSS-GSI";

    // the GSI mechanism name according to RFC2222
    public static final String NAME    = "GSS-OWNYQ6NTEOAUVGWG";

    public static final String MAX_SEND_BUF = "javax.security.sasl.sendmaxbuffer";
    public static final String MAX_BUFFER   = "javax.security.sasl.maxbuffer";
    public static final String STRENGTH     = "javax.security.sasl.strength";
    public static final String QOP          = "javax.security.sasl.qop";
    
    public static final String PROXY        = "org.globus.mds.gsi.proxy";
    
    // default 0 (no protection); 1 (integrity only)
    protected static final byte NO_PROTECTION = (byte)1;
    protected static final byte INTEGRITY_ONLY_PROTECTION = (byte)2;
    protected static final byte PRIVACY_PROTECTION = (byte)4;
    
    protected static final byte LOW_STRENGTH = (byte)1;
    protected static final byte MEDIUM_STRENGTH = (byte)2;
    protected static final byte HIGH_STRENGTH = (byte)4;

    private final static byte[] DEFAULT_QOP = new byte[]{NO_PROTECTION};
    private final static String[] QOP_TOKENS = {"auth-conf", 
						"auth-int", 
						"auth"};
    private final static byte[] QOP_MASKS = {PRIVACY_PROTECTION,
					     INTEGRITY_ONLY_PROTECTION,
					     NO_PROTECTION};

    private final static byte[] DEFAULT_STRENGTH = new byte[] {
	HIGH_STRENGTH, MEDIUM_STRENGTH, LOW_STRENGTH };
    private final static String[] STRENGTH_TOKENS = {"low", 
						     "medium", 
						     "high"};
    private final static byte[] STRENGTH_MASKS = {LOW_STRENGTH, 
						  MEDIUM_STRENGTH,
						  HIGH_STRENGTH};

    
    protected GSSContext context = null;
    protected boolean completed = false;
    
    protected byte[] qop;           // ordered list of qops
    protected byte allQop;          // a mask indicating which QOPs are requested
    protected byte[] strength;      // ordered list of cipher strengths (not used)
    
    // these are negotaited
    protected boolean privacy = false;
    protected boolean integrity = false;

    protected int sendMaxBufSize = 0;     // specified by peer but can override
    protected int recvMaxBufSize = 65536; // optionally specified by client


    public String getMechanismName() {
	return NAME;
    }
    
    public boolean isComplete() {
        return completed;
    }

    /* This has to be called after the sendMaxBufSize and recvMaxBufSize
     * variables were set.
     */
    protected void init(String serverName, Map props) 
	throws Exception {

	GSSCredential cred = null;

	Object tmp = props.get(SECURITY_CREDENTIALS);
	if (tmp != null) {
	    if (tmp instanceof GSSCredential) {
		cred = (GSSCredential)tmp;
	    } else {
		throw new Exception("Invalid credential type passed");
	    }
	}

	GSSManager manager = ExtendedGSSManager.getInstance();

	GSSName target = manager.createName("ldap@" + serverName, 
					    GSSName.NT_HOSTBASED_SERVICE);

	context = manager.createContext(target,
					GSSConstants.MECH_OID,
					cred,
					GSSContext.DEFAULT_LIFETIME);

	context.requestCredDeleg(false);
	
	init(props);
    }
    
    private void init(Map props) throws Exception {
	if (props == null) return;
	
	// "auth", "auth-int", "auth-conf"
	qop = parseQop((String)props.get(QOP));
	allQop = combineMasks(qop);
	
	if (logger.isDebugEnabled()) {
	    logger.debug("client protections: ");
	    for (int i = 0; i < qop.length; i++) {
		logger.debug(" " + qop[i]);
	    }
	}
	
	setQOP(qop);

	// "low", "medium", "high"
	strength = parseStrength((String)props.get(STRENGTH));
	if (logger.isDebugEnabled()) {
	    logger.debug("cipher strengths: ");
	    for (int i = 0; i < strength.length; i++) {
		logger.debug(" " + strength[i]);
	    }
	}
	
	// sslThread.setStrength()?

	logger.debug("client allQop: " + allQop);
    }

    /* this is currently not called */
    private void initContext() {
	logger.debug("client allQop: " + allQop);
	
	if ((allQop&INTEGRITY_ONLY_PROTECTION) != 0) {
	    // Might need integrity
	    logger.debug("client requested integrity protection");
	}
	
	if ((allQop&PRIVACY_PROTECTION) != 0) {
	    // Might need privacy
	    logger.debug("client requested privacy protection");
	}
    }


    /*** All the functions below are copied from SaslImpl.java.
     *** It's part of the J2SE1.4 source code.
     ***/
    
    protected static byte combineMasks(byte[] in) {
	byte answer = 0;
	for (int i = 0; i < in.length; i++) {
	    answer |= in[i];
	}
	return answer;
    }

    protected byte[] parseQop(String qop) throws Exception {
	return parseQop(qop, null, false);
    }

    protected byte[] parseQop(String qop, String[] saveTokens, boolean ignore) 
	throws Exception {
	if (qop == null) {
	    return DEFAULT_QOP;   // default
	}
	
	return parseProp(QOP, qop, QOP_TOKENS, QOP_MASKS, saveTokens, ignore);
    }
    
    protected byte[] parseStrength(String strength) throws Exception {
	if (strength == null) {
	    return DEFAULT_STRENGTH;   // default
	}
	
	return parseProp(STRENGTH, strength, STRENGTH_TOKENS, 
			 STRENGTH_MASKS, null, false);
    }

    protected byte[] parseProp(String propName, String propVal, 
			       String[] vals, byte[] masks, String[] tokens, 
			       boolean ignore) 
	throws Exception {
	
	StringTokenizer parser = new StringTokenizer(propVal, ", \t\n");
	String token;
	byte[] answer = new byte[vals.length];
	int i = 0;
	boolean found;
	
	while (parser.hasMoreTokens() && i < answer.length) {
	    token = parser.nextToken();
	    found = false;
	    for (int j = 0; !found && j < vals.length; j++) {
		if (token.equalsIgnoreCase(vals[j])) {
		    found = true;
		    answer[i++] = masks[j];
		    if (tokens != null) {
			tokens[j] = token;    // save what was parsed
		    }
		} 
	    }
	    if (!found && !ignore) {
		throw new Exception("Invalid token in " + 
				    propName + ": " + propVal);
	    }
	}
	// Initialize rest of array with 0
	for (int j = i; j < answer.length; j++) {
	    answer[j] = 0;
	}
	return answer;
    }

    public static byte findPreferredMask(byte pref, byte[] in) {
	for (int i = 0; i < in.length; i++) {
	    if ((in[i]&pref) != 0) {
		return in[i];
	    }
	}
	return (byte)0;
    }

    public static void intToNetworkByteOrder(int num, 
                                             byte[] buf, 
                                             int start, 
                                             int count) {
        if (count > 4) {
            throw new IllegalArgumentException("Cannot handle more than 4 bytes");
        }
        
        for (int i = count-1; i >= 0; i--) {
            buf[start+i] = (byte)(num & 0xff);
            num >>>= 8;
        }
    }    

    public static int networkByteOrderToInt(byte[] buf,
                                            int start, 
                                            int count) {
        if (count > 4) {
            throw new IllegalArgumentException("Cannot handle more than 4 bytes");
        }
	
        int answer = 0;
        
        for (int i = 0; i < count; i++) {
            answer <<= 8;
            answer |= ((int)buf[start+i] & 0xff);
        }
        return answer;
    }

    /* Not used in the lastest Java SASL API RFC */
    public OutputStream getOutputStream(OutputStream dest) throws IOException {
	if (!isComplete()) throw new IOException("Not completed.");
	if (isNotProtected()) {
	    logger.debug("getOutputStream - current");
	    return dest;
	} else {
	    logger.debug("getOutputStream - new");
	    return new SaslOutputStream(dest, this.context);
	}
    }
  
    /* Not used in the lastest Java SASL API RFC */
    public InputStream getInputStream(InputStream src) throws IOException {
	if (!isComplete()) throw new IOException("Not completed.");
	if (isNotProtected()) {
	    logger.debug("getInputStream - current");
	    return src;
	} else {
	    logger.debug("getInputStream - new");
	    return new SaslInputStream(src, this.context);
	}
    }

    public byte[] exchangeData(byte[] challengeData)
	throws GSSException, Exception {
	logger.debug("exchangeData");
	byte [] token = null;
	if (context.isEstablished()) {
	    token = context.unwrap(challengeData, 0, challengeData.length, null);
	    if (token.length != 4) {
		throw new Exception("Invalid protection buffer");
	    }
	    negotiateProtections(token);
	    token = context.wrap(token, 0, token.length, null);
	    this.completed = true;
	} else {
	    token = context.initSecContext(challengeData, 0, challengeData.length);
	}
	return token;
    }

    // sets it on the context
    public void setQOP(byte[] qop) 
	throws GSSException {

	context.requestConf(true);

	if ((qop[0]&GSIMechanism.INTEGRITY_ONLY_PROTECTION) != 0) {
            // Might need integrity
	    logger.debug("Requested integrity protection");
	    context.requestConf(false);
        }
        
        if ((qop[0]&GSIMechanism.PRIVACY_PROTECTION) != 0) {
            // Might need privacy
	    logger.debug("Requested privacy protection");
	    context.requestConf(true);
        }

	logger.debug("Requested encryption: " + context.getConfState());
    }

    public byte[] negotiateProtections(byte [] sf) 
	throws Exception {

	logger.debug("Server protections: " + sf[0]);

	byte selectedQop = GSIMechanism.findPreferredMask(sf[0], qop);

	if (selectedQop == 0) {
	    throw new Exception("No common protection layer between client and server");
	}

	if ((selectedQop&GSIMechanism.PRIVACY_PROTECTION) != 0) {
	    this.privacy = true;
	    this.integrity = true;
	} else if ((selectedQop&GSIMechanism.INTEGRITY_ONLY_PROTECTION) != 0) {
	    this.privacy = false;
	    this.integrity = true;
	} 

	// 2nd-4th octets specifies maximum buffer size expected by
	// server (in network byte order)
	int srvMaxBufSize = GSIMechanism.networkByteOrderToInt(sf, 1, 3);
	    
	// Determine the max send buffer size based on what the
	// server is able to receive and our specified max
	this.sendMaxBufSize = (this.sendMaxBufSize == 0) ? srvMaxBufSize :
	    Math.min(this.sendMaxBufSize, srvMaxBufSize);
	
	logger.debug("client max recv size: " + recvMaxBufSize);
	logger.debug("server max recv size: " + srvMaxBufSize);
	    
	sf[0] = selectedQop;

	logger.debug("Client selected protection: " + selectedQop);
	logger.debug("Privacy: " + this.privacy);
	logger.debug("Integrity: " + this.integrity);

	GSIMechanism.intToNetworkByteOrder(recvMaxBufSize, sf, 1, 3);

	return sf;
    }

    public boolean isPrivacyQop() {
	return this.privacy;
    }

    public boolean isIntegrityQop() {
	return this.integrity;
    }

    public boolean isNotProtected() {
	return (!this.privacy && !this.integrity);
    }

    public int getSendMaxBufSize() {
	return this.sendMaxBufSize;
    }
    
    public int getRecvMaxBufSize() {
	return this.recvMaxBufSize;
    }

}
