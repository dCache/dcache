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
package org.globus.net;

import java.net.URL;
import java.net.URLConnection;

import org.globus.gsi.GSIConstants;
import org.globus.gsi.gssapi.auth.Authorization;
import org.globus.gsi.gssapi.auth.GSSAuthorization;

import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSName;
import org.ietf.jgss.GSSException;

public abstract class GSIURLConnection extends URLConnection {

    public static final String GSS_MODE_PROPERTY = 
        "gssMode";

    protected GSSCredential credentials;
    protected Authorization authorization;
    protected int delegationType;
    protected Integer gssMode;

    /**
     * Subclasses must overwrite.
     */
    protected GSIURLConnection(URL url) {
	super(url);
	this.delegationType = GSIConstants.DELEGATION_NONE;
	this.authorization = null; // no authorization?
    }

    public abstract void disconnect();

    public void setGSSMode(Integer mode) {
        this.gssMode = mode;
    }
    
    public Integer getGSSMode() {
        return this.gssMode;
    }

    public void setCredentials(GSSCredential credentials) {
        this.credentials = credentials;
    }

    public GSSCredential getCredentials() {
        return credentials;
    }

    public void setAuthorization(Authorization auth) {
        authorization = auth;
    }

    public Authorization getAuthorization() {
        return authorization;
    }
    
    public void setDelegationType(int delegationType) {
	this.delegationType = delegationType;
    }

    public int getDelegationType() {
	return delegationType;
    }

    protected GSSName getExpectedName() throws GSSException {
        if (this.authorization instanceof GSSAuthorization) {
            GSSAuthorization auth = (GSSAuthorization)this.authorization;
            return auth.getExpectedName(this.credentials,
                                        this.url.getHost());
        } else {
            return null;
        }
    }

    public void setRequestProperty(String key, String value) {
        if (key.equals(GSS_MODE_PROPERTY)) {
            if (value.equals("ssl")) {
                setGSSMode(GSIConstants.MODE_SSL);
            } else if (value.equals("gsi")) {
                setGSSMode(GSIConstants.MODE_GSI);
            } else {
                setGSSMode(null);
            }
        } else {
            super.setRequestProperty(key, value);
        }
    }
    
    
}
