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
package org.globus.myproxy;

/**
 * A generic class for representing basic parameters
 * needed for all MyProxy operations.
 */
public abstract class Params
    implements MyProxyConstants {

    private int command;

    protected String username;
    protected String passphrase;
    /** Defaults to DEFAULT_LIFETIME (12 hours). */
    protected int lifetime = DEFAULT_LIFETIME;
    
    public Params(int command) {
        setCommand(command);
    }

    public Params(int command,
                  String username,
                  String passphrase) {
        setCommand(command);
        setUserName(username);
        setPassphrase(passphrase);
    }
    
    protected void setCommand(int command) {
        this.command = command;
    }

    public void setUserName(String username) {
        this.username = username;
    }
    
    public String getUserName() {
        return this.username;
    }

    public void setPassphrase(String passphrase) {
        checkPassphrase(passphrase);
        this.passphrase = passphrase;
    }

    public String getPassphrase() {
        return this.passphrase;
    }

    public void setLifetime(int seconds) {
        this.lifetime = seconds;
    }
        
    public int getLifetime() {
        return this.lifetime;
    }

    protected void checkPassphrase(String passphrase) {
        if (passphrase == null) {
            throw new IllegalArgumentException("Password is not specified");
        }
        if (passphrase.length() < MIN_PASSWORD_LENGTH) {
            throw new IllegalArgumentException("Password must be at least " +
                                               MIN_PASSWORD_LENGTH + 
                                               " characters long");
        }
    }

    public String makeRequest() {
        return makeRequest(true);
    }

    /**
     * Serializes the parameters into a MyProxy request.
     * Subclasses should overwrite this function and
     * append the custom parameters to the output of
     * this function.
     */
    protected String makeRequest(boolean includePassword) {
        StringBuffer buf = new StringBuffer();
        buf.append(VERSION).append(CRLF);
        buf.append(COMMAND).append(String.valueOf(command)).append(CRLF);
        buf.append(USERNAME).append(this.username).append(CRLF);

	String pwd = getPassphrase();
        buf.append(PASSPHRASE);
        if (includePassword) {
	    if (pwd != null) {
		buf.append(pwd);
	    }
        } else {
            for (int i=0;pwd != null && i<pwd.length();i++) {
                buf.append('*');
            }
        }
        buf.append(CRLF);
        buf.append(LIFETIME).append(String.valueOf(lifetime)).append(CRLF);

        return buf.toString();
    }
    
    protected void add(StringBuffer buf, String prefix, String value) {
        if (value == null) {
            return;
        }
        buf.append(prefix).append(value).append(CRLF);
    }

    public String toString() {
        return makeRequest(false);
    }
    
}
