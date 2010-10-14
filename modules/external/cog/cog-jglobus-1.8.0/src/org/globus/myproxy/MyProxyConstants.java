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

public interface MyProxyConstants {

    /** The version string (MYPROXYv2) to send at the start of
     * communication with the MyProxy server. */
    public static final String MYPROXY_PROTOCOL_VERSION = "MYPROXYv2";

    /** MyProxy passwords must be 6 characters or longer. */
    public static final int MIN_PASSWORD_LENGTH         = 6;

    /** By default, put credentials with a policy restricting
        delegated credential lifetime to 12 hours or less, and get 12 hour
        credentials from the server.  */
    public static final int DEFAULT_LIFETIME            = 12 * 3600;

    static final String VERSION    = "VERSION=" + MYPROXY_PROTOCOL_VERSION;
    static final String COMMAND    = "COMMAND=";
    static final String USERNAME   = "USERNAME=";
    static final String PASSPHRASE = "PASSPHRASE=";
    static final String LIFETIME   = "LIFETIME=";
    static final String CRED_NAME  = "CRED_NAME=";
    static final String TRUSTROOTS = "TRUSTED_CERTS=";
    static final String RETRIEVER  = "RETRIEVER=";
    static final String RENEWER    = "RENEWER=";
    static final String CRED_DESC  = "CRED_DESC=";
    static final String NEW_PHRASE = "NEW_PHRASE=";
    static final String CRLF       = "\n";
}
