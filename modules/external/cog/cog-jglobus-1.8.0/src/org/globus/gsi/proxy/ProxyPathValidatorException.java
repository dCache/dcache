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
package org.globus.gsi.proxy;

import java.util.Date;
import java.text.DateFormat;
import java.util.TimeZone;
import java.text.SimpleDateFormat;

import java.security.cert.X509Certificate;

import org.globus.common.ChainedGeneralSecurityException;

/**
 */
public class ProxyPathValidatorException 
    extends ChainedGeneralSecurityException {
    
    public static final int FAILURE = -1;

    // proxy constraints violation
    public static final int PROXY_VIOLATION = 1;

    // unsupported critical extensions
    public static final int UNSUPPORTED_EXTENSION = 2;

    // proxy or CA path length exceeded
    public static final int PATH_LENGTH_EXCEEDED = 3;

    // unknown CA
    public static final int UNKNOWN_CA = 4;

    // unknown proxy policy
    public static final int UNKNOWN_POLICY = 5;

    // cert revoked
    public static final int REVOKED = 6;

    // limited proxy not accepted
    public static final int LIMITED_PROXY_ERROR = 7;

    // CRL expired
    public static final int EXPIRED_CRL = 8;

    // signing policy not found
    public static final int NO_SIGNING_POLICY_FILE = 9;

    // no relevant signing policy in the file
    public static final int NO_SIGNING_POLICY = 10;

    // DN violates signing policy
    public static final int SIGNING_POLICY_VIOLATION = 11;

    private X509Certificate cert;

    private int errorCode = FAILURE;
    
    public ProxyPathValidatorException(int errorCode) {
	this(errorCode, null);
    }

    public ProxyPathValidatorException(int errorCode,
				       Throwable root) {
	this(errorCode, "", root);
    }

    public ProxyPathValidatorException(int errorCode,
				       String msg,
				       Throwable root) {
	super(msg, root);
	this.errorCode = errorCode;
    }

    public ProxyPathValidatorException(int errorCode,
				       X509Certificate cert,
				       String msg) {
	super(msg, null);
	this.errorCode = errorCode;
	this.cert = cert;
    }
    
    public int getErrorCode() {
	return this.errorCode;
    }
    

    /**
     * Returns the certificate that was being validated when
     * the exception was thrown.
     *
     * @return the <code>Certificate</code> that was being validated when
     * the exception was thrown (or <code>null</code> if not specified)
     */
    public X509Certificate getCertificate() {
	return this.cert;
    }
    
    public static String getDateAsString(Date date) {
        TimeZone tz = TimeZone.getTimeZone("GMT");
        DateFormat df = new SimpleDateFormat("MMM dd HH:mm:ss yyyy z");
        df.setTimeZone(tz);
        return df.format(date);
    }
}
