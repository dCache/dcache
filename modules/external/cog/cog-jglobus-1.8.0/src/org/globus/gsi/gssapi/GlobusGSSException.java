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
package org.globus.gsi.gssapi;

import java.text.MessageFormat;
import java.util.ResourceBundle;
import java.util.MissingResourceException;

import org.ietf.jgss.GSSException;

public class GlobusGSSException extends GSSException {

    public static final int
	PROXY_VIOLATION = 5,
	BAD_ARGUMENT = 7,
	BAD_NAME = 25,
	CREDENTIAL_ERROR = 27,
	TOKEN_FAIL = 29,
	DELEGATION_ERROR = 30,
	BAD_MIC = 33,
	UNKNOWN_OPTION = 37;

    public static final int
	BAD_OPTION_TYPE = 100,
	BAD_OPTION = 101,
	UNKNOWN = 102;

    private static ResourceBundle resources;
    
    static {
	try {
	    resources = ResourceBundle.getBundle("org.globus.gsi.gssapi.errors");
	} catch (MissingResourceException e) {
	    throw new RuntimeException(e.getMessage());
	}
    }
    
    private Throwable exception;

    public GlobusGSSException(int majorCode, 
			      Throwable exception) {
	super(majorCode);
	this.exception = exception;
    }

    public GlobusGSSException(int majorCode, 
			      int minorCode,
			      String minorString,
			      Throwable exception) {
	super(majorCode, minorCode, minorString);
	this.exception = exception;
    }

    public GlobusGSSException(int majorCode,
			      int minorCode,
			      String key) {
	this(majorCode, minorCode, key, (Object[])null);
    }
    
    public GlobusGSSException(int majorCode,
			      int minorCode,
			      String key,
			      Object [] args) {
	super(majorCode);
	
	String msg = null;
	try {
	    msg = MessageFormat.format(resources.getString(key), args);
	} catch (MissingResourceException e) {
	    //msg = "No msg text defined for '" + key + "'";
	    throw new RuntimeException("bad" + key);
	}
	
	setMinor(minorCode, msg);
	this.exception = null;
    }

    
    /**
     * Prints this exception's stack trace to <tt>System.err</tt>.
     * If this exception has a root exception; the stack trace of the
     * root exception is printed to <tt>System.err</tt> instead.
     */
    public void printStackTrace() {
        printStackTrace( System.err );
    }

    /**
     * Prints this exception's stack trace to a print stream.
     * If this exception has a root exception; the stack trace of the
     * root exception is printed to the print stream instead.
     * @param ps The non-null print stream to which to print.
     */
    public void printStackTrace(java.io.PrintStream ps) {
        if ( exception != null ) {
            String superString = getLocalMessage();
            synchronized ( ps ) {
                ps.print(superString);
                ps.print((superString.endsWith(".") ? 
                          " Caused by " : ". Caused by "));
                exception.printStackTrace( ps );
            }
        } else {
            super.printStackTrace( ps );
        }
    }
    
    /**
     * Prints this exception's stack trace to a print writer.
     * If this exception has a root exception; the stack trace of the
     * root exception is printed to the print writer instead.
     * @param pw The non-null print writer to which to print.
     */
    public void printStackTrace(java.io.PrintWriter pw) {
        if ( exception != null ) {
            String superString = getLocalMessage();
            synchronized (pw) {
                pw.print(superString);
                pw.print((superString.endsWith(".") ? 
                          " Caused by " : ". Caused by "));
                exception.printStackTrace( pw );
            }
        } else {
            super.printStackTrace( pw );
        }
    }

    public String getMessage() {
        String answer = super.getMessage();
        if (exception != null && exception != this) {
            String msg = exception.getMessage();
            if (msg == null) {
                msg = exception.getClass().getName();
            }
            answer += " [Caused by: " + msg + "]";
        }
        return answer;
    }
    
    private String getLocalMessage() {
        String message = super.getMessage();
        return (message == null) ? getClass().getName() : message;
    }

}
