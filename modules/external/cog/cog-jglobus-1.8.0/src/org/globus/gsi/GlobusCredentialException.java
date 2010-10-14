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
package org.globus.gsi;

import java.text.MessageFormat;
import java.util.ResourceBundle;
import java.util.MissingResourceException;

import org.globus.common.ChainedException;

/**
 * Encapsulates the exceptions caused
 * by various errors in/problems with Globus proxies.
 */
public class GlobusCredentialException extends ChainedException {
    
    public static final int FAILURE = -1;
    public static final int EXPIRED = 1;
    public static final int DEFECTIVE = 2;
    public static final int IO_ERROR = 3;
    public static final int SEC_ERROR = 3;

    private static ResourceBundle resources;
    
    static {
	try {
	    resources = ResourceBundle.getBundle("org.globus.gsi.errors");
	} catch (MissingResourceException e) {
	    throw new RuntimeException(e.getMessage());
	}
    }

    private int errorCode = FAILURE;
    
    public GlobusCredentialException(int errorCode,
				     String msgId,
				     Throwable root) {
	this(errorCode, msgId, null, root);
    }

    public GlobusCredentialException(int errorCode,
				     String msgId,
				     Object [] args) {
	this(errorCode, msgId, args, null);
    }
    
    public GlobusCredentialException(int errorCode,
				     String msgId,
				     Object [] args,
				     Throwable root) {
	super(getMessage(msgId, args), root);
	this.errorCode = errorCode;
    }
    
    public int getErrorCode() {
	return this.errorCode;
    }

    private static String getMessage(String msgId, Object[] args) {
	try {
	    return MessageFormat.format(resources.getString(msgId), args);
	} catch (MissingResourceException e) {
	    //msg = "No msg text defined for '" + key + "'";
	    throw new RuntimeException("bad" + msgId);
	}
    }

}
