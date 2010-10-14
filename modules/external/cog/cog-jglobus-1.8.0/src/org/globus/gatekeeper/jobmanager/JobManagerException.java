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
package org.globus.gatekeeper.jobmanager;

import org.globus.gatekeeper.ServiceException;
import org.globus.gram.internal.GRAMProtocolErrorConstants;

import org.globus.gram.GramException;

public class JobManagerException extends ServiceException implements GRAMProtocolErrorConstants {

    private int _errorCode = -1;

    public JobManagerException() {
    }
    
    public JobManagerException(int errorCode) {
	super(GramException.getMessage(errorCode));
	_errorCode = errorCode;
    }

    public JobManagerException(int errorCode, String msg, Throwable ex) {
	super(msg, ex);
	_errorCode = errorCode;
    }

    public JobManagerException(int errorCode, String msg) {
	super(msg);
	_errorCode = errorCode;
    }
    
    public JobManagerException(int errorCode, Throwable ex) {
	this(errorCode, null, ex);
    }

    public JobManagerException(String msg) {
	super(msg);
    }

    public int getErrorCode() {
	return _errorCode;
    }
    
}
