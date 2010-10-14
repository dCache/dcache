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
package org.globus.ftp.exception;

/** 
    thrown by PerformanceMarker, mostly during construction.
**/
public class PerfMarkerException extends FTPException {

    /**
       
     **/
    public static final int NO_SUCH_PARAMETER = 1;

    private static String[] codeExplained;
    static {
	codeExplained = new String[]
	{"Unspecified category.",
	 "Marker does not contain the requested parameter."
	};
    }

    public String getCodeExplanation(int code) {
	if (codeExplained.length > code)
	    return codeExplained[code];
	else return "";
    }


    protected int code = UNSPECIFIED;

    //this message is not just explanation of the code.
    //it is a custom message informing of particular 
    //conditions of the error.
    protected String customMessage;

    public PerfMarkerException(int code, String message) {
	super(code, message);
	customMessage = message;
    }

    public PerfMarkerException(int code) {
	super(code);
    }
 
}
