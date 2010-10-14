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
    Indicates data channel problems. Thrown by local server at layer 2.
**/
public class DataChannelException extends FTPException {

    //public static final int UNSPECIFIED = 0;
    public static final int UNDEFINED_SERVER_MODE = 1;
    public static final int BAD_SERVER_MODE = 2;
    private static String[] codeExplained;
    static {
	codeExplained = new String[]
	{"Unspecified category.",
	 "Undefined server mode (active or passive?)",
	 "setPassive() must match store() and setActive() - retrieve() "
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

    public DataChannelException(int code, String message) {
	super(code, message);
	customMessage = message;
    }

    public DataChannelException(int code) {
	super(code);
    }
 
}
