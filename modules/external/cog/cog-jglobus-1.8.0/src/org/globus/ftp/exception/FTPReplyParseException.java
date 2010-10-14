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
   Indicates that the reply received from server failed to parse.
 */
public class FTPReplyParseException extends FTPException {

    //public static final int UNSPECIFIED = 0;
    public static final int STRING_TOO_SHORT = 1;
    public static final int FIRST_3_CHARS = 2;
    public static final int UNEXPECTED_4TH_CHAR = 3;
    public static final int MESSAGE_UNPARSABLE = 4;
    private static String[] codeExplained;
    static {
	codeExplained =new String[]{
	    "Unspecified exception.",
	    "Reply string too short.",
	    "First 3 characters are not digits.",
	    "Unexpected 4th character.",
	    "Reply message unparsable"
	};
    }

    public String getCodeExplanation(int code) {

	if (codeExplained.length > code)
	    return codeExplained[code];
	else return "";
    }

    public FTPReplyParseException(int code) {
	super(code);
    }

    public FTPReplyParseException(int code, String message) {
	super(code, message);
    }

}



