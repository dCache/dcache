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
package org.globus.ftp.vanilla;

import org.globus.ftp.exception.FTPReplyParseException;

import java.io.Serializable;
import java.io.EOFException;
import java.io.IOException;
import java.io.BufferedReader;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

// FTPReplyParseException - move line to exception
// internationalize exceptions

/**
 * <p>
 * Represents the FTP reply. 
 * </p>
 */
public class Reply 
    implements Serializable {
    
    private static Log logger = LogFactory.getLog(Reply.class.getName());

    public static final int POSITIVE_PRELIMINARY             = 1;
    public static final int POSITIVE_COMPLETION              = 2;
    public static final int POSITIVE_INTERMEDIATE            = 3;
    public static final int TRANSIENT_NEGATIVE_COMPLETION    = 4;
    public static final int PERMANENT_NEGATIVE_COMPLETION    = 5;

    //minimum length of 1st line:
    //message is defined as 3 chars + <sp> + <text> + <CRLF>
    //so if text is empty, minimum 1st line length = 4
    private static final int MIN_FIRST_LEN = 4;

    // instance members
    protected String message;
    protected int code;
    protected int category;
    protected boolean isMultiline;

    // for subclassing
    protected Reply() {};

    /**
     * @throws EOFException on end of stream
     * @throws IOException on I/O problem
     * @throws FTPReplyParseException if cannot parse
     **/
    public Reply(BufferedReader input) 
	throws FTPReplyParseException, 
	       EOFException,
	       IOException {
	logger.debug( "read 1st line");
	String line = input.readLine();
	if (logger.isDebugEnabled()) {
	    logger.debug( "1st line: " + line);
	}

	//end of stream
	if (line == null) {
	    throw new EOFException();
	}

	//for compatibility with GT2.0 wuftp server which is incorrectly inserting \0 between lines
	line = ignoreLeading0(line);
	
	if(line.length() < MIN_FIRST_LEN) {
	    throw new FTPReplyParseException(
					 FTPReplyParseException.STRING_TOO_SHORT,
					 "Minimum 1st line length = " + MIN_FIRST_LEN 
					 + ". Here's the incorrect 1st line ->"
					 + line + "<-");
	}

	// code
	String codeString = line.substring(0,3);

	try {
	    code = Integer.parseInt(codeString);
	} catch (NumberFormatException e) {
	    throw new FTPReplyParseException(
					 FTPReplyParseException.FIRST_3_CHARS,
					 "Here's the incorrect line ->" 
					 + line + "<-" +
					 "and the first 3 chars ->" + codeString + "<-"
					 );
	} 

	// category
	category = line.charAt(0) - '0';
	
	// message
	char char4 = line.charAt(3);

	//do not include 4th char in message
	message = line.substring(4, line.length());

	if (char4 == ' ') {	    

	    //single line reply
	    isMultiline = false;

	} else if (char4 == '-') {

	    //multi - line reply

            isMultiline = true;
	    
	    String lastLineStarts = codeString + ' ';
	    //platform dependent line separator
	    String lineSeparator = System.getProperty("line.separator");
	    if (logger.isDebugEnabled()) { 
		logger.debug(
			     "multiline reply; last line should start with ->" 
			     + lastLineStarts + "<-");
		logger.debug("lenght of line.separator on this OS: " + 
			     lineSeparator.length()); 
	    }
	    StringBuffer buf = new StringBuffer(message);
	    for (;;) {
		logger.debug( "read line");
		line = input.readLine();

		//end of stream
		if (line == null) {
		    throw new EOFException();
		}

		//for compatibility with GT2.0 wuftp server 
		//which is incorrectly inserting \0 between lines
		line = ignoreLeading0(line);
		if (logger.isDebugEnabled()) { 
		    logger.debug( "line : ->" + line + "<-");
		}
		buf.append(lineSeparator).append(line);

		if (line.startsWith(lastLineStarts)) { 
		    logger.debug("end reached");
		    break;
		}
	    }
	    message = buf.toString();

	} else  {
	    throw new FTPReplyParseException(
					     FTPReplyParseException.UNEXPECTED_4TH_CHAR,
					     "Here's the incorrect line ->" 
					     + line + "<-" );
	}
    }

    /**
     * 
     * @return the first digit of the reply code. 
     * 
     */
    public int getCategory() {
	return category;
    }

    /**
     *  @return the reply code
     */
    public int getCode() {
	return code;
    }

    public boolean isMultiline() {
        return isMultiline;
    }

    /**
     * <p>
     * Returns the text that came with the reply, between the leading space and
     * terminating CRLF, excluding the mentioned space and CRLF.
     * </p>
     * <p>
     * If the reply is multi-line, this returns the text between the leading
     * dash &quot;-&quot; and the CRLF following the last line, excluding the mentioned
     * dash and CRLF. Note that lines are separated by the local line separator
     * [as returned by System.getProperty("line.separator")] rather than CRLF.
     * 
     * </p>
     * <p>
     *
     * </p>
     */
    public String getMessage() {
        return message;
    } 

    public static boolean isPositivePreliminary(Reply reply) {
	return (reply.getCategory() == POSITIVE_PRELIMINARY);
    }

    public static boolean isPositiveCompletion(Reply reply) {
	return (reply.getCategory() == POSITIVE_COMPLETION);
    }

    public static boolean isPositiveIntermediate(Reply reply) {
	return (reply.getCategory() == POSITIVE_INTERMEDIATE);
    }

    public static boolean isTransientNegativeCompletion(Reply reply) {
	return (reply.getCategory() == TRANSIENT_NEGATIVE_COMPLETION);
    }

    public static boolean isPermanentNegativeCompletion(Reply reply) {
	return (reply.getCategory() == PERMANENT_NEGATIVE_COMPLETION);
    }


    public String toString() {
        String mult = isMultiline ? "-" : " ";
        return code + mult + message;
    }

    /**
       GT2.0 wuftp server incorrectly inserts \0 between lines. We have to deal with that.
     **/
    protected static String ignoreLeading0(String line)
    {
	if (line.length() > 0 && line.charAt(0) == 0) {
	    logger.debug("WARNING: The first character of the reply is 0. Ignoring the character.");
	    /*
	    logger.debug( "\n\nWARNING:\n In the reply received from the server, the first character's code is 0! I will ignore it but this means the server is not following the protocol. Here's the details: \n first line of the reply ->" + line + "<-");
	    logger.debug( "First 3 chars of reply->" +line.substring(0,3)+"<-"); 
	    logger.debug( "char 0 ->" + line.charAt(0) + "<- code = " + (int)line.charAt(0));
	    logger.debug( "char 1 ->" + line.charAt(1) + "<- code = " + (int)line.charAt(1));
	    logger.debug( "char 2 ->" + line.charAt(2) + "<- code = " + (int)line.charAt(2));
	    logger.debug( "char 3 ->" + line.charAt(3) + "<- code = " + (int)line.charAt(3));
	    */
	    return line.substring(1, line.length());
	}
	return line;
    }

} // end Reply
