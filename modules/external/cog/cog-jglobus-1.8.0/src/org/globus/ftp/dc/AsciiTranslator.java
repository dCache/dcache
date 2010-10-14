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
package org.globus.ftp.dc;

import java.io.ByteArrayOutputStream;

import org.globus.ftp.Buffer;

public class AsciiTranslator { 

    public static final byte[] CRLF = {'\r', '\n'};

    protected boolean possibleBreak = false;
    
    /* enables checking for \r\n */
    protected boolean rnSep;
    /* enables checking for \n */
    protected boolean nSep;
    
    protected byte[] lineSep;

    protected static byte[] systemLineSep;

    static {
	systemLineSep = System.getProperty("line.separator").getBytes();
    }

    /**
     * Output tokens with system specific line separators
     */
    public AsciiTranslator(boolean rnSep, 
			   boolean nSep) {
	this(rnSep, nSep, systemLineSep);
    }

    public AsciiTranslator(boolean rnSep, 
			   boolean nSep,
			   byte[] lineSeparator) {
	this.rnSep = rnSep;
	this.nSep = nSep;
	this.lineSep = lineSeparator;
    }

    public Buffer translate(Buffer buffer) {
	// TODO: This can be optimized if destination line separator
	// is the same
	byte[] buf = buffer.getBuffer();
	int len = buffer.getLength();
	int bufLastPos = 0;
	
	ByteArrayOutputStream byteArray = new ByteArrayOutputStream(len);

	if (possibleBreak) {
	    if (len > 0 && buf[0] == '\n') {
		byteArray.write(lineSep, 0, lineSep.length);
		bufLastPos = 1;
	    } else {
		byteArray.write('\r');
	    }
	    possibleBreak = false;
	}

	byte ch;
	for (int i=bufLastPos;i<len;i++) {
	    ch = buf[i];
	    if (rnSep && ch == '\r') {
		if (i+1 == len) {
		    // TODO: must test this condition but it might be rare
		    // append everything to the buffer except last byte
		    byteArray.write(buf, bufLastPos, i - bufLastPos);
		    bufLastPos = len;
		    possibleBreak = true;
		    break; 
		}

		if (buf[i+1] == '\n') {
		    i++;
		    byteArray.write(buf, bufLastPos, i - 1 - bufLastPos);
		    byteArray.write(lineSep, 0, lineSep.length);
		    bufLastPos = ++i;
		}
	    } else if (nSep && ch == '\n') {
		byteArray.write(buf, bufLastPos, i - bufLastPos);
		byteArray.write(lineSep, 0, lineSep.length);
		bufLastPos = i+1;
	    }
	}

	if (bufLastPos < len) {
	    byteArray.write(buf, bufLastPos, len - bufLastPos);
	}
	
	byte [] newBuf = byteArray.toByteArray();
	return new Buffer(newBuf, newBuf.length, buffer.getOffset());
    }
    
}

