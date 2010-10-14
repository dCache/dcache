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
package org.globus.util.http;

import java.io.InputStream;
import java.io.IOException;
import java.io.FilterInputStream;

public class LineReader extends FilterInputStream {
    
    private static final int MAX_LEN = 16* 1024;

    protected int _charsRead    = 0;
    
    public LineReader(InputStream is) {
	super(is);
    }

    public InputStream getInputStream() {
	return in;
    }

    public int getCharsRead() {
	return _charsRead;
    }
    
    public String readLine()
	throws IOException {
	return readLine(in);
    }
    
    /**
     * Read a line of text from the given Stream and return it
     * as a String.  Assumes lines end in CRLF.
     * @param in a connected stream which contains the entire
     * message being sen.
     * @exception IOException if a connection fails or abnormal connection
     * termination.
     * @return the next line read from the stream.
     */
    protected String readLine(InputStream in) 
	throws IOException {
	StringBuffer buf = new StringBuffer();
	int c, length = 0;
	
	while(true) {
	    c = in.read();
	    if (c == -1 || c == '\n' || length > MAX_LEN) {
		_charsRead++;
		break;
	    } else if (c == '\r') {
		in.read();
		_charsRead+=2;
		break;
	    } else {
		buf.append((char)c);
		length++;
	    }
	}
	_charsRead += length;
	return buf.toString();
    }
}
