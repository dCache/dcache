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
package org.globus.ftp.extended;

import java.io.InputStream;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.EOFException;

import org.globus.gsi.gssapi.net.GssInputStream;
import org.globus.util.Base64;

import org.ietf.jgss.GSSContext;

public class GridFTPInputStream extends GssInputStream {
  
    public GridFTPInputStream(InputStream in, GSSContext context) {
	super(new BufferedInputStream(in), context);
    }

    private String readLine() throws IOException {
        int c = this.in.read();
        if (c == -1) {
            return null;
        }

        StringBuffer buf = new StringBuffer();
        buf.append((char)c);
        while( (c = this.in.read()) != -1 ) {
            if (c == '\r') {
		c = this.in.read();
		if (c == '\n' || c == -1) {
		    break;
		} else {
		    throw new IOException("bad format");
		}
            } else {
                buf.append( (char)c);
            }
        }

        return buf.toString();
    }

    public byte[] readHandshakeToken()
	throws IOException {
	String line = readLine();
	if (line == null) { 
	    throw new EOFException();
	}

	if (line.startsWith("335 ADAT=") ||
	    line.startsWith("334 ADAT=") ) {
	    // TODO: this can be optimized
	    return Base64.decode(line.substring(9).getBytes());
	} else if ( line.startsWith("335 more data needed") ) {
	    return new byte[0];
	} else {
	    throw new IOException(handleReply(line));
	}
    }

    private String handleReply(String line) 
	throws IOException {
	line = line.trim();
	if (line.length() > 4 && line.charAt(3) == '-') {
	    String lineSeparator = System.getProperty("line.separator");
	    String lastLineStarts = line.substring(0, 3) + ' ';
	    StringBuffer buf = new StringBuffer();
	    buf.append(line);
	    for (;;) {
		line = readLine();
		if (line == null) { 
		    throw new EOFException();
		}
		line = line.trim();
		buf.append(lineSeparator).append(line);
		if (line.startsWith(lastLineStarts)) { 
		    break;
		}
	    }
	    return buf.toString();
	} else {
	    return line;
	}
    }
  
    protected void readMsg()
	throws IOException {
	String line = readLine();
	if (line == null) { 
	    throw new EOFException();
	}

	if (line.charAt(0) == '6') {
	    this.buff = unwrap(Base64.decode(line.substring(4).getBytes()));
	    this.index = 0;

	    /**
	     * This is a fix for messages that are not
	     * \r\n terminated
	     */
	    byte last = this.buff[this.buff.length-1];
	    if (last == 0) {
		// this is a bug in older gridftp servers
		// line should be terminated with \r\n0
		if (this.buff[this.buff.length-2] != 10) {
		    this.buff[this.buff.length-1] = 10;
		}
	    } else if (last != 10) {
		byte [] newBuff = new byte[this.buff.length+1];
		System.arraycopy(buff, 0, newBuff, 0, this.buff.length);
		newBuff[this.buff.length]=10;
		this.buff = newBuff;
	    }
	} else {
	    throw new IOException(line);
	}
	 
    }

}
