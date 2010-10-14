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
package org.globus.gsi.gssapi.net;

import java.io.InputStream;
import java.io.IOException;

import org.globus.common.ChainedIOException;

import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSException;

public abstract class GssInputStream extends InputStream {
  
    protected InputStream in;
    protected GSSContext context;
  
    protected byte [] buff;
    protected int index;

    public GssInputStream(InputStream in, GSSContext context) {
	this.in = in;
	this.context = context;
	this.buff = new byte[0];
	this.index = 0;
    }
  
    protected byte[] unwrap(byte [] msg) 
	throws IOException {
	try {
	    return this.context.unwrap(msg, 0, msg.length, null);
	} catch (GSSException e) {
	    throw new ChainedIOException("unwrap failed", e);
	}
    }
	
    protected abstract void readMsg()
	throws IOException;

    public int read(byte [] data)
	throws IOException {
	return read(data, 0, data.length);
    }

    public int read(byte [] data, int off, int len) 
	throws IOException {
	if (!hasData()) {
	    return -1;
	}

	int max = (index + len > buff.length) ? buff.length - index : len;

	System.arraycopy(buff, index, data, off, max);
	index += max;
	return max;
    }

    public int read() 
	throws IOException {
	if (!hasData()) {
	    return -1;
	}

	return buff[index++] & 0xff;
    }

    protected boolean hasData() 
	throws IOException {
	if (this.buff == null) {
	    return false;
	}
	if (this.buff.length == this.index) {
	    readMsg();
	}
	if (this.buff == null) {
            return false;
        }
	return (this.buff.length != this.index);
    }
    
    /* does not dispose of the context */
    public void close()
	throws IOException {
	this.buff = null;
	in.close();
    }

    public int available()
	throws IOException {
	if (this.buff == null) {
	    return -1;
	}
	int avail = this.buff.length - this.index;
	return (avail == 0) ? in.available() : avail;
    }
    
}
