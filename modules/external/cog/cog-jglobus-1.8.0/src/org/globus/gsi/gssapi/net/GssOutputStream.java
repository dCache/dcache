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

import java.io.OutputStream;
import java.io.IOException;

import org.globus.common.ChainedIOException;

import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public abstract class GssOutputStream extends OutputStream {

    private static Log logger = 
	LogFactory.getLog(GssOutputStream.class.getName());

    protected OutputStream out;
    protected GSSContext context;

    protected boolean autoFlush = false;

    protected byte [] buff;
    protected int index;

    public GssOutputStream(OutputStream out, GSSContext context) {
	this(out, context, 16384);
    }

    public GssOutputStream(OutputStream out, GSSContext context, int size) {
	this.out = out;
	this.context = context;
	this.buff = new byte[size];
	this.index = 0;
    }

    public void setAutoFlush(boolean autoFlush) {
        this.autoFlush = autoFlush;
    }

    public boolean getAutoFlush() {
        return this.autoFlush;
    }

    public void write(int b)
        throws IOException {
	if (this.index == this.buff.length) {
	    flushData();
	}
	
        buff[index++] = (byte)b;

        if (this.autoFlush) {
            flushData();
        }
    }
    
    public void write(byte[] data)
	throws IOException {
	write(data, 0, data.length);
    }
    
    public void write(byte [] data, int off, int len) 
	throws IOException {
	int max;
	while (len > 0) {
	    if (this.index + len > this.buff.length) {
		max = (this.buff.length - this.index);
		System.arraycopy(data, off, this.buff, this.index, max);
		this.index += max;
		flushData();
		len -= max;
		off += max;
	    } else {
		System.arraycopy(data, off, this.buff, this.index, len);
		this.index += len;
                if (this.autoFlush) {
                    flushData();
                }
		break;
	    }
	}
    }

    protected byte[] wrap()
	throws IOException {
	try {
	    return context.wrap(this.buff, 0, this.index, null);
	} catch (GSSException e) {
	    throw new ChainedIOException("wrap failed", e);
	}
    }

    public abstract void flush() 
	throws IOException;

    private void flushData()
	throws IOException {
	flush();
	this.index = 0;
    }
    
    public void close()
	throws IOException {
	logger.debug("close");
	flushData();
	this.out.close();
    }

}
