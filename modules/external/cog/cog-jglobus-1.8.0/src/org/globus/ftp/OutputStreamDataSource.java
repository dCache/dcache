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
package org.globus.ftp;

import java.io.IOException;
import java.io.OutputStream;
import java.io.EOFException;
import java.io.InterruptedIOException;

import org.globus.util.CircularBuffer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class OutputStreamDataSource implements DataSource {

    protected CircularBuffer buffers = null;
    private DataOutputStream out;
    
    public OutputStreamDataSource(int size) {
	this.buffers = new CircularBuffer(5);
	this.out = new DataOutputStream(size);
    }
    
    // returns null if EOF
    public Buffer read() 
	throws IOException {
	try {
	    return (Buffer)this.buffers.get();
	} catch (InterruptedException e) {
	    // that should not happen
	    throw new InterruptedIOException();
	}
    }

    public void close() 
	throws IOException {
	this.buffers.interruptBoth();
	this.out.setClosed();
    }
    
    public OutputStream getOutputStream() {
	return this.out;
    }
    
    public long totalSize()
	throws  IOException {
        return -1;
    }

    // if write() is blocked, it will not be unblocked by close()
    class DataOutputStream extends OutputStream {

	protected byte[] buff;
	protected int index;
	private boolean closed = false;

	public DataOutputStream(int size) {
	    this.buff = new byte[size];
	    this.index = 0;
	}

	public void write(byte[] data)
	    throws IOException {
	    write(data, 0, data.length);
	}
	
	public synchronized void write(byte[] data, int off, int len)
	    throws IOException {
	    if (isClosed()) {
		throw new EOFException();
	    }
	    int max;
	    while (len > 0) {
		if (this.index + len > this.buff.length) {
		    max = (this.buff.length - this.index);
		    System.arraycopy(data, off, this.buff, this.index, max);
		    this.index += max;
		    flush();
		    len -= max;
		    off += max;
		} else {
		    System.arraycopy(data, off, this.buff, this.index, len);
		    this.index += len;
		    break;
		}
	    }
	}
	
	public synchronized void write(int b) 
	    throws IOException {
	    if (isClosed()) {
		throw new EOFException();
	    }
	    if (this.index == this.buff.length) {
		flush();
	    }
	
	    buff[index++] = (byte)b;
	}
	
	public synchronized void flush()
	    throws IOException {
	    if (this.index == 0) {
		return;
	    }
	    byte [] data = new byte[this.index];
	    System.arraycopy(this.buff, 0, data, 0, this.index);
	    Buffer b = new Buffer(data, this.index);
	    try {
		if (!buffers.put(b)) {
		    throw new EOFException();
		}
	    } catch (InterruptedException e) {
		// that should never happen
		throw new InterruptedIOException();
	    }
	    this.index = 0;
	}

	public void close()
	    throws IOException {
	    flush();
	    // will let get run until it returns null
	    // and will throe EOFException on next put call
	    // but existing put calls will not be interrupted
	    setClosed();
	    buffers.closePut();
	}

	protected synchronized void setClosed() {
	    this.closed = true;
	}
	
	private synchronized boolean isClosed() {
	    return this.closed;
	}

    }
}
