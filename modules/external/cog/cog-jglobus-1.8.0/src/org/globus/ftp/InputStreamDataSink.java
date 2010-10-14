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
import java.io.InputStream;
import java.io.EOFException;
import java.io.InterruptedIOException;

import org.globus.util.CircularBuffer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

// if write() blocked, it will not be unblocked by close
public class InputStreamDataSink implements DataSink {

    protected CircularBuffer buffers = null;
    private DataInputStream in;
    private boolean closed = false;

    public InputStreamDataSink() {
	this.buffers = new CircularBuffer(5);
	this.in = new DataInputStream();
    }

    public void write(Buffer buffer) 
	throws IOException {
	if (isClosed()) {
	    throw new EOFException();
	}
	try {
	    if (!buffers.put(buffer)) {
		throw new EOFException();
	    }
	} catch (InterruptedException e) {
	    throw new InterruptedIOException();
	}
    }

    public void close() 
	throws IOException {
	// will let get run until it returns null
	// and will throe EOFException on next put call
	// but existing put calls will not be interrupted
	setClosed();
	this.buffers.closePut();
    }

    private synchronized void setClosed() {
	this.closed = true;
    }

    private synchronized boolean isClosed() {
	return this.closed;
    }

    public InputStream getInputStream() {
	return this.in;
    }

    class DataInputStream extends InputStream {

	protected byte[] buff;
	protected int index;
	protected int length;

	public synchronized int read(byte [] data) 
	    throws IOException {
	    return read(data, 0, data.length);
	}
	
	public synchronized int read(byte[] data, int off, int len)
	    throws IOException {
	    if (!ensureData()) {
		return -1;
	    }
	    int max = (index + len > length) ? length - index : len;
	    System.arraycopy(buff, index, data, off, max);
	    index += max;
	    return max;
	}
	
	public synchronized int read()
	    throws IOException {
	    if (!ensureData()) {
		return -1;
	    }
	    return buff[index++] & 0xff;
	}
	
	public void close()
	    throws IOException {
	    buffers.interruptBoth();
	}

	protected synchronized boolean ensureData() 
	    throws IOException {
	    if (buffers.isGetInterrupted()) {
		return false;
	    }
	    if (this.length == this.index) {
		try {
		    Buffer buf = (Buffer)buffers.get();
		    if (buf == null) {
			return false;
		    }
		    this.index = 0;
		    this.length = buf.getLength();
		    this.buff = buf.getBuffer();
		} catch (InterruptedException e) {
		    // that should never happen
		    throw new InterruptedIOException();
		}
	    }
	    return true;
	}
	
    }
}
