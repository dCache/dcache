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

/**
   Reference implementation of DataSink. It can be used with non-parallel transfers.
   It cannot be used with Extended Block Mode because it uses implicit assumption 
   that data arrives in correct sequence. It is not thread safe.  
 **/
public class DataSinkStream implements DataSink {

    protected OutputStream out;
    protected boolean autoFlush;
    protected boolean ignoreOffset;
    protected long offset = 0;

    public DataSinkStream(OutputStream out) {
	this(out, false, false);
    }

    public DataSinkStream(OutputStream out,
			  boolean autoFlush,
			  boolean ignoreOffset) {
	this.out = out;
	this.autoFlush = autoFlush;
	this.ignoreOffset = ignoreOffset;
    }

    public void write(Buffer buffer)
	throws IOException {
	long bufOffset = buffer.getOffset();
	if (ignoreOffset ||
	    bufOffset == -1 ||
	    bufOffset == offset) {
	    out.write(buffer.getBuffer(), 0, buffer.getLength());
	    if (autoFlush) {
		out.flush();
	    }
	    offset += buffer.getLength();
	} else {
	    throw new IOException("Random offsets not supported.");
	}
    }
    
    public void close()
	throws IOException {
	out.close();
    }
    
}
