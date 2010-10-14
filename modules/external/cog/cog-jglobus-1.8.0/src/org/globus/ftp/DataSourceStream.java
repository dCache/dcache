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

/**
 * Reference implementation of DataSource. It can be used with
 * non-parallel transfers. It cannot be used with Extended Block Mode because
 * it is not thread safe.
 **/
public class DataSourceStream implements DataSource {

    private static final int DEFAULT_BUFFER_SIZE = 16384;

    protected InputStream in;
    protected int bufferSize;
    protected long totalRead = 0;

    public DataSourceStream(InputStream in) {
        this(in, DEFAULT_BUFFER_SIZE);
    }
    
    public DataSourceStream(InputStream in, int bufferSize) {
        this.in = in;
        this.bufferSize = bufferSize;
    }

    public Buffer read()
        throws IOException {
        byte [] buf = new byte[bufferSize];
        int read = in.read(buf);
        if (read == -1) {
            return null;
        } else {
            Buffer result =  new Buffer(buf, read, totalRead);
            totalRead += read;
            return result;
        }
    }
    
    public void close()
        throws IOException {
        in.close();
    }
    
    public long totalSize() {
	    return -1;
    }
}
