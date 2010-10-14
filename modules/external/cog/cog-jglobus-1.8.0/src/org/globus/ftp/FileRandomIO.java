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
import java.io.RandomAccessFile;

/**
   Thread safe reference implementation of DataSink and DataSource.
   Implements reading and writing data to a local file. 
   <b>Note: Does not work with {@link Session#MODE_STREAM Session.STREAM}
   transfer mode, only with {@link GridFTPSession#MODE_EBLOCK 
   GridFTPSession.EBLOCK} mode.</b>
 */
public class FileRandomIO
    implements DataSink, DataSource {

    public static final int DEFAULT_BUFFER_SIZE = 16384;

    protected int bufferSize;
    protected RandomAccessFile file;
    protected long offset;
    
    /**
       Behave like FileRandomIO(file, DEFAULT_BUFFER_SIZE)
       @param file local file that will be be used as data source or
       destination
     */
    public FileRandomIO(RandomAccessFile file) {
	this(file, DEFAULT_BUFFER_SIZE);
    }

    /**
       @param file local file that will be be used as data source or
       destination
       @param bufferSize size of the buffer returned during single
       read operation
     */
    public FileRandomIO(RandomAccessFile file, int bufferSize) {
	this.file = file;
	this.bufferSize = bufferSize;
    }

    public synchronized void write(Buffer buffer)
	throws IOException {
	long bufOffset = buffer.getOffset();

	if (bufOffset == -1) {
	    if (file.getFilePointer() != this.offset) {
		throw new IOException("Invalid offset: " + bufOffset);
	    }
	} else {
	    file.seek(bufOffset);
	}

	file.write(buffer.getBuffer(), 0, buffer.getLength());
	this.offset += buffer.getLength();
    }

    /**
       In this implementation, each read() returns data sequentially.
     */
    public synchronized Buffer read()
	throws IOException {
	long offset = file.getFilePointer();
	byte [] buf = new byte[bufferSize];
	int read = file.read(buf);
	if (read == -1) {
	    return null;
	} else {
	    return new Buffer(buf, read, offset);
	}
    }
    
    /**
       Closes the underlying file
     */
    public synchronized void close()
	throws IOException {
	file.close();
    }
    
    
    public long totalSize() 
	throws IOException {
        return file.length();
    }
}
