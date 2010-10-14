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
package org.globus.io.streams;

import java.io.IOException;
import java.io.FileInputStream;
import java.io.File;

public class GlobusFileInputStream extends GlobusInputStream {
    
    private FileInputStream input;
    private long size = -1;
    
    public GlobusFileInputStream(String file) throws IOException {
	File f = new File(file);
	input = new FileInputStream(f);
	size = f.length();
    }

    public long getSize() {
	return size;
    }

    public void abort() {
	try {
	    input.close();
	} catch(Exception e) {}
    }

    // standard InputStream methods

    public void close() 
	throws IOException {
	input.close();
    }
    
    public int read(byte [] msg) 
	throws IOException {
	return input.read(msg);
    }
    
    public int read(byte [] buf, int off, int len) 
	throws IOException {
	return input.read(buf, off, len);
    }
    
    public int read()
        throws IOException {
        return input.read();
    }
    
    public int available()
        throws IOException {
        return input.available();
    }
    
}
