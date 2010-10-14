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

import java.io.OutputStream;
import java.io.IOException;
import java.io.FileOutputStream;

public class GlobusFileOutputStream extends GlobusOutputStream {
    
    private OutputStream output;
    
    public GlobusFileOutputStream(String file, boolean append) 
	throws IOException {
	output = new FileOutputStream(file, append);
    }

    public void abort() {
	try {
	    output.close();
	} catch(Exception e) {}
    }

    public void close() 
	throws IOException {
	output.close();
    }

    public void write(byte [] msg) 
	throws IOException {
	output.write(msg);
    }

    public void write(byte [] msg, int from, int length) 
	throws IOException {
	output.write(msg, from, length);
    }

    public void write(int b)
	throws IOException {
	output.write(b);
    }

    public void flush() 
	throws IOException {
	output.flush();
    }
    
}






