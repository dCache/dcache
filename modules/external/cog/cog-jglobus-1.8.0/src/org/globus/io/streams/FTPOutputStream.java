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

import org.globus.ftp.FTPClient;
import org.globus.ftp.Session;
import org.globus.ftp.OutputStreamDataSource;
import org.globus.ftp.vanilla.TransferState;
import org.globus.ftp.exception.FTPException;
import org.globus.common.ChainedIOException;

public class FTPOutputStream extends GlobusOutputStream {
    
    protected OutputStream output;
    protected FTPClient ftp;
    protected TransferState state;

    protected FTPOutputStream() {
    }

    public FTPOutputStream(String host, 
			   int port, 
			   String user, 
			   String pwd,
			   String file,
			   boolean append) 
	throws IOException, FTPException {
	this(host, port, user, pwd, file, append,
	     true, Session.TYPE_IMAGE);
    }

    public FTPOutputStream(String host, 
			   int port, 
			   String user, 
			   String pwd,
			   String file,
			   boolean append,
			   boolean passive,
			   int type) 
	throws IOException, FTPException {
	this.ftp = new FTPClient(host, port);
	this.ftp.authorize(user, pwd);
	put(passive, type, file, append);
    }

    public void abort() {
	if (this.output != null) {
	    try {
		this.output.close();
	    } catch(Exception e) {}
	}
	try {
	    this.ftp.close();
	} catch (IOException e) {
	} catch (FTPException e) {
	}
    }

    public void close() 
	throws IOException {

	if (this.output != null) {
	    try {
		this.output.close();
	    } catch(Exception e) {}
	}

	try {
	    if (this.state != null) {
		this.state.waitForEnd();
	    }
	} catch (FTPException e) {
	    throw new ChainedIOException("close failed.", e);
	} finally {
	    try {
		this.ftp.close();
	    } catch (FTPException ee) {
		throw new ChainedIOException("close failed.", ee);
	    }
	}
    }
    
    protected void put(boolean passive,
		       int type,
		       String remoteFile,
		       boolean append)
	throws IOException, FTPException {

	OutputStreamDataSource source = null;

	try {
	    this.ftp.setType(type);

	    if (passive) {
		this.ftp.setPassive();
		this.ftp.setLocalActive();
	    } else {
		this.ftp.setLocalPassive();
		this.ftp.setActive();
	    }
	    
	    source = new OutputStreamDataSource(2048);
	    
	    this.state = this.ftp.asynchPut(remoteFile,
					    source,
					    null,
					    append);
	    
	    this.state.waitForStart();

	    this.output = source.getOutputStream();
	} catch (FTPException e) {
	    if (source != null) {
		source.close();
	    }
	    close();
	    throw e;
	}
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
