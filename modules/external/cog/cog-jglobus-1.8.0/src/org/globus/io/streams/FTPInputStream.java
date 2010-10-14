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

import java.io.InputStream;
import java.io.IOException;

import org.globus.ftp.FTPClient;
import org.globus.ftp.Session;
import org.globus.ftp.InputStreamDataSink;
import org.globus.ftp.vanilla.TransferState;
import org.globus.ftp.exception.FTPException;
import org.globus.common.ChainedIOException;

public class FTPInputStream extends GlobusInputStream {
    
    protected InputStream input;
    protected FTPClient ftp;
    protected TransferState state;

    protected FTPInputStream() {
    }

    public FTPInputStream(String host, 
			  int port, 
			  String user, 
			  String pwd, 
			  String file)
	throws IOException, FTPException {
	this(host, port, user, pwd, 
	     file, true, Session.TYPE_IMAGE);
    }

    public FTPInputStream(String host, 
			  int port, 
			  String user, 
			  String pwd, 
			  String file,
			  boolean passive,
			  int type) 
	throws IOException, FTPException {
	this.ftp = new FTPClient(host, port);
	this.ftp.authorize(user, pwd);
	get(passive, type, file);
    }
    
    protected void get(boolean passive,
		       int type,
		       String remoteFile)
        throws IOException, FTPException {

	InputStreamDataSink sink = null;

	try {
	    this.ftp.setType(type);

	    if (passive) {
		this.ftp.setPassive();
		this.ftp.setLocalActive();
	    } else {
		this.ftp.setLocalPassive();
		this.ftp.setActive();
	    }
	    
	    sink = new InputStreamDataSink();

	    this.input = sink.getInputStream();

	    this.state = this.ftp.asynchGet(remoteFile,
					    sink,
					    null);
	
	    this.state.waitForStart();

	} catch (FTPException e) {
	    if (sink != null) {
		sink.close();
	    }
	    close();
	    throw e;
	}
    }
    
    public long getSize() {
	return -1;
    }

    public void abort() {
	if (this.input != null) {
	    try {
		this.input.close();
	    } catch(Exception e) {}
	}
	try {
	    this.ftp.close();
	} catch (IOException e) {
	} catch (FTPException e) {
	}
    }
    
    // standard InputStream methods 

    public void close() 
	throws IOException {

	if (this.input != null) {
	    try {
		this.input.close();
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
    
    public int read(byte [] msg) 
	throws IOException {
	return this.input.read(msg);
    }

    public int read(byte [] buf, int off, int len) 
	throws IOException {
	return this.input.read(buf, off, len);
    }
    
    public int read()
        throws IOException {
        return this.input.read();
    }

    public int available()
        throws IOException {
        return this.input.available();
    }
    
}
