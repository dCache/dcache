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
import java.io.OutputStream;
import java.io.IOException;
import java.net.Socket;

import org.globus.io.gass.client.internal.GASSProtocol;
import org.globus.io.gass.client.GassException;
import org.globus.net.SocketFactory;
import org.globus.util.http.HttpResponse;
import org.globus.common.ChainedIOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class HTTPOutputStream extends GlobusOutputStream {

    private static Log logger =
        LogFactory.getLog(HTTPOutputStream.class.getName());

    private static final byte[] CRLF = "\r\n".getBytes();
    private static final int DEFAULT_TIME = 3000;

    protected OutputStream output;
    protected InputStream in;
    protected Socket socket;
    protected long size = -1;
    protected boolean append = false;

    /**
     * Private constructor used by subclasses.
     */
    protected HTTPOutputStream() {
    }
    
    
    /**
     * Opens HTTP output stream (unsecure)
     *
     * @param host host name of the HTTP server.
     * @param port port number of the HTTP server.
     * @param file name of the file on the remote side.
     * @param length total size of the data to be transfered.
     *               Use -1 if unknown. The data then will be
     *               transfered in chunks.
     * @param append if true, append data to existing file.
     *               Otherwise, the file will be overwritten.
     */
    public HTTPOutputStream(String host, 
			    int port, 
			    String file, 
			    long length, 
			    boolean append)
	throws GassException, IOException {
	init(host, port, file, length, append);
    }
    
    private void init(String host, 
		      int port, 
		      String file,
		      long length, 
		      boolean append)
	throws GassException, IOException {
	size        = length;
	this.append = append;
	
	// default waiting time for response from the server
	int time = DEFAULT_TIME;
	
	long st = System.currentTimeMillis();	
	socket = SocketFactory.getDefault().createSocket(host, port);
	long et = System.currentTimeMillis();	
	
	time = 2*(int)(et - st);
	
	put(host, file, length, time);
    }
  
    private void sleep(int time) {
	try {
	    Thread.sleep(time);
	} catch(Exception e) {}
    }

    protected void put(String host, String file, long length, int waittime) 
	throws IOException {
	
	output = socket.getOutputStream();
	in  = socket.getInputStream();
	
	String msg =  GASSProtocol.PUT(file,
				       host,
				       length,
				       append);
	
	if (logger.isTraceEnabled()) {
	    logger.trace("SENT: " + msg);
	}
	
	output.write( msg.getBytes() );
	output.flush();
	
	if (waittime < 0) {
	    int maxsleep = DEFAULT_TIME;
	    while(maxsleep != 0) {
		sleep(1000);
		maxsleep -= 1000;
		checkForReply();
	    }
	} else {
	    sleep(waittime);
	}

	checkForReply();
    }
    
    private void checkForReply() 
	throws IOException {

	if (in.available() <= 0) {
	    return;
	}

	HttpResponse reply = new HttpResponse(in);
	
	if (logger.isTraceEnabled()) {
	    logger.trace("REPLY: " + reply);
	}
	
	if (reply.httpCode != 100) {
	    abort();
	    throw new IOException("Gass PUT failed: " + reply.httpMsg);
	} else {
	    logger.debug("Received continuation reply");
	}
    }

    private void finish() throws IOException {
	if (size == -1) {
	    String lHex = Integer.toHexString(0);
	    output.write(lHex.getBytes());
	    output.write(CRLF);
	    output.write(CRLF);
	}
	output.flush();
    }
    
    private void closeSocket() {
	try {
	    if (socket != null) socket.close();
	    if (in != null) in.close();
	    if (output != null) output.close();
	} catch(Exception e) {}
    }
    
    public void abort() {
	try {
	    finish();
	} catch(Exception e) {}
	closeSocket();
    }

    public void close() 
	throws IOException {
	
	// is there a way to get rid of that wait for final reply?
	
	finish();
	
	HttpResponse hd = new HttpResponse(in);

	closeSocket();
	
	if (logger.isTraceEnabled()) {
	    logger.trace("REPLY: " + hd);
	}
	
	if (hd.httpCode != 200) {
	    throw new ChainedIOException("Gass close failed.",
					 new GassException("Gass PUT failed: " + hd.httpMsg));
	}
    }
    
    public void write(byte [] msg) 
	throws IOException {
	write(msg, 0, msg.length);
    }
    
    public void write(byte [] msg, int from, int length) 
	throws IOException {
	checkForReply();
	if (size == -1) {
	    String lHex = Integer.toHexString(length);
	    output.write(lHex.getBytes());
	    output.write(CRLF);
	    output.write(msg, from, length);
	    output.write(CRLF);
	} else {
	    output.write(msg, from, length);
	}
    }

    public void write(int b) 
	throws IOException {
	checkForReply();
	if (size == -1) {
            output.write("01".getBytes());
            output.write(CRLF);
            output.write(b);
            output.write(CRLF);
	} else {
	    output.write(b);
	}
    }

    public void flush() 
	throws IOException {	
	output.flush();
    }
}
