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
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

import org.globus.io.gass.client.internal.GASSProtocol;
import org.globus.net.SocketFactory;
import org.globus.util.http.HttpResponse;
import org.globus.util.http.HTTPChunkedInputStream;
import org.globus.util.GlobusURL;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class HTTPInputStream extends GlobusInputStream {

    private static Log logger =
        LogFactory.getLog(HTTPInputStream.class.getName());

    protected InputStream input;
    protected Socket socket;
    protected long size = -1;
    
    /**
     * Private constructor used by subclasses.
     */
    protected HTTPInputStream() {
    }

    /**
     * Opens HTTP input stream connection (unsecure)
     *
     * @param host host name of the HTTP server.
     * @param port port number of the HTTP server.
     * @param file file to retrieve from the server.
     */
    public HTTPInputStream(String host,
			   int port,
			   String file) 
	throws IOException {
	get(host, port, file);
    }
    
    // subclasses should overwrite this function
    protected Socket openSocket(String host, int port) 
	throws IOException {
        return SocketFactory.getDefault().createSocket(host, port);
    }

    protected void get(String host, int port, String file)
 	throws IOException {
	
	HttpResponse hd = null;

	while(true) {
	    this.socket = openSocket(host, port);
	    this.input = this.socket.getInputStream();
	    OutputStream out = socket.getOutputStream();
	
	    String msg = GASSProtocol.GET(file, host + ":" + port);

	    try {
		out.write( msg.getBytes() );
		out.flush();
	    
		if (logger.isTraceEnabled()) {
		    logger.trace("SENT: " + msg);
		}
	    
		hd = new HttpResponse(input);
	    } catch(IOException e) {
		abort();
		throw e;
	    }
	
	    if (hd.httpCode == 200) {
		break;
	    } else {
		abort();
		switch(hd.httpCode) {
		case 404:
		    throw new FileNotFoundException(
                            "File " + file + " not found on the server."
		    );
		case 301:
		case 302:
		    logger.debug("Received redirection to: " + hd.location);
		    GlobusURL newLocation = new GlobusURL(hd.location);
		    host = newLocation.getHost();
		    port = newLocation.getPort();
		    file = newLocation.getPath();
		    break;
		default:
		     throw new IOException(
                            "Failed to retrieve file from server. " + 
			    " Server returned error: " + hd.httpMsg +
			    " (" + hd.httpCode + ")"
                    );
		}
	    }
	}

	if (hd.chunked) {
	    input = new HTTPChunkedInputStream(input);
	} else if (hd.contentLength > 0) {
	    size = hd.contentLength;
	}
    }
    
    public void abort() {
	try { 
	    close();
	} catch (Exception e) {}
    }
    
    public long getSize() {
	return size;
    }
    
    public void close() 
	throws IOException {
	if (this.input != null) {
	    this.input.close();
	}
	if (this.socket != null) {
	    this.socket.close();
	}
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

