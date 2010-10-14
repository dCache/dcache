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
package org.globus.io.gass.server;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.IOException;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.net.Socket;
import java.net.URLDecoder;
import java.util.Hashtable;

import org.globus.util.GlobusURL;
import org.globus.util.http.HttpResponse;
import org.globus.io.gass.client.internal.GASSProtocol;
import org.globus.net.BaseServer;
import org.globus.net.SocketFactory;
import org.globus.gsi.GSIConstants;
import org.globus.gsi.gssapi.auth.SelfAuthorization;
import org.globus.gsi.gssapi.auth.AuthorizationException;
import org.globus.gsi.gssapi.GSSConstants;
import org.globus.gsi.gssapi.net.GssSocket;
import org.globus.gsi.gssapi.net.GssSocketFactory;

import org.gridforum.jgss.ExtendedGSSManager;
import org.gridforum.jgss.ExtendedGSSContext;

import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSContext;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * The <code>GassServer</code> class acts as a basic multi-threaded HTTPS
 * server that handles GASS requests.
 *
 * @version $Revision 1.21 $
 */
public class GassServer extends BaseServer {

    private static Log logger =
        LogFactory.getLog(GassServer.class.getName());
    
    public static final int READ_ENABLE            = 8;
    public static final int WRITE_ENABLE           = 16;
    public static final int STDOUT_ENABLE          = 32;
    public static final int STDERR_ENABLE          = 64;
    public static final int CLIENT_SHUTDOWN_ENABLE = 128;
  
    public static final String SHUTDOWN_STR = "/dev/globus_gass_client_shutdown";

    private Hashtable jobOutputs = null;
    private int options          = 0;
    
    /**
     * Starts Gass Server with default user credentials.
     * Port of the server will be dynamically assigned
     */
    public GassServer() 
	throws IOException {
	this(null, 0);
    }
  
    /**
     * Starts Gass Server on given port with default user credentials.
     *
     * @param port
     *         port of the server, if 0 it will be dynamically assigned
     */
    public GassServer(int port) 
	throws IOException {
	this(null, port);
    }

    /**
     * Starts Gass Server on given port and given credentials.
     *
     * @param cred
     *         credentials to use. if null default user credentials
     *         will be used
     * @param port
     *         port of the server, if 0 it will be dynamically assigned
     */
    public GassServer(GSSCredential cred, int port)
	throws IOException {
	super(cred, port);
	init();
    }

    /**
     * Starts Gass Server on given port and mode.
     * If secure mode, it will use default user credentials
     *
     * @param secure 
     *         if true starts server in secure mode, otherwise unsecure
     * @param port
     *         port of the server, if 0 it will be dynamically assigned
     */
    public GassServer(boolean secure, int port) 
	throws IOException {
	super(secure, port);
	init();
    }
  
    private void init() {
	jobOutputs = new Hashtable();
	options = READ_ENABLE | WRITE_ENABLE | STDOUT_ENABLE | STDERR_ENABLE;
	super.initialize();
	setAuthorization(SelfAuthorization.getInstance());
    }

    /**
     * Sets the options of the gass server such 
     * as enabling client shutdown, etc.
     *
     * @param options server options 
     */
    public void setOptions(int options) {
	this.options = options;
    }
  
    /**
     * Returns current options of the server.
     *
     * @return options of the server. O if not
     *         none set.
     */
    public int getOptions() {
	return options;
    }
    
    /**
     * Registers a output stream with a job. This is
     * used for job stdout/err redirection.
     * The label of the job should be the ending of the
     * job redirected url. For example, given following RSL
     * (stdout=$(GASS_URL)/dev/stdout-5) the label to register
     * the output stream with should be 'out-5'. 
     *
     * @param lb job label as described above.
     * @param out the output stream to redirect output to.
     */
    public void registerJobOutputStream(String lb, OutputStream out) {
	jobOutputs.put(lb, out);
    }

    /**
     * Unregisters a job output stream for specified output label. See
     * registerJobOutputStream() for more details.
     *
     * @param lb job output label.
     */
    public void unregisterJobOutputStream(String lb) {
	jobOutputs.remove(lb);
    }
    
    /**
     * Unregisters a job output stream. This method is deprecated.
     */
    public void unregisterJobOutputStream(String lb, OutputStream out) {
	unregisterJobOutputStream(lb);
    }
    
    protected OutputStream getJobOutputStream(String id) {
	return (OutputStream)jobOutputs.get(id);
    }
    
    protected void handleConnection(Socket socket) {
	GassClientHandler gcb = new GassClientHandler(this, 
						      socket);
	(new Thread(gcb)).start();
    }
  
    public String toString() {
	StringBuffer buf = new StringBuffer("GassServer: ");
	try {
	    buf.append(getURL());
	} catch(Exception e) {}
	buf.append(" options (");
	boolean op = ((options & READ_ENABLE) != 0);
	buf.append("r:" + ( (op) ? "+" : "-" ));
	op = ((options & WRITE_ENABLE) != 0);
	buf.append(" w:" + ( (op) ? "+" : "-" ));
	op = ((options & STDOUT_ENABLE) != 0);
	buf.append(" so:" + ( (op) ? "+" : "-"));
	op = ((options & STDERR_ENABLE) != 0);
	buf.append(" se:" + ( (op) ? "+" : "-"));
	op = ((options & CLIENT_SHUTDOWN_ENABLE) != 0);
	buf.append(" rc:" + ( (op) ? "+" : "-"));
	buf.append(")");
	return buf.toString();
    }
  
    /**
     * Shutdowns a remote gass server. The server must have the
     * CLIENT_SHUTDOWN option enabled for this to work.
     *
     * @param  cred    credentials to use.
     * @param  gassURL the url of the remote gass server.
     */
    public static void shutdown(GSSCredential cred, GlobusURL gassURL) 
	throws IOException, GSSException {
	
	OutputStream output     = null;
	InputStream input       = null;
	Socket socket           = null;
	
	try {

	    if (gassURL.getProtocol().equalsIgnoreCase("https")) {

		GSSManager manager = ExtendedGSSManager.getInstance();
		
		ExtendedGSSContext context = 
		    (ExtendedGSSContext)manager.createContext(null,
							      GSSConstants.MECH_OID,
							      cred,
							      GSSContext.DEFAULT_LIFETIME);

		context.setOption(GSSConstants.GSS_MODE,
				  GSIConstants.MODE_SSL);

		GssSocketFactory factory = GssSocketFactory.getDefault();

		socket = factory.createSocket(gassURL.getHost(), 
					      gassURL.getPort(),
					      context);

		((GssSocket)socket).setAuthorization(SelfAuthorization.getInstance());
	    } else {
                SocketFactory factory = SocketFactory.getDefault();
		socket = factory.createSocket(gassURL.getHost(), 
                                              gassURL.getPort());
	    }
	
	    output = socket.getOutputStream();
	    input  = socket.getInputStream();
	
	    String msg =  GASSProtocol.SHUTDOWN(SHUTDOWN_STR,
						gassURL.getHost());
	
	    
            if (logger.isTraceEnabled()) {
                logger.trace("Shutdown msg: " + msg);
            }
	
	    output.write( msg.getBytes() );
	    output.flush();
	
	    HttpResponse rp = new HttpResponse(input);
	    if (rp.httpCode == -1 && rp.httpMsg == null) {
		/* this is a workaround for C gass-server.
		 * The server just shuts down - it does
		 * not send the reply */
	    } else if (rp.httpCode != 200) {
		throw new IOException("Remote shutdown failed (" + rp.httpCode + " " + rp.httpMsg + ")");
	    }
	    
	} finally {
	    try {
		if (output != null) output.close();
		if (input != null) input.close();
		if (socket != null) socket.close();
	    } catch(Exception e) {}
	}
    }
	
}

class GassClientHandler implements Runnable {

    private static Log logger =
        LogFactory.getLog(GassClientHandler.class.getName());

    private static final boolean DEBUG_ON = false;

    private static final String CRLF = "\r\n";
    private static final String OKHEADER = "HTTP/1.1 200 OK\r\n";
    private static final String SERVER = "Server: Globus-GASS-HTTP/1.1.0\r\n";
    private static final String CONTENT_LENGTH = "Content-Length:";
    private static final String TRANSFER_ENCODING = "Transfer-Encoding: chunked";
    private static final String JAVA_CLIENT = "User-Agent: Java-Globus-GASS-HTTP/1.1.0";
    private static final String HTTP_CONTINUE = "HTTP/1.1 100 Continue\r\n";
    
    private static final String CONTENT_BINARY = "Content-Type: application/octet-stream" + CRLF;
    private static final String CONTENT_HTML   = "Content-Type: text/html" + CRLF;
    private static final String CONTENT_TEXT   = "Content-Type: text/plain" + CRLF;
    
    private static final String CONNECTION_CLOSE = "Connection: close\r\n";
    
    private static final String HEADER404 = "HTTP/1.1 404 File Not Found\r\n";

    private static final String MSG404 =
        "<html><head><title>404 File Not Found</title></head><body>\r\n" +
        "<h1>404 File Not Found</h1></body></html>\r\n";
    
    private int BUFFER_SIZE = 4096;

    private GassServer server;
    private Socket socket;
    private int options;

    public GassClientHandler(GassServer server, 
			     Socket socket) {
	this.server  = server;
	this.socket  = socket;
	this.options = server.getOptions();
    }
    
    private void write(OutputStream out, String msg) throws IOException {
	out.write(msg.getBytes());
	out.flush();
    }

    private void writeln(OutputStream out, String msg) throws IOException {
	out.write(msg.getBytes());
	out.write(SERVER.getBytes());
	out.write(CRLF.getBytes());
	out.flush();
    }

    /**
     * Listen on the server socket for a client, start another thread to
     * keep listening on the server socket, then deal with the client.
     */
    public void run() {

	InputStream in   = null;
	OutputStream out = null;
    
	try {
	    in  = socket.getInputStream();
	    out = socket.getOutputStream();

	    try {

		String line;
		line = readLine(in);
	  
		if (DEBUG_ON) debug("header: " + line);
	  
		if (line.startsWith("GET") && 
		    (options & GassServer.READ_ENABLE) != 0) {
		    
		    // copy to client
	    
		    String path = 
			line.substring(4, line.indexOf(' ', 4) );
	    
		    do {
			line = readLine(in);
			if (DEBUG_ON) debug("header (get): " + line);
		    } while ( (line.length() != 0)
			      && (line.charAt(0) != '\r')
			      && (line.charAt(0) != '\n') );
		    
		    transfer(out, path);
	    
		} else if (line.startsWith("PUT") &&
			   ( ((options & GassServer.WRITE_ENABLE) != 0) ||
			     ((options & GassServer.CLIENT_SHUTDOWN_ENABLE) != 0) )) {
	  
		    // copy from client
	    
		    String path = 
			line.substring(4, line.indexOf(' ', 4) );

		    transfer(in, path, false, out);
	    
		    writeln(out, OKHEADER);
	    
		} else if (line.startsWith("POST") &&
			   ( ((options & GassServer.WRITE_ENABLE) != 0) ||
			     ((options & GassServer.STDOUT_ENABLE) != 0) ||
			     ((options & GassServer.STDERR_ENABLE) != 0) ||
			     ((options & GassServer.CLIENT_SHUTDOWN_ENABLE) != 0) )) {
		    // append from client
	    
		    int index = line.indexOf('?') + 1;
		    String path = 
			line.substring(index, line.indexOf(' ', index) );
		    
		    transfer(in, path, true, out);
		    
		    writeln(out, OKHEADER);

		} else {
		    writeln(out, "HTTP/1.1 400 Bad Request" + CRLF);
		}

	    } catch (FileNotFoundException ex) {
		logger.debug("FileNotFoundException occured: " + ex.getMessage(), ex);
		
		StringBuffer buf = new StringBuffer(HEADER404)
		    .append(CONNECTION_CLOSE)
		    .append(SERVER)
		    .append(CONTENT_HTML)
		    .append(CONTENT_LENGTH).append(" ").append(MSG404.length())
		    .append(CRLF).append(CRLF)
		    .append(MSG404);
		
		out.write(buf.toString().getBytes());
		out.flush();
	    } catch (AuthorizationException ex) {
		logger.debug("Exception occured: Authorization failed");
		writeln(out, "HTTP/1.1 401 Authorization Failed" + CRLF);
	    } catch (Exception ex) {
		logger.debug("Exception occured: " + ex.getMessage(), ex);
		writeln(out, "HTTP/1.1 400 " + ex.getMessage() + CRLF);
	    }
	} catch (IOException e) {
	    logger.error("Error writing response: " + e.getMessage(), e);
	} finally {
	    try {	
		socket.close();
	    } catch (IOException e) {}
	}
    }

    private String decodeUrlPath(String path) {
	if (path.length() == 0) return path;
	if (path.charAt(0) == '/') path = path.substring(1);
	try {
	    return URLDecoder.decode(path);
	} catch(Exception e) {
	    return path;
	}
    }
    
    /**
     * Transfer from a file, given its path, to the given OutputStream.
     * The BufferedWriter points to the same stream but is used to write
     * HTTP header information.
     */
    private void transfer(OutputStream os, String path)
	throws IOException {

	path = decodeUrlPath(path);

        File f = new File(path);
        FileInputStream file = new FileInputStream(f);

        long length = f.length();

        StringBuffer buf = new StringBuffer(OKHEADER)
            .append(CONNECTION_CLOSE)
            .append(SERVER)
            .append(CONTENT_BINARY)
            .append(CONTENT_LENGTH).append(" ").append(length)
            .append(CRLF).append(CRLF);

        os.write(buf.toString().getBytes());
        os.flush();
	
	byte [] buffer = new byte[BUFFER_SIZE];
	int read;
	
	while (length != 0) {
	    read = file.read(buffer);
	    if (read == -1) break;
	    os.write(buffer, 0, read);
	    length -= read;
	}
	
	file.close();
	os.flush();
	os.close();
    }
  
  private OutputStream pickOutputStream(String path, String str, 
					OutputStream def) {
    
    int strl   = str.length();
    int pos    = path.indexOf(str);
    
    if (pos != -1) {
      OutputStream out = server.getJobOutputStream(path.substring(pos + strl - 3));
      if (out == null) {
	return def;
      } else {
	return out;
      }
      
    }
    
    return null;
  }

    /**
     * Transfer from the given InputStream to a file, given its path.
     * The Reader points to the same stream but is used to read
     * the HTTP header information.
     */
    private void transfer(InputStream is, String path, boolean append,
			  OutputStream outs)
	throws IOException {
	 
	if (((options & GassServer.CLIENT_SHUTDOWN_ENABLE) != 0) &&
	    path.indexOf(GassServer.SHUTDOWN_STR) != -1) {
	    server.shutdown();
	    return;
	}
	 
	 OutputStream out = null;
	 
	 String line;
	 long length = 0;
	 boolean chunked = false;
	 boolean javaclient = false;
	 do {
	   line = readLine(is);
	   if (DEBUG_ON) debug("header (put/post): " + line);
	   if (line.startsWith(CONTENT_LENGTH)) {
	     length = Long.parseLong( line.substring(
						     line.indexOf(':') + 1 ).trim() );
	   } else if (line.startsWith(TRANSFER_ENCODING)) {
	     chunked = true;
	   } else if (line.startsWith(JAVA_CLIENT)) {
	     javaclient = true;
	   }
	 } while ( (line.length() != 0)
		   && (line.charAt(0) != '\r')
		   && (line.charAt(0) != '\n') );
	 
	 out = pickOutputStream(path, "/dev/stdout", System.out);
	 if (out != null) {
	   // this is stdout
	   if ( (options & GassServer.STDOUT_ENABLE) == 0 ) {
	     throw new IOException("Bad Request");
	   }
	 } else {
	   out = pickOutputStream(path, "/dev/stderr", System.err);
	   if (out != null) {
	     // this is stderr
	     if ( (options & GassServer.STDERR_ENABLE) == 0 ) {
	       throw new IOException("Bad Request");
	     }
	   } else {
	     // this is a file
	     if ( (options & GassServer.WRITE_ENABLE) == 0 ) {
               throw new IOException("Bad Request");
             }
	     path = decodeUrlPath(path);
	     out = new FileOutputStream(path, append);
	   }
	 }
	 
	 if (javaclient) {
	   writeln(outs, HTTP_CONTINUE);
	 }
	 
	 byte [] buffer = new byte[BUFFER_SIZE];
	 int read;
	 
	 if (!chunked) {
	   
	   while (length != 0) {
	     read = is.read(buffer);
	     if (read == -1) break;
	     out.write(buffer, 0, read);
	     length -= read;
	   }
	   
	 } else {
	   
	   /*
	    * Chunks are of the form
	    *
	    *   lengthCRLF
	    *   dataCRLF
	    *
	    * which can be repeated ad infinitum until we meet
	    *
	    *   0CRLF
	    *   CRLF
	    *
	    * NOTE: length is represented in hex!
	    *
	    */
	   
	   long chunkLength;
	   int bytes;
	   
	   do {
	     line = readLine(is);		
	     length = fromHex(line);
	     
	     if (DEBUG_ON) debug("chunk: '" + line + "' size:" + length);
	     
	     chunkLength = length;
	     
	     while (chunkLength != 0) {
	       
	       if (chunkLength > buffer.length) {
		 bytes = buffer.length;
	       } else {
		 bytes = (int)chunkLength;
	       }
	       
	       read = is.read(buffer, 0, bytes); 
	       if (read == -1) break;
	       out.write(buffer, 0, read);
	       chunkLength -= read;
	     }
	     
	     is.read(); // skip CR
	     is.read(); // skip LF
	   } while (length > 0);
	   
	   if (DEBUG_ON) debug("finished chunking");
	 }
	 
	 out.flush();
	 // do not close System.out or System.err!
	 if (out == System.out || out == System.err) return;
	 out.close();
  }
  
  /**
   * Read a line of text from the given Stream and return it
   * as a String.  Assumes lines end in CRLF.
   */
  private String readLine(InputStream in) throws IOException {
    StringBuffer buf = new StringBuffer();
    int c, length = 0;
    
    while(true) {
      c = in.read();

      if (c == -1 || c == '\n' || length > 512) {
	break;
      } else if (c == '\r') { 
	in.read(); 
	return buf.toString();
      } else {	    
	buf.append((char)c);
	length++;	
      }
      
    }
    return buf.toString();
  }
  
  /**
   * Convert a String representing a hex number to a long.
   */
  private long fromHex(String s) {
    long result = 0;
    int size = s.length();
    for (int i = 0; i < size; i++) {
      char c = s.charAt(i);
      result *= 16;
      switch (c) {
      case '0':
      case '1':
      case '2':
      case '3':
      case '4':
      case '5':
      case '6':
      case '7':
      case '8':
      case '9':		  
	result += (c - '0');
	break;
      case 'a':
      case 'b':
      case 'c':
      case 'd':
      case 'e':
      case 'f':
	result += (c - 'a' + 10);
	break;
      case 'A':
      case 'B':
      case 'C':
      case 'D':
      case 'E':
      case 'F':
	result += (c - 'A' + 10);
	break;
      default :
	// TODO: throw a ParseException
      }
    }
    return result;
  }
  
    private void debug(String msg) {
	System.err.println(msg);
    }
  
}











