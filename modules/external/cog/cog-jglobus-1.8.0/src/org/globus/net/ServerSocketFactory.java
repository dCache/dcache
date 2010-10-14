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
package org.globus.net;

import java.net.ServerSocket;
import java.net.InetAddress;
import java.io.IOException;

/**
 * This factory allows for creating regular server sockets. 
 * If the tcp.port.range system property is set it will create
 * server sockets within the specified port range (if the port
 * number is set to 0).
 */
public class ServerSocketFactory {

    private static ServerSocketFactory defaultFactory = null;
    
    private PortRange portRange = null;
  
    protected ServerSocketFactory() {
        this.portRange = PortRange.getTcpInstance();
    }

    /**
     * Returns the default instance of this class.
     *
     * @return ServerSocketFactory instance of this class.
     */
    public static synchronized ServerSocketFactory getDefault() {
        if (defaultFactory == null) {
            defaultFactory = new ServerSocketFactory();
        }
        return defaultFactory;
    }

    /**
     * Creates a server socket on a specified port. A port of 
     * <code>0</code> creates a socket on any free port or if the
     * tcp.port.range system property is set it creates a socket
     * within the specified port range.
     * <p>
     * The maximum queue length for incoming connection indications (a 
     * request to connect) is set to <code>50</code>. If a connection 
     * indication arrives when the queue is full, the connection is refused.
     *
     * @param      port  the port number, or <code>0</code> to use any
     *                   free port or if the tcp.port.range property set
     *                   to use any available port within the specified port
     *                   range.
     * @exception  IOException  if an I/O error occurs when opening the socket.
     */
    public ServerSocket createServerSocket(int port) 
        throws IOException {
        return createServerSocket(port, 50, null);
    }
    
    /**
     * Creates a server socket on a specified port. A port of
     * <code>0</code> creates a socket on any free port or if the
     * tcp.port.range system property is set it creates a socket
     * within the specified port range.
     * <p>
     * The maximum queue length for incoming connection indications (a 
     * request to connect) is set to the <code>backlog</code> parameter. If 
     * a connection indication arrives when the queue is full, the 
     * connection is refused. 
     *
     * @param      port  the port number, or <code>0</code> to use any
     *                   free port or if the tcp.port.range property set
     *                   to use any available port within the specified port
     *                   range.
     * @param      backlog  the maximum length of the queue.
     * @exception  IOException  if an I/O error occurs when opening the socket.
     */
    public ServerSocket createServerSocket(int port, int backlog) 
        throws IOException {
        return createServerSocket(port, backlog, null);
    }
    
    /** 
     * Create a server with the specified port, listen backlog, and 
     * local IP address to bind to.  The <i>bindAddr</i> argument
     * can be used on a multi-homed host for a ServerSocket that
     * will only accept connect requests to one of its addresses.
     * If <i>bindAddr</i> is null, it will default accepting
     * connections on any/all local addresses.
     * The port must be between 0 and 65535, inclusive.
     *
     * @param port the local TCP port
     * @param backlog the listen backlog
     * @param bindAddr the local InetAddress the server will bind to
     * @exception  IOException  if an I/O error occurs when opening the socket.
     */
    public ServerSocket createServerSocket(int port, int backlog, 
                                           InetAddress bindAddr)
        throws IOException {
        if (this.portRange.isEnabled() && port == 0) {
            return createServerSocket(backlog, bindAddr);
        } else {
            return new ServerSocket(port, backlog, bindAddr);
        }
    }
  
    /**
     * Tries to find first available port within the port range specified.
     * If it finds a free port, it first checks if the port is not used
     * by any other server. If it is, it keeps looking for a next available
     * port. If none found, it throws an exception. If the port is available
     * the server instance is returned.
     */
    private ServerSocket createServerSocket(int backlog, InetAddress binAddr) 
        throws IOException {
        
        ServerSocket server = null ;
        int port = 0;
        
        while(true) {
            port = this.portRange.getFreePort(port);
            try {
                server = new PrServerSocket(port, backlog, binAddr);
                this.portRange.setUsed(port);
                return server;
            } catch(IOException e) {
                // continue on
                port++;
            }
            
        }
    }

    class PrServerSocket extends ServerSocket {
  
        public PrServerSocket(int port, int backlog, InetAddress bindAddr)
            throws IOException {
            super(port, backlog, bindAddr);
        }
  
        public void close()
            throws IOException {
            int port = getLocalPort();
            try {
                super.close();
            } finally {
                if (port != -1) {
                    portRange.free(port);
                }
            }
        }
    }
}
