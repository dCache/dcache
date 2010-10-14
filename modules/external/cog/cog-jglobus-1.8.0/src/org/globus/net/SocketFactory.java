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

import java.net.Socket;
import java.net.InetAddress;
import java.net.BindException;
import java.io.IOException;

/**
 * This factory allows for creating regular sockets. 
 * If the tcp.source.port.range system property is set it will create
 * sockets within the specified local port range (if the local port
 * number is set to 0).
 */
public class SocketFactory {
    
    private static SocketFactory defaultFactory = null;

    private PortRange portRange = null;
    
    protected SocketFactory() {
        this.portRange = PortRange.getTcpSourceInstance();
    }

    /**
     * Returns the default instance of this class.
     *
     * @return SocketFactory instance of this class.
     */
    public static synchronized SocketFactory getDefault() {
        if (defaultFactory == null) {
            defaultFactory = new SocketFactory();
        }
        return defaultFactory;
    }

    public Socket createSocket(String host, int port)
        throws IOException {
        return createSocket(InetAddress.getByName(host), port, null, 0);
    }
    
    public Socket createSocket(InetAddress address, int port) 
        throws IOException {
        return createSocket(address, port, null, 0);
    }
    
    public Socket createSocket(String host, int port, 
                               InetAddress localAddr, int localPort) 
        throws IOException {
        return createSocket(InetAddress.getByName(host), port, 
                            localAddr, localPort);
    }
    
    public Socket createSocket(InetAddress address, int port, 
                               InetAddress localAddr, int localPort) 
        throws IOException {
        if (this.portRange.isEnabled() && localPort == 0) {
            return new PrSocket(createSocket(address, port, localAddr));
        } else {
            return new Socket(address, port, localAddr, localPort);
        }
    }

    private Socket createSocket(InetAddress address, int port, 
                                InetAddress localAddr)
        throws IOException {
        Socket socket = null;
        int localPort = 0;
        
        while(true) {
            localPort = this.portRange.getFreePort(localPort);

            try {
                socket = new Socket(address, port, localAddr, localPort);
                this.portRange.setUsed(localPort);
                return socket;
            } catch(BindException e) {
                // continue on
                localPort++;
            }
        }
    }

    class PrSocket extends WrappedSocket {

        public PrSocket(Socket socket) {
            super(socket);
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
