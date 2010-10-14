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

import java.net.DatagramSocket;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.BindException;
import java.net.SocketException;
import java.io.IOException;

/**
 * This factory allows for creating datagram sockets. 
 * If the udp.source.port.range system property is set it will create
 * datagram sockets within the specified local port range (if the local port
 * number is set to 0).
 */
public class DatagramSocketFactory {
    
    private static DatagramSocketFactory defaultFactory = null;

    private PortRange portRange = null;
    
    protected DatagramSocketFactory() {
        this.portRange = PortRange.getUdpSourceInstance();
    }

    /**
     * Returns the default instance of this class.
     *
     * @return DatagramSocketFactory instance of this class.
     */
    public static synchronized DatagramSocketFactory getDefault() {
        if (defaultFactory == null) {
            defaultFactory = new DatagramSocketFactory();
        }
        return defaultFactory;
    }

    public DatagramSocket createDatagramSocket() 
        throws IOException {
        return createDatagramSocket(0, null);
    }
    
    public DatagramSocket createDatagramSocket(int port) 
        throws IOException {
        return createDatagramSocket(port, null);
    }
    
    public DatagramSocket createDatagramSocket(int port, InetAddress localAddr)
        throws IOException {
        if (this.portRange.isEnabled() && port == 0) {
            return new PrDatagramSocket(createDatagramSocket(localAddr));
        } else {
            return new DatagramSocket(port, localAddr);
        }
    }

    private DatagramSocket createDatagramSocket(InetAddress localAddr)
        throws IOException {
        DatagramSocket socket = null;
        int localPort = 0;
        
        while(true) {
            localPort = this.portRange.getFreePort(localPort);

            try {
                socket = new DatagramSocket(localPort, localAddr);
                this.portRange.setUsed(localPort);
                return socket;
            } catch(BindException e) {
                // continue on
                localPort++;
            }
        }
    }

    class PrDatagramSocket extends DatagramSocket {

        private DatagramSocket socket;

        public PrDatagramSocket(DatagramSocket socket) 
            throws SocketException {
            super.close();
            this.socket = socket;
        }

        public void connect(InetAddress address, int port) {
            this.socket.connect(address, port);
        }
        
        public void disconnect() {
            this.socket.disconnect();
        }

        public InetAddress getInetAddress() {
            return this.socket.getInetAddress();
        }

        public int getPort() {
            return this.socket.getPort();
        }
        
        public void send(DatagramPacket p) throws IOException  {
            this.socket.send(p);
        }
        
        public void receive(DatagramPacket p) throws IOException {
            this.socket.receive(p);
        }
        
        public InetAddress getLocalAddress() {
            return this.socket.getLocalAddress();
        }

        public int getLocalPort() {
            return this.socket.getLocalPort();
        }
        
        public void setSoTimeout(int timeout) throws SocketException {
            this.socket.setSoTimeout(timeout);
        }

        public int getSoTimeout() throws SocketException {
            return this.socket.getSoTimeout();
        }

        public void setSendBufferSize(int size) throws SocketException {
            this.socket.setSendBufferSize(size);
        }

        public int getSendBufferSize() throws SocketException {
            return this.socket.getSendBufferSize();
        }

        public void setReceiveBufferSize(int size) throws SocketException {
            this.socket.setReceiveBufferSize(size);
        }

        public int getReceiveBufferSize() throws SocketException {
            return this.socket.getReceiveBufferSize();
        }
        
        public void close() {
            int port = getLocalPort();
            socket.close();
            if (port != -1) {
                portRange.free(port);
            }
        }
    }
}
