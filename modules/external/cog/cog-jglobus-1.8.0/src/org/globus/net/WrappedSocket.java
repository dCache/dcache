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
import java.net.SocketException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class WrappedSocket extends Socket {

    protected Socket socket;
    
    protected WrappedSocket() {}

    public WrappedSocket(Socket socket) {
	super();
	this.socket = socket;
    }

    public Socket getWrappedSocket() {
        return this.socket;
    }

    public OutputStream getOutputStream() 
	throws IOException {
        return this.socket.getOutputStream();
    }
    
    public synchronized InputStream getInputStream() 
	throws IOException {
	return this.socket.getInputStream();
    }

    public void close() 
	throws IOException {
        this.socket.close();
    }
    
    public InetAddress getInetAddress() {
	return this.socket.getInetAddress();
    }

    public boolean getKeepAlive()
	throws SocketException {
	return this.socket.getKeepAlive();
    }

    public InetAddress getLocalAddress() {
	return this.socket.getLocalAddress();
    }

    public int getLocalPort() {
	return this.socket.getLocalPort();
    }
    
    public int getPort() {
	return this.socket.getPort();
    }

    public int getReceiveBufferSize()
	throws SocketException {
	return this.socket.getReceiveBufferSize();
    }

    public int getSendBufferSize()
	throws SocketException {
	return this.socket.getSendBufferSize();
    }

    public int getSoLinger()
	throws SocketException {
	return this.socket.getSoLinger();
    }

    public int getSoTimeout()
	throws SocketException {
	return this.socket.getSoTimeout();
    }

    public boolean getTcpNoDelay()
	throws SocketException {
	return this.socket.getTcpNoDelay();
    }

    public void setKeepAlive(boolean on)
	throws SocketException {
	this.socket.setKeepAlive(on);
    }

    public void setReceiveBufferSize(int size)
	throws SocketException {
	this.socket.setReceiveBufferSize(size);
    }

    public void setSendBufferSize(int size)
	throws SocketException {
	this.socket.setSendBufferSize(size);
    }
    
    public void setSoLinger(boolean on,
			    int linger)
	throws SocketException {
	this.socket.setSoLinger(on, linger);
    }
    
    public void setSoTimeout(int timeout)
	throws SocketException {
	this.socket.setSoTimeout(timeout);
    }

    public void setTcpNoDelay(boolean on)
	throws SocketException {
	this.socket.setTcpNoDelay(on);
    }

    public void shutdownInput()
	throws IOException {
	this.socket.shutdownInput();
    }

    public void shutdownOutput()
	throws IOException {
	this.socket.shutdownOutput();
    }

    public String toString() {
	return this.socket.toString();
    }
}
