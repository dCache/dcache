/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2015 Deutsches Elektronen-Synchrotron
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package javatunnel;

import javatunnel.token.Base64TokenReader;
import javatunnel.token.Base64TokenWriter;
import javatunnel.token.TokenReader;
import javatunnel.token.TokenWriter;
import javatunnel.token.UnwrappingInputStream;
import javatunnel.token.WrappingOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.net.SocketImpl;
import java.net.UnknownHostException;

import org.dcache.dss.DssContext;
import org.dcache.dss.DssContextFactory;
import org.dcache.util.Exceptions;

public class DssSocket extends Socket implements TunnelSocket
{
    private static final Logger LOGGER = LoggerFactory.getLogger(DssSocket.class);

    private DssContext context;
    private final DssContextFactory factory;
    private WrappingOutputStream out;
    private UnwrappingInputStream in;

    DssSocket(DssContextFactory factory)
    {
        this.factory = factory;
    }

    DssSocket(SocketImpl impl, DssContextFactory factory) throws SocketException
    {
        super(impl);
        this.factory = factory;
    }

    DssSocket(InetAddress address, int port, DssContextFactory factory)
            throws IOException
    {
        super(address, port);
        this.factory = factory;
    }

    DssSocket(InetAddress address, int port, InetAddress localAddr, int localPort, DssContextFactory factory)
            throws IOException
    {
        super(address, port, localAddr, localPort);
        this.factory = factory;
    }

    DssSocket(String host, int port, DssContextFactory factory)
            throws UnknownHostException, IOException
    {
        super(host, port);
        this.factory = factory;
    }

    DssSocket(String host, int port, InetAddress localAddr, int localPort, DssContextFactory factory)
            throws IOException
    {
        super(host, port, localAddr, localPort);
        this.factory = factory;
    }

    @Override
    public synchronized OutputStream getOutputStream() throws IOException
    {
        if (isClosed())
            throw new SocketException("Socket is closed");
        if (!isConnected())
            throw new SocketException("Socket is not connected");
        if (isOutputShutdown())
            throw new SocketException("Socket output is shutdown");
        if (context == null || !context.isEstablished()) {
            throw new SocketException("Security context is not established");
        }
        return out;
    }

    @Override
    public synchronized InputStream getInputStream() throws IOException
    {
        if (isClosed())
            throw new SocketException("Socket is closed");
        if (!isConnected())
            throw new SocketException("Socket is not connected");
        if (isInputShutdown())
            throw new SocketException("Socket input is shutdown");
        if (context == null || !context.isEstablished()) {
            throw new SocketException("Security context is not established");
        }
        return in;
    }

    private synchronized void acceptSecurityContext() throws IOException
    {
        try {
            context = factory.create((InetSocketAddress) getRemoteSocketAddress(),
                                     (InetSocketAddress) getLocalSocketAddress());

            TokenWriter writer = new Base64TokenWriter(super.getOutputStream());
            TokenReader reader = new Base64TokenReader(super.getInputStream());

            while (!context.isEstablished()) {
                byte[] inToken = reader.readToken();
                if (inToken == null) {
                    throw new EOFException();
                }
                byte[] outToken = context.accept(inToken);
                if (outToken != null) {
                    writer.write(outToken);
                }
            }

            out = new WrappingOutputStream(writer, context);
            in = new UnwrappingInputStream(reader, context);
        } catch (IOException e) {
            try {
                close();
            } catch (IOException e1) {
                e.addSuppressed(e1);
            }
            throw e;
        }
    }

    private synchronized void initSecurityContext() throws IOException
    {
        try {
            context = factory.create((InetSocketAddress) getRemoteSocketAddress(),
                                     (InetSocketAddress) getLocalSocketAddress());

            TokenWriter writer = new Base64TokenWriter(super.getOutputStream());
            TokenReader reader = new Base64TokenReader(super.getInputStream());

            byte[] outToken = context.init(new byte[0]);
            if (outToken != null) {
                writer.write(outToken);
            }
            while (!context.isEstablished()) {
                byte[] inToken = reader.readToken();
                if (inToken == null) {
                    throw new EOFException();
                }
                outToken = context.init(inToken);
                if (outToken != null) {
                    writer.write(outToken);
                }
            }

            out = new WrappingOutputStream(writer, context);
            in = new UnwrappingInputStream(reader, context);
        } catch (IOException e) {
            try {
                close();
            } catch (IOException e1) {
                e.addSuppressed(e1);
            }
            throw e;
        }
    }

    @Override
    public void connect(SocketAddress endpoint) throws IOException
    {
        super.connect(endpoint);
        initSecurityContext();
    }

    @Override
    public void connect(SocketAddress endpoint, int timeout) throws IOException
    {
        super.connect(endpoint, timeout);
        initSecurityContext();
    }

    @Override
    public void verify() throws IOException
    {
        acceptSecurityContext();
    }

    @Override
    public Subject getSubject()
    {
        return (context == null || !context.isEstablished()) ? null : context.getSubject();
    }
}
