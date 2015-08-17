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

import javatunnel.dss.DssContextFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;

class DssServerSocket extends ServerSocket
{
    private final DssContextFactory _factory;

    public DssServerSocket(DssContextFactory factory) throws IOException
    {
        super();
        _factory = factory;
    }

    public DssServerSocket(int port, DssContextFactory factory) throws IOException
    {
        super(port);
        _factory = factory;
    }

    public DssServerSocket(int port, int backlog, DssContextFactory factory) throws IOException
    {
        super(port, backlog);
        _factory = factory;
    }

    public DssServerSocket(int port, int backlog, InetAddress bindAddr, DssContextFactory factory) throws IOException
    {
        super(port, backlog, bindAddr);
        _factory = factory;
    }

    @Override
    public Socket accept() throws IOException
    {
        if (isClosed()) {
            throw new SocketException("Socket is closed");
        }
        if (!isBound()) {
            throw new SocketException("Socket is not bound yet");
        }
        Socket s = new DssSocket(_factory);
        implAccept(s);
        return s;
    }
}
