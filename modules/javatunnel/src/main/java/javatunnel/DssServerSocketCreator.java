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

import javax.net.ServerSocketFactory;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.InetAddress;
import java.net.ServerSocket;

public class DssServerSocketCreator extends ServerSocketFactory
{
    private final DssContextFactory factory;

    public DssServerSocketCreator(String[] args) throws Throwable
    {
        Class<? extends DssContextFactory> factory = Class.forName(args[0]).asSubclass(DssContextFactory.class);
        Constructor<? extends DssContextFactory> cc = factory.getConstructor(String.class);
        try {
            this.factory = cc.newInstance(args[1]);
        } catch (InvocationTargetException e) {
            throw e.getCause();
        }
    }

    @Override
    public ServerSocket createServerSocket(int port) throws IOException
    {
        return new DssServerSocket(port, factory);
    }

    @Override
    public ServerSocket createServerSocket() throws IOException
    {
        return new DssServerSocket(factory);
    }

    @Override
    public ServerSocket createServerSocket(int port, int backlog)
            throws IOException
    {
        return new DssServerSocket(port, backlog, factory);
    }

    @Override
    public ServerSocket createServerSocket(int port, int backlog, InetAddress ifAddress)
            throws IOException
    {
        return new DssServerSocket(port, backlog, ifAddress, factory);
    }
}
