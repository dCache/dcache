/*
 * dCache - http://www.dcache.org/
 *
 * Copyright (C) 2016 Deutsches Elektronen-Synchrotron
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
package diskCacheV111.doors;

import java.net.InetSocketAddress;
import java.util.concurrent.Executor;

import diskCacheV111.util.ConfigurationException;

import dmg.cells.nucleus.CellAddressCore;
import dmg.cells.nucleus.CellEndpoint;
import dmg.util.LineWriter;
import dmg.util.StreamEngine;

import org.dcache.poolmanager.PoolManagerHandler;
import org.dcache.util.Args;

/**
 * Factory of implementations of {@link LineBasedInterpreter}.
 *
 * <p>To be used with {@link LineBasedDoor}.
 */
public interface NettyLineBasedInterpreterFactory
{
    /**
     * Injects the cell command line arguments.
     *
     * <p>The factory may use this to initialize itself.
     */
    void configure(Args args) throws ConfigurationException;

    /**
     * Creates a fully initialized interpreter.
     *
     * <p>The interpreter should communicate with dCache though the given endpoint.
     * Replies to be sent to the client can be sent through the {@link StreamEngine}
     * provided.
     *
     * <p>The interpreter may use the given executor for background operations.
     *
     * @param endpoint Cell endpoint of the line based door
     * @param myAddress Cell address of the line based door
     * @param writer Output writer
     * @param remoteAddress Address of the FTP client
     * @param proxyAddress Address the FTP client connected to
     * @param localAddress Address of the local socket
     * @param executor Executor for background operations
     * @param poolManager Handler for pool manager communication
     * @return Fully initialized interpreter
     * @throws Exception If the interpreter could not be initialized
     */
    LineBasedInterpreter create(CellEndpoint endpoint, CellAddressCore myAddress,
                                InetSocketAddress remoteAddress, InetSocketAddress proxyAddress, InetSocketAddress localAddress,
                                LineWriter writer, Executor executor, PoolManagerHandler poolManager)
            throws Exception;
}
