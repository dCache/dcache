/*
 * dCache - http://www.dcache.org/
 *
 * Copyright (C) 1998 - 2018 Deutsches Elektronen-Synchrotron
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
package org.dcache.ftp.proxy;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.util.EnumSet;
import java.util.stream.Collectors;

import org.dcache.util.ColumnWriter;
import org.dcache.util.ColumnWriter.TabulatedRow;

/**
 * Provide an ASCII-art description of the current state of a proxy.
 */
public class ProxyPrinter
{
    private enum ConnectionState {
        ESTABLISHED, HALF_CLOSED, CLOSED
    }

    private final EnumSet<ConnectionState> connectionStates = EnumSet.noneOf(ConnectionState.class);
    private final ColumnWriter table;

    private Socket client;
    private Socket pool;

    public ProxyPrinter()
    {
        table = new ColumnWriter()
                .right("client-remote")
                .centre("client-net")
                .left("client-local")
                .centre("proxy")
                .right("pool-local")
                .centre("pool-net")
                .left("pool-remote");
        table.row()
                .value("client-remote", "Client")
                .value("client-net", "        +-")
                .fill("client-local", "-")
                .value("proxy", "-Adapter-")
                .fill("pool-local", "-")
                .value("pool-net", "-+        ")
                .value("pool-remote", "Pool");
    }

    public ProxyPrinter client(Socket connection)
    {
        client = connection;
        return this;
    }

    public ProxyPrinter pool(Socket connection)
    {
        pool = connection;
        return this;
    }

    public ProxyPrinter add()
    {
        TabulatedRow row = table.row();

        if (client == null) {
            row.value("client-net", "        | ");
        } else {
            row.value("client-remote", format(client.getRemoteSocketAddress()))
                    .value("client-net", networkConnection(client, true))
                    .value("client-local", format(client.getLocalSocketAddress()));
        }

        if (pool == null) {
            row.value("pool-net", " |        ");
        } else {
            row.value("pool-local", format(pool.getLocalSocketAddress()))
                .value("pool-net", networkConnection(pool, false))
                .value("pool-remote", format(pool.getRemoteSocketAddress()));
        }

        client = null;
        pool = null;
        return this;
    }

    private String format(SocketAddress sa)
    {
        if (sa instanceof InetSocketAddress) {
            InetSocketAddress isa = (InetSocketAddress)sa;
            return isa.getAddress().getHostAddress() + ":" + isa.getPort();
        } else {
            return sa.toString();
        }
    }

    private String networkConnection(Socket s, boolean isRemoteLeft)
    {
        if (s.isInputShutdown() && s.isOutputShutdown()) {
            connectionStates.add(ConnectionState.CLOSED);
            return isRemoteLeft ? "........| " : " |........";
        } else if (!s.isInputShutdown() && !s.isOutputShutdown()) {
            connectionStates.add(ConnectionState.ESTABLISHED);
            return isRemoteLeft ? "========| " : " |========";
        } else if (!s.isInputShutdown() && isRemoteLeft) {
            connectionStates.add(ConnectionState.HALF_CLOSED);
            return "-->-->--| ";
        } else if (!s.isOutputShutdown() && !isRemoteLeft) {
            connectionStates.add(ConnectionState.HALF_CLOSED);
            return " |-->-->--";
        } else {
            connectionStates.add(ConnectionState.HALF_CLOSED);
            return isRemoteLeft ? "--<--<--| " : " |--<--<--";
        }
    }

    @Override
    public String toString()
    {
        table.row()
                .value("client-net", "        +-")
                .fill("client-local", "-")
                .value("proxy", "---------")
                .fill("pool-local", "-")
                .value("pool-net", "-+        ");

        return connectionStates.isEmpty()
                ? table.toString()
                : table + connectionStates.stream()
                        .sorted()
                        .map(ProxyPrinter::legend)
                        .collect(Collectors.joining(", ", "\nTCP states: ", ""));
    }

    private static String legend(ConnectionState state)
    {
        switch (state) {
        case ESTABLISHED:
            return "\"========\" means Established";
        case HALF_CLOSED:
            return "\"--------\" means Half-closed (arrows show open dirn)";
        case CLOSED:
            return "\"........\" means Closed";
        }
        throw new RuntimeException("Unknown state " + state);
    }
}
