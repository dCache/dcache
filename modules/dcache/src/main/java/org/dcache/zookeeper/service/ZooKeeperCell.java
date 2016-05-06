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
package org.dcache.zookeeper.service;

import com.google.common.base.Strings;
import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

import org.dcache.cells.AbstractCell;
import org.dcache.util.Args;
import org.dcache.util.Option;

/**
 * Embedded standalone ZooKeeper as a dCache cell.
 */
public class ZooKeeperCell extends AbstractCell
{
    private static final Logger LOG = LoggerFactory.getLogger(ZooKeeperCell.class);

    @Option(name = "data-log-dir", required = true)
    protected File dataLogDir;

    @Option(name = "data-dir", required = true)
    protected File dataDir;

    @Option(name = "tick-time", required = true)
    protected int tickTime;

    @Option(name = "tick-time-unit", required = true)
    protected TimeUnit tickTimeUnit;

    @Option(name = "min-session-timeout", required = false)
    protected int minSessionTimeout = -1;

    @Option(name = "min-session-timeout-unit", required = true)
    protected TimeUnit minSessionTimeoutUnit;

    @Option(name = "max-session-timeout", required = false)
    protected int maxSessionTimeout = -1;

    @Option(name = "max-session-timeout-unit", required = true)
    protected TimeUnit maxSessionTimeoutUnit;

    @Option(name = "max-client-connections", required = true)
    protected int maxClientConnections;

    @Option(name = "listen", required = false)
    protected String address;

    @Option(name = "port", required = true)
    protected int port;

    private ServerCnxnFactory cnxnFactory;
    private FileTxnSnapLog txnLog;
    private ZooKeeperServer zkServer;

    public ZooKeeperCell(String cellName, String arguments)
    {
        super(cellName, "System", new Args(arguments));
    }

    @Override
    protected void startUp() throws Exception
    {
        super.startUp();

        InetSocketAddress socketAddress =
                Strings.isNullOrEmpty(address) ? new InetSocketAddress(port) : new InetSocketAddress(address, port);

        zkServer = new ZooKeeperServer();
        txnLog = new FileTxnSnapLog(dataLogDir, dataDir);
        zkServer.setTxnLogFactory(txnLog);
        zkServer.setTickTime((int) tickTimeUnit.toMillis(tickTime));
        zkServer.setMinSessionTimeout(minSessionTimeout == -1 ? -1 : (int) minSessionTimeoutUnit.toMillis(minSessionTimeout));
        zkServer.setMaxSessionTimeout(maxSessionTimeout == -1 ? -1 : (int) maxSessionTimeoutUnit.toMillis(maxSessionTimeout));
        ServerCnxnFactory cnxnFactory;
        cnxnFactory = new NIOServerCnxnFactory() {
            @Override
            protected void configureSaslLogin() throws IOException
            {
                // ZooKeeper gets confused by dCache configuring a JAAS configuration without a section for ZooKeeper, so
                // we disable the whole thing. Use a non-embedded ZooKeeper if you want security.
            }
        };
        cnxnFactory.configure(socketAddress, maxClientConnections);
        this.cnxnFactory = cnxnFactory;
        this.cnxnFactory.startup(zkServer);
    }

    @Override
    public void cleanUp()
    {
        if (cnxnFactory != null) {
            cnxnFactory.shutdown();
        }
        if (zkServer != null) {
            zkServer.shutdown();
        }
        if (txnLog != null) {
            try {
                txnLog.close();
            } catch (IOException e) {
                LOG.error("Failed to close ZooKeeper transaction log: {}", e.toString());
            }
        }
        super.cleanUp();
    }

    @Override
    public void getInfo(PrintWriter printWriter)
    {
        printWriter.println("[ZooKeeper configuration]");
        zkServer.dumpConf(printWriter);

        printWriter.println();
        printWriter.println("[ZooKeeper statistics]");
        printWriter.append(zkServer.serverStats().toString());
    }
}
