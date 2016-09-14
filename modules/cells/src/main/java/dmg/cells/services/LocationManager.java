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

package dmg.cells.services;

import com.google.common.net.HostAndPort;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.nodes.PersistentNode;
import org.apache.curator.utils.CloseableUtils;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;

import dmg.cells.network.LocationManagerConnector;
import dmg.cells.nucleus.CellAdapter;
import dmg.cells.nucleus.CellDomainRole;
import dmg.cells.nucleus.CellEvent;
import dmg.cells.nucleus.CellEventListener;
import dmg.cells.services.login.LoginManager;
import dmg.cells.zookeeper.PathChildrenCache;
import dmg.util.CommandException;
import dmg.util.command.Command;

import org.dcache.util.Args;
import org.dcache.util.ColumnWriter;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.stream.Collectors.toMap;

/**
 * The location manager establishes the cell communication topology.
 */
public class LocationManager extends CellAdapter
{
    private static final Logger LOGGER =
            LoggerFactory.getLogger(LocationManager.class);

    private static final String ZK_CORES = "/dcache/lm/cores";

    private final CoreDomains coreDomains;
    private final Args args;
    private final CellDomainRole role;
    private final Client client;

    /**
     * Represents a group of listening domains in ZooKeeper. For each
     * listening domain the socket address is registered. May be used
     * by non-listening domains too to learn about listening domains.
     */
    private static class CoreDomains implements Closeable
    {
        private final String domainName;
        private final CuratorFramework client;
        private final PathChildrenCache cores;

        /* Only created if the local domain is a core. */
        private PersistentNode local;

        CoreDomains(String domainName, CuratorFramework client)
        {
            this.domainName = domainName;
            this.client = client;
            cores = new PathChildrenCache(client, ZK_CORES, true);
        }

        void onChange(Consumer<PathChildrenCacheEvent> consumer)
        {
            cores.getListenable().addListener((client, event) -> consumer.accept(event));
        }

        void start() throws Exception
        {
            cores.start();
        }

        @Override
        public void close() throws IOException
        {
            CloseableUtils.closeQuietly(cores);
            if (local != null) {
                CloseableUtils.closeQuietly(local);
            }
        }

        HostAndPort getLocalAddress()
        {
            return (local == null) ? null : toHostAndPort(local.getData());
        }

        void setLocalAddress(HostAndPort address) throws Exception
        {
            if (local == null) {
                PersistentNode node = new PersistentNode(client, CreateMode.EPHEMERAL, false, pathOf(domainName), toBytes(address));
                node.start();
                local = node;
            } else {
                local.setData(toBytes(address));
            }
        }

        Map<String,HostAndPort> cores()
        {
            return cores.getCurrentData().stream()
                    .collect(toMap(d -> ZKPaths.getNodeFromPath(d.getPath()), d -> toHostAndPort(d.getData())));
        }

        String pathOf(String domainName)
        {
            return ZKPaths.makePath(ZK_CORES, domainName);
        }

        byte[] toBytes(HostAndPort address)
        {
            return address.toString().getBytes(StandardCharsets.US_ASCII);
        }
    }

    /**
     * Client component of the location manager for satellite domains.
     *
     * Its primary task is to discover core domains and create and kill connector cells.
     */
    public class Client implements CellEventListener
    {
        private final Map<String, String> connectors = new HashMap<>();

        public Client()
        {
            addCommandListener(this);
            addCellEventListener(this);
        }

        public void start() throws Exception
        {
        }

        public void close()
        {
        }

        public void update(PathChildrenCacheEvent event)
        {
            LOGGER.debug("{}", event);
            String domain = ZKPaths.getNodeFromPath(event.getData().getPath());
            switch (event.getType()) {
            case CHILD_ADDED:
                try {
                    if (shouldConnectTo(domain)) {
                        connectors.put(domain, startConnector(domain, toHostAndPort(event.getData().getData())));
                    }
                } catch (ExecutionException e) {
                    LOGGER.error("Failed to start tunnel connector to {}: {}", domain, e.getCause());
                } catch (InterruptedException ignored) {
                }
                break;
            case CHILD_REMOVED:
                String cell = connectors.remove(domain);
                if (cell != null) {
                    getNucleus().kill(cell);
                }
                break;
            }
        }

        protected boolean shouldConnectTo(String domain)
        {
            return true;
        }

        @Override
        public void cellDied(CellEvent ce)
        {
            connectors.values().remove((String) ce.getSource());
        }
    }

    /**
     * Client component of location manager for core domains.
     *
     * Its task is to allow a listener to register itself in ZooKeeper and to connect to
     * core domains with a domain name lexicographically smaller than the local domain.
     */
    public class CoreClient extends Client
    {
        @Override
        protected boolean shouldConnectTo(String domain)
        {
            return domain.compareTo(getCellDomainName()) < 0;
        }

        @Override
        public void start() throws Exception
        {
            int port = startListener(String.join(" ", args.getArguments()));
            HostAndPort address = HostAndPort.fromParts(InetAddress.getLocalHost().getCanonicalHostName(), port);
            coreDomains.setLocalAddress(address);
        }
    }

    /**
     * Usage : ... [-legacy=<port>] [-role=satellite|core] -- [<port>] <client options>
     */
    public LocationManager(String name, String args) throws CommandException, IOException
    {
        super(name, "System", args);

        this.args = getArgs();
        coreDomains = new CoreDomains(getCellDomainName(), getCuratorFramework());

        if (this.args.hasOption("role")) {
            role = CellDomainRole.valueOf(this.args.getOption("role").toUpperCase());
            switch (role) {
            case CORE:
                checkArgument(this.args.argc() >= 1, "Listening port is required.");
                client = new CoreClient();
                coreDomains.onChange(event -> invokeOnMessageThread(() -> client.update(event)));
                break;
            default:
                client = new Client();
                coreDomains.onChange(event -> invokeOnMessageThread(() -> client.update(event)));
                break;
            }
        } else {
            role = null;
            client = null;
        }
    }

    @Override
    protected void started()
    {
        try {
            coreDomains.start();
            if (client != null) {
                client.start();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            LOGGER.error("Failed to start location manager: {}", e.getCause().toString());
            kill();
        } catch (RuntimeException e) {
            LOGGER.error("Failed to start location manager", e);
            kill();
        } catch (Exception e) {
            LOGGER.error("Failed to start location manager: {}", e.toString());
            kill();
        }
    }

    @Override
    public void stopping()
    {
        CloseableUtils.closeQuietly(coreDomains);
        if (client != null) {
            client.close();
        }
    }

    private int startListener(String args) throws ExecutionException, InterruptedException
    {
        String cellName = "l*";
        String cellClass = "dmg.cells.network.LocationMgrTunnel";
        String cellArgs = args + ' ' + cellClass + ' ' + "-prot=raw" + " -role=" + role;
        LOGGER.info("Starting acceptor with arguments: {}", cellArgs);
        LoginManager c = new LoginManager(cellName, "System", cellArgs);
        c.start().get();
        LOGGER.info("Created : {}", c);

        return c.getListenPort();
    }

    private String startConnector(String remoteDomain, HostAndPort address) throws ExecutionException, InterruptedException
    {
        String cellName = "c-" + remoteDomain + '*';
        String clientKey = args.getOpt("clientKey");
        clientKey = (clientKey != null) && (!clientKey.isEmpty()) ? ("-clientKey=" + clientKey) : "";
        String clientName = args.getOpt("clientUserName");
        clientName = (clientName != null) && (!clientName.isEmpty()) ? ("-clientUserName=" + clientName) : "";

        String cellArgs =
                "-domain=" + remoteDomain + ' '
                + "-lm=" + getCellName() + ' '
                + "-role=" + role + ' '
                + "-where=" + address + ' '
                + clientKey + ' '
                + clientName;
        LOGGER.info("Starting connector with {}", cellArgs);
        LocationManagerConnector c = new LocationManagerConnector(cellName, cellArgs);
        c.start().get();
        LOGGER.info("Created : {}", c);
        return c.getCellName();
    }

    @Command(name = "ls", hint = "list core domains",
            description = "Provides information on available core domains.")
    class ListCommand implements Callable<String>
    {
        @Override
        public String call() throws Exception
        {
            ColumnWriter writer = new ColumnWriter()
                    .header("NAME").left("name").space()
                    .header("ADDRESS").left("address");
            for (Map.Entry<String, HostAndPort> entry : coreDomains.cores().entrySet()) {
                writer.row()
                        .value("name", entry.getKey())
                        .value("address", entry.getValue());

            }
            return writer.toString();
        }
    }

    private static HostAndPort toHostAndPort(byte[] bytes)
    {
        return (bytes == null) ? null : HostAndPort.fromString(new String(bytes, StandardCharsets.US_ASCII));
    }
}
