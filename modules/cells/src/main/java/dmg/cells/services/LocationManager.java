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

import ch.qos.logback.core.util.CloseUtil;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteSource;
import com.google.common.io.CharStreams;
import com.google.common.net.HostAndPort;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.nodes.PersistentNode;
import org.apache.curator.utils.CloseableUtils;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.Immutable;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.PrintWriter;
import java.io.Serializable;
import java.io.StringWriter;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.function.BiFunction;
import java.util.function.Function;

import dmg.cells.network.LocationManagerConnector;
import dmg.cells.nucleus.CellAdapter;
import dmg.cells.nucleus.CellDomainRole;
import dmg.cells.nucleus.CellNucleus;
import dmg.cells.nucleus.CellRoute;
import dmg.cells.nucleus.DelayedReply;
import dmg.cells.nucleus.Reply;
import dmg.cells.services.login.LoginManager;
import dmg.util.CommandException;
import dmg.util.CommandExitException;
import dmg.util.CommandInterpreter;
import dmg.util.command.Argument;
import dmg.util.command.Command;
import dmg.util.command.CommandLine;
import dmg.util.command.Option;

import org.dcache.util.Args;
import org.dcache.util.ColumnWriter;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Iterables.filter;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toMap;

/**
 * The location manager establishes the cell communication topology.
 *
 * In server mode it pushes configuration information to a ZooKeeper node, while
 * in client mode it fetches this configuration and interprets it for the local
 * domain. A domain instructed to listen for incoming connections registers itself
 * with ZooKeeper such that other domains can locate it.
 */
public class LocationManager extends CellAdapter
{
    private static final Logger LOGGER =
            LoggerFactory.getLogger(LocationManager.class);

    private static final String ZK_LISTENERS = "/dcache/lm/listeners";
    private static final String ZK_CONFIG = "/dcache/lm/config";

    private final RemoteLocationManagerConfig config;
    private final ListeningDomains listeningDomains;
    private final Server server;
    private final Client client;
    private final Args args;
    private final CellNucleus nucleus;

    /**
     * A configuration of the location manager.
     *
     * The configuration can be serialized as a sequence of shell commands. This class
     * is a command interpreter for such a sequence of commands.
     *
     * Instances are created from a versioned UTF-8 encoded representation of this
     * serialized form. Although the input data is versioned, instances of this class
     * are mutable. The version is not changed when the configuration is modified -
     * the version merely represent the configuration from which the instance was
     * originally created (e.g. to be used for optimistic locking).
     *
     * The class is not thread safe when mutated. It is however clonable, so the typical
     * pattern is to clone the configuration, apply modifications to the clone and then
     * atomically replace the original configuration (typically through ZooKeeper).
     */
    public static class LocationManagerConfig extends CommandInterpreter implements Cloneable
    {
        private final int version;
        private final Date createdAt = new Date();
        private Map<String, NodeInfo> nodes = new HashMap<>();

        LocationManagerConfig()
        {
            version = -1;
        }

        LocationManagerConfig(int version, byte[] data) throws CommandExitException
        {
            this.version = version;
            try {
                for (String s : ByteSource.wrap(data).asCharSource(StandardCharsets.UTF_8).readLines()) {
                    command(s);
                }
            } catch (IOException e) {
                Throwables.propagate(e);
            }
        }

        @Override
        protected LocationManagerConfig clone()
        {
            try {
                LocationManagerConfig clone = (LocationManagerConfig) super.clone();
                clone.nodes = nodes.entrySet().stream().collect(toMap(Map.Entry::getKey, Map.Entry::getValue));
                return clone;
            } catch (CloneNotSupportedException e) {
                throw Throwables.propagate(e);
            }
        }

        /**
         * Returns the UTF-8 encoded form of the serialized configuration.
         */
        byte[] getData()
        {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            PrintWriter pw = new PrintWriter(out);
            print(pw);
            pw.flush();
            return out.toByteArray();
        }

        /**
         * Returns the ZooKeeper node version number from which ths configuration was created.
         */
        int getVersion()
        {
            return version;
        }

        void print(PrintWriter pw)
        {
            pw.println("#");
            pw.println("# This setup was created by the LocationManager at " + createdAt);
            pw.println("#");
            for (NodeInfo info : nodes.values()) {
                info.print(pw);
            }
        }

        NodeInfo get(String nodeName)
        {
            return nodes.get(nodeName);
        }

        Collection<NodeInfo> nodes()
        {
            return Collections.unmodifiableCollection(nodes.values());
        }

        void reset(List<String> lines) throws IOException, CommandException
        {
            nodes.clear();
            for (String line : lines) {
                if (line.length() >= 1 && line.charAt(0) != '#') {
                    LOGGER.info("Exec : {}", line);
                    command(new Args(line));
                }
            }
        }

        public String ac_define_$_1(Args args)
        {
            nodes.computeIfAbsent(args.argv(0), NodeInfo::new);
            return "";
        }

        public String ac_undefine_$_1(Args args)
        {
            String nodeName = args.argv(0);
            nodes.remove(nodeName);
            for (Map.Entry<String, NodeInfo> entry : nodes.entrySet()) {
                entry.setValue(entry.getValue().removeConnection(nodeName));
            }
            return "";
        }

        public String ac_role_$_2(Args args)
        {
            CellDomainRole role = CellDomainRole.valueOf(args.argv(1).toUpperCase());
            nodes.computeIfPresent(args.argv(0), (domain, info) -> info.setRole(role));
            return "";
        }

        public String ac_nodefaultroute_$_1(Args args)
        {
            nodes.computeIfPresent(args.argv(0), (domain, info) -> info.setDefault(null));
            return "";
        }

        public String ac_defaultroute_$_2(Args args)
        {
            nodes.computeIfAbsent(args.argv(1), NodeInfo::new);
            nodes.compute(args.argv(0), createOrUpdate(n -> n.setDefault(args.argv(1))));
            return "";
        }

        public String ac_connect_$_2(Args args)
        {
            nodes.compute(args.argv(1), createOrUpdate(n -> n.setListen(true)));
            nodes.compute(args.argv(0), createOrUpdate(n -> n.addConnection(args.argv(1))));
            return "";
        }

        public String ac_disconnect_$_2(Args args)
        {
            nodes.computeIfPresent(args.argv(0), (domain, info) -> info.removeConnection(args.argv(1)));
            return "";
        }

        public String ac_listen_$_1_99(Args args)
        {
            int port = args.getIntOption("port", 0);
            String security = args.getOpt("security");
            for (int i = 0; i < args.argc(); i++) {
                nodes.compute(args.argv(i), createOrUpdate(
                        info -> {
                            if (port > 0) {
                                info = info.setPort(port);
                            }
                            if (security != null && security.length() > 0 && !security.equalsIgnoreCase("none")) {
                                info = info.setSecurity(security);
                            }
                            return info.setListen(true);
                        }));
            }
            return "";
        }

        public String ac_unlisten_$_1_99(Args args)
        {
            for (int i = 0; i < args.argc(); i++) {
                nodes.computeIfPresent(args.argv(i), (domain, info) -> info.setListen(false));
            }
            return "";
        }

        private BiFunction<String, NodeInfo, NodeInfo> createOrUpdate(Function<NodeInfo, NodeInfo> f)
        {
            return (domain, node) -> f.apply((node == null) ? new NodeInfo(domain) : node);
        }

        @Immutable
        static class NodeInfo
        {
            private final String domainName;
            private final String defaultRoute;
            private final boolean listen;
            private final int port;
            private final CellDomainRole role;
            private final String sec;
            private final ImmutableList<String> connections;

            NodeInfo(String domainName)
            {
                this(domainName, null, false, 0, CellDomainRole.SATELLITE, null, ImmutableList.of());
            }

            NodeInfo(String domainName, String defaultRoute, boolean listen, int port, CellDomainRole role, String sec,
                     ImmutableList<String> connections)
            {
                this.domainName = domainName;
                this.defaultRoute = defaultRoute;
                this.listen = listen;
                this.port = port;
                this.role = role;
                this.sec = sec;
                this.connections = connections;
            }

            String getDomainName()
            {
                return domainName;
            }

            NodeInfo setRole(CellDomainRole role)
            {
                return (this.role == role)
                       ? this : new NodeInfo(domainName, defaultRoute, listen, port, role, sec, connections);
            }

            CellDomainRole getRole()
            {
                return role;
            }

            NodeInfo setDefault(String defaultRoute)
            {
                return Objects.equals(defaultRoute, this.defaultRoute)
                       ? this : new NodeInfo(domainName, defaultRoute, listen, port, role, sec, connections);
            }

            String getDefault()
            {
                return defaultRoute;
            }

            int getConnectionCount()
            {
                return connections.size();
            }

            Collection<String> connections()
            {
                return connections;
            }

            NodeInfo addConnection(String connection)
            {
                if (!connections.contains(connection)) {
                    return new NodeInfo(domainName, defaultRoute, listen, port, role, sec,
                                        ImmutableList.copyOf(concat(connections, singletonList(connection))));
                }
                return this;
            }

            NodeInfo removeConnection(String connection)
            {
                if (connections.contains(connection)) {
                    return new NodeInfo(domainName, defaultRoute, listen, port, role, sec,
                                        ImmutableList.copyOf(filter(connections, c -> !c.equals(connection))));
                }
                return this;
            }

            NodeInfo setPort(int port)
            {
                return (port == this.port)
                       ? this : new NodeInfo(domainName, defaultRoute, listen, port, role, sec, connections);
            }

            int getPort()
            {
                return port;
            }

            NodeInfo setSecurity(String sec)
            {
                return Objects.equals(sec, this.sec)
                       ? this : new NodeInfo(domainName, defaultRoute, listen, port, role, sec, connections);
            }

            NodeInfo setListen(boolean listen)
            {
                return (listen == this.listen)
                       ? this : new NodeInfo(domainName, defaultRoute, listen, port, role, sec, connections);
            }

            boolean mustListen()
            {
                return listen;
            }

            String getSecurity()
            {
                return sec;
            }

            String toWhatToDoReply()
            {
                StringBuilder sb = new StringBuilder();
                sb.append(domainName).append(" ");
                if (listen) {
                    sb.append("\"l:");
                    if (port > 0) {
                        sb.append(port);
                    }
                    sb.append(":");
                    if (sec != null) {
                        sb.append(sec);
                    }
                    sb.append(":");
                    sb.append('"');
                } else {
                    sb.append("nl");
                }
                for (String node : connections()) {
                    sb.append(" c:").append(node);
                }
                if (defaultRoute != null) {
                    sb.append(" d:").append(defaultRoute);
                }
                return sb.toString();
            }

            void print(PrintWriter pw)
            {
                String domainName = this.domainName;
                pw.append("define ").append(domainName).println();
                if (listen) {
                    pw.append("listen ").append(domainName);
                    if (port > 0) {
                        pw.append(" -port=").append(String.valueOf(port));
                    }
                    pw.println();
                }
                if (role != CellDomainRole.SATELLITE) {
                    pw.append("role ").append(domainName).append(' ').append(role.toString().toLowerCase()).println();
                }
                String def = defaultRoute;
                if (def != null) {
                    pw.append("defaultroute ").append(domainName).append(' ').append(def).println();
                }
                for (String node : connections) {
                    pw.append("connect ").append(domainName).append(" ").append(node).println();
                }
            }
        }
    }

    /**
     * Wraps a LocationManagerConfig and keeps it up to do date with configuration
     * stored in a ZooKeeper node.
     *
     * The synchronization is only happening in one direction. Any modifications
     * to the local modification are not pushed to ZooKeeper and will be overwritten
     * the next time the ZooKeeper node gets updated.
     */
    private static class RemoteLocationManagerConfig implements Closeable
    {
        private final NodeCache remoteConfiguration;

        volatile LocationManagerConfig localConfiguration;

        RemoteLocationManagerConfig(CuratorFramework client)
        {
            localConfiguration = new LocationManagerConfig();
            remoteConfiguration = new NodeCache(client, ZK_CONFIG);
            remoteConfiguration.getListenable().addListener(() -> {
                ChildData currentData = remoteConfiguration.getCurrentData();
                if (currentData != null) {
                    localConfiguration =
                            new LocationManagerConfig(currentData.getStat().getVersion(), currentData.getData());
                } else {
                    localConfiguration = new LocationManagerConfig();
                }
            });
        }

        void start() throws Exception
        {
            remoteConfiguration.start();
        }

        @Override
        public void close()
        {
            CloseUtil.closeQuietly(remoteConfiguration);
        }

        void print(PrintWriter pw)
        {
            localConfiguration.print(pw);
        }

        Collection<LocationManagerConfig.NodeInfo> nodes()
        {
            return localConfiguration.nodes();
        }

        LocationManagerConfig.NodeInfo get(String name)
        {
            return localConfiguration.get(name);
        }

        LocationManagerConfig copy()
        {
            return localConfiguration;
        }
    }

    @FunctionalInterface
    private interface Update<T>
    {
        T apply(LocationManagerConfig config) throws Exception;
    }

    /**
     * Represents a group of listening domains in ZooKeeper. For each
     * listening domain the socket address is registered. May be used
     * by non-listening domains too to learn about listening domains.
     */
    private static class ListeningDomains implements Closeable
    {
        private final String domainName;
        private final CuratorFramework client;
        private final PathChildrenCache listeners;

        /* Only created if the local domain is a listener. */
        private PersistentNode _listener;

        ListeningDomains(String domainName, CuratorFramework client)
        {
            this.domainName = domainName;
            this.client = client;
            listeners = new PathChildrenCache(client, ZK_LISTENERS, true);
        }

        void start() throws Exception
        {
            listeners.start();
        }

        @Override
        public void close() throws IOException
        {
            CloseableUtils.closeQuietly(listeners);
            if (_listener != null) {
                CloseableUtils.closeQuietly(_listener);
            }
        }

        synchronized HostAndPort getLocalAddress()
        {
            return (_listener == null) ? null : toHostAndPort(_listener.getData());
        }

        synchronized void setLocalAddress(HostAndPort address) throws Exception
        {
            if (_listener == null) {
                PersistentNode node = new PersistentNode(client, CreateMode.EPHEMERAL, false, pathOf(domainName), toBytes(address));
                node.start();
                _listener = node;
            } else {
                _listener.setData(toBytes(address));
            }
        }

        HostAndPort readAddressOf(String domainName)
        {
            ChildData data = listeners.getCurrentData(pathOf(domainName));
            return (data == null) ? null : toHostAndPort(data.getData());
        }

        Map<String,HostAndPort> domains()
        {
            return listeners.getCurrentData().stream()
                    .collect(toMap(d -> ZKPaths.getNodeFromPath(d.getPath()), d -> toHostAndPort(d.getData())));
        }

        String pathOf(String domainName)
        {
            return ZKPaths.makePath(ZK_LISTENERS, domainName);
        }

        byte[] toBytes(HostAndPort address)
        {
            return address.toString().getBytes(StandardCharsets.US_ASCII);
        }

        HostAndPort toHostAndPort(byte[] bytes)
        {
            return (bytes == null) ? null : HostAndPort.fromString(new String(bytes, StandardCharsets.US_ASCII));
        }
    }

    /**
     * Server component of the location manager, i.e. the location manager daemon.
     *
     * It's main task is to provide admin shell commands to manipulate the
     * location manager configuration and to push this configuration to ZooKeeper.
     *
     * For backwards compatibility is also provides a UDP server on which pre-2.16 domains
     * main discover the location manager configuration and message brokers. We do not
     * support pre-2.16 message brokers (aka listening domains).
     */
    public class Server implements Runnable, Closeable
    {
        private final int port;
        private final DatagramSocket socket;
        private final Thread worker;
        private final boolean isStrict;
        private final RemoteCommands remoteCommands = new RemoteCommands();

        private final CuratorFramework client;
        private final LocationManagerConfig seedConfiguration;

        private File setupFile;

        public Server(int port, boolean isStrict, File setupFile, LocationManagerConfig seedConfiguration)
                throws SocketException
        {
            this.client = getCuratorFramework();
            this.port = port;
            this.isStrict = isStrict;
            this.setupFile = setupFile;
            this.seedConfiguration = seedConfiguration;

            socket = new DatagramSocket(this.port);
            worker = nucleus.newThread(this, "Server");
        }

        public void start()
        {
            createNode();
        }

        /**
         * Creates the node in ZooKeeper unless it is already present. Will retry until
         * it succeeds. Admin shell commands are not registered until the node has been
         * created or is confirmed to already exist.
         */
        private void createNode()
        {
            try {
                client.create().creatingParentsIfNeeded().inBackground((client, event) -> {
                    if (event.getResultCode() == KeeperException.Code.NODEEXISTS.intValue() ||
                        event.getResultCode() == KeeperException.Code.OK.intValue()) {
                        addCommandListener(this);
                        worker.start();
                    } else {
                        LOGGER.warn("ZooKeeper failure during {} creation [{}]", ZK_CONFIG, event.getResultCode());
                        invokeLater(this::createNode);
                    }
                }).forPath(ZK_CONFIG, seedConfiguration.getData());
            } catch (Exception e) {
                Throwables.propagate(e);
            }
        }

        /**
         * Shutdown the server. Notice that the method will not wait
         * for the worker thread to shut down.
         */
        @Override
        public void close()
        {
            worker.interrupt();
            socket.close();
        }

        /**
         * Apply an update to the configuration and provide the result through a delayed reply. The
         * updated configuration is pushed to ZooKeeper.
         *
         * Takes care of recreating the node and detecting hidden updates.
         */
        private <T extends Serializable> Reply apply(DelayedReply reply, Update<T> f)
        {
            LocationManagerConfig config = LocationManager.this.config.copy();
            try {
                T result = f.apply(config);
                byte[] data = config.getData();
                int version = config.getVersion();

                if (version == -1) {
                    client.create().creatingParentsIfNeeded().inBackground((client, event) -> {
                        if (event.getResultCode() == KeeperException.Code.NODEEXISTS.intValue()) {
                            // ZooKeeper notification ordering rules assert that localConfiguration
                            // will have been updated by now.
                            apply(reply, f);
                        } else if (event.getResultCode() == KeeperException.Code.OK.intValue()) {
                            reply.reply(result);
                        } else {
                            reply.reply(new CommandException("ZooKeeper failure [" + event.getResultCode() + "]"));
                        }
                    }).forPath(ZK_CONFIG, data);
                } else {
                    client.setData().withVersion(version).inBackground((client, event) -> {
                        if (event.getResultCode() == KeeperException.Code.NONODE.intValue() ||
                            event.getResultCode() == KeeperException.Code.BADVERSION.intValue()) {
                            // ZooKeeper notification ordering rules assert that localConfiguration
                            // will have been updated by now.
                            apply(reply, f);
                        } else if (event.getResultCode() == KeeperException.Code.OK.intValue()) {
                            reply.reply(result);
                        } else {
                            reply.reply(new CommandException("ZooKeeper failure [" + event.getResultCode() + "]"));
                        }
                    }).forPath(ZK_CONFIG, data);
                }
            } catch (Exception e) {
                reply.reply(e);
            }
            return reply;
        }

        private <T extends Serializable> Reply update(Update<T> f) throws Exception
        {
            return apply(new DelayedReply(), f);
        }

        public void getInfo(PrintWriter pw)
        {
            pw.println("      # of nodes : " + config.nodes().size());
            if (setupFile != null) {
                pw.println("      Setup file : " + setupFile);
            }
        }

        @Override
        public String toString()
        {
            return "Server:Nodes=" + config.nodes().size();
        }

        @Override
        public void run()
        {
            /* Legacy UDP location manager daemon.
             */
            DatagramPacket packet;
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    packet = new DatagramPacket(new byte[1024], 1024);
                    socket.receive(packet);
                } catch (SocketException e) {
                    if (!Thread.currentThread().isInterrupted()) {
                        LOGGER.warn("Exception in Server receive loop (exiting)", e);
                    }
                    break;
                } catch (Exception ie) {
                    LOGGER.warn("Exception in Server receive loop (exiting)", ie);
                    break;
                }
                try {
                    process(packet);
                    socket.send(packet);
                } catch (Exception se) {
                    LOGGER.warn("Exception in send ", se);
                }
            }
            socket.close();
        }

        public void process(DatagramPacket packet) throws Exception
        {
            byte[] data = packet.getData();
            int datalen = packet.getLength();
            InetAddress address = packet.getAddress();
            if (datalen <= 0) {
                LOGGER.warn("Empty Packet arrived from {}", packet.getAddress());
                return;
            }
            String message = new String(data, 0, datalen);
            LOGGER.info("server query : [{}] ({}) {}", address, message.length(), message);
            Args args = new Args(message);
            message = (args.argc() == 0) ? "" : (String) remoteCommands.command(args);

            LOGGER.info("server reply : {}", message);
            data = message.getBytes();
            packet.setData(data);
            packet.setLength(data.length);
        }

        @Command(name = "define", hint = "add domain configuration",
                description = "Add configuration of a domain.")
        class DefineCommand implements Callable<Reply>
        {
            @Argument(usage = "A dCache domain name. An asterix may be used to add a default " +
                              "configuration that applies to domain that have not been explicitly " +
                              "configured.")
            String domainName;

            @CommandLine
            Args args;

            @Override
            public Reply call() throws Exception
            {
                return update(c -> c.ac_define_$_1(args));
            }
        }

        @Command(name = "undefine", hint = "remove domain configuration",
                description = "Removes configuration of a domain.")
        class UndefineCommand implements Callable<Reply>
        {
            @Argument(usage = "A dCache domain name.")
            String domainName;

            @CommandLine
            Args args;

            @Override
            public Reply call() throws Exception
            {
                return update(c -> c.ac_undefine_$_1(args));
            }
        }

        @Command(name = "role", hint = "set role of domain in cell topology",
                description = "Domains are either satellite or core domains. Satellite domains are the default " +
                              "role.\n\n" +
                              "Core domains are central hubs in the messaging topology, forwarding messages from " +
                              "other domains. Satellite domains can forward messages from other domains too, but " +
                              "core domains have a number of unique properties:\n\n" +
                              "- all domains automatically connect to all core domains;\n" +
                              "- satellite domains automatically and dynamically add a default\n" +
                              "  route to any core domain they connect to;\n" +
                              "- messages from core domains are not forwarded;\n" +
                              "- core domains publish their well known cells and topics to\n" +
                              "  connected core domains;\n" +
                              "- core domains publish their well known cells to connected\n" +
                              "  satellite domains.\n\n" +
                              "These properties enable installations with multiple core domains and a redundant " +
                              "cell message topology with multiple paths between domains.")
        class RoleCommand implements Callable<Reply>
        {
            @Argument(usage = "A dCache domain name.", index = 0)
            String domainName;

            @Argument(usage = "Role of the domain in the cell topology.", index = 1)
            CellDomainRole role;

            @CommandLine
            Args args;

            @Override
            public Reply call() throws Exception
            {
                return update(c -> c.ac_role_$_2(args));
            }
        }

        @Command(name = "nodefaultroute", hint = "clear default route",
                description = "Configures a domain not to install a default route.")
        class NoDefaultRouteCommand implements Callable<Reply>
        {
            @Argument(usage = "A dCache domain name.")
            String domainName;

            @CommandLine
            Args args;

            @Override
            public Reply call() throws Exception
            {
                return update(c -> c.ac_nodefaultroute_$_1(args));
            }
        }

        @Command(name = "defaultroute", hint = "set default route",
                description = "Configures a domain to install a default route to another domain.")
        class DefaultRouteCommand implements Callable<Reply>
        {
            @Argument(index = 0, usage = "A dCache domain name.")
            String domainName;

            @Argument(index = 1, usage = "dCache domain name of the default route target.")
            String upstream;

            @CommandLine
            Args args;

            @Override
            public Reply call() throws Exception
            {
                return update(c -> c.ac_defaultroute_$_2(args));
            }
        }

        @Command(name = "connect", hint = "connect to another domain",
                description = "Configures a domain to connect to another domain.")
        class ConnectCommand implements Callable<Reply>
        {
            @Argument(index = 0, usage = "A dCache domain name.")
            String doainName;

            @Argument(index = 1, usage = "The name of the dCache domain to connect to.")
            String destinationDomain;

            @CommandLine
            Args args;

            @Override
            public Reply call() throws Exception
            {
                return update(c -> c.ac_connect_$_2(args));
            }
        }

        @Command(name = "disconnect", hint = "stop connecting to another domain",
                description = "Configures a domain not to connect to another domain.")
        class DisconnectCommand implements Callable<Reply>
        {
            @Argument(index = 0, usage = "A dCache domain name.")
            String domainName;

            @Argument(index = 1, usage = "The name of the dCache domain not to connect to.")
            String destinationDomain;

            @CommandLine
            Args args;

            @Override
            public Reply call() throws Exception
            {
                return update(c -> c.ac_disconnect_$_2(args));
            }
        }

        @Command(name = "listen", hint = "listen for cell connections",
                description = "Configures a domain to listen for incoming cell connections.")
        class ListenCommand implements Callable<Reply>
        {
            @Argument(usage = "A dCache domain name.")
            String domainName;

            @Option(name = "port", usage = "A TCP port.")
            Integer port;

            @Option(name = "security", usage = "Security settings.")
            String sec;

            @CommandLine
            Args args;

            @Override
            public Reply call() throws Exception
            {
                return update(c -> c.ac_listen_$_1_99(args));
            }
        }

        @Command(name = "unlisten", hint = "stop listening for cell connections",
                description = "Configures domains not to listen for incoming cell connections.")
        class UnlistenCommand implements Callable<Reply>
        {
            @Argument(usage = "A dCache domain name.")
            String[] domainName;

            @CommandLine
            Args arg;

            @Override
            public Reply call() throws Exception
            {
                return update(c -> c.ac_unlisten_$_1_99(args));
            }
        }

        @Command(name = "reload", hint = "load configuration",
                description = "Loads the setup file and resets the location manager configuration. " +
                              "Note that the configuration is persistent in ZooKeeper and except for " +
                              "the first time the location manage starts, issuing this command is the " +
                              "only way to load the setup file.")
        class ReloadCommand implements Callable<String>
        {
            @Option(name = "yes", required = true,
                    usage = "Confirms that the current setup should be destroyed and replaced " +
                            "with the one on disk.")
            boolean confirmed;

            @Override
            public String call() throws Exception
            {
                checkState(setupFile != null, "Setup file is undefined.");
                checkState(setupFile.exists(), setupFile + " does not exist.");
                checkArgument(confirmed, "Required option is missing.");
                update(c -> {
                    c.reset(Files.readAllLines(setupFile.toPath(), StandardCharsets.UTF_8));
                    return null;
                });
                return "";
            }
        }

        @Command(name = "save", hint = "save configuration",
                description = "Saves the current location manager configuration to the setup file.")
        class SaveCommand implements Callable<String>
        {
            @Override
            public String call() throws Exception
            {
                if (setupFile == null) {
                    throw new IllegalStateException("Setup file is undefined.");
                }

                File tmpFile = new File(setupFile.getParent(), "$-" + setupFile.getName());
                try (PrintWriter pw = new PrintWriter(new FileWriter(tmpFile))) {
                    config.print(pw);
                }
                if (!tmpFile.renameTo(setupFile)) {
                    throw new IOException("Failed to write setup.");
                }
                return "";
            }
        }

        @Command(name = "show setup", hint = "show configuration",
                description = "Outputs the current location manager configuration. The " +
                              "information is similar to that provided by ls node, except " +
                              "the configuration file format is used.")
        class ShowSetupCommand implements Callable<String>
        {
            @Override
            public String call() throws Exception
            {
                StringWriter sw = new StringWriter();
                PrintWriter pw = new PrintWriter(sw);
                config.print(pw);
                pw.flush();
                sw.flush();
                return sw.getBuffer().toString();
            }
        }

        @Command(name = "ls", hint = "list node configurations",
                description = "Provides information on how domains must be connected.")
        class ListCommand implements Callable<String>
        {
            @Option(name = "listening", usage = "Show listening domains rather than configured domains.")
            boolean mustShowListening;

            @Option(name = "v", usage = "Show listening address in addition to static configuration.")
            boolean isVerbose;

            @Override
            public String call() throws Exception
            {
                ColumnWriter writer;
                if (mustShowListening) {
                    writer = new ColumnWriter()
                            .header("NAME").left("name").space()
                            .header("ADDRESS").left("address");
                    for (Map.Entry<String, HostAndPort> entry : listeningDomains.domains().entrySet()) {
                        writer.row()
                                .value("name", entry.getKey())
                                .value("address", entry.getValue());

                    }
                } else {
                    writer = new ColumnWriter()
                            .header("NAME").left("name").space()
                            .header("ROLE").left("role").space()
                            .header("UPSTREAM").left("default").space()
                            .header("SECURITY").left("sec").space()
                            .header("LISTEN").left("listen").space();
                    if (isVerbose) {
                        writer.header("ADDRESS").left("address").space();
                    }
                    writer.header("CONNECT TO").left("connect");
                    for (LocationManagerConfig.NodeInfo info : config.nodes()) {
                        writer.row()
                                .value("name", info.getDomainName())
                                .value("role", info.getRole())
                                .value("default", info.getDefault())
                                .value("sec", info.getSecurity())
                                .value("listen", info.mustListen() ? (info.getPort() == 0 ? "true" : info.getPort()) : "")
                                .value("address", listeningDomains.readAddressOf(info.getDomainName()))
                                .value("connect", info.connections().stream().collect(joining(",")));
                    }
                }
                return writer.toString();
            }
        }

        /**
         * Legacy UDP remote commands used for backwards compatibility with pre 2.16 pools.
         *
         * Maps whatToDo and whereIs commands to equivalent ZooKeeper representations.
         */
        @Deprecated // drop in 2.17
        public class RemoteCommands extends CommandInterpreter
        {
            public static final String hh_whatToDo = "<domainName>";
            public String ac_whatToDo_$_1(Args args)
            {
                LocationManagerConfig.NodeInfo info = config.get(args.argv(0));
                if (info == null) {
                    if (isStrict || ((info = config.get("*")) == null)) {
                        throw new IllegalArgumentException("Domain not defined : " + args.argv(0));
                    }

                }
                String serial = args.getOpt("serial");
                return "do" + (serial != null ? " -serial=" + serial : "") + " " + info.toWhatToDoReply();
            }

            public static final String hh_whereIs = "<domainName>";
            public String ac_whereIs_$_1(Args args)
            {
                String domainName = args.argv(0);

                HostAndPort address = listeningDomains.readAddressOf(domainName);
                if (address == null) {
                    throw new IllegalArgumentException("Domain not listening: " + domainName);
                }

                StringBuilder sb = new StringBuilder();
                sb.append("location");
                String serial = args.getOpt("serial");
                if (serial != null) {
                    sb.append(" -serial=").append(serial);
                }
                sb.append(" ").append(domainName);
                sb.append(" ").append(address);
                LocationManagerConfig.NodeInfo info = config.get(domainName);
                if (info != null) {
                    String security = info.getSecurity();
                    if (security != null) {
                        sb.append(" -security=\"").append(security).append("\"");
                    }
                }

                return sb.toString();
            }
        }
    }

    /**
     * Client component of the location manager.
     *
     * Its primary task is to fetch the location manager configuration from ZooKeeper and
     * interpret it for the local domain. This includes creating listeners, connectors
     * and installing default routes.
     *
     * It provides support for listeners and connectors to register the local address and
     * to query the address of other listeners in ZooKeeper.
     */
    public class Client implements Runnable
    {
        private final Thread whatToDo;
        private String toDo;
        private int state;

        Client() throws CommandExitException
        {
            addCommandListener(this);
            whatToDo = nucleus.newThread(this, "WhatToDo");
        }

        synchronized void boot()
        {
            if (!args.hasOption("noboot")) {
                whatToDo.start();
            }
        }

        /**
         * Shutdown the client. Notice that the method will not wait
         * for the worker thread to shut down.
         */
        synchronized void halt()
        {
            whatToDo.interrupt();
        }

        synchronized void getInfo(PrintWriter pw)
        {
            HostAndPort localAddress = listeningDomains.getLocalAddress();
            pw.println("            ToDo : " + (state > -1 ? ("Still Busy (" + state + ")") : toDo));
            pw.println("      Registered : " + (localAddress == null ? "no" : localAddress));
        }

        @Override
        public synchronized String toString()
        {
            return "" + (state > -1 ? ("Client<init>(" + state + ")") : "ClientReady");
        }

        public synchronized String ac_where_is_$_1(Args args)
        {
            String domain = args.argv(0);

            HostAndPort address = listeningDomains.readAddressOf(domain);
            if (address == null) {
                throw new IllegalArgumentException("Domain not listening: " + domain);
            }

            LocationManagerConfig.NodeInfo info = config.get(domain);
            StringBuilder sb = new StringBuilder();
            sb.append("location");
            sb.append(" ").append(domain);
            sb.append(" ").append(address);

            if (info != null) {
                String security = info.getSecurity();
                if (security != null) {
                    sb.append(" -security=\"").append(security).append("\"");
                }
            }
            return sb.toString();
        }

        public synchronized String ac_listening_on_$_2(Args args) throws Exception
        {
            int port = Integer.parseInt(args.argv(1));
            HostAndPort address = HostAndPort.fromParts(InetAddress.getLocalHost().getCanonicalHostName(), port);
            listeningDomains.setLocalAddress(address);
            return "";
        }

        private void startListener(int port, String securityContext, CellDomainRole role) throws Exception
        {
            String cellName = "l*";
            String cellClass = "dmg.cells.network.LocationMgrTunnel";
            String protocol;
            if ((securityContext == null) ||
                (securityContext.length() == 0) ||
                (securityContext.equalsIgnoreCase("none"))) {
                protocol = "-prot=raw";
            } else {
                protocol = securityContext;
            }
            String cellArgs = port + " " + cellClass + " " + protocol + " -lm=" + getCellName() + " -role=" + role;
            LOGGER.info("Starting acceptor with arguments: {}", cellArgs);
            LoginManager c = new LoginManager(cellName, "System", cellArgs);
            c.start().get();
            LOGGER.info("Created : {}", c);
        }

        private void startConnector(final String remoteDomain, CellDomainRole role)
                throws Exception
        {
            String cellName = "c-" + remoteDomain + "*";
            String clientKey = args.getOpt("clientKey");
            clientKey = (clientKey != null) && (clientKey.length() > 0) ? ("-clientKey=" + clientKey) : "";
            String clientName = args.getOpt("clientUserName");
            clientName = (clientName != null) && (clientName.length() > 0) ? ("-clientUserName=" + clientName) : "";

            String cellArgs =
                    "-domain=" + remoteDomain + " "
                    + "-lm=" + getCellName() + " "
                    + "-role=" + role + " "
                    + clientKey + " "
                    + clientName;
            LOGGER.info("LocationManager starting connector with {}", cellArgs);
            LocationManagerConnector c = new LocationManagerConnector(cellName, cellArgs);
            c.start().get();
            LOGGER.info("Created : {}", c);
        }

        private void setDefaultRoute(String domain)
        {
            nucleus.routeAdd(new CellRoute(null, "*@" + domain, CellRoute.DEFAULT));
        }

        @Override
        public void run()
        {
            while (true) {
                synchronized (this) {
                    state++;
                }

                try {
                    LocationManagerConfig.NodeInfo info = config.get(getCellDomainName());
                    if (info == null) {
                        info = config.get("*");
                    }
                    if (info != null) {
                        synchronized (info) {
                            if (info.mustListen()) {
                                startListener(info.getPort(), info.getSecurity(), info.getRole());
                            }
                            for (String domain : info.connections()) {
                                startConnector(domain, info.getRole());
                            }
                            String defaultRoute = info.getDefault();
                            if (defaultRoute != null) {
                                setDefaultRoute(defaultRoute);
                            }
                        }

                        synchronized (this) {
                            toDo = info.toWhatToDoReply();
                            state = -1;
                        }

                        LOGGER.info("whatToDo finished");
                        return;
                    }

                    LOGGER.info(toDo = "whatToDo : Domain not defined: " + getCellDomainName());
                } catch (InterruptedException ie) {
                    LOGGER.warn(toDo = "whatToDo : interrupted");
                    break;
                } catch (InterruptedIOException ie) {
                    LOGGER.warn(toDo = "whatToDo : interrupted(io)");
                    break;
                } catch (Exception ee) {
                    LOGGER.warn(toDo = "whatToDo : exception : " + ee);
                }
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException iie) {
                    LOGGER.warn(toDo = "whatToDo : interrupted sleep");
                    break;
                }
            }
        }
    }

    /**
     * Usage : ... [<port>] -noclient
     * Server Options : -strict=[yes|no] -setup=<setupFile>
     */
    public LocationManager(String name, String args) throws CommandException, IOException
    {
        super(name, "System", args);

        this.args = getArgs();
        nucleus = getNucleus();

        checkArgument(this.args.argc() <= 1, "Usage : ... [<port>] [-noclient]");

        config = new RemoteLocationManagerConfig(getCuratorFramework());
        listeningDomains = new ListeningDomains(getCellDomainName(), getCuratorFramework());

        if (this.args.argc() == 0) {
            server = null;
        } else {
            // Determine setup mode
            String setupFileName = this.args.getOpt("setup");
            File setupFile = (setupFileName == null) ? null : new File(setupFileName);

            // Load default configuration
            LocationManagerConfig seedConfiguration = new LocationManagerConfig();
            String defaults = this.args.getOption("defaults");
            if (defaults != null) {
                try {
                    seedConfiguration.reset(CharStreams.readLines(getDomainContextReader(defaults)));
                    LOGGER.info("Loaded default configuration from context {}.", defaults);
                } catch (FileNotFoundException e) {
                    LOGGER.error("Default context {} is undefined.", defaults);
                }
            }

            // Load configuration file
            if (setupFile != null && setupFile.exists()) {
                seedConfiguration.reset(Files.readAllLines(setupFile.toPath(), StandardCharsets.UTF_8));
                LOGGER.info("Loaded setup file {}.", setupFile);
            }

            // Create server
            int port = Integer.parseInt(this.args.argv(0));
            String strict = this.args.getOpt("strict");
            boolean isStrict = strict == null || !strict.equals("off") && !strict.equals("no");

            server = new Server(port, isStrict, setupFile, seedConfiguration);
        }

        client = this.args.hasOption("noclient") ? null : new Client();
    }

    @Override
    protected void startUp() throws Exception
    {
        config.start();
        listeningDomains.start();
    }

    @Override
    protected void started()
    {
        if (server != null) {
            server.start();
            LOGGER.info("Server Setup Done");
        }
        if (client != null) {
            client.boot();
            LOGGER.info("Client started");
        }
    }

    @Override
    public void cleanUp()
    {
        if (server != null) {
            server.close();
        }
        if (client != null) {
            client.halt();
        }
        CloseableUtils.closeQuietly(listeningDomains);
        CloseableUtils.closeQuietly(config);
    }

    @Override
    public void getInfo(PrintWriter pw)
    {
        if (client != null) {
            pw.println("Client\n--------");
            client.getInfo(pw);
        }
        if (server != null) {
            pw.println("Server\n--------");
            server.getInfo(pw);
        }
    }

    @Override
    public String toString()
    {
        if (client != null && server != null) {
            return client + ";" + server;
        } else if (client != null) {
            return client.toString();
        } else if (server != null) {
            return server.toString();
        } else {
            return "";
        }
    }
}
