/*
 * dCache - http://www.dcache.org/
 *
 * Copyright (C) 2016 - 2024 Deutsches Elektronen-Synchrotron
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

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.collect.ImmutableMap;
import com.google.common.net.HostAndPort;
import dmg.cells.network.LocationManagerConnector;
import dmg.cells.nucleus.CellAdapter;
import dmg.cells.nucleus.CellDomainRole;
import dmg.cells.nucleus.CellEvent;
import dmg.cells.nucleus.CellEventListener;
import dmg.cells.services.login.LoginManager;
import dmg.cells.zookeeper.LmPersistentNode;
import dmg.cells.zookeeper.LmPersistentNode.PersistentNodeException;
import dmg.util.CommandException;
import dmg.util.command.Command;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import javax.net.SocketFactory;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent.Type;
import org.apache.curator.utils.CloseableUtils;
import org.apache.curator.utils.ZKPaths;
import org.dcache.ssl.CanlSslSocketCreator;
import org.dcache.util.Args;
import org.dcache.util.ColumnWriter;
import org.dcache.util.NetworkUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The location manager establishes the cell communication topology.
 */
public class LocationManager extends CellAdapter {

    private static final Logger LOGGER =
          LoggerFactory.getLogger(LocationManager.class);

    private static final String ZK_CORES_URI = "/dcache/lm/cores-uri";

    private final CoreDomains coreDomains;
    private final Args args;
    private final CellDomainRole role;
    private final Client client;

    enum State {
        BRING_UP,
        TEAR_DOWN
    }

    public enum ConnectionType {
        PLAIN("none", "tcp"),
        TLS("tls", "tls");

        private static final ImmutableMap<String, ConnectionType> CONFIG_TO_VALUE =
              ImmutableMap.of(PLAIN.config, PLAIN, TLS.config, TLS);

        private final String config;
        private final String scheme;

        ConnectionType(String config, String scheme) {
            this.config = config;
            this.scheme = scheme;
        }

        public static Optional<ConnectionType> fromConfig(String value) {
            return Optional.ofNullable(CONFIG_TO_VALUE.get(value));
        }

        /**
         * Get url scheme corresponding to given connection type.
         *
         * @return scheme corresponding to connection type.
         */
        public String getConnectionScheme() {
            return scheme;
        }
    }

    enum Mode {
        PLAIN("none"),
        PLAIN_TLS("none,tls"),
        TLS("tls");

        private final String _mode;

        Mode(String mode) {
            _mode = mode;
        }

        @Override
        public String toString() {
            return _mode;
        }

        public String getMode() {
            return this._mode;
        }

        public static Mode fromString(String mode) {
            String m = filterAndSort(mode);

            for (Mode b : Mode.values()) {
                if (b._mode.equalsIgnoreCase(m)) {
                    return b;
                }
            }
            throw new IllegalArgumentException("No Mode of type: " + mode);
        }

        private static String filterAndSort(String mode) {
            return Arrays.stream(mode.split(","))
                  .map(String::trim)
                  .filter(s -> !s.isEmpty())
                  .distinct()
                  .sorted()
                  .collect(Collectors.joining(","));
        }
    }

    private static class CoreDomainInfo {

        private final Set<URI> endpoints;

        public CoreDomainInfo() {
            endpoints = new HashSet<>();
        }

        public CoreDomainInfo(byte[] bytes) {
            this();

            try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
                  DataInputStream in = new DataInputStream(bais)) {
                while (in.available() > 0) {
                    String info = in.readUTF();
                    URI entry = URI.create(info);
                    switch (entry.getScheme()) {
                        case "tls":
                        case "tcp":
                            if (canResoveHost(entry)) {
                                endpoints.add(entry);
                            }
                            break;
                        default:
                            LOGGER.warn("Unknown Scheme for LocationManager Cores: {}", entry);
                            break;
                    }
                }
            } catch (IOException ie) {
                throw new IllegalArgumentException(
                      "Failed deserializing LocationManager Cores as uri", ie);
            }
        }

        /**
         * Check if hostname provided by urc can be resolved.
         * @param endpoint core domain endpoint
         * @return true it host name can be resolved. Otherwise false.
         */
        private boolean canResoveHost(URI endpoint) {
            var h = endpoint.getHost();
            if (h == null) {
                LOGGER.warn("Ignoring URL without host: {}", endpoint);
                return false;
            }
            try {
                var ip = java.net.InetAddress.getByName(h);
            } catch (UnknownHostException e) {
                LOGGER.warn("Ignoring unknown host: {} : {}", endpoint, e.toString());
                return false;
            }
            return true;
        }

        void addCore(String scheme, String host, int port) {
            switch (scheme) {
                case "tls":
                case "tcp":
                    endpoints.add(URI.create(String.format("%s://%s:%s", scheme, host, port)));
                    break;
                default:
                    throw new RuntimeException(
                          "Unknown Scheme " + scheme + " for LocationManager Cores");
            }
        }

        public void removeTcp() {
            endpoints.removeIf(u -> u.getScheme().equalsIgnoreCase("tcp"));
        }

        public void removeTls() {
            endpoints.removeIf(u -> u.getScheme().equalsIgnoreCase("tls"));
        }

        byte[] toBytes() {
            try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                  DataOutputStream out = new DataOutputStream(baos)) {
                for (URI uri : endpoints) {
                    out.writeUTF(uri.toString());
                }
                return baos.toByteArray();
            } catch (IOException ie) {
                LOGGER.warn("Failed to serialize CoreDomain Info {}", this);
            }
            return new byte[0];
        }

        public Optional<HostAndPort> getEndpointForSchema(String schema) {
            return endpoints.stream()
                  .filter(u -> u.getScheme().equalsIgnoreCase(schema))
                  .findAny()
                  .map(u -> HostAndPort.fromParts(u.getHost(), u.getPort()));
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (!(other instanceof CoreDomainInfo)) {
                return false;
            }

            CoreDomainInfo that = (CoreDomainInfo) other;
            return Objects.equals(endpoints, that.endpoints);
        }

        @Override
        public int hashCode() {
            return endpoints.hashCode();
        }

        @Override
        public String toString() {
            return endpoints.toString();
        }
    }

    private static class BadConfigException extends Exception {

        public BadConfigException() {
            super();
        }

        public BadConfigException(String message) {
            super(message);
        }
    }

    /**
     * Represents a group of listening domains in ZooKeeper. For each listening domain the socket
     * address is registered. May be used by non-listening domains too to learn about listening
     * domains.
     */
    private abstract static class CoreDomains implements Closeable {

        private final String domainName;
        private final CuratorFramework client;
        protected final PathChildrenCache cores;


        /* Only created if the local domain is a core. */
        private LmPersistentNode<CoreDomainInfo> localUri;

        protected CoreDomains(String domainName, CuratorFramework client, PathChildrenCache cores) {
            this.domainName = domainName;
            this.client = client;
            this.cores = cores;
        }

        public static CoreDomains createWithMode(String domainName, CuratorFramework client,
              String mode)
              throws BadConfigException {
            LOGGER.info("Creating CoreDomains: {}, {}", domainName, mode);
            ConnectionType type = ConnectionType.fromConfig(mode)
                  .orElseThrow(() -> new BadConfigException("Bad mode " + mode));

            return new CoreDomainLocations(domainName, client, type);
        }

        void onChange(Consumer<PathChildrenCacheEvent> consumer) {
            cores.getListenable().addListener((client, event) -> consumer.accept(event));
        }

        void start() throws Exception {
            cores.start();
        }

        @Override
        public void close() throws IOException {
            CloseableUtils.closeQuietly(cores);
            if (localUri != null) {
                CloseableUtils.closeQuietly(localUri);
            }
        }

        Map<String, CoreDomainInfo> cores() {
            return Collections.emptyMap();
        }

        String pathOf(String core, String domainName) {
            return ZKPaths.makePath(core, domainName);
        }

        private void setCoreDomainInfoUri(CoreDomainInfo coreDomainInfo)
              throws PersistentNodeException {
            localUri = LmPersistentNode.createOrUpdate(client,
                  pathOf(ZK_CORES_URI, domainName),
                  coreDomainInfo,
                  CoreDomainInfo::toBytes,
                  localUri);
        }
    }

    private static class CoreDomainLocations extends CoreDomains {

        private final ConnectionType connectionType;

        CoreDomainLocations(String domainName, CuratorFramework client,
              ConnectionType connectionType) {
            super(domainName, client, new PathChildrenCache(client, ZK_CORES_URI, true));
            this.connectionType = connectionType;
        }

        @Override
        Map<String, CoreDomainInfo> cores() {
            Map<String, CoreDomainInfo> coresInfo = new HashMap<>();
            for (ChildData d : cores.getCurrentData()) {
                CoreDomainInfo urlInfo = new CoreDomainInfo(d.getData());

                if (urlInfo.getEndpointForSchema(connectionType.getConnectionScheme())
                      .isPresent()) {
                    coresInfo.put(ZKPaths.getNodeFromPath(d.getPath()), urlInfo);
                }
            }
            return coresInfo;
        }
    }

    /**
     * Client component of the location manager for satellite domains.
     * <p>
     * Its primary task is to discover core domains and create and kill connector cells.
     */
    public class Client implements CellEventListener {

        private final Map<String, String> connectors = new HashMap<>();

        public Client() {
            addCommandListener(this);
            addCellEventListener(this);
        }

        public void start() throws Exception {
        }

        public void close() {
        }

        public void reset(Mode mode, State state) {
        }

        private static boolean hasNoData(ChildData data) {
            return data == null || data.getData() == null || data.getData().length == 0;
        }

        public void update(PathChildrenCacheEvent event) {
            LOGGER.info("{}", event);
            String cell;

            if (hasNoData(event.getData())) {
                LOGGER.warn("Ignoring empty event {}", event.getType());
                return;
            }

            String domain = ZKPaths.getNodeFromPath(event.getData().getPath());

            switch (event.getType()) {
                case CHILD_REMOVED:
                    cell = connectors.remove(domain);
                    if (cell != null) {
                        killConnector(cell);
                    }
                    break;
                case CHILD_UPDATED:
                case CHILD_ADDED:

                    CoreDomainInfo info = infoFromZKEvent(event);
                    if (info.endpoints.isEmpty()) {
                        LOGGER.warn("Ignoring invalid core URI", domain);
                        break;
                    }

                    if (event.getType() == Type.CHILD_UPDATED) {
                        cell = connectors.remove(domain);
                        if (cell != null) {
                            killConnector(cell);
                        }
                    }

                    try {
                        if (shouldConnectTo(domain)) {
                            cell = connectors.put(domain, startConnector(domain, info));
                            if (cell != null) {
                                LOGGER.error(
                                      "Created a tunnel to core domain {}, but to my surprise " +
                                            "a tunnel called {} already exists. Will kill it. Please contact "
                                            +
                                            "support@dcache.org.", domain, cell);
                                killConnector(cell);
                            }
                        }
                    } catch (ExecutionException e) {
                        LOGGER.error("Failed to start tunnel connector to {}: {}", domain,
                              e.getCause());
                    } catch (InterruptedException ignored) {
                    } catch (BadConfigException be) {
                        LOGGER.error("Invalid ports provided for starting connector in mode {}",
                              args.getOpt("mode"));
                    }
                    break;
            }
        }

        protected CoreDomainInfo infoFromZKEvent(PathChildrenCacheEvent event) {
            return new CoreDomainInfo(event.getData().getData());
        }

        protected boolean shouldConnectTo(String domain) {
            return true;
        }

        @Override
        public void cellDied(CellEvent ce) {
            connectors.values().remove((String) ce.getSource());
        }
    }

    /**
     * Client component of location manager for core domains.
     * <p>
     * Its task is to allow a listener to register itself in ZooKeeper and to connect to core
     * domains with a domain name lexicographically smaller than the localPlain domain.
     */
    public class CoreClient extends Client {

        private LoginManager lmTls;
        private LoginManager lmPlain;
        private final Mode connectionType;
        private volatile CoreDomainInfo info = new CoreDomainInfo();

        public CoreClient(Mode connectionType) {
            this.connectionType = connectionType;
        }

        @Override
        protected boolean shouldConnectTo(String domain) {
            return domain.compareTo(getCellDomainName()) < 0;
        }

        @Override
        public void start() throws Exception {
            switch (connectionType) {
                case PLAIN:
                    startListenerWithTcp();
                    break;
                case PLAIN_TLS:
                    startListenerWithTcp();
                    //fall through
                case TLS:
                    startListenerWithTls();
                    break;
                default:
                    throw new IllegalArgumentException(
                          "Mode " + connectionType + "not supported for Core Domain");
            }
            coreDomains.setCoreDomainInfoUri(info);
        }

        @Override
        public synchronized void reset(Mode mode, State state) {
            try {
                switch (mode) {
                    case PLAIN:
                        switch (state) {
                            case BRING_UP:
                                startListenerWithTcp();
                                break;
                            case TEAR_DOWN:
                                stopListenerTcp();
                                break;
                        }
                        break;
                    case TLS:
                        switch (state) {
                            case BRING_UP:
                                startListenerWithTls();
                                break;
                            case TEAR_DOWN:
                                stopListenerTls();
                                break;
                        }
                        break;
                    case PLAIN_TLS:
                        // should not happen
                        break;
                }
                coreDomains.setCoreDomainInfoUri(info);
            } catch (PersistentNodeException e) {
                LOGGER.error("Failed to reset location manager on CoreConfig update: {}",
                      e.getMessage());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (ExecutionException e) {
                LOGGER.error("Failed to reset location manager on CoreConfig update: {}",
                      e.getCause().toString());
                kill();
            } catch (RuntimeException e) {
                LOGGER.error("Failed to reset location manager on CoreConfig update", e);
                kill();
            } catch (Exception e) {
                LOGGER.error("Failed to reset location manager on CoreConfig update: {}",
                      e.toString());
                kill();
            }
        }

        private void startListenerWithTcp()
              throws ExecutionException, InterruptedException, UnknownHostException {
            String cellArgs = String.format("%s -netmask='%s'",
                  args.argv(0),
                  args.getOption("netmask", ""));
            lmPlain = startListener(cellArgs);
            LOGGER.info("lmPlain: {}; port; {} ", lmPlain, lmPlain.getListenPort());
            info.addCore("tcp", NetworkUtils.getCanonicalHostName(),
                  lmPlain.getListenPort());
        }

        private void startListenerWithTls()
              throws ExecutionException, InterruptedException, UnknownHostException {
            checkArgument(args.hasOption("socketfactory"),
                  "No Socketfactory provided to Core Domain for channel encryption");

            String cellArgs = String.format("%s -socketfactory='%s'",
                  Integer.parseInt((args.argc() == 1) ? args.argv(0) : args.argv(1)),
                  args.getOpt("socketfactory"));
            lmTls = startListener(cellArgs);
            LOGGER.info("lmTls: {}; port; {} ", lmTls, lmTls.getListenPort());
            info.addCore("tls", NetworkUtils.getCanonicalHostName(),
                  lmTls.getListenPort());
        }

        private void stopListenerTls() {
            if (lmTls != null) {
                killListener(lmTls.getCellName());
                lmTls = null;
            }
            info.removeTls();
        }

        private void stopListenerTcp() {
            if (lmPlain != null) {
                killListener(lmPlain.getCellName());
                lmPlain = null;
            }
            info.removeTcp();
        }
    }

    /**
     * Usage : ... [-legacy=<port>] [-role=satellite|core] -mode=none|tls -- [<port>] <client
     * options>
     */
    public LocationManager(String name, String args)
          throws CommandException, IOException, BadConfigException {
        super(name, "System", args);
        this.args = getArgs();

        Mode connectionType = Mode.fromString(this.args.getOption("mode"));
        coreDomains = CoreDomains.createWithMode(getCellDomainName(), getCuratorFramework(),
              this.args.getOpt("mode"));

        if (this.args.hasOption("role")) {
            role = CellDomainRole.valueOf(this.args.getOption("role").toUpperCase());
            switch (role) {
                case CORE:
                    checkArgument(this.args.argc() >= 1, "Listening port is required.");
                    client = new CoreClient(connectionType);
                    coreDomains.onChange(client::update);
                    break;
                default:
                    client = new Client();
                    coreDomains.onChange(client::update);
                    break;
            }
        } else {
            role = null;
            client = null;
        }
    }

    @Override
    protected void started() {
        try {
            coreDomains.start();
            if (client != null) {
                client.start();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (RuntimeException e) {
            LOGGER.error("Failed to start location manager", e);
            kill();
        } catch (Exception e) {
            LOGGER.error("Failed to start location manager: {}", e.toString());
            kill();
        }
    }

    @Override
    public void stopping() {
        CloseableUtils.closeQuietly(coreDomains);
        if (client != null) {
            client.close();
        }
    }

    private LoginManager startListener(String args)
          throws ExecutionException, InterruptedException {
        String cellName = "l*";
        String cellClass = "dmg.cells.network.LocationMgrTunnel";
        String cellArgs = args + ' ' + cellClass + ' ' + "-prot=raw" + " -role=" + role;
        LOGGER.info("Starting acceptor with arguments: {}", cellArgs);
        LoginManager c = new LoginManager(cellName, "System", cellArgs);
        c.start().get();
        LOGGER.info("Created : {}", c);
        return c;
    }

    private String startConnector(String remoteDomain, CoreDomainInfo domainInfo)
          throws ExecutionException, InterruptedException, BadConfigException {
        checkArgument(args.hasOption("mode"), "No mode specified to run connector");

        HostAndPort where;
        SocketFactory socketFactory;
        Mode mode = Mode.fromString(args.getOption("mode"));

        switch (mode) {
            case PLAIN:
                LOGGER.info("Starting Connection in mode: PLAIN with {}", args.getArguments());
                where = domainInfo.getEndpointForSchema("tcp").orElseThrow(BadConfigException::new);
                socketFactory = SocketFactory.getDefault();
                break;
            case TLS:
                LOGGER.info("Starting Connection in mode: TLS with {}", args.getArguments());
                where = domainInfo.getEndpointForSchema("tls").orElseThrow(BadConfigException::new);
                try {
                    switch (role) {
                        case CORE:
                            socketFactory = new CanlSslSocketCreator(
                                  new Args(args.getOption("socketfactory")));
                            break;
                        default:
                            args.shift(2);
                            socketFactory = new CanlSslSocketCreator(args);
                    }
                } catch (IOException e) {
                    throw new IllegalArgumentException(
                          String.format("Problem creating socket factory with arguments: %s",
                                args.toString()));
                }
                break;
            default:
                throw new IllegalArgumentException(
                      "Invalid mode to start connector: " + args.getOption("mode"));
        }

        String cellName = "c-" + remoteDomain + '*';
        LOGGER.info("Starting connector {} to {} ({})", cellName, remoteDomain, where);
        InetSocketAddress tunnelEndpoint = new InetSocketAddress(where.getHost(), where.getPort());
        LocationManagerConnector c = new LocationManagerConnector(cellName, socketFactory, remoteDomain, tunnelEndpoint);
        c.start().get();
        return c.getCellName();
    }

    private void killConnector(String cell) {
        LOGGER.info("Killing connector {}", cell);
        getNucleus().kill(cell);
    }

    private void killListener(String cell) {
        LOGGER.info("Killing listener {}", cell);
        getNucleus().kill(cell);
    }

    @Command(name = "ls", hint = "list core domain information",
          description = "Provides information on available core domains.")
    class ListCommand implements Callable<String> {

        @Override
        public String call() {
            ColumnWriter writer = new ColumnWriter()
                  .header("NAME").left("name").space()
                  .header("PROTOCOL").left("protocol").space()
                  .header("HOST").left("host").space()
                  .header("PORT").right("port");
            for (Map.Entry<String, CoreDomainInfo> entry : coreDomains.cores().entrySet()) {
                CoreDomainInfo info = entry.getValue();
                info.getEndpointForSchema("tcp").ifPresent(tcp -> {
                    writer.row()
                          .value("name", entry.getKey())
                          .value("protocol", "PLAIN")
                          .value("host", tcp.getHost())
                          .value("port", tcp.getPort());
                });

                info.getEndpointForSchema("tls").ifPresent(tls -> {
                    writer.row()
                          .value("name", entry.getKey())
                          .value("protocol", "TLS")
                          .value("host", tls.getHost())
                          .value("port", tls.getPort());
                });
            }
            return writer.toString();
        }
    }
}
