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

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.net.HostAndPort;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.utils.CloseableUtils;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException.ConnectionLossException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.SocketFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
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
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import dmg.cells.network.LocationManagerConnector;
import dmg.cells.nucleus.CellAdapter;
import dmg.cells.nucleus.CellDomainRole;
import dmg.cells.nucleus.CellEvent;
import dmg.cells.nucleus.CellEventListener;
import dmg.cells.services.login.LoginManager;
import dmg.cells.zookeeper.LmPersistentNode;
import dmg.cells.zookeeper.LmPersistentNode.PersistentNodeException;
import dmg.util.CommandException;
import dmg.util.command.Argument;
import dmg.util.command.Command;

import org.dcache.ssl.CanlSslSocketCreator;
import org.dcache.util.Args;
import org.dcache.util.ColumnWriter;

import static com.google.common.base.Preconditions.checkArgument;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * The location manager establishes the cell communication topology.
 */
public class LocationManager extends CellAdapter
{
    private static final Logger LOGGER =
            LoggerFactory.getLogger(LocationManager.class);

    private static final String ZK_CORES_PLAIN = "/dcache/lm/cores";
    private static final String ZK_CORES_URI = "/dcache/lm/cores-uri";
    private static final String ZK_CORE_CONFIG = "/dcache/lm/core-config";

    private final CoreDomains coreDomains;
    private final CoreConfig  coreConfig;
    private final Args args;
    private final CellDomainRole role;
    private final Client client;

    enum State
    {
        BRING_UP,
        TEAR_DOWN
    }

    public enum ConnectionType
    {
        PLAIN("none"),
        TLS("tls");

        private static final ImmutableMap<String,ConnectionType> CONFIG_TO_VALUE =
                ImmutableMap.of(PLAIN._config, PLAIN, TLS._config, TLS);

        private final String _config;

        ConnectionType(String config)
        {
            _config = config;
        }

        public static Optional<ConnectionType> fromConfig(String value)
        {
            return Optional.ofNullable(CONFIG_TO_VALUE.get(value));
        }
    }

    enum Mode
    {
        PLAIN("none"),
        PLAIN_TLS("none,tls"),
        TLS("tls");

        private final String _mode;

        private static final ImmutableSet<Mode> tls = ImmutableSet.of(Mode.TLS);
        private static final ImmutableSet<Mode> plain = ImmutableSet.of(Mode.PLAIN);
        private static final ImmutableSet<Mode> plainAndTls = ImmutableSet.of(Mode.PLAIN, Mode.TLS);

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

        public Set<Mode> getModeAsSet()
        {
            switch (this) {
                case PLAIN_TLS:
                    return plainAndTls;
                case TLS:
                    return tls;
                default:
                    return plain;
            }
        }

        public static Mode fromString(String mode)
        {
            String m = filterAndSort(mode);

            for (Mode b : Mode.values()) {
                if (b._mode.equalsIgnoreCase(m)) {
                    return b;
                }
            }
            throw new IllegalArgumentException("No Mode of type: " + mode);
        }

        public static boolean isValid(String mode)
        {
            String m = filterAndSort(mode);

            for (Mode b : Mode.values()) {
                if (b._mode.equalsIgnoreCase(m)) {
                    return true;
                }
            }
            return false;
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

    enum Type {
        URI,
        PLAIN
    }

    private static class CoreDomainInfo
    {
        protected URI tls;            // tls://hostname:port
        protected URI tcp;            // tcp://hostname:port

        protected CoreDomainInfo() {
        }

        void addCore(String scheme, String host, int port)
        {
            switch (scheme) {
            case "tls":
                tls = URI.create(String.format("%s://%s:%s", scheme, host, port));
                break;
            case "tcp":
                tcp = URI.create(String.format("%s://%s:%s", scheme, host, port));
                break;
            default:
                LOGGER.warn("Unknown Scheme {} for LocationManager Cores", scheme);
            }
        }

        public void removeTcp() {
            tcp = null;
        }

        public void removeTls() {
            tls = null;
        }

        byte[] toBytes()
        {
            try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(baos)) {
                if (tls != null) {
                    out.writeUTF(tls.toString());
                }
                if (tcp != null) {
                    out.writeUTF(tcp.toString());
                }
                return baos.toByteArray();
            } catch (IOException ie) {
                LOGGER.warn("Failed to serialize CoreDomain Info {}", this);
            }
            return new byte[0];
        }

        Optional<HostAndPort> tcpPort()
        {
            return Optional.ofNullable(tcp).map(tcp -> HostAndPort.fromParts(tcp.getHost(), tcp.getPort()));
        }

        Optional<HostAndPort> tlsPort()
        {
            return Optional.ofNullable(tls).map(tls -> HostAndPort.fromParts(tls.getHost(), tls.getPort()));
        }

        boolean isCompatible(CoreDomainInfo other)
        {
            return Optional.ofNullable(other)
                           .map(o -> tlsPort().map(h -> o.tlsPort()
                                                         .map(h::equals)
                                                         .orElse(true))
                                              .orElse(true)
                                  && tcpPort().map(p -> o.tcpPort()
                                                         .map(p::equals)
                                                         .orElse(true))
                                              .orElse(true))
                           .orElse(false);
        }

        public static Type infoTypefromZKPath(String path) {
            if (ZKPaths.getPathAndNode(path).getPath().equals(ZK_CORES_URI)) {
                return Type.URI;
            } else {
                return Type.PLAIN;
            }
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
            return Objects.equals(tls, that.tls) && Objects.equals(tcp, that.tcp);
        }

        @Override
        public int hashCode() {
            return Objects.hash(tls, tcp);
        }

        @Override
        public String toString() {
            return String.format("%s, %s", tls, tcp);
        }
    }

    private static class CoreDomainInfoUri extends CoreDomainInfo
    {
        public CoreDomainInfoUri(byte[] bytes)
        {
            try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
                 DataInputStream in = new DataInputStream(bais)) {
                while (in.available() > 0) {
                    String info = in.readUTF();
                    URI entry = URI.create(info);
                    switch (entry.getScheme()) {
                        case "tls":
                            tls = entry;
                            break;
                        case "tcp":
                            tcp = entry;
                            break;
                        default:
                            LOGGER.warn("Unknown Scheme for LocationManager Cores: {}; tried URI and HostAndPort", entry);
                            break;
                    }
                }
            } catch (IOException ie) {
                throw new IllegalArgumentException("Failed deserializing LocationManager Cores as uri: {}", ie.getCause());
            }
        }
    }

    private static class CoreDomainInfoPlain extends CoreDomainInfo
    {
        public CoreDomainInfoPlain(byte[] bytes)
        {
            HostAndPort hostAndPort = toHostAndPort(bytes);
            tcp = URI.create(String.format("%s://%s:%s", "tcp", hostAndPort.getHost(), hostAndPort.getPort()));
        }
    }

    private static class BadConfigException extends Exception
    {
        public BadConfigException() {
            super();
        }

        public BadConfigException(String message) {
            super(message);
        }
    }

    /**
     * Represents a group of listening domains in ZooKeeper. For each
     * listening domain the socket address is registered. May be used
     * by non-listening domains too to learn about listening domains.
     */
    private abstract static class CoreDomains implements Closeable
    {
        private final String domainName;
        private final CuratorFramework client;
        protected final PathChildrenCache cores;


        /* Only created if the local domain is a core. */
        private LmPersistentNode localPlain;
        private LmPersistentNode localUri;

        protected CoreDomains(String domainName, CuratorFramework client, PathChildrenCache cores)
        {
            this.domainName = domainName;
            this.client = client;
            this.cores = cores;
        }

        public static CoreDomains createWithMode(String domainName, CuratorFramework client, String mode)
                throws BadConfigException
        {
            LOGGER.info("Creating CoreDomains: {}, {}", domainName, mode);
            ConnectionType type = ConnectionType.fromConfig(mode).orElseThrow(() -> new BadConfigException("Bad mode " + mode));

            switch (type) {
            case PLAIN:
                return new CoreDomainsPlain(domainName, client);
            case TLS:
                return new CoreDomainsTls(domainName, client);
            default:
                throw new BadConfigException("Unexpected mode " + mode);
            }
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
            if (localPlain != null) {
                CloseableUtils.closeQuietly(localPlain);
            }
            if (localUri != null) {
                CloseableUtils.closeQuietly(localUri);
            }
        }

        void setCoreDomainInfo(CoreDomainInfo coreDomainInfo) throws PersistentNodeException
        {
            // For backwards compatibility by adding zookeeper hostname:port to /dcache/lm/cores
            setCoreDomainInfoOld(coreDomainInfo);

            // Add zookeeper entries in the format scheme://hostname:port to /dcache/lm/cores-uri
            // e.g. tls://hostname:port, tcp://hostname:port
            setCoreDomainInfoUri(coreDomainInfo);
        }

        Map<String,CoreDomainInfo> cores()
        {
            return Collections.emptyMap();
        }

        String pathOf(String core, String domainName)
        {
            return ZKPaths.makePath(core, domainName);
        }

        private void setCoreDomainInfoUri(CoreDomainInfo coreDomainInfo) throws PersistentNodeException
        {
            localUri = LmPersistentNode.createOrUpdate(client,
                                                        pathOf(ZK_CORES_URI, domainName),
                                                        coreDomainInfo,
                                                        CoreDomainInfo::toBytes,
                                                        localUri);
        }

        private void setCoreDomainInfoOld(CoreDomainInfo coreDomainInfo) throws PersistentNodeException
        {
            if (coreDomainInfo.tcpPort().isPresent()) {
                localPlain = LmPersistentNode.createOrUpdate(client,
                                                            pathOf(ZK_CORES_PLAIN, domainName),
                                                            coreDomainInfo.tcpPort().get(),
                                                            address -> address.toString()
                                                                              .getBytes(StandardCharsets.US_ASCII),
                                                            localPlain);
            }
        }
    }

    private static class CoreDomainsTls extends CoreDomains
    {
        CoreDomainsTls(String domainName, CuratorFramework client)
        {
            super(domainName, client, new PathChildrenCache(client, ZK_CORES_URI, true));
        }

        @Override
        Map<String,CoreDomainInfo> cores()
        {
            Map<String, CoreDomainInfo> coresInfo = new HashMap<>();
            for (ChildData d: cores.getCurrentData()) {
                coresInfo.put(ZKPaths.getNodeFromPath(d.getPath()), new CoreDomainInfoUri(d.getData()));
            }
            return coresInfo;
        }
    }

    private static class CoreDomainsPlain extends CoreDomains
    {
        CoreDomainsPlain(String domainName, CuratorFramework client)
        {
            super(domainName, client, new PathChildrenCache(client, ZK_CORES_PLAIN, true));
        }

        @Override
        Map<String,CoreDomainInfo> cores()
        {
            Map<String, CoreDomainInfo> coresInfo = new HashMap<>();
            for (ChildData d: cores.getCurrentData()) {
                coresInfo.put(ZKPaths.getNodeFromPath(d.getPath()), new CoreDomainInfoPlain(d.getData()));
            }
            return coresInfo;
        }
    }

    private class CoreConfig implements NodeCacheListener, Closeable
    {
        private final CuratorFramework _curator;

        /**
         * Current modes extracted from the CoreDomain configuration.
         */
        private Mode _mode = Mode.PLAIN;

        /**
         * Cache of the ZooKeeper node identified by {@code _node}.
         */
        private final NodeCache _cache;

        /**
         * Stat of the last value loaded from the ZooKeeper node identified by {@code _node}.
         */
        private Stat _current;

        /**
         * Callable to reset the CoreDomains when a change in config is detected
         */
        private final BiConsumer<Mode, State> _reset;

        CoreConfig(CuratorFramework curator, BiConsumer<Mode, State> f)
        {
            _curator = curator;
            _cache = new NodeCache(_curator, ZK_CORE_CONFIG);
            _reset = f;
        }

        public Mode getMode()
        {
            return _mode;
        }

        synchronized void start() throws Exception
        {
            _cache.getListenable().addListener(this);
            try {
                _cache.start(true);
                apply(_cache.getCurrentData());
            } catch (ConnectionLossException e) {
                LOGGER.warn("Failed to connect to zookeeper, using mode {} until connection reestablished", _mode);
            }
        }

        private void apply(ChildData currentData) {
            if (currentData == null) {
                _current = null;
                _mode = Mode.PLAIN;
                LOGGER.info("CoreDomain config node " + ZK_CORE_CONFIG + " not present; assuming mode {}", _mode);
            } else if (_current == null || currentData.getStat().getVersion() > _current.getVersion()) {
                _mode = Mode.fromString(new String(currentData.getData(), UTF_8));
                LOGGER.info("CoreDomain config node " + ZK_CORE_CONFIG + " switching to mode {}", _mode);
                _current = currentData.getStat();
            } else {
                LOGGER.info("Ignoring spurious CoreDomain config node " + ZK_CORE_CONFIG + " updated");
            }
        }

        @Override
        public synchronized void nodeChanged() throws Exception
        {
            Set<Mode> oldModes = _mode.getModeAsSet();
            apply(_cache.getCurrentData());
            Set<Mode> curModes = _mode.getModeAsSet();

            // old           cur        down     up
            // none,tls   -  tls    =   none
            // none,tls  -  none    =   tls
            // none      -   tls    =   none     tls
            // tls       -  none    =   tls      none
            // none  -  none,tls    =            tls
            // tls   -  none,tls    =            none

            Set<Mode> up = Sets.difference(curModes, oldModes).copyInto(new HashSet<>());
            LOGGER.info("Following modes from CoreDomain are being brought up: [{}]",
                    Joiner.on(',').join(up));
            up.stream().forEach(u -> _reset.accept(u, State.BRING_UP));

            Set<Mode> down = Sets.difference(oldModes, curModes).copyInto(new HashSet<>());
            LOGGER.info("Following modes from CoreDomain are being taken down: [{}]",
                            Joiner.on(',').join(down));
            down.stream().forEach(d -> _reset.accept(d, State.TEAR_DOWN));
        }

        @Override
        public void close()
        {
            CloseableUtils.closeQuietly(_cache);
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
        private Map<String, CoreDomainInfo> infoFromPlain = new HashMap<>();
        private Map<String, CoreDomainInfo> infoFromUri = new HashMap<>();

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

        public void reset(Mode mode, State state)
        {
        }

        public void update(PathChildrenCacheEvent event)
        {
            LOGGER.info("{}", event);
            String cell;

            switch (event.getType()) {
            case CHILD_REMOVED:
                cell = connectors.remove(ZKPaths.getNodeFromPath(event.getData().getPath()));
                if (cell != null) {
                    killConnector(cell);
                }
                break;
            case CHILD_UPDATED:
                cell = connectors.remove(ZKPaths.getNodeFromPath(event.getData().getPath()));
                if (cell != null) {
                    killConnector(cell);
                }
                // fall through
            case CHILD_ADDED:
                //Log if the Core Domain Information received is incompatible with previous
                CoreDomainInfo info = infoFromZKEvent(event);
                String domain = ZKPaths.getNodeFromPath(event.getData().getPath());

                try {
                    if (shouldConnectTo(domain)) {
                        cell = connectors.remove(domain);
                        if (cell != null) {
                            LOGGER.error("About to create tunnel to core domain {}, but to my surprise " +
                                         "a tunnel called {} already exists. Will kill it. Please contact " +
                                         "support@dcache.org.", domain, cell);
                            killConnector(cell);
                        }
                        cell = connectors.put(domain, startConnector(domain, info));
                        if (cell != null) {
                            LOGGER.error("Created a tunnel to core domain {}, but to my surprise " +
                                         "a tunnel called {} already exists. Will kill it. Please contact " +
                                         "support@dcache.org.", domain, cell);
                            killConnector(cell);
                        }
                    }
                } catch (ExecutionException e) {
                    LOGGER.error("Failed to start tunnel connector to {}: {}", domain, e.getCause());
                } catch (InterruptedException ignored) {
                } catch (BadConfigException be) {
                    LOGGER.error("Invalid ports provided for starting connector in mode {}", args.getOpt("mode"));
                }
                break;
            }
        }

        protected CoreDomainInfo infoFromZKEvent(PathChildrenCacheEvent event)
        {
            String path = event.getData().getPath();
            String domain = ZKPaths.getNodeFromPath(path);

            CoreDomainInfo info;
            CoreDomainInfo plain = infoFromPlain.get(domain);
            CoreDomainInfo uri = infoFromUri.get(domain);

            switch (CoreDomainInfo.infoTypefromZKPath(path))
            {
            case PLAIN:
                info = new CoreDomainInfoPlain(event.getData().getData());
                infoFromPlain.put(domain, info);
                logCompatibilityCheck(event, info, uri);
                break;
            case URI:
                info = new CoreDomainInfoUri(event.getData().getData());
                infoFromUri.put(domain, info);
                logCompatibilityCheck(event, info, plain);
                break;
            default:
                LOGGER.error("CoreDomainInfo can't be extracted from ZK Event");
                info = new CoreDomainInfo();
            }
            return info;
        }

        private void logCompatibilityCheck(PathChildrenCacheEvent event,
                                           CoreDomainInfo received,
                                           CoreDomainInfo existing)
        {
            if (existing != null && !received.isCompatible(existing)) {
                LOGGER.warn("CoreDomainInfo received from ZK Node {} which is different from the previously " +
                            "received values, check core domain configuration", event.getData().getPath());
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
     * core domains with a domain name lexicographically smaller than the localPlain domain.
     */
    public class CoreClient extends Client
    {
        private LoginManager lmTls;
        private LoginManager lmPlain;
        private volatile CoreDomainInfo info = new CoreDomainInfo();

        @Override
        protected boolean shouldConnectTo(String domain)
        {
            return domain.compareTo(getCellDomainName()) < 0;
        }

        @Override
        public void start() throws Exception
        {
            switch (coreConfig.getMode()) {
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
                throw new IllegalArgumentException("Mode " + coreConfig.getMode() + "not supported for Core Domain");
            }
            coreDomains.setCoreDomainInfo(info);
        }

        @Override
        public void close() {
            coreConfig.close();
        }

        @Override
        public synchronized void reset(Mode mode, State state)
        {
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
                coreDomains.setCoreDomainInfo(info);
            } catch (PersistentNodeException e) {
                LOGGER.error("Failed to reset location manager on CoreConfig update: {}", e.getMessage());
            }  catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (ExecutionException e) {
                LOGGER.error("Failed to reset location manager on CoreConfig update: {}", e.getCause().toString());
                kill();
            } catch (RuntimeException e) {
                LOGGER.error("Failed to reset location manager on CoreConfig update", e);
                kill();
            } catch (Exception e) {
                LOGGER.error("Failed to reset location manager on CoreConfig update: {}", e.toString());
                kill();
            }
        }

        private void startListenerWithTcp()
                throws ExecutionException, InterruptedException, UnknownHostException
        {
            String cellArgs = String.format("%s -netmask='%s'",
                                            args.argv(0),
                                            args.getOption("netmask", ""));
            lmPlain = startListener(cellArgs);
            LOGGER.info("lmPlain: {}; port; {} ", lmPlain, lmPlain.getListenPort());
            info.addCore("tcp", InetAddress.getLocalHost().getCanonicalHostName(), lmPlain.getListenPort());
        }

        private void startListenerWithTls()
                throws ExecutionException, InterruptedException, UnknownHostException
        {
            checkArgument(args.hasOption("socketfactory"),
                    "No Socketfactory provided to Core Domain for channel encryption");

            String cellArgs = String.format("%s -socketfactory='%s'",
                    Integer.parseInt((args.argc() == 1) ? args.argv(0) : args.argv(1)),
                    args.getOpt("socketfactory"));
            lmTls = startListener(cellArgs);
            LOGGER.info("lmTls: {}; port; {} ", lmTls, lmTls.getListenPort());
            info.addCore("tls", InetAddress.getLocalHost().getCanonicalHostName(), lmTls.getListenPort());
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
     * Usage : ... [-legacy=<port>] [-role=satellite|core] -mode=none|tls -- [<port>] <client options>
     */
    public LocationManager(String name, String args) throws CommandException, IOException, BadConfigException
    {
        super(name, "System", args);
        this.args = getArgs();

        coreDomains = CoreDomains.createWithMode(getCellDomainName(), getCuratorFramework(), this.args.getOpt("mode"));

        if (this.args.hasOption("role")) {
            role = CellDomainRole.valueOf(this.args.getOption("role").toUpperCase());
            switch (role) {
            case CORE:
                checkArgument(this.args.argc() >= 1, "Listening port is required.");
                client = new CoreClient();
                coreDomains.onChange(client::update);
                coreConfig = new CoreConfig(getCuratorFramework(), client::reset);
                break;
            default:
                client = new Client();
                coreDomains.onChange(client::update);
                coreConfig = null;
                break;
            }
        } else {
            role = null;
            client = null;
            coreConfig = null;
        }
    }

    @Override
    protected void started()
    {
        try {
            coreDomains.start();
            if (coreConfig != null) {
                coreConfig.start();
            }
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
    public void stopping()
    {
        CloseableUtils.closeQuietly(coreDomains);
        if (client != null) {
            client.close();
        }
    }

    private LoginManager startListener(String args) throws ExecutionException, InterruptedException
    {
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
            throws ExecutionException, InterruptedException, BadConfigException
    {
        checkArgument(args.hasOption("mode"), "No mode specified to run connector");

        String cellName = "c-" + remoteDomain + '*';
        String clientKey = args.getOpt("clientKey");
        clientKey = (clientKey != null) && (!clientKey.isEmpty()) ? ("-clientKey=" + clientKey) : "";
        String clientName = args.getOpt("clientUserName");
        clientName = (clientName != null) && (!clientName.isEmpty()) ? ("-clientUserName=" + clientName) : "";

        HostAndPort where;
        SocketFactory socketFactory;
        Mode mode = Mode.fromString(args.getOption("mode"));

        switch(mode) {
        case PLAIN:
            LOGGER.info("Starting Connection in mode: PLAIN with {}", args.getArguments());
            where = domainInfo.tcpPort().orElseThrow(BadConfigException::new);
            socketFactory = SocketFactory.getDefault();
            break;
        case TLS:
            LOGGER.info("Starting Connection in mode: TLS with {}", args.getArguments());
            where = domainInfo.tlsPort().orElseThrow(BadConfigException::new);
            try {
                switch (role) {
                case CORE:
                    socketFactory = new CanlSslSocketCreator(new Args(args.getOption("socketfactory")));
                    break;
                default:
                    args.shift(2);
                    socketFactory = new CanlSslSocketCreator(args);
                }
            } catch (IOException e) {
                throw new IllegalArgumentException(
                                String.format("Problem creating socket factory with arguments: %s", args.toString()));
            }
            break;
        default:
            throw new IllegalArgumentException("Invalid mode to start connector: " + args.getOption("mode"));
        }

        String cellArgs = "-domain=" + remoteDomain + ' '
                + "-lm=" + getCellName() + ' '
                + "-role=" + role + ' '
                + "-where=" + where + ' '
                + clientKey + ' '
                + clientName;

        LOGGER.info("Starting connector with {}", cellArgs);
        LocationManagerConnector c = new LocationManagerConnector(cellName, cellArgs, socketFactory);
        c.start().get();
        return c.getCellName();
    }

    private void killConnector(String cell)
    {
        LOGGER.info("Killing connector {}", cell);
        getNucleus().kill(cell);
    }

    private void killListener(String cell)
    {
        LOGGER.info("Killing listener {}", cell);
        getNucleus().kill(cell);
    }

    @Command(name = "ls", hint = "list core domain information",
            description = "Provides information on available core domains.")
    class ListCommand implements Callable<String>
    {
        @Override
        public String call()
        {
            ColumnWriter writer = new ColumnWriter()
                    .header("NAME").left("name").space()
                    .header("PROTOCOL").left("protocol").space()
                    .header("HOST").left("host").space()
                    .header("PORT").right("port");
            for (Map.Entry<String, CoreDomainInfo> entry : coreDomains.cores().entrySet()) {
                CoreDomainInfo info = entry.getValue();
                info.tcpPort().ifPresent( tcp -> {
                    writer.row()
                          .value("name", entry.getKey())
                          .value("protocol", "PLAIN")
                          .value("host", tcp.getHost())
                          .value("port", tcp.getPort());
                });

                info.tlsPort().ifPresent(tls -> {
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

    @Command(name = "set core-config", hint = "set operating mode for CoreDomain",
                description = "Specify the mode to be none, tls or none,tls in which the CoreDomain should run")
    class SetCoreConfigCommand implements Callable<String>
    {
        @Argument(index = 0,
                usage = "Mode in which CoreDomain should run.")
        String _modes;

        @Override
        public synchronized String call() throws Exception
        {
            CuratorFramework curator = getCuratorFramework();
            Set<String> modes = Sets.newHashSet(_modes.split(","))
                                    .stream()
                                    .map(String::trim)
                                    .filter(s -> !s.isEmpty())
                                    .collect(Collectors.toSet());

            if (modes.stream().allMatch(Mode::isValid))
            {
                String config = Joiner.on(",").join(modes);
                byte[] data = config.getBytes(UTF_8);

                if(curator.checkExists().forPath(ZK_CORE_CONFIG) != null)
                {
                    curator.setData()
                           .forPath(ZK_CORE_CONFIG, data);
                } else {
                    curator.create()
                           .creatingParentContainersIfNeeded()
                           .withMode(CreateMode.PERSISTENT)
                           .forPath(ZK_CORE_CONFIG, data);
                }

                if (Arrays.equals(curator.getData().forPath(ZK_CORE_CONFIG), data))
                {
                    return "Successfully updated CoreDomain mode configuration to " + config;
                } else {
                    return "Could not change CoreDomain configuration to " + config;
                }
            }

            throw new BadConfigException("Invalid Modes provided for CoreDomain configuration. " +
                                            "Valid modes are \"none\", \"tls\" or \"none,tls\"");
        }
    }

    @Command(name = "get core-config", hint = "get current mode of operation for CoreDomain",
            description = "Get the current operating modes of the CoreDomain. It should be one of the following " +
                    "\"none\", \"tls\" or \"none,tls\".")
    class GetCoreConfigCommand implements Callable<String>
    {
        @Override
        public String call() throws Exception
        {
            return new String(getCuratorFramework().getData().forPath(ZK_CORE_CONFIG), UTF_8);
        }
    }

    private static HostAndPort toHostAndPort(byte[] bytes)
    {
        return (bytes == null) ? null : HostAndPort.fromString(new String(bytes, StandardCharsets.US_ASCII));
    }
}
