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

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Stream;

import dmg.cells.nucleus.CellAdapter;
import dmg.cells.nucleus.CellAddressCore;
import dmg.cells.nucleus.CellDomainInfo;
import dmg.cells.nucleus.CellDomainRole;
import dmg.cells.nucleus.CellEvent;
import dmg.cells.nucleus.CellEventListener;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellNucleus;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.CellRoute;
import dmg.cells.nucleus.CellTunnelInfo;
import dmg.cells.nucleus.FutureCellMessageAnswerable;
import dmg.cells.nucleus.NoRouteToCellException;
import dmg.util.Releases;

import org.dcache.util.Args;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toMap;

/**
 * Routing manager to publish exported cells and topics to peers. The routing manager receives
 * its own updates from its peers and manages WELLKNOWN and TOPIC routes to other domains.
 *
 * Local exports are published to connected satellite domains, while both local exports and
 * local subscriptions are published to connected core domains.
 */
public class CoreRoutingManager
    extends CellAdapter
    implements CellEventListener
{
    private static final Logger LOG =
        LoggerFactory.getLogger(CoreRoutingManager.class);

    private final CellNucleus nucleus;

    /** Names of local cells that have been exported. */
    private final Set<String> localExports = new HashSet<>();

    /** Local cells that subscribed to topics. */
    private final Multimap<String, String> localSubscriptions = HashMultimap.create();

    /** Role of this domain. */
    private final CellDomainRole role;

    /**
     * Routing updates from downstream domains are processed sequentially on a dedicated
     * thread.
     */
    private final ExecutorService executor = Executors.newSingleThreadExecutor(getNucleus());

    /**
     * Map to collapse update messages from connected domains.
     */
    private final ConcurrentMap<String, CoreRouteUpdate> updates = new ConcurrentHashMap<>();

    /**
     * Map to collapse update messages from connected domains.
     */
    private final ConcurrentMap<String, List<String>> legacyWellknownUpdates = new ConcurrentHashMap<>();

    /**
     * Map to collapse update messages from connected domains.
     */
    private final ConcurrentMap<String, Collection<String>> legacyTopicUpdates = new ConcurrentHashMap<>();

    /**
     * Topic routes installed by routing manager.
     */
    private final Multimap<String, String> topicRoutes = HashMultimap.create();

    /**
     * Well known routes installed by routing manager.
     */
    private final Multimap<String, String> wellknownRoutes = HashMultimap.create();

    /**
     * Tunnels to core domains.
     */
    private final Map<CellAddressCore,CellTunnelInfo> coreTunnels = new HashMap<>();

    /**
     * Tunnels to satellite domains.
     */
    private final Map<CellAddressCore,CellTunnelInfo> satelliteTunnels = new HashMap<>();

    /**
     * Tunnels to legacy domains.
     */
    private final Map<CellAddressCore,CellTunnelInfo> legacyTunnels = new HashMap<>();

    /**
     * Default routes that still have to be added.
     *
     * Routing manager when running in a satellite domain adds default routes whenever
     * it sees a new domain route to a core domain. To give other domains time to connect
     * to a newly created core domain, the installation of all but the first default route
     * is delayed for a moment.
     */
    private final List<CellRoute> delayedDefaultRoutes = new ArrayList<>();

    /**
     * A non-system cell that acts as an early warning if the domain is shutting down. Upon shutdown
     * we inform all downstream routing managers to remove their default routes pointing to this
     * domain. That way we can achieve a clean shutdown.
     */
    private volatile CellAdapter canary;

    public CoreRoutingManager(String name, String arguments)
    {
        super(name, "System", arguments);
        nucleus = getNucleus();
        nucleus.addCellEventListener(this);
        role = getArgs().hasOption("role")
                ? CellDomainRole.valueOf(getArgs().getOption("role").toUpperCase())
                : CellDomainRole.SATELLITE;
    }

    @Override
    protected void startUp() throws ExecutionException, InterruptedException
    {
        if (role == CellDomainRole.CORE) {
            canary = new CellAdapter(getCellName() + "-canary", "Generic", "") {
                @Override
                protected void cleanUp()
                {
                    notifyDownstreamOfDomainDeath();
                }

                @Override
                public void getInfo(PrintWriter pw)
                {
                    pw.println("If I die, downstream domains will drop their default routes to my domain.");
                }
            };
            canary.start().get();
        }
    }

    private synchronized void notifyDownstreamOfDomainDeath()
    {
        canary = null;
        try {
            sendToPeers(
                    new PeerShutdownNotification(getCellDomainName()), satelliteTunnels.values(), 1000).get();
        } catch (ExecutionException e) {
            LOG.info("Failed to notify downstream of shutdown: {}", e.toString());
        } catch (InterruptedException ignored) {
        }
    }

    @Override
    public synchronized void cleanUp()
    {
        CellAdapter canary = this.canary;
        if (canary != null) {
            try {
                getNucleus().kill(canary.getCellName());
            } catch (IllegalArgumentException ignored) {
            }
        }
        executor.shutdown();
    }

    @Override
    public synchronized void getInfo(PrintWriter pw)
    {
        pw.append("Local exports: ").println(localExports);
        pw.println();

        pw.println("Local subscriptions:");
        for (Map.Entry<String, Collection<String>> e : localSubscriptions.asMap().entrySet()) {
            pw.append(" ").append(e.getKey()).append(" : ").println(e.getValue());
        }

        pw.println();
        pw.println("Managed topic routes:");
        for (Map.Entry<String,Collection<String>> e : topicRoutes.asMap().entrySet()) {
            pw.append(" ").append(e.getKey()).append(" : ").println(e.getValue());
        }
        pw.println();
        pw.println("Managed well-known routes:");
        for (Map.Entry<String,Collection<String>> e : wellknownRoutes.asMap().entrySet()) {
            pw.append(" ").append(e.getKey()).append(" : ").println(e.getValue());
        }
    }

    private synchronized void sendToCoreDomains()
    {
        sendToPeers(new CoreRouteUpdate(localExports, localSubscriptions.values()), coreTunnels.values());
    }

    private synchronized void sendToSatelliteDomains()
    {
        sendToPeers(new CoreRouteUpdate(localExports), satelliteTunnels.values());
    }

    private void sendToPeers(Serializable msg, Collection<CellTunnelInfo> tunnels)
    {
        CellAddressCore peer = new CellAddressCore(nucleus.getCellName());
        for (CellTunnelInfo tunnel : tunnels) {
            CellAddressCore domain = new CellAddressCore("*", tunnel.getRemoteCellDomainInfo().getCellDomainName());
            nucleus.sendMessage(new CellMessage(new CellPath(domain, peer), msg), false, true);
        }
    }

    private ListenableFuture<List<CellMessage>> sendToPeers(Serializable msg, Collection<CellTunnelInfo> tunnels, long timeout)
    {
        List<FutureCellMessageAnswerable> futures = new ArrayList<>();
        CellAddressCore peer = new CellAddressCore(nucleus.getCellName());
        for (CellTunnelInfo tunnel : tunnels) {
            CellAddressCore domain = new CellAddressCore("*", tunnel.getRemoteCellDomainInfo().getCellDomainName());
            FutureCellMessageAnswerable future = new FutureCellMessageAnswerable();
            futures.add(future);
            nucleus.sendMessage(new CellMessage(new CellPath(domain, peer), msg), false, true,
                                future, MoreExecutors.directExecutor(), timeout);
        }
        return Futures.allAsList(futures);
    }

    private void updateRoutes(String domain, Collection<String> destinations, Multimap<String, String> routes, int type)
    {
        Set<String> newDestinations = new HashSet<>(destinations);
        Iterator<String> iterator = routes.get(domain).iterator();
        while (iterator.hasNext()) {
            String destination = iterator.next();
            if (!newDestinations.remove(destination)) {
                try {
                    nucleus.routeDelete(new CellRoute(destination, "*@" + domain, type));
                    iterator.remove();
                } catch (IllegalArgumentException ignored) {
                    // Route didn't exist
                }
            }
        }
        for (String destination : newDestinations) {
            try {
                nucleus.routeAdd(new CellRoute(destination, "*@" + domain, type));
                routes.put(domain, destination);
            } catch (IllegalArgumentException ignored) {
                // Already exists
            }
        }
    }

    private synchronized void updateTopicRoutes(String domain, Collection<String> topics)
    {
        updateRoutes(domain, topics, topicRoutes, CellRoute.TOPIC);
    }

    private synchronized void updateWellknownRoutes(String domain, Collection<String> cells)
    {
        updateRoutes(domain, cells, wellknownRoutes, CellRoute.WELLKNOWN);
    }

    @Override
    public void messageArrived(CellMessage msg)
    {
        Serializable obj = msg.getMessageObject();
        if (obj instanceof CoreRouteUpdate) {
            String domain = msg.getSourceAddress().getCellDomainName();
            if (updates.put(domain, (CoreRouteUpdate) obj) == null) {
                executor.execute(() -> {
                    CoreRouteUpdate update = updates.remove(domain);
                    updateTopicRoutes(domain, update.getTopics());
                    updateWellknownRoutes(domain, update.getExports());
                });
            }
        } else if (obj instanceof GetAllDomainsRequest) {
            CellTunnelInfo tunnel = Iterables.getFirst(coreTunnels.values(), null);
            if (role == CellDomainRole.SATELLITE && tunnel != null) {
                msg.getDestinationPath().insert(new CellPath(nucleus.getCellName(),
                                                             tunnel.getRemoteCellDomainInfo().getCellDomainName()));
                msg.nextDestination();
                nucleus.sendMessage(msg, false, true);
            } else {
                Map<String, Collection<String>> domains = new HashMap<>();
                synchronized (this) {
                    domains.put(getCellDomainName(), new ArrayList<>(localExports));
                    asList(coreTunnels, satelliteTunnels, legacyTunnels)
                            .stream()
                            .flatMap(map -> map.values().stream())
                            .map(CellTunnelInfo::getRemoteCellDomainInfo)
                            .map(CellDomainInfo::getCellDomainName)
                            .forEach(domain -> domains.put(domain, new ArrayList<>()));
                    wellknownRoutes.asMap().forEach(
                            (domain, cells) -> domains.put(domain, Lists.newArrayList(cells)));
                }
                msg.revertDirection();
                msg.setMessageObject(new GetAllDomainsReply(domains));
                sendMessage(msg);
            }
        } else if (obj instanceof NoRouteToCellException) {
            LOG.info(((NoRouteToCellException) obj).getMessage());
        } else if (obj instanceof String[]) {
            // Legacy support for pre-2.16 pools
            String[] info = (String[]) obj;
            if (info.length < 1) {
                LOG.warn("Protocol error 1 in routing info");
                return;
            }
            final String domain = info[0];
            LOG.info("Routing info arrived for domain {}", domain);

            if (legacyWellknownUpdates.put(domain, asList(info).subList(1, info.length)) == null) {
                executor.execute(() -> updateWellknownRoutes(domain, legacyWellknownUpdates.remove(domain)));
            }
        } else if (obj instanceof TopicRouteUpdate) {
            String domain = msg.getSourceAddress().getCellDomainName();
            if (legacyTopicUpdates.put(domain, ((TopicRouteUpdate) obj).getTopics()) == null) {
                executor.execute(() -> updateTopicRoutes(domain, legacyTopicUpdates.remove(domain)));
            }
        } else if (obj instanceof PeerShutdownNotification) {
            PeerShutdownNotification notification = (PeerShutdownNotification) obj;
            String remoteDomain = notification.getDomainName();
            synchronized (this) {
                coreTunnels.values().stream()
                        .filter(i -> i.getRemoteCellDomainInfo().getCellDomainName().equals(remoteDomain))
                        .forEach(i -> {
                            CellRoute route = new CellRoute(null, i.getTunnel(), CellRoute.DEFAULT);
                            delayedDefaultRoutes.remove(route);
                            if (!hasAlternativeDefaultRoute(route)) {
                                installDefaultRoute();
                            }
                            try {
                                nucleus.routeDelete(route);
                            } catch (IllegalArgumentException ignored) {
                            }
                        });
            }
            msg.revertDirection();
            sendMessage(msg);
        } else {
            LOG.warn("Unidentified message ignored: {}", obj);
        }
    }

    @Override
    public void cellCreated(CellEvent ce)
    {
        String name = (String)ce.getSource();
        LOG.info("Cell created: {}", name);
    }

    @Override
    public synchronized void cellExported(CellEvent ce)
    {
        String name = (String)ce.getSource();
        LOG.info("Cell exported: {}", name);
        localExports.add(name);
        sendToCoreDomains();
        sendToSatelliteDomains();
    }

    @Override
    public synchronized void cellDied(CellEvent ce)
    {
        String name = (String) ce.getSource();
        LOG.info("Cell died: {}", name);
        if (localExports.remove(name)) {
            sendToSatelliteDomains();
            sendToCoreDomains();
        }
    }

    @Override
    public synchronized void routeAdded(CellEvent ce)
    {
        CellRoute cr = (CellRoute) ce.getSource();
        LOG.info("Got 'route added' event: {}", cr);
        switch (cr.getRouteType()) {
        case CellRoute.DOMAIN:
            Optional<CellTunnelInfo> tunnelInfo = getTunnelInfo(cr.getTarget());
            tunnelInfo
                    .filter(i -> i.getRemoteCellDomainInfo().getRole() == CellDomainRole.CORE)
                    .ifPresent(i -> { coreTunnels.put(i.getTunnel(), i); sendToCoreDomains(); });
            tunnelInfo
                    .filter(i -> i.getRemoteCellDomainInfo().getRole() == CellDomainRole.SATELLITE &&
                                 i.getRemoteCellDomainInfo().getRelease() >= Releases.RELEASE_2_16)
                    .ifPresent(i -> { satelliteTunnels.put(i.getTunnel(), i); sendToSatelliteDomains(); });
            tunnelInfo
                    .filter(i -> i.getRemoteCellDomainInfo().getRole() == CellDomainRole.SATELLITE &&
                                 i.getRemoteCellDomainInfo().getRelease() < Releases.RELEASE_2_16)
                    .ifPresent(i -> legacyTunnels.put(i.getTunnel(), i));
            tunnelInfo
                    .filter(i -> i.getLocalCellDomainInfo().getRole() == CellDomainRole.SATELLITE &&
                                 i.getRemoteCellDomainInfo().getRole() == CellDomainRole.CORE)
                    .ifPresent(i -> {
                        delayedDefaultRoutes.add(new CellRoute(null, i.getTunnel(), CellRoute.DEFAULT));
                        if (nucleus.getRoutingTable().hasDefaultRoute()) {
                            invokeLater(this::installDefaultRoute);
                        } else {
                            installDefaultRoute();
                        }
                    });
            break;
        case CellRoute.TOPIC:
            String topic = cr.getCellName();
            CellAddressCore target = cr.getTarget();
            if (target.getCellDomainName().equals(nucleus.getCellDomainName())) {
                localSubscriptions.put(target.getCellName(), topic);
                sendToCoreDomains();
            }
            break;
        case CellRoute.DEFAULT:
            LOG.info("Default route {} was added", cr);
            break;
        }
    }

    @Override
    public synchronized void routeDeleted(CellEvent ce)
    {
        CellRoute cr = (CellRoute) ce.getSource();
        switch (cr.getRouteType()) {
        case CellRoute.DOMAIN:
            updateTopicRoutes(cr.getDomainName(), Collections.emptyList());
            updateWellknownRoutes(cr.getDomainName(), Collections.emptyList());
            getTunnelInfo(cr.getTarget())
                    .map(CellTunnelInfo::getTunnel)
                    .ifPresent(name -> {
                        coreTunnels.remove(name);
                        satelliteTunnels.remove(name);
                        legacyTunnels.remove(name);
                    });
            break;
        case CellRoute.TOPIC:
            String topic = cr.getCellName();
            CellAddressCore target = cr.getTarget();
            if (target.getCellDomainName().equals(nucleus.getCellDomainName())) {
                if (localSubscriptions.remove(target.getCellName(), topic)) {
                    sendToCoreDomains();
                }
            }
            break;
        }
    }

    private synchronized void installDefaultRoute()
    {
        for (CellRoute route : delayedDefaultRoutes) {
            try {
                nucleus.routeAdd(route);
            } catch (IllegalArgumentException e) {
                LOG.info("Failed to add route: {}", e.getMessage());
            }
        }
        delayedDefaultRoutes.clear();
    }

    private boolean hasAlternativeDefaultRoute(CellRoute route)
    {
        return asList(nucleus.getRoutingList()).stream()
                .filter(r -> r.getRouteType() == CellRoute.DEFAULT)
                .anyMatch(r -> !r.equals(route));
    }

    private Optional<CellTunnelInfo> getTunnelInfo(CellAddressCore tunnel)
    {
        return nucleus.getCellTunnelInfos().stream()
                .filter(i -> i.getTunnel().equals(tunnel))
                .findAny();
    }

    /**
     * This method returns the current state of the RoutingMgr cell as a (binary) Object.
     * <p>
     * NB. <b>This is a hack</b>.  The correct method of receiving information from a
     * Cell is via a Vehicle.  However, as the RoutingMgr is within the cells module (which
     * does not have the concept of Vehicles) this cannot be (easily) done.  Instead, we
     * use the existing mechanism of obtaining a binary object via the admin interface and
     * flag this functionality as something that should be improved later.
     *
     * @return a representation of the RoutingManager's little brain.
     */
    public static final String hh_ls = "[-x]";
    @Deprecated
    public synchronized Object ac_ls_$_0(Args args)
    {
        return new Object[] {
                getCellDomainName(),
                Sets.newHashSet(localExports),
                wellknownRoutes.asMap().entrySet().stream().collect(
                        toMap(Map.Entry::getKey, e -> Sets.newHashSet(e.getValue())))
        };
    }

}
