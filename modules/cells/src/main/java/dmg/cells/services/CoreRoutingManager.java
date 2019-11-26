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

import javax.annotation.concurrent.GuardedBy;

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

import org.dcache.util.Args;

import static java.util.stream.Collectors.toMap;

/**
 * Routing manager to publish exported cells and topics to peers. The routing manager receives
 * its own updates from its peers and manages QUEUE and TOPIC routes to other domains.
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

    /** Local cells that consume from named queues. */
    private final Multimap<String, String> localConsumers = HashMultimap.create();

    /** Local cells that subscribed to topics. */
    private final Multimap<String, String> localSubscriptions = HashMultimap.create();

    /** Role of this domain. */
    private final CellDomainRole role;

    /**
     * Routing updates from downstream domains are processed sequentially on a dedicated
     * thread.  In the past we have observed that processing updates with many routes can
     * overwhelm a routing manager and the separate thread allows us to drop repeated
     * updates from the same domain.
     */
    private final ExecutorService executor = Executors.newSingleThreadExecutor(getNucleus());

    /**
     * Map to collapse update messages from connected domains.
     */
    private final ConcurrentMap<String, CoreRouteUpdate> updates = new ConcurrentHashMap<>();

    /**
     * Topic routes installed by routing manager.
     */
    @GuardedBy("this")
    private final Multimap<String, String> topicRoutes = HashMultimap.create();

    /**
     * Well known routes installed by routing manager.
     */
    @GuardedBy("this")
    private final Multimap<String, String> queueRoutes = HashMultimap.create();

    /**
     * Tunnels to core domains.
     */
    @GuardedBy("this")
    private final Map<CellAddressCore,CellTunnelInfo> coreTunnels = new HashMap<>();

    /**
     * Tunnels to satellite domains.
     */
    @GuardedBy("this")
    private final Map<CellAddressCore,CellTunnelInfo> satelliteTunnels = new HashMap<>();

    /**
     * Default routes that still have to be added.
     *
     * Routing manager when running in a satellite domain adds default routes whenever
     * it sees a new domain route to a core domain. To give other domains time to connect
     * to a newly created core domain, the installation of all but the first default route
     * is delayed for a moment.
     */
    @GuardedBy("this")
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
        role = getArgs().hasOption("role")
                ? CellDomainRole.valueOf(getArgs().getOption("role").toUpperCase())
                : CellDomainRole.SATELLITE;
    }

    @Override
    protected void starting() throws ExecutionException, InterruptedException
    {
        if (role == CellDomainRole.CORE) {
            canary = new CellAdapter(getCellName() + "-canary", "Generic", "")
            {
                @Override
                protected void stopped()
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

    private void notifyDownstreamOfDomainDeath()
    {
        canary = null;

        ListenableFuture<List<CellMessage>> future;
        synchronized (this) {
            future = sendToPeers(
                    new PeerShutdownNotification(getCellDomainName()), satelliteTunnels.values(), 7000);
        }
        try {
            future.get();
        } catch (ExecutionException e) {
            LOG.info("Failed to notify downstream of shutdown: {}", e.toString());
        } catch (InterruptedException ignored) {
        }
    }

    @Override
    public void stopped()
    {
        CellAdapter canary = this.canary;
        if (canary != null) {
            getNucleus().kill(canary.getCellName());
        }
        executor.shutdown();
    }

    @Override
    public synchronized void getInfo(PrintWriter pw)
    {
        pw.println("Local consumers: ");
        println(pw, localConsumers);
        pw.println();

        pw.println("Local subscriptions:");
        println(pw, localSubscriptions);
        pw.println();

        pw.println("Managed topic routes:");
        println(pw, topicRoutes);
        pw.println();

        pw.println("Managed well-known routes:");
        println(pw, queueRoutes);
    }

    private void println(PrintWriter pw, Multimap<String, String> map)
    {
        for (Map.Entry<String, Collection<String>> e : map.asMap().entrySet()) {
            pw.append(" ").append(e.getKey()).append(" : ").println(e.getValue());
        }
    }

    private synchronized void sendToCoreDomains()
    {
        sendToPeers(new CoreRouteUpdate(localConsumers.values(), localSubscriptions.values(), nucleus.getZone()), coreTunnels.values());
    }

    private synchronized void sendToSatelliteDomains()
    {
        sendToPeers(new CoreRouteUpdate(localConsumers.values(), nucleus.getZone()), satelliteTunnels.values());
    }

    private void sendToPeers(Serializable msg, Collection<CellTunnelInfo> tunnels)
    {
        CellAddressCore peer = new CellAddressCore(nucleus.getCellName());
        for (CellTunnelInfo tunnel : tunnels) {
            CellAddressCore domain = new CellAddressCore("*", tunnel.getRemoteCellDomainInfo().getCellDomainName());
            nucleus.sendMessage(new CellMessage(new CellPath(domain, peer), msg), false, true, true);
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
                                true, future, MoreExecutors.directExecutor(), timeout);
        }
        return Futures.allAsList(futures);
    }

    private void updateRoutes(String domain, Collection<String> destinations, Multimap<String, String> routes, Optional<String> zone, int type)
    {
        Set<String> newDestinations = new HashSet<>(destinations);
        Iterator<String> iterator = routes.get(domain).iterator();
        while (iterator.hasNext()) {
            String destination = iterator.next();
            if (!newDestinations.remove(destination)) {
                try {
                    nucleus.routeDelete(new CellRoute(destination, new CellAddressCore("*", domain), zone, type));
                    iterator.remove();
                } catch (IllegalArgumentException ignored) {
                    // Route didn't exist
                }
            }
        }
        for (String destination : newDestinations) {
            try {
                nucleus.routeAdd(new CellRoute(destination, new CellAddressCore("*", domain), zone, type));
                routes.put(domain, destination);
            } catch (IllegalArgumentException ignored) {
                // Already exists
            }
        }
    }

    private synchronized void updateTopicRoutes(String domain, Collection<String> topics, Optional<String> zone)
    {
        updateRoutes(domain, topics, topicRoutes, zone, CellRoute.TOPIC);
    }

    private synchronized void updateQueueRoutes(String domain, Collection<String> cells, Optional<String> zone)
    {
        updateRoutes(domain, cells, queueRoutes, zone, CellRoute.QUEUE);
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
                    updateTopicRoutes(domain, update.getTopics(), update.getZone());
                    updateQueueRoutes(domain, update.getExports(), update.getZone());
                });
            }
        } else if (obj instanceof GetAllDomainsRequest) {
            CellTunnelInfo tunnel;
            synchronized (this) {
                tunnel = Iterables.getFirst(coreTunnels.values(), null);
            }
            if (role == CellDomainRole.SATELLITE && tunnel != null) {
                msg.getDestinationPath().insert(new CellPath(nucleus.getCellName(),
                                                             tunnel.getRemoteCellDomainInfo().getCellDomainName()));
                nucleus.sendMessage(msg, false, true, false);
            } else {
                Map<String, Collection<String>> domains = new HashMap<>();
                synchronized (this) {
                    domains.put(getCellDomainName(), new ArrayList<>(localConsumers.values()));
                    Stream.of(coreTunnels, satelliteTunnels)
                            .flatMap(map -> map.values().stream())
                            .map(CellTunnelInfo::getRemoteCellDomainInfo)
                            .map(CellDomainInfo::getCellDomainName)
                            .forEach(domain -> domains.put(domain, new ArrayList<>()));
                    queueRoutes.asMap().forEach(
                            (domain, cells) -> domains.put(domain, Lists.newArrayList(cells)));
                }
                msg.revertDirection();
                msg.setMessageObject(new GetAllDomainsReply(domains));
                sendMessage(msg);
            }
        } else if (obj instanceof NoRouteToCellException) {
            LOG.info(((NoRouteToCellException) obj).getMessage());
        } else if (obj instanceof PeerShutdownNotification) {
            PeerShutdownNotification notification = (PeerShutdownNotification) obj;
            String remoteDomain = notification.getDomainName();
            synchronized (this) {
                coreTunnels.values().stream()
                        .filter(i -> i.getRemoteCellDomainInfo().getCellDomainName().equals(remoteDomain))
                        .forEach(i -> {
                            CellRoute route = new CellRoute(null, i.getTunnel(), i.getRemoteCellDomainInfo().getZone(), CellRoute.DEFAULT);
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
    public synchronized void cellDied(CellEvent ce)
    {
        String name = (String) ce.getSource();
        LOG.info("Cell died: {}", name);
    }

    @Override
    public synchronized void routeAdded(CellEvent ce)
    {
        super.routeAdded(ce);
        CellRoute cr = (CellRoute) ce.getSource();
        CellAddressCore target = cr.getTarget();
        LOG.info("Got 'route added' event: {}", cr);
        switch (cr.getRouteType()) {
        case CellRoute.DOMAIN:
            Optional<CellTunnelInfo> tunnelInfo = getTunnelInfo(target);
            tunnelInfo
                    .filter(i -> i.getRemoteCellDomainInfo().getRole() == CellDomainRole.CORE)
                    .ifPresent(i -> { coreTunnels.put(i.getTunnel(), i); sendToCoreDomains(); });
            tunnelInfo
                    .filter(i -> i.getRemoteCellDomainInfo().getRole() == CellDomainRole.SATELLITE)
                    .ifPresent(i -> { satelliteTunnels.put(i.getTunnel(), i); sendToSatelliteDomains(); });
            tunnelInfo
                    .filter(i -> i.getLocalCellDomainInfo().getRole() == CellDomainRole.SATELLITE &&
                                 i.getRemoteCellDomainInfo().getRole() == CellDomainRole.CORE)
                    .ifPresent(i -> {
                        delayedDefaultRoutes.add(new CellRoute(null, i.getTunnel(), i.getRemoteCellDomainInfo().getZone(), CellRoute.DEFAULT));
                        if (nucleus.getRoutingTable().hasDefaultRoute()) {
                            invokeLater(this::installDefaultRoute);
                        } else {
                            installDefaultRoute();
                        }
                    });
            break;
        case CellRoute.TOPIC:
            String topic = cr.getCellName();
            if (target.getCellDomainName().equals(nucleus.getCellDomainName())) {
                localSubscriptions.put(target.getCellName(), topic);
                sendToCoreDomains();
            }
            break;
        case CellRoute.QUEUE:
            String queue = cr.getCellName();
            if (target.getCellDomainName().equals(nucleus.getCellDomainName())) {
                localConsumers.put(target.getCellName(), queue);
                sendToCoreDomains();
                sendToSatelliteDomains();
            }
            break;
        case CellRoute.DEFAULT:
            break;
        }
    }

    @Override
    public synchronized void routeDeleted(CellEvent ce)
    {
        CellRoute cr = (CellRoute) ce.getSource();
        CellAddressCore target = cr.getTarget();
        LOG.info("Got 'route deleted' event: {}", cr);
        switch (cr.getRouteType()) {
        case CellRoute.DOMAIN:
            updateTopicRoutes(cr.getDomainName(), Collections.emptyList(), cr.getZone());
            updateQueueRoutes(cr.getDomainName(), Collections.emptyList(), cr.getZone());
            coreTunnels.remove(target);
            satelliteTunnels.remove(target);
            delayedDefaultRoutes.remove(new CellRoute(null, target, cr.getZone(), CellRoute.DEFAULT));
            break;
        case CellRoute.TOPIC:
            String topic = cr.getCellName();
            if (target.getCellDomainName().equals(nucleus.getCellDomainName())) {
                if (localSubscriptions.remove(target.getCellName(), topic)) {
                    sendToCoreDomains();
                }
            }
            break;
        case CellRoute.QUEUE:
            String queue = cr.getCellName();
            if (target.getCellDomainName().equals(nucleus.getCellDomainName())) {
                if (localSubscriptions.remove(target.getCellName(), queue)) {
                    sendToCoreDomains();
                    sendToSatelliteDomains();
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
        return Stream.of(nucleus.getRoutingList())
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
    @Deprecated
    public synchronized Object ac_ls_$_0(Args args)
    {
        return new Object[] {
                getCellDomainName(),
                Sets.newHashSet(localConsumers.values()),
                queueRoutes.asMap().entrySet().stream().collect(
                        toMap(Map.Entry::getKey, e -> Sets.newHashSet(e.getValue())))
        };
    }
}
