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
import dmg.cells.nucleus.NoRouteToCellException;

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
    private final Map<String,CellTunnelInfo> coreTunnels = new HashMap<>();

    /**
     * Tunnels to satellite domains.
     */
    private final Map<String,CellTunnelInfo> satelliteTunnels = new HashMap<>();

    public CoreRoutingManager(String name, String arguments)
    {
        super(name,"System", arguments);
        nucleus = getNucleus();
        nucleus.addCellEventListener(this);
        role = getArgs().hasOption("role")
                ? CellDomainRole.valueOf(getArgs().getOption("role").toUpperCase())
                : CellDomainRole.SATELLITE;
    }

    @Override
    public void cleanUp()
    {
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
        sendCoreUpdate(new CoreRouteUpdate(localExports, localSubscriptions.values()), coreTunnels.values());
    }

    private synchronized void sendToSatelliteDomains()
    {
        sendCoreUpdate(new CoreRouteUpdate(localExports), satelliteTunnels.values());
    }

    private void sendCoreUpdate(CoreRouteUpdate msg, Collection<CellTunnelInfo> tunnels)
    {
        CellAddressCore peer = new CellAddressCore(nucleus.getCellName());
        for (CellTunnelInfo tunnel : tunnels) {
            CellAddressCore domain = new CellAddressCore("*", tunnel.getRemoteCellDomainInfo().getCellDomainName());
            nucleus.sendMessage(new CellMessage(new CellPath(domain, peer), msg), false, true);
        }
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
                    Stream.concat(coreTunnels.values().stream(), satelliteTunnels.values().stream())
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
                    .ifPresent(i -> { coreTunnels.put(i.getTunnelName(), i); sendToCoreDomains(); });
            tunnelInfo
                    .filter(i -> i.getRemoteCellDomainInfo().getRole() == CellDomainRole.SATELLITE)
                    .ifPresent(i -> { satelliteTunnels.put(i.getTunnelName(), i); sendToSatelliteDomains(); });
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
            LOG.info("Default route was added");
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
                    .map(CellTunnelInfo::getTunnelName)
                    .ifPresent(name -> { coreTunnels.remove(name); satelliteTunnels.remove(name); });
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

    private Optional<CellTunnelInfo> getTunnelInfo(CellAddressCore tunnel)
    {
        return nucleus.getCellTunnelInfos().stream()
                .filter(i -> i.getTunnelName().equals(tunnel.getCellName()))
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
