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
import com.google.common.collect.Multimap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import dmg.cells.nucleus.CellAdapter;
import dmg.cells.nucleus.CellAddressCore;
import dmg.cells.nucleus.CellDomainRole;
import dmg.cells.nucleus.CellEvent;
import dmg.cells.nucleus.CellEventListener;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellNucleus;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.CellRoute;
import dmg.cells.nucleus.CellTunnelInfo;
import dmg.cells.nucleus.NoRouteToCellException;

/**
 * Routing manager to publish exported cells and topics to peers. Since core domains
 * don't have a default route, the regular routing manager doesn't publish this
 * information.
 *
 * This routing manager receives its own updates from its peers and manages
 * WELLKNOWN and TOPIC routes to core domains.
 */
public class CoreRoutingManager
    extends CellAdapter
    implements CellEventListener
{
    private static final Logger _log =
        LoggerFactory.getLogger(CoreRoutingManager.class);

    private final CellNucleus _nucleus;

    /** Names of local cells that have been exported. */
    private final Set<String> _localExports = new HashSet<>();

    /** Local cells that subscribed to topics. */
    private final Multimap<String, String> _localSubscriptions = HashMultimap.create();

    /**
     * Routing updates from downstream domains are processed sequentially on a dedicated
     * thread.
     */
    private final ExecutorService _executor = Executors.newSingleThreadExecutor(getNucleus());

    /**
     * Map to collapse update messages from connected domains.
     */
    private final ConcurrentMap<String, CoreRouteUpdate> _updates = new ConcurrentHashMap<>();

    /**
     * Topic routes installed by routing manager.
     */
    private final Multimap<String, String> _topicRoutes = HashMultimap.create();

    /**
     * Well known routes installed by routing manager.
     */
    private final Multimap<String, String> _wellknownRoutes = HashMultimap.create();

    /**
     * Tunnels between core domains.
     */
    private final Map<String,CellTunnelInfo> _coreTunnels = new HashMap<>();

    /**
     * Tunnels from core domains to satellite domains.
     */
    private final Map<String,CellTunnelInfo> _satelliteTunnels = new HashMap<>();

    public CoreRoutingManager(String name, String arguments)
    {
        super(name,"System", arguments);
        _nucleus = getNucleus();
        _nucleus.addCellEventListener(this);
    }

    @Override
    public void cleanUp()
    {
        _executor.shutdown();
    }

    @Override
    public synchronized void getInfo(PrintWriter pw)
    {
        pw.append("Local exports: ").println(_localExports);
        pw.println();

        pw.println("Local subscriptions:");
        for (Map.Entry<String, Collection<String>> e : _localSubscriptions.asMap().entrySet()) {
            pw.append(" ").append(e.getKey()).append(" : ").println(e.getValue());
        }

        pw.println();
        pw.println("Managed routes:");
        for (Map.Entry<String,Collection<String>> e : _topicRoutes.asMap().entrySet()) {
            pw.append(" ").append(e.getKey()).append(" : ").println(e.getValue());
        }
        for (Map.Entry<String,Collection<String>> e : _wellknownRoutes.asMap().entrySet()) {
            pw.append(" ").append(e.getKey()).append(" : ").println(e.getValue());
        }
    }

    private synchronized void sendToCoreDomains()
    {
        sendCoreUpdate(new CoreRouteUpdate(_localExports, _localSubscriptions.values()), _coreTunnels.values());
    }

    private synchronized void sendToSatelliteDomains()
    {
        sendCoreUpdate(new CoreRouteUpdate(_localExports), _satelliteTunnels.values());
    }

    private void sendCoreUpdate(CoreRouteUpdate msg, Collection<CellTunnelInfo> tunnels)
    {
        CellAddressCore peer = new CellAddressCore(_nucleus.getCellName());
        for (CellTunnelInfo tunnel : tunnels) {
            CellAddressCore domain = new CellAddressCore("*", tunnel.getRemoteCellDomainInfo().getCellDomainName());
            _nucleus.sendMessage(new CellMessage(new CellPath(domain, peer), msg), false, true);
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
        updateRoutes(domain, topics, _topicRoutes, CellRoute.TOPIC);
    }

    private synchronized void updateWellknownRoutes(String domain, Collection<String> cells)
    {
        updateRoutes(domain, cells, _wellknownRoutes, CellRoute.WELLKNOWN);
    }

    @Override
    public void messageArrived(CellMessage msg)
    {
        Serializable obj = msg.getMessageObject();
        if (obj instanceof CoreRouteUpdate) {
            String domain = msg.getSourceAddress().getCellDomainName();
            if (_updates.put(domain, (CoreRouteUpdate) obj) == null) {
                _executor.execute(() -> {
                    CoreRouteUpdate update = _updates.remove(domain);
                    updateTopicRoutes(domain, update.getTopics());
                    updateWellknownRoutes(domain, update.getExports());
                });
            }
        } else if (obj instanceof NoRouteToCellException) {
            _log.info(((NoRouteToCellException) obj).getMessage());
        } else {
            _log.warn("Unidentified message ignored: {}", obj);
        }
    }

    @Override
    public void cellCreated(CellEvent ce)
    {
        String name = (String)ce.getSource();
        _log.info("Cell created: {}", name);
    }

    @Override
    public synchronized void cellExported(CellEvent ce)
    {
        String name = (String)ce.getSource();
        _log.info("Cell exported: {}", name);
        _localExports.add(name);
        sendToCoreDomains();
        sendToSatelliteDomains();
    }

    @Override
    public synchronized void cellDied(CellEvent ce)
    {
        String name = (String) ce.getSource();
        _log.info("Cell died: {}", name);
        if (_localExports.remove(name)) {
            sendToSatelliteDomains();
            sendToCoreDomains();
        }
    }

    @Override
    public synchronized void routeAdded(CellEvent ce)
    {
        CellRoute cr = (CellRoute) ce.getSource();
        _log.info("Got 'route added' event: {}", cr);
        switch (cr.getRouteType()) {
        case CellRoute.DOMAIN:
            Optional<CellTunnelInfo> tunnelInfo = getTunnelInfo(cr.getTarget());
            tunnelInfo
                    .filter(i -> i.getLocalCellDomainInfo().getRole() == CellDomainRole.CORE &&
                                 i.getRemoteCellDomainInfo().getRole() == CellDomainRole.CORE)
                    .ifPresent(i -> { _coreTunnels.put(i.getTunnelName(), i); sendToCoreDomains(); });
            tunnelInfo
                    .filter(i -> i.getLocalCellDomainInfo().getRole() == CellDomainRole.CORE &&
                                 i.getRemoteCellDomainInfo().getRole() == CellDomainRole.SATELLITE)
                    .ifPresent(i -> { _satelliteTunnels.put(i.getTunnelName(), i); sendToSatelliteDomains(); });
            break;
        case CellRoute.TOPIC:
            String topic = cr.getCellName();
            CellAddressCore target1 = cr.getTarget();
            if (target1.getCellDomainName().equals(_nucleus.getCellDomainName())) {
                _localSubscriptions.put(target1.getCellName(), topic);
                sendToCoreDomains();
            }
            break;
        case CellRoute.DEFAULT:
            _log.info("Default route was added");
            CellAddressCore target = cr.getTarget();
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
                    .ifPresent(name -> { _coreTunnels.remove(name); _satelliteTunnels.remove(name); });
            break;
        case CellRoute.TOPIC:
            String topic = cr.getCellName();
            CellAddressCore target = cr.getTarget();
            if (target.getCellDomainName().equals(_nucleus.getCellDomainName())) {
                if (_localSubscriptions.remove(target.getCellName(), topic)) {
                    sendToCoreDomains();
                }
            }
            break;
        }
    }

    private Optional<CellTunnelInfo> getTunnelInfo(CellAddressCore tunnel)
    {
        return _nucleus.getCellTunnelInfos().stream()
                .filter(i -> i.getTunnelName().equals(tunnel.getCellName()))
                .findAny();
    }
}
