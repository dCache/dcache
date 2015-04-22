package dmg.cells.nucleus;

import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.SetMultimap;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.dcache.util.ColumnWriter;

import static java.util.stream.Collectors.toList;

public class CellRoutingTable implements Serializable
{
    private static final long serialVersionUID = -1456280129622980563L;

    private final SetMultimap<String, CellRoute> _wellknown = LinkedHashMultimap.create();
    private final SetMultimap<String, CellRoute> _domain = LinkedHashMultimap.create();
    private final SetMultimap<String, CellRoute> _exact = LinkedHashMultimap.create();
    private final Map<String, Set<CellRoute>> _topic = new HashMap<>();
    private final AtomicReference<CellRoute> _dumpster = new AtomicReference<>();
    private final AtomicReference<CellRoute> _default = new AtomicReference<>();

    public void add(CellRoute route)
            throws IllegalArgumentException
    {
        String dest;
        switch (route.getRouteType()) {
        case CellRoute.EXACT:
        case CellRoute.ALIAS:
            dest = route.getCellName() + "@" + route.getDomainName();
            synchronized (_exact) {
                if (!_exact.put(dest, route)) {
                    throw new IllegalArgumentException("Duplicated route entry for : " + dest);
                }
            }
            break;
        case CellRoute.WELLKNOWN:
            dest = route.getCellName();
            synchronized (_wellknown) {
                if (!_wellknown.put(dest, route)) {
                    throw new IllegalArgumentException("Duplicated route entry for : " + dest);
                }
            }
            break;
        case CellRoute.TOPIC:
            dest = route.getCellName();
            synchronized (_topic) {
                if (!_topic.computeIfAbsent(dest, key -> new CopyOnWriteArraySet()).add(route)) {
                    throw new IllegalArgumentException("Duplicated route entry for : " + dest);
                }
            }
            break;
        case CellRoute.DOMAIN:
            dest = route.getDomainName();
            synchronized (_domain) {
                if (!_domain.put(dest, route)) {
                    throw new IllegalArgumentException("Duplicated route entry for : " + dest);
                }
            }
            break;
        case CellRoute.DEFAULT:
            if (!_default.compareAndSet(null, route)) {
                throw new IllegalArgumentException("Duplicated route entry for default.");
            }
            break;
        case CellRoute.DUMPSTER:
            if (!_dumpster.compareAndSet(null, route)) {
                throw new IllegalArgumentException("Duplicated route entry for dumpster");
            }
            break;
        }
    }

    public void delete(CellRoute route)
            throws IllegalArgumentException
    {
        String dest;
        switch (route.getRouteType()) {
        case CellRoute.EXACT:
        case CellRoute.ALIAS:
            dest = route.getCellName() + "@" + route.getDomainName();
            synchronized (_exact) {
                if (!_exact.remove(dest, route)) {
                    throw new IllegalArgumentException("Route entry not found for : " + dest);
                }
            }
            break;
        case CellRoute.WELLKNOWN:
            dest = route.getCellName();
            synchronized (_wellknown) {
                if (!_wellknown.remove(dest, route)) {
                    throw new IllegalArgumentException("Route entry not found for : " + dest);
                }
            }
            break;
        case CellRoute.TOPIC:
            dest = route.getCellName();
            synchronized (_topic) {
                Set<CellRoute> routes = _topic.get(dest);
                if (!routes.remove(route)) {
                    throw new IllegalArgumentException("Route entry not found for : " + dest);
                }
                if (routes.isEmpty()) {
                    _topic.remove(dest);
                }
            }
            break;
        case CellRoute.DOMAIN:
            dest = route.getDomainName();
            synchronized (_domain) {
                if (!_domain.remove(dest, route)) {
                    throw new IllegalArgumentException("Route entry not found for : " + dest);
                }
            }
            break;
        case CellRoute.DEFAULT:
            if (!_default.compareAndSet(route, null)) {
                throw new IllegalArgumentException("Route entry not found for default");
            }
            break;
        case CellRoute.DUMPSTER:
            if (!_dumpster.compareAndSet(route, null)) {
                throw new IllegalArgumentException("Route entry not found dumpster");
            }
            break;
        }
    }

    public void delete(CellAddressCore target)
    {
        String addr = target.toString();
        delete(_exact, addr);
        delete(_wellknown, addr);
        delete(_domain, addr);

        synchronized (_topic) {
            /* We cannot use the regular delete method because a CopyOnWriteArraySet iterator
             * doesn't allow manipulating operations. We trade expensive deletion for not having
             * to copy the set of topic routes in findTopicRoutes.
             */
            Iterator<Set<CellRoute>> iterator = _topic.values().iterator();
            while (iterator.hasNext()) {
                Set<CellRoute> routes = iterator.next();
                List<CellRoute> toRemove =
                        routes.stream().filter(route -> route.getTargetName().equals(addr)).collect(toList());
                routes.removeAll(toRemove);
                if (routes.isEmpty()) {
                    iterator.remove();
                }
            }
        }
    }

    private void delete(SetMultimap<String,CellRoute> routes, String addr)
    {
        synchronized (routes) {
            Iterator<CellRoute> iterator = routes.values().iterator();
            while (iterator.hasNext()) {
                CellRoute route = iterator.next();
                if (route.getTargetName().equals(addr)) {
                    iterator.remove();
                }
            }
        }
    }

    public CellRoute find(CellAddressCore addr)
    {
        String cellName = addr.getCellName();
        String domainName = addr.getCellDomainName();
        Optional<CellRoute> route;
        synchronized (_exact) {
            route = _exact.get(cellName + "@" + domainName).stream().findFirst();
        }
        if (route.isPresent()) {
            return route.get();
        }
        if (domainName.equals("local")) {
            //
            // this is not really local but wellknown
            // we checked for local before we called this.
            //
            synchronized (_wellknown) {
                route = _wellknown.get(cellName).stream().findFirst();
            }
            if (route.isPresent()) {
                return route.get();
            }
        } else {
            synchronized (_domain) {
                route = _domain.get(domainName).stream().findFirst();
            }
            if (route.isPresent()) {
                return route.get();
            }
        }
        return _default.get();
    }

    public Set<CellRoute> findTopicRoutes(CellAddressCore addr)
    {
        String cellName = addr.getCellName();
        String domainName = addr.getCellDomainName();
        if (!domainName.equals("local")) {
            return Collections.emptySet();
        }
        synchronized (_topic) {
            return _topic.getOrDefault(cellName, Collections.emptySet());
        }
    }

    public String toString()
    {
        ColumnWriter writer = new ColumnWriter()
                .header("CELL").left("cell").space()
                .header("DOMAIN").left("domain").space()
                .header("GATEWAY").left("gateway").space()
                .header("TYPE").left("type");

        Consumer<CellRoute> append =
                route -> writer.row()
                        .value("cell", route.getCellName())
                        .value("domain", route.getDomainName())
                        .value("gateway", route.getTargetName())
                        .value("type", route.getRouteTypeName());

        synchronized (_topic) {
            _topic.values().forEach(routes -> routes.forEach(append));
        }
        synchronized (_exact) {
            _exact.values().forEach(append);
        }
        synchronized (_wellknown) {
            _wellknown.values().forEach(append);
        }
        synchronized (_domain) {
            _domain.values().forEach(append);
        }
        CellRoute defaultRoute = _default.get();
        if (defaultRoute != null) {
            append.accept(defaultRoute);
        }
        CellRoute dumpsterRoute = _dumpster.get();
        if (dumpsterRoute != null) {
            append.accept(dumpsterRoute);
        }
        return writer.toString();
    }

    public CellRoute[] getRoutingList()
    {
        List<CellRoute> routes = new ArrayList<>();
        synchronized (_topic) {
            _topic.values().forEach(routes::addAll);
        }
        synchronized (_exact) {
            routes.addAll(_exact.values());
        }
        synchronized (_wellknown) {
            routes.addAll(_wellknown.values());
        }
        synchronized (_domain) {
            routes.addAll(_domain.values());
        }
        CellRoute defaultRoute = _default.get();
        if (defaultRoute != null) {
            routes.add(defaultRoute);
        }
        CellRoute dumpsterRoute = _dumpster.get();
        if (dumpsterRoute != null) {
            routes.add(dumpsterRoute);
        }
        return routes.toArray(new CellRoute[routes.size()]);
    }
}
