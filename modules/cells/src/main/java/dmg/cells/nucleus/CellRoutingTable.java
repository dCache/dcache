package dmg.cells.nucleus;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;

import org.dcache.util.Args;

public class CellRoutingTable implements Serializable
{
    private static final long serialVersionUID = -1456280129622980563L;

    private final ConcurrentMap<String, CellRoute> _wellknown = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, CellRoute> _domain = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, CellRoute> _exact = new ConcurrentHashMap<>();
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
            if (_exact.putIfAbsent(dest, route) != null) {
                throw new IllegalArgumentException("Duplicated route Entry for : " + dest);
            }
            break;
        case CellRoute.WELLKNOWN:
            dest = route.getCellName();
            if (_wellknown.putIfAbsent(dest, route) != null) {
                throw new IllegalArgumentException("Duplicated route Entry for : " + dest);
            }
            break;
        case CellRoute.DOMAIN:
            dest = route.getDomainName();
            if (_domain.putIfAbsent(dest, route) != null) {
                throw new IllegalArgumentException("Duplicated route Entry for : " + dest);
            }
            break;
        case CellRoute.DEFAULT:
            if (!_default.compareAndSet(null, route)) {
                throw new IllegalArgumentException("Duplicated route Entry for default.");
            }
            break;
        case CellRoute.DUMPSTER:
            if (!_dumpster.compareAndSet(null, route)) {
                throw new IllegalArgumentException("Duplicated route Entry for dumpster");
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
            if (!_exact.remove(dest, route)) {
                throw new IllegalArgumentException("Route Entry Not Found for : " + dest);
            }
            break;
        case CellRoute.WELLKNOWN:
            dest = route.getCellName();
            if (!_wellknown.remove(dest, route)) {
                throw new IllegalArgumentException("Route Entry Not Found for : " + dest);
            }
            break;
        case CellRoute.DOMAIN:
            dest = route.getDomainName();
            if (!_domain.remove(dest, route)) {
                throw new IllegalArgumentException("Route Entry Not Found for : " + dest);
            }
            break;
        case CellRoute.DEFAULT:
            if (!_default.compareAndSet(route, null)) {
                throw new IllegalArgumentException("Route Entry Not Found for default");
            }
            break;
        case CellRoute.DUMPSTER:
            if (!_dumpster.compareAndSet(route, null)) {
                throw new IllegalArgumentException("Route Entry Not Found dumpster");
            }
            break;
        }
    }

    public CellRoute find(CellAddressCore addr)
    {
        String cellName = addr.getCellName();
        String domainName = addr.getCellDomainName();
        CellRoute route;
        if (domainName.equals("local")) {
            //
            // this is not really local but wellknown
            // we checked for local before we called this.
            //
            route = _wellknown.get(cellName);
            if (route != null) {
                return route;
            }
        } else {
            route = _exact.get(cellName + "@" + domainName);
            if (route != null) {
                return route;
            }
            route = _domain.get(domainName);
            if (route != null) {
                return route;
            }
        }
        route = _exact.get(cellName + "@" + domainName);
        return route == null ? _default.get() : route;
    }

    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append(CellRoute.headerToString()).append("\n");
        for (CellRoute route : _exact.values()) {
            sb.append(route).append("\n");
        }
        for (CellRoute route : _wellknown.values()) {
            sb.append(route).append("\n");
        }
        for (CellRoute route : _domain.values()) {
            sb.append(route).append("\n");
        }
        CellRoute defaultRoute = _default.get();
        if (defaultRoute != null) {
            sb.append(defaultRoute).append("\n");
        }
        CellRoute dumpsterRoute = _dumpster.get();
        if (dumpsterRoute != null) {
            sb.append(dumpsterRoute).append("\n");
        }
        return sb.toString();
    }

    public CellRoute[] getRoutingList()
    {
        List<CellRoute> routes = new ArrayList<>();
        routes.addAll(_exact.values());
        routes.addAll(_wellknown.values());
        routes.addAll(_domain.values());
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
