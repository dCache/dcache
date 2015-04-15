package dmg.cells.nucleus;

import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.SetMultimap;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.dcache.util.ColumnWriter;

public class CellRoutingTable implements Serializable
{
    private static final long serialVersionUID = -1456280129622980563L;

    private final SetMultimap<String, CellRoute> _wellknown = LinkedHashMultimap.create();
    private final SetMultimap<String, CellRoute> _domain = LinkedHashMultimap.create();
    private final SetMultimap<String, CellRoute> _exact = LinkedHashMultimap.create();
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
