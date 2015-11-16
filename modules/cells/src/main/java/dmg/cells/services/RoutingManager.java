package dmg.cells.services;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.PrintWriter;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import dmg.cells.nucleus.CellAdapter;
import dmg.cells.nucleus.CellAddressCore;
import dmg.cells.nucleus.CellEvent;
import dmg.cells.nucleus.CellEventListener;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellNucleus;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.CellRoute;
import dmg.cells.nucleus.NoRouteToCellException;

import org.dcache.util.Args;

import static java.util.stream.Collectors.*;

/**
 * The dmg.cells.services.RoutingManager is a cell performing the following services:
 * <ul>
 * <li>Watching a specified tunnel cell and setting the
 *     default route to this cell as soon as this tunnel
 *     cell establishes its domain route.
 * <li>Assembling downstream routing information and
 *     the exportCell EventListener Event and maintaining
 *     a wellknown Cell list.
 * <li>Sending its wellknown cell list upstream as soon
 *     as a default route is available and whenever
 *     the wellknown cell list changes.
 * </ul>
 */
public class RoutingManager
    extends CellAdapter
    implements CellEventListener
{
    private static final Logger _log =
        LoggerFactory.getLogger(RoutingManager.class);

    private final CellNucleus _nucleus;

    /** Names of local cells that have been exported. */
    private final Set<String> _localExports = new HashSet<>();

    /**
     * Map from domain names to the routes announced by that domain. Each route is
     * represented by a path with the last element being the target and the preceding
     * addresses being domains to traverse to reach that target.
     */
    private final Map<String, Set<CellPath>> _domainHash = new HashMap<>();

    /**
     * Topics for which the local domain has routes installed.
     */
    private final Set<String> _topics = new HashSet<>();

    /**
     * Whether a default route has been installed in our domain. We don't
     * announce our routes until a default route has been installed.
     */
    private boolean _defaultInstalled;

    /**
     * Name of a tunnel cell that will be used as a default route if it appears. Usually
     * not used in dCache as the default route gets installed by the location manager.
     */
    private final String _watchCell;

    /**
     * Queued routing information updates from downstream domains. Used to collapse
     * repeated updates from the same domain to allow us to better cope with high churn.
     */
    private final ConcurrentMap<String, String[]> _updates = Maps.newConcurrentMap();

    /**
     * Routing updates from downstream domains are processed sequentially on a dedicated
     * thread.
     */
    private final ExecutorService _executor = Executors.newSingleThreadExecutor(getNucleus());

    /**
     * To suppress routing loops, we prefix downstream routes with our domain address
     * before forwarding them to upstream.
     */
    private final CellAddressCore _domainAddress = new CellAddressCore("*", getCellDomainName());

    /**
     * Downstream topic announcements.
     */
    private final Map<String, Collection<String>> _downstreamTopics = new HashMap<>();

    /**
     * Queued topic routing information updates from downstream domains. Used to collapse
     * repeated updates from the same domain to allow us to better cope with high churn.
     */
    private final ConcurrentMap<String, Collection<String>> _topicUpdates = Maps.newConcurrentMap();

    public RoutingManager(String name, String arguments) throws Exception
    {
        super(name,"System", arguments);
        _nucleus = getNucleus();
        _nucleus.addCellEventListener(this);
        Args args = getArgs();
        _watchCell = (args.argc() == 0) ? null : args.argv(0);
        try {
            start();
        } catch (ExecutionException e) {
            Throwables.propagateIfInstanceOf(e.getCause(), Exception.class);
            throw Throwables.propagate(e.getCause());
        }
    }

    @Override
    public void cleanUp()
    {
        _executor.shutdown();
    }

    @Override
    public synchronized void getInfo(PrintWriter pw)
    {
        pw.println("Our routing knowledge:");
        pw.append(" Local : ").println(_localExports);

        for (Map.Entry<String,Set<CellPath>> e : _domainHash.entrySet()) {
            pw.append(" ").append(e.getKey()).append(" : ")
                    .println(e.getValue().stream().map(CellPath::toAddressString).collect(joining(",", "[", "]")));
        }
        pw.println();
        pw.println("Topic subscriptions:");
        pw.append(" Local : " ).println(_topics);
        for (Map.Entry<String,Collection<String>> e : _downstreamTopics.entrySet()) {
            pw.append(" ").append(e.getKey()).append(" : ").println(e.getValue());
        }
    }

    private synchronized void setDefaultInstalled(boolean value)
    {
        _defaultInstalled = value;
    }

    private synchronized boolean isDefaultInstalled()
    {
        return _defaultInstalled;
    }

    private void addRoute(CellPath dest, String domain)
    {
        try {
            CellAddressCore address = dest.getDestinationAddress();
            if (isWellKnown(address)) {
                _nucleus.routeAdd(new CellRoute(address.getCellName(), "*@" + domain, CellRoute.WELLKNOWN));
            } else if (address.getCellName().equals("*")) {
                _nucleus.routeAdd(new CellRoute(address.getCellDomainName(), "*@" + domain, CellRoute.DOMAIN));
            } else {
                _log.debug("Ignoring downstream route advertisement from {}: {}", domain, address);
            }
        } catch (IllegalArgumentException e) {
            _log.warn("Failed to add route: {}", e.getMessage());
        }
    }

    private void removeRoute(CellPath dest, String domain)
    {
        try {
            CellAddressCore address = dest.getDestinationAddress();
            if (isWellKnown(address)) {
                _nucleus.routeDelete(new CellRoute(address.getCellName(), "*@" + domain, CellRoute.WELLKNOWN));
            } else if (address.getCellName().equals("*")) {
                _nucleus.routeDelete(new CellRoute(address.getCellDomainName(), "*@" + domain, CellRoute.DOMAIN));
            } else {
                _log.error("Unexpected attempt to remove route: {}", address);
            }
        } catch (IllegalArgumentException e) {
            _log.warn("Failed to delete route: {}", e.getMessage());
        }
    }

    private synchronized void updateUpstream()
    {
        if (!isDefaultInstalled()) {
            return;
        }

        List<String> all = Lists.newArrayList();
        _log.info("update requested to upstream Domains");
        //
        // the protocol requires the local DomainName
        // first
        //
        all.add(getCellDomainName());
        //
        // here we add our own exportables
        //
        all.addAll(_localExports);

        //
        // now all the downstream well known cells
        //
        _domainHash.forEach(
                (domain, paths) -> paths.forEach(path -> all.add("*@" + domain + ":" + path.toAddressString())));

        //
        // and finally the downstream domains
        //
        _domainHash.forEach((domain, paths) -> all.add("*@" + domain));

        String destinationManager = _nucleus.getCellName();
        _log.info("Resending to {}: {}", destinationManager, all);
        CellPath path = new CellPath(destinationManager);
        String[] arr = all.toArray(new String[all.size()]);

        _nucleus.sendMessage(new CellMessage(path, arr), false, true);
    }

    private synchronized void updateRoutingInfo(String[] info)
    {
        String domain = info[0];
        Set<CellPath> oldPaths = _domainHash.get(domain);
        Set<CellPath> newPaths =
                Arrays.asList(info).subList(1, info.length).stream()
                        .map(CellPath::new)
                        .filter(p -> !p.contains(_domainAddress))    // Avoid routing loops
                        .collect(toSet());                           // No duplicate routes

        boolean changed = false;
        if (oldPaths == null) {
            _log.info("Adding domain {}", domain);
            for (CellPath path : newPaths) {
                addRoute(path, domain);
                changed = true;
            }
        } else {
            _log.info("Updating domain {}", domain);
            for (CellPath path : newPaths) {
                if (!oldPaths.remove(path)) {
                    _log.debug("Adding {}", path);
                    // entry not found, so make it
                    addRoute(path, domain);
                    changed = true;
                }
            }
            // all additional routes added now, need to remove the rest
            for (CellPath path : oldPaths) {
                _log.debug("Removing {}", path);
                removeRoute(path, domain);
                changed = true;
            }
        }
        _domainHash.put(domain, newPaths);
        if (changed) {
            updateUpstream();
        }
    }

    private synchronized void removeRoutingInfo(String domain)
    {
        _log.info("Removing all routes to domain {}", domain);
        Set<CellPath> paths = _domainHash.remove(domain);
        if (paths == null){
            _log.info("No entry found for domain {}", domain);
            return;
        }
        for (CellPath path : paths) {
            removeRoute(path, domain);
        }
        updateUpstream();
    }

    private synchronized void updateTopicsUpstream()
    {
        if (!isDefaultInstalled()) {
            return;
        }
        String[] topics = _topics.toArray(new String[_topics.size()]);
        CellMessage msg = new CellMessage(new CellAddressCore(_nucleus.getCellName()),
                                          new TopicRouteUpdate(topics));
        _nucleus.sendMessage(msg, false, true);
    }

    private synchronized void topicRouteAdded(CellRoute cr)
    {
        if (_topics.add(cr.getCellName())) {
            updateTopicsUpstream();
        }
    }

    private synchronized void topicRouteRemoved(CellRoute cr)
    {
        if (_topics.remove(cr.getCellName())) {
            updateTopicsUpstream();
        }
    }

    private synchronized void updateDownstreamTopics(String domain, Collection<String> newTopics)
    {
        Collection<String> oldTopics = _downstreamTopics.get(domain);
        if (oldTopics == null) {
            for (String topic : newTopics) {
                try {
                    _nucleus.routeAdd(new CellRoute(topic, "*@" + domain, CellRoute.TOPIC));
                } catch (IllegalArgumentException ignored) {
                    // Route exists already
                }
            }
        } else {
            oldTopics = new HashSet<>(oldTopics);
            for (String topic : newTopics) {
                if (!oldTopics.remove(topic)) {
                    try {
                        _nucleus.routeAdd(new CellRoute(topic, "*@" + domain, CellRoute.TOPIC));
                    } catch (IllegalArgumentException ignored) {
                        // Route exists already
                    }
                }
            }
            for (String topic : oldTopics) {
                try {
                    _nucleus.routeDelete(new CellRoute(topic, "*@" + domain, CellRoute.TOPIC));
                } catch (IllegalArgumentException ignored) {
                    // Route didn't exist
                }
            }
        }
        if (newTopics.isEmpty()) {
            _downstreamTopics.remove(domain);
        } else {
            _downstreamTopics.put(domain, newTopics);
        }
    }

    @Override
    public void messageArrived(CellMessage msg)
    {
        Serializable obj = msg.getMessageObject();
        if (obj instanceof String[]) {
            String[] info = (String[]) obj;
            if (info.length < 1) {
                _log.warn("Protocol error 1 in routing info");
                return;
            }
            final String domain = info[0];
            _log.info("Routing info arrived for domain {}", domain);

            if (_updates.put(domain, info) == null) {
                _executor.execute(new Runnable()
                {
                    @Override
                    public void run()
                    {
                        try {
                            updateRoutingInfo(_updates.remove(domain));
                        } catch (Throwable e) {
                            Thread t = Thread.currentThread();
                            t.getUncaughtExceptionHandler().uncaughtException(t, e);
                        }
                    }
                });
            }
        } else if (obj instanceof TopicRouteUpdate) {
            String domain = msg.getSourceAddress().getCellDomainName();
            if (_topicUpdates.put(domain, ((TopicRouteUpdate) obj).getTopics()) == null) {
                _executor.execute(() -> updateDownstreamTopics(domain, _topicUpdates.remove(domain)));
            }
        } else if (obj instanceof GetAllDomainsRequest) {
            if (_defaultInstalled && !msg.getSourcePath().contains(_domainAddress)) {
                msg.getDestinationPath().insert(new CellPath(_nucleus.getCellName()));
                msg.nextDestination();
                _nucleus.sendMessage(msg, false, true);
            } else {
                Map<String, Collection<String>> domains;
                synchronized (this) {
                    domains = getWellKnownCellsByDomain(ArrayList::new);
                    domains.put(getCellDomainName(), new ArrayList<>(_localExports));
                    /* Add the domains without wellknown cells too. */
                    _domainHash.keySet().stream()
                            .filter(domain -> !domains.containsKey(domain))
                            .forEach(domain -> domains.put(domain, new ArrayList<>()));
                }
                msg.revertDirection();
                msg.setMessageObject(new GetAllDomainsReply(domains));
                sendMessage(msg);
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
    public synchronized void cellDied(CellEvent ce)
    {
        String name = (String) ce.getSource();
        _log.info("Cell died: {}", name);
        _localExports.remove(name);
        updateUpstream();
    }

    @Override
    public synchronized void cellExported(CellEvent ce)
    {
        String name = (String)ce.getSource();
        _log.info("Cell exported: {}", name);
        _localExports.add(name);
        updateUpstream();
    }

    @Override
    public void routeAdded(CellEvent ce)
    {
        CellRoute       cr   = (CellRoute)ce.getSource();
        CellAddressCore gate = new CellAddressCore(cr.getTargetName());
        _log.info("Got 'route added' event: {}", cr);
        switch (cr.getRouteType()) {
        case CellRoute.DOMAIN:
            if ((_watchCell != null) && gate.getCellName().equals(_watchCell)) {
                //
                // the upstream route (we only support one)
                //
                try {
                    CellRoute defRoute =
                            new CellRoute("", "*@" + cr.getDomainName(), CellRoute.DEFAULT);
                    _nucleus.routeAdd(defRoute);
                } catch (IllegalArgumentException e) {
                    _log.warn("Couldn't add default route: {}", e.getMessage());
                }
            } else {
                //
                // possible downstream routes
                //
                // _log.info("Downstream route added : "+ cr);
                _log.info("Downstream route added to domain {}", cr.getDomainName());
                //
                // If the locationManager takes over control
                // the default route may be installed before
                // the actual domainRouted is added. Therefore
                // we have to 'updateUpstream' for each route.
                updateUpstream();
                updateTopicsUpstream();
            }
            break;
        case CellRoute.TOPIC:
            topicRouteAdded(cr);
            break;
        case CellRoute.DEFAULT:
            _log.info("Default route was added");
            setDefaultInstalled(true);
            updateUpstream();
            updateTopicsUpstream();
            break;
        }
    }

    @Override
    public void routeDeleted(CellEvent ce)
    {
        CellRoute cr = (CellRoute)ce.getSource();
        CellAddressCore gate = new CellAddressCore(cr.getTargetName());
        switch (cr.getRouteType()) {
        case CellRoute.DOMAIN:
            if ((_watchCell != null) && gate.getCellName().equals(_watchCell)) {
                CellRoute defRoute =
                        new CellRoute("", "*@" + cr.getDomainName(), CellRoute.DEFAULT);
                _nucleus.routeDelete(defRoute);
            } else {
                removeRoutingInfo(cr.getDomainName());
            }
            break;
        case CellRoute.TOPIC:
            topicRouteRemoved(cr);
            break;
        case CellRoute.DEFAULT:
            setDefaultInstalled(false);
            break;
        }
    }

    public String ac_update(Args args)
    {
        updateUpstream();
        return "Done";
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
    public synchronized Object ac_ls_$_0( Args args) {

    	Object info;

    	if (!args.hasOption("x")) {
    		// Throw together some meaningful output.
    		ByteArrayOutputStream os = new ByteArrayOutputStream();
    		PrintWriter pw = new PrintWriter( os);
        	getInfo( pw);
        	pw.flush();
        	info = os.toString();
        } else {
            info = new Object[] {
                    getCellDomainName(),
                    Sets.newHashSet(_localExports),
                    getWellKnownCellsByDomain(HashSet::new)
            };
        }

    	return info;
    }

    private static boolean isWellKnown(CellAddressCore address)
    {
        return address.getCellDomainName().equals("local");
    }

    private static CellAddressCore getAddressOfWellknown(String domain, CellPath path)
    {
        String cell = path.getDestinationAddress().getCellName();
        List<CellAddressCore> addresses = path.getAddresses();
        if (addresses.size() > 1) {
            domain = addresses.get(addresses.size() - 2).getCellDomainName();
        }
        return new CellAddressCore(cell, domain);
    }

    private static Stream<CellAddressCore> getAddressesOfWellknown(String domain, Set<CellPath> paths)
    {
        return paths.stream()
                .filter(path -> isWellKnown(path.getDestinationAddress()))
                .map(path -> getAddressOfWellknown(domain, path));
    }

    private synchronized <T extends Collection<String>> Map<String,T> getWellKnownCellsByDomain(Supplier<T> collectionFactory)
    {
        return _domainHash.entrySet().stream()
                .flatMap(e -> getAddressesOfWellknown(e.getKey(), e.getValue()))
                .collect(
                        toMultimap(CellAddressCore::getCellDomainName, CellAddressCore::getCellName, collectionFactory));
    }

    /**
     * Returns a {@code Collector} that accumulates the input elements into a
     * new multimap composed from a map mapping keys to collections of values.
     *
     * @param keyMapper a mapping function to produce keys
     * @param valueMapper a mapping function to produce values
     * @param collectionFactory collectionFactory a function which returns new, empty {@code Collection}s into
     *                    which values will be inserted
     * @param <T> the type of the input elements
     * @param <K> the output type of the key mapping function
     * @param <V> the output type of the value mapping function
     * @param <C> the type of the value collections of the generated multimap
     * @return
     */
    private static <T, K, V, C extends Collection<V>> Collector<T, ?, Map<K, C>> toMultimap(
            Function<? super T, ? extends K> keyMapper,
            Function<? super T, ? extends V> valueMapper,
            Supplier<C> collectionFactory)
    {
        return Collectors.toMap(
                keyMapper,
                v -> Stream.of(valueMapper.apply(v)).collect(toCollection(collectionFactory)),
                (s1, s2) -> Stream.concat(s1.stream(), s2.stream()).collect(toCollection(collectionFactory)));
    }
}
