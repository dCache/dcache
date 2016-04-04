package dmg.cells.nucleus;

import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import com.google.common.io.BaseEncoding;
import com.google.common.primitives.Longs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import dmg.util.TimebasedCounter;

/**
 * @author Patrick Fuhrmann
 * @version 0.1, 15 Feb 1998
 */
class CellGlue
{
    private static final Logger LOGGER =
            LoggerFactory.getLogger(CellGlue.class);

    private final String _cellDomainName;
    private final ConcurrentMap<String, CellNucleus> _cellList = Maps.newConcurrentMap();
    private final Set<CellNucleus> _killedCells = Collections.newSetFromMap(
            Maps.<CellNucleus, Boolean>newConcurrentMap());
    private final Map<String, List<CellEventListener>> _cellEventListener =
            new ConcurrentHashMap<>();
    private final Map<String, Object> _cellContext =
            new ConcurrentHashMap<>();
    private final TimebasedCounter _uniqueCounter = new TimebasedCounter();
    private final BaseEncoding COUNTER_ENCODING = BaseEncoding.base64Url().omitPadding();
    private CellNucleus _systemNucleus;
    private ClassLoaderProvider _classLoader;
    private CellRoutingTable _routingTable = new CellRoutingTable();
    private ThreadGroup _masterThreadGroup;

    private ThreadGroup _killerThreadGroup;
    private final Executor _killerExecutor;
    private final ThreadPoolExecutor _emergencyKillerExecutor;
    private final CellAddressCore _domainAddress;

    CellGlue(String cellDomainName)
    {
        String cellDomainNameLocal = cellDomainName;

        if ((cellDomainName == null) || (cellDomainName.equals(""))) {
            cellDomainNameLocal = "*";
        }

        if (cellDomainNameLocal.charAt(cellDomainNameLocal.length() - 1) == '*') {
            cellDomainNameLocal =
                    cellDomainNameLocal.substring(0, cellDomainNameLocal.length()) +
                    System.currentTimeMillis();
        }
        _cellDomainName = cellDomainNameLocal;
        _domainAddress = new CellAddressCore("*", _cellDomainName);
        _classLoader = new ClassLoaderProvider();
        _masterThreadGroup = new ThreadGroup("Master-Thread-Group");
        _killerThreadGroup = new ThreadGroup("Killer-Thread-Group");
        ThreadFactory killerThreadFactory = new ThreadFactory()
        {
            @Override
            public Thread newThread(Runnable r)
            {
                return new Thread(_killerThreadGroup, r);
            }
        };
        _killerExecutor = Executors.newCachedThreadPool(killerThreadFactory);
        _emergencyKillerExecutor = new ThreadPoolExecutor(1, 1,
                                                          0L, TimeUnit.MILLISECONDS,
                                                          new LinkedBlockingQueue<Runnable>(),
                                                          killerThreadFactory);
        _emergencyKillerExecutor.prestartCoreThread();
    }

    ThreadGroup getMasterThreadGroup()
    {
        return _masterThreadGroup;
    }

    void addCell(String name, CellNucleus cell)
            throws IllegalArgumentException
    {
        if (_cellList.putIfAbsent(name, cell) != null) {
            throw new IllegalArgumentException("Name Mismatch ( cell " + name + " exist )");
        }
        sendToAll(new CellEvent(name, CellEvent.CELL_CREATED_EVENT));
    }

    void setSystemNucleus(CellNucleus nucleus)
    {
        _systemNucleus = nucleus;
    }

    CellNucleus getSystemNucleus()
    {
        return _systemNucleus;
    }

    String[][] getClassProviders()
    {
        return _classLoader.getProviders();
    }

    void setClassProvider(String selection, String provider)
    {
        String type;
        String value = null;
        int pos = provider.indexOf(':');
        if (pos < 0) {
            if (provider.indexOf('/') >= 0) {
                type = "dir";
                value = provider;
            } else if (provider.indexOf('@') >= 0) {
                type = "cell";
                value = provider;
            } else if (provider.equals("system")) {
                type = "system";
            } else if (provider.equals("none")) {
                type = "none";
            } else {
                throw new
                        IllegalArgumentException("Can't determine provider type");
            }
        } else {
            type = provider.substring(0, pos);
            value = provider.substring(pos + 1);
        }
        switch (type) {
        case "dir":
            File file = new File(value);
            if (!file.isDirectory()) {
                throw new
                        IllegalArgumentException("Not a directory : " + value);
            }
            _classLoader.addFileProvider(selection, new File(value));
            break;
        case "cell":
            _classLoader.addCellProvider(selection,
                                         _systemNucleus,
                                         new CellPath(value));
            break;
        case "system":
            _classLoader.addSystemProvider(selection);
            break;
        case "none":
            _classLoader.removeSystemProvider(selection);
            break;
        default:
            throw new
                    IllegalArgumentException("Provider type not supported : " + type);
        }

    }

    void export(CellNucleus cell)
    {
        sendToAll(new CellEvent(cell.getCellName(), CellEvent.CELL_EXPORTED_EVENT));
    }

    void subscribe(CellNucleus cell, String topic)
    {
        routeAdd(new CellRoute(topic, cell.getThisAddress().toString(), CellRoute.TOPIC));
    }

    private Class<?> _loadClass(String className) throws ClassNotFoundException
    {
        return _classLoader.loadClass(className);
    }

    public Class<?> loadClass(String className) throws ClassNotFoundException
    {
        return _classLoader.loadClass(className);
    }

    Cell _newInstance(String className,
                      String cellName,
                      Object[] args,
                      boolean systemOnly)
            throws ClassNotFoundException,
            NoSuchMethodException,
            SecurityException,
            InstantiationException,
            InvocationTargetException,
            IllegalAccessException,
            ClassCastException
    {
        Class<? extends Cell> newClass;
        if (systemOnly) {
            newClass = Class.forName(className).asSubclass(Cell.class);
        } else {
            newClass = _loadClass(className).asSubclass(Cell.class);
        }

        Object[] arguments = new Object[args.length + 1];
        arguments[0] = cellName;
        System.arraycopy(args, 0, arguments, 1, args.length);
        Class<?>[] argClass = new Class<?>[arguments.length];
        for (int i = 0; i < arguments.length; i++) {
            argClass[i] = arguments[i].getClass();
        }

        return newClass.getConstructor(argClass).
                newInstance(arguments);
    }

    Cell _newInstance(String className,
                      String cellName,
                      String[] argsClassNames,
                      Object[] args,
                      boolean systemOnly)
            throws ClassNotFoundException,
            NoSuchMethodException,
            SecurityException,
            InstantiationException,
            InvocationTargetException,
            IllegalAccessException,
            ClassCastException
    {
        Class<? extends Cell> newClass;
        if (systemOnly) {
            newClass = Class.forName(className).asSubclass(Cell.class);
        } else {
            newClass = _loadClass(className).asSubclass(Cell.class);
        }

        Object[] arguments = new Object[args.length + 1];
        arguments[0] = cellName;

        System.arraycopy(args, 0, arguments, 1, args.length);

        Class<?>[] argClasses = new Class<?>[arguments.length];

        ClassLoader loader = newClass.getClassLoader();
        argClasses[0] = String.class;
        if (loader == null) {
            for (int i = 1; i < argClasses.length; i++) {
                argClasses[i] = Class.forName(argsClassNames[i - 1]);
            }
        } else {
            for (int i = 1; i < argClasses.length; i++) {
                argClasses[i] = loader.loadClass(argsClassNames[i - 1]);
            }
        }

        Constructor<? extends Cell> constructor = newClass.getConstructor(argClasses);
        try {
            return constructor.newInstance(arguments);
        } catch (InvocationTargetException e) {
            for (Class<?> clazz : constructor.getExceptionTypes()) {
                if (clazz.isAssignableFrom(e.getTargetException().getClass())) {
                    throw e;
                }
            }
            throw Throwables.propagate(e.getTargetException());
        }
    }

    Map<String, Object> getCellContext()
    {
        return _cellContext;
    }

    Object getCellContext(String str)
    {
        return _cellContext.get(str);
    }

    public void routeAdd(CellRoute route)
    {
        _routingTable.add(route);
        sendToAll(new CellEvent(route, CellEvent.CELL_ROUTE_ADDED_EVENT));
    }

    public void routeDelete(CellRoute route)
    {
        _routingTable.delete(route);
        sendToAll(new CellEvent(route, CellEvent.CELL_ROUTE_DELETED_EVENT));
    }

    CellRoutingTable getRoutingTable()
    {
        return _routingTable;
    }

    CellRoute[] getRoutingList()
    {
        return _routingTable.getRoutingList();
    }

    List<CellTunnelInfo> getCellTunnelInfos()
    {
        List<CellTunnelInfo> v = new ArrayList<>();
        for (CellNucleus cellNucleus : _cellList.values()) {
            Cell c = cellNucleus.getThisCell();
            if (c instanceof CellTunnel) {
                v.add(((CellTunnel) c).getCellTunnelInfo());
            }
        }
        return v;
    }

    List<String> getCellNames()
    {
        return new ArrayList<>(_cellList.keySet());
    }

    String getUnique()
    {
        return COUNTER_ENCODING.encode(Longs.toByteArray(_uniqueCounter.next()));
    }

    CellInfo getCellInfo(String name)
    {
        CellNucleus nucleus = getCell(name);
        return (nucleus == null) ? null : nucleus._getCellInfo();
    }

    Thread[] getThreads(String name)
    {
        CellNucleus nucleus = getCell(name);
        return (nucleus == null) ? null : nucleus.getThreads();
    }

    private void sendToAll(CellEvent event)
    {
        //
        // inform our event listener
        //

        for (List<CellEventListener> listners : _cellEventListener.values()) {

            for (CellEventListener hallo : listners) {

                if (hallo == null) {
                    LOGGER.trace("event distributor found NULL");
                    continue;
                }
                try {
                    switch (event.getEventType()) {
                    case CellEvent.CELL_CREATED_EVENT:
                        hallo.cellCreated(event);
                        break;
                    case CellEvent.CELL_EXPORTED_EVENT:
                        hallo.cellExported(event);
                        break;
                    case CellEvent.CELL_DIED_EVENT:
                        hallo.cellDied(event);
                        break;
                    case CellEvent.CELL_ROUTE_ADDED_EVENT:
                        hallo.routeAdded(event);
                        break;
                    case CellEvent.CELL_ROUTE_DELETED_EVENT:
                        hallo.routeDeleted(event);
                        break;
                    }
                } catch (Exception e) {
                    LOGGER.info("Error while sending {}: {}", event, e);
                }
            }

        }
    }

    String getCellDomainName()
    {
        return _cellDomainName;
    }

    void kill(CellNucleus nucleus)
    {
        _kill(nucleus, nucleus, 0);
    }

    void kill(CellNucleus sender, String cellName)
            throws IllegalArgumentException
    {
        CellNucleus nucleus = _cellList.get(cellName);
        if (nucleus == null || _killedCells.contains(nucleus)) {
            throw new IllegalArgumentException("Cell Not Found : " + cellName);
        }
        _kill(sender, nucleus, 0);

    }

    /**
     * Print diagnostic information about a cell's ThreadGroup to
     * stdout.
     */
    void threadGroupList(String cellName)
    {
        CellNucleus nucleus = _cellList.get(cellName);
        if (nucleus != null) {
            nucleus.threadGroupList();
        } else {
            LOGGER.warn("cell {} is not running", cellName);
        }
    }

    /**
     * Returns a named cell. This method also returns cells that have
     * been killed, but which are not dead yet.
     *
     * @param cellName the name of the cell
     * @return The cell with the given name or null if there is no such
     * cell.
     */
    CellNucleus getCell(String cellName)
    {
        return _cellList.get(cellName);
    }

    /**
     * Blocks until the given cell is dead.
     *
     * @param cellName the name of the cell
     * @param timeout  the time to wait in milliseconds. A timeout
     *                 of 0 means to wait forever.
     * @return True if the cell died, false in case of a timeout.
     * @throws InterruptedException if another thread interrupted the
     *                              current thread before or while the current thread was
     *                              waiting for a notification. The interrupted status of
     *                              the current thread is cleared when this exception is
     *                              thrown.
     */
    synchronized boolean join(String cellName, long timeout) throws InterruptedException
    {
        if (timeout == 0) {
            while (getCell(cellName) != null) {
                wait();
            }
            return true;
        } else {
            while (getCell(cellName) != null && timeout > 0) {
                long time = System.currentTimeMillis();
                wait(timeout);
                timeout = timeout - (System.currentTimeMillis() - time);
            }
            return (timeout > 0);
        }
    }

    synchronized void destroy(CellNucleus nucleus)
    {
        _cellEventListener.remove(nucleus.getCellName());
        _cellList.remove(nucleus.getCellName());
        _killedCells.remove(nucleus);
        LOGGER.trace("destroy : sendToAll : killed {}", nucleus.getCellName());
        notifyAll();
    }

    private void _kill(CellNucleus source, final CellNucleus destination, long to)
    {
        String cellToKill = destination.getCellName();
        if (!_killedCells.add(destination)) {
            LOGGER.trace("Cell is being killed: {}", cellToKill);
            return;
        }

        CellPath sourceAddr = new CellPath(source.getCellName(), getCellDomainName());
        KillEvent killEvent = new KillEvent(sourceAddr, to);
        sendToAll(new CellEvent(cellToKill, CellEvent.CELL_DIED_EVENT));

        for (CellRoute route : _routingTable.delete(destination.getThisAddress())) {
            sendToAll(new CellEvent(route, CellEvent.CELL_ROUTE_DELETED_EVENT));
        }

        Runnable command = () -> destination.shutdown(killEvent);
        try {
            _killerExecutor.execute(command);
        } catch (OutOfMemoryError e) {
            /* This can signal that we cannot create any more threads. The emergency
             * pool has one thread preallocated for this situation.
             */
            _emergencyKillerExecutor.execute(command);
        }
    }

    private static final int MAX_ROUTE_LEVELS = 16;

    void sendMessage(CellMessage msg, boolean resolveLocally, boolean resolveRemotely)
            throws SerializationException
    {
        if (!msg.isStreamMode()) {
            msg = msg.encode();
        }
        CellPath destination = msg.getDestinationPath();
        LOGGER.trace("sendMessage : {} send to {}", msg.getUOID(), destination);
        sendMessage(msg, destination.getCurrent(), resolveLocally, resolveRemotely, MAX_ROUTE_LEVELS);
    }

    private void sendMessage(CellMessage msg, CellAddressCore address, boolean resolveLocally, boolean resolveRemotely, int steps)
    {
        CellPath destination = msg.getDestinationPath();

        /* We track whether we advanced the current position in the destination path. If not, we refuse
         * to send the message back to the domain we got it from.
         */
        boolean hasDestinationChanged = false;

        /* We track whether the message has been delivered with any topic routes. If so, failure to find
         * any other routes will not generate an error.
         */
        boolean hasTopicRoutes = false;

        while (steps > 0) {
            /* Skip our own domain in the address as we are already here.
             */
            while (address.equals(_domainAddress)) {
                if (!destination.next()) {
                    sendException(msg, "*");
                    return;
                }
                address = destination.getCurrent();
                hasDestinationChanged = true;
            }

            LOGGER.trace("sendMessage : next hop at {}: {}", steps, address);

            /* If explicitly addressed to a cell in our domain we have to deliver
             * it now.
             */
            if (address.getCellDomainName().equals(_cellDomainName)) {
                if (!deliverLocally(msg, address)) {
                    sendException(msg, address.toString());
                }
                return;
            }

            /* If the address if not fully qualified we have the choice of resolving
             * it locally or through the routing table.
             */
            if (address.getCellDomainName().equals("local")) {
                if (resolveLocally && deliverLocally(msg, address)) {
                    return;
                }
                if (!resolveRemotely) {
                    sendException(msg, address.toString());
                    return;
                }

                /* Topic routes are special because they cause messages to be
                 * duplicated.
                 */
                for (CellRoute route : _routingTable.findTopicRoutes(address)) {
                    CellMessage m = msg.clone();
                    CellAddressCore target = route.getTarget();
                    if (!target.getCellName().equals("*")) {
                        m.getDestinationPath().replaceCurrent(target);
                    }
                    sendMessage(m, target, true, true, steps - 1);
                    hasTopicRoutes = true;
                }
            }

            /* Unless we updated the destination path, there is no reason to send the message back from
             * where we got it. Note that we cannot detect non-trivial loops, i.e. loops involving three
             * or more domains: Such loops may have legitimate alias-routes rewriting the destination
             * and sending the message to where it has been before may be perfectly reasonable.
             */
            if (!hasDestinationChanged && msg.getSourcePath().getDestinationAddress().equals(address)) {
                if (!hasTopicRoutes) {
                    sendException(msg, address.toString());
                }
                return;
            }

            /* The delivery restrictions do not apply to routes.
             */
            resolveLocally = true;
            resolveRemotely = true;

            /* Lookup a route.
             */
            CellRoute route = _routingTable.find(address);
            if (route == null) {
                LOGGER.trace("sendMessage : no route destination for : {}", address);
                if (!hasTopicRoutes) {
                    sendException(msg, address.toString());
                }
                return;
            }
            LOGGER.trace("sendMessage : using route : {}", route);
            address = route.getTarget();

            /* Alias routes rewrite the address.
             */
            if (route.getRouteType() == CellRoute.ALIAS) {
                destination.replaceCurrent(address);
                hasDestinationChanged = true;
            }
            steps--;
        }
        // end of big iteration loop

        LOGGER.trace("sendMessage : max route iteration reached: {}", destination);
        sendException(msg, address.toString());
    }

    private boolean deliverLocally(CellMessage msg, CellAddressCore address)
    {
        CellNucleus destNucleus = _cellList.get(address.getCellName());
        if (destNucleus != null && !_killedCells.contains(destNucleus)) {
            /* Is the message addressed to the cell or is the cell merely a router.
             */
            CellPath destinationPath = msg.getDestinationPath();
            if (address.equals(destinationPath.getCurrent())) {
                try {
                    destNucleus.addToEventQueue(new MessageEvent(msg.decode()));
                } catch (SerializationException e) {
                    LOGGER.error("Received malformed message from %s with UOID %s and session [%s]: %s",
                                 msg.getSourcePath(), msg.getUOID(), msg.getSession(), e.getMessage());
                    sendException(msg, address.toString());
                }
            } else if (msg.getSourcePath().hops() > 30) {
                LOGGER.error("Hop count exceeds 30: {}", msg);
                sendException(msg, address.toString());
            } else {
                msg.addSourceAddress(_domainAddress);
                destNucleus.addToEventQueue(new RoutedMessageEvent(msg));
            }
            return true;
        }
        return false;
    }

    private void sendException(CellMessage msg,
                               String routeTarget)
            throws SerializationException
    {
        if (msg instanceof CellExceptionMessage) {
            LOGGER.warn(
                    "Unable to notify {} about delivery failure of message sent to {}: No route for {} in {}.",
                    msg.getDestinationPath(), ((CellExceptionMessage) msg.decode()).getException().getDestinationPath(),
                    routeTarget, _cellDomainName);
        } else {
            LOGGER.debug(
                    "Message from {} could not be delivered because no route to {} is known; the sender will be notified.",
                    msg.getSourcePath(), routeTarget);
            NoRouteToCellException exception =
                    new NoRouteToCellException(msg,
                            "Route for >" + routeTarget +
                            "< not found at >" + _cellDomainName + "<");
            CellPath retAddr = msg.getSourcePath().revert();
            CellExceptionMessage ret = new CellExceptionMessage(retAddr, exception);
            ret.setLastUOID(msg.getUOID());
            ret.addSourceAddress(_domainAddress);
            sendMessage(ret, true, true);
        }
    }

    void addCellEventListener(CellNucleus nucleus, CellEventListener listener)
    {
        _cellEventListener.computeIfAbsent(nucleus.getCellName(), k -> new CopyOnWriteArrayList<>()).add(listener);
    }

    @Override
    public String toString()
    {
        return _cellDomainName;
    }
}
