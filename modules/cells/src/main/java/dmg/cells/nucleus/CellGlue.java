package dmg.cells.nucleus;

import com.google.common.collect.Maps;
import com.google.common.io.BaseEncoding;
import com.google.common.primitives.Longs;
import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
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

import static com.google.common.base.Preconditions.checkState;

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
    private CellRoutingTable _routingTable = new CellRoutingTable();
    private ThreadGroup _masterThreadGroup;

    private ThreadGroup _killerThreadGroup;
    private final Executor _killerExecutor;
    private final ThreadPoolExecutor _emergencyKillerExecutor;
    private final CellAddressCore _domainAddress;
    private final CuratorFramework _curatorFramework;

    CellGlue(String cellDomainName, CuratorFramework curatorFramework)
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
        _curatorFramework = curatorFramework;
        _domainAddress = new CellAddressCore("*", _cellDomainName);
        _masterThreadGroup = new ThreadGroup("Master-Thread-Group");
        _killerThreadGroup = new ThreadGroup("Killer-Thread-Group");
        ThreadFactory killerThreadFactory = r -> new Thread(_killerThreadGroup, r);
        _killerExecutor = Executors.newCachedThreadPool(killerThreadFactory);
        _emergencyKillerExecutor = new ThreadPoolExecutor(1, 1,
                                                          0L, TimeUnit.MILLISECONDS,
                                                          new LinkedBlockingQueue<>(),
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
        checkState(_systemNucleus == null);
        _systemNucleus = nucleus;
    }

    CellNucleus getSystemNucleus()
    {
        return _systemNucleus;
    }

    void export(CellNucleus cell)
    {
        sendToAll(new CellEvent(cell.getCellName(), CellEvent.CELL_EXPORTED_EVENT));
    }

    void subscribe(CellNucleus cell, String topic)
    {
        routeAdd(new CellRoute(topic, cell.getThisAddress(), CellRoute.TOPIC));
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
        CellAddressCore target = route.getTarget();
        if (target.getCellDomainName().equals(getCellDomainName()) && !_cellList.containsKey(target.getCellName())) {
            throw new IllegalArgumentException("No such cell: " + target);
        }
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
        /* Remove routes to this cell first to allow it to be drained cleanly.
         */
        for (CellRoute route : _routingTable.delete(destination.getThisAddress())) {
            sendToAll(new CellEvent(route, CellEvent.CELL_ROUTE_DELETED_EVENT));
        }

        /* Mark the cell as being killed to prevent it from being killed more
         * than once and to block certain operations while it is being killed.
         */
        String cellToKill = destination.getCellName();
        if (!_killedCells.add(destination)) {
            LOGGER.trace("Cell is being killed: {}", cellToKill);
            return;
        }

        /* Post the obituary.
         */
        CellPath sourceAddr = new CellPath(source.getCellName(), getCellDomainName());
        KillEvent killEvent = new KillEvent(sourceAddr, to);
        sendToAll(new CellEvent(cellToKill, CellEvent.CELL_DIED_EVENT));

        /* Put out a contract.
         */
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

    /**
     * Send a message to another cell.
     *
     * @param msg The cell envelope
     * @param resolveLocally Whether to deliver messages for @local addresses to local cells
     * @param resolveRemotely Whether to deliver messages for @local addresses to remote cells
     * @throws SerializationException
     */
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

                /* Topic routes are special because they cause messages to be
                 * duplicated.
                 */
                for (CellRoute route : _routingTable.findTopicRoutes(address)) {
                    CellAddressCore target = route.getTarget();
                    boolean isLocalSubscriber = !target.getCellName().equals("*");
                    if (isLocalSubscriber || resolveRemotely) {
                        CellMessage m = msg.clone();
                        if (isLocalSubscriber) {
                            m.getDestinationPath().replaceCurrent(target);
                        }
                        sendMessage(m, target, true, resolveRemotely, steps - 1);
                    }
                    hasTopicRoutes = true;
                }

                if (!resolveRemotely) {
                    if (!hasTopicRoutes) {
                        sendException(msg, address.toString());
                    }
                    return;
                }
            }

            /* Unless we updated the destination path, there is no reason to send the message back to where
             * we got it from. Note that we cannot detect non-trivial loops, i.e. loops involving three
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
                    LOGGER.error("Received malformed message from {} with UOID {} and session [{}]: {}",
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

    private void sendException(CellMessage msg, String routeTarget)
            throws SerializationException
    {
        if (msg.getSourceAddress().getCellName().equals("*")) {
            Serializable messageObject = msg.decode().getMessageObject();
            if (messageObject instanceof NoRouteToCellException) {
                LOGGER.warn(
                        "Unable to notify {} about delivery failure of message sent to {}: No route for {} in {}.",
                        msg.getDestinationPath(), ((NoRouteToCellException) messageObject).getDestinationPath(),
                        routeTarget, _cellDomainName);
            } else {
                LOGGER.warn(
                        "Message from {} could not be delivered because no route to {} is known.",
                        msg.getSourcePath(), routeTarget);
            }
        } else {
            LOGGER.debug(
                    "Message from {} could not be delivered because no route to {} is known; the sender will be notified.",
                    msg.getSourcePath(), routeTarget);
            CellMessage envelope = new CellMessage(msg.getSourcePath().revert(),
                                                   new NoRouteToCellException(msg,
                                                                              "Route for >" + routeTarget +
                                                                              "< not found at >" + _cellDomainName + "<"));
            envelope.setLastUOID(msg.getUOID());
            envelope.addSourceAddress(_domainAddress);
            sendMessage(envelope, true, true);
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

    public CuratorFramework getCuratorFramework()
    {
        return _curatorFramework;
    }
}
