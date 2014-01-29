package org.dcache.cells;

import com.google.common.base.Strings;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.ConnectionMetaData;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Session;
import javax.jms.StreamMessage;
import javax.jms.TextMessage;

import java.io.ByteArrayOutputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutionException;

import dmg.cells.nucleus.CDC;
import dmg.cells.nucleus.CellDomainInfo;
import dmg.cells.nucleus.CellEvent;
import dmg.cells.nucleus.CellEventListener;
import dmg.cells.nucleus.CellExceptionMessage;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellNucleus;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.CellRoute;
import dmg.cells.nucleus.CellTunnel;
import dmg.cells.nucleus.CellTunnelInfo;
import dmg.cells.nucleus.MessageEvent;
import dmg.cells.nucleus.NoRouteToCellException;
import dmg.cells.nucleus.RoutedMessageEvent;

import org.dcache.util.Args;

/**
 * Gateway between Cells and JMS.
 *
 * JMSTunnel defines a default route to itself in the local
 * domain. Routed messages are forwarded to corresponding JMS queues.
 *
 * JMSTunnel acts as a RoutingManager replacement, receiving routing
 * information from downstream RoutingManagers (if any). JMS consumers
 * for these targets are created, and any messages received from JMS
 * are handed over to Cells for further processing.
 *
 * Notice that the local domain should not contain a
 * dmg.cells.services.RoutingManager. The local domain should not
 * contain a dmg.cells.services.LocationManager, except if the local
 * domain acts as upstream for classic Cells tunnels.
 */
public class JMSTunnel
    extends AbstractCell
    implements CellTunnel, ExceptionListener
{
    private static final Logger LOGGER =
        LoggerFactory.getLogger(JMSTunnel.class);

    public final static long CNS_TIMEOUT = 20000;
    public final static long CNS_REGISTRATION_PERIOD = 120000;
    public final static long CACHE_TIME = 120000;
    public final static long MESSAGE_TTL = 300000;

    private final Set<String> _localExports =
        new HashSet<>();

    /** Domain to well known cells in that domain. */
    private final Map<String,Set<String>> _domains =
        new HashMap<>();

    private Timer _timer;

    private final CellNucleus  _nucleus;
    private final ConnectionFactory _factory;
    private Connection _connection;
    private CnsClient _cns;
    private Receiver _receiver;
    private Sender _sender;

    public JMSTunnel(String name, ConnectionFactory factory)
        throws InterruptedException,
               ExecutionException
    {
        super(name, "System", new Args(""));
        _factory = factory;
        _nucleus = getNucleus();
        doInit();
    }

    @Override
    protected void init()
        throws JMSException
    {
        _timer = new Timer(getCellName() + " Timeout Timer", true);

        addCellEventListener();

        /* Setting the default route to this cell ensures that it gets
         * any messages that don't have anywhere else to go.
         */
        _nucleus.routeAdd(new CellRoute(null, getCellName(), CellRoute.DEFAULT));
        _connection = _factory.createConnection();
        _connection.setExceptionListener(this);

        _cns = new CnsClient(getNucleus(), _connection);
        _sender = new Sender(_connection);
        _receiver = new Receiver(_connection);
        _receiver.addConsumer(getCellDomainName());

        _connection.start();
    }

    /**
     * Kills the tunnel and waits for it to shut down. Times out after
     * 1 second.
     */
    public void shutdown()
        throws InterruptedException
    {
        kill();
        getNucleus().join(getCellName(), 1000);
    }

    /**
     * Called by JMS on fatal exceptions.
     *
     * We restart the domain.
     */
    @Override
    public void onException(JMSException exception)
    {
        LOGGER.error("Fatal failure in JMS connection: {}", exception);
        getNucleus().kill("System");
    }

    protected String getDomainQueue(String domain)
    {
        return "cells.domain." + domain.replaceAll("_", "__").replaceAll("-", "_");
    }

    @Override
    synchronized public void cleanUp()
    {
        if (_timer != null) {
            _timer.cancel();
        }

        try {
            if (_cns != null) {
                _cns.unregister();
            }
        } catch (JMSException e) {
            LOGGER.warn("Failed to unregister from cell name service: {}",
                    e.getMessage());
        }

        try {
            if (_connection != null) {
                _connection.close();
            }
        } catch (JMSException e) {
            LOGGER.warn("Failed to close JMS connection: {}",
                    e.getMessage());
        }
    }

    @Override
    synchronized public void getInfo(PrintWriter pw)
    {
        pw.println("Cell name  : " + getCellName());
        pw.println("-> JMS     : " + _sender.getMessageCount());
        pw.println("-> Domain  : " + _receiver.getMessageCount());

        pw.println("Our routing knowledge:");
        pw.append(" Local: ").println(_localExports);
        for (Map.Entry<String,Set<String>> e : _domains.entrySet()) {
            pw.append(' ').append(e.getKey()).append(": ").println(e.getValue());
        }

        pw.println("Our JMS consumers:");
        for (String domain: _receiver.getConsumers()) {
            pw.append(' ').println(getDomainQueue(domain));
        }

        pw.println("CNS cache:");
        for (Map.Entry<String,CacheEntry> e: _sender.getCnsCache().entrySet()) {
            pw.println(String.format(" %-20s %s",
                                     e.getKey(), e.getValue().domain));
        }

        pw.println("JMS:");
        try {
            ConnectionMetaData data = _connection.getMetaData();
            pw.append(" Protocol version: ").println(data.getJMSVersion());
            pw.append(" Provider name: ").println(data.getJMSProviderName());
            pw.append(" Provider version: ").println(data.getProviderVersion());
        } catch (JMSException e) {
            LOGGER.error("Failed to query JMS meta data: {}", e.getMessage());
            pw.println(" Information unavailable");
        }
    }

    @Override
    synchronized public CellTunnelInfo getCellTunnelInfo()
    {
        return new CellTunnelInfo(getCellName(),
                new CellDomainInfo(_nucleus.getCellDomainName()),
                                  null);
    }

    /**
     * Returns Cells message to sender. Not synchronized because it is
     * called from the inner classes and doesn't touch any fields.
     */
    private void returnToSender(CellMessage msg, NoRouteToCellException e)
    {
        try {
            if (!(msg instanceof CellExceptionMessage)) {
                LOGGER.info("Cannot deliver {}", msg);

                CellPath retAddr = msg.getSourcePath().revert();
                CellExceptionMessage ret = new CellExceptionMessage(retAddr, e);
                ret.setLastUOID(msg.getUOID());
                sendMessage(ret);
            }
        } catch (NoRouteToCellException f) {
            LOGGER.warn("Unable to deliver message and unable to return it to sender: {}", msg);
        }
    }

    /**
     * Handles messages from downstream routing managers. Those
     * messages contain information about cells and domains known by
     * the downstream domain.
     */
    @Override
    synchronized public void messageArrived(CellMessage msg)
    {
        Object obj = msg.getMessageObject();
        if (!(obj instanceof String[])){
            return;
        }

        String[] info = (String[]) obj;
        if (info.length > 0){
            LOGGER.info("Routing info arrived for domain: {}", info[0]);

            String domain = info[0];
            Set<String> newCells =
                    new HashSet<>(Arrays.asList(info).subList(1, info.length));
            updateRoutingInfo(domain, newCells);
        }
    }

    /**
     * Handles messages from cells. These are forwarded via
     * JMS. Whether the origin of the message is the local cell or a
     * downstream cell does not matter.
     */
    @Override
    synchronized public void messageArrived(MessageEvent me)
    {
        if (me instanceof RoutedMessageEvent) {
            CellMessage envelope = me.getMessage();
            try {
                _sender.send(envelope);
            } catch (JMSException e) {
                LOGGER.error("Failed to send message: {}", e.getMessage());
                returnToSender(envelope,
                               new NoRouteToCellException(envelope.getUOID(),
                                                          envelope.getDestinationPath(),
                                                          "Communication failure. Message could not be delivered."));
            }
        } else {
            super.messageArrived(me);
        }
    }

    /**
     * Factory method for creating well-known route objects to a cell.
     */
    synchronized private CellRoute
        createWellKnownRoute(String cell, String domain)
    {
        return new CellRoute(cell, "*@" + domain, CellRoute.WELLKNOWN);
    }

    /**
     * Adds and removes routes to well-known cells in
     * <code>domain</code>.
     *
     * @param domain The name of a domain
     * @param cells Well-known cells in <code>domain</code>
     */
    synchronized private void updateRoutingInfo(String domain,
                                                Set<String> cells)
    {
        Set<String> oldCells = _domains.get(domain);
        if (oldCells == null) {
            oldCells = Collections.emptySet();
        }

        for (String cell: cells) {
            if (!_localExports.contains(cell) && !oldCells.remove(cell)) {
                // entry not found, so make it
                if (!cell.startsWith("@")) {
                    LOGGER.info("Adding: {}", cell);
                    try {
                        _nucleus.routeAdd(createWellKnownRoute(cell, domain));
                    } catch (IllegalArgumentException e) {
                        LOGGER.error("Could not add wellknown route: {}", e.toString());
                    }
                }
            }
        }

        // all new routes added now, need to remove the rest
        for (String cell: oldCells) {
            if (!cell.startsWith("@")) {
                LOGGER.info("Removing: {}", cell);
                try {
                    _nucleus.routeDelete(createWellKnownRoute(cell, domain));
                } catch (IllegalArgumentException e) {
                    LOGGER.warn("Could not delete wellknown route: {}",
                            e.getMessage());
                }
            }
        }
        _domains.put(domain, cells);
    }

    /**
     * Cell event listener: Keeps track of locally exported cells.
     */
    @Override
    synchronized public void cellDied(CellEvent ce)
    {
        String name = (String) ce.getSource();
        _localExports.remove(name);
    }

    /**
     * Cell event listener: Keeps track of locally exported cells.
     */
    @Override
    synchronized public void cellExported(CellEvent ce)
    {
        String name = (String) ce.getSource();
        _localExports.add(name);
    }

    /**
     * Cell event listener: Adds a JMS consumer whenever a new DOMAIN
     * route is registered in the cells routing table.
     */
    @Override
    synchronized public void routeAdded(CellEvent ce)
    {
        try {
            CellRoute cr = (CellRoute) ce.getSource();
            if (cr.getRouteType() == CellRoute.DOMAIN) {
                _receiver.addConsumer(cr.getDomainName());
            }
        } catch (JMSException e) {
            LOGGER.error("Failed to create JMS consumer: {}", e.getMessage());
        }
    }

    /**
     * Cell event listener: Removes the JMS consumer for a domain when
     * its DOMAIN route is removed. Also removes any well-known routes
     * to cells in that domain.
     */
    @Override
    synchronized public void routeDeleted(CellEvent ce)
    {
        try {
            CellRoute cr = (CellRoute) ce.getSource();
            if (cr.getRouteType() == CellRoute.DOMAIN) {
                String domain = cr.getDomainName();
                Set<String> empty = Collections.emptySet();
                updateRoutingInfo(domain, empty);
                _receiver.removeConsumer(domain);
            }
        } catch (JMSException e) {
            LOGGER.error("Failed to remove JMS consumer: {}", e.getMessage());
        }
    }

    /**
     * This method returns the current state of the RoutingMgr cell as
     * a (binary) Object.
     *
     * <p>NB. <b>This  is a hack</b>. The correct  method of receiving
     * information  from a  Cell is  via a  Vehicle.  However,  as the
     * RoutingMgr is within the cells  module (which does not have the
     * concept of Vehicles) this cannot be (easily) done.  Instead, we
     * use the existing mechanism of obtaining a binary object via the
     * admin interface  and flag this functionality  as something that
     * should be improved later.
     *
     * @return a representation of the RoutingManager's little brain.
     */
    @Deprecated
    synchronized public Object ac_ls_$_0(Args args)
    {
    	if (!args.hasOption("x")) {
    		// Throw together some meaningful output.
    		ByteArrayOutputStream os = new ByteArrayOutputStream();
    		PrintWriter pw = new PrintWriter(os);
        	getInfo(pw);
        	pw.flush();
        	return os.toString();
        } else {
            Object infoArray[] =
                {_nucleus.getCellDomainName(), _localExports, _domains };
            return infoArray;
        }
    }

    public final static String hh_ls = "[-x]";

    /**
     * Implements functionallity for sending messages through JMS.
     */
    class Sender
        implements MessageListener
    {
        /** Cell to CNS lookup. */
        private final Multimap<String,Lookup> _lookups =
            Multimaps.synchronizedMultimap(HashMultimap.<String,Lookup>create());

        /** CNS cache. */
        private final Map<String,CacheEntry> _cache =
            new LinkedHashMap<>();

        private final Session _session;

        /** Producer for outgoing messages. */
        private final MessageProducer _producer;

        /** Producer for CNS. */
        private final MessageProducer _cns;

        /** Queue for receiving CNS replies. */
        private final Destination _replyQueue;

        /** Message counter. */
        private long _counter;

        public Sender(Connection connection)
            throws JMSException
        {
            _session =
                connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            _producer = _session.createProducer(null);
            _producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
            _producer.setDisableMessageID(true);
            _producer.setDisableMessageTimestamp(true);
            _producer.setTimeToLive(MESSAGE_TTL);

            _cns = _session.createProducer(_session.createQueue(CellNameService.DESTINATION_LOOKUP));
            _cns.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
            _cns.setDisableMessageID(true);
            _cns.setDisableMessageTimestamp(true);
            _cns.setTimeToLive(2 * CNS_TIMEOUT);

            /* We have to use a second session for the message
             * listener, as we are not allowed to issue calls to the
             * session from outside the listener.
             */
            Session session =
                connection.createSession(false, Session.DUPS_OK_ACKNOWLEDGE);
            _replyQueue = session.createTemporaryQueue();
            session.createConsumer(_replyQueue).setMessageListener(this);
        }

        synchronized public Map<String,CacheEntry> getCnsCache()
        {
            return new HashMap(_cache);
        }

        synchronized public String lookup(String cell)
        {
            /* Remove old entries. The cache is kept in insertion
             * order. Since all entries have the same lifetime, we
             * only need to remove entries from the head of the cache.
             */
            long now = System.currentTimeMillis();
            Iterator<CacheEntry> i = _cache.values().iterator();
            while (i.hasNext() && i.next().time < now) {
                i.remove();
            }

            CacheEntry entry = _cache.get(cell);
            return (entry == null) ? null : entry.domain;
        }

        synchronized public void addToCache(String cell, String domain)
        {
            Collection<Lookup> lookupsForCell = _lookups.removeAll(cell);

            if (Strings.isNullOrEmpty(domain)) {
                for (Lookup lookup: lookupsForCell) {
                    if (lookup.cancel()) {
                        CellMessage envelope = lookup.envelope;
                        returnToSender(envelope,
                                       new NoRouteToCellException(envelope.getUOID(),
                                                                  envelope.getDestinationPath(),
                                                                  "No route to " + cell));
                    }
                }
            } else {
                CacheEntry entry =
                    new CacheEntry(System.currentTimeMillis() + CACHE_TIME, domain);
                _cache.put(cell, entry);

                for (Lookup lookup: lookupsForCell) {
                    if (lookup.cancel()) {
                        CellMessage envelope = lookup.envelope;
                        try {
                            send(envelope);
                            continue;
                        } catch (JMSException e) {
                            LOGGER.error("Failed to send message: {}",
                                    e.getMessage());
                        }
                        returnToSender(envelope,
                                       new NoRouteToCellException(envelope.getUOID(),
                                                                  envelope.getDestinationPath(),
                                                                  "Communication failure. Message could not be delivered."));
                    }
                }
            }
        }

        synchronized public void resolve(String cell, CellMessage envelope)
            throws JMSException
        {
            /* Send CNS lookup request.
             */
            TextMessage request = _session.createTextMessage(cell);
            request.setJMSReplyTo(_replyQueue);
            _cns.send(request);

            /* Register request.
             */
            Lookup lookup = new Lookup(cell, envelope);
            _lookups.put(cell, lookup);
            _timer.schedule(lookup, CNS_TIMEOUT);
        }

        /** Called by JMS on CNS reply. */
        @Override
        synchronized public void onMessage(Message message)
        {
            try (CDC ignored = CDC.reset(_nucleus)) {
                TextMessage textMessage = (TextMessage) message;
                String cell = textMessage.getJMSCorrelationID();
                String domain = textMessage.getText();
                addToCache(cell, domain);
            } catch (ClassCastException e) {
                LOGGER.error("Received unexpected reply to CNS request: {}",
                        message);
            } catch (JMSException e) {
                LOGGER.error("Error while resolving well known cell: {}",
                        e.getMessage());
            }
        }

        /** Sends Cells message to JMS queue. */
        synchronized public void send(CellMessage envelope)
            throws JMSException
        {
            CellPath address = envelope.getDestinationPath();
            String cell = address.getCellName();
            String domain = address.getCellDomainName();

            if (cell == null || domain == null) {
                throw new JMSException("Message has no destination: " + envelope);
            }

            if (domain.equals("local") && (domain = lookup(cell)) == null) {
                resolve(cell, envelope);
            } else {
                Destination destination =
                    _session.createQueue(getDomainQueue(domain));
                Message message = _session.createObjectMessage(envelope);
                _producer.send(destination, message);
                _counter++;
            }
        }

        synchronized public long getMessageCount()
        {
            return _counter;
        }

        /** CNS request object and expiration timer. */
        class Lookup extends TimerTask
        {
            final String cell;
            final CellMessage envelope;

            Lookup(String cell, CellMessage envelope)
            {
                this.cell = cell;
                this.envelope = envelope;
            }

            @Override
            public void run()
            {
                try {
                    _lookups.remove(cell, this);
                    returnToSender(envelope,
                                   new NoRouteToCellException(envelope.getUOID(),
                                                              envelope.getDestinationPath(),
                                                              "Failed to resolve well known cell"));
                } catch (RuntimeException e) {
                    LOGGER.error("Message timeout failed", e);

                }
            }
        }
    }

    /** CNS cache entry. */
    static class CacheEntry
    {
        final long time;
        final String domain;

        public CacheEntry(long time, String domain)
        {
            this.time = time;
            this.domain = domain;
        }
    }

    /**
     * Implements functionallity for receiving messages through JMS.
     */
    class Receiver
        implements MessageListener
    {
        /** JMS Connection. */
        private final Connection _connection;

        /** Queue name to JMS sessions. */
        private final Map<String,Session> _sessions = new HashMap();

        /** Message counter. */
        private long _counter;

        public Receiver(Connection connection)
        {
            _connection = connection;
        }

        synchronized public long getMessageCount()
        {
            return _counter;
        }

        synchronized public Collection<String> getConsumers()
        {
            return new ArrayList(_sessions.keySet());
        }

        /**
         * Handles messages from JMS. These are forwarded to cells.
         */
        @Override
        synchronized public void onMessage(Message message)
        {
            try (CDC ignored = CDC.reset(_nucleus)) {
                ObjectMessage objectMessage = (ObjectMessage) message;
                Object object = objectMessage.getObject();
                CellMessage envelope = (CellMessage) object;
                try {
                    sendMessage(envelope);
                    _counter++;
                } catch (NoRouteToCellException e) {
                    returnToSender(envelope, e);
                }
            } catch (ClassCastException e) {
                LOGGER.warn("Dropping unknown message: {}", message);
            } catch (JMSException e) {
                LOGGER.error("Failed to retrieve object from JMS message: {}",
                        e.getMessage());
            }
        }

        synchronized public void addConsumer(String domain)
            throws JMSException
        {
            if (!_sessions.containsKey(domain)) {
                Session session =
                    _connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                Destination queue =
                    session.createQueue(getDomainQueue(domain));
                MessageConsumer consumer = session.createConsumer(queue);
                consumer.setMessageListener(this);
                _sessions.put(domain, session);
            }
        }

        synchronized public void removeConsumer(String domain)
            throws JMSException
        {
            Session session = _sessions.remove(domain);
            if (session != null) {
                session.close();
            }
        }
    }

    /**
     * Implements the CNS client.
     *
     * The client periodically and when well known cells are added
     * registers the domain with the cell name service. Removal of
     * well known cells do not trigger a reregistration: The periodic
     * update will unregister those lazily.
     *
     * Responds to refresh broadcasts.
     */
    class CnsClient
        extends TimerTask
        implements MessageListener,
                   CellEventListener
    {
        protected final Session _session;
        protected final MessageConsumer _consumer;
        protected final MessageProducer _producer;

        /** Name of local domain. */
        protected final String _domain;

        /** Known well known cells. */
        protected final Set<String> _names = new HashSet<>();

        public CnsClient(CellNucleus nucleus, Connection connection)
            throws JMSException
        {
            _domain = nucleus.getCellDomainName();
            _session =
                connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            _producer = _session.createProducer(_session.createTopic(CellNameService.DESTINATION_REGISTRATION));
            _producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
            _producer.setDisableMessageID(true);
            _producer.setDisableMessageTimestamp(true);
            _producer.setTimeToLive(CNS_REGISTRATION_PERIOD);

            Session session =
                connection.createSession(false, Session.DUPS_OK_ACKNOWLEDGE);
            _consumer =
                session.createConsumer(session.createTopic(CellNameService.DESTINATION_REFRESH));
            _consumer.setMessageListener(this);
            nucleus.addCellEventListener(this);

            /* FIXME: How can we be sure that there are not already
             * unregistered exported cells?
             */

            _timer.schedule(this, 0, CNS_REGISTRATION_PERIOD);
        }

        @Override
        public void run()
        {
            try {
                register();
            } catch (RuntimeException e) {
                LOGGER.error("Failed to register with cell name service", e);
            }
        }

        synchronized public void register()
        {
            try {
                StreamMessage msg = _session.createStreamMessage();
                msg.writeString(_domain);
                msg.writeLong(3 * CNS_REGISTRATION_PERIOD);
                msg.writeInt(_names.size());
                for (String name: _names) {
                    msg.writeString(name);
                }
                _producer.send(msg);
            } catch (JMSException e) {
                LOGGER.error("Failed to register with cell name service: {}",
                        e.getMessage());
            }
        }

        synchronized public void unregister()
            throws JMSException
        {
            StreamMessage msg = _session.createStreamMessage();
            msg.writeString(_domain);
            msg.writeLong(0);
            msg.writeInt(0);
            _producer.send(msg);
        }

        /**
         * Handles messages from JMS.
         */
        @Override
        public void onMessage(Message message)
        {
            try (CDC ignored = CDC.reset(_nucleus)) {
                register();
            }
        }

        @Override
        synchronized public void routeAdded(CellEvent ce)
        {
            CellRoute cr = (CellRoute) ce.getSource();
            if (cr.getRouteType() == CellRoute.WELLKNOWN) {
                if (_names.add(cr.getCellName())) {
                    register();
                }
            }
        }

        @Override
        synchronized public void routeDeleted(CellEvent ce)
        {
            CellRoute cr = (CellRoute) ce.getSource();
            if (cr.getRouteType() == CellRoute.WELLKNOWN) {
                if (_names.remove(cr.getCellName())) {
                    register();
                }
            }
        }

        @Override
        synchronized public void cellDied(CellEvent ce)
        {
            String name = (String) ce.getSource();
            if (_names.remove(name)) {
                register();
            }
        }

        @Override
        synchronized public void cellExported(CellEvent ce)
        {
            String name = (String) ce.getSource();
            if (_names.add(name)) {
                register();
            }
        }

        @Override
        public void cellCreated(CellEvent ce) {}
    }
}
