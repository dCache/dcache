package org.dcache.cells;

import java.util.Set;
import java.util.Map;
import java.util.HashSet;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Iterator;
import java.util.Timer;
import java.util.TimerTask;
import java.util.Collection;
import java.util.Collections;
import java.util.ArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ConcurrentHashMap;
import java.io.PrintWriter;
import java.io.ByteArrayOutputStream;

import dmg.cells.nucleus.CellNucleus;
import dmg.cells.nucleus.CellRoute;
import dmg.cells.nucleus.CellEvent;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.CellAddressCore;
import dmg.cells.nucleus.MessageEvent;
import dmg.cells.nucleus.RoutedMessageEvent;
import dmg.cells.nucleus.NoRouteToCellException;
import dmg.cells.nucleus.CellExceptionMessage;
import dmg.cells.nucleus.CellTunnel;
import dmg.cells.nucleus.CellTunnelInfo;
import dmg.cells.nucleus.CellEventListener;
import dmg.util.Args;

import org.apache.log4j.Logger;
import javax.jms.ConnectionFactory;
import javax.jms.Connection;
import javax.jms.Session;
import javax.jms.Destination;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Message;
import javax.jms.TextMessage;
import javax.jms.ObjectMessage;
import javax.jms.MessageListener;
import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.ConnectionMetaData;

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
    implements CellTunnel
{
    private final static Logger _logMessages =
        Logger.getLogger("logger.org.dcache.cells.messages");
    private final static Logger _log =
        Logger.getLogger(JMSTunnel.class);

    private final static long ARP_TIMEOUT = 10000;
    private final static long CACHE_TIME = 120000;

    private final static String ARP_TOPIC = "cells.arp";

    private final Set<String> _localExports = new HashSet();

    /** Domain to well known cells in that domain. */
    private final Map<String,Set<String>> _domains = new HashMap();

    private final Timer _timer = new Timer(true);

    private final CellNucleus  _nucleus;
    private final ConnectionFactory _factory;
    private Connection _connection;
    private ArpServer _arp;
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

    protected void init()
        throws JMSException
    {
        addCellEventListener();

        /* Setting the default route to this cell ensures that it gets
         * any messages that don't have anywhere else to go.
         */
        _nucleus.routeAdd(new CellRoute(null, getCellName(), CellRoute.DEFAULT));
        _connection = _factory.createConnection();
        _connection.start();

        _arp = new ArpServer(getNucleus(), _connection);
        _sender = new Sender(_connection);
        _receiver = new Receiver(_connection);
        _receiver.addConsumer(getCellDomainName());
    }

    protected String getDomainQueue(String domain)
    {
        return "cells.domain." + domain;
    }

    synchronized public void cleanUp()
    {
        try {
            _timer.cancel();

            if (_connection != null) {
                _connection.close();
            }
        } catch (JMSException e) {
            _log.warn("Failed to close JMS connection: " + e);
        }
    }

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

        pw.println("ARP cache:");
        for (Map.Entry<String,CacheEntry> e: _sender.getArpCache().entrySet()) {
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
            _log.error("Failed to query JMS meta data: " + e.getMessage());
            pw.println(" Information unavailable");
        }
    }

    synchronized public CellTunnelInfo getCellTunnelInfo()
    {
        return new CellTunnelInfo(getCellName(),
                                  _nucleus.getCellDomainInfo(),
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
                _log.info("Cannot deliver " + msg);

                CellPath retAddr = (CellPath)msg.getSourcePath().clone();
                retAddr.revert();
                CellExceptionMessage ret = new CellExceptionMessage(retAddr, e);
                ret.setLastUOID(msg.getUOID());
                sendMessage(ret);
            }
        } catch (NoRouteToCellException f) {
            _log.warn("Unable to deliver message and unable to return it to sender: " + msg);
        }
    }

    /**
     * Handles messages from downstream routing managers. Those
     * messages contain information about cells and domains known by
     * the downstream domain.
     */
    synchronized public void messageArrived(CellMessage msg)
    {
        Object obj = msg.getMessageObject();
        if (!(obj instanceof String[])){
            return;
        }

        String[] info = (String[]) obj;
        if (info.length > 0){
            _log.info("Routing info arrived for domain: " + info[0]);

            String domain = info[0];
            Set<String> newCells = new HashSet<String>();
            for (int i = 1; i < info.length; i++){
                newCells.add(info[i]);
            }
            updateRoutingInfo(domain, newCells);
        }
    }

    /**
     * Handles messages from cells. These are forwarded via
     * JMS. Whether the origin of the message is the local cell or a
     * downstream cell does not matter.
     */
    synchronized public void messageArrived(MessageEvent me)
    {
        if (me instanceof RoutedMessageEvent) {
            CellMessage envelope = me.getMessage();
            try {
                _sender.send(envelope);
            } catch (JMSException e) {
                _log.error("Error while sending message: " + e.getMessage());
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
                    _log.info("Adding: " + cell);
                    try {
                        _nucleus.routeAdd(createWellKnownRoute(cell, domain));
                    } catch (IllegalArgumentException e) {
                        _log.error("Could not add wellknown route: " + e);
                    }
                }
            }
        }

        // all new routes added now, need to remove the rest
        for (String cell: oldCells) {
            if (!cell.startsWith("@")) {
                _log.info("Removing: " + cell);
                try {
                    _nucleus.routeDelete(createWellKnownRoute(cell, domain));
                } catch (IllegalArgumentException e) {
                    _log.warn("Could not delete wellknown route: " + e);
                }
            }
        }
        _domains.put(domain, cells);
    }

    /**
     * Cell event listener: Keeps track of locally exported cells.
     */
    synchronized public void cellDied(CellEvent ce)
    {
        String name = (String) ce.getSource();
        _localExports.remove(name);
    }

    /**
     * Cell event listener: Keeps track of locally exported cells.
     */
    synchronized public void cellExported(CellEvent ce)
    {
        String name = (String) ce.getSource();
        _localExports.add(name);
    }

    /**
     * Cell event listener: Adds a JMS consumer whenever a new DOMAIN
     * route is registered in the cells routing table.
     */
    synchronized public void routeAdded(CellEvent ce)
    {
        try {
            CellRoute cr = (CellRoute) ce.getSource();
            if (cr.getRouteType() == CellRoute.DOMAIN) {
                _receiver.addConsumer(cr.getDomainName());
            }
        } catch (JMSException e) {
            _log.error("Failed to create JMS consumer: " + e);
        }
    }

    /**
     * Cell event listener: Removes the JMS consumer for a domain when
     * its DOMAIN route is removed. Also removes any well-known routes
     * to cells in that domain.
     */
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
            _log.error("Failed to remove JMS consumer: " + e);
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
    	if (args.getOpt("x") == null) {
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
        /** JMS Message ID to ARP lookup. */
        private final Map<String,Lookup> _lookups = new ConcurrentHashMap();

        /** ARP cache. */
        private final Map<String,CacheEntry> _cache = new LinkedHashMap();

        private final Session _session;

        /** Producer for outgoing messages. */
        private final MessageProducer _producer;

        /** Producer for ARP. */
        private final MessageProducer _arp;

        /** Queue for receiving ARP replies. */
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

            _arp = _session.createProducer(_session.createTopic(ARP_TOPIC));
            _arp.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
            _arp.setDisableMessageTimestamp(true);

            /* We have to use a second session for the message
             * listener, as we are not allowed to issue calls to the
             * session from outside the listener.
             */
            Session session =
                connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            _replyQueue = session.createTemporaryQueue();
            session.createConsumer(_replyQueue).setMessageListener(this);
        }

        synchronized private void logSend(CellMessage envelope)
        {
            if (_logMessages.isDebugEnabled()) {
                Object object = envelope.getMessageObject();
                String messageObject =
                    object == null ? "NULL" : object.getClass().getName();
                _logMessages.debug("tunnelMessageArrived src="
                                   + envelope.getSourceAddress()
                                   + " dest=" + envelope.getDestinationAddress()
                                   + " [" + messageObject + "] UOID="
                                   + envelope.getUOID().toString());
            }
        }

        synchronized public Map<String,CacheEntry> getArpCache()
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
            CacheEntry entry =
                new CacheEntry(System.currentTimeMillis() + CACHE_TIME, domain);
            _cache.put(cell, entry);
        }

        synchronized public void resolve(String cell, CellMessage envelope)
            throws JMSException
        {
            /* Send arp request.
             */
            TextMessage request = _session.createTextMessage(cell);
            request.setJMSReplyTo(_replyQueue);
            _arp.send(request);

            /* Register request.
             */
            String id = request.getJMSMessageID();
            Lookup lookup = new Lookup(id, cell, envelope);
            _lookups.put(id, lookup);
            _timer.schedule(lookup, ARP_TIMEOUT);
        }

        /** Called by JMS on ARP reply. */
        public void onMessage(Message message)
        {
            try {
                Lookup lookup = _lookups.remove(message.getJMSCorrelationID());
                if (lookup != null && lookup.cancel()) {
                    CellMessage envelope = lookup.envelope;
                    try {
                        TextMessage textMessage = (TextMessage) message;
                        String domain = textMessage.getText();

                        addToCache(lookup.cell, domain);

                        send(envelope);
                        return;
                    } catch (ClassCastException e) {
                        _log.error("Received unexpected reply to ARP request: " + message);
                    } catch (JMSException e) {
                        _log.error("Error while resolving well known cell: " + e.getMessage());
                    }
                    returnToSender(envelope,
                                   new NoRouteToCellException(envelope.getUOID(),
                                                              envelope.getDestinationPath(),
                                                              "Communication failure. Message could not be delivered."));
                }
            } catch (JMSException e) {
                _log.error("Error while resolving well known cell: "
                           + e.getMessage());
            }
        }

        /** Sends Cells message to JMS queue. */
        synchronized public void send(CellMessage envelope)
            throws JMSException
        {
            CellPath address = envelope.getDestinationAddress();
            String cell = address.getCellName();
            String domain = address.getCellDomainName();

            if (domain.equals("local")) {
                domain = lookup(cell);
            }

            if (domain == null) {
                resolve(cell, envelope);
            } else {
                logSend(envelope);
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

        /** ARP request object and expiration timer. */
        class Lookup extends TimerTask
        {
            final String id;
            final String cell;
            final CellMessage envelope;

            Lookup(String id, String cell, CellMessage envelope)
            {
                this.id = id;
                this.cell = cell;
                this.envelope = envelope;
            }

            public void run()
            {
                _lookups.remove(id);
                returnToSender(envelope,
                               new NoRouteToCellException(envelope.getUOID(),
                                                          envelope.getDestinationPath(),
                                                          "Failed to resolve well known cell"));
            }
        }
    }

    /** ARP cache entry. */
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

        synchronized private void logReceive(CellMessage envelope)
        {
            if (_logMessages.isDebugEnabled()) {
                String messageObject =
                    envelope.getMessageObject() == null
                    ? "NULL"
                    : envelope.getMessageObject().getClass().getName();

                _logMessages.debug("tunnelSendMessage src="
                                   + envelope.getSourceAddress()
                                   + " dest=" + envelope.getDestinationAddress()
                                   + " [" + messageObject + "] UOID="
                                   + envelope.getUOID().toString());
            }
        }

        synchronized public Collection<String> getConsumers()
        {
            return new ArrayList(_sessions.keySet());
        }

        /**
         * Handles messages from JMS. These are forwarded to cells.
         */
        synchronized public void onMessage(Message message)
        {
            try {
                ObjectMessage objectMessage = (ObjectMessage) message;
                Object object = objectMessage.getObject();
                CellMessage envelope = (CellMessage) object;
                try {
                    logReceive(envelope);
                    sendMessage(envelope);
                    _counter++;
                } catch (NoRouteToCellException e) {
                    returnToSender(envelope, e);
                }
            } catch (ClassCastException e) {
                _log.warn("Dropping unknown message: " + message);
            } catch (JMSException e) {
                _log.error("Failed to retrieve object from JMS message: " + e);
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
     * Implements the ARP server. The ARP server responds to ARP
     * lookups on locally exported cells.
     */
    static class ArpServer
        implements MessageListener,
                   CellEventListener
    {
        protected final Session _session;
        protected final MessageConsumer _consumer;
        protected final MessageProducer _producer;

        /** Name of local domain. Lookups resolve to this domain. */
        protected final String _domain;

        /** Known well known cells. */
        protected final Set<String> _names = new HashSet();

        public ArpServer(CellNucleus nucleus, Connection connection)
            throws JMSException
        {
            _domain = nucleus.getCellDomainName();
            _session =
                connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            _producer = _session.createProducer(null);
            _consumer =
                _session.createConsumer(_session.createTopic(ARP_TOPIC));
            _consumer.setMessageListener(this);
            nucleus.addCellEventListener(this);
        }

        /**
         * Handles messages from JMS.
         */
        synchronized public void onMessage(Message message)
        {
            try {
                TextMessage textMessage = (TextMessage) message;
                String name = textMessage.getText();
                if (_names.contains(name)) {
                    TextMessage reply = _session.createTextMessage(_domain);
                    reply.setText(_domain);
                    reply.setJMSCorrelationID(textMessage.getJMSMessageID());
                    _producer.send(textMessage.getJMSReplyTo(), reply);
                }
            } catch (ClassCastException e) {
                _log.warn("Dropping unknown message: " + message);
            } catch (JMSException e) {
                _log.error("JMS failure in cell name resolver: " + e);
            }
        }

        synchronized public void routeAdded(CellEvent ce)
        {
            CellRoute cr = (CellRoute) ce.getSource();
            if (cr.getRouteType() == CellRoute.WELLKNOWN) {
                _names.add(cr.getCellName());
            }
        }

        synchronized public void routeDeleted(CellEvent ce)
        {
            CellRoute cr = (CellRoute) ce.getSource();
            if (cr.getRouteType() == CellRoute.WELLKNOWN) {
                _names.remove(cr.getCellName());
            }
        }

        synchronized public void cellDied(CellEvent ce)
        {
            String name = (String) ce.getSource();
            _names.remove(name);
        }

        synchronized public void cellExported(CellEvent ce)
        {
            String name = (String) ce.getSource();
            _names.add(name);
        }

        public void cellCreated(CellEvent ce) {}
    }
}