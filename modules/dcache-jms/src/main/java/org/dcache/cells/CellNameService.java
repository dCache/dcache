package org.dcache.cells;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

/**
 * A central lookup service for well known cell names.
 *
 * Well known cell names are resolved to domain names by sending a
 * text message containing the cell name on the cells.cns.lookup
 * queue. The reply is a text message containing the domain name.
 *
 * Domains register all well known cell names by sending a
 * registration message to the cells.cns.register topic.
 */
public class CellNameService
    extends AbstractCellComponent
    implements ExceptionListener
{
    private final static Logger _log =
        LoggerFactory.getLogger(CellNameService.class);

    public final static String DESTINATION_REGISTRATION =
        "cells.cns.registration";
    public final static String DESTINATION_LOOKUP =
        "cells.cns.lookup";
    public final static String DESTINATION_REFRESH =
        "cells.cns.refresh";

    private ConnectionFactory _connectionFactory;
    private Connection _connection;
    private Session _session;
    private MessageProducer _producer;

    private CellNameServiceRegistry _registry;

    public void setConnectionFactory(ConnectionFactory factory)
    {
        _connectionFactory = factory;
    }

    public void setCellNameServiceRegistry(CellNameServiceRegistry registry)
    {
        _registry = registry;
    }

    public void start()
        throws JMSException
    {
        _connection = _connectionFactory.createConnection();
        try {
            _connection.setExceptionListener(this);

            /* CellNameService is single threaded: it is purely
             * reactive and doesn't do anything except when it
             * receives a message. We can thus use the same session
             * for both consumer and producers as all outgoing
             * messages are sent from within message listeners.
             */
            _session =
                _connection.createSession(false, Session.DUPS_OK_ACKNOWLEDGE);

            /* Used to send replies to lookup requests.
             */
            _producer = _session.createProducer(null);
            _producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
            _producer.setDisableMessageID(true);
            _producer.setDisableMessageTimestamp(true);
            _producer.setTimeToLive(2 * JMSTunnel.CNS_TIMEOUT);

            MessageConsumer consumer;

            /* Processes CNS registration requests.
             */
            consumer =
                _session.createConsumer(_session.createTopic(DESTINATION_REGISTRATION));
            consumer.setMessageListener(_registry);

            /* Processes CNS lookups.
             */
            consumer =
                _session.createConsumer(_session.createQueue(DESTINATION_LOOKUP));
            consumer.setMessageListener(new LookupHandler());

            /* Send a refresh requests to all domains.
             */
            try {
                requestUpdate(_session);
            } catch (JMSException e) {
                _log.trace("Failed to request CNS update: {}", e.getMessage());
            }
        } finally {
            _connection.start();
        }
    }

    public void stop()
        throws JMSException
    {
        _connection.close();
    }

    /**
     * Called by the JMS provider on fatal errors.
     */
    @Override
    public void onException(JMSException exception)
    {
        _log.error("Fatal JMS connection failure: {}", exception.getMessage());

        /* We will stop the connection and start a new one with new
         * sessions and all. We do this in a separate thread as I'm
         * not certain how the JMS provider would react to doing this
         * in the callback.
         *
         * It is quite likely that other operations in the process of
         * using the old connection will fail miserably.
         */
        new Thread("CNS-recover") {
            @Override
            public void run()
            {
                while (true) {
                    try {
                        CellNameService.this.stop();
                    } catch (JMSException e) {
                        _log.error("Failed to shut down JMS connection: {}",
                                   e.getMessage());
                    }
                    try {
                        CellNameService.this.start();
                        return;
                    } catch (JMSException e) {
                        _log.error("Failed to shut down JMS connection: {}",
                                   e.getMessage());
                    }
                }
            }
        }.start();
    }

    class LookupHandler implements MessageListener
    {
        @Override
        public void onMessage(Message message)
        {
            try {
                TextMessage textMessage = (TextMessage) message;
                String cellName = textMessage.getText();
                String domainName = _registry.getDomain(cellName);

                if (domainName != null) {
                    TextMessage reply = _session.createTextMessage(domainName);
                    reply.setJMSCorrelationID(cellName);
                    _producer.send(textMessage.getJMSReplyTo(), reply);
                } else {
                    TextMessage reply = _session.createTextMessage("");
                    reply.setJMSCorrelationID(cellName);
                    _producer.send(textMessage.getJMSReplyTo(), reply);
                }
            } catch (ClassCastException e) {
                _log.warn("Dropping unknown message: {}", message);
            } catch (JMSException e) {
                _log.error("Failed to lookup well-known cells: {}", e.getMessage());
            }
        }
    }

    /**
     * Sends a refresh requests to all domains.
     */
    public static void requestUpdate(Session session)
        throws JMSException
    {
        MessageProducer producer =
            session.createProducer(session.createTopic(DESTINATION_REFRESH));
        try {
            producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
            producer.setDisableMessageID(true);
            producer.setDisableMessageTimestamp(true);
            producer.setTimeToLive(JMSTunnel.CNS_REGISTRATION_PERIOD);
            producer.send(session.createMessage());
        } finally {
            producer.close();
        }
    }
}
