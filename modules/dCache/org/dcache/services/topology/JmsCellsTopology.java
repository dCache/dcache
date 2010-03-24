package org.dcache.services.topology;

import dmg.cells.network.CellDomainNode;
import dmg.util.Args;

import org.dcache.cells.CellCommandListener;
import org.dcache.cells.JMSTunnel;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;

import javax.jms.Message;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.ConnectionFactory;
import javax.jms.Connection;
import javax.jms.Session;
import javax.jms.Destination;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Message;
import javax.jms.TextMessage;
import javax.jms.MessageListener;
import javax.jms.DeliveryMode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * CellsTopology for JMS based dCache installations.
 *
 * The algorithm used is to broadcast a wildcard query no the
 * cells.arp topic and listen for replies. For each domain that
 * replies we query the System cell of that domain for classic cells
 * tunnels.
 */
public class JmsCellsTopology
    extends AbstractCellsTopology
    implements CellsTopology,
               CellCommandListener,
               MessageListener
{
    private static final Logger _log =
        LoggerFactory.getLogger(JmsCellsTopology.class);

    private Destination _arpReplyQueue;
    private Executor _executor;
    private ConnectionFactory _connectionFactory;
    private Connection _connection;
    private Session _sendSession;
    private Session _receiveSession;
    private MessageProducer _arpProducer;
    private MessageConsumer _arpConsumer;

    private volatile Map<String,CellDomainNode> _currentMap =
        new ConcurrentHashMap<String,CellDomainNode>();
    private volatile Map<String,CellDomainNode> _nextMap =
        new ConcurrentHashMap<String,CellDomainNode>();

    public void setConnectionFactory(ConnectionFactory factory)
    {
        _connectionFactory = factory;
    }

    public void setExecutor(Executor executor)
    {
        _executor = executor;
    }

    public void start()
        throws JMSException
    {
        _connection = _connectionFactory.createConnection();

        _sendSession =
            _connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        _arpProducer =
            _sendSession.createProducer(_sendSession.createTopic(JMSTunnel.ARP_TOPIC));
        _arpProducer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
        _arpProducer.setDisableMessageTimestamp(true);

        _receiveSession =
            _connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        _arpReplyQueue = _receiveSession.createTemporaryQueue();
        _arpConsumer = _receiveSession.createConsumer(_arpReplyQueue);
        _arpConsumer.setMessageListener(this);

        _connection.start();
    }

    public void stop()
        throws JMSException
    {
        _connection.close();
    }

    private void sendArpRequest()
        throws JMSException
    {
        TextMessage request =
            _sendSession.createTextMessage(JMSTunnel.WILDCARD_QUERY);
        request.setJMSReplyTo(_arpReplyQueue);
        _arpProducer.send(request);
    }

    /** Called by JMS on ARP reply. */
    public void onMessage(Message message)
    {
        try {
            if (message instanceof TextMessage) {
                TextMessage textMessage = (TextMessage) message;
                final String domain = textMessage.getText();
                _executor.execute(new Runnable() {
                        public void run() {
                            try {
                                Map<String,CellDomainNode> map =
                                    buildTopologyMap(domain);
                                _currentMap.putAll(map);
                                _nextMap.putAll(map);
                            } catch (InterruptedException e) {
                                _log.info("Topology construction was interrupted");
                            }
                        }
                    });
            }
        } catch (JMSException e) {
            _log.error("Error while processing ARP reply: " + e.getMessage());
        }
    }

    /**
     * Triggers an update of the topology map.
     *
     * At any given time we maintain two maps. The current map is the
     * one we provide to the outside world. The next map is the one we
     * are currently populating from scratch. Calling the update
     * method will make the next map the current map and trigger a new
     * update.
     *
     * New cells are added to both maps, however using a next map
     * allows us to expire old entries.
     */
    public synchronized void update()
        throws JMSException
    {
        _currentMap = _nextMap;
        _nextMap = new ConcurrentHashMap<String,CellDomainNode>();
        sendArpRequest();
    }

    @Override
    public CellDomainNode[] getInfoMap()
    {
        return _currentMap.values().toArray(new CellDomainNode[0]);
    }

    public final String hh_update = "# initiates background update";
    public String ac_update(Args args)
        throws JMSException
    {
        sendArpRequest();
        return "Background update started";
    }
}
