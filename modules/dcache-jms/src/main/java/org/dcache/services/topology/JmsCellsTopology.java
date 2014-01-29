package org.dcache.services.topology;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.Session;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;

import dmg.cells.network.CellDomainNode;
import dmg.cells.nucleus.CellCommandListener;

import org.dcache.cells.CellNameService;
import org.dcache.cells.CellNameServiceRegistry;
import org.dcache.util.Args;

/**
 * CellsTopology for JMS based dCache installations.
 *
 * Subscribes to cell name service updates. For each domain it queries
 * the System cell of that domain for classic cells tunnels.
 */
public class JmsCellsTopology
    extends AbstractCellsTopology
    implements CellsTopology,
               CellCommandListener,
               ExceptionListener
{
    private static final Logger _log =
        LoggerFactory.getLogger(JmsCellsTopology.class);

    private Executor _executor;
    private ConnectionFactory _connectionFactory;
    private Connection _connection;
    private Session _sendSession;
    private Session _receiveSession;
    private MessageConsumer _cnsConsumer;

    private CellNameServiceRegistry _registry;

    private volatile Map<String,CellDomainNode> _currentMap =
        new ConcurrentHashMap<>();
    private volatile Map<String,CellDomainNode> _nextMap =
        new ConcurrentHashMap<>();

    public void setConnectionFactory(ConnectionFactory factory)
    {
        _connectionFactory = factory;
    }

    public void setExecutor(Executor executor)
    {
        _executor = executor;
    }

    public void setCellNameServiceRegistry(CellNameServiceRegistry registry)
    {
        _registry = registry;
    }

    public void start()
        throws JMSException
    {
        _connection = _connectionFactory.createConnection();
        _connection.setExceptionListener(this);

        _sendSession =
            _connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        _receiveSession =
            _connection.createSession(false, Session.DUPS_OK_ACKNOWLEDGE);

        _cnsConsumer =
            _receiveSession.createConsumer(_receiveSession.createTopic(CellNameService.DESTINATION_REGISTRATION));
        _cnsConsumer.setMessageListener(_registry);

        try {
            CellNameService.requestUpdate(_sendSession);
        } catch (JMSException e) {
            _log.debug("Failed to request CNS update: {}", e.getMessage());
        }

        _connection.start();
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
        new Thread("topo-recover") {
            @Override
            public void run()
            {
                while (true) {
                    try {
                        JmsCellsTopology.this.stop();
                    } catch (JMSException e) {
                        _log.error("Failed to shut down JMS connection: {}",
                                   e.getMessage());
                    }
                    try {
                        JmsCellsTopology.this.start();
                        return;
                    } catch (JMSException e) {
                        _log.error("Failed to shut down JMS connection: {}",
                                   e.getMessage());
                    }
                }
            }
        }.start();
    }

    private void addDomain(final String domain)
    {
        _executor.execute(new Runnable() {
                @Override
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
    {
        _currentMap = _nextMap;
        _nextMap = new ConcurrentHashMap<>();
        for (String domain: _registry.getDomains()) {
            addDomain(domain);
        }
    }

    @Override
    public CellDomainNode[] getInfoMap()
    {
        return _currentMap.values().toArray(
                new CellDomainNode[_currentMap.size()]);
    }

    public static final String hh_update = "# initiates background update";
    public String ac_update(Args args)
        throws JMSException
    {
        CellNameService.requestUpdate(_sendSession);
        for (String domain: _registry.getDomains()) {
            addDomain(domain);
        }
        return "Background update started";
    }
}
