package org.dcache.cells;

import java.io.Serializable;
import java.util.Map;

import org.dcache.commons.stats.RequestCounterImpl;
import org.dcache.commons.stats.RequestExecutionTimeGauge;
import org.dcache.commons.stats.RequestCounters;
import org.dcache.commons.stats.RequestExecutionTimeGauges;

import diskCacheV111.vehicles.Message;

import dmg.util.Args;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.NoRouteToCellException;
import dmg.cells.nucleus.SerializationException;
import dmg.cells.nucleus.CellEndpoint;
import dmg.cells.nucleus.CellInfo;
import dmg.cells.nucleus.CellMessageAnswerable;

public class MessageProcessingMonitor
    implements CellCommandListener, CellMessageSender
{
    /**
     * Request counters used to count message processing.
     */
    private final RequestCounters<Class<? extends Serializable>> _counters;

    /**
     * Request gauges used to measure message processing.
     */
    private final RequestExecutionTimeGauges<Class<? extends Serializable>> _gauges;

    private CellEndpoint _endpoint;

    /**
     * If true then message processing will be monitored and
     * administrative commands to query the monitoring results are
     * activated.
     */
    private boolean _enabled;

    public MessageProcessingMonitor()
    {
        _counters = new RequestCounters<>("Messages");
        _gauges = new RequestExecutionTimeGauges<>("Messages");
        _enabled = false;
    }

    @Override
    public void setCellEndpoint(CellEndpoint endpoint)
    {
        _endpoint = endpoint;
    }

    public void setEnabled(boolean enabled)
    {
        _enabled = enabled;
    }

    public boolean isEnabled()
    {
        return _enabled;
    }

    public CellEndpoint getReplyCellEndpoint(CellMessage envelope)
    {
        if (_enabled) {
            Class<? extends Serializable> type = envelope.getMessageObject().getClass();
            return new MonitoringReplyCellEndpoint(type);
        } else {
            return _endpoint;
        }
    }

    public final static String hh_monitoring_enable =
        "# Enables monitoring of message processing";
    public String ac_monitoring_enable(Args args)
    {
        _enabled = true;
        return "";
    }

    public final static String hh_monitoring_disable =
        "# Disables monitoring of message processing";
    public String ac_monitoring_disable(Args args)
    {
        _enabled = false;
        return "";
    }

    public final static String hh_monitoring_info =
        "# Provides information about message processing";
    public String ac_monitoring_info(Args args)
    {
        return _counters.toString() + "\n\n" + _gauges.toString();
    }

    public class MonitoringReplyCellEndpoint implements CellEndpoint
    {
        private final Class<? extends Serializable> _type;
        private final long _startTime;

        public MonitoringReplyCellEndpoint(Class<? extends Serializable> type)
        {
            _startTime = System.currentTimeMillis();
            _type = type;
            _counters.incrementRequests(_type);
        }

        @Override
        public void sendMessage(CellMessage envelope)
            throws SerializationException,
                   NoRouteToCellException
        {
            boolean success = false;
            try {
                _endpoint.sendMessage(envelope);
                success = true;
            } finally {
                _gauges.update(_type, System.currentTimeMillis() - _startTime);
                Object o = envelope.getMessageObject();
                if (!success || o instanceof Exception ||
                    (o instanceof Message) && ((Message) o).getReturnCode() != 0) {
                    _counters.incrementFailed(_type);
                }
            }
        }

        @Override
        public void sendMessage(CellMessage envelope,
                                CellMessageAnswerable callback,
                                long timeout)
        {
            throw new UnsupportedOperationException("Cannot use callback for reply");
        }

        @Override
        public CellMessage sendAndWait(CellMessage envelope, long timeout)
        {
            throw new UnsupportedOperationException("Cannot use blocking send for reply");
        }

        @Override
        public CellMessage sendAndWaitToPermanent(CellMessage envelope, long timeout)
        {
            throw new UnsupportedOperationException("Cannot use blocking send for reply");
        }

        @Override
        public CellInfo getCellInfo()
        {
            return _endpoint.getCellInfo();
        }

        @Override
        public Map<String,Object> getDomainContext()
        {
            return _endpoint.getDomainContext();
        }

        @Override
        public Args getArgs()
        {
            return _endpoint.getArgs();
        }
    }
}
