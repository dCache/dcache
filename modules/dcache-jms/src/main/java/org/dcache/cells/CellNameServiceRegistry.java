package org.dcache.cells;

import com.google.common.base.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.StreamMessage;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import dmg.cells.nucleus.CellInfo;
import dmg.cells.nucleus.CellInfoProvider;

/**
 * MessageListener which processes cell name registrations and
 * provides a registry of domains and well known cells.
 */
public class CellNameServiceRegistry
    implements MessageListener,
               CellInfoProvider
{
    private final static Logger _log =
        LoggerFactory.getLogger(CellNameServiceRegistry.class);

    private final Map<String,String> _cells =
        new ConcurrentHashMap<>();

    private final Map<String,Set<String>> _domains =
        new ConcurrentHashMap<>();

    private final Map<String,Long> _timeouts =
        new ConcurrentHashMap<>();

    private synchronized boolean isValid(long now, String domainName)
    {
        Long timeout = _timeouts.get(domainName);
        if (timeout != null && timeout > now) {
            return true;
        } else {
            unregister(domainName);
            return false;
        }
    }

    public Collection<String> getDomains()
    {
        long now = System.currentTimeMillis();
        Collection<String> domains = new ArrayList<>();
        for (String domainName: _domains.keySet()) {
            if (isValid(now, domainName)) {
                domains.add(domainName);
            }
        }
        return domains;
    }

    public String getDomain(String cell)
    {
        String domain = _cells.get(cell);
        if (domain != null && isValid(System.currentTimeMillis(), domain)) {
            return domain;
        }
        return null;
    }

    private synchronized void unregister(String domainName)
    {
        _timeouts.remove(domainName);

        Set<String> oldCells = _domains.remove(domainName);
        if (oldCells != null) {
            for (String cell: oldCells) {
                if (Objects.equal(domainName, _cells.get(cell))) {
                    _cells.remove(cell);
                }
            }
        }
    }

    private synchronized void register(String domainName,
                                       Set<String> cells,
                                       long timeout)
    {
        unregister(domainName);

        /* Add new entries */
        for (String cellName: cells) {
            _cells.put(cellName, domainName);
        }
        _domains.put(domainName, cells);

        /* Schedule new timeout */
        _timeouts.put(domainName, System.currentTimeMillis() + timeout);
    }

    /* Registration messages follow the format
     *
     *   StreamMessage {
     *      domainName: String;
     *      timeout: long;
     *      length: int;
     *      cellNames: String[length];
     *   }
     */
    @Override
    public void onMessage(Message message)
    {
        try {
            StreamMessage streamMessage = (StreamMessage) message;
            String domainName = streamMessage.readString();

            long timeout = streamMessage.readLong();
            if (timeout == 0) {
                unregister(domainName);
            } else {
                int length = streamMessage.readInt();
                Set<String> cells = new HashSet<>();
                for (int i = 0; i < length; i++) {
                    cells.add(streamMessage.readString());
                }
                register(domainName, cells, timeout);
            }
        } catch (ClassCastException e) {
            _log.warn("Dropping unknown message: {}", message);
        } catch (JMSException e) {
            _log.error("Failed to register well-known cells: {}", e.getMessage());
        }
    }

    /**
     * Provides information in clear text by appending it to the
     * PrintWriter.
     */
    @Override
    public void getInfo(PrintWriter pw)
    {
        for (String domain: getDomains()) {
            pw.append(domain);
            Set<String> cells = _domains.get(domain);
            if (cells != null) {
                pw.append(": ");
                for (String cell: cells) {
                    pw.append(cell).append(" ");
                }
            }
            pw.println();
        }
    }

    /**
     * Provides information in binary form by updating or replacing
     * the CellInfo object. The method may return the same or a new
     * CellInfo object. It may choose to return a subclass of
     * CellInfo. Care must be taken that existing information is not
     * discarded in the process.
     */
    @Override
    public CellInfo getCellInfo(CellInfo info)
    {
        return info;
    }
}
