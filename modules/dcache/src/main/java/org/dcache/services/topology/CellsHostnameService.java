package org.dcache.services.topology;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import diskCacheV111.util.SpreadAndWait;

import dmg.cells.network.CellDomainNode;
import dmg.cells.nucleus.CellEndpoint;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellPath;

import org.dcache.cells.CellMessageSender;

/**
 *
 * @author jans
 */
public class CellsHostnameService implements HostnameService, CellMessageSender {

    private long _timeout;
    private CellEndpoint _endpoint;
    private CellsTopology _topology;
    private Set<String> _hostnames = new HashSet<>();
    private static final Logger _log =
            LoggerFactory.getLogger(CellsHostnameService.class);

    @Override
    public synchronized String getHostnames() {
        return _hostnames.toString();
    }

    @Override
    public void updateHostnames() {
        CellDomainNode[] info = _topology.getInfoMap();
        if (info == null) {
            _log.info("Cannot update Hostnames. Domains not known yet." +
                    " Try running update first");
            return;
        }
        _log.info("Hostnames update started");
        SpreadAndWait spreader = new SpreadAndWait(_endpoint, _timeout);
        for (CellDomainNode domain : info) {
            spreader.send(createMessage(domain.getAddress()));
        }
        try {
            spreader.waitForReplies();
            buildHostnameList(spreader.getReplyList());
            _log.info("Hostnames update finished");
        } catch (InterruptedException ex) {
        }
    }

    private void buildHostnameList(List<CellMessage> replyList) {
        Set<String> hostnames = new HashSet<>();
        for (CellMessage msg : replyList) {
            hostnames.add((String) msg.getMessageObject());
        }
        synchronized (this) {
            _hostnames = hostnames;
        }
    }

    private CellMessage createMessage(String cellPath) {
        CellMessage message = new CellMessage(new CellPath(cellPath), "get hostname");
        return message;
    }

    public void setTopology(CellsTopology topology) {
        _topology = topology;
    }

    public void setTimeout(long timeout) {
        _timeout = timeout;
    }

    @Override
    public void setCellEndpoint(CellEndpoint endpoint) {
        _endpoint = endpoint;
    }
}
