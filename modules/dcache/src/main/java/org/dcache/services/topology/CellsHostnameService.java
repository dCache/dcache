package org.dcache.services.topology;

import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

import diskCacheV111.util.SpreadAndWait;

import dmg.cells.network.CellDomainNode;
import dmg.cells.nucleus.CellPath;

import org.dcache.cells.CellStub;

/**
 *
 * @author jans
 */
public class CellsHostnameService implements HostnameService
{
    private static final Logger LOGGER =
            LoggerFactory.getLogger(CellsHostnameService.class);

    private CellsTopology _topology;
    private Set<String> _hostnames = new HashSet<>();
    private CellStub _stub;

    @Override
    public synchronized String getHostnames() {
        return _hostnames.toString();
    }

    @Override
    public void updateHostnames() {
        CellDomainNode[] info = _topology.getInfoMap();
        if (info == null) {
            LOGGER.info("Cannot update host names. Domains not known yet. Try to run update first.");
            return;
        }
        LOGGER.debug("Host name update started");
        SpreadAndWait<String> spreader = new SpreadAndWait<>(_stub);
        for (CellDomainNode domain : info) {
            spreader.send(new CellPath(domain.getAddress()), String.class, "get hostname");
        }
        try {
            spreader.waitForReplies();
            setHostnames(spreader.getReplies().values());
            LOGGER.debug("Host name update finished");
        } catch (InterruptedException ignored) {
        }
    }

    private synchronized void setHostnames(Iterable<String> hostnames) {
        _hostnames = Sets.newHashSet(hostnames);
    }

    public void setTopology(CellsTopology topology) {
        _topology = topology;
    }

    public void setCellStub(CellStub stub) {
        _stub = stub;
    }
}
