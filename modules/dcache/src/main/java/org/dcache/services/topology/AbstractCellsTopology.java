package org.dcache.services.topology;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import diskCacheV111.util.CacheException;

import dmg.cells.network.CellDomainNode;
import dmg.cells.nucleus.CellInfoProvider;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.CellTunnelInfo;

import dmg.cells.nucleus.AbstractCellComponent;
import org.dcache.cells.CellStub;

/**
 * Base class for CellsTopology implementations. Provides support for
 * building topology maps.
 */
public class AbstractCellsTopology
    extends AbstractCellComponent implements CellInfoProvider
{
    private static final Logger _log =
        LoggerFactory.getLogger(AbstractCellsTopology.class);

    private CellStub _stub;

    public void setCellStub(CellStub stub)
    {
        _stub = stub;
    }

    private CellTunnelInfo[] getCellTunnelInfos(String address)
        throws CacheException, InterruptedException
    {
        List<CellTunnelInfo> tunnels = new ArrayList<>();

        _log.debug("Sending topology info request to " + address);
        CellTunnelInfo[] infos =
            _stub.sendAndWait(new CellPath(address),
                              "getcelltunnelinfos",
                              CellTunnelInfo[].class);
        _log.debug("Got reply from " + address);

        for (CellTunnelInfo info: infos) {
            if (info.getRemoteCellDomainInfo() != null) {
                tunnels.add(info);
            }
        }

        return tunnels.toArray(new CellTunnelInfo[tunnels.size()]);
    }

    private List<CellDomainNode> getConnectedNodes(CellDomainNode node)
    {
        List<CellDomainNode> nodes = new ArrayList<>();

        for (CellTunnelInfo info: node.getLinks()) {
            String address = node.getAddress();
            String domain =
                info.getRemoteCellDomainInfo().getCellDomainName();
            nodes.add(new CellDomainNode(domain, address + ":System@" + domain));
        }

        return nodes;
    }

    protected Map<String,CellDomainNode> buildTopologyMap(String domain)
        throws InterruptedException
    {
        Queue<CellDomainNode> queue = new ArrayDeque<>();
        Map<String, CellDomainNode> map =
            new HashMap<>();

        CellDomainNode node = new CellDomainNode(domain, "System@" + domain);
        queue.add(node);
        map.put(node.getName(), node);

        while ((node = queue.poll()) != null) {
            try {
                node.setLinks(getCellTunnelInfos(node.getAddress()));

                for (CellDomainNode connectedNode: getConnectedNodes(node)) {
                    String name = connectedNode.getName();
                    if (!map.containsKey(name)) {
                        queue.add(connectedNode);
                        map.put(name, connectedNode);
                    }
                }
            } catch (CacheException e) {
                _log.warn("Failed to fetch topology info from {}: {}",
                          node.getAddress(), e.getMessage());
            }
        }
        return map;
    }
}
