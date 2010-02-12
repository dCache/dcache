package org.dcache.services.topology;

import dmg.cells.nucleus.CellTunnelInfo;
import dmg.cells.nucleus.CellPath;
import dmg.cells.network.CellDomainNode;
import dmg.util.Args;

import diskCacheV111.util.CacheException;

import org.dcache.cells.CellStub;
import org.dcache.cells.CellCommandListener;
import org.dcache.cells.AbstractCellComponent;

import java.util.Map;
import java.util.HashMap;
import java.util.Queue;
import java.util.ArrayDeque;
import java.util.Set;
import java.util.HashSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClassicCellsTopology
    extends AbstractCellComponent
    implements CellsTopology,
               CellCommandListener
{
    private Logger _log = LoggerFactory.getLogger(ClassicCellsTopology.class);

    private volatile CellDomainNode[] _infoMap;

    private CellStub _stub;

    public void setCellStub(CellStub stub)
    {
        _stub = stub;
    }

    public void update()
        throws InterruptedException
    {
        _infoMap = buildTopologyMap();
    }

    @Override
    public CellDomainNode[] getInfoMap()
    {
        return _infoMap;
    }

    private CellDomainNode[] buildTopologyMap()
        throws InterruptedException
    {
        Queue<CellDomainNode> queue = new ArrayDeque<CellDomainNode>();
        Map<String, CellDomainNode> hash =
            new HashMap<String, CellDomainNode>();

        String domain = getCellDomainName();
        CellDomainNode node = new CellDomainNode(domain, "System@" + domain);
        queue.add(node);
        hash.put(node.getName(), node);

        while ((node = queue.poll()) != null) {
            String name = node.getName();
            String address = node.getAddress();
            try {
                _log.debug("Sending topology info request to " + address);
                CellTunnelInfo[] infos =
                    _stub.sendAndWait(new CellPath(address),
                                      "getcelltunnelinfos",
                                      CellTunnelInfo[].class);
                _log.debug("Got reply from " + address);
                Set<CellTunnelInfo> acceptedTunnels =
                    new HashSet<CellTunnelInfo>();
                for (CellTunnelInfo info: infos) {
                    try {
                        domain =
                            info.getRemoteCellDomainInfo().getCellDomainName();
                        if (!hash.containsKey(domain)) {
                            CellDomainNode n =
                                new CellDomainNode(domain, address+":System@" + domain);
                            queue.add(n);
                            hash.put(domain, n);
                        }
                        acceptedTunnels.add(info);
                    } catch (RuntimeException e) {
                        _log.warn("Exception in domain info [" + info + "]: " + e.getMessage());
                    }
                }

                node.setLinks(acceptedTunnels.toArray(new CellTunnelInfo[0]));
            } catch (CacheException e) {
                _log.warn("Failed to fetch topology info from " + address +
                          ": " + e.getMessage());
            }
        }
        return hash.values().toArray(new CellDomainNode[0]);
    }

    public final String hh_update = "# initiates background update";
    public String ac_update(Args args)
    {
        Thread thread = new Thread() {
                public void run()
                {
                    try {
                        update();
                    } catch (InterruptedException e) {
                    }
                }
            };
        thread.start();
        return "Background update started";
    }
}
