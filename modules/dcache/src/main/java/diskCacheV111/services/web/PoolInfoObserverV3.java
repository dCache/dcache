package diskCacheV111.services.web;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.PrintWriter;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

import diskCacheV111.poolManager.PoolManagerCellInfo;
import diskCacheV111.pools.PoolCellInfo;
import diskCacheV111.util.CacheException;

import dmg.cells.nucleus.CellAddressCore;
import dmg.cells.nucleus.CellPath;

import org.dcache.cells.AbstractCell;
import org.dcache.cells.AbstractMessageCallback;
import org.dcache.cells.CellStub;
import org.dcache.cells.Option;
import org.dcache.util.Args;

public class PoolInfoObserverV3 extends AbstractCell
{
    private final static Logger _log =
        LoggerFactory.getLogger(PoolInfoObserverV3.class);

    private final static String THREAD_NAME = "pool-info-observer";
    private final static String POOL_MANAGER = "PoolManager";
    private final static String TOPOLOGY_NAME = "PoolManager";
    private final static String TOPOLOGY_CONTEXT_KEY = "poolgroup-map.ser";

    @Option(
        name = "refresh-time",
        unit = "seconds",
        defaultValue = "60"
    )
    protected long _interval;

    private Thread _refreshThread;

    private CellStub _poolManager;
    private CellStub _pool;

    public PoolInfoObserverV3(String name, String args)
        throws InterruptedException, ExecutionException
    {
        super(name, PoolInfoObserverV3.class.getName(), new Args(args));
        doInit();
    }

    @Override
    protected void init()
    {
        _poolManager = new CellStub(this, new CellPath(POOL_MANAGER), 30000);
        _pool = new CellStub(this, null, 60000);

        _refreshThread = new Thread(THREAD_NAME) {
                @Override
                public void run() {
                    try {
                        while (!Thread.interrupted()) {
                            try {
                                refresh();
                            } catch (CacheException e) {
                                _log.error("Failed to update topology map: " + e.getMessage());
                            } catch (RuntimeException e) {
                                _log.error("Failed to update topology map: " + e);
                            }

                            Thread.sleep(_interval * 1000);
                        }
                    } catch(InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
            };
        _refreshThread.start();
    }

    private void refresh()
        throws CacheException, InterruptedException
    {
        CellInfoContainer container = collectPoolGroups();
        PoolCellQueryContainer topology = collectPoolInfo(container);
        getNucleus().setDomainContext(TOPOLOGY_CONTEXT_KEY, topology);
    }

    private CellInfoContainer collectPoolGroups()
        throws CacheException, InterruptedException
    {
        CellInfoContainer container = new CellInfoContainer();
        Object[] poolGroups =
            _poolManager.sendAndWait("psux ls pgroup", Object[].class);
        for (Object o: poolGroups) {
            if (o == null) {
                continue;
            }
            String name = o.toString();
            Object[] props =
                _poolManager.sendAndWait("psux ls pgroup " + name, Object[].class);
            if ((props.length < 3) ||
                (! (props[0] instanceof String)) ||
                (! (props[1] instanceof Object []))) {
                _log.error("Unexpected reply from PoolManager: {}", props);
                continue;
            }
            for (Object p: (Object[]) props[1]) {
                container.addPool(TOPOLOGY_NAME, name, p.toString());
            }
        }
        return container;
    }

    private PoolCellQueryContainer
        collectPoolInfo(final CellInfoContainer container)
        throws CacheException, InterruptedException
    {
        PoolManagerCellInfo poolManagerInfo =
            _poolManager.sendAndWait("xgetcellinfo", PoolManagerCellInfo.class);
        Set<CellAddressCore> pools = poolManagerInfo.getPoolCells();
        final PoolCellQueryContainer result = new PoolCellQueryContainer();
        final CountDownLatch latch = new CountDownLatch(pools.size());
        for (final CellAddressCore pool: pools) {
            final long start = System.currentTimeMillis();
            Futures.addCallback(_pool.send(new CellPath(pool), "xgetcellinfo", PoolCellInfo.class),
                                new FutureCallback<PoolCellInfo>()
                                {
                                    @Override
                                    public void onSuccess(PoolCellInfo info)
                                    {
                                        long now = System.currentTimeMillis();
                                        long ping = now - start;
                                        result.put(info.getCellName(),
                                                   new PoolCellQueryInfo(info, ping, now));
                                        container.addInfo(info.getCellName(), info);
                                        latch.countDown();
                                    }

                                    @Override
                                    public void onFailure(Throwable t)
                                    {
                                        _log.warn("Failed to query {}: {}", pool, t.getMessage());
                                        latch.countDown();
                                    }
                                });
        }

        latch.await();

        Map<String,Map<String,Map<String,Object>>> allClasses =
            container.createExternalTopologyMap();
        for (Map<String,Map<String,Object>> groupMap: allClasses.values()) {
            for (Map<String,Object> tableMap: groupMap.values()) {
                for (String poolName: tableMap.keySet()) {
                    tableMap.put(poolName, result.getInfoByName(poolName));
                }
            }
        }

        result.setTopology(allClasses);

        return result;
    }

    public String ac_show_topology(Args args)
    {
        Object o = getNucleus().getDomainContext(TOPOLOGY_CONTEXT_KEY);
        return (o == null) ? "" : o.toString();
    }

    @Override
    public void cleanUp()
    {
        _refreshThread.interrupt();
    }

    @Override
    public void getInfo(PrintWriter pw)
    {
        pw.println("Update interval: " + _interval + " [sec]");
    }
}
