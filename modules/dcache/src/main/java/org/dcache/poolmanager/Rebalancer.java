package org.dcache.poolmanager;

import com.google.common.base.Joiner;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Locale;
import java.util.NoSuchElementException;
import java.util.concurrent.Callable;

import diskCacheV111.poolManager.CostModule;
import diskCacheV111.poolManager.PoolSelectionUnit;
import diskCacheV111.poolManager.PoolSelectionUnit.SelectionPool;
import diskCacheV111.pools.PoolCostInfo;
import diskCacheV111.pools.PoolCostInfo.PoolSpaceInfo;
import diskCacheV111.util.CacheException;

import dmg.cells.nucleus.CellCommandListener;
import dmg.cells.nucleus.CellPath;

import dmg.util.command.Argument;
import dmg.util.command.Command;
import dmg.util.command.Option;

import org.dcache.cells.CellStub;
import org.dcache.pool.migration.PoolMigrationJobCancelMessage;

/**
 * Implements commands to generate migration jobs to rebalance pools.
 */
public class Rebalancer
    implements CellCommandListener
{
    private static final String JOB_NAME = "rebalance";

    private static final String METRIC_RELATIVE = "relative";
    private static final String METRIC_FREE_COST = "free";

    private PoolSelectionUnit _psu;
    private CostModule _cm;
    private CellStub _poolStub;

    public void setPoolSelectionUnit(PoolSelectionUnit psu)
    {
        _psu = psu;
    }

    public void setCostModule(CostModule cm)
    {
        _cm = cm;
    }

    public void setPoolStub(CellStub poolStub)
    {
        _poolStub = poolStub;
    }

    private void cancelAll(Collection<SelectionPool> pools)
        throws CacheException, InterruptedException
    {
        // TODO: Use SpreadAndWait
        for (SelectionPool pool: pools) {
            _poolStub.sendAndWait(new CellPath(pool.getName()),
                                  new PoolMigrationJobCancelMessage(JOB_NAME, true));
        }
    }

    @Command(name = "rebalance pgroup",
            hint = "rearrange files to balance space usage",
            description = "A migration job will be submitted to each pool in the pool group. " +
                    "The combined effect of these migration jobs is to move files until " +
                    "either the relative space usage (used space relative to the total " +
                    "size of the pool) is the same or the space cost is the same; which " +
                    "depends on the metric used. The default is balance relative space " +
                    "usage.\n\n" +
                    "A pool can only be the source of one rebalance run at a time. " +
                    "Previous rebalancing jobs will be cancelled. The PoolManager " +
                    "maintains no state for the rebalancing job, however migration jobs " +
                    "created by the rebalancer have a well known name (rebalance).\n\n" +
                    "Migration jobs periodically query PoolManager about how much space " +
                    "is used on each pool and about the space cost. There will thus be " +
                    "a delay between files being moved between pools and the metric " +
                    "being updated. It is expected that rebalancing jobs will overshoot " +
                    "the target slightly. For very small pools on test instances this " +
                    "effect will be more profound than on large pools. The effect can " +
                    "be reduced by specifying a shorter refresh period via the refresh " +
                    "option, which accepts an integer number of seconds. The default " +
                    "period is 30 seconds.\n\n" +
                    "The migration jobs created by the rebalancer will not survive a " +
                    "pool restart. If the lots of files are written, deleted or moved " +
                    "while the rebalancing job runs, then the pool group may not be " +
                    "completely balanced when the jobs terminate. Run the rebalancer " +
                    "a second time to improve the balance further.\n\n" +
                    "If 'relative' metric is used then files are moved around so that " +
                    "pools in the poolgroup have about the same fractional usage (e.g., " +
                    "each pool is 30% full). If pools have different capacities then " +
                    "bigger pools will have more free space.\n\n" +
                    "If metric is 'free' then files are moved around so that pools in " +
                    "the poolgroup have about the same amount of free space. If pools " +
                    "have different capacities then bigger pools will store a larger " +
                    "fraction of the files.\n\n" +
                    "By default the 'relative' metric is used.  If all pools in the " +
                    "poolgroup have identical capacities then the metric used does not " +
                    "matter.")
    public class RebalancePgroupCommand implements Callable<String>
    {
        @Argument(usage = "The name of the pool group to balance.")
        String poolGroup;

        @Option(name = "metric", values = {"relative","free"})
        String metric = METRIC_RELATIVE;

        @Option(name="refresh", metaVar="seconds")
        int period = 30;

        @Override
        public String call() throws CacheException, InterruptedException,
                NoSuchElementException, IllegalArgumentException
        {
            long used = 0;
            long total = 0;
            Collection<SelectionPool> pools = new ArrayList<>();
            Collection<String> names = new ArrayList<>();
            for (SelectionPool pool: _psu.getPoolsByPoolGroup(poolGroup)) {
                PoolCostInfo cost = _cm.getPoolCostInfo(pool.getName());
                if (pool.getPoolMode().isEnabled() && cost != null) {
                    PoolSpaceInfo spaceInfo = cost.getSpaceInfo();
                    used += spaceInfo.getUsedSpace();
                    total += spaceInfo.getTotalSpace();
                    pools.add(pool);
                    names.add(pool.getName());
                }
            }

            String command;
            switch (metric) {
                case METRIC_RELATIVE:
                    double factor = (double) used / (double) total;
                    command =
                            String.format(Locale.US, "migration move -id=%s -include-when='target.used " +
                                            "< %2$f * target.total' -stop-when='targets == 0 or source." +
                                            "used <= %2$f * source.total' -refresh=%3$d %4$s",
                                    JOB_NAME, factor, period, Joiner.on(" ").join(names));
                    break;
                case METRIC_FREE_COST:
                    command =
                            String.format(Locale.US, "migration move -id=%s -include-when='target.free > " +
                                            "source.free' -stop-when='targets == 0' -refresh=%d %s",
                                    JOB_NAME, period, Joiner.on(" ").join(names));
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported value for -metric: " + metric);
            }

            cancelAll(pools);

            boolean success = false;
            try {
                // TODO: Use SpreadAndWait
                for (SelectionPool pool: pools) {
                    _poolStub.sendAndWait(new CellPath(pool.getName()), command,
                            String.class);
                }
                success = true;
            } finally {
                if (!success) {
                    cancelAll(pools);
                }
            }

            return "Rebalancing jobs have been submitted to " +
                    Joiner.on(", ").join(names) + ".";
        }
    }

    @Command(name = "rebalance cancel pgroup",
            hint = "cancel rebalancing operation",
            description = "Cancels migration jobs created by the rebalancer.")
    public class rebalance_cancel_pgroupCommand implements Callable<String>
    {
        @Argument(usage = "The name of the pool group.")
        String poolGroup;

        @Override
        public String call() throws CacheException, InterruptedException,
                NoSuchElementException
        {
            cancelAll(_psu.getPoolsByPoolGroup(poolGroup));
            return "";
        }
    }
}
