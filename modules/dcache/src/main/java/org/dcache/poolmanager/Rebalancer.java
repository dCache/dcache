package org.dcache.poolmanager;

import com.google.common.base.Joiner;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.NoSuchElementException;
import java.util.concurrent.Callable;

import diskCacheV111.poolManager.CostModule;
import diskCacheV111.poolManager.PoolSelectionUnit;
import diskCacheV111.poolManager.PoolSelectionUnit.SelectionPool;
import diskCacheV111.pools.PoolCostInfo;
import diskCacheV111.pools.PoolCostInfo.PoolSpaceInfo;

import dmg.cells.nucleus.CellCommandListener;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.DelayedReply;
import dmg.cells.nucleus.Reply;
import dmg.util.command.Argument;
import dmg.util.command.Command;
import dmg.util.command.Option;

import org.dcache.cells.CellStub;
import org.dcache.pool.migration.PoolMigrationJobCancelMessage;

import static com.google.common.util.concurrent.Futures.*;
import static java.util.stream.Collectors.toList;

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

    private ListenableFuture<List<PoolMigrationJobCancelMessage>> cancelAll(Collection<SelectionPool> pools, String why)
    {
        return allAsList(pools.stream()
                                 .map(pool -> new CellPath(pool.getName()))
                                 .map(path -> CellStub.transform(_poolStub.send(path, new PoolMigrationJobCancelMessage(JOB_NAME, true, why)), m -> m))
                                 .collect(toList()));
    }

    private ListenableFuture<List<String>> sendToAll(Collection<SelectionPool> pools, String command)
    {
        return allAsList(pools.stream()
                                 .map(pool -> new CellPath(pool.getName()))
                                 .map(path -> _poolStub.send(path, command, String.class))
                                 .collect(toList()));
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
    public class RebalancePgroupCommand extends DelayedReply implements Callable<Reply>
    {
        @Argument(usage = "The name of the pool group to balance.")
        String poolGroup;

        @Option(name = "metric", values = {"relative","free"})
        String metric = METRIC_RELATIVE;

        @Option(name="refresh", metaVar="seconds")
        int period = 30;

        @Override
        public Reply call() throws NoSuchElementException, IllegalArgumentException
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

            addCallback(
                    transformAsync(cancelAll(pools, "\"rebalance pgroup\" admin command, cancelling old jobs"),
                            ignored -> startAllPoolsOrFail(pools, command)),
                    new FutureCallback<Object>()
                    {
                        @Override
                        public void onSuccess(Object ignored)
                        {
                            reply("Rebalancing jobs have been submitted to " + Joiner.on(", ").join(names) + ".");
                        }

                        @Override
                        public void onFailure(Throwable t)
                        {
                            reply(t);
                        }
                    }
            );

            return this;
        }

        protected ListenableFuture<Object> startAllPoolsOrFail(Collection<SelectionPool> pools, String command)
        {
            return catchingAsync(sendToAll(pools, command), Exception.class, t -> cancelAllPoolsAndFail(pools, t));
        }

        protected <V> ListenableFuture<V> cancelAllPoolsAndFail(Collection<SelectionPool> pools, Exception t)
        {
            return Futures.transformAsync(cancelAll(pools, "\"rebalance pgroup\" admin command, aborting failed command"),
                    ignored -> immediateFailedFuture(t));
        }
    }

    @Command(name = "rebalance cancel pgroup",
            hint = "cancel rebalancing operation",
            description = "Cancels migration jobs created by the rebalancer.")
    public class RebalanceCancelCommand extends DelayedReply implements Callable<Reply>
    {
        @Argument(usage = "The name of the pool group.")
        String poolGroup;

        @Override
        public Reply call()
        {
            Collection<SelectionPool> pools = _psu.getPoolsByPoolGroup(poolGroup);
            addCallback(cancelAll(pools, "\"rebalance cancel pgroup\" admin command"),
                        new FutureCallback<List<PoolMigrationJobCancelMessage>>()
                                {
                                    @Override
                                    public void onSuccess(List<PoolMigrationJobCancelMessage> result)
                                    {
                                        String s = "Cancelled rebalancing on {0,choice,0#zero " +
                                                   "pools|1#one pool|1<{0,number,integer} pools}.";
                                        reply(MessageFormat.format(s, result.size()));
                                    }

                                    @Override
                                    public void onFailure(Throwable t)
                                    {
                                        reply(t);
                                    }
                                });
            return this;
        }
    }
}
