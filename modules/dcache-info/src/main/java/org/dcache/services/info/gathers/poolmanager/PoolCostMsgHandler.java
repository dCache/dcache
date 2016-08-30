package org.dcache.services.info.gathers.poolmanager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;

import diskCacheV111.pools.PoolCostInfo;
import diskCacheV111.pools.PoolCostInfo.NamedPoolQueueInfo;
import diskCacheV111.pools.PoolCostInfo.PoolQueueInfo;
import diskCacheV111.pools.PoolCostInfo.PoolSpaceInfo;
import diskCacheV111.vehicles.CostModulePoolInfoTable;

import dmg.cells.nucleus.UOID;

import org.dcache.services.info.base.FloatingPointStateValue;
import org.dcache.services.info.base.IntegerStateValue;
import org.dcache.services.info.base.StatePath;
import org.dcache.services.info.base.StateUpdate;
import org.dcache.services.info.base.StateUpdateManager;
import org.dcache.services.info.base.StringStateValue;
import org.dcache.services.info.gathers.CellMessageHandlerSkel;
import org.dcache.services.info.gathers.MessageMetadataRepository;
import org.dcache.services.info.stateInfo.SpaceInfo;

/**
 * This class processing incoming CellMessages that contain CostModulePoolInfoTable
 *
 * @author Paul Millar <paul.millar@desy.de>
 */
public class PoolCostMsgHandler extends CellMessageHandlerSkel
{
    private static final Logger LOGGER = LoggerFactory.getLogger(PoolCostMsgHandler.class);

    public PoolCostMsgHandler(StateUpdateManager sum,
            MessageMetadataRepository<UOID> msgMetaRepo)
    {
        super(sum, msgMetaRepo);
    }

    @Override
    public void process(Object msgPayload, long msgDeliveryPeriod)
    {
        long metricLifetime = (long) (msgDeliveryPeriod * 2.5); // Give metrics a lifetime of 2.5* message deliver period

        if (!(msgPayload instanceof CostModulePoolInfoTable)) {
            LOGGER.error("received non-CostModulePoolInfoTable object in message");
            return;
        }

        CostModulePoolInfoTable poolInfoTbl = (CostModulePoolInfoTable) msgPayload;

        Collection<PoolCostInfo> poolInfos = poolInfoTbl.poolInfos();

        if (poolInfos.size() > 0) {
            StateUpdate update = buildUpdate(poolInfos, metricLifetime);
            applyUpdates(update);
        }
    }


    /**
     * Build a StateUpdate for the supplied collection of pool information
     * @param poolInfos a collection of informaiton about the pools
     * @param metricLifetime the duration metrics should remain
     * @return a StateUpdate that updates the state
     */
    private StateUpdate buildUpdate(Collection<PoolCostInfo> poolInfos,
            long metricLifetime)
    {
        StatePath poolsPath = new StatePath("pools");

        StateUpdate update = new StateUpdate();

        for (PoolCostInfo thisPoolInfo : poolInfos) {

            String poolName = thisPoolInfo.getPoolName();

            StatePath pathToThisPool = poolsPath.newChild(poolName);
            StatePath pathToQueues = pathToThisPool.newChild("queues");


            /*
             *  Add all the standard queues
             */
            addTapeQueueInfo(update, pathToQueues, "store", thisPoolInfo.getStoreQueue(), metricLifetime);
            addTapeQueueInfo(update, pathToQueues, "restore", thisPoolInfo.getRestoreQueue(), metricLifetime);
            addQueueInfo(update, pathToQueues, "p2p-queue", thisPoolInfo.getP2pQueue(), metricLifetime);
            addQueueInfo(update, pathToQueues, "p2p-clientqueue", thisPoolInfo.getP2pClientQueue(), metricLifetime);


            /*
             *  Add the "extra" named queues
             */
            addNamedQueues(update, pathToQueues, thisPoolInfo, metricLifetime);


            /*
             *  Add information about our default queue's name, if we have one.
             */
            String defaultQueue = thisPoolInfo.getDefaultQueueName();
            update.appendUpdate(pathToQueues.newChild("default-queue"),
                                new StringStateValue(defaultQueue, metricLifetime));


            /**
             *  Add information about this pool's space utilisation.
             */

            addSpaceInfo(update, pathToThisPool.newChild("space"), thisPoolInfo.getSpaceInfo(), metricLifetime);
        }

        return update;
    }



    /**
     * Add information about a specific queue to a pool's portion of dCache state.
     * The state tree looks like:
     *
     * <pre>
     * [dCache]
     *  |
     *  +--[pools]
     *  |   |
     *  |   +--[&lt;poolName>]
     *  |   |   |
     *  |   |   +--[queues]
     *  |   |   |   |
     *  |   |   |   +--[&lt;queueName1>]
     *  |   |   |   |    |
     *  |   |   |   |    +--active: nnn
     *  |   |   |   |    +--max-active: nnn
     *  |   |   |   |    +--queued: nnn
     *  |   |   |   |
     *  |   |   |   +--[&lt;queueName2>]
     * </pre>
     *
     * @param pathToQueues the StatePath pointing to queues (e.g.,
     * "pools.mypool_1.queues")
     * @param queueName the name of the queue.
     */
    private void addQueueInfo(StateUpdate stateUpdate, StatePath pathToQueues,
            String queueName, PoolQueueInfo info, long lifetime)
    {
        StatePath queuePath = pathToQueues.newChild(queueName);

        stateUpdate.appendUpdate(queuePath.newChild("active"),
                new IntegerStateValue(info.getActive(), lifetime));
        stateUpdate.appendUpdate(queuePath.newChild("max-active"),
                new IntegerStateValue(info.getMaxActive(), lifetime));
        stateUpdate.appendUpdate(queuePath.newChild("queued"),
                new IntegerStateValue(info.getQueued(), lifetime));
    }

    /**
     * Add information about a specific tape queue to a pool's portion of dCache state.
     * The state tree looks like:
     *
     * <pre>
     * [dCache]
     *  |
     *  +--[pools]
     *  |   |
     *  |   +--[&lt;poolName>]
     *  |   |   |
     *  |   |   +--[queues]
     *  |   |   |   |
     *  |   |   |   +--[&lt;queueName1>]
     *  |   |   |   |    |
     *  |   |   |   |    +--active: nnn
     *  |   |   |   |    +--queued: nnn
     *  |   |   |   |
     *  |   |   |   +--[&lt;queueName2>]
     * </pre>
     *
     * The difference to regular queues is that tape queues to not have a public maximum
     * value for active tasks (specific providers may have one, but this is internal to
     * a provider).
     *
     * @param pathToQueues the StatePath pointing to queues (e.g.,
     * "pools.mypool_1.queues")
     * @param queueName the name of the queue.
     */
    private void addTapeQueueInfo(StateUpdate stateUpdate, StatePath pathToQueues,
            String queueName, PoolQueueInfo info, long lifetime)
    {
        StatePath queuePath = pathToQueues.newChild(queueName);

        stateUpdate.appendUpdate(queuePath.newChild("active"),
                new IntegerStateValue(info.getActive(), lifetime));
        stateUpdate.appendUpdate(queuePath.newChild("queued"),
                new IntegerStateValue(info.getQueued(), lifetime));
    }


    /**
     * Adds information from a pool's PoolSpaceInfo object.
     * We add this into the state in the following way:
     *
     * <pre>
     * [dCache]
     *  |
     *  +--[pools]
     *  |   |
     *  |   +--[&lt;poolName>]
     *  |   |   |
     *  |   |   +--[space]
     *  |   |   |   |
     *  |   |   |   +--total: nnn
     *  |   |   |   +--free: nnn
     *  |   |   |   +--precious: nnn
     *  |   |   |   +--removable: nnn
     *  |   |   |   +--pinned: nnn
     *  |   |   |   +--used: nnn
     *  |   |   |   +--gap: nnn
     *  |   |   |   +--break-even: nnn
     *  |   |   |   +--LRU-seconds: nnn
     * </pre>
     *
     * @param stateUpdate the StateUpdate we will append
     * @param path the StatePath pointing to the space branch
     * @param info the space information to include.
     */
    private void addSpaceInfo(StateUpdate stateUpdate, StatePath pathToSpace,
            PoolSpaceInfo info, long lifetime)
    {
        SpaceInfo si = new SpaceInfo(info);

        si.addMetrics(stateUpdate, pathToSpace, lifetime);

        stateUpdate.appendUpdate(pathToSpace.newChild("gap"),
                new IntegerStateValue(info.getGap(), lifetime));
        stateUpdate.appendUpdate(pathToSpace.newChild("break-even"),
                new FloatingPointStateValue(info.getBreakEven(), lifetime));
        stateUpdate.appendUpdate(pathToSpace.newChild("LRU-seconds"),
                new IntegerStateValue(info.getLRUSeconds(), lifetime));
    }


    /**
     * Add information about all "named" queues.  The available information is the
     * same as with regular queues, but there are arbirary number of these. The
     * information is presented underneath the named-queues branch of the queues
     * branch:
     *
     * <pre>
     * [dCache]
     *  |
     *  +--[pools]
     *  |   |
     *  |   +--[&lt;poolName>]
     *  |   |   |
     *  |   |   +--[queues]
     *  |   |   |   |
     *  |   |   |   +--[named-queues]
     *  |   |   |   |   |
     *  |   |   |   |   +--[&lt;namedQueue1>]
     *  |   |   |   |   |    |
     *  |   |   |   |   |    +--active: nnn
     *  |   |   |   |   |    +--max-active: nnn
     *  </pre>
     *
     * @param update the StateUpdate we are appending to
     * @param pathToQueues the StatePath pointing to [queues] above
     * @param thisPoolInfo the information about this pool.
     */
    private void addNamedQueues(StateUpdate update, StatePath pathToQueues,
            PoolCostInfo thisPoolInfo, long lifetime)
    {
        Map<String, NamedPoolQueueInfo> namedQueuesInfo = thisPoolInfo.getExtendedMoverHash();

        if (namedQueuesInfo == null) {
            return;
        }

        StatePath pathToNamedQueues = pathToQueues.newChild("named-queues");

        for (NamedPoolQueueInfo thisNamedQueueInfo : namedQueuesInfo.values()) {
            addQueueInfo(update, pathToNamedQueues, thisNamedQueueInfo.getName(),
                    thisNamedQueueInfo, lifetime);
        }
    }
}
