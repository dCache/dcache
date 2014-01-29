package org.dcache.services.hsmcleaner;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import diskCacheV111.pools.PoolV2Mode;
import diskCacheV111.vehicles.PoolManagerPoolUpMessage;

import dmg.cells.nucleus.CellMessageReceiver;

import org.dcache.util.Args;

/**
 * Maintains an index of available pools.
 *
 * The information maintained is based on pool up messages send by the
 * pools. The class does not itself subscribe to these messages, see
 * the <code>messageArrived</code> method.
 */
public class PoolInformationBase implements CellMessageReceiver
{
    /**
     * Time in milliseconds after which pool information is
     * invalidated.
     */
    private long _timeout = 5 * 60 * 1000; // 5 minutes

    /**
     * Map of all pools currently up.
     */
    private Map<String, PoolInformation> _pools =
        new HashMap<>();

    /**
     * Map from HSM instance name to the set of pools attached to that
     * HSM.
     */
    private Map<String, Collection<PoolInformation>> _hsmToPool =
        new HashMap<>();

    /**
     *
     */
    synchronized public PoolInformation getPool(String pool)
    {
        return _pools.get(pool);
    }

    /**
     *
     */
    synchronized public Collection<PoolInformation> getPools()
    {
        return _pools.values();
    }

    /**
     * Returns a pool attached to a given HSM instance.
     *
     * @param hsm An HSM instance name.
     */
    synchronized public PoolInformation getPoolWithHSM(String hsm)
    {
        Collection<PoolInformation> pools = _hsmToPool.get(hsm);
        if (pools != null) {
            for (PoolInformation pool : pools) {
                if (pool.getAge() <= _timeout
                    && !pool.isDisabled(PoolV2Mode.DISABLED_STAGE)) {
                    return pool;
                }
            }
        }
        return null;
    }

    /**
     * Removes information about a pool. The pool will be added again
     * next time a pool up message is received.
     *
     * @param name A pool name.
     */
    synchronized public void remove(String name)
    {
        PoolInformation pool = _pools.remove(name);
        if (pool != null) {
            for (String hsm : pool.getHsmInstances()) {
                Collection<PoolInformation> pools = _hsmToPool.get(hsm);
                pools.remove(pool);
                if (pools.isEmpty()) {
                    _hsmToPool.remove(hsm);
                }
            }
        }
    }

    /**
     * Message handler for PoolUp messages. The class does not
     * subscribe to these messages, so the client must implement a
     * mechanism with which these messages arrive here.
     */
    synchronized public void messageArrived(PoolManagerPoolUpMessage message)
    {
        String name = message.getPoolName();

        remove(name);

        PoolInformation pool = new PoolInformation(message);
        _pools.put(name, pool);

        /* Update HSM to pool map.
         */
        for (String hsm : pool.getHsmInstances()) {
            Collection<PoolInformation> pools = _hsmToPool.get(hsm);
            if (pools == null) {
                pools = new ArrayList<>();
                _hsmToPool.put(hsm, pools);
            }
            pools.add(pool);
        }
    }

    public final static String hh_pools_ls = "# Lists known pools";
    synchronized public String ac_pools_ls(Args args)
    {
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("%-20s %s\n", "Pool", "HSM Instances"));
        for (PoolInformation pool: getPools()) {
            sb.append(String.format("%-20s %s\n",
                                    pool.getName(),
                                    pool.getHsmInstances()));
        }
        return sb.toString();
    }

    public final static String hh_pools_attached_to_hsm = "<hsm> # Lists pools attached to HSM <hsm>";
    synchronized public String ac_pools_attached_to_hsm_$_1(Args args)
    {
        String hsmName = args.argv(0);
        StringBuilder sb = new StringBuilder();
        Collection<PoolInformation> listPools = _hsmToPool.get(hsmName);

        if ( listPools == null || listPools.isEmpty() ) {
              sb.append("There are no pools attached to HSM ").append(hsmName);
        } else {
              sb.append("List of pools attached to HSM ").append(hsmName).append(" : \n");
              for (PoolInformation pool : listPools) {
                   sb.append("Pool:  ").append(pool.getName()).append("\n");
              }
        }
        return sb.toString();
    }

    public final static String hh_hsms_attached_to_pool = "<pool> # Lists HSMs attached to pool <pool>";
    synchronized public String ac_hsms_attached_to_pool_$_1(Args args)
    {
        String poolName = args.argv(0);
        StringBuilder sb = new StringBuilder();
        PoolInformation pool = this.getPool(poolName);

        if (pool == null) {
            sb.append("No information available about pool " + poolName);
        } else {
            Collection<String> listHSMs = pool.getHsmInstances();
            if (listHSMs == null || listHSMs.isEmpty()) {
                sb.append("There are no HSMs attached to pool ").append(poolName);
            } else {
                sb.append("List of HSMs attached to pool ").append(poolName).append(" : \n");
                for(String hsm : listHSMs) {
                    sb.append("HSM:  ").append(hsm).append("\n");
                }
            }
        }
        return sb.toString();
    }
}
