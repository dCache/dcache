package org.dcache.webadmin.model.businessobjects;

import diskCacheV111.poolManager.PoolSelectionUnit.SelectionPool;
import diskCacheV111.pools.PoolCostInfo;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * This is a simple Data-Container Object for the relevant information
 * of dCache-Pools for later displaying.
 *
 * @author jan schaefer
 */
public class Pool {

    private String _name = "";
    private PoolCostInfo _costinfo;
    private SelectionPool _selectionPool;

    public Pool(PoolCostInfo costinfo, SelectionPool selectionPool) {
        checkNotNull(costinfo);
        checkNotNull(selectionPool);
        _costinfo = costinfo;
        _selectionPool = selectionPool;
        _name = costinfo.getPoolName();
    }

    public String getName() {
        return _name;
    }

    public PoolCostInfo getCostinfo() {
        return _costinfo;
    }

    public SelectionPool getSelectionPool() {
        return _selectionPool;
    }

    @Override
    public int hashCode() {
        return _name.hashCode();
    }

    @Override
    public boolean equals(Object testObject) {
        if (this == testObject) {
            return true;
        }

        if (!(testObject instanceof Pool)) {
            return false;
        }

        Pool otherPool = (Pool) testObject;

        if (!(otherPool._name.equals(_name))) {
            return false;
        }

        if (!(otherPool._costinfo.equals(_costinfo))) {
            return false;
        }
        if (!(otherPool._selectionPool.equals(_selectionPool))) {
            return false;
        }

        return true;
    }
}
