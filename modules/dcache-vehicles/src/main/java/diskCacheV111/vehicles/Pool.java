package diskCacheV111.vehicles;

import dmg.cells.nucleus.CellAddressCore;
import java.io.Serializable;

import org.dcache.pool.assumption.Assumption;
import org.dcache.pool.assumption.Assumptions;

/**
 * Object representing Pool name and associated address.
 */
public class Pool implements Serializable {

    private static final long serialVersionUID = -5093980092916270836L;

    private final String poolName;
    private final CellAddressCore poolAddress;
    private final Assumption assumption;

    public Pool(String poolName, CellAddressCore poolAddress, Assumption assumption) {
        this.poolName = poolName;
        this.poolAddress = poolAddress;
        this.assumption = assumption == null ? Assumptions.none() : assumption;
    }

    /**
     * Get pool's name.
     * @return pool's name.
     */
    public String getName() {
        return poolName;
    }

    /**
     * Get cell address of this pool.
     * @return cell address of this pool.
     */
    public CellAddressCore getAddress() {
        return poolAddress;
    }

    public Assumption getAssumption() {
        return assumption;
    }

    @Override
    public String toString() {
        return "PoolName=" + poolName + " PoolAddress=" + poolAddress;
    }
}
