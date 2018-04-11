package diskCacheV111.vehicles;

import dmg.cells.nucleus.CellAddressCore;
import java.io.Serializable;

/**
 * Object representing Pool name and associated address.
 */
public class Pool implements Serializable {

    private static final long serialVersionUID = -5093980092916270836L;

    private final String poolName;
    private final CellAddressCore poolAddress;

    public Pool(String poolName, CellAddressCore poolAddress) {
        this.poolName = poolName;
        this.poolAddress = poolAddress;
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

    @Override
    public String toString() {
        return "PoolName=" + poolName + " PoolAddress=" + poolAddress;
    }
}
