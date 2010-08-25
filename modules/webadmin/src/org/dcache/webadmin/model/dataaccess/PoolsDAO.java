package org.dcache.webadmin.model.dataaccess;

import diskCacheV111.pools.PoolV2Mode;
import java.util.Set;
import org.dcache.webadmin.model.businessobjects.Pool;
import org.dcache.webadmin.model.exceptions.DAOException;

/**
 * A PoolsDAO implements the services to get the Data out of a dCache that is
 * needed to display information about Pools.
 * @author jan schaefer 29-10-2009
 */
public interface PoolsDAO {

    /**
     *
     * @return delivers a list of Pools in dCache
     */
    public Set<Pool> getPools() throws DAOException;

    /**
     *
     * @param poolIds pools to change mode
     * @param poolMode mode to change to
     * @param userName user who calls the method
     */
    public void changePoolMode(Set<String> poolIds, PoolV2Mode poolMode, String userName)
            throws DAOException;
}
