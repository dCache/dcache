package org.dcache.webadmin.model.dataaccess;

import java.util.Set;
import org.dcache.webadmin.model.exceptions.DAOException;

/**
 * A PoolGroupDAO implements the services concerning Data of PoolGroups
 * of dCache.
 * @author jans
 */
public interface PoolGroupDAO {

    /**
     *
     * @return delivers a list of Poolgroups in dCache
     */
    public Set<String> getPoolGroupNames() throws DAOException;
}
