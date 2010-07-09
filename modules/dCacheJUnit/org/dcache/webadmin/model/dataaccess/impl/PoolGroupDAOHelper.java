package org.dcache.webadmin.model.dataaccess.impl;

import java.util.HashSet;
import java.util.Set;
import org.dcache.webadmin.model.dataaccess.PoolGroupDAO;
import org.dcache.webadmin.model.exceptions.DAOException;

/**
 *
 * @author jans
 */
public class PoolGroupDAOHelper implements PoolGroupDAO {

    private HashSet<String> _poolGroups = new HashSet<String>();
    private boolean _alwaysThrowsDaoException = false;

    public PoolGroupDAOHelper() {
        _poolGroups.add(XMLDataGathererHelper.POOL1_POOLGROUP1);
        _poolGroups.add("testgroup1");
    }

    public Set<String> getPoolGroupNames() throws DAOException {
        if (_alwaysThrowsDaoException) {
            throw new DAOException("you are a bad boy!");
        }
        return _poolGroups;
    }

    public void setAlwaysThrowsDaoException(boolean alwaysThrowsDaoException) {
        _alwaysThrowsDaoException = alwaysThrowsDaoException;
    }
}
