package org.dcache.webadmin.model.dataaccess.impl;

import java.util.HashSet;
import java.util.Set;

import diskCacheV111.services.space.LinkGroup;
import diskCacheV111.services.space.Space;

import org.dcache.webadmin.model.dataaccess.LinkGroupsDAO;
import org.dcache.webadmin.model.exceptions.DAOException;

/**
 * HelperDAO class to simulate an datasource for unittests
 * @author jans
 */
public class LinkGroupsDAOHelper implements LinkGroupsDAO {

    @Override
    public Set<LinkGroup> getLinkGroups() throws DAOException {
        return new HashSet<>();
    }

    @Override
    public Set<Space> getSpaceReservations() throws DAOException {
        return new HashSet<>();
    }
}
