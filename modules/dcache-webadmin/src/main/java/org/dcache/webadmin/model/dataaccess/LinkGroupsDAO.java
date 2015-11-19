package org.dcache.webadmin.model.dataaccess;

import java.util.Collection;

import diskCacheV111.services.space.LinkGroup;
import diskCacheV111.services.space.Space;

import org.dcache.webadmin.model.exceptions.DAOException;

/**
 * Dataaccessobject for information concerning the dCache entities linkgroups
 * and spacereservations. An implementation should provide means to retrieve
 * these from the connected dCache-Instance.
 * @author jans
 */
public interface LinkGroupsDAO {

    Collection<LinkGroup> getLinkGroups() throws DAOException;

    Collection<Space> getSpaceReservations() throws DAOException;
}
