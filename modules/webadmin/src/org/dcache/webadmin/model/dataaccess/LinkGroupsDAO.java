package org.dcache.webadmin.model.dataaccess;

import java.util.Set;
import org.dcache.webadmin.model.businessobjects.LinkGroup;
import org.dcache.webadmin.model.exceptions.DAOException;

/**
 * Dataaccessobject for information concerning the dCache entities linkgroups
 * and spacereservations. An implementation should provide means to retrieve
 * these from the connected dCache-Instance.
 * @author jans
 */
public interface LinkGroupsDAO {

    public Set<LinkGroup> getLinkGroups() throws DAOException;
}
