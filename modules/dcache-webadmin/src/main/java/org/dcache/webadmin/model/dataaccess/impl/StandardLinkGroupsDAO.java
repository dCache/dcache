package org.dcache.webadmin.model.dataaccess.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

import diskCacheV111.services.space.LinkGroup;
import diskCacheV111.services.space.Space;

import org.dcache.webadmin.model.dataaccess.LinkGroupsDAO;
import org.dcache.webadmin.model.dataaccess.communication.CommandSenderFactory;
import org.dcache.webadmin.model.dataaccess.communication.ContextPaths;
import org.dcache.webadmin.model.dataaccess.communication.impl.PageInfoCache;
import org.dcache.webadmin.model.exceptions.DAOException;
import org.dcache.webadmin.model.exceptions.NoSuchContextException;

/**
 * Standard implementation  of a LinkGroupsDAO in dCache using the pageCache to
 * retrieve information on linkgroups and SpaceTokens
 * @author jans
 */
public class StandardLinkGroupsDAO implements LinkGroupsDAO {

    private PageInfoCache _pageCache;
    private static final Logger _log = LoggerFactory.getLogger(StandardLinkGroupsDAO.class);

    public StandardLinkGroupsDAO(PageInfoCache pageCache,
            CommandSenderFactory commandSenderFactory) {
        _pageCache = pageCache;
    }

    @Override
    public Collection<Space> getSpaceReservations() throws DAOException {
        _log.debug("getSpaceReservations called");
        try {
            return tryToGetSpaceReservations();
        } catch (NoSuchContextException ex) {
            throw new DAOException("Data not available yet - Srm up already?", ex);
        }
    }

    @Override
    public Collection<LinkGroup> getLinkGroups() throws DAOException {
        _log.debug("getLinkGroups called");
        try {
            return tryToGetLinkGroups();
        } catch (NoSuchContextException ex) {
            throw new DAOException("Data not available yet - Srm up already?", ex);
        }
    }

    private Collection<LinkGroup> tryToGetLinkGroups() throws NoSuchContextException {
        return (Collection<LinkGroup>) _pageCache.getCacheContent(ContextPaths.LINKGROUPS);
    }

    private Collection<Space> tryToGetSpaceReservations() throws NoSuchContextException {
        return (Collection<Space>) _pageCache.getCacheContent(ContextPaths.SPACETOKENS);
    }
}
