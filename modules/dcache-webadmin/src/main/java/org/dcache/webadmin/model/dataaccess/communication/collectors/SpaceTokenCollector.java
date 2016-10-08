package org.dcache.webadmin.model.dataaccess.communication.collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import diskCacheV111.services.space.message.GetLinkGroupsMessage;
import diskCacheV111.services.space.message.GetSpaceTokensMessage;
import diskCacheV111.util.CacheException;

import dmg.cells.nucleus.NoRouteToCellException;

import org.dcache.util.backoff.IBackoffAlgorithm.Status;
import org.dcache.webadmin.model.dataaccess.communication.ContextPaths;

/**
 *
 * @author jans
 */
public class SpaceTokenCollector extends Collector {

    private static final Logger _log
        = LoggerFactory.getLogger(SpaceTokenCollector.class);

    private void collectSpaceTokens() throws CacheException,
            InterruptedException, NoRouteToCellException
    {
        _log.debug("Retrieving space tokens");
        GetSpaceTokensMessage reply
            = _cellStub.sendAndWait(new GetSpaceTokensMessage());
        _pageCache.put(ContextPaths.SPACETOKENS, reply.getSpaceTokenSet());
        _log.debug("Space tokens retrieved successfully");
    }

    private void collectLinkGroups() throws CacheException,
            InterruptedException, NoRouteToCellException
    {
        _log.debug("Retrieving linkgroups");
        GetLinkGroupsMessage reply
            = _cellStub.sendAndWait(new GetLinkGroupsMessage());
        _pageCache.put(ContextPaths.LINKGROUPS, reply.getLinkGroups());
        _log.debug("Linkgroups retrieved successfully");
    }

    @Override
    public Status call() throws InterruptedException {
        try {
            collectSpaceTokens();
        } catch (NoRouteToCellException | CacheException ce) {
            _log.debug("problem retrieving space tokens from space manager: {}",
                            ce.getMessage());
            _pageCache.remove(ContextPaths.SPACETOKENS);
            return Status.FAILURE;
        }

        try {
            collectLinkGroups();
        } catch (NoRouteToCellException | CacheException ce) {
            _log.debug("problem retrieving link groups from space manager: {}",
                            ce.getMessage());
            _pageCache.remove(ContextPaths.LINKGROUPS);
            return Status.FAILURE;
        }

        return Status.SUCCESS;
    }

    public void setSpaceManagerEnabled(String enabled) {
        setEnabled( "yes".equalsIgnoreCase(enabled)
                 || "on".equalsIgnoreCase(enabled)
                 || "true".equalsIgnoreCase(enabled)
                 || "enabled".equalsIgnoreCase(enabled));
    }
}
