package org.dcache.webadmin.model.dataaccess.communication.collectors;

import diskCacheV111.services.space.message.GetLinkGroupsMessage;
import diskCacheV111.services.space.message.GetSpaceTokensMessage;
import diskCacheV111.util.CacheException;
import org.dcache.webadmin.model.dataaccess.communication.ContextPaths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author jans
 */
public class SpaceTokenCollector extends Collector {

    private final static Logger _log = LoggerFactory.getLogger(SpaceTokenCollector.class);

    @Override
    public void run() {

        try {
            while (true) {
                try {
                    collectSpaceTokens();
                    collectLinkGroups();
//                  catch everything - maybe next round it works out
                } catch (RuntimeException e) {
                    _log.debug(e.toString(), e);
                }
                Thread.sleep(10000);
            }
        } catch (InterruptedException e) {
            _log.info("Space token Collector interrupted");
        }
    }

    private void collectSpaceTokens() throws InterruptedException {
        try {
            _log.debug("Retrieving space tokens");
            GetSpaceTokensMessage reply = _cellStub.sendAndWait(
                    new GetSpaceTokensMessage());
            _pageCache.put(ContextPaths.SPACETOKENS,
                    reply.getSpaceTokenSet());
            _log.debug("Space tokens retrieved successfully");
        } catch (CacheException ex) {
            _log.debug("Could not retrieve Space tokens ", ex);
            _pageCache.remove(ContextPaths.SPACETOKENS);
        }
    }

    private void collectLinkGroups() throws InterruptedException {
        try {
            _log.debug("Retrieving linkgroups");
            GetLinkGroupsMessage reply = _cellStub.sendAndWait(
                    new GetLinkGroupsMessage());
            _pageCache.put(ContextPaths.LINKGROUPS,
                    reply.getLinkGroupSet());
            _log.debug("Linkgroups retrieved successfully");
        } catch (CacheException ex) {
            _log.debug("Could not retrieve linkgroups ", ex);
            _pageCache.remove(ContextPaths.LINKGROUPS);
        }
    }
}
