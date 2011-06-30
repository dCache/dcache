package org.dcache.webadmin.model.dataaccess.impl;

import org.dcache.webadmin.model.dataaccess.DomainsDAO;
import org.dcache.webadmin.model.dataaccess.InfoDAO;
import org.dcache.webadmin.model.dataaccess.LinkGroupsDAO;
import org.dcache.webadmin.model.dataaccess.MoverDAO;
import org.dcache.webadmin.model.dataaccess.PoolGroupDAO;
import org.dcache.webadmin.model.dataaccess.PoolsDAO;
import org.dcache.webadmin.model.dataaccess.DAOFactory;
import org.dcache.webadmin.model.dataaccess.communication.CommandSenderFactory;
import org.dcache.webadmin.model.dataaccess.communication.impl.PageInfoCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory class for the DAOs. The whole design with an factory is mainly
 * introduced for better testablility with Unittests
 * @author jans
 */
public class DAOFactoryImpl implements DAOFactory {

    private Logger _log = LoggerFactory.getLogger(DAOFactory.class);
    private CommandSenderFactory _defaultCommandSenderFactory;
    private PageInfoCache _pageCache;

    @Override
    public PoolsDAO getPoolsDAO() {
        _log.debug("PoolsDAO requested");
        checkDefaultCommandSenderSet();
//      maybe better make it an singleton - they all end up using one cell anyway?
        return new StandardPoolsDAO(_defaultCommandSenderFactory);
    }

    @Override
    public PoolGroupDAO getPoolGroupDAO() {
        _log.debug("PoolGroupDAO requested");
        checkDefaultCommandSenderSet();
        return new StandardPoolGroupDAO(_defaultCommandSenderFactory);
    }

    @Override
    public InfoDAO getInfoDAO() {
        checkDefaultCommandSenderSet();
        return new StandardInfoDAO(_defaultCommandSenderFactory);
    }

    @Override
    public DomainsDAO getDomainsDAO() {
        checkDefaultCommandSenderSet();
        checkPageCacheSet();
        return new StandardDomainsDAO(_pageCache, _defaultCommandSenderFactory);
    }

    @Override
    public LinkGroupsDAO getLinkGroupsDAO() {
        checkDefaultCommandSenderSet();
        return new StandardLinkGroupsDAO(_defaultCommandSenderFactory);
    }

    @Override
    public MoverDAO getMoverDAO() {
        checkPageCacheSet();
        return new StandardMoverDAO(_pageCache,
                _defaultCommandSenderFactory);
    }

    @Override
    public void setDefaultCommandSenderFactory(CommandSenderFactory commandSenderFactory) {
        _log.debug("DefaultCommandSenderFactory set {}", commandSenderFactory.toString());
        _defaultCommandSenderFactory = commandSenderFactory;
    }

    public void setPageCache(PageInfoCache pageCache) {
        _log.debug("PageCache set {}", pageCache);
        _pageCache = pageCache;
    }

    private void checkDefaultCommandSenderSet() {
        if (_defaultCommandSenderFactory == null) {
            throw new IllegalStateException("DefaultPoolCommandSender not set");
        }
    }

    private void checkPageCacheSet() {
        if (_pageCache == null) {
            throw new IllegalStateException("PageCache not set");
        }
    }
}
