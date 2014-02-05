package org.dcache.webadmin.model.dataaccess.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.dcache.webadmin.model.dataaccess.DAOFactory;
import org.dcache.webadmin.model.dataaccess.DomainsDAO;
import org.dcache.webadmin.model.dataaccess.ILogEntryDAO;
import org.dcache.webadmin.model.dataaccess.InfoDAO;
import org.dcache.webadmin.model.dataaccess.LinkGroupsDAO;
import org.dcache.webadmin.model.dataaccess.MoverDAO;
import org.dcache.webadmin.model.dataaccess.PoolsDAO;
import org.dcache.webadmin.model.dataaccess.communication.CommandSenderFactory;
import org.dcache.webadmin.model.dataaccess.communication.impl.PageInfoCache;

/**
 * Factory class for the DAOs. The whole design with an factory is mainly
 * introduced for better testablility with Unittests
 *
 * @author jans
 */
public class DAOFactoryImpl implements DAOFactory {
    private Logger _log = LoggerFactory.getLogger(DAOFactory.class);
    private CommandSenderFactory _defaultCommandSenderFactory;
    private PageInfoCache _pageCache;
    private ILogEntryDAO _logEntryDAO;

    @Override
    public synchronized ILogEntryDAO getLogEntryDAO() {
        return _logEntryDAO;
    }

    @Override
    public DomainsDAO getDomainsDAO() {
        checkDefaultsSet();
        return new StandardDomainsDAO(_pageCache, _defaultCommandSenderFactory);
    }

    @Override
    public InfoDAO getInfoDAO() {
        checkDefaultsSet();
        return new StandardInfoDAO(_defaultCommandSenderFactory);
    }

    @Override
    public LinkGroupsDAO getLinkGroupsDAO() {
        checkDefaultsSet();
        return new StandardLinkGroupsDAO(_pageCache,
                        _defaultCommandSenderFactory);
    }

    @Override
    public MoverDAO getMoverDAO() {
        checkDefaultsSet();
        return new StandardMoverDAO(_pageCache, _defaultCommandSenderFactory);
    }

    @Override
    public PoolsDAO getPoolsDAO() {
        checkDefaultsSet();
        return new StandardPoolsDAO(_pageCache, _defaultCommandSenderFactory);
    }

    @Override
    public void setDefaultCommandSenderFactory(
                    CommandSenderFactory commandSenderFactory) {
        _log.trace("DefaultCommandSenderFactory set {}",
                        commandSenderFactory.toString());
        _defaultCommandSenderFactory = commandSenderFactory;
    }

    public void setLogEntryDAO(ILogEntryDAO alarmDAO) {
        _logEntryDAO = alarmDAO;
    }

    public void setPageCache(PageInfoCache pageCache) {
        _log.trace("PageCache set {}", pageCache);
        _pageCache = pageCache;
    }

    private void checkDefaultCommandSenderSet() {
        if (_defaultCommandSenderFactory == null) {
            throw new IllegalStateException("DefaultPoolCommandSender not set");
        }
    }

    private void checkDefaultsSet() {
        checkDefaultCommandSenderSet();
        checkPageCacheSet();
    }

    private void checkPageCacheSet() {
        if (_pageCache == null) {
            throw new IllegalStateException("PageCache not set");
        }
    }
}
