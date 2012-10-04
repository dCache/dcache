package org.dcache.webadmin.model.dataaccess.impl;

import org.dcache.webadmin.model.dataaccess.DAOFactory;
import org.dcache.webadmin.model.dataaccess.DomainsDAO;
import org.dcache.webadmin.model.dataaccess.IAlarmDAO;
import org.dcache.webadmin.model.dataaccess.InfoDAO;
import org.dcache.webadmin.model.dataaccess.LinkGroupsDAO;
import org.dcache.webadmin.model.dataaccess.MoverDAO;
import org.dcache.webadmin.model.dataaccess.PoolsDAO;
import org.dcache.webadmin.model.dataaccess.communication.CommandSenderFactory;
import org.dcache.webadmin.model.dataaccess.communication.impl.PageInfoCache;
import org.dcache.webadmin.model.exceptions.DAOException;
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
    private IAlarmDAO _alarmDAO;
    private String _alarmDAOProperties;
    private String _alarmXMLPath;
    private boolean _alarmCleanerEnabled;
    private int _alarmCleanerSleepInterval;
    private int _alarmCleanerDeleteThreshold;

    @Override
    public synchronized IAlarmDAO getAlarmDAO() throws DAOException {
        if (_alarmDAO == null) {
            _alarmDAO = new DataNucleusAlarmStore(_alarmXMLPath,
                            _alarmDAOProperties,
                            _alarmCleanerEnabled,
                            _alarmCleanerSleepInterval,
                            _alarmCleanerDeleteThreshold);
        }
        return _alarmDAO;
    }

    public void setAlarmDAOProperties(String alarmDAOProperties) {
        _alarmDAOProperties = alarmDAOProperties;
    }

    public void setAlarmXMLPath(String alarmXMLPath) {
        _alarmXMLPath = alarmXMLPath;
    }

    public void setAlarmCleanerEnabled(boolean alarmCleanerEnabled) {
        _alarmCleanerEnabled = alarmCleanerEnabled;
    }

    public void setAlarmCleanerSleepInterval(int alarmCleanerSleepInterval) {
        _alarmCleanerSleepInterval = alarmCleanerSleepInterval;
    }

    public void setAlarmCleanerDeleteThreshold(int alarmCleanerDeleteThreshold) {
        _alarmCleanerDeleteThreshold = alarmCleanerDeleteThreshold;
    }

    @Override
    public PoolsDAO getPoolsDAO() {
        checkDefaultsSet();
        return new StandardPoolsDAO(_pageCache, _defaultCommandSenderFactory);
    }

    @Override
    public InfoDAO getInfoDAO() {
        checkDefaultsSet();
        return new StandardInfoDAO(_defaultCommandSenderFactory);
    }

    @Override
    public DomainsDAO getDomainsDAO() {
        checkDefaultsSet();
        return new StandardDomainsDAO(_pageCache, _defaultCommandSenderFactory);
    }

    @Override
    public LinkGroupsDAO getLinkGroupsDAO() {
        checkDefaultsSet();
        return new StandardLinkGroupsDAO(_pageCache, _defaultCommandSenderFactory);
    }

    @Override
    public MoverDAO getMoverDAO() {
        checkDefaultsSet();
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

    private void checkDefaultsSet() {
        checkDefaultCommandSenderSet();
        checkPageCacheSet();
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
