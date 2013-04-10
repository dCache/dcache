package org.dcache.webadmin.model.dataaccess.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

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
    private String _alarmsXMLPath;
    private String _alarmsDbDriver;
    private String _alarmsDbUrl;
    private String _alarmsDbUser;
    private String _alarmsDbPass;
    private String _alarmsPropertiesPath;
    private boolean _alarmCleanerEnabled;
    private int _alarmCleanerSleepInterval;
    private int _alarmCleanerDeleteThreshold;

    @Override
    public synchronized ILogEntryDAO getLogEntryDAO() {
        if (_logEntryDAO == null) {
            try {
                DataNucleusAlarmStore store = new DataNucleusAlarmStore(
                                                  _alarmsXMLPath,
                                                  getAlarmsProperties(),
                                                  _alarmCleanerEnabled,
                                                  _alarmCleanerSleepInterval,
                                                  _alarmCleanerDeleteThreshold);
                store.initialize();
                _logEntryDAO = store;
            } catch (IOException t) {
                _log.error("NOP logging store DAO: {}; cause: {}",
                                t.getMessage(), t.getCause());
            }
        }
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

    public void setAlarmsCleanerDeleteThreshold(
                    int alarmCleanerDeleteThreshold) {
        _alarmCleanerDeleteThreshold = alarmCleanerDeleteThreshold;
    }

    public void setAlarmsCleanerEnabled(boolean alarmCleanerEnabled) {
        _alarmCleanerEnabled = alarmCleanerEnabled;
    }

    public void setAlarmsCleanerSleepInterval(
                    int alarmCleanerSleepInterval) {
        _alarmCleanerSleepInterval = alarmCleanerSleepInterval;
    }

    public void setAlarmsDbDriver(String alarmsDbDriver) {
        _alarmsDbDriver = alarmsDbDriver;
    }

    public void setAlarmsDbPass(String alarmsDbPass) {
        _alarmsDbPass = alarmsDbPass;
    }

    public void setAlarmsDbUrl(String alarmsDbUrl) {
        _alarmsDbUrl = alarmsDbUrl;
    }

    public void setAlarmsDbUser(String alarmsDbUser) {
        _alarmsDbUser = alarmsDbUser;
    }

    public void setAlarmsPropertiesPath(String alarmsPropertiesPath) {
        _alarmsPropertiesPath = alarmsPropertiesPath;
    }

    public void setAlarmsXMLPath(String alarmsXMLPath) {
        _alarmsXMLPath = alarmsXMLPath;
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

    private Properties getAlarmsProperties() throws IOException {
        Properties properties = new Properties();
        properties.setProperty("datanucleus.ConnectionDriverName",
                        _alarmsDbDriver);
        properties.setProperty("datanucleus.ConnectionPassword", _alarmsDbPass);
        properties.setProperty("datanucleus.ConnectionURL", _alarmsDbUrl);
        properties.setProperty("datanucleus.ConnectionUserName", _alarmsDbUser);
        if (_alarmsPropertiesPath != null
                        && !_alarmsPropertiesPath.trim().isEmpty()) {
            File file = new File(_alarmsPropertiesPath);
            if (!file.exists()) {
                throw new FileNotFoundException("Cannot find properties file: "
                                + file);
            }
            try (InputStream stream = new FileInputStream(file)) {
                properties.load(stream);
            }
        }
        return properties;
    }
}
