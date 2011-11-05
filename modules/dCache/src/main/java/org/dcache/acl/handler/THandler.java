package org.dcache.acl.handler;

import java.sql.Driver;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.dcache.acl.ACLException;
import org.dcache.acl.config.Config;

import com.jolbox.bonecp.BoneCPDataSource;

/**
 * Basic component, extended by AclHandler, FPathHandler and PrincipalHandler.
 *
 * @author David Melkumyan, DESY Zeuthen
 */
class THandler {

    protected Config _config;

    protected DataSource _ds_pooled;

    /**
     * @param config
     *            Configuration
     */
    public THandler(Config config) throws ACLException {
        initialize(config);
    }

    /**
     * Initializes Handler
     *
     * @param config
     *            Configuration
     * @throws ACLException
     */
    protected void initialize(Config config) throws ACLException {
        _config = config;
        _ds_pooled = getDataSource();
    }

    private DataSource getDataSource() throws ACLException {
        if ( _ds_pooled == null ) {
            try {
                final Driver d = (Driver) Class.forName(_config.getDriver()).newInstance();
                final String url = _config.getUrl();
                if ( d.acceptsURL(url) == false )
                    throw new ACLException("Get DataSource", "Driver not accept the URL: " + url);

                BoneCPDataSource ds = new BoneCPDataSource();
                ds.setJdbcUrl(url);
                ds.setUsername(_config.getUser());
                ds.setPassword(_config.getPswd());
                ds.setIdleConnectionTestPeriodInMinutes(60);
                ds.setIdleMaxAgeInMinutes(240);
                ds.setMaxConnectionsPerPartition(30);
                ds.setMaxConnectionsPerPartition(10);
                ds.setPartitionCount(3);
                ds.setAcquireIncrement(5);
                ds.setStatementsCacheSize(100);
                ds.setReleaseHelperThreads(3);
                _ds_pooled = ds;

                initPreparedStatements();

            } catch (InstantiationException e) {
                throw new ACLException("Get DataSource", e);

            } catch (IllegalAccessException e) {
                throw new ACLException("Get DataSource", e);

            } catch (ClassNotFoundException e) {
                throw new ACLException("Get DataSource", e);

            } catch (SQLException e) {
                throw new ACLException("Get DataSource", e);
            }
        }
        return _ds_pooled;
    }

    protected void initPreparedStatements() {
    }

//	protected void attemptRollback(Connection conn) throws ACLException {
//		if ( conn != null ) {
//			try {
//				conn.rollback();
//
//			} catch (SQLException e) {
//				throw new ACLException("Attempt rollback", "SQLException", e);
//			}
//		}
//	}

    public Config getConfig() {
        return _config;
    }

    public void setConfig(Config config) {
        _config = config;
    }

    public DataSource getDs_pooled() {
        return _ds_pooled;
    }

    public void setDs_pooled(DataSource ds_pooled) {
        _ds_pooled = ds_pooled;
    }

    public void close() throws ACLException {
        try {
            ((BoneCPDataSource) _ds_pooled).close();
        } catch (Exception e) {
            throw new ACLException("Close ACL Handler", e);
        }
    }
}
