package org.dcache.acl.handler;

import java.sql.Driver;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.dcache.acl.ACLException;
import org.dcache.acl.config.Config;

import com.mchange.v2.c3p0.DataSources;
import com.mchange.v2.c3p0.DriverManagerDataSource;

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

                DataSource unpooled = DataSources.unpooledDataSource(url, _config.getUser(), _config.getPswd());
                ((DriverManagerDataSource) unpooled).setDescription(this.getClass().getSimpleName() + ": ACL support");
                _ds_pooled = DataSources.pooledDataSource(unpooled);

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
            DataSources.destroy(_ds_pooled);

        } catch (Exception e) {
            throw new ACLException("Close ACL Handler", e);
        }
    }
}
