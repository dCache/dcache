/*
 * JdbcConnectionPool.java
 *
 * Created on June 21, 2004, 2:12 PM
 */

package org.dcache.util;

import java.sql.*;
import java.util.*;
import javax.sql.DataSource;

import org.apache.log4j.Logger;

import org.apache.commons.pool.ObjectPool;
import org.apache.commons.pool.impl.GenericObjectPool;
import org.apache.commons.pool.impl.StackKeyedObjectPoolFactory;
import org.apache.commons.dbcp.ConnectionFactory;
import org.apache.commons.dbcp.PoolingDataSource;
import org.apache.commons.dbcp.PoolableConnectionFactory;
import org.apache.commons.dbcp.DriverManagerConnectionFactory;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
/**
 *
 * @author  timur
 */
public class JdbcConnectionPool {
    
    private final String jdbcUrl;
    private final String jdbcClass;
    private final String user;
    private final String pass;
    private final DataSource dataSource;
    private static Logger _logSql = 
            Logger.getLogger(
            "logger.org.dcache.db.sql"+
            JdbcConnectionPool.class.getName());

    
    private static final HashSet pools = new HashSet();
    
    /**
     * DataSource should not be closed
     */
    public synchronized static final DataSource getDataSource(
    String jdbcUrl,
    String jdbcClass,
    String user,
    String pass) throws SQLException {
        JdbcConnectionPool pool = getPool(jdbcUrl,jdbcClass, user, pass);
        return pool.dataSource;
    }
    
    public synchronized static final JdbcConnectionPool getPool(String jdbcUrl,
    String jdbcClass,
    String user,
    String pass) throws SQLException {
        long starttimestamp = System.currentTimeMillis();
        for (Iterator i = pools.iterator();
        i.hasNext();) {
            JdbcConnectionPool pool = (JdbcConnectionPool) i.next();
            if(pool.jdbcClass.equals(jdbcClass) &&
            pool.jdbcUrl.equals(jdbcUrl) &&
            pool.pass.equals(pass) &&
            pool.user.equals(user) ) {
            long elapsed = System.currentTimeMillis()-starttimestamp;
            if( _logSql.isDebugEnabled() ) {
                _logSql.debug( "getPool() took "+elapsed+" ms");
            }
                return pool;
            }
        }
        JdbcConnectionPool pool = new JdbcConnectionPool(jdbcUrl,jdbcClass,user,pass);
        pools.add(pool);
        long elapsed = System.currentTimeMillis()-starttimestamp;
        if( _logSql.isDebugEnabled() ) {
            _logSql.debug( "getPool() took "+elapsed+" ms");
        }

        return pool;
        
    }
    
    /** Creates a new instance of ResuestsPropertyStorage */
    protected JdbcConnectionPool(  String jdbcUrl,
    String jdbcClass,
    String user,
    String pass) throws SQLException {
        if( jdbcUrl == null )
        {
            throw new NullPointerException("JdbcConnectionPool, jdbc url is null");
        }
        if( jdbcClass == null )
        {
            throw new NullPointerException("JdbcConnectionPool, jdbc class is null");
        }
        if( user == null )
        {
            throw new NullPointerException("JdbcConnectionPool, jdbc user is null");
        }
        if( pass == null )
        {
            throw new NullPointerException("JdbcConnectionPool, jdbc pass is null (check jdbcPass or pgPass options)");
        }
        try {
            Class.forName(jdbcClass);
        }
        catch(Exception e) {
            throw new SQLException("can not initialize jdbc driver : "+jdbcClass);
        }
        
        this.jdbcUrl = jdbcUrl;
        this.jdbcClass = jdbcClass;
        this.user = user;
        this.pass = pass;
        
        try {
            Class.forName(jdbcClass);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            throw new SQLException("could not initialize driver : "+e);
        }

        // Copied from Vladimir's apache common's Pool Setup code '
        // in Replica DB
//       final ObjectPool connectionPool = new GenericObjectPool(null);
//       GenericObjectPool(PoolableObjectFactory factory,
//                         int maxActive,
//                         byte whenExhaustedAction,
//                         long maxWait,
//                         int maxIdle,
//                         int minIdle,
//                         boolean testOnBorrow,
//                         boolean testOnReturn,
//                         long timeBetweenEvictionRunsMillis,
//                         int numTestsPerEvictionRun,
//                         long minEvictableIdleTimeMillis,
//                         boolean testWhileIdle,
//                         long softMinEvictableIdleTimeMillis)
        final ObjectPool connectionPool = new GenericObjectPool(null,
                         10,
                         GenericObjectPool.WHEN_EXHAUSTED_GROW,
                         0, // Ignored because GenericObjectPool.WHEN_EXHAUSTED_GROW
                         8,
                         4,
                         true,
                         false,
                         600000,
                         2,
                         300000,
                         true,
                         300000);


        
        final ConnectionFactory connectionFactory = new DriverManagerConnectionFactory(jdbcUrl, user, pass);

        // Create InvocationHandler
        InvocationHandler retryHandler = new DMCFRetryProxyHandler(connectionFactory, 3600);

        // Create Proxy
        ConnectionFactory proxyConnectionFactory =
                (ConnectionFactory)Proxy.newProxyInstance(connectionFactory.getClass().getClassLoader(),
                                                          connectionFactory.getClass().getInterfaces(),
                                                          retryHandler);

        // PoolableConnectionFactory(     ConnectionFactory connFactory,
        //                                ObjectPool pool,
        //                                KeyedObjectPoolFactory stmtPoolFactory,
        //                                String validationQuery,
        //                                boolean defaultReadOnly,
        //                                boolean defaultAutoCommit)
        final PoolableConnectionFactory poolableConnectionFactory = 
            new PoolableConnectionFactory(proxyConnectionFactory, 
                                          connectionPool,
                                          new StackKeyedObjectPoolFactory(), // null,
                                          "select current_date", 
                                          false, 
                                          true);
        dataSource = new PoolingDataSource(connectionPool);
        
    if( _logSql.isDebugEnabled() ) {
        _logSql.debug("getMaxActive()="+
                ((GenericObjectPool) connectionPool).getMaxActive());
        _logSql.debug("getMaxIdle()="+
                ((GenericObjectPool) connectionPool).getMaxIdle());
        _logSql.debug("getMaxWait()="+
                ((GenericObjectPool) connectionPool).getMaxWait());
        _logSql.debug("getMinEvictableIdleTimeMillis()="+
                ((GenericObjectPool) connectionPool).getMinEvictableIdleTimeMillis());
        _logSql.debug("getMinIdle()="+
                ((GenericObjectPool) connectionPool).getMinIdle());
        _logSql.debug("getWhenExhaustedAction()="+
                ((GenericObjectPool) connectionPool).getWhenExhaustedAction());
    }   
        
    }
    
    
    /**
     * we use getConnection and return connection to assure
     * that the same connection is not used to do more then one thing at a time
     */
    public  Connection getConnection() throws SQLException {
        Connection con = dataSource.getConnection();
        con.setAutoCommit(false);
        return con;
    }
    
    public void returnFailedConnection(Connection _con) {
        long starttimestamp = System.currentTimeMillis();
        try {
            _con.rollback();
        }
        catch (SQLException sqle) {
            
        }
        
        try {
            _con.close();
        }
        catch (SQLException sqle) {
            if( _logSql.isDebugEnabled()) {
                _logSql.debug("returnFailedConnection() exception: ",sqle);
            }
            
        }
        
        long elapsed = System.currentTimeMillis()-starttimestamp;
        if( _logSql.isDebugEnabled() ) {
            _logSql.debug( "returnFailedConnection() took "+elapsed+" ms");
        }
        
    }
    
    public void returnConnection(Connection _con) {
        long starttimestamp = System.currentTimeMillis();
        try {
            _con.commit();
        }
        catch (SQLException sqle) {
            
        }
        try {
            _con.close();
        }
        catch (SQLException sqle) {
            if( _logSql.isDebugEnabled()) {
                _logSql.debug("returnConnection() exception: ",sqle);
            }
            
        }
        
        long elapsed = System.currentTimeMillis()-starttimestamp;
        if( _logSql.isDebugEnabled() ) {
            _logSql.debug( "returnConnection() took "+elapsed+" ms");
        }
    }
    
    public boolean equals(Object o) {
        if( this == o) {
            return true;
        }
        
        if(o == null || !(o instanceof JdbcConnectionPool)) {
            return false;
        }
        JdbcConnectionPool pool = (JdbcConnectionPool)o;
        return pool.jdbcClass.equals(jdbcClass) &&
        pool.jdbcUrl.equals(jdbcUrl) &&
        pool.pass.equals(pass) &&
        pool.user.equals(user) ;
    }
    
    public int hashCode() {
        return jdbcClass.hashCode() ^
        jdbcUrl.hashCode() ^
        pass.hashCode() ^
        user.hashCode() ;
    }
        
}
