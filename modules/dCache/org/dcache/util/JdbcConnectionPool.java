/*
 * JdbcConnectionPool.java
 *
 * Created on June 21, 2004, 2:12 PM
 */

package org.dcache.util;

import java.sql.*;
import java.util.*;
import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.pool.ObjectPool;
import org.apache.commons.pool.impl.GenericObjectPool;
import org.apache.commons.pool.impl.StackKeyedObjectPoolFactory;
import org.apache.commons.pool.KeyedObjectPoolFactory;
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
    /**
     * keep on increasing number of connections if needed
     */
    public static final byte WHEN_EXHAUSTED_GROW = GenericObjectPool.WHEN_EXHAUSTED_GROW;
    /**
     * block in the getConnection, if maxActive number of connections is given
     */
    public static final byte WHEN_EXHAUSTED_BLOCK = GenericObjectPool.WHEN_EXHAUSTED_BLOCK;
    
    private final String jdbcUrl;
    private final String jdbcClass;
    private final String user;
    private final String pass;
    private final GenericObjectPool connectionPool;
    private final DataSource dataSource;
    private static Logger _logSql = 
            LoggerFactory.getLogger(
            "logger.org.dcache.db.sql."+
            JdbcConnectionPool.class.getName());

    
    private static final HashSet pools = new HashSet();
    
    /**
     * DataSource should not be closed
     * @param jdbcUrl URL of the Database
     * @param jdbcClass JDBC Driver class name
     * @param user Database user
     * @param pass Database password
     * @throws java.sql.SQLException if the underlying jdbc code throws exception
     * @return DataSource
     */
    public synchronized static final DataSource getDataSource(
    String jdbcUrl,
    String jdbcClass,
    String user,
    String pass) throws SQLException {
        return getDataSource(jdbcUrl,
            jdbcClass, 
            user, 
            pass,
            10,
            WHEN_EXHAUSTED_GROW,
            0, 
            8,
            true);
    }
    
    /**
     * 
     * @param jdbcUrl URL of the Database
     * @param jdbcClass JDBC Driver class name
     * @param user Database user
     * @param pass Database password
     * @param maxActive sets the cap on the total number of active instances 
     *                  from the pool.
     *                  Use a negative value for no limit.
     * @param whenExhaustedAction <code>WHEN_EXHAUSTED_GROW</code> or
     *                            <code>WHEN_EXHAUSTED_BLOCK</code>
     * @param maxWait see <code> setMaxWait </code> method 
     * @param maxIdle max number of Idle connections, Use a negative value to 
     *                indicate an unlimited number of idle instances.
     * @throws java.sql.SQLException if the underlying jdbc code throws exception
     * @return DataSource for the connection pool with indicated parameters
     */
    public synchronized static final DataSource getDataSource(
    String jdbcUrl,
    String jdbcClass,
    String user,
    String pass,
    int maxActive,
    byte whenExhaustedAction,
    long maxWait,
    int maxIdle,
    boolean poolPreparedStatements ) throws SQLException {
        JdbcConnectionPool pool = 
            getPool(jdbcUrl,
            jdbcClass, 
            user, 
            pass,
            maxActive,
            whenExhaustedAction,
            maxWait, 
            maxIdle,
            poolPreparedStatements);
        return pool.dataSource;
    }

    /**
     * gets existing or creates a new JdbcConnectionPool
     * @param jdbcUrl URL of the Database
     * @param jdbcClass JDBC Driver class name
     * @param user Database user
     * @param pass Database password
     * @throws java.sql.SQLException if the underlying jdbc code throws exception
     * @return JdbcConnectionPool  with indicated parameters
     */
    public synchronized static final JdbcConnectionPool getPool(String jdbcUrl,
    String jdbcClass,
    String user,
    String pass) throws SQLException {
        return getPool(jdbcUrl,
            jdbcClass,
            user,
            pass,            
            10,
            WHEN_EXHAUSTED_GROW,
            0, 
            8,
            true);
    }

    /**
     * gets existing or creates a new JdbcConnectionPool
     * @param jdbcUrl URL of the Database
     * @param jdbcClass JDBC Driver class name
     * @param user Database user
     * @param pass Database password
     * @param maxActive sets the cap on the total number of active instances 
     *                  from the pool.
     *                  Use a negative value for no limit.
     * @param whenExhaustedAction <code>WHEN_EXHAUSTED_GROW</code> or
     *                            <code>WHEN_EXHAUSTED_BLOCK</code>
     * @param maxWait see <code> setMaxWait </code> method 
     * @param maxIdle max number of Idle connections, Use a negative value to 
     *                indicate an unlimited number of idle instances.
     * @param poolPreparedStatements if true, prepared statement pooling will 
     * be enabled
     * @throws java.sql.SQLException if the underlying jdbc code throws exception
     * @return JdbcConnectionPool with indicated parameters
     */
    public synchronized static final JdbcConnectionPool getPool(String jdbcUrl,
    String jdbcClass,
    String user,
    String pass,
    int maxActive,
    byte whenExhaustedAction,
    long maxWait,
    int maxIdle,
    boolean poolPreparedStatements ) throws SQLException {
        if(pass == null) pass="";
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
        JdbcConnectionPool pool = 
            new JdbcConnectionPool(
            jdbcUrl,
            jdbcClass,
            user,
            pass,
            maxActive,
            whenExhaustedAction,
            maxWait, 
            maxIdle,
            poolPreparedStatements);

        pools.add(pool);
        long elapsed = System.currentTimeMillis()-starttimestamp;
        if( _logSql.isDebugEnabled() ) {
            _logSql.debug( "getPool() took "+elapsed+" ms");
        }

        return pool;
        
    }
    
    /**
     * Creates a new instance of ResuestsPropertyStorage
     * @param jdbcUrl URL of the Database
     * @param jdbcClass JDBC Driver class name
     * @param user Database user
     * @param pass Database password
     * @param maxActive sets the cap on the total number of active instances 
     *                  from the pool.
     *                  Use a negative value for no limit.
     * @param whenExhaustedAction <code>WHEN_EXHAUSTED_GROW</code> or
     *                            <code>WHEN_EXHAUSTED_BLOCK</code>
     * @param maxWait see <code> setMaxWait </code> method 
     * @param maxIdle max number of Idle connections, Use a negative value to 
     *                indicate an unlimited number of idle instances.
     * @throws java.sql.SQLException 
     */
    protected JdbcConnectionPool(  String jdbcUrl,
    String jdbcClass,
    String user,
    String pass,
    int maxActive,
    byte whenExhaustedAction,
    long maxWait,
    int maxIdle,
    boolean poolPreparedStatements
    ) throws SQLException {
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
        if(whenExhaustedAction != WHEN_EXHAUSTED_GROW && whenExhaustedAction != WHEN_EXHAUSTED_BLOCK) {
            throw new IllegalArgumentException("Illegal whenExhaustedAction value = "+whenExhaustedAction+
                "; should be either JdbcConnectionPool.WHEN_EXHAUSTED_GROW or JdbcConnectionPool.WHEN_EXHAUSTED_BLOCK");
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
        connectionPool = new GenericObjectPool(null,
                         maxActive,
                         whenExhaustedAction,
                         maxWait, // Ignored if GenericObjectPool.WHEN_EXHAUSTED_GROW
                         maxIdle,
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
        KeyedObjectPoolFactory stmtPoolFactory =
            poolPreparedStatements? 
                new StackKeyedObjectPoolFactory():
                null;
            
        final PoolableConnectionFactory poolableConnectionFactory = 
            new PoolableConnectionFactory(proxyConnectionFactory, 
                                          connectionPool,
                                          stmtPoolFactory, 
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
     * @throws java.sql.SQLException 
     * @return 
     */
    public  Connection getConnection() throws SQLException {
        
        Connection con = dataSource.getConnection();
        con.setAutoCommit(false);
        return con;
    }
    
    /**
     * 
     * @param _con 
     */
    public void returnFailedConnection(Connection _con) {
        if( _logSql.isDebugEnabled()) {
            _logSql.debug("returnConnection() : "+_con);
        }
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
    
    /**
     * should be called every time the connection is not in use anymore
     * @param _con Connection that is not in use anymore
     */
    public void returnConnection(Connection _con) {
        if( _logSql.isDebugEnabled()) {
            _logSql.debug("returnConnection() :"+_con);
        }
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
    
    /**
     * 
     * @param o 
     * @return 
     */
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
        pool.user.equals(user) &&
        pool.getMaxActive() == getMaxActive() &&
        pool.getWhenExhaustedAction() == getWhenExhaustedAction() &&
        pool.getMaxIdle() == getMaxIdle() &&
        pool.getMaxWait() == getMaxWait();
    }
    
    /**
     * 
     * @return 
     */
    public int hashCode() {
        return jdbcClass.hashCode() ^
            jdbcUrl.hashCode()      ^
            pass.hashCode()         ^
            user.hashCode()         ^
            getMaxActive()          ^
            getMaxIdle()            ^
            (int)getMaxWait()       ^
            (int)getWhenExhaustedAction();
    }
    
    /**
     * Gets the cap on the total number of active instances from the pool.
     * @return the cap on the total number of active instances from the pool.
     */
    public int getMaxActive() {
        return connectionPool.getMaxActive();
    }
    
    /**
     * Sets the cap on the total number of active instances from the pool.
     * Use a negative value for no limit.
     * @param maxActive  
     */
    public void setMaxActive(int maxActive) {
        connectionPool.setMaxActive(maxActive);
    }
        
    /**
     * 
     * @return WhenExhaustedAction 
     * (<code>WHEN_EXHAUSTED_GROW</code> or <code>WHEN_EXHAUSTED_BLOCK</code>)
     */
    public byte  getWhenExhaustedAction() {
        return connectionPool.getWhenExhaustedAction();
    }
     /**
      * sets WhenExhaustedAction.
      * @param whenExhaustedAction new action
      * @throws IllegalArgumentException is  argument is not 
      * <code>WHEN_EXHAUSTED_GROW</code> or <code>WHEN_EXHAUSTED_BLOCK</code>
      */
   public void setWhenExhaustedAction(byte whenExhaustedAction) {
        if(whenExhaustedAction != WHEN_EXHAUSTED_GROW && whenExhaustedAction != WHEN_EXHAUSTED_BLOCK) {
            throw new IllegalArgumentException("Illegal whenExhaustedAction value = "+whenExhaustedAction+
                "; should be either JdbcConnectionPool.WHEN_EXHAUSTED_GROW or JdbcConnectionPool.WHEN_EXHAUSTED_BLOCK");
        }
        connectionPool.setWhenExhaustedAction(whenExhaustedAction);
    }
   
    /**
     * gets max number of idle connections
     * @return Max number of Idle connections
     */
    public int  getMaxIdle() {
        return connectionPool.getMaxIdle();
    }
    
    /**
     * Sets max number of Idle connections, Use a negative value to indicate an unlimited number of idle instances.
     * @param maxIdle 
     */
    public void setMaxIdle(int maxIdle) {
        connectionPool.setMaxIdle(maxIdle);
    }
    
    /**
     * gets Max Wait Time
     * @return Max Wait Time (in milliseconds) 
     */
    public long  getMaxWait() {
        return connectionPool.getMaxWait();
    }
    
    /**
     * Set Max Wait Time (in milliseconds)the <code> getConnection </code> 
     * method should block before throwing an exception when the pool is 
     * exhausted and the "when exhausted" action is <code> WHEN_EXHAUSTED_BLOCK</code>. 
     *  When less than or equal to 0, the  <code> getConnection </code>  method
     * may block indefinitely.
     * @param maxWait 
     */
    public void setMaxWait(Long maxWait) {
        connectionPool.setMaxWait(maxWait);
    }
}
