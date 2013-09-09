/*
 * JdbcConnectionPool.java
 *
 * Created on June 21, 2004, 2:12 PM
 */

package org.dcache.srm.request.sql;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 *
 * @author  timur
 */
public class JdbcConnectionPool
{
    String jdbcUrl;
    String jdbcClass;
    String user;
    String pass;

    private static Collection<JdbcConnectionPool> pools = new HashSet<>();

    private static final Logger _log =
        LoggerFactory.getLogger(JdbcConnectionPool.class);

    public synchronized static final JdbcConnectionPool getPool(String jdbcUrl,
    String jdbcClass,
    String user,
    String pass) throws SQLException {
        long starttimestamp = System.currentTimeMillis();
        for (Object pool1 : pools) {
            JdbcConnectionPool pool = (JdbcConnectionPool) pool1;
            if (pool.jdbcClass.equals(jdbcClass) &&
                    pool.jdbcUrl.equals(jdbcUrl) &&
                    pool.pass.equals(pass) &&
                    pool.user.equals(user)) {
                long elapsed = System.currentTimeMillis() - starttimestamp;
                if (_log.isDebugEnabled()) {
                    _log.debug("getPool() took " + elapsed + " ms");
                }
                return pool;
            }
        }
        JdbcConnectionPool pool = new JdbcConnectionPool(jdbcUrl,jdbcClass,user,pass);
        pools.add(pool);
        long elapsed = System.currentTimeMillis()-starttimestamp;
        if( _log.isDebugEnabled() ) {
            _log.debug( "getPool() took "+elapsed+" ms");
        }
        return pool;

    }

    /** Creates a new instance of ResuestsPropertyStorage */
    protected JdbcConnectionPool(  String jdbcUrl,
    String jdbcClass,
    String user,
    String pass) throws SQLException {
        if(
        jdbcUrl == null ||
        jdbcClass == null ||
        user == null ||
        pass == null
        ) {
            throw new NullPointerException("all arguments must be non-null!!");
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
    }


    private final Set<Connection> connections = new HashSet<>();
    private int max_connections = 50;
    private int max_connections_out = 50;
    private int connections_out;
    /**
     * we use getConnection and return connection to assure
     * that the same connection is not used to do more then one thing at a time
     */
    public  Connection getConnection() throws SQLException {
        long starttimestamp = System.currentTimeMillis();
        synchronized(connections) {
            while(connections_out >= max_connections_out) {
                try {
                    connections.wait(1000);
                }
                catch(InterruptedException ie) {
                }
            }
            connections_out++;
            connections.notify();
            //new Exception("connection given, connections_out="+connections_out+" thread is "+Thread.currentThread()).printStackTrace();


            if(connections.size() > 0) {

                Connection _con = connections.iterator().next();
                connections.remove(_con);
                try {
                    if(!_con.isClosed()) {
                        long elapsed = System.currentTimeMillis()-starttimestamp;
                        if( _log.isDebugEnabled() ) {
                            _log.debug( "getConnection() took "+elapsed+" ms");
                        }
                        return _con;
                    }
                }
                catch(SQLException sqle) {
                    //esay(sqle);
                }
            }

        }
        Connection _con  = DriverManager.getConnection(jdbcUrl, user, pass);
        _con.setAutoCommit(false);
        long elapsed = System.currentTimeMillis()-starttimestamp;
        if( _log.isDebugEnabled() ) {
            _log.debug( "getConnection() took "+elapsed+" ms");
        }
        return _con;
    }

    public void returnFailedConnection(Connection _con) {

        long starttimestamp = System.currentTimeMillis();
        try {
            _con.rollback();
        }
        catch (SQLException sqle) {

        }

        synchronized(connections) {
            connections_out--;
            //new Exception("failed connection returned, connections_out="+connections_out+" thread is "+Thread.currentThread()).printStackTrace();
            connections.notifyAll();
            try {
                _con.close();
                long elapsed = System.currentTimeMillis()-starttimestamp;
                if( _log.isDebugEnabled() ) {
                    _log.debug( "returnFailedConnection() took "+elapsed+" ms");
                }
                return;
            }
            catch(SQLException sqle) {
                if( _log.isDebugEnabled()) {
                    _log.debug("returnFailedConnection() exception: ",sqle);
                }
            }
        }

        long elapsed = System.currentTimeMillis()-starttimestamp;
        if( _log.isDebugEnabled() ) {
            _log.debug( "returnFailedConnection() took "+elapsed+" ms");
        }
   }

    public void returnConnection(Connection _con) {

        long starttimestamp = System.currentTimeMillis();
        try {
            _con.commit();
        }
        catch (SQLException sqle) {

        }

        synchronized(connections) {
            connections_out--;
            // new Exception("connection returned, connections_out="+connections_out+" thread is "+Thread.currentThread()).printStackTrace();
            connections.notifyAll();
            try {
                if(_con.isClosed()) {
                    long elapsed = System.currentTimeMillis()-starttimestamp;
                    if( _log.isDebugEnabled() ) {
                        _log.debug( "returnConnection() took "+elapsed+" ms");
                    }
                    return;
                }
                if(connections.size() >= max_connections) {
                    _con.close();
                    long elapsed = System.currentTimeMillis()-starttimestamp;
                    if( _log.isDebugEnabled() ) {
                        _log.debug( "returnConnection() took "+elapsed+" ms");
                    }
                    return;
                }

                connections.add(_con);
            }
            catch(SQLException sqle) {
                //esay(sqle);
                if( _log.isDebugEnabled()) {
                    _log.debug("returnConnection() exception: ",sqle);
                }
            }
        }
        long elapsed = System.currentTimeMillis()-starttimestamp;
        if( _log.isDebugEnabled() ) {
            _log.debug( "returnConnection() took "+elapsed+" ms");
        }
    }

    @Override
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

    @Override
    public int hashCode() {
        return jdbcClass.hashCode() ^
        jdbcUrl.hashCode() ^
        pass.hashCode() ^
        user.hashCode() ;
    }
}

