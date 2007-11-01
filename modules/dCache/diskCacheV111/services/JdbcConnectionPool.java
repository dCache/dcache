/*
 * JdbcConnectionPool.java
 *
 * Created on June 21, 2004, 2:12 PM
 */

package diskCacheV111.services;
import java.sql.*;
import java.util.*;
import org.apache.log4j.Logger;
/**
 *
 * @author  timur
 */
public class JdbcConnectionPool {
    
    String jdbcUrl;
    String jdbcClass;
    String user;
    String pass;
    private static Logger _logSql = 
            Logger.getLogger(
            "logger.org.dcache.db.sql"+
            JdbcConnectionPool.class.getName());

    
    private static final HashSet pools = new HashSet();
    
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
    }
    
    
    private Set connections = new HashSet();
    private int max_connections = 50;
    private int max_connections_out = 50;
    private int connections_out=0;
    /**
     * we use getConnection and return connection to assure
     * that the same connection is not used to do more then one thing at a time
     */
    public  Connection getConnection() throws SQLException {
        long starttimestamp = System.currentTimeMillis();
        synchronized(connections) {
            while(connections_out >= max_connections_out) {
                _logSql.debug( "getConnection() : all connections taken, will wait");
                try {
                    connections.wait();
                }
                catch(InterruptedException ie) {
                }
            }
            
            connections_out++;
            //new Exception("connection given, connections_out="+connections_out+" thread is "+Thread.currentThread()).printStackTrace();
            
            
            if(connections.size() > 0) {
                
                Connection _con = (Connection)connections.iterator().next();
                connections.remove(_con);
                try {
                    if(!_con.isClosed()) {
                        long elapsed = System.currentTimeMillis()-starttimestamp;
                        if( _logSql.isDebugEnabled() ) {
                            _logSql.debug( "getConnection() took "+elapsed+" ms");
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
        if( _logSql.isDebugEnabled() ) {
            _logSql.debug( "getConnection() took "+elapsed+" ms");
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
                return;
            }
            catch(SQLException sqle) {
                if( _logSql.isDebugEnabled()) {
                    _logSql.debug("returnFailedConnection() exception: ",sqle);
                }
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
        
        synchronized(connections) {
            connections_out--;
            // new Exception("connection returned, connections_out="+connections_out+" thread is "+Thread.currentThread()).printStackTrace();
            connections.notifyAll();
            try {
                if(_con.isClosed()) {
                    long elapsed = System.currentTimeMillis()-starttimestamp;
                    if( _logSql.isDebugEnabled() ) {
                        _logSql.debug( "returnConnection() took "+elapsed+" ms");
                    }
                    return;
                }
                if(connections.size() >= max_connections) {
                    _con.close();
                    long elapsed = System.currentTimeMillis()-starttimestamp;
                    if( _logSql.isDebugEnabled() ) {
                        _logSql.debug( "returnConnection() took "+elapsed+" ms");
                    }
                    return;
                }
                
                connections.add(_con);
            }
            catch(SQLException sqle) {
                if( _logSql.isDebugEnabled()) {
                    _logSql.debug("returnConnection() exception: ",sqle);
                }
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
