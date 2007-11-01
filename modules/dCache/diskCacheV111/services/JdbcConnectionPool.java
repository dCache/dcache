/*
 * JdbcConnectionPool.java
 *
 * Created on June 21, 2004, 2:12 PM
 */

package diskCacheV111.services;
import java.sql.*;
import java.util.*;
/**
 *
 * @author  timur
 */
public class JdbcConnectionPool {
    
    String jdbcUrl;
    String jdbcClass;
    String user;
    String pass;
    
    public static HashSet pools = new HashSet();
    
    public synchronized static final JdbcConnectionPool getPool(String jdbcUrl,
    String jdbcClass,
    String user,
    String pass) throws SQLException {
        for (Iterator i = pools.iterator();
        i.hasNext();) {
            JdbcConnectionPool pool = (JdbcConnectionPool) i.next();
            if(pool.jdbcClass.equals(jdbcClass) &&
            pool.jdbcUrl.equals(jdbcUrl) &&
            pool.pass.equals(pass) &&
            pool.user.equals(user) ) {
                return pool;
            }
        }
        JdbcConnectionPool pool = new JdbcConnectionPool(jdbcUrl,jdbcClass,user,pass);
        pools.add(pool);
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
    
    
    private Set connections = new HashSet();
    private int max_connections = 10;
    private int max_connections_out = 10;
    private int connections_out=0;
    /**
     * we use getConnection and return connection to assure
     * that the same connection is not used to do more then one thing at a time
     */
    public  Connection getConnection() throws SQLException {
        synchronized(connections) {
            while(connections_out >= max_connections_out) {
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
        return _con;
    }
    
    public void returnFailedConnection(Connection _con) {
        synchronized(connections) {
            connections_out--;
            //new Exception("failed connection returned, connections_out="+connections_out+" thread is "+Thread.currentThread()).printStackTrace();
            connections.notify();
            try {
                _con.close();
                return;
            }
            catch(SQLException sqle) {
                //esay(sqle);
            }
        }
        
    }
    
    public void returnConnection(Connection _con) {
        synchronized(connections) {
            connections_out--;
            // new Exception("connection returned, connections_out="+connections_out+" thread is "+Thread.currentThread()).printStackTrace();
            connections.notify();
            try {
                if(_con.isClosed()) {
                    return;
                }
                if(connections.size() >= max_connections) {
                    _con.close();
                    return;
                }
                
                connections.add(_con);
            }
            catch(SQLException sqle) {
                //esay(sqle);
            }
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
