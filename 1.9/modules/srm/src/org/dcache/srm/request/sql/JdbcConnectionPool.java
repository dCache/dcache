/*
 * JdbcConnectionPool.java
 *
 * Created on June 21, 2004, 2:12 PM
 */

package org.dcache.srm.request.sql;
import java.sql.*;
import java.util.*;
import org.apache.log4j.Logger;

/**
 *
 * @author  timur
 */
public class JdbcConnectionPool implements Runnable{
    
    String jdbcUrl;
    String jdbcClass;
    String user;
    String pass;
    
    private static HashSet pools = new HashSet();
    
    private Thread[] execution_threads;
    private List jdbcTasks = new LinkedList();
    
    private Thread vacuum_thread;
    private long vacuum_period=60*60*1000;//every hour
    private static int executionThreadNum=5;
    private static int maxJdbcTasksNum = 1000 ;
    
    private static Logger _logSql = 
            Logger.getLogger(
            "logger.org.dcache.db.sql"+
            JdbcConnectionPool.class.getName());

    
    
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
        startExecutionThreads();
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
                long elapsed = System.currentTimeMillis()-starttimestamp;
                if( _logSql.isDebugEnabled() ) {
                    _logSql.debug( "returnFailedConnection() took "+elapsed+" ms");
                }
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
                //esay(sqle);
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

    
    public void startVacuumThread(long vacuum_period){
            if(vacuum_thread != null)
            {
                return;
            }
            if(vacuum_period >0) {
                this.vacuum_period = vacuum_period;
            }
            vacuum_thread = new Thread (this);
            vacuum_thread.start();
    }
    public void startExecutionThreads(){
            if(execution_threads != null)
            {
                return;
            }
            execution_threads = new Thread[executionThreadNum];
            for(int i = 0; i<execution_threads.length; ++i){
               execution_threads[i] =  new Thread (this);
               execution_threads[i].start();
            }
            
    }
    
    public void vacuum() {
        try
        {
            Connection _con  = DriverManager.getConnection(jdbcUrl, user, pass);
            while(true) {
                Thread.sleep(vacuum_period);
                Statement sqlStatement = _con.createStatement();
                System.out.println("executing statement: VACUUM ANALYZE");
                sqlStatement.execute("VACUUM ANALYZE");
                System.out.println("finished statement: VACUUM ANALYZE");

            }
        }
        catch(InterruptedException ie){
            vacuum_thread = null;
            ie.printStackTrace();
            System.err.println("quiting posgres vacuuming thread");
        }
        catch(SQLException sqle) {
           sqle.printStackTrace();
           vacuum_thread = null;
           System.err.println("quiting posgres vacuuming thread");
        }
    }
    
    public void execution_thread_loop() {
        while (true) {
            try{
                JdbcTask nextTask;
                synchronized(jdbcTasks) {
                    while(jdbcTasks.isEmpty()) {
                        jdbcTasks.wait(10000);
                    } 

                    nextTask  = (JdbcTask)jdbcTasks.remove(0);
                }
                Connection _con =null;
                try {
                    _con = getConnection();
                    nextTask.execute(_con);
                    returnConnection(_con);
                    _con = null;
                }
                catch(SQLException sqle) {
                    sqle.printStackTrace();
                    if(_con != null) {
                        returnFailedConnection(_con);
                        _con = null;
                    }

                    throw sqle;
                }
                finally {
                    if(_con != null) {
                        returnConnection(_con);
                    }
                }
            }
            catch(InterruptedException ie) {
                ie.printStackTrace();
                System.err.println("jdbc execution thread interrupted, quitting");
                return;
            }
            catch(Exception e) {
                e.printStackTrace();
            }
            catch(Throwable t){
                t.printStackTrace();
                System.err.println("fatal failure in jdbc execution thread, quitting");
                return;
            }
        }
    }
    
    public void run() {
        Thread current = Thread.currentThread();
        if (  current == vacuum_thread) {
            vacuum();
        }
        else {
             execution_thread_loop();
        }
    }
    
    public static interface JdbcTask {
        public void execute(Connection connection) throws SQLException ;
    }
    
    public void execute(JdbcTask task) throws SQLException {
        if(task == null) {
            return;
        }
        long jdbcTasksSize;
        synchronized (jdbcTasks) {
            jdbcTasksSize = jdbcTasks.size();
            if( jdbcTasks.size() <= maxJdbcTasksNum) {
                jdbcTasks.add(task);
                jdbcTasks.notifyAll();
                return;
            }
        }
        System.err.println("Execution of JdbcTask failed, jdbcTaskQueue is too long:"+jdbcTasksSize+
                " task is:"+task.toString());
        throw new SQLException("jdbcTaskQueue is too long:"+jdbcTasksSize);
        
    }

    public static int getExecutionThreadNum() {
        return executionThreadNum;
    }

    public static void setExecutionThreadNum(int aExecutionThreadNum) {
        executionThreadNum = aExecutionThreadNum;
    }

    public static int getMaxQueuedJdbcTasksNum() {
        return maxJdbcTasksNum;
    }

    public static void setMaxQueuedJdbcTasksNum(int aMaxJdbcTasksNum) {
        maxJdbcTasksNum = aMaxJdbcTasksNum;
    }
    
    public static final void main(final String[] args) throws Exception {
        if(args == null || args.length <5) {
            System.err.println("Usage: java org.dcache.srm.request.sql.JdbcConnectionPool "+
                    " jdbcUrl jdbcClass user pass <SQL Statement1> [<SQL Statement2> ...[<SQL StatementN>]]");
            System.exit(1);
        }
        JdbcConnectionPool pool = JdbcConnectionPool.getPool(args[0],
                args[1],
                args[2],
                args[3]);
        for(int i = 4; i <args.length ; ++i) {
            final String updateStatementString = args[i];
            JdbcConnectionPool.JdbcTask task = 
                    new JdbcConnectionPool.JdbcTask() {
             public void say(String s){
                 System.out.println(s);
             }
             
             public void esay(String s){
                 System.err.println(s);
             }
             
             public void esay(Throwable t){
                 t.printStackTrace();
             }
             
             public void execute(Connection connection) throws SQLException {
                int result = 0;
                try {
                    say(" executing statement: "+updateStatementString);

                    Statement sqlStatement = connection.createStatement();
                    result = sqlStatement.executeUpdate( updateStatementString );
                    say("executeUpdate result is "+result);
                    sqlStatement.close();
                    connection.commit();
                }
                catch(SQLException sqle) {
                    esay("execution of " +updateStatementString+" failed with ");
                    esay(sqle);
                    try {
                        connection.rollback();
                    }
                    catch(SQLException sqle1) {
                    }
                }
            }
            };
            try {
                pool.execute(task);
            } catch (SQLException sqle) {
                //ignore the saving errors, this will affect monitoring and
                // future status updates, but is not critical
            }
            
        }
        
    }
    
}

