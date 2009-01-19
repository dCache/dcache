// $Id$
// $Log: not supported by cvs2svn $

/*
COPYRIGHT STATUS:
  Dec 1st 2001, Fermi National Accelerator Laboratory (FNAL) documents and
  software are sponsored by the U.S. Department of Energy under Contract No.
  DE-AC02-76CH03000. Therefore, the U.S. Government retains a  world-wide
  non-exclusive, royalty-free license to publish or reproduce these documents
  and software for U.S. Government purposes.  All documents and software
  available from this server are protected under the U.S. and Foreign
  Copyright Laws, and FNAL reserves all rights.
 
 
 Distribution of the software available from this server is free of
 charge subject to the user following the terms of the Fermitools
 Software Legal Information.
 
 Redistribution and/or modification of the software shall be accompanied
 by the Fermitools Software Legal Information  (including the copyright
 notice).
 
 The user is asked to feed back problems, benefits, and/or suggestions
 about the software to the Fermilab Software Providers.
 
 
 Neither the name of Fermilab, the  URA, nor the names of the contributors
 may be used to endorse or promote products derived from this software
 without specific prior written permission.
 
 
 
  DISCLAIMER OF LIABILITY (BSD):
 
  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
  "AS IS" AND ANY EXPRESS OR IMPLIED  WARRANTIES, INCLUDING, BUT NOT
  LIMITED TO, THE IMPLIED  WARRANTIES OF MERCHANTABILITY AND FITNESS
  FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL FERMILAB,
  OR THE URA, OR THE U.S. DEPARTMENT of ENERGY, OR CONTRIBUTORS BE LIABLE
  FOR  ANY  DIRECT, INDIRECT,  INCIDENTAL, SPECIAL, EXEMPLARY, OR
  CONSEQUENTIAL DAMAGES  (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT
  OF SUBSTITUTE  GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR
  BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY  OF
  LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
  NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT  OF THE USE OF THIS
  SOFTWARE, EVEN IF ADVISED OF THE  POSSIBILITY OF SUCH DAMAGE.
 
 
  Liabilities of the Government:
 
  This software is provided by URA, independent from its Prime Contract
  with the U.S. Department of Energy. URA is acting independently from
  the Government and in its own private capacity and is not acting on
  behalf of the U.S. Government, nor as its contractor nor its agent.
  Correspondingly, it is understood and agreed that the U.S. Government
  has no connection to this software and in no manner whatsoever shall
  be liable for nor assume any responsibility or obligation for any claim,
  cost, or damages arising out of or resulting from the use of the software
  available from this server.
 
 
  Export Control:
 
  All documents and software available from this server are subject to U.S.
  export control laws.  Anyone downloading information from this server is
  obligated to secure any necessary Government licenses before exporting
  documents or software obtained from this server.
 */

/*
 * TestDatabaseJobStorage.java
 *
 * Created on April 26, 2004, 3:27 PM
 */

package org.dcache.srm.request.sql;
import java.sql.*;
import java.util.Set;
import java.util.HashSet;
import java.util.Iterator;
import org.dcache.srm.scheduler.Job;
import org.dcache.srm.scheduler.Job.JobHistory;
import org.dcache.srm.scheduler.JobStorage;
import org.dcache.srm.scheduler.State;
import org.dcache.srm.scheduler.Scheduler;
import org.dcache.srm.util.Configuration;
import org.dcache.srm.Logger;
import java.util.concurrent.Semaphore;
/**
 *
 * @author  timur
 */
public abstract class DatabaseJobStorage implements JobStorage, Runnable {
    
    protected static final String INDEX_SUFFIX="_idx";
    private boolean useJobsSet;
    private java.util.Set jobsSet = java.util.Collections.synchronizedSet(new java.util.HashSet());

    protected Configuration configuration;
    private final String jdbcUrl;
    private final String jdbcClass;
    private final String user;
    private final String pass;
    protected Logger logger;
    protected boolean logHistory;
    protected static final String stringType=" VARCHAR(32672) ";
    protected static final String longType=" BIGINT ";
    protected static final String intType=" INTEGER ";
    protected static final String dateTimeType= " TIMESTAMP ";
    protected static final String booleanType= " INT ";
    protected static final int stringType_int=java.sql.Types.VARCHAR;
    protected static final int longType_int=java.sql.Types.BIGINT;
    protected static final int intType_int=java.sql.Types.INTEGER;
    protected static final int dateTimeType_int= java.sql.Types.TIMESTAMP;
    protected static final int booleanType_int= java.sql.Types.INTEGER;
    public DatabaseJobStorage(      Configuration configuration) 
    throws SQLException {
        this.configuration = configuration;
        
        this.jdbcUrl = configuration.getJdbcUrl();
        this.jdbcClass = configuration.getJdbcClass();
        this.user = configuration.getJdbcUser();
        this.pass = configuration.getJdbcPass();
        this.logger = configuration.getStorage();
        this.logHistory = configuration.isJdbcMonitoringEnabled();
        
        dbInit(configuration.isCleanPendingRequestsOnRestart());
        //updatePendingJobs();
        new Thread(this,"update"+getTableName()).start();
    }
    
    public void say(String s){
        if(logger != null) {
           logger.log(" DatabaseJobStorage: "+s);
        }
    }
    
    public void esay(String s){
        if(logger != null) {
           logger.elog(" DatabaseJobStorage: "+s);
        }
    }
    
    public void esay(Throwable t){
        if(logger != null) {
           logger.elog(t);
        }
    }
 
   
    public static final String createFileRequestTablePrefix =
    "ID "+         longType+" NOT NULL PRIMARY KEY"+
    ","+
    "NEXTJOBID "+           longType+
    ","+
    "CREATIONTIME "+        longType
    +","+
    "LIFETIME "+            longType
    +","+
    "STATE "+               intType
    +","+
    "ERRORMESSAGE "+        stringType+
    ","+
    "SCHEDULERID "+         stringType+
    ","+
    "SCHEDULERTIMESTAMP "+  longType+
    ","+
    "NUMOFRETR "+  longType+
    ","+
    "MAXNUMOFRETR "+  longType+
    ","+
    "LASTSTATETRANSITIONTIME"+ longType;

    public static final String srmStateTableName = 
    "SRMJOBSTATE";
    public static final String createStateTable =
    "CREATE TABLE "+srmStateTableName+" ( "+
    "ID "+   longType+" NOT NULL PRIMARY KEY"+
    ","+
    "STATE "+           stringType+
     " )";
    
    public static final String createHistroyTablePrefix =
    "ID "+         longType+" NOT NULL PRIMARY KEY"+
    ","+
    "JOBID "+         longType+
    ","+
    "STATEID "+           longType+
    ","+
    "TRANSITIONTIME "+        longType
    +","+
    "DESCRIPTION "+            stringType;
    
    //this should always reflect the number of field definde in the 
    // prefix above
    private static int COLLUMNS_NUM= 11;
    public abstract String getTableName();
    
    public abstract String getCreateTableFields();
    
    JdbcConnectionPool pool;
    /**
     * in case the subclass needs to create/initialize more tables
     */
    protected abstract void _dbInit() throws SQLException;
    
    protected boolean reanamed_old_table = false;
    private String getHistoryTableName() {
        return getTableName().toLowerCase()+"history";
    }
    
    private void dbInit(boolean clean)
    throws SQLException {
        createTable(srmStateTableName, createStateTable);
        insertStates();
        String tableName = getTableName().toLowerCase();
        String createStatement = "CREATE TABLE " + tableName + "(" +
                    createFileRequestTablePrefix +getCreateTableFields()+", "+
       " CONSTRAINT fk_"+tableName+"_ST FOREIGN KEY (STATE) REFERENCES "+
        srmStateTableName +" (ID) "+
        " )";
        createTable(tableName,createStatement,true,clean);
        String historyTableName = getHistoryTableName();
        if(reanamed_old_table) {
            try{
                renameTable(historyTableName);
            }catch(SQLException sqle)
            {
                //ignore since table might not have existed yet
            }
        }
        String createHistoryTable = "CREATE TABLE "+ historyTableName+" ( "+
            createHistroyTablePrefix+", "+
            " CONSTRAINT fk_"+tableName+"_HI FOREIGN KEY (JOBID) REFERENCES "+
            tableName +" (ID) "+
            " ON DELETE CASCADE"+
            " )";
            createTable(historyTableName, createHistoryTable);
	    //
	    // create indexes (litvinse@fnal.gov), some hack
	    //
	    String columns[] = {
		    "NEXTJOBID",
		    "CREATIONTIME",
		    "STATE",
		    "SCHEDULERID"};
	    createIndex(columns, getTableName().toLowerCase());
	    

	    String history_columns[] = {
		    "STATEID",
		    "TRANSITIONTIME", 
	            "JOBID"};
	    createIndex(history_columns,getHistoryTableName().toLowerCase());
	    _dbInit();
    }
    
    private void insertStates() throws  SQLException{
       Connection _con =null;

        try {
            _con = pool.getConnection();
            State[] states = State.getAllStates();
            String countStates = "SELECT count(*) from "+srmStateTableName;
            Statement sqlStatement = _con.createStatement();
            ResultSet rs = sqlStatement.executeQuery( countStates);
            if(rs.next()){
                int count = rs.getInt(1);
                sqlStatement.close();
                if(count != states.length) {
                    for(int i = 0; i<states.length; ++i) {

                        String insertState = "INSERT INTO "+
                         srmStateTableName+" VALUES ("+
                         states[i].getStateId()+
                         ", '"+states[i].toString()+"' )";
                        say("executing statement: "+insertState);
                        try{
                            sqlStatement = _con.createStatement();
                            int result = sqlStatement.executeUpdate( insertState );
                            sqlStatement.close();
                            _con.commit();
                        }
                        catch(SQLException sqle) {
                            //ignoring, state might be already in the table
                            esay(sqle);
                        }
                    }
                }
            }
            else {
                sqlStatement.close();
            }
        }
        catch(SQLException sqle) {
            if(_con != null) {
                pool.returnFailedConnection(_con);
                _con = null;
            }

            throw sqle;
        }
        finally {
            if(_con != null) {
                pool.returnConnection(_con);
            }
        }
    }
    
    protected abstract Job getJob(
    Connection _con,
    Long ID,
    Long NEXTJOBID ,
    long CREATIONTIME,
    long LIFETIME,
    int STATE,
    String ERRORMESSAGE,
    String SCHEDULERID,
    long SCHEDULER_TIMESTAMP,
    int NUMOFRETR,
    int MAXNUMOFRETR,
    long LASTSTATETRANSITIONTIME,
    ResultSet set,
    int next_index) throws SQLException;
    
    public Job getJob(Long jobId) throws SQLException {
        
           if(jobId == null) {

               throw new NullPointerException ("jobId is null");
           }
           
           Connection _con =null;

            try {
                _con = pool.getConnection();
                Job job = getJob(jobId,_con);
                pool.returnConnection(_con);
                _con = null;
                updateJobsSet(job);
                return job;
            }
            catch(SQLException sqle) {
                if(_con != null) {
                    pool.returnFailedConnection(_con);
                    _con = null;
                }
                
                throw sqle;
            }
            finally {
                if(_con != null) {
                    pool.returnConnection(_con);
                }
            }
    }

       public Job getJob(Long jobId,Connection _con) throws SQLException {
       if(jobId == null) {
           throw new NullPointerException ("jobId is null");
       }
        Statement sqlStatement = _con.createStatement();
        String sqlStatementString = "SELECT * FROM " + getTableName() +
        " WHERE ID="+jobId;
        say("executing statement: "+sqlStatementString);
        ResultSet set = sqlStatement.executeQuery(sqlStatementString);
        if(!set.next()) {
            sqlStatement.close();
            return null;
        }
        
        Long ID = new Long(set.getLong(1));
        Long NEXTJOBID = new Long(set.getLong(2));
        long CREATIONTIME = set.getLong(3);
        long LIFETIME = set.getLong(4);
        int STATE = set.getInt(5);
        String ERRORMESSAGE = set.getString(6);
        String SCHEDULERID=set.getString(7);
        long SCHEDULER_TIMESTAMP=set.getLong(8);
        int NUMOFRETR = set.getInt(9);
        int MAXNUMOFRETR = set.getInt(10);
        long LASTSTATETRANSITIONTIME = set.getLong(11);
        Job job = getJob(_con,
        ID,
        NEXTJOBID ,
        CREATIONTIME,
        LIFETIME,
        STATE,
        ERRORMESSAGE,
        SCHEDULERID,
        SCHEDULER_TIMESTAMP,
        NUMOFRETR,
        MAXNUMOFRETR,
        LASTSTATETRANSITIONTIME,
        set,
        12 );
        sqlStatement.close();
        updateJobsSet(job);
        return job;
    }

    public abstract void getUpdateAssignements(Job job, StringBuffer sb);
    
    private void updateJobsSet(Job job) {
        if(job ==null) {
            return;
        }
        if(useJobsSet) {
            State state = job.getState();
            if(jobsSet.contains(job)  && State.isFinalState(state)) {
                say("removing job #"+job.getId() +" from hash set");
                jobsSet.remove(job);
            }
            if(!jobsSet.contains(job) && !State.isFinalState(state)) {
                say("putting job #"+job.getId() +" to hash set");
                jobsSet.add(job);
            }
                
        }
        
    }
    
    public String  getUpdateJobStatement(Job job) throws SQLException{
        
        StringBuffer sb = new StringBuffer();
        sb.append("UPDATE ").append( getTableName());
        sb.append(" SET ");
        Long tmpl =job.getNextJobId();
        if(tmpl == null) {
            sb.append(" NEXTJOBID =NULL, ");
        }
        else {
            sb.append("NEXTJOBID = ").append(tmpl).append(", ");
        }

        sb.append(" CREATIONTIME = ").append( job.getCreationTime()).append(",");
        sb.append(" LIFETIME = ").append(job.getLifetime()).append(",");
        sb.append(" STATE = ").append(job.getState().getStateId()).append(",");
        String tmp =fixStringForSQL(job.getErrorMessage());
        if(tmp == null) {
            sb.append(" ERRORMESSAGE =NULL, ");
        }
        else {
            sb.append("ERRORMESSAGE = '").append(tmp).append("', ");
        }
        tmp =job.getSchedulerId();
        if(tmp == null) {
            sb.append(" SCHEDULERID =NULL, ");
        }
        else {
            sb.append("SCHEDULERID = '").append(tmp).append("', ");
        }
        sb.append(" SCHEDULERTIMESTAMP = ").append(job.getSchedulerTimeStamp()).append(",");
        sb.append(" NUMOFRETR = ").append( job.getNumberOfRetries()).append(",");
        sb.append(" MAXNUMOFRETR = ").append( job.getMaxNumberOfRetries()).append(",");
        sb.append(" LASTSTATETRANSITIONTIME = ").append(job.getLastStateTransitionTime());
        getUpdateAssignements(job,sb);

        sb.append(" WHERE ID=").append(job.getId());
        return sb.toString();
    }
    
        
    public void saveJob(final Job job,boolean saveifmonitoringisdesabled) throws SQLException{
        // statements for the job itself
        if(!saveifmonitoringisdesabled && !logHistory) {
            return;
        }
        
        final String updateStatementString  = getUpdateJobStatement(job);
        final String createStatementString  = getCreateStatement(job);
        final String[] additionalCreateStatements = getAdditionalCreateStatements(job);
        final Semaphore lock = job.getLock(); 
        updateJobsSet(job);
        final long jobId = job.getId().longValue();
        //statements for the history elements
                String historyTableName = 
                getHistoryTableName();
        Iterator historyIterator = job.getHistoryIterator();
        final String[] historyInserts;
        if(logHistory) {
            Set historyInsertSet = new HashSet();
            while(historyIterator.hasNext()) {
                Job.JobHistory historyElement = 
                    (Job.JobHistory) historyIterator.next();
                if(historyElement.isSaved()) {
                    continue;
                }
                long id = historyElement.getId();
                String insert = "INSERT INTO "+historyTableName+
                " VALUES ("+id+", "+
                job.getId()+", "+
                historyElement.getState().getStateId()+", "+
                historyElement.getTransitionTime()+", "+
                ( historyElement.getDescription()==null?
                    "NULL":
                    ("'"+historyElement.getDescription()+"'"))+
                ")";
                historyInsertSet.add(insert);
                historyElement.setSaved();
            }
            historyInserts = (String[])historyInsertSet.toArray(
                new String[0]);
        } else {
            historyInserts = null;
        }
        JdbcConnectionPool.JdbcTask task = 
                new JdbcConnectionPool.JdbcTask() {
         public String toString() {
           return  "save Job and its history : "+ updateStatementString; 
         }
         
         public void innersay(String s) {
             say(" JdbcTask for Job "+jobId+" :"+s);
         }
         
         public void execute(Connection connection) throws SQLException {
            boolean locked = false;
            try {
                   lock.acquire();
                   locked = true;
   
            int result = 0;
            try {
                innersay(" executing statement: "+updateStatementString);
                
                Statement sqlStatement = connection.createStatement();
                result = sqlStatement.executeUpdate( updateStatementString );
                innersay(" executeUpdate result is "+result);
                sqlStatement.close();
                connection.commit();
            }
            catch(SQLException sqle) {
                esay("storage of job="+jobId+" failed with ");
                esay(sqle);
                try {
                    connection.rollback();
                }
                catch(SQLException sqle1) {
                }
            }
            if(result == 0) {
                innersay("update result is 0, calling createJob()");
                Statement sqlStatement1 = connection.createStatement();
                String sqlStatementString1 = "SELECT * FROM "  + getTableName();
                sqlStatementString1 +=" WHERE ID='"+jobId+"'";
                innersay("executing statement: "+sqlStatementString1);
                ResultSet set = sqlStatement1.executeQuery(sqlStatementString1);
                boolean shouldcreate = false;
                if(!set.next()) {
                    shouldcreate = true;
                   
                }
                
                set.close();
                sqlStatement1.close();
                if(shouldcreate)
                {
                    try {
                        Statement sqlStatement = connection.createStatement();
                        innersay("executing statement: "+createStatementString);
                        result = sqlStatement.executeUpdate( createStatementString );
                        sqlStatement.close();
                        if(additionalCreateStatements != null){
                            for(int i =0 ; i<additionalCreateStatements.length; ++i) {
                                innersay("executing statement: "+additionalCreateStatements[i]);
                                sqlStatement = connection.createStatement();
                                result = sqlStatement.executeUpdate( additionalCreateStatements[i] );
                                sqlStatement.close();
                            }
                        }
                        connection.commit();
                        
                    }
                    catch(SQLException sqle) {
                        esay("storageof job="+jobId+" failed with ");
                        esay(sqle);
                        connection.rollback();
                        throw sqle;
                    }

                }
                else
                {
                    esay("update result is 0, but the record exists, ignore, but the job state might not be saved correctly");
                }
            }
            if(historyInserts != null) {
                for(int i =0; i< historyInserts.length; ++i) {
                    innersay("executing statement: "+historyInserts[i]);
                    Statement statement = connection.createStatement();
                    statement.executeUpdate(historyInserts[i]);
                    statement.close();
                }
            }
        } catch (InterruptedException ie) {
            innersay("interrupted");
        } finally {
           if(locked) {
               lock.release();
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
    public abstract String[] getAdditionalCreateStatements(Job job) throws SQLException;
    
    public abstract void getCreateList(Job job, StringBuffer sb);
    
    private String getCreateStatement(Job job){
            StringBuffer sb = new StringBuffer();
            sb.append("INSERT INTO ").append( getTableName());
            sb.append( " VALUES ( ");
            sb.append(job.getId());
            sb.append(", ");
            
            Long tmpl =job.getNextJobId();
            if(tmpl == null) {
                sb.append("NULL, ");
            }
            else {
                sb.append(tmpl).append(", ");
            }
            sb.append(job.getCreationTime()).append(", ");
            sb.append(job.getLifetime()).append(", ");
            sb.append(job.getState().getStateId()).append(", ");
            String tmp =fixStringForSQL(job.getErrorMessage());
            if(tmp == null) {
                sb.append("NULL, ");
            }
            else {
                sb.append('\'').append(tmp).append("', ");
            }
            tmp =job.getSchedulerId();
            if(tmp == null) {
                sb.append("NULL, ");
            }
            else {
                sb.append('\'').append(tmp).append("', ");
            }
            
            sb.append(job.getSchedulerTimeStamp()).append(", ");
            sb.append(job.getNumberOfRetries()).append(", ");
            sb.append(job.getMaxNumberOfRetries()).append(", ");
            sb.append(job.getLastStateTransitionTime()).append(" ");
            getCreateList(job,sb);
            sb.append(" ) ");
            return sb.toString();
    }
    
     //
    // this method should be run only once by the scheduler itself
    // otherwise it is possible to create multiple inconsistant copies of the 
    // job
    private boolean getJobsRan=false;
    public java.util.Set getJobs(String schedulerId) throws SQLException{
        if(getJobsRan)
        {
            throw new SQLException("getJobs("+schedulerId+") has already run" );
        }
        getJobsRan = true;
        
        Set jobs = new java.util.HashSet();
        Connection _con =null;
        
        try {
            _con = pool.getConnection();
            Statement sqlStatement = _con.createStatement();
            String sqlStatementString = "SELECT * FROM " + getTableName() +
            " WHERE SCHEDULERID='"+schedulerId+"'";
            say("executing statement: "+sqlStatementString);
            ResultSet set = sqlStatement.executeQuery(sqlStatementString);
            while(set.next()) {
                Long ID = new Long(set.getLong(1));
                Long NEXTJOBID = new Long(set.getLong(2));
                //Date CREATIONTIME = set.getDate(3);
                long CREATIONTIME = set.getLong(3);
                long LIFETIME = set.getLong(4);
                int STATE = set.getInt(5);
                String ERRORMESSAGE = set.getString(6);
                String SCHEDULERID=set.getString(7);
                long SCHEDULER_TIMESTAMP=set.getLong(8);
                int NUMOFRETR = set.getInt(9);
                int MAXNUMOFRETR = set.getInt(10);
                long LASTSTATETRANSITIONTIME = set.getLong(11);
                State state = State.getState(STATE);
                Job job = getJob(
                _con,
                ID,
                NEXTJOBID ,
                CREATIONTIME,
                LIFETIME,
                STATE,
                ERRORMESSAGE,
                SCHEDULERID,
                SCHEDULER_TIMESTAMP,
                NUMOFRETR,
                MAXNUMOFRETR,
                LASTSTATETRANSITIONTIME,
                set,
                13 );
                
                say("==========> deserialization from database of job id"+job.getId());
                say("==========> jobs submitter id is "+job.getSubmitterId());
                jobs.add(job);
            }
            
            set.close();
            sqlStatement.close();
            pool.returnConnection(_con);
            _con = null;
            return jobs;
        }
        catch(SQLException sqle1) {
            if(_con != null) {
                pool.returnFailedConnection(_con);
                _con = null;
            }
            throw sqle1;
        }
        finally {
            if(_con != null) {
                pool.returnConnection(_con);
            }
        }
        
    }
    
    protected Job.JobHistory[] getJobHistory(Long jobId,Connection _con) throws SQLException{
        java.util.List l = new java.util.ArrayList();
        String select = "SELECT * FROM " +getHistoryTableName()+
        " WHERE JOBID="+jobId;
        say("executing statement: "+select);
        Statement statement = _con.createStatement();
        ResultSet set = statement.executeQuery(select);
        if(!set.next()) {
            say("no history elements in table "+getHistoryTableName() +" found, returning NULL");
            statement.close();
            return null;
        }
        
        do {
            long ID = set.getLong(1);
            long JOBID = set.getLong(2);
            int STATEID = set.getInt(3);
            long TRANSITIONTIME  = set.getLong(4);
            String DESCRIPTION  = set.getString(5);
            Job.JobHistory jh = new Job.JobHistory(ID,
                        State.getState(STATEID), 
                        DESCRIPTION, 
                        TRANSITIONTIME);
            jh.setSaved();
            l.add(jh);
            say("found JobHistory:"+jh.toString());

        } while (set.next());
        statement.close();
        return (Job.JobHistory[])l.toArray(new Job.JobHistory[0]);
    }
    
    private boolean updatePendingJobsRan=false;
      
    public void schedulePendingJobs(Scheduler scheduler) throws SQLException, InterruptedException,org.dcache.srm.scheduler.IllegalStateTransition{
        Connection _con =null;
        try {
            _con = pool.getConnection();
            Statement sqlStatement = _con.createStatement();
            String sqlStatementString = "SELECT ID FROM " + getTableName() +
            " WHERE SCHEDULERID is NULL and State="+State.PENDING.getStateId();
            say("executing statement: "+sqlStatementString);
            ResultSet set = sqlStatement.executeQuery(sqlStatementString);
            //save in the memory the ids to prevent the exhaust of the connections
            // so we return connections before trying to schedule the pending 
            // requests
            java.util.Set idsSet = new java.util.HashSet();
            while(set.next()) {
                idsSet.add(new Long(set.getLong(1)));
            }
            
            set.close();
            sqlStatement.close();
            pool.returnConnection(_con);
            _con = null;
            
            for(java.util.Iterator i =idsSet.iterator(); i.hasNext();)
            {   
                Long ID = (Long)i.next();
                Job job = Job.getJob(ID,
                
                _con);
                scheduler.schedule(job);
            }
        }
        catch(SQLException sqle1) {
            if(_con != null) {
                pool.returnFailedConnection(_con);
                _con = null;
            }
            throw sqle1;
        }
        finally {
            if(_con != null) {
                pool.returnConnection(_con);
            }
        }
    }

public void updatePendingJobs() throws SQLException, InterruptedException,org.dcache.srm.scheduler.IllegalStateTransition{
       if(updatePendingJobsRan)
        {
            throw new SQLException("updatePendingJobs() has already ran" );
        }
       updatePendingJobsRan = true;
        Connection _con =null;
        try {
            _con = pool.getConnection();
            Statement sqlStatement = _con.createStatement();
            String sqlStatementString = "SELECT ID FROM " + getTableName() +
            " WHERE SCHEDULERID is NULL and State="+State.PENDING.getStateId();
            say("executing statement: "+sqlStatementString);
            ResultSet set = sqlStatement.executeQuery(sqlStatementString);
            //save in the memory the ids to prevent the exhaust of the connections
            // so we return connections before trying to restore the pending 
            // requests
            java.util.Set idsSet = new java.util.HashSet();
            while(set.next()) {
                idsSet.add(new Long(set.getLong(1)));
            }
            
            set.close();
            sqlStatement.close();
            pool.returnConnection(_con);
            _con = null;
            for(java.util.Iterator i =idsSet.iterator(); i.hasNext();)
            {   
                Long ID = (Long)i.next();
                // we just restore the job, which will triger the experation of the job, if its lifetime
                // is over
                
                Job job = Job.getJob(ID,
                _con);
                
            }
        }
        catch(SQLException sqle1) {
            if(_con != null) {
                pool.returnFailedConnection(_con);
                _con = null;
            }
            throw sqle1;
        }
        finally {
            if(_con != null) {
                pool.returnConnection(_con);
            }
        }
    }
    // this method returns ids as a set of "Long" id
    protected java.util.Set<Long> getJobIdsByCondition(String sqlCondition) throws SQLException{
        Set<Long> jobIds = new java.util.HashSet();
        Connection _con =null;
        try {
            _con = pool.getConnection();
            Statement sqlStatement = _con.createStatement();
            String sqlStatementString = "SELECT ID FROM " + getTableName() +
            " WHERE "+sqlCondition+" ";
            say("executing statement: "+sqlStatementString);
            ResultSet set = sqlStatement.executeQuery(sqlStatementString);
            while(set.next()) {
                Long ID = new Long(set.getLong(1));
                jobIds.add(ID);
            }
            
            set.close();
            sqlStatement.close();
            pool.returnConnection(_con);
            _con = null;
            return jobIds;
        }
        catch(SQLException sqle1) {
            if(_con != null) {
                pool.returnFailedConnection(_con);
                _con = null;
            }
            throw sqle1;
        }
        finally {
            if(_con != null) {
                pool.returnConnection(_con);
            }
        }
        
    }
    
    public java.util.Set getJobs(String schedulerId, org.dcache.srm.scheduler.State state) throws SQLException {
        Set jobs = new java.util.HashSet();
        Connection _con =null;
        try {
            _con = pool.getConnection();
            Statement sqlStatement = _con.createStatement();
            String sqlStatementString = "SELECT * FROM " + getTableName();
            sqlStatementString += " WHERE SCHEDULERID='"+schedulerId+"' AND STATE='"+state+"' ";
            say("executing statement: "+sqlStatementString);
            ResultSet set = sqlStatement.executeQuery(sqlStatementString);
            while(set.next()) {
                Long ID = set.getLong(1);
                Long NEXTJOBID = set.getLong(2);
                //Date CREATIONDATE = set.getDate(3);
                long CREATIONTIME = set.getLong(3);
                long LIFETIME = set.getLong(4);
                int STATE = set.getInt(5);
                String ERRORMESSAGE = set.getString(6);
                String SCHEDULERID=set.getString(7);
                long SCHEDULER_TIMESTAMP=set.getLong(8);
                int NUMOFRETR = set.getInt(9);
                int MAXNUMOFRETR = set.getInt(10);
                long LASTSTATETRANSITIONTIME = set.getLong(11);
                State jobstate = State.getState(STATE);
                Job job = getJob(
                _con,
                ID,
                NEXTJOBID ,
                CREATIONTIME,
                LIFETIME,
                STATE,
                ERRORMESSAGE,
                SCHEDULERID,
                SCHEDULER_TIMESTAMP,
                NUMOFRETR,
                MAXNUMOFRETR,
                LASTSTATETRANSITIONTIME, 
                set,
                13 );
                say("==========> deserialization from database of "+job);
                say("==========> jobs creator is "+job.getSubmitterId());
                jobs.add(job);
            }
            
            set.close();
            sqlStatement.close();
            pool.returnConnection(_con);
            _con = null;
            return jobs;
        }
        catch(SQLException sqle1) {
            if(_con != null) {
                pool.returnFailedConnection(_con);
                _con = null;
            }
            throw sqle1;
        }
        finally {
            if(_con != null) {
                pool.returnConnection(_con);
            }
        }
    }
    
    /**
     * Getter for property useJobsSet.
     * @return Value of property useJobsSet.
     */
    public boolean isUseJobsSet() {
        return useJobsSet;
    }
    
    /**
     * Setter for parameter controlling the 
     * caching of requests in memory
     * @param value If false, the incomplete requests will be cached 
     *   memory , if true, they might be Garbage collected and
     *  be reread from DB, when accessed
     */
    public void saveMemory(boolean value) {
        this.useJobsSet = !value;
        if(useJobsSet == false && !jobsSet.isEmpty()) {
            try
            {
                jobsSet.clear();
            }
            catch(Exception e)
            {
                //clear did not work, try garbage collection
                jobsSet = java.util.Collections.synchronizedSet(new java.util.HashSet());
            }
        }
    }
         protected void createTable(String tableName, String createStatement) throws SQLException {
            createTable(tableName, createStatement,false,false);
         }
   
        protected void createTable(String tableName, String createStatement,boolean verify,boolean clean) throws SQLException {
        Connection _con =null;
        try {
            pool = JdbcConnectionPool.getPool(jdbcUrl, jdbcClass, user, pass);
            //connect
            _con = pool.getConnection();
            _con.setAutoCommit(true);
            
            //get database info
            DatabaseMetaData md = _con.getMetaData();
            
            
            ResultSet tableRs = md.getTables(null, null, tableName.toLowerCase() , null );
            
            
            //fields to be saved from the  Job object in the database:
                /*
                    this.id = id;
                    this.nextJobId = nextJobId;
                    this.creationTime = creationTime;
                    this.lifetime = lifetime;
                    this.state = state;
                    this.errorMessage = errorMessage;
                    this.creator = creator;
                 
                 */
            if(!tableRs.next()) {
                say("DatabaseMetaData.getTables returned empty result set");
                try {
                    say(tableName+" does not exits");
                    Statement s = _con.createStatement();
                    say("executing statement: "+createStatement);
                    int result = s.executeUpdate(createStatement);
                    s.close();
                }
                catch(SQLException sqle) {
                    esay(sqle);
                    esay("relation could already exist (bug in postgres driver), ignoring");
                }
            }
            else if(verify)
            {
                ResultSet columns = md.getColumns(null, null, tableName, null);
                int columnIndex = 0;
                try {
                    while(columns.next()){
                        columnIndex++;
                        String columnName = columns.getString("COLUMN_NAME");
                        int columnDataType = columns.getInt("DATA_TYPE");
                        verify(columnIndex,tableName,columnName,columnDataType);
                    }
                    if(getColumnNum() != columnIndex) {
                        throw new SQLException("database table schema changed:"+
                        " table named "+tableName+
                        " has wrong number of fields: "+columnIndex+", should be :"+getColumnNum());
                    }
                } catch(SQLException sqle) {
                    esay("Verification failed: "+sqle);
                    esay("trying to rename the table and create the new one");
                    renameTable(tableName,_con);
                    reanamed_old_table = true;
                    
                    Statement s = _con.createStatement();
                    esay("executing statement: "+createStatement);
                    int result = s.executeUpdate(createStatement);
                    s.close();
                }
            }
            if(clean) {
                  String sqlStatementString = "UPDATE " + getTableName() +
                        " SET STATE="+State.DONE.getStateId()+
                        " WHERE STATE="+State.READY.getStateId();
                    Statement s = _con.createStatement();
                    say("executing statement: "+sqlStatementString);
                    int result = s.executeUpdate(sqlStatementString);
                    s.close();
                    sqlStatementString = "UPDATE " + getTableName() +
                        " SET STATE="+State.FAILED.getStateId()+
                        " WHERE STATE !="+State.FAILED.getStateId()+" AND"+
                            " STATE !="+State.CANCELED.getStateId()+" AND "+
                            " STATE !="+State.DONE.getStateId();
                    s = _con.createStatement();
                    say("executing statement: "+sqlStatementString);
                    result = s.executeUpdate(sqlStatementString);
                    s.close();
            }
                
            tableRs.close();
            // to be fast
            _con.setAutoCommit(false);
            pool.returnConnection(_con);
            _con =null;
        }
        catch (SQLException sqe) {
            if(_con != null) {
                pool.returnFailedConnection(_con);
                _con = null;
            }
            throw sqe;
        }
        catch (Exception ex) {
            esay(ex);
            if(_con != null) {
                pool.returnFailedConnection(_con);
                _con = null;
            }
            throw new SQLException(ex.toString());
        }
        finally 
        {
            if(_con != null) {
                _con.setAutoCommit(false);
                pool.returnConnection(_con);
            }
        }
        
    }
    protected int renameTable(String oldName) throws SQLException {
        Connection _con =null;
        try {
            pool = JdbcConnectionPool.getPool(jdbcUrl, jdbcClass, user, pass);
            //connect
            _con = pool.getConnection();
            _con.setAutoCommit(true);
            return renameTable(oldName,_con);
        
       }
        catch (SQLException sqe) {
            if(_con != null) {
                pool.returnFailedConnection(_con);
                _con = null;
            }
            throw sqe;
        }
        catch (Exception ex) {
            esay(ex);
            if(_con != null) {
                pool.returnFailedConnection(_con);
                _con = null;
            }
            throw new SQLException(ex.toString());
        }
        finally 
        {
            if(_con != null) {
                _con.setAutoCommit(false);
                pool.returnConnection(_con);
            }
        }
    }
    
    protected int renameTable(String oldName,Connection _con) throws SQLException {
        /*
            String alterStatement = "ALTER TABLE "+oldName+
            " RENAME TO "+oldName+"_RENAMED_ON_"+System.currentTimeMillis();
            Statement s = _con.createStatement();
            esay("executing statement: "+alterStatement);
            return s.executeUpdate(alterStatement);
         */
        
        //rename does not work because the implicit index created for the 
        // original table remains connected to the renamed table 
        /// and the new table creation fails because of imposibility of creation 
        // of the new index
            String dropStatement = "DROP TABLE "+oldName+ " CASCADE";
            Statement s = _con.createStatement();
            esay("executing statement: "+dropStatement);
            int result =  s.executeUpdate(dropStatement);
            s.close();
            return result;
        
    }
    protected abstract int getAdditionalColumnsNum();
    private int getColumnNum() {
	    return COLLUMNS_NUM + getAdditionalColumnsNum();
    }
        
	protected void createIndex(String[] columnNames, 
				   String tableName) 
		throws SQLException {
		Connection _con =null;
		try {
			pool = JdbcConnectionPool.getPool(jdbcUrl, jdbcClass, user, pass);
			_con = pool.getConnection();
			_con.setAutoCommit(true);
			DatabaseMetaData dbMetaData = _con.getMetaData();
			ResultSet index_rset        = dbMetaData.getIndexInfo(null, 
									      null, 
									      tableName, 
									      false, 
									      false);
			
			HashSet listOfColumnsToBeIndexed = new HashSet();
			for(int i=0;i<columnNames.length;i++) {
				listOfColumnsToBeIndexed.add(columnNames[i].toLowerCase());
			}
			while (index_rset.next()) {
				String s = index_rset.getString("column_name").toLowerCase();
				if (listOfColumnsToBeIndexed.contains(s)) { 
					listOfColumnsToBeIndexed.remove(s);
				}
			}
			if (listOfColumnsToBeIndexed.size()==0) { 
				say("all indexes were already made for table "+tableName);
				pool.returnConnection(_con);
				_con =null;
				return;
			}
			Iterator i = listOfColumnsToBeIndexed.iterator();
			while(i.hasNext()) { 
				String columnName=(String)i.next();
				String indexName=tableName.toLowerCase()+"_"+columnName+INDEX_SUFFIX;
				String createIndexStatementText = "CREATE INDEX "+indexName+" ON "+tableName+" ("+columnName+")";
				Statement createIndexStatement = _con.createStatement();
				say("Executing "+createIndexStatementText);
				try { 
					createIndexStatement.executeUpdate(createIndexStatementText);
				}
				catch (Exception e) { 
					esay("failed to execute : "+createIndexStatementText);
					esay(e);
				}
                                createIndexStatement.close();
			}
			_con.setAutoCommit(false);
			pool.returnConnection(_con);
			_con =null;
		}
		catch (SQLException sqe) {
			if(_con != null) {
				pool.returnFailedConnection(_con);
				_con = null;
			}
			throw sqe;
		}
		catch (Exception ex) {
			esay(ex);
			if(_con != null) {
				pool.returnFailedConnection(_con);
				_con = null;
			}
			throw new SQLException(ex.toString());
		}
		finally {
			if(_con != null) {
				_con.setAutoCommit(false);
				pool.returnConnection(_con);
			}
		}
	}

 

    protected abstract void _verify(int nextIndex, int columnIndex,String tableName,String columnName,int columnType) 
    throws SQLException;
    
    protected String getTypeName(int type){
         java.lang.reflect.Field[] fields = java.sql.Types.class.getFields();
         for(int i = 0; i<fields.length; ++i) {
                java.lang.reflect.Field field = fields[i];
                try{
                
                     Object val = field.get(null);
                      int value = ((Integer)val).intValue();
                      if (value == type ){
                         return field.getName();
                      }
                }catch(Exception e) {/*ignore*/}
                
            }
        return "UNKNOWN SQL TYPE:"+type;
    }
    
    protected void verifyLongType(String expectedName,int columnIndex,String tableName, String columnName,int columnType)
    throws SQLException
    {
                if(!columnName.equalsIgnoreCase(expectedName) )
                {
                    throw new SQLException("database table schema changed:"+
                    "table named "+tableName+
                    " column #"+columnIndex+" has name \""+columnName+
                    "\" should be \""+expectedName+"\"");
                }
                if( columnType != longType_int)
                {
                    throw new SQLException("database table schema changed:"+
                    "table named "+tableName+
                    " column #"+columnIndex+" has type \""+getTypeName(columnType)+
                    "\" should be \""+longType+"\"");
                    
                }

    }
    protected void verifyIntType(String expectedName,int columnIndex,String tableName, String columnName,int columnType)
    throws SQLException
    {
                if(!columnName.equalsIgnoreCase(expectedName) )
                {
                    throw new SQLException("database table schema changed:"+
                    "table named "+tableName+
                    " column #"+columnIndex+" has name \""+columnName+
                    "\" should be \""+expectedName+"\"");
                }
                if( columnType != intType_int)
                {
                    throw new SQLException("database table schema changed:"+
                    "table named "+tableName+
                    " column #"+columnIndex+" has type \""+getTypeName(columnType)+
                    "\" should be \""+intType+"\"");
                    
                }

    }
    
    protected void verifyBooleanType(String expectedName,int columnIndex,String tableName, String columnName,int columnType)
    throws SQLException
    {
                if(!columnName.equalsIgnoreCase(expectedName) )
                {
                    throw new SQLException("database table schema changed:"+
                    "table named "+tableName+
                    " column #"+columnIndex+" has name \""+columnName+
                    "\" should be \""+expectedName+"\"");
                }
                if( columnType != booleanType_int)
                {
                    throw new SQLException("database table schema changed:"+
                    "table named "+tableName+
                    " column #"+columnIndex+" has type \""+getTypeName(columnType)+
                    "\" should be \""+booleanType+"\"");
                    
                }

    }
    
    protected void verifyStringType(String expectedName,int columnIndex,String tableName, String columnName,int columnType)
    throws SQLException
    {
                if(!columnName.equalsIgnoreCase(expectedName) )
                {
                    throw new SQLException("database table schema changed:"+
                    "table named "+tableName+
                    " column #"+columnIndex+" has name \""+columnName+
                    "\" should be \""+expectedName+"\"");
                }
                if( columnType != stringType_int)
                {
                    throw new SQLException("database table schema changed:"+
                    "table named "+tableName+
                    " column #"+columnIndex+" named \""+columnName+"\" has type \""+getTypeName(columnType)+
                    "\" should be \""+stringType+"\"");
                    
                }

    }
    
    public void verify(int columnIndex,String tableName, String columnName,int columnType) throws SQLException {
        switch (columnIndex) {
            /*    "ID "+         longType+" NOT NULL PRIMARY KEY"+
    ","+
    "NEXTJOBID "+           longType+
    ","+
    "CREATIONTIME "+        longType
    +","+
    "LIFETIME "+            longType
    +","+
    "STATE "+               stringType
    +","+
    "ERRORMESSAGE "+        stringType+
    ","+
    "CREATORID "+           stringType+
    ","+
    "SCHEDULERID "+         stringType+
    ","+
    "SCHEDULERTIMESTAMP "+  longType+
    ","+
    "NUMOFRETR "+  longType+
    ","+
    "MAXNUMOFRETR "+  longType;
 */
            case 1:
                verifyLongType("ID",columnIndex,tableName, columnName, columnType);
                break;
            case 2:
                verifyLongType("NEXTJOBID",columnIndex,tableName, columnName, columnType);
                break;
            case 3:
                verifyLongType("CREATIONTIME",columnIndex,tableName, columnName, columnType);
                break;
            case 4:
                verifyLongType("LIFETIME",columnIndex,tableName, columnName, columnType);
                break;
            case 5:
                verifyIntType("STATE",columnIndex,tableName, columnName, columnType);
                break;
            case 6:
                verifyStringType("ERRORMESSAGE",columnIndex,tableName, columnName, columnType);
                break;
            case 7:
                verifyStringType("SCHEDULERID",columnIndex,tableName, columnName, columnType);
                break;
            case 8:
                verifyLongType("SCHEDULERTIMESTAMP",columnIndex,tableName, columnName, columnType);
                break;
            case 9:
                verifyLongType("NUMOFRETR",columnIndex,tableName, columnName, columnType);
                break;
            case 10:
                verifyLongType("MAXNUMOFRETR",columnIndex,tableName, columnName, columnType);
                break;
            case 11: 
                verifyLongType("LASTSTATETRANSITIONTIME",columnIndex,tableName, columnName, columnType);
                break;
                
            default:
                _verify(12,columnIndex,tableName, columnName, columnType);
        }
    }
    
    public static String fixStringForSQL(String s)
    {
        if(s == null) { return null;}
        return s.replace('\'', '`');
    }
    
    public void run(){
        long update_period = configuration.getOldRequestRemovePeriodSecs()*1000L;
        long history_lifetime = configuration.getNumDaysHistory()*24*3600*1000L;
        while(true) {
            try {
                Thread.sleep(update_period);
            } 
            catch(InterruptedException ie) {
                esay("database update thread interrupted");
                return;
            }
            long currenttime = System.currentTimeMillis();
            long cutout_expiration_time = currenttime - history_lifetime;
            Connection _con =null;
            try {
                pool = JdbcConnectionPool.getPool(jdbcUrl, jdbcClass, user, pass);
                //connect
                _con = pool.getConnection();
                String sqlStatementString =
                        "DELETE from "+getTableName() +
                        " WHERE CREATIONTIME+LIFETIME < "+
                        cutout_expiration_time;
                        Statement s = _con.createStatement();
                        //say("executing statement: "+sqlStatementString);
                        int result = s.executeUpdate(sqlStatementString);
                        s.close();
                        _con.commit();
                        //say("deleted "+result+" records ");
               }
                catch (SQLException sqe) {
                    if(_con != null) {
                        pool.returnFailedConnection(_con);
                        _con = null;
                    }
                }
                catch (Exception ex) {
                    esay(ex);
                    if(_con != null) {
                        pool.returnFailedConnection(_con);
                        _con = null;
                    }
                }
                finally 
                {
                    if(_con != null) {
                        pool.returnConnection(_con);
                    }
                }
        }
    }
}
