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

package org.dcache.srm.request.sql;

import com.google.common.base.Predicate;
import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.PreparedStatementCreator;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.dcache.commons.util.SqlHelper;
import org.dcache.srm.SRMInvalidRequestException;
import org.dcache.srm.request.Job;
import org.dcache.srm.scheduler.IllegalStateTransition;
import org.dcache.srm.scheduler.JobStorage;
import org.dcache.srm.scheduler.Scheduler;
import org.dcache.srm.scheduler.State;
import org.dcache.srm.util.Configuration;

import static com.google.common.collect.Iterables.filter;
import static org.dcache.srm.request.sql.Utilities.getIdentifierAsStored;

/**
 *
 * @author  timur
 */
public abstract class DatabaseJobStorage<J extends Job> implements JobStorage<J>, Runnable {
    private final static Logger logger =
            LoggerFactory.getLogger(DatabaseJobStorage.class);

    protected static final String INDEX_SUFFIX="_idx";
    public static final Predicate<Job.JobHistory> NOT_SAVED = new Predicate<Job.JobHistory>()
    {
        @Override
        public boolean apply(Job.JobHistory element)
        {
            return !element.isSaved();
        }
    };

    @SuppressWarnings("unchecked")
    private final Class<J> jobType = (Class<J>) new TypeToken<J>(getClass()) {}.getRawType();

    protected final Configuration.DatabaseParameters configuration;
    private final String jdbcUrl;
    private final String jdbcClass;
    private final String user;
    private final String pass;
    protected boolean logHistory;
    protected static final String stringType=" VARCHAR(32672) ";
    protected static final String longType=" BIGINT ";
    protected static final String intType=" INTEGER ";
    protected static final String dateTimeType= " TIMESTAMP ";
    protected static final String booleanType= " INT ";
    protected static final int stringType_int= Types.VARCHAR;
    protected static final int longType_int= Types.BIGINT;
    protected static final int intType_int= Types.INTEGER;
    protected static final int dateTimeType_int= Types.TIMESTAMP;
    protected static final int booleanType_int= Types.INTEGER;

    public DatabaseJobStorage(Configuration.DatabaseParameters configuration)
            throws SQLException
            {
        this.configuration = configuration;

        this.jdbcUrl = configuration.getJdbcUrl();
        this.jdbcClass = configuration.getJdbcClass();
        this.user = configuration.getJdbcUser();
        this.pass = configuration.getJdbcPass();
        this.logHistory = configuration.isRequestHistoryDatabaseEnabled();

        dbInit(configuration.isCleanPendingRequestsOnRestart());
        //updatePendingJobs();
        new Thread(this,"update"+getTableName()).start();
            }

    @Override
    public boolean isJdbcLogRequestHistoryInDBEnabled()
    {
        return logHistory;
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

    protected boolean reanamed_old_table;
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
                "STATE",
        "SCHEDULERID"};
        createIndex(columns, getTableName().toLowerCase());
        //
        // create index on expirationtime (CREATIONTIME+LIFETIME)
        // which is used to find requests that can be removed from DB
        //
        createIndex(getTableName().toLowerCase()+"_expirationtime_idx",
                "(CREATIONTIME+LIFETIME)".toLowerCase(),
                getTableName().toLowerCase());



        String history_columns[] = {
                "STATEID",
                "TRANSITIONTIME",
        "JOBID"};
        createIndex(history_columns,getHistoryTableName().toLowerCase());
        _dbInit();
    }

    private void insertStates() throws  SQLException{
        Connection _con =null;
        Statement sqlStatement = null;
        try {
            _con = pool.getConnection();
            State[] states = State.values();
            String countStates = "SELECT count(*) from "+srmStateTableName;
            sqlStatement = _con.createStatement();
            ResultSet rs = sqlStatement.executeQuery( countStates);
            if(rs.next()){
                int count = rs.getInt(1);
                if(count != states.length) {
                    for (State state : states) {
                        String insertState = "INSERT INTO " +
                                srmStateTableName + " VALUES (?, ?)";
                        logger.debug("inserting into SRMJOBSTATE values: {} {}",
                                state.getStateId(), state);
                        PreparedStatement sqlStatement1 = null;
                        try {
                            sqlStatement1 = _con.prepareStatement(insertState);
                            sqlStatement1.setInt(1, state.getStateId());
                            sqlStatement1.setString(2, state.toString());
                            int result = sqlStatement1.executeUpdate();
                            _con.commit();
                        } catch (SQLException sqle) {
                            //ignoring, state might be already in the table
                            logger.error(sqle.toString());
                        } finally {
                            SqlHelper.tryToClose(sqlStatement1);
                        }
                    }
                }
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
            SqlHelper.tryToClose(sqlStatement);
            if(_con != null) {
                pool.returnConnection(_con);
            }
        }
    }

    protected abstract J getJob(
            Connection _con,
            long ID,
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

    @Override
    public void init() throws SQLException
    {
    }

    @Override
    public J getJob(long jobId) throws SQLException
    {
        Connection _con =null;

        try {
            _con = pool.getConnection();
            J job = getJob(jobId,_con);
            pool.returnConnection(_con);
            _con = null;
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

    @Override
    public J getJob(long jobId,Connection _con) throws SQLException
    {
        logger.debug("executing statement: SELECT * FROM {} WHERE ID=?({})",
                     getTableName(), jobId);
        PreparedStatement statement =null;
        ResultSet set = null;
        try{
            statement = getPreparedStatement(_con,
                    "SELECT * FROM " + getTableName() + " WHERE ID=?",
                    jobId);

            set = statement.executeQuery();
            if(!set.next()) {
                return null;
            }
            return getJob(_con, set);
        } finally {
            SqlHelper.tryToClose(set);
            SqlHelper.tryToClose(statement);
        }
    }

    private J getJob(Connection _con, ResultSet set) throws SQLException
    {
        long ID = set.getLong(1);
        Long NEXTJOBID = set.getLong(2);
        long CREATIONTIME = set.getLong(3);
        long LIFETIME = set.getLong(4);
        int STATE = set.getInt(5);
        String ERRORMESSAGE = set.getString(6);
        String SCHEDULERID=set.getString(7);
        long SCHEDULER_TIMESTAMP=set.getLong(8);
        int NUMOFRETR = set.getInt(9);
        int MAXNUMOFRETR = set.getInt(10);
        long LASTSTATETRANSITIONTIME = set.getLong(11);
        J job = getJob(_con,
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
        return job;
    }

    private int updateJob(Connection connection, Job job) throws SQLException
    {
        PreparedStatement updateStatement = null;
        try {
            job.rlock();
            try {
                updateStatement = getUpdateStatement(connection, job);
            } finally {
                job.runlock();
            }
            return updateStatement.executeUpdate();
        } finally {
            SqlHelper.tryToClose(updateStatement);
        }
    }

    private void createJob(Connection connection, Job job) throws SQLException
    {
        PreparedStatement createStatement = null;
        PreparedStatement batchCreateStatement = null;
        try {
            job.rlock();
            try {
                createStatement = getCreateStatement(connection, job);
                batchCreateStatement = getBatchCreateStatement(connection, job);
            } finally {
                job.runlock();
            }
            createStatement.executeUpdate();
            if (batchCreateStatement != null) {
                batchCreateStatement.executeBatch();
            }
        } finally {
            SqlHelper.tryToClose(createStatement);
            SqlHelper.tryToClose(batchCreateStatement);
        }
    }

    private void saveHistory(Connection connection, Job job,
                             List<Job.JobHistory> history) throws SQLException
    {
        PreparedStatement stmt =
                connection.prepareStatement("INSERT INTO " + getHistoryTableName() + " VALUES (?,?,?,?,?)");
        try {
            for (Job.JobHistory element : history) {
                stmt.setLong(1, element.getId());
                stmt.setLong(2, job.getId());
                stmt.setInt(3, element.getState().getStateId());
                stmt.setLong(4, element.getTransitionTime());
                stmt.setString(5, element.getDescription());
                stmt.addBatch();
            }
            stmt.executeBatch();
        } finally {
            SqlHelper.tryToClose(stmt);
        }
    }

    private void markHistoryAsSaved(List<Job.JobHistory> history)
    {
        for (Job.JobHistory element : history) {
            element.setSaved();
        }
    }

    private List<Job.JobHistory> getJobHistoriesToSave(Job job)
    {
        return logHistory
                ? Lists.newArrayList(filter(job.getJobHistory(), NOT_SAVED))
                : Collections.<Job.JobHistory>emptyList();
    }


    @Override
    public void saveJob(final Job job,boolean saveifmonitoringisdesabled) throws SQLException
    {
        if (!saveifmonitoringisdesabled && !logHistory) {
            return;
        }

        List<Job.JobHistory> history = getJobHistoriesToSave(job);

        boolean success = false;
        Connection connection = pool.getConnection();
        try {
            connection.setAutoCommit(false);
            int rowCount = updateJob(connection, job);
            if (rowCount == 0) {
                createJob(connection, job);
            }
            if (!history.isEmpty()) {
                saveHistory(connection, job, history);
            }
            success = true;
        } finally {
            if (!success) {
                pool.returnFailedConnection(connection);
            } else {
                pool.returnConnection(connection);
            }
        }
        markHistoryAsSaved(history);
    }

    protected PreparedStatement getBatchCreateStatement(Connection connection, Job job)
            throws SQLException
    {
        return null;
    }

    public abstract PreparedStatement getCreateStatement(Connection connection, Job job) throws SQLException;
    public abstract PreparedStatement getUpdateStatement(Connection connection, Job job) throws SQLException;

    @Override
    public Set<J> getJobs(final String schedulerId) throws SQLException
    {
        return getJobs(new PreparedStatementCreator()
        {
            @Override
            public PreparedStatement createPreparedStatement(Connection connection)
                    throws SQLException
            {
                String sql = "SELECT * FROM " + getTableName() + " WHERE SCHEDULERID=?";
                PreparedStatement stmt = connection.prepareStatement(sql);
                stmt.setString(1, schedulerId);
                return stmt;
            }
        });
    }

    protected Job.JobHistory[] getJobHistory(long jobId,Connection _con) throws SQLException{
        List<Job.JobHistory> l = new ArrayList<>();
        String select = "SELECT * FROM " +getHistoryTableName()+
                " WHERE JOBID="+jobId;
        logger.debug("executing statement: {}", select);
        Statement statement = _con.createStatement();
        ResultSet set = statement.executeQuery(select);
        if(!set.next()) {
            logger.debug("no history elements in table {} found, returning NULL",
                         getHistoryTableName());
            statement.close();
            return null;
        }

        do {
            long ID = set.getLong(1);
            int STATEID = set.getInt(3);
            long TRANSITIONTIME  = set.getLong(4);
            String DESCRIPTION  = set.getString(5);
            Job.JobHistory jh = new Job.JobHistory(ID,
                    State.getState(STATEID),
                    DESCRIPTION,
                    TRANSITIONTIME);
            jh.setSaved();
            l.add(jh);
            logger.debug("found JobHistory: {}", jh.toString());

        } while (set.next());
        statement.close();
        return l.toArray(new Job.JobHistory[l.size()]);
    }

    public void schedulePendingJobs(Scheduler scheduler)
            throws SQLException,
            InterruptedException,
            IllegalStateTransition {
        Connection _con =null;
        try {
            _con = pool.getConnection();
            Statement sqlStatement = _con.createStatement();
            String sqlStatementString = "SELECT ID FROM " + getTableName() +
                    " WHERE SCHEDULERID is NULL and State="+State.PENDING.getStateId();
            logger.debug("executing statement: {}", sqlStatementString);
            ResultSet set = sqlStatement.executeQuery(sqlStatementString);
            //save in the memory the ids to prevent the exhaust of the connections
            // so we return connections before trying to schedule the pending
            // requests
            Set<Long> idsSet = new HashSet<>();
            while(set.next()) {
                idsSet.add(set.getLong(1));
            }

            set.close();
            sqlStatement.close();
            pool.returnConnection(_con);
            _con = null;

            for(Long ID : idsSet)
            {
                try {
                    J job = Job.getJob(ID, jobType, _con);
                    scheduler.schedule(job);
                } catch (SRMInvalidRequestException ire) {
                    logger.error(ire.toString());
                }
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
    protected Set<Long> getJobIdsByCondition(String sqlCondition) throws SQLException{
        Set<Long> jobIds = new HashSet<>();
        Connection _con =null;
        try {
            _con = pool.getConnection();
            Statement sqlStatement = _con.createStatement();
            String sqlStatementString = "SELECT ID FROM " + getTableName() +
                    " WHERE "+sqlCondition+" ";
            logger.debug("executing statement: {}", sqlStatementString);
            ResultSet set = sqlStatement.executeQuery(sqlStatementString);
            while(set.next()) {
                long ID = set.getLong(1);
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

    @Override
    public Set<Long> getLatestCompletedJobIds(int maxNum)  throws SQLException {
        return getJobIdsByCondition(
                " STATE =" + State.DONE.getStateId() +
                        " OR STATE =" + State.CANCELED.getStateId() +
                        " OR STATE = " + State.FAILED.getStateId() +
                        " ORDER BY ID DESC" +
                        " LIMIT " + maxNum + " ");
    }

    @Override
    public Set<Long> getLatestDoneJobIds(int maxNum)  throws SQLException {
        return getJobIdsByCondition("STATE ="+State.DONE.getStateId()+
                " ORDERED BY ID DESC"+
                " LIMIT "+maxNum+" ");
    }

    @Override
    public Set<Long> getLatestFailedJobIds(int maxNum)  throws SQLException {
        return getJobIdsByCondition("STATE !="+State.FAILED.getStateId()+
                " ORDERED BY ID DESC"+
                " LIMIT "+maxNum+" ");
    }

    @Override
    public Set<Long> getLatestCanceledJobIds(int maxNum)  throws SQLException {
        return getJobIdsByCondition("STATE != "+State.CANCELED.getStateId()+
                " ORDERED BY ID DESC"+
                " LIMIT "+maxNum+" ");
    }

    private Set<J> getJobs(PreparedStatementCreator psc) throws SQLException
    {
        Set<J> jobs = new HashSet<>();
        Connection _con = pool.getConnection();
        PreparedStatement sqlStatement = null;
        ResultSet set = null;
        try {
            sqlStatement = psc.createPreparedStatement(_con);
            set = sqlStatement.executeQuery();
            while (set.next()) {
                J job = getJob(_con, set);
                logger.debug("==========> deserialization from database of job id {}", job.getId());
                logger.debug("==========> jobs submitter id is {}", job.getSubmitterId());
                jobs.add(job);
            }

            set.close();
            sqlStatement.close();
            pool.returnConnection(_con);
            _con = null;
            return jobs;
        } finally {
            SqlHelper.tryToClose(set);
            SqlHelper.tryToClose(sqlStatement);
            if (_con != null) {
                pool.returnFailedConnection(_con);
            }
        }
    }

    @Override
    public Set<J> getActiveJobs() throws SQLException
    {
        return getJobs(new PreparedStatementCreator()
        {
            @Override
            public PreparedStatement createPreparedStatement(Connection connection)
                    throws SQLException
            {
                String sql =
                        "SELECT * FROM " + getTableName() +
                                " WHERE STATE !=" + State.DONE.getStateId() +
                                " AND STATE !=" + State.CANCELED.getStateId() +
                                " AND STATE !=" + State.FAILED.getStateId();
                return connection.prepareStatement(sql);
            }
        });
    }

    @Override
    public Set<J> getJobs(final String schedulerId, final State state) throws SQLException
    {
        return getJobs(new PreparedStatementCreator()
        {
            @Override
            public PreparedStatement createPreparedStatement(Connection connection)
                    throws SQLException
            {
                PreparedStatement stmt;
                if (schedulerId == null) {
                    stmt = connection
                            .prepareStatement("SELECT * FROM " + getTableName() + " WHERE SCHEDULERID IS NULL AND STATE=?");
                    stmt.setInt(1, state.getStateId());
                } else {
                    stmt = connection
                            .prepareStatement("SELECT * FROM " + getTableName() + " WHERE SCHEDULERID=? AND STATE=?");
                    stmt.setString(1, schedulerId);
                    stmt.setInt(2, state.getStateId());
                }
                return stmt;
            }
        });
    }

    protected void createTable(String tableName, String createStatement) throws SQLException {
        createTable(tableName, createStatement,false,false);
    }

    protected void createTable(String tableName, String createStatement,boolean verify,boolean clean) throws SQLException {
        Connection _con = null;
        try {
            pool = JdbcConnectionPool.getPool(jdbcUrl, jdbcClass, user, pass);
            //connect
            _con = pool.getConnection();
            _con.setAutoCommit(true);

            //get database info
            DatabaseMetaData md = _con.getMetaData();

            String tableNameAsStored = getIdentifierAsStored(md, tableName);
            ResultSet tableRs =
                md.getTables(null, null, tableNameAsStored, null);

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
                logger.debug("DatabaseMetaData.getTables returned empty result set");
                try {
                    logger.debug("{} does not exits", tableName);
                    Statement s = _con.createStatement();
                    logger.debug("executing statement: {}", createStatement);
                    int result = s.executeUpdate(createStatement);
                    s.close();
                }
                catch(SQLException sqle) {
                    logger.error("relation could already exist, ignoring: {}", sqle.toString());
                }
            }
            else if(verify)
            {
                ResultSet columns = md.getColumns(null, null, tableNameAsStored, null);
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
                    logger.warn("Verification failed, trying to rename " +
                                "the table and create the new one", sqle);
                    renameTable(tableName,_con);
                    reanamed_old_table = true;

                    Statement s = _con.createStatement();
                    logger.warn("Creating table {}", tableName);
                    logger.debug("executing statement: {}", createStatement);
                    int result = s.executeUpdate(createStatement);
                    s.close();
                }
            }
            if(clean) {
                String sqlStatementString = "UPDATE " + getTableName() +
                        " SET STATE="+State.DONE.getStateId()+
                        " WHERE STATE="+State.READY.getStateId();
                Statement s = _con.createStatement();
                logger.debug("executing statement: {}", sqlStatementString);
                int result = s.executeUpdate(sqlStatementString);
                s.close();
                sqlStatementString = "UPDATE " + getTableName() +
                        " SET STATE="+State.FAILED.getStateId()+
                        " WHERE STATE !="+State.FAILED.getStateId()+" AND"+
                        " STATE !="+State.CANCELED.getStateId()+" AND "+
                        " STATE !="+State.DONE.getStateId();
                s = _con.createStatement();
                logger.debug("executing statement: {}", sqlStatementString);
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
            logger.error(ex.toString());
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
            logger.error(ex.toString());
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
            logger.error("executing statement: "+alterStatement);
            return s.executeUpdate(alterStatement);
         */

        //rename does not work because the implicit index created for the
        // original table remains connected to the renamed table
        /// and the new table creation fails because of imposibility of creation
        // of the new index
        String dropStatement = "DROP TABLE IF EXISTS "+oldName+ " CASCADE";
        Statement s = _con.createStatement();
        logger.warn("Moving SRM table {} away as its schema is out-of-date", oldName);
        logger.debug("executing statement: {}", dropStatement);
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
            ResultSet index_rset =
                dbMetaData.getIndexInfo(null,
                                        null,
                                        getIdentifierAsStored(dbMetaData,
                                                              tableName),
                                        false,
                                        false);

            Set<String> listOfColumnsToBeIndexed = new HashSet<>();
            for (String columnName1 : columnNames) {
                listOfColumnsToBeIndexed.add(columnName1.toLowerCase());
            }
            while (index_rset.next()) {
                String s = index_rset.getString("column_name").toLowerCase();
                if (listOfColumnsToBeIndexed.contains(s)) {
                    listOfColumnsToBeIndexed.remove(s);
                }
            }
            if (listOfColumnsToBeIndexed.size()==0) {
                logger.debug("all indexes were already made for table {}", tableName);
                _con.setAutoCommit(false);
                pool.returnConnection(_con);
                _con =null;
                return;
            }
            for (String columnName : listOfColumnsToBeIndexed) {
                String indexName=tableName.toLowerCase()+"_"+columnName+INDEX_SUFFIX;
                createIndex(_con,indexName,  tableName,columnName);
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
            logger.error(ex.toString());
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

    protected void createIndex(String indexname,
            String  expression,
            String tableName)
                    throws SQLException {
        indexname=indexname.toLowerCase();
        Connection _con =null;
        try {
            pool = JdbcConnectionPool.getPool(jdbcUrl, jdbcClass, user, pass);
            _con = pool.getConnection();
            _con.setAutoCommit(true);
            DatabaseMetaData dbMetaData = _con.getMetaData();
            ResultSet index_rset =
                dbMetaData.getIndexInfo(null,
                                        null,
                                        getIdentifierAsStored(dbMetaData,
                                                              tableName),
                                        false,
                                        false);

            while (index_rset.next()) {
                String s = index_rset.getString("index_name").toLowerCase();
                if (indexname.equals(s)){
                    logger.debug("index {} already exists", indexname);
                    _con.setAutoCommit(false);
                    pool.returnConnection(_con);
                    _con =null;
                    return;
                }
            }
            createIndex(_con,indexname,  tableName,expression);
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
            logger.error(ex.toString());
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


    protected void createIndex( final Connection _con,
            final String indexName,
            final String tableName,
            final String column_or_expression) throws SQLException {
        String createIndexStatementText = "CREATE INDEX "+indexName+
                " ON "+tableName+" ("+column_or_expression+")";
        logger.debug("Executing {}", createIndexStatementText);
        try (Statement createIndexStatement = _con.createStatement()) {
            createIndexStatement.executeUpdate(createIndexStatementText);
        } catch (SQLException e) {
            logger.error("failed to execute '{}': {}",
                    createIndexStatementText, e.toString());
        }

    }



    protected abstract void _verify(int nextIndex, int columnIndex,String tableName,String columnName,int columnType)
            throws SQLException;

    protected String getTypeName(int type){
        Field[] fields = Types.class.getFields();
        for (Field field : fields) {
            try {

                Object val = field.get(null);
                int value = (Integer) val;
                if (value == type) {
                    return field.getName();
                }
            } catch (Exception e) {/*ignore*/}

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

    @Override
    public void run(){
        long update_period =
                TimeUnit.SECONDS.toMillis(configuration.getExpiredRequestRemovalPeriod());
        long history_lifetime =
                TimeUnit.DAYS.toMillis(configuration.getKeepRequestHistoryPeriod());
        while(true) {
            try {
                Thread.sleep(update_period);
            }
            catch(InterruptedException ie) {
                logger.info("database update thread interrupted");
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
                //logger.debug("executing statement: "+sqlStatementString);
                int result = s.executeUpdate(sqlStatementString);
                s.close();
                _con.commit();
                //logger.debug("deleted "+result+" records ");
            }
            catch (SQLException sqe) {
                if(_con != null) {
                    pool.returnFailedConnection(_con);
                    _con = null;
                }
            }
            catch (Exception ex) {
                logger.error(ex.toString());
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

    public PreparedStatement getPreparedStatement(
            Connection connection,
            String query,
            Object ... args)
                    throws SQLException {
        PreparedStatement stmt = connection.prepareStatement(query);
        for (int i = 0; i < args.length; i++) {
            stmt.setObject(i + 1, args[i]);
        }
        return stmt;
    }
}

