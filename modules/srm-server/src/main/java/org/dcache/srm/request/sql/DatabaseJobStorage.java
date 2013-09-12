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
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.BadSqlGrammarException;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCreator;
import org.springframework.jdbc.core.PreparedStatementSetter;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionTemplate;

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

    private final Configuration.DatabaseParameters configuration;
    protected final JdbcTemplate jdbcTemplate;
    protected final TransactionTemplate transactionTemplate;
    private final boolean logHistory;

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
            throws DataAccessException
    {
        this.configuration = configuration;
        this.logHistory = configuration.isRequestHistoryDatabaseEnabled();
        this.jdbcTemplate = new JdbcTemplate(configuration.getDataSource());
        this.transactionTemplate = new TransactionTemplate(configuration.getTransactionManager());

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

    protected boolean droppedOldTable;
    private String getHistoryTableName() {
        return getTableName().toLowerCase()+"history";
    }

    protected void dbInit(boolean clean)
            throws DataAccessException
    {
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
        if (droppedOldTable) {
            dropTable(historyTableName);
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
        try {
            createIndex(getTableName().toLowerCase()+"_expirationtime_idx",
                    "(CREATIONTIME+LIFETIME)".toLowerCase(),
                    getTableName().toLowerCase());
        } catch (BadSqlGrammarException ignored) {
            // Not all databases support computed indexes
        }


        String history_columns[] = {
                "STATEID",
                "TRANSITIONTIME",
        "JOBID"};
        createIndex(history_columns, getHistoryTableName().toLowerCase());
    }

    private void insertStates() throws DataAccessException
    {
        String insertState = "INSERT INTO " + srmStateTableName + " VALUES (?, ?)";
        for (final State state : State.values()) {
            try {
                logger.debug("inserting into {} values: {} {}",
                        srmStateTableName, state.getStateId(), state);
                jdbcTemplate.update(insertState, new PreparedStatementSetter()
                {
                    @Override
                    public void setValues(PreparedStatement ps) throws SQLException
                    {
                        ps.setInt(1, state.getStateId());
                        ps.setString(2, state.toString());
                    }
                });
            } catch (DuplicateKeyException ignored) {
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
    public void init()
    {
    }

    @Override
    public J getJob(final long jobId) throws DataAccessException
    {
        return jdbcTemplate.execute(new ConnectionCallback<J>()
        {
            @Override
            public J doInConnection(Connection con) throws SQLException, DataAccessException
            {
                return getJob(jobId, con);
            }
        });
    }

    @Override
    public J getJob(long jobId,Connection _con) throws SQLException
    {
        logger.debug("executing statement: SELECT * FROM {} WHERE ID=?({})",
                getTableName(), jobId);
        try (PreparedStatement statement = getPreparedStatement(_con,
                "SELECT * FROM " + getTableName() + " WHERE ID=?", jobId);
             ResultSet set = statement.executeQuery()) {
            if (!set.next()) {
                return null;
            }
            return getJob(_con, set);
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
    public void saveJob(final Job job, boolean saveifmonitoringisdesabled) throws DataAccessException
    {
        if (!saveifmonitoringisdesabled && !logHistory) {
            return;
        }

        final List<Job.JobHistory> history = getJobHistoriesToSave(job);
        transactionTemplate.execute(new TransactionCallback<Void>()
        {
            @Override
            public Void doInTransaction(TransactionStatus status)
            {
                return jdbcTemplate.execute(new ConnectionCallback<Void>()
                {
                    @Override
                    public Void doInConnection(Connection con) throws SQLException, DataAccessException
                    {
                        int rowCount = updateJob(con, job);
                        if (rowCount == 0) {
                            createJob(con, job);
                        }
                        if (!history.isEmpty()) {
                            saveHistory(con, job, history);
                        }
                        return null;
                    }
                });

            }
        });
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
    public Set<J> getJobs(final String schedulerId) throws DataAccessException
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
                   IllegalStateTransition
    {
        String sql = "SELECT ID FROM " + getTableName() +
                " WHERE SCHEDULERID is NULL and State=" + State.PENDING.getStateId();
        for (Long ID : jdbcTemplate.queryForList(sql, Long.class)) {
            try {
                scheduler.schedule(Job.getJob(ID, jobType));
            } catch (SRMInvalidRequestException ire) {
                logger.error(ire.toString());
            }
        }
    }

    // this method returns ids as a set of "Long" id
    protected Set<Long> getJobIdsByCondition(String sqlCondition) throws DataAccessException
    {
        String sql = "SELECT ID FROM " + getTableName() + " WHERE " + sqlCondition;
        return new HashSet<>(jdbcTemplate.queryForList(sql, Long.class));
    }

    @Override
    public Set<Long> getLatestCompletedJobIds(int maxNum) throws DataAccessException
    {
        return getJobIdsByCondition(
                " STATE =" + State.DONE.getStateId() +
                        " OR STATE =" + State.CANCELED.getStateId() +
                        " OR STATE = " + State.FAILED.getStateId() +
                        " ORDER BY ID DESC" +
                        " LIMIT " + maxNum + " ");
    }

    @Override
    public Set<Long> getLatestDoneJobIds(int maxNum) throws DataAccessException
    {
        return getJobIdsByCondition("STATE =" + State.DONE.getStateId() +
                " ORDERED BY ID DESC" +
                " LIMIT " + maxNum + " ");
    }

    @Override
    public Set<Long> getLatestFailedJobIds(int maxNum) throws DataAccessException
    {
        return getJobIdsByCondition("STATE !="+State.FAILED.getStateId()+
                " ORDERED BY ID DESC"+
                " LIMIT "+maxNum+" ");
    }

    @Override
    public Set<Long> getLatestCanceledJobIds(int maxNum) throws DataAccessException
    {
        return getJobIdsByCondition("STATE != "+State.CANCELED.getStateId()+
                " ORDERED BY ID DESC"+
                " LIMIT "+maxNum+" ");
    }

    private Set<J> getJobs(PreparedStatementCreator psc) throws DataAccessException
    {
        return new HashSet<>(jdbcTemplate.query(psc, new RowMapper<J>()
        {
            @Override
            public J mapRow(ResultSet rs, int rowNum) throws SQLException
            {
                J job = getJob(rs.getStatement().getConnection(), rs);
                logger.debug("==========> deserialization from database of job id {}", job.getId());
                logger.debug("==========> jobs submitter id is {}", job.getSubmitterId());
                return job;
            }
        }));

    }

    @Override
    public Set<J> getActiveJobs() throws DataAccessException
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
    public Set<J> getJobs(final String schedulerId, final State state) throws DataAccessException
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

    protected void createTable(String tableName, String createStatement)
            throws DataAccessException
    {
        createTable(tableName, createStatement,false,false);
    }

    protected void createTable(final String tableName, final String createStatement, final boolean verify, final boolean clean)
            throws DataAccessException
    {
            jdbcTemplate.execute(new ConnectionCallback<Void>()
            {
                @Override
                public Void doInConnection(Connection con)
                        throws SQLException, DataAccessException
                {
                    DatabaseMetaData md = con.getMetaData();
                    String tableNameAsStored = getIdentifierAsStored(md, tableName);
                    try (ResultSet tableRs = md.getTables(null, null, tableNameAsStored, null)) {
                        if (!tableRs.next()) {
                            logger.debug("DatabaseMetaData.getTables returned empty result set");
                            logger.debug("{} does not exits", tableName);
                            logger.debug("executing statement: {}", createStatement);
                            try (Statement s = con.createStatement()) {
                                s.executeUpdate(createStatement);
                            }
                        } else if (verify) {
                            try (ResultSet columns = md
                                    .getColumns(null, null, tableNameAsStored, null)) {
                                int columnIndex = 0;
                                while (columns.next()) {
                                    columnIndex++;
                                    String columnName = columns.getString("COLUMN_NAME");
                                    int columnDataType = columns.getInt("DATA_TYPE");
                                    verify(columnIndex, tableName, columnName, columnDataType);
                                }
                                if (getColumnNum() != columnIndex) {
                                    throw new SQLException("database table schema changed:" +
                                            " table named " + tableName +
                                            " has wrong number of fields: " + columnIndex + ", should be :" + getColumnNum());
                                }
                            } catch (SQLException e) {
                                logger.warn("Verification failed. Trying to drop the table and create a new one: {}",
                                        e.toString());
                                dropTable(tableName, con);
                                droppedOldTable = true;

                                try (Statement s = con.createStatement()) {
                                    logger.debug("executing statement: {}", createStatement);
                                    s.executeUpdate(createStatement);
                                }
                            }
                        }
                        if (clean) {
                            String sqlStatementString = "UPDATE " + getTableName() +
                                    " SET STATE=" + State.DONE.getStateId() +
                                    " WHERE STATE=" + State.READY.getStateId();
                            try (Statement s = con.createStatement()) {
                                logger.debug("executing statement: {}", sqlStatementString);
                                s.executeUpdate(sqlStatementString);
                            }
                            sqlStatementString = "UPDATE " + getTableName() +
                                    " SET STATE=" + State.FAILED.getStateId() +
                                    " WHERE STATE !=" + State.FAILED.getStateId() + " AND" +
                                    " STATE !=" + State.CANCELED.getStateId() + " AND " +
                                    " STATE !=" + State.DONE.getStateId();
                            try (Statement s = con.createStatement()) {
                                logger.debug("executing statement: {}", sqlStatementString);
                                s.executeUpdate(sqlStatementString);
                            }
                        }
                    }
                    return null;
                }
            });
    }

    protected int dropTable(final String oldName) throws DataAccessException
    {
        return jdbcTemplate.execute(new ConnectionCallback<Integer>()
        {
            @Override
            public Integer doInConnection(Connection con) throws SQLException, DataAccessException
            {
                return dropTable(oldName, con);
            }
        });
    }

    private int dropTable(String oldName, Connection con) throws SQLException
    {
        //rename does not work because the implicit index created for the
        // original table remains connected to the renamed table
        /// and the new table creation fails because of imposibility of creation
        // of the new index
        String dropStatement = "DROP TABLE IF EXISTS " + oldName + " CASCADE";
        try (Statement s = con.createStatement()) {
            logger.debug("executing statement: {}", dropStatement);
            return s.executeUpdate(dropStatement);
        }
    }

    protected abstract int getAdditionalColumnsNum();
    private int getColumnNum() {
        return COLLUMNS_NUM + getAdditionalColumnsNum();
    }

    protected void createIndex(final String[] columns, final String tableName)
            throws DataAccessException
    {
        jdbcTemplate.execute(new ConnectionCallback<Void>()
        {
            @Override
            public Void doInConnection(Connection con) throws SQLException, DataAccessException
            {
                Set<String> indexedColumns = getExistingIndexes(con);
                createNewIndexes(con, indexedColumns);
                return null;
            }

            private void createNewIndexes(Connection con, Set<String> indexedColumns)
                    throws SQLException
            {
                for (String column : columns) {
                    column = column.toLowerCase();
                    if (!indexedColumns.contains(column)) {
                        String indexName = tableName.toLowerCase() + "_" + column + INDEX_SUFFIX;
                        createIndex(con, indexName, tableName, column);
                    }
                }
            }

            private Set<String> getExistingIndexes(Connection con) throws SQLException
            {
                Set<String> indexedColumns = new HashSet<>();
                DatabaseMetaData md = con.getMetaData();
                String tableNameAsStored = getIdentifierAsStored(md, tableName);
                try (ResultSet rs = md.getIndexInfo(null, null, tableNameAsStored, false, false)) {
                    while (rs.next()) {
                        indexedColumns.add(rs.getString("column_name").toLowerCase());
                    }
                }
                return indexedColumns;
            }
        });
    }

    protected void createIndex(final String indexname, final String expression, final String tableName)
                    throws DataAccessException
    {
        jdbcTemplate.execute(new ConnectionCallback<Void>()
        {
            @Override
            public Void doInConnection(Connection con) throws SQLException, DataAccessException
            {
                DatabaseMetaData dbMetaData = con.getMetaData();
                ResultSet index_rset =
                        dbMetaData.getIndexInfo(null,
                                null,
                                getIdentifierAsStored(dbMetaData, tableName),
                                false,
                                false);

                while (index_rset.next()) {
                    String s = index_rset.getString("index_name").toLowerCase();
                    if (indexname.equalsIgnoreCase(s)) {
                        logger.debug("index {} already exists", indexname);
                        return null;
                    }
                }
                createIndex(con, indexname, tableName, expression);
                return null;
            }
        });
    }

    private void createIndex(
            final Connection con,
            final String indexName,
            final String tableName,
            final String columnOrExpression) throws SQLException
    {
        String createIndexStatementText = "CREATE INDEX "+indexName+
                " ON "+tableName+" ("+columnOrExpression+")";
        logger.debug("Executing {}", createIndexStatementText);
        try (Statement createIndexStatement = con.createStatement()) {
            createIndexStatement.executeUpdate(createIndexStatementText);
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
        String sql = "DELETE from " + getTableName() + " WHERE CREATIONTIME + LIFETIME < ?";
        while (true) {
            try {
                Thread.sleep(update_period);
            }
            catch(InterruptedException ie) {
                logger.info("database update thread interrupted");
                return;
            }
            long currenttime = System.currentTimeMillis();
            long cutout_expiration_time = currenttime - history_lifetime;
            jdbcTemplate.update(sql, cutout_expiration_time);
        }
    }

    protected PreparedStatement getPreparedStatement(
            Connection connection,
            String query,
            Object... args)
            throws SQLException
    {
        PreparedStatement stmt = connection.prepareStatement(query);
        for (int i = 0; i < args.length; i++) {
            stmt.setObject(i + 1, args[i]);
        }
        return stmt;
    }
}

