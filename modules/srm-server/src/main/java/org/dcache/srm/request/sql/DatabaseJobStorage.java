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

import com.google.common.reflect.TypeToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCreator;
import org.springframework.transaction.TransactionException;
import org.springframework.transaction.support.TransactionTemplate;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.dcache.util.SqlHelper;
import org.dcache.srm.request.Job;
import org.dcache.srm.scheduler.JobStorage;
import org.dcache.srm.scheduler.State;
import org.dcache.srm.util.Configuration;

/**
 *
 * @author  timur
 */
public abstract class DatabaseJobStorage<J extends Job> implements JobStorage<J>, Runnable {
    private static final Logger LOGGER =
            LoggerFactory.getLogger(DatabaseJobStorage.class);

    @SuppressWarnings("unchecked")
    private final Class<J> jobType = (Class<J>) new TypeToken<J>(getClass()) {}.getRawType();

    private final Configuration.DatabaseParameters configuration;
    private final ScheduledExecutorService executor;
    protected final JdbcTemplate jdbcTemplate;
    protected final TransactionTemplate transactionTemplate;
    private final boolean logHistory;

    public DatabaseJobStorage(Configuration.DatabaseParameters configuration, ScheduledExecutorService executor)
            throws DataAccessException
    {
        this.configuration = configuration;
        this.executor = executor;
        this.logHistory = configuration.isRequestHistoryDatabaseEnabled();
        this.jdbcTemplate = new JdbcTemplate(configuration.getDataSource());
        this.transactionTemplate = new TransactionTemplate(configuration.getTransactionManager());
    }

    //this should always reflect the number of field definde in the
    // prefix above
    public abstract String getTableName();

    private String getHistoryTableName() {
        return getTableName().toLowerCase()+"history";
    }

    protected abstract J getJob(
            Connection _con,
            long ID,
            Long NEXTJOBID ,
            long CREATIONTIME,
            long LIFETIME,
            int STATE,
            String SCHEDULERID,
            long SCHEDULER_TIMESTAMP,
            int NUMOFRETR,
            long LASTSTATETRANSITIONTIME,
            ResultSet set,
            int next_index) throws SQLException;

    @Override
    public void init()
    {
        executor.scheduleWithFixedDelay(this, 0, configuration.getExpiredRequestRemovalPeriod(), TimeUnit.SECONDS);
    }

    @Override
    public J getJob(final long jobId) throws DataAccessException
    {
        return jdbcTemplate.execute((Connection con) -> getJob(jobId, con));
    }

    @Override
    public J getJob(long jobId,Connection _con) throws SQLException
    {
        LOGGER.debug("executing statement: SELECT * FROM {} WHERE ID=?({})",
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
        // index 5: ERRORMESSAGE is an historic artifact, not used.
        String SCHEDULERID=set.getString(7);
        long SCHEDULER_TIMESTAMP=set.getLong(8);
        int NUMOFRETR = set.getInt(9);
        // index 10: MAXNUMOFRETR is an history artifact, not used.
        long LASTSTATETRANSITIONTIME = set.getLong(11);
        return getJob(_con,
                      ID,
                      NEXTJOBID ,
                      CREATIONTIME,
                      LIFETIME,
                      STATE,
                      SCHEDULERID,
                      SCHEDULER_TIMESTAMP,
                      NUMOFRETR,
                      LASTSTATETRANSITIONTIME,
                      set,
                      12 );
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
        history.forEach(Job.JobHistory::setSaved);
    }

    private List<Job.JobHistory> getJobHistoriesToSave(Job job)
    {
        return logHistory
                ? job.getJobHistory().stream().filter(history -> !history.isSaved()).collect(Collectors.toList())
                : Collections.emptyList();
    }


    @Override
    public void saveJob(final Job job, boolean force) throws TransactionException
    {
        List<Job.JobHistory> savedHistory =
                transactionTemplate.execute(status -> jdbcTemplate.execute((Connection con) -> {
                    List<Job.JobHistory> history;
                    PreparedStatement updateStatement = null;
                    PreparedStatement createStatement = null;
                    PreparedStatement batchCreateStatement = null;
                    try {
                        job.rlock();
                        try {
                            history = getJobHistoriesToSave(job);
                            updateStatement = getUpdateStatement(con, job);
                            createStatement = getCreateStatement(con, job);
                            batchCreateStatement = getBatchCreateStatement(con, job);
                        } finally {
                            job.runlock();
                        }

                        int rowCount = updateStatement.executeUpdate();
                        if (rowCount == 0) {
                            createStatement.executeUpdate();
                            if (batchCreateStatement != null) {
                                batchCreateStatement.executeBatch();
                            }
                        }
                        if (!history.isEmpty()) {
                            saveHistory(con, job, history);
                        }
                    } finally {
                        SqlHelper.tryToClose(createStatement);
                        SqlHelper.tryToClose(batchCreateStatement);
                        SqlHelper.tryToClose(updateStatement);
                    }
                    return history;
                }));
        markHistoryAsSaved(savedHistory);
    }

    protected PreparedStatement getBatchCreateStatement(Connection connection, Job job)
            throws SQLException
    {
        return null;
    }

    public abstract PreparedStatement getCreateStatement(Connection connection, Job job) throws SQLException;
    public abstract PreparedStatement getUpdateStatement(Connection connection, Job job) throws SQLException;

    protected Job.JobHistory[] getJobHistory(long jobId,Connection _con) throws SQLException{
        List<Job.JobHistory> l = new ArrayList<>();
        String select = "SELECT * FROM " +getHistoryTableName()+
                " WHERE JOBID="+jobId + " ORDER BY ID";
        LOGGER.debug("executing statement: {}", select);
        Statement statement = _con.createStatement();
        ResultSet set = statement.executeQuery(select);
        if(!set.next()) {
            LOGGER.debug("no history elements in table {} found, returning NULL",
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
            LOGGER.debug("found JobHistory: {}", jh);

        } while (set.next());
        statement.close();
        return l.toArray(new Job.JobHistory[l.size()]);
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
    public Set<Long> getLatestFailedJobIds(int maxNum) throws DataAccessException
    {
        return getJobIdsByCondition("STATE ="+State.FAILED.getStateId()+
                " ORDER BY ID DESC"+
                " LIMIT "+maxNum+" ");
    }

    @Override
    public Set<Long> getLatestCanceledJobIds(int maxNum) throws DataAccessException
    {
        return getJobIdsByCondition("STATE = "+State.CANCELED.getStateId()+
                " ORDER BY ID DESC"+
                " LIMIT "+maxNum+" ");
    }

    private Set<J> getJobs(PreparedStatementCreator psc) throws DataAccessException
    {
        return new HashSet<>(jdbcTemplate.query(psc, (rs, rowNum) -> {
            J job = getJob(rs.getStatement().getConnection(), rs);
            LOGGER.debug("==========> deserialized job with id {}", job.getId());
            return job;
        }));

    }

    @Override
    public Set<J> getActiveJobs() throws DataAccessException
    {
        return getJobs(connection -> {
            String sql =
                    "SELECT * FROM " + getTableName() +
                            " WHERE STATE !=" + State.DONE.getStateId() +
                            " AND STATE !=" + State.CANCELED.getStateId() +
                            " AND STATE !=" + State.FAILED.getStateId();
            return connection.prepareStatement(sql);
        });
    }

    @Override
    public void run()
    {
        long lifetime =
                TimeUnit.DAYS.toMillis(configuration.getKeepRequestHistoryPeriod());
        long timestamp = System.currentTimeMillis() - lifetime;
        try {
            jdbcTemplate.update("DELETE FROM " + getTableName() + " WHERE CREATIONTIME + LIFETIME < ?", timestamp);
        } catch (DataAccessException e) {
            LOGGER.warn("Failed to remove out-of-date historic data from {}: {}", getTableName(), e.toString());
        } catch (RuntimeException e) {
            LOGGER.error("Bug detected", e);
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
