/*
 * GetRequestStorage.java
 *
 * Created on June 22, 2004, 2:48 PM
 */

package org.dcache.srm.request.sql;

import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;

import javax.annotation.Nonnull;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;

import org.dcache.srm.SRMUser;
import org.dcache.srm.SRMUserPersistenceManager;
import org.dcache.srm.request.Job;
import org.dcache.srm.request.PutFileRequest;
import org.dcache.srm.request.PutRequest;
import org.dcache.srm.util.Configuration;

/**
 *
 * @author  timur
 */
public class PutRequestStorage extends DatabaseContainerRequestStorage<PutRequest,PutFileRequest> {
   private static final Logger LOGGER =
            LoggerFactory.getLogger(PutRequestStorage.class);

     public static final String TABLE_NAME ="putrequests";
    private static final String UPDATE_PREFIX = "UPDATE " + TABLE_NAME + " SET "+
        "NEXTJOBID=?, " +
        "CREATIONTIME=?,  " +
        "LIFETIME=?, " +
        "STATE=?, " +
        "ERRORMESSAGE=?, " +//5
        "SCHEDULERID=?, " +
        "SCHEDULERTIMESTAMP=?," +
        "NUMOFRETR=?," +
        "LASTSTATETRANSITIONTIME=? ";

    private static final String INSERT_SQL = "INSERT INTO "+ TABLE_NAME+ "(    " +
        "ID ,"+
        "NEXTJOBID ,"+
        "CREATIONTIME ,"+
        "LIFETIME ,"+
        "STATE ,"+ //5
        "ERRORMESSAGE ,"+
        "SCHEDULERID ,"+
        "SCHEDULERTIMESTAMP ,"+
        "NUMOFRETR ,"+
        "LASTSTATETRANSITIONTIME,"+ // 10
         //Database Request Storage
        "RETRYDELTATIME , "+
        "SHOULDUPDATERETRYDELTATIME ,"+
        "DESCRIPTION ,"+
        "CLIENTHOST ,"+ // 15
        "STATUSCODE ,"+
        "USERID  ) " +
        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

    @Override
    public PreparedStatement getCreateStatement(Connection connection, Job job) throws SQLException {
        PutRequest pr = (PutRequest)job;
        return getPreparedStatement(connection,
                                    INSERT_SQL,
                                    pr.getId(),
                                    pr.getNextJobId(),
                                    pr.getCreationTime(),
                                    pr.getLifetime(),
                                    pr.getState().getStateId(),//5
                                    pr.latestHistoryEvent(),
                                    pr.getSchedulerId(),
                                    pr.getSchedulerTimeStamp(),
                                    0, // num of retries
                                    pr.getLastStateTransitionTime(), // 10
                                    //Database Request Storage
                                    pr.getRetryDeltaTime(),
                                  pr.isShould_updateretryDeltaTime()?0:1,
                                    pr.getDescription(),
                                    pr.getClient_host(),
                                    pr.getStatusCodeString(),
                                    pr.getUser().getId());
    }

    private static final String UPDATE_REQUEST_SQL =
            UPDATE_PREFIX + ", RETRYDELTATIME=?," +
                " SHOULDUPDATERETRYDELTATIME=?," +
                " DESCRIPTION=?," +
                " CLIENTHOST=?," +
                " STATUSCODE=?," +
                " USERID=?" +
                " WHERE ID=?";
    @Override
    public PreparedStatement getUpdateStatement(Connection connection,
            Job job) throws SQLException {
        PutRequest pr = (PutRequest)job;
        return getPreparedStatement(
                                  connection,
                                  UPDATE_REQUEST_SQL,
                                  pr.getNextJobId(),
                                  pr.getCreationTime(),
                                  pr.getLifetime(),
                                  pr.getState().getStateId(),
                                  pr.latestHistoryEvent(),//5
                                  pr.getSchedulerId(),
                                  pr.getSchedulerTimeStamp(),
                                  0, // num of retries
                                  pr.getLastStateTransitionTime(),
                                  //Database Request Storage
                                  pr.getRetryDeltaTime(), // 10
                                  pr.isShould_updateretryDeltaTime()?0:1,
                                  pr.getDescription(),
                                  pr.getClient_host(),
                                  pr.getStatusCodeString(),
                                  pr.getUser().getId(),
                                  pr.getId());
    }


    /** Creates a new instance of GetRequestStorage */
    public PutRequestStorage(@Nonnull String srmId,
            Configuration.DatabaseParameters configuration,
            ScheduledExecutorService executor, SRMUserPersistenceManager manager)
            throws DataAccessException
    {
        super(srmId, configuration, executor, manager);
    }

    private String getProtocolsTableName() {
        return getTableName()+"_protocols";
    }

    @Override
    protected PutRequest getContainerRequest(
            Connection _con,
            long ID,
            Long NEXTJOBID,
            long CREATIONTIME,
            long LIFETIME,
            int STATE,
            SRMUser user,
            String SCHEDULERID,
            long SCHEDULER_TIMESTAMP,
            int NUMOFRETR,
            long LASTSTATETRANSITIONTIME,
            Long CREDENTIALID,
            int RETRYDELTATIME,
            boolean SHOULDUPDATERETRYDELTATIME,
            String DESCRIPTION,
            String CLIENTHOST,
            String STATUSCODE,
            ImmutableList<PutFileRequest> fileRequests,
            ResultSet set,
            int next_index)throws SQLException {
        String sqlStatementString = "SELECT PROTOCOL FROM " + getProtocolsTableName() +
                " WHERE RequestID="+ID;
        Statement sqlStatement = _con.createStatement();
        LOGGER.debug("executing statement: {}", sqlStatementString);
        ResultSet fileIdsSet = sqlStatement.executeQuery(sqlStatementString);
        List<String> protocols = new ArrayList<>();
        while(fileIdsSet.next()) {
            protocols.add(fileIdsSet.getString(1));
        }
        sqlStatement.close();
        Job.JobHistory[] jobHistoryArray =
                getJobHistory(ID,_con);
        return new  PutRequest(
                srmId,
                ID,
                NEXTJOBID,
                CREATIONTIME,
                LIFETIME,
                STATE,
                user,
                SCHEDULERID,
                SCHEDULER_TIMESTAMP,
                NUMOFRETR,
                LASTSTATETRANSITIONTIME,
                jobHistoryArray,
                fileRequests,
                RETRYDELTATIME,
                SHOULDUPDATERETRYDELTATIME,
                DESCRIPTION,
                CLIENTHOST,
                STATUSCODE,
                protocols);

    }

    @Override
    public String getTableName() {
        return TABLE_NAME;
    }

    private final String insertProtocols =
        "INSERT INTO "+getProtocolsTableName()+
        " (PROTOCOL, RequestID) "+
        " VALUES (?,?)";

    @Override
    public PreparedStatement getBatchCreateStatement(Connection connection, Job job)
            throws SQLException {
        if (job == null || !(job instanceof PutRequest)) {
            throw new IllegalArgumentException("Request is not PutRequest" );
        }
        PutRequest pr = (PutRequest) job;
        String[] protocols = pr.getProtocols();
        if (protocols == null) {
            return null;
        }
        PreparedStatement statement = connection.prepareStatement(insertProtocols);
        for (String protocol : protocols) {
            statement.setString(1, protocol);
            statement.setLong(2, pr.getId());
            statement.addBatch();
        }
        return statement;
    }

    @Override
    public String getFileRequestsTableName() {
        return PutFileRequestStorage.TABLE_NAME;
    }
}
