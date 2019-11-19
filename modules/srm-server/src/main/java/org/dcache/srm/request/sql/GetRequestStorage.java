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
import org.dcache.srm.request.GetFileRequest;
import org.dcache.srm.request.GetRequest;
import org.dcache.srm.request.Job;
import org.dcache.srm.util.Configuration;

/**
 *
 * @author  timur
 */
public class GetRequestStorage extends DatabaseContainerRequestStorage<GetRequest,GetFileRequest> {
    private static final Logger LOGGER =
            LoggerFactory.getLogger(GetRequestStorage.class);
    public static final String TABLE_NAME ="getrequests";
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
        GetRequest gr = (GetRequest)job;
        return getPreparedStatement(connection,
                                    INSERT_SQL,
                                    gr.getId(),
                                    gr.getNextJobId(),
                                    gr.getCreationTime(),
                                    gr.getLifetime(),
                                    gr.getState().getStateId(),//5
                                    gr.latestHistoryEvent(),
                                    gr.getSchedulerId(),
                                    gr.getSchedulerTimeStamp(),
                                    0, // num of retries
                                    gr.getLastStateTransitionTime(), // 10
                                    //Database Request Storage
                                    gr.getRetryDeltaTime(),
                                  gr.isShould_updateretryDeltaTime()?0:1,
                                    gr.getDescription(),
                                    gr.getClient_host(),
                                    gr.getStatusCodeString(),
                                    gr.getUser().getId());
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
        GetRequest gr = (GetRequest)job;
        return getPreparedStatement(connection,
                                    UPDATE_REQUEST_SQL,
                                    gr.getNextJobId(),
                                    gr.getCreationTime(),
                                    gr.getLifetime(),
                                    gr.getState().getStateId(),
                                    gr.latestHistoryEvent(),//5
                                    gr.getSchedulerId(),
                                    gr.getSchedulerTimeStamp(),
                                    0, // num of retries
                                    gr.getLastStateTransitionTime(),
                                    //Database Request Storage
                                    gr.getRetryDeltaTime(), // 10
                                  gr.isShould_updateretryDeltaTime()?0:1,
                                    gr.getDescription(),
                                    gr.getClient_host(),
                                    gr.getStatusCodeString(),
                                    gr.getUser().getId(),
                                    gr.getId());
    }


    /** Creates a new instance of GetRequestStorage */
    public GetRequestStorage(@Nonnull String srmId,
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
    protected GetRequest getContainerRequest(
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
            ImmutableList<GetFileRequest> fileRequests,
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
        return new  GetRequest(
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
    public PreparedStatement getBatchCreateStatement(Connection connection, Job job) throws SQLException {
        if (job == null || !(job instanceof GetRequest)) {
            throw new IllegalArgumentException("Request is not GetRequest" );
        }
        GetRequest gr = (GetRequest)job;
        String[] protocols = gr.getProtocols();
        if (protocols == null) {
            return null;
        }
        PreparedStatement statement = connection.prepareStatement(insertProtocols);
        for (String protocol : protocols) {
            statement.setString(1, protocol);
            statement.setLong(2, gr.getId());
            statement.addBatch();
        }
        return statement;
    }

    @Override
    public String getFileRequestsTableName() {
        return GetFileRequestStorage.TABLE_NAME;
    }
}
