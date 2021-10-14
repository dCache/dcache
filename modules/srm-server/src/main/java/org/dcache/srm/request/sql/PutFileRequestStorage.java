/*
 * PutFileRequestStorage.java
 *
 * Created on June 17, 2004, 4:49 PM
 */

package org.dcache.srm.request.sql;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.ScheduledExecutorService;
import org.dcache.srm.request.Job;
import org.dcache.srm.request.PutFileRequest;
import org.dcache.srm.util.Configuration;
import org.dcache.srm.v2_2.TAccessLatency;
import org.dcache.srm.v2_2.TRetentionPolicy;
import org.springframework.dao.DataAccessException;

/**
 * @author timur
 */
public class PutFileRequestStorage extends DatabaseFileRequestStorage<PutFileRequest> {

    public static final String TABLE_NAME = "putfilerequests";

    private static final String UPDATE_PREFIX = "UPDATE " + TABLE_NAME + " SET " +
          "NEXTJOBID=?, " +
          "CREATIONTIME=?,  " +
          "LIFETIME=?, " +
          "STATE=?, " +
          "ERRORMESSAGE=?, " +//5
          "SCHEDULERID=?, " +
          "SCHEDULERTIMESTAMP=?," +
          "NUMOFRETR=?," +
          "LASTSTATETRANSITIONTIME=? ";


    private PreparedStatement getStatement(Connection connection,
          String query,
          Job job) throws SQLException {
        PutFileRequest request = (PutFileRequest) job;
        TRetentionPolicy retentionPolicy = request.getRetentionPolicy();
        TAccessLatency accessLatency = request.getAccessLatency();
        return getPreparedStatement(connection,
              query,
              request.getNextJobId(),
              request.getCreationTime(),
              request.getLifetime(),
              request.getState().getStateId(),
              request.latestHistoryEvent(),
              request.getSchedulerId(),
              request.getSchedulerTimeStamp(),
              0, // num of retries
              request.getLastStateTransitionTime(),
              request.getRequestId(),
              request.getStatusCodeString(),
              request.getSurlString(),
              request.getTurlString(),
              request.getFileId(),
              null, // parentFileId (unused)
              request.getSpaceReservationId(),
              request.getSize(),
              (retentionPolicy != null ? retentionPolicy.getValue() : null),
              (accessLatency != null ? accessLatency.getValue() : null),
              request.getId());
    }

    private static final String UPDATE_REQUEST_SQL =
          UPDATE_PREFIX + ", REQUESTID=?, " +
                "STATUSCODE=?, " +
                "SURL=?, " +
                "TURL=? ," +
                "FILEID=? ," +
                "PARENTFILEID=? ," +
                "SPACERESERVATIONID=? ," +
                "SIZE=? ," +
                "RETENTIONPOLICY=? ," +
                "ACCESSLATENCY=? " +
                "WHERE ID=? ";

    @Override
    public PreparedStatement getUpdateStatement(Connection connection,
          Job job)
          throws SQLException {
        if (job == null || !(job instanceof PutFileRequest)) {
            throw new IllegalArgumentException("job is not PutFileRequest");
        }
        PutFileRequest request = (PutFileRequest) job;
        return getStatement(connection, UPDATE_REQUEST_SQL, request);
    }

    private static final String INSERT_SQL = "INSERT INTO " + TABLE_NAME + "(    " +
          "ID ," +
          "NEXTJOBID ," +
          "CREATIONTIME ," +
          "LIFETIME ," +
          "STATE ," + //5
          "ERRORMESSAGE ," +
          "SCHEDULERID ," +
          "SCHEDULERTIMESTAMP ," +
          "NUMOFRETR ," +
          "LASTSTATETRANSITIONTIME," + // 10
          //DATABASE FILE REQUEST STORAGE
          "REQUESTID , " +
          "STATUSCODE , " +
          "SURL ," +
          "TURL ," +
          "FILEID ," +
          "PARENTFILEID ," +
          "SPACERESERVATIONID ," +
          "SIZE ," +
          "RETENTIONPOLICY ," +
          "ACCESSLATENCY )" +
          "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

    @Override
    public PreparedStatement getCreateStatement(Connection connection,
          Job job)
          throws SQLException {
        if (job == null || !(job instanceof PutFileRequest)) {
            throw new IllegalArgumentException("fr is not PutFileRequest");
        }
        PutFileRequest request = (PutFileRequest) job;
        TRetentionPolicy retentionPolicy = request.getRetentionPolicy();
        TAccessLatency accessLatency = request.getAccessLatency();
        return getPreparedStatement(connection,
              INSERT_SQL,
              request.getId(),
              request.getNextJobId(),
              request.getCreationTime(),
              request.getLifetime(),
              request.getState().getStateId(),
              request.latestHistoryEvent(),
              request.getSchedulerId(),
              request.getSchedulerTimeStamp(),
              0, // num of retries
              request.getLastStateTransitionTime(),
              request.getRequestId(),
              request.getStatusCodeString(),
              request.getSurlString(),
              request.getTurlString(),
              request.getFileId(),
              null, // parentFileId (unused)
              request.getSpaceReservationId(),
              request.getSize(),
              (retentionPolicy != null ? retentionPolicy.getValue() : null),
              (accessLatency != null ? accessLatency.getValue() : null));
    }

    /**
     * Creates a new instance of PutFileRequestStorage
     */
    public PutFileRequestStorage(Configuration.DatabaseParameters configuration,
          ScheduledExecutorService executor)
          throws DataAccessException {
        super(configuration, executor);
    }

    @Override
    protected PutFileRequest getFileRequest(
          Connection _con,
          long ID,
          Long NEXTJOBID,
          long CREATIONTIME,
          long LIFETIME,
          int STATE,
          String SCHEDULERID,
          long SCHEDULER_TIMESTAMP,
          int NUMOFRETR,
          long LASTSTATETRANSITIONTIME,
          long REQUESTID,
          Long CREDENTIALID,
          String STATUSCODE,
          ResultSet set,
          int next_index) throws SQLException {
        String SURL = set.getString(next_index++);
        String TURL = set.getString(next_index++);
        String FILEID = set.getString(next_index++);
        String PARENTFILEID = set.getString(next_index++);
        String SPACERESERVATIONID = set.getString(next_index++);
        Long SIZE = set.getLong(next_index++);
        if (set.wasNull()) {
            SIZE = null;
        }
        String RETENTIONPOLICY = set.getString(next_index++);
        String ACCESSLATENCY = set.getString(next_index++);
        TRetentionPolicy retentionPolicy =
              RETENTIONPOLICY == null || RETENTIONPOLICY.equalsIgnoreCase("null") ?
                    null : TRetentionPolicy.fromString(RETENTIONPOLICY);
        TAccessLatency accessLatency =
              ACCESSLATENCY == null || ACCESSLATENCY.equalsIgnoreCase("null") ?
                    null : TAccessLatency.fromString(ACCESSLATENCY);

        Job.JobHistory[] jobHistoryArray =
              getJobHistory(ID, _con);
        return new PutFileRequest(
              ID,
              NEXTJOBID,
              CREATIONTIME,
              LIFETIME,
              STATE,
              SCHEDULERID,
              SCHEDULER_TIMESTAMP,
              NUMOFRETR,
              LASTSTATETRANSITIONTIME,
              jobHistoryArray,
              REQUESTID,
              STATUSCODE,
              SURL,
              TURL,
              FILEID,
              SPACERESERVATIONID,
              SIZE,
              retentionPolicy,
              accessLatency);
    }

    @Override
    public String getTableName() {
        return TABLE_NAME;
    }
}
