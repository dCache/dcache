package org.dcache.srm.request.sql;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.ScheduledExecutorService;
import org.dcache.srm.request.Job;
import org.dcache.srm.request.LsFileRequest;
import org.dcache.srm.util.Configuration;
import org.springframework.dao.DataAccessException;

public class LsFileRequestStorage extends DatabaseFileRequestStorage<LsFileRequest> {

    public static final String TABLE_NAME = "lsfilerequests";
    private static final String UPDATE_PREFIX = "UPDATE " + TABLE_NAME + " SET " +
          "NEXTJOBID=?, " +
          "CREATIONTIME=?,  " +
          "LIFETIME=?, " +
          "STATE=?, " +
          "ERRORMESSAGE=?, " +//5
          "SCHEDULERID=?, " +
          "SCHEDULERTIMESTAMP=?," +
          "NUMOFRETR=?," +
          "LASTSTATETRANSITIONTIME=? ";//10

    public LsFileRequestStorage(Configuration.DatabaseParameters configuration,
          ScheduledExecutorService executor)
          throws DataAccessException {
        super(configuration, executor);
    }

    @Override
    protected LsFileRequest getFileRequest(Connection connection,
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
        Job.JobHistory[] jobHistoryArray =
              getJobHistory(ID, connection);
        return new LsFileRequest(ID,
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
              SURL);
    }

    @Override
    public String getTableName() {
        return TABLE_NAME;
    }


    private PreparedStatement getStatement(Connection connection,
          String query,
          Job fr) throws SQLException {
        LsFileRequest gfr = (LsFileRequest) fr;
        return getPreparedStatement(connection,
              query,
              gfr.getNextJobId(),
              gfr.getCreationTime(),
              gfr.getLifetime(),
              gfr.getState().getStateId(),
              gfr.latestHistoryEvent(),
              gfr.getSchedulerId(),
              gfr.getSchedulerTimeStamp(),
              0, // num of retries
              gfr.getLastStateTransitionTime(),
              gfr.getRequestId(),
              gfr.getStatusCodeString(),
              gfr.getSurlString(),
              gfr.getId());
    }

    private static final String UPDATE_REQUEST_SQL =
          UPDATE_PREFIX +
                ", REQUESTID=?" +
                ", STATUSCODE=?" +
                ", SURL=? WHERE ID=?";

    @Override
    public PreparedStatement getUpdateStatement(Connection connection,
          Job fr)
          throws SQLException {
        if (fr == null || !(fr instanceof LsFileRequest)) {
            throw new IllegalArgumentException("fr is not LsFileRequest");
        }
        return getStatement(connection, UPDATE_REQUEST_SQL, fr);
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
          "LASTSTATETRANSITIONTIME," +
          //DATABASE FILE REQUEST STORAGE
          "REQUESTID , " +
          "STATUSCODE,  " +
          "SURL )" +
          "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)";

    @Override
    public PreparedStatement getCreateStatement(Connection connection,
          Job fr)
          throws SQLException {
        if (fr == null || !(fr instanceof LsFileRequest)) {
            throw new IllegalArgumentException("fr is not LsFileRequest");
        }
        LsFileRequest gfr = (LsFileRequest) fr;
        return getPreparedStatement(connection,
              INSERT_SQL,
              gfr.getId(),
              gfr.getNextJobId(),
              gfr.getCreationTime(),
              gfr.getLifetime(),
              gfr.getState().getStateId(),
              gfr.latestHistoryEvent(),
              gfr.getSchedulerId(),
              gfr.getSchedulerTimeStamp(),
              0, // num of retries
              gfr.getLastStateTransitionTime(),
              gfr.getRequestId(),
              gfr.getStatusCodeString(),
              gfr.getSurlString());
    }
}
