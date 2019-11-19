package org.dcache.srm.request.sql;

import com.google.common.collect.ImmutableList;
import org.springframework.dao.DataAccessException;

import javax.annotation.Nonnull;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.ScheduledExecutorService;

import org.dcache.srm.SRMUser;
import org.dcache.srm.SRMUserPersistenceManager;
import org.dcache.srm.request.Job;
import org.dcache.srm.request.LsFileRequest;
import org.dcache.srm.request.LsRequest;
import org.dcache.srm.util.Configuration;

public class LsRequestStorage extends DatabaseContainerRequestStorage<LsRequest,LsFileRequest> {
    public static final String TABLE_NAME ="lsrequests";

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
        "CLIENTHOST ,"+ //15
        "STATUSCODE ,"+
        "USERID , " +
        // LS REQUEST
        "EXPLANATION ,"+
        "LONGFORMAT ,"+
        "NUMOFLEVELS ,"+
        "CNT ,"+
        "LSOFFSET ) "+
        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

    @Override
    public PreparedStatement getCreateStatement(Connection connection, Job job) throws SQLException {
        LsRequest lr = (LsRequest)job;
        return getPreparedStatement(connection,
                                    INSERT_SQL,
                                    lr.getId(),
                                    lr.getNextJobId(),
                                    lr.getCreationTime(),
                                    lr.getLifetime(),
                                    lr.getState().getStateId(),//5
                                    lr.latestHistoryEvent(),
                                    lr.getSchedulerId(),
                                    lr.getSchedulerTimeStamp(),
                                    0, // num of retries
                                    lr.getLastStateTransitionTime(), // 10
                                    //Database Request Storage
                                    lr.getRetryDeltaTime(),
                                    lr.isShould_updateretryDeltaTime()?0:1,
                                    lr.getDescription(),
                                    lr.getClient_host(),
                                    lr.getStatusCodeString(),
                                    lr.getUser().getId(),
                                    lr.getExplanation(),
                                  lr.getLongFormat() ?1:0,
                                    lr.getNumOfLevels(),
                                    lr.getCount(),
                                    lr.getOffset()
                                  );
    }

    private static final String UPDATE_REQUEST_SQL =
            UPDATE_PREFIX + ", RETRYDELTATIME=?," +
                " SHOULDUPDATERETRYDELTATIME=?," +
                " DESCRIPTION=?," +
                " CLIENTHOST=?," +
                " STATUSCODE=?," +
                " USERID=?," +
                // LS REQUEST
                " EXPLANATION=?,"+
                " LONGFORMAT=?,"+
                " NUMOFLEVELS=?,"+
                " CNT=?,"+
                " LSOFFSET=? "+
                " WHERE ID=?";
    @Override
    public PreparedStatement getUpdateStatement(Connection connection,
            Job job) throws SQLException {
        LsRequest lr = (LsRequest)job;
        return getPreparedStatement(connection,
                                    UPDATE_REQUEST_SQL,
                                    lr.getNextJobId(),
                                    lr.getCreationTime(),
                                    lr.getLifetime(),
                                    lr.getState().getStateId(),
                                    lr.latestHistoryEvent(),//5
                                    lr.getSchedulerId(),
                                    lr.getSchedulerTimeStamp(),
                                    0, // num of retries
                                    lr.getLastStateTransitionTime(),
                                    //Database Request Storage
                                    lr.getRetryDeltaTime(), // 10
                                    lr.isShould_updateretryDeltaTime()?0:1,
                                    lr.getDescription(),
                                    lr.getClient_host(),
                                    lr.getStatusCodeString(),
                                    lr.getUser().getId(),
                                    lr.getExplanation(),
                                  lr.getLongFormat() ?1:0,
                                    lr.getNumOfLevels(),
                                    lr.getCount(),
                                    lr.getOffset(),
                                    lr.getId());
    }

        public LsRequestStorage(@Nonnull String srmId,
                Configuration.DatabaseParameters configuration,
                ScheduledExecutorService executor, SRMUserPersistenceManager manager)
                throws DataAccessException
        {
                super(srmId, configuration, executor, manager);
        }

        @Override
        protected LsRequest getContainerRequest(Connection connection,
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
                                                       ImmutableList<LsFileRequest> fileRequests,
                                                       ResultSet set,
                                                       int next_index) throws SQLException {
                Job.JobHistory[] jobHistoryArray = getJobHistory(ID,connection);
                String explanation=set.getString(next_index++);
                boolean longFormat=set.getInt(next_index++)==1;
                int numOfLevels=set.getInt(next_index++);
                int count=set.getInt(next_index++);
                int offset=set.getInt(next_index++);
                return new  LsRequest(srmId,
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
                                      explanation,
                                      longFormat,
                                      numOfLevels,
                                      count,
                                      offset);
        }

    @Override
        public String getTableName() {
                return TABLE_NAME;
        }

        @Override
        public String getFileRequestsTableName() {
                return LsFileRequestStorage.TABLE_NAME;
        }
}
