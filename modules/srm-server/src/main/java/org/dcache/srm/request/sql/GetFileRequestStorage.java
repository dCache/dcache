/*
 * GetFileRequestStorage.java
 *
 * Created on June 17, 2004, 4:49 PM
 */

package org.dcache.srm.request.sql;

import org.springframework.dao.DataAccessException;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.ScheduledExecutorService;

import org.dcache.srm.request.GetFileRequest;
import org.dcache.srm.request.Job;
import org.dcache.srm.util.Configuration;

/**
 *
 * @author  timur
 */
public class GetFileRequestStorage extends DatabaseFileRequestStorage<GetFileRequest> {
    public static final String TABLE_NAME = "getfilerequests";

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

    private PreparedStatement getStatement(Connection connection,
                                          String query,
                                          Job job) throws SQLException {
        GetFileRequest request = (GetFileRequest)job;
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
                                    request.getPinId(),
                                    request.getId());
    }

    private static final String UPDATE_REQUEST_SQL =
            UPDATE_PREFIX +
            ", REQUESTID=?, "+
            "STATUSCODE=?, "+
            "SURL=?, "+
            "TURL=? ,"+
            "FILEID=? ,"+
            "PINID=? "+
            "WHERE ID=? ";
    @Override
    public PreparedStatement getUpdateStatement(Connection connection,
                                                Job job)
        throws SQLException {
        if(job == null || !(job instanceof GetFileRequest)) {
            throw new IllegalArgumentException("fr is not GetFileRequest" );
        }
        GetFileRequest request = (GetFileRequest)job;
        return getStatement(connection,UPDATE_REQUEST_SQL, request);
    }

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
            //DATABASE FILE REQUEST STORAGE
            "REQUESTID , " +
            "STATUSCODE , "+
            "SURL ,"+
            "TURL ,"+
            "FILEID ,"+
            "PINID ) "+
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

    @Override
    public PreparedStatement getCreateStatement(Connection connection,
                                                Job job)
        throws SQLException {
        if(job == null || !(job instanceof GetFileRequest)) {
            throw new IllegalArgumentException("job is not GetFileRequest" );
        }
        GetFileRequest request = (GetFileRequest)job;
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
                                    request.getPinId());
    }

    /** Creates a new instance of GetFileRequestStorage */
    public GetFileRequestStorage(Configuration.DatabaseParameters configuration, ScheduledExecutorService executor)
            throws DataAccessException
    {
        super(configuration, executor);
    }

    @Override
    protected GetFileRequest getFileRequest(Connection _con,
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
           String PINID = set.getString(next_index);
            Job.JobHistory[] jobHistoryArray =
            getJobHistory(ID,_con);
           return new GetFileRequest(
            ID,
            NEXTJOBID ,
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
            PINID);
    }

    @Override
    public String getTableName() {
        return TABLE_NAME;
    }
}
