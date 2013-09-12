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
        "MAXNUMOFRETR=?," +
        "LASTSTATETRANSITIONTIME=? ";//10

    public PreparedStatement getStatement(Connection connection,
                                          String query,
                                          Job job) throws SQLException {
        GetFileRequest request = (GetFileRequest)job;
        PreparedStatement stmt = getPreparedStatement(connection,
                                                      query,
                                                      request.getNextJobId(),
                                                      request.getCreationTime(),
                                                      request.getLifetime(),
                                                      request.getState().getStateId(),
                                                      request.getErrorMessage(),
                                                      request.getSchedulerId(),
                                                      request.getSchedulerTimeStamp(),
                                                      request.getNumberOfRetries(),
                                                      request.getMaxNumberOfRetries(),
                                                      request.getLastStateTransitionTime(),
                                                      request.getRequestId(),
                                                      request.getCredentialId(),
                                                      request.getStatusCodeString(),
                                                      request.getSurlString(),
                                                      request.getTurlString(),
                                                      request.getFileId(),
                                                      request.getPinId(),
                                                      request.getId());
        return stmt;
    }

    private static final String UPDATE_REQUEST_SQL =
            UPDATE_PREFIX +
            ", REQUESTID=?, "+
            "CREDENTIALID=?, "+
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
            "MAXNUMOFRETR ,"+ //10
            "LASTSTATETRANSITIONTIME,"+
            //DATABASE FILE REQUEST STORAGE
            "REQUESTID , " +
            "CREDENTIALID , "+
            "STATUSCODE , "+
            "SURL ,"+
            "TURL ,"+
            "FILEID ,"+
            "PINID ) "+
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

    @Override
    public PreparedStatement getCreateStatement(Connection connection,
                                                Job job)
        throws SQLException {
        if(job == null || !(job instanceof GetFileRequest)) {
            throw new IllegalArgumentException("job is not GetFileRequest" );
        }
        GetFileRequest request = (GetFileRequest)job;
        PreparedStatement stmt = getPreparedStatement(connection,
                                                      INSERT_SQL,
                                                      request.getId(),
                                                      request.getNextJobId(),
                                                      request.getCreationTime(),
                                                      request.getLifetime(),
                                                      request.getState().getStateId(),
                                                      request.getErrorMessage(),
                                                      request.getSchedulerId(),
                                                      request.getSchedulerTimeStamp(),
                                                      request.getNumberOfRetries(),
                                                      request.getMaxNumberOfRetries(),
                                                      request.getLastStateTransitionTime(),
                                                      request.getRequestId(),
                                                      request.getCredentialId(),
                                                      request.getStatusCodeString(),
                                                      request.getSurlString(),
                                                      request.getTurlString(),
                                                      request.getFileId(),
                                                      request.getPinId());
        return stmt;
    }

    /** Creates a new instance of GetFileRequestStorage */
    public GetFileRequestStorage(Configuration.DatabaseParameters configuration)
            throws DataAccessException
    {
        super(configuration);
    }

    @Override
    protected GetFileRequest getFileRequest(Connection _con,
        long ID,
        Long NEXTJOBID,
        long CREATIONTIME,
        long LIFETIME,
        int STATE,
        String ERRORMESSAGE,
        String SCHEDULERID,
        long SCHEDULER_TIMESTAMP,
        int NUMOFRETR,
        int MAXNUMOFRETR,
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
            ERRORMESSAGE,
            SCHEDULERID,
            SCHEDULER_TIMESTAMP,
            NUMOFRETR,
            MAXNUMOFRETR,
            LASTSTATETRANSITIONTIME,
            jobHistoryArray,
            REQUESTID,
            CREDENTIALID,
            STATUSCODE,
            SURL,
            TURL,
            FILEID,
            PINID);
    }

    @Override
    public String getFileRequestCreateTableFields() {
        return
        ","+
        "SURL "+  stringType+
        ","+
        "TURL "+  stringType+
        ","+
        "FILEID "+  stringType+
        ","+
        "PINID "+  stringType;
    }

    private static int ADDITIONAL_FIELDS = 4;

    @Override
    public String getTableName() {
        return TABLE_NAME;
    }

     @Override
     public String getRequestTableName() {
         return GetRequestStorage.TABLE_NAME;
     }

     @Override
     protected void __verify(int nextIndex, int columnIndex, String tableName, String columnName, int columnType) throws SQLException {
         /*
          *       "SURL "+  stringType+
        ","+
        "TURL "+  stringType+
        ","+
        "FILEID "+  stringType+
        ","+
        "PINID "+  stringType;
         */
        if(columnIndex == nextIndex) {
            verifyStringType("SURL",columnIndex,tableName, columnName, columnType);
        }
        else if(columnIndex == nextIndex+1)
        {
            verifyStringType("TURL",columnIndex,tableName, columnName, columnType);

        }
        else if(columnIndex == nextIndex+2)
        {
            verifyStringType("FILEID",columnIndex,tableName, columnName, columnType);
        }
        else if(columnIndex == nextIndex+3)
        {
            verifyStringType("PINID",columnIndex,tableName, columnName, columnType);
        }
        else {
            throw new SQLException("database table schema changed:"+
                    "table named "+tableName+
                    " column #"+columnIndex+" has name \""+columnName+
                    "\"  has type \""+getTypeName(columnType)+
                    " this column should not be present!!!");
        }
     }

    @Override
    protected int getMoreCollumnsNum() {
         return ADDITIONAL_FIELDS;
     }

}
