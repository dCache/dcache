/*
 * PutFileRequestStorage.java
 *
 * Created on June 17, 2004, 4:49 PM
 */

package org.dcache.srm.request.sql;

import org.springframework.dao.DataAccessException;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.dcache.srm.request.Job;
import org.dcache.srm.request.PutFileRequest;
import org.dcache.srm.util.Configuration;
import org.dcache.srm.v2_2.TAccessLatency;
import org.dcache.srm.v2_2.TRetentionPolicy;

/**
 *
 * @author  timur
 */
public class PutFileRequestStorage extends DatabaseFileRequestStorage<PutFileRequest> {
    public static final String TABLE_NAME="putfilerequests";

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
        PutFileRequest request = (PutFileRequest)job;
        TRetentionPolicy retentionPolicy = request.getRetentionPolicy();
        TAccessLatency  accessLatency = request.getAccessLatency();
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
                                                      request.getParentFileId(),
                                                      request.getSpaceReservationId(),
                                                      request.getSize(),
                                                      (retentionPolicy!=null? retentionPolicy.getValue():null),
                                                      (accessLatency!=null? accessLatency.getValue():null),
                                                      request.getId());
        return stmt;
    }

    private static final String UPDATE_REQUEST_SQL =
            UPDATE_PREFIX + ", REQUESTID=?, "+
            "CREDENTIALID=?, "+
            "STATUSCODE=?, "+
            "SURL=?, "+
            "TURL=? ,"+
            "FILEID=? ,"+
            "PARENTFILEID=? ,"+
            "SPACERESERVATIONID=? ,"+
            "SIZE=? ,"+
            "RETENTIONPOLICY=? ,"+
            "ACCESSLATENCY=? "+
            "WHERE ID=? ";
    @Override
    public PreparedStatement getUpdateStatement(Connection connection,
                                                Job job)
        throws SQLException {
        if(job == null || !(job instanceof PutFileRequest)) {
            throw new IllegalArgumentException("job is not PutFileRequest" );
        }
        PutFileRequest request = (PutFileRequest)job;
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
            "PARENTFILEID ,"+
            "SPACERESERVATIONID ,"+
            "SIZE ,"+
            "RETENTIONPOLICY ," +
            "ACCESSLATENCY )"+
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

    @Override
    public PreparedStatement getCreateStatement(Connection connection,
                                                Job job)
        throws SQLException {
        if(job == null || !(job instanceof PutFileRequest)) {
            throw new IllegalArgumentException("fr is not PutFileRequest" );
        }
        PutFileRequest request = (PutFileRequest)job;
        TRetentionPolicy retentionPolicy = request.getRetentionPolicy();
        TAccessLatency  accessLatency = request.getAccessLatency();
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
                                                      request.getParentFileId(),
                                                      request.getSpaceReservationId(),
                                                      request.getSize(),
                                                      (retentionPolicy!=null? retentionPolicy.getValue():null),
                                                      (accessLatency!=null? accessLatency.getValue():null));
        return stmt;
    }

   /** Creates a new instance of PutFileRequestStorage */
    public PutFileRequestStorage(Configuration.DatabaseParameters configuration)
            throws DataAccessException
    {
        super(configuration);
    }

    @Override
    protected PutFileRequest getFileRequest(
    Connection _con,
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
    int next_index)throws SQLException {
        String SURL = set.getString(next_index++);
        String TURL = set.getString(next_index++);
        String FILEID = set.getString(next_index++);
        String PARENTFILEID = set.getString(next_index++);
        String SPACERESERVATIONID = set.getString(next_index++);
        long SIZE = set.getLong(next_index++);
        String RETENTIONPOLICY = set.getString(next_index++);
        String ACCESSLATENCY = set.getString(next_index++);
        TRetentionPolicy retentionPolicy =
                RETENTIONPOLICY == null || RETENTIONPOLICY.equalsIgnoreCase("null") ?
                    null:TRetentionPolicy.fromString(RETENTIONPOLICY);
        TAccessLatency accessLatency =
                ACCESSLATENCY == null || ACCESSLATENCY.equalsIgnoreCase("null") ?
                    null:TAccessLatency.fromString(ACCESSLATENCY);

        Job.JobHistory[] jobHistoryArray =
        getJobHistory(ID,_con);
        return new PutFileRequest(
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
        PARENTFILEID,
        SPACERESERVATIONID,
        SIZE,
        retentionPolicy,
        accessLatency);
    }

    @Override
    public String getFileRequestCreateTableFields() {
        return
        ", "+
        "SURL "+  stringType+
        ", "+
        "TURL "+  stringType+
        ", "+
        "FILEID "+  stringType+
        ", "+
        "PARENTFILEID "+  stringType+
        ", "+
        "SPACERESERVATIONID "+  stringType+
        ", "+
        "SIZE "+ longType+
        ", "+
        "RETENTIONPOLICY "+ stringType+
        ", "+
        "ACCESSLATENCY "+ stringType;
    }

   private static int ADDITIONAL_FIELDS = 8;


    @Override
    public String getTableName() {
        return TABLE_NAME;
    }

    @Override
    public String getRequestTableName() {
         return PutRequestStorage.TABLE_NAME;
    }

    @Override
    protected void __verify(int nextIndex, int columnIndex, String tableName, String columnName, int columnType) throws SQLException {
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
            verifyStringType("PARENTFILEID",columnIndex,tableName, columnName, columnType);
        }
        else if(columnIndex == nextIndex+4)
        {
            verifyStringType("SPACERESERVATIONID",columnIndex,tableName, columnName, columnType);
        }
        else if(columnIndex == nextIndex+5)
        {
            verifyLongType("SIZE",columnIndex,tableName, columnName, columnType);
        }
        else if(columnIndex == nextIndex+6)
        {
            verifyStringType("RETENTIONPOLICY",columnIndex,tableName, columnName, columnType);
        }
        else if(columnIndex == nextIndex+7)
        {
            verifyStringType("ACCESSLATENCY",columnIndex,tableName, columnName, columnType);
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
