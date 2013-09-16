/*
 * GetRequestStorage.java
 *
 * Created on June 22, 2004, 2:48 PM
 */

package org.dcache.srm.request.sql;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.dcache.srm.SRMUser;
import org.dcache.srm.request.CopyFileRequest;
import org.dcache.srm.request.CopyRequest;
import org.dcache.srm.request.Job;
import org.dcache.srm.util.Configuration;
import org.dcache.srm.v2_2.TAccessLatency;
import org.dcache.srm.v2_2.TFileStorageType;
import org.dcache.srm.v2_2.TRetentionPolicy;
/**
 *
 * @author  timur
 */
public class CopyRequestStorage extends DatabaseContainerRequestStorage<CopyRequest,CopyFileRequest> {
    public static final String TABLE_NAME="copyrequests";
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
     //Database Request Storage
    "CREDENTIALID , " +
    "RETRYDELTATIME , "+
    "SHOULDUPDATERETRYDELTATIME ,"+
    "DESCRIPTION ,"+ //15
    "CLIENTHOST ,"+
    "STATUSCODE ,"+
    "USERID ,"+
    // Copy Request
    "STORAGETYPE, " +
    "RETENTIONPOLICY, "+
    "ACCESSLATENCY ) " +
    "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

    @Override
    public PreparedStatement getCreateStatement(Connection connection, Job job) throws SQLException {
        CopyRequest cr = (CopyRequest)job;
        String storageTypeValue=null;
        if(cr.getStorageType() != null) {
            storageTypeValue = cr.getStorageType().getValue();
        }
        String retentionPolicyValue=null;
        if(cr.getTargetRetentionPolicy() != null) {
            retentionPolicyValue = cr.getTargetRetentionPolicy().getValue();
        }
        String accessLatencyValue=null;
        if(cr.getTargetAccessLatency() != null) {
            accessLatencyValue = cr.getTargetAccessLatency().getValue();
        }
        PreparedStatement stmt = getPreparedStatement(connection,
                                  INSERT_SQL,
                                  cr.getId(),
                                  cr.getNextJobId(),
                                  cr.getCreationTime(),
                                  cr.getLifetime(),
                                  cr.getState().getStateId(),//5
                                  cr.getErrorMessage(),
                                  cr.getSchedulerId(),
                                  cr.getSchedulerTimeStamp(),
                                  cr.getNumberOfRetries(),
                                  cr.getMaxNumberOfRetries(),//10
                                  cr.getLastStateTransitionTime(),
                                  //Database Request Storage
                                  cr.getCredentialId(),
                                  cr.getRetryDeltaTime(),
                                  cr.isShould_updateretryDeltaTime()?0:1,
                                  cr.getDescription(),
                                  cr.getClient_host(),
                                  cr.getStatusCodeString(),
                                  cr.getUser().getId(),
                                  storageTypeValue,
                                  retentionPolicyValue,
                                  accessLatencyValue);
       return stmt;
    }

    private static final String UPDATE_REQUEST_SQL =
            UPDATE_PREFIX + ", CREDENTIALID=?," +
                " RETRYDELTATIME=?," +
                " SHOULDUPDATERETRYDELTATIME=?," +
                " DESCRIPTION=?," +
                " CLIENTHOST=?," +
                " STATUSCODE=?," +
                " USERID=?," +
                " STORAGETYPE=?, " +
                " RETENTIONPOLICY=?,"+
                " ACCESSLATENCY=?" +
                " WHERE ID=?";

    @Override
    public PreparedStatement getUpdateStatement(Connection connection,
            Job job) throws SQLException {
        CopyRequest cr = (CopyRequest)job;
        String storageTypeValue=null;
        if(cr.getStorageType() != null) {
            storageTypeValue = cr.getStorageType().getValue();
        }
        String retentionPolicyValue=null;
        if(cr.getTargetRetentionPolicy() != null) {
            retentionPolicyValue = cr.getTargetRetentionPolicy().getValue();
        }
        String accessLatencyValue=null;
        if(cr.getTargetAccessLatency() != null) {
            accessLatencyValue = cr.getTargetAccessLatency().getValue();
        }
        PreparedStatement stmt = getPreparedStatement(connection,
                                  UPDATE_REQUEST_SQL,
                                  cr.getNextJobId(),
                                  cr.getCreationTime(),
                                  cr.getLifetime(),
                                  cr.getState().getStateId(),
                                  cr.getErrorMessage(),//5
                                  cr.getSchedulerId(),
                                  cr.getSchedulerTimeStamp(),
                                  cr.getNumberOfRetries(),
                                  cr.getMaxNumberOfRetries(),
                                  cr.getLastStateTransitionTime(),//10
                                  //Database Request Storage
                                  cr.getCredentialId(),
                                  cr.getRetryDeltaTime(),
                                  cr.isShould_updateretryDeltaTime()?0:1,
                                  cr.getDescription(),
                                  cr.getClient_host(),
                                  cr.getStatusCodeString(),
                                  cr.getUser().getId(),
                                  storageTypeValue,
                                  retentionPolicyValue,
                                  accessLatencyValue,
                                  cr.getId());

        return stmt;
    }

    /** Creates a new instance of GetRequestStorage */
     public CopyRequestStorage(Configuration.DatabaseParameters configuration) throws SQLException {
        super(configuration
        );
    }

    @Override
    public void dbInit1() throws SQLException {
   }

    @Override
    protected CopyRequest getContainerRequest(Connection _con,
            long ID,
            Long NEXTJOBID,
            long CREATIONTIME,
            long LIFETIME,
            int STATE,
            String ERRORMESSAGE,
            SRMUser user,
            String SCHEDULERID,
            long SCHEDULER_TIMESTAMP,
            int NUMOFRETR,
            int MAXNUMOFRETR,
            long LASTSTATETRANSITIONTIME,
            Long CREDENTIALID,
            int RETRYDELTATIME,
            boolean SHOULDUPDATERETRYDELTATIME,
            String DESCRIPTION,
            String CLIENTHOST,
            String STATUSCODE,
            CopyFileRequest[] fileRequests,
            ResultSet set,
            int next_index) throws SQLException {

        Job.JobHistory[] jobHistoryArray =
        getJobHistory(ID,_con);
        String STORAGETYPE = set.getString(next_index++);
        String RETENTIONPOLICY = set.getString(next_index++);
        String ACCESSLATENCY = set.getString(next_index++);
        TFileStorageType storageType =
                STORAGETYPE == null || STORAGETYPE.equalsIgnoreCase("null") ?
                    null:TFileStorageType.fromString(STORAGETYPE);

        TRetentionPolicy retentionPolicy =
                RETENTIONPOLICY == null || RETENTIONPOLICY.equalsIgnoreCase("null") ?
                    null:TRetentionPolicy.fromString(RETENTIONPOLICY);
        TAccessLatency accessLatency =
                ACCESSLATENCY == null || ACCESSLATENCY.equalsIgnoreCase("null") ?
                    null:TAccessLatency.fromString(ACCESSLATENCY);

            return new  CopyRequest(
                        ID,
                        NEXTJOBID,
                        CREATIONTIME,
                        LIFETIME,
                        STATE,
                        ERRORMESSAGE,
                        user,
                        SCHEDULERID,
                        SCHEDULER_TIMESTAMP,
                        NUMOFRETR,
                        MAXNUMOFRETR,
                        LASTSTATETRANSITIONTIME,
                        jobHistoryArray,
                        CREDENTIALID,
                        fileRequests,
                        RETRYDELTATIME,
                        SHOULDUPDATERETRYDELTATIME,
                        DESCRIPTION,
                        CLIENTHOST,
                        STATUSCODE,
                        storageType,
                        retentionPolicy,
                        accessLatency);

    }

    @Override
    public String getRequestCreateTableFields() {
        return ", "+
                "STORAGETYPE "+ stringType+
        ", "+
                "RETENTIONPOLICY "+ stringType+
        ", "+
        "ACCESSLATENCY "+ stringType;
    }

    private static int ADDITIONAL_FIELDS = 3;

    @Override
    public String getTableName() {
        return TABLE_NAME;
    }

    @Override
    public String getFileRequestsTableName() {
        return CopyFileRequestStorage.TABLE_NAME;
    }

    @Override
    protected void __verify(int nextIndex, int columnIndex, String tableName, String columnName, int columnType) throws SQLException {
        if(columnIndex == nextIndex)
        {
            verifyStringType("STORAGETYPE",columnIndex,tableName, columnName, columnType);
        }
        else if(columnIndex == nextIndex+1)
        {
            verifyStringType("RETENTIONPOLICY",columnIndex,tableName, columnName, columnType);
        }
        else if(columnIndex == nextIndex+2)
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
