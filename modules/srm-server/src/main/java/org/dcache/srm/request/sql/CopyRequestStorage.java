/*
 * GetRequestStorage.java
 *
 * Created on June 22, 2004, 2:48 PM
 */

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
    "CREDENTIALID , " +
    "RETRYDELTATIME , "+
    "SHOULDUPDATERETRYDELTATIME ,"+
    "DESCRIPTION ,"+
    "CLIENTHOST ,"+ // 15
    "STATUSCODE ,"+
    "USERID ,"+
    // Copy Request
    "STORAGETYPE, " +
    "RETENTIONPOLICY, "+
    "ACCESSLATENCY ) " +
    "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

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
        return getPreparedStatement(connection,
                                    INSERT_SQL,
                                    cr.getId(),
                                    cr.getNextJobId(),
                                    cr.getCreationTime(),
                                    cr.getLifetime(),
                                    cr.getState().getStateId(),//5
                                    cr.latestHistoryEvent(),
                                    cr.getSchedulerId(),
                                    cr.getSchedulerTimeStamp(),
                                    0, // num of retries
                                    cr.getLastStateTransitionTime(), // 10
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

        return getPreparedStatement(connection,
                                    UPDATE_REQUEST_SQL,
                                    cr.getNextJobId(),
                                    cr.getCreationTime(),
                                    cr.getLifetime(),
                                    cr.getState().getStateId(),
                                    cr.latestHistoryEvent(),//5
                                    cr.getSchedulerId(),
                                    cr.getSchedulerTimeStamp(),
                                    0, // num of retries
                                    cr.getLastStateTransitionTime(),
                                    //Database Request Storage
                                    cr.getCredentialId(), // 10
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
    }

    /** Creates a new instance of GetRequestStorage */
     public CopyRequestStorage(@Nonnull String srmId, Configuration.DatabaseParameters configuration,
             ScheduledExecutorService executor, SRMUserPersistenceManager manager)
             throws DataAccessException
     {
         super(srmId, configuration, executor, manager);
     }

    @Override
    protected CopyRequest getContainerRequest(Connection _con,
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
            ImmutableList<CopyFileRequest> fileRequests,
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
    public String getTableName() {
        return TABLE_NAME;
    }

    @Override
    public String getFileRequestsTableName() {
        return CopyFileRequestStorage.TABLE_NAME;
    }
}
