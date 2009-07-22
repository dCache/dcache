// $Id$
// $Log: not supported by cvs2svn $
// Revision 1.9  2007/01/10 23:00:25  timur
// implemented srmGetRequestTokens, store request description in database, fixed several srmv2 issues
//
// Revision 1.8  2007/01/06 00:23:55  timur
// merging production branch changes to database layer to improve performance and reduce number of updates
//
// Revision 1.7  2006/10/02 23:29:17  timur
// implemented srmPing and srmBringOnline (not tested yet), refactored Request.java
//
// Revision 1.6.2.1  2007/01/04 02:58:55  timur
// changes to database layer to improve performance and reduce number of updates
//
// Revision 1.6  2006/04/26 17:17:56  timur
// store the history of the state transitions in the database
//
// Revision 1.5  2006/04/12 23:16:23  timur
// storing state transition time in database, storing transferId for copy requests in database, renaming tables if schema changes without asking
//
// Revision 1.4  2005/03/30 22:42:10  timur
// more database schema changes
//
// Revision 1.3  2005/03/09 23:21:17  timur
// more database checks, more space reservation code
//
// Revision 1.2  2005/03/01 23:10:39  timur
// Modified the database scema to increase database operations performance and to account for reserved space"and to account for reserved space
//
/*
 * GetRequestStorage.java
 *
 * Created on June 22, 2004, 2:48 PM
 */

package org.dcache.srm.request.sql;
import org.dcache.srm.request.ContainerRequest;
import org.dcache.srm.request.Request;
import org.dcache.srm.request.FileRequest;
import org.dcache.srm.request.CopyRequest;
import org.dcache.srm.util.Configuration;
import java.sql.*;
import org.dcache.srm.scheduler.State;
import org.dcache.srm.scheduler.Job;
import org.dcache.srm.v2_2.TAccessLatency;
import org.dcache.srm.v2_2.TRetentionPolicy;
import org.dcache.srm.v2_2.TFileStorageType;
import org.dcache.srm.SRMUser;
/**
 *
 * @author  timur
 */
public class CopyRequestStorage extends DatabaseContainerRequestStorage{
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
     public CopyRequestStorage(Configuration configuration) throws SQLException {
        super(configuration
        );
    }
        
    public void say(String s){
        if(logger != null) {
           logger.log(" CopyRequestStorage: "+s);
        }
    }
    
    public void esay(String s){
        if(logger != null) {
           logger.elog(" CopyRequestStorage: "+s);
        }
    }
    
    public void esay(Throwable t){
        if(logger != null) {
           logger.elog(t);
        }
    }
    


    public void dbInit1() throws SQLException {
   }
    
    public void getCreateList(ContainerRequest r, StringBuffer sb) {
        CopyRequest cr = (CopyRequest)r;
        TFileStorageType storageType = cr.getStorageType();
        if(storageType == null) {
            sb.append(", NULL ");
        } else {
            sb.append(", '").append(storageType.getValue()).append("' ");
        }
        
        TRetentionPolicy retentionPolicy = cr.getTargetRetentionPolicy();
        if(retentionPolicy == null) {
            sb.append(", NULL ");
        } else {
            sb.append(", '").append(retentionPolicy.getValue()).append("' ");
        }
        TAccessLatency  accessLatency = cr.getTargetAccessLatency();
        if(accessLatency == null) {
            sb.append(",NULL ");
        } else {
            sb.append(", '").append(accessLatency.getValue()).append('\'');
        }
        
    }
    
    protected ContainerRequest getContainerRequest(Connection _con, 
            Long ID, 
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
            FileRequest[] fileRequests, 
            java.sql.ResultSet set, 
            int next_index) throws java.sql.SQLException {
           
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
                        this,
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
                        accessLatency,
                        configuration
                        );

    }
    
    public String getRequestCreateTableFields() {
        return ", "+
                "STORAGETYPE "+ stringType+
        ", "+
                "RETENTIONPOLICY "+ stringType+
        ", "+
        "ACCESSLATENCY "+ stringType;
    }
    
    private static int ADDITIONAL_FIELDS = 3;
    
    public String getTableName() {
        return TABLE_NAME;
    }
    
    public void getUpdateAssignements(ContainerRequest r, StringBuffer sb) {
        CopyRequest cr = (CopyRequest)r;

        TFileStorageType storageType = cr.getStorageType();
        if(storageType == null) {
            sb.append(", STORAGETYPE=NULL ");
        } else {
            sb.append(", STORAGETYPE='").append(storageType.getValue()).append("' ");
        }
        TRetentionPolicy retentionPolicy = cr.getTargetRetentionPolicy();
        if(retentionPolicy == null) {
            sb.append(", RETENTIONPOLICY=NULL ");
        } else {
            sb.append(", RETENTIONPOLICY='").append(retentionPolicy.getValue()).append("' ");
        }
        TAccessLatency  accessLatency = cr.getTargetAccessLatency();
        if(accessLatency == null) {
            sb.append(", ACCESSLATENCY=NULL ");
        } else {
            sb.append(", ACCESSLATENCY='").append(accessLatency.getValue()).append('\'');
        }
    }
    
   
    public String getFileRequestsTableName() {
        return CopyFileRequestStorage.TABLE_NAME;
    }    
     
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
    
   
    protected int getMoreCollumnsNum() {
         return ADDITIONAL_FIELDS;
     }

}
