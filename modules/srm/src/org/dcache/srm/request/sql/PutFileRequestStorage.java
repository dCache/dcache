/*
 * GetFileRequestStorage.java
 *
 * Created on June 17, 2004, 4:49 PM
 */

package org.dcache.srm.request.sql;
import java.sql.*;
import org.dcache.srm.request.FileRequest;
import org.dcache.srm.request.PutFileRequest;
import org.dcache.srm.util.Configuration;
import org.dcache.srm.scheduler.State;
import org.dcache.srm.scheduler.Job;
import org.dcache.srm.scheduler.Job;
import org.dcache.srm.v2_2.TRetentionPolicy;
import org.dcache.srm.v2_2.TAccessLatency;

/**
 *
 * @author  timur
 */
public class PutFileRequestStorage extends DatabaseFileRequestStorage {
    
    /** Creates a new instance of GetFileRequestStorage */
    public PutFileRequestStorage(Configuration configuration) throws SQLException {
        super(
        configuration
        );
    }
    
        
    public void say(String s){
        if(logger != null) {
           logger.log(" PutFileRequestStorage: "+s);
        }
    }
    
    public void esay(String s){
        if(logger != null) {
           logger.elog(" PutFileRequestStorage: "+s);
        }
    }
    
    public void esay(Throwable t){
        if(logger != null) {
           logger.elog(t);
        }
    }

    protected FileRequest getFileRequest(
    Connection _con,
    Long ID,
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
    Long REQUESTID,
    Long CREDENTIALID,
    String STATUSCODE,
    java.sql.ResultSet set,
    int next_index)throws java.sql.SQLException {
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
        this,
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
        configuration,
        SURL,
        TURL,
        FILEID,
        PARENTFILEID,
        SPACERESERVATIONID,
        SIZE,
        retentionPolicy,
        accessLatency
        );
    }
    
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

    
    public static final String TABLE_NAME="putfilerequests";
    public String getTableName() {
        return TABLE_NAME;
    }
    
    public void getUpdateAssignements(FileRequest fr,StringBuffer sb) {
        if(fr == null || !(fr instanceof PutFileRequest)) {
            throw new IllegalArgumentException("fr is not GetFileRequest" );
        }
        PutFileRequest pfr = (PutFileRequest)fr;
        sb.append(", SURL = '").append(pfr.getSurlString()).append("',");
        String tmp =pfr.getTurlString();
        if(tmp == null) {
            sb.append(" TURL =NULL, ");
        }
        else {
            sb.append("TURL = '").append(tmp).append("', ");
        }
        
        tmp =pfr.getFileId();
        if(tmp == null) {
            sb.append(" FILEID =NULL, ");
        }
        else {
            sb.append("FILEID = '").append(tmp).append("', ");
        }
        tmp =pfr.getParentFileId();
        if(tmp == null) {
            sb.append(" PARENTFILEID =NULL, ");
        }
        else {
            sb.append("PARENTFILEID = '").append(tmp).append("', ");
        }
        tmp =pfr.getSpaceReservationId();
        if(tmp == null) {
            sb.append(" SPACERESERVATIONID =NULL, ");
        }
        else {
            sb.append("SPACERESERVATIONID = '").append(tmp).append("', ");
        }
        sb.append("SIZE = ").append(pfr.getSize()).append(", ");
        TRetentionPolicy retentionPolicy = pfr.getRetentionPolicy();
        if(retentionPolicy == null) {
            sb.append("RETENTIONPOLICY=NULL, ");
        } else {
            sb.append("RETENTIONPOLICY='").append(retentionPolicy.getValue()).append("', ");
        }
        TAccessLatency  accessLatency = pfr.getAccessLatency();
        if(accessLatency == null) {
            sb.append("ACCESSLATENCY=NULL ");
        } else {
            sb.append("ACCESSLATENCY='").append(accessLatency.getValue()).append('\'');
        }
   }
    
    public void getCreateList(FileRequest fr,StringBuffer sb) {
        if(fr == null || !(fr instanceof PutFileRequest)) {
            throw new IllegalArgumentException("fr is not GetFileRequest" );
        }
        PutFileRequest pfr = (PutFileRequest)fr;
        sb.append(", '").append(pfr.getSurlString()).append("', ");
        String tmp = pfr.getTurlString();
        if(tmp == null) {
            sb.append("NULL, ");
        }
        else {
            sb.append('\'').append(tmp).append("', ");
        }
        tmp =pfr.getFileId();
        if(tmp == null) {
            sb.append("NULL, ");
        }
        else {
            sb.append('\'').append(tmp).append("', ");
        }
        tmp = pfr.getParentFileId();
        if(tmp == null) {
            sb.append("NULL, ");
        }
        else {
            sb.append('\'').append(tmp).append("', ");
        }
        tmp = pfr.getSpaceReservationId();
        if(tmp == null) {
            sb.append("NULL, ");
        }
        else {
            sb.append('\'').append(tmp).append("', ");
        }
        sb.append(pfr.getSize()).append(", ");
        TRetentionPolicy retentionPolicy = pfr.getRetentionPolicy();
        if(retentionPolicy == null) {
            sb.append("NULL, ");
        } else {
            sb.append('\'').append(retentionPolicy.getValue()).append("', ");
        }
        TAccessLatency  accessLatency = pfr.getAccessLatency();
        if(accessLatency == null) {
            sb.append("NULL ");
        } else {
            sb.append('\'').append(accessLatency.getValue()).append('\'');
        }
   }
    
    public String getRequestTableName() {
         return PutRequestStorage.TABLE_NAME;
    }    
    
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
    
     protected int getMoreCollumnsNum() {
         return ADDITIONAL_FIELDS;
     }
   
}
