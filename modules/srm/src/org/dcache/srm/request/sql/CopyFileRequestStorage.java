/*
 * GetFileRequestStorage.java
 *
 * Created on June 17, 2004, 4:49 PM
 */

package org.dcache.srm.request.sql;
import java.sql.*;
import org.dcache.srm.request.FileRequest;
import org.dcache.srm.request.CopyFileRequest;
import org.dcache.srm.util.Configuration;
import org.dcache.srm.scheduler.State;
import org.globus.util.GlobusURL;
import org.dcache.srm.scheduler.Job;

/**
 *
 * @author  timur
 */
public class CopyFileRequestStorage extends DatabaseFileRequestStorage {
    
    /** Creates a new instance of GetFileRequestStorage */
    public CopyFileRequestStorage(Configuration configuration) throws SQLException {
        super(configuration        );
    }
    
      
    public void say(String s){
        if(logger != null) {
           logger.log(" CopyFileRequestStorage: "+s);
        }
    }
    
    public void esay(String s){
        if(logger != null) {
           logger.elog(" CopyFileRequestStorage: "+s);
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
    String CREATORID,
    String SCHEDULERID,
    long SCHEDULER_TIMESTAMP,
    int NUMOFRETR,
    int MAXNUMOFRETR,
    long LASTSTATETRANSITIONTIME,
    Long REQUESTID,
    Long CREDENTIALID,
    java.sql.ResultSet set,
    int next_index)throws java.sql.SQLException {
            
        String FROMURL = set.getString(next_index++);
        String TOURL = set.getString(next_index++);
        String FROMTURL = set.getString(next_index++);
        String TOTURL = set.getString(next_index++);
        String FROMLOCALPATH = set.getString(next_index++);
        String TOLOCALPATH = set.getString(next_index++);
        long size = set.getLong(next_index++);
        String fromFileId = set.getString(next_index++);
        String toFileId = set.getString(next_index++);
        String REMOTEREQUESTID = set.getString(next_index++);
        String REMOTEFILEID = set.getString(next_index++);
        String SPACERESERVATIONID = set.getString(next_index++);
        String TRANSFERID = set.getString(next_index);
        Job.JobHistory[] jobHistoryArray = 
        getJobHistory(ID,_con);
        
   
           return new CopyFileRequest(
            ID,
            NEXTJOBID ,
            this,
            CREATIONTIME,
            LIFETIME,
            STATE,
            ERRORMESSAGE,
            CREATORID,
            SCHEDULERID,
            SCHEDULER_TIMESTAMP,
            NUMOFRETR,
            MAXNUMOFRETR,
            LASTSTATETRANSITIONTIME,
            jobHistoryArray,
            REQUESTID,
            CREDENTIALID,
            configuration,
            FROMURL,
             TOURL,
             FROMTURL,
             TOTURL,
             FROMLOCALPATH,
             TOLOCALPATH,
             size,
             fromFileId,
             toFileId,
             REMOTEREQUESTID,
             REMOTEFILEID,
             SPACERESERVATIONID,
             TRANSFERID
            );
    }
    
    public String getFileRequestCreateTableFields() {
        return                     
        ","+
        "FROMURL "+  stringType+
        ","+
        "TOURL "+  stringType+
        ","+
        "FROMTURL "+  stringType+
        ","+
        "TOTURL "+  stringType+
        ","+
        "FROMLOCALPATH "+  stringType+
        ","+
        "TOLOCALPATH "+  stringType+
        ","+
        "SIZE "+  longType+
        ","+
        "FROMFILEID "+  stringType+
        ","+
        "TOFILEID "+  stringType+
        ","+
        "REMOTEREQUESTID "+  stringType+
        ","+
        "REMOTEFILEID "+  stringType+
         ","+
        "SPACERESERVATIONID "+  stringType+
        ","+
        "TRANSFERID "+ stringType;
  }
    private static int ADDITIONAL_FIELDS = 13;
    public static final String TABLE_NAME="copyfilerequests";
    public String getTableName() {
        return TABLE_NAME;
    }
    
    public void getUpdateAssignements(FileRequest fr,StringBuffer sb) {
        if(fr == null || !(fr instanceof CopyFileRequest)) {
            throw new IllegalArgumentException("fr is not CopyFileRequest" );
        }
        CopyFileRequest cfr = (CopyFileRequest)fr;
        sb.append(", FROMURL = '").append(cfr.getFromURL()).append("',");
        sb.append(" TOURL = '").append(cfr.getToURL()).append("',");
        GlobusURL tmpurl =cfr.getFrom_turl();
        
        if(tmpurl == null) {
            sb.append(" FROMTURL =NULL, ");
        }
        else {
            sb.append("FROMTURL = '").append(tmpurl.getURL()).append("', ");
        }
        
        tmpurl =cfr.getTo_turl();
        
        if(tmpurl == null) {
            sb.append(" TOTURL =NULL, ");
        }
        else {
            sb.append("TOTURL = '").append(tmpurl.getURL()).append("', ");
        }
        
        
        String tmp =cfr.getLocal_from_path();
        if(tmp == null) {
            sb.append(" FROMLOCALPATH =NULL, ");
        }
        else {
            sb.append("FROMLOCALPATH = '").append(tmp).append("', ");
        }
        
        tmp =cfr.getLocal_to_path();
        if(tmp == null) {
            sb.append(" TOLOCALPATH =NULL, ");
        }
        else {
            sb.append("TOLOCALPATH = '").append(tmp).append("', ");
        }
        
        sb.append(" SIZE = ").append(cfr.getSize()).append(",");
        
        tmp =cfr.getFromFileId();
        if(tmp == null) {
            sb.append(" FROMFILEID =NULL, ");
        }
        else {
            sb.append("FROMFILEID = '").append(tmp).append("', ");
        }
        
        tmp =cfr.getToFileId();
        if(tmp == null) {
            sb.append(" TOFILEID =NULL, ");
        }
        else {
            sb.append("TOFILEID = '").append(tmp).append("', ");
        }
        
        tmp  =cfr.getRemoteRequestId();
        if(tmp == null) {
            sb.append(" REMOTEREQUESTID =NULL, ");
        }
        else {
            sb.append("REMOTEREQUESTID = '").append(tmp).append("', ");
        }
        
        tmp  =cfr.getRemoteFileId();
        if(tmp == null) {
            sb.append(" REMOTEFILEID =NULL, ");
        }
        else {
            sb.append("REMOTEFILEID = '").append(tmp).append("', ");
        }
        
        tmp =cfr.getSpaceReservationId();
        if(tmp == null) {
            sb.append(" SPACERESERVATIONID =NULL, ");
        }
        else {
            sb.append("SPACERESERVATIONID = '").append(tmp).append("', ");
        }
        tmp =cfr.getTransferId();
        if(tmp == null) {
            sb.append(" TRANSFERID =NULL ");
        }
        else {
            sb.append("TRANSFERID = '").append(tmp).append("' ");
        }
  }
    
     public void getCreateList(FileRequest fr,StringBuffer sb) {
        if(fr == null || !(fr instanceof CopyFileRequest)) {
            throw new IllegalArgumentException("fr is not CopyFileRequest" );
        }
        CopyFileRequest cfr = (CopyFileRequest)fr;
        sb.append(", '").append(cfr.getFromURL()).append("',");
        sb.append(" '").append(cfr.getToURL()).append("',");
        GlobusURL tmpurl =cfr.getFrom_turl();
        
        if(tmpurl == null) {
            sb.append(" NULL, ");
        }
        else {
            sb.append("'").append(tmpurl.getURL()).append("', ");
        }
        
        tmpurl =cfr.getTo_turl();
        
        if(tmpurl == null) {
            sb.append(" NULL, ");
        }
        else {
            sb.append("'").append(tmpurl.getURL()).append("', ");
        }
        
        
        String tmp =cfr.getLocal_from_path();
        if(tmp == null) {
            sb.append(" NULL, ");
        }
        else {
            sb.append("'").append(tmp).append("', ");
        }
        
        tmp =cfr.getLocal_to_path();
        if(tmp == null) {
            sb.append(" NULL, ");
        }
        else {
            sb.append("'").append(tmp).append("', ");
        }
        
        sb.append(cfr.getSize()).append(", ");
        
        tmp =cfr.getFromFileId();
        if(tmp == null) {
            sb.append(" NULL, ");
        }
        else {
            sb.append("'").append(tmp).append("', ");
        }
        
        tmp =cfr.getToFileId();
        if(tmp == null) {
            sb.append("NULL, ");
        }
        else {
            sb.append("'").append(tmp).append("', ");
        }
        
        tmp  =cfr.getRemoteRequestId();
        if(tmp == null) {
            sb.append("NULL, ");
        }
        else {
            sb.append("'").append(tmp).append("', ");
        }
        
        tmp  =cfr.getRemoteFileId();
        if(tmp == null) {
            sb.append("NULL, ");
        }
        else {
            sb.append("'").append(tmp).append("', ");
        }
        
        tmp = cfr.getSpaceReservationId();
        if(tmp == null) {
            sb.append("NULL, ");
        }
        else {
            sb.append('\'').append(tmp).append("', ");
        }
        
        tmp = cfr.getTransferId();
        if(tmp == null) {
            sb.append("NULL ");
        }
        else {
            sb.append('\'').append(tmp).append("' ");
        }
    }
   
     public String getRequestTableName() {
          return CopyRequestStorage.TABLE_NAME;
     }     
     
     protected void __verify(int nextIndex, int columnIndex, String tableName, String columnName, int columnType) throws SQLException {
        /*","+
        ","+
        "FROMURL "+  stringType+
        ","+
        "TOURL "+  stringType+
        ","+
        "FROMTURL "+  stringType+
        ","+
        "TOTURL "+  stringType+
        ","+
        "FROMLOCALPATH "+  stringType+
        ","+
        "TOLOCALPATH "+  stringType+
        ","+
        "SIZE "+  longType+
        ","+
        "FROMFILEID "+  stringType+
        ","+
        "TOFILEID "+  stringType+
        ","+
        "REMOTEREQUESTID "+  stringType+
        ","+
        "REMOTEFILEID "+  stringType+
         ","+
        "SPACERESERVATIONID "+  stringType+
         ","+
        "TRANSFERID "+  stringType;
        */
        if(columnIndex == nextIndex) {
            verifyStringType("FROMURL",columnIndex,tableName, columnName, columnType);
        }
        else if(columnIndex == nextIndex+1)
        {
            verifyStringType("TOURL",columnIndex,tableName, columnName, columnType);
            
        }
        else if(columnIndex == nextIndex+2)
        {
            verifyStringType("FROMTURL",columnIndex,tableName, columnName, columnType);
        }
        else if(columnIndex == nextIndex+3)
        {
            verifyStringType("TOTURL",columnIndex,tableName, columnName, columnType);
        }
        else if(columnIndex == nextIndex+4)
        {
            verifyStringType("FROMLOCALPATH",columnIndex,tableName, columnName, columnType);
        }
        else if(columnIndex == nextIndex+5)
        {
            verifyStringType("TOLOCALPATH",columnIndex,tableName, columnName, columnType);
        }
        else if(columnIndex == nextIndex+6)
        {
            verifyLongType("SIZE",columnIndex,tableName, columnName, columnType);
        }
        else if(columnIndex == nextIndex+7)
        {
            verifyStringType("FROMFILEID",columnIndex,tableName, columnName, columnType);
        }
        else if(columnIndex == nextIndex+8)
        {
            verifyStringType("TOFILEID",columnIndex,tableName, columnName, columnType);
        }
        else if(columnIndex == nextIndex+9)
        {
            verifyStringType("REMOTEREQUESTID",columnIndex,tableName, columnName, columnType);
        }
        else if(columnIndex == nextIndex+10)
        {
            verifyStringType("REMOTEFILEID",columnIndex,tableName, columnName, columnType);
        }
        else if(columnIndex == nextIndex+11)
        {
            verifyStringType("SPACERESERVATIONID",columnIndex,tableName, columnName, columnType);
        }
       else if(columnIndex == nextIndex+12)
        {
            verifyStringType("TRANSFERID",columnIndex,tableName, columnName, columnType);
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
