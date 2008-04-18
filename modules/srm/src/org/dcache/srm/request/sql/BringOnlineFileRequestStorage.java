// $Id$
// $Log: not supported by cvs2svn $
// Revision 1.3  2007/01/10 23:00:25  timur
// implemented srmGetRequestTokens, store request description in database, fixed several srmv2 issues
//
// Revision 1.2  2007/01/06 00:23:55  timur
// merging production branch changes to database layer to improve performance and reduce number of updates
//
/*
 * BringOnlineFileRequestStorage.java
 *
 * Created on June 17, 2004, 4:49 PM
 */

package org.dcache.srm.request.sql;
import java.sql.*;
import org.dcache.srm.request.FileRequest;
import org.dcache.srm.request.BringOnlineFileRequest;
import org.dcache.srm.util.Configuration;
import org.dcache.srm.scheduler.State;
import org.dcache.srm.scheduler.Job;

/**
 *
 * @author  timur
 */
public class BringOnlineFileRequestStorage extends DatabaseFileRequestStorage {
    
    /** Creates a new instance of BringOnlineFileRequestStorage */
    public BringOnlineFileRequestStorage(    
    Configuration configuration
    )  throws SQLException {
        super(configuration);
    }
   
        
    public void say(String s){
        if(logger != null) {
           logger.log(" BringOnlineFileRequestStorage: "+s);
        }
    }
    
    public void esay(String s){
        if(logger != null) {
           logger.elog(" BringOnlineFileRequestStorage: "+s);
        }
    }
    
    public void esay(Throwable t){
        if(logger != null) {
           logger.elog(t);
        }
    }

    
    protected FileRequest getFileRequest(Connection _con,
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
        String STATUSCODE,
        java.sql.ResultSet set, 
        int next_index) throws SQLException {
           String SURL = set.getString(next_index++);
           String FILEID = set.getString(next_index++);
           String PINID = set.getString(next_index);
            Job.JobHistory[] jobHistoryArray = 
            getJobHistory(ID,_con);
           return new BringOnlineFileRequest(
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
            STATUSCODE,
            configuration,
            SURL,
            FILEID,
            PINID
            );
    }
    
    public String getFileRequestCreateTableFields() {
        return                     
        ","+
        "SURL "+  stringType+
        ","+
        "FILEID "+  stringType+
        ","+
        "PINID "+  stringType;
    }
    
    private static int ADDITIONAL_FIELDS = 3;

    public static final String TABLE_NAME = "bringonlinefilerequests";
    public String getTableName() {
        return TABLE_NAME;
    }
    
    public void getUpdateAssignements(FileRequest fr,StringBuffer sb) {
        if(fr == null || !(fr instanceof BringOnlineFileRequest)) {
            throw new IllegalArgumentException("fr is not BringOnlineRequest" );
        }
        BringOnlineFileRequest gfr = (BringOnlineFileRequest)fr;
         sb.append(", SURL = '").append(gfr.getSurlString()).append("',");
        String tmp =gfr.getFileId();
        if(tmp == null) {
            sb.append(" FILEID =NULL, ");
        }
        else {
            sb.append("FILEID = '").append(tmp).append("', ");
        }
        tmp =gfr.getPinId();
        if(tmp == null) {
            sb.append(" PINID =NULL ");
        } 
        else {
            sb.append("PINID = '").append(tmp).append("' ");
        }
    }
    
     public void getCreateList(FileRequest fr,StringBuffer sb) {
        if(fr == null || !(fr instanceof BringOnlineFileRequest)) {
            throw new IllegalArgumentException("fr is not BringOnlineRequest" );
        }
        BringOnlineFileRequest gfr = (BringOnlineFileRequest)fr;
        sb.append(", '").append(gfr.getSurlString()).append("', ");
        String tmp =gfr.getFileId();
        if(tmp == null) {
            sb.append("NULL, ");
        }
        else {
            sb.append('\'').append(tmp).append("', ");
        }
        tmp = gfr.getPinId();
        if(tmp == null) {
            sb.append("NULL ");
        }
        else {
            sb.append('\'').append(tmp).append("' ");
        }
    }
   
     public String getRequestTableName() {
         return GetRequestStorage.TABLE_NAME;
     }     
     
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
            verifyStringType("FILEID",columnIndex,tableName, columnName, columnType);
        }
        else if(columnIndex == nextIndex+2)
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
     
    protected int getMoreCollumnsNum() {
         return ADDITIONAL_FIELDS;
     }
     
}
