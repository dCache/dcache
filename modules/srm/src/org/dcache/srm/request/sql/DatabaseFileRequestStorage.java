// $Id$
// $Log: not supported by cvs2svn $
// Revision 1.11  2007/01/06 00:23:55  timur
// merging production branch changes to database layer to improve performance and reduce number of updates
//
/*
 * DatabaseFileRequestStorage.java
 *
 * Created on June 17, 2004, 3:18 PM
 */

package org.dcache.srm.request.sql;

import org.dcache.srm.request.FileRequest;
//import org.dcache.srm.request.FileRequestStorage;
import org.dcache.srm.scheduler.Job;
import org.dcache.srm.util.Configuration;
import java.sql.*;
import org.dcache.srm.scheduler.State;
import java.util.Set;
//import java.util.HashSet;
//import java.util.Iterator;

/**
 *
 * @author  timur
 */
public abstract class DatabaseFileRequestStorage extends DatabaseJobStorage  implements org.dcache.srm.request.FileRequestStorage{
    
    /** Creates a new instance of FileRequestStorage */
    public DatabaseFileRequestStorage
    (
    Configuration configuration)  throws SQLException {
        super(configuration);
    }
     
    public void say(String s){
        if(logger != null) {
           logger.log(" DatabaseFileRequestStorage: "+s);
        }
    }
    
    public void esay(String s){
        if(logger != null) {
           logger.elog(" DatabaseFileRequestStorage: "+s);
        }
    }
    
    public void esay(Throwable t){
        if(logger != null) {
           logger.elog(t);
        }
    }

    /**
     * empty emplementations
     */
    protected void _dbInit() throws SQLException{
       
	    String columns[] = {
		    "REQUESTID"};
	   createIndex(columns, getTableName().toLowerCase());
    }
    
       
    public abstract String getFileRequestCreateTableFields();
    
    public abstract String getRequestTableName();
    
    public String getCreateTableFields() {
        return
        ","+
        "REQUESTID "+  longType+
        ", CREDENTIALID "+  longType+
        ", "+
        "STATUSCODE "+ stringType+
        getFileRequestCreateTableFields();
    }
   
    private static int ADDITIONAL_FIELDS_NUM=3;
 
    protected abstract FileRequest getFileRequest(
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
    int next_index)throws java.sql.SQLException;
    
    protected org.dcache.srm.scheduler.Job
    getJob(
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
    java.sql.ResultSet set,
    int next_index) throws java.sql.SQLException {
        Long REQUESTID = new Long(set.getLong(next_index++));
        Long CREDENTIALID = new Long(set.getLong(next_index++));
        String STATUSCODE= set.getString(next_index++);
        return getFileRequest(
        _con, 
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
        REQUESTID,
        CREDENTIALID,
        STATUSCODE,
        set,
        next_index );
    }
    
    public abstract String getTableName();
    
    protected abstract void __verify(int nextIndex, int columnIndex, String tableName, String columnName, int columnType) throws SQLException ;
    
    protected void _verify(int nextIndex, int columnIndex, String tableName, String columnName, int columnType) throws SQLException {
        /*
         *additional fields:
         "REQUESTID "+  longType+
        ", CREDENTIALID "+  stringType+*/
        if(columnIndex == nextIndex) {
            verifyLongType("REQUESTID",columnIndex,tableName, columnName, columnType);
        }
        else if(columnIndex == nextIndex+1)
        {
            verifyLongType("CREDENTIALID",columnIndex,tableName, columnName, columnType);
            
        }
        else if(columnIndex == nextIndex+2)
        {
            verifyStringType("STATUSCODE",columnIndex,tableName, columnName, columnType);
        }
        else
        {
            __verify(nextIndex+3,columnIndex,tableName, columnName, columnType);
        }
   }
    
    protected abstract int getMoreCollumnsNum();
    protected int getAdditionalColumnsNum() {
        return ADDITIONAL_FIELDS_NUM +getMoreCollumnsNum();
    }
    
    /*protected java.util.Set getFileRequests(String requestId) throws java.sql.SQLException{
        return getJobsByCondition(" REQUESTID = '"+requestId+"'");
    }*/
    
    
    public Set<Long> getActiveFileRequestIds(String schedulerid)  throws java.sql.SQLException {
        String condition = " SCHEDULERID='"+schedulerid+
        "' AND STATE !="+State.DONE.getStateId()+
        " AND STATE !="+State.CANCELED.getStateId()+
        " AND STATE !="+State.FAILED.getStateId();
        return getJobIdsByCondition(condition);
    }
    
}
