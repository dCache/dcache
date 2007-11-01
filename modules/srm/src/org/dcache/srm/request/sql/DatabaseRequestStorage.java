/*
 * FileRequestStorage.java
 *
 * Created on June 17, 2004, 3:18 PM
 */

package org.dcache.srm.request.sql;

import org.dcache.srm.request.Request;
import org.dcache.srm.request.FileRequest;
import org.dcache.srm.request.RequestStorage;
import org.dcache.srm.scheduler.Job;
import org.dcache.srm.scheduler.State;
import org.dcache.srm.util.Configuration;
import java.sql.*;
import java.util.Set;

/**
 *
 * @author  timur
 */
public abstract class DatabaseRequestStorage extends DatabaseJobStorage implements RequestStorage{
    
    
    /** Creates a new instance of FileRequestStorage */
    public DatabaseRequestStorage(Configuration configuration) throws SQLException {
        super(configuration);
    }
    
     
    public void say(String s){
        if(logger != null) {
           logger.log(" DatabaseRequestStorage: "+s);
        }
    }
    
    public void esay(String s){
        if(logger != null) {
           logger.elog(" DatabaseRequestStorage: "+s);
        }
    }
    
    public void esay(Throwable t){
        if(logger != null) {
           logger.elog(t);
        }
    }

   public abstract String getFileRequestsTableName();
   /*{
        return getTableName()+"_filerequestids";
    }
    **/
    
    public abstract void dbInit1() throws SQLException;
    
    protected void _dbInit() throws SQLException {
        dbInit1();
        
    }
    
    public abstract String getRequestCreateTableFields();
    
    public String getCreateTableFields() {
        return
        ","+
        "CREDENTIALID "+  longType+
        ","+
        "RETRYDELTATIME "+  intType+
        ","+
        "SHOULDUPDATERETRYDELTATIME "+  booleanType+
        
        getRequestCreateTableFields();
    }
    
    protected abstract Request getRequest(
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
    Long CREDENTIALID,
    int RETRYDELTATIME,
    boolean SHOULDUPDATERETRYDELTATIME,
    FileRequest[] fileRequests,
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
    String CREATORID,
    String SCHEDULERID,
    long SCHEDULER_TIMESTAMP,
    int NUMOFRETR,
    int MAXNUMOFRETR,
    long LASTSTATETRANSITIONTIME,
    java.sql.ResultSet set,
    int next_index) throws java.sql.SQLException {
        
        String sqlStatementString = "SELECT ID FROM " + getFileRequestsTableName() +
        " WHERE RequestID="+ID;
        Statement sqlStatement = _con.createStatement();
        say("executing statement: "+sqlStatementString);
        ResultSet fileIdsSet = sqlStatement.executeQuery(sqlStatementString);
        java.util.Set utilset = new java.util.HashSet();
        while(fileIdsSet.next()) {
            utilset.add(new Long(fileIdsSet.getLong(1)));
        }
        fileIdsSet.close();
        sqlStatement.close();

        Long [] fileIds = (Long[]) utilset.toArray(new Long[0]);
        sqlStatement.close();
        FileRequest[] fileRequests = new FileRequest[fileIds.length];
        for(int i = 0; i<fileRequests.length; ++i) {
            fileRequests[i] = (FileRequest) Job.getJob(fileIds[i],_con);
        }
        Long CREDENTIALID = new Long(set.getLong(next_index++));
        int RETRYDELTATIME = set.getInt(next_index++);
        boolean SHOULDUPDATERETRYDELTATIME = set.getBoolean(next_index++);
        return getRequest(
        _con,
        ID,
        NEXTJOBID ,
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
        CREDENTIALID,
        RETRYDELTATIME, 
        SHOULDUPDATERETRYDELTATIME,
        fileRequests,
        set,
        next_index );
    }
    
    public abstract String getTableName();
    
    public abstract void getUpdateAssignements(Request r,StringBuffer sb);
    
    public void getUpdateAssignements(org.dcache.srm.scheduler.Job job,StringBuffer sb) {
        if(job == null || !(job instanceof Request)) {
            throw new IllegalArgumentException("job is not Request" );
        }
        Request r = (Request)job;
        sb.append(", CREDENTIALID = ").append( r.getCredentialId()).append(" ");
        sb.append(", RETRYDELTATIME = ").append( r.getRetryDeltaTime());
        sb.append(", SHOULDUPDATERETRYDELTATIME = ").append( r.isShould_updateretryDeltaTime() ? 1 : 0 );
        getUpdateAssignements(r,sb);
        
    }
    
    private static int ADDITIONAL_FIELDS_NUM=3;
    
    public abstract  void getCreateList(Request r,StringBuffer sb);
    
    public void getCreateList(org.dcache.srm.scheduler.Job job, StringBuffer sb) {
        
        if(job == null || !(job instanceof Request)) {
            throw new IllegalArgumentException("job is not Request" );
        }
        Request r = (Request)job;
        
        sb.append(", ").append(r.getCredentialId()).append(" ");
        sb.append(", ").append(r.getRetryDeltaTime());
        sb.append(", ").append(r.isShould_updateretryDeltaTime() ? 1 : 0);
        getCreateList(r,sb);
        
    }
    
    
    public abstract String[] getAdditionalCreateRequestStatements(Request r);
    
    public String[] getAdditionalCreateStatements(Job job)  {
        if(job == null || !(job instanceof Request)) {
            throw new IllegalArgumentException("job is not Request" );
        }
        Request r = (Request)job;
        return getAdditionalCreateRequestStatements(r);
    }
    
    public Set getActiveRequestIds(String schedulerid)  throws java.sql.SQLException {
        String condition = " SCHEDULERID='"+schedulerid+
        "' AND STATE !="+State.DONE.getStateId()+
        " AND STATE !="+State.CANCELED.getStateId()+
        " AND STATE !="+State.FAILED.getStateId();
        return getJobIdsByCondition(condition);
    }
    
    public Set getLatestCompletedRequestIds(int maxNum)  throws java.sql.SQLException {
        return getJobIdsByCondition(
        " STATE ="+State.DONE.getStateId()+
        " OR STATE ="+State.CANCELED.getStateId()+
        " OR STATE = "+State.FAILED.getStateId()+
        " ORDER BY ID"+
        " LIMIT "+maxNum+" ");
    }
    
    public Set getLatestDoneRequestIds(int maxNum)  throws java.sql.SQLException {
        return getJobIdsByCondition("STATE ="+State.DONE.getStateId()+
        " ORDERED BY ID"+
        " LIMIT "+maxNum+" ");
    }
    
    public Set getLatestFailedRequestIds(int maxNum)  throws java.sql.SQLException {
        return getJobIdsByCondition("STATE !="+State.FAILED.getStateId()+
        " ORDERED BY ID"+
        " LIMIT "+maxNum+" ");
    }
    
    public Set getLatestCanceledRequestIds(int maxNum)  throws java.sql.SQLException {
        return getJobIdsByCondition("STATE != "+State.CANCELED.getStateId()+
        " ORDERED BY ID"+
        " LIMIT "+maxNum+" ");
    }
   
   protected abstract void __verify(int nextIndex, int columnIndex, String tableName, String columnName, int columnType) throws SQLException ;
    
    protected void _verify(int nextIndex, int columnIndex, String tableName, String columnName, int columnType) throws SQLException {
        /*
         *additional fields:
                 ","+
        "CREDENTIALID "+  stringType+
        ","+
        "RETRYDELTATIME "+  intType+
        ","+
        "SHOULDUPDATERETRYDELTATIME "+  booleanType+
        */
        if(columnIndex == nextIndex) {
            verifyLongType("CREDENTIALID",columnIndex,tableName, columnName, columnType);
        }
        else if(columnIndex == nextIndex+1)
        {
            verifyIntType("RETRYDELTATIME",columnIndex,tableName, columnName, columnType);
            
        }
        else if(columnIndex == nextIndex+2)
        {
            verifyBooleanType("SHOULDUPDATERETRYDELTATIME",columnIndex,tableName, columnName, columnType);
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
    

}

