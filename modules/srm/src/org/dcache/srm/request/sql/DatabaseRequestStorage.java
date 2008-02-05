/*
 * DatabaseRequestStorage.java
 *
 * Created on February 16, 2007, 1:24 PM
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package org.dcache.srm.request.sql;

import org.dcache.srm.request.Request;
import org.dcache.srm.request.RequestStorage;
import org.dcache.srm.scheduler.Job;
import org.dcache.srm.scheduler.State;
import org.dcache.srm.util.Configuration;
import java.sql.*;
import java.util.Set;


/**
 *
 * @author timur
 */
public abstract class DatabaseRequestStorage extends DatabaseJobStorage implements RequestStorage{
    
    /** Creates a new instance of DatabaseRequestStorage */
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
    public abstract String getRequestCreateTableFields();
    
    public String getCreateTableFields() {
        return
        ", "+
        "CREDENTIALID "+  longType+
        ", "+
        "RETRYDELTATIME "+  intType+
        ", "+
        "SHOULDUPDATERETRYDELTATIME "+  booleanType+
        ", "+
        "DESCRIPTION "+ stringType+
        ", "+
        "CLIENTHOST "+ stringType+
        ", "+
        "STATUSCODE "+ stringType+
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
    String DESCRIPTION,
    String CLIENTHOST,
    String STATUSCODE,
    java.sql.ResultSet set,
    int next_index)throws java.sql.SQLException;
    
    protected final org.dcache.srm.scheduler.Job
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
        
        Long CREDENTIALID = new Long(set.getLong(next_index++));
        int RETRYDELTATIME = set.getInt(next_index++);
        boolean SHOULDUPDATERETRYDELTATIME = set.getBoolean(next_index++);
        String DESCRIPTION = set.getString(next_index++);
        String CLIENTHOST = set.getString(next_index++);
        String STATUSCODE= set.getString(next_index++);
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
        DESCRIPTION,
        CLIENTHOST,
        STATUSCODE,
        set,
        next_index );
    }
    public abstract void getUpdateAssignements(Request r,StringBuffer sb);
    
    public final void getUpdateAssignements(Job job,StringBuffer sb) {
        if(job == null || !(job instanceof Request)) {
            throw new IllegalArgumentException("job is not Request" );
        }
        Request r = (Request)job;
        sb.append(", CREDENTIALID = ").append( r.getCredentialId()).append(" ");
        sb.append(", RETRYDELTATIME = ").append( r.getRetryDeltaTime());
        sb.append(", SHOULDUPDATERETRYDELTATIME = ").append( r.isShould_updateretryDeltaTime() ? 1 : 0 );
        sb.append(", DESCRIPTION = ");
        String DESCRIPTION = r.getDescription();
        if(DESCRIPTION == null) {
            sb.append( "NULL ");
        }
        else {
            sb.append('\'').append(DESCRIPTION).append('\'');
        }
        sb.append(", CLIENTHOST = ");
        String CLIENTHOST = r.getClient_host();
        if(CLIENTHOST == null) {
            sb.append( "NULL ");
        }
        else {
            sb.append('\'').append(CLIENTHOST).append('\'');
        }
        sb.append(", STATUSCODE = ");
        String STATUSCODE = r.getStatusCodeString();
        if(STATUSCODE == null) {
            sb.append( "NULL ");
        }
        else {
            sb.append('\'').append(STATUSCODE).append('\'');
        }
        getUpdateAssignements(r,sb);
        
    }
    private static int ADDITIONAL_FIELDS_NUM=6;
    
    public abstract  void getCreateList(Request r,StringBuffer sb);
    
    public final void getCreateList(Job job, StringBuffer sb) {
        
        if(job == null || !(job instanceof Request)) {
            throw new IllegalArgumentException("job is not Request" );
        }
        Request r = (Request)job;
        
        sb.append(", ").append(r.getCredentialId()).append(" ");
        sb.append(", ").append(r.getRetryDeltaTime());
        sb.append(", ").append(r.isShould_updateretryDeltaTime() ? 1 : 0);
        String DESCRIPTION = r.getDescription();
        if(DESCRIPTION == null) {
            sb.append( ", NULL ");
        }
        else {
            sb.append(", '").append(DESCRIPTION).append('\'');
        }
        String CLIENTHOST = r.getClient_host();
        if(CLIENTHOST == null) {
            sb.append( ", NULL ");
        }
        else {
            sb.append(", '").append(CLIENTHOST).append('\'');
        }
        String STATUSCODE = r.getStatusCodeString();
        if(STATUSCODE == null) {
            sb.append( ", NULL ");
        }
        else {
            sb.append(", '").append(STATUSCODE).append('\'');
        }
        getCreateList(r,sb);
        
    }
    public abstract String[] getAdditionalCreateRequestStatements(Request r);
    
    public final String[] getAdditionalCreateStatements(Job job)  {
        if(job == null || !(job instanceof Request)) {
            throw new IllegalArgumentException("job is not a Request" );
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

    public Set getActiveRequestIds(String schedulerid, 
            String userId,
            String description )  throws java.sql.SQLException {
        String condition = " SCHEDULERID='"+schedulerid+
        "' AND STATE !="+State.DONE.getStateId()+
        " AND STATE !="+State.CANCELED.getStateId()+
        " AND STATE !="+State.FAILED.getStateId()+
        " AND CREATORID = '"+userId+'\'';
        if(description != null) {
            condition += " AND DESCRIPTION = '"+
                    description+'\'';
        }
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
    
    protected final void _verify(int nextIndex, int columnIndex, String tableName, String columnName, int columnType) throws SQLException {
        /*
         *additional fields:
                 ","+
        "CREDENTIALID "+  stringType+
        ","+
        "RETRYDELTATIME "+  intType+
        ","+
        "SHOULDUPDATERETRYDELTATIME "+  booleanType+
        ","+
        "DESCRIPTION "+  stringType+
        ","+
        "CLIENTHOST "+ stringType+
        ", "+
        "STATUSCODE "+ stringType+
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
        else if(columnIndex == nextIndex+3)
        {
            verifyStringType("DESCRIPTION",columnIndex,tableName, columnName, columnType);
        }
        else if(columnIndex == nextIndex+4)
        {
            verifyStringType("CLIENTHOST",columnIndex,tableName, columnName, columnType);
        }
        else if(columnIndex == nextIndex+5)
        {
            verifyStringType("STATUSCODE",columnIndex,tableName, columnName, columnType);
        }
        else
        {
            __verify(nextIndex+5,columnIndex,tableName, columnName, columnType);
        }
   }
       
    protected abstract int getMoreCollumnsNum();
    
    protected final int getAdditionalColumnsNum() {
        return ADDITIONAL_FIELDS_NUM +getMoreCollumnsNum();
    }

}
