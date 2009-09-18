/*
 * FileRequestStorage.java
 *
 * Created on June 17, 2004, 3:18 PM
 */

package org.dcache.srm.request.sql;

import org.dcache.srm.request.ContainerRequest;
import org.dcache.srm.request.FileRequest;
import org.dcache.srm.request.Request;
import org.dcache.srm.scheduler.Job;
import org.dcache.srm.scheduler.State;
import org.dcache.srm.util.Configuration;
import java.sql.*;
import java.util.Set;
import org.dcache.srm.SRMUser;
import org.dcache.srm.SRMInvalidRequestException;

/**
 *
 * @author  timur
 */
public abstract class DatabaseContainerRequestStorage extends DatabaseRequestStorage {
    
    
    /** Creates a new instance of DatabaseContainerRequestStorage */
    public DatabaseContainerRequestStorage(Configuration configuration) throws SQLException {
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
    
    
    protected abstract ContainerRequest getContainerRequest(
    Connection _con,
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
    int next_index)throws java.sql.SQLException;
    
    protected org.dcache.srm.request.Request
    getRequest(
    Connection _con,
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
            try {
                fileRequests[i] = (FileRequest) Job.getJob(fileIds[i],_con);
            } catch (SRMInvalidRequestException ire){
                logger.elog(ire);
            }
        }
        return getContainerRequest(
        _con,
        ID,
        NEXTJOBID ,
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
        CREDENTIALID,
        RETRYDELTATIME, 
        SHOULDUPDATERETRYDELTATIME,
        DESCRIPTION,
        CLIENTHOST,
        STATUSCODE,
        fileRequests,
        set,
        next_index );
    }
    
    public abstract String getTableName();
    
    
    public abstract  void getCreateList(ContainerRequest cr,StringBuffer sb);
    
    public void getCreateList(Request r, StringBuffer sb) {
        
        if(r == null || !(r instanceof ContainerRequest)) {
            throw new IllegalArgumentException("Request is not ContainerRequest" );
        }
        ContainerRequest cr = (ContainerRequest)r;
        
        getCreateList(cr,sb);
        
    }
    
}

