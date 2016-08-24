/*
 * DatabaseFileRequestStorage.java
 *
 * Created on June 17, 2004, 3:18 PM
 */

package org.dcache.srm.request.sql;

import org.springframework.dao.DataAccessException;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.ScheduledExecutorService;

import org.dcache.srm.request.FileRequest;
import org.dcache.srm.util.Configuration;

/**
 *
 * @author  timur
 */
public abstract class DatabaseFileRequestStorage<F extends FileRequest<?>> extends DatabaseJobStorage<F>  {

    /** Creates a new instance of FileRequestStorage */
    public DatabaseFileRequestStorage(Configuration.DatabaseParameters configuration, ScheduledExecutorService executor)
            throws DataAccessException
    {
        super(configuration, executor);
    }

    protected abstract F getFileRequest(
    Connection _con,
    long ID,
    Long NEXTJOBID,
    long CREATIONTIME,
    long LIFETIME,
    int STATE,
    String SCHEDULERID,
    long SCHEDULER_TIMESTAMP,
    int NUMOFRETR,
    long LASTSTATETRANSITIONTIME,
    long REQUESTID,
    Long CREDENTIALID,
    String STATUSCODE,
    ResultSet set,
    int next_index)throws SQLException;

    @Override
    protected F
    getJob(
    Connection _con,
    long ID,
    Long NEXTJOBID,
    long CREATIONTIME,
    long LIFETIME,
    int STATE,
    String SCHEDULERID,
    long SCHEDULER_TIMESTAMP,
    int NUMOFRETR,
    long LASTSTATETRANSITIONTIME,
    ResultSet set,
    int next_index) throws SQLException {
        long REQUESTID = set.getLong(next_index++);
        Long CREDENTIALID = set.getLong(next_index++);
        String STATUSCODE= set.getString(next_index++);
        return getFileRequest(
        _con,
        ID,
        NEXTJOBID ,
        CREATIONTIME,
        LIFETIME,
        STATE,
        SCHEDULERID,
        SCHEDULER_TIMESTAMP,
        NUMOFRETR,
        LASTSTATETRANSITIONTIME,
        REQUESTID,
        CREDENTIALID,
        STATUSCODE,
        set,
        next_index );
    }

    @Override
    public abstract String getTableName();

    /*protected java.util.Set getFileRequests(String requestId) throws java.sql.SQLException{
        return getJobsByCondition(" REQUESTID = '"+requestId+"'");
    }*/
}
