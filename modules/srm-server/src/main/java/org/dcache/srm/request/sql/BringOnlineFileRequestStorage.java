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

import org.springframework.dao.DataAccessException;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.ScheduledExecutorService;

import org.dcache.srm.request.BringOnlineFileRequest;
import org.dcache.srm.request.Job;
import org.dcache.srm.util.Configuration;

/**
 *
 * @author  timur
 */
public class BringOnlineFileRequestStorage extends DatabaseFileRequestStorage<BringOnlineFileRequest> {

    public static final String TABLE_NAME = "bringonlinefilerequests";
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


    /** Creates a new instance of BringOnlineFileRequestStorage */
    public BringOnlineFileRequestStorage(Configuration.DatabaseParameters configuration, ScheduledExecutorService executor)
            throws DataAccessException
    {
        super(configuration, executor);
    }


    @Override
    protected BringOnlineFileRequest getFileRequest(Connection _con,
        long ID,
        Long NEXTJOBID,
        long CREATIONTIME,
        long LIFETIME,
        int STATE,
        String ERRORMESSAGE,
        String SCHEDULERID,
        long SCHEDULER_TIMESTAMP,
        int NUMOFRETR,
        long LASTSTATETRANSITIONTIME,
        long REQUESTID,
        Long CREDENTIALID,
        String STATUSCODE,
        ResultSet set,
        int next_index) throws SQLException {
           String SURL = set.getString(next_index++);
           String FILEID = set.getString(next_index++);
           String PINID = set.getString(next_index);
            Job.JobHistory[] jobHistoryArray =
            getJobHistory(ID,_con);
           return new BringOnlineFileRequest(
            ID,
            NEXTJOBID ,
            CREATIONTIME,
            LIFETIME,
            STATE,
            ERRORMESSAGE,
            SCHEDULERID,
            SCHEDULER_TIMESTAMP,
            NUMOFRETR,
            LASTSTATETRANSITIONTIME,
            jobHistoryArray,
            REQUESTID,
            STATUSCODE,
            SURL,
            FILEID,
            PINID);
    }

    @Override
    public String getTableName() {
        return TABLE_NAME;
    }

    private PreparedStatement getStatement(Connection connection,
                                          String query,
                                          Job fr) throws SQLException {
        BringOnlineFileRequest gfr = (BringOnlineFileRequest)fr;
        PreparedStatement stmt = getPreparedStatement(connection,
                                  query,
                                  gfr.getNextJobId(),
                                  gfr.getCreationTime(),
                                  gfr.getLifetime(),
                                  gfr.getState().getStateId(),
                                  gfr.getErrorMessage(),//5
                                  gfr.getSchedulerId(),
                                  gfr.getSchedulerTimeStamp(),
                                  0, // num of retries
                                  gfr.getLastStateTransitionTime(),
                                  gfr.getRequestId(),//10
                                  gfr.getStatusCodeString(),
                                  gfr.getSurlString(),
                                  gfr.getFileId(),
                                  gfr.getPinId(),
                                  gfr.getId());//15
        return stmt;
    }

    private static final String UPDATE_REQUEST_SQL =
            UPDATE_PREFIX +
            ", REQUESTID=?" +
            ", STATUSCODE=?" +
            ", SURL=?" +
            ", FILEID=?" +
            ", PINID=? WHERE ID=?";
    @Override
    public PreparedStatement getUpdateStatement(Connection connection,
                                                Job fr)
        throws SQLException {
        return getStatement(connection,UPDATE_REQUEST_SQL, fr);
    }

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
        "LASTSTATETRANSITIONTIME,"+
         //DATABSE FILE REQUEST STORAGE
        "REQUESTID , " +
        "STATUSCODE ,"+
         // BRING ONLINE FILE REQUEST
        "SURL ,"+ //15
        "FILEID ,"+
        "PINID  ) " +
        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

    @Override
    public PreparedStatement getCreateStatement(Connection connection,
                                                Job fr)
        throws SQLException {
        BringOnlineFileRequest gfr = (BringOnlineFileRequest)fr;
        PreparedStatement stmt = getPreparedStatement(connection,
                                  INSERT_SQL,
                                  gfr.getId(),
                                  gfr.getNextJobId(),
                                  gfr.getCreationTime(),
                                  gfr.getLifetime(),
                                  gfr.getState().getStateId(),//5
                                  gfr.getErrorMessage(),
                                  gfr.getSchedulerId(),
                                  gfr.getSchedulerTimeStamp(),
                                  0, // num of retries
                                  gfr.getLastStateTransitionTime(),
                                 //DATABSE FILE REQUEST STORAGE
                                  gfr.getRequestId(),
                                  gfr.getStatusCodeString(),
                                 // BRING ONLINE FILE REQUEST
                                  gfr.getSurlString(),
                                  gfr.getFileId(),//15
                                  gfr.getPinId());
       return stmt;
    }
}
