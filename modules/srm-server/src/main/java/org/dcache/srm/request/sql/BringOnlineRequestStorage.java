// $Id$
// $Log: not supported by cvs2svn $
// Revision 1.3  2007/01/10 23:00:25  timur
// implemented srmGetRequestTokens, store request description in database, fixed several srmv2 issues
//
// Revision 1.2  2007/01/06 00:23:55  timur
// merging production branch changes to database layer to improve performance and reduce number of updates
//
/*
 * BringOnlineRequestStorage.java
 *
 * Created on June 22, 2004, 2:48 PM
 */

package org.dcache.srm.request.sql;

import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;

import javax.annotation.Nonnull;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;

import org.dcache.srm.SRMUser;
import org.dcache.srm.SRMUserPersistenceManager;
import org.dcache.srm.request.BringOnlineFileRequest;
import org.dcache.srm.request.BringOnlineRequest;
import org.dcache.srm.request.Job;
import org.dcache.srm.util.Configuration;

/**
 *
 * @author  timur
 */
public class BringOnlineRequestStorage extends DatabaseContainerRequestStorage<BringOnlineRequest,BringOnlineFileRequest> {
   private static final Logger LOGGER = LoggerFactory.getLogger(BringOnlineRequestStorage.class);
     public static final String TABLE_NAME ="bringonlinerequests";

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
    "LASTSTATETRANSITIONTIME,"+ // 10
     //Database Request Storage
    "RETRYDELTATIME , "+
    "SHOULDUPDATERETRYDELTATIME ,"+
    "DESCRIPTION ,"+
    "CLIENTHOST ,"+ // 15
    "STATUSCODE ,"+
    "USERID  ) " +
    "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";


    @Override
    public PreparedStatement getCreateStatement(Connection connection, Job job) throws SQLException {
        BringOnlineRequest bor = (BringOnlineRequest)job;
        return getPreparedStatement(connection,
                                    INSERT_SQL,
                                    bor.getId(),
                                    bor.getNextJobId(),
                                    bor.getCreationTime(),
                                    bor.getLifetime(),
                                    bor.getState().getStateId(),//5
                                    bor.latestHistoryEvent(),
                                    bor.getSchedulerId(),
                                    bor.getSchedulerTimeStamp(),
                                    0, // num of retries
                                    bor.getLastStateTransitionTime(), // 10
                                    //Database Request Storage
                                    bor.getRetryDeltaTime(),
                                                      bor.isShould_updateretryDeltaTime()?0:1,
                                    bor.getDescription(),
                                    bor.getClient_host(),
                                    bor.getStatusCodeString(),
                                    bor.getUser().getId());
    }

    private static final String UPDATE_REQUEST_SQL =
            UPDATE_PREFIX + ", RETRYDELTATIME=?," +
                " SHOULDUPDATERETRYDELTATIME=?," +
                " DESCRIPTION=?," +
                " CLIENTHOST=?," +
                " STATUSCODE=?," +
                " USERID=?" +
                " WHERE ID=?";
    @Override
    public PreparedStatement getUpdateStatement(Connection connection,
            Job job) throws SQLException {
        BringOnlineRequest bor = (BringOnlineRequest)job;
        return getPreparedStatement(connection,
                                    UPDATE_REQUEST_SQL,
                                    bor.getNextJobId(),
                                    bor.getCreationTime(),
                                    bor.getLifetime(),
                                    bor.getState().getStateId(),
                                    bor.latestHistoryEvent(),//5
                                    bor.getSchedulerId(),
                                    bor.getSchedulerTimeStamp(),
                                    0, // num of retries
                                    bor.getLastStateTransitionTime(),//10
                                    //Database Request Storage
                                    bor.getRetryDeltaTime(),
                                  bor.isShould_updateretryDeltaTime()?0:1,
                                    bor.getDescription(),
                                    bor.getClient_host(),
                                    bor.getStatusCodeString(),
                                    bor.getUser().getId(),
                                    bor.getId());
    }


    /** Creates a new instance of BringOnlineRequestStorage */
    public BringOnlineRequestStorage(@Nonnull String srmId,
            Configuration.DatabaseParameters configuration,
            ScheduledExecutorService executor, SRMUserPersistenceManager manager)
            throws DataAccessException
    {
        super(srmId, configuration, executor, manager);
    }

    private String getProtocolsTableName()
    {
        return getTableName()+"_protocols";
    }

    @Override
    protected BringOnlineRequest getContainerRequest(
    Connection _con,
    long ID,
    Long NEXTJOBID,
    long CREATIONTIME,
    long LIFETIME,
    int STATE,
    SRMUser user,
    String SCHEDULERID,
    long SCHEDULER_TIMESTAMP,
    int NUMOFRETR,
    long LASTSTATETRANSITIONTIME,
    Long CREDENTIALID,
    int RETRYDELTATIME,
    boolean SHOULDUPDATERETRYDELTATIME,
    String DESCRIPTION,
    String CLIENTHOST,
    String STATUSCODE,
    ImmutableList<BringOnlineFileRequest> fileRequests,
    ResultSet set,
    int next_index)throws SQLException {

        String sql = "SELECT PROTOCOL FROM "+ getProtocolsTableName() +"  WHERE RequestID=?";
            PreparedStatement statement = _con.prepareStatement(sql);
            statement.setLong(1, ID);
            LOGGER.debug("executing: SELECT PROTOCOL FROM {} WHERE RequestID={} ",
                    getProtocolsTableName(),ID);
            ResultSet fileIdsSet = statement.executeQuery();
            List<String> protocols = new ArrayList<>();
            while (fileIdsSet.next()) {
                protocols.add(fileIdsSet.getString(1));
            }
            statement.close();
            Job.JobHistory[] jobHistoryArray =
            getJobHistory(ID,_con);
            return new  BringOnlineRequest(
                        srmId,
                        ID,
                        NEXTJOBID,
                        CREATIONTIME,
                        LIFETIME,
                        STATE,
                        user,
                        SCHEDULERID,
                        SCHEDULER_TIMESTAMP,
                        NUMOFRETR,
                        LASTSTATETRANSITIONTIME,
                        jobHistoryArray,
                        fileRequests,
                        RETRYDELTATIME,
                        SHOULDUPDATERETRYDELTATIME,
                        DESCRIPTION,
                        CLIENTHOST,
                        STATUSCODE,
                        protocols.toArray(new String[protocols.size()]));

    }

    @Override
    public String getTableName() {
        return TABLE_NAME;
    }

    private final String insertProtocols =
        "INSERT INTO "+getProtocolsTableName()+
        " (PROTOCOL, RequestID) "+
        " VALUES (?,?)";

    @Override
    public PreparedStatement getBatchCreateStatement(Connection connection, Job job) throws SQLException {
        if(job == null || !(job instanceof BringOnlineRequest)) {
            throw new IllegalArgumentException("Request is not BringOnlineRequest" );
        }
        BringOnlineRequest bor = (BringOnlineRequest) job;
        String[] protocols = bor.getProtocols();
        if (protocols == null) {
            return null;
        }
        PreparedStatement statement = connection.prepareStatement(insertProtocols);
        for (String protocol : protocols) {
            statement.setString(1, protocol);
            statement.setLong(2, bor.getId());
            statement.addBatch();
        }
        return statement;
   }


    @Override
    public String getFileRequestsTableName() {
        return BringOnlineFileRequestStorage.TABLE_NAME;
    }
}
