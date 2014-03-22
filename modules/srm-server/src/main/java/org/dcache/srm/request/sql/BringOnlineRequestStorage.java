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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;

import org.dcache.srm.SRMUser;
import org.dcache.srm.request.BringOnlineFileRequest;
import org.dcache.srm.request.BringOnlineRequest;
import org.dcache.srm.request.Job;
import org.dcache.srm.util.Configuration;

/**
 *
 * @author  timur
 */
public class BringOnlineRequestStorage extends DatabaseContainerRequestStorage<BringOnlineRequest,BringOnlineFileRequest> {
   private final static Logger logger =
            LoggerFactory.getLogger(BringOnlineRequestStorage.class);
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
        "MAXNUMOFRETR=?," +
        "LASTSTATETRANSITIONTIME=? ";//10

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
    "MAXNUMOFRETR ,"+ //10
    "LASTSTATETRANSITIONTIME,"+
     //Database Request Storage
    "CREDENTIALID , " +
    "RETRYDELTATIME , "+
    "SHOULDUPDATERETRYDELTATIME ,"+
    "DESCRIPTION ,"+ //15
    "CLIENTHOST ,"+
    "STATUSCODE ,"+
    "USERID  ) " +
    "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";


    @Override
    public PreparedStatement getCreateStatement(Connection connection, Job job) throws SQLException {
        BringOnlineRequest bor = (BringOnlineRequest)job;
        PreparedStatement stmt = getPreparedStatement(connection,
                                                      INSERT_SQL,
                                                      bor.getId(),
                                                      bor.getNextJobId(),
                                                      bor.getCreationTime(),
                                                      bor.getLifetime(),
                                                      bor.getState().getStateId(),//5
                                                      bor.getErrorMessage(),
                                                      bor.getSchedulerId(),
                                                      bor.getSchedulerTimeStamp(),
                                                      bor.getNumberOfRetries(),
                                                      bor.getMaxNumberOfRetries(),//10
                                                      bor.getLastStateTransitionTime(),
                                                      //Database Request Storage
                                                      bor.getCredentialId(),
                                                      bor.getRetryDeltaTime(),
                                                      bor.isShould_updateretryDeltaTime()?0:1,
                                                      bor.getDescription(),
                                                      bor.getClient_host(),
                                                      bor.getStatusCodeString(),
                                                      bor.getUser().getId());
       return stmt;
    }

    private static final String UPDATE_REQUEST_SQL =
            UPDATE_PREFIX + ", CREDENTIALID=?," +
                " RETRYDELTATIME=?," +
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
        PreparedStatement stmt = getPreparedStatement(connection,
                                  UPDATE_REQUEST_SQL,
                                  bor.getNextJobId(),
                                  bor.getCreationTime(),
                                  bor.getLifetime(),
                                  bor.getState().getStateId(),
                                  bor.getErrorMessage(),//5
                                  bor.getSchedulerId(),
                                  bor.getSchedulerTimeStamp(),
                                  bor.getNumberOfRetries(),
                                  bor.getMaxNumberOfRetries(),
                                  bor.getLastStateTransitionTime(),//10
                                  //Database Request Storage
                                  bor.getCredentialId(),
                                  bor.getRetryDeltaTime(),
                                  bor.isShould_updateretryDeltaTime()?0:1,
                                  bor.getDescription(),
                                  bor.getClient_host(),
                                  bor.getStatusCodeString(),
                                  bor.getUser().getId(),
                                  bor.getId());

        return stmt;
    }


    /** Creates a new instance of BringOnlineRequestStorage */
    public BringOnlineRequestStorage(Configuration.DatabaseParameters configuration, ScheduledExecutorService executor)
            throws IOException, DataAccessException
    {
        super(configuration, executor);
    }

    private String getProtocolsTableName()
    {
        return getTableName()+"_protocols";
    }

    @Override
    protected void dbInit(boolean clean) throws DataAccessException
    {
            super.dbInit(clean);
            if (droppedOldTable) {
                    dropTable(getProtocolsTableName());
            }
            String protocolsTableName = getProtocolsTableName().toLowerCase();
            String createProtocolsTable = "CREATE TABLE "+ protocolsTableName+" ( "+
            " PROTOCOL "+stringType+ ","+
            " RequestID "+longType+ ", "+ //forein key
            " CONSTRAINT fk_"+getTableName()+"_PG FOREIGN KEY (RequestID) REFERENCES "+
            getTableName() +" (ID) "+
            " ON DELETE CASCADE"+
            " )";
            createTable(protocolsTableName, createProtocolsTable);
   }

    private static int ADDITIONAL_FIELDS;

    @Override
    protected BringOnlineRequest getContainerRequest(
    Connection _con,
    long ID,
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
    BringOnlineFileRequest[] fileRequests,
    ResultSet set,
    int next_index)throws SQLException {

        String sql = "SELECT PROTOCOL FROM "+ getProtocolsTableName() +"  WHERE RequestID=?";
            PreparedStatement statement = _con.prepareStatement(sql);
            statement.setLong(1, ID);
            logger.debug("executing: SELECT PROTOCOL FROM {} WHERE RequestID={} ",
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
                        ID,
                        NEXTJOBID,
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
                        jobHistoryArray,
                        CREDENTIALID,
                        fileRequests,
                        RETRYDELTATIME,
                        SHOULDUPDATERETRYDELTATIME,
                        DESCRIPTION,
                        CLIENTHOST,
                        STATUSCODE,
                        protocols.toArray(new String[protocols.size()]));

    }

    @Override
    public String getRequestCreateTableFields() {
        return "";
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

    @Override
    protected void __verify(int nextIndex, int columnIndex, String tableName, String columnName, int columnType) throws SQLException {
    }


    @Override
    protected int getMoreCollumnsNum() {
         return ADDITIONAL_FIELDS;
     }

}
