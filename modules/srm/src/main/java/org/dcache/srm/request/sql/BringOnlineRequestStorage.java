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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

import org.dcache.srm.SRMUser;
import org.dcache.srm.request.BringOnlineRequest;
import org.dcache.srm.request.ContainerRequest;
import org.dcache.srm.request.FileRequest;
import org.dcache.srm.request.Job;
import org.dcache.srm.util.Configuration;

/**
 *
 * @author  timur
 */
public class BringOnlineRequestStorage extends DatabaseContainerRequestStorage{
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
    public BringOnlineRequestStorage(
    Configuration.DatabaseParameters configuration
    )  throws SQLException {
        super(configuration);
    }

    private String getProtocolsTableName()
    {
        return getTableName()+"_protocols";
    }

    @Override
    public void dbInit1() throws SQLException {
             if(reanamed_old_table) {
                    renameTable(getProtocolsTableName());

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

    @Override
    public void getCreateList(ContainerRequest r, StringBuffer sb) {

    }
    private static int ADDITIONAL_FIELDS;

    @Override
    protected ContainerRequest getContainerRequest(
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
    ResultSet set,
    int next_index)throws SQLException {

        String sql = "SELECT PROTOCOL FROM "+ getProtocolsTableName() +"  WHERE RequestID=?";
            PreparedStatement statement = _con.prepareStatement(sql);
            statement.setLong(1, ID);
            logger.debug("executing: SELECT PROTOCOL FROM {} WHERE RequestID={} ",
                    getProtocolsTableName(),ID);
            ResultSet fileIdsSet = statement.executeQuery();
            Set<String> utilset = new HashSet<>();
            while(fileIdsSet.next()) {
                utilset.add(fileIdsSet.getString(1));
            }
            String [] protocols = utilset.toArray(new String[utilset.size()]);
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
                        protocols);

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
    public PreparedStatement[] getAdditionalCreateStatements(Connection connection,
                                                             Job job) throws SQLException {
        if(job == null || !(job instanceof BringOnlineRequest)) {
            throw new IllegalArgumentException("Request is not BringOnlineRequest" );
        }
        BringOnlineRequest bor = (BringOnlineRequest)job;
        String[] protocols = bor.getProtocols();
        if(protocols ==null) {
            return null;
        }
        PreparedStatement[] statements  = new PreparedStatement[protocols.length];
        for(int i=0; i<protocols.length ; ++i){
            statements[i] = getPreparedStatement(connection,
                    insertProtocols,
                    protocols[i],
                    bor.getId());
        }
        return statements;
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
