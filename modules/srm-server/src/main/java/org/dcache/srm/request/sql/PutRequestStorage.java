/*
 * GetRequestStorage.java
 *
 * Created on June 22, 2004, 2:48 PM
 */

package org.dcache.srm.request.sql;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ConnectionCallback;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.dcache.srm.SRMUser;
import org.dcache.srm.request.Job;
import org.dcache.srm.request.PutFileRequest;
import org.dcache.srm.request.PutRequest;
import org.dcache.srm.util.Configuration;

import static org.dcache.srm.request.sql.Utilities.getIdentifierAsStored;

/**
 *
 * @author  timur
 */
public class PutRequestStorage extends DatabaseContainerRequestStorage<PutRequest,PutFileRequest> {
   private final static Logger logger =
            LoggerFactory.getLogger(PutRequestStorage.class);

     public static final String TABLE_NAME ="putrequests";
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
        PutRequest pr = (PutRequest)job;
        PreparedStatement stmt = getPreparedStatement(connection,
                                  INSERT_SQL,
                                  pr.getId(),
                                  pr.getNextJobId(),
                                  pr.getCreationTime(),
                                  pr.getLifetime(),
                                  pr.getState().getStateId(),//5
                                  pr.getErrorMessage(),
                                  pr.getSchedulerId(),
                                  pr.getSchedulerTimeStamp(),
                                  pr.getNumberOfRetries(),
                                  pr.getMaxNumberOfRetries(),//10
                                  pr.getLastStateTransitionTime(),
                                  //Database Request Storage
                                  pr.getCredentialId(),
                                  pr.getRetryDeltaTime(),
                                  pr.isShould_updateretryDeltaTime()?0:1,
                                  pr.getDescription(),
                                  pr.getClient_host(),
                                  pr.getStatusCodeString(),
                                  pr.getUser().getId());
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
        PutRequest pr = (PutRequest)job;
        PreparedStatement stmt = getPreparedStatement(
                                  connection,
                                  UPDATE_REQUEST_SQL,
                                  pr.getNextJobId(),
                                  pr.getCreationTime(),
                                  pr.getLifetime(),
                                  pr.getState().getStateId(),
                                  pr.getErrorMessage(),//5
                                  pr.getSchedulerId(),
                                  pr.getSchedulerTimeStamp(),
                                  pr.getNumberOfRetries(),
                                  pr.getMaxNumberOfRetries(),
                                  pr.getLastStateTransitionTime(),//10
                                  //Database Request Storage
                                  pr.getCredentialId(),
                                  pr.getRetryDeltaTime(),
                                  pr.isShould_updateretryDeltaTime()?0:1,
                                  pr.getDescription(),
                                  pr.getClient_host(),
                                  pr.getStatusCodeString(),
                                  pr.getUser().getId(),
                                  pr.getId());

        return stmt;
    }


    /** Creates a new instance of GetRequestStorage */
    public PutRequestStorage(Configuration.DatabaseParameters configuration)
            throws IOException, DataAccessException
    {
        super(configuration);
    }

    private String getProtocolsTableName() {
        return getTableName()+"_protocols";
    }

    private Boolean validateProtocolsTableSchema(final String tableName)
    {
        return jdbcTemplate.execute(new ConnectionCallback<Boolean>()
        {
            @Override
            public Boolean doInConnection(Connection con) throws SQLException, DataAccessException
            {
                DatabaseMetaData md = con.getMetaData();
                String tableNameAsStored =
                        getIdentifierAsStored(md, tableName);
                try (ResultSet columns = md.getColumns(null, null, tableNameAsStored, null)) {
                    if (!columns.next()) {
                        return false;
                    }
                    try {
                        verifyStringType("PROTOCOL", 1, tableName,
                                columns.getString("COLUMN_NAME"), columns.getInt("DATA_TYPE"));
                    } catch (SQLException e) {
                        return false;
                    }
                    if (!columns.next()) {
                        return false;
                    }
                    try {
                        verifyLongType("RequestID", 2, tableName,
                                columns.getString("COLUMN_NAME"), columns.getInt("DATA_TYPE"));
                    } catch (SQLException e) {
                        return false;
                    }
                    return true;
                }
            }
        });
    }

    @Override
    protected void dbInit(boolean clean) throws DataAccessException
    {
        super.dbInit(clean);

        String protocolsTableName = getProtocolsTableName().toLowerCase();
        if (droppedOldTable || !validateProtocolsTableSchema(protocolsTableName)) {
            dropTable(protocolsTableName);
        }

        String createProtocolsTable = "CREATE TABLE "+ protocolsTableName+" ( "+
                " PROTOCOL "+stringType+","+
                " RequestID "+longType+", "+ //forein key
                " CONSTRAINT fk_"+getTableName()+"_PP FOREIGN KEY (RequestID) REFERENCES "+
                getTableName() +" (ID) "+
                " ON DELETE CASCADE"+
                " )";
        logger.trace("calling createTable for {}", protocolsTableName);
        createTable(protocolsTableName, createProtocolsTable);
        createIndex(new String[]{ "RequestID" }, protocolsTableName);
    }

    private static int ADDITIONAL_FIELDS;

    @Override
    protected PutRequest getContainerRequest(
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
            PutFileRequest[] fileRequests,
            ResultSet set,
            int next_index)throws SQLException {
        String sqlStatementString = "SELECT PROTOCOL FROM " + getProtocolsTableName() +
                " WHERE RequestID="+ID;
        Statement sqlStatement = _con.createStatement();
        logger.debug("executing statement: "+sqlStatementString);
        ResultSet fileIdsSet = sqlStatement.executeQuery(sqlStatementString);
        List<String> protocols = new ArrayList<>();
        while(fileIdsSet.next()) {
            protocols.add(fileIdsSet.getString(1));
        }
        sqlStatement.close();
        Job.JobHistory[] jobHistoryArray =
                getJobHistory(ID,_con);
        return new  PutRequest(
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
    public PreparedStatement getBatchCreateStatement(Connection connection, Job job)
            throws SQLException {
        if (job == null || !(job instanceof PutRequest)) {
            throw new IllegalArgumentException("Request is not PutRequest" );
        }
        PutRequest pr = (PutRequest) job;
        String[] protocols = pr.getProtocols();
        if (protocols == null) {
            return null;
        }
        PreparedStatement statement = connection.prepareStatement(insertProtocols);
        for (String protocol : protocols) {
            statement.setString(1, protocol);
            statement.setLong(2, pr.getId());
            statement.addBatch();
        }
        return statement;
    }

    @Override
    public String getFileRequestsTableName() {
        return PutFileRequestStorage.TABLE_NAME;
    }

    @Override
    protected void __verify(int nextIndex, int columnIndex, String tableName, String columnName, int columnType) throws SQLException {
    }

    @Override
    protected int getMoreCollumnsNum() {
        return ADDITIONAL_FIELDS;
    }

}
