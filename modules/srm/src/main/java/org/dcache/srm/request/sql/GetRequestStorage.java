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
import org.dcache.srm.request.GetFileRequest;
import org.dcache.srm.request.GetRequest;
import org.dcache.srm.request.Job;
import org.dcache.srm.util.Configuration;

import static org.dcache.srm.request.sql.Utilities.getIdentifierAsStored;

/**
 *
 * @author  timur
 */
public class GetRequestStorage extends DatabaseContainerRequestStorage<GetRequest,GetFileRequest> {
    private final static Logger logger =
            LoggerFactory.getLogger(GetRequestStorage.class);
    public static final String TABLE_NAME ="getrequests";
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
        GetRequest gr = (GetRequest)job;
        PreparedStatement stmt = getPreparedStatement(connection,
                                  INSERT_SQL,
                                  gr.getId(),
                                  gr.getNextJobId(),
                                  gr.getCreationTime(),
                                  gr.getLifetime(),
                                  gr.getState().getStateId(),//5
                                  gr.getErrorMessage(),
                                  gr.getSchedulerId(),
                                  gr.getSchedulerTimeStamp(),
                                  gr.getNumberOfRetries(),
                                  gr.getMaxNumberOfRetries(),//10
                                  gr.getLastStateTransitionTime(),
                                  //Database Request Storage
                                  gr.getCredentialId(),
                                  gr.getRetryDeltaTime(),
                                  gr.isShould_updateretryDeltaTime()?0:1,
                                  gr.getDescription(),
                                  gr.getClient_host(),
                                  gr.getStatusCodeString(),
                                  gr.getUser().getId());
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
        GetRequest gr = (GetRequest)job;
        PreparedStatement stmt = getPreparedStatement(connection,
                                  UPDATE_REQUEST_SQL,
                                  gr.getNextJobId(),
                                  gr.getCreationTime(),
                                  gr.getLifetime(),
                                  gr.getState().getStateId(),
                                  gr.getErrorMessage(),//5
                                  gr.getSchedulerId(),
                                  gr.getSchedulerTimeStamp(),
                                  gr.getNumberOfRetries(),
                                  gr.getMaxNumberOfRetries(),
                                  gr.getLastStateTransitionTime(),//10
                                  //Database Request Storage
                                  gr.getCredentialId(),
                                  gr.getRetryDeltaTime(),
                                  gr.isShould_updateretryDeltaTime()?0:1,
                                  gr.getDescription(),
                                  gr.getClient_host(),
                                  gr.getStatusCodeString(),
                                  gr.getUser().getId(),
                                  gr.getId());

        return stmt;
    }


    /** Creates a new instance of GetRequestStorage */
    public GetRequestStorage(Configuration.DatabaseParameters configuration)
            throws DataAccessException, IOException
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
    protected GetRequest getContainerRequest(
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
            GetFileRequest[] fileRequests,
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
        return new  GetRequest(
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
    public PreparedStatement getBatchCreateStatement(Connection connection, Job job) throws SQLException {
        if (job == null || !(job instanceof GetRequest)) {
            throw new IllegalArgumentException("Request is not GetRequest" );
        }
        GetRequest gr = (GetRequest)job;
        String[] protocols = gr.getProtocols();
        if (protocols == null) {
            return null;
        }
        PreparedStatement statement = connection.prepareStatement(insertProtocols);
        for (String protocol : protocols) {
            statement.setString(1, protocol);
            statement.setLong(2, gr.getId());
            statement.addBatch();
        }
        return statement;
    }

    @Override
    public String getFileRequestsTableName() {
        return GetFileRequestStorage.TABLE_NAME;
    }

    @Override
    protected void __verify(int nextIndex, int columnIndex, String tableName, String columnName, int columnType) throws SQLException {
    }


    @Override
    protected int getMoreCollumnsNum() {
        return ADDITIONAL_FIELDS;
    }

}
