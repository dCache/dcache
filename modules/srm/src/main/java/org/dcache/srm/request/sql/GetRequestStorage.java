/*
 * GetRequestStorage.java
 *
 * Created on June 22, 2004, 2:48 PM
 */

package org.dcache.srm.request.sql;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    public GetRequestStorage(
            Configuration.DatabaseParameters configuration
            )  throws SQLException {
        super(configuration);
    }

    private String getProtocolsTableName() {
        return getTableName()+"_protocols";
    }

    @Override
    public void dbInit1() throws SQLException {
        logger.debug("dbInit1");
        String protocolsTableName = getProtocolsTableName().toLowerCase();
        boolean should_reanamed_old_table = reanamed_old_table;
        logger.debug("dbInit1 reanamed_old_table="+reanamed_old_table);
        Connection _con =null;
        try {
            _con = pool.getConnection();
            _con.setAutoCommit(true);

            DatabaseMetaData md = _con.getMetaData();

            String tableNameAsStored =
                getIdentifierAsStored(md, protocolsTableName);
            ResultSet columns =
                md.getColumns(null, null, tableNameAsStored, null);
            if(columns.next()){
                String columnName = columns.getString("COLUMN_NAME");
                int columnDataType = columns.getInt("DATA_TYPE");
                verifyStringType("PROTOCOL",1,protocolsTableName,
                        columnName,columnDataType);
                if(columns.next()){
                    columnName = columns.getString("COLUMN_NAME");
                    columnDataType = columns.getInt("DATA_TYPE");
                    verifyLongType("RequestID",2,getProtocolsTableName(),
                            columnName,columnDataType);
                } else {

                    should_reanamed_old_table = true;
                }
            } else {
                // If getColumns returns a zero-sized ResourceSet then
                // the table doesn't exist; no need to rename it.
            }
            _con.setAutoCommit(false);
            pool.returnConnection(_con);
            _con =null;
        } catch (SQLException sqe) {
            logger.error(sqe.toString());
            should_reanamed_old_table = true;
            if(_con != null) {
                pool.returnFailedConnection(_con);
                _con = null;
            }
        } catch (Exception ex) {
            logger.error(ex.toString());
            should_reanamed_old_table = true;
            if(_con != null) {
                pool.returnFailedConnection(_con);
                _con = null;
            }
        } finally {
            if(_con != null) {
                _con.setAutoCommit(false);
                pool.returnConnection(_con);
            }
        }
        try {

            if(should_reanamed_old_table) {
                renameTable(protocolsTableName);
            }
        }
        catch (SQLException sqle) {
            logger.error("renameTable  "+protocolsTableName+" failed, might have been removed already, ignoring");
        }
        String createProtocolsTable = "CREATE TABLE "+ protocolsTableName+" ( "+
                " PROTOCOL "+stringType+ ","+
                " RequestID "+longType+ ", "+ //forein key
                " CONSTRAINT fk_"+getTableName()+"_PG FOREIGN KEY (RequestID) REFERENCES "+
                getTableName() +" (ID) "+
                " ON DELETE CASCADE"+
                " )";
        logger.debug("calling createTable for "+protocolsTableName);
        createTable(protocolsTableName, createProtocolsTable);
        String protocols_columns[] = {
            "RequestID"};
        createIndex(protocols_columns,getProtocolsTableName());
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
