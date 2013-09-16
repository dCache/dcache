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
    public PutRequestStorage(Configuration.DatabaseParameters configuration) throws SQLException {
        super(
                configuration);
    }

    private String getProtocolsTableName() {
        return getTableName()+"_protocols";
    }

    @Override
    public void dbInit1() throws SQLException {
        boolean should_reanamed_old_table = reanamed_old_table;
        String protocolsTableName = getProtocolsTableName().toLowerCase();
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
                verifyStringType("PROTOCOL",1,protocolsTableName ,
                        columnName,columnDataType);
                if(columns.next()){
                    columnName = columns.getString("COLUMN_NAME");
                    columnDataType = columns.getInt("DATA_TYPE");
                    verifyLongType("RequestID",2,protocolsTableName ,
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
                " PROTOCOL "+stringType+","+
                " RequestID "+longType+", "+ //forein key
                " CONSTRAINT fk_"+getTableName()+"_PP FOREIGN KEY (RequestID) REFERENCES "+
                getTableName() +" (ID) "+
                " ON DELETE CASCADE"+
                " )";
        logger.debug("calling createTable for "+protocolsTableName);
        createTable(protocolsTableName, createProtocolsTable);
        String protocols_columns[] = {
            "RequestID"};
        createIndex(protocols_columns,protocolsTableName );

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
