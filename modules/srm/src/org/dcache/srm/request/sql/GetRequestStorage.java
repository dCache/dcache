/*
 * GetRequestStorage.java
 *
 * Created on June 22, 2004, 2:48 PM
 */

package org.dcache.srm.request.sql;
import org.dcache.srm.request.ContainerRequest;
import org.dcache.srm.request.FileRequest;
import org.dcache.srm.request.GetRequest;
import org.dcache.srm.util.Configuration;
import java.sql.*;
import org.dcache.srm.scheduler.Job;
import org.dcache.srm.SRMUser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author  timur
 */
public class GetRequestStorage extends DatabaseContainerRequestStorage{
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
            Configuration configuration
            )  throws SQLException {
        super(configuration);
    }
    
    private String getProtocolsTableName() {
        return getTableName()+"_protocols";
    }
    
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
            ResultSet columns = md.getColumns(null, null, protocolsTableName, null);
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
                should_reanamed_old_table = true;
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
    
    public void getCreateList(ContainerRequest r, StringBuffer sb) {
        
    }
    private static int ADDITIONAL_FIELDS = 0;
    
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
            java.sql.ResultSet set,
            int next_index)throws java.sql.SQLException {
        
        String sqlStatementString = "SELECT PROTOCOL FROM " + getProtocolsTableName() +
                " WHERE RequestID="+ID;
        Statement sqlStatement = _con.createStatement();
        logger.debug("executing statement: "+sqlStatementString);
        ResultSet fileIdsSet = sqlStatement.executeQuery(sqlStatementString);
        java.util.Set utilset = new java.util.HashSet();
        while(fileIdsSet.next()) {
            utilset.add(fileIdsSet.getString(1));
        }
        String [] protocols = (String[]) utilset.toArray(new String[0]);
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
    
    public String getRequestCreateTableFields() {
        return "";
    }
    public String getTableName() {
        return TABLE_NAME;
    }
    
    public void getUpdateAssignements(ContainerRequest r, StringBuffer sb) {
    }
    
    private final String insertProtocols =
        "INSERT INTO "+getProtocolsTableName()+
        " (PROTOCOL, RequestID) "+
        " VALUES (?,?)";

    @Override
    public PreparedStatement[] getAdditionalCreateStatements(Connection connection,
                                                             Job job) throws SQLException {
        if(job == null || !(job instanceof GetRequest)) {
            throw new IllegalArgumentException("Request is not GetRequest" );
        }
        GetRequest gr = (GetRequest)job;
        String[] protocols = gr.getProtocols();
        if(protocols ==null)  return null;
        PreparedStatement[] statements  = new PreparedStatement[protocols.length];
        for(int i=0; i<protocols.length ; ++i){
            statements[i] = getPreparedStatement(connection,
                    insertProtocols,
                    protocols[i],
                    gr.getId());
        }
        return statements;
    }
    
    public String getFileRequestsTableName() {
        return GetFileRequestStorage.TABLE_NAME;
    }
    
    protected void __verify(int nextIndex, int columnIndex, String tableName, String columnName, int columnType) throws SQLException {
    }
    
    
    protected int getMoreCollumnsNum() {
        return ADDITIONAL_FIELDS;
    }
    
}
