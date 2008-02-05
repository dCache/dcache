/*
 * GetRequestStorage.java
 *
 * Created on June 22, 2004, 2:48 PM
 */

package org.dcache.srm.request.sql;
import org.dcache.srm.request.ContainerRequest;
import org.dcache.srm.request.Request;
import org.dcache.srm.request.FileRequest;
import org.dcache.srm.request.GetRequest;
import org.dcache.srm.util.Configuration;
import java.sql.*;
import org.dcache.srm.scheduler.State;
import org.dcache.srm.scheduler.Job;

/**
 *
 * @author  timur
 */
public class GetRequestStorage extends DatabaseContainerRequestStorage{
    
    
    /** Creates a new instance of GetRequestStorage */
    public GetRequestStorage(
            Configuration configuration
            )  throws SQLException {
        super(configuration);
    }
    
    public void say(String s){
        if(logger != null) {
            logger.log(" GetRequestStorage: "+s);
        }
    }
    
    public void esay(String s){
        if(logger != null) {
            logger.elog(" GetRequestStorage: "+s);
        }
    }
    
    public void esay(Throwable t){
        if(logger != null) {
            logger.elog(t);
        }
    }
    
    
    private String getProtocolsTableName() {
        return getTableName()+"_protocols";
    }
    
    public void dbInit1() throws SQLException {
        say("dbInit1");
        String protocolsTableName = getProtocolsTableName().toLowerCase();
        boolean should_reanamed_old_table = reanamed_old_table;
        say("dbInit1 reanamed_old_table="+reanamed_old_table);
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
            esay(sqe);
            should_reanamed_old_table = true;
            if(_con != null) {
                pool.returnFailedConnection(_con);
                _con = null;
            }
        } catch (Exception ex) {
            esay(ex);
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
            esay("renameTable  "+protocolsTableName+" failed, might have been removed already, ignoring");
        }
        String createProtocolsTable = "CREATE TABLE "+ protocolsTableName+" ( "+
                " PROTOCOL "+stringType+ ","+
                " RequestID "+longType+ ", "+ //forein key
                " CONSTRAINT fk_"+getTableName()+"_PG FOREIGN KEY (RequestID) REFERENCES "+
                getTableName() +" (ID) "+
                " ON DELETE CASCADE"+
                " )";
        say("calling createTable for "+protocolsTableName);
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
            String CREATORID,
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
        say("executing statement: "+sqlStatementString);
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
                this,
                CREATIONTIME,
                LIFETIME,
                STATE,
                ERRORMESSAGE,
                CREATORID,
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
                configuration,
                protocols
                );
        
    }
    
    public String getRequestCreateTableFields() {
        return "";
    }
    public static final String TABLE_NAME ="getrequests";
    public String getTableName() {
        return TABLE_NAME;
    }
    
    public void getUpdateAssignements(ContainerRequest r, StringBuffer sb) {
    }
    
    public String[] getAdditionalCreateRequestStatements(ContainerRequest r)  {
        if(r == null || !(r instanceof GetRequest)) {
            throw new IllegalArgumentException("Request is not GetRequest" );
        }
        GetRequest gr = (GetRequest)r;
        String[] protocols = gr.getProtocols();
        String[] statements = new String[protocols.length];
        for(int i=0; i<protocols.length ; ++i){
            StringBuffer sb = new StringBuffer();
            sb.append("INSERT INTO ").append(getProtocolsTableName());
            
            sb.append( " VALUES ( '");
            sb.append(protocols[i]);
            sb.append("', '");
            sb.append(r.getId());
            sb.append("') ");
            statements[i] = sb.toString();
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
