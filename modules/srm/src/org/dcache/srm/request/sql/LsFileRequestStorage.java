package org.dcache.srm.request.sql;
import java.sql.*;
import org.dcache.srm.request.FileRequest;
import org.dcache.srm.request.LsFileRequest;
import org.dcache.srm.util.Configuration;
import org.dcache.srm.scheduler.Job;

public class LsFileRequestStorage extends DatabaseFileRequestStorage {
    public static final String TABLE_NAME = "lsfilerequests";
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

        private static int ADDITIONAL_FIELDS = 1;
        public LsFileRequestStorage(Configuration.DatabaseParameters configuration)
                throws SQLException {
                super(configuration);
        }

        protected FileRequest getFileRequest(Connection connection,
                                             Long ID,
                                             Long NEXTJOBID,
                                             long CREATIONTIME,
                                             long LIFETIME,
                                             int STATE,
                                             String ERRORMESSAGE,
                                             String SCHEDULERID,
                                             long SCHEDULER_TIMESTAMP,
                                             int NUMOFRETR,
                                             int MAXNUMOFRETR,
                                             long LASTSTATETRANSITIONTIME,
                                             Long REQUESTID,
                                             Long CREDENTIALID,
                                             String STATUSCODE,
                                             java.sql.ResultSet set,
                                             int next_index) throws SQLException {
                String SURL = set.getString(next_index++);
                Job.JobHistory[] jobHistoryArray =
                        getJobHistory(ID,connection);
                return new LsFileRequest(ID,
                                         NEXTJOBID ,
                                         CREATIONTIME,
                                         LIFETIME,
                                         STATE,
                                         ERRORMESSAGE,
                                         SCHEDULERID,
                                         SCHEDULER_TIMESTAMP,
                                         NUMOFRETR,
                                         MAXNUMOFRETR,
                                         LASTSTATETRANSITIONTIME,
                                         jobHistoryArray,
                                         REQUESTID,
                                         CREDENTIALID,
                                         STATUSCODE,
                                         SURL);
        }

        public String getFileRequestCreateTableFields() {
                return
                        ","+
                        "SURL "+  stringType;
        }

        public String getTableName() {
                return TABLE_NAME;
        }


        public PreparedStatement getStatement(Connection connection,
                                              String query,
                                              Job fr) throws SQLException {
                LsFileRequest gfr = (LsFileRequest)fr;
                PreparedStatement stmt = getPreparedStatement(connection,
                                          query,
                                          gfr.getNextJobId(),
                                          gfr.getCreationTime(),
                                          gfr.getLifetime(),
                                          gfr.getState().getStateId(),
                                          gfr.getErrorMessage(),
                                          gfr.getSchedulerId(),
                                          gfr.getSchedulerTimeStamp(),
                                          gfr.getNumberOfRetries(),
                                          gfr.getMaxNumberOfRetries(),
                                          gfr.getLastStateTransitionTime(),
                                          gfr.getRequestId(),
                                          gfr.getCredentialId(),
                                          gfr.getStatusCodeString(),
                                          gfr.getSurlString(),
                                          gfr.getId());
                return stmt;
        }

        private static final String UPDATE_REQUEST_SQL =
                UPDATE_PREFIX +
                ", REQUESTID=?" +
                ", CREDENTIALID=?" +
                ", STATUSCODE=?" +
                ", SURL=? WHERE ID=?";
        public PreparedStatement getUpdateStatement(Connection connection,
                                                    Job fr)
                throws SQLException {
                if(fr == null || !(fr instanceof LsFileRequest)) {
                        throw new IllegalArgumentException("fr is not LsFileRequest" );
                }
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
            "MAXNUMOFRETR ,"+ //10
            "LASTSTATETRANSITIONTIME,"+
            //DATABASE FILE REQUEST STORAGE
            "REQUESTID , " +
            "CREDENTIALID , "+
            "STATUSCODE,  "+
            "SURL )"+
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

        public PreparedStatement getCreateStatement(Connection connection,
                                                Job fr)
                throws SQLException {
                if(fr == null || !(fr instanceof LsFileRequest)) {
                        throw new IllegalArgumentException("fr is not LsFileRequest" );
                }
                LsFileRequest gfr = (LsFileRequest)fr;
                PreparedStatement stmt = getPreparedStatement(connection,
                                          INSERT_SQL,
                                          gfr.getId(),
                                          gfr.getNextJobId(),
                                          gfr.getCreationTime(),
                                          gfr.getLifetime(),
                                          gfr.getState().getStateId(),
                                          gfr.getErrorMessage(),
                                          gfr.getSchedulerId(),
                                          gfr.getSchedulerTimeStamp(),
                                          gfr.getNumberOfRetries(),
                                          gfr.getMaxNumberOfRetries(),
                                          gfr.getLastStateTransitionTime(),
                                          gfr.getRequestId(),
                                          gfr.getCredentialId(),
                                          gfr.getStatusCodeString(),
                                          gfr.getSurlString());
                return stmt;
        }


        public String getRequestTableName() {
                return LsRequestStorage.TABLE_NAME;
        }

        protected void __verify(int nextIndex,
                                int columnIndex,
                                String tableName,
                                String columnName,
                                int columnType) throws SQLException {
                if(columnIndex == nextIndex) {
                        verifyStringType("SURL",columnIndex,tableName, columnName, columnType);
                }
                else {
                        throw new SQLException("database table schema changed:"+
                                               "table named "+tableName+
                                               " column #"+columnIndex+" has name \""+columnName+
                                               "\"  has type \""+getTypeName(columnType)+
                                               " this column should not be present!!!");
                }
        }

        protected int getMoreCollumnsNum() {
                return ADDITIONAL_FIELDS;
        }
}
