package org.dcache.srm.request.sql;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.dcache.srm.SRMUser;
import org.dcache.srm.request.Job;
import org.dcache.srm.request.LsFileRequest;
import org.dcache.srm.request.LsRequest;
import org.dcache.srm.util.Configuration;

public class LsRequestStorage extends DatabaseContainerRequestStorage<LsRequest,LsFileRequest> {
    public static final String TABLE_NAME ="lsrequests";

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
        "USERID , " +
        // LS REQUEST
        "EXPLANATION ,"+
        "LONGFORMAT ,"+
        "NUMOFLEVELS ,"+
        "\"count\" ,"+
        "LSOFFSET ) "+
        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

    @Override
    public PreparedStatement getCreateStatement(Connection connection, Job job) throws SQLException {
        LsRequest lr = (LsRequest)job;
        PreparedStatement stmt = getPreparedStatement(connection,
                                  INSERT_SQL,
                                  lr.getId(),
                                  lr.getNextJobId(),
                                  lr.getCreationTime(),
                                  lr.getLifetime(),
                                  lr.getState().getStateId(),//5
                                  lr.getErrorMessage(),
                                  lr.getSchedulerId(),
                                  lr.getSchedulerTimeStamp(),
                                  lr.getNumberOfRetries(),
                                  lr.getMaxNumberOfRetries(),//10
                                  lr.getLastStateTransitionTime(),
                                  //Database Request Storage
                                  lr.getCredentialId(),
                                  lr.getRetryDeltaTime(),
                                  lr.isShould_updateretryDeltaTime()?0:1,
                                  lr.getDescription(),
                                  lr.getClient_host(),
                                  lr.getStatusCodeString(),
                                  lr.getUser().getId(),
                                  lr.getExplanation(),
                                  lr.getLongFormat()==true?1:0,
                                  lr.getNumOfLevels(),
                                  lr.getCount(),
                                  lr.getOffset()
                                  );
       return stmt;
    }

    private static final String UPDATE_REQUEST_SQL =
            UPDATE_PREFIX + ", CREDENTIALID=?," +
                " RETRYDELTATIME=?," +
                " SHOULDUPDATERETRYDELTATIME=?," +
                " DESCRIPTION=?," +
                " CLIENTHOST=?," +
                " STATUSCODE=?," +
                " USERID=?," +
                // LS REQUEST
                " EXPLANATION=?,"+
                " LONGFORMAT=?,"+
                " NUMOFLEVELS=?,"+
                " \"count\"=?,"+
                " LSOFFSET=? "+
                " WHERE ID=?";
    @Override
    public PreparedStatement getUpdateStatement(Connection connection,
            Job job) throws SQLException {
        LsRequest lr = (LsRequest)job;
        PreparedStatement stmt = getPreparedStatement(connection,
                                  UPDATE_REQUEST_SQL,
                                  lr.getNextJobId(),
                                  lr.getCreationTime(),
                                  lr.getLifetime(),
                                  lr.getState().getStateId(),
                                  lr.getErrorMessage(),//5
                                  lr.getSchedulerId(),
                                  lr.getSchedulerTimeStamp(),
                                  lr.getNumberOfRetries(),
                                  lr.getMaxNumberOfRetries(),
                                  lr.getLastStateTransitionTime(),//10
                                  //Database Request Storage
                                  lr.getCredentialId(),
                                  lr.getRetryDeltaTime(),
                                  lr.isShould_updateretryDeltaTime()?0:1,
                                  lr.getDescription(),
                                  lr.getClient_host(),
                                  lr.getStatusCodeString(),
                                  lr.getUser().getId(),
                                  lr.getExplanation(),
                                  lr.getLongFormat()==true?1:0,
                                  lr.getNumOfLevels(),
                                  lr.getCount(),
                                  lr.getOffset(),
                                  lr.getId());

        return stmt;
    }

        private static int ADDITIONAL_FIELDS = 5;

        public LsRequestStorage(Configuration.DatabaseParameters configuration)
                throws SQLException {
                super(configuration);
        }

        @Override
        public void dbInit1() throws SQLException {
        }

        @Override
        protected LsRequest getContainerRequest(Connection connection,
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
                                                       LsFileRequest[] fileRequests,
                                                       ResultSet set,
                                                       int next_index) throws SQLException {
                Job.JobHistory[] jobHistoryArray = getJobHistory(ID,connection);
                String explanation=set.getString(next_index++);
                boolean longFormat=set.getInt(next_index++)==1;
                int numOfLevels=set.getInt(next_index++);
                int count=set.getInt(next_index++);
                int offset=set.getInt(next_index++);
                return new  LsRequest(ID,
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
                                      explanation,
                                      longFormat,
                                      numOfLevels,
                                      count,
                                      offset);
        }

        @Override
        public String getRequestCreateTableFields() {
                return " "+
                        ",EXPLANATION "+stringType +
                        ",LONGFORMAT "+booleanType +
                        ",NUMOFLEVELS "+intType +
                        ",\"count\" "+intType +
                        ",LSOFFSET "+intType;
        }

        @Override
        public String getTableName() {
                return TABLE_NAME;
        }

        @Override
        public String getFileRequestsTableName() {
                return LsFileRequestStorage.TABLE_NAME;
        }

        @Override
        protected void __verify(int nextIndex,
                                int columnIndex,
                                String tableName,
                                String columnName,
                                int columnType) throws SQLException {
                if(columnIndex == nextIndex) {
                        verifyStringType("EXPLANATION",columnIndex,tableName, columnName, columnType);
                }
                else if(columnIndex == nextIndex+1) {
                        verifyBooleanType("LONGFORMAT",columnIndex,tableName, columnName, columnType);
                }
                else if(columnIndex == nextIndex+2) {
                        verifyIntType("NUMOFLEVELS",columnIndex,tableName, columnName, columnType);
                }
                else if(columnIndex == nextIndex+3) {
                        verifyIntType("count",columnIndex,tableName, columnName, columnType);
                }
                else if(columnIndex == nextIndex+4) {
                        verifyIntType("LSOFFSET",columnIndex,tableName, columnName, columnType);
                }
                else {
                        throw new SQLException("database table schema changed:"+
                                               "table named "+tableName+
                                               " column #"+columnIndex+" has name \""+columnName+
                                               "\"  has type \""+getTypeName(columnType)+
                                               " this column should not be present!!!");
                }
        }

        @Override
        protected int getMoreCollumnsNum() {
                return ADDITIONAL_FIELDS;
        }
}
