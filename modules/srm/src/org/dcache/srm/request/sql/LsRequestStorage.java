package org.dcache.srm.request.sql;
import org.dcache.srm.request.ContainerRequest;
import org.dcache.srm.request.Request;
import org.dcache.srm.request.FileRequest;
import org.dcache.srm.request.LsRequest;
import org.dcache.srm.util.Configuration;
import java.sql.*;
import org.dcache.srm.scheduler.State;
import org.dcache.srm.scheduler.Job;
import org.dcache.srm.SRMUser;

public class LsRequestStorage extends DatabaseContainerRequestStorage{
        public static final String TABLE_NAME ="lsrequests";
        private static int ADDITIONAL_FIELDS = 5;

        public LsRequestStorage(Configuration configuration)
                throws SQLException {
                super(configuration);
        }

        public void say(String s){
                if(logger != null) {
                        logger.log(" LsRequestStorage: "+s);
                }
        }

        public void esay(String s){
                if(logger != null) {
                        logger.elog(" LsRequestStorage: "+s);
                }
        }

        public void esay(Throwable t){
                if(logger != null) {
                        logger.elog(t);
                }
        }

        public void dbInit1() throws SQLException {
        }

        public void getCreateList(ContainerRequest r, StringBuffer sb) {
                if (r==null || !(r instanceof LsRequest)){ 
                        throw new IllegalArgumentException("r is not LsRequest" );
                }
                LsRequest lsRequest = (LsRequest)r;
                if(lsRequest.getExplanation()!=null) {
                        sb.append(",'").append(lsRequest.getExplanation()).append("'");
                }
                else { 
                        sb.append(",NULL");
                }
                sb.append(",").append(lsRequest.getLongFormat()==true?1:0);
                sb.append(",").append(lsRequest.getNumOfLevels());
                sb.append(",").append(lsRequest.getCount());
                sb.append(",").append(lsRequest.getOffset());
        }
        
        protected ContainerRequest getContainerRequest(Connection connection,
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
                                                       int next_index) throws java.sql.SQLException {
                Job.JobHistory[] jobHistoryArray = getJobHistory(ID,connection);
                String explanation=set.getString(next_index++);
                boolean longFormat=set.getInt(next_index++)==1;
                int numOfLevels=set.getInt(next_index++);
                int count=set.getInt(next_index++);
                int offset=set.getInt(next_index++);
                return new  LsRequest(ID,
                                      NEXTJOBID,
                                      this,
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
                                      configuration,
                                      explanation,
                                      longFormat,
                                      numOfLevels,
                                      count,
                                      offset);
        }

        public String getRequestCreateTableFields() {
                return " "+
                        ",EXPLANATION "+stringType +
                        ",LONGFORMAT "+booleanType +
                        ",NUMOFLEVELS "+intType + 
                        ",COUNT "+intType +
                        ",LSOFFSET "+intType;
        }

        public String getTableName() {
                return TABLE_NAME;
        }
        
        public void getUpdateAssignements(ContainerRequest r, StringBuffer sb) {
                if (r==null || !(r instanceof LsRequest)){ 
                        throw new IllegalArgumentException("r is not LsRequest" );
                }
                LsRequest lsRequest = (LsRequest)r;
                if(lsRequest.getExplanation()!=null) {
                        sb.append(",EXPLANATION='").append(lsRequest.getExplanation()).append("'");
                }
                else { 
                        sb.append(",EXPLANATION=NULL");
                }
                sb.append(",LONGFORMAT=").append(lsRequest.getLongFormat()==true?1:0);
                sb.append(",NUMOFLEVELS=").append(lsRequest.getNumOfLevels());
                sb.append(",COUNT=").append(lsRequest.getCount());
                sb.append(",LSOFFSET=").append(lsRequest.getOffset());
        }
        
        public String[] getAdditionalCreateRequestStatements(ContainerRequest r)  {
                return null;
        }
        
        public String getFileRequestsTableName() {
                return LsFileRequestStorage.TABLE_NAME;
        }
        
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
                        verifyIntType("COUNT",columnIndex,tableName, columnName, columnType);
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
        
        protected int getMoreCollumnsNum() {
                return ADDITIONAL_FIELDS;
        }
}
