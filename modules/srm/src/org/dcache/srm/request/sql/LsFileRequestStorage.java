package org.dcache.srm.request.sql;
import java.sql.*;
import org.dcache.srm.request.FileRequest;
import org.dcache.srm.request.LsFileRequest;
import org.dcache.srm.util.Configuration;
import org.dcache.srm.scheduler.State;
import org.dcache.srm.scheduler.Job;

public class LsFileRequestStorage extends DatabaseFileRequestStorage {
        public static final String TABLE_NAME = "lsfilerequests";
        private static int ADDITIONAL_FIELDS = 0;
        public LsFileRequestStorage(Configuration configuration)
                throws SQLException {
                super(configuration);
        }

        public void say(String s){
                if(logger != null) {
                        logger.log(" LsFileRequestStorage: "+s);
                }
        }

        public void esay(String s){
                if(logger != null) {
                        logger.elog(" LsFileRequestStorage: "+s);
                }
        }

        public void esay(Throwable t){
                if(logger != null) {
                        logger.elog(t);
                }
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
                                         this,
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
                                         configuration,
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

        public void getUpdateAssignements(FileRequest fr,StringBuffer sb) {
                if(fr == null || !(fr instanceof LsFileRequest)) {
                        throw new IllegalArgumentException("fr is not LsFileRequest" );
                }
                LsFileRequest lsfr = (LsFileRequest)fr;
                sb.append(", SURL = '").append(lsfr.getSurlString()).append("' ");
        }

        public void getCreateList(FileRequest fr,StringBuffer sb) {
                if(fr == null || !(fr instanceof LsFileRequest)) {
                        throw new IllegalArgumentException("fr is not LsFileRequest" );
                }
                LsFileRequest lsfr = (LsFileRequest)fr;
                sb.append(", '").append(lsfr.getSurlString()).append("' ");
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
