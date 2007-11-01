// $Id: CopyRequestStorage.java,v 1.6.2.1 2007-01-04 02:58:55 timur Exp $
// $Log: not supported by cvs2svn $
// Revision 1.6  2006/04/26 17:17:56  timur
// store the history of the state transitions in the database
//
// Revision 1.5  2006/04/12 23:16:23  timur
// storing state transition time in database, storing transferId for copy requests in database, renaming tables if schema changes without asking
//
// Revision 1.4  2005/03/30 22:42:10  timur
// more database schema changes
//
// Revision 1.3  2005/03/09 23:21:17  timur
// more database checks, more space reservation code
//
// Revision 1.2  2005/03/01 23:10:39  timur
// Modified the database scema to increase database operations performance and to account for reserved space"and to account for reserved space
//
/*
 * GetRequestStorage.java
 *
 * Created on June 22, 2004, 2:48 PM
 */

package org.dcache.srm.request.sql;
import org.dcache.srm.request.Request;
import org.dcache.srm.request.FileRequest;
import org.dcache.srm.request.CopyRequest;
import org.dcache.srm.util.Configuration;
import java.sql.*;
import org.dcache.srm.scheduler.State;
import org.dcache.srm.scheduler.Job;

/**
 *
 * @author  timur
 */
public class CopyRequestStorage extends DatabaseRequestStorage{
    
     
    /** Creates a new instance of GetRequestStorage */
     public CopyRequestStorage(Configuration configuration) throws SQLException {
        super(configuration
        );
    }
        
    public void say(String s){
        if(logger != null) {
           logger.log(" CopyRequestStorage: "+s);
        }
    }
    
    public void esay(String s){
        if(logger != null) {
           logger.elog(" CopyRequestStorage: "+s);
        }
    }
    
    public void esay(Throwable t){
        if(logger != null) {
           logger.elog(t);
        }
    }
    


    public void dbInit1() throws SQLException {
   }
    
    public void getCreateList(Request r, StringBuffer sb) {
        
    }
    
    protected Request getRequest(Connection _con, Long ID, Long NEXTJOBID, long CREATIONTIME, long LIFETIME, int STATE, String ERRORMESSAGE, String CREATORID, String SCHEDULERID, long SCHEDULER_TIMESTAMP, int NUMOFRETR, int MAXNUMOFRETR,long LASTSTATETRANSITIONTIME, Long CREDENTIALID, int RETRYDELTATIME, boolean SHOULDUPDATERETRYDELTATIME, FileRequest[] fileRequests, java.sql.ResultSet set, int next_index) throws java.sql.SQLException {
           
        Job.JobHistory[] jobHistoryArray = 
        getJobHistory(ID,_con);
            return new  CopyRequest( 
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
                        configuration
                        );

    }
    
    public String getRequestCreateTableFields() {
        return "";
    }
    
    private static int ADDITIONAL_FIELDS = 0;
    
    public static final String TABLE_NAME="copyrequests";
    public String getTableName() {
        return TABLE_NAME;
    }
    
    public void getUpdateAssignements(Request r, StringBuffer sb) {
    }
    
    public String[] getAdditionalCreateRequestStatements(Request r)  {
        return null;
   }    
   
    public String getFileRequestsTableName() {
        return CopyFileRequestStorage.TABLE_NAME;
    }    
     
    protected void __verify(int nextIndex, int columnIndex, String tableName, String columnName, int columnType) throws SQLException {
    }
    
   
    protected int getMoreCollumnsNum() {
         return ADDITIONAL_FIELDS;
     }

}
