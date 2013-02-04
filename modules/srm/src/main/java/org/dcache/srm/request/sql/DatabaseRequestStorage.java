/*
 * DatabaseRequestStorage.java
 *
 * Created on February 16, 2007, 1:24 PM
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package org.dcache.srm.request.sql;

import org.dcache.srm.request.Request;
import org.dcache.srm.request.RequestStorage;
import org.dcache.srm.request.Job;
import org.dcache.srm.scheduler.State;
import org.dcache.srm.util.Configuration;
import java.sql.*;
import java.util.Set;
import org.dcache.srm.SRMUser;
import org.dcache.srm.SRMUserPersistenceManager;


/**
 *
 * @author timur
 */
public abstract class DatabaseRequestStorage extends DatabaseJobStorage implements RequestStorage{
    SRMUserPersistenceManager srmUserPersistenceManager;
    /** Creates a new instance of DatabaseRequestStorage */
    public DatabaseRequestStorage(Configuration.DatabaseParameters configuration) throws SQLException {
        super(configuration);
        srmUserPersistenceManager = configuration.getSrmUserPersistenceManager();
        if(srmUserPersistenceManager == null) {
            throw new IllegalArgumentException("srmUserPersistenceManager == null");
        }
    }

    public abstract String getRequestCreateTableFields();

    @Override
    public String getCreateTableFields() {
        return
                ", "+
                "CREDENTIALID "+  longType+
                ", "+
                "RETRYDELTATIME "+  intType+
                ", "+
                "SHOULDUPDATERETRYDELTATIME "+  booleanType+
                ", "+
                "DESCRIPTION "+ stringType+
                ", "+
                "CLIENTHOST "+ stringType+
                ", "+
                "STATUSCODE "+ stringType+
                ", "+
                "USERID "+ longType+

                getRequestCreateTableFields();
    }

    protected abstract Request getRequest(
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
            ResultSet set,
            int next_index)throws SQLException;

    @Override
    protected final Job
    getJob(
            Connection _con,
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
            ResultSet set,
            int next_index) throws SQLException {

        Long CREDENTIALID = set.getLong(next_index++);
        int RETRYDELTATIME = set.getInt(next_index++);
        boolean SHOULDUPDATERETRYDELTATIME = set.getBoolean(next_index++);
        String DESCRIPTION = set.getString(next_index++);
        String CLIENTHOST = set.getString(next_index++);
        String STATUSCODE= set.getString(next_index++);
        SRMUser user =
                srmUserPersistenceManager.find(set.getLong(next_index++));
        return getRequest(
                _con,
                ID,
                NEXTJOBID ,
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
                CREDENTIALID,
                RETRYDELTATIME,
                SHOULDUPDATERETRYDELTATIME,
                DESCRIPTION,
                CLIENTHOST,
                STATUSCODE,
                set,
                next_index );
    }
    private static int ADDITIONAL_FIELDS_NUM=7;

    public abstract  void getCreateList(Request r,StringBuffer sb);

    public final void getCreateList(Job job, StringBuffer sb) {

        if(job == null || !(job instanceof Request)) {
            throw new IllegalArgumentException("job is not Request" );
        }
        Request r = (Request)job;

        sb.append(", ").append(r.getCredentialId()).append(" ");
        sb.append(", ").append(r.getRetryDeltaTime());
        sb.append(", ").append(r.isShould_updateretryDeltaTime() ? 1 : 0);
        String DESCRIPTION = r.getDescription();
        if(DESCRIPTION == null) {
            sb.append( ", NULL ");
        }
        else {
            sb.append(", '").append(DESCRIPTION).append('\'');
        }
        String CLIENTHOST = r.getClient_host();
        if(CLIENTHOST == null) {
            sb.append( ", NULL ");
        }
        else {
            sb.append(", '").append(CLIENTHOST).append('\'');
        }
        String STATUSCODE = r.getStatusCodeString();
        if(STATUSCODE == null) {
            sb.append( ", NULL ");
        }
        else {
            sb.append(", '").append(STATUSCODE).append('\'');
        }
        sb.append(", ").append(
                r.getUser().getId()
                ).append(" ");
        getCreateList(r,sb);

    }

    public Set<Long> getActiveRequestIds(String schedulerid)  throws SQLException {
        String condition = " SCHEDULERID='"+schedulerid+
                "' AND STATE !="+State.DONE.getStateId()+
                " AND STATE !="+State.CANCELED.getStateId()+
                " AND STATE !="+State.FAILED.getStateId();
        return getJobIdsByCondition(condition);
    }

    public Set<Long> getActiveRequestIds(String schedulerid,
            SRMUser user,
            String description )  throws SQLException {
        String condition = " SCHEDULERID='"+schedulerid+
                "' AND STATE !="+State.DONE.getStateId()+
                " AND STATE !="+State.CANCELED.getStateId()+
                " AND STATE !="+State.FAILED.getStateId()+
                " AND USERID = '"+user.getId()+'\'';
        if(description != null) {
            condition += " AND DESCRIPTION = '"+
                    description+'\'';
        }
        return getJobIdsByCondition(condition);
    }

    public Set<Long> getLatestCompletedRequestIds(int maxNum)  throws SQLException {
        return getJobIdsByCondition(
                " STATE ="+State.DONE.getStateId()+
                " OR STATE ="+State.CANCELED.getStateId()+
                " OR STATE = "+State.FAILED.getStateId()+
                " ORDER BY ID"+
                " LIMIT "+maxNum+" ");
    }

    public Set<Long> getLatestDoneRequestIds(int maxNum)  throws SQLException {
        return getJobIdsByCondition("STATE ="+State.DONE.getStateId()+
                " ORDERED BY ID"+
                " LIMIT "+maxNum+" ");
    }

    public Set<Long> getLatestFailedRequestIds(int maxNum)  throws SQLException {
        return getJobIdsByCondition("STATE !="+State.FAILED.getStateId()+
                " ORDERED BY ID"+
                " LIMIT "+maxNum+" ");
    }

    public Set<Long> getLatestCanceledRequestIds(int maxNum)  throws SQLException {
        return getJobIdsByCondition("STATE != "+State.CANCELED.getStateId()+
                " ORDERED BY ID"+
                " LIMIT "+maxNum+" ");
    }

    protected abstract void __verify(int nextIndex, int columnIndex, String tableName, String columnName, int columnType) throws SQLException ;

    @Override
    protected final void _verify(int nextIndex, int columnIndex, String tableName, String columnName, int columnType) throws SQLException {
        /*
         *additional fields:
                 ","+
        "CREDENTIALID "+  stringType+
        ","+
        "RETRYDELTATIME "+  intType+
        ","+
        "SHOULDUPDATERETRYDELTATIME "+  booleanType+
        ","+
        "DESCRIPTION "+  stringType+
        ","+
        "CLIENTHOST "+ stringType+
        ", "+
        "STATUSCODE "+ stringType+
         */
        if(columnIndex == nextIndex) {
            verifyLongType("CREDENTIALID",columnIndex,tableName, columnName, columnType);
        }
        else if(columnIndex == nextIndex+1)
        {
            verifyIntType("RETRYDELTATIME",columnIndex,tableName, columnName, columnType);

        }
        else if(columnIndex == nextIndex+2)
        {
            verifyBooleanType("SHOULDUPDATERETRYDELTATIME",columnIndex,tableName, columnName, columnType);
        }
        else if(columnIndex == nextIndex+3)
        {
            verifyStringType("DESCRIPTION",columnIndex,tableName, columnName, columnType);
        }
        else if(columnIndex == nextIndex+4)
        {
            verifyStringType("CLIENTHOST",columnIndex,tableName, columnName, columnType);
        }
        else if(columnIndex == nextIndex+5)
        {
            verifyStringType("STATUSCODE",columnIndex,tableName, columnName, columnType);
        }
        else if(columnIndex == nextIndex+6)
        {
            verifyLongType("USERID",columnIndex,tableName, columnName, columnType);
        }
        else
        {
            __verify(nextIndex+7,columnIndex,tableName, columnName, columnType);
        }
    }

    protected abstract int getMoreCollumnsNum();

    @Override
    protected final int getAdditionalColumnsNum() {
        return ADDITIONAL_FIELDS_NUM +getMoreCollumnsNum();
    }

}
