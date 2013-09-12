/*
 * FileRequestStorage.java
 *
 * Created on June 17, 2004, 3:18 PM
 */

package org.dcache.srm.request.sql;

import com.google.common.collect.ObjectArrays;
import com.google.common.reflect.TypeToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.dcache.srm.SRMInvalidRequestException;
import org.dcache.srm.SRMUser;
import org.dcache.srm.request.ContainerRequest;
import org.dcache.srm.request.FileRequest;
import org.dcache.srm.request.Job;
import org.dcache.srm.util.Configuration;

/**
 *
 * @author  timur
 */
public abstract class DatabaseContainerRequestStorage<C extends ContainerRequest<F>, F extends FileRequest<C>> extends DatabaseRequestStorage<C> {
   private final static Logger logger =
            LoggerFactory.getLogger(DatabaseContainerRequestStorage.class);

    @SuppressWarnings("unchecked")
    final Class<F> fileRequestType = (Class<F>) new TypeToken<F>(getClass()) {}.getRawType();

    /** Creates a new instance of DatabaseContainerRequestStorage */
    public DatabaseContainerRequestStorage(Configuration.DatabaseParameters configuration)
            throws DataAccessException
    {
        super(configuration);
    }

   public abstract String getFileRequestsTableName();
   /*{
        return getTableName()+"_filerequestids";
    }
    **/

    protected abstract C getContainerRequest(
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
    F[] fileRequests,
    ResultSet set,
    int next_index)throws SQLException;

    @Override
    protected C
    getRequest(
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
    ResultSet set,
    int next_index) throws SQLException {

        String sqlStatementString =
                "SELECT ID FROM " + getFileRequestsTableName() + " WHERE RequestID=" + ID;
        Statement sqlStatement = _con.createStatement();
        logger.debug("executing statement: {}", sqlStatementString);
        ResultSet fileIdsSet = sqlStatement.executeQuery(sqlStatementString);
        List<Long> fileIds = new ArrayList<>();
        while(fileIdsSet.next()) {
            fileIds.add(fileIdsSet.getLong(1));
        }
        fileIdsSet.close();
        sqlStatement.close();

        F[] fileRequests = ObjectArrays.newArray(fileRequestType, fileIds.size());
        for(int i = 0; i<fileRequests.length; ++i) {
            try {
                fileRequests[i] = Job.getJob(fileIds.get(i), fileRequestType, _con);
            } catch (SRMInvalidRequestException ire){
                logger.error(ire.toString());
            }
        }
        return getContainerRequest(
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
        fileRequests,
        set,
        next_index );
    }

    @Override
    public abstract String getTableName();
}

