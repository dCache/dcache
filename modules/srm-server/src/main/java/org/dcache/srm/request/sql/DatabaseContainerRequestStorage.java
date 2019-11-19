package org.dcache.srm.request.sql;

import com.google.common.collect.ImmutableList;
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
import java.util.concurrent.ScheduledExecutorService;

import org.dcache.srm.SRMUser;
import org.dcache.srm.SRMUserPersistenceManager;
import org.dcache.srm.request.ContainerRequest;
import org.dcache.srm.request.FileRequest;
import org.dcache.srm.request.Job;
import org.dcache.srm.scheduler.JobStorageFactory;
import org.dcache.srm.util.Configuration;

public abstract class DatabaseContainerRequestStorage<C extends ContainerRequest<F>, F extends FileRequest<C>>
        extends DatabaseRequestStorage<C>
{
   private static final Logger LOGGER =
            LoggerFactory.getLogger(DatabaseContainerRequestStorage.class);

    @SuppressWarnings("unchecked")
    final Class<F> fileRequestType = (Class<F>) new TypeToken<F>(getClass()) {}.getRawType();

    /** Creates a new instance of DatabaseContainerRequestStorage */
    public DatabaseContainerRequestStorage(String srmId,
            Configuration.DatabaseParameters configuration,
            ScheduledExecutorService executor, SRMUserPersistenceManager manager)
            throws DataAccessException
    {
        super(srmId, configuration, executor, manager);
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
    SRMUser user,
    String SCHEDULERID,
    long SCHEDULER_TIMESTAMP,
    int NUMOFRETR,
    long LASTSTATETRANSITIONTIME,
    Long CREDENTIALID,
    int RETRYDELTATIME,
    boolean SHOULDUPDATERETRYDELTATIME,
    String DESCRIPTION,
    String CLIENTHOST,
    String STATUSCODE,
    ImmutableList<F> fileRequests,
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
    SRMUser user,
    String SCHEDULERID,
    long SCHEDULER_TIMESTAMP,
    int NUMOFRETR,
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
        LOGGER.debug("executing statement: {}", sqlStatementString);
        ResultSet fileIdsSet = sqlStatement.executeQuery(sqlStatementString);
        List<Long> fileIds = new ArrayList<>();
        while(fileIdsSet.next()) {
            fileIds.add(fileIdsSet.getLong(1));
        }
        fileIdsSet.close();
        sqlStatement.close();

        ImmutableList.Builder<F> fileRequests = ImmutableList.builder();
        for (Long fileId : fileIds) {
            F job = getFileRequest(fileId, _con);
            if (job != null) {
                fileRequests.add(job);
            }
        }

        return getContainerRequest(
        _con,
        ID,
        NEXTJOBID ,
        CREATIONTIME,
        LIFETIME,
        STATE,
        user,
        SCHEDULERID,
        SCHEDULER_TIMESTAMP,
        NUMOFRETR,
        LASTSTATETRANSITIONTIME,
        CREDENTIALID,
        RETRYDELTATIME,
        SHOULDUPDATERETRYDELTATIME,
        DESCRIPTION,
        CLIENTHOST,
        STATUSCODE,
        fileRequests.build(),
        set,
        next_index );
    }

    private F getFileRequest(long jobId, Connection _con)
    {
        try {
            Job job = JobStorageFactory.getJobStorageFactory().getJobStorage(fileRequestType).getJob(jobId, _con);
            if (job != null) {
                return fileRequestType.cast(job);
            }
            LOGGER.error("Job {} not found in database.", jobId);
        } catch (DataAccessException | SQLException e) {
            LOGGER.error("Failed to read job {}: {}", jobId, e.toString());
        }
        return null;
    }

    @Override
    public abstract String getTableName();
}
