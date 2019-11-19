/*
 * DatabaseRequestStorage.java
 *
 * Created on February 16, 2007, 1:24 PM
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package org.dcache.srm.request.sql;

import org.springframework.dao.DataAccessException;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.ScheduledExecutorService;

import org.dcache.srm.SRMUser;
import org.dcache.srm.SRMUserPersistenceManager;
import org.dcache.srm.request.Request;
import org.dcache.srm.util.Configuration;

import static com.google.common.base.Preconditions.checkNotNull;


/**
 *
 * @author timur
 */
public abstract class DatabaseRequestStorage<R extends Request> extends DatabaseJobStorage<R> {
    final SRMUserPersistenceManager srmUserPersistenceManager;
    protected final String srmId;

    /** Creates a new instance of DatabaseRequestStorage */
    public DatabaseRequestStorage(String srmId, Configuration.DatabaseParameters configuration,
            ScheduledExecutorService executor, SRMUserPersistenceManager manager)
            throws DataAccessException
    {
        super(configuration, executor);
        srmUserPersistenceManager = checkNotNull(manager);
        this.srmId = checkNotNull(srmId);
    }

    protected abstract R getRequest(
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
            int next_index)throws SQLException;

    @Override
    protected final R
    getJob(
            Connection _con,
            long ID,
            Long NEXTJOBID,
            long CREATIONTIME,
            long LIFETIME,
            int STATE,
            String SCHEDULERID,
            long SCHEDULER_TIMESTAMP,
            int NUMOFRETR,
            long LASTSTATETRANSITIONTIME,
            ResultSet set,
            int next_index) throws SQLException {

        Long CREDENTIALID = set.getLong(next_index++);
        int RETRYDELTATIME = set.getInt(next_index++);
        boolean SHOULDUPDATERETRYDELTATIME = set.getBoolean(next_index++);
        String DESCRIPTION = set.getString(next_index++);
        String CLIENTHOST = set.getString(next_index++);
        String STATUSCODE= set.getString(next_index++);
        long id = set.getLong(next_index++);
        SRMUser user = set.wasNull()
                       ? srmUserPersistenceManager.createAnonymous()
                       // FIXME: Using client host for this is not correct as the client may specify a different host
                       : srmUserPersistenceManager.find(CLIENTHOST, id);
        return getRequest(
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
                set,
                next_index );
    }
}
