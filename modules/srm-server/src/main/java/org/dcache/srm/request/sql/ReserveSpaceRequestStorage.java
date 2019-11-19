/*
 * FileRequestStorage.java
 *
 * Created on June 17, 2004, 3:18 PM
 */

package org.dcache.srm.request.sql;

import com.google.common.base.Splitter;
import com.google.common.escape.Escaper;
import com.google.common.net.PercentEscaper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;

import javax.annotation.Nonnull;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

import org.dcache.srm.SRMUser;
import org.dcache.srm.SRMUserPersistenceManager;
import org.dcache.srm.request.Job;
import org.dcache.srm.request.ReserveSpaceRequest;
import org.dcache.srm.util.Configuration;

/**
 *
 * @author  timur
 */
public class ReserveSpaceRequestStorage extends DatabaseRequestStorage<ReserveSpaceRequest> {
    private static final Logger LOGGER = LoggerFactory.getLogger(ReserveSpaceRequestStorage.class);

    public static final String TABLE_NAME ="reservespacerequests";
    private static final String UPDATE_PREFIX = "UPDATE " + TABLE_NAME + " SET "+
        "NEXTJOBID=?, " +
        "CREATIONTIME=?,  " +
        "LIFETIME=?, " +
        "STATE=?, " +
        "ERRORMESSAGE=?, " +//5
        "SCHEDULERID=?, " +
        "SCHEDULERTIMESTAMP=?," +
        "NUMOFRETR=?," +
        "LASTSTATETRANSITIONTIME=? ";

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
    "LASTSTATETRANSITIONTIME,"+ // 10
     //Database Request Storage
    "RETRYDELTATIME , "+
    "SHOULDUPDATERETRYDELTATIME ,"+
    "DESCRIPTION ,"+
    "CLIENTHOST ,"+ // 15
    "STATUSCODE ,"+
    "USERID ,"+
    // Reserve Space Request
    "SIZEINBYTES, "+
    "RESERVATIONLIFETIME, "+
    "SPACETOKEN, "+
    "RETENTIONPOLICY, "+
    "ACCESSLATENCY, " +
    "EXTRAINFO ) "+
    "VALUES (?,?,?,?,?,?,?,?,?,?,?," +//Job
                "?,?,?,?,?,?,?," +//Request
                "?,?,?,?)";

    private final Escaper extraInfoEscaper = new PercentEscaper("./\\;:'#~@[]{}-_+ ", false);


    @Override
    public PreparedStatement getCreateStatement(Connection connection, Job job) throws SQLException {
        ReserveSpaceRequest rsr = (ReserveSpaceRequest)job;
        String retentionPolicyValue=null;
        if(rsr.getRetentionPolicy() != null) {
            retentionPolicyValue = rsr.getRetentionPolicy().getValue();
        }
        String accessLatencyValue=null;
        if(rsr.getAccessLatency() != null) {
            accessLatencyValue = rsr.getAccessLatency().getValue();
        }
        return getPreparedStatement(connection,
                                    INSERT_SQL,
                                    rsr.getId(),
                                    rsr.getNextJobId(),
                                    rsr.getCreationTime(),
                                    rsr.getLifetime(),
                                    rsr.getState().getStateId(),//5
                                    rsr.latestHistoryEvent(),
                                    rsr.getSchedulerId(),
                                    rsr.getSchedulerTimeStamp(),
                                    0, // num of retries
                                    rsr.getLastStateTransitionTime(), // 10
                                    //Database Request Storage
                                    rsr.getRetryDeltaTime(),
                                    rsr.isShould_updateretryDeltaTime()?0:1,
                                    rsr.getDescription(),
                                    rsr.getClient_host(),
                                    rsr.getStatusCodeString(),
                                    rsr.getUser().getId(),
                                    rsr.getSizeInBytes(),
                                    rsr.getSpaceReservationLifetime(),
                                    rsr.getSpaceToken(),
                                    retentionPolicyValue,
                                    accessLatencyValue,
                                    encodeExtraInfo(rsr.getExtraInfo()));
    }

    private static final String UPDATE_REQUEST_SQL =
            UPDATE_PREFIX + ", RETRYDELTATIME=?," +
                " SHOULDUPDATERETRYDELTATIME=?," +
                " DESCRIPTION=?," +
                " CLIENTHOST=?," +
                " STATUSCODE=?," +
                " USERID=?," +
                // Reserve Space Request
                " SIZEINBYTES=?, "+
                " RESERVATIONLIFETIME=?, "+
                " SPACETOKEN=?, "+
                " RETENTIONPOLICY=?,"+
                " ACCESSLATENCY=?," +
                " EXTRAINFO=?" +
                " WHERE ID=?";
    @Override
    public PreparedStatement getUpdateStatement(Connection connection,
            Job job) throws SQLException {
        ReserveSpaceRequest rsr = (ReserveSpaceRequest)job;
        String retentionPolicyValue=null;
        if(rsr.getRetentionPolicy() != null) {
            retentionPolicyValue = rsr.getRetentionPolicy().getValue();
        }
        String accessLatencyValue=null;
        if(rsr.getAccessLatency() != null) {
            accessLatencyValue = rsr.getAccessLatency().getValue();
        }
        return getPreparedStatement(connection,
                                    UPDATE_REQUEST_SQL,
                                    rsr.getNextJobId(),
                                    rsr.getCreationTime(),
                                    rsr.getLifetime(),
                                    rsr.getState().getStateId(),
                                    rsr.latestHistoryEvent(),//5
                                    rsr.getSchedulerId(),
                                    rsr.getSchedulerTimeStamp(),
                                    0, // num of retries
                                    rsr.getLastStateTransitionTime(),
                                    //Database Request Storage
                                    rsr.getRetryDeltaTime(), // 10
                                    rsr.isShould_updateretryDeltaTime()?0:1,
                                    rsr.getDescription(),
                                    rsr.getClient_host(),
                                    rsr.getStatusCodeString(),
                                    rsr.getUser().getId(),
                                    rsr.getSizeInBytes(),
                                    rsr.getSpaceReservationLifetime(),
                                    rsr.getSpaceToken(),
                                    retentionPolicyValue,
                                    accessLatencyValue,
                                    encodeExtraInfo(rsr.getExtraInfo()),
                                    rsr.getId());
    }

    private String encodeExtraInfo(Map<String,String> extraInfo)
    {
        if (extraInfo == null || extraInfo.isEmpty()) {
            return null;
        }

        return extraInfo.entrySet().stream()
                .map(e -> extraInfoEscaper.escape(e.getKey()) + '=' + extraInfoEscaper.escape(e.getValue()))
                .collect(Collectors.joining(","));
    }

    private Map<String,String> decodeExtraInfo(String value)
    {
        if (value == null || value.isEmpty()) {
            return Collections.emptyMap();
        }

        Map<String,String> items = new HashMap<>();
        for (String item : Splitter.on(',').split(value)) {
            List<String> elements = Splitter.on('=').limit(2).splitToList(item);
            if (elements.size() == 2) {
                try {
                    items.put(URLDecoder.decode(elements.get(0), "UTF-8"),
                            URLDecoder.decode(elements.get(1), "UTF-8"));
                } catch (UnsupportedEncodingException e) {
                    LOGGER.error("JVM does not support UTF-8", e);
                }
            } else {
                LOGGER.error("Skipping malformed extraInfo item: {}", item);
            }
        }
        return items;
    }

    /** Creates a new instance of FileRequestStorage */
    public ReserveSpaceRequestStorage(@Nonnull String srmId,
            Configuration.DatabaseParameters configuration,
            ScheduledExecutorService executor, SRMUserPersistenceManager manager)
            throws DataAccessException
    {
        super(srmId, configuration, executor, manager);
    }


    @Override
    protected ReserveSpaceRequest getRequest(
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

            Job.JobHistory[] jobHistoryArray =
            getJobHistory(ID,_con);

        long SIZEINBYTES = set.getLong(next_index++);
        long RESERVATIONLIFETIME = set.getLong(next_index++);
        String SPACETOKEN = set.getString(next_index++);
        String RETENTIONPOLICY = set.getString(next_index++);
        String ACCESSLATENCY = set.getString(next_index++);
        Map<String,String> extraInfo = decodeExtraInfo(set.getString(next_index++));
        return new ReserveSpaceRequest(
                srmId,
                ID,
                NEXTJOBID,
                CREATIONTIME,
                LIFETIME,
                STATE,
                user,
                SCHEDULERID,
                SCHEDULER_TIMESTAMP,
                NUMOFRETR,
                LASTSTATETRANSITIONTIME,
                jobHistoryArray,
                RETRYDELTATIME,
                SIZEINBYTES,
                RESERVATIONLIFETIME,
                SPACETOKEN,
                RETENTIONPOLICY,
                ACCESSLATENCY,
                DESCRIPTION,
                CLIENTHOST,
                extraInfo,
                STATUSCODE);
    }

    @Override
    public String getTableName() {
        return TABLE_NAME;
    }
}
