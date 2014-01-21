package diskCacheV111.services.space;

import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.dao.IncorrectResultSizeDataAccessException;
import org.springframework.jdbc.JdbcUpdateAffectedIncorrectNumberOfRowsException;
import org.springframework.jdbc.core.PreparedStatementCreator;
import org.springframework.jdbc.core.RowCallbackHandler;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcDaoSupport;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import diskCacheV111.util.AccessLatency;
import diskCacheV111.util.FsPath;
import diskCacheV111.util.PnfsId;
import diskCacheV111.util.RetentionPolicy;
import diskCacheV111.util.VOInfo;

import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.collect.Iterables.getFirst;
import static java.util.Arrays.asList;

@Repository
public class JdbcSpaceManagerDatabase extends NamedParameterJdbcDaoSupport implements SpaceManagerDatabase
{
    private static final Logger LOGGER = LoggerFactory.getLogger(JdbcSpaceManagerDatabase.class);

    private static final String RETENTION_POLICY_TABLE = "srmretentionpolicy";
    private static final String ACCESS_LATENCY_TABLE = "srmaccesslatency";

    /*
                     Table "public.srmlinkgroup"
            Column        |           Type           | Modifiers
    ----------------------+--------------------------+-----------
     id                   | bigint                   | not null
     name                 | character varying(32672) |
     freespaceinbytes     | bigint                   | not null
     lastupdatetime       | bigint                   |
     onlineallowed        | integer                  |
     nearlineallowed      | integer                  |
     replicaallowed       | integer                  |
     outputallowed        | integer                  |
     custodialallowed     | integer                  |
     reservedspaceinbytes | bigint                   | not null
    */
    private static final String LINKGROUP_TABLE = "srmlinkgroup";

    /*
               Table "public.srmlinkgroupvos"
       Column    |           Type           | Modifiers
    -------------+--------------------------+-----------
     vogroup     | character varying(32672) | not null
     vorole      | character varying(32672) | not null
     linkgroupid | bigint                   | not null
    */

    private static final String LINKGROUP_VO_TABLE = "srmlinkgroupvos";

    /*
           Column           |           Type           | Modifiers
    ------------------------+--------------------------+-----------
      id                    | bigint                   | not null
      vogroup               | character varying(32672) |
      vorole                | character varying(32672) |
      retentionpolicy       | integer                  |
      accesslatency         | integer                  |
      linkgroupid           | bigint                   | not null
      sizeinbytes           | bigint                   | not null
      creationtime          | bigint                   |
      lifetime              | bigint                   |
      description           | character varying(32672) |
      state                 | integer                  | not null
      usedspaceinbytes      | bigint                   | not null
      allocatedspaceinbytes | bigint                   | not null
    */
    private static final String SPACE_TABLE = "srmspace";

    /*
                Table "public.srmspacefile"
           Column       |           Type           | Modifiers
    --------------------+--------------------------+-----------
     id                 | bigint                   | not null
     vogroup            | character varying(32672) |
     vorole             | character varying(32672) |
     spacereservationid | bigint                   | not null
     sizeinbytes        | bigint                   | not null
     creationtime       | bigint                   |
     lifetime           | bigint                   |
     pnfspath           | character varying(32672) |
     pnfsid             | character varying(32672) | unique
     state              | integer                  | not null
     deleted            | integer                  |
    */
    private static final String SPACEFILE_TABLE = "srmspacefile";


    private final RowMapper<Space> spaceReservationMapper = new RowMapper<Space>()
    {
        @Override
        public Space mapRow(ResultSet set, int rowNum) throws SQLException
        {
            return new Space(set.getLong("id"),
                             set.getString("vogroup"),
                             set.getString("vorole"),
                             RetentionPolicy.getRetentionPolicy(set.getInt("retentionPolicy")),
                             AccessLatency.getAccessLatency(set.getInt("accessLatency")),
                             set.getLong("linkgroupid"),
                             set.getLong("sizeinbytes"),
                             set.getLong("creationtime"),
                             set.getLong("lifetime"),
                             set.getString("description"),
                             SpaceState.getState(set.getInt("state")),
                             set.getLong("usedspaceinbytes"),
                             set.getLong("allocatedspaceinbytes"));
        }
    };

    private final RowMapper<LinkGroup> linkGroupMapper = new RowMapper<LinkGroup>()
    {
        @Override
        public LinkGroup mapRow(ResultSet set, int rowNum) throws SQLException
        {
            LinkGroup lg = new LinkGroup();
            lg.setId(set.getLong("id"));
            lg.setName(set.getString("name"));
            lg.setFreeSpace(set.getLong("freeSpaceInBytes"));
            lg.setUpdateTime(set.getLong("lastUpdateTime"));
            lg.setOnlineAllowed(set.getBoolean("onlineAllowed"));
            lg.setNearlineAllowed(set.getBoolean("nearlineAllowed"));
            lg.setReplicaAllowed(set.getBoolean("replicaAllowed"));
            lg.setOutputAllowed(set.getBoolean("outputAllowed"));
            lg.setCustodialAllowed(set.getBoolean("custodialAllowed"));
            lg.setReservedSpaceInBytes(set.getLong("reservedspaceinbytes"));
            List<VOInfo> vos = getJdbcTemplate().query(
                    "SELECT voGroup,voRole FROM " + LINKGROUP_VO_TABLE + " WHERE linkGroupId=?", voInfoMapper,
                    lg.getId());
            lg.setVOs(vos.toArray(new VOInfo[vos.size()]));
            return lg;
        }
    };

    private final RowMapper<File> fileMapper = new RowMapper<File>()
    {
        @Override
        public File mapRow(ResultSet set, int rowNum) throws SQLException
        {
            String pnfsId = set.getString("pnfsId");
            return new File(set.getLong("id"),
                            set.getString("vogroup"),
                            set.getString("vorole"),
                            set.getLong("spacereservationid"),
                            set.getLong("sizeinbytes"),
                            set.getLong("creationtime"),
                            set.getLong("lifetime"),
                            set.getString("pnfspath"),
                            (pnfsId != null) ? new PnfsId(pnfsId) : null,
                            FileState.getState(set.getInt("state")),
                            (set.getObject("deleted") != null) ? set.getInt("deleted") : 0);
        }
    };

    private final RowMapper<VOInfo> voInfoMapper = new RowMapper<VOInfo>()
    {
        @Override
        public VOInfo mapRow(ResultSet rs, int rowNum) throws SQLException
        {
            return new VOInfo(rs.getString("vogroup"), rs.getString("vorole"));
        }
    };

    public void init() throws DataAccessException
    {
        insertRetentionPolicies();
        insertAccessLatencies();
    }

    private void insertRetentionPolicies() throws DataAccessException
    {
        RetentionPolicy[] policies = RetentionPolicy.getAllPolicies();
        Long cnt = getJdbcTemplate().queryForObject("SELECT count(*) FROM " + RETENTION_POLICY_TABLE,
                                                    Long.class);
        if (cnt == policies.length) {
            return;
        }
        for (RetentionPolicy policy : policies) {
            try {
                getJdbcTemplate().update("INSERT INTO " + RETENTION_POLICY_TABLE + " (id, name) VALUES (?,?)",
                                         policy.getId(), policy.toString());
            } catch (DataAccessException sqle) {
                LOGGER.error("insert retention policy {} failed: {}",
                             policy, sqle.getMessage());
            }
        }
    }

    private void insertAccessLatencies() throws DataAccessException
    {
        AccessLatency[] latencies = AccessLatency.getAllLatencies();
        Long cnt = getJdbcTemplate().queryForObject(
                "SELECT count(*) from " + ACCESS_LATENCY_TABLE, Long.class);
        if (cnt == latencies.length) {
            return;
        }
        for (AccessLatency latency : latencies) {
            try {
                getJdbcTemplate().update("INSERT INTO " + ACCESS_LATENCY_TABLE + " (id, name) VALUES (?,?)",
                                         latency.getId(), latency.toString());
            } catch (DataAccessException sqle) {
                LOGGER.error("insert access latency {} failed: {}",
                             latency, sqle.getMessage());
            }
        }
    }

    @Override
    public List<File> getFilesInSpace(long spaceId)
            throws DataAccessException
    {
        return getJdbcTemplate().query(
                "SELECT * FROM " + SPACEFILE_TABLE + " WHERE spacereservationid = ?", fileMapper, spaceId);
    }

    @Override
    public void removeExpiredFilesFromSpace(long spaceId, Set<FileState> states)
            throws DataAccessException
    {
        List<File> files = getJdbcTemplate().query(
                "SELECT * FROM " + SPACEFILE_TABLE + " WHERE creationTime+lifetime < ? AND spacereservationid=?",
                fileMapper,
                System.currentTimeMillis(),
                spaceId);
        for (File file : files) {
            if (states.contains(file.getState())) {
                removeFile(file.getId());
            }
        }
    }

    @Override
    public void removeFile(long fileId) throws DataAccessException
    {
        int rc = getJdbcTemplate().update("DELETE FROM " + SPACEFILE_TABLE + " WHERE id=?", fileId);
        if (rc > 1) {
            throw new JdbcUpdateAffectedIncorrectNumberOfRowsException("delete returned row count = " + rc, 1, rc);
        }
    }

    @Override @Transactional(propagation = Propagation.MANDATORY, noRollbackFor = EmptyResultDataAccessException.class)
    public Space selectSpaceForUpdate(long id, long sizeInBytes) throws DataAccessException
    {
        try {
            return getJdbcTemplate().queryForObject(
                    "SELECT * FROM " + SPACE_TABLE + " WHERE  id = ? AND sizeinbytes-allocatedspaceinbytes >= ? FOR UPDATE",
                    spaceReservationMapper, id, sizeInBytes);
        } catch (EmptyResultDataAccessException e) {
            throw new EmptyResultDataAccessException(
                    "No space reservation with id " + id + " and " + sizeInBytes + " bytes available.", 1, e);
        }
    }

    @Override @Transactional(propagation = Propagation.MANDATORY, noRollbackFor = EmptyResultDataAccessException.class)
    public Space selectSpaceForUpdate(long id) throws DataAccessException
    {
        try {
            return getJdbcTemplate().queryForObject(
                    "SELECT * FROM " + SPACE_TABLE + " WHERE id = ? FOR UPDATE", spaceReservationMapper, id);
        } catch (EmptyResultDataAccessException e) {
            throw new EmptyResultDataAccessException("No such space reservation: " + id, 1, e);
        }
    }

    @Override @Transactional(propagation = Propagation.MANDATORY, noRollbackFor = EmptyResultDataAccessException.class)
    public File selectFileForUpdate(PnfsId pnfsId) throws DataAccessException
    {
        try {
            return getJdbcTemplate().queryForObject(
                    "SELECT * FROM " + SPACEFILE_TABLE + " WHERE pnfsid = ? FOR UPDATE ", fileMapper,
                    pnfsId.toString());
        } catch (EmptyResultDataAccessException e) {
            throw new EmptyResultDataAccessException("No file with PNFS ID: " + pnfsId, 1, e);
        }
    }

    @Override @Transactional(propagation = Propagation.MANDATORY, noRollbackFor = EmptyResultDataAccessException.class)
    public File selectFileForUpdate(long id) throws DataAccessException
    {
        try {
            return getJdbcTemplate().queryForObject(
                    "SELECT * FROM " + SPACEFILE_TABLE + " WHERE id = ? FOR UPDATE ", fileMapper, id);
        } catch (EmptyResultDataAccessException e) {
            throw new EmptyResultDataAccessException("No such file id: " + id, 1, e);
        }
    }

    @Override @Transactional(propagation = Propagation.MANDATORY, noRollbackFor = EmptyResultDataAccessException.class)
    public File selectFileFromSpaceForUpdate(String pnfsPath, long reservationId)
            throws DataAccessException
    {
        try {
            return getJdbcTemplate().queryForObject(
                    "SELECT * FROM " + SPACEFILE_TABLE + " WHERE  pnfspath=? AND spacereservationid=? AND state IN "
                            + "(" + FileState.RESERVED.getStateId() + "," + FileState.TRANSFERRING.getStateId() + ") FOR UPDATE",
                    fileMapper,
                    pnfsPath,
                    reservationId);
        } catch (EmptyResultDataAccessException e) {
            throw new EmptyResultDataAccessException(
                    "No such transient file in space " + reservationId + ": " + pnfsPath, 1, e);
        }
    }

    @Override
    public Space updateSpace(long id,
                             String voGroup,
                             String voRole,
                             RetentionPolicy retentionPolicy,
                             AccessLatency accessLatency,
                             Long linkGroupId,
                             Long sizeInBytes,
                             Long lifetime,
                             String description,
                             SpaceState state)
            throws DataAccessException
    {
        return updateSpace(selectSpaceForUpdate(id),
                           voGroup,
                           voRole,
                           retentionPolicy,
                           accessLatency,
                           linkGroupId,
                           sizeInBytes,
                           lifetime,
                           description,
                           state);
    }

    @Override
    public Space updateSpace(Space space,
                             String voGroup,
                             String voRole,
                             RetentionPolicy retentionPolicy,
                             AccessLatency accessLatency,
                             Long linkGroupId,
                             Long sizeInBytes,
                             Long lifetime,
                             String description,
                             SpaceState state)
            throws DataAccessException
    {
        if (voGroup != null) {
            space.setVoGroup(voGroup);
        }
        if (voRole != null) {
            space.setVoRole(voRole);
        }
        if (retentionPolicy != null) {
            space.setRetentionPolicy(retentionPolicy);
        }
        if (accessLatency != null) {
            space.setAccessLatency(accessLatency);
        }
        if (sizeInBytes != null) {
            long usedSpace = space.getUsedSizeInBytes() + space.getAllocatedSpaceInBytes();
            if (sizeInBytes < usedSpace) {
                throw new DataIntegrityViolationException(
                        "Cannot downsize space reservation below " + usedSpace + "bytes, remove files first ");
            }
            space.setSizeInBytes(sizeInBytes);
        }
        if (lifetime != null) {
            space.setLifetime(lifetime);
        }
        if (description != null) {
            space.setDescription(description);
        }
        SpaceState oldState = space.getState();
        if (state != null) {
            if (SpaceState.isFinalState(oldState)) {
                throw new DataIntegrityViolationException(
                        "change from " + oldState + " to " + state + " is not allowed");
            }
            space.setState(state);
        }
        getJdbcTemplate().update(
                "UPDATE " + SPACE_TABLE
                        + " SET vogroup=?,vorole=?,retentionpolicy=?,accesslatency=?,linkgroupid=?,sizeinbytes=?,"
                        + " creationtime=?,lifetime=?,description=?,state=? WHERE id=?",
                space.getVoGroup(),
                space.getVoRole(),
                space.getRetentionPolicy().getId(),
                space.getAccessLatency().getId(),
                space.getLinkGroupId(),
                space.getSizeInBytes(),
                space.getCreationTime(),
                space.getLifetime(),
                space.getDescription(),
                space.getState().getStateId(),
                space.getId());
        return space;
    }


    @Override @Transactional
    public long updateLinkGroup(final String linkGroupName,
                                final long freeSpace,
                                final long updateTime,
                                final boolean onlineAllowed,
                                final boolean nearlineAllowed,
                                final boolean replicaAllowed,
                                final boolean outputAllowed,
                                final boolean custodialAllowed,
                                VOInfo[] linkGroupVOs) throws DataAccessException
    {
        long id;
        try {
            LinkGroup group =
                    getJdbcTemplate().queryForObject(
                            "SELECT * FROM " + LINKGROUP_TABLE + " WHERE  name = ? FOR UPDATE",
                            linkGroupMapper,
                            linkGroupName);
            id = group.getId();
            getJdbcTemplate().update(
                    "UPDATE " + LINKGROUP_TABLE + " SET freeSpaceInBytes=?,lastUpdateTime=?,onlineAllowed=?,nearlineAllowed=?,"
                            + "replicaAllowed=?,outputAllowed=?,custodialAllowed=? WHERE  id = ?",
                    freeSpace,
                    updateTime,
                    (onlineAllowed ? 1 : 0),
                    (nearlineAllowed ? 1 : 0),
                    (replicaAllowed ? 1 : 0),
                    (outputAllowed ? 1 : 0),
                    (custodialAllowed ? 1 : 0),
                    id);
        } catch (EmptyResultDataAccessException e) {
            try {
                KeyHolder keyHolder = new GeneratedKeyHolder();
                getJdbcTemplate().update(
                        new PreparedStatementCreator()
                        {
                            @Override
                            public PreparedStatement createPreparedStatement(Connection con) throws SQLException
                            {
                                /* Note that neither prepareStatement(String, String[]) nor prepareStatement(String, int[])
                                 * work for us: The former suffers from different interpretations of case in HSQLDB and
                                 * PostgreSQL and the latter is not support by the PostgreSQL JDBC driver.
                                 */
                                PreparedStatement stmt = con.prepareStatement(
                                        "INSERT INTO " + LINKGROUP_TABLE
                                                + " (name, freeSpaceInBytes, lastUpdateTime, onlineAllowed,"
                                                + " nearlineAllowed, replicaAllowed, outputAllowed, custodialAllowed,reservedspaceinbytes)"
                                                + " VALUES (?,?,?,?,?,?,?,?,?)", Statement.RETURN_GENERATED_KEYS);
                                stmt.setString(1, linkGroupName);
                                stmt.setLong(2, freeSpace);
                                stmt.setLong(3, updateTime);
                                stmt.setInt(4, (onlineAllowed ? 1 : 0));
                                stmt.setInt(5, (nearlineAllowed ? 1 : 0));
                                stmt.setInt(6, (replicaAllowed ? 1 : 0));
                                stmt.setInt(7, (outputAllowed ? 1 : 0));
                                stmt.setInt(8, (custodialAllowed ? 1 : 0));
                                stmt.setLong(9, (long) 0);
                                return stmt;
                            }
                        },
                        keyHolder);
                id = (Long) keyHolder.getKeys().get("id");
            } catch (DataAccessException e1) {
                LOGGER.error("failed to insert linkgroup {}: {}",
                             linkGroupName, e.getMessage());
                throw e1;
            }
        }

        final Set<VOInfo> deleteVOs = new HashSet<>();
        final Set<VOInfo> insertVOs = new HashSet<>();
        if (linkGroupVOs != null) {
            insertVOs.addAll(asList(linkGroupVOs));
        }

        getJdbcTemplate().query("SELECT VOGroup,VORole FROM " + LINKGROUP_VO_TABLE + " WHERE linkGroupId=?",
                                new RowCallbackHandler()
                                {
                                    @Override
                                    public void processRow(ResultSet rs) throws SQLException
                                    {
                                        String nextVOGroup = rs.getString(1);
                                        String nextVORole = rs.getString(2);
                                        VOInfo nextVO = new VOInfo(nextVOGroup, nextVORole);
                                        if (!insertVOs.remove(nextVO)) {
                                            deleteVOs.add(nextVO);
                                        }
                                    }
                                }, id);

        for (VOInfo nextVo : insertVOs) {
            getJdbcTemplate().update(
                    "INSERT INTO " + LINKGROUP_VO_TABLE + " ( VOGroup, VORole, linkGroupId ) VALUES ( ? , ? , ? )",
                    nextVo.getVoGroup(),
                    nextVo.getVoRole(),
                    id);
        }
        for (VOInfo nextVo : deleteVOs) {
            getJdbcTemplate().update(
                    "DELETE FROM " + LINKGROUP_VO_TABLE + " WHERE VOGroup  = ? AND VORole = ? AND linkGroupId = ? ",
                    nextVo.getVoGroup(),
                    nextVo.getVoRole(),
                    id);
        }
        return id;
    }

    @Override
    public Space insertSpace(final String voGroup,
                             final String voRole,
                             final RetentionPolicy retentionPolicy,
                             final AccessLatency accessLatency,
                             final long linkGroupId,
                             final long sizeInBytes,
                             final long lifetime,
                             final String description,
                             final SpaceState state,
                             final long used,
                             final long allocated)
            throws DataAccessException
    {
        final long creationTime = System.currentTimeMillis();
        KeyHolder keyHolder = new GeneratedKeyHolder();
        int rc = getJdbcTemplate().update(
                new PreparedStatementCreator()
                {
                    @Override
                    public PreparedStatement createPreparedStatement(Connection con) throws SQLException
                    {
                        /* Note that neither prepareStatement(String, String[]) nor prepareStatement(String, int[])
                         * work for us: The former suffers from different interpretations of case in HSQLDB and
                         * PostgreSQL and the latter is not support by the PostgreSQL JDBC driver.
                         */
                        PreparedStatement stmt = con.prepareStatement(
                                "INSERT INTO " + SPACE_TABLE
                                        + " (vogroup,vorole,retentionpolicy,accesslatency,linkgroupid,"
                                        + "sizeinbytes,creationtime,lifetime,description,state,usedspaceinbytes,allocatedspaceinbytes)"
                                        + " VALUES  (?,?,?,?,?,?,?,?,?,?,?,?)", Statement.RETURN_GENERATED_KEYS);
                        stmt.setString(1, voGroup);
                        stmt.setString(2, voRole);
                        stmt.setInt(3, retentionPolicy == null ? 0 : retentionPolicy.getId());
                        stmt.setInt(4, accessLatency == null ? 0 : accessLatency.getId());
                        stmt.setLong(5, linkGroupId);
                        stmt.setLong(6, sizeInBytes);
                        stmt.setLong(7, creationTime);
                        stmt.setLong(8, lifetime);
                        stmt.setString(9, description);
                        stmt.setInt(10, state.getStateId());
                        stmt.setLong(11, used);
                        stmt.setLong(12, allocated);
                        return stmt;
                    }
                },
                keyHolder);
        if (rc != 1) {
            throw new JdbcUpdateAffectedIncorrectNumberOfRowsException("insert returned row count =" + rc, 1, rc);
        }
        return new Space((Long) keyHolder.getKeys().get("id"),
                         voGroup,
                         voRole,
                         retentionPolicy,
                         accessLatency,
                         linkGroupId,
                         sizeInBytes,
                         creationTime,
                         lifetime,
                         description,
                         state,
                         used,
                         allocated);
    }

    @Override
    public List<Long> findSpaceTokensByVoGroupAndRole(String voGroup, String voRole)
            throws DataAccessException
    {
        if (!isNullOrEmpty(voGroup) && !isNullOrEmpty(voRole)) {
            return getJdbcTemplate().queryForList(
                    "SELECT id FROM " + SPACE_TABLE + " WHERE  state = ? AND voGroup = ? AND voRole = ?",
                    Long.class,
                    SpaceState.RESERVED.getStateId(),
                    voGroup,
                    voRole);
        }
        if (!isNullOrEmpty(voGroup)) {
            return getJdbcTemplate().queryForList(
                    "SELECT id FROM " + SPACE_TABLE + " WHERE  state = ? AND voGroup = ?", Long.class,
                    SpaceState.RESERVED.getStateId(),
                    voGroup);
        }
        if (!isNullOrEmpty(voRole)) {
            return getJdbcTemplate().queryForList(
                    "SELECT id FROM " + SPACE_TABLE + " WHERE  state = ? AND  voRole = ?", Long.class,
                    SpaceState.RESERVED.getStateId(),
                    voRole);
        }
        return Collections.emptyList();
    }

    @Override
    public Space getSpace(long id) throws DataAccessException
    {
        try {
            return getJdbcTemplate().queryForObject(
                    "SELECT * FROM " + SPACE_TABLE + " WHERE id=?", spaceReservationMapper, id);
        } catch (EmptyResultDataAccessException e) {
            throw new EmptyResultDataAccessException("No such space reservation: " + id, 1, e);
        }
    }

    @Override
    public LinkGroup getLinkGroup(long id) throws DataAccessException
    {
        try {
            return getJdbcTemplate().queryForObject(
                    "SELECT * FROM " + LINKGROUP_TABLE + " WHERE  id = ?", linkGroupMapper, id);
        } catch (EmptyResultDataAccessException e) {
            throw new EmptyResultDataAccessException("No such link group: " + id, 1, e);
        }
    }

    @Override
    public List<LinkGroup> getLinkGroups()
    {
        return getJdbcTemplate().query("SELECT * FROM " + LINKGROUP_TABLE, linkGroupMapper);
    }

    @Override
    public List<LinkGroup> getLinkGroupsRefreshedAfter(long lastUpdateTime)
    {
        return getJdbcTemplate().query("SELECT * FROM " + LINKGROUP_TABLE + " WHERE lastUpdateTime >= ?",
                                       linkGroupMapper, lastUpdateTime);
    }

    @Override
    public LinkGroup getLinkGroupByName(String name) throws DataAccessException
    {
        try {
            return getJdbcTemplate().queryForObject(
                    "SELECT * FROM " + LINKGROUP_TABLE + " WHERE  name = ?", linkGroupMapper, name);
        } catch (EmptyResultDataAccessException e) {
            throw new EmptyResultDataAccessException("No such link group: " + name, 1, e);
        }
    }

    @Override
    public void updateFile(String voGroup,
                           String voRole,
                           PnfsId pnfsId,
                           Long sizeInBytes,
                           Long lifetime,
                           FileState state,
                           Boolean deleted,
                           File f)
            throws DataAccessException
    {
        if (voGroup != null) {
            f.setVoGroup(voGroup);
        }
        if (voRole != null) {
            f.setVoRole(voRole);
        }
        if (sizeInBytes != null) {
            f.setSizeInBytes(sizeInBytes);
        }
        if (lifetime != null) {
            f.setLifetime(lifetime);
        }
        if (state != null) {
            f.setState(state);
        }
        if (pnfsId != null) {
            f.setPnfsId(pnfsId);
        }
        if (deleted != null) {
            f.setDeleted(deleted ? 1 : 0);
        }
        int rc = getJdbcTemplate().update(
                "UPDATE " + SPACEFILE_TABLE +
                        " SET vogroup=?, vorole=?, sizeinbytes=?, lifetime=?, pnfsid=?, state=? WHERE id=?",
                f.getVoGroup(),
                f.getVoRole(),
                f.getSizeInBytes(),
                f.getLifetime(),
                Objects.toString(f.getPnfsId(), null),
                f.getState().getStateId(),
                f.getId());
        if (rc != 1) {
            throw new JdbcUpdateAffectedIncorrectNumberOfRowsException("Update failed, row count=" + rc, 1, rc);
        }
    }

    private static final String onlineSelectionCondition =
            "lg.onlineallowed = 1 ";
    private static final String nearlineSelectionCondition =
            "lg.nearlineallowed = 1 ";
    private static final String replicaSelectionCondition =
            "lg.replicaallowed = 1 ";
    private static final String outputSelectionCondition =
            "lg.outputallowed = 1 ";
    private static final String custodialSelectionCondition =
            "lg.custodialAllowed = 1 ";

    private static final String voGroupSelectionCondition =
            " ( lgvo.VOGroup = ? OR lgvo.VOGroup = '*' ) ";
    private static final String voRoleSelectionCondition =
            " ( lgvo.VORole = ? OR lgvo.VORole = '*' ) ";

    private static final String spaceCondition =
            " lg.freespaceinbytes-lg.reservedspaceinbytes >= ? ";
    private static final String orderBy =
            " ORDER BY lg.freespaceinbytes-lg.reservedspaceinbytes DESC ";

    private static final String selectLinkGroupIdPart1 =
            "SELECT lg.id FROM " + LINKGROUP_TABLE + " lg, " + LINKGROUP_VO_TABLE + " lgvo " +
                    "WHERE lg.id = lgvo.linkGroupId  AND  lg.lastUpdateTime >= ? ";

    private static final String selectLinkGroupInfoPart1 =
            "SELECT lg.* FROM " + LINKGROUP_TABLE + " lg " +
                    "WHERE lg.lastUpdateTime >= ? ";

    private static final String selectOnlineReplicaLinkGroup =
            selectLinkGroupIdPart1 + " and " +
                    onlineSelectionCondition + " and " +
                    replicaSelectionCondition + " and " +
                    voGroupSelectionCondition + " and " +
                    voRoleSelectionCondition + " and " +
                    spaceCondition +
                    orderBy;

    private static final String selectOnlineOutputLinkGroup =
            selectLinkGroupIdPart1 + " and " +
                    onlineSelectionCondition + " and " +
                    outputSelectionCondition + " and " +
                    voGroupSelectionCondition + " and " +
                    voRoleSelectionCondition + " and " +
                    spaceCondition +
                    orderBy;

    private static final String selectOnlineCustodialLinkGroup =
            selectLinkGroupIdPart1 + " and " +
                    onlineSelectionCondition + " and " +
                    custodialSelectionCondition + " and " +
                    voGroupSelectionCondition + " and " +
                    voRoleSelectionCondition + " and " +
                    spaceCondition +
                    orderBy;

    private static final String selectNearlineReplicaLinkGroup =
            selectLinkGroupIdPart1 + " and " +
                    nearlineSelectionCondition + " and " +
                    replicaSelectionCondition + " and " +
                    voGroupSelectionCondition + " and " +
                    voRoleSelectionCondition + " and " +
                    spaceCondition +
                    orderBy;

    private static final String selectNearlineOutputLinkGroup =
            selectLinkGroupIdPart1 + " and " +
                    nearlineSelectionCondition + " and " +
                    outputSelectionCondition + " and " +
                    voGroupSelectionCondition + " and " +
                    voRoleSelectionCondition + " and " +
                    spaceCondition +
                    orderBy;


    private static final String selectNearlineCustodialLinkGroup =
            selectLinkGroupIdPart1 + " and " +
                    nearlineSelectionCondition + " and " +
                    custodialSelectionCondition + " and " +
                    voGroupSelectionCondition + " and " +
                    voRoleSelectionCondition + " and " +
                    spaceCondition +
                    orderBy;

    private static final String selectAllOnlineReplicaLinkGroup =
            selectLinkGroupInfoPart1 + " and " +
                    onlineSelectionCondition + " and " +
                    replicaSelectionCondition + " and " +
                    spaceCondition +
                    orderBy;

    private static final String selectAllOnlineOutputLinkGroup =
            selectLinkGroupInfoPart1 + " and " +
                    onlineSelectionCondition + " and " +
                    outputSelectionCondition + " and " +
                    spaceCondition +
                    orderBy;

    private static final String selectAllOnlineCustodialLinkGroup =
            selectLinkGroupInfoPart1 + " and " +
                    onlineSelectionCondition + " and " +
                    custodialSelectionCondition + " and " +
                    spaceCondition +
                    orderBy;

    private static final String selectAllNearlineReplicaLinkGroup =
            selectLinkGroupInfoPart1 + " and " +
                    nearlineSelectionCondition + " and " +
                    replicaSelectionCondition + " and " +
                    spaceCondition +
                    orderBy;

    private static final String selectAllNearlineOutputLinkGroup =
            selectLinkGroupInfoPart1 + " and " +
                    nearlineSelectionCondition + " and " +
                    outputSelectionCondition + " and " +
                    spaceCondition +
                    orderBy;


    private static final String selectAllNearlineCustodialLinkGroup =
            selectLinkGroupInfoPart1 + " and " +
                    nearlineSelectionCondition + " and " +
                    custodialSelectionCondition + " and " +
                    spaceCondition +
                    orderBy;

    @Override
    public List<Long> findLinkGroupIds(long sizeInBytes,
                                       String voGroup,
                                       String voRole,
                                       AccessLatency al,
                                       RetentionPolicy rp,
                                       long lastUpdateTime)
            throws DataAccessException
    {
        LOGGER.trace("findLinkGroups(sizeInBytes={}, " +
                             "voGroup={} voRole={}, AccessLatency={}, " +
                             "RetentionPolicy={})", sizeInBytes, voGroup,
                     voRole, al, rp);
        String select;
        if (al.equals(AccessLatency.ONLINE)) {
            if (rp.equals(RetentionPolicy.REPLICA)) {
                select = selectOnlineReplicaLinkGroup;
            } else if (rp.equals(RetentionPolicy.OUTPUT)) {
                select = selectOnlineOutputLinkGroup;
            } else {
                select = selectOnlineCustodialLinkGroup;
            }
        } else {
            if (rp.equals(RetentionPolicy.REPLICA)) {
                select = selectNearlineReplicaLinkGroup;
            } else if (rp.equals(RetentionPolicy.OUTPUT)) {
                select = selectNearlineOutputLinkGroup;
            } else {
                select = selectNearlineCustodialLinkGroup;
            }
        }
        return getJdbcTemplate().queryForList(select, Long.class,
                                              lastUpdateTime,
                                              voGroup,
                                              voRole,
                                              sizeInBytes);
    }

    @Override
    public List<LinkGroup> findLinkGroups(long sizeInBytes,
                                          AccessLatency al,
                                          RetentionPolicy rp,
                                          long lastUpdateTime)
            throws DataAccessException
    {
        LOGGER.trace("findLinkGroups(sizeInBytes={}, AccessLatency={}, RetentionPolicy={})",
                     sizeInBytes, al, rp);
        String select;
        if (al.equals(AccessLatency.ONLINE)) {
            if (rp.equals(RetentionPolicy.REPLICA)) {
                select = selectAllOnlineReplicaLinkGroup;
            } else if (rp.equals(RetentionPolicy.OUTPUT)) {
                select = selectAllOnlineOutputLinkGroup;
            } else {
                select = selectAllOnlineCustodialLinkGroup;
            }

        } else {
            if (rp.equals(RetentionPolicy.REPLICA)) {
                select = selectAllNearlineReplicaLinkGroup;
            } else if (rp.equals(RetentionPolicy.OUTPUT)) {
                select = selectAllNearlineOutputLinkGroup;
            } else {
                select = selectAllNearlineCustodialLinkGroup;
            }
        }
        return getJdbcTemplate().query(select, linkGroupMapper, lastUpdateTime, sizeInBytes);
    }


    @Override
    public void clearPnfsIdOfFile(long id)
            throws DataAccessException
    {
        int rc = getJdbcTemplate().update("UPDATE " + SPACEFILE_TABLE + " SET pnfsid = NULL WHERE id=?", id);
        if (rc != 1) {
            throw new JdbcUpdateAffectedIncorrectNumberOfRowsException("Update failed, row count=" + rc, 1, rc);
        }
    }

    @Override
    public void removePnfsIdAndChangeStateOfFile(long id, FileState state)
            throws DataAccessException
    {
        int rc = getJdbcTemplate().update(
                "UPDATE " + SPACEFILE_TABLE + " SET pnfsid = NULL, STATE=? WHERE id=?", state.getStateId(), id);
        if (rc != 1) {
            throw new JdbcUpdateAffectedIncorrectNumberOfRowsException("Update failed, row count=" + rc, 1, rc);
        }
    }

    @Override
    public File getFile(PnfsId pnfsId) throws DataAccessException
    {
        try {
            return getJdbcTemplate().queryForObject("SELECT * FROM " + SPACEFILE_TABLE + " WHERE pnfsId=?", fileMapper,
                                                    pnfsId.toString());
        } catch (EmptyResultDataAccessException e) {
            throw new EmptyResultDataAccessException("file with pnfsId=" + pnfsId + " is not found", 1, e);
        } catch (IncorrectResultSizeDataAccessException e) {
            throw new IncorrectResultSizeDataAccessException("found more than one record with pnfsId=" + pnfsId, 1,
                                                             e.getActualSize(), e);
        }
    }

    @Override
    public long insertFile(final long reservationId,
                           final String voGroup,
                           final String voRole,
                           final long sizeInBytes,
                           final long lifetime,
                           String pnfsPath,
                           final PnfsId pnfsId)
            throws DataAccessException, SpaceException
    {
        //
        // check that there is no such file already being transferred
        //
        final FsPath path;
        if (pnfsPath != null) {
            path = new FsPath(pnfsPath);
            List<File> files = getJdbcTemplate().query(
                    "SELECT * FROM " + SPACEFILE_TABLE + " WHERE pnfspath=? AND state IN "
                            + '(' + FileState.RESERVED.getStateId() + "," + FileState.TRANSFERRING.getStateId() + ") AND deleted <> 1",
                    fileMapper, path.toString());
            if (!files.isEmpty()) {
                throw new DataIntegrityViolationException(
                        "Already have " + files.size() + " record(s) with pnfsPath=" + pnfsPath);
            }
        } else {
            path = null;
        }
        final long creationTime = System.currentTimeMillis();
        Space space = selectSpaceForUpdate(reservationId,
                                           0L); // "0L" is a hack needed to get a better error code from comparison below
        long currentTime = System.currentTimeMillis();
        if (space.getLifetime() != -1 && space.getCreationTime() + space.getLifetime() < currentTime) {
            throw new SpaceExpiredException("space with id=" + reservationId + " has expired");
        }
        if (space.getState() == SpaceState.EXPIRED) {
            throw new SpaceExpiredException("space with id=" + reservationId + " has expired");
        }
        if (space.getState() == SpaceState.RELEASED) {
            throw new SpaceReleasedException("space with id=" + reservationId + " was released");
        }
        if (space.getAvailableSpaceInBytes() < sizeInBytes) {
            throw new NoFreeSpaceException("space with id=" + reservationId + " does not have enough space");
        }

        KeyHolder keyHolder = new GeneratedKeyHolder();

        int rc = getJdbcTemplate().update(new PreparedStatementCreator()
        {
            @Override
            public PreparedStatement createPreparedStatement(Connection con) throws SQLException
            {
                /* Note that neither prepareStatement(String, String[]) nor prepareStatement(String, int[])
                 * work for us: The former suffers from different interpretations of case in HSQLDB and
                 * PostgreSQL and the latter is not support by the PostgreSQL JDBC driver.
                 */
                PreparedStatement stmt = con.prepareStatement(
                        "INSERT INTO " + SPACEFILE_TABLE
                                + " (vogroup,vorole,spacereservationid,sizeinbytes,creationtime,lifetime,pnfspath,pnfsid,state,deleted) "
                                + " VALUES  (?,?,?,?,?,?,?,?,?,0)", Statement.RETURN_GENERATED_KEYS);
                stmt.setString(1, voGroup);
                stmt.setString(2, voRole);
                stmt.setLong(3, reservationId);
                stmt.setLong(4, sizeInBytes);
                stmt.setLong(5, creationTime);
                stmt.setLong(6, lifetime);
                stmt.setString(7, Objects.toString(path, null));
                stmt.setString(8, Objects.toString(pnfsId, null));
                stmt.setInt(9, FileState.RESERVED.getStateId());
                return stmt;
            }
        }, keyHolder);
        if (rc != 1) {
            throw new JdbcUpdateAffectedIncorrectNumberOfRowsException("insert returned row count =" + rc, 1, rc);
        }
        return (Long) keyHolder.getKeys().get("id");
    }


    @Override
    public List<Long> findSpaceTokensByDescription(String description)
    {
        return getJdbcTemplate().queryForList(
                "SELECT id FROM " + SPACE_TABLE + " WHERE  state = ? AND description = ?",
                Long.class,
                SpaceState.RESERVED.getStateId(),
                description);
    }


    @Override
    public List<Long> getSpaceTokensOfFile(PnfsId pnfsId, FsPath pnfsPath) throws DataAccessException
    {
        List<Long> files;
        if (pnfsId != null && pnfsPath != null) {
            files = getJdbcTemplate().queryForList(
                    "SELECT spacereservationid FROM " + SPACEFILE_TABLE + " WHERE pnfsId = ? AND pnfsPath = ?",
                    Long.class,
                    pnfsId.toString(),
                    pnfsPath.toString());
        } else if (pnfsId != null) {
            files = getJdbcTemplate().queryForList(
                    "SELECT spacereservationid FROM " + SPACEFILE_TABLE + " WHERE pnfsId = ? ",
                    Long.class,
                    pnfsId.toString());
        } else if (pnfsPath != null) {
            files = getJdbcTemplate().queryForList(
                    "SELECT spacereservationid FROM " + SPACEFILE_TABLE + " WHERE pnfsPath = ? ",
                    Long.class,
                    pnfsPath.toString());
        } else {
            throw new IllegalArgumentException("getSpaceTokensOfFile: all arguments are nulls, not supported");
        }
        return files;
    }

    @Override
    public void expireSpaces()
    {
        getJdbcTemplate().update(
                "UPDATE " + SPACE_TABLE + " SET state = " + SpaceState.EXPIRED.getStateId()
                        + " WHERE state = " + SpaceState.RESERVED.getStateId() + " AND lifetime != -1 AND creationTime + lifetime < ?",
                System.currentTimeMillis());
    }

    @Override
    public List<File> getExpiredFiles()
    {
        return getJdbcTemplate().query(
                "SELECT * FROM " + SPACEFILE_TABLE + " WHERE state IN "
                        + '(' + FileState.RESERVED.getStateId() + ',' + FileState.TRANSFERRING.getStateId() + ") AND creationTime+lifetime < ?",
                fileMapper,
                System.currentTimeMillis());
    }

    @Override
    public List<Space> getReservedSpaces()
    {
        return getJdbcTemplate().query(
                "SELECT * FROM " + SPACE_TABLE + " WHERE state = " + SpaceState.RESERVED.getStateId(),
                spaceReservationMapper);
    }

    @Override
    public File getUnboundFile(String pnfsPath)
    {
        List<File> files = getJdbcTemplate().query(
                "SELECT * FROM " + SPACEFILE_TABLE + " WHERE pnfspath=? AND pnfsid IS NULL AND deleted != 1",
                fileMapper,
                pnfsPath);
        return getFirst(files, null);
    }

    @Override
    public List<Space> findSpaces(String group, String role, String description, LinkGroup lg)
    {
        // FIXME: This is postgresql specific
        StringBuilder query = new StringBuilder("SELECT * FROM " + SPACE_TABLE + " WHERE");
        Map<String, Object> params = new HashMap<>();

        if (group != null) {
            query.append(" vogroup ~ :group AND ");
            params.put("group", group);
        }
        if (role != null) {
            query.append(" vorole ~ :role AND");
            params.put("role", role);
        }
        if (description != null) {
            query.append(" description ~ :description AND");
            params.put("description", description);
        }
        if (lg != null) {
            query.append(" linkgroupid = :linkgroupid AND");
            params.put("linkgroupid", lg.getId());
        }
        query.append(" state = ").append(SpaceState.RESERVED.getStateId());

        return getNamedParameterJdbcTemplate().query(query.toString(), params, spaceReservationMapper);
    }

    @Override
    public List<Space> getSpaces(Set<SpaceState> states, int nRows)
            throws DataAccessException
    {
        String query = "SELECT * FROM " + SPACE_TABLE + " WHERE state IN "
                + '(' + Joiner.on(",").join(Iterables.transform(states, SpaceState.getStateId)) + ')'
                + " LIMIT " + nRows;
        return getJdbcTemplate().query(query, spaceReservationMapper);
    }
}
