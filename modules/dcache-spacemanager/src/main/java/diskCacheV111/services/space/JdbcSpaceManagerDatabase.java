package diskCacheV111.services.space;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.joining;

import diskCacheV111.util.AccessLatency;
import diskCacheV111.util.PnfsId;
import diskCacheV111.util.RetentionPolicy;
import diskCacheV111.util.VOInfo;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;
import org.dcache.util.SqlGlob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.dao.support.DataAccessUtils;
import org.springframework.jdbc.JdbcUpdateAffectedIncorrectNumberOfRowsException;
import org.springframework.jdbc.core.support.JdbcDaoSupport;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

@Repository
public class JdbcSpaceManagerDatabase extends JdbcDaoSupport implements SpaceManagerDatabase {

    private static final Logger LOGGER = LoggerFactory.getLogger(JdbcSpaceManagerDatabase.class);

    private static final String RETENTION_POLICY_TABLE = "srmretentionpolicy";
    private static final String ACCESS_LATENCY_TABLE = "srmaccesslatency";

    /*
                     Table "public.srmlinkgroup"
            Column        |           Type           | Modifiers
    ----------------------+--------------------------+-----------
     id                   | bigint                   | not null
     name                 | character varying(32672) |
     availablespaceinbytes| bigint                   | not null
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
      creationtime          | bigint                   | not null
      expirationtime        | bigint                   |
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
     creationtime       | bigint                   | not null
     pnfsid             | character varying(36)    | unique
     state              | integer                  | not null
    */
    private static final String SPACEFILE_TABLE = "srmspacefile";

    public void init() throws DataAccessException {
        insertRetentionPolicies();
        insertAccessLatencies();
    }

    private void insertRetentionPolicies() throws DataAccessException {
        RetentionPolicy[] policies = RetentionPolicy.getAllPolicies();
        Long cnt = getJdbcTemplate().queryForObject(
              "SELECT count(*) FROM " + RETENTION_POLICY_TABLE,
              Long.class);
        if (cnt == policies.length) {
            return;
        }
        for (RetentionPolicy policy : policies) {
            try {
                getJdbcTemplate().update(
                      "INSERT INTO " + RETENTION_POLICY_TABLE + " (id, name) VALUES (?,?)",
                      policy.getId(), policy.toString());
            } catch (DataAccessException sqle) {
                LOGGER.error("insert retention policy {} failed: {}",
                      policy, sqle.getMessage());
            }
        }
    }

    private void insertAccessLatencies() throws DataAccessException {
        AccessLatency[] latencies = AccessLatency.getAllLatencies();
        Long cnt = getJdbcTemplate().queryForObject(
              "SELECT count(*) from " + ACCESS_LATENCY_TABLE, Long.class);
        if (cnt == latencies.length) {
            return;
        }
        for (AccessLatency latency : latencies) {
            try {
                getJdbcTemplate().update(
                      "INSERT INTO " + ACCESS_LATENCY_TABLE + " (id, name) VALUES (?,?)",
                      latency.getId(), latency.toString());
            } catch (DataAccessException sqle) {
                LOGGER.error("insert access latency {} failed: {}",
                      latency, sqle.getMessage());
            }
        }
    }

    @Override
    public void removeFile(long fileId) throws DataAccessException {
        int rc = getJdbcTemplate().update("DELETE FROM " + SPACEFILE_TABLE + " WHERE id=?", fileId);
        if (rc > 1) {
            throw new JdbcUpdateAffectedIncorrectNumberOfRowsException(
                  "delete returned row count = " + rc, 1, rc);
        }
    }

    @Override
    @Transactional(propagation = Propagation.MANDATORY, noRollbackFor = EmptyResultDataAccessException.class)
    public Space selectSpaceForUpdate(long id) throws DataAccessException {
        try {
            return getJdbcTemplate().queryForObject(
                  "SELECT * FROM " + SPACE_TABLE + " WHERE id = ? FOR UPDATE", this::toSpace, id);
        } catch (EmptyResultDataAccessException e) {
            throw new EmptyResultDataAccessException("No such space reservation: " + id, 1, e);
        }
    }

    @Override
    @Transactional(propagation = Propagation.MANDATORY, noRollbackFor = EmptyResultDataAccessException.class)
    public File selectFileForUpdate(PnfsId pnfsId) throws DataAccessException {
        try {
            return getJdbcTemplate().queryForObject(
                  "SELECT * FROM " + SPACEFILE_TABLE + " WHERE pnfsid = ? FOR UPDATE ",
                  this::toFile,
                  pnfsId.toString());
        } catch (EmptyResultDataAccessException e) {
            throw new EmptyResultDataAccessException(
                  "Space reservation for " + pnfsId + " not found.", 1, e);
        }
    }

    @Override
    @Transactional(propagation = Propagation.MANDATORY, noRollbackFor = EmptyResultDataAccessException.class)
    public File selectFileForUpdate(long id) throws DataAccessException {
        try {
            return getJdbcTemplate().queryForObject(
                  "SELECT * FROM " + SPACEFILE_TABLE + " WHERE id = ? FOR UPDATE ", this::toFile,
                  id);
        } catch (EmptyResultDataAccessException e) {
            throw new EmptyResultDataAccessException("No such file id: " + id, 1, e);
        }
    }

    @Override
    public Space updateSpace(Space space)
          throws DataAccessException {
        getJdbcTemplate().update(
              "UPDATE " + SPACE_TABLE
                    + " SET vogroup=?,vorole=?,retentionpolicy=?,accesslatency=?,linkgroupid=?,sizeinbytes=?,"
                    + " creationtime=?,expirationTime=?,description=?,state=? WHERE id=?",
              space.getVoGroup(),
              space.getVoRole(),
              space.getRetentionPolicy().getId(),
              space.getAccessLatency().getId(),
              space.getLinkGroupId(),
              space.getSizeInBytes(),
              space.getCreationTime(),
              space.getExpirationTime(),
              space.getDescription(),
              space.getState().getStateId(),
              space.getId());
        return space;
    }


    @Override
    @Transactional
    public long updateLinkGroup(final String linkGroupName,
          final long freeSpace,
          final long updateTime,
          final boolean onlineAllowed,
          final boolean nearlineAllowed,
          final boolean replicaAllowed,
          final boolean outputAllowed,
          final boolean custodialAllowed,
          VOInfo[] linkGroupVOs) throws DataAccessException {
        long id;
        try {
            /* FOR UPDATE to avoid lock upgrade below */
            id = getJdbcTemplate().queryForObject(
                  "SELECT id FROM " + LINKGROUP_TABLE + " WHERE name = ? FOR UPDATE", Long.class,
                  linkGroupName);
            getJdbcTemplate().update(
                  "UPDATE " + LINKGROUP_TABLE
                        + " SET availableSpaceInBytes=?-reservedSpaceInBytes,lastUpdateTime=?,onlineAllowed=?,nearlineAllowed=?,"
                        + "replicaAllowed=?,outputAllowed=?,custodialAllowed=? WHERE id = ?",
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
                      con -> {
                          /* Note that neither prepareStatement(String, String[]) nor prepareStatement(String, int[])
                           * work for us: The former suffers from different interpretations of case in HSQLDB and
                           * PostgreSQL and the latter is not support by the PostgreSQL JDBC driver.
                           */
                          PreparedStatement stmt = con.prepareStatement(
                                "INSERT INTO " + LINKGROUP_TABLE
                                      + " (name, availableSpaceInBytes, lastUpdateTime, onlineAllowed,"
                                      + " nearlineAllowed, replicaAllowed, outputAllowed, custodialAllowed,reservedspaceinbytes)"
                                      + " VALUES (?,?,?,?,?,?,?,?,?)",
                                Statement.RETURN_GENERATED_KEYS);
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

        getJdbcTemplate().query(
              "SELECT VOGroup,VORole FROM " + LINKGROUP_VO_TABLE + " WHERE linkGroupId=?",
              (ResultSet rs) -> {
                  String nextVOGroup = rs.getString(1);
                  String nextVORole = rs.getString(2);
                  VOInfo nextVO = new VOInfo(nextVOGroup, nextVORole);
                  if (!insertVOs.remove(nextVO)) {
                      deleteVOs.add(nextVO);
                  }
              }, id);

        for (VOInfo nextVo : insertVOs) {
            getJdbcTemplate().update(
                  "INSERT INTO " + LINKGROUP_VO_TABLE
                        + " ( VOGroup, VORole, linkGroupId ) VALUES ( ? , ? , ? )",
                  nextVo.getVoGroup(),
                  nextVo.getVoRole(),
                  id);
        }
        for (VOInfo nextVo : deleteVOs) {
            getJdbcTemplate().update(
                  "DELETE FROM " + LINKGROUP_VO_TABLE
                        + " WHERE VOGroup  = ? AND VORole = ? AND linkGroupId = ? ",
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
          throws DataAccessException {
        final long creationTime = System.currentTimeMillis();
        KeyHolder keyHolder = new GeneratedKeyHolder();
        int rc = getJdbcTemplate().update(
              con -> {
                  /* Note that neither prepareStatement(String, String[]) nor prepareStatement(String, int[])
                   * work for us: The former suffers from different interpretations of case in HSQLDB and
                   * PostgreSQL and the latter is not support by the PostgreSQL JDBC driver.
                   */
                  PreparedStatement stmt = con.prepareStatement(
                        "INSERT INTO " + SPACE_TABLE
                              + " (vogroup,vorole,retentionpolicy,accesslatency,linkgroupid,"
                              + "sizeinbytes,creationtime,expirationtime,description,state,usedspaceinbytes,allocatedspaceinbytes)"
                              + " VALUES  (?,?,?,?,?,?,?,?,?,?,?,?)",
                        Statement.RETURN_GENERATED_KEYS);
                  stmt.setString(1, voGroup);
                  stmt.setString(2, voRole);
                  stmt.setInt(3, retentionPolicy == null ? 0 : retentionPolicy.getId());
                  stmt.setInt(4, accessLatency == null ? 0 : accessLatency.getId());
                  stmt.setLong(5, linkGroupId);
                  stmt.setLong(6, sizeInBytes);
                  stmt.setLong(7, creationTime);
                  stmt.setObject(8, (lifetime == -1) ? null : creationTime + lifetime);
                  stmt.setString(9, description);
                  stmt.setInt(10, state.getStateId());
                  stmt.setLong(11, used);
                  stmt.setLong(12, allocated);
                  return stmt;
              },
              keyHolder);
        if (rc != 1) {
            throw new JdbcUpdateAffectedIncorrectNumberOfRowsException(
                  "insert returned row count =" + rc, 1, rc);
        }
        return new Space((Long) keyHolder.getKeys().get("id"),
              voGroup,
              voRole,
              retentionPolicy,
              accessLatency,
              linkGroupId,
              sizeInBytes,
              creationTime,
              (lifetime == -1) ? null : creationTime + lifetime,
              description,
              state,
              used,
              allocated);
    }

    @Override
    public Space getSpace(long id) throws DataAccessException {
        try {
            return getJdbcTemplate().queryForObject(
                  "SELECT * FROM " + SPACE_TABLE + " WHERE id=?", this::toSpace, id);
        } catch (EmptyResultDataAccessException e) {
            throw new EmptyResultDataAccessException("No such space reservation: " + id, 1, e);
        }
    }

    @Override
    public LinkGroup getLinkGroup(long id) throws DataAccessException {
        try {
            return getJdbcTemplate().queryForObject(
                  "SELECT * FROM " + LINKGROUP_TABLE + " WHERE  id = ?", this::toLinkGroup, id);
        } catch (EmptyResultDataAccessException e) {
            throw new EmptyResultDataAccessException("No such link group: " + id, 1, e);
        }
    }

    @Override
    public LinkGroup getLinkGroupByName(String name) throws DataAccessException {
        try {
            return getJdbcTemplate().queryForObject(
                  "SELECT * FROM " + LINKGROUP_TABLE + " WHERE  name = ?", this::toLinkGroup, name);
        } catch (EmptyResultDataAccessException e) {
            throw new EmptyResultDataAccessException("No such link group: " + name, 1, e);
        }
    }

    @Override
    public void updateFile(File f)
          throws DataAccessException {
        int rc = getJdbcTemplate().update(
              "UPDATE " + SPACEFILE_TABLE +
                    " SET vogroup=?, vorole=?, sizeinbytes=?, pnfsid=?, state=? WHERE id=?",
              f.getVoGroup(),
              f.getVoRole(),
              f.getSizeInBytes(),
              Objects.toString(f.getPnfsId(), null),
              f.getState().getStateId(),
              f.getId());
        if (rc != 1) {
            throw new JdbcUpdateAffectedIncorrectNumberOfRowsException(
                  "Update failed, row count=" + rc, 1, rc);
        }
    }

    @Override
    public File findFile(PnfsId pnfsId) throws DataAccessException {
        List<File> results = getJdbcTemplate().query(
              "SELECT * FROM " + SPACEFILE_TABLE + " WHERE pnfsId=?",
              this::toFile, pnfsId.toString());
        return DataAccessUtils.singleResult(results);
    }

    @Override
    public LinkGroupCriterion linkGroups() {
        return new LinkGroupCriterionImpl();
    }

    @Override
    public List<LinkGroup> get(LinkGroupCriterion criterion) {
        JdbcCriterion c = (JdbcCriterion) criterion;
        return getJdbcTemplate().query(
              "SELECT * from " + LINKGROUP_TABLE + " WHERE " + c.getPredicate(), c.getArguments(),
              this::toLinkGroup);
    }

    @Override
    public SpaceCriterion spaces() {
        return new SpaceCriterionImpl();
    }

    @Override
    public List<Space> get(SpaceCriterion criterion, Integer limit) {
        JdbcCriterion c = (JdbcCriterion) criterion;
        return getJdbcTemplate().query(
              "SELECT * FROM " + SPACE_TABLE + " WHERE " + c.getPredicate() + (limit != null ?
                    " LIMIT " + limit : ""),
              c.getArguments(), this::toSpace);
    }

    @Override
    public List<Long> getSpaceTokensOf(SpaceCriterion criterion) {
        JdbcCriterion c = (JdbcCriterion) criterion;
        return getJdbcTemplate().queryForList(
              "SELECT id FROM " + SPACE_TABLE + " WHERE " + c.getPredicate(), c.getArguments(),
              Long.class);
    }

    @Override
    public int count(SpaceCriterion criterion) {
        JdbcCriterion c = (JdbcCriterion) criterion;
        return getJdbcTemplate().queryForObject(
              "SELECT count(*) FROM " + SPACE_TABLE + " WHERE " + c.getPredicate(),
              c.getArguments(), Integer.class);
    }

    @Override
    public FileCriterion files() {
        return new FileCriterionImpl();
    }

    @Override
    public List<File> get(FileCriterion criterion, Integer limit) {
        JdbcCriterion c = (JdbcCriterion) criterion;
        return getJdbcTemplate().query(
              "SELECT * FROM " + SPACEFILE_TABLE + " WHERE " + c.getPredicate() + (limit != null ?
                    " LIMIT " + limit : ""),
              c.getArguments(), this::toFile);
    }

    @Override
    public int count(FileCriterion criterion) {
        JdbcCriterion c = (JdbcCriterion) criterion;
        return getJdbcTemplate().queryForObject(
              "SELECT count(*) FROM " + SPACEFILE_TABLE + " WHERE " + c.getPredicate(),
              c.getArguments(),
              Integer.class);
    }

    @Override
    public int remove(FileCriterion criterion) {
        JdbcCriterion c = (JdbcCriterion) criterion;
        return getJdbcTemplate().update(
              "DELETE FROM " + SPACEFILE_TABLE + " WHERE " + c.getPredicate(), c.getArguments());
    }

    @Override
    public int remove(SpaceCriterion criterion) {
        JdbcCriterion c = (JdbcCriterion) criterion;
        return getJdbcTemplate().update(
              "DELETE FROM " + SPACE_TABLE + " WHERE " + c.getPredicate(), c.getArguments());
    }

    @Override
    public long insertFile(final long reservationId,
          final String voGroup,
          final String voRole,
          final long sizeInBytes,
          final PnfsId pnfsId,
          final FileState state)
          throws DataAccessException, SpaceException {
        final long creationTime = System.currentTimeMillis();

        Space space = selectSpaceForUpdate(reservationId);
        long currentTime = System.currentTimeMillis();
        if (space.getExpirationTime() != null && space.getExpirationTime() <= currentTime) {
            throw new SpaceExpiredException("space with id=" + reservationId + " has expired");
        }
        if (space.getState() == SpaceState.EXPIRED) {
            throw new SpaceExpiredException("space with id=" + reservationId + " has expired");
        }
        if (space.getState() == SpaceState.RELEASED) {
            throw new SpaceReleasedException("space with id=" + reservationId + " was released");
        }
        if (space.getAvailableSpaceInBytes() < sizeInBytes) {
            throw new NoFreeSpaceException(
                  "space with id=" + reservationId + " does not have enough space");
        }

        KeyHolder keyHolder = new GeneratedKeyHolder();

        int rc = getJdbcTemplate().update(
              con -> {
                  /* Note that neither prepareStatement(String, String[]) nor prepareStatement(String, int[])
                   * work for us: The former suffers from different interpretations of case in HSQLDB and
                   * PostgreSQL and the latter is not support by the PostgreSQL JDBC driver.
                   */
                  PreparedStatement stmt = con.prepareStatement(
                        "INSERT INTO " + SPACEFILE_TABLE
                              + " (vogroup,vorole,spacereservationid,sizeinbytes,creationtime,pnfsid,state) "
                              + " VALUES  (?,?,?,?,?,?,?)", Statement.RETURN_GENERATED_KEYS);
                  stmt.setString(1, voGroup);
                  stmt.setString(2, voRole);
                  stmt.setLong(3, reservationId);
                  stmt.setLong(4, sizeInBytes);
                  stmt.setLong(5, creationTime);
                  stmt.setString(6, Objects.toString(pnfsId, null));
                  stmt.setInt(7, state.getStateId());
                  return stmt;
              }, keyHolder);
        if (rc != 1) {
            throw new JdbcUpdateAffectedIncorrectNumberOfRowsException(
                  "insert returned row count =" + rc, 1, rc);
        }
        return (Long) keyHolder.getKeys().get("id");
    }

    @Override
    public void expire(SpaceCriterion criterion) {
        JdbcCriterion c = (JdbcCriterion) criterion;
        getJdbcTemplate().update(
              "UPDATE " + SPACE_TABLE + " SET state = " + SpaceState.EXPIRED.getStateId()
                    + " WHERE " + c.getPredicate(), c.getArguments());
    }

    private static class JdbcCriterion {

        final StringBuilder predicate = new StringBuilder();
        final List<Object> arguments = new ArrayList<>();

        protected void addClause(String clause, Object... arguments) {
            if (predicate.length() > 0) {
                predicate.append(" AND ");
            }
            predicate.append(clause);
            this.arguments.addAll(asList(arguments));
        }

        protected void whereFieldMatches(String field, SqlGlob pattern) {
            if (pattern.isGlob()) {
                addClause(field + "LIKE ?", pattern.toSql());
            } else {
                addClause(field + " = ?", pattern.toString());
            }
        }

        public String getPredicate() {
            return predicate.length() == 0 ? "true" : predicate.toString();
        }

        public Object[] getArguments() {
            return arguments.toArray(Object[]::new);
        }
    }

    private static class LinkGroupCriterionImpl extends JdbcCriterion implements
          LinkGroupCriterion {

        @Override
        public LinkGroupCriterion whereUpdateTimeAfter(long latestLinkGroupUpdateTime) {
            addClause("lastupdatetime >= ?", latestLinkGroupUpdateTime);
            return this;
        }

        @Override
        public LinkGroupCriterion allowsAccessLatency(AccessLatency al) {
            if (al == AccessLatency.NEARLINE) {
                addClause("nearlineallowed=1");
            } else if (al == AccessLatency.ONLINE) {
                addClause("onlineallowed=1");
            }
            return this;
        }

        @Override
        public LinkGroupCriterion allowsRetentionPolicy(RetentionPolicy rp) {
            if (rp == RetentionPolicy.OUTPUT) {
                addClause("outputallowed=1");
            } else if (rp == RetentionPolicy.REPLICA) {
                addClause("replicaallowed=1");
            } else if (rp == RetentionPolicy.CUSTODIAL) {
                addClause("custodialallowed=1");
            }
            return this;
        }

        @Override
        public LinkGroupCriterion whereNameMatches(SqlGlob name) {
            whereFieldMatches("name", name);
            return this;
        }

        @Override
        public LinkGroupCriterion hasAvailable(long bytes) {
            addClause("availablespaceinbytes >= " + bytes);
            return this;
        }
    }

    private static class SpaceCriterionImpl extends JdbcCriterion implements SpaceCriterion {

        @Override
        public SpaceCriterion whereStateIsIn(SpaceState... states) {
            addClause(Stream.of(states)
                  .mapToInt(SpaceState::getStateId)
                  .mapToObj(String::valueOf)
                  .collect(joining(",", "state IN (", ")")));
            return this;
        }

        @Override
        public SpaceCriterion whereRetentionPolicyIs(RetentionPolicy rp) {
            addClause("retentionpolicy = ?", rp.getId());
            return this;
        }

        @Override
        public SpaceCriterion whereAccessLatencyIs(AccessLatency al) {
            addClause("accesslatency = ?", al.getId());
            return this;
        }

        @Override
        public SpaceCriterion whereDescriptionMatches(SqlGlob desc) {
            whereFieldMatches("description", desc);
            return this;
        }

        @Override
        public SpaceCriterion whereRoleMatches(SqlGlob role) {
            whereFieldMatches("vorole", role);
            return this;
        }

        @Override
        public SpaceCriterion whereGroupMatches(SqlGlob group) {
            whereFieldMatches("vogroup", group);
            return this;
        }

        @Override
        public SpaceCriterion whereTokenIs(long token) {
            addClause("id = ?", token);
            return this;
        }

        @Override
        public SpaceCriterion thatNeverExpire() {
            addClause("expirationtime IS NULL");
            return this;
        }

        @Override
        public SpaceCriterion whereLinkGroupIs(long id) {
            addClause("linkgroupid = ?", id);
            return this;
        }

        @Override
        public SpaceCriterion whereGroupIs(String group) {
            addClause("vogroup = ?", group);
            return this;
        }

        @Override
        public SpaceCriterion whereRoleIs(String role) {
            addClause("vorole = ?", role);
            return this;
        }

        @Override
        public SpaceCriterion whereDescriptionIs(String description) {
            addClause("description = ?", description);
            return this;
        }

        @Override
        public SpaceCriterion thatExpireBefore(long millis) {
            addClause("expirationtime < ?", millis);
            return this;
        }

        @Override
        public SpaceCriterion thatHaveNoFiles() {
            addClause("NOT EXISTS (SELECT * FROM " + SPACEFILE_TABLE
                  + " WHERE spacereservationid = srmspace.id)");
            return this;
        }
    }

    private static class FileCriterionImpl extends JdbcCriterion implements FileCriterion {

        @Override
        public FileCriterion whereGroupMatches(SqlGlob group) {
            whereFieldMatches("group", group);
            return this;
        }

        @Override
        public FileCriterion whereRoleMatches(SqlGlob role) {
            whereFieldMatches("role", role);
            return this;
        }

        @Override
        public FileCriterion whereSpaceTokenIs(Long token) {
            addClause("spacereservationid = ?", token);
            return this;
        }

        @Override
        public FileCriterion whereStateIsIn(FileState... states) {
            addClause(Stream.of(states)
                  .mapToInt(FileState::getStateId)
                  .mapToObj(String::valueOf)
                  .collect(joining(",", "state IN (", ")")));
            return this;
        }

        @Override
        public FileCriterion wherePnfsIdIs(PnfsId pnfsId) {
            addClause("pnfsid = ?", pnfsId.toString());
            return this;
        }

        @Override
        public FileCriterion in(SpaceCriterion spaceCriterion) {
            JdbcCriterion criterion = (JdbcCriterion) spaceCriterion;
            addClause("spacereservationid IN (SELECT id FROM " + SPACE_TABLE + " WHERE "
                  + criterion.getPredicate() + ")", criterion.getArguments());
            return this;
        }

        @Override
        public FileCriterion whereCreationTimeIsBefore(long millis) {
            addClause("creationtime < ?", millis);
            return this;
        }
    }

    private Space toSpace(ResultSet set, int rowNum) throws SQLException {
        return new Space(set.getLong("id"),
              set.getString("vogroup"),
              set.getString("vorole"),
              RetentionPolicy.getRetentionPolicy(set.getInt("retentionPolicy")),
              AccessLatency.getAccessLatency(set.getInt("accessLatency")),
              set.getLong("linkgroupid"),
              set.getLong("sizeinbytes"),
              set.getLong("creationtime"),
              toNull(set.getLong("expirationtime"), set.wasNull()),
              set.getString("description"),
              SpaceState.valueOf(set.getInt("state")),
              set.getLong("usedspaceinbytes"),
              set.getLong("allocatedspaceinbytes"));
    }

    private LinkGroup toLinkGroup(ResultSet set, int rowNum) throws SQLException {
        LinkGroup lg = new LinkGroup();
        lg.setId(set.getLong("id"));
        lg.setName(set.getString("name"));
        lg.setAvailableSpace(set.getLong("availablespaceinbytes"));
        lg.setUpdateTime(set.getLong("lastupdatetime"));
        lg.setOnlineAllowed(set.getBoolean("onlineallowed"));
        lg.setNearlineAllowed(set.getBoolean("nearlineallowed"));
        lg.setReplicaAllowed(set.getBoolean("replicaallowed"));
        lg.setOutputAllowed(set.getBoolean("outputallowed"));
        lg.setCustodialAllowed(set.getBoolean("custodialallowed"));
        lg.setReservedSpace(set.getLong("reservedspaceinbytes"));
        List<VOInfo> vos = getJdbcTemplate().query(
              "SELECT voGroup,voRole FROM " + LINKGROUP_VO_TABLE + " WHERE linkGroupId=?",
              (vo, i) -> new VOInfo(vo.getString("vogroup"), vo.getString("vorole")),
              lg.getId());
        lg.setVOs(vos.toArray(VOInfo[]::new));
        return lg;
    }

    private File toFile(ResultSet set, int rowNum) throws SQLException {
        String pnfsId = set.getString("pnfsId");
        return new File(set.getLong("id"),
              set.getString("vogroup"),
              set.getString("vorole"),
              set.getLong("spacereservationid"),
              set.getLong("sizeinbytes"),
              set.getLong("creationtime"),
              (pnfsId != null) ? new PnfsId(pnfsId) : null,
              FileState.valueOf(set.getInt("state")));
    }

    private static <T> T toNull(T value, boolean makeNull) {
        return makeNull ? null : value;
    }
}
