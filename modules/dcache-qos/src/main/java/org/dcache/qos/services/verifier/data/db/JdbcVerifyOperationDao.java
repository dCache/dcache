/*
 * dCache - http://www.dcache.org/
 *
 * Copyright (C) 2016 - 2020 Deutsches Elektronen-Synchrotron
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.dcache.qos.services.verifier.data.db;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsId;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import javax.security.auth.Subject;
import org.dcache.db.JdbcCriterion;
import org.dcache.db.JdbcUpdate;
import org.dcache.qos.QoSException;
import org.dcache.qos.data.QoSAction;
import org.dcache.qos.data.QoSMessageType;
import org.dcache.qos.services.verifier.data.VerifyOperation;
import org.dcache.qos.services.verifier.data.VerifyOperationState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.dao.support.DataAccessUtils;
import org.springframework.jdbc.JdbcUpdateAffectedIncorrectNumberOfRowsException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.support.JdbcDaoSupport;
import org.springframework.jdbc.support.rowset.SqlRowSet;

/**
 * Provides the Jdbc/Sql implementation of the operation dao interface.
 */
@ParametersAreNonnullByDefault
public class JdbcVerifyOperationDao extends JdbcDaoSupport implements VerifyOperationDao {

    private static final Logger LOGGER = LoggerFactory.getLogger(JdbcVerifyOperationDao.class);

    /**
     * Based on the ResultSet returned by the query, construct a VerifyOperation object.
     *
     * @param rs  from the query.
     * @param row unused, but needs to be there to satisfy the template function signature.
     * @return operation object
     * @throws SQLException if access to the ResultSet fails.
     */
    private static VerifyOperation toOperation(ResultSet rs, int row) throws SQLException {
        VerifyOperation operation
              = new VerifyOperation(new PnfsId(rs.getString("pnfsid")));
        operation.setArrived(rs.getLong("arrived"));
        operation.setLastUpdate(rs.getLong("updated"));
        operation.setMessageType(QoSMessageType.valueOf(rs.getString("msg_type")));
        operation.setPoolGroup(rs.getString("pool_group"));
        operation.setStorageUnit(rs.getString("storage_unit"));
        operation.setState(VerifyOperationState.valueOf(rs.getString("state")));

        String value = rs.getString("action");
        if (value != null) {
            operation.setAction(QoSAction.valueOf(value));
        }

        value = rs.getString("prev_action");
        if (value != null) {
            operation.setPreviousAction(QoSAction.valueOf(value));
        }

        operation.setNeeded(rs.getInt("needed"));
        operation.setRetried(rs.getInt("retried"));
        operation.setParent(rs.getString("parent"));
        operation.setSource(rs.getString("source"));
        operation.setTarget(rs.getString("target"));

        String tried = rs.getString("tried");
        if (tried != null) {
            operation.setTried(Arrays.stream(tried.split(","))
                  .map(String::trim)
                  .collect(Collectors.toSet()));
        }

        String error = rs.getString("error");
        if (error != null) {
            operation.setException(new CacheException(rs.getInt("rc"), error));
        }

        operation.setSubject(Subject.class.cast(deserialize(rs.getString("subject"))));

        LOGGER.debug("toOperation, returning {}.", operation);
        return operation;
    }

    private static String serialize(Subject subject) throws QoSException {
        if (subject == null) {
            return null;
        }
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ObjectOutputStream ostream = new ObjectOutputStream(baos)) {
            ostream.writeObject(subject);
        } catch (IOException e) {
            throw new QoSException("problem serializing subject", e);
        }
        return Base64.getEncoder().encodeToString(baos.toByteArray());
    }

    private static Object deserialize(String base64) throws SQLException {
        if (base64 == null) {
            return null;
        }
        byte[] array = Base64.getDecoder().decode(base64);
        ByteArrayInputStream bais = new ByteArrayInputStream(array);
        try (ObjectInputStream istream = new ObjectInputStream(bais)) {
            return istream.readObject();
        } catch (IOException | ClassNotFoundException e) {
            throw new SQLException("problem deserializing subject", e);
        }
    }

    private static Object[] concatArguments(Collection<Object> first, Collection<Object> second) {
        return Stream.concat(first.stream(), second.stream()).toArray(Object[]::new);
    }

    private Integer fetchSize;

    @Required
    public void setFetchSize(Integer fetchSize) {
        this.fetchSize = fetchSize;
    }

    @Override
    public VerifyOperationCriterion where() {
        return new JdbcOperationCriterion();
    }

    @Override
    public UniqueOperationCriterion whereUnique() {
        return new UniqueJdbcOperationCriterion();
    }

    @Override
    public VerifyOperationUpdate set() {
        return new JdbcOperationUpdate();
    }

    @Override
    public VerifyOperationUpdate fromOperation(VerifyOperation operation) throws QoSException {
        return set().exception(operation.getException())
              .retried(operation.getRetried())
              .tried(operation.getTried())
              .lastUpdate(operation.getLastUpdate())
              .state(operation.getState())
              .action(operation.getAction())
              .previous(operation.getPreviousAction())
              .needed(operation.getNeededAdjustments())
              .source(operation.getSource())
              .target(operation.getTarget())
              .subject(serialize(operation.getSubject()));
    }

    /**
     * Since inactive operations are not immediately purged, it is now possible to encounter
     * pre-existing pnsfids in the table (unlike the in-memory map version used to implement
     * Resilience).
     * <p/>
     * If there is already an operation for the pnfsid, it is updated (the duplicate key exception
     * is caught).
     *
     * @param operation passed in from the receiver.
     * @return true if stored, false if just updated.
     */
    @Override
    public boolean store(VerifyOperation operation) throws QoSException {
        PnfsId pnfsId = operation.getPnfsId();
        String storageUnit = operation.getStorageUnit();
        VerifyOperationUpdate insert = set().pnfsid(pnfsId)
              .arrivalTime(operation.getArrived())
              .lastUpdate(operation.getLastUpdate())
              .poolGroup(operation.getPoolGroup())
              .storageUnit(storageUnit)
              .messageType(operation.getMessageType())
              .retried(operation.getRetried())
              .needed(operation.getNeededAdjustments())
              .parent(operation.getParent())
              .source(operation.getSource())
              .state(operation.getState())
              .subject(serialize(operation.getSubject()));

        LOGGER.debug("store operation for {}.", operation.getPnfsId());

        /*
         * Try to store it.  If it is already present, return false.
         */
        return create(insert);
    }

    @Override
    public List<VerifyOperation> get(VerifyOperationCriterion criterion, int limit) {
        LOGGER.trace("get {}, limit {}.", criterion, limit);

        JdbcCriterion c = (JdbcCriterion) criterion;

        Boolean reverse = c.reverse();
        String direction = reverse == null || !reverse ? "ASC" : "DESC";

        String sql = "SELECT * FROM qos_operation WHERE " + c.getPredicate()
              + " ORDER BY " + c.orderBy() + " " + direction + " LIMIT " + limit;

        LOGGER.trace("get {} ({}).", sql, c.getArguments());

        int maxfetch = Math.max(limit, fetchSize);

        JdbcTemplate template = getJdbcTemplate();
        template.setFetchSize(maxfetch);

        return template.query(sql, c.getArgumentsAsArray(), JdbcVerifyOperationDao::toOperation);
    }

    @Nullable
    @Override
    public VerifyOperation get(UniqueOperationCriterion criterion) {
        LOGGER.trace("get {}.", criterion);

        JdbcCriterion c = (JdbcCriterion) criterion;

        String sql = "SELECT * FROM qos_operation WHERE " + c.getPredicate();

        LOGGER.trace("get {} ({}).", sql, c.getArguments());

        return DataAccessUtils.singleResult(getJdbcTemplate()
              .query(sql, c.getArgumentsAsArray(), JdbcVerifyOperationDao::toOperation));
    }

    @Override
    public Map<String, Long> counts(VerifyOperationCriterion criterion) {
        LOGGER.trace("counts {}.", criterion);

        JdbcCriterion c = (JdbcCriterion) criterion;
        String groupBy = c.groupBy();

        String sql = "SELECT " + groupBy + ", count(*) FROM qos_operation WHERE "
              + c.getPredicate() + " GROUP BY " + groupBy;

        LOGGER.trace("counts {} ({}).", sql, c.getArguments());

        SqlRowSet rowSet = getJdbcTemplate().queryForRowSet(sql, c.getArgumentsAsArray());

        Map<String, Long> counts = new HashMap<>();

        while (rowSet.next()) {
            counts.put(rowSet.getString(1), rowSet.getLong(2));
        }

        return counts;
    }

    @Override
    public int count(VerifyOperationCriterion criterion) {
        LOGGER.trace("count {}.", criterion);

        JdbcCriterion c = (JdbcCriterion) criterion;

        String sql = "SELECT count(*) FROM qos_operation WHERE " + c.getPredicate();

        LOGGER.trace("count {} ({}).", sql, c.getArguments());

        return getJdbcTemplate().queryForObject(sql, c.getArgumentsAsArray(), Integer.class);
    }

    @Nullable
    @Override
    public int update(UniqueOperationCriterion criterion, VerifyOperationUpdate update) {
        LOGGER.trace("update {} : {}.", criterion, update);

        JdbcCriterion c = (JdbcCriterion) criterion;
        JdbcUpdate u = (JdbcUpdate) update;

        String sql = "UPDATE qos_operation SET " + u.getUpdate() + " WHERE " + c.getPredicate();

        LOGGER.trace("update {} ({}, {}).", sql, u.getArguments(), c.getArguments());

        int n = getJdbcTemplate()
              .update(sql, concatArguments(u.getArguments(), c.getArguments()));

        if (n > 1) {
            throw new JdbcUpdateAffectedIncorrectNumberOfRowsException(sql, 1, n);
        }

        return n;
    }

    @Override
    public int update(VerifyOperationCriterion criterion, VerifyOperationUpdate update) {
        LOGGER.trace("update {} : {}.", criterion, update);

        JdbcCriterion c = (JdbcCriterion) criterion;
        JdbcUpdate u = (JdbcUpdate) update;

        String sql = "UPDATE qos_operation SET " + u.getUpdate() + " WHERE " + c.getPredicate();

        LOGGER.trace("update {} ({}, {}).", sql, u.getArguments(), c.getArguments());

        return getJdbcTemplate().update(sql, concatArguments(u.getArguments(), c.getArguments()));
    }


    @Override
    public int delete(VerifyOperationCriterion criterion) {
        LOGGER.trace("delete {}.", criterion);

        JdbcCriterion c = (JdbcCriterion) criterion;

        String sql = "DELETE FROM qos_operation WHERE " + c.getPredicate();

        LOGGER.trace("delete {} ({}).", sql, c.getArguments());

        return getJdbcTemplate().update(sql, c.getArgumentsAsArray());
    }

    /**
     * @return true is INSERT succeeds, false if the pnfsid already exists in the table.
     */
    private boolean create(VerifyOperationUpdate update) {
        LOGGER.trace("create {}.", update);

        JdbcUpdate u = (JdbcUpdate) update;

        String sql = "INSERT INTO qos_operation " + u.getInsert();

        try {
            getJdbcTemplate().update(
                  con -> {
                      PreparedStatement ps = con.prepareStatement(sql, Statement.NO_GENERATED_KEYS);
                      Collection<Object> arguments = u.getArguments();

                      int i = 1;
                      for (Object argument : arguments) {
                          ps.setObject(i++, argument);
                      }

                      LOGGER.trace("create {}.", ps);
                      return ps;
                  });
        } catch (DuplicateKeyException e) {
            LOGGER.trace("create {}, {}.", update, e.toString());
            return false;
        }

        LOGGER.trace("create {}, succeeded.", update);
        return true;
    }
}
