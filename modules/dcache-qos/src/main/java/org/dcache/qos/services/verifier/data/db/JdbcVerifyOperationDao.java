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

import static org.dcache.qos.services.verifier.data.VerifyOperationState.READY;

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
import java.util.Base64;
import java.util.Collection;
import java.util.List;
import javax.annotation.ParametersAreNonnullByDefault;
import javax.security.auth.Subject;
import org.dcache.db.JdbcCriterion;
import org.dcache.db.JdbcUpdate;
import org.dcache.qos.QoSException;
import org.dcache.qos.data.QoSMessageType;
import org.dcache.qos.services.verifier.data.VerifyOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ParameterizedPreparedStatementSetter;
import org.springframework.jdbc.core.support.JdbcDaoSupport;

/**
 * Provides the Jdbc/Sql implementation of the operation dao interface.
 */
@ParametersAreNonnullByDefault
public class JdbcVerifyOperationDao extends JdbcDaoSupport implements VerifyOperationDao {

    private static final Logger LOGGER = LoggerFactory.getLogger(
          JdbcVerifyOperationDao.class);

    private static final String BATCH_DELETE = "DELETE FROM qos_operation WHERE pnfsid = ?";

    private static final ParameterizedPreparedStatementSetter<PnfsId> SETTER =
          (ps, target) -> ps.setString(1, target.toString());

    /**
     * Based on the ResultSet returned by the query, construct a VerifyOperation object.
     * This is used only on reload.
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
        operation.setLastUpdate(System.currentTimeMillis());
        operation.setMessageType(QoSMessageType.valueOf(rs.getString("msg_type")));
        operation.setPoolGroup(rs.getString("pool_group"));
        operation.setStorageUnit(rs.getString("storage_unit"));
        operation.setParent(rs.getString("parent"));
        operation.setSource(rs.getString("source"));
        operation.setTarget(rs.getString("target"));
        operation.setRetried(0);
        operation.setNeeded(0);
        operation.setState(READY);
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

    private Integer fetchSize;

    @Required
    public void setFetchSize(Integer fetchSize) {
        this.fetchSize = fetchSize;
    }

    @Override
    public int delete(VerifyOperationCriterion criterion) {
        if (criterion.isEmpty()) {
            LOGGER.error("delete not permitted using an empty criterion.");
            return 0;
        }

        LOGGER.trace("delete {}.", criterion);

        JdbcCriterion c = (JdbcCriterion) criterion;

        String sql = "DELETE FROM qos_operation WHERE " + c.getPredicate();

        LOGGER.trace("delete {} ({}).", sql, c.getArguments());

        return getJdbcTemplate().update(sql, c.getArgumentsAsArray());
    }

    public void deleteBatch(List<PnfsId> targets, int batchSize) {
        getJdbcTemplate().batchUpdate(BATCH_DELETE, targets, batchSize, SETTER);
    }

    @Override
    public List<VerifyOperation> load() throws QoSException {
        JdbcCriterion c = (JdbcCriterion) where().sorter("arrived");
        String sql = "SELECT * FROM qos_operation WHERE " + c.getPredicate()
              + " ORDER BY " + c.orderBy() + " ASC";

        LOGGER.trace("get {} ({}).", sql, c.getArguments());

        JdbcTemplate template = getJdbcTemplate();
        template.setFetchSize(fetchSize);

        return template.query(sql, c.getArgumentsAsArray(), JdbcVerifyOperationDao::toOperation);
    }

    @Override
    public VerifyOperationUpdate set() {
        return new JdbcOperationUpdate();
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
              .poolGroup(operation.getPoolGroup())
              .storageUnit(storageUnit)
              .messageType(operation.getMessageType())
              .parent(operation.getParent())
              .source(operation.getSource())
              .subject(serialize(operation.getSubject()));

        LOGGER.debug("store operation for {}.", operation.getPnfsId());

        /*
         * Try to store it.  If it is already present, return false.
         */
        return create(insert);
    }

    @Override
    public VerifyOperationCriterion where() {
        return new JdbcOperationCriterion();
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
