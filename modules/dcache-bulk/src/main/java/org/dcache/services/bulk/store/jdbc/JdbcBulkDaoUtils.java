/*
COPYRIGHT STATUS:
Dec 1st 2001, Fermi National Accelerator Laboratory (FNAL) documents and
software are sponsored by the U.S. Department of Energy under Contract No.
DE-AC02-76CH03000. Therefore, the U.S. Government retains a  world-wide
non-exclusive, royalty-free license to publish or reproduce these documents
and software for U.S. Government purposes.  All documents and software
available from this server are protected under the U.S. and Foreign
Copyright Laws, and FNAL reserves all rights.

Distribution of the software available from this server is free of
charge subject to the user following the terms of the Fermitools
Software Legal Information.

Redistribution and/or modification of the software shall be accompanied
by the Fermitools Software Legal Information  (including the copyright
notice).

The user is asked to feed back problems, benefits, and/or suggestions
about the software to the Fermilab Software Providers.

Neither the name of Fermilab, the  URA, nor the names of the contributors
may be used to endorse or promote products derived from this software
without specific prior written permission.

DISCLAIMER OF LIABILITY (BSD):

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED  WARRANTIES, INCLUDING, BUT NOT
LIMITED TO, THE IMPLIED  WARRANTIES OF MERCHANTABILITY AND FITNESS
FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL FERMILAB,
OR THE URA, OR THE U.S. DEPARTMENT of ENERGY, OR CONTRIBUTORS BE LIABLE
FOR  ANY  DIRECT, INDIRECT,  INCIDENTAL, SPECIAL, EXEMPLARY, OR
CONSEQUENTIAL DAMAGES  (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT
OF SUBSTITUTE  GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR
BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY  OF
LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT  OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE  POSSIBILITY OF SUCH DAMAGE.

Liabilities of the Government:

This software is provided by URA, independent from its Prime Contract
with the U.S. Department of Energy. URA is acting independently from
the Government and in its own private capacity and is not acting on
behalf of the U.S. Government, nor as its contractor nor its agent.
Correspondingly, it is understood and agreed that the U.S. Government
has no connection to this software and in no manner whatsoever shall
be liable for nor assume any responsibility or obligation for any claim,
cost, or damages arising out of or resulting from the use of the software
available from this server.

Export Control:

All documents and software available from this server are subject to U.S.
export control laws.  Anyone downloading information from this server is
obligated to secure any necessary Government licenses before exporting
documents or software obtained from this server.
 */
package org.dcache.services.bulk.store.jdbc;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.dcache.db.JdbcCriterion;
import org.dcache.db.JdbcUpdate;
import org.dcache.services.bulk.BulkStorageException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ParameterizedPreparedStatementSetter;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.support.JdbcDaoSupport;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.jdbc.support.rowset.SqlRowSet;

/**
 * CRUD methods in support of all bulk service JDBC Dao implementations.
 */
public final class JdbcBulkDaoUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(JdbcBulkDaoUtils.class);

    private static final String SELECT_COUNTS_BY_STATE =
          "SELECT * FROM counts_by_state";

    public static <T> Set<T> toSetOrNull(T[] array) {
        return array == null ? null : Arrays.stream(array).collect(Collectors.toSet());
    }

    private int fetchSize;

    public Object[] concatArguments(Collection<Object> first, Collection<Object> second) {
        return Stream.concat(first.stream(), second.stream()).toArray(Object[]::new);
    }

    public int count(JdbcCriterion criterion, String tableName, JdbcDaoSupport support) {
        LOGGER.trace("count {}.", criterion);
        String sql = "SELECT count(*) FROM " + tableName + " WHERE " + criterion.getPredicate();
        LOGGER.trace("count {} ({}).", sql, criterion.getArguments());
        return support.getJdbcTemplate()
              .queryForObject(sql, criterion.getArgumentsAsArray(), Integer.class);
    }

    public Map<String, Long> countsByState(JdbcDaoSupport support) {
        LOGGER.trace("countStates.");
        SqlRowSet rowSet = support.getJdbcTemplate().queryForRowSet(SELECT_COUNTS_BY_STATE);
        Map<String, Long> counts = new HashMap<>();
        while (rowSet.next()) {
            counts.put(rowSet.getString(1), rowSet.getLong(2));
        }
        return counts;
    }

    public Map<String, Long> countGrouped(JdbcCriterion criterion, String tableName,
          JdbcDaoSupport support) {
        LOGGER.trace("countGrouped {}.", criterion);
        String groupBy = criterion.groupBy();
        String sql = "SELECT " + groupBy + ", count(*) FROM " + tableName + " WHERE "
              + criterion.getPredicate() + " GROUP BY " + groupBy;
        LOGGER.trace("countGrouped {} ({}).", sql, criterion.getArguments());
        SqlRowSet rowSet = support.getJdbcTemplate()
              .queryForRowSet(sql, criterion.getArgumentsAsArray());

        Map<String, Long> counts = new HashMap<>();
        while (rowSet.next()) {
            counts.put(rowSet.getString(1), rowSet.getLong(2));
        }
        return counts;
    }

    public int delete(JdbcCriterion criterion, String tableName, JdbcDaoSupport support) {
        LOGGER.trace("delete {}.", criterion);
        String sql = "DELETE FROM " + tableName + " WHERE " + criterion.getPredicate();
        LOGGER.trace("delete {} ({}).", sql, criterion.getArguments());
        return support.getJdbcTemplate().update(sql, criterion.getArgumentsAsArray());
    }

    public int delete(JdbcCriterion criterion, String tableName, String secondaryTable,
          JdbcDaoSupport support) {
        LOGGER.trace("delete {}.", criterion);
        String sql =
              "DELETE FROM " + tableName + " WHERE EXISTS (SELECT * FROM " + secondaryTable
                    + " WHERE " + criterion.getPredicate() + ")";
        LOGGER.trace("delete {} ({}).", sql, criterion.getArguments());
        return support.getJdbcTemplate().update(sql, criterion.getArgumentsAsArray());
    }

    /**
     * @throws SQLException in order to support the jdbc template API.
     */
    public Object deserializeFromBase64(Long id, String field, String base64)
          throws SQLException {
        if (base64 == null) {
            return null;
        }
        byte[] array = Base64.getDecoder().decode(base64);
        ByteArrayInputStream bais = new ByteArrayInputStream(array);
        try (ObjectInputStream istream = new ObjectInputStream(bais)) {
            return istream.readObject();
        } catch (IOException | ClassNotFoundException e) {
            throw new SQLException("problem deserializing " + field + " for "
                  + id, e);
        }
    }

    public <T> List<T> get(String select, JdbcCriterion criterion, int limit, String tableName,
          JdbcDaoSupport support, RowMapper<T> mapper) {
        LOGGER.trace("get {}, {}, limit {}.", select, criterion, limit);
        Boolean reverse = criterion.reverse();
        String direction = reverse == null || !reverse ? "ASC" : "DESC";
        String sql = select + " FROM " + tableName + " WHERE " + criterion.getPredicate()
              + " ORDER BY " + criterion.orderBy() + " " + direction + " LIMIT " + limit;

        LOGGER.trace("get {} ({}).", sql, criterion.getArguments());
        JdbcTemplate template = support.getJdbcTemplate();
        template.setFetchSize(fetchSize);

        return template.query(sql, criterion.getArgumentsAsArray(), mapper);
    }

    public <T> List<T> get(JdbcCriterion criterion, int limit, String tableName,
          JdbcDaoSupport support, RowMapper<T> mapper) {
        return get("SELECT *", criterion, limit, tableName, support, mapper);
    }

    public <T> List<T> get(String sql, List args, int limit, JdbcDaoSupport support,
          RowMapper<T> mapper) {
        LOGGER.trace("get {}, {}, limit {}.", sql, args, limit);
        JdbcTemplate template = support.getJdbcTemplate();
        template.setFetchSize(fetchSize);
        return template.query(sql + " LIMIT " + limit, args.toArray(Object[]::new), mapper);
    }

    public Optional<KeyHolder> insert(JdbcUpdate update, String tableName, JdbcDaoSupport support) {
        LOGGER.trace("insert {}.", update);
        String sql = "INSERT INTO " + tableName + " " + update.getInsert();
        return insert(sql, update.getArguments(), support);
    }

    public <T> void insertBatch(List<T> targets, String sql,
          ParameterizedPreparedStatementSetter<T> setter, JdbcDaoSupport support) {
        support.getJdbcTemplate().batchUpdate(sql, targets, 100, setter);
    }

    public String serializeToBase64(String field, Serializable serializable)
          throws BulkStorageException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ObjectOutputStream ostream = new ObjectOutputStream(baos)) {
            ostream.writeObject(serializable);
        } catch (IOException e) {
            throw new BulkStorageException("problem serializing "
                  + field, e);
        }
        return Base64.getEncoder().encodeToString(baos.toByteArray());
    }

    @Required
    public void setFetchSize(int fetchSize) {
        this.fetchSize = fetchSize;
    }

    public int update(JdbcCriterion criterion, JdbcUpdate update, String tableName,
          JdbcDaoSupport support) {
        LOGGER.trace("update {} : {}.", criterion, update);
        String sql = "UPDATE " + tableName + " SET " + update.getUpdate() + " WHERE "
              + criterion.getPredicate();
        LOGGER.trace("update {} ({}, {}).", sql, update.getArguments(),
              criterion.getArguments());
        return support.getJdbcTemplate().update(sql,
              concatArguments(update.getArguments(), criterion.getArguments()));
    }

    public int update(JdbcCriterion criterion, JdbcUpdate update, String tableName,
          String secondaryTable, JdbcDaoSupport support) {
        LOGGER.trace("update {} : {}.", criterion, update);
        String sql =
              "UPDATE " + tableName + " SET " + update.getUpdate() + " FROM " + secondaryTable
                    + " WHERE " + criterion.getPredicate();
        LOGGER.trace("update {} ({}, {}).", sql, update.getArguments(),
              criterion.getArguments());
        return support.getJdbcTemplate().update(sql,
              concatArguments(update.getArguments(), criterion.getArguments()));
    }

    public Optional<KeyHolder> insert(String sql, Collection<Object> arguments,
          JdbcDaoSupport support) {
        KeyHolder keyHolder = new GeneratedKeyHolder();

        try {
            support.getJdbcTemplate().update(
                  con -> {
                      PreparedStatement ps = con.prepareStatement(sql,
                            Statement.RETURN_GENERATED_KEYS);

                      int i = 1;
                      for (Object argument : arguments) {
                          ps.setObject(i++, argument);
                      }

                      LOGGER.trace("insert {}.", ps);
                      return ps;
                  }, keyHolder);
        } catch (DuplicateKeyException e) {
            LOGGER.trace("insert {}, {}.", arguments, e.toString());
            return Optional.empty();
        }

        LOGGER.trace("insert {}, succeeded.", arguments);
        return Optional.of(keyHolder);
    }
}
