/*
 * dCache - http://www.dcache.org/
 *
 * Copyright (C) 2016 Deutsches Elektronen-Synchrotron
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
package org.dcache.pinmanager;

import org.springframework.dao.support.DataAccessUtils;
import org.springframework.jdbc.JdbcUpdateAffectedIncorrectNumberOfRowsException;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.support.JdbcDaoSupport;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;

import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import javax.security.auth.Subject;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import diskCacheV111.util.PnfsId;

import org.dcache.auth.Subjects;
import org.dcache.pinmanager.model.Pin;
import org.dcache.util.SqlGlob;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.joining;

@ParametersAreNonnullByDefault
public class JdbcDao extends JdbcDaoSupport implements PinDao
{
    @Override
    public PinCriterion where()
    {
        return new JdbcPinCriterion();
    }

    @Override
    public PinDao.PinUpdate set()
    {
        return new JdbcPinUpdate();
    }

    @Override
    public List<Pin> get(PinCriterion criterion)
    {
        JdbcCriterion c = (JdbcCriterion) criterion;
        return getJdbcTemplate().query(
                "SELECT * FROM pins WHERE " + c.getPredicate(),
                c.getArgumentsAsArray(), (RowMapper<Pin>) this::toPin);
    }

    @Override
    public List<Pin> get(PinCriterion criterion, int limit)
    {
        JdbcCriterion c = (JdbcCriterion) criterion;
        return getJdbcTemplate().query(
                "SELECT * FROM pins WHERE " + c.getPredicate() + " LIMIT " + limit,
                c.getArgumentsAsArray(), (RowMapper<Pin>) this::toPin);
    }

    @Override
    public Pin get(UniquePinCriterion criterion)
    {
        JdbcCriterion c = (JdbcCriterion) criterion;
        return DataAccessUtils.singleResult(
                getJdbcTemplate().query(
                        "SELECT * FROM pins WHERE " + c.getPredicate(),
                        c.getArgumentsAsArray(),
                        (RowMapper<Pin>) this::toPin));
    }

    @Override
    public int count(PinCriterion criterion)
    {
        JdbcCriterion c = (JdbcCriterion) criterion;
        return getJdbcTemplate().queryForObject(
                "SELECT count(*) FROM pins WHERE " + c.getPredicate(), c.getArgumentsAsArray(),
                Integer.class);

    }

    @Override
    public int update(PinCriterion criterion, PinUpdate update)
    {
        JdbcCriterion c = (JdbcCriterion) criterion;
        JdbcUpdate u = (JdbcUpdate) update;
        Object[] arguments = Stream.concat(u.getArguments().stream(), c.getArguments().stream()).toArray(Object[]::new);
        return getJdbcTemplate().update("UPDATE pins SET " + u.getUpdate() + " WHERE " + c.getPredicate(), arguments);
    }

    @Override
    public Pin update(UniquePinCriterion criterion, PinDao.PinUpdate update)
    {
        JdbcCriterion c = (JdbcCriterion) criterion;
        JdbcUpdate u = (JdbcUpdate) update;
        Object[] arguments = Stream.concat(u.getArguments().stream(), c.getArguments().stream()).toArray(Object[]::new);

        String sql = "UPDATE pins SET " + u.getUpdate() + " WHERE " + c.getPredicate();
        int n = getJdbcTemplate().update(sql, arguments);
        if (n == 0) {
            return null;
        }
        if (n > 1) {
            throw new JdbcUpdateAffectedIncorrectNumberOfRowsException(sql, 1, n);
        }
        return get(where().sameIdAs(criterion));

    }

    @Override
    public int delete(PinCriterion criterion)
    {
        JdbcCriterion c = (JdbcCriterion) criterion;
        return getJdbcTemplate().update("DELETE FROM pins WHERE " + c.getPredicate(), c.getArgumentsAsArray());
    }

    @Override
    public void foreach(PinCriterion criterion, InterruptibleConsumer<Pin> f)
            throws InterruptedException
    {
        JdbcCriterion c = (JdbcCriterion) criterion;
        InterruptedException exception =
                getJdbcTemplate().query(
                        "SELECT * FROM pins WHERE " + c.getPredicate(), c.getArgumentsAsArray(),
                        rs -> {
                            try {
                                while (rs.next()) {
                                    f.accept(toPin(rs));
                                }
                            } catch (InterruptedException e) {
                                return e;
                            }
                            return null;
                        });
        if (exception != null) {
            throw exception;
        }
    }

    private static class JdbcCriterion
    {
        final StringBuilder predicate = new StringBuilder();
        final List<Object> arguments = new ArrayList<>();

        protected void addClause(String clause, Object... arguments)
        {
            if (predicate.length() > 0) {
                predicate.append(" AND ");
            }
            predicate.append(clause);
            this.arguments.addAll(asList(arguments));
        }

        protected void whereFieldMatches(String field, SqlGlob pattern)
        {
            if (pattern.isGlob()) {
                addClause(field + "LIKE ?", pattern.toSql());
            } else {
                addClause(field + " = ?", pattern.toString());
            }
        }

        public String getPredicate()
        {
            return predicate.length() == 0 ? "true" : predicate.toString();
        }

        public List<Object> getArguments()
        {
            return arguments;
        }

        public Object[] getArgumentsAsArray()
        {
            return arguments.toArray(new Object[arguments.size()]);
        }
    }

    private static class JdbcPinCriterion extends JdbcCriterion implements UniquePinCriterion
    {
        private Long id;
        private PnfsId pnfsId;
        private String requestId;

        @Override
        public JdbcPinCriterion id(long id)
        {
            addClause("id = ?", id);
            this.id = id;
            return this;
        }

        @Override
        public JdbcPinCriterion pnfsId(PnfsId id)
        {
            addClause("pnfsid = ?", id.toString());
            this.pnfsId = id;
            return this;
        }

        @Override
        public JdbcPinCriterion requestId(String requestId)
        {
            addClause("request_id = ?", requestId);
            this.requestId = requestId;
            return this;
        }

        @Override
        public JdbcPinCriterion expirationTimeBefore(Date date)
        {
            addClause("expires_at < ?", new Timestamp(date.getTime()));
            return this;
        }

        @Override
        public JdbcPinCriterion state(Pin.State state)
        {
            addClause("state = ?", state.toString());
            return this;
        }

        @Override
        public JdbcPinCriterion stateIsNot(Pin.State state)
        {
            addClause("state <> ?", state.toString());
            return this;
        }

        @Override
        public JdbcPinCriterion pool(String pool)
        {
            addClause("pool = ?", pool);
            return this;
        }

        @Override
        public JdbcPinCriterion sticky(String sticky)
        {
            addClause("sticky = ?", sticky);
            return this;
        }

        @Override
        public JdbcPinCriterion sameIdAs(UniquePinCriterion criterion)
        {
            JdbcPinCriterion c = (JdbcPinCriterion) criterion;
            if (c.id != null) {
                return id(c.id);
            } else {
                return pnfsId(c.pnfsId).requestId(c.requestId);
            }
        }
    }

    private static class JdbcUpdate
    {
        final Map<String,Object> updates = new LinkedHashMap<>();

        protected void set(String column, @Nullable Object value)
        {
            updates.put(column, value);
        }

        public Map<String,Object> updates()
        {
            return updates;
        }

        public Collection<Object> getArguments()
        {
            return updates.values();
        }

        public String getUpdate()
        {
            return updates.keySet().stream().map(s -> s + " = ?").collect(joining(","));
        }

        public String getInsert()
        {
            return updates.keySet().stream().collect(joining(",", "(", ")")) + " VALUES "
                   + updates.keySet().stream().map(a -> "?").collect(joining(",", "(", ")"));
        }

        public Object get(String column)
        {
            return updates.get(column);
        }
    }

    private static class JdbcPinUpdate extends JdbcUpdate implements PinUpdate
    {
        @Override
        public PinUpdate expirationTime(@Nullable Date expirationTime)
        {
            set("expires_at", (expirationTime == null) ? null : new Timestamp(expirationTime.getTime()));
            return this;
        }

        @Override
        public PinUpdate pool(@Nullable String pool)
        {
            set("pool", pool);
            return this;
        }

        @Override
        public PinUpdate requestId(@Nullable String requestId)
        {
            set("request_id", requestId);
            return this;
        }

        @Override
        public PinUpdate state(Pin.State state)
        {
            set("state" , state.toString());
            return this;
        }

        @Override
        public PinUpdate sticky(@Nullable String sticky)
        {
            set("sticky", sticky);
            return this;
        }

        @Override
        public PinUpdate subject(Subject subject)
        {
            set("uid", Subjects.getUid(subject));
            set("gid", Subjects.getPrimaryGid(subject));
            return this;
        }

        @Override
        public PinUpdate pnfsId(PnfsId pnfsId)
        {
            set("pnfsid", pnfsId.toString());
            return this;
        }
    }

    @Override
    public Pin create(PinUpdate update)
    {
        JdbcUpdate u = (JdbcUpdate) update;
        KeyHolder keyHolder = new GeneratedKeyHolder();
        u.set("created_at", new Timestamp(System.currentTimeMillis()));
        getJdbcTemplate().update(
                con -> {
                    PreparedStatement ps = con.prepareStatement(
                            "INSERT INTO pins " + u.getInsert(), Statement.RETURN_GENERATED_KEYS);
                    Collection<Object> arguments = u.getArguments();
                    int i = 1;
                    for (Object argument : arguments) {
                        ps.setObject(i++, argument);
                    }
                    return ps;
                }, keyHolder);
        u.set("id", keyHolder.getKeys().get("id"));
        return toPin(u);
    }

    private Pin toPin(JdbcUpdate update)
    {
        Timestamp createdAt = (Timestamp) update.get("created_at");
        Timestamp expiresAt = (Timestamp) update.get("expires_at");
        return new Pin((long) update.get("id"),
                       new PnfsId((String) update.get("pnfsid")),
                       (String) update.get("request_id"),
                       new Date(createdAt.getTime()),
                       (expiresAt == null) ? null : new Date(expiresAt.getTime()),
                       (long) update.get("uid"),
                       (long) update.get("gid"),
                       Pin.State.valueOf((String) update.get("state")),
                       (String) update.get("pool"),
                       (String) update.get("sticky"));
    }

    private Pin toPin(ResultSet rs, int rownum) throws SQLException
    {
        return toPin(rs);
    }

    private Pin toPin(ResultSet rs) throws SQLException
    {
        Timestamp createdAt = rs.getTimestamp("created_at");
        Timestamp expiresAt = rs.getTimestamp("expires_at");
        return new Pin(rs.getLong("id"),
                       new PnfsId(rs.getString("pnfsid")),
                       rs.getString("request_id"),
                       new Date(createdAt.getTime()),
                       (expiresAt == null) ? null : new Date(expiresAt.getTime()),
                       rs.getLong("uid"),
                       rs.getLong("gid"),
                       Pin.State.valueOf(rs.getString("state")),
                       rs.getString("pool"),
                       rs.getString("sticky"));
    }
}
