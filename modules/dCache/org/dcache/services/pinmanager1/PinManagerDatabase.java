package org.dcache.services.pinmanager1;

import java.util.Collection;
import java.util.Set;
import java.util.HashSet;
import java.util.Map;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;
import java.sql.SQLException;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.DatabaseMetaData;
import java.sql.Types;

import javax.sql.DataSource;
import diskCacheV111.util.PnfsId;
import org.dcache.auth.AuthorizationRecord;
import org.dcache.auth.persistence.AuthRecordPersistenceManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.PreparedStatementSetter;
import org.springframework.beans.factory.annotation.Required;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.TransactionException;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.DefaultTransactionDefinition;

class PinManagerDatabase
{
    private static final Logger _logger =
        LoggerFactory.getLogger(PinManagerDatabase.class);
    // keep the names spelled in the lower case to make postgress
    // driver to work correctly
    private static final String PinManagerPinsTableName="pinsv3";
    private static final String PinManagerRequestsTableName = "pinrequestsv3";
    private static final String PinManagerNextRequestIdTableName = "nextpinrequestid";

    private static final long NEXT_LONG_STEP = 1000;

        // Expiration is of type long,
        // its value is time in milliseconds since the midnight of 1970 GMT (i think)
        // which has the same meaning as the value returned by
        // System.currentTimeMillis()
        // working with TIMESTAMP and with
        // java.sql.Date and java.sql.Time proved to be upredicatble and
        // too complex to work with.

    private static final String CreatePinManagerPinsTable =
        "CREATE TABLE " + PinManagerPinsTableName + " ( " +
        " Id numeric PRIMARY KEY," +
        " PnfsId VARCHAR," +
        " Creation numeric, " +
        " Expiration numeric, " +
        " Pool VARCHAR, " +
        " StateTranstionTime numeric, "+
        " State numeric" +
        ")";

    private static final String CreatePinManagerRequestsTable =
        "CREATE TABLE " + PinManagerRequestsTableName + " ( " +
        " Id numeric PRIMARY KEY," +
        " PinId numeric," +
        " SRMId numeric, "+
        " Creation numeric, " +
        " Expiration numeric ," +
        " AuthRecId numeric ," +
        " CONSTRAINT fk_"+PinManagerRequestsTableName+
        "_L FOREIGN KEY (PinId) REFERENCES "+
        PinManagerPinsTableName +" (Id) "+
        " ON DELETE RESTRICT"+
        ")";
    private static final String CreateNextPinRequestIdTable =
	"CREATE TABLE " + PinManagerNextRequestIdTableName + "(NEXTLONG BIGINT)";
    private static final String insertNextPinRequestId =
        "INSERT INTO " + PinManagerNextRequestIdTableName + " VALUES (0)";

    private static final String SelectNextPinRequestId =
        "SELECT NEXTLONG FROM " + PinManagerNextRequestIdTableName;

    /**
     * In the begining we examine the whole database to see is there
     * is a list of outstanding pins which need to be expired or timed
     * for experation.
     */
    private static final String SelectNextPinRequestIdForUpdate =
        "SELECT NEXTLONG FROM " + PinManagerNextRequestIdTableName + " FOR UPDATE";
    private static final String IncreasePinRequestId =
        "UPDATE " + PinManagerNextRequestIdTableName +
        " SET NEXTLONG=NEXTLONG+" + NEXT_LONG_STEP;

    private static final String InsertIntoPinsTable =
        "INSERT INTO " + PinManagerPinsTableName
        + " (Id, PnfsId, Creation,Expiration,Pool, StateTranstionTime, State ) VALUES (?,?,?,?,?,?,?)";

    private static final String InsertIntoPinRequestsTable =
        "INSERT INTO " + PinManagerRequestsTableName
        + " (Id, SRMId, PinId,Creation, Expiration, AuthRecId ) VALUES (?,?,?,?,?,?)";

    private static final String deletePin =
                "DELETE FROM "+ PinManagerPinsTableName +
                " WHERE  id =?";
    private static final String deletePinRequest =
                "DELETE FROM "+ PinManagerRequestsTableName +
                " WHERE  id =?";

    private static final String AddAuthRecIdToPinRequestsTable =
        "ALTER TABLE " + PinManagerRequestsTableName
        + " ADD COLUMN AuthRecId numeric";

    private long _nextLongOffset = NEXT_LONG_STEP;
    private long _nextLongBase = 0;

    private AuthRecordPersistenceManager authRecordPM;
    private JdbcTemplate _template;
    private PlatformTransactionManager _txManager;

    private final ThreadLocal<TransactionStatus> _transactionStatus =
        new ThreadLocal<TransactionStatus>();

    @Required
    public void setTransactionManager(PlatformTransactionManager txManager)
    {
        _txManager = txManager;
    }

    @Required
    public void setDataSource(DataSource dataSource)
    {
        _template = new JdbcTemplate(dataSource);
    }

    @Required
    public void setAuthRecordPersistenceManager(AuthRecordPersistenceManager pm)
    {
        authRecordPM = pm;
    }

    private synchronized long nextLong()
    {
        if (_nextLongOffset >= NEXT_LONG_STEP) {
            long base =
                _template.queryForLong(SelectNextPinRequestIdForUpdate);
            _template.update(IncreasePinRequestId);
            _nextLongBase = base;
            _nextLongOffset = 0;
        }
        return _nextLongBase + (_nextLongOffset++);
    }

    private void insertPin(final long id,
                           final PnfsId pnfsId,
                           final long expirationTime,
                           final String pool,
                           final PinManagerPinState state)
    {
        final long creationTime = System.currentTimeMillis();
        _template.update(InsertIntoPinsTable,
                         new PreparedStatementSetter() {
                             public void setValues(PreparedStatement ps)
                                 throws SQLException
                             {
                                 ps.setLong(1, id);
                                 ps.setString(2, pnfsId.toIdString());
                                 ps.setLong(3, creationTime);
                                 ps.setLong(4, expirationTime);
                                 ps.setString(5, pool);
                                 ps.setLong(6, creationTime);
                                 ps.setInt(7, state.getStateId());
                             }
                         });
    }

    public void deletePin(long id) throws PinDBException
    {
        try {
            int count = _template.update(deletePin, id);
            if (count != 1) {
                throw new PinDBException("delete returned row count " + count);
            }
        } catch (DataAccessException e) {
            throw new PinDBException(e.toString());
        }
    }

    public void deletePinRequest(long id) throws PinDBException
    {
        try {
            int count = _template.update(deletePinRequest, id);
            if (count !=1 ){
                throw new PinDBException("delete returned row count " + count);
            }
        } catch (DataAccessException e) {
            throw new PinDBException(e.toString());
        }
    }

    private void insertPinRequest(final long id,
                                  final long srmRequestId,
                                  final long pinId,
                                  final long expirationTime,
                                  final Long authRecId)
    {
        final long creationTime = System.currentTimeMillis();
        _template.update(InsertIntoPinRequestsTable,
                         new PreparedStatementSetter() {
                             public void setValues(PreparedStatement ps)
                                 throws SQLException
                             {
                                 ps.setLong(1, id);
                                 ps.setLong(2, srmRequestId);
                                 ps.setLong(3, pinId);
                                 ps.setLong(4, creationTime);
                                 ps.setLong(5, expirationTime);
                                 if (authRecId == null) {
                                     ps.setNull(6, Types.NUMERIC);
                                 } else {
                                     ps.setLong(6, authRecId);
                                 }
                             }
                         });
    }

    public void updatePin(long id,
                          Long expirationTime,
                          String pool,
                          PinManagerPinState state)
        throws PinDBException
    {
        StringBuilder sql = new StringBuilder();
        List<Object> parameters = new ArrayList<Object>();

        sql.append("UPDATE ").append(PinManagerPinsTableName).append(" SET ");
        if (expirationTime != null) {
            sql.append("Expiration = ?");
            parameters.add(Long.valueOf(expirationTime));
        }
        if (pool != null) {
            if (!parameters.isEmpty()) {
                sql.append(", ");
            }
            sql.append("Pool = ?");
            parameters.add(pool);
        }
        if (state != null) {
            if (!parameters.isEmpty()) {
                sql.append(", ");
            }
            sql.append("StateTranstionTime = ?, State = ?");
            parameters.add(Long.valueOf(System.currentTimeMillis()));
            parameters.add(Integer.valueOf(state.getStateId()));
        }
        sql.append(" WHERE id = ?");
        parameters.add(id);

        try {
            _template.update(sql.toString(), parameters.toArray());
        } catch (DataAccessException e) {
            throw new PinDBException(e.toString());
        }
    }

    private static final String updatePinRequest =
        "UPDATE  "+ PinManagerRequestsTableName +
        " SET  Expiration =?"+
        " WHERE  id = ?";
    public void updatePinRequest(final long id, final long expiration)
        throws PinDBException
    {
        try {
            _template.update(updatePinRequest,
                             new PreparedStatementSetter() {
                                 public void setValues(PreparedStatement ps)
                                     throws SQLException
                                 {
                                     ps.setLong(1, expiration);
                                     ps.setLong(2, id);
                                 }
                             });
        } catch (DataAccessException e) {
            throw new PinDBException(e.toString());
        }
    }

    private static final String movePinRequest =
        "UPDATE  "+ PinManagerRequestsTableName +
        " SET  PinId =?"+
        " WHERE  id = ?";
    public void movePinRequest(final long requestid, final long newPinId)
        throws PinDBException
    {
        try {
            int count =
                _template.update(movePinRequest,
                                 new PreparedStatementSetter() {
                                     public void setValues(PreparedStatement ps)
                                         throws SQLException
                                     {
                                         ps.setLong(1,newPinId);
                                         ps.setLong(2,requestid);
                                     }
                                 });
            if (count != 1) {
                throw new PinDBException("Pin request update failed in database");
            }
        } catch (DataAccessException e) {
            throw new PinDBException(e.toString());
        }
    }

    private static final String selectPin =
            "SELECT * FROM "+ PinManagerPinsTableName +
            " WHERE  id = ?";
    private Pin _getPin(long id)
    {
        return _template.queryForObject(selectPin, new PinMapper(), id);
    }

    private static final String selectPinForUpdate =
        "SELECT * FROM "+ PinManagerPinsTableName +
        " WHERE  id = ? FOR UPDATE";
    private Pin _getPinForUpdate(long id)
    {
        return _template.queryForObject(selectPinForUpdate,
                                        new PinMapper(), id);
    }

    private static final String selectPinRequest =
                    "SELECT * FROM "+ PinManagerRequestsTableName +
                    " WHERE  id = ?";
    private PinRequest getPinRequest(long id)
    {
        return _template.queryForObject(selectPinRequest,
                                        new PinRequestMapper(), id);
    }

    private static final String selectActivePinsByPnfsId =
        "SELECT * FROM "+ PinManagerPinsTableName +
        " WHERE  PnfsId = ?"+
        " OR state = "+PinManagerPinState.INITIAL.getStateId()+
        " OR state = "+PinManagerPinState.PINNING.getStateId()+
        " FOR UPDATE";;

    public Pin getAndLockActivePinWithRequestsByPnfsId(PnfsId pnfsId)
        throws PinDBException
    {
        try {
            Pin pin =
                _template.query(selectActivePinsByPnfsId,
                                new OptionalObjectExtractor<Pin>(new PinMapper()),
                                pnfsId.toString());
            if (pin != null) {
                pin.setRequests(getPinRequestsByPin(pin));
            }
            return pin;
        } catch (DataAccessException e) {
            throw new PinDBException(e.toString());
        }
    }

   private static final String selectPinRequestByPnfsIdandSrmRequestId =
                    "SELECT "+PinManagerRequestsTableName+".id " +
                    "FROM "+ PinManagerRequestsTableName +", "+
                     PinManagerPinsTableName+

                    " WHERE  "+PinManagerRequestsTableName+".PinId = " +
                          PinManagerPinsTableName+".id AND "+
                    PinManagerPinsTableName+".PnfsId = ? AND "+
                    PinManagerRequestsTableName+ ".SRMId = ?";

    public long getPinRequestIdByByPnfsIdandSrmRequestId(PnfsId pnfsId, long id)
        throws PinDBException
    {
        try {
            return _template.queryForLong(selectPinRequestByPnfsIdandSrmRequestId,
                                          pnfsId.toIdString(), id);
        } catch (DataAccessException e) {
            throw new PinDBException(e.toString());
        }
    }

    private static final String selectPinRequestsByPin =
                "SELECT * FROM "+ PinManagerRequestsTableName +
                " WHERE  PinId = ?";
    public Set<PinRequest> getPinRequestsByPin(Pin pin)
    {
        Set<PinRequest> set =
            _template.query(selectPinRequestsByPin,
                            new SetExtractor<PinRequest>(new PinRequestMapper()),
                            pin.getId());
        for (PinRequest request: set) {
            request.setPin(pin);
        }
        return set;
    }

    private static final String selectPinByPnfsIdForUpdate =
                    "SELECT * FROM "+ PinManagerPinsTableName +

                    " WHERE  PnfsId = ?"+
                    " AND  ( state = "+PinManagerPinState.PINNED.getStateId()+
                    " OR state = "+PinManagerPinState.INITIAL.getStateId()+
                  //  " OR state = "+PinManagerPinState.PNFSINFOWAITING.getStateId()+
                    " OR state = "+PinManagerPinState.PINNING.getStateId()+
                    " )"+ " FOR UPDATE " ;

    private Pin lockPinForInsertOfNewPinRequest(PnfsId pnfsId)
    {
        return _template.query(selectPinByPnfsIdForUpdate,
                               new OptionalObjectExtractor<Pin>(new PinMapper()),
                               pnfsId.toString());
    }

    public Pin getPin(long id) throws PinDBException
    {
        try {
            Pin pin = _getPin(id);
            pin.setRequests(getPinRequestsByPin(pin));
            return pin;
        } catch (DataAccessException e) {
            throw new PinDBException(e.toString());
        }
    }

    private static final String selectAllPins =
        "SELECT * FROM "+ PinManagerPinsTableName;
    public void allPinsToStringBuilder(StringBuilder sb)
        throws PinDBException
    {
        try {
            _template.query(selectAllPins,
                            new PinsToStringBuilderExtractor(sb));
        } catch (DataAccessException e) {
            throw new PinDBException(e.toString());
        }
    }

    private static final String selectAllPinsWithPnfsid =
        "SELECT * FROM "+ PinManagerPinsTableName+" WHERE PnfsId =?";
    public void allPinsByPnfsIdToStringBuilder(StringBuilder sb, PnfsId pnfsId)
        throws PinDBException
    {
        try {
            _template.query(selectAllPinsWithPnfsid,
                            new PinsToStringBuilderExtractor(sb),
                            pnfsId.toString());
        } catch (DataAccessException e) {
            throw new PinDBException(e.toString());
        }
    }

    public Set<Pin> allPinsByPnfsId(PnfsId pnfsId)
        throws PinDBException
    {
        try {
            Set<Pin> pins =
                _template.query(selectAllPinsWithPnfsid,
                                new SetExtractor<Pin>(new PinMapper()),
                                pnfsId.toIdString());
            for (Pin pin: pins) {
                pin.setRequests(getPinRequestsByPin(pin));
            }
            return pins;
        } catch (DataAccessException e) {
            throw new PinDBException(e.toString());
        }
    }

    private static final String selectPinsByState =
        "SELECT * FROM "+ PinManagerPinsTableName+
        " WHERE State = ?";
    public Collection<Pin> getPinsByState(PinManagerPinState state)
        throws PinDBException
    {
        try {
            Set<Pin> pins =
                _template.query(selectPinsByState,
                                new SetExtractor<Pin>(new PinMapper()),
                                state.getStateId());
            for (Pin pin: pins) {
                pin.setRequests(getPinRequestsByPin(pin));
            }
            return pins;
        } catch (DataAccessException e) {
            throw new PinDBException(e.toString());
        }
    }

    private static final String selectUnpinnedPins =
        "SELECT * FROM "+ PinManagerPinsTableName+
        " WHERE State != "+PinManagerPinState.PINNED.getStateId();
    public Collection<Pin> getAllPinsThatAreNotPinned()
        throws PinDBException
    {
        try {
            Set<Pin> pins =
                _template.query(selectUnpinnedPins,
                                new SetExtractor<Pin>(new PinMapper()));
            for (Pin pin: pins) {
                pin.setRequests(getPinRequestsByPin(pin));
            }
            return pins;
        } catch (DataAccessException e) {
            throw new PinDBException(e.toString());
        }
    }

    private static final String selectExpiredPinRequest =
        "SELECT * FROM "+ PinManagerRequestsTableName +
        " WHERE  Expiration != -1 AND " +
        " Expiration < ?";
    public Set<PinRequest> getExpiredPinRequests()
        throws PinDBException
    {
        try {
            Set<PinRequest> set =
                _template.query(selectExpiredPinRequest,
                                new SetExtractor<PinRequest>(new PinRequestMapper()),
                                System.currentTimeMillis());
            for (PinRequest request: set) {
                request.setPin(_getPin(request.getPinId()));
            }
            return set;
        } catch (DataAccessException e) {
            throw new PinDBException(e.toString());
        }
    }

    private static final String selectIDsOfExpiredPinWithoutRequest =
        " SELECT AllPinIds.Id FROM \n"+
        "    ( SELECT "+
            PinManagerPinsTableName+".Id as Id, "+
            PinManagerPinsTableName+".Expiration as Expiration, "+
            PinManagerRequestsTableName+".PinId as PinId\n"+
        "      FROM "+ PinManagerPinsTableName +
        " LEFT OUTER JOIN " +PinManagerRequestsTableName +'\n'+
        "      ON "+ PinManagerPinsTableName+".Id = "+
        PinManagerRequestsTableName+".PinId \n"+
        "      GROUP BY "+
        PinManagerPinsTableName+".Id, "+
        PinManagerPinsTableName+".Expiration, "+
        PinManagerRequestsTableName+".PinId "+
        " ) AS AllPinIds \n"+
        " WHERE  AllPinIds.PinId IS NULL AND "  +
        " AllPinIds.Expiration != -1 AND " +
        " AllPinIds.Expiration < ?";
    public List<Pin> getExpiredPinsWithoutRequests()
        throws PinDBException
    {
        try {
            return
                _template.query(selectIDsOfExpiredPinWithoutRequest,
                                new RowMapper<Pin>() {
                                    public Pin mapRow(ResultSet rs, int rowNum)
                                        throws SQLException
                                    {
                                        Pin pin =  _getPin(rs.getLong(1));
                                        pin.setRequests(getPinRequestsByPin(pin));
                                        assert pin.getRequests().isEmpty();
                                        return pin;
                                    }
                                }, System.currentTimeMillis());
        } catch (DataAccessException e) {
            throw new PinDBException(e.toString());
        }
    }

    public PinRequest insertPinRequestIntoNewOrExistingPin(
        PnfsId pnfsId,
        long lifetime,
        long srmRequestId,
        AuthorizationRecord authRec) throws PinDBException
    {
        long expirationTime =
            (lifetime == -1) ? -1: System.currentTimeMillis() + lifetime;
        if (authRec != null) {
            /* Since AuthorizationRecord is a converted Subject we
             * cannot rely on the correctness of the ID. We need to
             * resolve conflicts here.
             */
            AuthorizationRecord persistent = authRecordPM.find(authRec.getId());
            while (persistent != null && !persistent.equals(authRec)) {
                persistent = authRecordPM.find(persistent.getId() + 1);
            }

            if (persistent == null) {
                persistent = authRecordPM.persist(authRec);
                if (!persistent.equals(authRec)) {
                    _logger.error("Persisted authorization record is different from the original");
                }
            }

            authRec = persistent;
        }

        try {
            Pin pin = lockPinForInsertOfNewPinRequest(pnfsId);
            if (pin == null) {
                long id = nextLong();
                insertPin(id,
                          pnfsId,
                          expirationTime,
                          null,
                          PinManagerPinState.INITIAL);
                pin = lockPinForInsertOfNewPinRequest(pnfsId);
            }

            if (pin == null) {
                throw new PinDBException(1,"Could not get or lock pin");
            }

            long requestId = nextLong();
            insertPinRequest(requestId,srmRequestId,pin.getId(),
                             expirationTime,
                             (authRec==null) ? null : authRec.getId());
            pin.setRequests(getPinRequestsByPin(pin));
            for (PinRequest aPinRequest: pin.getRequests()) {
                if (aPinRequest.getId() == requestId) {
                    return aPinRequest;
                }
            }
            return null;
        } catch (DataAccessException e) {
            throw new PinDBException(e.toString());
        }
    }

    public Pin newPinForPinMove(PnfsId pnfsId,
                                String newPool,
                                long expirationTime) throws PinDBException
    {
        try {
            long id = nextLong();
            insertPin(id,
                      pnfsId,
                      expirationTime,
                      newPool,
                      PinManagerPinState.MOVING);
            return _getPin(id);
        } catch (DataAccessException e) {
            throw new PinDBException(e.toString());
        }
    }

    public Pin newPinForRepin(PnfsId pnfsId,
                              long expirationTime) throws PinDBException
    {
        try {
            long id = nextLong();
            insertPin(id,
                      pnfsId,
                      expirationTime,
                      null,
                      PinManagerPinState.PINNING);
            return _getPin(id);
        } catch (DataAccessException e) {
            throw new PinDBException(e.toString());
        }
    }

    public Pin getPinForUpdateByRequestId(long requestId)
        throws PinDBException
    {
        try {
            PinRequest request = getPinRequest(requestId);
            long pinId = request.getPinId();
            Pin pin = _getPinForUpdate(pinId);
            pin.setRequests(getPinRequestsByPin(pin));
            return pin;
        } catch (DataAccessException e) {
            throw new PinDBException(e.toString());
        }
    }

    public Pin getPinForUpdate(long pinId)
        throws PinDBException
    {
        try {
            Pin pin = _getPinForUpdate(pinId);
            pin.setRequests(getPinRequestsByPin(pin));
            return pin;
        } catch (DataAccessException e) {
            throw new PinDBException(e.toString());
        }
    }

    public void initDBConnection() throws PinDBException
    {
        if (_transactionStatus.get() != null) {
            throw new PinDBException(1, "DB is already initialized in this thread!!!");
        }

        try {
            TransactionStatus status =
                _txManager.getTransaction(new DefaultTransactionDefinition());
            _transactionStatus.set(status);
        } catch (TransactionException e) {
            throw new PinDBException(e.toString());
        }
   }

    public void commitDBOperations() throws PinDBException
    {
        TransactionStatus status = _transactionStatus.get();
        if (status != null) {
            _transactionStatus.remove();
            try {
                _txManager.commit(status);
            } catch (TransactionException e) {
                _logger.error("Commit failed with {}", e.getMessage());
                throw new PinDBException(2, "Commit failed with " + e.getMessage()) ;
            }
        }
    }

    public void rollbackDBOperations() throws PinDBException
    {
        TransactionStatus status = _transactionStatus.get();
        if (status != null) {
            _transactionStatus.remove();
            try {
                _txManager.rollback(status);
            } catch (TransactionException e) {
                _logger.error("Rollback failed with {}", e.getMessage());
                throw new PinDBException(2, "Rollback failed with " + e.getMessage());
            }
        }
    }

    private class PinMapper implements RowMapper<Pin>
    {
        @Override
        public Pin mapRow(ResultSet rs, int rowNum)
            throws SQLException
        {
            return new Pin(rs.getLong("id"),
                           new PnfsId(rs.getString("PnfsId")),
                           rs.getLong("Creation"),
                           rs.getLong("Expiration"),
                           rs.getString("Pool"),
                           rs.getLong("StateTranstionTime"),
                           PinManagerPinState.getState(rs.getInt("State")));
        }
    }

    private class PinRequestMapper implements RowMapper<PinRequest>
    {
        @Override
        public PinRequest mapRow(ResultSet rs, int rowNum)
            throws SQLException
        {
            AuthorizationRecord ar;
            long authRecId = rs.getLong("AuthRecId");
            if (authRecId == 0 && rs.wasNull()) {
                _logger.debug("authRecId is NULL");
                ar = null;
            } else {
                ar = authRecordPM.find(authRecId);
            }

            return new PinRequest(rs.getLong("id"),
                                  rs.getLong("SRMId"),
                                  rs.getLong("PinId"),
                                  rs.getLong("Creation"),
                                  rs.getLong("Expiration"),
                                  ar);
        }
    }

    private class OptionalObjectExtractor<T> implements ResultSetExtractor<T>
    {
        private final RowMapper<T> mapper;

        public OptionalObjectExtractor(RowMapper<T> mapper) {
            this.mapper = mapper;
        }

        @Override
        public T extractData(ResultSet rs)
            throws SQLException
        {
            return rs.next() ? mapper.mapRow(rs, 0) : null;
        }
    }

    private class SetExtractor<T> implements ResultSetExtractor<Set<T>>
    {
        private final RowMapper<T> mapper;

        public SetExtractor(RowMapper<T> mapper) {
            this.mapper = mapper;
        }

        @Override
        public Set<T> extractData(ResultSet rs)
            throws SQLException
        {
            Set<T> set = new HashSet<T>();
            while (rs.next()) {
                T o = mapper.mapRow(rs, 0);
                set.add(o);
            }
            return set;
        }
    }

    private class PinsToStringBuilderExtractor
        implements ResultSetExtractor<Void>
    {
        private final PinMapper mapper = new PinMapper();
        private final StringBuilder sb;

        public PinsToStringBuilderExtractor(StringBuilder sb) {
            this.sb = sb;
        }

        @Override
        public Void extractData(ResultSet rs)
            throws SQLException
        {
            int pcount = 0;
            int preqcount = 0;

            while (rs.next()) {
                pcount++;
                Pin pin = mapper.mapRow(rs, 0);
                pin.setRequests(getPinRequestsByPin(pin));
                preqcount += pin.getRequestsNum();
                sb.append(pin).append('\n');
            }

            if (pcount == 0)  {
                sb.append("no files are pinned");
            } else {
                sb.append("total number of pins: ").append(pcount);
                sb.append("\n total number of pin requests:").append(preqcount);
            }
            return null;
        }
    }
}
