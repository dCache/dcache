package diskCacheV111.services.space;

/*
      Column      |           Type           | Modifiers
------------------+--------------------------+-----------
 id               | bigint                   | not null
 vogroup          | character varying(32672) |
 vorole           | character varying(32672) |
 retentionpolicy  | integer                  |
 accesslatency    | integer                  |
 linkgroupid      | bigint                   |
 sizeinbytes      | bigint                   |
 creationtime     | bigint                   |
 lifetime         | bigint                   |
 description      | character varying(32672) |
 state            | integer                  |
 usedspaceinbytes | bigint                   |
 allocatedspaceinbytes | bigint                   |
*/

import org.springframework.jdbc.core.PreparedStatementCreator;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

import diskCacheV111.util.AccessLatency;
import diskCacheV111.util.RetentionPolicy;

public class SpaceReservationIO
{
        public static final String SRM_SPACE_TABLE = ManagerSchemaConstants.SPACE_TABLE_NAME;
        public static final String INSERT = "INSERT INTO "+SRM_SPACE_TABLE+
                " (vogroup,vorole,retentionpolicy,accesslatency,linkgroupid,"+
                "sizeinbytes,creationtime,lifetime,description,state,usedspaceinbytes,allocatedspaceinbytes)"+
                " VALUES  (?,?,?,?,?,?,?,?,?,?,?,?)";
        public static final String UPDATE = "UPDATE "+SRM_SPACE_TABLE+
                " set vogroup=?,vorole=?,retentionpolicy=?,accesslatency=?,linkgroupid=?,sizeinbytes=?,"+
                " creationtime=?,lifetime=?,description=?,state=? where id=?";
        public static final String SELECT_SPACE_RESERVATION_BY_ID="SELECT * FROM "+SRM_SPACE_TABLE+" where id=?";
        public static final String SELECT_SPACE_RESERVATION_BY_LINKGROUP_ID="SELECT * FROM "+SRM_SPACE_TABLE+
                " where linkgroupid=? and state = "+SpaceState.RESERVED.getStateId();
        public static final String SELECT_SPACE_RESERVATION_BY_DESC="SELECT * FROM "+SRM_SPACE_TABLE+" where description ~ ?";
        public static final String SELECT_SPACE_RESERVATION_BY_DESC_AND_LINKGROUP_ID="SELECT * FROM "+SRM_SPACE_TABLE+
                " where description ~ ? and linkgroupid=? and state = "+SpaceState.RESERVED.getStateId();
        public static final String SELECT_SPACE_RESERVATION_BY_VOROLE="SELECT * FROM "+SRM_SPACE_TABLE+
                " where vorole ~ ? and state = "+SpaceState.RESERVED.getStateId();
        public static final String SELECT_SPACE_RESERVATION_BY_VOROLE_AND_LINKGROUP_ID="SELECT * FROM "+SRM_SPACE_TABLE+
                " where vorole ~ ? and linkgroupid=? and state = "+SpaceState.RESERVED.getStateId();
        public static final String SELECT_SPACE_RESERVATION_BY_VOGROUP="SELECT * FROM "+SRM_SPACE_TABLE+
                " where vogroup ~ ? and state = "+SpaceState.RESERVED.getStateId();
        public static final String SELECT_SPACE_RESERVATION_BY_VOGROUP_AND_LINKGROUP_ID="SELECT * FROM "+SRM_SPACE_TABLE+
                " where vogroup ~ ? and linkgroupid=? and state = "+SpaceState.RESERVED.getStateId();
        public static final String SELECT_SPACE_RESERVATION_BY_VOGROUP_AND_VOROLE="SELECT * FROM "+SRM_SPACE_TABLE+
                " where vogroup ~ ? and vorole ? and state = "+SpaceState.RESERVED.getStateId();
        public static final String SELECT_SPACE_RESERVATION_BY_VOGROUP_AND_VOROLE_AND_LINKGROUP_ID="SELECT * FROM "+SRM_SPACE_TABLE+
                " where vogroup ~ ? and vorole ~ ? and linkgroupid=? and state = "+SpaceState.RESERVED.getStateId();
        public static final String SELECT_EXPIRED_SPACE_RESERVATIONS1="SELECT * FROM "+SRM_SPACE_TABLE+ " WHERE state = "+SpaceState.RESERVED.getStateId() +
                " AND lifetime != -1 and creationTime+lifetime < ?";
        public static final String SELECT_CURRENT_SPACE_RESERVATIONS="SELECT * FROM "+SRM_SPACE_TABLE+
                " WHERE state = "+SpaceState.RESERVED.getStateId();
        public static final String SELECT_FOR_UPDATE_BY_ID  = "SELECT * FROM "+SRM_SPACE_TABLE +
                " WHERE  id = ? FOR UPDATE ";
        public static final String SELECT_FOR_UPDATE_BY_ID_AND_SIZE = "SELECT * FROM "+SRM_SPACE_TABLE +
                " WHERE  id = ? AND sizeinbytes-allocatedspaceinbytes >= ? FOR UPDATE ";
        public static final String UPDATE_LIFETIME = "UPDATE "+SRM_SPACE_TABLE+ "SET lifetime=?  WHERE id=? ";

        public static PreparedStatementCreator insert(final String voGroup,
                                                      final String voRole,
                                                      final RetentionPolicy retentionPolicy,
                                                      final AccessLatency accessLatency,
                                                      final long linkGroupId,
                                                      final long size,
                                                      final long creationTime,
                                                      final long lifetime,
                                                      final String description,
                                                      final SpaceState state,
                                                      final long usedspace,
                                                      final long allocatedspace)
        {
            return new PreparedStatementCreator()
            {
                @Override
                public PreparedStatement createPreparedStatement(Connection con) throws SQLException
                {
                    /* Note that neither prepareStatement(String, String[]) nor prepareStatement(String, int[])
                     * work for us: The former suffers from different interpretations of case in HSQLDB and
                     * PostgreSQL and the latter is not support by the PostgreSQL JDBC driver.
                     */
                    PreparedStatement stmt = con.prepareStatement(INSERT, Statement.RETURN_GENERATED_KEYS);
                    stmt.setString(1, voGroup);
                    stmt.setString(2, voRole);
                    stmt.setInt(3, retentionPolicy == null ? 0 : retentionPolicy.getId());
                    stmt.setInt(4, accessLatency == null ? 0 : accessLatency.getId());
                    stmt.setLong(5, linkGroupId);
                    stmt.setLong(6, size);
                    stmt.setLong(7, creationTime);
                    stmt.setLong(8, lifetime);
                    stmt.setString(9, description);
                    stmt.setInt(10, state.getStateId());
                    stmt.setLong(11, usedspace);
                    stmt.setLong(12, allocatedspace);
                    return stmt;
                }
            };
        }
}
