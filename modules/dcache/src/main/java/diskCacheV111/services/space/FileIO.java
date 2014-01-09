package diskCacheV111.services.space;

/*
                Table "public.srmspacefile"
       Column       |           Type           | Modifiers
--------------------+--------------------------+-----------
 id                 | bigint                   | not null
 vogroup            | character varying(32672) |
 vorole             | character varying(32672) |
 spacereservationid | bigint                   |
 sizeinbytes        | bigint                   |
 creationtime       | bigint                   |
 lifetime           | bigint                   |
 pnfspath           | character varying(32672) |
 pnfsid             | character varying(32672) |
 state              | integer                  |
 deleted            | integer                  |
Indexes:
    "srmspacefile_pkey" PRIMARY KEY, btree (id)
*/

import org.springframework.jdbc.core.PreparedStatementCreator;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Objects;

import diskCacheV111.util.FsPath;
import diskCacheV111.util.PnfsId;

public class FileIO
{
    public static final String SRM_SPACEFILE_TABLE = ManagerSchemaConstants.SPACE_FILE_TABLE_NAME;
    public static final String SELECT_BY_SPACERESERVATION_ID =
        "SELECT * FROM "+SRM_SPACEFILE_TABLE+" WHERE spacereservationid = ?";
    public static final String SELECT_BY_PNFSID =
        "SELECT * FROM "+SRM_SPACEFILE_TABLE+" WHERE pnfsId=?";
    public static final String SELECT_TRANSIENT_FILES_BY_PNFSPATH_AND_RESERVATIONID =
        "SELECT * FROM "+SRM_SPACEFILE_TABLE+" WHERE  pnfspath=? AND spacereservationid=? and (state= "+
        FileState.RESERVED.getStateId()+" or state = "+ FileState.TRANSFERRING.getStateId() + ") FOR UPDATE";
    public static final String SELECT_TRANSFERRING_OR_RESERVED_BY_PNFSPATH =
        "SELECT * FROM "+SRM_SPACEFILE_TABLE+" WHERE pnfspath=? AND (state= "+FileState.RESERVED.getStateId()+
        " or state = "+ FileState.TRANSFERRING.getStateId() +") and deleted!=1";

    public static final String REMOVE_PNFSID_ON_SPACEFILE="UPDATE "+SRM_SPACEFILE_TABLE+
        " SET pnfsid = NULL WHERE id=?";
    public static final String REMOVE_PNFSID_AND_CHANGE_STATE_SPACEFILE="UPDATE "+SRM_SPACEFILE_TABLE+
        " SET pnfsid = NULL, STATE=? WHERE id=?";
    public static final String INSERT  = "INSERT INTO "+SRM_SPACEFILE_TABLE+
        " (vogroup,vorole,spacereservationid,sizeinbytes,creationtime,lifetime,pnfspath,pnfsid,state,deleted) "+
        " VALUES  (?,?,?,?,?,?,?,?,?,0)";
    public static final String DELETE="DELETE FROM "+SRM_SPACEFILE_TABLE+" WHERE id=?";

    public static final String SELECT_FOR_UPDATE_BY_PNFSID   = "SELECT * FROM "+SRM_SPACEFILE_TABLE+" WHERE  pnfsid=?   FOR UPDATE ";
    public static final String SELECT_FOR_UPDATE_BY_ID       = "SELECT * FROM "+SRM_SPACEFILE_TABLE+" WHERE  id=?       FOR UPDATE ";

    public static final String UPDATE = "UPDATE "+SRM_SPACEFILE_TABLE+
        " SET vogroup=?, vorole=?, sizeinbytes=?, lifetime=?, pnfsid=?, state=? WHERE id=?";

    public static final String UPDATE_DELETED_FLAG = "UPDATE "+SRM_SPACEFILE_TABLE+
        " SET deleted=? WHERE id=?";

    public static final String SELECT_EXPIRED_SPACEFILES="SELECT * FROM "+SRM_SPACEFILE_TABLE+ " WHERE (state= "+FileState.RESERVED.getStateId()+
        " or state = "+ FileState.TRANSFERRING.getStateId() +") and creationTime+lifetime < ?";
    public static final String SELECT_EXPIRED_SPACEFILES1="SELECT * FROM "+SRM_SPACEFILE_TABLE+
        " WHERE creationTime+lifetime < ? AND spacereservationid=?";

    public static PreparedStatementCreator insert(final long reservationId,
                                                  final String voGroup,
                                                  final String voRole,
                                                  final long sizeInBytes,
                                                  final long creationTime,
                                                  final long lifetime,
                                                  final FsPath path,
                                                  final PnfsId pnfsId,
                                                  final FileState state)
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
                PreparedStatement stmt = con.prepareStatement(INSERT, Statement.RETURN_GENERATED_KEYS );
                stmt.setString(1, voGroup);
                stmt.setString(2, voRole);
                stmt.setLong(3, reservationId);
                stmt.setLong(4, sizeInBytes);
                stmt.setLong(5, creationTime);
                stmt.setLong(6, lifetime);
                stmt.setString(7, Objects.toString(path));
                stmt.setString(8, Objects.toString(pnfsId));
                stmt.setInt(9, state.getStateId());
                return stmt;
            }
        };
    }
}
