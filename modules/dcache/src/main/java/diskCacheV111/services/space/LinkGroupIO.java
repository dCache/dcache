package diskCacheV111.services.space;

/*
dcache=# \d srmlinkgroup;
                 Table "public.srmlinkgroup"
        Column        |           Type           | Modifiers
----------------------+--------------------------+-----------
 id                   | bigint                   | not null
 name                 | character varying(32672) |
 freespaceinbytes     | bigint                   |
 lastupdatetime       | bigint                   |
 onlineallowed        | integer                  |
 nearlineallowed      | integer                  |
 replicaallowed       | integer                  |
 outputallowed        | integer                  |
 custodialallowed     | integer                  |
 reservedspaceinbytes | bigint                   |
Indexes:
    "srmlinkgroup_pkey" PRIMARY KEY, btree (id)
*/
/*
dcache=# \d srmlinkgroupvos;
           Table "public.srmlinkgroupvos"
   Column    |           Type           | Modifiers
-------------+--------------------------+-----------
 vogroup     | character varying(32672) | not null
 vorole      | character varying(32672) | not null
 linkgroupid | bigint                   | not null
Indexes:
    "srmlinkgroupvos_pkey" PRIMARY KEY, btree (vogroup, vorole, linkgroupid)
Foreign-key constraints:
    "fk_srmlinkgroupvos_l" FOREIGN KEY (linkgroupid) REFERENCES srmlinkgroup(id) ON DELETE RESTRICT
*/


import org.springframework.jdbc.core.PreparedStatementCreator;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

public class LinkGroupIO
{
    public static final String LINKGROUP_TABLE  = "srmLinkGroup".toLowerCase();
    public static final String LINKGROUP_VO_TABLE = "srmLinkGroupVOs".toLowerCase();
    public static final String INSERT =
        "INSERT INTO "+LINKGROUP_TABLE +
        " (name, freeSpaceInBytes, lastUpdateTime, onlineAllowed," +
        " nearlineAllowed, replicaAllowed, outputAllowed, custodialAllowed,reservedspaceinbytes)"+
        " VALUES (?,?,?,?,?,?,?,?,?)";
    public static final String SELECT_LINKGROUP_FOR_UPDATE_BY_NAME="SELECT * FROM "+LINKGROUP_TABLE+
        " WHERE  name = ? FOR UPDATE";
    public static final String SELECT_LINKGROUP_BY_ID  = "SELECT * FROM "+ LINKGROUP_TABLE + " WHERE  id = ?";
    public static final String SELECT_LINKGROUP_BY_NAME= "SELECT * FROM "+ LINKGROUP_TABLE + " WHERE  name = ?";
    public static final String SELECT_LINKGROUP_VO     ="SELECT voGroup,voRole FROM "+LINKGROUP_VO_TABLE+" WHERE linkGroupId=?";
    public static final String SELECT_CURRENT_LINKGROUPS = "SELECT * FROM "+ LINKGROUP_TABLE + " where lastUpdateTime >= ?";
    public static final String SELECT_ALL_LINKGROUPS = "SELECT * FROM "+ LINKGROUP_TABLE;
    public static final String UPDATE = "UPDATE "+LINKGROUP_TABLE+" SET freeSpaceInBytes=?,lastUpdateTime=?,onlineAllowed=?,nearlineAllowed=?,"+
        "replicaAllowed=?,outputAllowed=?,custodialAllowed=? WHERE  id = ?";

    public static PreparedStatementCreator insert(final String linkGroupName,
                                                  final long freeSpace,
                                                  final long updateTime,
                                                  final boolean onlineAllowed,
                                                  final boolean nearlineAllowed,
                                                  final boolean replicaAllowed,
                                                  final boolean outputAllowed,
                                                  final boolean custodialAllowed,
                                                  final long reservedSpace)
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
                stmt.setString(1, linkGroupName);
                stmt.setLong(2, freeSpace);
                stmt.setLong(3, updateTime);
                stmt.setInt(4, (onlineAllowed ? 1 : 0));
                stmt.setInt(5, (nearlineAllowed ? 1 : 0));
                stmt.setInt(6, (replicaAllowed ? 1 : 0));
                stmt.setInt(7, (outputAllowed ? 1 : 0));
                stmt.setInt(8, (custodialAllowed ? 1 : 0));
                stmt.setLong(9, reservedSpace);
                return stmt;
            }
        };
    }
}
