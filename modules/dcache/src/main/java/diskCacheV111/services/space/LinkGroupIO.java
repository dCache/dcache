//______________________________________________________________________________
//
// $Id: LinkGroupIO.java 8022 2008-01-07 21:25:23Z litvinse $
// $Author: litvinse $
//
// Infrastructure to retrieve objects from DB
//
// created 11/07 by Dmitry Litvintsev (litvinse@fnal.gov)
//
//______________________________________________________________________________

package diskCacheV111.services.space;

import java.util.Set;
import java.util.HashSet;
import java.sql.SQLException;
import java.sql.ResultSet;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import diskCacheV111.util.AccessLatency;
import diskCacheV111.util.RetentionPolicy;
import diskCacheV111.util.VOInfo;
import diskCacheV111.util.IoPackage;

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



public class LinkGroupIO extends IoPackage<LinkGroup>  {

	public static final String LINKGROUP_TABLE  = "srmLinkGroup".toLowerCase();
	public static final String LINKGROUP_VO_TABLE = "srmLinkGroupVOs".toLowerCase();
	public static final String INSERT =
		"INSERT INTO "+LINKGROUP_TABLE +
		" (id, name, freeSpaceInBytes, lastUpdateTime, onlineAllowed," +
		" nearlineAllowed, replicaAllowed, outputAllowed, custodialAllowed,reservedspaceinbytes)"+
		" VALUES ( ?,?,?,?,?,?,?,?,?,?)";
	public static final String INSERT_LINK_GROUP_VO="INSERT INTO "+LINKGROUP_VO_TABLE+
		" (linkgroupid,voGroup,voRole) VALUES (?,?,?) ";
	public static final String SELECT_LINKGROUP_FOR_UPDATE_BY_NAME="SELECT * FROM "+LINKGROUP_TABLE+
		" WHERE  name = ? FOR UPDATE";
	public static final String SELECT_LINKGROUP_FOR_UPDATE_BY_ID="SELECT * FROM "+LINKGROUP_TABLE+
		" WHERE  id = ? FOR UPDATE";
	public static final String SELECT_LINKGROUP_INFO_FOR_UPDATE="SELECT * FROM "+ LINKGROUP_TABLE +
		" WHERE  id = ? and freespaceinbytes-reservedspaceinbytes>=? FOR UPDATE";
	public static final String SELECT_LINKGROUP_BY_ID  = "SELECT * FROM "+ LINKGROUP_TABLE + " WHERE  id = ?";
	public static final String SELECT_LINKGROUP_BY_NAME= "SELECT * FROM "+ LINKGROUP_TABLE + " WHERE  name = ?";
	public static final String SELECT_LINKGROUP_VO     ="SELECT voGroup,voRole FROM "+LINKGROUP_VO_TABLE+" WHERE linkGroupId=?";
	public static final String SELECT_CURRENT_LINKGROUPS = "SELECT * FROM "+ LINKGROUP_TABLE + " where lastUpdateTime >= ?";
	public static final String SELECT_ALL_LINKGROUPS = "SELECT * FROM "+ LINKGROUP_TABLE;
	public static final String DECREMENT_RESERVED_SPACE = "UPDATE "+LINKGROUP_TABLE+" SET reservedspaceinbytes = reservedspaceinbytes - ? where id=?";
	public static final String INCREMENT_RESERVED_SPACE = "UPDATE "+LINKGROUP_TABLE+" SET reservedspaceinbytes = reservedspaceinbytes + ? where id=?";
	public static final String DECREMENT_FREE_SPACE = "UPDATE "+LINKGROUP_TABLE+" SET freespaceinbytes = freespaceinbytes - ? where id=?";
	public static final String INCREMENT_FREE_SPACE = "UPDATE "+LINKGROUP_TABLE+" SET freespaceinbytes = freespaceinbytes + ? where id=?";
	public static final String UPDATE = "UPDATE "+LINKGROUP_TABLE+" SET freeSpaceInBytes=?,lastUpdateTime=?,onlineAllowed=?,nearlineAllowed=?,"+
		"replicaAllowed=?,outputAllowed=?,custodialAllowed=? WHERE  id = ?";
	public static final String SELECT_ALL = "SELECT * FROM "+LINKGROUP_TABLE;

	public LinkGroupIO() {
	}

	@Override
        public Set<LinkGroup> select(Connection connection,
                                     String txt) throws SQLException {
		Set<LinkGroup> container = new HashSet<>();
		Statement stmt = connection.createStatement();
		ResultSet set = stmt.executeQuery(txt);
		while (set.next()) {
			LinkGroup lg = new LinkGroup();
			long id = set.getLong("id");
			lg.setId(id);
			lg.setName(set.getString("name"));
			lg.setFreeSpace(set.getLong("freeSpaceInBytes"));
			lg.setUpdateTime(set.getLong("lastUpdateTime"));
			lg.setOnlineAllowed(set.getBoolean("onlineAllowed"));
			lg.setNearlineAllowed(set.getBoolean("nearlineAllowed"));
			lg.setReplicaAllowed(set.getBoolean("replicaAllowed"));
			lg.setOutputAllowed(set.getBoolean("outputAllowed"));
			lg.setCustodialAllowed(set.getBoolean("custodialAllowed"));
			lg.setReservedSpaceInBytes(set.getLong("reservedspaceinbytes"));
			PreparedStatement s = connection.prepareStatement(SELECT_LINKGROUP_VO);
			s.setLong(1,id);
			ResultSet vos = s.executeQuery();
			Set<VOInfo> volist = new HashSet<>();
			while (vos.next()) {
				volist.add(new VOInfo(vos.getString("vogroup"),
						      vos.getString("vorole")));
			}
			lg.setVOs(volist.toArray(new VOInfo[volist.size()]));
			container.add(lg);
			s.close();
		}
		stmt.close();
		return container;
	}


	@Override
        public Set<LinkGroup> selectPrepared(Connection connection,
				      PreparedStatement statement) throws SQLException {
		Set<LinkGroup> container = new HashSet<>();
		ResultSet set = statement.executeQuery();
		while (set.next()) {
			LinkGroup lg = new LinkGroup();
			long id = set.getLong("id");
			lg.setId(id);
			lg.setName(set.getString("name"));
			lg.setFreeSpace(set.getLong("freeSpaceInBytes"));
			lg.setUpdateTime(set.getLong("lastUpdateTime"));
			lg.setOnlineAllowed(set.getBoolean("onlineAllowed"));
			lg.setNearlineAllowed(set.getBoolean("nearlineAllowed"));
			lg.setReplicaAllowed(set.getBoolean("replicaAllowed"));
			lg.setOutputAllowed(set.getBoolean("outputAllowed"));
			lg.setCustodialAllowed(set.getBoolean("custodialAllowed"));
			lg.setReservedSpaceInBytes(set.getLong("reservedspaceinbytes"));
			PreparedStatement s = statement.getConnection().prepareStatement(SELECT_LINKGROUP_VO);
			s.setLong(1,id);
			ResultSet vos = s.executeQuery();
			Set<VOInfo> volist = new HashSet<>();
			while (vos.next()) {
				volist.add(new VOInfo(vos.getString("vogroup"),
						      vos.getString("vorole")));
			}
			lg.setVOs(volist.toArray(new VOInfo[volist.size()]));
			container.add(lg);
			s.close();
		}
		return container;
	}


}
