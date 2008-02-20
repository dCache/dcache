//______________________________________________________________________________
//
// $Id$ 
// $Author$
//
// Infrastructure to retrieve objects from DB 
//
// created 11/07 by Dmitry Litvintsev (litvinse@fnal.gov)
//
//______________________________________________________________________________


package diskCacheV111.services.space;
import java.sql.*;
import java.util.Set;
import java.util.HashSet;
import diskCacheV111.util.*;
import diskCacheV111.util.IoPackage;
import diskCacheV111.util.AccessLatency;
import diskCacheV111.util.RetentionPolicy;
import diskCacheV111.util.PnfsId;

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
Indexes:
    "srmspacefile_pkey" PRIMARY KEY, btree (id)
*/;



public class FileIO extends IoPackage { 
	
	public static final String SRM_SPACEFILE_TABLE = ManagerSchemaConstants.SpaceFileTableName;
	public static final String SELECT_BY_SPACERESERVATION_ID = 
		"SELECT * FROM "+SRM_SPACEFILE_TABLE+" WHERE spacereservationid = ?";
	public static final String SELECT_BY_ID="SELECT * FROM "+
		SRM_SPACEFILE_TABLE+" WHERE  id = ?";
	public static final String SELECT_BY_PNFSID =
		"SELECT * FROM "+SRM_SPACEFILE_TABLE+" WHERE pnfsId=?";
	public static final String SELECT_BY_PNFSPATH =
		"SELECT * FROM "+SRM_SPACEFILE_TABLE+" WHERE pnfspath=?";
	public static final String SELECT_BY_PNFSID_AND_PNFSPATH =
		"SELECT * FROM "+SRM_SPACEFILE_TABLE+" WHERE pnfsid=? AND pnfspath=?";
	public static final String SELECT_USED_SPACE_IN_SPACEFILES = "SELECT sum(sizeinbytes)  FROM "+
		SRM_SPACEFILE_TABLE+" WHERE spacereservationid = ? AND state != ? "+FileState.FLUSHED.getStateId();

	public static final String REMOVE_PNFSID_ON_SPACEFILE="UPDATE "+SRM_SPACEFILE_TABLE+
		" SET pnfsid = NULL WHERE id=?";
	public static final String REMOVE_PNFSID_AND_CHANGE_STATE_SPACEFILE="UPDATE "+SRM_SPACEFILE_TABLE+
		" SET pnfsid = NULL, STATE=? WHERE id=?";
	public static final String INSERT_WO_PNFSID  = "INSERT INTO "+SRM_SPACEFILE_TABLE+
		" (id,vogroup,vorole,spacereservationid,sizeinbytes,creationtime,lifetime,pnfspath,pnfsid,state) "+
		" VALUES  (?,?,?,?,?,?,?,?,NULL,?)";
	public static final String INSERT_W_PNFSID  = "INSERT INTO "+SRM_SPACEFILE_TABLE+
		" (id,vogroup,vorole,spacereservationid,sizeinbytes,creationtime,lifetime,pnfspath,pnfsid,state) "+
		" VALUES  (?,?,?,?,?,?,?,?,?,?)";
	public static final String DELETE="DELETE FROM "+SRM_SPACEFILE_TABLE+" WHERE id=?";

	public static final String SELECT_FOR_UPDATE_BY_PNFSPATH = "SELECT * FROM "+SRM_SPACEFILE_TABLE+" WHERE  pnfspath=? FOR UPDATE ";
	public static final String SELECT_FOR_UPDATE_BY_PNFSID   = "SELECT * FROM "+SRM_SPACEFILE_TABLE+" WHERE  pnfsid=?   FOR UPDATE ";
	public static final String SELECT_FOR_UPDATE_BY_ID       = "SELECT * FROM "+SRM_SPACEFILE_TABLE+" WHERE  id=?       FOR UPDATE ";
	
	public static final String UPDATE = "UPDATE "+SRM_SPACEFILE_TABLE+
		" SET vogroup=?, vorole=?, sizeinbytes=?, lifetime=?, pnfsid=?, state=? WHERE id=?";

	public static final String UPDATE_WO_PNFSID = "UPDATE "+SRM_SPACEFILE_TABLE+
		" SET vogroup=?, vorole=?, sizeinbytes=?, lifetime=?, state=? WHERE id=?";

	public static final String SELECT_EXPIRED_SPACEFILES="SELECT * FROM "+SRM_SPACEFILE_TABLE+ " WHERE state = "+FileState.RESERVED.getStateId()+
		" AND lifetime != -1 and creationTime+lifetime < ? AND spacereservationid=?";

	public FileIO() {
	}

	public HashSet select( Connection connection,
				String txt) throws SQLException {
		HashSet<File>  container = new HashSet<File>();
 		Statement s = connection.createStatement();
 		ResultSet set = s.executeQuery(txt);
 		while (set.next()) { 
			String pnfsIdString = set.getString("pnfsId");
			PnfsId pnfsId = null;
			if ( pnfsIdString != null ) {
				pnfsId = new PnfsId( pnfsIdString );
			}
 			container.add(
 				new File(set.getLong("id"),
					 set.getString("vogroup"),
					 set.getString("vorole"),
					 set.getLong("spacereservationid"),
					 set.getLong("sizeinbytes"),
					 set.getLong("creationtime"),
					 set.getLong("lifetime"),
					 set.getString("pnfspath"),
					 pnfsId, 
					 FileState.getState(set.getInt("state"))));
		}
 		s.close();
		return container;
	}
	
	public HashSet selectPrepared(Connection connection,
				     PreparedStatement statement) 
		throws SQLException {
		HashSet<File>  container = new HashSet<File>();
		ResultSet set = statement.executeQuery();
 		while (set.next()) { 
			String pnfsIdString = set.getString("pnfsId");
			PnfsId pnfsId = null;
			if ( pnfsIdString != null ) {
				pnfsId = new PnfsId( pnfsIdString );
			}
 			container.add(
 				new File(set.getLong("id"),
					 set.getString("vogroup"),
					 set.getString("vorole"),
					 set.getLong("spacereservationid"),
					 set.getLong("sizeinbytes"),
					 set.getLong("creationtime"),
					 set.getLong("lifetime"),
					 set.getString("pnfspath"),
					 pnfsId, 
					 FileState.getState(set.getInt("state"))));
		}
		return container;
	}
	
}