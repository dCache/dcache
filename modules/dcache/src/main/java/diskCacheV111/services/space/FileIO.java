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

import java.util.Set;
import java.util.HashSet;
import java.sql.SQLException;
import java.sql.ResultSet;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import diskCacheV111.util.IoPackage;
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
 deleted            | integer                  |
Indexes:
    "srmspacefile_pkey" PRIMARY KEY, btree (id)
*/;



public class FileIO extends IoPackage<File> {

	public static final String SRM_SPACEFILE_TABLE = ManagerSchemaConstants.SpaceFileTableName;
	public static final String SELECT_BY_SPACERESERVATION_ID =
		"SELECT * FROM "+SRM_SPACEFILE_TABLE+" WHERE spacereservationid = ?";
	public static final String SELECT_BY_ID="SELECT * FROM "+
		SRM_SPACEFILE_TABLE+" WHERE  id = ?";
	public static final String SELECT_BY_PNFSID =
		"SELECT * FROM "+SRM_SPACEFILE_TABLE+" WHERE pnfsId=?";
	public static final String SELECT_BY_PNFSPATH =
		"SELECT * FROM "+SRM_SPACEFILE_TABLE+" WHERE pnfspath=? and deleted != 1";
	public static final String SELECT_BY_PNFSID_AND_PNFSPATH =
		"SELECT * FROM "+SRM_SPACEFILE_TABLE+" WHERE pnfsid=? AND pnfspath=?";
	public static final String SELECT_TRANSIENT_FILES_BY_PNFSPATH_AND_RESERVATIONID =
		"SELECT * FROM "+SRM_SPACEFILE_TABLE+" WHERE  pnfspath=? AND spacereservationid=? and (state= "+
		FileState.RESERVED.getStateId()+" or state = "+ FileState.TRANSFERRING.getStateId() + ") FOR UPDATE";
	public static final String SELECT_USED_SPACE_IN_SPACEFILES = "SELECT sum(sizeinbytes)  FROM "+
		SRM_SPACEFILE_TABLE+" WHERE spacereservationid = ? AND state != ? "+FileState.FLUSHED.getStateId();
	public static final String SELECT_TRANSFERRING_OR_RESERVED_BY_PNFSPATH =
		"SELECT * FROM "+SRM_SPACEFILE_TABLE+" WHERE pnfspath=? AND (state= "+FileState.RESERVED.getStateId()+
		" or state = "+ FileState.TRANSFERRING.getStateId() +") and deleted!=1";

	public static final String REMOVE_PNFSID_ON_SPACEFILE="UPDATE "+SRM_SPACEFILE_TABLE+
		" SET pnfsid = NULL WHERE id=?";
	public static final String REMOVE_PNFSID_AND_CHANGE_STATE_SPACEFILE="UPDATE "+SRM_SPACEFILE_TABLE+
		" SET pnfsid = NULL, STATE=? WHERE id=?";
	public static final String INSERT_WO_PNFSID  = "INSERT INTO "+SRM_SPACEFILE_TABLE+
		" (id,vogroup,vorole,spacereservationid,sizeinbytes,creationtime,lifetime,pnfspath,pnfsid,state,deleted) "+
		" VALUES  (?,?,?,?,?,?,?,?,NULL,?,0)";
	public static final String INSERT_W_PNFSID  = "INSERT INTO "+SRM_SPACEFILE_TABLE+
		" (id,vogroup,vorole,spacereservationid,sizeinbytes,creationtime,lifetime,pnfspath,pnfsid,state,deleted) "+
		" VALUES  (?,?,?,?,?,?,?,?,?,?,0)";
	public static final String DELETE="DELETE FROM "+SRM_SPACEFILE_TABLE+" WHERE id=?";

	public static final String SELECT_FOR_UPDATE_BY_PNFSPATH = "SELECT * FROM "+SRM_SPACEFILE_TABLE+" WHERE  pnfspath=? FOR UPDATE ";
	public static final String SELECT_FOR_UPDATE_BY_PNFSID   = "SELECT * FROM "+SRM_SPACEFILE_TABLE+" WHERE  pnfsid=?   FOR UPDATE ";
	public static final String SELECT_FOR_UPDATE_BY_ID       = "SELECT * FROM "+SRM_SPACEFILE_TABLE+" WHERE  id=?       FOR UPDATE ";

	public static final String UPDATE = "UPDATE "+SRM_SPACEFILE_TABLE+
		" SET vogroup=?, vorole=?, sizeinbytes=?, lifetime=?, pnfsid=?, state=? WHERE id=?";

	public static final String UPDATE_DELETED_FLAG = "UPDATE "+SRM_SPACEFILE_TABLE+
		" SET deleted=? WHERE id=?";

	public static final String UPDATE_WO_PNFSID = "UPDATE "+SRM_SPACEFILE_TABLE+
		" SET vogroup=?, vorole=?, sizeinbytes=?, lifetime=?, state=? WHERE id=?";
	public static final String SELECT_EXPIRED_SPACEFILES="SELECT * FROM "+SRM_SPACEFILE_TABLE+ " WHERE (state= "+FileState.RESERVED.getStateId()+
		" or state = "+ FileState.TRANSFERRING.getStateId() +") and creationTime+lifetime < ? AND spacereservationid=?";
	public static final String SELECT_EXPIRED_SPACEFILES1="SELECT * FROM "+SRM_SPACEFILE_TABLE+
		" WHERE creationTime+lifetime < ? AND spacereservationid=?";
	public static final String SELECT_DELETED_FILES="SELECT * FROM "+SRM_SPACEFILE_TABLE+
		" WHERE deleted=1 and  spacereservationid=?";

	public FileIO() {
	}

	@Override
        public Set<File> select( Connection connection,
				 String txt) throws SQLException {
		Set<File>  container = new HashSet<>();
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
                                               FileState.getState(set.getInt("state")),
                                               (set.getObject("deleted")!=null?set.getInt("deleted"):0)));
                }
                s.close();
                return container;
        }

        @Override
        public Set<File> selectPrepared(Connection connection,
                                        PreparedStatement statement)
                throws SQLException {
                Set<File>  container = new HashSet<>();
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
                                               FileState.getState(set.getInt("state")),
                                               (set.getObject("deleted")!=null?set.getInt("deleted"):0)));
                }
                return container;
        }
}
