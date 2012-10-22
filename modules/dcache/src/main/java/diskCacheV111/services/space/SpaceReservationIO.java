//______________________________________________________________________________
//
// $Id: SpaceReservationIO.java 8022 2008-01-07 21:25:23Z litvinse $
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

import diskCacheV111.util.IoPackage;
import diskCacheV111.util.AccessLatency;
import diskCacheV111.util.RetentionPolicy;


/*
      Column      |           Type           | Modifiers
------------------+--------------------------+-----------
 id               | bigint                   | not null
 vogroup          | character varying(32672) |
 vorole           | character varying(32672) |
 retentionpolicy  | integer                  |
 accesslatency    | integer                  |
 linkgroupid       | bigint                   |
 sizeinbytes      | bigint                   |
 creationtime     | bigint                   |
 lifetime         | bigint                   |
 description      | character varying(32672) |
 state            | integer                  |
 usedspaceinbytes | bigint                   |
 allocatedspaceinbytes | bigint                   |
*/




public class SpaceReservationIO extends IoPackage<Space> {
        public static final String SRM_SPACE_TABLE = ManagerSchemaConstants.SpaceTableName;
        public static final String INSERT = "INSERT INTO "+SRM_SPACE_TABLE+
                " (id,vogroup,vorole,retentionpolicy,accesslatency,linkgroupid,"+
                "sizeinbytes,creationtime,lifetime,description,state,usedspaceinbytes,allocatedspaceinbytes)"+
                " VALUES  (?,?,?,?,?,?,?,?,?,?,?,?,?)";
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
        public static final String SELECT_ALL_SPACE_RESERVATIONS ="SELECT * FROM "+SRM_SPACE_TABLE;
        public static final String SELECT_EXPIRED_SPACE_RESERVATIONS="SELECT * FROM "+SRM_SPACE_TABLE+ " WHERE state = "+SpaceState.EXPIRED.getStateId();
        public static final String SELECT_EXPIRED_SPACE_RESERVATIONS1="SELECT * FROM "+SRM_SPACE_TABLE+ " WHERE state = "+SpaceState.RESERVED.getStateId() +
                " AND lifetime != -1 and creationTime+lifetime < ?";
        public static final String SELECT_RELEASED_SPACE_RESERVATIONS="SELECT * FROM "+SRM_SPACE_TABLE+ " WHERE state = "+SpaceState.RELEASED.getStateId();
        public static final String SELECT_INVALID_SPACE_RESERVATIONS="SELECT * FROM "+SRM_SPACE_TABLE+ " WHERE state !="+SpaceState.RESERVED.getStateId();
        public static final String SELECT_CURRENT_SPACE_RESERVATIONS="SELECT * FROM "+SRM_SPACE_TABLE+
                " WHERE state = "+SpaceState.RESERVED.getStateId();
        public static final String DELETE_SPACE_RESERVATION ="DELETE FROM   "+SRM_SPACE_TABLE+" where id=?";
        public static final String SELECT_FOR_UPDATE_BY_ID  = "SELECT * FROM "+SRM_SPACE_TABLE +
                " WHERE  id = ? FOR UPDATE ";
        public static final String SELECT_FOR_UPDATE_BY_ID_AND_SIZE = "SELECT * FROM "+SRM_SPACE_TABLE +
                " WHERE  id = ? AND sizeinbytes-allocatedspaceinbytes >= ? FOR UPDATE ";
        public static final String UPDATE_STATUS   = "UPDATE "+SRM_SPACE_TABLE+ "SET status=?  WHERE id=? ";
        public static final String UPDATE_LIFETIME = "UPDATE "+SRM_SPACE_TABLE+ "SET lifetime=?  WHERE id=? ";
        public static final String DECREMENT_ALLOCATED_SPACE = "UPDATE "+SRM_SPACE_TABLE+" SET allocatedspaceinbytes = allocatedspaceinbytes - ? where id=?";
        public static final String INCREMENT_ALLOCATED_SPACE = "UPDATE "+SRM_SPACE_TABLE+" SET allocatedspaceinbytes = allocatedspaceinbytes + ? where id=?";
        public static final String DECREMENT_USED_SPACE = "UPDATE "+SRM_SPACE_TABLE+" SET usedspaceinbytes = usedspaceinbytes - ? where id=?";
        public static final String INCREMENT_USED_SPACE = "UPDATE "+SRM_SPACE_TABLE+" SET usedspaceinbytes = usedspaceinbytes + ? where id=?";
        public static final String SELECT_SPACE_RESERVATIONS_FOR_EXPIRED_FILES="select * from srmspace where id in (select distinct spacereservationid from srmspacefile where (state= "+FileState.RESERVED.getStateId()+" or state = "+ FileState.TRANSFERRING.getStateId() +") and creationtime+lifetime<?)";

        public SpaceReservationIO() {
        }

        @Override
        public Set<Space> select( Connection connection,
                                  String txt) throws SQLException {
                Set<Space>  container = new HashSet<>();
                Statement s = connection.createStatement();
                ResultSet set = s.executeQuery(txt);
                while (set.next()) {
                        container.add(
                                new Space(set.getLong("id"),
                                          set.getString("vogroup"),
                                          set.getString("vorole"),
                                          RetentionPolicy.getRetentionPolicy(set.getInt("retentionPolicy")),
                                          AccessLatency.getAccessLatency(set.getInt("accessLatency")),
                                          set.getLong("linkgroupid"),
                                          set.getLong("sizeinbytes"),
                                          set.getLong("creationtime"),
                                          set.getLong("lifetime"),
                                          set.getString("description"),
                                          SpaceState.getState(set.getInt("state")),
                                          set.getLong("usedspaceinbytes"),
                                          set.getLong("allocatedspaceinbytes")));
                }
                s.close();
                return container;
        }

        @Override
        public Set<Space> selectPrepared(Connection connection,
                                       PreparedStatement statement)
                throws SQLException {
                Set<Space>  container = new HashSet<>();
                ResultSet set = statement.executeQuery();
                while (set.next()) {
                        container.add(
                                new Space(set.getLong("id"),
                                          set.getString("vogroup"),
                                          set.getString("vorole"),
                                          RetentionPolicy.getRetentionPolicy(set.getInt("retentionPolicy")),
                                          AccessLatency.getAccessLatency(set.getInt("accessLatency")),
                                          set.getLong("linkgroupid"),
                                          set.getLong("sizeinbytes"),
                                          set.getLong("creationtime"),
                                          set.getLong("lifetime"),
                                          set.getString("description"),
                                          SpaceState.getState(set.getInt("state")),
                                          set.getLong("usedspaceinbytes"),
                                          set.getLong("allocatedspaceinbytes")));
                }
                return container;
        }
}