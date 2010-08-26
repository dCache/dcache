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

package diskCacheV111.util;

import java.util.HashSet;
import java.sql.*;

public abstract class IoPackage { 

	public IoPackage () { 
	}

	public abstract HashSet select  (
		Connection connection,
		String query) throws SQLException;

       public abstract HashSet selectPrepared  (
                Connection connection,
                PreparedStatement statement) throws SQLException;
  
	public Object selectForUpdate(Connection connection,
				      PreparedStatement statement) throws SQLException { 
		HashSet container = selectPrepared(connection,statement);
		if (container.isEmpty()) { 
			throw new SQLException("No records found");
		}
		if (container.size()>1) {
			throw new SQLException("Multiple records found, disallowed");
		}
		return container.toArray()[0];
	}


// 	public int updatePrepared(Connection connection,
// 				  PreparedStatement statement) throws SQLException {
// 		int result = statement.executeUpdate();
// 		return result;
// 	}

// 	public int updatePrepared(
// 		Connection connection,
// 		PreparedStatement statement) throws SQLException;

}