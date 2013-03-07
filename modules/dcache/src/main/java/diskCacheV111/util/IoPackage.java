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

import javax.annotation.Nonnull;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Set;

public abstract class IoPackage<T> {

	public IoPackage () {
	}

	public abstract Set<T> select (Connection connection,
                                       String query)
                throws SQLException;

        public abstract Set<T> selectPrepared  (Connection connection,
                                                PreparedStatement statement)
                throws SQLException;

	public @Nonnull T selectForUpdate(Connection connection,
                                 PreparedStatement statement)
                throws SQLException {
		Set<T> container = selectPrepared(connection,statement);
		if (container.isEmpty()) {
			throw new SQLException("No records found", "02000");
		}
		if (container.size()>1) {
			throw new SQLException("Multiple records found, disallowed");
		}
		return container.iterator().next();
	}
}
