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

import java.util.Iterator;
import java.util.HashSet;
import java.sql.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.dcache.util.JdbcConnectionPool;

public class DBManager {
	private static DBManager _instance=null;
	private JdbcConnectionPool connectionPool=null;
	private DBManager() {
	}
	private static Logger _logger =
		LoggerFactory.getLogger("logger.org.dcache.db.sql");

	synchronized public void initConnectionPool(String url,
				       String driver,
				       String user,
				       String password ) throws SQLException {

		connectionPool=JdbcConnectionPool.getPool(url,
							  driver,
							  user,
							  (password!=null?password:"srm"));

	}
	synchronized public static final DBManager getInstance()  {
		if ( DBManager._instance==null) {
			DBManager._instance = new DBManager();
		}
		return DBManager._instance;
	}

	public JdbcConnectionPool getConnectionPool() {
		return connectionPool;
	}


	public HashSet select(IoPackage pkg,
			  String query) throws SQLException {
		//
		// Handling of connection is localized here
		//
		Connection connection = null;
		HashSet set=null;
		try {
			connection = connectionPool.getConnection();
			set =  pkg.select(connection,query);
                        return set;
		}
		catch (SQLException e) {
			if (connection!=null){
				connectionPool.returnFailedConnection(connection);
				connection = null;
			}
			throw e;
		}
		finally {
			if(connection != null) {
				connectionPool.returnConnection(connection);
			}
		}
	}

	public Object selectForUpdate(Connection connection,
				      IoPackage pkg,
                                      String query,
                                      Object ... args) throws SQLException {
		PreparedStatement stmt = null;
                try {
                        stmt = connection.prepareStatement(query);
                        for (int i = 0; i < args.length; i++) {
                                stmt.setObject(i + 1, args[i]);
                        }
                        Object o =  pkg.selectForUpdate(connection,stmt);
                        return o;
                }
                finally {
                        if (stmt != null) {
                                try {
                                        stmt.close();
                                        stmt=null;
                                }
                                catch (SQLException e1) { }
                        }
                }
        }

	public HashSet selectPrepared(IoPackage pkg,
				      String query,
				      Object ... args) throws SQLException {
		Connection connection = null;
		HashSet set=null;
                PreparedStatement stmt=null;
		try {
			connection = connectionPool.getConnection();
			stmt = connection.prepareStatement(query);
			for (int i = 0; i < args.length; i++) {
				stmt.setObject(i + 1, args[i]);
			}
			set =  pkg.selectPrepared(connection,stmt);
                        stmt.close();
                        connectionPool.returnConnection(connection);
                        connection=null;
                        stmt=null;
                        return set;
		}
		finally {
			if(connection != null) {
                                if (stmt != null) {
                                        try {
                                                stmt.close();
                                                stmt=null;
                                        }
                                        catch (SQLException e1) { }
                                }
				connectionPool.returnFailedConnection(connection);
                                connection=null;
			}
		}
	}


//------------------------------------------------------------------------------

	public Object selectPrepared(int columnIndex,
				     String query,
				     Object ... args) throws SQLException {
		Connection connection = null;
		Object obj=null;
                PreparedStatement stmt=null;
                ResultSet set = null;
		try {
			connection = connectionPool.getConnection();
			stmt = connection.prepareStatement(query);
			for (int i = 0; i < args.length; i++) {
				stmt.setObject(i + 1, args[i]);
			}
			set = stmt.executeQuery();
			if (set.next()) {
				obj=set.getObject(columnIndex);
			}
                        //
                        // No need to close set - it is closed if stmt is closed
                        //
                        stmt.close();
                        connectionPool.returnConnection(connection);
                        connection=null;
                        stmt=null;
                        return obj;
		}
		finally {
			if(connection != null) {
                                if (stmt != null) {
                                        try {
                                                stmt.close();
                                                stmt=null;
                                        }
                                        catch (SQLException e1) { }
                                }
				connectionPool.returnFailedConnection(connection);
                                connection=null;
			}
		}
	}

	public boolean hasTable(String table) throws SQLException {
	    boolean hasTable = false;
        Connection connection = connectionPool.getConnection();
        try {
            hasTable = hasTable(connection, table);
            connectionPool.returnConnection(connection);
            connection=null;
        } finally {
            if(connection!= null) {
                connectionPool.returnFailedConnection(connection);
            }
        }

        return hasTable;
	}

	private boolean hasTable(Connection connection, String table) throws SQLException {
        DatabaseMetaData metadata = connection.getMetaData();
        ResultSet set = metadata.getTables(null, null, table, null);
        boolean hasTable = false;
        try {
            hasTable = set.next();
        } finally {
            set.close();
        }
        return hasTable;
	}

	public void createTable( String name,
				 String ... statements)	throws SQLException {
	    Connection connection = null;
	    ResultSet set = null;
	    try {
	        connection = connectionPool.getConnection();
	        if(hasTable(connection, name)) {
	            throw new SQLException("Table \"" + name + "\" already exists");
	        }

	        for (String statement : statements) {
	            Statement s=null;
	            try {
	                s = connection.createStatement();
	                if (_logger.isDebugEnabled()) {
	                    _logger.debug("Executing  "+statement);
	                }
	                s.executeUpdate(statement);
	                connection.commit();
	                s.close();
	                s=null;
	            }
	            catch (SQLException e) {
	                try {
	                    connection.rollback();
	                    if (s!=null) {
	                        s.close();
	                        s=null;
	                    }
	                }
	                catch (SQLException e1) { }
	                if (_logger.isDebugEnabled()) {
	                    _logger.debug("Failed: "+e.getMessage());
	                }
	            }
	        }
	    } finally {
	        if(connection!= null) {
	            if (set!=null) {
	                try {
	                    set.close();
	                    set=null;
	                }
	                catch (SQLException e1) { }
	            }
	            connectionPool.returnConnection(connection);
	            connection=null;
	        }
	    }
	}

	public void createIndexes( String name,
				   String ... columns)
		throws SQLException {
		Connection connection = null;
                ResultSet set = null;
		try {
				connection          = connectionPool.getConnection();
				DatabaseMetaData md = connection.getMetaData();
				set                 = md.getIndexInfo(null,
								      null,
								      name,
								      false,
								      false);
				HashSet<String> listOfColumnsToBeIndexed = new HashSet<String>();
				for (String column : columns) {
					listOfColumnsToBeIndexed.add(column.toLowerCase());
				}
				while(set.next()) {
					String s = set.getString("column_name").toLowerCase();
					if (listOfColumnsToBeIndexed.contains(s)) {
						listOfColumnsToBeIndexed.remove(s);
					}
				}
				for (Iterator<String> i=listOfColumnsToBeIndexed.iterator();i.hasNext();) {
					String column = i.next();
                                        //
                                        // column contains name(s) of columns on which we would like to
                                        // create an index. Comma separated column names imply creation
                                        // of compound index. Replace non alpha-numeric characters with "_".
                                        // an index will be named like :
                                        // tableName_column1_column2_idx
                                        //
                                        StringBuilder indexName = new StringBuilder();
                                        indexName.append(name.toLowerCase()).
                                                append("_").
                                                append(column.replaceAll("\\W","_")).
                                                append("_idx");
                                        StringBuilder createIndexStatementText=new StringBuilder();
                                        createIndexStatementText.append("CREATE INDEX ").
                                                append(indexName.toString()).
                                                append(" ON ").
                                                append(name).
                                                append(" (").
                                                append(column).
                                                append(")");
                                        Statement s=null;
                                        try {
                                                s = connection.createStatement();
                                                int result = s.executeUpdate(createIndexStatementText.toString());
                                                connection.commit();
                                                s.close();
                                                s=null;
                                        }
                                        catch (SQLException e) {
                                                try {
                                                        connection.rollback();
                                                        if (s!=null) {
                                                                s.close();
                                                                s=null;
                                                        }
                                                }
                                                catch (SQLException e1) { }
                                        }
                                }
                }
		catch (SQLException e) {
			if (connection!=null) {
                                if (set!=null) {
                                        try {
                                                set.close();
                                                set=null;
                                        }
                                        catch (SQLException e1) { }
                                }
				connectionPool.returnFailedConnection(connection);
				connection = null;
                        }
			throw e;
		}
		finally {
			if(connection != null) {
                                if (set!=null) {
                                        try {
                                                set.close();
                                                set=null;
                                        }
                                        catch (SQLException e1) { }
                                }
				connectionPool.returnConnection(connection);
				connection = null;
			}
		}
	}

	public int update(String query,
			  Object ... args) throws SQLException {
		Connection connection = null;
		PreparedStatement stmt = null;
		try {
			connection = connectionPool.getConnection();
			stmt = connection.prepareStatement(query);
			for (int i = 0; i < args.length; i++)
				stmt.setObject(i + 1, args[i]);
			int result = stmt.executeUpdate();
			connection.commit();
			stmt.close();
			connectionPool.returnConnection(connection);
			connection = null;
                        stmt=null;
			return result;
		}
		finally {
			if (connection!=null) {
                                try {
                                        connection.rollback();
                                        if (stmt != null) {
                                                stmt.close();
                                                stmt=null;
                                        }
                                }
                                catch (SQLException e1) { }
				connectionPool.returnConnection(connection);
                                connection=null;
			}
		}
	}

	public int delete(String query,Object ... args)  throws SQLException {
		return update(query,args);
	}

	public int insert(String query, Object ... args)  throws SQLException {
		return update(query,args);
	}

	public void batchUpdates(String ... statements) throws SQLException {
		for (String statement : statements) {
			try {
				update(statement);
			}
			catch (SQLException sql) {
				throw new SQLException("Statement "+statement+ " failed");
			}
		}
	}

	public void batchDeletes(String ... statements) throws SQLException {
		 batchUpdates(statements);
	}

	public void batchInserts(String ... statements) throws SQLException {
		batchUpdates(statements);
	}

//------------------------------------------------------------------------------
//    below the connection is exposed
//------------------------------------------------------------------------------

	public int update(Connection connection,
			   String query,
			   Object ... args)  throws SQLException {
                PreparedStatement stmt=null;
                try {
                        stmt =  connection.prepareStatement(query);
                        for (int i = 0; i < args.length; i++)
                                stmt.setObject(i + 1, args[i]);
                        return stmt.executeUpdate();
                }
                finally {
                        if (stmt != null) {
                                try {
                                        stmt.close();
                                        stmt=null;
                                }
                                catch (SQLException e1) { }
                        }
                }
        }

        public int delete(Connection connection,
			  String query,
                          Object ... args)  throws SQLException {
		return update(connection, query, args);
	}

	public int insert(Connection connection,
			  String query,
			  Object ... args)  throws SQLException {
		return update(connection, query, args);
	}
}
