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
import java.util.Set;
import java.util.HashSet;
import java.sql.*;
import org.apache.log4j.Logger;
import diskCacheV111.services.JdbcConnectionPool;

public class DBManager {
	private static DBManager _instance=null;
	private JdbcConnectionPool connectionPool=null;
	private DBManager() { 
	}
	private static Logger _logger =
		Logger.getLogger("logger.org.dcache.db.sql");

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
		}
		catch (SQLException e) { 
			connectionPool.returnFailedConnection(connection);
			connection = null;
			throw e;
		}
		finally {
			if(connection != null) {
				connectionPool.returnConnection(connection);
			}
			
		}
		return set;
	}

	public Object selectForUpdate(Connection connection,
				      IoPackage pkg,
				       String query,
				       Object ... args) throws SQLException { 
		PreparedStatement stmt = connection.prepareStatement(query);
		for (int i = 0; i < args.length; i++) { 
			stmt.setObject(i + 1, args[i]);
		}
		Object o =  pkg.selectForUpdate(connection,stmt);
		return o;
	}

	public HashSet selectPrepared(IoPackage pkg, 
				      String query,
				      Object ... args) throws SQLException { 
		Connection connection = null;
		HashSet set=null;
		try { 
			connection = connectionPool.getConnection();
			PreparedStatement stmt = connection.prepareStatement(query);
			for (int i = 0; i < args.length; i++) { 
				stmt.setObject(i + 1, args[i]);
			}
			set =  pkg.selectPrepared(connection,stmt);
		}
		catch (SQLException e) { 
			connectionPool.returnFailedConnection(connection);
			connection = null;
			throw e;
		}
		finally {
			if(connection != null) {
				connectionPool.returnConnection(connection);
			}
			
		}
		return set;
	}


//------------------------------------------------------------------------------

	public Object selectPrepared(int columnIndex,
				     String query, 
				     Object ... args) throws SQLException { 
		Connection connection = null;
		Object obj=null;
		try { 
			connection = connectionPool.getConnection();
			PreparedStatement stmt = connection.prepareStatement(query);
			for (int i = 0; i < args.length; i++) { 
				stmt.setObject(i + 1, args[i]);
			}
			ResultSet set = stmt.executeQuery();
			if (set.next()) { 
				obj=set.getObject(columnIndex);
			}
		}
		catch (SQLException e) { 
			connectionPool.returnFailedConnection(connection);
			connection = null;
			throw e;
		}
		finally {
			if(connection != null) {
				connectionPool.returnConnection(connection);
			}
			
		}
		return obj;
	}


	public void createTable( String name,
				 String ... statements)
		throws SQLException {		
			Connection connection = null; 
			try { 
				connection = connectionPool.getConnection();
				DatabaseMetaData md = connection.getMetaData();
				ResultSet set = md.getTables(null, null, name, null);
				if (!set.next()) {
					try {
						for (String statement : statements) {
							Statement s = connection.createStatement();
							if (_logger.isDebugEnabled()) {
								_logger.debug("Executing  "+statement);
							}
							int result = s.executeUpdate(statement);
							connection.commit();
							s.close();
						}
					} 
					catch (SQLException e) {
						if (_logger.isDebugEnabled()) {
							_logger.debug("SQL Exception (relation could already exist): "+e.getMessage());
						}
						throw e;
					}
				}
				else { 
					throw new SQLException("Table \""+name+"\" already exist");
				}
			}
			catch (SQLException e) {
				connectionPool.returnFailedConnection(connection);
				connection = null;
				throw e;
			}
			finally {
				if(connection != null) {
					connectionPool.returnConnection(connection);
				}
				connection=null;
			}
	}

	public void createIndexes( String name,
				   String ... columns) 
		throws SQLException {		
		Connection connection = null; 
		try { 
				connection = connectionPool.getConnection();
				DatabaseMetaData md = connection.getMetaData();
				ResultSet set       = md.getIndexInfo(null, 
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
					String indexName=name.toLowerCase()+"_"+column+"_idx";
					String createIndexStatementText = "CREATE INDEX "+indexName+" ON "+name+" ("+column+")";
					Statement s = connection.createStatement();
					int result = s.executeUpdate(createIndexStatementText);
					connection.commit();
					s.close();
				}
		}
		catch (SQLException e) {
			connectionPool.returnFailedConnection(connection);
			connection = null;
			throw e;
		}
		finally {
			if(connection != null) {
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
			stmt.close();
			connection.commit();	
			connectionPool.returnConnection(connection);
			connection = null;
			return result;
		}
		catch (SQLException e) { 
			connection.rollback();
			connectionPool.returnFailedConnection(connection);
			connection = null;
			throw e;
		}
		finally {
			if(connection != null) {
				connectionPool.returnConnection(connection);
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
		PreparedStatement stmt =  connection.prepareStatement(query);
		for (int i = 0; i < args.length; i++)
			stmt.setObject(i + 1, args[i]);
		int result = stmt.executeUpdate();
		stmt.close();
		return result;
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
