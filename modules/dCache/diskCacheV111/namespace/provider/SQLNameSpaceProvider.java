/*
 * $Id: SQLNameSpaceProvider.java,v 1.14.4.1 2006-12-04 11:55:36 tigran Exp $
 */

package diskCacheV111.namespace.provider;

import dmg.util.Args;
import dmg.cells.nucleus.CellNucleus;
import diskCacheV111.util.*;
import diskCacheV111.namespace.*;

import java.util.List;
import java.util.Vector;
import java.sql.*;

public class SQLNameSpaceProvider implements  CacheLocationProvider {
    
    
    private Connection _con = null;
    private String _url = "jdbc:mysql://localhost/Himera";
    private String _driver = "org.gjt.mm.mysql.Driver";
    private String _user = "root";
    private String _pass = "";
    private String _pwdfile = null;
    private String _tableName = "cacheinfo";
    
    
    private final String _addCacheLocationSQL;
    private final String _getCacheLocationSQL;
    private final String _clearCacheLocationSQL;
    
    
    public SQLNameSpaceProvider(Args args, CellNucleus nucleus) throws Exception {                        
        
        //TODO: remove old style
        String cfURL = args.getOpt("dbURL");
        if( cfURL != null) {
            nucleus.esay("WARNING: deprecated option '-dbURL'. Use '-cachelocation-provider-dbURL'");
            _url = cfURL;
        }
        
        String cfDriver = args.getOpt("jdbcDrv");
        if( cfDriver != null ) {
            nucleus.esay("WARNING: deprecated option '-jdbcDrv'. Use '-cachelocation-provider-jdbcDrv'");
            _driver = cfDriver;
        }
        
        String cfUser = args.getOpt("dbUser");
        if( cfUser != null ) {
            nucleus.esay("WARNING: deprecated option '-dbUser'. Use '-cachelocation-provider-dbUser'");
            _user = cfUser;
        }
        
        String cfPass = args.getOpt("dbPass");
        if( cfPass != null ) {
            nucleus.esay("WARNING: deprecated option '-dbPass'. Use '-cachelocation-provider-dbPass'");
            _pass = cfPass;
        }
        
        String cfPwdfile = args.getOpt("pgPass");
        if( cfPwdfile != null ) {
            nucleus.esay("WARNING: deprecated option '-pgPass'. Use '-cachelocation-provider-pgPass'");
            _pwdfile = cfPwdfile;
        }

        // new style
        String __cfURL = args.getOpt("cachelocation-provider-dbURL");
        if( __cfURL != null) {
            _url = __cfURL;
        }
        
        String __cfDriver = args.getOpt("cachelocation-provider-jdbcDrv");
        if( __cfDriver != null ) {
            _driver = __cfDriver;
        }
        
        String __cfUser = args.getOpt("cachelocation-provider-dbUser");
        if( __cfUser != null ) {
            _user = __cfUser;
        }
        
        String __cfPass = args.getOpt("cachelocation-provider-dbPass");
        if( __cfPass != null ) {
            _pass = __cfPass;
        }
        
        String __cfPwdfile = args.getOpt("cachelocation-provider-pgPass");
        if( __cfPwdfile != null ) {
            _pwdfile = __cfPwdfile;
        }
        
        String __cfTableName = args.getOpt("cachelocation-provider-tableName");
        if( __cfTableName != null ) {
            _tableName = __cfTableName;
        }
        
        
        _addCacheLocationSQL = "INSERT INTO " + _tableName + " VALUES(?,?,?)";
        _getCacheLocationSQL = "SELECT pool FROM " + _tableName + " WHERE pnfsid=? ORDER BY ctime DESC";
        _clearCacheLocationSQL = "DELETE  FROM " + _tableName + " WHERE pnfsid=? AND pool LIKE ?";        
        
        
        this.dbInit(_url, _driver, _user, _pass, _pwdfile);        
    }    
    
    
    void dbInit(String jdbcUrl, String jdbcClass, String user, String pass, String pwdfile)  throws SQLException {
        
        if( (jdbcUrl == null )  || (jdbcClass == null) ||
        ( user == null) ||  (pass == null && pwdfile == null) ) {
            throw new
            IllegalArgumentException("Not enough arguments to Init SQL database");
        }
        
        if (pwdfile != null) {
            Pgpass pgpass = new Pgpass(pwdfile);      //VP
            pass = pgpass.getPgpass(jdbcUrl, user);   //VP
        }

        try {
            
            // Add driver to JDBC
            Class.forName(jdbcClass);
            
            _con = DriverManager.getConnection(jdbcUrl, user, pass);
            
        }
        catch (SQLException sqe) {
            sqe.printStackTrace();
            throw sqe;
        }
        catch (Exception ex) {
            ex.printStackTrace();
            throw new SQLException(ex.toString());
        }        
    }
    
    
    public void addCacheLocation(PnfsId pnfsId, String cacheLocation) {
        
    	PreparedStatement ps = null;
        try {
            
            connectIfNeeded();
                        
            ps = _con.prepareStatement(_addCacheLocationSQL);
                        
            ps.setString(1, pnfsId.toString() );
            ps.setString(2,  cacheLocation );
            ps.setTimestamp(3, new Timestamp( System.currentTimeMillis()) );
            
            int result = ps.executeUpdate( );

        }catch( SQLException e) {
        	String sqlState = e.getSQLState();
        	
        	// according to SQL-92 standart, class-code 23 is
        	// Constraint Violation, in our case
        	// same pool for the same file,
        	// which is OK
        	if( !sqlState.startsWith("23") ) {
        		e.printStackTrace();	
        	}            
        }finally{
        	tryToClose(ps);
        }
        
    }
    
    public void clearCacheLocation(PnfsId pnfsId, String cacheLocation, boolean removeIfLast) throws Exception {
        
    	PreparedStatement ps = null;
        try {
            
            connectIfNeeded();
                        
            ps = _con.prepareStatement(_clearCacheLocationSQL);
                        
            ps.setString(1, pnfsId.toString() );
            ps.setString(2,  cacheLocation.equals("*") ? "%" : cacheLocation );
            
            int result = ps.executeUpdate( );
            
            
        }catch( SQLException e) {
            e.printStackTrace();
        }finally{
        	tryToClose(ps);
        }
    }
    
    public List getCacheLocation(PnfsId pnfsId) throws Exception{
        
        List locations = new Vector();
        PreparedStatement ps = null;
        ResultSet result = null;
        try {
            
            connectIfNeeded();
            
            ps = _con.prepareStatement(_getCacheLocationSQL);                        
            ps.setString(1, pnfsId.toString() );
            
            result = ps.executeQuery();            
            
            while( result.next()) {
                locations.add( result.getString("pool") );
            }
            
            
        }catch( SQLException e) {
            e.printStackTrace();
        }finally{
        	tryToClose(result);
        	tryToClose(ps);
        }
        return locations;
        
    }    
    
    void connectIfNeeded() throws SQLException {
        
        if( _con == null || _con.isClosed() ) {
            dbInit( _url, _driver, _user, _pass, _pwdfile);
        }        
    }
        
    
    /**
     * database resource cleanup
     */
     static void tryToClose(ResultSet o) {
         try {
             if (o != null) o.close();
         } catch (Exception e) {
             
         }
     }    
    
    /**
     * database resource cleanup
     */

     static void tryToClose(Statement o) {
         try {
             if (o != null) o.close();
         } catch (SQLException e) {

         }
     }

   /**
     * database resource cleanup
     */
     static void tryToClose(PreparedStatement o) {
         try {
             if (o != null) o.close();
         } catch (SQLException e) {

         }
     }
     
   /**
     * database resource cleanup
     */    
     static void tryToClose(Connection o) {
         try {
             if (o != null) o.close();
         } catch (SQLException e) {

         }
     }    
    
    
    public String toString() {        
        return "$Id: SQLNameSpaceProvider.java,v 1.14.4.1 2006-12-04 11:55:36 tigran Exp $";        
    }
}
