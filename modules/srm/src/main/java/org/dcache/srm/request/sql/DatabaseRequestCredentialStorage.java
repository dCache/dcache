/*
COPYRIGHT STATUS:
  Dec 1st 2001, Fermi National Accelerator Laboratory (FNAL) documents and
  software are sponsored by the U.S. Department of Energy under Contract No.
  DE-AC02-76CH03000. Therefore, the U.S. Government retains a  world-wide
  non-exclusive, royalty-free license to publish or reproduce these documents
  and software for U.S. Government purposes.  All documents and software
  available from this server are protected under the U.S. and Foreign
  Copyright Laws, and FNAL reserves all rights.


 Distribution of the software available from this server is free of
 charge subject to the user following the terms of the Fermitools
 Software Legal Information.

 Redistribution and/or modification of the software shall be accompanied
 by the Fermitools Software Legal Information  (including the copyright
 notice).

 The user is asked to feed back problems, benefits, and/or suggestions
 about the software to the Fermilab Software Providers.


 Neither the name of Fermilab, the  URA, nor the names of the contributors
 may be used to endorse or promote products derived from this software
 without specific prior written permission.



  DISCLAIMER OF LIABILITY (BSD):

  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
  "AS IS" AND ANY EXPRESS OR IMPLIED  WARRANTIES, INCLUDING, BUT NOT
  LIMITED TO, THE IMPLIED  WARRANTIES OF MERCHANTABILITY AND FITNESS
  FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL FERMILAB,
  OR THE URA, OR THE U.S. DEPARTMENT of ENERGY, OR CONTRIBUTORS BE LIABLE
  FOR  ANY  DIRECT, INDIRECT,  INCIDENTAL, SPECIAL, EXEMPLARY, OR
  CONSEQUENTIAL DAMAGES  (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT
  OF SUBSTITUTE  GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR
  BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY  OF
  LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
  NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT  OF THE USE OF THIS
  SOFTWARE, EVEN IF ADVISED OF THE  POSSIBILITY OF SUCH DAMAGE.


  Liabilities of the Government:

  This software is provided by URA, independent from its Prime Contract
  with the U.S. Department of Energy. URA is acting independently from
  the Government and in its own private capacity and is not acting on
  behalf of the U.S. Government, nor as its contractor nor its agent.
  Correspondingly, it is understood and agreed that the U.S. Government
  has no connection to this software and in no manner whatsoever shall
  be liable for nor assume any responsibility or obligation for any claim,
  cost, or damages arising out of or resulting from the use of the software
  available from this server.


  Export Control:

  All documents and software available from this server are subject to U.S.
  export control laws.  Anyone downloading information from this server is
  obligated to secure any necessary Government licenses before exporting
  documents or software obtained from this server.
 */

/*
 * TestDatabaseJobStorage.java
 *
 * Created on April 26, 2004, 3:27 PM
 */

package org.dcache.srm.request.sql;
import java.sql.*;
import org.dcache.srm.request.RequestCredential;
import org.dcache.srm.request.RequestCredentialStorage;
import org.ietf.jgss.GSSCredential;
import org.globus.gsi.GlobusCredential;
import org.globus.gsi.gssapi.GlobusGSSCredentialImpl;
import org.gridforum.jgss.ExtendedGSSCredential;
import java.io.ByteArrayInputStream;
import org.dcache.srm.util.Configuration;
import java.io.FileWriter;
import java.io.FileReader;
import java.io.File;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.dcache.srm.request.sql.Utilities.getIdentifierAsStored;

/**
 *
 * @author  timur
 */
public class DatabaseRequestCredentialStorage implements RequestCredentialStorage {
    private static final Logger logger =
            LoggerFactory.getLogger(DatabaseRequestCredentialStorage.class);

   /** Creates a new instance of TestDatabaseJobStorage */
   private final String jdbcUrl;
   private final String jdbcClass;
   private final String user;
   private final String pass;
   private final Configuration configuration;
   protected static final String stringType=" VARCHAR(32672)  ";
   protected static final String longType=" BIGINT ";
   protected static final String intType=" INTEGER ";
   protected static final String dateTimeType= " TIMESTAMP ";
   protected static final String booleanType= " INT ";
   private final String credentialsDirectory;
   public DatabaseRequestCredentialStorage(    Configuration configuration
      )  throws SQLException {
      this.jdbcUrl = configuration.getJdbcUrl();
      this.jdbcClass = configuration.getJdbcClass();
      this.user = configuration.getJdbcUser();
      this.pass = configuration.getJdbcPass();
      this.configuration = configuration;
      this.credentialsDirectory = configuration.getCredentialsDirectory();
      File dir = new File(credentialsDirectory);
      if(!dir.exists()) {
          if(!dir.mkdir()) {
              logger.error("failed to create directory "+credentialsDirectory);
          }
      }
      if(!dir.isDirectory() || !dir.canWrite()) {
          logger.error("credential directory "+credentialsDirectory+
                  " does not exist or is not writable");
      }
      dbInit();
   }

   public String getTableName() {
      return requestCredentialTableName;
   }

   public static final String requestCredentialTableName = "srmrequestcredentials";

   public static final String createRequestCredentialTable =
      "CREATE TABLE "+requestCredentialTableName+" ( "+
      "id "+         longType+" NOT NULL PRIMARY KEY,"+
      "creationtime "+        longType  +","+
      "credentialname "+        stringType+ ","+
      "role "+           stringType+   ","+
      "numberofusers "+           intType+   ","+
      "delegatedcredentials "+         stringType+    ","+
      "credentialexpiration "+ longType+
      " )";


   JdbcConnectionPool pool;
   /**
    * in case the subclass needs to create/initialize more tables
    */

   private void dbInit()
   throws SQLException {
      Connection _con = null;

      try {
         pool = JdbcConnectionPool.getPool(jdbcUrl, jdbcClass, user, pass);
         //connect
         _con = pool.getConnection();
         _con.setAutoCommit(true);

         //get database info
         DatabaseMetaData md = _con.getMetaData();

         String tableNameAsStored =
             getIdentifierAsStored(md, getTableName());
         ResultSet tableRs = md.getTables(null, null, tableNameAsStored, null);

         if(!tableRs.next()) {
            try {
               // Table does not exist
               //logger.debug(getTableName()+" does not exits");
               Statement s = _con.createStatement();
               logger.debug("dbInit trying "+createRequestCredentialTable);
               int result = s.executeUpdate(createRequestCredentialTable);
               s.close();
            } catch(SQLException sqle) {
               logger.error(sqle.toString());
               logger.error("relation could already exist");
            }
         }
         // to be fast
         _con.setAutoCommit(false);
         pool.returnConnection(_con);
         _con =null;
      } catch (SQLException sqe) {
         if(_con != null) {
            pool.returnFailedConnection(_con);
            _con = null;
         }

         throw sqe;
      } catch (Exception ex) {
         if(_con != null) {
            pool.returnFailedConnection(_con);
            _con = null;
         }
         logger.error(ex.toString());
         throw new SQLException(ex.toString());
      } finally {
         if(_con != null) {
            pool.returnFailedConnection(_con);
         }

      }
   }

    public static final String INSERT = "INSERT INTO "+requestCredentialTableName +
       " (id, creationtime, credentialname, role, numberofusers, delegatedcredentials, credentialexpiration) "+
       " VALUES ( ?,?,?,?,?,?,?) ";

    public void createRequestCredential(RequestCredential requestCredential) {
        Statement sqlStatement =null;
        Connection _con = null;
        try {
            GSSCredential credential = requestCredential.getDelegatedCredential();
            String credentialFileName=null;
            if(credential != null) {
                credentialFileName = credentialsDirectory+"/"+
                    requestCredential.getId();
                writeCredentialInFile(credential,credentialFileName);
            }
            _con = pool.getConnection();
            int result = insert(_con,
                                INSERT,
                                requestCredential.getId(),
                                requestCredential.getCreationtime(),
                                requestCredential.getCredentialName(),
                                requestCredential.getRole(),
                                requestCredential.getCredential_users(),
                                credentialFileName,
                                requestCredential.getDelegatedCredentialExpiration());
            _con.commit();
        }
        catch (SQLException e) {
            if (_con!=null) {
                try {
                    _con.rollback();
                }
                catch(SQLException e1) { }
                pool.returnFailedConnection(_con);
                _con = null;
            }
        }
        finally {
            if (_con!=null) {
                pool.returnConnection(_con);
                _con = null;
            }
        }
    }

    public static final String SELECT = "SELECT * FROM "+requestCredentialTableName +
        " WHERE ";

    private RequestCredential getRequestCredentialByCondition(String query,
                                                              Object ...args) {
        Connection _con = null;
        RequestCredential credential = null;
        ResultSet set = null;
        PreparedStatement stmt=null;
        try {
            _con = pool.getConnection();
            stmt = prepare(_con, query,args);
            set = stmt.executeQuery();
            //
            // we expect a single record, so the loop below is fine
            //
            if(set.next()) {
                credential = new RequestCredential(set.getLong("id"),
                                                   set.getLong("creationtime"),
                                                   set.getString("credentialname"),
                                                   set.getString("role"),
                                                   fileNameToGSSCredentilal(set.getString("delegatedcredentials")),
                                                   set.getLong("credentialexpiration"),
                                                   this);
                credential.setSaved(true);
            }
         }
        catch (SQLException e) {
            if(_con != null) {
                        if ( set != null ) {
                            try {
                                    set.close();
                            }
                            catch (SQLException e1) {
                                    logger.debug("Failed to close ResultSet "+e1.getMessage());
                            }
                    }
                    if ( stmt != null ) {
                            try {
                                    stmt.close();
                            }
                            catch (SQLException e1) {
                                    logger.debug("Failed to close ResultSet "+e1.getMessage());
                            }
                    }
                pool.returnFailedConnection(_con);
                _con = null;
            }
        }
        catch (Exception e) {
            logger.error(e.toString());
        }
        finally {
            if (_con != null) {
            if ( set != null ) {
                try {
                    set.close();
                }
                catch (SQLException e1) {
                    logger.debug("Failed to close ResultSet "+e1.getMessage());
                }
            }
                    if ( stmt != null ) {
                            try {
                                    stmt.close();
                            }
                            catch (SQLException e1) {
                                    logger.debug("Failed to close ResultSet "+e1.getMessage());
                            }
                    }
                pool.returnConnection(_con);
                _con=null;
            }
        }
        return credential;
    }

    public static final String SELECT_BY_ID = "SELECT * FROM "+requestCredentialTableName +
        " WHERE id=?";

    public RequestCredential getRequestCredential(Long requestCredentialId) {
        return getRequestCredentialByCondition(SELECT_BY_ID,requestCredentialId.longValue());
    }

    public static final String SELECT_BY_NAME = "SELECT * FROM "+requestCredentialTableName +
        " WHERE credentialname=? and role is null";

    public static final String SELECT_BY_NAME_AND_ROLE = "SELECT * FROM "+requestCredentialTableName +
        " WHERE credentialname=? and role=?";


   public RequestCredential getRequestCredential(String credentialName,
                                                 String role) {
      if(role == null || role.equalsIgnoreCase("null")) {
          return getRequestCredentialByCondition(SELECT_BY_NAME,credentialName);
      }
      else {
          return getRequestCredentialByCondition(SELECT_BY_NAME_AND_ROLE,credentialName,role);
      }
   }

    private static final String UPDATE = "UPDATE " +requestCredentialTableName +
       " SET creationtime=?, credentialname=?, role=?, " +
       " numberofusers=?, delegatedcredentials=?, credentialexpiration=? where id=? ";

   public void saveRequestCredential(RequestCredential requestCredential)  {
      Statement sqlStatement=null;
      int result = 0;
      Connection _con = null;
      try {
          GSSCredential credential = requestCredential.getDelegatedCredential();
          String credentialFileName = null;
          if(credential != null) {
              credentialFileName = credentialsDirectory+"/"+
                  requestCredential.getId();
              writeCredentialInFile(credential,credentialFileName);
          }
          _con = pool.getConnection();
          result=update(_con,UPDATE,
                        requestCredential.getCreationtime(),
                        requestCredential.getCredentialName(),
                        requestCredential.getRole(),
                        requestCredential.getCredential_users(),
                        credentialFileName,
                        requestCredential.getDelegatedCredentialExpiration(),
                        requestCredential.getId());
          _con.commit();
      }
      catch (SQLException e) {
          logger.error(e.toString());
          if (_con!=null) {
              try {
                  _con.rollback();
              }
              catch(SQLException e1) {
                  logger.debug("Failed rollback connection "+e1.getMessage());
              }
              pool.returnFailedConnection(_con);
              _con = null;
          }
      }
      finally {
          if(_con != null) {
              pool.returnConnection(_con);
              _con = null;
          }
      }
      if(result == 0) {
         //logger.debug("update result is 0, calling createRequestCredential()");
         createRequestCredential(requestCredential);
      }
   }

   private void writeCredentialInFile(GSSCredential credential, String credentialFileName) {
       FileWriter writer = null;
       try {
           String credentialString = gssCredentialToString(credential);
           writer = new FileWriter(credentialFileName,
                   false);
            writer.write(credentialString);
            writer.close();
       }catch(IOException ioe) {
           if(writer != null) {
               try{
                writer.close();
               }catch(IOException ioe1) {
               }
           }
           logger.error(ioe.toString());
       }

   }

   private static String gssCredentialToString(GSSCredential credential) {
      try {
         if(credential != null && credential instanceof ExtendedGSSCredential) {
            byte [] data = ((ExtendedGSSCredential)(credential)).export(
               ExtendedGSSCredential.IMPEXP_OPAQUE);
            return new String(data);

         }
      } catch(Exception e) {
         System.err.println("conversion of credential to string failed with exception");
         e.printStackTrace();
      }
      return null;
   }
   private GSSCredential fileNameToGSSCredentilal(String fileName) {
       if(fileName == null) {
           return null;
       }
       FileReader reader = null;
       try {
           reader = new FileReader(fileName);
           StringBuffer sb = new StringBuffer();
           char[] cbuf = new char[1024];
           int len;
           while ( (len =reader.read(cbuf) ) != -1 ) {
               sb.append(cbuf,0,len);
           }
           reader.close();
           return stringToGSSCredential(sb.toString());

       } catch (IOException ioe) {
           if(reader != null) {
               try{
            reader.close();
               }catch(IOException ioe1) {
               }
           }
           // this is not an error, as the file will
           // written if it is not found
           // so we just make a debug log
           logger.debug("fileNameToGSSCredentilal("+fileName+") failed with "+ioe);
       }
       return null;
   }
   private GSSCredential stringToGSSCredential(String credential_string) {
      if(credential_string != null) {
         ByteArrayInputStream in = new ByteArrayInputStream(credential_string.getBytes());
         try {
            GlobusCredential gc = new GlobusCredential(in);
            GSSCredential cred =
               new GlobusGSSCredentialImpl(gc, GSSCredential.INITIATE_ONLY);
            return cred;
         } catch(Exception e) {
            logger.error("error reading the credentials from database");
            logger.error(e.toString());
         }
      }
      return null;
   }

    public static int update(Connection connection,
                             String query,
                             Object ... args)  throws SQLException {
        PreparedStatement stmt = null;
        try {
            stmt =  connection.prepareStatement(query);
            for (int i = 0; i < args.length; i++) {
                stmt.setObject(i + 1, args[i]);
            }
            return stmt.executeUpdate();
        }
        finally {
            if (stmt!=null) {
                stmt.close();
            }
        }
    }

    public static int delete(Connection connection,
                             String query,
                             Object ... args)  throws SQLException {
        return update(connection, query, args);
    }

    public static int insert(Connection connection,
                             String query,
                             Object ... args)  throws SQLException {
        return update(connection, query, args);
    }

    public static PreparedStatement prepare(Connection connection,
                                   String query,
                                   Object ... args)  throws SQLException {
            PreparedStatement stmt =  connection.prepareStatement(query);
            for (int i = 0; i < args.length; i++) {
                stmt.setObject(i + 1, args[i]);
            }
            return  stmt;
    }
}
