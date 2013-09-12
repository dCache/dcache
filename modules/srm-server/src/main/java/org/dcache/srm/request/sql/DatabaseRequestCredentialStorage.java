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

import org.globus.gsi.X509Credential;
import org.globus.gsi.gssapi.GlobusGSSCredentialImpl;
import org.gridforum.jgss.ExtendedGSSCredential;
import org.ietf.jgss.GSSCredential;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import org.dcache.srm.request.RequestCredential;
import org.dcache.srm.request.RequestCredentialStorage;
import org.dcache.srm.util.Configuration;

import static com.google.common.collect.Iterables.getFirst;
import static org.dcache.srm.request.sql.Utilities.getIdentifierAsStored;

/**
 *
 * @author  timur
 */
public class DatabaseRequestCredentialStorage implements RequestCredentialStorage {
    private static final Logger logger =
            LoggerFactory.getLogger(DatabaseRequestCredentialStorage.class);

   /** Creates a new instance of TestDatabaseJobStorage */
   private final JdbcTemplate jdbcTemplate;
   protected static final String stringType=" VARCHAR(32672)  ";
   protected static final String longType=" BIGINT ";
   protected static final String intType=" INTEGER ";
   protected static final String dateTimeType= " TIMESTAMP ";
   protected static final String booleanType= " INT ";
   private final String credentialsDirectory;

   public DatabaseRequestCredentialStorage(Configuration configuration)
           throws DataAccessException
   {
      this.credentialsDirectory = configuration.getCredentialsDirectory();
      this.jdbcTemplate = new JdbcTemplate(configuration.getDataSource());
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

   private void dbInit()
           throws DataAccessException
   {
       jdbcTemplate.execute(new ConnectionCallback<Void>()
       {
           @Override
           public Void doInConnection(Connection con) throws SQLException, DataAccessException
           {
               //get database info
               DatabaseMetaData md = con.getMetaData();
               String tableNameAsStored = getIdentifierAsStored(md, getTableName());
               try (ResultSet tableRs = md.getTables(null, null, tableNameAsStored, null)) {
                   if (!tableRs.next()) {
                       // Table does not exist
                       try (Statement s = con.createStatement()) {
                           logger.debug("dbInit trying " + createRequestCredentialTable);
                           s.executeUpdate(createRequestCredentialTable);
                       }
                   }
               }
               return null;
           }
       });
   }

    public static final String INSERT = "INSERT INTO "+requestCredentialTableName +
       " (id, creationtime, credentialname, role, numberofusers, delegatedcredentials, credentialexpiration) "+
       " VALUES ( ?,?,?,?,?,?,?) ";

    public void createRequestCredential(RequestCredential requestCredential) {
        GSSCredential credential = requestCredential.getDelegatedCredential();
        String credentialFileName = null;
        if (credential != null) {
            credentialFileName = credentialsDirectory + "/" + requestCredential.getId();
            writeCredentialInFile(credential, credentialFileName);
        }
        jdbcTemplate.update(INSERT,
                requestCredential.getId(),
                requestCredential.getCreationtime(),
                requestCredential.getCredentialName(),
                requestCredential.getRole(),
                requestCredential.getCredential_users(),
                credentialFileName,
                requestCredential.getDelegatedCredentialExpiration());
    }

    public static final String SELECT = "SELECT * FROM "+requestCredentialTableName +
        " WHERE ";

    private RequestCredential getRequestCredentialByCondition(String query, Object ...args)
            throws DataAccessException
    {
        List<RequestCredential> result = jdbcTemplate.query(query, args,
                new RowMapper<RequestCredential>()
                {
                    @Override
                    public RequestCredential mapRow(ResultSet rs, int rowNum) throws SQLException
                    {
                        RequestCredential credential =
                                new RequestCredential(rs.getLong("id"),
                                        rs.getLong("creationtime"),
                                        rs.getString("credentialname"),
                                        rs.getString("role"),
                                        fileNameToGSSCredentilal(rs
                                                .getString("delegatedcredentials")),
                                        rs.getLong("credentialexpiration"),
                                        DatabaseRequestCredentialStorage.this);
                        credential.setSaved(true);
                        return credential;
                    }
                });
        return getFirst(result, null);
    }

    public static final String SELECT_BY_ID = "SELECT * FROM "+requestCredentialTableName +
        " WHERE id=?";

    @Override
    public RequestCredential getRequestCredential(Long requestCredentialId) throws DataAccessException {
        return getRequestCredentialByCondition(SELECT_BY_ID, requestCredentialId);
    }

    public static final String SELECT_BY_NAME = "SELECT * FROM "+requestCredentialTableName +
        " WHERE credentialname=? and role is null";

    public static final String SELECT_BY_NAME_AND_ROLE = "SELECT * FROM "+requestCredentialTableName +
        " WHERE credentialname=? and role=?";


   @Override
   public RequestCredential getRequestCredential(String credentialName,
                                                 String role) throws DataAccessException {
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
   @Override
   public void saveRequestCredential(RequestCredential requestCredential)  {
       GSSCredential credential = requestCredential.getDelegatedCredential();
       String credentialFileName = null;
       if (credential != null) {
           credentialFileName = credentialsDirectory + "/" + requestCredential.getId();
           writeCredentialInFile(credential,credentialFileName);
       }
       int result = jdbcTemplate.update(UPDATE,
               requestCredential.getCreationtime(),
               requestCredential.getCredentialName(),
               requestCredential.getRole(),
               requestCredential.getCredential_users(),
               credentialFileName,
               requestCredential.getDelegatedCredentialExpiration(),
               requestCredential.getId());
       if (result == 0) {
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
           StringBuilder sb = new StringBuilder();
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
            X509Credential gc = new X509Credential(in);
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
        try (PreparedStatement stmt = connection.prepareStatement(query)) {
            for (int i = 0; i < args.length; i++) {
                stmt.setObject(i + 1, args[i]);
            }
            return stmt.executeUpdate();
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
