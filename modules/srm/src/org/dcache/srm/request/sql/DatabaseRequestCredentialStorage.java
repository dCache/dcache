// $Id: DatabaseRequestCredentialStorage.java,v 1.8 2007-08-03 15:47:59 timur Exp $
// $Log: not supported by cvs2svn $
// Revision 1.7  2007/03/08 23:36:55  timur
// merging changes from the 1-7 branch related to database performance and reduced usage of database when monitoring is not used
//
// Revision 1.6  2007/01/06 00:23:55  timur
// merging production branch changes to database layer to improve performance and reduce number of updates
//
// Revision 1.5.10.2  2007/03/08 01:14:04  timur
// make RequestCredentialStorage store delegated credential in file instead of database
//
// Revision 1.5.10.1  2007/01/04 02:58:55  timur
// changes to database layer to improve performance and reduce number of updates
//
// Revision 1.5  2005/07/22 17:32:55  leoheska
// srm-ls modifications
//
// Revision 1.4  2005/03/30 22:42:11  timur
// more database schema changes
//
// Revision 1.3  2005/03/07 22:55:33  timur
// refined the space reservation call, restored logging of sql commands while debugging the sql performance
//
// Revision 1.2  2005/02/21 20:48:54  timur
// use lowercase in tables' names as workaround the postgres jdbc driver bug
//
// Revision 1.1  2005/01/14 23:07:15  timur
// moving general srm code in a separate repository
//
// Revision 1.5  2004/12/17 18:45:54  timur
// make sure the connections are returned to connection pool even in case of exceptions
//
// Revision 1.4  2004/11/10 03:29:00  timur
// modified the sql code to be compatible with both Cloudescape and postges
//
// Revision 1.3  2004/10/28 02:41:31  timur
// changed the database scema a little bit, fixed various synchronization bugs in the scheduler, added interactive shell to the File System srm
//
// Revision 1.2  2004/08/06 19:35:24  timur
// merging branch srm-branch-12_May_2004 into the trunk
//
// Revision 1.1.2.2  2004/07/02 20:10:25  timur
// fixed the leak of sql connections, added propogation of srm errors
//
// Revision 1.1.2.1  2004/06/23 21:56:01  timur
// Get Requests are now stored in database, Request Credentials are now stored in database too
//
// Revision 1.1.2.3  2004/06/22 17:06:53  timur
// continue on database part
//
// Revision 1.1.2.2  2004/06/22 01:38:07  timur
// working on the database part, created persistent storage for getFileRequests, for the next requestId
//
// Revision 1.1.2.1  2004/06/18 22:20:52  timur
// adding sql database storage for requests
//
// Revision 1.1.2.2  2004/06/16 19:44:34  timur
// added cvs logging tags and fermi copyright headers at the top, removed Copier.java and CopyJob.java
//

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
import java.util.Set;
import org.dcache.srm.request.RequestCredential;
import org.dcache.srm.request.RequestCredentialStorage;
import org.ietf.jgss.GSSCredential;
import org.globus.gsi.GlobusCredential;
import org.globus.gsi.gssapi.GlobusGSSCredentialImpl;
import org.gridforum.jgss.ExtendedGSSCredential;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import org.dcache.srm.util.Configuration;
import org.dcache.srm.Logger;
import java.io.FileWriter;
import java.io.FileReader;
import java.io.File;
import java.io.IOException;
/**
 *
 * @author  timur
 */
public class DatabaseRequestCredentialStorage implements RequestCredentialStorage {
   
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
   protected Logger logger;
   public DatabaseRequestCredentialStorage(    Configuration configuration
      )  throws SQLException {
      this.jdbcUrl = configuration.getJdbcUrl();
      this.jdbcClass = configuration.getJdbcClass();
      this.user = configuration.getJdbcUser();
      this.pass = configuration.getJdbcPass();
      this.configuration = configuration;
      this.logger = configuration.getStorage();
      this.credentialsDirectory = configuration.getCredentialsDirectory();
      File dir = new File(credentialsDirectory);
      if(!dir.exists()) {
          dir.mkdir();
      }
      if(!dir.isDirectory() || !dir.canWrite()) {
          esay("credential directory "+credentialsDirectory+ 
                  " does not exist or is not writable");
      }
      dbInit();
   }
   
   public void say(String s){
      if(logger != null) {
         logger.log(" DatabaseRequestCredentialStorage: "+s);
      }
   }
   
   public void esay(String s){
      if(logger != null) {
         logger.elog(" DatabaseRequestCredentialStorage: "+s);
      }
   }
   
   public void esay(Throwable t){
      if(logger != null) {
         logger.elog(t);
      }
   }
   
   public String getTableName() {
      return requestCredentialTableName;
   }
   
   public static final String requestCredentialTableName = "srmrequestcredentials";
   
   public static final String createRequestCredentialTable =
      "CREATE TABLE "+requestCredentialTableName+" ( "+
      "ID "+         longType+" NOT NULL PRIMARY KEY,"+
      "CREATIONTIME "+        longType  +","+
      "CREDENTIALNAME "+        stringType+ ","+
      "ROLE "+           stringType+   ","+
      "NUMBEROFUSERS "+           intType+   ","+
      "DELEGATEDCREDENTIALS "+         stringType+    ","+
      "CREDENTIALEXPIRATION "+ longType+
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
         
         
         ResultSet tableRs = md.getTables(null, null, getTableName() , null );
         
         
         if(!tableRs.next()) {
            try {
               // Table does not exist
               //say(getTableName()+" does not exits");
               Statement s = _con.createStatement();
               say("dbInit trying "+createRequestCredentialTable);
               int result = s.executeUpdate(createRequestCredentialTable);
               s.close();
            } catch(SQLException sqle) {
               esay(sqle);
               esay("relation could already exist");
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
         esay(ex);
         throw new SQLException(ex.toString());
      } finally {
         if(_con != null) {
            pool.returnFailedConnection(_con);
         }
         
      }
   }
   
   
   
   
   
   public void createRequestCredential(RequestCredential requestCredential) {
      
      Statement sqlStatement =null;
      Connection _con = null;
      try {
         _con = pool.getConnection();
         try {
            sqlStatement = _con.createStatement();
            StringBuffer sb = new StringBuffer();
            sb.append("INSERT INTO ").append( getTableName()).append( " VALUES ( ");
            sb.append(requestCredential.getId());
            sb.append(", ");
            
            sb.append( requestCredential.getCreationtime()).append(", '");
            sb.append( requestCredential.getCredentialName()).append("', ");
            String tmp =requestCredential.getRole();
            if(tmp == null) {
               sb.append(" NULL, ");
            } else {
               sb.append(" '").append(tmp).append("', ");
            }
            sb.append( requestCredential.getCredential_users()).append(", ");
            GSSCredential credential = requestCredential.getDelegatedCredential();
            
            if(credential != null) {
                String credentialFileName = credentialsDirectory+"/"+
                        requestCredential.getId();
                writeCredentialInFile(credential,credentialFileName);
                sb.append(" '").append(credentialFileName).append("', ");
            } else {
                sb.append(" NULL, ");
            }
            sb.append( requestCredential.getDelegatedCredentialExpiration());
            sb.append(" ) ");
            String sqlStatementString = sb.toString();
            say("executing statement: "+sqlStatementString);
            int result = sqlStatement.executeUpdate( sqlStatementString );
            sqlStatement.close();
            _con.commit();
            return;
         } catch(SQLException sqle) {
            //esay("storageof requestCredential="+requestCredential+" failed with ");
            esay(sqle);
            try {
               _con.rollback();
               sqlStatement.close();
            } catch(SQLException sqle1) {
            }
            esay(sqle);
         }
         pool.returnConnection(_con);
         _con = null;
      } catch(SQLException sqle1) {
         if(_con != null) {
            pool.returnFailedConnection(_con);
            _con = null;
         }
         esay(sqle1);
      } finally {
         if(_con != null) {
            pool.returnFailedConnection(_con);
         }
         
      }
   }
   
   private RequestCredential getRequestCredentialByCondition(String condition) {
      Connection _con = null;
      try {
         _con = pool.getConnection();
         Statement sqlStatement = _con.createStatement();
         String sqlStatementString = "SELECT * FROM " + getTableName() +
            " WHERE "+condition;
         say("executing statement: "+sqlStatementString);
         ResultSet set = sqlStatement.executeQuery(sqlStatementString);
         if(!set.next()) {
             sqlStatement.close();
            return null;
         }
         
         Long ID = new Long(set.getLong(1));
         long CREATIONTIME = set.getLong(2);
         String CREDENTIALNAME = set.getString(3);
         String ROLE = set.getString(4);
         int NUMBEROFUSERS = set.getInt(5);
         String DELEGATEDCREDENTIALSFILENAME=set.getString(6);
         long CREDENTIALEXPIRATION=set.getLong(7);
         RequestCredential credential = new RequestCredential(
            ID,
            CREATIONTIME,
            CREDENTIALNAME,
            ROLE,
            fileNameToGSSCredentilal(DELEGATEDCREDENTIALSFILENAME),
            CREDENTIALEXPIRATION,
            this);
         credential.setSaved(true);
         set.close();
         sqlStatement.close();
         pool.returnConnection(_con);
         _con = null;
         return credential;
      } catch(Exception e) {
         if(_con != null) {
            pool.returnFailedConnection(_con);
            _con = null;
         }
         esay("deserialization of requestCredentialId satisfying condition= "+
            condition +" failed with ");
         esay(e);
         return null;
         
      } finally {
         if(_con != null) {
            pool.returnFailedConnection(_con);
         }
         
      }
   }
   
   
   
   public RequestCredential getRequestCredential(Long requestCredentialId) {
      return getRequestCredentialByCondition(" ID='"+requestCredentialId+"' ");
   }
   
   public RequestCredential getRequestCredential(String credentialName,
      String role) {
      //say("DatabaseRequestCredentialStorage.getRequestCredential("+credentialName+","+role+")");
      
      
      String condition;
      
      if(role == null || role.equalsIgnoreCase("null")) 
         condition = " CREDENTIALNAME='" +credentialName + "'";
         
      else condition = " CREDENTIALNAME='" + credentialName + "' AND " +
                       "ROLE='" + role + "' ";
      //
      // It used to work differently.  Replacing the below with the
      // above is a pretty big change.  Consult with Timur to make
      // sure that it didn't break something.
      //
      // String condition =" CREDENTIALNAME='"+credentialName+"' AND ";
      // if(role == null) condition += "ROLE is NULL";
      // else condition += "ROLE='" + role+ "' ";
      
      //say("condition ="+condition );
      return getRequestCredentialByCondition(condition);
   }
   
   public void saveRequestCredential(RequestCredential requestCredential)  {
      Statement sqlStatement=null;
      int result = 0;
      Connection _con = null;
      try {
         _con = pool.getConnection();
         try {
            
            sqlStatement = _con.createStatement();
            StringBuffer sb = new StringBuffer();
            sb.append("UPDATE ").append( getTableName()).append(" SET ID = ");
            sb.append( requestCredential.getId()).append(",");
            sb.append(" CREATIONTIME = ").append( requestCredential.getCreationtime()).append(",");
            sb.append(" CREDENTIALNAME = '").append(requestCredential.getCredentialName()).append("',");
            String tmp = requestCredential.getRole();
            if(tmp == null) {
               sb.append(" ROLE = NULL,");
            } else {
               sb.append(" ROLE = '").append(tmp).append("',");
            }
            sb.append(" NUMBEROFUSERS = ").append(requestCredential.getCredential_users()).append(",");
            GSSCredential credential = requestCredential.getDelegatedCredential();
            if(credential != null) {
                  String credentialFileName = credentialsDirectory+"/"+
                        requestCredential.getId();
                writeCredentialInFile(credential,credentialFileName);
               sb.append("DELEGATEDCREDENTIALS = '").append(credentialFileName).append("', ");
            } else {
               sb.append(" DELEGATEDCREDENTIALS =NULL, ");
            }
            sb.append(" CREDENTIALEXPIRATION = ").append(requestCredential.getDelegatedCredentialExpiration());
            sb.append(" WHERE ID='").append(requestCredential.getId()).append("'");
            String sqlStatementString  = sb.toString();
            //say(" sqlStatementString = \n"+sqlStatementString);
            result = sqlStatement.executeUpdate( sqlStatementString );
            sqlStatement.close();
            say("executeUpdate result is "+result);
            _con.commit();
         } catch(SQLException sqle) {
            esay("storageof requestCredential="+requestCredential+" failed with ");
            esay(sqle);
            try {
               _con.rollback();
               sqlStatement.close();
            } catch(SQLException sqle1) {
            }
         }
         pool.returnConnection(_con);
         _con = null;
      } catch(SQLException sqle1) {
         if(_con != null) {
            pool.returnFailedConnection(_con);
            _con = null;
         }
         esay(sqle1);
      } finally {
         if(_con != null) {
            pool.returnFailedConnection(_con);
         }
         
      }
      if(result == 0) {
         //say("update result is 0, calling createRequestCredential()");
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
           esay(ioe);
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
           esay(ioe);
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
            esay("error reading the credentials from database");
            esay(e);
         }
      }
      return null;
   }
   
   
}
