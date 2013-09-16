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
 * ResuestsPropertyStorage.java
 *
 * Created on April 27, 2004, 4:23 PM
 */

package org.dcache.srm.request.sql;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.dcache.srm.scheduler.JobIdGenerator;
import org.dcache.srm.scheduler.JobIdGeneratorFactory;

import static org.dcache.srm.request.sql.Utilities.getIdentifierAsStored;

/**
 * This class is used by srm to generate long and int ids
 * that are guaranteed to be unique as long as the database used by the
 * class is preserved and can be connected to.
 * @author  timur
 */
public class RequestsPropertyStorage extends JobIdGeneratorFactory implements JobIdGenerator {

    private String jdbcUrl;
    private String jdbcClass;
    private String user;
    private String pass;
    private String nextRequestIdTableName;
    private static Logger logger = LoggerFactory.getLogger(RequestsPropertyStorage.class);
    private int nextIntBase;
    private static int NEXT_INT_STEP=1000;
    private int nextIntIncrement=NEXT_INT_STEP;
    private long nextLongBase;
    private static long NEXT_LONG_STEP=10000;
    private long nextLongIncrement=NEXT_LONG_STEP;
    private static RequestsPropertyStorage requestsPropertyStorage;

    /** Creates a new instance of RequestsPropertyStorage */
    private RequestsPropertyStorage(  String jdbcUrl,
            String jdbcClass,
            String user,
            String pass,
            String nextRequestIdTableName) {
        this.jdbcUrl = jdbcUrl;
        this.jdbcClass = jdbcClass;
        this.user = user;
        this.pass = pass;
        this.nextRequestIdTableName = nextRequestIdTableName;
        try{
            dbInit();

        } catch(SQLException sqle){
            sqle.printStackTrace();
        }
    }

    public void say(String s){
        logger.debug(" RequestsPropertyStorage: "+s);
        }

    public void esay(String s){
        logger.error(" RequestsPropertyStorage: "+s);
        }

    public void esay(Throwable t){
        logger.error("RequestsPropertyStorage",t);
        }

    JdbcConnectionPool pool;

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
                getIdentifierAsStored(md, nextRequestIdTableName);
            ResultSet tableRs =
                md.getTables(null, null, tableNameAsStored, null);

            //fields to be saved from the  Job object in the database:
                /*
                    this.id = id;
                    this.nextJobId = nextJobId;
                    this.creationTime = creationTime;
                    this.lifetime = lifetime;
                    this.state = state;
                    this.errorMessage = errorMessage;
                    this.creator = creator;

                 */
            if(!tableRs.next()) {
                try {
                    String createTable = "CREATE TABLE " + nextRequestIdTableName + "(" +
                            "NEXTINT INTEGER ,NEXTLONG BIGINT)";
                    say(nextRequestIdTableName+" does not exits");
                    Statement s = _con.createStatement();
                    say("dbInit trying "+createTable);
                    int result = s.executeUpdate(createTable);
                    s.close();
                    String select = "SELECT * FROM "+nextRequestIdTableName;
                    s = _con.createStatement();
                    ResultSet set = s.executeQuery(select);
                    if(!set.next()) {
                        s.close();
                        String insert = "INSERT INTO "+ nextRequestIdTableName+ " VALUES ("+Integer.MIN_VALUE+
                                ", "+Long.MIN_VALUE+")";
                        //say("dbInit trying "+insert);
                        s = _con.createStatement();
                        say("dbInit trying "+insert);
                        result = s.executeUpdate(insert);
                        s.close();
                    } else {
                        s.close();
                        say("dbInit set.next() returned nonnull");
                    }


                } catch(SQLException sqle) {
                    esay(sqle);
                    say("relation could already exist");
                }
            }
            // to be fast
            _con.setAutoCommit(false);
            pool.returnConnection(_con);
            _con = null;
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
            throw new SQLException(ex.toString());
        } finally {
            if(_con != null) {
                _con.setAutoCommit(false);
                pool.returnConnection(_con);
            }
        }
    }



    /**
     * Generate a next unique int request id
     * Databse table is used to preserve the state of the request generator.
     * In this table we store a single record with field NEXTINT
     * We read the value of this field and increase it by NEXT_INT_STEP (1000 by
     * default), Thus reserving the 1000 values for use by this instance.
     * Once we used up all of the values we repeat the database update.
     * This is done to minimize the number of database operations.
     * @return new int request id
     */
    public  int getNextRequestId()  {
        return nextInt();
    }
    int _nextIntBase;
    public synchronized  int nextInt()   {
        if(nextIntIncrement >= NEXT_INT_STEP) {
            nextIntIncrement =0;
            Connection _con = null;
            try {
                _con = pool.getConnection();
                String select_for_update = "SELECT * from "+nextRequestIdTableName+" FOR UPDATE ";
                Statement s = _con.createStatement();
                say("nextInt trying "+select_for_update);
                ResultSet set = s.executeQuery(select_for_update);
                if(!set.next()) {
                    s.close();
                    throw new SQLException("table "+nextRequestIdTableName+" is empty!!!");
                }
                nextIntBase = set.getInt(1);
                s.close();
                say("nextIntBase is ="+nextIntBase);
                String increase_nextint = "UPDATE "+nextRequestIdTableName+
                        " SET NEXTINT=NEXTINT+"+NEXT_INT_STEP;
                s = _con.createStatement();
                say("executing statement: "+increase_nextint);
                int i = s.executeUpdate(increase_nextint);
                s.close();
                _con.commit();
                pool.returnConnection(_con);
                _con = null;
            } catch(SQLException e) {
                e.printStackTrace();
                try{
                    _con.rollback();
                }catch(SQLException e1) {
                }
                nextIntBase = _nextIntBase;
            } finally {
                if(_con != null) {
                    pool.returnConnection(_con);
                }

            }
            _nextIntBase = nextIntBase+NEXT_INT_STEP;
        }
        int nextInt = nextIntBase +(nextIntIncrement++);
        say(" return nextInt="+nextInt);
        return nextInt;


    }

    private final SimpleDateFormat dateformat =
            new SimpleDateFormat("yyMMddHHmmssSSSSZ");

    public  String nextUniqueToken() throws SQLException{
        long nextLong = nextLong();
        return dateformat.format(new Date())+
                "-"+nextLong;
    }

    @Override
    public long getNextId() {
        return nextInt();
    }

    long _nextLongBase;
    /**
     * Generate a next unique long id
     * Databse table is used to preserve the state of the long id generator.
     * In this table we store a single record with field NEXTLONG
     * We read the value of this field and increase it by NEXT_LONG_STEP (10000
     * by default), thus reserving the 10000 values for use by this instance.
     * Once we used up all of the values we repeat the database update.
     * This is done to minimize the number of database operations.

     * @return next unique long number
     */
    @Override
    public synchronized long nextLong() {
        if(nextLongIncrement >= NEXT_LONG_STEP) {
            nextLongIncrement =0;
            Connection _con = null;
            String select_for_update = "SELECT * from "+nextRequestIdTableName+" FOR UPDATE ";
            try {
                _con = pool.getConnection();
                Statement s = _con.createStatement();
                say("nextLong trying "+select_for_update);
                ResultSet set = s.executeQuery(select_for_update);
                if(!set.next()) {
                    s.close();
                    throw new SQLException("table "+nextRequestIdTableName+" is empty!!!");
                }
                nextLongBase = set.getLong(2);
                s.close();
                say("nextLongBase is ="+nextLongBase);
                String increase_nextint = "UPDATE "+nextRequestIdTableName+
                        " SET NEXTLONG=NEXTLONG+"+NEXT_LONG_STEP;
                s = _con.createStatement();
                say("executing statement: "+increase_nextint);
                int i = s.executeUpdate(increase_nextint);
                s.close();
                _con.commit();
            } catch(SQLException e) {
                e.printStackTrace();
                try{
                    _con.rollback();
                }catch(Exception e1) {

                }
                pool.returnFailedConnection(_con);
                _con = null;
                nextLongBase = _nextLongBase;

            } finally {
                if(_con != null) {
                    pool.returnConnection(_con);

                }
            }
            _nextLongBase = nextLongBase+ NEXT_LONG_STEP;
        }

        long nextLong = nextLongBase +(nextLongIncrement++);
        say(" return nextLong="+nextLong);
        return nextLong;
    }

    @Override
    public boolean equals(Object o) {
        if( this == o) {
            return true;
        }

        if(o == null || !(o instanceof RequestsPropertyStorage)) {
            return false;
        }
        RequestsPropertyStorage rps = (RequestsPropertyStorage)o;
        return rps.jdbcClass.equals(jdbcClass) &&
        rps.jdbcUrl.equals(jdbcUrl) &&
        rps.pass.equals(pass) &&
        rps.user.equals(user) &&
        rps.nextRequestIdTableName.equals(nextRequestIdTableName);
    }

    @Override
    public int hashCode() {
        return jdbcClass.hashCode() ^
        jdbcUrl.hashCode() ^
        pass.hashCode() ^
        user.hashCode() ^
        nextRequestIdTableName.hashCode();
    }

    @Override
    public JobIdGenerator getJobIdGenerator() {
        return this;
    }

        public synchronized static final void initPropertyStorage(String jdbcUrl,
    String jdbcClass,
    String user,
    String pass,
    String nextRequestIdTableName)  {
        if(RequestsPropertyStorage.requestsPropertyStorage != null) {
            throw new IllegalStateException("RequestsPropertyStorage is already initialized");
            }
       requestsPropertyStorage  =   new RequestsPropertyStorage(jdbcUrl,jdbcClass,user,pass,
            nextRequestIdTableName);
       initJobIdGeneratorFactory(requestsPropertyStorage);

    }



}
