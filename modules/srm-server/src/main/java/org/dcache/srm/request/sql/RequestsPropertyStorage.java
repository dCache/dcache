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

package org.dcache.srm.request.sql;

import static com.google.common.base.Preconditions.checkState;

import javax.sql.DataSource;
import org.dcache.srm.scheduler.JobIdGenerator;
import org.dcache.srm.scheduler.JobIdGeneratorFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.IncorrectResultSizeDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * This class is used by srm to generate long and int ids that are guaranteed to be unique as long
 * as the database used by the class is preserved and can be connected to.
 *
 * @author timur
 */
public class RequestsPropertyStorage extends JobIdGeneratorFactory implements JobIdGenerator {

    private static final Logger LOGGER = LoggerFactory.getLogger(RequestsPropertyStorage.class);
    private static final int NEXT_INT_STEP = 1000;
    private static final long NEXT_LONG_STEP = 10000;
    private static RequestsPropertyStorage requestsPropertyStorage;

    private final JdbcTemplate jdbcTemplate;
    private final TransactionTemplate transactionTemplate;

    private int nextIntBase;
    private int nextIntIncrement = NEXT_INT_STEP;
    private long nextLongBase;
    private long nextLongIncrement = NEXT_LONG_STEP;

    /**
     * Creates a new instance of RequestsPropertyStorage
     */
    private RequestsPropertyStorage(PlatformTransactionManager transactionManager,
          DataSource dataSource)
          throws DataAccessException {
        this.transactionTemplate = new TransactionTemplate(transactionManager);
        this.jdbcTemplate = new JdbcTemplate(dataSource);
    }

    /**
     * Generate a next unique int request id Database table is used to preserve the state of the
     * request generator. In this table we store a single record with field NEXTINT We read the
     * value of this field and increase it by NEXT_INT_STEP (1000 by default), Thus reserving the
     * 1000 values for use by this instance. Once we used up all of the values we repeat the
     * database update. This is done to minimize the number of database operations.
     *
     * @return new int request id
     */
    public int getNextRequestId() {
        return nextInt();
    }

    public synchronized int nextInt() {
        if (nextIntIncrement >= NEXT_INT_STEP) {
            nextIntBase = transactionTemplate.execute(status -> {
                int base = jdbcTemplate.queryForObject(
                      "SELECT NEXTINT FROM srmnextrequestid FOR UPDATE", Integer.class);
                int newBase = base + NEXT_INT_STEP;
                int n = jdbcTemplate.update("UPDATE srmnextrequestid SET NEXTINT=?", newBase);
                if (n != 1) {
                    throw new IncorrectResultSizeDataAccessException(
                          "Unexpected number of rows got updated in srmnextrequestid", 1, n);
                }
                return newBase;
            });
            nextIntIncrement = 0;
        }
        int nextInt = nextIntBase + nextIntIncrement++;
        LOGGER.trace("RequestsPropertyStorage: return nextInt={}", nextInt);
        return nextInt;
    }

    @Override
    public long getNextId() {
        return nextInt();
    }

    /**
     * Generate a next unique long id Database table is used to preserve the state of the long id
     * generator. In this table we store a single record with field NEXTLONG We read the value of
     * this field and increase it by NEXT_LONG_STEP (10000 by default), thus reserving the 10000
     * values for use by this instance. Once we used up all of the values we repeat the database
     * update. This is done to minimize the number of database operations.
     *
     * @return next unique long number
     */
    @Override
    public synchronized long nextLong() {
        if (nextLongIncrement >= NEXT_LONG_STEP) {
            nextLongBase = transactionTemplate.execute(status -> {
                long base = jdbcTemplate.queryForObject(
                      "SELECT NEXTLONG FROM srmnextrequestid FOR UPDATE", Long.class);
                long newBase = base + NEXT_LONG_STEP;
                int n = jdbcTemplate.update("UPDATE srmnextrequestid SET NEXTLONG=?", newBase);
                if (n != 1) {
                    throw new IncorrectResultSizeDataAccessException(
                          "Unexpected number of rows got updated in srmnextrequestid", 1, n);
                }
                return newBase;
            });
            nextLongIncrement = 0;
        }

        long nextLong = nextLongBase + nextLongIncrement++;
        LOGGER.debug("RequestsPropertyStorage: return nextLong={}", nextLong);
        return nextLong;
    }

    @Override
    public JobIdGenerator getJobIdGenerator() {
        return this;
    }

    public static final synchronized void initPropertyStorage(
          PlatformTransactionManager transactionManager, DataSource dataSource)
          throws DataAccessException {
        checkState(RequestsPropertyStorage.requestsPropertyStorage == null,
              "RequestsPropertyStorage is already initialized");
        requestsPropertyStorage =
              new RequestsPropertyStorage(transactionManager, dataSource);
        initJobIdGeneratorFactory(requestsPropertyStorage);
    }
}
