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

package org.dcache.chimera.quota;

import diskCacheV111.util.RetentionPolicy;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import javax.sql.DataSource;
import org.dcache.util.FireAndForgetTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JdbcQuota implements QuotaHandler {

    private static final Logger LOGGER =
          LoggerFactory.getLogger(JdbcQuota.class);

    private final QuotaSqlDriver sqlDriver;
    private volatile Map<Integer, Quota> userQuotas;
    private volatile Map<Integer, Quota> groupQuotas;
    private ScheduledExecutorService quotaRefreshExecutor;

    public JdbcQuota(DataSource ds)
          throws SQLException {
        sqlDriver = QuotaSqlDriver.getDriverInstance(ds);
        userQuotas = sqlDriver.getUserQuotas();
        groupQuotas = sqlDriver.getGroupQuotas();
    }

    public void setQuotaRefreshExecutor(ScheduledExecutorService executor) {
        quotaRefreshExecutor = executor;
    }

    public void scheduleRefreshQuota() {
        ScheduledFuture<?> refreshUserQuota = quotaRefreshExecutor.
              scheduleWithFixedDelay(
                    new FireAndForgetTask(new Runnable() {
                        @Override
                        public void run() {
                            try {
                                refreshUserQuotas();
                            } catch (Exception ignore) {
                                LOGGER.warn("refreshUserQuotas failed {}", ignore.getMessage());
                            }
                        }
                    }),
                    60000,
                    60000,
                    TimeUnit.MILLISECONDS);

        ScheduledFuture<?> refreshGroupQuota = quotaRefreshExecutor.
              scheduleWithFixedDelay(
                    new FireAndForgetTask(new Runnable() {
                        @Override
                        public void run() {
                            try {
                                refreshGroupQuotas();
                            } catch (Exception ignore) {
                                LOGGER.warn("refreshGroupQuotas failed {}", ignore.getMessage());
                            }
                        }
                    }),
                    60000,
                    60000,
                    TimeUnit.MILLISECONDS);
    }


    @Override
    public Map<Integer, Quota> getUserQuotas() {
        return userQuotas;
    }

    @Override
    public Map<Integer, Quota> getGroupQuotas() {
        return groupQuotas;
    }

    @Override
    public void setUserQuota(Quota quota) {
        sqlDriver.setUserQuota(quota);
    }

    @Override
    public void createUserQuota(Quota quota) {
        sqlDriver.createUserQuota(quota);
    }

    @Override
    public void setGroupQuota(Quota quota) {
        sqlDriver.setGroupQuota(quota);
    }

    @Override
    public void createGroupQuota(Quota quota) {
        sqlDriver.createGroupQuota(quota);
    }

    @Override
    public void deleteUserQuota(int uid) {
        sqlDriver.deleteUserQuota(uid);
    }

    @Override
    public void deleteGroupQuota(int gid) {
        sqlDriver.deleteGroupQuota(gid);
    }

    @Override
    public boolean checkUserQuota(int uid, RetentionPolicy rp) {
        Quota quota = userQuotas.get(uid);
        if (quota == null) {
            return true;
        } else {
            return quota.check(rp);
        }
    }

    @Override
    public boolean checkGroupQuota(int gid, RetentionPolicy rp) {
        Quota quota = groupQuotas.get(gid);
        if (quota == null) {
            return true;
        } else {
            return quota.check(rp);
        }
    }

    @Override
    public void refreshUserQuotas() {
        LOGGER.debug("Running refreshUserQuotas.");
        Map<Integer, Quota> tmp = sqlDriver.getUserQuotas();
        userQuotas = tmp;
    }

    @Override
    public void refreshGroupQuotas() {
        LOGGER.debug("Running refreshGroupQuotas.");
        Map<Integer, Quota> tmp = sqlDriver.getGroupQuotas();
        groupQuotas = tmp;
    }

    @Override
    public void updateUserQuotas() {
        LOGGER.info("Running updateUserQuotas.");
        sqlDriver.updateUserQuota();
    }

    @Override
    public void updateGroupQuotas() {
        LOGGER.info("Running updateGroupQuotas.");
        sqlDriver.updateGroupQuota();
    }
}
