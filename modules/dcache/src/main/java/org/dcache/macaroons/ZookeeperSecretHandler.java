/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2017 Deutsches Elektronen-Synchrotron
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.dcache.macaroons;

import com.google.common.base.Throwables;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.curator.utils.CloseableUtils;
import org.apache.curator.utils.ZKPaths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import dmg.cells.nucleus.CellAddressCore;
import dmg.cells.nucleus.CellIdentityAware;
import dmg.cells.nucleus.CellLifeCycleAware;

import org.dcache.cells.CuratorFrameworkAware;
import org.dcache.util.FireAndForgetTask;


/**
 * A SecretSupplier that uses Zookeeper as a back-end to share secrets
 * across multiple dCache cells.  All instances may add a new secret.
 * Leader election is used to identify a single instance that is responsible
 * for removing expired secrets.
 */
public class ZookeeperSecretHandler implements SecretHandler,
        CuratorFrameworkAware, CellIdentityAware, CellLifeCycleAware
{
    private static final Logger LOG = LoggerFactory.getLogger(ZookeeperSecretHandler.class);
    private static final long INITIAL_EXPIRATION_DELAY = TimeUnit.MINUTES.toMillis(5);
    private static final Duration MINIMUM_SECRET_VALIDITY = Duration.ofMinutes(5);

    public static final String ZK_MACAROONS = "/dcache/macaroons";

    private final String zkPath = ZKPaths.makePath(ZK_MACAROONS, "leader");

    private ZookeeperSecretStorage secrets;
    private long expirationPeriod;
    private TimeUnit expirationPeriodUnit;
    private ScheduledExecutorService executor;
    private CellAddressCore address;
    private LeaderLatch leaderLatch;
    private CuratorFramework client;

    /**
     * Class that starts or stops secret expiration depending on whether or not
     * the current client is the leader.
     */
    private class LeaderListener implements LeaderLatchListener
    {
        private final Runnable expirationTask = new FireAndForgetTask(secrets::removeExpiredSecrets);
        private ScheduledFuture<?> expirationFuture;

        @Override
        public void isLeader()
        {
            LOG.debug("Have become leader");
            expirationFuture = executor.scheduleWithFixedDelay(expirationTask,
                    INITIAL_EXPIRATION_DELAY, expirationPeriodUnit.toMillis(expirationPeriod),
                    TimeUnit.MILLISECONDS);
        }

        @Override
        public void notLeader()
        {
            LOG.debug("Have yielded leadership");
            expirationFuture.cancel(false);
        }
    }

    @Override
    public void setCuratorFramework(CuratorFramework client)
    {
        this.client = client;
    }

    @Required
    public void setExecutor(ScheduledExecutorService executor)
    {
        this.executor = executor;
    }

    @Override
    public void setCellAddress(CellAddressCore address)
    {
        this.address = address;
    }

    @Required
    public void setExpirationPeriod(long period)
    {
        expirationPeriod = period;
    }

    public long getExpirationPeriod()
    {
        return expirationPeriod;
    }

    @Required
    public void setExpirationPeriodUnit(TimeUnit unit)
    {
        expirationPeriodUnit = unit;
    }

    public TimeUnit getExpirationPeriodUnit()
    {
        return expirationPeriodUnit;
    }

    @Required
    public void setZookeeperSecretStorage(ZookeeperSecretStorage storage)
    {
        secrets = storage;
    }

    @Override
    public void afterStart()
    {
        try {
            leaderLatch = new LeaderLatch(client, zkPath, address.toString());
            leaderLatch.addListener(new LeaderListener());
            leaderLatch.start();
        } catch (Exception e) {
            Throwables.throwIfUnchecked(e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void beforeStop()
    {
        if (leaderLatch != null) {
            CloseableUtils.closeQuietly(leaderLatch);
        }
    }

    /**
     * To avoid having too many concurrent secrets, a secret's expiry may
     * be some time after the desired macaroon expiry time.  This method
     * calculates the actual secret expiry time.
     */
    private Instant secretExpiry(Instant macaroonExpiry)
    {
        Instant now = Instant.now();
        Duration validity = Duration.between(now, macaroonExpiry);

        /* Assuming many requests with the same validity, by doubling the
         * requested validity duration, there are (at most) two active
         * secrets at any time.
         *
         * For example:
         *
         *    ------ time increasing ------>
         *
         *   --[+++++++]-------------------------  request, needs new secret
         *   --[== SECRET A ==]------------------  new secret
         *
         *   --------[+++++++]-------------------  request, uses SECRET A
         *
         *   ---------[+++++++]------------------  request, uses SECRET A
         *
         *   ----------[+++++++]-----------------  request, needs new secret
         *   ----------[== SECRET B ==]----------  new secret
         *
         *   ----------------[+++++++]-----------  request, uses SECRET B
         *
         *   -----------------[+++++++]----------  request, uses SECRET B
         *
         *   ------------------[+++++++]---------  request, needs new secret
         *   ------------------[== SECRET C ==]--  new secret
         *
         * Note that, due to delays in garbage collecting, there may be
         * additional, expired secrets.
         */
        validity = validity.multipliedBy(2);

        /* To avoid too much stress on the secret-sharing infrastructure,
         * impose a minimum validity for this secret.
         */
        if (validity.compareTo(MINIMUM_SECRET_VALIDITY) < 0) {
            validity = MINIMUM_SECRET_VALIDITY;
        }

        return now.plus(validity);
    }

    @Override
    public IdentifiedSecret secretExpiringAfter(Instant expiry, Supplier<IdentifiedSecret> newSecret) throws Exception
    {
        Optional<IdentifiedSecret> existing = secrets.firstExpiringAfter(expiry);
        return existing.isPresent() ? existing.get() : secrets.put(secretExpiry(expiry), newSecret.get());
    }

    @Override
    public byte[] findSecret(String identifier)
    {
        return secrets.get(identifier);
    }
}
