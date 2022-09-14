/*
 * dCache - http://www.dcache.org/
 *
 * Copyright (C) 2017 - 2022 Deutsches Elektronen-Synchrotron
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
package org.dcache.chimera.namespace;

import dmg.cells.nucleus.CellPath;
import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import javax.sql.DataSource;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.dcache.cells.CellStub;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * Abstract base class representing common properties for DiskCleaner and HsmCleaner.
 */
public abstract class AbstractCleaner implements LeaderLatchListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractCleaner.class);

    protected ScheduledThreadPoolExecutor _executor;
    private ScheduledFuture<?> _cleanerTask;

    /**
     * CellStub used for sending messages to pools.
     */
    protected CellStub _poolStub;
    private DataSource _dataSource;

    /**
     * Set PoolInformationBase from which the request tracker learns about available pools.
     */
    protected PoolInformationBase _pools;
    protected JdbcTemplate _db;
    protected CellPath[] _deleteNotificationTargets;
    protected long _refreshInterval;
    protected TimeUnit _refreshIntervalUnit;

    /**
     * Time period that cleaner has to wait before a deleted file is removed by cleaner.
     */
    protected Duration _gracePeriod;

    protected boolean _hasHaLeadership = false;

    @Required
    public void setExecutor(ScheduledThreadPoolExecutor executor) {
        _executor = executor;
    }

    @Required
    public void setPoolStub(CellStub stub) {
        _poolStub = stub;
    }

    @Required
    public void setPoolInformationBase(PoolInformationBase pools) {
        _pools = pools;
    }

    @Required
    public void setDataSource(DataSource dataSource) {
        _dataSource = dataSource;
        _db = new JdbcTemplate(_dataSource);
    }

    @Required
    public void setReportRemove(String[] reportRemove) {
        _deleteNotificationTargets = Arrays.stream(reportRemove)
              .filter(t -> !t.isEmpty())
              .map(CellPath::new)
              .toArray(CellPath[]::new);
    }

    @Required
    public void setRefreshInterval(long refreshInterval) {
        _refreshInterval = refreshInterval;
    }

    @Required
    public void setRefreshIntervalUnit(TimeUnit refreshIntervalUnit) {
        _refreshIntervalUnit = refreshIntervalUnit;
    }

    @Required
    public void setGracePeriod(Duration gracePeriod) {
        _gracePeriod = gracePeriod;
    }

    protected abstract void runDelete() throws InterruptedException;

    private void scheduleCleanerTask() {
        _cleanerTask = _executor.scheduleWithFixedDelay(() -> {
                  try {
                      AbstractCleaner.this.runDelete();
                  } catch (InterruptedException e) {
                      LOGGER.info("Cleaner was interrupted");
                  } catch (DataAccessException e) {
                      LOGGER.error("Database failure: {}", e.getMessage());
                  } catch (IllegalStateException e) {
                      LOGGER.error("Illegal state: {}", e.getMessage());
                  }

              }, _refreshInterval, _refreshInterval,
              _refreshIntervalUnit);
    }

    private void cancelCleanerTask() {
        if (_cleanerTask != null) {
            _cleanerTask.cancel(true);
        }
    }

    @Override
    public void isLeader() {
        _hasHaLeadership = true;
        scheduleCleanerTask();
    }

    @Override
    public void notLeader() {
        _hasHaLeadership = false;
        cancelCleanerTask();
    }

}