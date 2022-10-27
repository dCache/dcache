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

import static dmg.util.CommandException.checkCommand;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.dcache.cells.HAServiceLeadershipManager.HA_NOT_LEADER_MSG;

import dmg.util.CommandException;
import dmg.util.command.Argument;
import dmg.util.command.Command;
import java.time.Duration;
import java.util.concurrent.Callable;
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

    @Command(name = "set refresh",
          hint = "Alters refresh rate and triggers a new run. Minimum rate is every 5 seconds." +
                "If no time is provided, the old one is kept.")
    public class SetRefreshCommand implements Callable<String> {

        @Argument(required = false, usage = "refresh time in seconds")
        Long refreshInterval;

        @Override
        public String call() throws CommandException {
            checkCommand(_hasHaLeadership, HA_NOT_LEADER_MSG);
            if (refreshInterval == null) {
                return "Refresh interval unchanged: " + _refreshInterval + " "
                      + _refreshIntervalUnit;
            }
            if (refreshInterval < 5) {
                throw new IllegalArgumentException("Time must be greater than 5 seconds");
            }

            setRefreshInterval(refreshInterval);
            setRefreshIntervalUnit(SECONDS);

            if (_cleanerTask != null) {
                _cleanerTask.cancel(true);
            }
            _cleanerTask = _executor.scheduleWithFixedDelay(() -> {
                try {
                    runDelete();
                } catch (InterruptedException e) {
                    LOGGER.info("Cleaner was interrupted");
                }
            }, _refreshInterval, _refreshInterval, _refreshIntervalUnit);
            return "Refresh set to " + _refreshInterval + " " + _refreshIntervalUnit;
        }
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
    public synchronized void isLeader() {
        _hasHaLeadership = true;
        scheduleCleanerTask();
    }

    @Override
    public synchronized void notLeader() {
        _hasHaLeadership = false;
        cancelCleanerTask();
    }

}