/*
 * dCache - http://www.dcache.org/
 *
 * Copyright (C) 2017 - 2019 Deutsches Elektronen-Synchrotron
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;

import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import dmg.cells.nucleus.CellPath;

import org.dcache.cells.CellStub;


/**
 *
 * Abstract base class representing common properties for DiskCleaner and HsmCleaner.
 *
 */
public abstract class AbstractCleaner
{

    private static final Logger _log =
            LoggerFactory.getLogger(DiskCleaner.class);
    protected ScheduledExecutorService _executor;
    /**
     * CellStub used for sending messages to pools.
     */
    protected CellStub _poolStub;
    private DataSource _dataSource;

    /**
     * Set PoolInformationBase from which the request tracker learns
     * about available pools.
     */
    protected PoolInformationBase _pools;
    protected JdbcTemplate _db;
    protected CellPath[] _deleteNotificationTargets;
    protected long _refreshInterval;
    protected TimeUnit _refreshIntervalUnit;

    /**
     * Time period that cleaner have to wait before deleted file is
     * removed by cleaner.
     */
    protected Duration _gracePeriod;

    @Required
    public void setExecutor(ScheduledExecutorService executor)
    {
        _executor = executor;
    }

    @Required
    public void setPoolStub(CellStub stub)
    {
        _poolStub = stub;
    }

    @Required
    public void setPoolInformationBase(PoolInformationBase pools)
    {
        _pools = pools;
    }

    @Required
    public void setDataSource(DataSource dataSource)
    {
        _dataSource = dataSource;
        _db = new JdbcTemplate(_dataSource);
    }

    @Required
    public void setReportRemove(String[] reportRemove)
    {
        _deleteNotificationTargets = Arrays.stream(reportRemove)
                .filter(t -> !t.isEmpty())
                .map(CellPath::new)
                .toArray(CellPath[]::new);
    }

    @Required
    public void setRefreshInterval(long refreshInterval)
    {
        _refreshInterval = refreshInterval;
    }

    @Required
    public void setRefreshIntervalUnit(TimeUnit refreshIntervalUnit)
    {
        _refreshIntervalUnit = refreshIntervalUnit;
    }

    @Required
    public void setGracePeriod(Duration gracePeriod) {
        _gracePeriod = gracePeriod;
    }

    protected abstract void runDelete() throws InterruptedException;


    public void init() {
        _executor.scheduleWithFixedDelay(() -> {

                    try {
                        AbstractCleaner.this.runDelete();
                    } catch (InterruptedException e) {
                        _log.info("Cleaner was interrupted");
                    } catch (DataAccessException e) {
                        _log.error("Database failure: {}", e.getMessage());
                    }

                }, _refreshInterval, _refreshInterval,
                _refreshIntervalUnit);
    }
}
