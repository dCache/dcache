/*
 * dCache - http://www.dcache.org/
 *
 * Copyright (C) 2019 Deutsches Elektronen-Synchrotron
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
package dmg.cells.nucleus;

import com.google.common.collect.ImmutableMap;

import static com.google.common.base.Preconditions.checkArgument;

import dmg.util.CpuUsage;
import dmg.util.FractionalCpuUsage;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Provides the engine for calculating the CPU activity per cell.  Each domain
 * needs at most one instance of this class.
 */
public class CpuMonitoringTask implements Runnable
{
    private static final Logger LOGGER = LoggerFactory.getLogger(CpuMonitoringTask.class);

    private static final Duration DEFAULT_DELAY_BETWEEN_UPDATES = Duration.ofSeconds(2);
    private static final Duration MINIMUM_DELAY_BETWEEN_UPDATES = Duration.ofSeconds(1);
    private static final Duration MAXIMUM_DELAY_BETWEEN_UPDATES = Duration.ofSeconds(60);

    /**
     * An identifier for a thread.  Each thread has a numerical ID; however,
     * that id may be reused once the thread dies.  To mitigate against this
     * potential reuse, the thread's name and ThreadGroup are also compared.
     */
    private static class ThreadId
    {
        private final long id;
        private final String name;
        private final ThreadGroup group;

        ThreadId(Thread thread)
        {
            id = thread.getId();
            name = thread.getName();
            group = thread.getThreadGroup();
        }

        @Override
        public int hashCode()
        {
            return (int) id
                    ^ (int)(id >> 32)
                    ^ name.hashCode()
                    ^ group.hashCode();
        }

        @Override
        public boolean equals(Object other)
        {
            if (other == this) {
                return true;
            }

            if (!(other instanceof ThreadId)) {
                return false;
            }

            ThreadId otherId = (ThreadId) other;
            return otherId.id == this.id
                    && otherId.name.equals(this.name)
                    && otherId.group.equals(this.group);
        }

        public String toString()
        {
            return "{id=" + id + ", name=" + name + ", group=" + group + "}";
        }
    }

    /**
     * Holds information about a live thread.  This includes caching in which
     * cell the thread belongs, and the cumulative CPU usage as discovered
     * during the last run.
     */
    private static class ThreadInfo
    {
        private final String _cell;
        private CpuUsage _cpuUsage = new CpuUsage();

        ThreadInfo(String cell)
        {
            _cell = cell;
        }

        public String getCellName()
        {
            return _cell;
        }

        public Duration getTotal()
        {
            return _cpuUsage.getTotal();
        }

        public Duration getUser()
        {
            return _cpuUsage.getUser();
        }

        public Duration getSystem()
        {
            return _cpuUsage.getSystem();
        }

        public CpuUsage advanceTo(CpuUsage newValue)
        {
            if (newValue.getSystem().compareTo(getSystem()) < 0
                    || newValue.getUser().compareTo(getUser()) < 0) {
                // WTF, cpu usage has reduced.
                LOGGER.error("WORK_AROUND for updating {} to {}", _cpuUsage, newValue);
                Duration system = newValue.getSystem();
                if (system.compareTo(getSystem()) < 0) {
                    system = getSystem();
                }
                Duration user = newValue.getUser();
                if (user.compareTo(getUser()) < 0) {
                    user = getUser();
                }
                newValue = new CpuUsage(system, user);
                LOGGER.error("    updated new value {}", newValue);
            }
            CpuUsage difference = newValue.minus(_cpuUsage);
            _cpuUsage = newValue;
            return difference;
        }

        @Override
        public String toString()
        {
            return "C:" + _cell + ",CU:" + _cpuUsage;
        }
    }

    private final ThreadMXBean _threadMonitoring = ManagementFactory.getThreadMXBean();
    private final Map<ThreadId,ThreadInfo> _threadInfos = new HashMap<>();
    private final CellGlue _glue;
    private final ScheduledExecutorService _executor;

    private ScheduledFuture _task;
    private boolean _isFirstRun;
    private volatile Instant _lastUpdate;
    private boolean _wasThreadCpuTimeEnabled;

    private Duration _delayBetweenUpdates = DEFAULT_DELAY_BETWEEN_UPDATES;

    CpuMonitoringTask(CellGlue glue, ScheduledExecutorService service)
    {
        _glue = glue;
        _executor = service;
    }

    public void start()
    {
        if (!_threadMonitoring.isThreadCpuTimeSupported()) {
            throw new UnsupportedOperationException("Per-thread CPU " +
                    "monitoring not available in this JVM");
        }

        _wasThreadCpuTimeEnabled = _threadMonitoring.isThreadCpuTimeEnabled();
        if (!_wasThreadCpuTimeEnabled) {
            LOGGER.debug("Per-thread CPU monitoring not enabled; enabling it...");
            _threadMonitoring.setThreadCpuTimeEnabled(true);
        }

        if (_task == null) {
            LOGGER.debug("scheduling for every {}", _delayBetweenUpdates);
            _isFirstRun = true;
            _task = scheduleTask();
        }
    }


    public Duration getUpdateDelay()
    {
        return _delayBetweenUpdates;
    }

    public void setUpdateDelay(Duration value)
    {
        checkArgument(value.compareTo(MINIMUM_DELAY_BETWEEN_UPDATES) >= 0,
                "value too small");
        checkArgument(value.compareTo(MAXIMUM_DELAY_BETWEEN_UPDATES) <= 0,
                "value too large");

        _delayBetweenUpdates = value;

        if (_task != null) {
            LOGGER.debug("rescheduling for every {}", _delayBetweenUpdates);
            _task.cancel(true);
            _task = scheduleTask();
        }
    }

    private ScheduledFuture scheduleTask()
    {
        return _executor.scheduleWithFixedDelay(this, _delayBetweenUpdates.toMillis(),
                _delayBetweenUpdates.toMillis(), TimeUnit.MILLISECONDS);
    }

    public void stop()
    {
        if (_task != null) {
            LOGGER.debug("cancelling CPU profiling");
            _task.cancel(true);
            _task = null;
            _glue.setAccumulatedCellCpuUsage(Collections.emptyMap());
            _glue.setCurrentCellCpuUsage(Collections.emptyMap());
        }

        _threadMonitoring.setThreadCpuTimeEnabled(_wasThreadCpuTimeEnabled);
    }

    @Override
    public void run()
    {
        Instant thisUpdate = Instant.now();

        try {
            List<ThreadId> threadIds = discoverAllThreads();

            _threadInfos.keySet().retainAll(threadIds);

            Map<String,CpuUsage> cellCpuUsage = new HashMap<>();
            for (ThreadId id : threadIds) {
                ThreadInfo info = _threadInfos.computeIfAbsent(id, i -> new ThreadInfo(_glue.cellNameFor(i.group)));

                Optional<CpuUsage> cumulativeUsage = cumulativeUsage(id);

                LOGGER.error("Thread {} advanced from {} to {}", id, info, cumulativeUsage);

                cumulativeUsage.ifPresent(u -> {
                            CpuUsage increaseUsage = info.advanceTo(u);
                            String cell = info.getCellName();
                            cellCpuUsage.compute(cell, (k,v) -> v == null ? increaseUsage : v.plus(increaseUsage));
                        });
            }

            thisUpdate = Instant.now();

            _glue.setAccumulatedCellCpuUsage(cellCpuUsage);

            if (!_isFirstRun) {
                Duration elapsed = Duration.between(_lastUpdate, thisUpdate);
                Map<String,FractionalCpuUsage> fractionalUsage = cellCpuUsage.entrySet().stream()
                        .collect(Collectors.toMap(Map.Entry::getKey, e -> new FractionalCpuUsage(e.getValue(), elapsed)));
                _glue.setCurrentCellCpuUsage(fractionalUsage);
            }

        } catch (RuntimeException e) {
            LOGGER.warn("Failed:", e);
        }

        _isFirstRun = false;
        _lastUpdate = thisUpdate;
    }


    private Optional<CpuUsage> cumulativeUsage(ThreadId thread)
    {
        long totalNanos = _threadMonitoring.getThreadCpuTime(thread.id);
        long userNanos = _threadMonitoring.getThreadUserTime(thread.id);

        if (totalNanos == -1 || userNanos == -1) {
            // thread died between getOrCreateThreadInfo and getThread*Time methods
            return Optional.empty();
        }

        if (userNanos > totalNanos) {
            LOGGER.error("WORK_AROUND: userNanos ({}) larger than totalNanos ({})", userNanos, totalNanos);
            // This shouldn't happen, but some JVM implementations have
            // different resolutions for different types and seem to round value
            // up.  To compensate, we limit 'user' to 'total'.
            userNanos = totalNanos;
        }

        Duration user = Duration.ofNanos(userNanos);
        Duration system = Duration.ofNanos(totalNanos-userNanos);

        return Optional.of(new CpuUsage(system, user));
    }

    private List<ThreadId> discoverAllThreads()
    {
        return Thread.getAllStackTraces().keySet().stream()
                .map(ThreadId::new)
                .filter(i -> i.group != null)
                .collect(Collectors.toList());
    }
}
