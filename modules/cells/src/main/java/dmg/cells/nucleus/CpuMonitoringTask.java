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
    }

    /**
     * Holds information about a live thread.  This includes caching in which
     * cell the thread belongs, and the cumulative CPU usage as discovered
     * during the last run.
     */
    private static class ThreadInfo
    {
        private final String _cell;
        private final CpuUsage _cpuUsage = new CpuUsage();

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
            return _cpuUsage.advanceTo(newValue);
        }
    }

    private final ThreadMXBean _threadMonitoring = ManagementFactory.getThreadMXBean();
    private final Map<ThreadId,ThreadInfo> _threadInfos = new HashMap<>();
    private final CellGlue _glue;
    private final ScheduledExecutorService _executor;

    private ScheduledFuture _task;
    private boolean _isFirstRun;
    private Instant _lastUpdate;
    private boolean _threadMonitoringWasEnabled;

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

        if (!_threadMonitoring.isThreadCpuTimeEnabled()) {
            LOGGER.debug("Per-thread CPU monitoring not enabled; enabling it...");
            _threadMonitoring.setThreadCpuTimeEnabled(true);
            _threadMonitoringWasEnabled = true;
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
            _glue.setAccumulatedCellCpuUsage(Collections.<String,CpuUsage>emptyMap());
            _glue.setCurrentCellCpuUsage(Collections.<String,FractionalCpuUsage>emptyMap());
        }

        if (_threadMonitoringWasEnabled) {
            _threadMonitoring.setThreadCpuTimeEnabled(false);
            _threadMonitoringWasEnabled = false;
        }
    }

    @Override
    public void run()
    {
        Instant thisUpdate = Instant.now();

        try {
            List<Thread> liveThreads = discoverAllThreads();

            List<ThreadId> liveThreadIds = new ArrayList<>(liveThreads.size());
            Map<String,CpuUsage> cellCpuUsage = new HashMap<>();
            for (Thread thread : liveThreads) {
                ThreadId id = new ThreadId(thread);
                liveThreadIds.add(id);
                ThreadInfo info = getOrCreateThreadInfo(id);

                Optional<CpuUsage> cumulativeUsage = cumulativeUsage(thread);

                cumulativeUsage.ifPresent(u -> {
                            CpuUsage increaseUsage = info.advanceTo(u);
                            String cell = info.getCellName();
                            cellCpuUsage.computeIfAbsent(cell, c -> new CpuUsage())
                                    .increaseBy(increaseUsage);
                        });
            }

            _threadInfos.keySet().retainAll(liveThreadIds);

            thisUpdate = Instant.now();
            Duration elapsed = Duration.between(_lastUpdate, thisUpdate);

            _glue.setAccumulatedCellCpuUsage(ImmutableMap.copyOf(cellCpuUsage));

            if (!_isFirstRun) {
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


    private Optional<CpuUsage> cumulativeUsage(Thread thread)
    {
        long totalNanos = _threadMonitoring.getThreadCpuTime(thread.getId());
        long userNanos = _threadMonitoring.getThreadUserTime(thread.getId());

        if (totalNanos == -1 || userNanos == -1) {
            // thread died between getOrCreateThreadInfo and getThread* methods
            return Optional.empty();
        }

        if (userNanos > totalNanos) {
            // This shouldn't happen, but some JVM implementations have
            // different resolutions for different types and seem to round value
            // up.  To compensate, we limit 'user' to 'total'.
            userNanos = totalNanos;
        }

        Duration user = Duration.ofNanos(userNanos);
        Duration system = Duration.ofNanos(totalNanos-userNanos);

        return Optional.of(new CpuUsage(system, user));
    }

    private ThreadInfo getOrCreateThreadInfo(ThreadId id)
    {
        return _threadInfos.computeIfAbsent(id, i -> new ThreadInfo(_glue.cellNameFor(i.group)));
    }

    private List<Thread> discoverAllThreads()
    {
        Map<Thread,StackTraceElement[]> stacktraces = Thread.getAllStackTraces();
        return new ArrayList<>(stacktraces.keySet());
    }
}
