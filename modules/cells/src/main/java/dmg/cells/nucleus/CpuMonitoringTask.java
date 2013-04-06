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

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.primitives.Longs;

import dmg.util.CpuUsage;
import dmg.util.FractionalCpuUsage;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 *  Provides the engine for calculating the CPU activity per cell.
 */
public class CpuMonitoringTask implements Runnable
{
    private static final Logger LOGGER = LoggerFactory.getLogger(CpuMonitoringTask.class);

    private static final Duration DEFAULT_DELAY_BETWEEN_UPDATES = Duration.ofSeconds(2);
    private static final Duration MINIMUM_DELAY_BETWEEN_UPDATES = Duration.ofSeconds(1);
    private static final Duration MAXIMUM_DELAY_BETWEEN_UPDATES = Duration.ofSeconds(60);

    /**
     * Holds information about a thread: caching information and allowing
     * calculation the amount of CPU used since last time.
     */
    private static class ThreadInfo
    {
        private final WeakReference<Thread> _thread;
        private final String _cell;
        private final CpuUsage _cpuUsage = new CpuUsage();

        ThreadInfo(Thread thread, String cell)
        {
            _thread = new WeakReference<>(thread);
            _cell = cell;
        }

        public String getCellName()
        {
            return _cell;
        }

        public boolean isAlive()
        {
            Thread thread = _thread.get();
            return thread != null && thread.isAlive();
        }

        public Duration getTotal()
        {
            return _cpuUsage.getCombined();
        }

        public Duration getUser()
        {
            return _cpuUsage.getUser();
        }

        public Duration setTotal(Duration newValue)
        {
            return _cpuUsage.setCombined(newValue);
        }

        public Duration setUser(Duration newValue)
        {
            return _cpuUsage.setUser(newValue);
        }

        public void assertValues()
        {
            _cpuUsage.assertValues();
        }
    }

    private final ThreadMXBean _threads = ManagementFactory.getThreadMXBean();
    private final Map<Long,ThreadInfo> _threadInfos = new HashMap<>();
    private final Map<String,CpuUsage> _cellCpuUsage = new HashMap<>();
    private final CellGlue _glue;
    private final ScheduledExecutorService _executor;

    private ScheduledFuture _task;
    private List<Thread> _allThreads;
    private boolean _isFirstRun;
    private Instant _lastUpdate;
    private boolean _threadMonitoringEnabled;

    private Duration _delayBetweenUpdates = DEFAULT_DELAY_BETWEEN_UPDATES;

    CpuMonitoringTask(CellGlue glue, ScheduledExecutorService service)
    {
        _glue = glue;
        _executor = service;
    }

    public void start()
    {
        if(!_threads.isThreadCpuTimeSupported()) {
            throw new UnsupportedOperationException("Per-thread CPU " +
                    "monitoring not available in this JVM");
        }

        if(!_threads.isThreadCpuTimeEnabled()) {
            LOGGER.debug("Per-thread CPU monitoring not enabled; enabling it...");
            _threads.setThreadCpuTimeEnabled(true);
            _threadMonitoringEnabled = true;
        }

        if(_task == null) {
            LOGGER.debug("scheduling for every {}", _delayBetweenUpdates);
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
        _isFirstRun = true;
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

        if (_threadMonitoringEnabled) {
            _threads.setThreadCpuTimeEnabled(false);
            _threadMonitoringEnabled = false;
        }
    }

    @Override
    public void run()
    {
        Instant thisUpdate = Instant.now();

        try {
            resetForQuantum();

            long[] threads = _threads.getAllThreadIds();

            for (long id : threads) {
                updateCellFromThread(id);
            }

            Duration duration = Duration.between(_lastUpdate, Instant.now());

            _cellCpuUsage.keySet().retainAll(aliveCellsAndNull());
            _threadInfos.keySet().retainAll(Longs.asList(threads));

            _glue.setAccumulatedCellCpuUsage(accumulatedCellCpuUsage());

            if(!_isFirstRun) {
                _glue.setCurrentCellCpuUsage(fractionalCellCpuUsage(duration));
            }

        } catch (RuntimeException e) {
            LOGGER.warn("Failed:", e);
        }

        _isFirstRun = false;
        _lastUpdate = thisUpdate;
    }


    private List<String> aliveCellsAndNull()
    {
        List<String> cells = _glue.getCellNames();
        cells.add(null);
        return cells;
    }


    private Map<String,CpuUsage> accumulatedCellCpuUsage()
    {
        Map<String,CpuUsage> result = new HashMap<>();
        for(Map.Entry<String,CpuUsage> e : _cellCpuUsage.entrySet()) {
            result.put(e.getKey(), e.getValue().clone());
        }
        return result;
    }


    private Map<String,FractionalCpuUsage> fractionalCellCpuUsage(Duration duration)
    {
        Map<String,FractionalCpuUsage> result = new HashMap<>();
        for(Map.Entry<String,CpuUsage> e : _cellCpuUsage.entrySet()) {
            String cell = e.getKey();
            CpuUsage usage = e.getValue();

            try {
                result.put(cell, new FractionalCpuUsage(usage, duration));
            } catch (RuntimeException re) {
                LOGGER.warn("Failed for {}: {}", cell, re.getMessage());
            }
        }
        return result;
    }


    private void updateCellFromThread(long id)
    {
        Optional<ThreadInfo> maybeInfo = getOrCreateThreadInfo(id);

        if (!maybeInfo.isPresent()) {
            // Give up: we couldn't identify this thread.
            return;
        }

        ThreadInfo info = maybeInfo.get();

        long totalNanos = _threads.getThreadCpuTime(id);
        long userNanos = _threads.getThreadUserTime(id);

        if(totalNanos == -1 || userNanos == -1) {
            // thread died between getOrCreateThreadInfo and getThread* methods
            return;
        }

        Duration total = Duration.ofNanos(totalNanos);
        Duration user = Duration.ofNanos(userNanos);

        if (user.compareTo(total) > 0) {
            // This shouldn't happen, but some JVM implementations have different
            // resolutions for different types and seem to round-up.  We limit
            // the 'user' value at 'total' to compensate.
            user = total;
        }

        Duration diffTotal = info.setTotal(total);
        Duration diffUser = info.setUser(user);
        info.assertValues();


        if (diffUser.compareTo(diffTotal) > 0) {
            // Again, this shouldn't happen, but due to resolution and
            // rounding problems, it does.  Use the same compensation strategy
            // of limiting diffUser to diffTotal.
            diffUser = diffTotal;
        }

        CpuUsage cellUsage = getOrCreateCpuUsageForCell(info.getCellName());

        cellUsage.addCombined(diffTotal);
        cellUsage.addUser(diffUser);
        cellUsage.assertValues();
    }

    private void resetForQuantum()
    {
        _cellCpuUsage.values().stream().forEach(CpuUsage::reset);
        _allThreads = discoverAllThreadsFromStackTraces();
    }

    private CpuUsage getOrCreateCpuUsageForCell(String cellName)
    {
        CpuUsage usage;

        if(_cellCpuUsage.containsKey(cellName)) {
            usage = _cellCpuUsage.get(cellName);
        } else {
            usage = new CpuUsage();
            _cellCpuUsage.put(cellName, usage);
        }

        return usage;
    }

    private Optional<ThreadInfo> getOrCreateThreadInfo(long id)
    {
        Optional<ThreadInfo> info = Optional.ofNullable(_threadInfos.get(id));

        if (!info.isPresent() || !info.get().isAlive()) {
            info = addNewThread(id);
        }

        return info;
    }

    private Optional<Thread> getThreadFromId(long id)
    {
        return _allThreads.stream()
                .filter(t -> t.getId() == id)
                .findAny();
    }

    private List<Thread> discoverAllThreadsFromStackTraces()
    {
        Set<Thread> threads = Thread.getAllStackTraces().keySet();
        return new ArrayList<>(threads);
    }


    private List<Thread> discoverAllThreadsFromThreadGroup()
    {
        ThreadGroup tg = Thread.currentThread().getThreadGroup();
        while(tg.getParent() != null) {
            tg = tg.getParent();
        }

        Thread[] list;
        do {
            list = new Thread[tg.activeCount()+20];
        } while(tg.enumerate(list) == list.length);

        return Arrays.asList(list);
    }


    private Optional<ThreadInfo> addNewThread(long id)
    {
        Optional<ThreadInfo> info = getThreadFromId(id)
                .map(t -> new ThreadInfo(t, _glue.cellNameFor(t)));
        info.ifPresent(i -> _threadInfos.put(id, i));
        return info;
    }
}
