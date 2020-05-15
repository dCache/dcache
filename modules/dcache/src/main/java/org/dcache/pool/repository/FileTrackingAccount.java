/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2020 Deutsches Elektronen-Synchrotron
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
package org.dcache.pool.repository;

import com.google.common.base.Objects;
import com.google.common.collect.Interner;
import com.google.common.collect.Interners;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Named;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import diskCacheV111.util.PnfsId;

import org.dcache.util.Strings;

/**
 * A variation of Account that does file-based accounting.  This is
 * particularly useful in detecting places where a file's capacity usage goes
 * negative, potentially resulting in the overall pool account going negative.
 */
public class FileTrackingAccount extends Account
{
    private static final Logger LOGGER = LoggerFactory.getLogger(FileTrackingAccount.class);

    private static class AccountModifyingEvent
    {
        private final Instant when = Instant.now();
        private final List<StackTraceElement> stacktrace;
        private final String name;
        private final long delta;
        private final long newValue;

        AccountModifyingEvent(List<StackTraceElement> stacktrace, String name,
                long delta, long newValue)
        {
            this.stacktrace = stacktrace;
            this.name = name;
            this.delta = delta;
            this.newValue = newValue;
        }

        public boolean isSimilarTo(AccountModifyingEvent otherEvent)
        {
            return otherEvent != null && otherEvent.stacktrace.equals(stacktrace)
                    && otherEvent.name.equals(name) && otherEvent.delta == delta;
        }

        public void reportTo(StringBuilder sb, String prefix)
        {
            sb.append(prefix).append("At ").append(when);
            sb.append(" \u0394 ").append(name).append(" capacity of ").append(Strings.describeSize(delta));
            sb.append(" \u21D2 ").append(Strings.describeSize(newValue)).append("\n");

            for (StackTraceElement e : stacktrace) {
                sb.append(prefix).append("        ").append(e).append("\n");
            }
        }
    }

    private static class DelayedFileRemoval implements Delayed
    {
        private final Instant whenToRemove;
        private final PnfsId id;

        DelayedFileRemoval(Instant whenToRemove, PnfsId id)
        {
            this.whenToRemove = whenToRemove;
            this.id = id;
        }

        public long getDelay(TimeUnit unit)
        {
            Duration delay = Duration.between(Instant.now(), whenToRemove);
            return unit.convert(delay.toMillis(), TimeUnit.MILLISECONDS); // REVISIT is there no better way?
        }

        public int compareTo(Delayed o)
        {
            return Long.compare(getDelay(TimeUnit.MILLISECONDS), o.getDelay(TimeUnit.MILLISECONDS));
        }
    }

    private class FileSpaceUsage
    {
        private final long used;
        private final long precious;
        private final long removable;

        public FileSpaceUsage(PnfsId id)
        {
            this(0L, 0L, 0L);
        }

        private FileSpaceUsage(long used, long precious, long removable)
        {
            this.used = used;
            this.precious = precious;
            this.removable = removable;
        }

        private void checkChange(PnfsId id, long newValue, long delta, String name)
        {
            if (newValue < 0) {
                reportProblem("negative capacity", id, delta, newValue, name);
            } else if (delta == 0) {
                reportProblem("redundant update", id, delta, newValue, name);
            }
        }

        private void reportProblem(String problemDescription, PnfsId id, long delta, long newValue, String name)
        {
            StringBuilder sb = new StringBuilder();
            sb.append("Problem detected: ").append(problemDescription).append(".  Details follow...\n");
            sb.append(id).append(" currently has ").append(this).append(";");
            sb.append(" \u0394 ").append(name).append(" capacity of ").append(Strings.describeSize(delta));
            sb.append(" \u21D2 ").append(Strings.describeSize(newValue)).append("\n");
            List<AccountModifyingEvent> events = _accountEvents.get(id);
            if (events != null) {
                sb.append("Request history:\n");
                AccountModifyingEvent previousEvent = null;
                int suppressed = 0;
                for (AccountModifyingEvent e : events) {
                    if (e.isSimilarTo(previousEvent)) {
                        suppressed++;
                    } else {
                        if (suppressed > 0) {
                            sb.append("    .... and ").append(suppressed).append(" similar calls ...\n");
                            suppressed = 0;
                        }
                        e.reportTo(sb, "    ");
                        previousEvent = e;
                    }
                }
                if (suppressed > 0) {
                    sb.append("    .... and ").append(suppressed).append(" similar calls\n");
                }
            } else {
                sb.append("Request history lost");
            }
            LOGGER.warn(sb.toString());
        }

        public FileSpaceUsage withAdjustUsed(PnfsId id, long delta)
        {
            long newUsed = used + delta;
            checkChange(id, newUsed, delta, "used");
            return new FileSpaceUsage(newUsed, precious, removable);
        }

        public FileSpaceUsage withAdjustPrecious(PnfsId id, long delta)
        {
            long newPrecious = precious + delta;
            checkChange(id, newPrecious, delta, "precious");
            return new FileSpaceUsage(used, newPrecious, removable);
        }

        public FileSpaceUsage withAdjustRemovable(PnfsId id, long delta)
        {
            long newRemovable = removable + delta;
            checkChange(id, newRemovable, delta, "removable");
            return new FileSpaceUsage(used, precious, newRemovable);
        }

        public boolean isEmpty()
        {
            return used <= 0 && precious <= 0 && removable <= 0;
        }

        @Override
        public String toString()
        {
            return "[used " + Strings.describeSize(used)
                    + ", precious " + Strings.describeSize(precious)
                    + ", removable " + Strings.describeSize(removable) + "]";
        }
    }

    private final Map<PnfsId,FileSpaceUsage> _fileSizes = new HashMap<>();
    private final Interner<StackTraceElement> _canonicalStackTraceElements = Interners.newWeakInterner();
    private final Map<PnfsId,List<AccountModifyingEvent>> _accountEvents = new HashMap<>();
    private final Queue<DelayedFileRemoval> _delayedFileRemovals = new DelayQueue<DelayedFileRemoval>();
    private boolean isFirst = true;

    @Inject
    @Named("workerThreadPool")
    private ScheduledExecutorService _executor;
    private ScheduledFuture _backgroundRemoval;

    public FileTrackingAccount()
    {
        LOGGER.warn("File-tracking pool account activated!");
    }

    @PostConstruct
    public void start()
    {
        LOGGER.debug("Starting background removal");
        _backgroundRemoval = _executor.scheduleWithFixedDelay(this::checkForRemovals,
                    1, 1, TimeUnit.MINUTES);
    }

    @PreDestroy
    public void stop()
    {
        LOGGER.debug("Stopping background removal");
        _backgroundRemoval.cancel(true);
    }

    private List<StackTraceElement> getStacktrace()
    {
        StackTraceElement[] stacktrace = Thread.currentThread().getStackTrace();
        return Arrays.stream(stacktrace)
                .map(_canonicalStackTraceElements::intern)
                .collect(Collectors.toList());
    }

    private void checkForRemovals()
    {
        LOGGER.debug("Checking for removals");

        while (true) {
            DelayedFileRemoval removal = _delayedFileRemovals.poll();
            if (removal == null) {
                break;
            }
            PnfsId id = removal.id;
            LOGGER.debug("Removing accounting information about {}", id);
            _accountEvents.remove(id);
            _fileSizes.remove(id);
        }
    }

    private void storeFileUsage(PnfsId id, FileSpaceUsage newUsage)
    {
        if (newUsage.isEmpty()) {
            Instant whenToRemove = Instant.now().plus(1, ChronoUnit.MINUTES);
            _delayedFileRemovals.add(new DelayedFileRemoval(whenToRemove, id));
        }
        _fileSizes.put(id, newUsage);
    }

    private void adjustFileUsed(PnfsId id, long delta)
    {
        FileSpaceUsage oldUsage = _fileSizes.computeIfAbsent(id, FileSpaceUsage::new);
        logEvent(id, getStacktrace(), "used", delta, oldUsage.used + delta);
        FileSpaceUsage newUsage = oldUsage.withAdjustUsed(id, delta);
        storeFileUsage(id, newUsage);
    }

    private void adjustFileRemovable(PnfsId id, long delta)
    {
        FileSpaceUsage oldUsage = _fileSizes.computeIfAbsent(id, FileSpaceUsage::new);
        logEvent(id, getStacktrace(), "removable", delta, oldUsage.removable + delta);
        FileSpaceUsage newUsage = oldUsage.withAdjustRemovable(id, delta);
        storeFileUsage(id, newUsage);
    }

    private void adjustFilePrecious(PnfsId id, long delta)
    {
        FileSpaceUsage oldUsage = _fileSizes.computeIfAbsent(id, FileSpaceUsage::new);
        logEvent(id, getStacktrace(), "precious", delta, oldUsage.precious + delta);
        FileSpaceUsage newUsage = oldUsage.withAdjustPrecious(id, delta);
        storeFileUsage(id, newUsage);
    }

    private void logEvent(PnfsId id, List<StackTraceElement> stacktrace,
            String name, long delta, long newValue)
    {
        List<AccountModifyingEvent> log = _accountEvents.computeIfAbsent(id, k -> new ArrayList());
        log.add(new AccountModifyingEvent(stacktrace, name, delta, newValue));
    }


    @Override
    public synchronized void free(PnfsId id, long space)
    {
        super.free(id, space);
        adjustFileUsed(id, -space);
    }

    @Override
    public synchronized boolean allocateNow(PnfsId id, long request)
             throws InterruptedException
    {
        boolean result = super.allocateNow(id, request);
        adjustFileUsed(id, request);
        return result;
    }

    @Override
    public synchronized void allocate(PnfsId id, long request)
             throws InterruptedException
    {
        super.allocate(id, request);
        adjustFileUsed(id, request);
    }

    @Override
    public synchronized void growTotalAndUsed(PnfsId id, long delta)
    {
        super.growTotalAndUsed(id, delta);
        adjustFileUsed(id, delta);
    }

    @Override
    public synchronized void adjustRemovable(PnfsId id, long delta)
    {
        super.adjustRemovable(id, delta);
        adjustFileRemovable(id, delta);
    }

    @Override
    public synchronized void adjustPrecious(PnfsId id, long delta)
    {
        super.adjustPrecious(id, delta);
        adjustFilePrecious(id, delta);
    }
}
