/*
 * dCache - http://www.dcache.org/
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
package org.dcache.util;

import static java.util.Objects.requireNonNull;

import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This class provides thread-safe logic that enables a task to be run multiple times.  The task is
 * run in a thread from the supplied Executor and there is (at most) a single thread running the
 * task at any one time.  For each run, it is NOT guaranteed with which thread the task will be run;
 * therefore, care must be taken if the task holds any locks.
 * <p>
 * The {@link #start()} method requests the task will be run or, if the task is already running,
 * requests an additional run once the current run has completed.
 */
public class RepeatableTaskRunner {

    private static final Logger LOGGER = LoggerFactory.getLogger(RepeatableTaskRunner.class);

    /**
     * The current state of the task runner.
     */
    private enum RunnerState {
        /**
         * The task is currently not running.
         */
        NOT_RUNNING,

        /**
         * The task is running and will terminate once finished.
         */
        FINAL_RUN,

        /**
         * The task is running and another run will start once the current run completes.
         */
        NONFINAL_RUN
    }

    private final AtomicReference<RunnerState> runner =
          new AtomicReference<>(RunnerState.NOT_RUNNING);
    private final Executor executor;
    private final Runnable task;

    /**
     * Build a new runner that can execute a task multiple times.  A single thread (at most) is used
     * from the Executor.  This thread is returned while the runner is idle.
     *
     * @param executor The Executor that will supply the Thread to run the task.
     * @param task     The execution to be performed.
     */
    public RepeatableTaskRunner(Executor executor, Runnable task) {
        this.executor = requireNonNull(executor);
        this.task = requireNonNull(task);
    }

    /**
     * Run the supplied task. If the task is not currently running then calling {@literal start}
     * will start the running of the task, otherwise the task will be executed once the current run
     * is complete.
     * <p>
     * The {@literal start} method does not block and may be called from within the task.
     * <p>
     * This method is idempotent in many cases.  Multiple calls to {@literal start} are equivalent
     * to a single call, provided a task run has not started since the previous {@literal start}
     * call.
     */
    public void start() {
        if (runner.compareAndSet(RunnerState.FINAL_RUN, RunnerState.NONFINAL_RUN)) {
            LOGGER.debug("Runner updated to non-final run");
        } else if (runner.compareAndSet(RunnerState.NOT_RUNNING, RunnerState.FINAL_RUN)) {
            LOGGER.debug("Executing task");
            executor.execute(this::runTask);
        }
    }

    private void runTask() {
        do {
            LOGGER.debug("Runner starting task");
            runner.set(RunnerState.FINAL_RUN);

            try {
                task.run();
            } catch (RuntimeException e) {
                LOGGER.error("Bug detected.  Please report this to support@dcache.org", e);
            }

        } while (!runner.compareAndSet(RunnerState.FINAL_RUN, RunnerState.NOT_RUNNING));
        LOGGER.debug("Runner terminating");
    }
}
