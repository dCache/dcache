/*
 * dCache - http://www.dcache.org/
 *
 * Copyright (C) 2018 Deutsches Elektronen-Synchrotron
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
package org.dcache.restful.util;

import java.util.ArrayList;
import java.util.List;

/**
 * A skeleton implementation that implements a close method and that allows for
 * registering "clean up" activity.
 */
public class CloseableWithTasks
{
    private final List<Runnable> closeTasks = new ArrayList<>();
    private boolean isClosed;

    /**
     * Run any registered onClose tasks.  This method may be called multiple
     * times and has the same effect as calling once.
     */
    public void close()
    {
        synchronized (closeTasks) {
            if (!isClosed) {
                isClosed = true;

                RuntimeException exception = null;
                for (Runnable task : closeTasks) {
                    try {
                        task.run();
                    } catch (RuntimeException e) {
                        if (exception == null) {
                            exception = e;
                        } else {
                            exception.addSuppressed(e);
                        }
                    }
                }

                if (exception != null) {
                    throw exception;
                }
            }
        }
    }

    public boolean isClosed()
    {
        synchronized (closeTasks) {
            return isClosed;
        }
    }


    /**
     * Register some activity that should run when close is called.  If close
     * has already been called then the task is executed straight away.
     */
    public final void onClose(Runnable task)
    {
        synchronized (closeTasks) {
            if (isClosed) {
                task.run();
            } else {
                closeTasks.add(task);
            }
        }
    }
}
