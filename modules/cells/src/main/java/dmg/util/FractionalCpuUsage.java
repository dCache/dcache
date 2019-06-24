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
package dmg.util;

import java.time.Duration;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Represents the percentage CPU usage within some time-period.  The values are
 * scaled by the number of cores, so the durations never exceed the quantum.
 *
 * A computer's CPU may have multiple cores while a single thread can utilise
 * only one core at any time.  Therefore a thread may be using 100% of the
 * available CPU, yet the computer will not be utilising 100% of available
 * resources.
 */
public class FractionalCpuUsage
{
    private final double _systemUsage;
    private final double _userUsage;
    private final Duration _quantum;

    /**
     * Create a new FractionalCpuUsage for CPU usage over some period.  The
     * CPU usage is scaled by the number of cores, so the time never exceeds
     * quantum.
     * @param usage The increase of CPU usage over a period
     * @param quantum The period over which CPU was consumed.
     */
    public FractionalCpuUsage(CpuUsage usage, Duration quantum)
    {
        int cores = Runtime.getRuntime().availableProcessors();

        Duration systemPerCore = usage.getSystem().dividedBy(cores);
        Duration userPerCore = usage.getUser().dividedBy(cores);

        checkArgument(systemPerCore.compareTo(quantum) <= 0,
                "system usage (%s) exceeds quantum (%s)", systemPerCore, quantum);
        checkArgument(userPerCore.compareTo(quantum) <= 0,
                "user usage (%s) exceeds quantum (%s)", userPerCore, quantum);

        _quantum = quantum;
        _systemUsage = systemPerCore.toNanos() / (double)quantum.toNanos();
        _userUsage = userPerCore.toNanos() / (double)quantum.toNanos();
    }


    /**
     * Fraction of time in quantum where CPU was active.
     * @return number [0..1]
     */
    public double getTotalUsage()
    {
        return _systemUsage + _userUsage;
    }


    /**
     * Fraction of time in quantum where CPU was active with user activity.
     * @return number [0..1]
     */
    public double getUserUsage()
    {
        return _userUsage;
    }


    /**
     * Fraction of time in quantum where CPU was active with system activity.
     * @return number [0..1]
     */
    public double getSystemUsage()
    {
        return _systemUsage;
    }


    /**
     * Duration between the two CPU consumption observations described by
     * this object.
     */
    public Duration getQuantum()
    {
        return _quantum;
    }
}
