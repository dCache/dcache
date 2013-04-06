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
 * Represents a fractions of CPU time within some time-period.  Models CPU usage
 * logically as combined = user + system, with 'combined', 'user' and 'system'
 * as integers >= 0.
 *
 * A computer's CPU may have multiple cores while a single thread can utilise
 * only one core at any time.  Therefore a thread may be using 100% of the
 * available CPU, yet the computer will not be utilising 100% of available
 * resources.
 *
 * The model here looks at the CPU usage of the system as a whole; therefore
 * on a n-core machine, a thread can use, at most, 100/n percent of the
 * available CPU.
 *
 * Returned usage numbers are from  [0..1] with 'user' less than or equal to
 * 'combined'.
 */
public class FractionalCpuUsage
{
    private final double _combinedCpuUsage;
    private final double _userCpuUsage;
    private final Duration _quantum;

    /**
     *  quantum is the duration in which the CpuUsage was consumed.  The number
     * of cores is that of the machine that the JVM is running that creates
     * this object.
     */
    public FractionalCpuUsage(CpuUsage usage, Duration quantum)
    {
        this(usage.getCombined(), usage.getUser(), quantum,
                Runtime.getRuntime().availableProcessors());
    }

    public FractionalCpuUsage(Duration totalCpuUsage, Duration userCpuUsage,
            Duration quantum, int cores)
    {
        /* Since max CPU usage for quantum in NUMBER_OF_CORES*quantum, we
         * normalise the usage by averaging over the number or cores.  This
         * means that on a multi-core machine, a single thread will never
         * consume 100%
         */
        Duration totalPerCore = totalCpuUsage.dividedBy(cores);
        Duration userPerCore = userCpuUsage.dividedBy(cores);

        checkArgument(totalPerCore.compareTo(quantum) <= 0,
                "total (%s) usage exceeds quantum (%s)", totalPerCore, quantum);
        checkArgument(userPerCore.compareTo(quantum) <= 0,
                "user usage exceeds quantum");
        checkArgument(userPerCore.compareTo(totalPerCore) <= 0,
                "user usage (%s) exceeds total (%s)", userPerCore, totalPerCore);
        _quantum = quantum;
        _combinedCpuUsage = totalPerCore.toNanos() / (double)quantum.toNanos();
        _userCpuUsage = userPerCore.toNanos() / (double)quantum.toNanos();
    }


    /**
     * Fraction of time in quantum where CPU was active.
     * @return number [0..1]
     */
    public double getCombinedCpuUsage()
    {
        return _combinedCpuUsage;
    }


    /**
     * Fraction of time in quantum where CPU was active with user activity.
     * @return number [0..1]
     */
    public double getUserCpuUsage()
    {
        return _userCpuUsage;
    }


    /**
     * Fraction of time in quantum where CPU was active with system activity.
     * @return number [0..1]
     */
    public double getSystemCpuUsage()
    {
        return _combinedCpuUsage - _userCpuUsage;
    }


    /**
     * Length of quantum.
     */
    public Duration getQuantum()
    {
        return _quantum;
    }
}
