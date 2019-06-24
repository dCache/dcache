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
 * An immutable store of CPU usage. CPU usage reports three metrics
 * with the following identity always holding:
 *
 *     total = user + system.
 *
 * User and system values are independently adjustable and non-negative
 * durations.
 * <p>
 * The stored CPU usage is not allowed to decreased and may be advanced in two
 * ways: by specifying a new value for both system and user usage, or by
 * specifying an increment to system and user usage.
 */
public class CpuUsage
{
    private final Duration _system;
    private final Duration _user;

    public CpuUsage()
    {
        this(Duration.ZERO, Duration.ZERO);
    }

    private static Duration requireNonNegative(Duration value, String message)
    {
        if (value.isNegative()) {
            throw new IllegalArgumentException(message);
        }
        return value;
    }

    public CpuUsage(Duration system, Duration user)
    {
        this._system = requireNonNegative(system, "system CPU usage may not be negative");
        this._user = requireNonNegative(user, "user CPU usage may not be negative");
    }

    /**
     * Return a CpuUsage that is the sum of this and other.
     * @param other the amount to increase the stored value
     * @return the combined CPU usage.
     */
    public CpuUsage plus(CpuUsage other)
    {
        return new CpuUsage(_system.plus(other._system), _user.plus(other._user));
    }

    /**
     * Return a CpuUsage that is the difference between this and other.
     * @param other the amount to subtract from the stored value
     * @return the difference in CPU usage.
     */
    public CpuUsage minus(CpuUsage other)
    {
        return new CpuUsage(_system.minus(other._system), _user.minus(other._user));
    }

    public Duration getTotal()
    {
        return _user.plus(_system);
    }

    public Duration getUser()
    {
        return _user;
    }

    public Duration getSystem()
    {
        return _system;
    }
}
