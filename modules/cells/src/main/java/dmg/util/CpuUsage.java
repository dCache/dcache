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
 * A non-thread-safe mutable store of CPU usage. CPU usage reports three metrics
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
    private Duration _system;
    private Duration _user;

    public CpuUsage()
    {
        this(Duration.ZERO, Duration.ZERO);
    }

    public CpuUsage(Duration system, Duration user)
    {
        this._system = system;
        this._user = user;
    }

    /**
     * Increase the storage usage by some non-negative delta.
     * @param delta the amount to increase the stored value
     * @throws IllegalArgumentException if delta contains negative values
     */
    public void increaseBy(CpuUsage delta)
    {
        checkArgument(!delta._system.isNegative(), "increasing system duration by a negative value is not allowed");
        checkArgument(!delta._user.isNegative(), "increasing user duration by a negative value is not allowed");
        _system = _system.plus(delta._system);
        _user = _user.plus(delta._user);
    }

    /**
     * Increase the usage values to some new value.  The new values must not
     * be less than the existing stored values.
     * @param newValue the updated values
     * @throws IllegalArgumentException if new values are less than existing values
     * @return the difference between the current value and the updated value.
     */
    public CpuUsage advanceTo(CpuUsage newValue)
    {
        checkArgument(newValue._system.compareTo(_system) >= 0, "retrograde clock adjust to system duration");
        checkArgument(newValue._user.compareTo(_user) >= 0, "retrograde clock adjust to user duration");

        Duration oldSystem = _system;
        Duration oldUser = _user;

        _system = newValue._system;
        _user = newValue._user;

        Duration diffSystem = newValue._system.minus(oldSystem);
        Duration diffUser = newValue._user.minus(oldUser);

        return new CpuUsage(diffSystem, diffUser);
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
