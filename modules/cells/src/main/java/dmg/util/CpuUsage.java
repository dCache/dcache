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
import static com.google.common.base.Preconditions.checkState;

/**
 * Store the accumulated CPU usage.  Models CPU usage as:
 *
 *     combined = user + system.
 *
 * Allows stored time to advance in two ways: by specifying a new value (which
 * return the increment) or by specifying an increment (which returns the new
 * value).
 *
 * Since user and combined values may be updated independently, there is a
 * separate assert method that may be called once both values have been
 * updated.
 */
public class CpuUsage implements Cloneable
{
    private Duration _combined;
    private Duration _user;

    public void reset()
    {
        _combined = Duration.ZERO;
        _user = Duration.ZERO;
    }

    @Override
    public CpuUsage clone()
    {
        CpuUsage cloned = new CpuUsage();
        cloned._combined = _combined;
        cloned._user = _user;
        return cloned;
    }

    public Duration addCombined(Duration delta)
    {
        checkArgument(!delta.isNegative(), "negative delta not allowed");
        _combined = _combined.plus(delta);
        return _combined;
    }

    public Duration addUser(Duration delta)
    {
        checkArgument(!delta.isNegative(), "negative delta not allowed");
        _user = _user.plus(delta);
        return _user;
    }

    public Duration setCombined(Duration newValue)
    {
        Duration oldValue = _combined;
        checkArgument(newValue.compareTo(oldValue) >= 0, "retrograde clock detected");
        _combined = newValue;
        return newValue.minus(oldValue);
    }

    public Duration setUser(Duration newValue)
    {
        Duration oldValue = _user;
        checkArgument(newValue.compareTo(oldValue) >= 0, "retrograde clock detected");
        _user = newValue;
        return newValue.minus(oldValue);
    }

    public Duration getCombined()
    {
        return _combined;
    }

    public Duration getUser()
    {
        return _user;
    }

    public Duration getSystem()
    {
        return _combined.minus(_user);
    }

    public void assertValues()
    {
        checkState(_combined.compareTo(_user) >= 0, "user value greater than combined");
    }
}
